//! E9 — logical decoding against **real SQL**, not fixtures.
//!
//! The unit tests in `src/replication/logical.rs` build WAL records by hand. That proves the
//! decoder decodes what I think the engine writes, which is a different and much weaker claim than
//! that it decodes what the engine *actually* writes. This runs genuine `INSERT`/`UPDATE`/`DELETE`
//! through the parser, binder, planner, executor and transaction manager, then decodes the log
//! those statements produced and checks the change events against the SQL.
//!
//! In particular it is the only thing that validates the load-bearing assumption underneath the
//! whole module: that a WAL record's `dir_root` is the same number as a catalog entry's
//! `first_directory_page_id`. If that were wrong, every unit test above would still pass and every
//! real change would decode as unresolved.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::logical::{ChangeOp, Decoded, LogicalDecoder};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db(tag: &str) -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("{tag}.db"));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{tag}.wal"))).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { _dir: dir, catalog, wal, bp, txn, session: Session::new() }
}

impl Db {
    fn sql(&mut self, sql: &str) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(
            stmts.remove(0),
            &mut self.catalog,
            self.bp.clone(),
            self.txn.clone(),
            &mut self.session,
        )
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    }

    fn decode(&self) -> Decoded {
        use std::sync::atomic::Ordering;
        self.wal.flush().unwrap();
        let decoder = LogicalDecoder::new(&self.catalog);
        assert!(
            decoder.known_tables() > 0,
            "the decoder resolved no tables from a catalog that has some; every change would \
             decode as unresolved and the test would prove nothing"
        );
        decoder
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode")
    }
}

fn ints(vals: &[i32]) -> Vec<Value> {
    vals.iter().map(|v| Value::Integer(*v)).collect()
}

#[test]
fn real_sql_decodes_into_the_changes_that_sql_described() {
    let mut d = db("sql");
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inventory VALUES (1, 10);");
    d.sql("INSERT INTO inventory VALUES (2, 20);");
    d.sql("UPDATE inventory SET qty = 999 WHERE id = 1;");
    d.sql("DELETE FROM inventory WHERE id = 2;");

    let out = d.decode();

    // The assumption the whole module rests on: a record's `dir_root` really is the catalog's
    // `first_directory_page_id`. If it were not, this would be the assertion that said so.
    assert!(
        out.unresolved.is_empty(),
        "real SQL produced records this decoder could not attribute to a table: {:?}. \
         `dir_root` may not be `first_directory_page_id` after all.",
        out.unresolved
    );
    assert!(
        out.undecodable.is_empty(),
        "records were attributed to a table but their bytes would not decode against its \
         schema: {:?}",
        out.undecodable
    );
    assert!(
        !out.events.is_empty(),
        "four data statements produced no change events at all; the decoder is not seeing the \
         engine's writes"
    );

    let kinds: Vec<&str> = out.events.iter().map(|e| e.op.name()).collect();
    assert!(
        kinds.contains(&"INSERT") && kinds.contains(&"UPDATE") && kinds.contains(&"DELETE"),
        "not every statement shape reached the feed: {kinds:?}"
    );
    for e in &out.events {
        assert_eq!(e.table, "inventory", "a change was attributed to the wrong table: {e:?}");
    }

    // The values, not just the shapes. A decoder that reported the right operations against
    // garbage columns would pass everything above.
    let inserted: Vec<&Vec<Value>> = out
        .events
        .iter()
        .filter_map(|e| match &e.op {
            ChangeOp::Insert { new } => Some(new),
            _ => None,
        })
        .collect();
    assert!(
        inserted.iter().any(|v| **v == ints(&[1, 10])),
        "the row (1, 10) never appeared as an insert: {inserted:?}"
    );
    assert!(
        inserted.iter().any(|v| **v == ints(&[2, 20])),
        "the row (2, 20) never appeared as an insert: {inserted:?}"
    );

    let update = out
        .events
        .iter()
        .find_map(|e| match &e.op {
            ChangeOp::Update { old, new } => Some((old, new)),
            _ => None,
        })
        .expect("no update event");
    assert_eq!(*update.0, ints(&[1, 10]), "the update's BEFORE image is wrong");
    assert_eq!(*update.1, ints(&[1, 999]), "the update's AFTER image is wrong");

    let deleted = out
        .events
        .iter()
        .find_map(|e| match &e.op {
            ChangeOp::Delete { old } => Some(old),
            _ => None,
        })
        .expect("no delete event");
    assert_eq!(*deleted, ints(&[2, 20]), "the delete carried the wrong row");
}

/// Events must arrive in commit order, and every commit lsn must be at or after the change it
/// released. A feed that hands a consumer changes out of order is worse than one that hands it
/// none.
#[test]
fn the_feed_is_ordered_and_every_change_is_attributed_to_a_commit() {
    let mut d = db("order");
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
    for i in 1..=25 {
        d.sql(&format!("INSERT INTO t VALUES ({i}, {});", i * 3));
    }

    let out = d.decode();
    assert!(out.events.len() >= 25, "only {} events for 25 inserts", out.events.len());

    let mut last_commit = 0u64;
    for e in &out.events {
        assert!(
            e.commit_lsn >= last_commit,
            "commit lsns went backwards: {} after {last_commit}",
            e.commit_lsn
        );
        assert!(
            e.commit_lsn >= e.lsn,
            "a change at lsn {} was released by a commit at {}, which precedes it",
            e.lsn,
            e.commit_lsn
        );
        last_commit = e.commit_lsn;
    }
    assert!(out.open.is_empty(), "transactions left open after autocommit: {:?}", out.open);
}

/// Decoding twice over the same range must produce the same feed. A change feed that is not
/// deterministic cannot be resumed, and resuming is the only way a consumer survives a restart.
#[test]
fn decoding_the_same_range_twice_gives_the_same_feed() {
    let mut d = db("stable");
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
    d.sql("INSERT INTO t VALUES (1, 1);");
    d.sql("INSERT INTO t VALUES (2, 2);");
    d.sql("UPDATE t SET v = 7 WHERE id = 1;");

    let a = d.decode();
    let b = d.decode();
    assert!(!a.events.is_empty(), "nothing was decoded, so equality would be vacuous");
    assert_eq!(a.events, b.events, "two decodes of the same log range disagreed");
}

/// A decode over a sub-range must not invent or lose changes: the halves must reassemble into the
/// whole. This is what makes a consumer able to resume from its own position.
#[test]
fn two_adjacent_ranges_reassemble_into_the_whole_feed() {
    use std::sync::atomic::Ordering;

    let mut d = db("split");
    d.sql("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);");
    for i in 1..=20 {
        d.sql(&format!("INSERT INTO t VALUES ({i}, {i});"));
    }
    d.wal.flush().unwrap();

    let whole = d.decode();
    let decoder = LogicalDecoder::new(&d.catalog);
    let base = d.wal.base_lsn.load(Ordering::SeqCst);
    let end = d.wal.next_lsn.load(Ordering::SeqCst);

    // Split at a commit boundary taken from the feed itself, so neither half cuts a transaction in
    // two. Splitting mid-transaction is a legitimate case, but it is the *withheld* case, and it is
    // covered by the unit test for open transactions.
    // `commit_end_lsn`, NOT `commit_lsn + 1`. LSNs are byte offsets and records are variable
    // length, so one byte past a commit lands inside it — which is how this test found the need for
    // the field in the first place (`eof before finished record`).
    let split = whole.events[whole.events.len() / 2].commit_end_lsn;
    assert!(split > base && split < end, "the split point is not inside the range");

    let first = decoder.decode(&d.wal, base, split).unwrap();
    let second = decoder.decode(&d.wal, split, end).unwrap();

    assert!(!first.events.is_empty() && !second.events.is_empty(), "one half is empty");
    let rejoined: Vec<_> = first.events.iter().chain(second.events.iter()).cloned().collect();
    assert_eq!(
        rejoined, whole.events,
        "decoding in two ranges did not reassemble into the single-range feed"
    );
}

