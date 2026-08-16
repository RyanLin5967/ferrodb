//! E14 — schema in the feed, so a consumer is not left guessing.
//!
//! The catalog is written outside the WAL, which meant the change feed carried rows and not the
//! schema change that preceded them: new columns appeared with no warning and no way to adapt.
//! `CREATE TABLE` is now logged, so the feed carries it and a decoder can track schema *as of each
//! point in the log* instead of assuming today's catalog describes yesterday's rows.
//!
//! The demonstration is the last test: a decoder built with **no catalog at all** decodes a full
//! history correctly, because the log told it what the table was.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::jsonl::write_feed;
use ferrodb::replication::logical::{Decoded, LogicalDecoder, SchemaChange};
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
    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(dir.path().join(format!("{tag}.db"))).unwrap();
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
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    }

    fn decode_with(&self, decoder: &LogicalDecoder) -> Decoded {
        use std::sync::atomic::Ordering;
        self.wal.flush().unwrap();
        decoder
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode")
    }
}

#[test]
fn create_table_reaches_the_feed_as_a_schema_event() {
    let mut d = db("ddl");
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, item VARCHAR(32), qty INTEGER);");
    d.sql("INSERT INTO inventory VALUES (1, 'widget', 10);");

    let out = d.decode_with(&LogicalDecoder::new(&d.catalog));

    assert_eq!(
        out.schema_changes.len(),
        1,
        "expected one schema change, got {:?}",
        out.schema_changes
    );
    let (_, table, change) = &out.schema_changes[0];
    assert_eq!(table, "inventory");
    assert_eq!(*change, SchemaChange::CreateTable);

    let mut buf = Vec::new();
    write_feed(&out.events, &mut buf).unwrap();
    let feed = String::from_utf8(buf).unwrap();

    assert!(feed.contains("\"op\":\"CREATE_TABLE\""), "no schema event in the feed:\n{feed}");
    // The column list must be complete enough for a consumer to recreate the table, VARCHAR width
    // included — a consumer that creates VARCHAR without the width guesses, and guesses truncate.
    assert!(feed.contains("\"name\":\"item\",\"type\":\"VARCHAR(32)\""), "{feed}");
    assert!(feed.contains("\"name\":\"id\",\"type\":\"INTEGER\",\"nullable\":false"), "{feed}");
    assert!(feed.contains("\"name\":\"qty\",\"type\":\"INTEGER\",\"nullable\":true"), "{feed}");

    // The schema event must precede the row it describes, or a consumer applies a row to a table it
    // has not created yet.
    let schema_at = feed.find("CREATE_TABLE").expect("no schema event");
    let insert_at = feed.find("INSERT").expect("no insert event");
    assert!(schema_at < insert_at, "the schema event came after the row it describes:\n{feed}");
}

/// **The point of the row.** A decoder with no catalog whatsoever decodes the whole history,
/// because the log carries the schema.
#[test]
fn a_decoder_with_no_catalog_learns_the_table_from_the_log() {
    let mut d = db("blank");
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);");
    for i in 1..=5 {
        d.sql(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10));
    }
    d.sql("UPDATE inventory SET qty = 999 WHERE id = 1;");

    let out = d.decode_with(&LogicalDecoder::blank());

    assert!(
        out.unresolved.is_empty(),
        "a blank decoder could not attribute records to a table even though the log carries the \
         schema: {:?}",
        out.unresolved
    );
    let rows = out
        .events
        .iter()
        .filter(|e| e.op.name() == "INSERT" || e.op.name() == "UPDATE")
        .count();
    assert!(
        rows >= 6,
        "a blank decoder recovered only {rows} row events from 5 inserts and an update"
    );
    assert_eq!(out.schema_changes.len(), 1, "the schema change was not seen");

    // And the values are right, not merely present — a decoder that learned the wrong schema would
    // still produce events, just nonsense ones.
    let mut buf = Vec::new();
    write_feed(&out.events, &mut buf).unwrap();
    let feed = String::from_utf8(buf).unwrap();
    assert!(feed.contains("\"after\":{\"id\":3,\"qty\":30}"), "wrong values decoded:\n{feed}");
    assert!(feed.contains("\"after\":{\"id\":1,\"qty\":999}"), "the update decoded wrong:\n{feed}");
}

/// Two tables must not be confused with one another when learned from the log.
#[test]
fn a_blank_decoder_keeps_two_tables_apart() {
    let mut d = db("two");
    d.sql("CREATE TABLE a (id INTEGER NOT NULL, v INTEGER);");
    d.sql("CREATE TABLE b (id INTEGER NOT NULL, name VARCHAR(16));");
    d.sql("INSERT INTO a VALUES (1, 42);");
    d.sql("INSERT INTO b VALUES (1, 'hello');");

    let out = d.decode_with(&LogicalDecoder::blank());
    assert!(out.unresolved.is_empty(), "unresolved: {:?}", out.unresolved);
    assert_eq!(out.schema_changes.len(), 2, "{:?}", out.schema_changes);

    let mut buf = Vec::new();
    write_feed(&out.events, &mut buf).unwrap();
    let feed = String::from_utf8(buf).unwrap();

    let a_line = feed
        .lines()
        .find(|l| l.contains("\"table\":\"a\"") && l.contains("INSERT"))
        .unwrap_or_else(|| panic!("no insert for table a:\n{feed}"));
    assert!(a_line.contains("\"v\":42"), "table a decoded with the wrong schema: {a_line}");

    let b_line = feed
        .lines()
        .find(|l| l.contains("\"table\":\"b\"") && l.contains("INSERT"))
        .unwrap_or_else(|| panic!("no insert for table b:\n{feed}"));
    assert!(b_line.contains("\"name\":\"hello\""), "table b decoded with the wrong schema: {b_line}");
}

/// **The generalisation test.** A checkpoint fires automatically every 256 commits, and it
/// truncates the log. If the schema is only re-established by the *manual* checkpoint that
/// `CREATE TABLE` performs, then a feed works right up until a table gets busy and then silently
/// stops being decodable — the exact shape of E8's "40 rows passes for a reason that does not
/// generalise".
///
/// So this writes past the threshold and asserts the truncation actually happened before claiming
/// anything.
#[test]
fn the_schema_survives_an_automatic_checkpoint() {
    use std::sync::atomic::Ordering;

    let mut d = db("auto");
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);");
    for i in 1..=300 {
        d.sql(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 2));
    }
    d.wal.flush().unwrap();

    // Anti-vacuity: without a truncation this is just the small test again.
    let base = d.wal.base_lsn.load(Ordering::SeqCst);
    assert!(
        base > 1,
        "the log never truncated at 300 commits, so this test did not exercise an automatic \
         checkpoint. Check CHECKPOINT_INTERVAL."
    );

    let out = d.decode_with(&LogicalDecoder::blank());
    assert!(
        out.unresolved.is_empty(),
        "after an automatic checkpoint a blank decoder could not attribute records to a table: \
         {:?} — the schema did not survive the truncation",
        out.unresolved
    );
    assert_eq!(
        out.schema_changes.len(),
        1,
        "expected the schema to be re-established exactly once at the new log base, got {:?}",
        out.schema_changes
    );
    let rows = out.events.iter().filter(|e| e.op.name() == "INSERT").count();
    assert!(rows > 0, "no rows survived the truncation to be decoded");
}
