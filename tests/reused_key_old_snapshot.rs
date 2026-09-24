//! A reused primary key hides its old version from every lookup that goes through the index.
//!
//! # The gap
//!
//! E63 lets `DELETE k; INSERT k` reuse a key. It writes the new row into a NEW heap slot and
//! repoints the primary-index entry at it (`execution::insert`, the `upsert` after `heap.insert`).
//! The new version's `prev` is zero. The dead version stays in its old slot, reachable from nothing
//! but a sequential scan.
//!
//! So a reader whose snapshot predates the reuse gets two answers for one row:
//!
//! - **scan:** `SeqScan` walks every slot, finds the dead version's slot, and `resolve_visibility`
//!   says it is still live for this reader (its deleter had not committed when the reader began).
//!   The row is there.
//! - **by key:** `IndexScan` (and `SecondaryIndexScan`, which resolves through the primary index)
//!   lands on the NEW slot. Its `begin_ts` is not committed for this reader, and `prev` is zero, so
//!   `resolve_visibility` returns `None`. The row is not there.
//!
//! Found by the D194 lane (`frontier/lane_d194_fork_snapshot.md` §6.1, probes 2 and 3), which makes
//! agent branches read as of their fork and so widens it. It predates D194: a plain `BEGIN` shows
//! it with no agent anywhere. E63's own comment said nothing needs the index to reach an old
//! version because there is no temporal `AS OF`. An explicit transaction's snapshot is exactly
//! that, and it has existed the whole time.
//!
//! # What each test pins
//!
//! RED at the base of this lane (`9aa6968`), the gap itself:
//! - `an_old_snapshot_finds_a_reused_key_by_key_as_its_scan_does`
//! - `an_older_reader_still_finds_a_row_deleted_and_reinserted_in_one_transaction`
//! - `a_write_by_key_through_an_old_snapshot_conflicts_as_a_write_by_scan_does`
//! - `a_secondary_lookup_through_an_old_snapshot_finds_the_reused_row`
//! - `an_old_snapshot_finds_a_reused_key_whose_new_row_had_to_move`
//! - `building_an_index_after_a_same_value_reuse_does_not_return_the_row_twice`, a neighbour the
//!   same two-slot heap causes: `create_index`'s backfill posts one entry per SLOT.
//!
//! GREEN at base, and each one guards a specific wrong fix:
//! - `a_reader_that_saw_the_delete_does_not_see_the_old_row_come_back`: a fix that re-stamps the
//!   archived dead version's `end_ts` with the inserter's id (copying UPDATE's archive line
//!   verbatim) resurrects the row for this reader.
//! - `a_reuse_that_relocates_emits_one_delete_not_two`: a fix whose new row does not fit the dead
//!   row's page relocates it, and `HeapFileManager::update` logs that as a `HeapDelete` of the dead
//!   image. The change feed must not report it as a second DELETE.
//! - `an_agent_branch_point_lookup_agrees_with_its_own_scan`: at base an agent branch reads the
//!   LATEST snapshot, so this cannot fail here. It is the tripwire for D194: D194 without this fix
//!   turns it red (probe 2 measured the split on the D194 lane tree).
//!
//! Every "by key" read below first proves, with `EXPLAIN`, that the plan really is an index scan.
//! Without that, an optimizer that picked a sequential scan would turn every red test green for a
//! reason that has nothing to do with the fix.

use std::fs::OpenOptions;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::logical::{ChangeOp, LogicalDecoder};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    wal: Arc<WalManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("reuse.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("reuse.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal.clone());
        Db { catalog, bp, txn, wal, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn rows(&mut self, sql: &str, s: &mut Session) -> Vec<Vec<Value>> {
        match self.ok(sql, s) {
            Outcome::Rows(r) => r,
            _ => panic!("`{sql}` did not return rows"),
        }
    }

    /// Fails unless the plan for `select` reads the index named by `needle`.
    fn assert_plan(&mut self, select: &str, needle: &str, s: &mut Session) {
        let sql = format!("EXPLAIN {select}");
        match self.ok(&sql, s) {
            Outcome::Explain(plan) => assert!(
                plan.contains(needle),
                "premise failed: `{select}` does not plan as `{needle}`, so this test would not be \
                 measuring the path it names:\n{plan}"
            ),
            _ => panic!("`{sql}` did not explain"),
        }
    }

    /// Where the primary index says `key` lives, read straight from the tree.
    fn primary_rid(&self, table: &str, key: i32) -> Option<RecordId> {
        let entry = self.catalog.get_table(table).expect("table");
        BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, self.bp.clone())
            .search(&Value::Integer(key))
            .expect("primary search")
    }
}

fn row(id: i32, qty: i32) -> Vec<Value> {
    vec![Value::Integer(id), Value::Integer(qty)]
}

fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort();
    rows
}

/// The D194 lane's probe fixture, unchanged: two rows, and row 3 is the one that gets reused.
fn seed(db: &mut Db, main: &mut Session) {
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", main);
    db.ok("INSERT INTO inventory VALUES (1, 20);", main);
    db.ok("INSERT INTO inventory VALUES (3, 7);", main);
}

const BY_KEY: &str = "SELECT id, qty FROM inventory WHERE id = 3;";
const ALL: &str = "SELECT id, qty FROM inventory;";
const PRIMARY: &str = "Index scan on inventory (col 0";

/// **The gap, in the lane's own shape (probe 3).**
#[test]
fn an_old_snapshot_finds_a_reused_key_by_key_as_its_scan_does() {
    let mut db = Db::new();
    let mut main = db.session();
    seed(&mut db, &mut main);
    db.assert_plan(BY_KEY, PRIMARY, &mut main);

    // The snapshot is taken at BEGIN (`TxnManager::begin_locked` stores it in the transaction's
    // table entry), so nothing needs to be read to pin it.
    let mut old = db.session();
    db.ok("BEGIN;", &mut old);
    db.ok("DELETE FROM inventory WHERE id = 3;", &mut main);
    db.ok("INSERT INTO inventory VALUES (3, 8);", &mut main);

    // Premise, and the guard against a fix that leaves the dead version in the heap AND chains to
    // it: the scan through the old snapshot has (3, 7), exactly once.
    assert_eq!(
        sorted(db.rows(ALL, &mut old)),
        vec![row(1, 20), row(3, 7)],
        "the old snapshot's scan is not (1, 20), (3, 7) exactly once each"
    );
    assert_eq!(
        db.rows(BY_KEY, &mut old),
        vec![row(3, 7)],
        "the old snapshot's scan shows (3, 7) but its lookup by key does not: the index reaches \
         only the new version, which this snapshot cannot see, and nothing links it to the old one"
    );
    db.ok("COMMIT;", &mut old);

    // Anti-vacuity: a reader that begins after the reuse sees (3, 8) both ways. A fix that made the
    // lookup return the OLDEST version would pass everything above and fail here.
    let mut fresh = db.session();
    assert_eq!(db.rows(BY_KEY, &mut fresh), vec![row(3, 8)], "a new reader does not see the new row by key");
    assert_eq!(sorted(db.rows(ALL, &mut fresh)), vec![row(1, 20), row(3, 8)], "a new reader's scan is wrong");
}

/// **The same gap when one transaction does both halves.**
///
/// The dead version's `end_ts` is the writer's OWN id here, which `is_commited_for_me` accepts
/// (`ts == self.txn_id`), so E63 reuses the key inside the transaction. An older reader must still
/// see (3, 7) before the writer commits and after it.
#[test]
fn an_older_reader_still_finds_a_row_deleted_and_reinserted_in_one_transaction() {
    let mut db = Db::new();
    let mut main = db.session();
    seed(&mut db, &mut main);
    db.assert_plan(BY_KEY, PRIMARY, &mut main);

    let mut old = db.session();
    db.ok("BEGIN;", &mut old);

    let mut writer = db.session();
    db.ok("BEGIN;", &mut writer);
    db.ok("DELETE FROM inventory WHERE id = 3;", &mut writer);
    db.ok("INSERT INTO inventory VALUES (3, 8);", &mut writer);
    // Anti-vacuity: the writer sees its own new row by key.
    assert_eq!(db.rows(BY_KEY, &mut writer), vec![row(3, 8)], "the writer cannot see its own reinsert");

    assert_eq!(
        db.rows(BY_KEY, &mut old),
        vec![row(3, 7)],
        "while the writer is still open, an older reader lost (3, 7) by key"
    );
    db.ok("COMMIT;", &mut writer);
    assert_eq!(
        db.rows(BY_KEY, &mut old),
        vec![row(3, 7)],
        "after the writer committed, an older reader lost (3, 7) by key"
    );
    assert_eq!(sorted(db.rows(ALL, &mut old)), vec![row(1, 20), row(3, 7)], "the older reader's scan is wrong");
    db.ok("COMMIT;", &mut old);
}

/// **Guard: a reader that already saw the DELETE must not see the old row come back.**
///
/// GREEN at base. It exists for the fix. UPDATE archives the version it supersedes after stamping
/// that version's `end_ts` with its own id, because the version was live. A reused key's dead
/// version is already ended by its deleter. Re-stamping it with the INSERTER's id makes it live
/// again for exactly this reader: the deleter has committed for it, and the inserter has not.
#[test]
fn a_reader_that_saw_the_delete_does_not_see_the_old_row_come_back() {
    let mut db = Db::new();
    let mut main = db.session();
    seed(&mut db, &mut main);
    db.assert_plan(BY_KEY, PRIMARY, &mut main);

    db.ok("DELETE FROM inventory WHERE id = 3;", &mut main);
    let mut between = db.session();
    db.ok("BEGIN;", &mut between);
    db.ok("INSERT INTO inventory VALUES (3, 8);", &mut main);

    assert_eq!(
        db.rows(BY_KEY, &mut between),
        Vec::<Vec<Value>>::new(),
        "a reader whose snapshot includes the DELETE and not the reinsert found row 3 by key"
    );
    assert_eq!(
        sorted(db.rows(ALL, &mut between)),
        vec![row(1, 20)],
        "a reader whose snapshot includes the DELETE and not the reinsert found row 3 by scan"
    );
    db.ok("COMMIT;", &mut between);
}

/// **A write by key through an old snapshot is silently a no-op; by scan it is a conflict.**
///
/// The same row, reached two ways by the same kind of statement. The sequential path finds the
/// dead version and `check_write_conflict` refuses it: its deleter is not committed for this
/// transaction. The index path finds nothing and reports `Affected(0)`, so the statement "succeeds"
/// having done nothing to a row its own scan shows. Postgres at REPEATABLE READ raises a
/// serialization failure here, which is what the sequential path already does.
#[test]
fn a_write_by_key_through_an_old_snapshot_conflicts_as_a_write_by_scan_does() {
    let mut db = Db::new();
    let mut main = db.session();
    seed(&mut db, &mut main);
    db.assert_plan(BY_KEY, PRIMARY, &mut main);
    // `build_scan` plans an UPDATE's or DELETE's rows with the SELECT optimizer (D178), so the
    // SELECT plan is the child plan of the write.
    db.assert_plan("SELECT id, qty FROM inventory WHERE qty = 7;", "Sequential scan on inventory", &mut main);

    let mut by_scan = db.session();
    db.ok("BEGIN;", &mut by_scan);
    let mut by_key = db.session();
    db.ok("BEGIN;", &mut by_key);
    let mut delete_by_key = db.session();
    db.ok("BEGIN;", &mut delete_by_key);

    db.ok("DELETE FROM inventory WHERE id = 3;", &mut main);
    db.ok("INSERT INTO inventory VALUES (3, 8);", &mut main);

    fn conflict(res: Result<Outcome, FerroError>, what: &str) {
        match res {
            Err(e @ FerroError::Txn(_)) => {
                assert!(e.to_string().contains("conflict"), "{what}: refused, but not as a write conflict: {e}")
            }
            Err(e) => panic!("{what}: refused, but not as a transaction conflict: {e}"),
            Ok(Outcome::Affected(n)) => panic!(
                "{what}: succeeded with {n} row(s) affected against a row whose deleter is not \
                 committed for this transaction"
            ),
            Ok(_) => panic!("{what}: succeeded with an unexpected outcome"),
        }
    }

    // Control: the sequential path conflicts today.
    conflict(db.exec("UPDATE inventory SET qty = 70 WHERE qty = 7;", &mut by_scan), "UPDATE by scan");
    // THE DEFECT: the same row by key.
    conflict(db.exec("UPDATE inventory SET qty = 70 WHERE id = 3;", &mut by_key), "UPDATE by key");
    conflict(db.exec("DELETE FROM inventory WHERE id = 3;", &mut delete_by_key), "DELETE by key");
}

/// Enough rows, and fresh statistics, that the optimizer takes the secondary index on `qty`.
///
/// Arithmetic from `cost_model::cost`, stated so it can be checked rather than trusted: with 1000
/// rows and 1000 distinct values, the index point lookup costs 2*4 + 1 + 4 + 4 = 17. The sequential
/// scan plus filter costs 8 pages + 10 + 10 = 28. At 2 rows the sequential scan wins, which is why
/// the lane's two-row fixture cannot reach this path. `assert_plan` checks the result either way.
fn seed_indexed(db: &mut Db, main: &mut Session) {
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", main);
    db.ok("CREATE INDEX iq ON inventory (qty);", main);
    db.ok("BEGIN;", main);
    for i in 1..=1000 {
        db.ok(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10), main);
    }
    db.ok("COMMIT;", main);
}

const SECONDARY: &str = "Index scan on inventory (col 1";

/// **The same gap through a secondary index.** `SecondaryIndexScan` turns each `(value, pk)` entry
/// into a heap read through `primary_index.search(&pk)`, so it lands on the same new slot.
#[test]
fn a_secondary_lookup_through_an_old_snapshot_finds_the_reused_row() {
    let mut db = Db::new();
    let mut main = db.session();
    seed_indexed(&mut db, &mut main);
    db.ok("ANALYZE inventory;", &mut main);
    let by_value = "SELECT id, qty FROM inventory WHERE qty = 30;";
    db.assert_plan(by_value, SECONDARY, &mut main);

    let mut old = db.session();
    db.ok("BEGIN;", &mut old);
    db.ok("DELETE FROM inventory WHERE id = 3;", &mut main);
    db.ok("INSERT INTO inventory VALUES (3, 31);", &mut main);

    // Premise: the old snapshot's scan has (3, 30).
    let scanned: Vec<Vec<Value>> = db
        .rows(ALL, &mut old)
        .into_iter()
        .filter(|r| r[0] == Value::Integer(3))
        .collect();
    assert_eq!(scanned, vec![row(3, 30)], "the old snapshot's scan does not hold (3, 30) exactly once");

    assert_eq!(
        db.rows(by_value, &mut old),
        vec![row(3, 30)],
        "the old snapshot finds (3, 30) by scan but not through the secondary index"
    );
    db.ok("COMMIT;", &mut old);

    // Anti-vacuity: the new value is found by a new reader.
    let mut fresh = db.session();
    assert_eq!(
        db.rows("SELECT id, qty FROM inventory WHERE qty = 31;", &mut fresh),
        vec![row(3, 31)],
        "a new reader does not find the reinserted row by its new value"
    );
}

/// **Neighbour: an index built after a same-value reuse returns the row twice.**
///
/// `Catalog::create_index` backfills one `(value, pk)` entry per heap SLOT and does not
/// de-duplicate. Its full-text twin's doc (`create_fulltext_index`, point 2) says it "gets away with
/// it", but that only holds when the reused row's value differs. With the same value, E63's two
/// slots post `(40, 4)` twice, and `SecondaryIndexScan` yields one row per entry. INFERRED from
/// source, never run: this is the red test that settles it. It goes away with the gap's fix, because
/// the heap then holds one slot per key.
#[test]
fn building_an_index_after_a_same_value_reuse_does_not_return_the_row_twice() {
    let mut db = Db::new();
    let mut main = db.session();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut main);
    db.ok("BEGIN;", &mut main);
    for i in 1..=1000 {
        db.ok(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10), &mut main);
    }
    db.ok("COMMIT;", &mut main);
    db.ok("DELETE FROM inventory WHERE id = 4;", &mut main);
    db.ok("INSERT INTO inventory VALUES (4, 40);", &mut main);

    db.ok("CREATE INDEX iq ON inventory (qty);", &mut main);
    db.ok("ANALYZE inventory;", &mut main);
    let by_value = "SELECT id, qty FROM inventory WHERE qty = 40;";
    db.assert_plan(by_value, SECONDARY, &mut main);

    assert_eq!(
        db.rows(by_value, &mut main),
        vec![row(4, 40)],
        "the index built over a reused key returns its row once per heap slot"
    );
}

/// A page with no room for the reused row: rows 1 and 2 fill it, and row 1 comes back larger.
///
/// Arithmetic from `Tuple::serialize` and `heap_page`: an `(INTEGER, VARCHAR)` tuple is
/// 24 (version header) + 1 (null bitmap) + 3 (padding) + 4 + 2 (length) + len bytes. Row 1 is 35
/// bytes and row 2 is 3934, plus two 4-byte slots, against 4073 usable. That leaves 96 free, and the
/// reused row 1 is 234. An in-place rewrite of row 1's slot therefore cannot fit and has to relocate.
/// The premise assertions check both halves of that.
fn packed_page(db: &mut Db, main: &mut Session) -> RecordId {
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));", main);
    db.ok("INSERT INTO notes VALUES (1, 'a');", main);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(3900)), main);
    let one = db.primary_rid("notes", 1).expect("row 1 is indexed");
    let two = db.primary_rid("notes", 2).expect("row 2 is indexed");
    assert_eq!(one.page_id, two.page_id, "premise failed: rows 1 and 2 are not on one page, so it is not full");
    one
}

fn big_note() -> String {
    "y".repeat(200)
}

/// **The gap when the new row cannot take the dead row's place on its page.**
#[test]
fn an_old_snapshot_finds_a_reused_key_whose_new_row_had_to_move() {
    let mut db = Db::new();
    let mut main = db.session();
    let before = packed_page(&mut db, &mut main);
    db.assert_plan("SELECT id, note FROM notes WHERE id = 1;", "Index scan on notes (col 0", &mut main);

    let mut old = db.session();
    db.ok("BEGIN;", &mut old);
    db.ok("DELETE FROM notes WHERE id = 1;", &mut main);
    db.ok(&format!("INSERT INTO notes VALUES (1, '{}');", big_note()), &mut main);
    let after = db.primary_rid("notes", 1).expect("row 1 is indexed");
    assert_ne!(after, before, "premise failed: the reused row did not move, so this is not the relocation case");

    assert_eq!(
        db.rows("SELECT id, note FROM notes WHERE id = 1;", &mut old),
        vec![vec![Value::Integer(1), Value::Varchar("a".into())]],
        "the old snapshot lost (1, 'a') by key after the reuse moved the row to another page"
    );
    db.ok("COMMIT;", &mut old);

    let mut fresh = db.session();
    assert_eq!(
        db.rows("SELECT id, note FROM notes WHERE id = 1;", &mut fresh),
        vec![vec![Value::Integer(1), Value::Varchar(big_note())]],
        "a new reader does not find the relocated reinsert by key"
    );
}

/// **Guard: a reuse that relocates is still one DELETE and one INSERT in the change feed.**
///
/// GREEN at base, where E63 never relocates anything: it calls `heap.insert`. It guards the fix.
/// A reuse written into the dead row's slot that does not fit goes through `HeapFileManager::update`'s
/// relocation arm, which logs `HeapDelete` of the slot's current bytes, and those bytes are the DEAD
/// version. Decoded like any other `HeapDelete`, that is a second DELETE of a row the consumer
/// already deleted.
#[test]
fn a_reuse_that_relocates_emits_one_delete_not_two() {
    let mut db = Db::new();
    let mut main = db.session();
    let before = packed_page(&mut db, &mut main);
    db.ok("DELETE FROM notes WHERE id = 1;", &mut main);
    db.ok(&format!("INSERT INTO notes VALUES (1, '{}');", big_note()), &mut main);
    assert_ne!(
        db.primary_rid("notes", 1).expect("row 1 is indexed"),
        before,
        "premise failed: the reused row did not move, so this is not the relocation case"
    );

    db.wal.flush().unwrap();
    let decoder = LogicalDecoder::new(&db.catalog);
    assert!(decoder.known_tables() > 0, "the decoder resolved no tables, so every change would decode as unresolved");
    let out = decoder
        .decode(&db.wal, db.wal.base_lsn.load(Ordering::SeqCst), db.wal.next_lsn.load(Ordering::SeqCst))
        .expect("decode");
    assert!(out.is_complete(), "the feed is incomplete, so no sequence claim below is sound: {out:?}");

    let key_one: Vec<&'static str> = out
        .events
        .iter()
        .filter(|e| {
            let image = match &e.op {
                ChangeOp::Insert { new } | ChangeOp::Update { new, .. } => Some(new),
                ChangeOp::Delete { old } => Some(old),
                _ => None,
            };
            matches!(image.and_then(|r| r.first()), Some(Value::Integer(1)))
        })
        .map(|e| e.op.name())
        .collect();
    assert_eq!(
        key_one,
        vec!["INSERT", "DELETE", "INSERT"],
        "key 1's history is not insert, delete, insert: a relocated dead version leaked into the feed"
    );
    let deletes = out.events.iter().filter(|e| matches!(e.op, ChangeOp::Delete { .. })).count();
    assert_eq!(deletes, 1, "one SQL DELETE ran and the feed carries {deletes}");
    match out.events.iter().rev().find(|e| matches!(e.op, ChangeOp::Insert { .. })).map(|e| &e.op) {
        Some(ChangeOp::Insert { new }) => assert_eq!(
            new,
            &vec![Value::Integer(1), Value::Varchar(big_note())],
            "the last INSERT does not carry the reused row's new values"
        ),
        other => panic!("no INSERT at the end of the feed: {other:?}"),
    }
}

/// **Tripwire for D194: an agent branch's lookup by key agrees with its own scan.**
///
/// GREEN at base, and it cannot be otherwise here: at `9aa6968` an agent branch reads shared tables
/// through the LATEST snapshot (`scan_table_where`: `read_snapshot_cached()`), so both paths see
/// (3, 8). D194 pins the branch's reads at its fork, and on the D194 lane tree the scan showed
/// (3, 7) while the lookup returned nothing (probe 2, `bench/d194_fork_snapshot/probe_run1.txt`).
/// So this is red exactly when D194 lands without the fix, and green with neither or both.
///
/// It asserts agreement, not a value, on purpose. Pinning (3, 8) would pin read-latest, which D194
/// removes, and pinning (3, 7) would fail here for a reason unrelated to this gap. The scan's row is
/// constrained to the two legal answers so that "both empty" cannot pass.
#[test]
fn an_agent_branch_point_lookup_agrees_with_its_own_scan() {
    let mut db = Db::new();
    let mut main = db.session();
    seed(&mut db, &mut main);
    db.assert_plan(BY_KEY, PRIMARY, &mut main);

    let mut agent = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut agent);
    db.ok("DELETE FROM inventory WHERE id = 3;", &mut main);
    db.ok("INSERT INTO inventory VALUES (3, 8);", &mut main);

    let scanned: Vec<Vec<Value>> = db
        .rows(ALL, &mut agent)
        .into_iter()
        .filter(|r| r[0] == Value::Integer(3))
        .collect();
    assert_eq!(scanned.len(), 1, "the branch's scan does not hold row 3 exactly once: {scanned:?}");
    assert!(
        scanned[0] == row(3, 7) || scanned[0] == row(3, 8),
        "the branch's scan holds a row 3 that no snapshot ever had: {scanned:?}"
    );
    assert_eq!(
        db.rows(BY_KEY, &mut agent),
        scanned,
        "the branch's lookup by key disagrees with its own scan"
    );
}
