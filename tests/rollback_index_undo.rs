//! D202 — a rollback must take back the primary-index writes its transaction made.
//!
//! # The defect (READ-FROM-SOURCE; lead-verified at `9aa6968`)
//!
//! `TxnManager::abort` undoes `Heap*` records and nothing else. Index pages are not logged, and no
//! abort path touches a tree. So after `BEGIN; INSERT (5, ..); ROLLBACK;`, the primary entry for 5
//! still points at the slot `undo_insert` freed. `HeapPage::read` answers `SlotDeleted` for it,
//! and every path that reads through the entry stops with that error: `IndexScan`,
//! `SecondaryIndexScan` (it resolves each entry through the primary index), and `Insert::execute`
//! (it reads the entry's slot before deciding uniqueness). Key 5 cannot be inserted again, ever.
//!
//! # Why the fix undoes the index, instead of treating a freed slot as an absent row
//!
//! E63's rule, "uniqueness is a question about the heap", would suggest a cheaper fix: read
//! `SlotDeleted` as "no row", and skip. That is sound only if a freed slot can never stand for a
//! live row. A relocating UPDATE breaks that:
//! - the UPDATE frees the row's slot R, writes the row at R', and repoints the key at R';
//! - a ROLLBACK frees R' (`undo_insert`) and puts the row back at R (`undo_delete` → `restore_at`).
//!
//! The key then points at a freed slot while the row is live at another slot. With that fix the
//! row would be absent by key, and a second INSERT of the key would be ADMITTED, giving two live
//! rows under one primary key. Today the same state fails with an error. That is why
//! `a_rolled_back_relocating_update_keeps_its_row_findable_by_key_and_its_key_taken` exists, and
//! why the fix records each primary-index write and undoes it at abort. The reasons are in
//! artie-research `frontier/lane_rollback_index_orphan.md`.
//!
//! # Pre-registered outcome
//!
//! At the tests commit (on `52b66d6`, which carries D197's fix): the first four tests FAIL and the
//! last passes. With the D202 fix: all five pass. Each failure is named on its test.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
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
            .open(dir.path().join("undo.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("undo.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
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

    fn assert_plan(&mut self, select: &str, needle: &str, s: &mut Session) {
        let sql = format!("EXPLAIN {select}");
        match self.ok(&sql, s) {
            Outcome::Explain(plan) => assert!(
                plan.contains(needle),
                "premise failed: `{select}` does not plan as `{needle}`:\n{plan}"
            ),
            _ => panic!("`{sql}` did not explain"),
        }
    }

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

fn note(id: i32, text: &str) -> Vec<Value> {
    vec![Value::Integer(id), Value::Varchar(text.to_string())]
}

fn assert_duplicate(res: Result<Outcome, FerroError>, what: &str) {
    match res {
        Err(FerroError::Constraint(m)) if m.contains("duplicate primary key") => {}
        Err(e) => panic!("{what}: refused, but not as a duplicate key: {e}"),
        Ok(_) => panic!("{what}: ADMITTED. Two live rows now share one primary key"),
    }
}

/// Rows 1 and 2 fill one page, so an UPDATE or reuse that grows row 1 must relocate it.
///
/// Arithmetic from `Tuple::serialize` and `heap_page`, shared with `reused_key_old_snapshot.rs`:
/// the rows are 35 B and 3934 B, which leaves 96 B free, and a 200-character note makes row 1
/// 234 B.
fn packed_page(db: &mut Db, s: &mut Session) -> RecordId {
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));", s);
    db.ok("INSERT INTO notes VALUES (1, 'a');", s);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(3900)), s);
    let one = db.primary_rid("notes", 1).expect("row 1 is indexed");
    let two = db.primary_rid("notes", 2).expect("row 2 is indexed");
    assert_eq!(one.page_id, two.page_id, "premise failed: rows 1 and 2 are not on one page");
    one
}

const NOTE_BY_KEY: &str = "SELECT id, note FROM notes WHERE id = 1;";

/// **The test that rules out "a freed slot means no row".**
///
/// FAILS at the tests commit, at the first lookup: `the slot is delted`, because the key still
/// points at the relocated slot the rollback freed. Under the rejected heap-authority fix it fails
/// too: the lookup returns `[]` and the INSERT below is admitted as a second live row 1.
#[test]
fn a_rolled_back_relocating_update_keeps_its_row_findable_by_key_and_its_key_taken() {
    let mut db = Db::new();
    let mut s = db.session();
    let home = packed_page(&mut db, &mut s);
    db.assert_plan(NOTE_BY_KEY, "Index scan on notes (col 0", &mut s);

    db.ok("BEGIN;", &mut s);
    db.ok(&format!("UPDATE notes SET note = '{}' WHERE id = 1;", "y".repeat(200)), &mut s);
    assert_ne!(
        db.primary_rid("notes", 1),
        Some(home),
        "premise failed: the UPDATE did not relocate row 1, so this is not the relocation case"
    );
    db.ok("ROLLBACK;", &mut s);

    assert_eq!(
        db.rows(NOTE_BY_KEY, &mut s),
        vec![note(1, "a")],
        "after the rollback, row 1 is not found by key at its original value"
    );
    assert_duplicate(db.exec("INSERT INTO notes VALUES (1, 'dup');", &mut s), "re-INSERT of live key 1");
    assert_eq!(
        db.primary_rid("notes", 1),
        Some(home),
        "the key does not point back at the slot the rollback restored the row to"
    );
}

/// **A secondary lookup after a rolled-back INSERT.** FAILS at the tests commit: the left-behind
/// `(5, 5000)` entry resolves through the primary index to the freed slot, `the slot is delted`.
///
/// Needs 1000 rows and `ANALYZE` to take the index. The arithmetic is in
/// `reused_key_old_snapshot.rs::seed_indexed`, and `assert_plan` checks it.
#[test]
fn a_rolled_back_insert_is_absent_through_a_secondary_index() {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
    db.ok("CREATE INDEX iq ON inventory (qty);", &mut s);
    db.ok("BEGIN;", &mut s);
    for i in 1..=1000 {
        db.ok(&format!("INSERT INTO inventory VALUES ({i}, {});", i * 10), &mut s);
    }
    db.ok("COMMIT;", &mut s);
    db.ok("ANALYZE inventory;", &mut s);
    let by_value = "SELECT id, qty FROM inventory WHERE qty = 5;";
    db.assert_plan(by_value, "Index scan on inventory (col 1", &mut s);

    db.ok("BEGIN;", &mut s);
    db.ok("INSERT INTO inventory VALUES (5000, 5);", &mut s);
    db.ok("ROLLBACK;", &mut s);

    assert_eq!(
        db.rows(by_value, &mut s),
        Vec::<Vec<Value>>::new(),
        "a rolled-back row is visible through the secondary index"
    );
    // Anti-vacuity: the index still answers for a committed row.
    assert_eq!(
        db.rows("SELECT id, qty FROM inventory WHERE qty = 50;", &mut s),
        vec![row(5, 50)],
        "the secondary index lost a committed row"
    );
}

/// **An abort the engine starts itself, on a statement error, undoes the index too.**
/// FAILS at the tests commit, at the lookup of key 5 (`the slot is delted`).
#[test]
fn a_statement_error_rolls_back_the_index_entries_of_its_transaction() {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
    db.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);

    db.ok("BEGIN;", &mut s);
    db.ok("INSERT INTO inventory VALUES (5, 50);", &mut s);
    assert_duplicate(db.exec("INSERT INTO inventory VALUES (1, 99);", &mut s), "INSERT of live key 1");
    assert!(
        s.current.is_none(),
        "premise failed: the error did not end the transaction, so nothing was rolled back"
    );

    assert_eq!(
        db.rows("SELECT id, qty FROM inventory WHERE id = 5;", &mut s),
        Vec::<Vec<Value>>::new(),
        "a row from the aborted transaction is visible by key"
    );
    db.ok("INSERT INTO inventory VALUES (5, 51);", &mut s);
    assert_eq!(db.rows("SELECT id, qty FROM inventory WHERE id = 5;", &mut s), vec![row(5, 51)]);
}

/// **D197's relocated reuse, rolled back.** It must put the key back on the dead version's slot,
/// so an older snapshot still reaches the dead version and the key can be used again. FAILS at the
/// tests commit, at the older snapshot's lookup: the key points at the relocated slot the rollback
/// freed (`the slot is delted`).
#[test]
fn a_rolled_back_reuse_that_relocated_gives_the_key_back_to_its_dead_version() {
    let mut db = Db::new();
    let mut main = db.session();
    let home = packed_page(&mut db, &mut main);
    db.assert_plan(NOTE_BY_KEY, "Index scan on notes (col 0", &mut main);

    let mut old = db.session();
    db.ok("BEGIN;", &mut old);
    db.ok("DELETE FROM notes WHERE id = 1;", &mut main);

    let mut writer = db.session();
    db.ok("BEGIN;", &mut writer);
    db.ok(&format!("INSERT INTO notes VALUES (1, '{}');", "y".repeat(200)), &mut writer);
    assert_ne!(
        db.primary_rid("notes", 1),
        Some(home),
        "premise failed: the reuse did not relocate, so this is not the relocation case"
    );
    db.ok("ROLLBACK;", &mut writer);

    assert_eq!(
        db.rows(NOTE_BY_KEY, &mut old),
        vec![note(1, "a")],
        "after the reuse rolled back, a snapshot older than the DELETE lost (1, 'a') by key"
    );
    db.ok("COMMIT;", &mut old);
    assert_eq!(db.primary_rid("notes", 1), Some(home), "the key did not go back to the dead version's slot");

    db.ok("INSERT INTO notes VALUES (1, 'b');", &mut main);
    let mut fresh = db.session();
    assert_eq!(db.rows(NOTE_BY_KEY, &mut fresh), vec![note(1, "b")], "the key could not be used again");
}

/// **Guard: a rollback touches only the entries its own transaction moved.**
///
/// GREEN at the tests commit: in-place UPDATEs and DELETEs write no index entry. It kills a fix
/// that records those writes as well, which would remove or repoint keys 2 and 3 at abort.
#[test]
fn a_rollback_leaves_every_committed_key_where_it_was() {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
    for (id, qty) in [(1, 20), (2, 30), (3, 7)] {
        db.ok(&format!("INSERT INTO inventory VALUES ({id}, {qty});"), &mut s);
    }
    db.assert_plan("SELECT id, qty FROM inventory WHERE id = 2;", "Index scan on inventory (col 0", &mut s);

    db.ok("BEGIN;", &mut s);
    db.ok("UPDATE inventory SET qty = 31 WHERE id = 2;", &mut s);
    db.ok("DELETE FROM inventory WHERE id = 3;", &mut s);
    db.ok("ROLLBACK;", &mut s);

    for (id, qty) in [(1, 20), (2, 30), (3, 7)] {
        assert_eq!(
            db.rows(&format!("SELECT id, qty FROM inventory WHERE id = {id};"), &mut s),
            vec![row(id, qty)],
            "key {id} does not hold its committed row after an unrelated rollback"
        );
    }
}

/// **The premise the unconditional undo rests on: nobody else can move a key between a
/// transaction's uncommitted write and its rollback.** (The lead's open question, D202 review.)
///
/// `TxnManager::undo_primary_writes` puts a key back without checking what the entry holds now.
/// That is sound only if no other transaction can repoint the key while the writer is open. The
/// source says three refusals exclude it (READ-FROM-SOURCE; this test measures it):
/// - another INSERT of the key reads the writer's head, whose `end_ts` is 0: not deleted for the
///   inserter, so it is refused as a duplicate (`Insert::execute`);
/// - another UPDATE or DELETE either cannot see the writer's row at all (a new key: its only version
///   is uncommitted, with no `prev`), so it moves nothing, or reaches it through the chain and is
///   refused by `check_write_conflict`, because the head's `begin_ts` is uncommitted for it;
/// - DDL that repoints entries (ALTER's rewrite) refuses while any transaction is active.
///
/// Both shapes of recorded write are covered: a relocated committed row, and a brand-new key. Each
/// interloper is tried, and then the rollback must land exactly where the undo assumes.
#[test]
fn nobody_else_can_move_a_key_between_its_uncommitted_write_and_its_rollback() {
    let mut db = Db::new();
    let mut s = db.session();
    let home = packed_page(&mut db, &mut s);
    db.assert_plan(NOTE_BY_KEY, "Index scan on notes (col 0", &mut s);

    fn conflict(res: Result<Outcome, FerroError>, what: &str) {
        match res {
            Err(e @ FerroError::Txn(_)) => assert!(e.to_string().contains("conflict"), "{what}: refused, but not as a conflict: {e}"),
            Err(e) => panic!("{what}: refused, but not as a transaction conflict: {e}"),
            Ok(_) => panic!("{what}: went through while another transaction held the row uncommitted"),
        }
    }

    // A relocated committed row.
    let mut writer = db.session();
    db.ok("BEGIN;", &mut writer);
    db.ok(&format!("UPDATE notes SET note = '{}' WHERE id = 1;", "y".repeat(200)), &mut writer);
    assert_ne!(db.primary_rid("notes", 1), Some(home), "premise failed: the UPDATE did not relocate row 1");
    let moved_to = db.primary_rid("notes", 1);

    let mut other = db.session();
    db.ok("BEGIN;", &mut other);
    conflict(db.exec("UPDATE notes SET note = 'z' WHERE id = 1;", &mut other), "a second UPDATE of row 1");
    let mut other = db.session();
    db.ok("BEGIN;", &mut other);
    conflict(db.exec("DELETE FROM notes WHERE id = 1;", &mut other), "a DELETE of row 1");
    assert_duplicate(db.exec("INSERT INTO notes VALUES (1, 'q');", &mut s), "an INSERT of key 1");
    assert_eq!(db.primary_rid("notes", 1), moved_to, "an interloper moved key 1 while the writer held it");

    db.ok("ROLLBACK;", &mut writer);
    assert_eq!(db.primary_rid("notes", 1), Some(home), "the rollback did not put key 1 back where it started");
    assert_eq!(db.rows(NOTE_BY_KEY, &mut s), vec![note(1, "a")], "row 1 is not its committed self by key");

    // A brand-new key.
    let mut writer = db.session();
    db.ok("BEGIN;", &mut writer);
    db.ok("INSERT INTO notes VALUES (7, 'n');", &mut writer);
    let held = db.primary_rid("notes", 7);
    assert!(held.is_some(), "premise failed: the INSERT did not index key 7");

    let mut other = db.session();
    db.ok("BEGIN;", &mut other);
    match db.exec("UPDATE notes SET note = 'z' WHERE id = 7;", &mut other) {
        Ok(Outcome::Affected(0)) => {}
        Ok(Outcome::Affected(n)) => panic!("a second transaction updated {n} row(s) of an uncommitted INSERT"),
        Ok(_) => panic!("an UPDATE returned something other than a count"),
        Err(e) => panic!("an UPDATE of a row it cannot see failed instead of finding nothing: {e}"),
    }
    db.ok("COMMIT;", &mut other);
    assert_duplicate(db.exec("INSERT INTO notes VALUES (7, 'm');", &mut s), "an INSERT of key 7");
    assert_eq!(db.primary_rid("notes", 7), held, "an interloper moved key 7 while the writer held it");

    db.ok("ROLLBACK;", &mut writer);
    assert_eq!(db.primary_rid("notes", 7), None, "the rollback left key 7 in the index");
    db.ok("INSERT INTO notes VALUES (7, 'm');", &mut s);
    assert_eq!(
        db.rows("SELECT id, note FROM notes WHERE id = 7;", &mut s),
        vec![note(7, "m")],
        "key 7 could not be used after the rollback"
    );
}
