//! D210 and D211: a rollback whose heap undo has no room on its page.
//!
//! Found by the fresh-context re-adversary on D205 (artie-research `frontier/d205_readversary.md`,
//! `fffdc62`) and lead-verified on main. INFERRED from source and never run; this file is the
//! measurement.
//!
//! - **D211.** `TxnManager::abort` appended each CLR and THEN applied its page undo with `?`. A
//!   failed undo returned with the CLR already logged, and a resumed abort followed `undo_next`
//!   straight past it, so the failed undo was never retried. `TxnStatus::Aborting` was written and
//!   never read, so the stuck transaction could keep running statements, or COMMIT. `ROLLBACK`
//!   took the session's id before aborting, so a failed rollback also forgot the transaction. The
//!   D205 lane's "recoverable, run ROLLBACK to resume it" was therefore FALSE, and that text is
//!   withdrawn where it was written.
//! - **D210.** `HeapPage::restore_at` had no free-space check. It splices the tuple and sets
//!   `offset = PAGE_SIZE - tuples.len()`, so rolling back a relocation onto a page that other
//!   inserts filled in the meantime ran the tuples into the slot array and header: silent page
//!   corruption.
//!
//! The schedules are the adversary's: a row's space is released by the transaction being rolled
//! back (a shrink in place, or a relocation away), and another transaction takes the page's free
//! space before the rollback runs.

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
            .open(dir.path().join("abort.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("abort.wal")).unwrap());
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

    fn primary_rid(&self, table: &str, key: i32) -> Option<RecordId> {
        let entry = self.catalog.get_table(table).expect("table");
        BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, self.bp.clone())
            .search(&Value::Integer(key))
            .expect("primary search")
    }
}

fn note(id: i32, text: &str) -> Vec<Value> {
    vec![Value::Integer(id), Value::Varchar(text.to_string())]
}

/// Row 1 (234 B) and row 2 (3034 B) on one page, leaving 797 B free. Arithmetic from
/// `Tuple::serialize` (34 B + the note's length) and `heap_page` (4073 B usable, a 4 B slot each).
/// Returns row 1's rid.
fn shrink_fixture(db: &mut Db, s: &mut Session) -> RecordId {
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));", s);
    db.ok(&format!("INSERT INTO notes VALUES (1, '{}');", "x".repeat(200)), s);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}');", "w".repeat(3000)), s);
    let one = db.primary_rid("notes", 1).expect("row 1");
    assert_eq!(one.page_id, db.primary_rid("notes", 2).expect("row 2").page_id, "premise: rows 1 and 2 share a page");
    one
}

/// T1 shrinks row 1 in place (to 35 B), then T2 takes 638 B of the 797 B free. That leaves 159 B,
/// and T1's undo must put 234 B back. Returns T1's session and transaction id.
fn shrink_then_fill(db: &mut Db, main: &mut Session) -> (Session, u64) {
    let one = shrink_fixture(db, main);
    let mut t1 = db.session();
    db.ok("BEGIN;", &mut t1);
    let id = t1.current.expect("BEGIN opened a transaction");
    db.ok("UPDATE notes SET note = 'a' WHERE id = 1;", &mut t1);
    assert_eq!(db.primary_rid("notes", 1), Some(one), "premise: the shrinking UPDATE was not in place");
    db.ok(&format!("INSERT INTO notes VALUES (3, '{}');", "f".repeat(600)), main);
    assert_eq!(db.primary_rid("notes", 3).expect("row 3").page_id, one.page_id, "premise: T2's row did not land on row 1's page");
    (t1, id)
}

/// **D211: a ROLLBACK whose undo has no room keeps the transaction, refuses everything else, and
/// retries the SAME undo next time. It does not skip it.**
///
/// FAILS at `c21eaff`..`d7891d5` at "the session forgot its transaction": `ROLLBACK` did
/// `session.current.take()` before `abort`. Past that point, the old code would fail each later
/// assertion in turn. The SELECT and the COMMIT ran, because `Aborting` was never read. The second
/// ROLLBACK "succeeded" by following `undo_next` past the logged-but-unapplied CLR, and the
/// uncommitted shrink then read as committed to every later snapshot.
#[test]
fn a_rollback_whose_undo_has_no_room_is_held_and_retried_not_skipped() {
    let mut db = Db::new();
    let mut main = db.session();
    let (mut t1, id) = shrink_then_fill(&mut db, &mut main);

    assert!(db.exec("ROLLBACK;", &mut t1).is_err(), "premise: the undo had room after all, so nothing here is tested");
    assert_eq!(t1.current, Some(id), "a failed ROLLBACK: the session forgot its transaction, which is still open and holding row 1");

    match db.exec("SELECT id, note FROM notes;", &mut t1) {
        Err(e) => assert!(e.to_string().contains("rolling back"), "refused, but not for rolling back: {e}"),
        Ok(_) => panic!("a transaction whose rollback did not finish ran a SELECT"),
    }
    assert!(db.exec("COMMIT;", &mut t1).is_err(), "a transaction whose rollback did not finish COMMITTED");
    assert_eq!(t1.current, Some(id), "a refused COMMIT: the session forgot its transaction");

    assert!(
        db.exec("ROLLBACK;", &mut t1).is_err(),
        "the second ROLLBACK succeeded with the page still full: it skipped the undo instead of retrying it"
    );
    assert_eq!(t1.current, Some(id), "the second failed ROLLBACK forgot the transaction");

    // Nothing answers wrongly meanwhile. T1 is still open, so its shrink is invisible, and the
    // committed row reads as committed.
    let mut fresh = db.session();
    assert_eq!(
        db.rows("SELECT id, note FROM notes WHERE id = 1;", &mut fresh),
        vec![note(1, &"x".repeat(200))],
        "a reader sees the uncommitted shrink of a transaction whose rollback did not finish"
    );
}

/// **The same failure reached from a failed STATEMENT, not from ROLLBACK:
/// `executor::roll_back_failed_statement`'s heap-undo-failed branch**, a gap the re-adversary
/// named. The statement's own error comes first, the transaction is held, and it refuses
/// everything but ROLLBACK.
///
/// FAILS at `d7891d5` at the SELECT, which ran because `Aborting` was never read.
#[test]
fn a_failed_statement_whose_rollback_cannot_finish_holds_the_transaction_and_says_so() {
    let mut db = Db::new();
    let mut main = db.session();
    let (mut t1, id) = shrink_then_fill(&mut db, &mut main);

    match db.exec(&format!("INSERT INTO notes VALUES (2, '{}');", "d"), &mut t1) {
        Err(e) => {
            let m = e.to_string();
            assert!(m.contains("duplicate primary key"), "the statement's own error is not first: {m}");
            assert!(m.contains("did not finish"), "the error does not say the rollback did not finish: {m}");
        }
        Ok(_) => panic!("a duplicate key was admitted"),
    }
    assert_eq!(t1.current, Some(id), "the session forgot a transaction whose rollback did not finish");
    assert!(db.exec("SELECT id FROM notes;", &mut t1).is_err(), "a transaction whose rollback did not finish ran a SELECT");
    assert!(db.exec("ROLLBACK;", &mut t1).is_err(), "ROLLBACK succeeded with the page still full");
    assert_eq!(t1.current, Some(id), "a failed ROLLBACK forgot the transaction");
}

/// **D210: rolling back a relocation onto a page that has filled up since then is REFUSED, and the
/// page's other rows are intact.**
///
/// Rows 1 (35 B) and 2 (3934 B) leave 96 B free. T1's UPDATE grows row 1 past that, so it
/// relocates: the slot is freed (a logged `HeapDelete`) and the row is written on another page.
/// T2 then takes 88 B of the 96 B. T1's ROLLBACK must `restore_at` 35 B into 8 B.
///
/// FAILS at `d7891d5` at "the ROLLBACK succeeded". `restore_at` spliced the tuple regardless, and
/// the tuples ran into the slot array and header of a page holding rows 2 and 3.
#[test]
fn rolling_back_a_relocation_onto_a_page_filled_since_refuses_and_leaves_the_page_intact() {
    let mut db = Db::new();
    let mut main = db.session();
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));", &mut main);
    db.ok("INSERT INTO notes VALUES (1, 'a');", &mut main);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(3900)), &mut main);
    let one = db.primary_rid("notes", 1).expect("row 1");

    let mut t1 = db.session();
    db.ok("BEGIN;", &mut t1);
    db.ok(&format!("UPDATE notes SET note = '{}' WHERE id = 1;", "y".repeat(200)), &mut t1);
    assert_ne!(db.primary_rid("notes", 1), Some(one), "premise: the UPDATE did not relocate row 1");
    db.ok(&format!("INSERT INTO notes VALUES (3, '{}');", "z".repeat(50)), &mut main);
    assert_eq!(db.primary_rid("notes", 3).expect("row 3").page_id, one.page_id, "premise: T2's row did not land on row 1's old page");

    assert!(
        db.exec("ROLLBACK;", &mut t1).is_err(),
        "the ROLLBACK succeeded: it restored 35 B into a page with 8 B free"
    );

    let mut fresh = db.session();
    assert_eq!(
        db.rows("SELECT id, note FROM notes WHERE id = 2;", &mut fresh),
        vec![note(2, &"x".repeat(3900))],
        "row 2, on the page the rollback tried to restore into, is damaged"
    );
    assert_eq!(
        db.rows("SELECT id, note FROM notes WHERE id = 3;", &mut fresh),
        vec![note(3, &"z".repeat(50))],
        "row 3, on the page the rollback tried to restore into, is damaged"
    );
}
