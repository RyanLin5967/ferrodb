//! D213: space freed by an UNCOMMITTED transaction is not reusable until that transaction
//! commits, so an undo always has room.
//!
//! This is the lead's design. InnoDB keeps delete-marked records until purge, and PostgreSQL never
//! frees a tuple in place before vacuum. It makes "an undo that can never find room" (the residual
//! of D210/D211) unrepresentable instead of detected. INFERRED from source and never run; this file
//! is the measurement.
//!
//! The schedules are the lead's:
//! - T1 frees space on a page with a relocating or a shrinking UPDATE;
//! - T2 inserts and COMMITS rows that need that space;
//! - T1 aborts, or the process dies with T1 open, and the database is reopened.
//!
//! Before the fix the abort cannot finish, because its undo is refused for want of room. A reopen
//! with T1 still open FAILS in recovery's undo of the loser. After the fix, T1's abort completes and
//! the reopen succeeds.
//!
//! "Frees" means two different things here, and the fixtures cover both:
//! - **A relocation deletes the row's old slot.** When that slot held the page's LOWEST tuple, its
//!   bytes fell below the new minimum live offset and became free at the next serialise, and T2
//!   took them. The fix RETIRES the slot instead: its bytes stay counted as occupied until T1
//!   commits, so T2's rows go to another page. When the slot was NOT the lowest, nothing was ever
//!   freed, and the undo still failed: `restore_at` wrote a NEW copy at the front of the page, whose
//!   ordinary free space T2 had taken. The fix restores a retired slot in place.
//! - **A shrink frees nothing reusable.** The remnant stays inside the tuple region. The undo
//!   failed for the second reason above: it grew the slot back by writing a new copy at the front.
//!   The fix keeps a shrunk slot's capacity (the tail is zero-filled), so growing it back is in
//!   place. T2 may still land on the page, and that is correct: it takes only space T1 never had.
//!
//! The last two tests are guards. They pass before the fix and pin what it must keep: once T1
//! COMMITS, the space its relocation freed is reusable, and that survives a crash between the
//! commit and the release.

use std::path::Path;

use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::db_lock::DbLock;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::wal::recovery::{open_recovered, OpenedDatabase};

/// Opened through the one open path, so a reopen is the real recovery. Field order is drop order:
/// every handle goes before the lock.
struct Db {
    o: OpenedDatabase,
    _lock: DbLock,
}

impl Db {
    fn open(path: &Path) -> Result<Db, FerroError> {
        let lock = DbLock::acquire(path)?;
        let o = open_recovered(path, &lock)?;
        Ok(Db { o, _lock: lock })
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.o.catalog, self.o.bp.clone(), self.o.txn.clone(), s)
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

    fn primary_rid(&self, key: i32) -> Option<RecordId> {
        let entry = self.o.catalog.get_table("notes").expect("table");
        BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, self.o.bp.clone())
            .search(&Value::Integer(key))
            .expect("primary search")
    }

    fn page_of(&self, key: i32) -> u32 {
        self.primary_rid(key).unwrap_or_else(|| panic!("row {key} is not indexed")).page_id
    }

    fn by_key(&mut self, id: i32) -> Vec<Vec<Value>> {
        let mut s = Session::new();
        self.rows(&format!("SELECT id, note FROM notes WHERE id = {id};"), &mut s)
    }

    fn count(&mut self) -> usize {
        let mut s = Session::new();
        self.rows("SELECT id FROM notes;", &mut s).len()
    }
}

fn note(id: i32, text: &str) -> Vec<Value> {
    vec![Value::Integer(id), Value::Varchar(text.to_string())]
}

const CREATE: &str = "CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));";

/// Arithmetic for every fixture: `Tuple::serialize` makes an `(INTEGER, VARCHAR)` row 34 B plus
/// the note's length, and a heap page has 4073 B for tuples and 4 B slots.
///
/// Row 2 (3934 B) goes in first, so it sits at the top of the page, and row 1 (35 B) is the LOWEST
/// tuple. 96 B stay free. Returns row 1's rid.
fn lowest_row_fixture(db: &mut Db, main: &mut Session) -> RecordId {
    db.ok(CREATE, main);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(3900)), main);
    db.ok("INSERT INTO notes VALUES (1, 'a');", main);
    let one = db.primary_rid(1).expect("row 1");
    assert_eq!(one.page_id, db.page_of(2), "premise: rows 1 and 2 share a page");
    one
}

/// Row 1 (35 B) goes in first, so it is at the TOP of the page and row 2 (3934 B) is the lowest.
/// 96 B stay free. This is the D210 fixture from `tests/abort_that_cannot_finish.rs`.
fn top_row_fixture(db: &mut Db, main: &mut Session) -> RecordId {
    db.ok(CREATE, main);
    db.ok("INSERT INTO notes VALUES (1, 'a');", main);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(3900)), main);
    let one = db.primary_rid(1).expect("row 1");
    assert_eq!(one.page_id, db.page_of(2), "premise: rows 1 and 2 share a page");
    one
}

/// T1 grows row 1 to 234 B, past the page's 96 B, so it relocates. Returns T1's session.
fn relocate_in_open_txn(db: &mut Db, home: RecordId) -> Session {
    let mut t1 = Session::new();
    db.ok("BEGIN;", &mut t1);
    db.ok(&format!("UPDATE notes SET note = '{}' WHERE id = 1;", "y".repeat(200)), &mut t1);
    assert_ne!(db.primary_rid(1), Some(home), "premise: the UPDATE did not relocate row 1");
    t1
}

/// The lowest-row relocation, then T2 commits a 114 B row (118 B with its slot). Before the fix,
/// deleting the lowest slot released its 35 B, so the page showed 131 B free and the row fitted
/// only in that released space.
fn relocate_lowest_then_commit_a_row(db: &mut Db, main: &mut Session) -> (Session, RecordId) {
    let home = lowest_row_fixture(db, main);
    let t1 = relocate_in_open_txn(db, home);
    db.ok(&format!("INSERT INTO notes VALUES (3, '{}');", "z".repeat(80)), main);
    (t1, home)
}

/// The top-row relocation, then T2 commits an 84 B row (88 B with its slot) onto the same page.
/// That leaves 8 B, and a restore at the front of the page needs 35 B.
fn relocate_top_then_fill(db: &mut Db, main: &mut Session) -> Session {
    let home = top_row_fixture(db, main);
    let t1 = relocate_in_open_txn(db, home);
    db.ok(&format!("INSERT INTO notes VALUES (3, '{}');", "z".repeat(50)), main);
    assert_eq!(db.page_of(3), home.page_id, "premise: T2's row did not land on row 1's old page");
    t1
}

/// Row 1 (234 B) and row 2 (3034 B) leave 797 B free. T1 shrinks row 1 in place to 35 B, then T2
/// commits a 634 B row (638 B with its slot) onto the page, leaving 159 B. Growing row 1 back by
/// writing a new 234 B copy at the front no longer fits.
fn shrink_then_fill(db: &mut Db, main: &mut Session) -> Session {
    db.ok(CREATE, main);
    db.ok(&format!("INSERT INTO notes VALUES (1, '{}');", "x".repeat(200)), main);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}');", "w".repeat(3000)), main);
    let one = db.primary_rid(1).expect("row 1");
    let mut t1 = Session::new();
    db.ok("BEGIN;", &mut t1);
    db.ok("UPDATE notes SET note = 'a' WHERE id = 1;", &mut t1);
    assert_eq!(db.primary_rid(1), Some(one), "premise: the shrinking UPDATE was not in place");
    db.ok(&format!("INSERT INTO notes VALUES (3, '{}');", "f".repeat(600)), main);
    t1
}

/// The process dies. There is no ROLLBACK and no checkpoint, and every handle is dropped. T2's
/// commit flushed the log through T1's earlier records, so recovery sees T1 as a loser.
fn crash_and_reopen(db: Db, open_txn: Session, path: &Path) -> Db {
    drop(open_txn);
    drop(db);
    Db::open(path).unwrap_or_else(|e| panic!("the database did not reopen: {e}"))
}

/// **The lead's schedule, relocation form.** FAILS at `00f4c39` at "T2's row landed". Past that
/// point the ROLLBACK fails, because its `restore_at` is refused with 13 B free.
#[test]
fn a_relocation_rolled_back_after_another_txn_committed_rows_gets_its_space_back_and_the_database_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reserve_relocate.db");
    let mut db = Db::open(&path).unwrap();
    let mut main = Session::new();
    let (mut t1, home) = relocate_lowest_then_commit_a_row(&mut db, &mut main);

    assert_ne!(
        db.page_of(3),
        home.page_id,
        "T2's row landed in the space that T1's UNCOMMITTED relocation freed on row 1's page"
    );
    db.ok("ROLLBACK;", &mut t1);
    assert_eq!(t1.current, None, "the ROLLBACK did not end the transaction");
    assert_eq!(db.by_key(1), vec![note(1, "a")], "row 1 is not its committed self after the rollback");
    assert_eq!(db.primary_rid(1), Some(home), "row 1's key does not name its original slot");
    assert_eq!(db.by_key(2), vec![note(2, &"x".repeat(3900))], "row 2 changed");
    assert_eq!(db.by_key(3), vec![note(3, &"z".repeat(80))], "T2's committed row changed");

    drop(t1);
    drop(db);
    let mut db = Db::open(&path).expect("the database did not reopen after the rollback");
    assert_eq!(db.by_key(1), vec![note(1, "a")], "after the reopen, row 1 is not its committed self");
    assert_eq!(db.count(), 3, "after the reopen, the table does not hold exactly rows 1, 2 and 3");
}

/// **The same, with T1 still OPEN when the process dies.** Recovery undoes it as a loser. FAILS at
/// `00f4c39` at the reopen: the loser's `restore_at` is refused (13 B free), `recover` returns the
/// error, and the database does not open.
#[test]
fn a_crash_with_the_relocating_txn_still_open_reopens_and_undoes_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reserve_crash.db");
    let mut db = Db::open(&path).unwrap();
    let mut main = Session::new();
    let (t1, _home) = relocate_lowest_then_commit_a_row(&mut db, &mut main);

    let mut db = crash_and_reopen(db, t1, &path);
    assert_eq!(db.by_key(1), vec![note(1, "a")], "recovery did not restore row 1");
    assert_eq!(db.by_key(3), vec![note(3, &"z".repeat(80))], "T2's committed row is missing after recovery");
    assert_eq!(db.count(), 3, "the table does not hold exactly rows 1, 2 and 3");
}

/// **The relocated row was not the lowest, so its delete freed nothing.** T2 took only the page's
/// ordinary free space, and that is allowed. The undo is what must not need it. FAILS at `00f4c39`
/// at the ROLLBACK: `restore_at` wanted 35 B at the front of a page with 8 B free (D210 refused it
/// rather than overwrite the slot array). This is the schedule of
/// `abort_that_cannot_finish::rolling_back_a_relocation_onto_a_page_filled_since_refuses_and_leaves_the_page_intact`,
/// with the outcome the design now requires.
#[test]
fn a_relocation_rolled_back_after_its_page_filled_is_restored_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reserve_top.db");
    let mut db = Db::open(&path).unwrap();
    let mut main = Session::new();
    let mut t1 = relocate_top_then_fill(&mut db, &mut main);

    db.ok("ROLLBACK;", &mut t1);
    assert_eq!(t1.current, None, "the ROLLBACK did not end the transaction");
    assert_eq!(db.by_key(1), vec![note(1, "a")], "row 1 is not its committed self after the rollback");
    assert_eq!(db.by_key(2), vec![note(2, &"x".repeat(3900))], "row 2, on the restored page, changed");
    assert_eq!(db.by_key(3), vec![note(3, &"z".repeat(50))], "row 3, on the restored page, changed");

    drop(t1);
    drop(db);
    let mut db = Db::open(&path).expect("the database did not reopen after the rollback");
    assert_eq!(db.by_key(1), vec![note(1, "a")], "after the reopen, row 1 is not its committed self");
    assert_eq!(db.count(), 3, "after the reopen, the table does not hold exactly rows 1, 2 and 3");
}

/// **The same, with T1 still open at the crash.** FAILS at `00f4c39` at the reopen, for the same
/// 35-into-8 refusal, reached by recovery's undo of the loser.
#[test]
fn a_crash_with_a_relocation_open_after_its_page_filled_reopens_and_undoes_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reserve_top_crash.db");
    let mut db = Db::open(&path).unwrap();
    let mut main = Session::new();
    let t1 = relocate_top_then_fill(&mut db, &mut main);

    let mut db = crash_and_reopen(db, t1, &path);
    assert_eq!(db.by_key(1), vec![note(1, "a")], "recovery did not restore row 1");
    assert_eq!(db.by_key(3), vec![note(3, &"z".repeat(50))], "T2's committed row is missing after recovery");
    assert_eq!(db.count(), 3, "the table does not hold exactly rows 1, 2 and 3");
}

/// **The lead's schedule, shrink form.** FAILS at `00f4c39` at the ROLLBACK: its undo wrote a new
/// 234 B copy at the front of the page, and 159 B were left.
#[test]
fn a_shrink_rolled_back_after_another_txn_committed_rows_completes_and_the_database_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reserve_shrink.db");
    let mut db = Db::open(&path).unwrap();
    let mut main = Session::new();
    let mut t1 = shrink_then_fill(&mut db, &mut main);

    db.ok("ROLLBACK;", &mut t1);
    assert_eq!(t1.current, None, "the ROLLBACK did not end the transaction");
    assert_eq!(db.by_key(1), vec![note(1, &"x".repeat(200))], "row 1 did not get its old value back");
    assert_eq!(db.by_key(3), vec![note(3, &"f".repeat(600))], "T2's committed row changed");

    drop(t1);
    drop(db);
    let mut db = Db::open(&path).expect("the database did not reopen after the rollback");
    assert_eq!(db.by_key(1), vec![note(1, &"x".repeat(200))], "after the reopen, row 1 lost its value");
    assert_eq!(db.count(), 3, "after the reopen, the table does not hold exactly rows 1, 2 and 3");
}

/// **The shrink with T1 still open at the crash.** FAILS at `00f4c39` at the reopen: redo replays
/// the shrink and T2's row, and the loser's undo meets the same 234-into-159 refusal.
#[test]
fn a_crash_with_the_shrinking_txn_still_open_reopens_and_undoes_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reserve_shrink_crash.db");
    let mut db = Db::open(&path).unwrap();
    let mut main = Session::new();
    let t1 = shrink_then_fill(&mut db, &mut main);

    let mut db = crash_and_reopen(db, t1, &path);
    assert_eq!(db.by_key(1), vec![note(1, &"x".repeat(200))], "recovery did not give row 1 its old value back");
    assert_eq!(db.by_key(3), vec![note(3, &"f".repeat(600))], "T2's committed row is missing after recovery");
    assert_eq!(db.count(), 3, "the table does not hold exactly rows 1, 2 and 3");
}

/// **Guard: once T1 COMMITS, the space its relocation freed is reusable.** GREEN at `00f4c39`,
/// where the space was free at once. After the fix it is free only from the commit on, so this goes
/// red if the commit never releases the retired slot, or releases it without telling the page
/// directory (`find_page_with_space` reads the directory, not the page).
#[test]
fn a_committed_relocation_frees_its_space_for_the_next_insert() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reserve_commit.db");
    let mut db = Db::open(&path).unwrap();
    let mut main = Session::new();
    let home = lowest_row_fixture(&mut db, &mut main);
    let mut t1 = relocate_in_open_txn(&mut db, home);
    db.ok("COMMIT;", &mut t1);

    db.ok(&format!("INSERT INTO notes VALUES (3, '{}');", "z".repeat(80)), &mut main);
    assert_eq!(
        db.page_of(3),
        home.page_id,
        "a row that fits only in the space the COMMITTED relocation freed went to another page"
    );
    assert_eq!(db.by_key(1), vec![note(1, &"y".repeat(200))], "the committed update is not visible");
}

/// **Guard: a crash after the commit, before the release reached the log, still frees the space.**
/// GREEN at `00f4c39`. After the fix, the release is logged after the `Commit`, and nothing flushes
/// the log between T1's commit and the crash, so the release is lost with the log buffer (at the
/// default checkpoint interval; `FERRODB_CHECKPOINT_INTERVAL=1` would checkpoint at the commit and
/// make this test pass without reaching the path it guards). Recovery must finish the release
/// itself, or the space is lost for good.
#[test]
fn a_crash_after_a_relocating_commit_still_frees_its_space() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reserve_commit_crash.db");
    let mut db = Db::open(&path).unwrap();
    let mut main = Session::new();
    let home = lowest_row_fixture(&mut db, &mut main);
    let mut t1 = relocate_in_open_txn(&mut db, home);
    db.ok("COMMIT;", &mut t1);
    drop(main);

    let mut db = crash_and_reopen(db, t1, &path);
    let mut main = Session::new();
    db.ok(&format!("INSERT INTO notes VALUES (3, '{}');", "z".repeat(80)), &mut main);
    assert_eq!(
        db.page_of(3),
        home.page_id,
        "after the crash and reopen, the space the committed relocation freed is not reusable"
    );
    assert_eq!(db.by_key(1), vec![note(1, &"y".repeat(200))], "the committed update was lost in the crash");
}
