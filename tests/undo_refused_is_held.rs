//! D211's machinery, on an undo failure that D213 leaves possible.
//!
//! `tests/abort_that_cannot_finish.rs` made an undo fail for want of room: another transaction
//! filled the page before the rollback. D213 makes that unrepresentable (an undo now writes into
//! bytes its transaction still holds), so that file is RETIRED, and every one of its assertions is
//! carried here (the map is in the lane report, `frontier/lane_rollback_index_orphan.md` §18).
//! What D211 built still guards every undo failure that remains: an I/O error, a failed log
//! append, or a page that disagrees with the log. These tests stage the last of those, because it
//! is the one an integration test can stage, by editing the page:
//! - the retired slot a relocation must restore claims 1 byte where the log holds 35;
//! - a shrunk slot claims its new 35 B instead of the 234 B it kept, which is exactly the page a
//!   binary before D213 left behind, so growing it back needs front room that T2 has taken;
//! - the retired slot is made FREE, the page a binary before D213 left after a relocation, so the
//!   restore takes D210's splice and its room check.
//!
//! What D211 promises, pinned here: the failed ROLLBACK keeps the session's transaction, the
//! transaction refuses everything but ROLLBACK (`snapshot_of`, `commit`), a second ROLLBACK
//! retries the SAME undo rather than skipping it (a CLR is logged only for an undo that applied),
//! and once the page is repaired the ROLLBACK finishes and the row is back. INFERRED from source
//! and never run.

use std::path::Path;

use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::db_lock::DbLock;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::storage::heap_page::{Page, RETIRED};
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::wal::recovery::{open_recovered, OpenedDatabase};

struct Db {
    o: OpenedDatabase,
    _lock: DbLock,
}

impl Db {
    fn open(path: &Path) -> Db {
        let lock = DbLock::acquire(path).unwrap();
        let o = open_recovered(path, &lock).unwrap();
        Db { o, _lock: lock }
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

    /// Rewrite one slot's `length` field on its page, keeping everything else, the LSN included.
    /// This is the staged disagreement between the page and the log.
    fn set_slot_length(&self, rid: RecordId, length: u16) {
        let bp = &self.o.bp;
        let frame_i = bp.fetch_page(rid.page_id).unwrap();
        {
            let mut frame = bp.frame_write(frame_i);
            let mut page = Page::deserialize(frame.data).unwrap();
            page.slot_arr[rid.slot_num as usize].length = length;
            frame.data = page.serialize().unwrap();
        }
        bp.unpin_page(rid.page_id, true);
    }

    /// A slot's `(offset, length)` as the page holds it.
    fn slot(&self, rid: RecordId) -> (u16, u16) {
        let bp = &self.o.bp;
        let frame_i = bp.fetch_page(rid.page_id).unwrap();
        let page = Page::deserialize(bp.frames[frame_i].read().unwrap().data).unwrap();
        bp.unpin_page(rid.page_id, false);
        let s = &page.slot_arr[rid.slot_num as usize];
        (s.offset, s.length)
    }

    /// Rewrite one slot's `(offset, length)`, keeping everything else, the LSN included.
    fn set_slot(&self, rid: RecordId, offset: u16, length: u16) {
        let bp = &self.o.bp;
        let frame_i = bp.fetch_page(rid.page_id).unwrap();
        {
            let mut frame = bp.frame_write(frame_i);
            let mut page = Page::deserialize(frame.data).unwrap();
            page.slot_arr[rid.slot_num as usize].offset = offset;
            page.slot_arr[rid.slot_num as usize].length = length;
            frame.data = page.serialize().unwrap();
        }
        bp.unpin_page(rid.page_id, true);
    }

    fn slot_length(&self, rid: RecordId) -> u16 {
        let bp = &self.o.bp;
        let frame_i = bp.fetch_page(rid.page_id).unwrap();
        let page = Page::deserialize(bp.frames[frame_i].read().unwrap().data).unwrap();
        bp.unpin_page(rid.page_id, false);
        page.slot_arr[rid.slot_num as usize].length
    }
}

fn note(id: i32, text: &str) -> Vec<Value> {
    vec![Value::Integer(id), Value::Varchar(text.to_string())]
}

/// Row 1 (35 B, `Tuple::serialize`'s 34 B plus one) and row 2 (3934 B) share a page with 96 B
/// free. T1 grows row 1 to 234 B, so it relocates and its old slot is RETIRED. Then the retired
/// slot is edited to claim 1 byte, so the rollback's in-place restore of 35 B is refused.
/// Returns T1's session, its transaction id, and row 1's home rid.
fn relocate_then_break_the_page(db: &mut Db, main: &mut Session) -> (Session, u64, RecordId) {
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));", main);
    db.ok("INSERT INTO notes VALUES (1, 'a');", main);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(3900)), main);
    let home = db.primary_rid(1).expect("row 1");

    let mut t1 = Session::new();
    db.ok("BEGIN;", &mut t1);
    let id = t1.current.expect("BEGIN opened a transaction");
    db.ok(&format!("UPDATE notes SET note = '{}' WHERE id = 1;", "y".repeat(200)), &mut t1);
    assert_ne!(db.primary_rid(1), Some(home), "premise: the UPDATE did not relocate row 1");
    assert_eq!(db.slot_length(home), 35 | RETIRED, "premise: the relocation did not retire row 1's 35 B slot");

    db.set_slot_length(home, 1 | RETIRED);
    (t1, id, home)
}

/// **A ROLLBACK whose undo is refused keeps the transaction, refuses everything else, and retries
/// the SAME undo next time.** The D211 assertions of
/// `abort_that_cannot_finish::a_rollback_whose_undo_has_no_room_is_held_and_retried_not_skipped`,
/// on a failure D213 leaves possible, plus the finish: once the page is repaired, the retry
/// completes and row 1 is its committed self.
#[test]
fn a_rollback_whose_undo_is_refused_is_held_and_retried_not_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(&dir.path().join("held.db"));
    let mut main = Session::new();
    let (mut t1, id, home) = relocate_then_break_the_page(&mut db, &mut main);

    assert!(db.exec("ROLLBACK;", &mut t1).is_err(), "premise: the broken page's restore was not refused");
    assert_eq!(t1.current, Some(id), "a failed ROLLBACK: the session forgot its transaction");

    match db.exec("SELECT id, note FROM notes;", &mut t1) {
        Err(e) => assert!(e.to_string().contains("rolling back"), "refused, but not for rolling back: {e}"),
        Ok(_) => panic!("a transaction whose rollback did not finish ran a SELECT"),
    }
    assert!(db.exec("COMMIT;", &mut t1).is_err(), "a transaction whose rollback did not finish COMMITTED");
    assert_eq!(t1.current, Some(id), "a refused COMMIT: the session forgot its transaction");
    assert!(
        db.exec("ROLLBACK;", &mut t1).is_err(),
        "the second ROLLBACK succeeded with the page still broken: it skipped the undo instead of retrying it"
    );
    assert_eq!(t1.current, Some(id), "the second failed ROLLBACK forgot the transaction");

    db.set_slot_length(home, 35 | RETIRED);
    db.ok("ROLLBACK;", &mut t1);
    assert_eq!(t1.current, None, "the ROLLBACK that finished did not end the transaction");
    let mut fresh = Session::new();
    assert_eq!(
        db.rows("SELECT id, note FROM notes WHERE id = 1;", &mut fresh),
        vec![note(1, "a")],
        "the retried undo did not put row 1 back"
    );
    assert_eq!(db.slot_length(home), 35, "row 1's slot is not live at its full 35 B after the rollback");
}

/// **The same failure reached from a failed STATEMENT:** `executor::roll_back_failed_statement`.
/// The statement's own error comes first, the error says the rollback did not finish, and the
/// transaction is held until a ROLLBACK can finish it.
#[test]
fn a_failed_statement_whose_rollback_is_refused_holds_the_transaction_and_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(&dir.path().join("held_stmt.db"));
    let mut main = Session::new();
    let (mut t1, id, home) = relocate_then_break_the_page(&mut db, &mut main);

    match db.exec("INSERT INTO notes VALUES (2, 'd');", &mut t1) {
        Err(e) => {
            let m = e.to_string();
            assert!(m.contains("duplicate primary key"), "the statement's own error is not first: {m}");
            assert!(m.contains("did not finish"), "the error does not say the rollback did not finish: {m}");
        }
        Ok(_) => panic!("a duplicate key was admitted"),
    }
    assert_eq!(t1.current, Some(id), "the session forgot a transaction whose rollback did not finish");
    assert!(db.exec("SELECT id FROM notes;", &mut t1).is_err(), "a transaction whose rollback did not finish ran a SELECT");
    assert!(db.exec("ROLLBACK;", &mut t1).is_err(), "ROLLBACK succeeded with the page still broken");
    assert_eq!(t1.current, Some(id), "a failed ROLLBACK forgot the transaction");

    db.set_slot_length(home, 35 | RETIRED);
    db.ok("ROLLBACK;", &mut t1);
    assert_eq!(t1.current, None, "the ROLLBACK that finished did not end the transaction");
    let mut fresh = Session::new();
    assert_eq!(db.rows("SELECT id, note FROM notes WHERE id = 1;", &mut fresh), vec![note(1, "a")]);
}

/// Row 1 (234 B) and row 2 (3034 B) leave 797 B free. T1 shrinks row 1 in place to 35 B, and since
/// D213 the slot keeps its 234 B. The page is then edited so the slot claims 35 B: the page a binary
/// before D213 left after the same shrink. T2 commits a 634 B row onto the page, leaving 159 B, so
/// growing row 1 back needs a new 234 B copy at the front of the page and is refused.
/// Returns T1's session, its transaction id, and row 1's rid.
fn shrink_then_break_the_page(db: &mut Db, main: &mut Session) -> (Session, u64, RecordId) {
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));", main);
    db.ok(&format!("INSERT INTO notes VALUES (1, '{}');", "x".repeat(200)), main);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}');", "w".repeat(3000)), main);
    let one = db.primary_rid(1).expect("row 1");
    assert_eq!(db.primary_rid(2).expect("row 2").page_id, one.page_id, "premise: rows 1 and 2 share a page");

    let mut t1 = Session::new();
    db.ok("BEGIN;", &mut t1);
    let id = t1.current.expect("BEGIN opened a transaction");
    db.ok("UPDATE notes SET note = 'a' WHERE id = 1;", &mut t1);
    assert_eq!(db.primary_rid(1), Some(one), "premise: the shrinking UPDATE was not in place");
    assert_eq!(db.slot_length(one), 234, "premise: the shrink did not keep the slot's 234 B (D213)");

    db.set_slot_length(one, 35);
    db.ok(&format!("INSERT INTO notes VALUES (3, '{}');", "f".repeat(600)), main);
    assert_eq!(
        db.primary_rid(3).expect("row 3").page_id,
        one.page_id,
        "premise: T2's row did not land on row 1's page"
    );
    (t1, id, one)
}

/// **The carrier of `abort_that_cannot_finish::a_rollback_whose_undo_has_no_room_is_held_and_retried_not_skipped`**
/// (retired, lane §18): the same shrink schedule and every one of its assertions, on the page a
/// binary before D213 leaves. Including the one the relocation tests here cannot carry: while T1 is
/// stuck, a fresh reader still sees the COMMITTED row 1, because the failed undo is an in-place
/// update and the head still chains to the committed version. Then the page is repaired and the
/// retried undo finishes.
#[test]
fn a_shrink_whose_undo_has_no_room_is_held_and_retried_and_readers_see_the_committed_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(&dir.path().join("held_shrink.db"));
    let mut main = Session::new();
    let (mut t1, id, one) = shrink_then_break_the_page(&mut db, &mut main);

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
    let mut fresh = Session::new();
    assert_eq!(
        db.rows("SELECT id, note FROM notes WHERE id = 1;", &mut fresh),
        vec![note(1, &"x".repeat(200))],
        "a reader sees the uncommitted shrink of a transaction whose rollback did not finish"
    );

    db.set_slot_length(one, 234);
    db.ok("ROLLBACK;", &mut t1);
    assert_eq!(t1.current, None, "the ROLLBACK that finished did not end the transaction");
    assert_eq!(
        db.rows("SELECT id, note FROM notes WHERE id = 1;", &mut fresh),
        vec![note(1, &"x".repeat(200))],
        "the retried undo did not give row 1 its old value back"
    );
}

/// **The carrier of `abort_that_cannot_finish::rolling_back_a_relocation_onto_a_page_filled_since_refuses_and_leaves_the_page_intact`**
/// (retired, lane §18). D210's refusal at the SQL level. Since D213 a relocation's old slot is
/// RETIRED and restored in place, so the refusal is reachable only for a FREE slot: the page a
/// binary before D213 left after the relocation. Rows 1 (35 B, at the top of the page) and 2
/// (3934 B) leave 96 B free. T1 relocates row 1, T2 takes 88 B of the 96 B, and the retired slot is
/// then made free. The restore must put 35 B at the front of a page with 8 B free. It must be
/// refused, and the rows on the page must be intact. Then the page is repaired and the retry
/// finishes.
#[test]
fn rolling_back_onto_a_freed_slot_without_room_refuses_and_leaves_the_page_intact() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(&dir.path().join("held_freed.db"));
    let mut main = Session::new();
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));", &mut main);
    db.ok("INSERT INTO notes VALUES (1, 'a');", &mut main);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(3900)), &mut main);
    let home = db.primary_rid(1).expect("row 1");

    let mut t1 = Session::new();
    db.ok("BEGIN;", &mut t1);
    let id = t1.current.expect("BEGIN opened a transaction");
    db.ok(&format!("UPDATE notes SET note = '{}' WHERE id = 1;", "y".repeat(200)), &mut t1);
    assert_ne!(db.primary_rid(1), Some(home), "premise: the UPDATE did not relocate row 1");
    db.ok(&format!("INSERT INTO notes VALUES (3, '{}');", "z".repeat(50)), &mut main);
    assert_eq!(
        db.primary_rid(3).expect("row 3").page_id,
        home.page_id,
        "premise: T2's row did not land on row 1's old page"
    );
    let retired = db.slot(home);
    assert_eq!(retired.1, 35 | RETIRED, "premise: the relocation did not retire row 1's 35 B slot");
    db.set_slot(home, 0, 0);

    assert!(
        db.exec("ROLLBACK;", &mut t1).is_err(),
        "the ROLLBACK succeeded: it restored 35 B into a page with 8 B free"
    );
    assert_eq!(t1.current, Some(id), "a failed ROLLBACK: the session forgot its transaction");
    let mut fresh = Session::new();
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

    db.set_slot(home, retired.0, retired.1);
    db.ok("ROLLBACK;", &mut t1);
    assert_eq!(t1.current, None, "the ROLLBACK that finished did not end the transaction");
    assert_eq!(db.rows("SELECT id, note FROM notes WHERE id = 1;", &mut fresh), vec![note(1, "a")]);
}
