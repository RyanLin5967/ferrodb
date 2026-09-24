//! Q3 of review 2 on `56f6752..368d0e1` (`frontier/rollback_review2.md` @ artie-research `5f6b821`): a
//! release that can NEVER succeed must not hold the log for ever.
//!
//! **This replaces `tests/failed_release_is_kept.rs`, retired by PREREG amendment (lane §20).** That
//! test made a release fail by editing the retired slot to read LIVE, and asserted that the checkpoint
//! kept the log. But a LIVE or out-of-range slot is a deterministic mismatch between the page and the
//! log. Retrying it is futile, so it kept the log growing for ever, and every restart replayed and
//! rebuilt it all (review 2, Q3).
//!
//! The lead's decision: `release_one` classifies its errors. A mismatch leaves the pending list: it is
//! counted apart, and its page and slot are printed. I/O and poison stay pending and are retried. The
//! pending path, which needs an I/O failure an integration test cannot inject, is now tested in the
//! crate's own unit tests (`wal::txn`, `wal::recovery`).
//!
//! This binary holds one test, because it reads `wal::txn::release_failures`, a process-wide counter.
//! INFERRED from source and never run.

use std::path::Path;
use std::sync::atomic::Ordering;

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
use ferrodb::wal::txn::{release_failures, release_mismatches};

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

    fn primary_rid(&self, key: i32) -> Option<RecordId> {
        let entry = self.o.catalog.get_table("notes").expect("table");
        BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, self.o.bp.clone())
            .search(&Value::Integer(key))
            .expect("primary search")
    }

    fn slot(&self, rid: RecordId) -> (u16, u16) {
        let bp = &self.o.bp;
        let frame_i = bp.fetch_page(rid.page_id).unwrap();
        let page = Page::deserialize(bp.frames[frame_i].read().unwrap().data).unwrap();
        bp.unpin_page(rid.page_id, false);
        let s = &page.slot_arr[rid.slot_num as usize];
        (s.offset, s.length)
    }

    /// Rewrite one slot's `length`, keeping everything else, the LSN included.
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
}

#[test]
fn a_release_that_can_never_succeed_does_not_hold_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(&dir.path().join("mismatch.db"));
    let mut main = Session::new();
    // Row 2 (3934 B) first, then row 1 (35 B), the lowest tuple; 96 B stay free.
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));", &mut main);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(3900)), &mut main);
    db.ok("INSERT INTO notes VALUES (1, 'a');", &mut main);
    let home = db.primary_rid(1).expect("row 1");

    let mut t1 = Session::new();
    db.ok("BEGIN;", &mut t1);
    db.ok(&format!("UPDATE notes SET note = '{}' WHERE id = 1;", "y".repeat(200)), &mut t1);
    assert_ne!(db.primary_rid(1), Some(home), "premise: the UPDATE did not relocate row 1");
    assert_eq!(db.slot(home).1, 35 | RETIRED, "premise: the relocation did not retire row 1's 35 B slot");

    // The page disagrees with the log: the retired slot now reads as live, so the release is refused,
    // and would be at every retry.
    db.set_slot_length(home, 35);
    let failures = release_failures();
    let mismatches = release_mismatches();
    db.ok("COMMIT;", &mut t1);
    assert_eq!(
        release_failures(),
        failures,
        "a release that can never succeed was counted as a retryable failure, so it is still pending"
    );
    // A guard added with the fix (the counter is new): the mismatch is counted where a reader sees it.
    assert_eq!(release_mismatches(), mismatches + 1, "the release that can never succeed was not counted as a mismatch");

    let base = db.o.wal.base_lsn.load(Ordering::SeqCst);
    db.o.txn.checkpoint().expect("the checkpoint was refused for a release that can never succeed");
    assert!(db.o.wal.base_lsn.load(Ordering::SeqCst) > base, "the checkpoint did not truncate the log");
    assert_eq!(db.slot(home), (4096 - 3934 - 35, 35), "premise: the page still holds the edited, live slot");
}
