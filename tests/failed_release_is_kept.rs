//! F2 of the adversary on `993145c..115f0b7` (`frontier/rollback_adversary.md` @ `4902c75`): a release
//! that fails at commit must not be lost at the next checkpoint.
//!
//! A committed transaction frees the slots its deletes retired, one logged `HeapRelease` each. When one
//! fails (an I/O error, or a page that disagrees with the log), it used to be counted and forgotten,
//! and the next checkpoint truncated the log that recovery would have re-derived it from, so the slot
//! stayed retired for good.
//!
//! The lead's decision: a failed release waits in a pending list. The checkpoint retries it BEFORE
//! truncating, and refuses to truncate while it still fails, so the log remains the durable record of
//! what is owed. There is no sweep of pages at open, because that would be a restart wall.
//!
//! The failure is staged by editing the page so the retired slot reads as LIVE, which the release
//! refuses, and then repaired. This binary holds one test, because it reads
//! `wal::txn::release_failures`, a process-wide counter. INFERRED from source and never run.

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
use ferrodb::wal::txn::release_failures;

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
fn a_release_that_fails_at_commit_holds_the_log_until_a_checkpoint_retries_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(&dir.path().join("pending.db"));
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

    // The page disagrees with the log: the retired slot now reads as live, so the release is refused.
    db.set_slot_length(home, 35);
    let failures = release_failures();
    db.ok("COMMIT;", &mut t1);
    assert_eq!(release_failures(), failures + 1, "premise: the release did not fail");

    let base = db.o.wal.base_lsn.load(Ordering::SeqCst);
    assert!(
        db.o.txn.checkpoint().is_err(),
        "the checkpoint truncated the log past a release that still fails, so the slot is retired for good"
    );
    assert_eq!(db.o.wal.base_lsn.load(Ordering::SeqCst), base, "a refused checkpoint truncated the log anyway");

    // Repaired: the next checkpoint's retry releases the slot, then truncates.
    db.set_slot_length(home, 35 | RETIRED);
    db.o.txn.checkpoint().expect("the checkpoint was refused after the release could succeed");
    assert_eq!(db.slot(home), (0, 0), "the checkpoint's retry did not release the slot");
    assert!(db.o.wal.base_lsn.load(Ordering::SeqCst) > base, "the checkpoint did not truncate once nothing was owed");
    assert_eq!(release_failures(), failures + 1, "a retry that succeeded was counted as a failure");
}
