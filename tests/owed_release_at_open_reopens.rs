//! N1 of review 2 on `56f6752..368d0e1` (`frontier/rollback_review2.md` @ artie-research `5f6b821`): an
//! open whose owed release fails must leave a database the NEXT open can read.
//!
//! At `368d0e1`, `open_recovered` skipped its checkpoint while a release was owed. But the rebuild
//! before it had already freed every old index tree and created the new one: `deallocate` clears the
//! bitmap bit on disk at once, and `new_page` writes a ZERO page on disk at once. Only the new nodes
//! and the catalog stayed in the pool, and nothing flushed them. So the next open's rebuild walked
//! the old root named by the on-disk catalog, found zeros ("invalid page type header"), and failed.
//! That repeated at every open, even after the release could succeed.
//!
//! The lead's decision: refuse the TRUNCATION, never the flush. While a release is owed, the
//! checkpoint still flushes the log and every page, and syncs; it skips only the truncation and the
//! replays after it.
//!
//! The release is made to fail by the page itself: the committed relocation's page is rewritten on
//! disk without the retired slot and with an LSN past the whole log, so redo leaves it alone and the
//! release finds no such slot. INFERRED from source and never run.

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
use ferrodb::storage::heap_page::Page;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::wal::recovery::{open_recovered, OpenedDatabase};

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

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.o.catalog, self.o.bp.clone(), self.o.txn.clone(), s)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn by_key(&mut self, id: i32) -> Vec<Vec<Value>> {
        let mut s = Session::new();
        match self.ok(&format!("SELECT id, note FROM notes WHERE id = {id};"), &mut s) {
            Outcome::Rows(r) => r,
            _ => panic!("SELECT did not return rows"),
        }
    }

    fn primary_rid(&self, key: i32) -> Option<RecordId> {
        let entry = self.o.catalog.get_table("notes").expect("table");
        BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, self.o.bp.clone())
            .search(&Value::Integer(key))
            .expect("primary search")
    }
}

fn note(id: i32, text: &str) -> Vec<Value> {
    vec![Value::Integer(id), Value::Varchar(text.to_string())]
}

#[test]
fn an_open_whose_owed_release_fails_leaves_a_database_the_next_open_can_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("owed.db");
    let mut db = Db::open(&path).unwrap();
    let mut main = Session::new();
    // Row 2 (3934 B), then row 1 (35 B), the lowest tuple on page P.
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(4000));", &mut main);
    db.ok(&format!("INSERT INTO notes VALUES (2, '{}');", "x".repeat(3900)), &mut main);
    db.ok("INSERT INTO notes VALUES (1, 'a');", &mut main);
    let home = db.primary_rid(1).expect("row 1");
    let mut t1 = Session::new();
    db.ok("BEGIN;", &mut t1);
    db.ok(&format!("UPDATE notes SET note = '{}' WHERE id = 1;", "y".repeat(200)), &mut t1);
    assert_ne!(db.primary_rid(1), Some(home), "premise: the UPDATE did not relocate row 1");
    db.ok("COMMIT;", &mut t1);

    // Page P on disk: row 2 alone, with no slot 1, and an LSN past everything in the log, so redo
    // leaves it as it is and the owed release of slot 1 fails at the next open.
    {
        let bp = &db.o.bp;
        let frame_i = bp.fetch_page(home.page_id).unwrap();
        let mut page = Page::deserialize(bp.frames[frame_i].read().unwrap().data).unwrap();
        bp.unpin_page(home.page_id, false);
        page.slot_arr.truncate(home.slot_num as usize);
        page.lsn = db.o.wal.next_lsn.load(Ordering::SeqCst);
        bp.disk_manager.write(home.page_id, &page.serialize().unwrap()).unwrap();
    }
    // The crash: every handle goes, and nothing more reaches disk.
    drop(main);
    drop(t1);
    drop(db);

    // Open #1 replays the log, owes the release, fails it, and rebuilds every index.
    let db = Db::open(&path).unwrap_or_else(|e| panic!("open #1 failed: {e}"));
    drop(db);

    // Open #2 rebuilds again from what open #1 left on disk.
    let mut db = Db::open(&path).unwrap_or_else(|e| panic!("open #2 failed after an open whose owed release failed: {e}"));
    assert_eq!(db.by_key(1), vec![note(1, &"y".repeat(200))], "row 1 is not reachable by key after open #2");
    assert_eq!(db.by_key(2), vec![note(2, &"x".repeat(3900))], "row 2 is not reachable by key after open #2");
}
