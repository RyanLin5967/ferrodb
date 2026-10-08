//! D256: an open that meets a heap page whose header cannot describe a heap page refuses, naming the
//! page. At `2c10f17` it panicked in `Page::deserialize`.
//!
//! Two of `open_recovered`'s steps parse heap pages from disk:
//! - **the rebuild.** `rebuild_indexes` scans every primary heap, at every open whose log is not
//!   empty. A page the directory lists and no record names is read as it is on disk. I1 makes it
//!   all zero bytes, which is what a page whose write never reached disk holds. At `2c10f17` that
//!   panicked (`bytes[23..0]`) at every such open.
//! - **redo.** `redo_one` parses each page the log names whose header carries the page's own id (a
//!   page whose header names another is taken for one never written, and rebuilt from empty). I2
//!   gives such a page a slot array that starts inside its header (`bytes[23..19]` at `2c10f17`).
//!
//! Lane report: artie-research `frontier/lane_d256_heap_page.md` §2, I1 and I2. INFERRED from
//! source and never run.

use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::sync::atomic::Ordering;

use ferrodb::error::FerroError;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::db_lock::DbLock;
use ferrodb::storage::disk_manager::PAGE_SIZE;
use ferrodb::storage::heap_file_manager::HeapFileManager;
use ferrodb::wal::log::{RecKind, WalManager};
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

    fn ok(&mut self, sql: &str) {
        let mut s = Session::new();
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.o.catalog, self.o.bp.clone(), self.o.txn.clone(), &mut s)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    }

    /// The one heap page holding every row of `table`.
    fn page_of(&self, table: &str) -> u32 {
        let entry = self.o.catalog.get_table(table).expect("table");
        let heap = HeapFileManager::open(entry.first_directory_page_id, self.o.bp.clone());
        let rows = heap.scan().collect::<Result<Vec<_>, _>>().expect("scan");
        let pages: BTreeSet<u32> = rows.iter().map(|(rid, _)| rid.page_id).collect();
        assert_eq!(pages.len(), 1, "premise: {table}'s rows are on one page: {pages:?}");
        *pages.first().unwrap()
    }
}

/// Every heap page the retained log names: the pages the next open's redo will parse. Walked as
/// `recover` walks it, from the base to the end.
fn pages_the_log_names(wal: &WalManager) -> BTreeSet<u32> {
    let mut pages = BTreeSet::new();
    let end = wal.next_lsn.load(Ordering::SeqCst);
    let mut lsn = wal.base_lsn.load(Ordering::SeqCst);
    while lsn < end {
        let (rec, next) = wal.read_record(lsn).expect("read the log");
        let kind = match &rec.kind {
            RecKind::Clr { redo, .. } => redo.as_ref(),
            other => other,
        };
        match kind {
            RecKind::HeapInsert { page_id, .. }
            | RecKind::HeapDelete { page_id, .. }
            | RecKind::HeapUpdate { page_id, .. }
            | RecKind::HeapRelease { page_id, .. } => {
                pages.insert(*page_id);
            }
            _ => {}
        }
        lsn = next;
    }
    pages
}

/// The open of `path` must refuse as corruption, naming heap page `page`: never succeed, never
/// panic.
fn open_refuses_naming(path: &Path, page: u32, which: &str) {
    match catch_unwind(AssertUnwindSafe(|| Db::open(path))) {
        Ok(Err(FerroError::Corruption(msg))) => assert!(
            msg.contains(&format!("heap page {page} ")),
            "{which} refused without naming heap page {page}: {msg}"
        ),
        Ok(Err(e)) => panic!("{which} failed, but not as corruption of heap page {page}: {e}"),
        Ok(Ok(_)) => panic!("{which} SUCCEEDED over a malformed heap page {page}"),
        Err(p) => panic!(
            "{which} PANICKED ({}) on heap page {page} instead of refusing",
            p.downcast_ref::<String>().map(String::as_str).or(p.downcast_ref::<&str>().copied()).unwrap_or("no text")
        ),
    }
}

/// I1: the rebuild meets an all-zero page that the directory lists and no record names.
#[test]
fn an_open_whose_rebuild_meets_a_zero_page_refuses_naming_it_and_so_does_the_next() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("d256_rebuild.db");
    let mut db = Db::open(&path).unwrap();
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(40));");
    db.ok("INSERT INTO notes VALUES (1, 'a');");
    let p = db.page_of("notes");
    // `other` sorts after `notes`, and the rebuild goes in name order, so it reaches `notes` before
    // it has freed any tree: the refusal changes nothing on disk, and the next open meets the same
    // page.
    db.ok("CREATE TABLE other (id INTEGER NOT NULL, note VARCHAR(40));");
    // Every page to disk, P included, and the log truncated past P's insert.
    let _ = db.o.txn.checkpoint().expect("checkpoint");
    db.ok("INSERT INTO other VALUES (1, 'b');");
    let q = db.page_of("other");
    db.o.wal.flush().expect("flush the log");
    let named = pages_the_log_names(&db.o.wal);
    assert!(!named.contains(&p), "premise: the log still names page {p}, so redo would rebuild it: {named:?}");
    assert!(named.contains(&q), "premise: the log does not name page {q}, so the open would not rebuild: {named:?}");
    // P as it is on disk when its write never got there.
    db.o.bp.disk_manager.write(p, &[0u8; PAGE_SIZE]).unwrap();
    // The crash: every handle goes, and nothing more reaches disk.
    drop(db);

    open_refuses_naming(&path, p, "open #1");
    open_refuses_naming(&path, p, "open #2");
}

/// I2: redo meets a page the log names whose header keeps the page's id but not a heap layout.
#[test]
fn an_open_whose_redo_meets_a_malformed_page_refuses_naming_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("d256_redo.db");
    let mut db = Db::open(&path).unwrap();
    db.ok("CREATE TABLE notes (id INTEGER NOT NULL, note VARCHAR(40));");
    db.ok("INSERT INTO notes VALUES (1, 'a');");
    // P to disk with row 1, and the log truncated; row 2's insert is then the log's record of P.
    let _ = db.o.txn.checkpoint().expect("checkpoint");
    db.ok("INSERT INTO notes VALUES (2, 'b');");
    let p = db.page_of("notes");
    db.o.wal.flush().expect("flush the log");
    let named = pages_the_log_names(&db.o.wal);
    assert!(named.contains(&p), "premise: the log does not name page {p}, so redo would not parse it: {named:?}");
    // P on disk keeps its own id, so redo parses it rather than rebuilding it from empty. Its slot
    // array now starts at byte 19, inside the 23-byte header.
    let mut bytes = db.o.bp.disk_manager.read(p).unwrap();
    assert_eq!(u32::from_be_bytes(bytes[1..5].try_into().unwrap()), p, "premise: page {p}'s bytes on disk do not carry its id");
    bytes[7..9].copy_from_slice(&19u16.to_be_bytes());
    db.o.bp.disk_manager.write(p, &bytes).unwrap();
    drop(db);

    open_refuses_naming(&path, p, "the open");
}
