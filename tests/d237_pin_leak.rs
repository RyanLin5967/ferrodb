//! D237: a pin leaked on an error path makes DROP TABLE free half a table.
//!
//! RED FIRST, against `9aa6968`, and UNBUILT: written in quiet mode, where no compiler runs. It
//! uses only API that exists at `9aa6968`, so it compiles against the base, and each test is
//! predicted red there for the reason its own message names.
//!
//! - `fetch_page` pins and returns a bare frame index; the caller unpins by hand. Every `?` between
//!   the two returned with the page still pinned. `HeapFileManager::read` is one: `Page::read`
//!   answers `SlotDeleted` for a freed slot, and the `?` skips the unpin.
//! - D202 reaches that from SQL: a rolled-back INSERT leaves its primary entry naming the freed
//!   slot, and the next INSERT of the key reads it (`execution::insert`, `self.heap.read(existing)?`).
//!   The pin lives as long as the process.
//! - `free_page` refuses a pinned page, and `Catalog::drop_table` freed one structure after another
//!   with `?`. A refusal part way returned with the pages before it already freed and the table still
//!   in the catalog, which names them. The next table to allocate gets them, and a retried DROP frees
//!   them again from under it.
//!
//! What each test pins, and the mutant it kills (`bench/d237/firecheck.sh`):
//! - `a_read_that_fails_unpins_its_page`: the heap-level trigger, no SQL. **This is the mechanism's
//!   killer**: it forces `HeapFileManager::read`'s error path directly, so it does not depend on any
//!   other defect staying unfixed. Kills M1 (the hand-written unpin back in `HeapFileManager::read`).
//! - `a_reinserted_rolled_back_key_leaves_no_page_pinned_and_drop_is_all_or_none`: the lead's SQL
//!   schedule. It asserts only what must hold whether the re-INSERT fails or not. At `9aa6968` it
//!   fails (D202), and the leak makes this test red. Once D202's fix lands (`rollback-index-orphan`,
//!   #16), the re-INSERT succeeds, nothing takes the error path, and the test stays green without
//!   measuring the leak. That is why the heap-level test above is the killer and this one is not.
//! - `a_drop_refused_for_a_pinned_page_has_freed_nothing`: holds a pin by hand on the page freed
//!   last, the primary root. Kills M2 (the batch free without its check) and M3 (`drop_table` back
//!   to one free per structure).

use std::fs::OpenOptions;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::heap_file_manager::{HeapFileManager, RecordId};
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::index_page::BPlusTreePage;
use ferrodb::storage::page_directory::PageDirectory;
use ferrodb::storage::tuple::Tuple;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
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
            .open(dir.path().join("d237.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d237.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, _dir: dir }
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
}

/// Pins held on `page`, read off the frame that holds it. A page no frame holds has none.
fn pins(bp: &BufferPoolManager, page: u32) -> u16 {
    bp.frames
        .iter()
        .map(|f| f.read().unwrap())
        .filter(|f| f.page_id == Some(page))
        .map(|f| f.pin_counter.load(Ordering::Relaxed))
        .sum()
}

/// Is `page`'s bit set in the allocator's first bitmap page? Read from the file, which is where
/// `DiskManager::allocate` and `deallocate` keep it, not from the pool.
fn allocated(bp: &BufferPoolManager, page: u32) -> bool {
    assert!(page < (PAGE_SIZE as u32 - 4) * 8, "page {page} is past the first bitmap page");
    let bitmap = bp.disk_manager.read(0).unwrap();
    bitmap[4 + (page / 8) as usize] & (1 << (page % 8)) != 0
}

/// Every page a heap owns: each data page its directory lists, then that directory page, down the
/// chain. Read through the pool, so a directory the statements dirtied is seen as it is now.
fn heap_pages(bp: &BufferPoolManager, first_dir: u32) -> Vec<u32> {
    let mut out = Vec::new();
    let mut dir_id = first_dir;
    while dir_id != 0 {
        let frame_i = bp.fetch_page(dir_id).unwrap();
        let dir = PageDirectory::deserialize(bp.frames[frame_i].read().unwrap().data);
        bp.unpin_page(dir_id, false);
        out.extend(dir.entries.iter().map(|e| e.page_id));
        out.push(dir_id);
        dir_id = dir.next_page_directory;
    }
    out
}

/// Every page of a primary tree, children before their parent.
fn tree_pages(bp: &Arc<BufferPoolManager>, root: u32) -> Vec<u32> {
    let tree = BPlusTreeManager::<Value, RecordId>::open(root, bp.clone());
    let mut out = Vec::new();
    let mut stack = vec![(root, false)];
    while let Some((page, expanded)) = stack.pop() {
        if expanded {
            out.push(page);
            continue;
        }
        stack.push((page, true));
        if let BPlusTreePage::Internal(node) = tree.read_node(page).unwrap() {
            stack.extend(node.child_ptrs.iter().rev().map(|&c| (c, false)));
        }
    }
    out
}

/// Every page `t` owns, in the order `drop_table` frees them: heap, time-travel heap, primary tree.
/// `t` has no secondary or full-text index in these tests.
fn table_pages(db: &Db, name: &str) -> Vec<u32> {
    let entry = db.catalog.get_table(name).expect("table exists");
    assert!(
        entry.indexes.is_empty() && entry.fulltext_indexes.is_empty(),
        "premise: `{name}` has no secondary or full-text index, so heap + time travel + primary is all of it"
    );
    let mut pages = heap_pages(&db.bp, entry.first_directory_page_id);
    pages.extend(heap_pages(&db.bp, entry.time_travel_root));
    pages.extend(tree_pages(&db.bp, entry.primary_index_root));
    pages
}

/// The lead's exit for DROP: it succeeds and frees every page of the table, or it refuses having
/// freed none of them and leaves the table named. `pages` must be read before the DROP.
fn drop_is_all_or_none(db: &mut Db, s: &mut Session, name: &str, pages: &[u32]) -> Result<(), FerroError> {
    for &p in pages {
        assert!(allocated(&db.bp, p), "premise: page {p} of `{name}` does not read as allocated before the DROP");
    }
    let outcome = db.exec(&format!("DROP TABLE {name};"), s).map(|_| ());
    let freed: Vec<u32> = pages.iter().copied().filter(|&p| !allocated(&db.bp, p)).collect();
    match &outcome {
        Ok(()) => {
            assert_eq!(
                freed.len(),
                pages.len(),
                "DROP TABLE {name} succeeded and freed only {freed:?} of {pages:?}"
            );
            assert!(db.catalog.get_table(name).is_none(), "DROP TABLE {name} succeeded and left it in the catalog");
        }
        Err(e) => {
            assert!(
                freed.is_empty(),
                "DROP TABLE {name} refused with `{e}` having already freed {freed:?} of {pages:?}; the catalog \
                 still names them, so the next allocation aliases a live table"
            );
            assert!(db.catalog.get_table(name).is_some(), "a refused DROP TABLE {name} removed it from the catalog");
        }
    }
    outcome
}

#[test]
fn a_read_that_fails_unpins_its_page() {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("heap.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let heap = HeapFileManager::new(bp.clone()).unwrap();

    let rid = heap.insert(Tuple::new(vec![7u8; 16])).unwrap();
    heap.delete(rid).unwrap();
    assert_eq!(pins(&bp, rid.page_id), 0, "premise: page {} is pinned before the read", rid.page_id);

    // A match, not `expect_err`: that needs `Tuple: Debug`, and `Tuple` derives nothing.
    let err = match heap.read(rid) {
        Ok(_) => panic!("premise failed: a read of a deleted slot succeeded"),
        Err(e) => e,
    };
    assert!(matches!(err, FerroError::SlotDeleted), "premise: expected SlotDeleted, got `{err}`");
    assert_eq!(
        pins(&bp, rid.page_id),
        0,
        "HeapFileManager::read returned `{err}` and left page {} pinned",
        rid.page_id
    );
}

/// `BEGIN; INSERT (5); ROLLBACK; INSERT (5)`, then: no page of `t` is pinned, and DROP frees every
/// page or refuses having freed none. **Nothing is asserted about the re-INSERT's own outcome.** At
/// `9aa6968` it fails through D202 and leaks a pin, which the no-pin assertion catches. After D202's
/// fix it succeeds and leaks nothing, and both assertions still hold. A test whose premise were "the
/// re-INSERT fails" would go red on main the day #16 lands, and fixing it then would be a test edit.
/// This test is therefore not the mechanism's killer; `a_read_that_fails_unpins_its_page` is.
#[test]
fn a_reinserted_rolled_back_key_leaves_no_page_pinned_and_drop_is_all_or_none() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut s);
    db.ok("INSERT INTO t VALUES (1, 10);", &mut s);
    db.ok("BEGIN;", &mut s);
    db.ok("INSERT INTO t VALUES (5, 50);", &mut s);
    db.ok("ROLLBACK;", &mut s);

    // Recorded for the failure message only.
    let outcome = match db.exec("INSERT INTO t VALUES (5, 51);", &mut s) {
        Ok(_) => "succeeded".to_string(),
        Err(e) => format!("failed with `{e}`"),
    };

    let pages = table_pages(&db, "t");
    let pinned: Vec<(u32, u16)> = pages.iter().map(|&p| (p, pins(&db.bp, p))).filter(|&(_, n)| n > 0).collect();
    assert!(
        pinned.is_empty(),
        "the re-INSERT of a rolled-back key {outcome} and left these pages of t pinned (page, pins): {pinned:?}"
    );

    let _ = drop_is_all_or_none(&mut db, &mut s, "t", &pages);
}

#[test]
fn a_drop_refused_for_a_pinned_page_has_freed_nothing() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, pad VARCHAR(200));", &mut s);
    for i in 0..40 {
        db.ok(&format!("INSERT INTO t VALUES ({i}, '{}');", "x".repeat(200)), &mut s);
    }

    let entry = db.catalog.get_table("t").unwrap();
    let (first_dir, root) = (entry.first_directory_page_id, entry.primary_index_root);
    let data_pages = heap_pages(&db.bp, first_dir).len() - 1;
    assert!(data_pages >= 2, "premise: t's rows span {data_pages} heap data page(s), wanted at least 2");
    let pages = table_pages(&db, "t");
    assert_eq!(
        pages.last(),
        Some(&root),
        "premise: the primary root is the page DROP frees last, so every other page of t is freed before it"
    );

    // Hold a pin on it, as a leaked one would.
    db.bp.fetch_page(root).unwrap();
    let refused = drop_is_all_or_none(&mut db, &mut s, "t", &pages);
    assert!(refused.is_err(), "DROP TABLE t succeeded while page {root} of t was pinned");

    db.bp.unpin_page(root, false);
    assert_eq!(pins(&db.bp, root), 0, "premise: the test's own pin on {root} is released");
    drop_is_all_or_none(&mut db, &mut s, "t", &pages)
        .unwrap_or_else(|e| panic!("DROP TABLE t refused with nothing pinned: {e}"));
}
