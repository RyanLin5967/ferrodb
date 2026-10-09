//! D260: `new_page` returns its page unpinned, and three callers unpinned it again.
//!
//! RED FIRST, against `bb6cde7`, and UNBUILT: written in quiet mode, where no compiler runs. It uses
//! only API that exists at `bb6cde7`. Pre-registration: artie-research
//! `frontier/lane_d260_d262_new_page.md` §D260.2.
//!
//! `BufferPoolManager::new_page` fetches the page it allocated and unpins it before returning
//! (since `575147f`). `HeapFileManager::new`, `add_empty_page` and
//! `TableBranchCatalog::create_with_header` each called `unpin_page(id, false)` after it anyway. The
//! pin count never goes below zero, so alone that does nothing; but a second holder of the page lost
//! its pin to it, and a page whose pin is gone can be evicted or freed under the one still using it.
//!
//! The second holder is made deterministic by `a_held_free_page`: a page is allocated and freed, so
//! it is the lowest free page and the next allocation hands it out, and then pinned from outside.
//! The count at the moment of an extra unpin is the same as with a holder that pinned it between
//! `new_page`'s return and the extra unpin, which is the schedule the ledger row names.
//!
//! What each test pins, and the mutant it kills (`bench/d260_d262/firecheck.sh`):
//! - T1 `heap_new_leaves_a_second_holders_pin_on_its_directory_page`: NA, ND.
//! - T2 `add_empty_page_leaves_a_second_holders_pin_on_the_page_it_adds`: NB, ND.
//! - T3 `create_with_header_leaves_a_second_holders_pin_on_its_header_page`: NC, ND.
//! - T4 `new_page_takes_back_its_own_pin_and_only_its_own`: the contract the removal rests on. ND.

use std::fs::OpenOptions;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::HeapFileManager;
use ferrodb::storage::page_directory::PageDirectory;

fn pool() -> (Arc<BufferPoolManager>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("d260.db"))
        .unwrap();
    (Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap()))), dir)
}

/// Pins held on `page`, read off the frames. A page no frame holds has none.
fn pins(bp: &BufferPoolManager, page: u32) -> u16 {
    bp.frames
        .iter()
        .map(|f| {
            let f = f.read().unwrap();
            if f.page_id == Some(page) { f.pin_counter.load(Ordering::Relaxed) } else { 0 }
        })
        .sum()
}

/// A page the next allocation will hand out, pinned by a second holder. See the module doc.
fn a_held_free_page(bp: &BufferPoolManager) -> u32 {
    let p = bp.new_page().unwrap();
    bp.free_page(p).unwrap();
    bp.fetch_page(p).unwrap();
    assert_eq!(pins(bp, p), 1, "premise: the second holder's pin on page {p}");
    p
}

/// The data pages a heap's directory chain lists.
fn listed(bp: &BufferPoolManager, first_dir: u32) -> Vec<u32> {
    let mut out = Vec::new();
    let mut dir_id = first_dir;
    while dir_id != 0 {
        let frame_i = bp.fetch_page(dir_id).unwrap();
        let dir = PageDirectory::deserialize(bp.frames[frame_i].read().unwrap().data);
        bp.unpin_page(dir_id, false);
        out.extend(dir.entries.iter().map(|e| e.page_id));
        dir_id = dir.next_page_directory;
    }
    out
}

#[test]
fn heap_new_leaves_a_second_holders_pin_on_its_directory_page() {
    let (bp, _dir) = pool();
    let p = a_held_free_page(&bp);
    let heap = HeapFileManager::new(bp.clone()).unwrap();
    assert_eq!(heap.first_directory_page_id, p, "premise: the directory page is the held page");
    assert_eq!(pins(&bp, p), 1, "HeapFileManager::new took the second holder's pin on page {p}");
    bp.unpin_page(p, false);
}

#[test]
fn add_empty_page_leaves_a_second_holders_pin_on_the_page_it_adds() {
    let (bp, _dir) = pool();
    let heap = HeapFileManager::new(bp.clone()).unwrap();
    let p = a_held_free_page(&bp);
    assert_eq!(heap.reserve_free_space(1).unwrap(), 1, "premise: exactly one page was added");
    assert_eq!(
        listed(&bp, heap.first_directory_page_id),
        vec![p],
        "premise: the page added is the held page"
    );
    assert_eq!(pins(&bp, p), 1, "add_empty_page took the second holder's pin on page {p}");
    bp.unpin_page(p, false);
}

#[test]
fn create_with_header_leaves_a_second_holders_pin_on_its_header_page() {
    let (bp, _dir) = pool();
    let p = a_held_free_page(&bp);
    let (_catalog, header) = TableBranchCatalog::create_with_header(bp.clone(), 1).unwrap();
    assert_eq!(header, p, "premise: the header page is the held page");
    assert_eq!(pins(&bp, p), 1, "create_with_header took the second holder's pin on page {p}");
    bp.unpin_page(p, false);
}

#[test]
fn new_page_takes_back_its_own_pin_and_only_its_own() {
    let (bp, _dir) = pool();
    let q = bp.new_page().unwrap();
    assert_eq!(pins(&bp, q), 0, "new_page returned page {q} still pinned; its contract is 'returned unpinned'");

    let p = a_held_free_page(&bp);
    assert_eq!(bp.new_page().unwrap(), p, "premise: the next allocation is the held page");
    assert_eq!(pins(&bp, p), 1, "new_page changed the second holder's pin count on page {p}");
    bp.unpin_page(p, false);
}
