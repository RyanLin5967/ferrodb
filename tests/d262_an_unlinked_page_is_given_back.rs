//! D262: a page allocated and then never linked stays allocated for good.
//!
//! RED FIRST, against `8ae086d`, and UNBUILT: written in quiet mode, where no compiler runs. It uses
//! only API that exists at `8ae086d`. Pre-registration: artie-research
//! `frontier/lane_d260_d262_new_page.md` §D262.2.
//!
//! `DiskManager::allocate` writes the page's bitmap bit to disk before anything else happens to the
//! page. `HeapFileManager::add_empty_page` then writes the empty page and lists it in the directory,
//! and `BufferPoolManager::new_page` writes it and loads it, each step with `?`. A failure in
//! between returned with the page allocated and named by nothing: no directory lists it, so no DROP
//! frees it, and `allocate` never hands out a set bit again.
//!
//! The faults are the real limits, with no test seam: the allocator floor (`reserve_from`, the way
//! `integration_alter_refusal_safety` closes the allocator) and a pool whose every frame is pinned.
//!
//! What each test pins, and the mutant it kills (`bench/d260_d262/firecheck.sh`):
//! - R1 `a_data_page_the_directory_cannot_list_is_given_back`: OA.
//! - R2 `a_page_new_page_cannot_load_is_given_back`: OB.
//! - R3 `a_page_that_is_linked_stays_allocated_and_listed`: the control that the give-back fires
//!   only on failure. OC.

use std::fs::OpenOptions;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::error::FerroError;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::heap_file_manager::HeapFileManager;
use ferrodb::storage::heap_page::HEADER_SIZE;
use ferrodb::storage::page_directory::PageDirectory;

/// The most entries one directory page holds: `(PAGE_SIZE - 11) / 6`, its 11-byte header and
/// 6-byte entries (`page_directory.rs`).
const ENTRIES_PER_DIRECTORY_PAGE: usize = (PAGE_SIZE - 11) / 6;

fn pool() -> (Arc<BufferPoolManager>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("d262.db"))
        .unwrap();
    (Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap()))), dir)
}

/// Is `page`'s bit set in the allocator's first bitmap page? Read from the FILE, where `allocate`
/// and `deallocate` keep it, not from the pool.
fn allocated(bp: &BufferPoolManager, page: u32) -> bool {
    assert!(page < (PAGE_SIZE as u32 - 4) * 8, "page {page} is past the first bitmap page");
    let bitmap = bp.disk_manager.read(0).unwrap();
    bitmap[4 + (page / 8) as usize] & (1 << (page % 8)) != 0
}

/// The page `allocate` hands out next: the first clear bit, its own rule on the first bitmap page.
fn lowest_free(bp: &BufferPoolManager) -> u32 {
    let bitmap = bp.disk_manager.read(0).unwrap();
    (0..(PAGE_SIZE as u32 - 4) * 8)
        .find(|&p| bitmap[4 + (p / 8) as usize] & (1 << (p % 8)) == 0)
        .expect("a free page in the first bitmap")
}

fn pins(bp: &BufferPoolManager, page: u32) -> u16 {
    bp.frames
        .iter()
        .map(|f| {
            let f = f.read().unwrap();
            if f.page_id == Some(page) { f.pin_counter.load(Ordering::Relaxed) } else { 0 }
        })
        .sum()
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
fn a_data_page_the_directory_cannot_list_is_given_back() {
    let (bp, _dir) = pool();
    let heap = HeapFileManager::new(bp.clone()).unwrap();
    let per_page = PAGE_SIZE - HEADER_SIZE;

    // Fill the first directory page exactly, so the next data page needs a new directory page.
    let added = heap.reserve_free_space(ENTRIES_PER_DIRECTORY_PAGE * per_page).unwrap();
    assert_eq!(added, ENTRIES_PER_DIRECTORY_PAGE, "premise: one directory page's worth of pages added");

    // Leave exactly one page below the floor: the data page gets it, the directory page gets none.
    let n = lowest_free(&bp);
    bp.disk_manager.reserve_from(n + 1).unwrap();

    let err = heap
        .reserve_free_space((ENTRIES_PER_DIRECTORY_PAGE + 1) * per_page)
        .expect_err("premise: with one page left, growing past a full directory page must fail");
    assert!(
        err.to_string().contains("reserved arena region"),
        "premise: the refusal must be the allocator's floor, met by the directory page: `{err}`"
    );
    let now_listed = listed(&bp, heap.first_directory_page_id);
    assert_eq!(now_listed.len(), ENTRIES_PER_DIRECTORY_PAGE, "premise: the directory gained no entry");
    assert!(!now_listed.contains(&n), "premise: page {n} is listed nowhere");

    assert!(
        !allocated(&bp, n),
        "page {n} is still allocated after its directory entry could not be written (`{err}`): it \
         belongs to no directory, so nothing will ever free or reuse it"
    );
    assert_eq!(pins(&bp, n), 0, "page {n} was given back with a pin on it");
    assert_eq!(bp.new_page().unwrap(), n, "page {n} did not come back to the allocator");
}

#[test]
fn a_page_new_page_cannot_load_is_given_back() {
    let (bp, _dir) = pool();
    let mut held = Vec::new();
    for _ in 0..bp.frames.len() {
        let id = bp.new_page().unwrap();
        bp.fetch_page(id).unwrap();
        held.push(id);
    }
    let n = lowest_free(&bp);

    let err = bp.new_page().expect_err("premise: with every frame pinned, new_page must fail");
    assert!(
        matches!(err, FerroError::NotEnoughSpace),
        "premise: the refusal must be the full pool's, after the allocation: `{err}`"
    );
    assert!(
        !allocated(&bp, n),
        "page {n} is still allocated after new_page failed to load it (`{err}`): new_page returned \
         no id, so no caller could ever free it"
    );

    for id in held {
        bp.unpin_page(id, false);
    }
    assert_eq!(bp.new_page().unwrap(), n, "page {n} did not come back to the allocator");
}

#[test]
fn a_page_that_is_linked_stays_allocated_and_listed() {
    let (bp, _dir) = pool();
    let heap = HeapFileManager::new(bp.clone()).unwrap();
    let n = lowest_free(&bp);
    assert_eq!(heap.reserve_free_space(1).unwrap(), 1, "premise: one page added");
    assert_eq!(listed(&bp, heap.first_directory_page_id), vec![n], "the page added is not the one listed");
    assert!(allocated(&bp, n), "a page the directory lists was given back to the allocator");
    assert_eq!(pins(&bp, n), 0, "the page added is still pinned");
}
