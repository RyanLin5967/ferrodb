//! **D128 site 2** — the durable pending-free queue must survive a fallible step mid-drain.
//!
//! The shape the row is about: take a pending collection out of shared state, process it, write
//! back what remains. A `?` between the take and the write-back drops *every* unprocessed entry
//! silently — they are not retried, not logged, and never revisited, because nothing names them
//! any more.
//!
//! `drain_pending_free` had exactly that: `mem::take(&mut inner.pending)`, a loop containing
//! `inner.release_page(..)?`, and `inner.pending = still_pending` on the last line.
//!
//! This module is a child of [`crate::cow::store`] so that it can build the defect's precondition
//! directly. That matters: `release_page`'s reachable failure is a **double free**, and reaching
//! one through the public API would mean reproducing the very race D125 fixed. Constructing the
//! state is not a weaker test than provoking it — the fallible call and the queue are the subject,
//! and how the page came to be double-freed is not.
//!
//! ⚠ **Both tests are fire-checked**: each asserts a property that the pre-fix code *cannot*
//! satisfy, and `bench/d128_firecheck.txt` records them failing against the old shape. A
//! restoration test that passes on the broken code is worse than no test, because it certifies
//! the defect.

use std::fs::OpenOptions;
use std::sync::Arc;

use tempfile::TempDir;

use crate::branch::record::PendingFree;
use crate::branch::types::{ArenaId, BranchId, Epoch, PageId};
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::storage::disk_manager::DiskManager;

use super::{ArenaExtent, CowStore, ExtentState};

/// A store with one 16-page extent owned by `BranchId::TRUNK`, and no live children — so
/// `reclaimable(&[], ..)` is vacuously true and every entry below is "clear" to release.
fn store_with_one_extent(dir: &TempDir) -> CowStore {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("cow.db"))
        .unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let store = CowStore::with_extent_pages(pool, 16);
    {
        let mut inner = store.inner.lock().unwrap();
        inner.extents.insert(
            1,
            ExtentState {
                extent: ArenaExtent {
                    arena_id: ArenaId(1),
                    owner: BranchId::TRUNK,
                    start_page: 100,
                    page_count: 16,
                    next_free: 16,
                },
                free_pages: Vec::new(),
            },
        );
    }
    store
}

fn entry(page: PageId) -> PendingFree {
    PendingFree {
        page_id: page,
        arena_id: ArenaId(1),
        birth_epoch: Epoch(1),
        free_epoch: Epoch(2),
        owner: BranchId::TRUNK,
    }
}

/// ⭐ **THE ROW'S TEST.** A `release_page` failure part-way through a drain must leave the queue
/// holding every entry that was not successfully released — the failing one included.
///
/// Pre-fix this fails on the very first assertion: `mem::take` emptied `pending`, the `?` returned
/// before the write-back, and the queue is *empty* — all five entries gone from a durable log.
#[test]
fn a_failed_release_mid_drain_loses_no_pending_entry() {
    let dir = TempDir::new().unwrap();
    let store = store_with_one_extent(&dir);

    // Page 103 is pre-freed, so releasing it again is the "double free of page 103" refusal —
    // `release_page`'s own guard, not an injected fault.
    {
        let mut inner = store.inner.lock().unwrap();
        inner.extents.get_mut(&1).unwrap().free_pages.push(103);
        inner.pending = vec![entry(100), entry(101), entry(103), entry(104), entry(105)];
    }
    assert_eq!(store.pending_free_len(), 5, "fixture: five entries parked before the drain");

    let err = store.drain_pending_free().expect_err(
        "fixture: the drain must actually fail, or this test proves nothing about the error path",
    );
    assert!(
        err.to_string().contains("double free"),
        "fixture: expected the double-free refusal, got: {err}",
    );

    // THE PROPERTY. Nothing that was not released may have vanished.
    let left: Vec<PageId> = {
        let inner = store.inner.lock().unwrap();
        inner.pending.iter().map(|pf| pf.page_id).collect()
    };
    assert!(
        !left.is_empty(),
        "D128 site 2: the drain returned Err and the pending-free log is EMPTY. Every entry it \
         had not finished with was dropped — silently, from a durable log, never revisited.",
    );
    assert!(
        left.contains(&103),
        "D128 site 2: the entry whose release FAILED must stay pending so the next drain retries \
         it. left = {left:?}",
    );

    // Exactly the un-released ones remain: 103 (failed) plus whatever the loop never reached.
    // Order is not asserted — `swap_remove` does not preserve it and does not need to.
    let freed = {
        let inner = store.inner.lock().unwrap();
        inner.extents[&1].free_pages.clone()
    };
    for p in &left {
        assert!(
            !freed.contains(p) || *p == 103,
            "D128 site 2: page {p:?} is BOTH released and still pending — a later drain would \
             double-free it. left = {left:?}, freed = {freed:?}",
        );
    }
    let accounted = left.len() + freed.len() - 1 /* 103 was pre-freed by the fixture */;
    assert_eq!(
        accounted, 5,
        "D128 site 2: {} entries accounted for, 5 went in. left = {left:?}, freed = {freed:?}",
        accounted,
    );
}

/// The drain is **retryable**, which is the whole reason to keep the entries. Clear the condition
/// that made it fail and the next drain must finish the work the first one left.
///
/// Pre-fix this fails too, and more damningly: the retry returns `Ok(0)` over an empty queue, so
/// the loss reads as success.
#[test]
fn the_next_drain_finishes_what_the_failed_one_left() {
    let dir = TempDir::new().unwrap();
    let store = store_with_one_extent(&dir);
    {
        let mut inner = store.inner.lock().unwrap();
        inner.extents.get_mut(&1).unwrap().free_pages.push(103);
        inner.pending = vec![entry(100), entry(101), entry(103), entry(104), entry(105)];
    }
    store.drain_pending_free().expect_err("fixture: the first drain must fail");

    // Clear the blocker the way a real retry would: 103 is genuinely free now, so drop its
    // entry — the page is already in `free_pages` and nothing more is owed on it.
    {
        let mut inner = store.inner.lock().unwrap();
        inner.pending.retain(|pf| pf.page_id != 103);
    }
    let remaining = store.pending_free_len();
    assert!(
        remaining > 0,
        "D128 site 2: after removing the one bad entry there is nothing left to retry, which \
         means the failed drain had already thrown the rest away.",
    );

    let released = store.drain_pending_free().expect("the retry must succeed");
    assert_eq!(
        released as usize, remaining,
        "D128 site 2: the retry released {released} of {remaining} still-pending entries",
    );
    assert_eq!(store.pending_free_len(), 0, "the queue must be empty once everything is released");
}
