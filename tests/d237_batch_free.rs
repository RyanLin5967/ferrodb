//! D237 review F2 and F4: `BufferPoolManager::free_pages` must free a whole set or none of it,
//! whatever else is running and whichever page cannot be freed.
//!
//! RED FIRST against `f651096`'s `src/`, and UNBUILT: written in quiet mode, where no compiler runs.
//! These tests use `free_pages`, which `9aa6968` does not have, so they live apart from
//! `d237_pin_leak.rs`, whose red arm is `9aa6968`.
//!
//! - **F4.** `f651096` checked pins, then called `free_page` per page. A page that `deallocate`
//!   refuses deterministically (unmapped, or inside a reserved region) therefore refused after the
//!   pages before it were already free. The caller still named those pages, and a retry freed
//!   them again, from under whoever had been handed them in between.
//! - **F2.** `f651096` let go of every lock between its pin check and its frees. A pin taken in
//!   between, by a hot base backup (`replication::backup::take` pins every page outside the
//!   statement lock), made `free_page` refuse part way.

use std::fs::OpenOptions;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

fn pool() -> (Arc<BufferPoolManager>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("batch.db"))
        .unwrap();
    (Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap()))), dir)
}

/// Is `page`'s bit set in the allocator's first bitmap page? Read from the file, where
/// `DiskManager` keeps it.
fn allocated(bp: &BufferPoolManager, page: u32) -> bool {
    assert!(page < (PAGE_SIZE as u32 - 4) * 8, "page {page} is past the first bitmap page");
    let bitmap = bp.disk_manager.read(0).unwrap();
    bitmap[4 + (page / 8) as usize] & (1 << (page % 8)) != 0
}

/// **F4.** Three real pages and one that no bitmap page maps. The call must refuse, and the three
/// real pages must still be allocated: a caller that retries must not free them twice.
#[test]
fn a_batch_free_with_an_unfreeable_page_frees_none_of_it() {
    let (bp, _dir) = pool();
    let ids: Vec<u32> = (0..3).map(|_| bp.new_page().unwrap()).collect();
    for (n, &p) in ids.iter().enumerate() {
        let i = bp.fetch_page(p).unwrap();
        bp.frame_write(i).data[0] = 0x40 + n as u8;
        bp.unpin_page(p, true);
    }
    // Four bitmap pages' span in: a fresh file has only bitmap page 0, so nothing maps it. A
    // reserved region covering it would refuse it too; either refusal is deterministic.
    let unmapped = (PAGE_SIZE as u32 - 4) * 8 * 4;
    let mut batch = ids.clone();
    batch.push(unmapped);
    assert!(ids.iter().all(|&p| allocated(&bp, p)), "premise: the three pages read as allocated");

    let err = match bp.free_pages(&batch) {
        Ok(()) => panic!("premise failed: freeing page {unmapped}, which no bitmap page maps, succeeded"),
        Err(e) => e,
    };
    let freed: Vec<u32> = ids.iter().copied().filter(|&p| !allocated(&bp, p)).collect();
    assert!(
        freed.is_empty(),
        "free_pages refused with `{err}` having already freed {freed:?}; a retry would free them again"
    );
    // Review 2 N4: after the refusal every page is still pinnable and still holds its bytes, so the
    // refusal took back whatever the call had done to their frames.
    for (n, &p) in ids.iter().enumerate() {
        let i = bp.fetch_page(p).unwrap_or_else(|e| panic!("page {p} cannot be pinned after the refusal: {e}"));
        let byte = bp.frames[i].read().unwrap().data[0];
        bp.unpin_page(p, false);
        assert_eq!(byte, 0x40 + n as u8, "page {p} lost its bytes to a refused free");
    }

    bp.free_pages(&ids).unwrap();
    assert!(ids.iter().all(|&p| !allocated(&bp, p)), "the batch without the bad page did not free every page");
}

/// **F2.** One thread pins and unpins the LAST page of a set, as a hot backup would, while
/// `free_pages` frees the set. Each trial must end with every page freed or with none freed.
/// Refusing is allowed; a set half freed is not.
///
/// Probabilistic as a red test: at `f651096` a trial goes wrong only when the check runs while
/// the pinner is between pins and the pinner holds the last page when its free comes round. That
/// is predicted at 25% or more per trial, so 200 trials almost surely hit it. It cannot pass
/// falsely against the fix: the fix holds the exclusion across the check and the frees, so no
/// trial can end half freed, whatever the schedule.
#[test]
fn a_concurrent_pinner_never_sees_a_half_freed_set() {
    const TRIALS: usize = 200;
    const PAGES: usize = 32;
    let (bp, _dir) = pool();
    let (mut whole, mut refused) = (0usize, 0usize);
    for trial in 0..TRIALS {
        let ids: Vec<u32> = (0..PAGES).map(|_| bp.new_page().unwrap()).collect();
        let target = *ids.last().unwrap();
        let stop = AtomicBool::new(false);
        let result = std::thread::scope(|s| {
            s.spawn(|| {
                while !stop.load(Ordering::Acquire) {
                    if bp.fetch_page(target).is_ok() {
                        std::hint::spin_loop();
                        bp.unpin_page(target, false);
                    }
                }
            });
            let r = bp.free_pages(&ids);
            stop.store(true, Ordering::Release);
            r
        });
        let freed: Vec<u32> = ids.iter().copied().filter(|&p| !allocated(&bp, p)).collect();
        match result {
            Ok(()) => {
                assert_eq!(freed.len(), PAGES, "trial {trial}: free_pages succeeded and left pages allocated");
                whole += 1;
            }
            Err(e) => {
                assert!(
                    freed.is_empty(),
                    "trial {trial}: free_pages refused with `{e}` having freed {freed:?} of {ids:?}: \
                     a pin landed between its check and its frees"
                );
                refused += 1;
            }
        }
    }
    eprintln!("{TRIALS} trials: {whole} freed whole, {refused} refused whole");
    // Review 2 N4: a run in which no trial freed anything tested nothing. The pinner holds the page
    // about half the time, so across 200 trials a zero here is not a schedule.
    assert!(whole > 0, "no trial freed its set: {refused} of {TRIALS} refused, so the run tested nothing");
}
