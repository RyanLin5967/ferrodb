//! D20 — `DiskManager` under real threads.
//!
//! The buffer pool calls this concurrently, and it had never been run that way. `allocate()` holds
//! `bitmap_lock` for its whole body, so it *looks* serialized — which is precisely what
//! `fetch_page` looked like before D18 lost writes and D19 panicked. The question is settled by
//! running it, not by reading it.
//!
//! Two page ids handed to two threads would be aliasing: two writers on one page, each silently
//! overwriting the other. That is worse than either of the bugs above, so the uniqueness check is
//! the one that matters and everything else here is supporting it.

use std::collections::BTreeSet;
use std::sync::Arc;

use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

fn manager(tag: &str) -> (tempfile::TempDir, Arc<DiskManager>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    (dir, Arc::new(DiskManager::new(file).unwrap()))
}

/// **The one that matters.** No page id may be handed to two callers.
#[test]
fn concurrent_allocation_never_hands_out_the_same_page_twice() {
    let (_d, dm) = manager("alloc");
    const THREADS: usize = 8;
    const EACH: usize = 250;

    let ids: Vec<u32> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let dm = Arc::clone(&dm);
                s.spawn(move || {
                    (0..EACH).filter_map(|_| dm.allocate().ok()).collect::<Vec<u32>>()
                })
            })
            .collect();
        handles.into_iter().flat_map(|h| h.join().expect("no thread panicked")).collect()
    });

    // A run where allocation mostly failed would satisfy uniqueness by doing nothing.
    assert!(
        ids.len() > THREADS * EACH / 2,
        "only {} of {} allocations succeeded; this proves little",
        ids.len(),
        THREADS * EACH
    );

    let unique: BTreeSet<u32> = ids.iter().copied().collect();
    assert_eq!(
        unique.len(),
        ids.len(),
        "{} page id(s) were handed out more than once — two writers would share a page and each \
         silently overwrite the other",
        ids.len() - unique.len()
    );
}

/// An allocated page must be writable and readable as itself. Uniqueness alone would be satisfied
/// by an allocator handing out ids that do not correspond to usable storage.
#[test]
fn concurrently_allocated_pages_hold_their_own_bytes() {
    let (_d, dm) = manager("bytes");
    const THREADS: usize = 8;
    const EACH: usize = 120;

    let bad = std::thread::scope(|s| {
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let dm = Arc::clone(&dm);
                s.spawn(move || {
                    let mut wrong = 0usize;
                    for _ in 0..EACH {
                        let Ok(id) = dm.allocate() else { continue };
                        let mut page = [0u8; PAGE_SIZE];
                        page[0..4].copy_from_slice(&id.to_be_bytes());
                        if dm.write(id, &page).is_err() {
                            continue;
                        }
                        let back = dm.read(id).expect("read back");
                        if u32::from_be_bytes([back[0], back[1], back[2], back[3]]) != id {
                            wrong += 1;
                        }
                    }
                    wrong
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("no panic")).sum::<usize>()
    });

    assert_eq!(bad, 0, "{bad} pages did not read back their own id after a concurrent allocation");
}

/// The high-water mark must never fall behind what has actually been handed out, or a later grow
/// hands out a page the bitmap already owns — the S3 aliasing bug, reached by a different route.
#[test]
fn the_high_water_mark_never_lags_what_was_allocated() {
    let (_d, dm) = manager("hwm");
    const THREADS: usize = 6;

    let max_seen = std::thread::scope(|s| {
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let dm = Arc::clone(&dm);
                s.spawn(move || (0..150).filter_map(|_| dm.allocate().ok()).max().unwrap_or(0))
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("no panic")).max().unwrap_or(0)
    });

    let hw = dm.high_water().expect("high water");
    assert!(
        hw > max_seen,
        "high water {hw} is not above the highest allocated page {max_seen}; a grow from here \
         would hand out a page the bitmap already owns"
    );
}

/// Allocation and reads of already-allocated pages run together, which is the shape the buffer
/// pool actually produces: one thread growing the file while others read.
#[test]
fn allocation_and_reads_can_run_at_the_same_time() {
    let (_d, dm) = manager("mixed");

    // Seed some pages to read.
    let seeded: Vec<u32> = (0..200).filter_map(|_| dm.allocate().ok()).collect();
    for &id in &seeded {
        let mut page = [0u8; PAGE_SIZE];
        page[0..4].copy_from_slice(&id.to_be_bytes());
        dm.write(id, &page).expect("seed write");
    }

    let bad = std::thread::scope(|s| {
        let readers: Vec<_> = (0..4)
            .map(|t| {
                let dm = Arc::clone(&dm);
                let seeded = seeded.clone();
                s.spawn(move || {
                    let mut wrong = 0usize;
                    for k in 0..300 {
                        let id = seeded[(k * 31 + t * 7) % seeded.len()];
                        let back = dm.read(id).expect("read");
                        if u32::from_be_bytes([back[0], back[1], back[2], back[3]]) != id {
                            wrong += 1;
                        }
                    }
                    wrong
                })
            })
            .collect();
        for _ in 0..4 {
            let dm = Arc::clone(&dm);
            s.spawn(move || {
                for _ in 0..150 {
                    let _ = dm.allocate();
                }
            });
        }
        readers.into_iter().map(|h| h.join().expect("no panic")).sum::<usize>()
    });

    assert_eq!(bad, 0, "{bad} reads returned the wrong page while allocation was in flight");
}
