//! D21 — `flush_page` racing page reassignment.
//!
//! D18 and D19 fixed `fetch_page`. `flush_page` kept the same shape: take
//! `page_table.read()`, pull out the frame index, **drop the lock**, then read the frame. Two
//! hazards live in that window — `pt[&page_id]` panics if the page has been evicted (the D19
//! crash class), and if the frame has been reassigned then the bytes written to disk under this
//! page's id belong to a different page.
//!
//! The second is the worse one by a distance. A lost write is recoverable in principle; a page
//! written to disk containing another page's contents is durable corruption that every later read
//! will faithfully reproduce.
//!
//! Every page is stamped with its own id, so what lands on disk can be judged without asking the
//! pool whether it thinks it did the right thing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

/// Comfortably more than the 1024 frames, so eviction and reassignment are constant.
const PAGES: u32 = 1600;

fn stamp_of(data: &[u8; PAGE_SIZE]) -> u32 {
    u32::from_be_bytes([data[0], data[1], data[2], data[3]])
}

fn setup(tag: &str) -> (tempfile::TempDir, Arc<DiskManager>, Arc<BufferPoolManager>, Vec<u32>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(Arc::clone(&dm)));

    let mut ids = Vec::with_capacity(PAGES as usize);
    for _ in 0..PAGES {
        let id = bp.new_page().expect("allocate");
        let idx = bp.fetch_page(id).expect("fetch");
        {
            let mut f = bp.frames[idx].write().unwrap();
            f.data[0..4].copy_from_slice(&id.to_be_bytes());
        }
        bp.unpin_page(id, true);
        ids.push(id);
    }
    bp.flush_all().expect("initial flush");
    (dir, dm, bp, ids)
}

/// Flushes running against fetches must never write one page's bytes under another page's id.
#[test]
fn concurrent_flush_and_fetch_never_write_the_wrong_page_to_disk() {
    let (_d, dm, bp, ids) = setup("flush");
    const FETCHERS: usize = 6;
    const FLUSHERS: usize = 4;

    let flushed = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|s| {
        for t in 0..FETCHERS {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            s.spawn(move || {
                for k in 0..500 {
                    let id = ids[((k * 7919) + t * 131) % ids.len()];
                    if let Ok(idx) = bp.fetch_page(id) {
                        // Dirty it, so flushing has something real to write.
                        {
                            let mut f = bp.frames[idx].write().unwrap();
                            f.data[0..4].copy_from_slice(&id.to_be_bytes());
                        }
                        bp.unpin_page(id, true);
                    }
                }
            });
        }
        for t in 0..FLUSHERS {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            let flushed = Arc::clone(&flushed);
            s.spawn(move || {
                for k in 0..500 {
                    let id = ids[((k * 4099) + t * 61) % ids.len()];
                    if bp.flush_page(id).is_ok() {
                        flushed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    assert!(
        flushed.load(Ordering::Relaxed) > 0,
        "no flush completed, so nothing about flushing was tested"
    );

    // The judgement is made against the bytes on disk, not against anything the pool reports.
    bp.flush_all().expect("final flush");
    let mut corrupt = Vec::new();
    for &id in &ids {
        let on_disk = dm.read(id).expect("read back");
        let got = stamp_of(&on_disk);
        if got != id {
            corrupt.push((id, got));
        }
    }
    assert!(
        corrupt.is_empty(),
        "{} page(s) on disk hold another page's bytes, e.g. {:?} — durable corruption",
        corrupt.len(),
        &corrupt[..corrupt.len().min(5)]
    );
}

/// Flushing a page that another thread is evicting must not panic. `pt[&page_id]` on a departed
/// key is the same crash D19 fixed in `fetch_page`.
#[test]
fn flushing_a_page_that_is_being_evicted_does_not_panic() {
    let (_d, _dm, bp, ids) = setup("panic");

    std::thread::scope(|s| {
        for t in 0..4 {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            s.spawn(move || {
                for k in 0..600 {
                    let id = ids[((k * 3571) + t * 97) % ids.len()];
                    if let Ok(_idx) = bp.fetch_page(id) {
                        bp.unpin_page(id, true);
                    }
                }
            });
        }
        for t in 0..4 {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            s.spawn(move || {
                for k in 0..600 {
                    // Deliberately aimed at pages the fetchers are pushing out of the pool.
                    let id = ids[((k * 6151) + t * 17) % ids.len()];
                    let _ = bp.flush_page(id);
                }
            });
        }
    });
}

/// `flush_all` holds the page-table read lock for its whole loop, so reassignment — which needs
/// the write lock — cannot interleave. Asserted so that "simplifying" it into per-page flushes
/// does not silently reintroduce the window.
#[test]
fn flush_all_is_atomic_against_concurrent_fetches() {
    let (_d, dm, bp, ids) = setup("all");

    std::thread::scope(|s| {
        for t in 0..6 {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            s.spawn(move || {
                for k in 0..400 {
                    let id = ids[((k * 7919) + t * 131) % ids.len()];
                    if let Ok(idx) = bp.fetch_page(id) {
                        {
                            let mut f = bp.frames[idx].write().unwrap();
                            f.data[0..4].copy_from_slice(&id.to_be_bytes());
                        }
                        bp.unpin_page(id, true);
                    }
                }
            });
        }
        for _ in 0..3 {
            let bp = Arc::clone(&bp);
            s.spawn(move || {
                for _ in 0..200 {
                    let _ = bp.flush_all();
                }
            });
        }
    });

    bp.flush_all().expect("final flush");
    for &id in &ids {
        let on_disk = dm.read(id).expect("read back");
        assert_eq!(stamp_of(&on_disk), id, "page {id} on disk holds page {}", stamp_of(&on_disk));
    }
}
