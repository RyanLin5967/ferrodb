//! D19 — the buffer pool's eviction path under concurrency.
//!
//! D18 fixed a lost-write race in `fetch_page`'s *no-evict* miss path. The **eviction** path has
//! the same shape and was never reached by that test, because eight threads never filled 1024
//! frames: it reads the page table, drops it, flushes, overwrites the frame, and only then
//! republishes the mapping. Check-then-act across independent locks, again.
//!
//! It is also the worse of the two. `frame.data` is replaced *before* the page table is updated,
//! so a concurrent lookup of the evicted page resolves to a frame that already holds the **new**
//! page's bytes. That serves WRONG DATA rather than merely losing a write, and wrong data is the
//! failure a storage engine has no way to apologise for.
//!
//! Each page is stamped with its own id, so "did this fetch return the right page" is decidable
//! from the bytes alone rather than from anything the pool reports about itself.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

/// More pages than the pool has frames (1024), so eviction is forced rather than hoped for.
const PAGES: u32 = 1600;

fn stamp(data: &mut [u8; PAGE_SIZE], page_id: u32) {
    data[0..4].copy_from_slice(&page_id.to_be_bytes());
}

fn read_stamp(data: &[u8; PAGE_SIZE]) -> u32 {
    u32::from_be_bytes([data[0], data[1], data[2], data[3]])
}

fn pool(tag: &str) -> (tempfile::TempDir, Arc<BufferPoolManager>, Vec<u32>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));

    // Stamp every page with its own id, then flush so the contents are on disk and the frames can
    // be evicted and reloaded.
    let mut ids = Vec::with_capacity(PAGES as usize);
    for _ in 0..PAGES {
        let id = bp.new_page().expect("allocate");
        let idx = bp.fetch_page(id).expect("fetch for stamping");
        {
            let mut frame = bp.frames[idx].write().unwrap();
            stamp(&mut frame.data, id);
        }
        bp.unpin_page(id, true);
        ids.push(id);
    }
    bp.flush_all().expect("flush");
    (dir, bp, ids)
}

/// Sequential control. If this ever fails, the concurrent result below says nothing about
/// concurrency — it would just mean eviction is broken outright.
#[test]
fn every_page_reads_back_its_own_stamp_single_threaded() {
    let (_d, bp, ids) = pool("seq");
    for &id in &ids {
        let idx = bp.fetch_page(id).expect("fetch");
        let got = read_stamp(&bp.frames[idx].read().unwrap().data);
        bp.unpin_page(id, false);
        assert_eq!(got, id, "page {id} came back holding page {got}");
    }
}

/// **The one D19 exists for.** Many threads, far more pages than frames, every fetch checked
/// against the page it asked for.
#[test]
fn concurrent_fetches_never_return_another_pages_bytes() {
    let (_d, bp, ids) = pool("conc");
    const THREADS: usize = 8;
    const FETCHES: usize = 400;

    let wrong = Arc::new(AtomicUsize::new(0));
    let checked = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            let wrong = Arc::clone(&wrong);
            let checked = Arc::clone(&checked);
            s.spawn(move || {
                for k in 0..FETCHES {
                    // Deterministic but thread-dependent, so the threads collide on some pages
                    // and diverge on others. No RNG, so a failure is reproducible.
                    let id = ids[((k * 7919) + t * 131) % ids.len()];
                    let Ok(idx) = bp.fetch_page(id) else { continue };
                    let got = read_stamp(&bp.frames[idx].read().unwrap().data);
                    bp.unpin_page(id, false);
                    checked.fetch_add(1, Ordering::Relaxed);
                    if got != id {
                        wrong.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    let n = checked.load(Ordering::Relaxed);
    // A run where every fetch was refused would report zero mismatches while testing nothing.
    assert!(
        n > THREADS * FETCHES / 2,
        "only {n} fetches completed of {}; the pool refused most of them and this proves little",
        THREADS * FETCHES
    );
    assert_eq!(
        wrong.load(Ordering::Relaxed),
        0,
        "{} of {n} concurrent fetches returned another page's bytes",
        wrong.load(Ordering::Relaxed)
    );
}

/// The page table must never point two live pages at one frame, which is the state that lets a
/// fetch return the wrong bytes in the first place.
#[test]
fn the_page_table_never_maps_two_pages_to_one_frame() {
    let (_d, bp, ids) = pool("table");
    const THREADS: usize = 8;

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            s.spawn(move || {
                for k in 0..300 {
                    let id = ids[((k * 4099) + t * 61) % ids.len()];
                    if bp.fetch_page(id).is_ok() {
                        bp.unpin_page(id, false);
                    }
                }
            });
        }
    });

    let pt = bp.page_table.read().unwrap();
    let mut seen: std::collections::HashMap<usize, u32> = std::collections::HashMap::new();
    for (&page, &frame) in pt.iter() {
        if let Some(&other) = seen.get(&frame) {
            panic!("frame {frame} is mapped from both page {other} and page {page}");
        }
        seen.insert(frame, page);
    }
    assert!(!seen.is_empty(), "the page table is empty, so nothing was checked");
}

/// A frame's own record of which page it holds must agree with the table that points at it.
/// Disagreement here is precisely the window where `frame.data` has been replaced but the mapping
/// has not caught up.
#[test]
fn every_frame_agrees_with_the_page_table_about_which_page_it_holds() {
    let (_d, bp, ids) = pool("agree");
    const THREADS: usize = 8;

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            s.spawn(move || {
                for k in 0..300 {
                    let id = ids[((k * 3571) + t * 97) % ids.len()];
                    if bp.fetch_page(id).is_ok() {
                        bp.unpin_page(id, false);
                    }
                }
            });
        }
    });

    let pt = bp.page_table.read().unwrap();
    for (&page, &frame) in pt.iter() {
        let f = bp.frames[frame].read().unwrap();
        assert_eq!(
            f.page_id,
            Some(page),
            "the table says frame {frame} holds page {page}, the frame says {:?}",
            f.page_id
        );
        assert_eq!(
            read_stamp(&f.data),
            page,
            "frame {frame} is labelled page {page} but contains page {}",
            read_stamp(&f.data)
        );
    }
}
