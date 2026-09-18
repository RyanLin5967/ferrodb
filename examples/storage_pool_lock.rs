//! S18 probe 3: isolate the buffer pool's global lock.
//!
//! ARM C is the narrowest possible workload: T threads each call `fetch_page`/`unpin_page` on ONE
//! page id that is guaranteed resident. No tree code, no descent, no disk, no allocation. If this
//! caps, the cap is `BufferPoolManager::fetch_page`'s single `arc_cache` Mutex and nothing else.
//!
//! ARM D is the control: the same loop with the pool replaced by an atomic increment, so a cap
//! that is really the machine's shows up here too.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;

fn main() {
    let ops: u64 = std::env::args().nth(1).map(|s| s.parse().unwrap()).unwrap_or(2_000_000);
    let path = std::env::temp_dir().join(format!("s18-lock-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let file =
        std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    // Make 8 real pages and keep them resident.
    let pages: Vec<u32> = (0..8).map(|_| pool.new_page().unwrap()).collect();
    for &p in &pages {
        let i = pool.fetch_page(p).unwrap();
        std::hint::black_box(i);
        pool.unpin_page(p, false);
    }

    println!("ARM C  fetch_page/unpin_page on 8 resident pages, {} ops total", ops);
    println!("  threads      ops/sec   speedup");
    let mut b1 = 0.0f64;
    for &t in &[1usize, 2, 4, 8, 16, 32] {
        let per = ops / t as u64;
        let t0 = std::time::Instant::now();
        let hs: Vec<_> = (0..t)
            .map(|ti| {
                let pool = Arc::clone(&pool);
                let pages = pages.clone();
                std::thread::spawn(move || {
                    for k in 0..per {
                        let p = pages[((k as usize) + ti) % pages.len()];
                        let i = pool.fetch_page(p).unwrap();
                        std::hint::black_box(i);
                        pool.unpin_page(p, false);
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        let r = (per * t as u64) as f64 / t0.elapsed().as_secs_f64();
        if t == 1 {
            b1 = r;
        }
        println!("  {:>7}   {:>10.0}   {:>7.2}x", t, r, r / b1);
    }

    println!("\nARM D (control)  same loop, atomic increment instead of the pool");
    println!("  threads      ops/sec   speedup");
    let ctr = Arc::new(AtomicU64::new(0));
    let mut d1 = 0.0f64;
    for &t in &[1usize, 2, 4, 8, 16, 32] {
        let per = ops / t as u64;
        let t0 = std::time::Instant::now();
        let hs: Vec<_> = (0..t)
            .map(|_| {
                let ctr = Arc::clone(&ctr);
                std::thread::spawn(move || {
                    let mut local = 0u64;
                    for k in 0..per {
                        local = local.wrapping_add(k ^ 0x9E37);
                        std::hint::black_box(local);
                    }
                    ctr.fetch_add(local, Ordering::Relaxed);
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        let r = (per * t as u64) as f64 / t0.elapsed().as_secs_f64();
        if t == 1 {
            d1 = r;
        }
        println!("  {:>7}   {:>10.0}   {:>7.2}x", t, r, r / d1);
    }
    let _ = std::fs::remove_file(&path);
}
