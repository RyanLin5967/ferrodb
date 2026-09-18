//! S18 storage probe 2: what does the 1024-frame page-granular buffer pool cost per branch?
//!
//! Arm A — READ throughput as the number of CONCURRENTLY ACTIVE branches rises, single-threaded.
//!   Every branch that has written owns a private COW copy of its root-to-leaf path. Those copies
//!   have distinct page ids, so the pool caches them separately: pool capacity is spent per
//!   branch, not per datum. Predicted cliff at roughly MAX_BUFFER_POOL_PAGES / (private pages
//!   per branch + shared working set).
//!
//! Arm B — READ throughput vs thread count at a FIXED small active set that fits the pool.
//!   `BufferPoolManager::fetch_page` holds one global `arc_cache` Mutex across the whole miss
//!   path, including `disk_manager.read`. Prediction: throughput does not improve with threads.
//!
//! Usage: cargo run --release --example storage_residency_cost -- <trunk_rows> <branches> <reads>

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline, PageId};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::{CowTree, PageStore};
use ferrodb::storage::disk_manager::DiskManager;

fn key(i: u32) -> Vec<u8> {
    format!("k{:08}", i).into_bytes()
}
fn val(i: u32) -> Vec<u8> {
    format!("v{:08}", i).into_bytes()
}

/// xorshift, so the read order is reproducible and costs nothing.
fn rnd(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let trunk_rows: u32 = a.get(1).map(|s| s.parse().unwrap()).unwrap_or(200_000);
    let branches: u32 = a.get(2).map(|s| s.parse().unwrap()).unwrap_or(2048);
    let reads: u64 = a.get(3).map(|s| s.parse().unwrap()).unwrap_or(200_000);

    let path = std::env::temp_dir().join(format!("s18-res-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let file = OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base)
            .unwrap(),
    );
    let tree = Arc::new(CowTree::new(store.clone() as Arc<dyn PageStore>));

    let e0 = catalog.next_epoch();
    let mut root = tree.create(BranchId::TRUNK, e0).unwrap();
    for i in 0..trunk_rows {
        let e = catalog.next_epoch();
        root = tree.insert(root, BranchId::TRUNK, e, &key(i), &val(i)).unwrap();
    }
    catalog.set_root(BranchId::TRUNK, root).unwrap();
    let trunk_pages = tree.walk_pages(root).unwrap().len();
    println!(
        "pool frames = 1024 (4 MB).  trunk rows={} pages={} ({:.1} MB) => trunk alone is {:.2}x \
         the pool",
        trunk_rows,
        trunk_pages,
        trunk_pages as f64 * 4096.0 / 1e6,
        trunk_pages as f64 / 1024.0
    );

    // Fork `branches` branches, each writing exactly one row, and record each one's root.
    let mut roots: Vec<PageId> = Vec::with_capacity(branches as usize);
    let live0 = store.live_page_count().unwrap();
    for j in 0..branches {
        let rec = catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(3_600_000)).unwrap();
        let e = catalog.next_epoch();
        let r = tree
            .insert(rec.root_page_id, rec.branch_id, e, &key(j % trunk_rows), &val(j))
            .unwrap();
        catalog.set_root(rec.branch_id, r).unwrap();
        roots.push(r);
    }
    let live1 = store.live_page_count().unwrap();
    println!(
        "forked {} branches, one row each: {} private pages total = {:.2} pages/branch\n",
        branches,
        live1 - live0,
        (live1 - live0) as f64 / branches as f64
    );

    // ---- ARM A: reads/sec vs number of concurrently active branches ------------------------
    println!("ARM A  single thread, {} reads per point, keys drawn uniformly", reads);
    println!("  active_branches   reads/sec    us/read");
    for &active in &[1u32, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048] {
        if active > branches {
            break;
        }
        let mut s: u64 = 0x2545F4914F6CDD1D;
        // warm: touch each active branch once
        for b in 0..active {
            let _ = tree.get(roots[b as usize], &key(b % trunk_rows)).unwrap();
        }
        let t0 = std::time::Instant::now();
        let mut hits = 0u64;
        for _ in 0..reads {
            let b = (rnd(&mut s) % active as u64) as usize;
            let k = (rnd(&mut s) % trunk_rows as u64) as u32;
            if tree.get(roots[b], &key(k)).unwrap().is_some() {
                hits += 1;
            }
        }
        let el = t0.elapsed();
        std::hint::black_box(hits);
        println!(
            "  {:>15}   {:>9.0}   {:>8.3}",
            active,
            reads as f64 / el.as_secs_f64(),
            el.as_secs_f64() * 1e6 / reads as f64
        );
    }

    // ---- ARM B: reads/sec vs threads, active set small enough to be resident ---------------
    println!("\nARM B  {} reads total, split across threads, active_branches = 4", reads);
    println!("  threads   reads/sec   speedup");
    let mut base_rate = 0.0f64;
    for &t in &[1usize, 2, 4, 8] {
        let per = reads / t as u64;
        let counter = Arc::new(AtomicU64::new(0));
        let t0 = std::time::Instant::now();
        let mut hs = Vec::new();
        for ti in 0..t {
            let tree = Arc::clone(&tree);
            let roots = roots[..4].to_vec();
            let counter = Arc::clone(&counter);
            hs.push(std::thread::spawn(move || {
                let mut s: u64 = 0x9E3779B97F4A7C15 ^ (ti as u64).wrapping_mul(0x1234567);
                let mut h = 0u64;
                for _ in 0..per {
                    let b = (rnd(&mut s) % 4) as usize;
                    let k = (rnd(&mut s) % trunk_rows as u64) as u32;
                    if tree.get(roots[b], &key(k)).unwrap().is_some() {
                        h += 1;
                    }
                }
                counter.fetch_add(h, Ordering::Relaxed);
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        let el = t0.elapsed();
        let rate = (per * t as u64) as f64 / el.as_secs_f64();
        if t == 1 {
            base_rate = rate;
        }
        println!("  {:>7}   {:>9.0}   {:>7.2}x", t, rate, rate / base_rate);
    }

    let _ = std::fs::remove_file(&path);
}
