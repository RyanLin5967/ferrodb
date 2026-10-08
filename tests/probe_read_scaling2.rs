//! PROBE v2. Two axes the committed read benchmark never varied:
//!   (a) THREADS  -- bench/branch_scaling.txt is single-process, single-threaded.
//!   (b) WORKING SET vs the buffer pool -- MAX_BUFFER_POOL_PAGES = 1024 is a compile-time const
//!       (src/buffer/buffer_pool.rs:45), and the committed bench's trunk is 44 pages.
//!
//! Every fetch takes one global Mutex (buffer_pool.rs:72, "This serialises fetches").
//! v1 measured x5.57 at 8 threads with a 298-page (fully resident) tree: contention is real but
//! not a wall. This run asks where the ceiling actually is, and what an over-subscribed pool does.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::{CowTree, PageStore};
use ferrodb::storage::disk_manager::DiskManager;

const READS_PER_THREAD: u32 = 30_000;

fn key(i: u32) -> Vec<u8> { format!("k{:08}", i).into_bytes() }
fn val(i: u32) -> Vec<u8> { format!("v{:08}", i).into_bytes() }

struct Env { catalog: Arc<LogBranchCatalog>, store: Arc<ArenaPageStore>, path: std::path::PathBuf }
impl Drop for Env { fn drop(&mut self) { let _ = std::fs::remove_file(&self.path); } }

fn env(tag: &str) -> Env {
    let path = std::env::temp_dir().join(format!("ferro-rs2-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(Arc::clone(&pool),
        Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base).unwrap());
    Env { catalog, store, path }
}

fn throughput(tree: &Arc<CowTree>, root: u32, rows: u32, threads: usize) -> f64 {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let hits = Arc::new(AtomicU64::new(0));
    let mut hs = Vec::new();
    for t in 0..threads {
        let tree = Arc::clone(tree);
        let barrier = Arc::clone(&barrier);
        let hits = Arc::clone(&hits);
        hs.push(std::thread::spawn(move || {
            let off = (t as u32).wrapping_mul(104729);
            for i in 0..200u32 { let _ = tree.get(root, &key((i.wrapping_mul(7919).wrapping_add(off)) % rows)); }
            barrier.wait();
            let mut found = 0u64;
            for i in 0..READS_PER_THREAD {
                let k = key((i.wrapping_mul(7919).wrapping_add(off)) % rows);
                if tree.get(root, &k).unwrap().is_some() { found += 1; }
            }
            hits.fetch_add(found, Ordering::Relaxed);
        }));
    }
    barrier.wait();
    let t0 = Instant::now();
    for h in hs { h.join().unwrap(); }
    let secs = t0.elapsed().as_secs_f64();
    let total = hits.load(Ordering::Relaxed);
    assert_eq!(total, threads as u64 * READS_PER_THREAD as u64, "a read missed; measuring nothing");
    total as f64 / secs
}

fn build(tag: &str, rows: u32) -> (Env, Arc<CowTree>, u32, usize) {
    let e = env(tag);
    let tree = Arc::new(CowTree::new(Arc::clone(&e.store) as Arc<dyn PageStore>));
    let ep = e.catalog.next_epoch();
    let mut root = tree.create(BranchId::TRUNK, ep).unwrap();
    for i in 0..rows { root = tree.insert(root, BranchId::TRUNK, ep, &key(i), &val(i)).unwrap(); }
    let pages = tree.walk_pages(root).unwrap().len();
    (e, tree, root, pages)
}

#[test]
fn read_scaling_across_threads_and_across_the_buffer_pool_boundary() {
    eprintln!("MAX_BUFFER_POOL_PAGES = 1024 frames (compile-time const, buffer_pool.rs:45)\n");
    // 20k rows ~ 300 pages (fits); 400k rows ~ 6000 pages (6x over the pool).
    for (tag, rows) in [("resident", 20_000u32), ("oversubscribed", 400_000u32)] {
        let (_e, tree, root, pages) = build(tag, rows);
        eprintln!("--- {tag}: {rows} rows, {pages} pages, pool residency {:.1}%",
                  100.0 * (pages.min(1024) as f64) / (pages as f64));
        let base = throughput(&tree, root, rows, 1);
        eprintln!("   threads |      reads/s | speedup");
        eprintln!("         1 | {:>12.0} |   x1.00", base);
        for t in [2usize, 4, 8, 12, 16] {
            let r = throughput(&tree, root, rows, t);
            eprintln!("    {:>6} | {:>12.0} |  x{:.2}", t, r, r / base);
        }
        eprintln!();
    }
}
