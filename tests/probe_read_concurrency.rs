//! PROBE: does the "reads are flat" claim survive CONCURRENT readers on distinct branches?
//!
//! bench/branch_scaling.txt is single-threaded ("single process, warm cache"). The BranchBench
//! workloads are many agents reading their own branches at once.
//!
//! `BufferPoolManager::fetch_page` holds `self.arc_cache.lock()` across the WHOLE fetch, hit or
//! miss (src/buffer/buffer_pool.rs:72, and the comment at :68 says "This serialises fetches").
//! A root-to-leaf descent of height h therefore takes one global mutex h times.
//!
//! Prediction: aggregate read throughput is ~flat in thread count; per-read latency rises ~linearly.
//! CONTROL: a pure-CPU loop on the same threads must scale, or the machine, not the pool, is the
//! bottleneck.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::{CowTree, PageStore};
use ferrodb::storage::disk_manager::DiskManager;

const ROWS: u32 = 20_000;
const READS_PER_THREAD: u32 = 20_000;

fn key(i: u32) -> Vec<u8> { format!("k{:08}", i).into_bytes() }
fn val(i: u32) -> Vec<u8> { format!("v{:08}", i).into_bytes() }

struct Env {
    catalog: Arc<LogBranchCatalog>,
    store: Arc<ArenaPageStore>,
    path: std::path::PathBuf,
}
impl Drop for Env {
    fn drop(&mut self) { let _ = std::fs::remove_file(&self.path); }
}

fn env(tag: &str) -> Env {
    let path = std::env::temp_dir().join(format!("ferro-conc-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base).unwrap(),
    );
    Env { catalog, store, path }
}

/// Aggregate reads/sec with `threads` readers, each on its OWN branch.
fn read_throughput(e: &Env, roots: &[u32], threads: usize) -> f64 {
    let tree = Arc::new(CowTree::new(Arc::clone(&e.store) as Arc<dyn PageStore>));
    let barrier = Arc::new(Barrier::new(threads + 1));
    let hits = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for t in 0..threads {
        let tree = Arc::clone(&tree);
        let barrier = Arc::clone(&barrier);
        let hits = Arc::clone(&hits);
        let root = roots[t % roots.len()];
        handles.push(std::thread::spawn(move || {
            // Warm this thread's path, then wait for everyone.
            for i in 0..200u32 { let _ = tree.get(root, &key((i * 7919) % ROWS)).unwrap(); }
            barrier.wait();
            let mut found = 0u64;
            for i in 0..READS_PER_THREAD {
                let k = key((i.wrapping_mul(7919)) % ROWS);
                if tree.get(root, &k).unwrap().is_some() { found += 1; }
            }
            hits.fetch_add(found, Ordering::Relaxed);
        }));
    }
    barrier.wait();
    let t0 = Instant::now();
    for h in handles { h.join().unwrap(); }
    let secs = t0.elapsed().as_secs_f64();
    let total = hits.load(Ordering::Relaxed);
    assert_eq!(total, threads as u64 * READS_PER_THREAD as u64, "a read missed; measuring nothing");
    total as f64 / secs
}

/// CONTROL: pure CPU, no shared state. Must scale, or the box has no spare cores.
fn cpu_throughput(threads: usize) -> f64 {
    const ITERS: u64 = 40_000_000;
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::new();
    for _ in 0..threads {
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut x = 1u64;
            for i in 0..ITERS { x = x.wrapping_mul(6364136223846793005).wrapping_add(i); }
            x
        }));
    }
    barrier.wait();
    let t0 = Instant::now();
    let mut sink = 0u64;
    for h in handles { sink ^= h.join().unwrap(); }
    assert_ne!(sink, 12345);
    (threads as u64 * ITERS) as f64 / t0.elapsed().as_secs_f64()
}

#[test]
fn concurrent_reads_on_distinct_branches_scale_but_not_with_the_cores() {
    let e = env("reads");
    let tree = CowTree::new(Arc::clone(&e.store) as Arc<dyn PageStore>);
    let ep = e.catalog.next_epoch();
    let mut root = tree.create(BranchId::TRUNK, ep).unwrap();
    for i in 0..ROWS { root = tree.insert(root, BranchId::TRUNK, ep, &key(i), &val(i)).unwrap(); }
    e.catalog.set_root(BranchId::TRUNK, root).unwrap();
    let height = {
        // report tree height so the per-read page count is on the record
        let mut h = 1; let mut pid = root;
        loop {
            let hd = e.store.read_page(pid).unwrap();
            if hd.header().unwrap().page_type != ferrodb::cow::PageType::BTreeInternal { break; }
            h += 1;
            pid = tree.walk_pages(pid).unwrap()[1];
        }
        h
    };
    let pages = tree.walk_pages(root).unwrap().len();

    // Eight branches, each diverged by one key, so each reads its own root.
    let mut roots = Vec::new();
    for i in 0..8u32 {
        let b = e.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
        let ep2 = e.catalog.next_epoch();
        let r = tree.insert(b.root_page_id, b.branch_id, ep2, &key(i), b"mine").unwrap();
        e.catalog.set_root(b.branch_id, r).unwrap();
        roots.push(r);
    }

    eprintln!("tree: {ROWS} rows, {pages} pages, height {height}; 8 diverged branches");
    eprintln!("buffer pool frames = 1024 (compile-time const, buffer_pool.rs:45)");

    let c1 = cpu_throughput(1);
    let c8 = cpu_throughput(8);
    eprintln!("CONTROL cpu    : 1 thread {:>12.0} ops/s | 8 threads {:>12.0} ops/s | x{:.2}",
              c1, c8, c8 / c1);

    let r1 = read_throughput(&e, &roots, 1);
    let r4 = read_throughput(&e, &roots, 4);
    let r8 = read_throughput(&e, &roots, 8);
    eprintln!("READS          : 1 thread {:>12.0} rd/s | 4 threads {:>12.0} rd/s | 8 threads {:>12.0} rd/s",
              r1, r4, r8);
    eprintln!("read scaling   : x{:.2} at 4 threads, x{:.2} at 8 threads", r4 / r1, r8 / r1);
    eprintln!("cpu  scaling   : x{:.2} at 8 threads", c8 / c1);

    assert!(c8 / c1 > 3.0, "CONTROL FAILED: cpu did not scale (x{:.2}); the box, not the pool, is the limit", c8 / c1);

    // MEASURED 2026-09-17, 18 logical cores, release build: reads x5.57 at 8 threads against a
    // cpu control of x8.59. The original prediction here -- that the global fetch mutex would
    // hold reads under x2 -- was WRONG for a working set that FITS the pool, and is recorded as
    // wrong rather than quietly restated. The lock costs ~30% of linear here, not a wall.
    // probe_read_scaling2 sweeps further and finds the actual ceiling (x6.0, and x2.3 once the
    // working set exceeds the 1024-frame pool).
    assert!(r8 / r1 > 2.0, "reads did not scale at all (x{:.2}); v1 measured x5.57", r8 / r1);
    assert!(
        r8 / r1 < c8 / c1,
        "reads scaled x{:.2} vs cpu x{:.2} -- reads would have to be scaling as well as pure cpu, \
         which would mean the shared buffer pool costs nothing",
        r8 / r1, c8 / c1
    );
}
