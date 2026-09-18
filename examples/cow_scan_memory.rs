//! S23: how much memory a `CowTree` range scan costs, as a function of the rows it returns.
//!
//! The claim under test is a SHAPE claim, not a constant-factor one: `CowTree::range_scan`
//! materialises every matching entry into a `Vec` before it returns, and `PagedRows::scan_table`
//! then builds a second `Vec` of decoded rows from it. Peak heap is therefore Θ(rows scanned)
//! rather than O(page), and no consumer — `LIMIT 1` included — can bound it.
//!
//!   cow_scan_memory [rows,comma,separated]
//!
//! # The instrument
//!
//! A tracking global allocator, not RSS. `ru_maxrss` is a high-water mark with no reset, so it
//! cannot separate the scan from the build that preceded it, and the allocator that serves this
//! process does not return freed pages to the kernel promptly either. The counter below is
//! exact, resettable, and attributes bytes to the phase that allocated them. Peak RSS is printed
//! alongside as a sanity check that the heap number is a real number and not an accounting
//! artefact — if the two disagree in shape, believe neither until that is explained.
//!
//! `heap_peak` is the high-water mark of *live* bytes between two `reset_peak()` calls, so it is
//! the number a memory ceiling is actually stated against. `allocs` counts allocation calls.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use ferrodb::agent_sql::paged_rows::{PagedRows, table_hi, table_lo};
use ferrodb::branch::BranchCatalog;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, PageId};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::storage::disk_manager::DiskManager;

// ---- the instrument --------------------------------------------------------------------------

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

struct Tracking;

impl Tracking {
    #[inline]
    fn grew(by: usize) {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        let now = LIVE.fetch_add(by, Ordering::Relaxed) + by;
        PEAK.fetch_max(now, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            Tracking::grew(l.size());
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, new) };
        if !q.is_null() {
            LIVE.fetch_sub(l.size(), Ordering::Relaxed);
            Tracking::grew(new);
        }
        q
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        if !p.is_null() {
            Tracking::grew(l.size());
        }
        p
    }
}

#[global_allocator]
static A: Tracking = Tracking;

/// Start a new measurement window: the peak becomes whatever is live right now.
fn reset_peak() -> (usize, usize) {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    let a = ALLOCS.load(Ordering::Relaxed);
    (live, a)
}

/// Bytes allocated above the baseline at the worst moment of the window, and allocation calls.
fn window(base_live: usize, base_allocs: usize) -> (usize, usize) {
    (
        PEAK.load(Ordering::Relaxed).saturating_sub(base_live),
        ALLOCS.load(Ordering::Relaxed) - base_allocs,
    )
}

fn peak_rss_bytes() -> u64 {
    // getrusage(RUSAGE_SELF).ru_maxrss; bytes on macOS, kilobytes on Linux.
    #[repr(C)]
    #[derive(Default)]
    struct RUsage {
        ru_utime: [i64; 2],
        ru_stime: [i64; 2],
        ru_maxrss: i64,
        rest: [i64; 14],
    }
    unsafe extern "C" {
        fn getrusage(who: i32, usage: *mut RUsage) -> i32;
    }
    let mut u = RUsage::default();
    if unsafe { getrusage(0, &mut u) } != 0 {
        return 0;
    }
    if cfg!(target_os = "macos") { u.ru_maxrss as u64 } else { u.ru_maxrss as u64 * 1024 }
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

// ---- the subject -----------------------------------------------------------------------------

const TABLE: u32 = 1;
const ARENA_BASE: u32 = 1024;

struct Db {
    _dir: tempfile::TempDir,
    rows: PagedRows,
    root: PageId,
}

/// One table of `n` rows on a real page-backed `CowTree`.
fn build(n: u64) -> Db {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("cow_scan_memory.db"))
        .expect("open");
    let dm = Arc::new(DiskManager::new(file).expect("disk manager"));
    let pool = Arc::new(BufferPoolManager::new(Arc::clone(&dm)));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let store = Arc::new(
        ArenaPageStore::new(pool, Arc::clone(&catalog) as Arc<dyn BranchCatalog>, ARENA_BASE)
            .expect("arena store"),
    );
    let rows = PagedRows::new(store as Arc<dyn PageStore>);
    let branch = BranchId::TRUNK;
    let epoch = catalog.next_epoch();
    let mut root = rows.create_root(branch, epoch).expect("root");
    for i in 0..n {
        let vals = [Value::Integer(i as i32), Value::Varchar(format!("row-{i:012}"))];
        root = rows.put(root, branch, epoch, TABLE, i, &vals).expect("put");
    }
    Db { _dir: dir, rows, root }
}

fn main() {
    let sizes: Vec<u64> = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "10000,100000,1000000".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    println!("# S23 cow range-scan memory");
    println!("# instrument: tracking global allocator (peak live bytes between resets)");
    println!("# row = (Integer, Varchar(16)); key = 12 bytes; buffer pool = 1024 frames (4 MiB)");
    println!("#");
    println!("# stream  = drain the cursor, keeping nothing   (before: `scan_heap`)");
    println!("# stream2 = the same drain a second time -- pool bookkeeping already warm");
    println!("# table   = drain `scan_table`, keeping nothing  (before: `table_heap`)");
    println!("# first10 = read the first ten rows and stop     (before: `first10`)");
    println!("# collect = ask for the whole table as a Vec -- the cost a caller opts into");
    println!();
    println!(
        "{:>9}  {:>10}  {:>10}  {:>10}  {:>10}  {:>12}  {:>11}  {:>9}",
        "rows", "stream_B", "stream2_B", "table_B", "first10_B", "collect_B", "collect_MiB",
        "rss_MiB"
    );

    for &n in &sizes {
        let t0 = Instant::now();
        let db = build(n);
        let built = t0.elapsed();
        let lo = table_lo(TABLE);
        let hi = table_hi(TABLE);

        // (a) the tree-level scan, consumed as it arrives. This is the shape claim: peak is one
        //     page plus one path, whatever `n` is.
        let (b, ba) = reset_peak();
        let mut got = 0u64;
        for e in db.rows.tree().range_scan(db.root, Some(&lo), Some(&hi)).expect("range_scan") {
            e.expect("scan entry");
            got += 1;
        }
        let (stream_heap, stream_allocs) = window(b, ba);
        assert_eq!(got, n, "range_scan yielded {got} of {n} rows");

        // (a2) the identical drain, again. The first window after a build also pays for whatever
        //      the buffer pool's ARC bookkeeping allocates as its working set turns over, and
        //      that is a cost of the pool (fixed at 1024 frames), not of the cursor. Repeating
        //      the drain attributes the two apart instead of asserting which is which.
        let (b, ba) = reset_peak();
        let mut got = 0u64;
        for e in db.rows.tree().range_scan(db.root, Some(&lo), Some(&hi)).expect("range_scan") {
            e.expect("scan entry");
            got += 1;
        }
        let (stream2_heap, stream2_allocs) = window(b, ba);
        assert_eq!(got, n, "range_scan yielded {got} of {n} rows on the second pass");

        // (b) the production path, likewise streamed.
        let (b, ba) = reset_peak();
        let mut got = 0u64;
        for r in db.rows.scan_table(db.root, TABLE).expect("scan_table") {
            r.expect("scan_table row");
            got += 1;
        }
        let (table_heap, table_allocs) = window(b, ba);
        assert_eq!(got, n, "scan_table yielded {got} of {n} rows");

        // (c) a consumer that wants ten rows and stops. Before the change this cost the same as
        //     the whole table, because the size of the answer was fixed before the caller saw it.
        let (b, ba) = reset_peak();
        let ten: Vec<_> = db
            .rows
            .tree()
            .range_scan(db.root, Some(&lo), Some(&hi))
            .expect("range_scan")
            .take(10)
            .map(|e| e.expect("scan entry"))
            .collect();
        let (first10_heap, first10_allocs) = window(b, ba);
        assert_eq!(ten.len(), 10.min(n as usize));
        drop(ten);

        // (d) the deliberate materialise. Still Theta(table) -- it has to be, the caller asked
        //     for the table in RAM -- and reported so the change cannot hide a regression here.
        let (b, ba) = reset_peak();
        let all: Vec<_> = db
            .rows
            .tree()
            .range_scan(db.root, Some(&lo), Some(&hi))
            .expect("range_scan")
            .map(|e| e.expect("scan entry"))
            .collect();
        let (collect_heap, collect_allocs) = window(b, ba);
        assert_eq!(all.len() as u64, n);
        drop(all);

        println!(
            "{:>9}  {:>10}  {:>10}  {:>10}  {:>10}  {:>12}  {:>11.2}  {:>9.1}",
            n,
            stream_heap,
            stream2_heap,
            table_heap,
            first10_heap,
            collect_heap,
            mib(collect_heap),
            peak_rss_bytes() as f64 / (1024.0 * 1024.0),
        );
        eprintln!(
            "  n={n} build={built:?} allocs: stream={stream_allocs} stream2={stream2_allocs} \
             table={table_allocs} first10={first10_allocs} collect={collect_allocs}"
        );
        drop(db);
    }
}
