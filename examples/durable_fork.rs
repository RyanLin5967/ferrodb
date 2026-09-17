//! Does the DURABLE branch catalog hold up when N branches fork from one parent?
//!
//! `fork` clones the parent record and appends it WHOLE, and `live_children` lives inside that
//! record — so the bytes written per fork grow with the number of children the parent already has.
//! That predicts O(N^2) total bytes and an O(N^2) reopen. The scaling bench cannot see any of it:
//! it uses `LogBranchCatalog::in_memory`, which never writes.
use std::sync::Arc;
use std::time::Instant;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;

fn main() {
    let n: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(2000);
    let dir = std::env::temp_dir().join(format!("ferrodb-durable-fork-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("branches.log");

    // O1 PROBE. Peak RSS was ~1417 B per ADDITIONAL branch at 128k, against a 68-byte on-disk
    // record. Printing the struct's own size turns "where does the memory go" from an argument
    // into a subtraction. See SCALE-DESIGN.md O1.
    eprintln!("O1 size_of::<BranchRecord>()={}", std::mem::size_of::<ferrodb::branch::BranchRecord>());

    // FERRODB_CATALOG=table runs the identical fork loop against the system-table catalog.
    // RSS and reopen are directly comparable; TOTAL TIME IS NOT, and the output says so: the log
    // catalog fsyncs every append while the table catalog writes through the buffer pool. Equalise
    // durability before comparing seconds.
    if std::env::var("FERRODB_CATALOG").as_deref() == Ok("table") {
        run_table(n, &dir);
        return;
    }
    let cat = Arc::new(LogBranchCatalog::open(&path, 1).expect("open catalog"));
    let lease = LeaseDeadline(u64::MAX);

    let mut first = 0f64;
    let mut last = 0f64;
    let t0 = Instant::now();
    for i in 0..n {
        let t = Instant::now();
        cat.fork(BranchId::TRUNK, lease).expect("fork");
        let us = t.elapsed().as_secs_f64() * 1e6;
        if i == 0 { first = us; }
        if i + 1 == n { last = us; }
    }
    let total = t0.elapsed().as_secs_f64();
    let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    // The reopen happens while `cat` is STILL ALIVE, so an external peak-RSS reading covers TWO
    // full catalogs, not one. That is a measurement artifact of this harness, not a property of
    // the catalog, and it has to be separable or the residency number means nothing.
    // FERRODB_SKIP_REOPEN=1 runs the identical fork loop with one catalog resident.
    let (reopen, live) = if std::env::var("FERRODB_SKIP_REOPEN").is_ok() {
        (f64::NAN, cat.live_count())
    } else {
        let t = Instant::now();
        let reopened = LogBranchCatalog::open(&path, 1).expect("reopen");
        let r = t.elapsed().as_secs_f64();
        let l = reopened.live_count();
        (r, l)
    };

    println!("{n}\t{first:.2}\t{last:.2}\t{total:.3}\t{bytes}\t{reopen:.3}\t{live}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The same loop against `TableBranchCatalog`, so the two are measured by one harness rather than
/// by two that could differ in something nobody noticed.
fn run_table(n: usize, dir: &std::path::Path) {
    use ferrodb::branch::table_catalog::TableBranchCatalog;
    use ferrodb::buffer::buffer_pool::BufferPoolManager;
    use ferrodb::storage::disk_manager::DiskManager;

    // TWO POOLS, because that is what the runtime has: one over the main database and one over the
    // catalog's own file. The main pool is idle here and that is the point - it is a FIXED cost
    // (1024 frames x 4 KB = 4 MB) that must appear in the measurement rather than be argued away,
    // since the earlier curve ran a single pool and the design records that as a falsifier.
    let main_path = dir.join("main.db");
    let mf = std::fs::OpenOptions::new().create(true).read(true).write(true)
        .open(&main_path).unwrap();
    let _main_pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(mf).unwrap())));

    let path = dir.join("branches.branchcat");
    let _ = std::fs::remove_file(&path);
    let cat = TableBranchCatalog::open_sidecar(&path, 1).expect("open catalog");
    let pool = Arc::clone(cat.pool_handle());
    let lease = LeaseDeadline(u64::MAX);

    let mut first = 0f64;
    let mut last = 0f64;
    let t0 = Instant::now();
    for i in 0..n {
        let t = Instant::now();
        cat.fork(BranchId::TRUNK, lease).expect("fork");
        let us = t.elapsed().as_secs_f64() * 1e6;
        if i == 0 { first = us; }
        if i + 1 == n { last = us; }
    }
    // Make the comparison honest about space: everything must be on disk before the file is
    // measured, or the table catalog reports a smaller file merely because it is still buffered.
    pool.flush_all().expect("flush");
    let total = t0.elapsed().as_secs_f64();
    let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    let root = cat.root_page_id();

    let (reopen, live) = if std::env::var("FERRODB_SKIP_REOPEN").is_ok() {
        (f64::NAN, cat.live_count().expect("live"))
    } else {
        let t = Instant::now();
        let re = TableBranchCatalog::open(Arc::clone(&pool), root).expect("reopen");
        let r = t.elapsed().as_secs_f64();
        // `live_count` walks the Live state span, so it is NOT part of the reopen measurement -
        // reopen is the header descent alone, which is the whole claim.
        (r, re.live_count().expect("live"))
    };
    println!("{n}\t{first:.2}\t{last:.2}\t{total:.3}\t{bytes}\t{reopen:.6}\t{live}");
    let _ = std::fs::remove_dir_all(dir);
}
