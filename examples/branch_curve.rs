//! S12: the curve to 10⁶ branches. The project's stated objective, measured rather than extrapolated.
//!
//! The largest N ever measured here before this was 64,000. Predictions were recorded BEFORE the
//! first run in `artie-research/S12-PREDICTIONS.md`; read them before reading these numbers, so the
//! prediction cannot be quietly retrofitted to whatever came out.
//!
//! One catalog, forked CUMULATIVELY, reporting at each checkpoint — not N separate runs. A fresh
//! catalog per point would measure N small trees instead of one big one, which is the question.
//!
//!   branch_curve [checkpoints,comma,separated] [threads]
//!
//! Reports at each checkpoint: forks/sec for THAT segment, per-fork ms, forks/fsync, catalog bytes,
//! bytes/branch, peak RSS, and reopen time. `live_count` is timed SEPARATELY and deliberately: it
//! walks the whole Live span, so at 10⁶ it is 10⁶ entries for one integer, and folding it into a
//! fork number would silently inflate the thing being measured.
use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;

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

fn main() {
    let checkpoints: Vec<usize> = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "10000,100000,250000,500000,1000000".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let threads: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(64);

    let dir = std::env::temp_dir().join(format!("ferrodb-curve-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let main_path = dir.join("main.db");
    let mf = std::fs::OpenOptions::new()
        .create(true).read(true).write(true).open(&main_path).unwrap();
    let _main_pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(mf).unwrap())));
    let cat_path = dir.join("branches.branchcat");
    let _ = std::fs::remove_file(&cat_path);
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&cat_path, 1).expect("open catalog"));
    let lease = LeaseDeadline(u64::MAX);

    // S13 CALIBRATION FIXTURE. 500 branches with a DISTINCT short deadline, seeded once. Every
    // other branch takes LeaseDeadline(MAX), so `expired_before(SEED_D - 1)` answers ZERO and
    // `expired_before(SEED_D + 1)` answers exactly 500, at EVERY N.
    //
    // That pair is the whole point: the reaper's claim is that its cost tracks the ANSWER, not N.
    // A single arm cannot show that -- a number that is flat might mean "proportional to the
    // answer" or might mean "the instrument cannot see anything". Two arms over the same function
    // at the same N, differing only in answer size, distinguish them. The repo's own scaling bench
    // already refuses to report flatness unless a 20x larger tree moves the number; same discipline.
    const SEED_D: u64 = 1_000_000;
    const SEED_N: usize = 500;
    for _ in 0..SEED_N {
        cat.fork(BranchId::TRUNK, LeaseDeadline(SEED_D)).expect("seed fork");
    }

    println!("S12: the curve to 10^6 branches. ONE catalog, forked cumulatively. {threads} threads.");
    println!("Predictions were recorded before this run: artie-research/S12-PREDICTIONS.md");
    println!();
    println!("         N   seg forks/sec   per-fork ms   fsyncs   f/fsync   cat MB   B/branch   peak RSS MB   reopen ms   live_count ms   read us   reap(0) us   reap(500) us");

    let mut done = 0usize;
    let mut prev_syncs = 0u64;
    for &target in &checkpoints {
        if target <= done {
            continue;
        }
        let seg = target - done;
        let per = seg / threads.max(1);
        let actually = per * threads;

        let t0 = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..threads {
                let cat = Arc::clone(&cat);
                s.spawn(move || {
                    for _ in 0..per {
                        cat.fork(BranchId::TRUNK, lease).expect("fork");
                    }
                });
            }
        });
        let secs = t0.elapsed().as_secs_f64();
        done += actually;

        let syncs = cat.syncs_issued();
        let seg_syncs = syncs - prev_syncs;
        prev_syncs = syncs;

        let bytes = std::fs::metadata(&cat_path).map(|m| m.len()).unwrap_or(0);

        // Timed separately and on purpose: this walks the whole Live span.
        let t = Instant::now();
        let live = cat.live_count().unwrap_or(0);
        let live_ms = t.elapsed().as_secs_f64() * 1000.0;
        assert!(live >= done, "live_count {live} < forks issued {done}");

        // S13: a READ. The standing claim is "reads flat to 50k branches (x1.00)"; 10^6 is 20x
        // beyond where that was ever tested. get_raw is a descent plus hydrate.
        let mut reads: Vec<f64> = Vec::with_capacity(200);
        let mut probe = 1u64;
        for _ in 0..200 {
            probe = probe.wrapping_mul(6364136223846793005).wrapping_add(1);
            let id = 1 + (probe >> 33) % (done as u64).max(1);
            let t = Instant::now();
            let _ = std::hint::black_box(cat.get_raw(id));
            reads.push(t.elapsed().as_secs_f64() * 1e6);
        }
        reads.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let read_us = reads[reads.len() / 2];

        // S13: the reaper, as a CALIBRATED PAIR. Same function, same N, different answer size.
        let t = Instant::now();
        let none = cat.expired_before(SEED_D - 1).expect("reap none");
        let reap0_us = t.elapsed().as_secs_f64() * 1e6;
        assert!(none.is_empty(), "expired_before(SEED_D-1) returned {} rows", none.len());

        let t = Instant::now();
        let some = cat.expired_before(SEED_D + 1).expect("reap some");
        let reap500_us = t.elapsed().as_secs_f64() * 1e6;
        assert_eq!(some.len(), SEED_N, "the 500-branch fixture did not come back whole");

        // Reopen from disk. The O(1) claim is that this does not grow with N.
        let root = cat.root_page_id();
        let t = Instant::now();
        let re = TableBranchCatalog::open_sidecar(&cat_path, root).expect("reopen");
        let reopen_ms = t.elapsed().as_secs_f64() * 1000.0;
        drop(re);

        println!(
            "  {:>8}   {:>12.1}   {:>11.4}   {:>6}   {:>7.1}   {:>6.1}   {:>8.1}   {:>11.1}   {:>9.4}   {:>12.3}   {:>7.2}   {:>10.1}   {:>12.1}",
            done,
            actually as f64 / secs,
            secs * 1000.0 / actually as f64,
            seg_syncs,
            actually as f64 / seg_syncs.max(1) as f64,
            bytes as f64 / 1e6,
            bytes as f64 / done as f64,
            peak_rss_bytes() as f64 / 1e6,
            reopen_ms,
            live_ms,
            read_us,
            reap0_us,
            reap500_us,
        );
    }
    println!();
    println!("seg forks/sec is for THAT SEGMENT only, so a knee shows as a drop between rows rather");
    println!("than being averaged away across the whole run.");
    println!();
    println!("reap(0) and reap(500) are the SAME function at the SAME N, differing only in how many");
    println!("rows the answer contains. If BOTH are flat in N, the reaper's cost tracks the answer");
    println!("and not the catalog -- which is the claim. If reap(0) grows with N, the claim is false.");
    println!("If reap(500) ~= reap(0), the instrument cannot see answer size and proves nothing.");
    let _ = std::fs::remove_dir_all(&dir);
}
