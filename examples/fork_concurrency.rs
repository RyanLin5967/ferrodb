//! Does fork throughput scale with CONCURRENCY? (D6 / D6a)
//!
//! `examples/durable_fork.rs` forks in ONE thread, so it cannot see the thing this measures. It
//! reported ~270 forks/sec and that number was read as "fork is fsync-bound", which is true but
//! incomplete: `TableBranchCatalog::fork` holds `self.logical` ACROSS `commit()`, and `commit()`
//! fsyncs. So forks serialize on the mutex and every one of them pays a private disk round-trip.
//! Group commit — the standard answer — batches CONCURRENT commits, and until the critical section
//! is split there are none to batch. Measuring it against the serial harness would have produced
//! x1.00 and been misread as a fact about batching rather than about a mutex.
//!
//! So: T threads, each forking N/T times from trunk, total wall time, forks/sec.
//!
//!   cargo run --release --example fork_concurrency -- [N] [T,T,T]
//!
//! CALIBRATION. The harness reports FORKS PER FSYNC alongside throughput, which is the direct
//! evidence that batching is happening at all: at one fork per fsync nothing is being shared, and
//! a rising ratio is group commit working. It also settles where the bottleneck is once throughput
//! plateaus — if forks/fsync keeps climbing while forks/sec does not, the fsync is no longer the
//! limit and the remaining cost is the tree mutations under `logical`.
//! (An earlier draft of this comment promised an env var that SKIPPED the fsync. That was not
//! built, deliberately: a durability bypass sitting in production code is a footgun, and a counter
//! answers the same question without one.)
use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;

fn open_catalog(dir: &std::path::Path, tag: &str) -> Arc<TableBranchCatalog> {
    // Two pools, matching the runtime and matching durable_fork, so the numbers are comparable.
    let main_path = dir.join(format!("main-{tag}.db"));
    let mf = std::fs::OpenOptions::new()
        .create(true).read(true).write(true).open(&main_path).unwrap();
    let _main_pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(mf).unwrap())));
    let path = dir.join(format!("branches-{tag}.branchcat"));
    let _ = std::fs::remove_file(&path);
    Arc::new(TableBranchCatalog::open_sidecar(&path, 1).expect("open catalog"))
}

fn main() {
    let n: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(2000);
    let threads: Vec<usize> = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "1,8,64".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let dir = std::env::temp_dir().join(format!("ferrodb-forkconc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    println!("D6: fork throughput vs concurrency. N={n} forks per arm, TableBranchCatalog, release.");
    println!("Recorded {}. One run per cell.", chrono_ish());
    println!();
    println!("  threads   forks     seconds    forks/sec   per-fork ms   fsyncs  forks/fsync");

    for &t in &threads {
        let cat = open_catalog(&dir, &format!("t{t}"));
        let lease = LeaseDeadline(u64::MAX);
        let per = n / t.max(1);
        let total = per * t;

        let t0 = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..t {
                let cat = Arc::clone(&cat);
                s.spawn(move || {
                    for _ in 0..per {
                        cat.fork(BranchId::TRUNK, lease).expect("fork");
                    }
                });
            }
        });
        let secs = t0.elapsed().as_secs_f64();
        let syncs = cat.syncs_issued();
        println!(
            "  {:7}   {:7}   {:8.3}   {:9.1}   {:8.3}   {:6}   {:9.1}",
            t, total, secs, total as f64 / secs, secs * 1000.0 / total as f64,
            syncs, total as f64 / syncs.max(1) as f64
        );
    }
    println!();
    println!("Read it this way: if forks/sec is FLAT across threads, the forks are serializing and");
    println!("group commit has nothing to batch. That is the D6 premise check, as a number.");
    let _ = std::fs::remove_dir_all(&dir);
}

fn chrono_ish() -> String {
    // No chrono dependency in this repo (zero runtime deps is the project's rule), and a wrong
    // date on an artifact is worse than none, so this prints what it can actually know.
    std::process::Command::new("date")
        .arg("+%Y-%m-%dT%H:%M:%S%z")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}
