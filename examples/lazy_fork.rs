//! Does moving the durability point to the first write pay? (`SCALE-DESIGN.md` D6, option 2)
//!
//! Three arms, because one number cannot settle this on its own:
//!
//! - **speculative** — fork and abandon. The case the change exists for: an agent forks, the
//!   branch is never written to, and the lease reaper takes it. If this does not move, the change
//!   is worthless.
//! - **fork+write** — fork, then publish a root. The durable path. This one MUST NOT REGRESS, and
//!   it is the arm that would expose the change as a shell game: deferring work is not removing it
//!   if the deferred work simply arrives later with interest.
//! - **fork+reap** — fork, then reap through the real `TwoTierReaper`. The whole speculative
//!   lifecycle, including the catalog writes the reaper performs, which a fork-only arm cannot see.
//!
//! Every arm prints **fsyncs**, not just throughput. A change that made forks fast by quietly
//! dropping durability would look identical on the time column and obvious on this one; and a
//! change that only deferred the fsync into the reaper would show up as fsyncs that did not fall.
//!
//!   cargo run --release --example lazy_fork -- [N] [T,T,T]
use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{ArenaPageStore, BranchCatalog, Reaper, TwoTierReaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;

fn open_catalog(dir: &std::path::Path, tag: &str) -> Arc<TableBranchCatalog> {
    // Two pools, matching the runtime and matching `fork_concurrency`, so the numbers sit beside
    // the ones already in bench/ rather than beside nothing.
    let main_path = dir.join(format!("main-{tag}.db"));
    let _ = std::fs::remove_file(&main_path);
    let mf =
        std::fs::OpenOptions::new().create(true).read(true).write(true).open(&main_path).unwrap();
    let _main_pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(mf).unwrap())));
    let path = dir.join(format!("branches-{tag}.branchcat"));
    let _ = std::fs::remove_file(&path);
    Arc::new(TableBranchCatalog::open_sidecar(&path, 1).expect("open catalog"))
}

/// The arena store the reaper needs. Its own file, so the catalog's page numbers are untouched.
fn open_store(
    dir: &std::path::Path,
    tag: &str,
    cat: Arc<TableBranchCatalog>,
) -> Arc<ArenaPageStore> {
    let path = dir.join(format!("arena-{tag}.db"));
    let _ = std::fs::remove_file(&path);
    let f = std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())));
    let base = pool.disk_manager.high_water().unwrap().max(1);
    Arc::new(ArenaPageStore::new(pool, cat, base).expect("arena store"))
}

fn row(arm: &str, threads: usize, total: usize, secs: f64, syncs: u64) {
    println!(
        "  {:<12} {:5}   {:7}   {:9.3}   {:11.1}   {:9.4}   {:8}",
        arm,
        threads,
        total,
        secs,
        total as f64 / secs,
        secs * 1000.0 / total as f64,
        syncs
    );
}

fn main() {
    let n: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(2000);
    let threads: Vec<usize> = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "1,8,64".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    let dir = std::env::temp_dir().join(format!("ferrodb-lazyfork-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    println!("D6 option 2: durability at first write. N={n} per arm, TableBranchCatalog, release.");
    println!("Recorded {}. One run per cell.", chrono_ish());
    println!();
    println!("  arm          thr     branches   seconds     per second   per-op ms    fsyncs");

    let lease = LeaseDeadline(u64::MAX);

    for &t in &threads {
        // ---- speculative: fork and abandon ----------------------------------------------------
        let cat = open_catalog(&dir, &format!("spec-t{t}"));
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
        row("speculative", t, total, t0.elapsed().as_secs_f64(), cat.syncs_issued());
        drop(cat);

        // ---- fork+write: the durable path, which must not regress -----------------------------
        let cat = open_catalog(&dir, &format!("write-t{t}"));
        let t0 = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..t {
                let cat = Arc::clone(&cat);
                s.spawn(move || {
                    for _ in 0..per {
                        let c = cat.fork(BranchId::TRUNK, lease).expect("fork");
                        cat.set_root(c.branch_id, 2).expect("set_root");
                    }
                });
            }
        });
        row("fork+write", t, total, t0.elapsed().as_secs_f64(), cat.syncs_issued());
        drop(cat);

        // ---- fork+reap: the whole speculative lifecycle ----------------------------------------
        let cat = open_catalog(&dir, &format!("reap-t{t}"));
        let store = open_store(&dir, &format!("reap-t{t}"), Arc::clone(&cat));
        let reaper =
            Arc::new(TwoTierReaper::new(Arc::clone(&cat) as Arc<dyn BranchCatalog>, store));
        let t0 = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..t {
                let cat = Arc::clone(&cat);
                let reaper = Arc::clone(&reaper);
                s.spawn(move || {
                    for _ in 0..per {
                        let c = cat.fork(BranchId::TRUNK, lease).expect("fork");
                        reaper.reap(c.branch_id).expect("reap");
                    }
                });
            }
        });
        row("fork+reap", t, total, t0.elapsed().as_secs_f64(), cat.syncs_issued());
        drop(reaper);
        drop(cat);
    }

    println!();
    println!("fsyncs is the honest column. A speculative arm at 0 fsyncs is the claim; a fork+write");
    println!("arm whose fsyncs fell would mean durability went missing rather than moved.");
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
