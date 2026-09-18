//! What does a fork that has not written yet COST IN MEMORY? (`SCALE-DESIGN.md` D6, option 2)
//!
//! ⛔ THIS IS THE COST COLUMN OF THE LAZY-FORK CHANGE, AND IT HAS TO BE MEASURED, NOT ASSERTED.
//! Deferring a fork's keys to a branch's first write makes the fork free on disk by making it
//! resident in RAM instead. That is a trade, not a free lunch, and the project has a specific
//! reason to be suspicious of it: `LogBranchCatalog` was replaced precisely because it held every
//! record in a process-lifetime `HashMap`, measured at ~685 resident bytes per branch
//! (`bench/o1_residency_marginal.txt`). A change that quietly puts a subset of branches back into
//! exactly that shape owes a number.
//!
//! So: fork N branches one way or the other, and read this process's RSS from `ps` before and
//! after. RSS is a coarse instrument — it counts the buffer pool, the allocator's slack and the
//! binary itself — which is why both arms run the same harness and the DIFFERENCE between them is
//! the answer rather than either absolute.
//!
//! The `fork-reap` mode answers the follow-up question, which is the one that decides whether the
//! trade is bounded: a pending fork's memory has to be RELEASED when the branch is reaped, or the
//! "cost" is a leak with a nicer name. It also exercises the in-memory retired-generation map and
//! free-id pool, which are the two structures that could quietly grow without one.
//!
//!   fork_residency <speculative|write|fork-reap> [N]
use std::sync::Arc;

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{ArenaPageStore, BranchCatalog, Reaper, TwoTierReaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;

/// Resident set size in bytes, from `ps`. No dependency, and it is the same number Activity
/// Monitor shows, so a reader can check it by hand.
fn rss_bytes() -> u64 {
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p"])
        .arg(std::process::id().to_string())
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "speculative".into());
    let n: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(50_000);

    let dir = std::env::temp_dir().join(format!("ferrodb-residency-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("branches.branchcat");
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&path, 1).expect("open catalog"));

    // The reaper, only for the mode that needs it, and built BEFORE the baseline so its own pool
    // is not charged to the branches.
    let reaper = if mode == "fork-reap" {
        let apath = dir.join("arena.db");
        let f =
            std::fs::OpenOptions::new().create(true).read(true).write(true).open(&apath).unwrap();
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())));
        let base = pool.disk_manager.high_water().unwrap().max(1);
        let store = Arc::new(
            ArenaPageStore::new(pool, Arc::clone(&cat) as Arc<dyn BranchCatalog>, base)
                .expect("arena store"),
        );
        Some(TwoTierReaper::new(Arc::clone(&cat) as Arc<dyn BranchCatalog>, store))
    } else {
        None
    };

    // Touch the catalog once before the baseline so the pool and the tree's first pages are
    // already allocated: otherwise their one-off cost is charged to the first arm.
    cat.get_raw(BranchId::TRUNK.id).expect("trunk");
    let before = rss_bytes();

    let lease = LeaseDeadline(u64::MAX);
    for _ in 0..n {
        let c = cat.fork(BranchId::TRUNK, lease).expect("fork");
        if mode == "write" {
            cat.set_root(c.branch_id, 2).expect("set_root");
        }
        if let Some(r) = &reaper {
            r.reap(c.branch_id).expect("reap");
        }
    }
    let after = rss_bytes();

    let file = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    println!(
        "{mode:12} n={n:<8} rss_before={before:>11}  rss_after={after:>11}  \
         delta={:>11}  bytes/branch={:>8.1}  catalog_file={file}",
        after as i64 - before as i64,
        (after as i64 - before as i64) as f64 / n as f64,
    );
    let _ = std::fs::remove_dir_all(&dir);
}
