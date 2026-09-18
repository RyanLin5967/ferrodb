//! D32: the same curve as `branch_curve`, but **every branch writes one page**.
//!
//! `branch_curve` reached 10⁶ branches and is honest about what it measured — the CATALOG. Its hot
//! loop calls exactly one operation, `cat.fork(..)`, and it builds a `BufferPoolManager` it binds to
//! `_main_pool` and never uses. **No branch in that run ever wrote a page**, so its `cat MB` column
//! is catalog bytes and the data file stays empty.
//!
//! That matters because of D31: `arena_for` gives each writing branch its own extent, and
//! `alloc_arena` reserves `ARENA_EXTENT_PAGES` (256) of them at once. So a branch's FIRST 4 KB page
//! costs 1 MiB. Fork-only workloads never pay it; every real agent workload does.
//!
//! This harness adds the three calls `branch_curve` leaves out — `arena_for`, `next_epoch`,
//! `alloc_in_arena` — and reports **DATA FILE bytes**, because reporting the catalog column here
//! would reproduce exactly the blindness the repo already paid for: `branch_scaling_bench.rs` used
//! an in-memory catalog and therefore could not see an O(N²) durable cost at all. The instrument has
//! to exercise the path being claimed about.
//!
//!   branch_curve_writes [checkpoints,comma,separated] [threads] [byte_budget_gb]
//!
//! **The byte budget is a refusal, not a tuning knob.** At 1 MiB/branch the 10⁶ point would need
//! ~1 TiB, which no machine here has, and a benchmark that fills the disk takes the machine down
//! with it. It stops at the budget and SAYS SO, naming the N it reached. "Stopped early on space" is
//! the result, not a failure of the run — and if it does NOT stop early, that kills D31, which is
//! the outcome D31's own falsifier asks for.
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline, ARENA_EXTENT_PAGES};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

/// Say which of the two worlds this number came from, and NAME THE HYPOTHESIS.
///
/// **This function exists because the line it replaces read backwards after the fix landed.**
/// The old text was *"If that is far below 1048576 B, the first-page amplification is NOT binding
/// here and D31 should be CLOSED rather than built"*. That reading holds only while geometric
/// extent growth is ABSENT: then a run that completes the budget really does falsify D31. Once the
/// growth rule is BUILT, the same branch prints for the opposite reason — the run completes
/// *because the fix worked* — so an unconditioned verdict tells its reader to close the row at the
/// exact moment the row succeeded, and a reader with no context believes the line rather than
/// inferring the inversion.
///
/// A conditional verdict has to name the hypothesis it is conditioned on. The two reference values
/// below are what distinguishes the cases, so both are printed in every outcome, including the one
/// where neither fits.
fn verdict(bytes_per_branch: f64) {
    /// One page: what a branch's first page costs when extents grow geometrically from one.
    const GROWN: f64 = PAGE_SIZE as f64;
    /// One whole extent: what it cost when every extent was `ARENA_EXTENT_PAGES` long.
    const FLAT: f64 = (ARENA_EXTENT_PAGES as usize * PAGE_SIZE) as f64;
    /// "Within a small factor of". Generous on purpose — the catalog, the trunk's own pages and
    /// the partially filled last extent all land in the gap, and the two references are 256x
    /// apart, so nothing can be near both.
    const NEAR: f64 = 8.0;

    println!();
    println!("  reference: one page = {GROWN:.0} B  ·  one full extent = {FLAT:.0} B  ({}x apart)",
             ARENA_EXTENT_PAGES);
    if bytes_per_branch <= GROWN * NEAR {
        println!("VERDICT — AMPLIFICATION GONE. {bytes_per_branch:.0} B/branch is within {NEAR:.0}x of a");
        println!("single page, so a branch's first page costs about a page. This is the tree WITH");
        println!("geometric extent growth (D31) built; without it this number is ~{FLAT:.0}.");
    } else if bytes_per_branch >= FLAT / NEAR {
        println!("VERDICT — AMPLIFICATION PRESENT. {bytes_per_branch:.0} B/branch is within {NEAR:.0}x of a");
        println!("whole extent, so every branch pays {ARENA_EXTENT_PAGES} pages for its first one. This is the");
        println!("tree WITHOUT geometric extent growth, and it is what D31 exists to remove.");
    } else {
        println!("VERDICT — AMBIGUOUS. {bytes_per_branch:.0} B/branch sits between the two references above,");
        println!("near neither. Do NOT read this as either result: say which tree it was taken on,");
        println!("and look at `pages live` and the allocated-blocks column before concluding.");
    }
}

fn main() {
    let checkpoints: Vec<usize> = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "1000,2000,4000,8000".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let threads: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(8);
    let budget_gb: f64 = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(8.0);
    let budget = (budget_gb * 1e9) as u64;

    let dir = std::env::temp_dir().join(format!("ferrodb-wcurve-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let main_path = dir.join("main.db");
    let mf = std::fs::OpenOptions::new()
        .create(true).read(true).write(true).open(&main_path).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(mf).unwrap())));
    let cat_path = dir.join("branches.branchcat");
    let _ = std::fs::remove_file(&cat_path);
    let cat: Arc<dyn BranchCatalog> =
        Arc::new(TableBranchCatalog::open_sidecar(&cat_path, 1).expect("open catalog"));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&cat), base).unwrap());
    let lease = LeaseDeadline(u64::MAX);

    println!("D32: the curve to 10^6 WITH ONE PAGE WRITTEN PER BRANCH. {threads} threads.");
    println!("ARENA_EXTENT_PAGES = {ARENA_EXTENT_PAGES}, so D31 predicts ~{} KiB of DATA file per branch.",
             ARENA_EXTENT_PAGES * 4);
    println!("Budget: {budget_gb} GB. The run REFUSES to exceed it rather than fill the disk.");
    println!();
    // Two space columns on purpose. `data MB` is FILE LENGTH; `alloc MB` is blocks*512, what the
    // filesystem actually gave out. A reservation scheme can inflate length far past allocation, and
    // quoting only length would overstate the wall. Both are reported so neither can be cherry-picked.
    println!("         N   forks/sec   data MB   alloc MB   len B/branch   alloc B/branch   cat B/branch   pages live");

    let mut done = 0usize;
    let mut stopped_early: Option<(usize, u64)> = None;

    for &target in &checkpoints {
        if target <= done || stopped_early.is_some() {
            continue;
        }
        let seg = target - done;
        let per = seg / threads.max(1);
        let actually = per * threads;
        if actually == 0 {
            continue;
        }

        let t0 = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..threads {
                let cat = Arc::clone(&cat);
                let store = Arc::clone(&store);
                s.spawn(move || {
                    for _ in 0..per {
                        let rec = cat.fork(BranchId::TRUNK, lease).expect("fork");
                        // THE THREE CALLS `branch_curve` LEAVES OUT. This is the whole difference.
                        let arena = store.arena_for(rec.branch_id).expect("arena");
                        let ep = cat.next_epoch();
                        let p = store
                            .alloc_in_arena(arena, PageType::BTreeLeaf, ep)
                            .expect("alloc");
                        let h = store.read_page(p).expect("read");
                        let mut f = h.write();
                        f.data[PAGE_HEADER_SIZE] = 0xD3;
                        stamp_checksum(&mut f.data);
                    }
                });
            }
        });
        let secs = t0.elapsed().as_secs_f64();
        done += actually;

        let md = std::fs::metadata(&main_path).ok();
        let data = md.as_ref().map(|m| m.len()).unwrap_or(0);
        let alloc = md.as_ref().map(|m| m.blocks() * 512).unwrap_or(0);
        let cbytes = std::fs::metadata(&cat_path).map(|m| m.len()).unwrap_or(0);
        println!(
            "  {:>8}   {:>9.1}   {:>7.1}   {:>8.1}   {:>12.0}   {:>14.0}   {:>12.0}   {:>10}",
            done,
            actually as f64 / secs,
            data as f64 / 1e6,
            alloc as f64 / 1e6,
            data as f64 / done as f64,
            alloc as f64 / done as f64,
            cbytes as f64 / done as f64,
            store.live_page_count().unwrap_or(0),
        );

        if data >= budget {
            stopped_early = Some((done, data));
            break;
        }
    }

    println!();
    match stopped_early {
        Some((n, bytes)) => {
            let alloc_b = std::fs::metadata(&main_path).map(|m| m.blocks() * 512).unwrap_or(0);
            let alloc_per = alloc_b as f64 / n as f64;
            let per_branch = bytes as f64 / n as f64;
            println!("Allocated (blocks*512) rather than merely addressed: {:.0} B/branch, {:.2} GB total.",
                     alloc_per, alloc_b as f64 / 1e9);
            println!("  -> 10^6 branches x {:.0} allocated B = {:.2} TB actually on disk.",
                     alloc_per, alloc_per * 1e6 / 1e12);
            println!("STOPPED EARLY ON SPACE at N = {n}, data file {:.2} GB ({:.0} bytes/branch).",
                     bytes as f64 / 1e9, per_branch);
            println!("Extrapolated (ARITHMETIC, NOT MEASURED) to the objective:");
            println!("  10^6 branches x {:.0} B = {:.2} TB of data file.", per_branch,
                     per_branch * 1e6 / 1e12);
            println!("bench/curve_to_1e6.txt was reached fork-only, on the one workload that does not");
            println!("pay this. Fix is geometric extent growth (SCALE-DESIGN D31 option 1), NOT");
            println!("lowering ARENA_EXTENT_PAGES -- reclamation is per-extent on purpose.");
            verdict(per_branch);
        }
        None => {
            let data = std::fs::metadata(&main_path).map(|m| m.len()).unwrap_or(0);
            println!("DID NOT stop early within the budget: N = {done}, {:.0} bytes/branch.",
                     data as f64 / done.max(1) as f64);
            verdict(data as f64 / done.max(1) as f64);
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
