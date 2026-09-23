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


/// Bytes actually ALLOCATED to a file, as opposed to its addressed length -- `None` where the
/// platform cannot answer.
///
/// This is the whole instrument of D31: the gap between `len()` and allocation is the 256x space
/// amplification, so a fabricated number here would fabricate the finding. `std` exposes
/// `MetadataExt::blocks()` on unix only and has no Windows equivalent, so Windows gets `None` and
/// the caller prints `NaN` rather than a zero that reads as "no amplification".
///
/// Gated the way `storage::disk_manager::pwrite` already gates its platform split.
fn allocated_bytes(path: &std::path::Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(path).ok().map(|m| m.blocks() * 512)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}


/// Free bytes on the filesystem holding `path`, via `df -k`.
///
/// **A shared-machine guard, not a tuning knob — D61.** The byte budget below refuses to let this
/// run's own database grow past a size; it says nothing about what else is on the disk. Another
/// session on this machine tripped a disk monitor twice on 2026-09-19 while this repo held three
/// worktree targets, and an ENOSPC in someone else's lane reads exactly like a real test failure.
/// So the run also stops when the DISK is low, whatever its own database weighs.
fn free_bytes(path: &std::path::Path) -> Option<u64> {
    let out = std::process::Command::new("df").arg("-k").arg(path).output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text.lines().nth(1)?;
    let avail_kb: u64 = line.split_whitespace().nth(3)?.parse().ok()?;
    Some(avail_kb * 1024)
}

/// Stop if the filesystem drops below this, whatever this run's own budget says. See
/// [`free_bytes`]. 20 GiB leaves a working margin for every other lane on a shared machine.
const FREE_FLOOR: u64 = 20 * (1u64 << 30);

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
    // Two handles to ONE catalog, deliberately: `root_page_id` is on the concrete type and not on
    // the `BranchCatalog` trait, and D65's reopen needs the CURRENT root rather than the 1 this
    // was opened with — reopening at a stale root would time the wrong thing.
    let cat_concrete = Arc::new(TableBranchCatalog::open_sidecar(&cat_path, 1).expect("open catalog"));
    let cat: Arc<dyn BranchCatalog> = cat_concrete.clone();
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&cat), base).unwrap());
    // **D79: this harness did NOT persist the free-space map, and the shipped binary does.**
    //
    // `ArenaPageStore` writes the map only if `checkpoint_to` has been called (see
    // `ArenaPageStore::checkpoint_to`; cited as `arena.rs:914` when D79 was written, and the line
    // has since moved — grep the name, not the number). `cli.rs` calls it; this file never did —
    // so D61's published 10^6 curve, and every other 10^6 result in this repo, measured a
    // configuration production does not run. THAT HALF STILL STANDS.
    //
    // ⛔ **THE NEXT SENTENCE OF D79 IS SUPERSEDED — BANDED, NOT DELETED, SO THE REVERSAL IS
    // VISIBLE WHERE THE CLAIM WAS MADE.** D79 said the map "is re-serialised and re-fsynced IN
    // FULL on every new branch's first page write, which is `sum(48i) = 24N^2` bytes over a run:
    // ~24 TB at 10^6." **That was true when written and is FALSE AT HEAD.** D81 (`53a6b66`) made
    // a claim append a 45-byte tail record instead, so the write volume is O(N); the three
    // remaining full-rewrite sites fire once per REAP, not once per branch created.
    //
    // D168 re-derived the consequence in THIS harness: the OFF/ON penalty is **flat in N**
    // (1.36x -> 1.36x across N=500..4000) where D80 measured it GROWING 1.88x -> 3.29x before
    // D81 landed. See `bench/d168_persistence_penalty_at_head.txt`. A flat ~1.4x remains and is
    // the per-claim fsync, which D81 never claimed to remove.
    //
    // `CURVE_PERSIST=1` turns it on so the two curves can be compared. It is OFF by default so
    // that re-running this file reproduces the historical numbers rather than silently replacing
    // them with different ones under the same name.
    let persist = std::env::var("CURVE_PERSIST").map(|v| v == "1").unwrap_or(false);
    if persist {
        store.checkpoint_to(dir.join("main.db.arena"));
    }
    println!("free-space map persistence: {}", if persist { "ON (as cli.rs:120 ships)" } else { "OFF (historical default)" });
    let lease = LeaseDeadline(u64::MAX);

    println!("D32: the curve to 10^6 WITH ONE PAGE WRITTEN PER BRANCH. {threads} threads.");
    println!("ARENA_EXTENT_PAGES = {ARENA_EXTENT_PAGES}, so D31 predicts ~{} KiB of DATA file per branch.",
             ARENA_EXTENT_PAGES * 4);
    println!("Budget: {budget_gb} GB. The run REFUSES to exceed it rather than fill the disk.");
    println!();
    // Two space columns on purpose. `data MB` is FILE LENGTH; `alloc MB` is blocks*512, what the
    // filesystem actually gave out. A reservation scheme can inflate length far past allocation, and
    // quoting only length would overstate the wall. Both are reported so neither can be cherry-picked.
    println!("         N   forks/sec   data MB   alloc MB   len B/branch   alloc B/branch   cat B/branch   pages live   reopen ms");

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
        let alloc = allocated_bytes(&main_path).unwrap_or(0);
        let cbytes = std::fs::metadata(&cat_path).map(|m| m.len()).unwrap_or(0);

        // D65 — reopen the catalog from disk, WITH DATA PRESENT.
        //
        // S4's O(1)-reopen claim has only ever been checked by `examples/branch_curve.rs`, which is
        // FORK-ONLY: no branch in it calls `arena_for` or `alloc_in_arena`, so it reopens a catalog
        // whose branches own no pages. This harness is the one that writes, and it did not measure
        // reopen at all — and it deletes its database at the end, so D61 could not answer this
        // after the fact. The timing block is `branch_curve.rs`'s own, reused rather than rewritten.
        //
        // Reported PER CHECKPOINT on purpose: one reopen number at 10^6 cannot separate O(1) from
        // O(log N) from a small O(N). The column across the decade is the measurement; a single
        // cell is an anecdote.
        let root = cat_concrete.root_page_id();
        let t_reopen = Instant::now();
        let re = TableBranchCatalog::open_sidecar(&cat_path, root).expect("reopen");
        let reopen_ms = t_reopen.elapsed().as_secs_f64() * 1000.0;
        drop(re);

        println!(
            "  {:>8}   {:>9.1}   {:>7.1}   {:>8.1}   {:>12.0}   {:>14.0}   {:>12.0}   {:>10}   {:>9.3}",
            done,
            actually as f64 / secs,
            data as f64 / 1e6,
            alloc as f64 / 1e6,
            data as f64 / done as f64,
            alloc as f64 / done as f64,
            cbytes as f64 / done as f64,
            store.live_page_count().unwrap_or(0),
            reopen_ms,
        );

        if data >= budget {
            stopped_early = Some((done, data));
            break;
        }
        // The disk, not just this run's share of it. See `free_bytes`.
        if let Some(free) = free_bytes(&main_path) {
            if free < FREE_FLOOR {
                println!(
                    "  STOPPING: {:.1} GiB free, floor is {:.0} GiB. Not this run's budget -- the \
                     DISK. Reported as a stop, not a result.",
                    free as f64 / (1u64 << 30) as f64,
                    FREE_FLOOR as f64 / (1u64 << 30) as f64,
                );
                stopped_early = Some((done, data));
                break;
            }
        }
    }

    println!();
    match stopped_early {
        Some((n, bytes)) => {
            let alloc_b = allocated_bytes(&main_path).unwrap_or(0);
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
