//! D166: what does `reaper::detach_from_parent` cost as a branch CHAIN deepens?
//!
//! **Every 10^6-branch result this project has produced was taken at DEPTH 1** — a flat fanout.
//! `d31_reap_cost` varies pages-per-branch over `for _ in 0..branches`, and that shape cannot see
//! this: the cascade only fires when a reaped branch's parent is itself reaped.
//!
//! # THE PREDICTION, REGISTERED HERE BEFORE THE FIRST RUN
//!
//! `detach_from_parent` takes O(depth) STEPS. Each step asks `has_live_children`, whose own doc
//! says the cost "is still not O(1)": a breadth-first walk of the reaped subtree, one `range_scan`
//! per node visited. In a pure chain the reaped subtree beneath the cursor is the whole chain
//! below it, and **that subtree GROWS as the cascade climbs**.
//!
//!   ⇒ PREDICTED: `child_scans` is **Θ(D²)**, so `scans / D²` goes FLAT and `scans / D` climbs.
//!   ⇒ FALSIFIED IF: `scans / D` goes flat instead — then the cost is merely linear in depth, the
//!     "unbounded work per step" note describes a case this workload never reaches, and D166 has
//!     found nothing. **That is a perfectly good outcome and must be reported as one**, not
//!     re-scoped into a finding.
//!
//! # WHY A COUNT AND NOT A CLOCK
//!
//! The claim is a COMPLEXITY CLASS. An integer proves a class directly; a wall clock only
//! illustrates it, and this box never goes quiet on its own. The clock is printed beside the count
//! so a right SHAPE with a wrong MAGNITUDE is visible, but **the count is the result**.
//! `child_scans` is read twice and differenced — it is process-wide and monotonic, so an absolute
//! is meaningless.
//!
//! # THE REAP ORDER IS THE EXPERIMENT
//!
//! Reaping b1..bD in chain order means every reap but the last returns early (`has_live_children`
//! is true — the next link is still live), and **the last one cascades the whole way**. That is
//! the worst case, and it is exactly what MCTS pruning produces — one of BranchBench's five
//! workloads.
//!
//!   d166_reap_depth_curve [depth,list]
use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;

fn main() {
    let depths: Vec<usize> = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "8,16,32,64,128,256".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    println!("D166 reap cost vs branch DEPTH. One chain per row, TableBranchCatalog on a real file.");
    println!("PREDICTED Θ(D²): scans/D² flat, scans/D climbing. FALSIFIED if scans/D is flat.");
    println!();
    println!("     depth        scans      scans/D    scans/D^2    reaped    ms");

    for &d in &depths {
        let dir = std::env::temp_dir()
            .join(format!("ferrodb-d166-{}-{}", std::process::id(), d));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let f = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(dir.join("main.db"))
            .unwrap();
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())));

        // Concrete Arc as well as the trait object: `child_scans()` is the instrument and it lives
        // on the concrete type. Same catalog, two handles.
        let concrete = Arc::new(
            TableBranchCatalog::open_sidecar(&dir.join("branches.branchcat"), 1).unwrap(),
        );
        let catalog: Arc<dyn BranchCatalog> = concrete.clone();
        let base = pool.disk_manager.high_water().unwrap();
        let store =
            Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), base).unwrap());
        let reaper = TwoTierReaper::new(Arc::clone(&catalog), Arc::clone(&store));

        // Build the CHAIN: each branch forks from the previous, not from trunk.
        let mut chain = Vec::with_capacity(d);
        let mut parent = BranchId::TRUNK;
        for _ in 0..d {
            let rec = catalog.fork(parent, LeaseDeadline::from_now(1)).unwrap();
            parent = rec.branch_id;
            chain.push(rec.branch_id);
        }

        let before = concrete.child_scans();
        let t0 = Instant::now();
        let mut reaped = 0usize;
        // Chain order: every reap but the last returns early; the last cascades the whole way.
        for id in &chain {
            if reaper.reap(*id).is_ok() {
                reaped += 1;
            }
        }
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        let scans = concrete.child_scans() - before;

        let df = d as f64;
        println!(
            "  {:>8}   {:>10}   {:>10.2}   {:>10.4}   {:>7}   {:>6.1}",
            d,
            scans,
            scans as f64 / df,
            scans as f64 / (df * df),
            reaped,
            ms
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    println!();
    println!("Read the SHAPE, not the absolutes: whichever of the two ratio columns goes FLAT names");
    println!("the complexity class. If NEITHER is flat, the chain is not the only thing growing and");
    println!("the harness is wrong before the engine is.");
}
