//! **D40's primary evidence: the catalog-descent curve, before and after.**
//!
//! The claim is a complexity class — O(branches x live_arenas) -> O(arenas actually touched) —
//! and a wall clock proves a class only indirectly. It also moves when the machine is loaded, and
//! this machine has been. An operation count does neither: `TwoTierReaper::sweep_descents` counts
//! the `catalog.get_raw` calls the extent sweep makes, each one a `BPlusTreeManager::search`, and
//! that number is identical on an idle machine and a thrashing one.
//!
//! The workload is `tests/d19_leak_is_a_race.rs`'s: fork off trunk, claim an extent, write one
//! novel page, abandon, then `reap_expired` the lot. One arena per branch, which is what makes
//! the arithmetic checkable by hand.
//!
//! **Pre-registered expectation for the unfixed code**, written before the first measurement.
//! `reap_expired` reaps N branches; each `reap` takes the fast path (childless leaf), frees its
//! own extent wholesale, then calls `drain_pending`, which ends in a scan over every arena still
//! live. After the i-th reap that is N-i-1 arenas, so
//!
//!     sum over i in [0, N) of (N - i - 1)  =  N(N-1)/2
//!
//! For d19's largest arm, N = 2016: **2,031,120 descents**. For the points below: 125 -> 7,750;
//! 250 -> 31,125; 500 -> 124,750; 1000 -> 499,500. If the measured "before" does not land on
//! these, the reading of the call graph is wrong and the fix is not to be trusted until that is
//! settled.
//!
//! Run with the shipped code for the AFTER curve. For the BEFORE curve, restore the pre-D40 call
//! graph (see `bench/d40_descent_curve.py`, which does it mechanically) and run it again.
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::storage::disk_manager::DiskManager;

/// One d19 arm at `n` branches. Returns (descents, visits, reaped, leak, reserved_after).
fn arm(n: usize) -> (u64, u64, usize, u32, u32) {
    let tag = format!("d40-curve-{}-{}", std::process::id(), n);
    let db = std::env::temp_dir().join(format!("{tag}.db"));
    let cat = std::env::temp_dir().join(format!("{tag}.cat"));
    let _ = std::fs::remove_file(&db);
    let _ = std::fs::remove_file(&cat);

    let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(&db).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    // The catalog that SHIPS. `LogBranchCatalog` holds records resident, so `get_raw` is a hash
    // lookup there and the descent this measures does not exist — D19's lesson, applied to the
    // instrument rather than to a test.
    let catalog: Arc<dyn BranchCatalog> =
        Arc::new(TableBranchCatalog::open_sidecar(&cat, 1).unwrap());
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog), base).unwrap());
    let reaper = TwoTierReaper::new(Arc::clone(&catalog), Arc::clone(&store));

    let baseline_live = store.live_page_count().unwrap();
    let baseline_reserved = store.reserved_page_count();

    for _ in 0..n {
        let rec = catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(50)).unwrap();
        let arena = store.arena_for(rec.branch_id).unwrap();
        let ep = catalog.next_epoch();
        let p = store.alloc_in_arena(arena, PageType::BTreeLeaf, ep).unwrap();
        let h = store.read_page(p).unwrap();
        let mut f = h.write();
        f.data[PAGE_HEADER_SIZE] = 0xD4;
        stamp_checksum(&mut f.data);
    }

    // Count only the reaping, not the forking: `fork` descends into the catalog too, and folding
    // that in would credit the sweep with work it never did.
    let d0 = reaper.sweep_descents();
    let v0 = reaper.sweep_visits();
    let reaped = reaper.reap_expired(u64::MAX).unwrap().len();
    reaper.drain_pending().ok();
    let descents = reaper.sweep_descents() - d0;
    let visits = reaper.sweep_visits() - v0;

    let leak = store.live_page_count().unwrap().saturating_sub(baseline_live);
    let reserved = store.reserved_page_count().saturating_sub(baseline_reserved);
    let _ = std::fs::remove_file(&db);
    let _ = std::fs::remove_file(&cat);
    (descents, visits, reaped, leak, reserved)
}

fn main() {
    let points: Vec<usize> = std::env::args()
        .skip(1)
        .map(|a| a.parse().expect("branch count"))
        .collect::<Vec<_>>();
    let points = if points.is_empty() { vec![125, 250, 500, 1000, 2016] } else { points };

    println!("{:>7}  {:>12}  {:>12}  {:>10}  {:>8}  {:>6}  {:>9}", "N", "descents", "visits", "N(N-1)/2", "reaped", "leak", "reserved");
    let mut prev: Option<(usize, u64)> = None;
    for n in points {
        let (descents, visits, reaped, leak, reserved) = arm(n);
        let quad = (n as u64) * (n as u64 - 1) / 2;
        print!(
            "{n:>7}  {descents:>12}  {visits:>12}  {quad:>10}  {reaped:>8}  {leak:>6}  {reserved:>9}"
        );
        // The growth ratio is the claim. Doubling N quadruples a quadratic and doubles a linear
        // one; printing it next to the doubling means nobody has to take the class on trust.
        if let Some((pn, pd)) = prev {
            if pd > 0 && n >= pn * 2 {
                print!("   x{:.2} per {:.0}x N", descents as f64 / pd as f64, n as f64 / pn as f64);
            }
        }
        println!();
        prev = Some((n, descents));
    }
}
