//! Wall #21 — a liveness question asked under a chain of reaped interior branches must not walk
//! the chain.
//!
//! `TableBranchCatalog::has_live_children` resolves a REAPED child by exploring its subtree
//! (D16: a reaped interior node with a live descendant is still a pin). Under a chain
//! `trunk → b1 → … → bD` whose interior nodes are reaped and whose leaf `bD` is live, a question
//! about `bk` therefore visits the span of every node from `bk` down to `b(D-1)`: D−k spans.
//! Three production callers ask it:
//!
//! - **the reaper**, three times per reap (`reap`'s fast/slow split, `detach_from_parent`'s first
//!   step, `release_id`), in the order `expired_candidates` imposes — DEEPEST FIRST — so the k-th
//!   interior reap pays for the whole reaped chain below it;
//! - **the drain and the slow path**, once per parked page, through `live_child_in_epoch_range`
//!   (`reaper.rs` `drain_pending_seeded`, `arena.rs` `retire_arenas_by_rule`);
//! - **the write path**, once per page shadowed, through `max_live_child` (`arena.rs` `cow_page`).
//!
//! The cascade that runs when the LEAF finally goes is a different walk and is already linear:
//! `detach_from_parent` detaches each child before asking about its parent, so every span it asks
//! about is empty. It is the CONTROL arm below — the fix must not move it.
//!
//! # PRE-REGISTERED, before the first run (and committed before the fix)
//!
//! Instrument: `TableBranchCatalog::child_spans_scanned`, one increment per CHILD-span range scan.
//! Fixture per depth D ∈ {16, 64}: a fresh catalog, a chain of D forks, interiors with a lease of
//! 100 ms and the leaf with `u64::MAX`, then `reap_expired(1_000)`. Counts derived from source, by
//! hand, not by calling anything:
//!
//! | arm | at HEAD `9aa6968` + instrument | after the fix |
//! |---|---|---|
//! | `reap_interiors` (D−1 reaps) | 3·D(D−1)/2 → **360 / 6048** | 3·(D−1) → **45 / 189** |
//! | `resolve_parked` (D−1 queries) | D(D−1)/2 → **120 / 2016** | D−1 → **15 / 63** |
//! | `write_path` (D queries) | D(D+1)/2 → **136 / 2080** | D → **16 / 64** |
//! | `cascade` (control) | D+2 → **18 / 66** | D+2 → **18 / 66** |
//!
//! ⇒ At HEAD the control holds and the FIRST failing assertion is `reap_interiors`' class check:
//! **96 spans per reap at D=64 against 24 at D=16**. After the fix every per-unit cost is flat
//! (3, 1, 1). If the CONTROL fails on either build, the fixture or this model is wrong — stop and
//! re-derive; do not read the class assertions.
//!
//! ⚠ **The "after the fix" column is the count at `2987369`, not at the branch tip.** Later commits
//! on this branch add liveness questions of their own. From `206af08`, `release_id` asks once per
//! released ancestor. From `b7e8d4e`, the `Reaped` flip asks once, and `detach_child` asks once
//! under a Reaped parent. At the tip, REAP is 4·(D−1) = 60 / 252 (still flat, 4 per reap), DRAIN
//! and WRITE PATH are unchanged, and the CONTROL is 3D+1 = 49 / 193 (lane report §8.6).
//!
//! The assertions are on the SLOPE (per-unit cost at D=64 against D=16), not on the exact cells,
//! so a constant this model missed cannot fail them; the exact cells are printed for the record.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::record::BranchRecord;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, BranchState, Epoch, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;

/// CHILD-span scans each arm cost at one depth.
#[derive(Debug)]
struct Arms {
    depth: u64,
    reap_interiors: u64,
    resolve_parked: u64,
    write_path: u64,
    cascade: u64,
}

fn run_at(depth: usize) -> Arms {
    assert!(depth >= 3, "fixture: a chain needs a reaped interior with a reaped child");
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(true)
        .open(dir.path().join("w21.db")).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    // One catalog, two handles: the counter lives on the concrete type, the reaper takes the trait.
    let concrete =
        Arc::new(TableBranchCatalog::open_sidecar(&dir.path().join("w21.branchcat"), 1).unwrap());
    let catalog: Arc<dyn BranchCatalog> = concrete.clone();
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog), base).unwrap());
    let reaper = TwoTierReaper::new(Arc::clone(&catalog), Arc::clone(&store));

    // trunk → b1 → … → bD. Every interior's lease has lapsed by t=1000; the leaf's never does.
    let mut chain: Vec<BranchRecord> = Vec::with_capacity(depth);
    let mut parent = BranchId::TRUNK;
    for level in 1..=depth {
        let lease = if level == depth { LeaseDeadline(u64::MAX) } else { LeaseDeadline(100) };
        let rec = catalog
            .fork(parent, lease)
            .unwrap_or_else(|e| panic!("fork at depth {level} refused: {e}"));
        parent = rec.branch_id;
        chain.push(rec);
    }
    let leaf = chain[depth - 1].branch_id;
    let scans = || concrete.child_spans_scanned();

    // ARM 1 — the production sweep: `reap_expired` is `expired_candidates` (deepest first) then
    // `reap_if_still_expired` per branch, the same two halves `lease_thread::scan_once` runs.
    let before = scans();
    let reaped = reaper.reap_expired(1_000).expect("the sweep failed");
    let reap_interiors = scans() - before;
    assert_eq!(
        reaped.len(),
        depth - 1,
        "fixture: the sweep reaped {} of {} interiors, so the arm measured something else",
        reaped.len(),
        depth - 1
    );
    for rec in &chain[..depth - 1] {
        assert_eq!(catalog.get_raw(rec.branch_id.id).unwrap().state, BranchState::Reaped);
    }
    // The pin must still hold — a fix that made the walk cheap by answering "no" is data loss.
    assert!(catalog.get(leaf).is_ok(), "the live leaf became unreadable");
    assert!(
        catalog.has_live_children(BranchId::TRUNK.id).unwrap(),
        "trunk no longer sees the live leaf through {} reaped interiors (D16)",
        depth - 1
    );

    // ARM 2 — the per-parked-page question the drain (`drain_pending_seeded`) and the slow path
    // (`retire_arenas_by_rule`) ask: is a live child forked in this page's window? One window per
    // interior, each holding exactly the next link down.
    let before = scans();
    for i in 0..depth - 1 {
        let e = chain[i + 1].fork_epoch;
        assert!(
            catalog.live_child_in_epoch_range(chain[i].branch_id.id, e, Epoch(e.0 + 1)).unwrap(),
            "a page of b{} visible to b{} was reported unpinned — it would be freed under the leaf",
            i + 1,
            i + 2
        );
    }
    let resolve_parked = scans() - before;

    // ARM 3 — the write path's question (`cow_page` → `privacy_barrier`), once per interior and
    // once for trunk, whose newest child is the top of the reaped chain.
    let before = scans();
    assert_eq!(
        catalog.max_live_child(BranchId::TRUNK.id).unwrap(),
        Some(chain[0].fork_epoch),
        "trunk's newest pinning child is b1"
    );
    for i in 0..depth - 1 {
        assert_eq!(
            catalog.max_live_child(chain[i].branch_id.id).unwrap(),
            Some(chain[i + 1].fork_epoch),
            "b{}'s newest pinning child is b{}",
            i + 1,
            i + 2
        );
    }
    let write_path = scans() - before;

    // ARM 4 — CONTROL. The leaf goes, and the cascade detaches the whole chain.
    let before = scans();
    reaper.reap(leaf).expect("reaping the leaf failed");
    let cascade = scans() - before;
    assert!(
        !catalog.has_live_children(BranchId::TRUNK.id).unwrap(),
        "the cascade stopped short: trunk still reads as pinned"
    );

    Arms {
        depth: depth as u64,
        reap_interiors,
        resolve_parked,
        write_path,
        cascade,
    }
}

#[test]
fn a_liveness_question_under_a_reaped_chain_costs_the_same_at_any_depth() {
    let small = run_at(16);
    let large = run_at(64);
    for a in [&small, &large] {
        eprintln!(
            "wall21 D={} reap_interiors={} resolve_parked={} write_path={} cascade={}",
            a.depth, a.reap_interiors, a.resolve_parked, a.write_path, a.cascade
        );
    }

    // CONTROL FIRST. The leaf's cascade asks one question about one (empty) span per level, and a
    // constant number more per level on the same branch's later changes. Below D it did not run.
    //
    // ⚠ AMENDED (lane §8.6, append-only). This asserted `<= D+2`, the count at `3d4d4d2` and
    // `2987369`. D200 (`206af08`) made `release_id` ask once per released ancestor, so the count
    // became 2D+1 from that commit on, and the bound failed there. This lane's pre-registration
    // missed that until the audit re-derivation. The §8.6 change adds one question per Reaped flip
    // and one per detach under a Reaped parent: 3D+1 at the tip (49 at D=16, 193 at D=64). The
    // bound is now the linear window [D, 4D]. A walk down the chain would be ~D²/2, 2048 at D=64,
    // so the window still refuses the defect this control exists to catch.
    for a in [&small, &large] {
        assert!(
            a.cascade >= a.depth && a.cascade <= 4 * a.depth,
            "CONTROL moved: cascade={} at D={} (expected 3D+1 at the branch tip, linear in D) — the \
             fixture or the pre-registered model is wrong; the class assertions below mean nothing \
             until it holds",
            a.cascade,
            a.depth
        );
    }

    // THE CLASS. Per-unit cost at D=64 may not exceed the D=16 figure by more than one span. A
    // walk down the chain makes it grow with D (24 → 96 per reap at HEAD); a flat cost does not.
    let per = |n: u64, unit: u64| n / unit;
    let (s, l) = (small.depth, large.depth);
    assert!(
        per(large.reap_interiors, l - 1) <= per(small.reap_interiors, s - 1) + 1,
        "REAP: {} spans per interior reap at D={l} against {} at D={s} — the reaper walks the \
         reaped chain below every branch it reaps",
        per(large.reap_interiors, l - 1),
        per(small.reap_interiors, s - 1)
    );
    assert!(
        per(large.resolve_parked, l - 1) <= per(small.resolve_parked, s - 1) + 1,
        "DRAIN: {} spans per parked-page question at D={l} against {} at D={s}",
        per(large.resolve_parked, l - 1),
        per(small.resolve_parked, s - 1)
    );
    assert!(
        per(large.write_path, l) <= per(small.write_path, s) + 1,
        "WRITE PATH: {} spans per max_live_child at D={l} against {} at D={s}",
        per(large.write_path, l),
        per(small.write_path, s)
    );
}
