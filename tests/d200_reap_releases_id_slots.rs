//! D200 — a reap must give back every id slot it retires, including the ancestors its cascade
//! detaches, and an open must give back the ones earlier reaps left behind.
//!
//! `release_id` is the only writer of a `FREE_ID` key, and before D200 its one production caller
//! was `reap` itself, for the branch being reaped. The cascade in `detach_from_parent` detaches
//! each reaped ancestor whose last pinning child just went and never released it, and the only
//! reaped-id sweep was `migrate_from`, a one-time log-to-table conversion. So every pruned
//! interior branch kept its slot for ever — permanent, and growing with branches ever pruned.
//!
//! Observed at the layer that pays: a leaked slot is one a later `fork` cannot recycle, so each
//! test forks and asserts WHICH ids come back (membership, not a count). A recycled slot is forked
//! at the reaped record's bumped generation, so `generation != 0` is exactly "this fork reused a
//! slot".
//!
//! # PRE-REGISTERED, before the fix
//!
//! | test | at `d04aeeb` (before) | after the fix |
//! |---|---|---|
//! | chain of 8, interiors then leaf | recycled = `{b8}` — **1 of 8** | recycled = `{b1..b8}` |
//! | open-time sweep | recycled = `{F}` — 1 of the 6 free-able | recycled = `{L, Q, X, Y, Z, F}`, `P` kept |
//! | recycled slot leaves the Reaped span | `in_state(Reaped)` returns the recycled slot, state `Live` | it does not |
//!
//! Each fails first at its recycled-set assertion (the third at its state assertion). A failure
//! anywhere earlier is a fixture that did not build what it says.

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

struct Fixture {
    concrete: Arc<TableBranchCatalog>,
    catalog: Arc<dyn BranchCatalog>,
    reaper: TwoTierReaper,
    _dir: tempfile::TempDir,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(true)
        .open(dir.path().join("d200.db")).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let concrete =
        Arc::new(TableBranchCatalog::open_sidecar(&dir.path().join("d200.branchcat"), 1).unwrap());
    let catalog: Arc<dyn BranchCatalog> = concrete.clone();
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog), base).unwrap());
    let reaper = TwoTierReaper::new(Arc::clone(&catalog), store);
    Fixture { concrete, catalog, reaper, _dir: dir }
}

/// Fork `n` branches off trunk and return, sorted, the ids of those that reused a slot.
fn recycled_by_forking(c: &dyn BranchCatalog, n: usize) -> Vec<u64> {
    let mut out: Vec<u64> = (0..n)
        .map(|_| c.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap().branch_id)
        .filter(|b| b.generation != 0)
        .map(|b| b.id)
        .collect();
    out.sort_unstable();
    out
}

/// A reap's state change without the reaper: what an interrupted or pre-D200 reap leaves.
fn mark_reaped(c: &dyn BranchCatalog, b: BranchId) {
    c.set_state(b, BranchState::Live, BranchState::Reaping).unwrap();
    c.set_state(b, BranchState::Reaping, BranchState::Reaped).unwrap();
}

fn window(r: &BranchRecord) -> (Epoch, Epoch) {
    (r.fork_epoch, Epoch(r.fork_epoch.0 + 1))
}

/// **The cascade.** Interiors reaped under a live leaf (the production deepest-first sweep), then
/// the leaf: the leaf's reap detaches all seven ancestors, and must release all seven.
#[test]
fn a_reaped_chain_gives_back_every_id_slot_it_held() {
    const D: usize = 8;
    let f = fixture();
    let mut chain: Vec<u64> = Vec::with_capacity(D);
    let mut parent = BranchId::TRUNK;
    for level in 1..=D {
        let lease = if level == D { LeaseDeadline(u64::MAX) } else { LeaseDeadline(100) };
        parent = f.catalog.fork(parent, lease).unwrap().branch_id;
        chain.push(parent.id);
    }
    let leaf = parent;
    let reaped = f.reaper.reap_expired(1_000).unwrap();
    assert_eq!(reaped.len(), D - 1, "fixture: the sweep did not reap every interior");
    f.reaper.reap(leaf).unwrap();
    assert!(
        !f.catalog.has_live_children(BranchId::TRUNK.id).unwrap(),
        "fixture: the leaf's cascade did not reach trunk"
    );

    let mut expected = chain.clone();
    expected.sort_unstable();
    assert_eq!(
        recycled_by_forking(&*f.catalog, D),
        expected,
        "D200: only these slots came back. Every pruned interior's slot is gone for good: the \
         cascade detached it and nothing released it"
    );
}

/// **The open-time sweep**, over every shape an earlier reap can leave a slot in.
///
/// - `L`: reaped and detached, never released — what the pre-D200 cascade left behind.
/// - `Q`: reaped, entry still under trunk — a crash between the state flip and the detach.
/// - `X → Y → Z`: a pinned chain whose leaf's reap crashed after its first detach, so `Y` and `X`
///   still hang from their parents with nothing alive below them.
/// - `P → C`: `P` reaped while `C` lives. PINNED: its slot must NOT come back.
/// - `F`: reaped by the reaper, so already free. It must come back exactly once.
#[test]
fn the_open_sweep_gives_back_leaked_slots_and_nothing_else() {
    let f = fixture();
    let c = &*f.catalog;
    let t = BranchId::TRUNK;

    let l = c.fork(t, LeaseDeadline(u64::MAX)).unwrap();
    mark_reaped(c, l.branch_id);
    assert!(c.detach_child(t.id, l.fork_epoch).unwrap(), "fixture: L had no entry to detach");

    let q = c.fork(t, LeaseDeadline(u64::MAX)).unwrap();
    mark_reaped(c, q.branch_id);

    let x = c.fork(t, LeaseDeadline(u64::MAX)).unwrap();
    let y = c.fork(x.branch_id, LeaseDeadline(u64::MAX)).unwrap();
    let z = c.fork(y.branch_id, LeaseDeadline(u64::MAX)).unwrap();
    for b in [&x, &y, &z] {
        mark_reaped(c, b.branch_id);
    }
    assert!(c.detach_child(y.branch_id.id, z.fork_epoch).unwrap(), "fixture: Z had no entry");

    let p = c.fork(t, LeaseDeadline(u64::MAX)).unwrap();
    let child = c.fork(p.branch_id, LeaseDeadline(u64::MAX)).unwrap();
    mark_reaped(c, p.branch_id);

    let free = c.fork(t, LeaseDeadline(u64::MAX)).unwrap();
    f.reaper.reap(free.branch_id).unwrap();

    let resumed = f.reaper.resume_interrupted_reaps().unwrap();
    assert!(resumed.is_empty(), "fixture: nothing was left Reaping, so nothing may be resumed");

    // IDEMPOTENT, and on a catalog with nothing to give back it WRITES NOTHING: a second open
    // issues no fsync at all. (Only `P` is left unreleased, and it is pinned.)
    let syncs = f.concrete.syncs_issued();
    assert!(f.reaper.resume_interrupted_reaps().unwrap().is_empty());
    assert_eq!(f.concrete.syncs_issued(), syncs, "a second open-time sweep wrote to the catalog");

    // The pin survives, and the live child is untouched.
    let (plo, phi) = window(&p);
    assert!(c.live_child_in_epoch_range(t.id, plo, phi).unwrap(), "the sweep unpinned P's parent");
    assert!(c.get(child.branch_id).is_ok(), "the sweep touched a live branch");

    // THE ASSERTION. Seven forks: six reuse exactly the six free-able slots, the seventh is new.
    let mut expected: Vec<u64> =
        [&l, &q, &x, &y, &z, &free].iter().map(|r| r.branch_id.id).collect();
    expected.sort_unstable();
    assert_eq!(
        recycled_by_forking(c, 7),
        expected,
        "D200: the open-time sweep did not give back exactly the leaked slots (L, Q, X, Y, Z) plus \
         the one already free (F), with P kept"
    );

    // A slot given back while its old CHILD entry still named it would, once recycled, pin its
    // OLD parent through the new branch. Q and X had such entries; they must be gone.
    for r in [&q, &x] {
        let (lo, hi) = window(r);
        assert!(
            !c.live_child_in_epoch_range(t.id, lo, hi).unwrap(),
            "slot {} was recycled with its old CHILD entry under trunk still in place",
            r.branch_id.id
        );
    }
}

/// **A recycled slot must leave the `Reaped` state span.** `fork` rewrote a recycled slot's record
/// without removing its `Reaped` STATE key, so `in_state(Reaped)` kept returning the new, LIVE
/// branch — and the open-time sweep, a range query over that span, would pay for every slot
/// ever recycled at every open.
#[test]
fn a_recycled_slot_leaves_the_reaped_span() {
    let f = fixture();
    let a = f.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
    f.reaper.reap(a.branch_id).unwrap();
    let again = f.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
    assert_eq!(again.branch_id.id, a.branch_id.id, "fixture: the slot was not recycled");

    let reaped = f.catalog.in_state(BranchState::Reaped).unwrap();
    assert!(
        reaped.iter().all(|r| r.state == BranchState::Reaped),
        "in_state(Reaped) returned a record that is not Reaped: {:?}",
        reaped.iter().map(|r| (r.branch_id, r.state)).collect::<Vec<_>>()
    );
    assert!(
        !reaped.iter().any(|r| r.branch_id.id == a.branch_id.id),
        "the recycled slot is still in the Reaped span"
    );
}
