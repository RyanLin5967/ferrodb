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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::record::{BranchRecord, CapabilityEnvelope, CoreRecord};
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{ArenaId, BranchError, BranchId, BranchState, Epoch, LeaseDeadline, PageId};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::error::FerroError;
use ferrodb::storage::disk_manager::DiskManager;

struct Fixture {
    concrete: Arc<TableBranchCatalog>,
    catalog: Arc<dyn BranchCatalog>,
    store: Arc<ArenaPageStore>,
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
    let reaper = TwoTierReaper::new(Arc::clone(&catalog), Arc::clone(&store));
    Fixture { concrete, catalog, store, reaper, _dir: dir }
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

    let keys_before = f.concrete.candidate_keys_scanned();
    let resumed = f.reaper.resume_interrupted_reaps().unwrap();
    assert!(resumed.is_empty(), "fixture: nothing was left Reaping, so nothing may be resumed");
    // F3 (review audit): the POSITIVE control for the grid test's `== 0`. L, Q and Z are keyed;
    // X, Y and P lost their keys at their pinned flips; F was released. So the candidate query
    // reads exactly 3 keys, which also proves the instrument counts (lane §8.7).
    assert_eq!(
        f.concrete.candidate_keys_scanned() - keys_before,
        3,
        "the open sweep's candidate query did not read exactly the three keyed slots L, Q and Z"
    );

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

/// Keys the open sweep's candidate query reads on a HEALTHY catalog: `released` reaped-and-freed
/// leaves (forked first, so each holds its own slot), plus a chain of `pinned` reaped interiors
/// above one live leaf, reaped deepest first as the lease scan orders them. Nothing here is
/// releasable, so a sweep that pays only for releasable slots reads nothing.
fn keys_read_by_a_healthy_open(released: usize, pinned: usize) -> u64 {
    let f = fixture();
    let c = &*f.catalog;
    let leaves: Vec<BranchId> = (0..released)
        .map(|_| c.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap().branch_id)
        .collect();
    for b in leaves {
        f.reaper.reap(b).unwrap();
    }
    let mut chain = Vec::with_capacity(pinned);
    let mut parent = BranchId::TRUNK;
    for _ in 0..pinned {
        parent = c.fork(parent, LeaseDeadline(u64::MAX)).unwrap().branch_id;
        chain.push(parent);
    }
    let _leaf = c.fork(parent, LeaseDeadline(u64::MAX)).unwrap();
    for b in chain.iter().rev() {
        f.reaper.reap(*b).unwrap();
    }
    assert!(
        f.catalog.has_live_children(BranchId::TRUNK.id).unwrap(),
        "fixture: the leaf must still pin the chain"
    );

    let before = f.concrete.candidate_keys_scanned();
    assert!(f.reaper.resume_interrupted_reaps().unwrap().is_empty());
    assert_eq!(f.reaper.open_slots_reclaimed(), 0, "fixture: a healthy catalog had a leak");
    f.concrete.candidate_keys_scanned() - before
}

/// **The open sweep pays for RELEASABLE slots only — not for released ones, and not for pinned.**
///
/// A released slot stays a `Reaped` record until a fork recycles it, and a pinned reaped interior
/// stays unreleased for as long as anything below it lives — under MCTS pruning, up to live
/// branches × chain depth. Neither can be released at this open, and this sweep runs under the
/// statement lock at start (`LeaseThread::start` → `resume_interrupted_reaps`), so a candidate
/// query that reads either pays a branch-count wall at every open.
///
/// PRE-REGISTRATION, amended append-only (`lane_wall21_reap_walk.md` §8.6). The first version of
/// this test (`0928923`, released axis only) predicted 17/129 at `0928923` and 1/1 at `17cbd4c`,
/// where the span still held the pinned interior. This version adds the pinned axis. Predicted at
/// `17cbd4c`: one key per pinned interior, so it **fails at the first cell, (8, 1), reading 1**.
/// Predicted after the fix: **0 in every cell**.
#[test]
fn a_healthy_open_reads_no_released_and_no_pinned_slot() {
    for (released, pinned) in [(8, 1), (64, 1), (8, 8), (64, 8)] {
        let read = keys_read_by_a_healthy_open(released, pinned);
        eprintln!("d200 open sweep keys read: released={released} pinned={pinned} -> {read}");
        assert_eq!(
            read, 0,
            "a healthy open read {read} candidate keys with {released} released and {pinned} pinned \
             slots; none of them can be released now, so every key read is the wall this removes"
        );
    }
}

/// A catalog that fails ONE operation on purpose, so a cascade is left in the state a crash or an
/// I/O error leaves it in. Everything else is delegated untouched, including
/// `unreleased_reaped_candidates` (W5, wall21 review audit 3): before, the trait's DEFAULT
/// answered through this wrapper (every `Reaped` record), so a sweep run through it used a
/// different candidate list from production's UNRELEASED span. Not forwarded: the defaults the
/// reaper and the page store never call (`await_fork_durable`, `scan_ids`, `envelope_of`).
struct Faulty {
    inner: Arc<dyn BranchCatalog>,
    /// Fail the Nth `detach_child` (1-based); 0 = never.
    fail_detach_at: u64,
    detaches: AtomicU64,
    /// Fail the next `get_raw` of this id, once; `u64::MAX` = never.
    fail_get_raw_of: AtomicU64,
    /// PANIC on the Nth `release_id` (1-based); 0 = never. A panic, not an error: `release_id`
    /// returns `()`, and a panic caught by the test is the closest thing to a crash between two
    /// releases, because nothing after it in the reap runs.
    panic_release_at: u64,
    releases: AtomicU64,
}

fn injected(what: &str) -> FerroError {
    BranchError::Corrupt(format!("injected failure: {what}")).into()
}

impl BranchCatalog for Faulty {
    fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> {
        if self
            .fail_get_raw_of
            .compare_exchange(id, u64::MAX, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(injected("get_raw"));
        }
        self.inner.get_raw(id)
    }
    fn detach_child(&self, parent_id: u64, fork_epoch: Epoch) -> Result<bool, FerroError> {
        let n = self.detaches.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.fail_detach_at {
            return Err(injected("detach_child"));
        }
        self.inner.detach_child(parent_id, fork_epoch)
    }
    fn get(&self, branch: BranchId) -> Result<BranchRecord, FerroError> {
        self.inner.get(branch)
    }
    fn next_epoch(&self) -> Epoch {
        self.inner.next_epoch()
    }
    fn current_epoch(&self) -> Epoch {
        self.inner.current_epoch()
    }
    fn fork(&self, parent: BranchId, lease: LeaseDeadline) -> Result<BranchRecord, FerroError> {
        self.inner.fork(parent, lease)
    }
    fn reparent(
        &self,
        branch: BranchId,
        parent: BranchId,
        fork_epoch: Epoch,
        root: PageId,
    ) -> Result<BranchRecord, FerroError> {
        self.inner.reparent(branch, parent, fork_epoch, root)
    }
    fn restrict_envelope(
        &self,
        branch: BranchId,
        envelope: CapabilityEnvelope,
    ) -> Result<(), FerroError> {
        self.inner.restrict_envelope(branch, envelope)
    }
    fn set_state(
        &self,
        branch: BranchId,
        expect: BranchState,
        to: BranchState,
    ) -> Result<(), FerroError> {
        self.inner.set_state(branch, expect, to)
    }
    fn set_root(&self, branch: BranchId, root: PageId) -> Result<(), FerroError> {
        self.inner.set_root(branch, root)
    }
    fn expired_before(&self, now_millis: u64) -> Result<Vec<CoreRecord>, FerroError> {
        self.inner.expired_before(now_millis)
    }
    fn in_state(&self, state: BranchState) -> Result<Vec<BranchRecord>, FerroError> {
        self.inner.in_state(state)
    }
    fn scan(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        self.inner.scan()
    }
    fn max_live_child(&self, parent_id: u64) -> Result<Option<Epoch>, FerroError> {
        self.inner.max_live_child(parent_id)
    }
    fn live_child_in_epoch_range(
        &self,
        parent_id: u64,
        lo: Epoch,
        hi: Epoch,
    ) -> Result<bool, FerroError> {
        self.inner.live_child_in_epoch_range(parent_id, lo, hi)
    }
    fn has_live_children(&self, parent_id: u64) -> Result<bool, FerroError> {
        self.inner.has_live_children(parent_id)
    }
    fn live_count(&self) -> usize {
        self.inner.live_count()
    }
    fn release_id(&self, id: u64) {
        let n = self.releases.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.panic_release_at {
            panic!("injected crash between two releases (release_id call {n}, slot {id})");
        }
        self.inner.release_id(id)
    }
    fn unreleased_reaped_candidates(&self) -> Result<Vec<u64>, FerroError> {
        self.inner.unreleased_reaped_candidates()
    }
    fn attach_child(
        &self,
        parent_id: u64,
        fork_epoch: Epoch,
        child_id: u64,
    ) -> Result<(), FerroError> {
        self.inner.attach_child(parent_id, fork_epoch, child_id)
    }
    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError> {
        self.inner.renew_lease(branch, lease)
    }
    fn charge_row_writes(&self, branch: BranchId, rows: u64) -> Result<(), FerroError> {
        self.inner.charge_row_writes(branch, rows)
    }
    fn add_arena(&self, branch: BranchId, arena: ArenaId) -> Result<(), FerroError> {
        self.inner.add_arena(branch, arena)
    }
}

fn faulty_over(f: &Fixture, fail_detach_at: u64) -> Arc<Faulty> {
    Arc::new(Faulty {
        inner: Arc::clone(&f.catalog),
        fail_detach_at,
        detaches: AtomicU64::new(0),
        fail_get_raw_of: AtomicU64::new(u64::MAX),
        panic_release_at: 0,
        releases: AtomicU64::new(0),
    })
}

fn reaper_through(f: &Fixture, faulty: &Arc<Faulty>) -> TwoTierReaper {
    TwoTierReaper::new(Arc::clone(faulty) as Arc<dyn BranchCatalog>, Arc::clone(&f.store))
}

/// T → Q → P → L, with P then Q reaped (both pinned by L) through a reaper over `faulty`, which
/// has not fired yet: a pinned reap stops before any detach. Returns (Q, P, L).
fn pinned_pair_over(f: &Fixture, faulty: &Arc<Faulty>) -> (BranchId, BranchId, BranchId) {
    let c = &*f.catalog;
    let q = c.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap().branch_id;
    let p = c.fork(q, LeaseDeadline(u64::MAX)).unwrap().branch_id;
    let l = c.fork(p, LeaseDeadline(u64::MAX)).unwrap().branch_id;
    let through = reaper_through(f, faulty);
    through.reap(p).unwrap();
    through.reap(q).unwrap();
    assert_eq!(faulty.detaches.load(Ordering::SeqCst), 0, "fixture: a pinned reap detached something");
    (q, p, l)
}

/// The ids among `forks` (already made) plus `more` further forks off trunk that reused a slot.
fn recycled_among(c: &dyn BranchCatalog, forks: Vec<BranchId>, more: usize) -> Vec<u64> {
    let mut all = forks;
    for _ in 0..more {
        all.push(c.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap().branch_id);
    }
    let mut out: Vec<u64> = all.into_iter().filter(|b| b.generation != 0).map(|b| b.id).collect();
    out.sort_unstable();
    out
}

/// **F1 (review audit), schedule 1: a cascade that fails part-way, then a fork that recycles a slot
/// the cascade had already released, then the open sweep.** The failure stands in for a crash at
/// the same point. At `23d8e97` the cascade released P before its third detach failed, the fork
/// recycles P, the sweep's re-walk from L stops at the recycled P, and Q is stranded: `Reaped`,
/// unreleased, with no key. PRE-REGISTERED (lane §8.7): `{L, P}` at `23d8e97`, which fails here,
/// and `{L, P, Q}` after the fix.
#[test]
fn a_cascade_that_fails_part_way_strands_no_ancestor() {
    let f = fixture();
    let faulty = faulty_over(&f, 3);
    let (q, p, l) = pinned_pair_over(&f, &faulty);
    assert!(
        reaper_through(&f, &faulty).reap(l).is_err(),
        "fixture: the third detach was meant to fail the leaf's reap"
    );

    // A fork before any sweep: whatever the failed cascade already released can be recycled now.
    let first = f.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap().branch_id;
    let fresh = TwoTierReaper::new(Arc::clone(&f.catalog), Arc::clone(&f.store));
    assert!(fresh.resume_interrupted_reaps().unwrap().is_empty());

    let mut want = vec![q.id, p.id, l.id];
    want.sort_unstable();
    assert_eq!(
        recycled_among(&*f.catalog, vec![first], 3),
        want,
        "F1: a slot the failed cascade had released was recycled, the sweep's walk stopped there, \
         and an ancestor above it was stranded with no key"
    );
}

/// **F1 (review audit), schedule 2: a cascade that cannot read a parent.** At `23d8e97` a `get_raw`
/// error was taken as "the parent is not Reaped", the cascade returned `Ok`, and the reap released
/// the originator. That removed the only key while P and Q were still unreleased.
/// PRE-REGISTERED (lane §8.7): `{L}` at `23d8e97`, which fails here, and `{L, P, Q}` after the fix.
#[test]
fn a_cascade_that_cannot_read_a_parent_fails_instead_of_releasing_the_originator() {
    let f = fixture();
    let faulty = faulty_over(&f, 0);
    let (q, p, l) = pinned_pair_over(&f, &faulty);
    // Armed only now: P's own reap read P's record, and that read must not be the one that fails.
    faulty.fail_get_raw_of.store(p.id, Ordering::SeqCst);
    eprintln!("F1 schedule 2: reap(L) -> {:?}", reaper_through(&f, &faulty).reap(l).map(|_| ()));
    assert_eq!(
        faulty.fail_get_raw_of.load(Ordering::SeqCst),
        u64::MAX,
        "fixture: the injected get_raw never fired"
    );

    let fresh = TwoTierReaper::new(Arc::clone(&f.catalog), Arc::clone(&f.store));
    assert!(fresh.resume_interrupted_reaps().unwrap().is_empty());
    let mut want = vec![q.id, p.id, l.id];
    want.sort_unstable();
    assert_eq!(
        recycled_among(&*f.catalog, Vec::new(), 3),
        want,
        "F1: a parent read that failed mid-cascade let the reap release the originator and remove \
         the only key, stranding every reaped ancestor above it"
    );
}

/// **M28 killed: the release ORDER is the crash-safety argument, so it is tested.** A crash between
/// two of the cascade's releases must strand nothing.
///
/// Top-down (§8.8), the released slots are the TOP ones, so the originator's re-walk climbs
/// through the unreleased lower ones and releases them. Bottom-up (M28), or inside the loop (M26),
/// the LOWER ones are released and recycled, the re-walk stops at the first recycled record, and
/// the upper ones are stranded: `Reaped`, unreleased, with no key.
///
/// The crash is a panic in the 3rd `release_id`, caught by the test. The reap never finishes, so
/// the originator keeps its key, exactly as after a real crash there. PRE-REGISTERED (lane §8.9):
/// stranded = `[]` at `e8909fa`; `[A1, A2]` under M28 and under M26.
#[test]
fn a_crash_between_two_releases_strands_no_ancestor() {
    let f = fixture();
    let c = &*f.catalog;
    // T → A1 → A2 → A3 → A4 → L: four reaped interiors (D ≥ 4), k = 2 releases before the crash.
    let mut chain: Vec<BranchId> = Vec::new();
    let mut parent = BranchId::TRUNK;
    for _ in 0..5 {
        parent = c.fork(parent, LeaseDeadline(u64::MAX)).unwrap().branch_id;
        chain.push(parent);
    }
    let l = chain[4];
    let ancestors: Vec<u64> = chain[..4].iter().map(|b| b.id).collect();
    // Pinned reaps through the INNER catalog, so the wrapper counts only the leaf's releases.
    for b in chain[..4].iter().rev() {
        f.reaper.reap(*b).unwrap();
    }

    let faulty = Arc::new(Faulty {
        inner: Arc::clone(&f.catalog),
        fail_detach_at: 0,
        detaches: AtomicU64::new(0),
        fail_get_raw_of: AtomicU64::new(u64::MAX),
        panic_release_at: 3,
        releases: AtomicU64::new(0),
    });
    let through = reaper_through(&f, &faulty);
    let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| through.reap(l)));
    assert!(crashed.is_err(), "fixture: the 3rd release was meant to crash the leaf's reap");

    // PREMISE: a fork before the sweep recycles exactly the two slots the cascade released.
    let recycled: Vec<BranchId> = (0..2)
        .map(|_| c.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap().branch_id)
        .collect();
    for b in &recycled {
        assert!(
            b.generation != 0 && ancestors.contains(&b.id),
            "fixture: fork {b:?} did not reuse a slot the crashed cascade had released"
        );
    }

    let fresh = TwoTierReaper::new(Arc::clone(&f.catalog), Arc::clone(&f.store));
    assert!(fresh.resume_interrupted_reaps().unwrap().is_empty());
    // Drain whatever the sweep freed, so a slot still `Reaped` now is one nothing released.
    for _ in 0..8 {
        c.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
    }

    let keyed = c.unreleased_reaped_candidates().unwrap();
    let stranded: Vec<u64> = chain
        .iter()
        .map(|b| b.id)
        .filter(|id| c.get_raw(*id).unwrap().state == BranchState::Reaped && !keyed.contains(id))
        .collect();
    assert!(
        stranded.is_empty(),
        "M28/M26: a crash between two releases stranded {stranded:?}: Reaped, unreleased and with no \
         key, above a released slot that a fork recycled before the sweep"
    );
}

/// **C2a (wall21 review audit 2): one resumed reap's error must not stop an open.**
/// `resume_interrupted_reaps` ran `self.reap(b)?`, so a `Branch` error inside a resumed reap's
/// cascade failed the whole open: the CLI returns it, and pgserver panics. D127's rule, which the
/// open sweep already follows, is that one slot the catalog cannot answer for must not cost every
/// other slot its reclamation. It is a counted refusal with its reason. PRE-REGISTERED (lane
/// §8.10): fails at "the open is Ok" at `fd7b5e0`, passes after the fix.
#[test]
fn a_resumed_reap_that_errs_does_not_fail_the_open() {
    let f = fixture();
    let c = &*f.catalog;
    let q = c.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap().branch_id;
    let p = c.fork(q, LeaseDeadline(u64::MAX)).unwrap().branch_id;
    let l = c.fork(p, LeaseDeadline(u64::MAX)).unwrap().branch_id;
    f.reaper.reap(p).unwrap();
    f.reaper.reap(q).unwrap();
    // What a crash mid-reap leaves: L durably `Reaping`.
    c.set_state(l, BranchState::Live, BranchState::Reaping).unwrap();

    let faulty = faulty_over(&f, 0);
    faulty.fail_get_raw_of.store(p.id, Ordering::SeqCst);
    let opener = reaper_through(&f, &faulty);
    let opened = opener.resume_interrupted_reaps();
    assert_eq!(
        faulty.fail_get_raw_of.load(Ordering::SeqCst),
        u64::MAX,
        "fixture: the injected get_raw never fired"
    );
    let resumed = opened.expect(
        "C2a: one resumed reap's catalog error failed the whole open; it must be a counted refusal",
    );
    assert!(resumed.is_empty(), "the refused reap was reported as resumed");
    assert_eq!(opener.refused_reaps(), 1, "the refusal was not counted");
    // W4 (wall21 review audit 3): the refusal is kept WITH its reason, and the sweep does not
    // overwrite it. Dropping the `extend` (M40), or moving it above the sweep, whose list assignment
    // then erases it (M41), fails here.
    let why = opener.open_slot_refusals();
    assert_eq!(why.len(), 1, "exactly the resumed reap's refusal was expected: {why:?}");
    assert!(
        why[0].starts_with(&format!("resumed reap of b{}@", l.id)),
        "the refusal does not name the resumed reap of L: {why:?}"
    );
    assert!(
        why[0].contains("injected failure: get_raw"),
        "the refusal lost the catalog's reason: {why:?}"
    );

    // The same open's sweep finishes what the refused reap started.
    let mut want = vec![q.id, p.id, l.id];
    want.sort_unstable();
    assert_eq!(recycled_among(c, Vec::new(), 3), want, "the open left part of the chain unreleased");
}
