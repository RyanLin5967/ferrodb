//! READ-VS-N arm 3 — the restart arm's two new counters, forced to move and held still
//! (`bench/read_vs_n/PREREG.md` A7 item 5).
//!
//! * `TwoTierReaper::open_sweep_visits` must equal the extents that exist, counted INDEPENDENTLY of
//!   the list the sweep iterates (`live_arenas()`): the `ArenaId`s this test's own claims returned,
//!   plus trunk's arenas as the CATALOG records them. It must NOT move when a later sweep moves the
//!   shared `sweep_visits` — that scoping is the whole reason it exists (D209: the lease thread's
//!   first pass sweeps again).
//! * `LeaseStats::finished` must rise only AFTER a pass's orphan sweep (the restart arm reads R3's
//!   first-pass visits the moment it sees `finished`), and must NOT rise for a pass that refused
//!   before that sweep (a cluster member with no `LeaseTick`), which is the path the restart arm's H5
//!   fire mode takes. PREREG A7.5, A9.5 and A11.2.
//!
//! **Why a seam and not a poll (A11.2).** A six-extent sweep takes microseconds, and a 10 ms poll
//! almost never lands inside it, so "`finished` bumped before the sweep" passed a polling version.
//! The reaper here asks its catalog through [`ParkFirstPassSweep`], which holds the lease thread's
//! first `get_raw` — the first pass's orphan sweep, on its first extent — until the test releases it.
//! While it is parked, `finished` must still be 0. That is deterministic, `tests/w4_sweep_slot_recycle.rs`'s
//! reason for the same shape.
//!
//! One test, in its own binary: the refusing half joins a cluster, which is process-wide state.

use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::lease_thread::{LeaseThread, RuntimeLock};
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::record::{BranchRecord, CapabilityEnvelope, CoreRecord};
use ferrodb::branch::types::{ArenaId, BranchId, BranchState, Epoch, LeaseDeadline, PageId};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cluster::ClusterScope;
use ferrodb::consensus::NodeId;
use ferrodb::cow::PageStore;
use ferrodb::error::FerroError;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;

/// A gate that holds nothing: nothing expires here, so no scan ever takes it.
struct NoGate;

impl RuntimeLock for NoGate {
    fn with_runtime_lock(&self, body: &mut dyn FnMut()) {
        body();
    }
}

const PATIENCE: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Park {
    Waiting,
    Parked,
    Released,
}

/// The reaper's catalog: `inner`, except that the FIRST `get_raw` made off the thread that built it
/// waits until [`Self::release`]. `LeaseThread::start` runs the open sweep on the caller's thread
/// and the passes on its own, and a pass with nothing expired reaches `get_raw` only in the orphan
/// sweep (`extent_is_collectable`), so the one call parked is the first pass's sweep, first extent.
/// The test asserts that premise (`sweep_visits == live + 1`) rather than trusting it.
///
/// The wait is bounded by `PATIENCE`, so a test that fails while the pass is parked cannot hang the
/// thread `LeaseThread::stop` joins.
struct ParkFirstPassSweep {
    /// A trait object, so every call below is the trait's method: `TableBranchCatalog` has
    /// inherent methods of the same names with other signatures (`live_count` returns a `Result`).
    inner: Arc<dyn BranchCatalog>,
    opener: ThreadId,
    park: Mutex<Park>,
    changed: Condvar,
}

impl ParkFirstPassSweep {
    fn new(inner: Arc<dyn BranchCatalog>) -> ParkFirstPassSweep {
        ParkFirstPassSweep {
            inner,
            opener: std::thread::current().id(),
            park: Mutex::new(Park::Waiting),
            changed: Condvar::new(),
        }
    }

    /// Whether the first pass reached `get_raw` within `PATIENCE`.
    fn wait_parked(&self) -> bool {
        let guard = self.park.lock().unwrap();
        let (guard, _) = self
            .changed
            .wait_timeout_while(guard, PATIENCE, |p| *p == Park::Waiting)
            .unwrap();
        *guard == Park::Parked
    }

    fn release(&self) {
        *self.park.lock().unwrap() = Park::Released;
        self.changed.notify_all();
    }
}

impl BranchCatalog for ParkFirstPassSweep {
    fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> {
        if std::thread::current().id() != self.opener {
            let mut park = self.park.lock().unwrap();
            if *park == Park::Waiting {
                *park = Park::Parked;
                self.changed.notify_all();
                let _ = self
                    .changed
                    .wait_timeout_while(park, PATIENCE, |p| *p == Park::Parked)
                    .unwrap();
            }
        }
        self.inner.get_raw(id)
    }

    // Everything else delegates, the defaulted methods included, so the reaper sees `inner`'s own
    // implementations and not the trait's generic ones.
    fn next_epoch(&self) -> Epoch {
        self.inner.next_epoch()
    }
    fn current_epoch(&self) -> Epoch {
        self.inner.current_epoch()
    }
    fn fork(&self, parent: BranchId, lease: LeaseDeadline) -> Result<BranchRecord, FerroError> {
        self.inner.fork(parent, lease)
    }
    fn fork_staged(
        &self,
        parent: BranchId,
        lease: LeaseDeadline,
    ) -> Result<(BranchRecord, Option<u64>), FerroError> {
        self.inner.fork_staged(parent, lease)
    }
    fn await_fork_durable(&self, seq: Option<u64>) -> Result<(), FerroError> {
        self.inner.await_fork_durable(seq)
    }
    fn get(&self, branch: BranchId) -> Result<BranchRecord, FerroError> {
        self.inner.get(branch)
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
    fn restrict_envelope(&self, branch: BranchId, envelope: CapabilityEnvelope) -> Result<(), FerroError> {
        self.inner.restrict_envelope(branch, envelope)
    }
    fn set_state(&self, branch: BranchId, expect: BranchState, to: BranchState) -> Result<(), FerroError> {
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
    fn scan_ids(
        &self,
        lo: u64,
        hi: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        self.inner.scan_ids(lo, hi)
    }
    fn max_live_child(&self, parent_id: u64) -> Result<Option<Epoch>, FerroError> {
        self.inner.max_live_child(parent_id)
    }
    fn live_child_in_epoch_range(&self, parent_id: u64, lo: Epoch, hi: Epoch) -> Result<bool, FerroError> {
        self.inner.live_child_in_epoch_range(parent_id, lo, hi)
    }
    fn has_live_children(&self, parent_id: u64) -> Result<bool, FerroError> {
        self.inner.has_live_children(parent_id)
    }
    fn live_count(&self) -> usize {
        self.inner.live_count()
    }
    fn release_id(&self, id: u64) {
        self.inner.release_id(id)
    }
    fn attach_child(&self, parent_id: u64, fork_epoch: Epoch, child_id: u64) -> Result<(), FerroError> {
        self.inner.attach_child(parent_id, fork_epoch, child_id)
    }
    fn detach_child(&self, parent_id: u64, fork_epoch: Epoch) -> Result<bool, FerroError> {
        self.inner.detach_child(parent_id, fork_epoch)
    }
    fn add_arena(&self, branch: BranchId, arena: ArenaId) -> Result<(), FerroError> {
        self.inner.add_arena(branch, arena)
    }
    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError> {
        self.inner.renew_lease(branch, lease)
    }
    fn envelope_of(&self, branch: BranchId) -> Result<Option<CapabilityEnvelope>, FerroError> {
        self.inner.envelope_of(branch)
    }
    fn charge_row_writes(&self, branch: BranchId, n: u64) -> Result<(), FerroError> {
        self.inner.charge_row_writes(branch, n)
    }
}

#[test]
fn the_open_sweep_count_is_its_own_and_finished_counts_only_completed_passes() {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dir.path().join("main.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog =
        Arc::new(TableBranchCatalog::open_sidecar(&dir.path().join("b.branchcat"), 1).unwrap());
    let base = bp.disk_manager.high_water().unwrap() + 1024;
    let store =
        Arc::new(ArenaPageStore::new(bp, catalog.clone() as Arc<dyn BranchCatalog>, base).unwrap());
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            catalog.clone() as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            store.clone() as Arc<dyn PageStore>,
        )
        .unwrap(),
    );
    // Live branches that each own an extent, claimed while this process is standalone (a cluster
    // member may not claim without a leader's grant). The ids the claims return are the test's own
    // record of what exists; trunk's come from the CATALOG's record, not from the store's list.
    let mut claimed: BTreeSet<_> = catalog.get(BranchId::TRUNK).unwrap().arenas.into_iter().collect();
    for _ in 0..5 {
        let b = catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap().branch_id;
        claimed.insert(store.arena_for(b).unwrap());
    }
    let live = claimed.len() as u64;
    assert!(live >= 5, "the fixture claimed {live} extents; the sweep would have nothing to visit");
    // Built on THIS thread, which `LeaseThread::start` runs the open sweep on.
    let park = Arc::new(ParkFirstPassSweep::new(catalog.clone() as Arc<dyn BranchCatalog>));
    let reaper = Arc::new(TwoTierReaper::new(park.clone() as Arc<dyn BranchCatalog>, store.clone()));

    // ---- a standalone open: the open sweep's share, then a pass that finishes ----------------
    let lease = LeaseThread::start(
        reaper.clone(),
        runtime.clone(),
        Arc::new(NoGate) as Arc<dyn RuntimeLock>,
        Duration::from_secs(86_400),
    )
    .unwrap();
    assert_eq!(reaper.open_sweep_visits(), live, "the open sweep visits every extent that exists, once");
    // ⚠ R3's BEFORE-D209 shape: the first pass sweeps every extent again. D209's fix makes the
    // first pass sweep nothing, so it never parks, and D209's own red test replaces this block
    // (PREREG A9.5, A11.2).
    assert!(park.wait_parked(), "the first lease pass never reached its orphan sweep: {:?}", lease.stats());
    // The premise: parked INSIDE the sweep, on its first extent. `sweep_visits` counts an extent
    // before asking the catalog about it, so it holds the open sweep's `live` plus this one.
    assert_eq!(
        reaper.sweep_visits(),
        live + 1,
        "parked somewhere other than the first pass's orphan sweep"
    );
    assert_eq!(
        lease.stats().finished,
        0,
        "`finished` rose while the pass was still inside its orphan sweep"
    );
    park.release();
    let deadline = Instant::now() + PATIENCE;
    while lease.stats().finished == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let first = lease.stats();
    assert!(first.finished >= 1, "a standalone pass never reached the end of scan_once: {first:?}");
    // At the first sighting, the pass's sweep is over and no third can have begun (the interval is
    // a day), so both sweeps are fully counted.
    let at_first_finish = reaper.sweep_visits();
    let done = lease.stop();
    assert_eq!(
        at_first_finish,
        2 * live,
        "at the first finished pass, the open sweep and the first pass's sweep are both complete"
    );
    assert!(done.finished <= done.attempts, "more passes finished than began: {done:?}");

    // A later sweep moves the shared count, and must not move the open's share.
    let before = reaper.sweep_visits();
    reaper.collect_orphaned_extents().unwrap();
    assert_eq!(reaper.sweep_visits() - before, live, "a full sweep visits every live extent");
    assert_eq!(
        reaper.open_sweep_visits(),
        live,
        "a sweep after the open moved the open sweep's own count"
    );

    // ---- a member with no cluster time: every pass refuses before the orphan sweep ------------
    let scope = ClusterScope::joined(NodeId(1));
    let refusing = LeaseThread::start(
        reaper.clone(),
        runtime.clone(),
        Arc::new(NoGate) as Arc<dyn RuntimeLock>,
        Duration::from_millis(20),
    )
    .unwrap();
    let deadline = Instant::now() + PATIENCE;
    while refusing.stats().refused_scans < 3 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let refused = refusing.stop();
    drop(scope);
    assert!(refused.refused_scans >= 3, "the passes did not refuse without a cluster time: {refused:?}");
    assert_eq!(
        refused.finished, 0,
        "a pass that refused before the orphan sweep was counted as finished: {refused:?}"
    );
}
