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
//!   fire mode takes. PREREG A7.5 and A9.5.
//!
//! One test, in its own binary: the refusing half joins a cluster, which is process-wide state.

use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::lease_thread::{LeaseThread, RuntimeLock};
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cluster::ClusterScope;
use ferrodb::consensus::NodeId;
use ferrodb::cow::PageStore;
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
    let reaper = Arc::new(TwoTierReaper::new(catalog.clone() as Arc<dyn BranchCatalog>, store.clone()));

    // ---- a standalone open: the open sweep's share, then a pass that finishes ----------------
    let lease = LeaseThread::start(
        reaper.clone(),
        runtime.clone(),
        Arc::new(NoGate) as Arc<dyn RuntimeLock>,
        Duration::from_secs(86_400),
    )
    .unwrap();
    assert_eq!(reaper.open_sweep_visits(), live, "the open sweep visits every extent that exists, once");
    let deadline = Instant::now() + PATIENCE;
    while lease.stats().finished == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    // `finished` rises AFTER the pass's orphan sweep. At the first sighting, that sweep is over and
    // no third can have begun (the interval is a day), so both sweeps are fully counted.
    // ⚠ R3's BEFORE-D209 shape: the first pass sweeps every extent again. D209's fix makes the
    // first pass sweep nothing, so this becomes `live` in D209's own red test (PREREG A9.5).
    let at_first_finish = reaper.sweep_visits();
    let done = lease.stop();
    assert_eq!(
        at_first_finish,
        2 * live,
        "at the first finished pass, the open sweep and the first pass's sweep are both complete"
    );
    assert!(done.finished >= 1, "a standalone pass never reached the end of scan_once: {done:?}");
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
