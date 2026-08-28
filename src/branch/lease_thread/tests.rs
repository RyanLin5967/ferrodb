//! Rule tests for the lease thread. One test per rule in the module header.
//!
//! Every test here has been run against a deliberately broken copy of the rule it names and seen
//! to fail; the mutants and what each printed are recorded in
//! `scratchpad/F11-server-reaps.md` and in the summary this row files.
//!
//! **Nothing here arms the process for a cluster.** The authority is process-scoped and `cargo
//! test` runs a binary's tests as threads of one process, so a unit test that joined would make
//! `branch/reaper.rs`'s and `cow/btree.rs`'s lease-taking tests refuse in the same run —
//! `src/cluster/tests.rs` says so in its own header. The refusal rule ("never guess the time") is
//! therefore pinned in `tests/integration_server_reaps.rs`, which is its own binary.

use std::sync::atomic::AtomicU64;
use std::time::Instant;

use super::*;
use crate::branch::arena::harness::Harness;
use crate::branch::record::BranchRecord;
use crate::branch::types::{BranchState, PageId};
use crate::branch::BranchCatalog;
use crate::cow::page_header::{stamp_checksum, PageType};
use crate::cow::{PageStore, PAGE_HEADER_SIZE};
use crate::tel::MemEffectLog;

/// Long enough that no scan can fire during a test that is not about scanning.
const NEVER: Duration = Duration::from_secs(3600);
/// Short enough that a test about scanning does not have to wait.
const BRISK: Duration = Duration::from_millis(10);
/// Longest a test will wait for a thread to do something it should do promptly.
const PATIENCE: Duration = Duration::from_secs(10);

struct Fixture {
    h: Harness,
    reaper: Arc<TwoTierReaper>,
    runtime: Arc<AgentRuntime>,
}

fn fixture() -> Fixture {
    let h = Harness::new();
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            Arc::clone(&h.catalog) as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&h.store) as Arc<dyn PageStore>,
        )
        .unwrap(),
    );
    let reaper = Arc::new(TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store)));
    Fixture { h, reaper, runtime }
}

/// Allocate `pages` real pages inside `branch`'s own extent and stamp each one, the way any real
/// writer must. Lifted from `branch/reaper.rs`'s own helper rather than reinvented.
fn write_pages(f: &Fixture, branch: BranchId, pages: usize) -> Vec<PageId> {
    let arena = f.h.store.arena_for(branch).unwrap();
    let epoch = f.h.catalog.next_epoch();
    (0..pages)
        .map(|i| {
            let p = f.h.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap();
            let handle = f.h.store.read_page(p).unwrap();
            let mut frame = handle.write();
            frame.data[PAGE_HEADER_SIZE] = (i & 0xff) as u8;
            stamp_checksum(&mut frame.data);
            p
        })
        .collect()
}

/// A lease that will not expire inside a test run, without reading any clock: the fixtures must
/// stay independent of the process authority (see the module header).
const FAR_FUTURE: LeaseDeadline = LeaseDeadline(u64::MAX - 1);
/// A lease that has already expired, likewise without reading a clock.
const EXPIRED: LeaseDeadline = LeaseDeadline(0);

/// A branch holding `pages` pages of its own, and whether its lease has expired.
fn branch_with_pages(f: &Fixture, lease: LeaseDeadline, pages: usize) -> BranchId {
    let rec = f.h.catalog.fork(BranchId::TRUNK, lease).unwrap();
    write_pages(f, rec.branch_id, pages);
    rec.branch_id
}

fn state_of(f: &Fixture, branch: BranchId) -> BranchState {
    f.h.catalog.get_raw(branch.id).unwrap().state
}

/// Spin until `cond` holds, or fail naming what never happened.
fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("waited {PATIENCE:?} for {what} and it never happened");
}

/// Stands in for a `MERGE` holding the statement lock.
///
/// `entries` counts acquisitions rather than attempts, which is the number the barrier test needs:
/// a scan that ran anyway would raise it while the test holds the lock.
struct TestGate {
    statement: Mutex<()>,
    entries: AtomicU64,
}

impl TestGate {
    fn new() -> Arc<TestGate> {
        Arc::new(TestGate { statement: Mutex::new(()), entries: AtomicU64::new(0) })
    }

    fn hold(&self) -> MutexGuard<'_, ()> {
        self.statement.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn entries(&self) -> u64 {
        self.entries.load(Ordering::SeqCst)
    }
}

impl RuntimeLock for TestGate {
    fn with_runtime_lock(&self, body: &mut dyn FnMut()) {
        let _statement = self.hold();
        self.entries.fetch_add(1, Ordering::SeqCst);
        body();
    }
}

// -------------------------------------------------------------------------------------------
// RULE 2 — never reap inside a merge.
// -------------------------------------------------------------------------------------------

#[test]
fn a_scan_does_not_reap_while_a_merge_holds_the_runtime_lock() {
    let f = fixture();
    let gate = TestGate::new();
    // Far future first: the branch must become expired only after the lock is held, so the test
    // cannot pass by having reaped it before the interesting window opened.
    let doomed = branch_with_pages(&f, FAR_FUTURE, 6);
    let with_branch = f.h.store.live_page_count().unwrap();

    let lease = LeaseThread::start(
        Arc::clone(&f.reaper),
        Arc::clone(&f.runtime),
        Arc::clone(&gate) as Arc<dyn RuntimeLock>,
        BRISK,
    )
    .unwrap();

    let held = gate.hold();
    let entries_at_hold = gate.entries();
    let attempts_at_hold = lease.stats().attempts;
    f.h.catalog.renew_lease(doomed, EXPIRED).unwrap();

    // The thread is alive and *wants* to scan: it raises `attempts` before it asks for the lock.
    // Without this the assertions below would also pass against a thread that had died.
    wait_for("the scan thread to try again while the lock is held", || {
        lease.stats().attempts > attempts_at_hold
    });
    std::thread::sleep(Duration::from_millis(200));

    assert_eq!(
        gate.entries(),
        entries_at_hold,
        "a scan ran while the statement lock was held; a merge's publication can be handed pages \
         the reap just freed, and every such page still passes its checksum"
    );
    assert_eq!(lease.stats().reaped, 0, "nothing may be reaped inside the lock");
    assert_eq!(state_of(&f, doomed), BranchState::Live, "the branch must still be untouched");
    assert_eq!(
        f.h.store.live_page_count().unwrap(),
        with_branch,
        "not one page may go back while a merge could be reading it"
    );

    // And the moment the merge is over, the same expired branch goes.
    drop(held);
    wait_for("the expired branch to be reaped once the lock is free", || {
        lease.stats().reaped >= 1
    });
    assert_eq!(state_of(&f, doomed), BranchState::Reaped);
    assert!(
        f.h.store.live_page_count().unwrap() < with_branch,
        "the reap that was blocked must actually have happened once it was allowed to"
    );
}

// -------------------------------------------------------------------------------------------
// RULE 1 — resume before scanning.
// -------------------------------------------------------------------------------------------

#[test]
fn start_finishes_a_reap_a_crash_interrupted_before_any_scan_runs() {
    let f = fixture();
    let baseline = f.h.store.live_page_count().unwrap();
    // The lease has NOT expired, so a lease scan has no business touching this branch. Only the
    // resume can explain its pages coming back, which is what makes this test about the resume.
    let interrupted = branch_with_pages(&f, FAR_FUTURE, 5);
    let peak = f.h.store.live_page_count().unwrap();
    assert!(peak > baseline, "the branch must really have allocated pages");

    let mut rec: BranchRecord = f.h.catalog.get_raw(interrupted.id).unwrap();
    rec.state = BranchState::Reaping;
    f.h.catalog.put(&rec).unwrap();

    let lease = LeaseThread::start(
        Arc::clone(&f.reaper),
        Arc::clone(&f.runtime),
        Arc::clone(&TestGate::new()) as Arc<dyn RuntimeLock>,
        NEVER,
    )
    .unwrap();

    assert_eq!(
        lease.resumed(),
        &[interrupted],
        "start must report the reap it finished, not do it silently"
    );
    assert_eq!(state_of(&f, interrupted), BranchState::Reaped);
    assert_eq!(
        f.h.store.live_page_count().unwrap(),
        baseline,
        "a branch left `Reaping` by a crash keeps its extents charged to it forever unless \
         something finishes the reap"
    );
    assert_eq!(
        lease.stats().reaped,
        0,
        "the lease scan must have reaped nothing: the branch's lease had not expired, so crediting \
         the scan would mean the reaper is ignoring deadlines"
    );
}

#[test]
fn a_database_with_no_interrupted_reap_resumes_nothing() {
    // The negative control for the test above: `resumed` naming something is only evidence if it
    // can also name nothing.
    let f = fixture();
    branch_with_pages(&f, FAR_FUTURE, 3);
    let lease = LeaseThread::start(
        Arc::clone(&f.reaper),
        Arc::clone(&f.runtime),
        Arc::clone(&TestGate::new()) as Arc<dyn RuntimeLock>,
        NEVER,
    )
    .unwrap();
    assert!(lease.resumed().is_empty());
}

// -------------------------------------------------------------------------------------------
// The scan itself, and its negative control.
// -------------------------------------------------------------------------------------------

#[test]
fn the_thread_reaps_an_expired_branch_with_nobody_asking_it_to() {
    let f = fixture();
    let baseline = f.h.store.live_page_count().unwrap();
    let reserved_baseline = f.h.store.reserved_page_count();
    let doomed = branch_with_pages(&f, EXPIRED, 7);
    assert!(f.h.store.live_page_count().unwrap() > baseline, "the branch really wrote pages");

    let lease = LeaseThread::start(
        Arc::clone(&f.reaper),
        Arc::clone(&f.runtime),
        Arc::clone(&TestGate::new()) as Arc<dyn RuntimeLock>,
        BRISK,
    )
    .unwrap();

    wait_for("the background scan to reap the expired branch", || lease.stats().reaped >= 1);
    assert_eq!(state_of(&f, doomed), BranchState::Reaped);
    assert_eq!(
        f.h.store.live_page_count().unwrap(),
        baseline,
        "allocated page count must return to baseline"
    );
    assert_eq!(
        f.h.store.reserved_page_count(),
        reserved_baseline,
        "the extent must go back to the free space map, not merely stop growing"
    );
}

#[test]
fn an_unexpired_branch_survives_every_scan() {
    // The negative control that matters most: a collector which reclaimed unconditionally would
    // pass the test above and be catastrophic.
    let f = fixture();
    let kept = branch_with_pages(&f, FAR_FUTURE, 4);
    let pages = f.h.store.live_page_count().unwrap();

    let lease = LeaseThread::start(
        Arc::clone(&f.reaper),
        Arc::clone(&f.runtime),
        Arc::clone(&TestGate::new()) as Arc<dyn RuntimeLock>,
        BRISK,
    )
    .unwrap();

    // Several scans, not one: "it did not reap yet" and "it does not reap" are different claims.
    wait_for("at least four scans to complete", || lease.stats().scans >= 4);
    let stats = lease.stop();
    assert_eq!(stats.reaped, 0, "a live lease was reaped");
    assert_eq!(stats.refused, 0, "a standalone node always knows the time");
    assert_eq!(stats.failed, 0, "a healthy scan must not be erroring");
    assert_eq!(state_of(&f, kept), BranchState::Live);
    assert_eq!(f.h.store.live_page_count().unwrap(), pages);
}

#[test]
fn a_reaped_branchs_workspace_is_forgotten_without_the_client_saying_anything() {
    // `AgentRuntime::forget_reaped_branches` exists for this caller and for no other: a branch the
    // lease reaper took is reclaimed with no cooperation, so nothing tells the runtime, and its
    // workspace, name and escrow claim would sit in the map for the life of the process.
    let f = fixture();
    let session = f.runtime.begin_session("pricing-agent", Some("r_1"), BranchId::TRUNK).unwrap();
    write_pages(&f, session.branch, 3);
    f.h.catalog.renew_lease(session.branch, EXPIRED).unwrap();

    let lease = LeaseThread::start(
        Arc::clone(&f.reaper),
        Arc::clone(&f.runtime),
        Arc::clone(&TestGate::new()) as Arc<dyn RuntimeLock>,
        BRISK,
    )
    .unwrap();

    wait_for("the abandoned session to be reaped and forgotten", || {
        let s = lease.stats();
        s.reaped >= 1 && s.forgotten >= 1
    });
    assert_eq!(
        f.runtime.forget_reaped_branches(),
        0,
        "the scan must have dropped the workspace already; a second sweep finding it means the \
         scan reaped the branch and left its bookkeeping behind"
    );
}

// -------------------------------------------------------------------------------------------
// Stoppable cleanly.
// -------------------------------------------------------------------------------------------

#[test]
fn stop_does_not_wait_out_the_scan_interval() {
    let f = fixture();
    let lease = LeaseThread::start(
        Arc::clone(&f.reaper),
        Arc::clone(&f.runtime),
        Arc::clone(&TestGate::new()) as Arc<dyn RuntimeLock>,
        NEVER,
    )
    .unwrap();
    let t0 = Instant::now();
    lease.stop();
    let took = t0.elapsed();
    assert!(
        took < Duration::from_secs(5),
        "stop took {took:?} against a {NEVER:?} interval, so it is sleeping the interval out \
         rather than being woken: a server would appear to hang on shutdown"
    );
}

#[test]
fn dropping_the_lease_thread_stops_it() {
    // A `LeaseThread` that outlived its owner would be scanning a store the owner is about to
    // checkpoint and close.
    let f = fixture();
    let lease = LeaseThread::start(
        Arc::clone(&f.reaper),
        Arc::clone(&f.runtime),
        Arc::clone(&TestGate::new()) as Arc<dyn RuntimeLock>,
        BRISK,
    )
    .unwrap();
    let counters = Arc::clone(&lease.counters);
    wait_for("the thread to be scanning at all", || counters.snapshot().scans >= 2);
    drop(lease);
    let after_drop = counters.snapshot().attempts;
    std::thread::sleep(BRISK * 20);
    assert_eq!(
        counters.snapshot().attempts,
        after_drop,
        "the scan thread is still running after its owner dropped"
    );
}

// -------------------------------------------------------------------------------------------
// The interval knob refuses rather than defaulting.
// -------------------------------------------------------------------------------------------

#[test]
fn the_scan_interval_knob_refuses_every_value_it_cannot_use() {
    assert_eq!(parse_scan_interval("250").unwrap(), Duration::from_millis(250));
    assert_eq!(parse_scan_interval("  250  ").unwrap(), Duration::from_millis(250));
    for bad in ["", "abc", "-1", "1.5", "0", "off", "86400001"] {
        let err = parse_scan_interval(bad).unwrap_err().to_string();
        assert!(
            err.contains(SCAN_INTERVAL_ENV) && err.contains("Refusing"),
            "{bad:?} was accepted or refused without saying why: {err}"
        );
    }
    // The ceiling is inclusive; the value one past it is not.
    assert!(parse_scan_interval(&MAX_SCAN_MILLIS.to_string()).is_ok());
}

#[test]
fn an_unset_knob_is_the_default_and_not_a_refusal() {
    // Read through the real entry point, so the default cannot be right in the parser and wrong in
    // the wrapper. This test does not set the variable — see the module header on why.
    if std::env::var(SCAN_INTERVAL_ENV).is_err() {
        assert_eq!(
            scan_interval_from_env().unwrap(),
            Duration::from_millis(DEFAULT_SCAN_MILLIS)
        );
    }
}

// -------------------------------------------------------------------------------------------
// The contract `with_lock` enforces on a RuntimeLock implementation.
// -------------------------------------------------------------------------------------------

#[test]
#[should_panic(expected = "returned without running the lease scan")]
fn a_gate_that_skips_the_scan_is_refused_rather_than_ignored() {
    // A gate that declined to run the body would present as a database that quietly stopped
    // reaping: every counter flat, no error anywhere, pages growing. Naming it is the whole point.
    struct SkippingGate;
    impl RuntimeLock for SkippingGate {
        fn with_runtime_lock(&self, _body: &mut dyn FnMut()) {}
    }
    with_lock(&SkippingGate, || 1u8);
}
