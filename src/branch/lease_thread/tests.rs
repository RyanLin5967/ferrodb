//! Rule tests for the lease thread. One test per rule in the module header.
//!
//! Every test here has been run against a deliberately broken copy of the rule it names and seen
//! to fail; the mutants and what each printed are recorded in
//! `bench/evidence/F11-server-reaps.md` and in the summary this row files.
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
use crate::branch::record::{BranchRecord, CapabilityEnvelope, CoreRecord};
use crate::branch::types::{ArenaId, BranchError, BranchState, Epoch, PageId};
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

/// Allocate `pages` real pages inside `branch`'s own extents and stamp each one, the way any real
/// writer must. Lifted from `branch/reaper.rs`'s own helper rather than reinvented.
///
/// **D31 — `alloc_for`, not a captured `ArenaId`.** Same change as the reaper's copy, for the same
/// reason: a branch's first extent is one page now, so filling a captured arena refuses on the
/// second allocation.
fn write_pages(f: &Fixture, branch: BranchId, pages: usize) -> Vec<PageId> {
    let epoch = f.h.catalog.next_epoch();
    (0..pages)
        .map(|i| {
            let p = f.h.store.alloc_for(branch, PageType::BTreeLeaf, epoch).unwrap();
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

    let rec: BranchRecord = f.h.catalog.get_raw(interrupted.id).unwrap();
    f.h.catalog.set_state(rec.branch_id, rec.state, BranchState::Reaping).unwrap();

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

/// Wait up to [`PATIENCE`] for `flag`, without ever joining the thread that sets it.
///
/// **Never `join`.** The defect these two tests exist to catch — a `thread::sleep` where the
/// condition variable should be — leaves the scan thread parked for a whole interval, and a `join`
/// against that parks the *test* for a whole interval too. A test that hangs instead of failing
/// reports nothing and takes the suite with it, so the wait is a poll on an atomic and the thread
/// is abandoned to the end of the process.
fn wait_for_flag(what: &str, flag: &std::sync::atomic::AtomicBool) -> Duration {
    let t0 = Instant::now();
    while t0.elapsed() < PATIENCE {
        if flag.load(Ordering::SeqCst) {
            return t0.elapsed();
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("{what} had not happened {PATIENCE:?} after it was signalled");
}

#[test]
fn a_signalled_halt_returns_at_once_instead_of_sleeping_the_interval() {
    // The rule, at the level it lives: `Halt::wait` must be *woken*, not time out. Tested here and
    // not only through `stop()` because `stop()` signals and then joins, and a signal that lands
    // before the thread has reached the wait is caught by the early flag check — so an
    // implementation that sleeps the interval out can win that race and pass. It did: this test
    // replaced one that a `thread::sleep` mutant survived.
    let halt = Arc::new(Halt { stopping: Mutex::new(false), wake: Condvar::new() });
    let returned = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let parked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let halt = Arc::clone(&halt);
        let returned = Arc::clone(&returned);
        let parked = Arc::clone(&parked);
        std::thread::spawn(move || {
            parked.store(true, Ordering::SeqCst);
            halt.wait(NEVER);
            returned.store(true, Ordering::SeqCst);
        });
    }
    // Parked for certain: the flag says the thread has been scheduled, and the sleep covers the few
    // instructions between the flag and the wait.
    wait_for_flag("the waiter to start", &parked);
    std::thread::sleep(Duration::from_millis(250));
    assert!(!returned.load(Ordering::SeqCst), "the wait returned before it was signalled");

    halt.signal();
    let took = wait_for_flag("the signalled wait to return", &returned);
    assert!(
        took < Duration::from_secs(5),
        "a signalled wait took {took:?} against a {NEVER:?} interval, so it is sleeping the \
         interval out rather than being woken: a server would appear to hang on shutdown"
    );
}

#[test]
fn stop_does_not_wait_out_the_scan_interval() {
    // The same rule through the public API, and with the same no-join discipline: `stop()` runs on
    // a helper thread so that an implementation which parks cannot park this test with it.
    let f = fixture();
    let lease = LeaseThread::start(
        Arc::clone(&f.reaper),
        Arc::clone(&f.runtime),
        Arc::clone(&TestGate::new()) as Arc<dyn RuntimeLock>,
        NEVER,
    )
    .unwrap();
    // Parked in the wait, not still scanning: otherwise a prompt `stop` proves only that the flag
    // was already set before the thread looked at it.
    wait_for("the first scan to finish", || lease.stats().scans >= 1);
    std::thread::sleep(Duration::from_millis(250));

    let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&stopped);
    std::thread::spawn(move || {
        lease.stop();
        flag.store(true, Ordering::SeqCst);
    });
    let took = wait_for_flag("stop() to return", &stopped);
    assert!(
        took < Duration::from_secs(5),
        "stop took {took:?} against a {NEVER:?} interval"
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

// -------------------------------------------------------------------------------------------
// D88 — the orphan sweep must NOT run inside the statement lock.
// -------------------------------------------------------------------------------------------

/// A lock that reports how many arenas the sweep visited **while it was held**.
///
/// `sweep_visits` is incremented once per arena examined by `collect_orphaned_extents` and by
/// `sweep_touched_extents`, so sampling it either side of the body measures exactly the work that
/// happened under the statement mutex.
struct WatchingGate {
    statement: Mutex<()>,
    reaper: Mutex<Option<Arc<TwoTierReaper>>>,
    visits_inside: AtomicU64,
}

impl RuntimeLock for WatchingGate {
    fn with_runtime_lock(&self, body: &mut dyn FnMut()) {
        let _statement = self.statement.lock().unwrap_or_else(PoisonError::into_inner);
        let before = self
            .reaper
            .lock()
            .unwrap()
            .as_ref()
            .map(|r| r.sweep_visits())
            .unwrap_or(0);
        body();
        let after = self
            .reaper
            .lock()
            .unwrap()
            .as_ref()
            .map(|r| r.sweep_visits())
            .unwrap_or(0);
        self.visits_inside.fetch_add(after - before, Ordering::SeqCst);
    }
}

/// **D88.** The O(live arenas) orphan sweep ran inside `with_lock`, and in production that lock is
/// the table-catalog mutex every SQL statement takes — so every connection stalled behind it once
/// a minute. This pins that it now runs after the lock is released.
///
/// It refuses to pass vacuously: the sweep must actually have run (visits outside > 0), so a
/// version that simply stopped sweeping would fail rather than look fixed.
#[test]
fn d88_the_orphan_sweep_does_not_run_inside_the_statement_lock() {
    let f = fixture();
    // Something for the sweep to find, so "zero visits inside" is not zero visits anywhere.
    let dead = f.h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
    write_pages(&f, dead.branch_id, 2);
    f.h.catalog
        .set_state(dead.branch_id, BranchState::Live, BranchState::Reaped)
        .unwrap();

    let gate = Arc::new(WatchingGate {
        statement: Mutex::new(()),
        reaper: Mutex::new(Some(Arc::clone(&f.reaper))),
        visits_inside: AtomicU64::new(0),
    });
    let counters = Counters::default();
    let before_total = f.reaper.sweep_visits();

    scan_once(&f.reaper, &f.runtime, &*gate, &counters);

    let inside = gate.visits_inside.load(Ordering::SeqCst);
    let total = f.reaper.sweep_visits() - before_total;
    assert!(
        total > 0,
        "the sweep visited no arenas at all, so this proves nothing about where it ran"
    );
    assert_eq!(
        inside, 0,
        "{inside} of {total} arena visits happened INSIDE the statement lock. In production that \
         lock is the table-catalog mutex, so every connection in the database stalls for the \
         duration of an O(live arenas) scan."
    );
}

// -------------------------------------------------------------------------------------------
// D98 — the statement lock must not be held across a whole sweep.
// -------------------------------------------------------------------------------------------

/// A lock that reports the **largest number of branches reaped inside one acquisition**.
///
/// The property itself, not a proxy for it. Sampling how many records are `Reaped` either side of
/// the body measures exactly the destructive work that happened while every SQL statement in the
/// database was blocked — which is the quantity D98 is about, and which a wall-clock assertion
/// could not state without being a timing test.
struct ChunkGate {
    statement: Mutex<()>,
    catalog: Arc<dyn BranchCatalog>,
    /// Most reaps seen inside a single acquisition.
    worst: AtomicU64,
    /// How many times the lock was taken at all.
    acquisitions: AtomicU64,
    /// Run after the body of each acquisition, so a test can move the world between chunks.
    after_body: Mutex<Option<Box<dyn FnMut(u64) + Send>>>,
}

impl ChunkGate {
    fn new(catalog: Arc<dyn BranchCatalog>) -> Arc<ChunkGate> {
        Arc::new(ChunkGate {
            statement: Mutex::new(()),
            catalog,
            worst: AtomicU64::new(0),
            acquisitions: AtomicU64::new(0),
            after_body: Mutex::new(None),
        })
    }

    fn reaped_now(&self) -> u64 {
        self.catalog.in_state(BranchState::Reaped).map(|v| v.len() as u64).unwrap_or(0)
    }
}

impl RuntimeLock for ChunkGate {
    fn with_runtime_lock(&self, body: &mut dyn FnMut()) {
        let _statement = self.statement.lock().unwrap_or_else(PoisonError::into_inner);
        let before = self.reaped_now();
        body();
        let after = self.reaped_now();
        let n = self.acquisitions.fetch_add(1, Ordering::SeqCst);
        self.worst.fetch_max(after.saturating_sub(before), Ordering::SeqCst);
        if let Some(f) = self.after_body.lock().unwrap().as_mut() {
            f(n);
        }
    }
}

/// **D98.** `scan_once` held ONE acquisition of the runtime lock across every reap on the tick, and
/// in production that lock is the mutex every SQL statement takes — so a statement waited for the
/// whole sweep, whose length is a function of how many branches expired. Measured before the fix
/// at 1 000 branches with 64 expired: 3.71 s at the median (`bench/d98_outer_runtime_lock.txt`).
///
/// This pins the shape rather than the constant: whatever [`REAP_CHUNK`] is, one acquisition must
/// never contain more reaps than it, however many branches expire at once.
///
/// **It cannot pass vacuously.** The tick must still reap every expired branch, and the lock must
/// still have been taken more than once — a version that stopped reaping, or one that reaped
/// everything without ever taking the lock, fails here rather than looking fixed.
#[test]
fn d98_one_acquisition_never_reaps_more_than_a_chunk() {
    let f = fixture();
    // Deliberately not a multiple of the chunk, so an off-by-one in the last group shows up, and
    // never fewer than a dozen: the assertion is that MANY expiries do not become one long hold,
    // and a fixture with three branches in it cannot distinguish that from anything.
    let n = (REAP_CHUNK * 5 + 3).max(12);
    for _ in 0..n {
        branch_with_pages(&f, EXPIRED, 1);
    }

    let gate = ChunkGate::new(Arc::clone(&f.h.catalog) as Arc<dyn BranchCatalog>);
    let counters = Counters::default();
    scan_once(&f.reaper, &f.runtime, &*gate, &counters);

    let stats = counters.snapshot();
    assert_eq!(
        stats.reaped, n as u64,
        "the tick reaped {} of {n} expired branches, so this proves nothing about how the lock \
         was held — a sweep that stopped sweeping must fail here, not pass",
        stats.reaped
    );
    let worst = gate.worst.load(Ordering::SeqCst);
    let taken = gate.acquisitions.load(Ordering::SeqCst);
    assert!(
        taken > 1,
        "the lock was taken {taken} time(s) for {n} reaps; bounding the hold means taking it \
         repeatedly, and one acquisition for the whole sweep is the defect itself"
    );
    assert!(
        worst <= REAP_CHUNK as u64,
        "{worst} branches were reaped inside ONE acquisition of the statement lock, against a \
         chunk of {REAP_CHUNK}. In production that lock is the per-statement mutex, so every \
         connection in the database stalls for all {worst} durable reaps — and that number grows \
         with the number of agents, which is the wall BranchBench reports across every branchable \
         DBMS it measured."
    );
}

/// **D98.** The candidate query now runs OUTSIDE the lock, which opens a window the lock used to
/// close: a keepalive can land between "which branches have expired" and "reap this one".
///
/// `reap_if_still_expired` re-reads each record inside the acquisition that is about to reap it,
/// so a branch renewed in that window is skipped. This drives exactly that: the first chunk reaps
/// normally, then every branch still alive is renewed, and nothing further may be reaped — even
/// though all of them were on the candidate list when it was built.
///
/// Without the re-check every branch is reaped and an agent that was still working loses its
/// branch, which a `BranchId` generation makes unrecoverable. That is why this is a test and not a
/// comment.
#[test]
fn d98_a_branch_renewed_after_the_query_is_not_reaped() {
    let f = fixture();
    let n = (REAP_CHUNK * 4).max(8);
    let all: Vec<BranchId> = (0..n).map(|_| branch_with_pages(&f, EXPIRED, 1)).collect();

    let gate = ChunkGate::new(Arc::clone(&f.h.catalog) as Arc<dyn BranchCatalog>);
    {
        // Renew everything still Live once the FIRST chunk has been reaped. The candidate list was
        // built before any of this, so every one of them is on it.
        let catalog = Arc::clone(&f.h.catalog);
        let all = all.clone();
        *gate.after_body.lock().unwrap() = Some(Box::new(move |acquisition| {
            if acquisition != 0 {
                return;
            }
            for b in &all {
                if catalog.get_raw(b.id).map(|r| r.state) == Ok(BranchState::Live) {
                    catalog.renew_lease(*b, FAR_FUTURE).unwrap();
                }
            }
        }));
    }

    let counters = Counters::default();
    scan_once(&f.reaper, &f.runtime, &*gate, &counters);

    let stats = counters.snapshot();
    assert_eq!(
        stats.reaped, REAP_CHUNK as u64,
        "the first chunk reaps {REAP_CHUNK} branches and the renewal must save the rest; {} were \
         reaped instead. More than {REAP_CHUNK} means a renewed lease was ignored — an agent that \
         was still working lost its branch, and the generation makes that unrecoverable.",
        stats.reaped
    );
    let survivors = all
        .iter()
        .filter(|b| f.h.catalog.get_raw(b.id).map(|r| r.state) == Ok(BranchState::Live))
        .count();
    assert_eq!(
        survivors,
        n - REAP_CHUNK,
        "expected the {} renewed branches to survive, found {survivors}",
        n - REAP_CHUNK
    );
}

// -------------------------------------------------------------------------------------------
// D127 — a REFUSED reap must be counted and reported, distinctly from "not expired".
// -------------------------------------------------------------------------------------------
//
// The chain this closes, all three links read from source at `ac2ab26`:
//
// 1. `From<BranchError> for FerroError` collapses every variant into `FerroError::Branch(String)`,
//    so `Corrupt` and `NotFound` are the same thing to a `match`.
// 2. `reap_if_still_expired` turned every `Err(FerroError::Branch(_))` into `Ok(false)`.
// 3. `scan_once` matched `Ok(false) => {}` — not pushed to `reaped`, not counted, never reported.
//
// So D124's refusal text — which names the parent, the fork epoch and the child on purpose —
// reached no log, no counter and no operator, and the branch it refused about was reported
// exactly like a branch whose agent had renewed its lease a moment earlier.
//
// **The refusal is injected, not manufactured by corrupting a catalog**, for the same reason
// `tests/d124_owner_record_refusal.rs` uses a decorator: the arms under test catch an `Err`, so
// what they must be given is an `Err` arriving at the exact call they swallowed. Corrupting a
// real catalog would prove something about the catalog and nothing about the arm.
//
// Both refusal sites are covered, and — the half that makes this more than a counter test — the
// OTHER meaning of the old `Ok(false)` is asserted too: a branch whose lease moved must raise
// neither the counter nor a line. A fix that reported every skip would pass a refusal test and
// fail that one.

/// A distinctive token planted inside the injected refusal, so the assertion can prove the
/// **message** survived rather than merely that some line was printed. D124 spends its refusal
/// text naming the parent, the fork epoch and the child; if a fix reports "1 branch refused" and
/// drops the text, that is the same defect one layer up.
const D124_TOKEN: &str = "CHILD entry (parent 41, fork epoch 4242) names branch 909";

/// Wraps a catalog and makes exactly one branch undecidable at exactly one of the two sites
/// `reap_if_still_expired` declines to act on. Everything else delegates untouched.
///
/// Two injection points because the swallow had two arms, and they are reached through different
/// code: `get_raw` is `reap_if_still_expired`'s own record read, while `has_live_children` is the
/// D124-guarded resolver that `Reaper::reap` calls *after* it has already marked the branch
/// `Reaping` — which is where a real `dangling_child` refusal arrives.
///
/// `u64::MAX` means "inject nothing": ids are minted from 0 up, so no real branch reaches it.
struct RefusesOneBranch {
    inner: Arc<dyn BranchCatalog>,
    hidden_from_get_raw: AtomicU64,
    corrupt_children_of: AtomicU64,
}

impl RefusesOneBranch {
    fn wrapping(inner: Arc<dyn BranchCatalog>) -> Arc<RefusesOneBranch> {
        Arc::new(RefusesOneBranch {
            inner,
            hidden_from_get_raw: AtomicU64::new(u64::MAX),
            corrupt_children_of: AtomicU64::new(u64::MAX),
        })
    }

    /// `get_raw` on this id fails the way a record mid-`upsert` fails: the first swallowed arm.
    fn hide_record_of(&self, id: u64) {
        self.hidden_from_get_raw.store(id, Ordering::SeqCst);
    }

    /// `has_live_children` on this id raises D124's refusal: the second swallowed arm, reached
    /// from inside `reap`.
    fn refuse_child_resolution_for(&self, id: u64) {
        self.corrupt_children_of.store(id, Ordering::SeqCst);
    }
}

impl BranchCatalog for RefusesOneBranch {
    fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> {
        if id == self.hidden_from_get_raw.load(Ordering::SeqCst) {
            return Err(BranchError::NotFound(BranchId::new(id, 0)).into());
        }
        self.inner.get_raw(id)
    }

    fn has_live_children(&self, parent_id: u64) -> Result<bool, FerroError> {
        if parent_id == self.corrupt_children_of.load(Ordering::SeqCst) {
            return Err(BranchError::Corrupt(format!(
                "{D124_TOKEN}, which has no record right now. A reader cannot tell a branch that \
                 was never published from one whose record is mid-rewrite, so this refuses \
                 instead of resolving it to \"not a live child\"."
            ))
            .into());
        }
        self.inner.has_live_children(parent_id)
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
    fn live_count(&self) -> usize {
        self.inner.live_count()
    }
    fn release_id(&self, id: u64) {
        self.inner.release_id(id)
    }
    fn attach_child(
        &self,
        parent_id: u64,
        fork_epoch: Epoch,
        child_id: u64,
    ) -> Result<(), FerroError> {
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
    fn charge_row_writes(&self, branch: BranchId, n: u64) -> Result<(), FerroError> {
        self.inner.charge_row_writes(branch, n)
    }
}

/// A [`Fixture`] whose **reaper** reads through [`RefusesOneBranch`], and the decorator itself.
///
/// The fork/inspection helpers keep talking to the undecorated catalog through `f.h.catalog`, so
/// building the fixture and reading the result back are never affected by the injection — only
/// the reaper's own reads are, which is the whole point.
fn refusing_fixture(table: bool) -> (Fixture, Arc<RefusesOneBranch>) {
    let h = Harness::new_with(table);
    let catalog = RefusesOneBranch::wrapping(Arc::clone(&h.catalog));
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            Arc::clone(&h.catalog) as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&h.store) as Arc<dyn PageStore>,
        )
        .unwrap(),
    );
    let reaper = Arc::new(TwoTierReaper::new(
        Arc::clone(&catalog) as Arc<dyn BranchCatalog>,
        Arc::clone(&h.store),
    ));
    (Fixture { h, reaper, runtime }, catalog)
}

/// The refusal lines this pass printed, as one blob. Empty when nothing was refused.
fn refusal_lines() -> String {
    reported::lines()
        .into_iter()
        .filter(|l| l.contains("REFUSED to decide"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// **D127, the second swallowed arm — the one D124 actually raises.**
///
/// `has_live_children` refuses mid-`reap`. Before this row the whole sweep reported "reaped 2
/// branches" and said nothing at all about the third, which is now a branch no sweep will ever
/// reclaim. The three assertions are the three halves of the exit criterion: counted, reported
/// with the message intact, and *not* at the cost of the sweep — the other branches still reap.
#[test]
fn d127_a_refusal_inside_reap_is_counted_and_reported() {
    for table in [false, true] {
        let (f, catalog) = refusing_fixture(table);
        let a = branch_with_pages(&f, EXPIRED, 1);
        let refused = branch_with_pages(&f, EXPIRED, 1);
        let c = branch_with_pages(&f, EXPIRED, 1);
        catalog.refuse_child_resolution_for(refused.id);

        let gate = TestGate::new();
        let counters = Counters::default();
        reported::clear();
        scan_once(&f.reaper, &f.runtime, &*gate, &counters);
        let stats = counters.snapshot();
        let lines = refusal_lines();

        assert_eq!(
            stats.refused_reaps, 1,
            "table={table}: the reaper declined to decide about branch {refused} and the pass \
             counted {} refusals. A refusal that raises no counter is a branch that can never be \
             reaped and looks exactly like a healthy one.",
            stats.refused_reaps
        );
        assert!(
            lines.contains(&refused.to_string()) && lines.contains(D124_TOKEN),
            "table={table}: the refusal reached no reader. Reported lines mentioning a refusal:\n\
             {lines}\nA counter without the message cannot tell a momentary miss (retry) from a \
             corrupt CHILD entry (act), which is the distinction D124 spent its text on."
        );
        // Not at the cost of the sweep: the swallow exists so one odd branch does not stop the
        // rest, and that must survive the fix.
        assert_eq!(stats.reaped, 2, "table={table}: the other two branches must still be reaped");
        assert_eq!(stats.failed, 0, "table={table}: a refusal must not fail the sweep");
        assert_eq!(stats.scans, 1, "table={table}: the pass must still complete");
        assert_eq!(state_of(&f, a), BranchState::Reaped, "table={table}");
        assert_eq!(state_of(&f, c), BranchState::Reaped, "table={table}");

        // Negative control, same bodies, injection removed: three reaps, no refusal, no line.
        let (f2, _c2) = refusing_fixture(table);
        for _ in 0..3 {
            branch_with_pages(&f2, EXPIRED, 1);
        }
        let counters2 = Counters::default();
        reported::clear();
        scan_once(&f2.reaper, &f2.runtime, &*TestGate::new(), &counters2);
        assert_eq!(counters2.snapshot().reaped, 3, "table={table}: control");
        assert_eq!(
            counters2.snapshot().refused_reaps,
            0,
            "table={table}: a guard that refuses on a healthy database is not a guard"
        );
        assert!(refusal_lines().is_empty(), "table={table}: control printed a refusal");
    }
}

/// **D127, the first swallowed arm.** `reap_if_still_expired`'s own record read fails — the
/// momentary miss `TableBranchCatalog::upsert` opens on a perfectly healthy branch, which is the
/// case D124's comment names. It is benign and retryable, and it still must be audible: the only
/// thing that tells a benign miss from a permanent one is whether it keeps happening, and a
/// counter that never moves cannot say.
#[test]
fn d127_a_refusal_at_the_record_read_is_counted_and_reported() {
    let (f, catalog) = refusing_fixture(true);
    let a = branch_with_pages(&f, EXPIRED, 1);
    let hidden = branch_with_pages(&f, EXPIRED, 1);
    catalog.hide_record_of(hidden.id);

    let counters = Counters::default();
    reported::clear();
    scan_once(&f.reaper, &f.runtime, &*TestGate::new(), &counters);
    let stats = counters.snapshot();

    assert_eq!(stats.refused_reaps, 1, "the hidden record's refusal was not counted");
    assert!(
        refusal_lines().contains(&hidden.to_string()),
        "the refusal did not name the branch it was about. Lines:\n{}",
        refusal_lines()
    );
    assert_eq!(stats.reaped, 1, "the reachable branch must still be reaped");
    assert_eq!(state_of(&f, a), BranchState::Reaped);
    assert_eq!(
        state_of(&f, hidden),
        BranchState::Live,
        "a refusal must free nothing: the branch is untouched, which is why this is a leak and \
         not data loss"
    );
}

/// **The other direction, and the reason this row is not a counter patch.**
///
/// `Ok(false)` meant two things. If the fix reports every skip, the leak becomes audible and so
/// does every ordinary keepalive race — an operator reading "1 branch refused" every tick learns
/// nothing, which is the same silence with more lines. A branch whose lease moved between the
/// candidate query and the reap must raise **neither** the counter nor a line.
///
/// The renewal is driven from inside the lock acquisition, which is exactly the window D98 opened
/// by moving the candidate query outside it.
#[test]
fn d127_a_branch_whose_lease_moved_is_not_reported_as_a_refusal() {
    struct RenewingGate {
        statement: Mutex<()>,
        catalog: Arc<dyn BranchCatalog>,
        branches: Vec<BranchId>,
    }
    impl RuntimeLock for RenewingGate {
        fn with_runtime_lock(&self, body: &mut dyn FnMut()) {
            let _statement = self.statement.lock().unwrap_or_else(PoisonError::into_inner);
            for b in &self.branches {
                self.catalog.renew_lease(*b, FAR_FUTURE).unwrap();
            }
            body();
        }
    }

    let (f, _catalog) = refusing_fixture(true);
    let all: Vec<BranchId> = (0..3).map(|_| branch_with_pages(&f, EXPIRED, 1)).collect();
    let gate = RenewingGate {
        statement: Mutex::new(()),
        catalog: Arc::clone(&f.h.catalog),
        branches: all.clone(),
    };

    let counters = Counters::default();
    reported::clear();
    scan_once(&f.reaper, &f.runtime, &gate, &counters);
    let stats = counters.snapshot();

    assert_eq!(stats.reaped, 0, "every candidate was renewed inside the lock");
    assert_eq!(
        stats.refused_reaps, 0,
        "a lease that moved is a DECISION the reaper made on the record, not a refusal to make \
         one. Counting it here would bury the refusals that matter under the ordinary keepalive \
         race this skip has always existed for."
    );
    assert!(
        refusal_lines().is_empty(),
        "an ordinary keepalive race printed a refusal:\n{}",
        refusal_lines()
    );
    for b in &all {
        assert_eq!(state_of(&f, *b), BranchState::Live);
    }
}

/// The type itself, without a sweep around it: the three outcomes are three values, so a caller
/// cannot silently collapse two of them again. This is the assertion that would have failed at
/// `ac2ab26` for a reason no amount of counting can express — there was nothing to return.
#[test]
fn d127_refused_and_not_expired_are_different_values() {
    let (f, catalog) = refusing_fixture(true);
    let refused = branch_with_pages(&f, EXPIRED, 1);
    let alive = branch_with_pages(&f, FAR_FUTURE, 1);
    let healthy = branch_with_pages(&f, EXPIRED, 1);
    catalog.refuse_child_resolution_for(refused.id);
    let now = 1u64;

    let out = f.reaper.reap_if_still_expired(refused, now).expect("a refusal must not propagate");
    assert!(
        matches!(out, ReapOutcome::Refused(_)),
        "a branch the catalog refused to resolve came back as {out:?}"
    );
    assert!(
        matches!(f.reaper.reap_if_still_expired(alive, now), Ok(ReapOutcome::NotExpired)),
        "a live lease must be a decision, not a refusal"
    );
    assert!(
        matches!(f.reaper.reap_if_still_expired(healthy, now), Ok(ReapOutcome::Reaped)),
        "fixture: an expired, resolvable branch must reap, or the two arms above prove nothing"
    );

    // The message is carried, not summarised: `refusal_report` is what both sweep shapes print.
    let ReapOutcome::Refused(e) = out else { unreachable!() };
    let line = refusal_report(&[(refused, e)]);
    assert!(
        line.contains(D124_TOKEN),
        "the builder dropped the refusal text, so an operator gets a count and no cause:\n{line}"
    );
}
