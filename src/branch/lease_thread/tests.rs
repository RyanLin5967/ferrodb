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
    assert_eq!(stats.refused_scans, 0, "a standalone node always knows the time");
    assert_eq!(
        stats.refused_branches, 0,
        "a healthy branch with a live lease must be reported as NOT EXPIRED, never as a branch \
         the reaper declined to decide about (D127)"
    );
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

    scan_once(&f.reaper, &f.runtime, &*gate, &counters, &report);

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
    scan_once(&f.reaper, &f.runtime, &*gate, &counters, &report);

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
    scan_once(&f.reaper, &f.runtime, &*gate, &counters, &report);

    let stats = counters.snapshot();
    assert_eq!(
        stats.reaped, REAP_CHUNK as u64,
        "the first chunk reaps {REAP_CHUNK} branches and the renewal must save the rest; {} were \
         reaped instead. More than {REAP_CHUNK} means a renewed lease was ignored — an agent that \
         was still working lost its branch, and the generation makes that unrecoverable.",
        stats.reaped
    );
    // **D127 control, on the largest population of NOT-EXPIRED answers this suite has.** Every
    // one of the `n - REAP_CHUNK` survivors takes `reap_if_still_expired`'s "the lease moved"
    // path. If that path were counted as a refusal, the new signal would arrive already
    // saturated with healthy branches and an operator would learn to ignore it.
    assert_eq!(
        stats.refused_branches, 0,
        "{} branches whose lease was renewed under the sweep were counted as refusals; stats: \
         {stats:?}",
        n - REAP_CHUNK
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
// RULE 4 — D127: a refusal about one branch must reach a reader.
// -------------------------------------------------------------------------------------------

/// Stands in for D124's refusal text. Deliberately not a substring of anything else this module
/// prints, so "the reason travelled" is a test about the reason and not about the word "refused"
/// appearing twice.
const CORRUPT_MARKER: &str = "CHILD entry (parent 7, fork epoch 11) names branch 4242";

/// Makes `has_live_children` refuse for one parent, and delegates everything else untouched.
///
/// **This is the exact shape D124 produces.** `TableBranchCatalog::child_liveness` answers
/// `Err(dangling_child(..))` — a `BranchError::Corrupt` naming the parent, the fork epoch and the
/// child — when a CHILD entry names a record it cannot read, and `has_live_children` is what
/// `Reaper::reap` consults on its way to choosing the fast or the slow path. Injecting it rather
/// than corrupting a catalog is the point: the arm under test catches an `Err` from that call, so
/// what matters is the error, not how a catalog came to produce it. Reproducing the real
/// corruption would test `TableBranchCatalog`'s key layout instead.
struct RefusesLiveChildren {
    inner: Arc<dyn BranchCatalog>,
    /// `u64::MAX` means nothing is armed; no real id reaches it, ids are minted from 0 up.
    armed: AtomicU64,
}

impl RefusesLiveChildren {
    fn new(inner: Arc<dyn BranchCatalog>) -> Arc<RefusesLiveChildren> {
        Arc::new(RefusesLiveChildren { inner, armed: AtomicU64::new(u64::MAX) })
    }
    fn arm(&self, parent_id: u64) {
        self.armed.store(parent_id, Ordering::SeqCst);
    }
}

impl BranchCatalog for RefusesLiveChildren {
    fn has_live_children(&self, parent_id: u64) -> Result<bool, FerroError> {
        if parent_id == self.armed.load(Ordering::SeqCst) {
            return Err(crate::branch::types::BranchError::Corrupt(format!(
                "{CORRUPT_MARKER}, which has no record right now"
            ))
            .into());
        }
        self.inner.has_live_children(parent_id)
    }

    fn next_epoch(&self) -> crate::branch::types::Epoch {
        self.inner.next_epoch()
    }
    fn current_epoch(&self) -> crate::branch::types::Epoch {
        self.inner.current_epoch()
    }
    fn fork(&self, p: BranchId, l: LeaseDeadline) -> Result<BranchRecord, FerroError> {
        self.inner.fork(p, l)
    }
    fn get(&self, b: BranchId) -> Result<BranchRecord, FerroError> {
        self.inner.get(b)
    }
    fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> {
        self.inner.get_raw(id)
    }
    fn reparent(
        &self,
        b: BranchId,
        p: BranchId,
        e: crate::branch::types::Epoch,
        r: PageId,
    ) -> Result<BranchRecord, FerroError> {
        self.inner.reparent(b, p, e, r)
    }
    fn restrict_envelope(
        &self,
        b: BranchId,
        env: crate::branch::record::CapabilityEnvelope,
    ) -> Result<(), FerroError> {
        self.inner.restrict_envelope(b, env)
    }
    fn set_state(
        &self,
        b: BranchId,
        expect: BranchState,
        to: BranchState,
    ) -> Result<(), FerroError> {
        self.inner.set_state(b, expect, to)
    }
    fn set_root(&self, b: BranchId, r: PageId) -> Result<(), FerroError> {
        self.inner.set_root(b, r)
    }
    fn expired_before(
        &self,
        now_millis: u64,
    ) -> Result<Vec<crate::branch::record::CoreRecord>, FerroError> {
        self.inner.expired_before(now_millis)
    }
    fn in_state(&self, s: BranchState) -> Result<Vec<BranchRecord>, FerroError> {
        self.inner.in_state(s)
    }
    fn scan(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        self.inner.scan()
    }
    fn max_live_child(&self, p: u64) -> Result<Option<crate::branch::types::Epoch>, FerroError> {
        self.inner.max_live_child(p)
    }
    fn live_child_in_epoch_range(
        &self,
        p: u64,
        lo: crate::branch::types::Epoch,
        hi: crate::branch::types::Epoch,
    ) -> Result<bool, FerroError> {
        self.inner.live_child_in_epoch_range(p, lo, hi)
    }
    fn live_count(&self) -> usize {
        self.inner.live_count()
    }
    fn release_id(&self, id: u64) {
        self.inner.release_id(id)
    }
    fn attach_child(
        &self,
        p: u64,
        e: crate::branch::types::Epoch,
        c: u64,
    ) -> Result<(), FerroError> {
        self.inner.attach_child(p, e, c)
    }
    fn detach_child(&self, p: u64, e: crate::branch::types::Epoch) -> Result<bool, FerroError> {
        self.inner.detach_child(p, e)
    }
    fn add_arena(&self, b: BranchId, a: crate::branch::types::ArenaId) -> Result<(), FerroError> {
        self.inner.add_arena(b, a)
    }
    fn renew_lease(&self, b: BranchId, l: LeaseDeadline) -> Result<(), FerroError> {
        self.inner.renew_lease(b, l)
    }
    fn envelope_of(
        &self,
        b: BranchId,
    ) -> Result<Option<crate::branch::record::CapabilityEnvelope>, FerroError> {
        self.inner.envelope_of(b)
    }
    fn charge_row_writes(&self, b: BranchId, n: u64) -> Result<(), FerroError> {
        self.inner.charge_row_writes(b, n)
    }
}

/// Collects what a scan would have printed, so "did a reader see it?" is a question a test can
/// ask. This is the reason `scan_once` takes its reporter as a parameter.
#[derive(Default)]
struct Printed(Mutex<Vec<String>>);

impl Printed {
    fn push(&self, m: String) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).push(m);
    }
    fn text(&self) -> String {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).join("\n")
    }
}

/// **D127 — the whole chain, end to end: D124's `Corrupt` must reach a counter and a reader.**
///
/// Before this row: `From<BranchError> for FerroError` flattened `Corrupt` into
/// `FerroError::Branch(String)`; `reap_if_still_expired` mapped that to `Ok(false)`; `scan_once`
/// matched `Ok(false)` with `{}`. So the sentence naming the parent, the fork epoch and the child
/// reached no log, no counter and no operator, and a branch that could never be reaped looked
/// exactly like a healthy one.
///
/// **Two expired branches, one armed, in ONE sweep**, because every interesting claim is
/// comparative:
///   * the armed branch is counted in `refused_branches`, **not** in `reaped` and **not** in
///     `failed` — a refusal is not a reap and is not a sweep that stopped;
///   * the other branch is reaped **in the same pass**, which is the guard on the fix itself: the
///     absorption exists so one odd branch does not kill the sweep, and a fix that traded a
///     silent leak for a reaper that stops on the first oddity would be worse than the bug
///     (SCALE-DESIGN D127, "keep what the swallow is for");
///   * the printed text names the refused branch **and carries the catalog's own reason**, which
///     is the only thing that can tell a benign mid-rewrite race from a corrupt CHILD entry.
///
/// The reaped branch is the anti-vacuity control throughout: a `scan_once` that refused
/// everything, or that printed a refusal line unconditionally, fails on it.
#[test]
fn d127_a_refused_reap_is_counted_and_printed_and_does_not_stop_the_sweep() {
    let f = fixture();
    let refusing = RefusesLiveChildren::new(Arc::clone(&f.h.catalog) as Arc<dyn BranchCatalog>);
    let reaper =
        TwoTierReaper::new(Arc::clone(&refusing) as Arc<dyn BranchCatalog>, Arc::clone(&f.h.store));

    let doomed = branch_with_pages(&f, EXPIRED, 3);
    let corrupt = branch_with_pages(&f, EXPIRED, 3);
    let with_both = f.h.store.live_page_count().unwrap();
    refusing.arm(corrupt.id);

    let counters = Counters::default();
    let printed = Printed::default();
    scan_once(&reaper, &f.runtime, &*TestGate::new(), &counters, &|m| printed.push(m));
    let stats = counters.snapshot();
    let text = printed.text();

    // 1. COUNTED, and counted as itself.
    assert_eq!(
        stats.refused_branches, 1,
        "the refusal was not counted. `Ok(false)` used to mean both \"the lease moved\" and \
         \"the catalog would not answer\", and `scan_once` matched it with an empty arm: under W4 \
         that is an unbounded leak with no signal at all. Stats: {stats:?}"
    );
    assert_eq!(stats.reaped, 1, "exactly one of the two branches was reapable; stats: {stats:?}");
    assert_eq!(
        stats.failed, 0,
        "a refusal was reported as a FAILED sweep. It is not one: nothing was freed, the other \
         expired branch was still reclaimed, and treating it as a failure would make the reaper \
         stop on the first oddity — strictly worse than the bug this row fixes."
    );
    assert_eq!(stats.scans, 1, "the sweep must have completed; stats: {stats:?}");

    // 2. THE SWEEP CONTINUED. This is what the absorption is for, and the reason the fix is a
    //    richer outcome rather than propagating the error.
    assert_eq!(
        state_of(&f, doomed),
        BranchState::Reaped,
        "a refusal on one branch stopped the other expired branch from being reaped"
    );
    assert!(
        f.h.store.live_page_count().unwrap() < with_both,
        "the healthy branch's pages did not come back, so the refusal aborted the reclamation"
    );

    // 3. PRINTED, naming the branch AND carrying the catalog's own reason. A count alone cannot
    //    tell the benign mid-rewrite race from a genuinely corrupt CHILD entry.
    assert!(
        text.contains(&corrupt.to_string()),
        "the refusal reached a reader without naming the branch it was about:\n{text}"
    );
    assert!(
        text.contains(CORRUPT_MARKER),
        "the refusal reached a reader without the reason the catalog gave. That reason is the \
         whole output of D124's guard — it names the parent, the fork epoch and the child — and \
         dropping it leaves an operator a number with nothing to act on:\n{text}"
    );

    // 4. ANTI-VACUITY. A reaped branch must NOT be described as refused, or the new signal is
    //    noise an operator learns to ignore — which is this defect again, one level up.
    assert!(
        !text.contains("REFUSED to decide on 2"),
        "the healthy branch was reported as a refusal too:\n{text}"
    );
    assert!(
        text.contains("reaped 1 expired branch"),
        "the successful reap stopped being reported:\n{text}"
    );
}

/// **D127, the other direction: a scan that refuses nothing must say nothing about refusals.**
///
/// A detector that fires on a clean run is not a detector. Same fixture, nothing armed, and it is
/// the control that makes every assertion in the test above mean something: without it, a
/// `scan_once` that counted and printed a refusal unconditionally would pass them all.
///
/// # ⛔ STRICTENED after a fire-check, because the stated control was not running
///
/// This body used to give `kept` a `FAR_FUTURE` lease and its comment claimed that made the pass
/// answer `NotExpired` for it. **It did not.** `expired_before` never returns an unexpired branch,
/// so `kept` was not a candidate and `reap_if_still_expired` was never called on it — the
/// `NotExpired` arm was not exercised at all, and a mutant that pushed a refusal from that arm
/// passed this test. (It was caught by `d98_a_branch_renewed_after_the_query_is_not_reaped`, which
/// owns the large version of the same control; this one claimed a coverage it did not have — the
/// header-against-body failure, in a test comment.)
///
/// The assertion is not weakened to match the body; the BODY is fixed to match the assertion.
/// `kept` is now expired when the candidate query runs and is renewed **inside the lock
/// acquisition**, which is the only way to reach `NotExpired`: `reap_if_still_expired` re-reads
/// the record there and sees a deadline that has moved. Strictly more than before — the old shape
/// asserted "a branch nothing asked about was not called a refusal".
#[test]
fn d127_a_clean_sweep_reports_no_refusal_at_all() {
    /// Renews one branch on every acquisition, before the body runs — the keepalive race D98
    /// opened by moving the candidate query outside the lock.
    struct RenewsOneBranch {
        statement: Mutex<()>,
        catalog: Arc<dyn BranchCatalog>,
        branch: BranchId,
    }
    impl RuntimeLock for RenewsOneBranch {
        fn with_runtime_lock(&self, body: &mut dyn FnMut()) {
            let _statement = self.statement.lock().unwrap_or_else(PoisonError::into_inner);
            self.catalog.renew_lease(self.branch, FAR_FUTURE).unwrap();
            body();
        }
    }

    let f = fixture();
    let refusing = RefusesLiveChildren::new(Arc::clone(&f.h.catalog) as Arc<dyn BranchCatalog>);
    let reaper =
        TwoTierReaper::new(Arc::clone(&refusing) as Arc<dyn BranchCatalog>, Arc::clone(&f.h.store));

    let doomed = branch_with_pages(&f, EXPIRED, 3);
    // Nothing armed, and both branches ARE candidates: `kept`'s lease has expired when the query
    // runs, and the gate renews it before the reaps. So this pass produces one `Reaped` and one
    // genuine `NotExpired`, and neither may land in `refused_branches`.
    let kept = branch_with_pages(&f, EXPIRED, 3);
    let gate = RenewsOneBranch {
        statement: Mutex::new(()),
        catalog: Arc::clone(&f.h.catalog),
        branch: kept,
    };

    let counters = Counters::default();
    let printed = Printed::default();
    scan_once(&reaper, &f.runtime, &gate, &counters, &|m| printed.push(m));
    let stats = counters.snapshot();
    let text = printed.text();

    assert_eq!(
        stats.refused_branches, 0,
        "a clean sweep counted a refusal. The candidate whose lease moved under the sweep is a \
         DECISION the reaper made on the record, not a refusal to make one — counting it here \
         buries the refusals that matter under the ordinary keepalive race: {stats:?}"
    );
    assert_eq!(stats.reaped, 1, "the expired branch was not reaped: {stats:?}");
    assert!(!text.contains("REFUSED"), "a clean sweep printed a refusal:\n{text}");
    assert_eq!(state_of(&f, doomed), BranchState::Reaped);
    assert_eq!(
        state_of(&f, kept),
        BranchState::Live,
        "the renewed branch was reaped anyway, so this body no longer exercises NotExpired and \
         the control above proves nothing"
    );
}

/// **D127 — the report is bounded, and the bound never eats the count.**
///
/// The reasons are paragraphs (D124's is six lines), so an uncapped list on a database with a
/// thousand refusals would be a log nobody reads — the same failure as a log nobody writes. What
/// must survive truncation is the number and the ids, because the number is what says whether
/// this is the benign mid-rewrite race or a leak.
#[test]
fn d127_a_capped_refusal_report_still_states_the_true_count() {
    let n = REFUSAL_DETAIL_CAP + 3;
    let refused: Vec<(BranchId, FerroError)> = (0..n)
        .map(|i| {
            (
                BranchId::new(100 + i as u64, 0),
                FerroError::Branch(format!("reason-number-{i}")),
            )
        })
        .collect();
    let text = refusal_report(&refused);

    assert!(
        text.contains(&format!("REFUSED to decide on {n} expired branch(es)")),
        "the exact count must survive the cap:\n{text}"
    );
    for (b, _) in &refused {
        assert!(text.contains(&b.to_string()), "branch {b} was not named at all:\n{text}");
    }
    assert!(text.contains("reason-number-0"), "the first reasons must be spelled out:\n{text}");
    assert!(
        text.contains(&format!("and {} more refusal(s)", n - REFUSAL_DETAIL_CAP)),
        "a truncated report must say how much it truncated:\n{text}"
    );
    assert!(
        !text.contains(&format!("reason-number-{}", n - 1)),
        "the cap did not actually cap anything, so this test proves nothing about it:\n{text}"
    );
}

/// **D127 — the OTHER caller shape: `Reaper::reap_expired` has no reporter, and must still not
/// lose a refusal.**
///
/// `scan_once` runs the two halves of a sweep itself and prints; the trait's whole-sweep method
/// runs them back to back and hands back only *the branches it reaped*. A refusal is by
/// construction not one of those, and there is nowhere in `Result<Vec<BranchId>>` to put it — so
/// this is the call shape where the row's defect could quietly survive its own fix.
///
/// It does not, because the counter lives at the refusal SITE (`TwoTierReaper::refuse`) rather
/// than in each caller, which is the part of the design that was argued in a comment and pinned
/// nowhere. **This test is that pin.** A counter moved into `scan_once` — the obvious tidier
/// shape, and the one a future reader is most likely to reach for — passes every other D127 test
/// in this file and fails here.
///
/// The reaped branch is the anti-vacuity control: a sweep that refused on everything, or that
/// aborted on the first refusal, fails on it rather than on the counter.
#[test]
fn d127_the_trait_sweep_counts_a_refusal_it_cannot_report() {
    use crate::branch::Reaper;

    let f = fixture();
    let refusing = RefusesLiveChildren::new(Arc::clone(&f.h.catalog) as Arc<dyn BranchCatalog>);
    let reaper =
        TwoTierReaper::new(Arc::clone(&refusing) as Arc<dyn BranchCatalog>, Arc::clone(&f.h.store));

    let doomed = branch_with_pages(&f, EXPIRED, 3);
    let corrupt = branch_with_pages(&f, EXPIRED, 3);
    let with_both = f.h.store.live_page_count().unwrap();
    refusing.arm(corrupt.id);

    let before = reaper.refused_reaps();
    let reaped = reaper.reap_expired(LeaseDeadline::now_millis()).expect(
        "a refusal must not be propagated as a sweep error: one odd branch stopping the sweep is \
         strictly worse than the silent leak D127 fixes",
    );

    assert_eq!(
        reaper.refused_reaps(),
        before + 1,
        "the trait sweep dropped a refusal. It has no reporter and its return type has no room \
         for one, so this counter — incremented where the refusal is PRODUCED, not where it is \
         consumed — is the only thing standing between this call shape and the original defect."
    );
    assert_eq!(
        reaped,
        vec![doomed],
        "exactly one of the two expired branches was reapable, and the sweep must have gone on \
         past the refused one to reach it"
    );
    assert!(
        f.h.store.live_page_count().unwrap() < with_both,
        "the healthy branch's pages did not come back, so the refusal aborted the reclamation"
    );
    assert_eq!(
        state_of(&f, corrupt),
        BranchState::Reaping,
        "a refusal must free nothing. `reap` publishes Live -> Reaping before it consults \
         has_live_children, so this branch is mid-reap and keeps every page — which is the safe \
         direction, and is also why the lease thread cannot see it again: expired_before is \
         Live-only and resume_interrupted_reaps runs at open, not per tick."
    );
}
