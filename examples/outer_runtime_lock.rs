//! D98 — does the OUTER `RuntimeLock` that `scan_once` holds stall statements as branches grow?
//!
//! # The claim under test
//!
//! `branch::lease_thread::scan_once` runs its body inside `with_lock(lock, ..)`, and in both
//! production shapes that lock is **the mutex every SQL statement takes**: `ServerContext::catalog`
//! for the pgwire server, `CatalogLock` for the CLI. Whatever the body costs, every connection in
//! the database waits for it. W4's earlier rows are about `AgentRuntime`'s *inner* `Mutex<State>`
//! and say nothing about this one — `bench/w4/DECISION.md` addendum 1 says so in as many words,
//! and its closing section lists the outer lock as still open.
//!
//! So the question this harness answers is narrow and structural: **is the time that lock is held
//! a function of how many branches exist?** If it is, the defect is the shape rather than the
//! constant, and it serializes writers in any engine that sweeps under a statement lock —
//! BranchBench (arXiv 2604.17180) reports exactly that absence of "concurrent branch management"
//! across every branchable DBMS it measured.
//!
//! ```text
//! outer_runtime_lock [N,comma,separated] [expired_denom] [probe_us] [reps] [warmup_ms]
//! ```
//!
//! # Three arms, and why a control is not optional
//!
//! A single "statement latency at N=10⁵" number cannot tell a lock from a cache effect: 10⁵
//! branches is more resident state whatever the lock does. Every arm therefore builds **the same
//! fixture at the same N** and differs only in which lock the sweep takes.
//!
//!   `idle`   — the prober alone; no lease scan runs at all. The NEGATIVE control and the noise
//!              floor of this machine. A stall here is the fleet, not the code.
//!   `locked` — the production wiring: `LeaseThread` is handed the `ServerContext` whose mutex the
//!              prober is taking. The thing under test.
//!   `free`   — identical N, identical expired set, identical sweep work, but the `LeaseThread`
//!              is handed a PRIVATE mutex nobody else holds. **This is the control the result
//!              rests on.** The sweep still walks the catalog, still reaps, still touches
//!              `AgentRuntime`'s state lock; it simply cannot block the prober. If statement
//!              latency rises with N here too, the rise is residency and not the lock, and the
//!              headline is withdrawn.
//!
//! The prober is one thread, not many: two probers contend with each other and that contention
//! also grows with nothing in particular, which would put a second unlabelled wall in the result.
//! It sleeps `probe_us` between acquisitions rather than spinning, because a spinning prober
//! starves the holder and a starved holder draws a reassuringly flat line for the wrong reason.
//!
//! # One sweep per fixture, on purpose
//!
//! `LeaseThread::start` runs `scan_once` once immediately and then sleeps for its interval, so an
//! interval of one hour fires exactly one sweep through the real production call path. That is the
//! quantity the question is about — *the length of a single hold* — and it is the shape
//! `bench/w4/statement-lock-*.txt` already validated. Nothing here reimplements `scan_once`; a
//! harness that claimed a protocol its body did not run is a failure this project has had three
//! times (`bench/w4/DECISION.md`, and the D88 commit message).
//!
//! # What it refuses to report
//!
//! - A cell where the sweep reaped **nothing** prints `--`, because a sweep that did no work has
//!   not been measured as fast, it has not been measured.
//! - A cell where **no probe overlapped** the sweep prints `--` for the overlap columns: at
//!   `probe_us` spacing a hold shorter than one probe interval falls between two probes, and that
//!   is a fact about the instrument to be stated, not a small number to be quoted.
//! - Any refusal makes the process exit non-zero, so a run that could not see its subject cannot
//!   be mistaken for a run that saw nothing wrong.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::session::AgentSession;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::lease_thread::{LeaseThread, RuntimeLock};
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper, TableBranchCatalog};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;

/// Long enough that `LeaseThread` fires its immediate scan and then never fires a second one.
const ONE_SWEEP_ONLY: Duration = Duration::from_secs(3600);

// ---------------------------------------------------------------------------------------------
// samples
// ---------------------------------------------------------------------------------------------

/// One prober acquisition: when it started asking, and how long it waited, in nanoseconds.
#[derive(Clone, Copy)]
struct Probe {
    /// Nanoseconds since the run's origin at which the acquisition was *requested*.
    asked_at: u64,
    /// Nanoseconds the acquisition took.
    waited: u64,
}

/// Sorted latencies in nanoseconds.
struct Samples(Vec<u64>);

impl Samples {
    fn of(mut v: Vec<u64>) -> Samples {
        v.sort_unstable();
        Samples(v)
    }
    fn pct(&self, p: f64) -> u64 {
        if self.0.is_empty() {
            return 0;
        }
        self.0[((self.0.len() - 1) as f64 * p).round() as usize]
    }
    fn max(&self) -> u64 {
        self.0.last().copied().unwrap_or(0)
    }
    fn n(&self) -> usize {
        self.0.len()
    }
}

// ---------------------------------------------------------------------------------------------
// the lock the `free` arm hands the sweep
// ---------------------------------------------------------------------------------------------

/// A `RuntimeLock` that is a mutex **nobody else takes**.
///
/// The control's whole content. The sweep runs under a real lock, acquired and released exactly as
/// in production, so nothing about its own code path changes; the lock simply has no other
/// contender. Any difference between this arm and `locked` at the same N is attributable to the
/// prober and the sweep sharing a lock, which is the claim.
struct PrivateLock(Mutex<()>);

impl RuntimeLock for PrivateLock {
    fn with_runtime_lock(&self, body: &mut dyn FnMut()) {
        let _g = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        body();
    }
}

/// Wraps whichever lock the sweep is handed and **counts acquisitions**.
///
/// # Why this exists, and why it is the number the result rests on
///
/// Everything else here is a duration, and this box runs a dozen build agents at load 20-60. A
/// latency curve taken under that load is partly a curve of the fleet, and a contention wall made
/// of sibling `rustc` processes looks exactly like the one being hunted.
///
/// `acquisitions` is not a duration. It is a count of events, and with the reaps the sweep
/// reported it gives **reaps per acquisition of the statement lock** — an integer ratio that says
/// the whole claim and that fleet load cannot move:
///
/// - before, ONE acquisition holds every reap on the tick, so the ratio rises linearly with the
///   number of branches that expired;
/// - after, it is bounded by `lease_thread::REAP_CHUNK` however many expired.
///
/// ⚠ Read the ratio, not the acquisition count, and do not expect it to equal K exactly.
/// `LeaseThread::start` takes the lock once more on the calling thread, for
/// `resume_interrupted_reaps`, and that acquisition is counted here too — correctly, since it is a
/// real hold of the statement mutex, though it happens before any client can connect. So the
/// before arm reads `acqs = 2` at every K (one resume, one sweep) and a ratio of K/2, and the
/// after arm reads `acqs = 1 + ceil(K / REAP_CHUNK)`. The shape is in how the ratio moves with K:
/// linear before, flat after.
///
/// "A lock whose hold grows with the branch count" and "a lock whose hold does not" is exactly the
/// difference between those two, stated without reference to a clock. The latency columns then say
/// what it costs in seconds on this machine, as an upper bound with its load stamped beside it.
///
/// `span_ns` is kept too, but it is a duration and is read as one — and it is the sweep's
/// **wait plus hold**, not its hold: this wrapper sits outside the inner lock and cannot see the
/// moment it was granted. So it is an upper bound on hold time, and it is reported as a total
/// divided by `acquisitions` rather than quoted as "the stall", which is the clients' number.
struct CountingLock {
    inner: Arc<dyn RuntimeLock>,
    acquisitions: AtomicU64,
    span_ns: AtomicU64,
}

impl CountingLock {
    fn new(inner: Arc<dyn RuntimeLock>) -> Arc<CountingLock> {
        Arc::new(CountingLock {
            inner,
            acquisitions: AtomicU64::new(0),
            span_ns: AtomicU64::new(0),
        })
    }
}

impl RuntimeLock for CountingLock {
    fn with_runtime_lock(&self, body: &mut dyn FnMut()) {
        let t0 = Instant::now();
        self.inner.with_runtime_lock(body);
        // Counted AFTER the inner call returns, so it counts acquisitions that completed. A sweep
        // still blocked on the statement mutex has not acquired anything, and must not be counted
        // as though it had.
        self.acquisitions.fetch_add(1, Ordering::SeqCst);
        self.span_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------------------------
// fixture
// ---------------------------------------------------------------------------------------------

/// A whole database, wired exactly as `examples/pgserver.rs` wires one.
///
/// `TableBranchCatalog`, not `LogBranchCatalog`: the log catalog's `expired_before` is a documented
/// O(N) walk over a `HashMap` and would manufacture the very slope this harness is looking for. The
/// table catalog answers from a deadline-key span, which is what ships, and is therefore the only
/// implementation whose slope means anything here.
struct Fixture {
    dir: PathBuf,
    ctx: Arc<ServerContext>,
    runtime: Arc<AgentRuntime>,
    reaper: Arc<TwoTierReaper>,
    branches: Arc<TableBranchCatalog>,
    /// Held open so the workspaces stay in `AgentRuntime`'s state map.
    _sessions: Vec<AgentSession>,
    /// Every branch this fixture forked, in creation order. `expire_next` walks it.
    live: Vec<BranchId>,
    /// How far `expire_next` has got. **A reap is destructive**, so no branch is ever handed to
    /// two arms: an arm that reaped the fixture's only expired branches would leave the next arm
    /// sweeping an empty catalog and reporting its own emptiness as speed. The first draft of this
    /// harness did exactly that and the "reaped nothing" refusal is what caught it.
    cursor: usize,
    build: Duration,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Fixture {
    /// Give the next `k` untouched branches an already-expired lease, and return how many.
    ///
    /// Called immediately before each arm that sweeps, so every such arm faces **exactly `k`
    /// reapable branches drawn from the same population of the same fixture**. Returning a short
    /// count rather than wrapping is deliberate: exhausting the fixture is a fact the caller must
    /// see, and reusing a branch would silently halve the work the next arm is credited with.
    fn expire_next(&mut self, k: usize) -> usize {
        let mut done = 0;
        while done < k && self.cursor < self.live.len() {
            let b = self.live[self.cursor];
            self.cursor += 1;
            // `LeaseDeadline(1)` rather than `0`: several fixtures in this tree use 0 as "never
            // set", and a deadline of 1 ms past the epoch is unambiguously in the past.
            if self.branches.renew_lease(b, LeaseDeadline(1)).is_ok() {
                done += 1;
            }
        }
        done
    }
}

/// Build `n` agent branches, none expired — expiry is handed out per arm by [`Fixture::expire_next`].
///
/// Each branch is a real `begin_session` — a fork plus a workspace — because both halves matter:
/// the fork is what the reap loop walks, and the workspace is what `forget_branches` removes.
/// A fixture of bare catalog forks would leave the runtime with nothing to forget and would
/// under-report the hold.
///
/// **Built on `threads` threads purely to make the fixture affordable.** Each `fork` is a durable
/// append and a single-threaded build pays one commit group per branch — measured at ~9 ms each,
/// which is 15 minutes at N=10⁵. Concurrent forks join the same commit group. Nothing measured
/// later depends on the build being serial; the arms run long after it finishes.
fn build(n: usize, threads: usize, root: &Path) -> Fixture {
    let t0 = Instant::now();
    let dir = root.join(format!("d98-n{n}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("fixture dir");
    let db = dir.join("ferro.db").to_string_lossy().into_owned();

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&db)
        .expect("open db");
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));
    let wal = Arc::new(WalManager::new(format!("{db}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let catalog = Catalog::create(bp.clone()).unwrap();

    let branches: Arc<TableBranchCatalog> =
        Arc::new(TableBranchCatalog::default_for_database(&db, FIRST_CATALOG_PAGE_ID).unwrap());
    let base = bp.disk_manager.high_water().expect("high water") + 32_736;
    let store: Arc<ArenaPageStore> =
        Arc::new(ArenaPageStore::new(bp.clone(), branches.clone() as Arc<dyn BranchCatalog>, base).unwrap());

    let reaper = Arc::new(TwoTierReaper::new(
        branches.clone() as Arc<dyn BranchCatalog>,
        store.clone(),
    ));
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches.clone() as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            store.clone() as Arc<dyn PageStore>,
        )
        .unwrap()
        .with_reaper(reaper.clone() as Arc<dyn Reaper>),
    );

    let threads = threads.max(1);
    let mut sessions: Vec<AgentSession> = Vec::with_capacity(n);
    std::thread::scope(|sc| {
        let mut handles = Vec::with_capacity(threads);
        for t in 0..threads {
            let runtime = &runtime;
            handles.push(sc.spawn(move || {
                let mut mine = Vec::with_capacity(n / threads + 1);
                let mut i = t;
                while i < n {
                    let agent = format!("a{i}");
                    mine.push(
                        runtime
                            .begin_session(&agent, None, BranchId::TRUNK)
                            .expect("begin_session"),
                    );
                    i += threads;
                }
                mine
            }));
        }
        for h in handles {
            sessions.extend(h.join().expect("fixture thread"));
        }
    });
    // Sorted so `expire_next` hands out a deterministic prefix regardless of how the threads
    // interleaved. Without this the set an arm reaps depends on scheduling, and two runs of the
    // same fixture would not be the same experiment.
    sessions.sort_by_key(|s| s.branch.id);
    let live: Vec<BranchId> = sessions.iter().map(|s| s.branch).collect();

    let ctx = Arc::new(ServerContext::new(catalog, bp, txn, runtime.clone()));
    Fixture {
        dir,
        ctx,
        runtime,
        reaper,
        branches,
        _sessions: sessions,
        live,
        cursor: 0,
        build: t0.elapsed(),
    }
}

// ---------------------------------------------------------------------------------------------
// one arm
// ---------------------------------------------------------------------------------------------

/// Which lock the sweep is handed — the only thing that differs between arms.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    Idle,
    Locked,
    Free,
}

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Idle => "idle",
            Arm::Locked => "locked",
            Arm::Free => "free",
        }
    }
}

/// Every overlap sample for one (N, arm, K) cell, pooled across reps.
///
/// Pooled because a single sweep gives each connection exactly one blocked acquisition, so a
/// per-rep p99 is a percentile over `clients` points. The reps are the same experiment repeated;
/// pooling them is the population the percentile is about.
#[derive(Default)]
struct Pool {
    waits: Vec<u64>,
    sweep_walls: Vec<u64>,
    reaped: u64,
    reps: usize,
    /// Summed across reps. Integers, so fleet load cannot move them.
    acquisitions: u64,
    done_during_sweep: u64,
}

/// What one arm produced.
struct Outcome {
    /// Every prober acquisition in the window.
    all: Samples,
    /// Only the acquisitions that were in flight while the sweep was running.
    overlap: Samples,
    /// Wall time of the sweep itself, nanoseconds; 0 for `idle`.
    sweep_wall: u64,
    /// Branches the sweep actually reaped.
    reaped: u64,
    /// Scans that ran with the lock held (should be 1), and scans that refused.
    scans: u64,
    refused: u64,
    /// **Times the sweep acquired the runtime lock.** An integer; fleet load cannot move it.
    acquisitions: u64,
    /// Total wait-plus-hold across those acquisitions, nanoseconds. An upper bound on hold time.
    span_ns: u64,
    /// **Client statements that began AND finished inside the sweep window.** An integer.
    done_during_sweep: u64,
}

/// Run one arm against a freshly built fixture.
///
/// The prober starts first and warms up, so the lock is already hot when the sweep begins and the
/// sweep is not credited with first-touch costs it did not cause.
fn run_arm(f: &Fixture, arm: Arm, clients: usize, probe_us: u64, warmup: Duration) -> Outcome {
    let origin = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let probes: Arc<Mutex<Vec<Probe>>> = Arc::new(Mutex::new(Vec::with_capacity(1 << 16)));

    // ---- the probers: `clients` connections, each issuing "statements" ---------------------
    //
    // `ctx.catalog()` is literally what `pgwire::extended::Statement::execute` takes before it
    // runs any SQL, so the wait recorded here is the wait a real statement suffers. Nothing is
    // done under the guard: the question is how long it took to GET the lock.
    //
    // **Why more than one.** A statement that blocks for the whole sweep means each connection
    // contributes exactly ONE sample per sweep, so a single prober can report a `max` but has no
    // population to take a p50 or a p99 over — the first draft of this harness drew two samples
    // in the `locked` arm and called them percentiles. `clients` connections give `clients`
    // samples per sweep, which is also what the question is actually about: a server with one
    // client has no concurrency problem to report.
    //
    // The probers do contend with EACH OTHER, and that contention is exactly what the `idle` arm
    // measures. If idle's p50 rises with `clients`, the instrument has become the thing it
    // measures and the run says so rather than quietly crediting it to the sweep.
    let mut probers = Vec::with_capacity(clients.max(1));
    for c in 0..clients.max(1) {
        let ctx = Arc::clone(&f.ctx);
        let stop = Arc::clone(&stop);
        let probes = Arc::clone(&probes);
        probers.push(
            std::thread::Builder::new()
                .name(format!("d98-client{c}"))
                .spawn(move || {
                    let mut local = Vec::with_capacity(1 << 14);
                    while !stop.load(Ordering::Relaxed) {
                        let asked = Instant::now();
                        let g = ctx.catalog();
                        let waited = asked.elapsed();
                        drop(g);
                        local.push(Probe {
                            asked_at: asked.duration_since(origin).as_nanos() as u64,
                            waited: waited.as_nanos() as u64,
                        });
                        // Sleeping rather than spinning: a spinning client starves the sweep, and
                        // a starved holder draws a reassuringly flat line for the wrong reason.
                        std::thread::sleep(Duration::from_micros(probe_us));
                    }
                    probes.lock().unwrap().extend(local);
                })
                .expect("prober"),
        );
    }

    std::thread::sleep(warmup);

    // ---- the sweep ------------------------------------------------------------------------
    let mut acquisitions = 0u64;
    let mut span_ns = 0u64;
    let (sweep_from, sweep_to, reaped, scans, refused) = if arm == Arm::Idle {
        // The negative control does no sweep at all. The window is the same length so the two
        // sample sets are the same size of draw.
        std::thread::sleep(warmup);
        (0u64, 0u64, 0, 0, 0)
    } else {
        let inner: Arc<dyn RuntimeLock> = match arm {
            Arm::Locked => Arc::clone(&f.ctx) as Arc<dyn RuntimeLock>,
            _ => Arc::new(PrivateLock(Mutex::new(()))) as Arc<dyn RuntimeLock>,
        };
        // Counted in BOTH arms, so `reaps per acquisition` is comparable between them: the
        // wrapper adds the same two atomics to each and changes which lock is underneath, which
        // is the one difference the arms are for.
        let counting = CountingLock::new(inner);
        let from = Instant::now();
        let lease = LeaseThread::start(
            Arc::clone(&f.reaper),
            Arc::clone(&f.runtime),
            Arc::clone(&counting) as Arc<dyn RuntimeLock>,
            ONE_SWEEP_ONLY,
        )
        .expect("lease thread");
        // Wait for the one immediate scan to finish. `attempts` rises before the lock is taken and
        // `scans`/`refused`/`failed` after the body ran, so their sum advancing is the sweep
        // having completed rather than having been scheduled.
        let deadline = Instant::now() + Duration::from_secs(600);
        loop {
            let s = lease.stats();
            if s.scans + s.refused + s.failed > 0 {
                break;
            }
            if Instant::now() > deadline {
                eprintln!("d98: the sweep did not finish within 600 s — refusing to report it");
                break;
            }
            std::thread::sleep(Duration::from_micros(200));
        }
        let to = Instant::now();
        let stats = lease.stop();
        // Read after `stop()` joined the scan thread, so nothing can still be incrementing them.
        acquisitions = counting.acquisitions.load(Ordering::SeqCst);
        span_ns = counting.span_ns.load(Ordering::SeqCst);
        (
            from.duration_since(origin).as_nanos() as u64,
            to.duration_since(origin).as_nanos() as u64,
            stats.reaped,
            stats.scans,
            stats.refused,
        )
    };

    stop.store(true, Ordering::Relaxed);
    for p in probers {
        p.join().expect("prober join");
    }
    let raw = probes.lock().unwrap().clone();

    // **Overlap, not the whole window.** A probe that started and finished before the sweep did
    // cannot have waited for it, and averaging those in credits the sweep with idle time — the
    // instrument defect `bench/w4/DECISION.md` addendum 3 records and fixes. A probe overlaps if
    // the interval it occupied intersects the sweep's.
    let overlap: Vec<u64> = raw
        .iter()
        .filter(|p| sweep_to > sweep_from && p.asked_at + p.waited >= sweep_from && p.asked_at <= sweep_to)
        .map(|p| p.waited)
        .collect();

    Outcome {
        all: Samples::of(raw.iter().map(|p| p.waited).collect()),
        overlap: Samples::of(overlap),
        sweep_wall: sweep_to.saturating_sub(sweep_from),
        reaped,
        scans,
        refused,
        acquisitions,
        span_ns,
        // **A count, not a duration: statements that COMPLETED while the sweep was running.**
        //
        // The second load-immune number. Before the fix every client is blocked for the whole
        // sweep, so this is at most one per client; after it, clients keep going and it is
        // thousands. Fleet load slows both arms together and cannot invert them.
        done_during_sweep: raw
            .iter()
            .filter(|p| sweep_to > sweep_from && p.asked_at >= sweep_from && p.asked_at + p.waited <= sweep_to)
            .count() as u64,
    }
}

// ---------------------------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------------------------

/// One measured cell: which lock the sweep took, and how many branches it had to reap.
#[derive(Clone, Copy)]
struct Spec {
    arm: Arm,
    /// Expired branches this arm faces. `None` means "N / denom", resolved once N is known.
    k: Option<usize>,
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let ns: Vec<usize> = a
        .get(1)
        .map(|s| s.split(',').map(|v| v.trim().parse().expect("N")).collect())
        .unwrap_or_else(|| vec![1_000, 10_000, 100_000]);
    let expired_denom: usize = a.get(2).map(|s| s.parse().expect("denom")).unwrap_or(100);
    let fixed_k: usize = a.get(3).map(|s| s.parse().expect("fixed_k")).unwrap_or(64);
    let probe_us: u64 = a.get(4).map(|s| s.parse().expect("probe_us")).unwrap_or(50);
    let reps: usize = a.get(5).map(|s| s.parse().expect("reps")).unwrap_or(3);
    let warmup = Duration::from_millis(a.get(6).map(|s| s.parse().expect("warmup")).unwrap_or(200));
    let threads: usize = a.get(7).map(|s| s.parse().expect("threads")).unwrap_or(8);
    let clients: usize = a.get(8).map(|s| s.parse().expect("clients")).unwrap_or(32);

    let root = std::env::temp_dir().join("d98-outer-runtime-lock");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("root");

    println!("# D98 — the outer RuntimeLock held across `scan_once`");
    println!("# ferrodb {}", ferrodb::build_provenance());
    println!(
        "# N={ns:?} K_prop=N/{expired_denom} K_fixed={fixed_k} clients={clients} \
         probe={probe_us}us reps={reps} warmup={warmup:?} fixture_threads={threads}"
    );
    println!("# catalog: TableBranchCatalog (the shipped one, deadline-indexed)");
    println!(
        "# arms: idle = prober alone (negative control) | locked = sweep under the STATEMENT lock \
         (production) | free = same sweep, same N, same K, PRIVATE lock (control)"
    );
    println!(
        "# TWO K series, because \"grows with branch count\" has two candidate mechanisms and one \
         series cannot separate them: K_prop rises with N (the agent workload — more agents, more \
         abandoned branches per sweep), K_fixed stays constant (isolating any cost that is a \
         function of TOTAL branches alone, e.g. the deadline-index descent)."
    );
    println!(
        "# stall columns are prober acquisitions OVERLAPPING the sweep; p50_all is the whole \
         window, i.e. the uncontended floor."
    );
    println!();

    // Rotated per rep, so arm order is a Latin square rather than a fixed sequence. Interleaving
    // alone does not cancel a monotone drift in the machine; rotating the order is what stops a
    // drift from being read as a difference between arms.
    let base: Vec<Spec> = vec![
        Spec { arm: Arm::Idle, k: Some(0) },
        Spec { arm: Arm::Locked, k: None },
        Spec { arm: Arm::Free, k: None },
        Spec { arm: Arm::Locked, k: Some(fixed_k) },
        Spec { arm: Arm::Free, k: Some(fixed_k) },
    ];

    let mut blind = 0usize;
    let mut vacuous = 0usize;
    let mut short = 0usize;
    // Keyed by (N, arm, K) and ordered, so the summary comes out in a stable order run to run.
    let mut pools: std::collections::BTreeMap<(usize, &'static str, usize), Pool> =
        std::collections::BTreeMap::new();

    println!(
        "{:>8} {:>4} {:>7} {:>7} {:>8} {:>8} {:>12} {:>12} {:>12} {:>14} {:>7} {:>5} {:>8} {:>10}",
        "N", "rep", "arm", "K", "probes", "overlap", "p50_ov_ns", "p99_ov_ns",
        "max_ns", "sweep_wall_ns", "reaped", "acqs", "reap/acq", "done_in_sw"
    );

    for &n in &ns {
        for rep in 0..reps {
            // One fixture per rep, shared by every arm in that rep: the arms must see the SAME
            // database or the comparison carries a second difference. Each sweeping arm is handed
            // its own fresh expired set by `expire_next`, so no arm inherits another's leftovers.
            let mut f = build(n, threads, &root);
            eprintln!("d98: N={n} rep={rep} fixture built in {:?}", f.build);

            let mut order = base.clone();
            let shift = rep % order.len();
            order.rotate_left(shift);

            for spec in order {
                let k = spec.k.unwrap_or_else(|| (n / expired_denom).max(1));
                let sweeps = spec.arm != Arm::Idle;
                let armed = if sweeps && k > 0 { f.expire_next(k) } else { 0 };
                if sweeps && armed < k {
                    eprintln!(
                        "d98:   {} wanted K={k} but the fixture had only {armed} branches left",
                        spec.arm.name()
                    );
                    short += 1;
                }

                let o = run_arm(&f, spec.arm, clients, probe_us, warmup);

                // ---- refusals, before any number is printed ------------------------------
                //
                // A sweep that reaped nothing has not been measured as fast; it has not been
                // measured. And a sweep no probe overlapped is a statement about the probe
                // spacing, not about the hold.
                let reaped_ok = !sweeps || o.reaped > 0;
                let seen_ok = !sweeps || o.overlap.n() > 0;
                if !reaped_ok {
                    vacuous += 1;
                }
                if reaped_ok && !seen_ok {
                    blind += 1;
                }
                let ok = reaped_ok && seen_ok;
                let cell = |v: u64| -> String {
                    if ok { v.to_string() } else { "--".into() }
                };

                // **reaps per acquisition is the load-immune statement of the whole claim.**
                // Before: one acquisition holds every reap, so it IS the number that expired and
                // it rises with the branch count. After: bounded by REAP_CHUNK whatever expires.
                let per_acq = if o.acquisitions == 0 {
                    "-".to_string()
                } else {
                    format!("{:.2}", o.reaped as f64 / o.acquisitions as f64)
                };
                println!(
                    "{:>8} {:>4} {:>7} {:>7} {:>8} {:>8} {:>12} {:>12} {:>12} {:>14} {:>7} {:>5} {:>8} {:>10}",
                    n,
                    rep,
                    spec.arm.name(),
                    if sweeps { armed.to_string() } else { "-".into() },
                    o.all.n(),
                    o.overlap.n(),
                    cell(o.overlap.pct(0.50)),
                    cell(o.overlap.pct(0.99)),
                    cell(o.overlap.max()),
                    o.sweep_wall,
                    o.reaped,
                    o.acquisitions,
                    per_acq,
                    o.done_during_sweep,
                );
                if sweeps {
                    eprintln!(
                        "d98:   {} K={armed} sweep_wall={}ns stall_max={}ns scans={} refused={} \
                         reaped={}",
                        spec.arm.name(),
                        o.sweep_wall,
                        o.overlap.max(),
                        o.scans,
                        o.refused,
                        o.reaped
                    );
                }

                // Pool for the summary. `idle` has no sweep to overlap, so its whole window IS
                // the population — that is what makes it the floor every other cell is read
                // against.
                let p = pools.entry((n, spec.arm.name(), if sweeps { k } else { 0 })).or_default();
                p.waits
                    .extend(if sweeps { o.overlap.0.iter() } else { o.all.0.iter() });
                if sweeps {
                    p.sweep_walls.push(o.sweep_wall);
                }
                p.reaped += o.reaped;
                p.acquisitions += o.acquisitions;
                p.done_during_sweep += o.done_during_sweep;
                p.reps += 1;
            }
            drop(f);
        }
    }

    let _ = std::fs::remove_dir_all(&root);

    // ---- the summary: statement latency, pooled across reps --------------------------------
    println!();
    println!("# SUMMARY — statement latency while the lease scan runs, pooled over {reps} rep(s).");
    println!(
        "# `idle` rows have no sweep, so their population is the whole window: the floor. Read \
         `locked` against `free` at the SAME N and K — that pair differs only in which lock the \
         sweep took."
    );
    println!(
        "# The two rightmost columns are COUNTS, not durations, and are the claim in a form fleet");
    println!(
        "# load cannot move: reap/acq is how many branches one acquisition of the statement lock");
    println!(
        "# reaped, and done_in_sw is how many client statements COMPLETED while the sweep ran.");
    println!(
        "{:>8} {:>7} {:>7} {:>9} {:>12} {:>12} {:>12} {:>14} {:>8} {:>6} {:>9} {:>11}",
        "N", "arm", "K", "samples", "p50_ns", "p99_ns", "max_ns", "sweep_wall_ns", "reaped",
        "acqs", "reap/acq", "done_in_sw"
    );
    for ((n, arm, k), p) in &pools {
        let s = Samples::of(p.waits.clone());
        let wall = if p.sweep_walls.is_empty() {
            0
        } else {
            let mut w = p.sweep_walls.clone();
            w.sort_unstable();
            w[w.len() / 2]
        };
        let per_acq = if p.acquisitions == 0 {
            "-".to_string()
        } else {
            format!("{:.2}", p.reaped as f64 / p.acquisitions as f64)
        };
        println!(
            "{:>8} {:>7} {:>7} {:>9} {:>12} {:>12} {:>12} {:>14} {:>8} {:>6} {:>9} {:>11}",
            n,
            arm,
            k,
            s.n(),
            s.pct(0.50),
            s.pct(0.99),
            s.max(),
            wall,
            p.reaped,
            p.acquisitions,
            per_acq,
            p.done_during_sweep
        );
    }

    println!();
    if vacuous > 0 {
        println!(
            "# REFUSED: {vacuous} cell(s) reaped nothing. A sweep that did no work has not been \
             measured as fast — it has not been measured."
        );
    }
    if blind > 0 {
        println!(
            "# REFUSED: {blind} cell(s) had no probe overlapping the sweep at {probe_us}us \
             spacing. The hold is shorter than one probe interval; tighten the spacing and say so, \
             do not quote the blind cell as fast."
        );
    }
    if short > 0 {
        println!(
            "# REFUSED: {short} arm(s) could not be given their full K because the fixture ran \
             out of unreaped branches. Those rows are credited with less work than they name."
        );
    }
    if vacuous + blind + short > 0 {
        std::process::exit(3);
    }
    println!("# every cell got its full K, reaped it, and was seen by at least one probe.");
}
