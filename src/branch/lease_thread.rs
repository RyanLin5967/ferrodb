//! F11 — the lease scan a **server** runs, so exit criterion 8 is a fact about the shipped binary.
//!
//! Design authority: DESIGN.md section 1 ("GC"), exit criterion 8, and `DISTRIBUTED.md` §F4 for
//! where the clock comes from.
//!
//! # What was missing, precisely
//!
//! [`crate::branch::Reaper::reap_expired`] and [`TwoTierReaper::resume_interrupted_reaps`] were
//! correct, tested, and **called by nothing outside tests and `examples/agent_isolation_demo.rs`**.
//! There was no background thread anywhere in `src/` — the only `thread::spawn` was `pgwire`'s
//! one-per-connection — so the thesis of the whole branch engine, *"an abandoned agent branch is
//! reclaimed with no client cooperation"*, held for a component and not for the database anyone
//! runs. This module is the caller.
//!
//! It also closes a second, quieter leak. Without a reaper attached to the runtime,
//! [`crate::agent_sql::runtime::AgentRuntime::seal`] takes its no-reaper branch: it marks a merged
//! or abandoned branch `Reaped` through the `BranchCatalog` trait and **never frees the extents the
//! branch allocated**. So in the shipped binary every `MERGE` and every `ABANDON` leaked its
//! branch's pages until the file was rebuilt. Both entry points now attach the reaper as well as
//! start this thread; the two are the same reclamation reached by cooperative and non-cooperative
//! doors.
//!
//! # The three rules this thread exists to get right
//!
//! **1. Resume before scanning.** `reap` marks a record `Reaping` durably *before* it frees
//! anything, so a crash in the middle leaves evidence instead of a leak — but that evidence is
//! only worth writing if something acts on it. A branch left `Reaping` is unreadable
//! (`check_readable` rejects it) while its extents are still charged to it, so the space is lost
//! for the life of the file. [`LeaseThread::start`] therefore runs
//! [`TwoTierReaper::resume_interrupted_reaps`] on the **calling** thread, before it spawns
//! anything, and returns its error to the caller rather than letting it disappear into a
//! background thread nobody is reading.
//!
//! **2. Never reap inside a merge.** A merge is an optimistic read of a branch followed by a
//! publication into its parent (`DESIGN.md` §4). Reaping the branch in that window frees its
//! arenas back to the free space map, from which the publication's own writes can immediately be
//! re-issued the same pages — and every such page still passes its checksum, so nothing
//! downstream can tell. The gate is [`RuntimeLock`], and the lock it names is **the one a
//! statement already holds**: `pgwire::serve` documents the catalog mutex as taken *outermost*,
//! for the duration of one statement, and `pgwire::extended::Statement::execute` is the single
//! place both the simple and the extended protocol run SQL. `MERGE` is therefore strictly inside
//! it. Taking that same lock adds no new lock and no new lock order — the alternative, a private
//! reap mutex, would be a third order to get wrong.
//!
//! **What the lock has to contain is a reap, not a sweep — D98.** This paragraph used to read
//! "a statement issued during a scan waits for the scan. Both are bounded by one reap", and the
//! body did not do that: [`scan_once`] held ONE acquisition across the clock read, the candidate
//! query, *every* reap on the tick and the reconciliation, so a statement waited for all of it.
//! Measured at 1 000 branches with 64 expired, a statement waited 3.71 s at the median
//! (`bench/d98_outer_runtime_lock.txt`). The sentence described the intended design and the code
//! ran a different one — the failure this project has now hit three times, and the reason a header
//! here is audited against its body rather than read.
//!
//! It is true as written now, and it takes TWO bounds, not one. The reaps run [`REAP_CHUNK`] at a
//! time, each group inside its own acquisition — that bounds how long the sweep HOLDS the lock.
//! Between groups it stands off for [`REAP_YIELD`] — that bounds how long a statement WAITS for
//! it, and without it the first bound buys a statement almost nothing, because `std::sync::Mutex`
//! is unfair and a sweep that unlocks and relocks is granted again before any waiter runs.
//! Measured: chunking alone left client p99 at 3.78 s against a 3.79 s sweep, i.e. the whole
//! sweep, with a correctly-bounded 3.76 reaps per acquisition all the while.
//!
//! Everything that does not need the lock — the clock, the candidate query, the error path's
//! O(open sessions) reconciliation, D88's orphan sweep — runs outside it. So a scan waits for
//! whatever statement is in flight, a statement issued during a scan waits for at most one reap
//! plus a stand-off instead of for the whole sweep, and the scan does no client I/O. A scan that
//! finds nothing expired now takes the lock **not at all**.
//!
//! Chunking does not weaken the guarantee rule 2 is about.
//! [`crate::branch::reaper::TwoTierReaper::reap_if_still_expired`] re-reads each record inside the
//! acquisition that is about to reap it, so a keepalive that lands after the candidate query
//! cannot have its branch reaped — the one thing whole-sweep atomicity provided for free. What
//! rule 2 needs is that no merge is running while a branch's extents go back to the free-space
//! map, and that is a property of one reap.
//!
//! **3. Never guess the time.** The clock is [`LeaseDeadline::try_now_millis`], i.e.
//! `cluster::lease_now_millis` — the local wall clock on a standalone node, the last applied
//! `Command::LeaseTick` on a cluster member. A member that has applied no tick does not know the
//! time, and this thread **refuses to reap** and says so, rather than substituting a reading:
//! reaping is destructive and a `BranchId` generation makes a wrong reap unrecoverable. The
//! refusal is counted in [`LeaseStats::refused_scans`] so "it is not reaping" and "it cannot" are
//! distinguishable from outside.
//!
//! **4 — D127. A refusal about ONE branch is a reading too, and it must reach a reader.**
//! Rule 3 is the whole-scan case and was always reported. The per-branch case was not:
//! `reap_if_still_expired` returned a `bool`, `false` meant both *"the lease was renewed under
//! us"* and *"the catalog would not answer"*, and this function dropped the second on the floor —
//! so D124's guard, whose entire output is a sentence naming the parent, the fork epoch and the
//! child, fired into nothing. The outcome type is now three-way
//! ([`crate::branch::ReapOutcome`]), the refusals are counted in
//! [`LeaseStats::refused_branches`], and their reasons are printed by [`refusal_report`]. The
//! sweep still continues past one — that is what the absorption is FOR — it just no longer
//! continues *quietly*.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::agent_sql::runtime::AgentRuntime;
use crate::branch::types::{BranchId, LeaseDeadline};
// `Reaper` is deliberately NOT imported: since D98 this module reaches the reaper only through its
// inherent methods (`resume_interrupted_reaps`, `expired_candidates`, `reap_if_still_expired`,
// `collect_orphans_if_due`), because the trait's `reap_expired` is the whole-sweep shape whose
// hold time is what this row removed.
use crate::branch::{ReapOutcome, TwoTierReaper};
use crate::catalog::catalog::Catalog;
use crate::error::FerroError;

/// How often a server scans for expired leases when nothing says otherwise.
///
/// Well under `DEFAULT_LEASE_MILLIS` (15 minutes), because the interval is the worst-case delay
/// between a lease expiring and its pages coming back, and it costs one pass over the live branch
/// records.
///
/// It used to say it was not shorter because "a scan takes the statement lock ... and a database
/// with nothing expired should not be taking that lock more often than it has to". Since D98 a
/// scan that finds nothing expired takes that lock zero times — the candidate query runs outside
/// it and the loop that acquires it has nothing to iterate — so that is no longer the reason. What
/// remains is the cost of the query itself: one deadline-index descent per tick against the branch
/// catalog, which is cheap but not free.
pub const DEFAULT_SCAN_MILLIS: u64 = 30_000;

/// Name of the environment variable [`scan_interval_from_env`] reads.
pub const SCAN_INTERVAL_ENV: &str = "FERRODB_LEASE_SCAN_MILLIS";

/// Longest interval that is still a lease scan rather than a switch that turns one off.
const MAX_SCAN_MILLIS: u64 = 24 * 60 * 60 * 1000;

/// The scan interval for this process: [`DEFAULT_SCAN_MILLIS`], or `FERRODB_LEASE_SCAN_MILLIS`.
///
/// **An unusable value is refused, never defaulted, and there is no "off".** Both halves are
/// deliberate. A knob that silently fell back would let an operator who set five seconds run on
/// thirty and read the delay as a reaper that does not work; and a value meaning "do not scan" is
/// a switch that turns exit criterion 8 off from the environment, which is the bug this whole
/// module exists to remove. `0` is therefore refused by name rather than treated as a disable.
///
/// Same shape as `consensus::tests_sim`'s `FERRODB_SIM_SEEDS`, and for the same stated reason.
pub fn scan_interval_from_env() -> Result<Duration, FerroError> {
    match std::env::var(SCAN_INTERVAL_ENV) {
        Err(_) => Ok(Duration::from_millis(DEFAULT_SCAN_MILLIS)),
        Ok(v) => parse_scan_interval(&v),
    }
}

/// The rule [`scan_interval_from_env`] applies, as a function of the string alone.
///
/// Separated so it can be tested without writing to the environment: `cargo test` runs a binary's
/// tests as threads of one process, and in edition 2024 `set_var` is `unsafe` precisely because a
/// test that set this variable would be setting it for every sibling test at the same time.
fn parse_scan_interval(raw: &str) -> Result<Duration, FerroError> {
    let refuse = |why: String| {
        Err(FerroError::Io(format!(
            "{SCAN_INTERVAL_ENV} is {raw:?}: {why}. Refusing to start rather than fall back to \
             {DEFAULT_SCAN_MILLIS}ms, because an operator who set this and got the default would \
             read the difference as a reaper that does not work. There is no value that disables \
             the scan: abandoned branches are reclaimed with no client cooperation, and a switch \
             for that would be a switch for exit criterion 8."
        )))
    };
    let millis: u64 = match raw.trim().parse() {
        Ok(n) => n,
        Err(e) => return refuse(format!("it takes a whole number of milliseconds ({e})")),
    };
    if millis == 0 {
        return refuse("a zero interval is not a scan period".into());
    }
    if millis > MAX_SCAN_MILLIS {
        return refuse(format!("it is longer than the {MAX_SCAN_MILLIS}ms ceiling"));
    }
    Ok(Duration::from_millis(millis))
}

/// **The lock a merge holds, which a lease scan must run outside of.**
///
/// One method, taking the body rather than returning a guard, because the guard's type differs
/// between the two callers and the *rule* does not: whatever this holds must contain every
/// `MERGE`, and must be released before `with_runtime_lock` returns.
///
/// See rule 2 in the module header for why the implementations hand over the existing outermost
/// statement lock instead of introducing a lock of their own.
pub trait RuntimeLock: Send + Sync {
    /// Run `body` exactly once with the lock held.
    ///
    /// Exactly once is a contract, not a suggestion: [`with_lock`] refuses rather than inventing a
    /// result if an implementation skips the body, because a gate that silently declined to run
    /// the scan would present as a database that quietly stopped reaping.
    fn with_runtime_lock(&self, body: &mut dyn FnMut());
}

/// Run `f` under `lock` and hand back what it returned.
///
/// The bridge between [`RuntimeLock`]'s `FnMut()` shape — forced by the trait needing to be object
/// safe — and a body that produces a value or an error.
fn with_lock<T>(lock: &dyn RuntimeLock, f: impl FnOnce() -> T) -> T {
    let mut once = Some(f);
    let mut out = None;
    let mut body = || {
        if let Some(f) = once.take() {
            out = Some(f());
        }
    };
    lock.with_runtime_lock(&mut body);
    out.expect(
        "a RuntimeLock implementation returned without running the lease scan. It must call its \
         body exactly once; skipping it silently stops a database reaping.",
    )
}

/// The pgwire server's statement lock, offered to the lease scan.
///
/// `ServerContext::catalog()` is the outermost lock `pgwire::serve` documents, held for exactly one
/// statement, and it already declines to propagate poisoning for a reason that applies here too: a
/// connection that panicked mid-statement must not stop the database reaping for the rest of its
/// life.
impl RuntimeLock for crate::pgwire::ServerContext {
    fn with_runtime_lock(&self, body: &mut dyn FnMut()) {
        let _statement = self.catalog();
        body();
    }
}

/// The CLI's half of [`RuntimeLock`].
///
/// The REPL has no `ServerContext`, and before F11 it needed none: it was single-threaded and owned
/// its `Catalog` outright. A lease thread makes it two threads, so the catalog moves behind a mutex
/// and is locked for exactly one statement — the identical rule, by hand, in the one other place
/// that runs SQL.
pub struct CatalogLock(Arc<Mutex<Catalog>>);

impl CatalogLock {
    pub fn new(catalog: Catalog) -> CatalogLock {
        CatalogLock(Arc::new(Mutex::new(catalog)))
    }

    /// The catalog, for the duration of one statement.
    ///
    /// Poisoning is absorbed for the same reason `ServerContext::catalog` absorbs it: what is
    /// behind the lock is the on-disk catalog, which is reloadable, and a panic in one statement
    /// must not end the session.
    pub fn lock(&self) -> MutexGuard<'_, Catalog> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl RuntimeLock for CatalogLock {
    fn with_runtime_lock(&self, body: &mut dyn FnMut()) {
        let _statement = self.lock();
        body();
    }
}

/// What this thread has done, readable while it runs.
///
/// `attempts` and `scans` are separate on purpose, and the gap between them is the only thing that
/// tells "the scan is waiting for a statement" apart from "the thread died": a thread blocked on
/// the runtime lock keeps raising `attempts` and never raises `scans`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LeaseStats {
    /// Scans begun, counted **before** the runtime lock is acquired.
    pub attempts: u64,
    /// Scans that ran to completion without failing.
    ///
    /// This used to read "scans that ran with the runtime lock held", and since D98 that is no
    /// longer what it counts: a scan that finds nothing expired completes without ever taking the
    /// lock, and one that finds many takes it once per [`REAP_CHUNK`]. It still separates "the
    /// scan is waiting for a statement" from "the thread died" — see the note on this struct —
    /// because a thread blocked inside a chunk raises `attempts` and not this.
    pub scans: u64,
    /// Branches reaped by those scans.
    pub reaped: u64,
    /// Workspaces dropped by [`AgentRuntime::forget_reaped_branches`] afterwards.
    pub forgotten: u64,
    /// **Whole scans** that refused because this node does not know the cluster's time.
    ///
    /// Named `refused` until D127, when a second, unrelated refusal became countable. The rename
    /// is the point of that row applied to its own instrument: a field called `refused` sitting
    /// next to `refused_branches` is the same conflation one level up, and the first reader to
    /// quote the wrong one would be reporting a node with no clock as a corrupt catalog.
    pub refused_scans: u64,
    /// **D127 — individual branches a scan declined to decide about**, summed over every scan.
    ///
    /// Distinct from [`LeaseStats::reaped`] (it freed nothing), from `candidates - reaped` (a
    /// branch whose lease was renewed under the sweep is healthy and is not counted here), and
    /// from [`LeaseStats::failed`] (the sweep did not stop; the other expired branches were still
    /// reclaimed). Each one keeps its pages and is retried on the next scan, and each one is
    /// printed with its reason — see `refusal_report`.
    ///
    /// **A steady trickle is the benign race** `TableBranchCatalog::upsert` opens: delete-then-
    /// insert with no latch across the two, so a concurrent `set_root` or `renew_lease` makes a
    /// healthy branch's record miss for a moment. **A number that climbs and never comes back is
    /// not**: it is a branch that can no longer be reaped, i.e. pages that never return. That is
    /// the difference this counter exists to make visible, and before D127 neither case reached
    /// any reader at all.
    pub refused_branches: u64,
    /// Scans whose reap returned an error.
    pub failed: u64,
}

#[derive(Default)]
struct Counters {
    attempts: AtomicU64,
    scans: AtomicU64,
    reaped: AtomicU64,
    forgotten: AtomicU64,
    refused_scans: AtomicU64,
    refused_branches: AtomicU64,
    failed: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> LeaseStats {
        LeaseStats {
            attempts: self.attempts.load(Ordering::SeqCst),
            scans: self.scans.load(Ordering::SeqCst),
            reaped: self.reaped.load(Ordering::SeqCst),
            forgotten: self.forgotten.load(Ordering::SeqCst),
            refused_scans: self.refused_scans.load(Ordering::SeqCst),
            refused_branches: self.refused_branches.load(Ordering::SeqCst),
            failed: self.failed.load(Ordering::SeqCst),
        }
    }
}

/// Stop flag and the condition variable that makes stopping prompt rather than one interval away.
struct Halt {
    stopping: Mutex<bool>,
    wake: Condvar,
}

impl Halt {
    fn is_stopping(&self) -> bool {
        *self.stopping.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sleep for `interval`, or until [`Halt::signal`]. Returns whether the thread should stop.
    ///
    /// A plain `thread::sleep` here would make `stop()` take up to one whole interval, which at the
    /// 30-second default is a server that appears to hang on shutdown.
    fn wait(&self, interval: Duration) -> bool {
        let guard = self.stopping.lock().unwrap_or_else(PoisonError::into_inner);
        if *guard {
            return true;
        }
        let (guard, _timeout) = self
            .wake
            .wait_timeout(guard, interval)
            .unwrap_or_else(PoisonError::into_inner);
        *guard
    }

    fn signal(&self) {
        *self.stopping.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.wake.notify_all();
    }
}

/// The background lease scan, owned by whoever owns the database.
///
/// Dropping it stops the thread and joins it, so a caller cannot leave a scan running against a
/// store it is about to checkpoint and close. [`LeaseThread::stop`] is the same shutdown with the
/// final [`LeaseStats`] handed back.
pub struct LeaseThread {
    counters: Arc<Counters>,
    halt: Arc<Halt>,
    handle: Option<std::thread::JoinHandle<()>>,
    resumed: Vec<BranchId>,
}

impl LeaseThread {
    /// Finish any interrupted reap, then scan every `interval` until stopped.
    ///
    /// The resume runs **here**, on the caller's thread and under the runtime lock, so that a
    /// caller holding an `Ok` knows the database has no half-reaped branch left in it, and so that
    /// a failure is returned rather than printed into a background thread's stderr. Callers should
    /// therefore call this before they begin serving.
    ///
    /// `reaper` is a concrete [`TwoTierReaper`] rather than a `dyn Reaper` because
    /// `resume_interrupted_reaps` is an inherent method: the trait carries only the steady-state
    /// half, and the resume is exactly the half a server must not skip.
    pub fn start(
        reaper: Arc<TwoTierReaper>,
        runtime: Arc<AgentRuntime>,
        lock: Arc<dyn RuntimeLock>,
        interval: Duration,
    ) -> Result<LeaseThread, FerroError> {
        let resumed = with_lock(&*lock, || reaper.resume_interrupted_reaps())?;
        if !resumed.is_empty() {
            // The reaper reclaimed pages without any client asking, so the runtime still holds
            // those branches' workspaces; nothing else will ever tell it.
            let forgotten = runtime.forget_reaped_branches();
            report(format!(
                "lease: finished {} reap(s) a crash interrupted ({}); {} workspace(s) forgotten",
                resumed.len(),
                join_ids(&resumed),
                forgotten
            ));
        }

        let counters = Arc::new(Counters::default());
        let halt = Arc::new(Halt { stopping: Mutex::new(false), wake: Condvar::new() });
        let handle = {
            let counters = Arc::clone(&counters);
            let halt = Arc::clone(&halt);
            std::thread::Builder::new()
                .name("ferrodb-lease".into())
                .spawn(move || loop {
                    if halt.is_stopping() {
                        return;
                    }
                    scan_once(&reaper, &runtime, &*lock, &counters, &report);
                    if halt.wait(interval) {
                        return;
                    }
                })
                .map_err(|e| {
                    FerroError::Io(format!(
                        "could not spawn the lease scan thread: {e}. Refusing to continue without \
                         it: a database that cannot reap grows without bound as agent branches are \
                         abandoned, and nothing else in the process would report the absence."
                    ))
                })?
        };

        Ok(LeaseThread { counters, halt, handle: Some(handle), resumed })
    }

    /// Branches whose interrupted reap [`LeaseThread::start`] finished.
    pub fn resumed(&self) -> &[BranchId] {
        &self.resumed
    }

    /// What the thread has done so far. Safe to read while it runs.
    pub fn stats(&self) -> LeaseStats {
        self.counters.snapshot()
    }

    /// Stop the thread, join it, and return its final counters.
    ///
    /// Prompt: it does not wait out the current interval. A scan already inside the runtime lock is
    /// allowed to finish — interrupting one would leave the reap half done, which is the state
    /// rule 1 exists to clean up rather than to create.
    pub fn stop(mut self) -> LeaseStats {
        self.shutdown();
        self.counters.snapshot()
    }

    fn shutdown(&mut self) {
        self.halt.signal();
        if let Some(h) = self.handle.take() {
            // A panicked scan thread is reported, not propagated: this runs from `Drop` as well,
            // and panicking there during an unwind aborts the process.
            if h.join().is_err() {
                report("lease: the scan thread panicked; no further leases will be reaped in this process".to_string());
            }
        }
    }
}

impl Drop for LeaseThread {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// How many expired branches one acquisition of the runtime lock is allowed to reap.
///
/// **This constant is what stops the hold being a function of the branch count.** It is a tuning
/// number; the shape is that there IS one. Whatever it is set to, an unrelated statement waits for
/// at most this many reaps rather than for every branch that happened to expire on the same tick,
/// and that is the property D98 is about.
///
/// One, because a reap is dominated by a durable catalog write — measured at ~58 ms each on this
/// machine, fsync-bound — so a chunk of four is a four-fsync stall for no benefit that survived
/// being measured.
///
/// ⛔ **This said four, and the argument for four was wrong.** It read: "It is not 1: the lock is
/// released between chunks and clients are queued on it, so the sweep goes to the back of the
/// queue every time it lets go — a chunk of one makes a sweep with many candidates as slow as the
/// queue is long." That is a plausible mechanism and it does not happen. Measured with
/// `examples/outer_runtime_lock.rs` at N=500, K=64, 16 clients, everything else identical:
///
/// | REAP_CHUNK | client p99 | client max | statements done during sweep | sweep wall |
/// |---|---|---|---|---|
/// | 4 | 366 ms | 438 ms |  4 080 | 5.03 s |
/// | 1 | **66.6 ms** | **91.0 ms** | **16 288** | **3.66 s** |
///
/// The sweep did not go to the back of any queue: at one reap per acquisition it finished
/// *sooner*, because [`REAP_YIELD`] lets the waiting clients drain in microseconds instead of
/// leaving them to fight the sweep for the lock. A chunk of one is better on every column,
/// including the one the argument for four was trying to protect.
///
/// `bench/d98_outer_runtime_lock.txt` reports the stall and the sweep's own wall time in the same
/// table, so buying latency by starving the sweep would show up there rather than being traded
/// silently.
const REAP_CHUNK: usize = 1;

/// A chunk size of zero would make `chunks()` panic, and it is a constant, so the wrong value is a
/// build failure rather than a lease thread that dies on its first tick in production.
const _: () = assert!(REAP_CHUNK > 0);

/// How long the sweep stands off the runtime lock between chunks, so a waiting statement gets it.
///
/// **This is not politeness, it is the other half of the bound.** See the call site: chunking caps
/// how long one acquisition lasts, and this caps how long a statement waits, because an unfair
/// mutex otherwise lets the re-acquiring sweep jump waiters for the length of the whole sweep —
/// measured, and the reason this constant exists.
///
/// One millisecond against a chunk that is a durable catalog write. It is deliberately a fixed
/// interval and not a fraction of the hold: a proportional back-off would make a slow disk slow
/// the reclaim down quadratically, and reclaim that falls behind is the unbounded growth this
/// whole module exists to prevent.
///
/// ⚠ **The cost is a function of how fast a reap is, and that is stated rather than hidden.** At
/// the ~58 ms per reap measured here it is under 2% of the sweep. A reap 100× faster would make
/// this the dominant term. It is still the right default in that world — a waiting statement is
/// bounded by one reap plus this, in every regime, and being wrong towards fairness is the safe
/// direction for a background task — but whoever makes reaps that fast should re-read this
/// constant, and the `sweep_wall` column in `bench/d98_outer_runtime_lock.txt` is where the trade
/// would show up. A conditional yield ("only stand off if the chunk was slow") was considered and
/// rejected: in the fast-reap regime it declines to yield and reintroduces exactly the starvation
/// this constant exists to remove, which is the failure mode that is hardest to notice.
const REAP_YIELD: Duration = Duration::from_millis(1);

/// One pass: read the cluster's time, reap what has expired, then let the runtime forget it.
///
/// # D98 — what is inside the runtime lock, and what is not
///
/// This whole body used to sit inside one `with_lock`, and in both production shapes that lock is
/// the mutex every SQL statement takes. So the length of one lease scan was the length of a stall
/// on every connection in the database — and the scan's length is a function of how many branches
/// expired on that tick, which in an agent fleet is a function of how many agents there are.
/// Measured before the change at N=1 000 branches with 64 of them expired: a statement waited
/// **3.71 s at the median**, against 333 ns in a control arm running the identical sweep under a
/// private lock (`bench/d98_outer_runtime_lock.txt`).
///
/// Three things moved out, each for a reason of its own:
///
/// - **The clock read**, which touches no catalog at all.
/// - **The candidate query**, which reads the BRANCH catalog. That is not the table catalog
///   statements serialise on, and it frees nothing; this is D88's argument for the orphan sweep,
///   applied to the other read the scan does.
/// - **The reconciliation on the error path**, which is O(open sessions) and walks
///   `AgentRuntime`'s own state under `AgentRuntime`'s own mutex. It was the largest remaining
///   unbounded hold and `bench/w4/DECISION.md` lists it as open. Moving it out costs nothing that
///   was actually held: `forget_reaped_branches` already releases its state lock every
///   `FORGET_CHUNK` entries, so "the reap and the forget are one atomic step" has not been true
///   since that chunking landed, and nothing depends on it.
///
/// What stays inside is the reap itself, in groups of [`REAP_CHUNK`] — because rule 2 in the
/// module header is about exactly that: a merge must not be running while a branch's extents go
/// back to the free-space map. That needs the reap of one branch to be inside the lock. It never
/// needed the whole sweep to be inside one acquisition, and the difference between those two
/// readings is this entire row.
/// One lease scan.
///
/// `out` is where everything this pass has to say goes. It is a parameter rather than a direct
/// call to [`report`] because of D127: the row is *"the refusal reaches no reader"*, and a
/// function that writes to the process's stderr cannot be asked, by a test, whether a reader would
/// have seen anything. Production passes `&report`; the tests pass a collector and assert on the
/// text. Without this seam the only assertable half of a refusal would be its counter, and a
/// counter with no accompanying reason is most of the defect still standing.
fn scan_once(
    reaper: &TwoTierReaper,
    runtime: &AgentRuntime,
    lock: &dyn RuntimeLock,
    counters: &Counters,
    out: &dyn Fn(String),
) {
    // Before the lock, so a scan blocked on a statement is visibly a scan that is waiting rather
    // than a thread that has died.
    counters.attempts.fetch_add(1, Ordering::SeqCst);

    // **Outside the lock.** A refusal is counted and reported, never rounded to a number. It used
    // to be read inside, for "the freshest reading that can still be acted on without another
    // statement intervening" — but what acts on the reading is `reap_if_still_expired`, which
    // re-reads each record inside the lock and refuses a branch whose lease moved. The freshness
    // that argument wanted is now enforced where the decision is made instead of being inferred
    // from where the clock was read.
    let now = match LeaseDeadline::try_now_millis() {
        Ok(now) => now,
        Err(e) => {
            counters.refused_scans.fetch_add(1, Ordering::SeqCst);
            out(format!(
                "lease: NOT reaping — this node does not know the cluster's time ({e}). \
                 Expired branches keep their pages until a LeaseTick is applied; reaping on a \
                 local clock is the divergence this refusal exists to prevent."
            ));
            return;
        }
    };

    // **Outside the lock.** An index descent on the branch catalog, which has its own lock.
    let candidates = match reaper.expired_candidates(now) {
        Ok(c) => c,
        Err(e) => {
            counters.failed.fetch_add(1, Ordering::SeqCst);
            out(format!(
                "lease: could not ask which branches have expired: {e}. Nothing was freed and \
                 nothing is lost; the next scan asks again."
            ));
            return;
        }
    };

    // ---- the reaps: inside the lock, [`REAP_CHUNK`] at a time -------------------------------
    //
    // Consumed in the order `expired_candidates` produced — deepest first — because reaping a
    // child is what lets its parent's own reap take the fast path. Chunking must not reorder it.
    let mut reaped_all: Vec<BranchId> = Vec::new();
    // **D127.** Refusals seen this pass, with the reason each one carried. Accumulated across
    // chunks and printed once at the end rather than inside `with_lock`: writing to stderr is I/O,
    // and rule 2's whole point is that this function does no client-blocking work while it holds
    // the statement lock. Bounded by the candidate list, which is what `reaped_all` is bounded by
    // too, so this adds no new growth.
    let mut refused_all: Vec<(BranchId, FerroError)> = Vec::new();
    let mut forgotten_all = 0usize;
    let mut failure: Option<FerroError> = None;
    let mut groups = candidates.chunks(REAP_CHUNK).peekable();
    while let Some(group) = groups.next() {
        let mut reaped: Vec<BranchId> = Vec::with_capacity(group.len());
        let mut refused: Vec<(BranchId, FerroError)> = Vec::new();
        with_lock(lock, || {
            for rec in group {
                match reaper.reap_if_still_expired(rec.branch_id(), now) {
                    Ok(ReapOutcome::Reaped) => reaped.push(rec.branch_id()),
                    // Its lease moved between the candidate query and the re-read inside this
                    // acquisition. The branch is healthy; there is nothing to say about it.
                    Ok(ReapOutcome::NotExpired) => {}
                    // **D127 — the arm this row exists for.** It used to be spelled the same as
                    // the one above, which is how D124's refusal reached no reader. The sweep
                    // still goes on to the next candidate; the difference is that this one is now
                    // remembered, counted and printed with its reason.
                    Ok(ReapOutcome::Refused(why)) => refused.push((rec.branch_id(), why)),
                    Err(e) => {
                        failure = Some(e);
                        return;
                    }
                }
            }
            if reaped.is_empty() {
                return;
            }
            // **Forget exactly what was reaped, not everything that might have been.**
            //
            // This used to call `forget_reaped_branches`, which re-derives the set by walking
            // every open session and asking the catalog about each one — O(open sessions),
            // inside this `with_lock`, which is the pgwire server's PER-STATEMENT mutex. So a
            // timer stopped every statement in the database for the length of that walk. The
            // list is right here; searching for what we were already handed was the whole cost.
            //
            // It stays inside the lock because it is now O(this chunk), which is bounded by
            // construction: moving it out would buy nothing and would widen the window in which
            // a statement can find a workspace whose branch is already gone.
            //
            // Measured in `bench/w4/statement-lock-FASTPATH.txt`: the reconciliation's
            // wall time — which is what this lock is held for — rises 91x across 100x open
            // sessions (269 us -> 24.5 ms at 10⁵), while this call shows no trend because it is
            // O(reaped). A larger figure for the same walk is reported on branch
            // S15-runtime-at-1e6 (commit 0ac1931, `bench/runtime_at_1e6.txt`, W4); that file is
            // not in this worktree and the number is NOT reproduced here, so it is motivation
            // rather than evidence.
            forgotten_all += runtime.forget_branches(&reaped);
        });
        reaped_all.append(&mut reaped);
        refused_all.append(&mut refused);
        if failure.is_some() {
            break;
        }
        // **Chunking bounds the HOLD. This is what bounds the WAIT — and without it the chunking
        // buys a statement almost nothing.**
        //
        // `std::sync::Mutex` does not queue fairly: a thread that unlocks and immediately relocks
        // can be granted again before any waiter is scheduled. The reap loop does exactly that, so
        // a statement blocked at the start of a sweep was still being jumped by the sweep for the
        // whole sweep. Measured with `examples/outer_runtime_lock.rs` at N=500, K=64, 16 clients:
        // `reap/acq` was already a correct 3.76, so the hold WAS bounded — and client p99 was
        // still 3.78 s against a 3.79 s sweep, i.e. the entire sweep, because the waiters never
        // got the lock. Only 80 statements completed in that window.
        //
        // Sleeping rather than `yield_now`: a yield is a hint the scheduler may decline while this
        // thread is still runnable, which is the case that produced the number above. Descheduling
        // for a bounded interval means the waiters definitely run. The cost is one interval per
        // chunk against a chunk that is several durable writes — at the reap cost measured here
        // (~58 ms each, fsync-bound) this is well under 1% of the sweep, and the sweep's own wall
        // time is reported beside the stall in the artifact so the trade is visible rather than
        // asserted.
        //
        // Skipped after the final chunk: there is no one left to yield to, and a lease scan should
        // not add a delay to its own completion for nothing.
        if groups.peek().is_some() {
            std::thread::sleep(REAP_YIELD);
        }
    }

    // **D127 — before the success/failure split, because a refusal is neither.**
    //
    // A sweep that refused on one branch and reaped six others is a successful sweep by every
    // other measure here, and a sweep that later failed outright still refused on whatever it
    // refused on. Putting this inside either arm would make the signal depend on what happened to
    // a *different* branch afterwards, which is one more way for it to go quiet.
    if !refused_all.is_empty() {
        counters.refused_branches.fetch_add(refused_all.len() as u64, Ordering::SeqCst);
        out(refusal_report(&refused_all));
    }

    match failure {
        None => {
            // Counted once per pass, including a pass that found nothing: `scans` is what tells
            // "the thread is scanning and there is nothing to do" apart from "the thread is stuck",
            // and a counter that only moved when something expired could not say that.
            counters.scans.fetch_add(1, Ordering::SeqCst);
            if !reaped_all.is_empty() {
                counters.reaped.fetch_add(reaped_all.len() as u64, Ordering::SeqCst);
                counters.forgotten.fetch_add(forgotten_all as u64, Ordering::SeqCst);
                out(format!(
                    "lease: reaped {} expired branch(es) with no client cooperation ({}); {} \
                     workspace(s) forgotten",
                    reaped_all.len(),
                    join_ids(&reaped_all),
                    forgotten_all
                ));
            }
        }
        Some(e) => {
            counters.failed.fetch_add(1, Ordering::SeqCst);
            counters.reaped.fetch_add(reaped_all.len() as u64, Ordering::SeqCst);
            counters.forgotten.fetch_add(forgotten_all as u64, Ordering::SeqCst);
            // **The one path where the list cannot be trusted, so the full sweep runs.**
            //
            // A reap can fail after freeing part of what it names, and `sweep_empty_extents` can
            // fail after a clean loop; either way a branch can be gone from the catalog without
            // this pass being able to name it, so the fast path above cannot see it and its
            // workspace would leak for the life of the process. The reconciliation covers that,
            // and this is the only tick that has to pay for it.
            //
            // **D98: it runs OUTSIDE the lock.** It is O(open sessions) — the largest hold left in
            // this function — and it needs none of the table catalog: it walks `AgentRuntime`'s
            // own state under `AgentRuntime`'s own mutex, releasing it every `FORGET_CHUNK`
            // entries, and asks the BRANCH catalog about the candidates. That it already lets go
            // of its own lock between chunks is also why moving it out gives nothing up: this was
            // never one atomic step with the reap.
            let forgotten = runtime.forget_reaped_branches();
            counters.forgotten.fetch_add(forgotten as u64, Ordering::SeqCst);
            out(format!(
                "lease: scan failed: {e}. Whatever it had already freed is durable and `reap` \
                 is re-entrant, so the next scan resumes rather than double-freeing. \
                 {forgotten} workspace(s) forgotten by reconciliation, because a failed scan \
                 does not report which branches it had already reaped."
            ));
        }
    }

    // **D88: the orphan sweep runs OUTSIDE the statement lock.**
    //
    // `with_lock` above is the table-catalog mutex in both production shapes, so anything inside
    // it stalls every connection for its duration. `collect_orphans_if_due` is O(live arenas) and
    // does not touch the table catalog at all — it reads `live_arenas`, asks the BRANCH catalog
    // whether each owner is dead, and frees through the page store, each of which has its own
    // lock. Running it here keeps the cadence and the work identical and removes the stall.
    //
    // ⚠ Ordered AFTER the reap, not before: a reap is what produces collectable extents, so
    // sweeping first would always be one tick behind. And any error is reported rather than
    // propagated — a failed mop-up must not stop the next lease scan, and `open` collects the
    // same extents anyway.
    //
    // D98 note: this no longer needs a `sweep_at` carried out of a closure, because the reading it
    // must use is now an ordinary local — but the rule that produced that variable is unchanged
    // and is why `now` is read once for the whole pass rather than re-read here.
    if let Err(e) = reaper.collect_orphans_if_due(now) {
        out(format!(
            "lease: orphan sweep failed: {e}. Nothing is lost — the complete answer is recomputed \
             at open by `resume_interrupted_reaps`, and the cadence retries."
        ));
    }
}

fn join_ids(ids: &[BranchId]) -> String {
    ids.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(", ")
}

/// How many refusals a single scan spells out in full before it starts summarising.
///
/// Every refusal is counted; this bounds only how much of one pass's stderr one tick may be. The
/// reasons are paragraphs — D124's is six lines naming a parent, a fork epoch and a child — so an
/// uncapped list on a database with a thousand expired branches would be a log nobody reads,
/// which is the same failure as a log nobody writes.
const REFUSAL_DETAIL_CAP: usize = 4;

/// **D127 — what a refused reap looks like to whoever is reading the server's stderr.**
///
/// A free function taking the list, rather than a `format!` buried in [`scan_once`], for one
/// reason: it makes the operator-facing text assertable. The row's finding was that a real event
/// reached no reader, and a test that can only check a counter cannot tell a report that names the
/// branch from a report that says "something happened".
///
/// The count is always exact and always first; only the reasons are capped. That ordering is
/// deliberate — the number is what says whether this is the benign mid-rewrite race or a leak, and
/// it must not be the thing that falls off the end of a truncated line.
fn refusal_report(refused: &[(BranchId, FerroError)]) -> String {
    let ids: Vec<BranchId> = refused.iter().map(|(b, _)| *b).collect();
    let mut msg = format!(
        "lease: REFUSED to decide on {} expired branch(es) ({}). Nothing was freed, so nothing is \
         lost, and the rest of this sweep ran. These are NOT branches whose lease was renewed \
         under the sweep — those are healthy and are not reported: the catalog would not answer \
         for these. A refusal raised before the reap began leaves the branch Live and the next \
         scan asks again; one raised after it began leaves the record Reaping, which the next \
         scan cannot see (expired_before is Live-only) and which resume_interrupted_reaps \
         re-enters at the next open. A count that does not come back down is a branch that can no \
         longer be reaped: pages that never return.",
        refused.len(),
        join_ids(&ids)
    );
    for (branch, why) in refused.iter().take(REFUSAL_DETAIL_CAP) {
        msg.push_str(&format!("\nlease:   {branch}: {why}"));
    }
    if refused.len() > REFUSAL_DETAIL_CAP {
        msg.push_str(&format!(
            "\nlease:   ... and {} more refusal(s) this scan, reasons not printed (cap {}); every \
             one is counted in LeaseStats::refused_branches.",
            refused.len() - REFUSAL_DETAIL_CAP,
            REFUSAL_DETAIL_CAP
        ));
    }
    msg
}

/// Say something on stderr, tolerating a closed one.
///
/// `eprintln!` **panics** on `EPIPE`, and this runs on a detached thread whose panic nobody sees —
/// so the observable result of a closed stderr would be a database that silently stopped reaping.
/// `examples/pgserver.rs` already avoids `println!` on its stdout for the same reason, proven
/// there against a real harness that drops its reader.
fn report(msg: String) {
    use std::io::Write;
    let _ = writeln!(std::io::stderr(), "{msg}");
}

#[cfg(test)]
mod tests;
