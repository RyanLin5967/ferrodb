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
//! The cost is stated rather than hidden: a scan waits for whatever statement is in flight, and a
//! statement issued during a scan waits for the scan. Both are bounded by one reap, and the scan
//! does no client I/O.
//!
//! **3. Never guess the time.** The clock is [`LeaseDeadline::try_now_millis`], i.e.
//! `cluster::lease_now_millis` — the local wall clock on a standalone node, the last applied
//! `Command::LeaseTick` on a cluster member. A member that has applied no tick does not know the
//! time, and this thread **refuses to reap** and says so, rather than substituting a reading:
//! reaping is destructive and a `BranchId` generation makes a wrong reap unrecoverable. The
//! refusal is counted in [`LeaseStats::refused`] so "it is not reaping" and "it cannot" are
//! distinguishable from outside.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::agent_sql::runtime::AgentRuntime;
use crate::branch::types::{BranchId, LeaseDeadline};
use crate::branch::{Reaper, TwoTierReaper};
use crate::catalog::catalog::Catalog;
use crate::error::FerroError;

/// How often a server scans for expired leases when nothing says otherwise.
///
/// Well under `DEFAULT_LEASE_MILLIS` (15 minutes), because the interval is the worst-case delay
/// between a lease expiring and its pages coming back, and it costs one pass over the live branch
/// records. It is not shorter because a scan takes the statement lock (rule 2 above), and a
/// database with nothing expired should not be taking that lock more often than it has to.
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
    /// Scans that ran with the runtime lock held.
    pub scans: u64,
    /// Branches reaped by those scans.
    pub reaped: u64,
    /// Workspaces dropped by [`AgentRuntime::forget_reaped_branches`] afterwards.
    pub forgotten: u64,
    /// Scans that refused because this node does not know the cluster's time.
    pub refused: u64,
    /// Scans whose reap returned an error.
    pub failed: u64,
}

#[derive(Default)]
struct Counters {
    attempts: AtomicU64,
    scans: AtomicU64,
    reaped: AtomicU64,
    forgotten: AtomicU64,
    refused: AtomicU64,
    failed: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> LeaseStats {
        LeaseStats {
            attempts: self.attempts.load(Ordering::SeqCst),
            scans: self.scans.load(Ordering::SeqCst),
            reaped: self.reaped.load(Ordering::SeqCst),
            forgotten: self.forgotten.load(Ordering::SeqCst),
            refused: self.refused.load(Ordering::SeqCst),
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
    interval: Duration,
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
                    scan_once(&reaper, &runtime, &*lock, &counters);
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

        Ok(LeaseThread { counters, halt, handle: Some(handle), resumed, interval })
    }

    /// Branches whose interrupted reap [`LeaseThread::start`] finished.
    pub fn resumed(&self) -> &[BranchId] {
        &self.resumed
    }

    /// The configured scan period.
    pub fn interval(&self) -> Duration {
        self.interval
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

/// One pass: read the cluster's time, reap what has expired, then let the runtime forget it.
fn scan_once(
    reaper: &TwoTierReaper,
    runtime: &AgentRuntime,
    lock: &dyn RuntimeLock,
    counters: &Counters,
) {
    // Before the lock, so a scan blocked on a statement is visibly a scan that is waiting rather
    // than a thread that has died.
    counters.attempts.fetch_add(1, Ordering::SeqCst);
    with_lock(lock, || {
        // Read inside the lock: the freshest reading that can still be acted on without another
        // statement intervening. A refusal is counted and reported, never rounded to a number.
        let now = match LeaseDeadline::try_now_millis() {
            Ok(now) => now,
            Err(e) => {
                counters.refused.fetch_add(1, Ordering::SeqCst);
                report(format!(
                    "lease: NOT reaping — this node does not know the cluster's time ({e}). \
                     Expired branches keep their pages until a LeaseTick is applied; reaping on a \
                     local clock is the divergence this refusal exists to prevent."
                ));
                return;
            }
        };
        match reaper.reap_expired(now) {
            Ok(reaped) => {
                counters.scans.fetch_add(1, Ordering::SeqCst);
                if reaped.is_empty() {
                    return;
                }
                counters.reaped.fetch_add(reaped.len() as u64, Ordering::SeqCst);
                let forgotten = runtime.forget_reaped_branches();
                counters.forgotten.fetch_add(forgotten as u64, Ordering::SeqCst);
                report(format!(
                    "lease: reaped {} expired branch(es) with no client cooperation ({}); {} \
                     workspace(s) forgotten",
                    reaped.len(),
                    join_ids(&reaped),
                    forgotten
                ));
            }
            Err(e) => {
                counters.failed.fetch_add(1, Ordering::SeqCst);
                report(format!(
                    "lease: scan failed: {e}. Whatever it had already freed is durable and `reap` \
                     is re-entrant, so the next scan resumes rather than double-freeing."
                ));
            }
        }
    });
}

fn join_ids(ids: &[BranchId]) -> String {
    ids.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(", ")
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
