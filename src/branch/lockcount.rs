//! ⛔⛔ **CEILING MEASUREMENT SCAFFOLD. THIS FILE AND ITS CALL SITES MUST NEVER MERGE.**
//!
//! Same status as `src/branch/d123_probe.rs` and the `D35-gate-stubtouch` edits at
//! `src/buffer/buffer_pool.rs:158`. Only the `bench/ceiling_*` artifacts produced on branch
//! `CEILING-park-or-structure` are citable.
//!
//! # What this answers, and why it is a COUNT
//!
//! `bench/d123_serial_attribution.txt` §3b records, at T=64 on the L0→L3 stub ladder:
//!
//! ```text
//!   stub  forks/sec     S_eff  HOLD_TOT       gap  U(hold)
//!      0     4788.6   0.20883   0.20079   0.00804    96.1%
//!      3     9019.1   0.11088   0.00941   0.10147     8.5%
//! ```
//!
//! HOLD falls 21×, throughput rises 1.9×, the GAP rises 12.6×, utilisation collapses 96.1% → 8.5%.
//! The project then read *"therefore the handoff is the floor"* off that pair and retired the
//! section-shortening candidate family against it. **The measurement licenses only the first
//! half** — that throughput stops tracking section length. What the remaining 0.10147 ms per fork
//! IS was never measured. `frontier/INVENTION-TRIGGER.md` pre-registers three outcomes for
//! settling it; Amendment 2 re-casts the experiment as a count because an integer is load-immune
//! and this box has a measured 46× quiet-vs-loaded spread.
//!
//! # The instrument, and why `try_lock` first is faithful rather than intrusive
//!
//! [`acquire_unwrap`] does `try_lock()` and, only if that fails, the real `lock()`.
//!
//! ⭐ **This is not an extra mechanism bolted onto the acquire path — it is std's own first step,
//! made observable.** `std::sync::Mutex::lock` already begins with an uncontended compare-exchange
//! and only parks after it fails. So `try_lock() == Err(WouldBlock)` is *precisely* the condition
//! under which an uninstrumented `lock()` would have gone on to wait. The wrapper adds one CAS,
//! on the contended path only, and changes no outcome.
//!
//! Two bounds follow, and both are stated wherever the numbers are:
//!
//! * `try_lock()` **succeeding** is EXACT: that acquisition demonstrably did not block.
//! * `try_lock()` **failing** is an UPPER BOUND on blocking: the holder may release in the window
//!   between the failed try and the `lock()`. So `contended` can only over-report waiting, never
//!   under-report it. **A LOW contended count is therefore the strong direction** — which matters,
//!   because a low count at L3 is what kills pre-registered outcome 1.
//!
//! # Why `thread_local` and not a global atomic
//!
//! Straight from D123's own note: a global `fetch_add` per acquisition from 64 threads bounces one
//! cache line, which is a mechanism adjacent to the one under test. The instrument would
//! manufacture its finding. Each thread accumulates privately and pushes once through
//! [`flush_thread`]; [`snapshot`] asserts the flush count so a dropped thread cannot silently
//! shrink every number.
//!
//! The pgwire-layer census (`pgwire::catalog_acquisitions`) DOES use a global atomic, because E.6
//! established it there and its statement rate is bounded by TCP round trips rather than by 64
//! threads spinning on one line. That asymmetry is deliberate and is stated in both places.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, TryLockError};

/// `TableBranchCatalog::logical` — the mutex the 21×/1.9× pair is about.
pub const LK_LOGICAL: usize = 0;
pub const NLOCK: usize = 1;

pub const LOCK_NAMES: [&str; NLOCK] = ["TableBranchCatalog::logical"];

#[derive(Clone, Copy)]
pub struct Counts {
    /// Every acquisition, contended or not.
    pub acq: [u64; NLOCK],
    /// Acquisitions whose `try_lock()` failed — the lock was held by someone else at that instant.
    pub contended: [u64; NLOCK],
    /// Completed `fork` calls, counted by the same instrument that counts the acquisitions, so the
    /// per-operation ratio is not assembled from two different bookkeepings.
    pub ops: u64,
    /// Threads that flushed. The anti-vacuity check.
    pub threads: u64,
}

impl Counts {
    const EMPTY: Self = Self { acq: [0; NLOCK], contended: [0; NLOCK], ops: 0, threads: 0 };

    /// Contended acquisitions per operation — **the discriminating number**.
    pub fn contended_per_op(&self, lk: usize) -> f64 {
        self.contended[lk] as f64 / self.ops.max(1) as f64
    }

    /// Acquisitions per operation. Free, and it bounds the answer: if `fork` takes `logical` once,
    /// this is 1.0 and any excess is another caller on the same lock.
    pub fn acq_per_op(&self, lk: usize) -> f64 {
        self.acq[lk] as f64 / self.ops.max(1) as f64
    }

    /// Of the acquisitions that happened, the fraction that were contended.
    pub fn contended_frac(&self, lk: usize) -> f64 {
        self.contended[lk] as f64 / self.acq[lk].max(1) as f64
    }
}

static ENABLED: AtomicBool = AtomicBool::new(false);

static TOTALS: Mutex<Counts> = Mutex::new(Counts::EMPTY);

thread_local! {
    static ACC: RefCell<Counts> = const { RefCell::new(Counts::EMPTY) };
}

/// ⭐ WHY THERE ARE TWO ACCUMULATION MODES, AND WHY THAT IS NOT A LOOSE END.
///
/// Thread-local accumulation needs every counting thread to call [`flush_thread`] before the
/// totals are read. That is satisfiable when the HARNESS owns the threads — the direct
/// `TableBranchCatalog` ladder spawns its own 64 — and it is the mode that must be used there,
/// because a global `fetch_add` from 64 threads bounces one cache line and would manufacture the
/// very contention under test.
///
/// ⛔ It is NOT satisfiable at the pgwire layer. Connection threads are spawned inside `serve()`,
/// the library owns them, and they exit at `Terminate` — possibly after the counter is read.
/// Their accumulators would simply be lost, and loss shrinks the count in the direction that
/// looks like *less contention*, i.e. the direction that would falsely kill outcome 1. So the
/// pgwire arms run in GLOBAL mode, where every bump lands on a process-wide atomic and no thread
/// has to co-operate. The rate there is bounded by TCP round trips, not by 64 threads spinning,
/// so the cache line is not the hazard it is at the direct layer.
///
/// ✅ The asymmetry is not taken on trust: the harness runs the direct ladder in BOTH modes and
/// refuses if they disagree. That cross-check is what makes either number citable.
static GLOBAL_MODE: AtomicBool = AtomicBool::new(false);

static G_ACQ: [std::sync::atomic::AtomicU64; NLOCK] =
    [const { std::sync::atomic::AtomicU64::new(0) }; NLOCK];
static G_CONT: [std::sync::atomic::AtomicU64; NLOCK] =
    [const { std::sync::atomic::AtomicU64::new(0) }; NLOCK];
static G_OPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Turn the census on. Off by default, so an ordinary build's acquire path is byte-identical to
/// what it is today: [`acquire_unwrap`] short-circuits to a plain `lock()`.
pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

/// Accumulate into process-wide atomics instead of thread-locals. See [`GLOBAL_MODE`].
pub fn set_global_mode(on: bool) {
    GLOBAL_MODE.store(on, Ordering::Relaxed);
}

#[inline(always)]
fn global_mode() -> bool {
    GLOBAL_MODE.load(Ordering::Relaxed)
}

#[inline(always)]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Acquire `m`, counting whether the acquisition was contended.
///
/// Panics on poison, exactly as the `self.logical.lock().unwrap()` it replaces did — the wrapper
/// must not quietly widen error behaviour while it is standing in for the real call.
#[inline]
pub fn acquire_unwrap<'a, T>(m: &'a Mutex<T>, which: usize) -> MutexGuard<'a, T> {
    if !enabled() {
        return m.lock().unwrap();
    }
    match m.try_lock() {
        Ok(g) => {
            bump(which, false);
            g
        }
        Err(TryLockError::WouldBlock) => {
            bump(which, true);
            m.lock().unwrap()
        }
        // Poisoned: `lock().unwrap()` would panic here and so must we. Counted as an acquisition
        // attempt first so the census does not lose it.
        Err(TryLockError::Poisoned(_)) => {
            bump(which, false);
            m.lock().unwrap()
        }
    }
}

#[inline(always)]
fn bump(which: usize, contended: bool) {
    if global_mode() {
        G_ACQ[which].fetch_add(1, Ordering::Relaxed);
        if contended {
            G_CONT[which].fetch_add(1, Ordering::Relaxed);
        }
        return;
    }
    ACC.with(|a| {
        let mut b = a.borrow_mut();
        b.acq[which] += 1;
        if contended {
            b.contended[which] += 1;
        }
    });
}

/// One completed operation on this thread.
#[inline(always)]
pub fn bump_op() {
    if !enabled() {
        return;
    }
    if global_mode() {
        G_OPS.fetch_add(1, Ordering::Relaxed);
        return;
    }
    ACC.with(|a| a.borrow_mut().ops += 1);
}

/// Push this thread's private counters into the shared totals.
///
/// ⛔ A thread that operates and never calls this contributes nothing, which shrinks every number
/// in the direction that looks like *less contention* — i.e. the direction that would falsely kill
/// outcome 1. The harness asserts `threads` against the arm's thread count for exactly that reason.
pub fn flush_thread() {
    let mine = ACC.with(|a| {
        let mut b = a.borrow_mut();
        let copy = *b;
        *b = Counts::EMPTY;
        copy
    });
    if mine.acq.iter().all(|&n| n == 0) && mine.ops == 0 {
        return;
    }
    let mut g = TOTALS.lock().unwrap();
    for i in 0..NLOCK {
        g.acq[i] += mine.acq[i];
        g.contended[i] += mine.contended[i];
    }
    g.ops += mine.ops;
    g.threads += 1;
}

pub fn reset() {
    *TOTALS.lock().unwrap() = Counts::EMPTY;
    for i in 0..NLOCK {
        G_ACQ[i].store(0, Ordering::Relaxed);
        G_CONT[i].store(0, Ordering::Relaxed);
    }
    G_OPS.store(0, Ordering::Relaxed);
}

/// The totals for whichever mode is active. In global mode `threads` is reported as 0, because no
/// thread flushes and a fabricated count there would be the kind of plausible-looking number this
/// project keeps getting burned by; the harness checks vacuity via `ops` instead.
pub fn snapshot() -> Counts {
    if global_mode() {
        let mut c = Counts::EMPTY;
        for i in 0..NLOCK {
            c.acq[i] = G_ACQ[i].load(Ordering::Relaxed);
            c.contended[i] = G_CONT[i].load(Ordering::Relaxed);
        }
        c.ops = G_OPS.load(Ordering::Relaxed);
        return c;
    }
    *TOTALS.lock().unwrap()
}
