//! ⛔⛔ **D123 MEASUREMENT SCAFFOLD. THIS FILE AND ITS CALL SITES MUST NEVER MERGE.**
//!
//! Same status as the `D35-gate-stubtouch` edits described at `src/buffer/buffer_pool.rs:158`:
//! only the `bench/d123_*` artifacts produced on branch `D123-serial-attribution` are citable.
//! The stub levels below deliberately skip writes that `fork` needs for correctness. A build with
//! `FERRODB_D123_STUB` set is **not a database**, it is an instrument.
//!
//! Answers two questions that `bench/serial_section_profile.txt` structurally cannot, because that
//! profiler runs **one thread at a time on uncontended operations**:
//!
//!   Q1  where does the 0.228 ms effective serial interval go at 64 threads?
//!   Q2  is that interval the ceiling at all?
//!
//! See `bench/d123_PREREGISTRATION.md` for the falsifiers this exists to decide.
//!
//! ## Why `thread_local` and not atomics
//!
//! A global `fetch_add` per phase per fork, from 64 threads, is cache-line bouncing on one line —
//! precisely the mechanism F4 is trying to detect. The instrument would manufacture its own
//! finding. Each thread accumulates privately and pushes once, at thread exit, through
//! [`flush_thread`].
//!
//! ## Why the disabled path must be genuinely free
//!
//! `mark()` returns `None` when the probe is off, so a probe-off run pays one relaxed load per
//! phase and no clock read. The harness reports probe-off and probe-on throughput side by side;
//! that difference is the instrument's own perturbation, measured rather than assumed.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::Instant;

pub const PH_WAIT: usize = 0;
pub const PH_CORE: usize = 1;
pub const PH_ENVELOPE: usize = 2;
pub const PH_FREE_SCAN: usize = 3;
pub const PH_MINT: usize = 4;
pub const PH_BUILD: usize = 5;
pub const PH_WRITE_RECORD: usize = 6;
pub const PH_CHILD_KEY: usize = 7;
pub const PH_HEADER: usize = 8;
pub const PH_STAGE: usize = 9;
pub const PH_EXTRA: usize = 10;
/// Dropping `parent_core` and `parent_envelope`. **Still under the lock** — see below.
pub const PH_DROPS: usize = 11;
/// The `Mutex` unlock itself, bracketed by an explicit `drop(g)`.
pub const PH_UNLOCK: usize = 12;
/// Acquire → after `stage()`. The section's WORK.
pub const PH_HOLD: usize = 13;
/// Acquire → after the unlock. The lock is genuinely held for all of this.
pub const PH_HOLD_TOTAL: usize = 14;
pub const PH_DURABLE: usize = 15;
pub const NPHASE: usize = 16;

/// ⭐ WHY `PH_DROPS` AND `PH_UNLOCK` EXIST (pre-registration Amendment 1).
///
/// Rust drops in **reverse declaration order** and the guard is declared FIRST, so it unlocks
/// LAST. A naive `HOLD` stamped at the end of the block therefore stops before `parent_core` and
/// `parent_envelope` are freed and before the mutex is released — all of which still happen with
/// the lock held. Freeing heap on 64 threads is itself one of the candidate mechanisms
/// (allocator contention), so leaving it in an unmeasured residual would have let a real
/// in-section cost be reported as "handoff". Both are now bracketed explicitly:
///
/// ```text
/// PH_HOLD_TOTAL = PH_HOLD + PH_DROPS + PH_UNLOCK          (identity, checked by the harness)
/// gap           = S_eff  − PH_HOLD_TOTAL                   (lock IDLE: scheduler wake-up only)
/// ```
pub const PHASE_NAMES: [&str; NPHASE] = [
    "wait_for_lock",
    "core(parent)",
    "envelope(parent)",
    "free_id_scan",
    "mint_id",
    "build_child",
    "write_record",
    "child_key_insert",
    "write_header",
    "stage(publish+ticket)",
    "extra_upserts(F5)",
    "drops(under lock)",
    "unlock",
    "HOLD(work only)",
    "HOLD_TOTAL(to unlock)",
    "durable(after release)",
];

static ENABLED: AtomicBool = AtomicBool::new(false);
static STUB: AtomicU8 = AtomicU8::new(0);
static EXTRA: AtomicU64 = AtomicU64::new(0);

/// Merged totals, written once per thread by [`flush_thread`].
#[derive(Clone, Copy)]
pub struct Totals {
    pub ns: [u64; NPHASE],
    pub forks: u64,
    pub threads: u64,
}

impl Totals {
    const EMPTY: Self = Self { ns: [0; NPHASE], forks: 0, threads: 0 };
}

static TOTALS: Mutex<Totals> = Mutex::new(Totals::EMPTY);

thread_local! {
    static ACC: RefCell<Totals> = const { RefCell::new(Totals::EMPTY) };
}

/// Read the knobs once. Call from a harness before any timed loop.
///
/// * `FERRODB_D123_PROBE=1` — turn the per-phase clock reads on.
/// * `FERRODB_D123_STUB=0..3` — see [`stub_level`].
/// * `FERRODB_D123_EXTRA=k` — k extra upserts under `logical` (F5's additive axis).
pub fn configure(probe: bool, stub: u8, extra: u64) {
    ENABLED.store(probe, Ordering::Relaxed);
    STUB.store(stub, Ordering::Relaxed);
    EXTRA.store(extra, Ordering::Relaxed);
}

#[inline(always)]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// How much of fork's critical section to skip. **Every level keeps `write_header` and `stage`.**
///
/// ⛔ That is not tidiness, it is the difference between a measurement and an artifact.
/// `serial_section_profile`'s own comment records that macOS short-circuits `F_FULLFSYNC` to
/// 0.010 ms when the file has nothing pending. A stub that left no dirty page would delete the
/// fsync along with the tree work and report a throughput explosion caused by the loss of
/// durability rather than by the loss of serial work. `write_header` guarantees ≥1 dirty page per
/// fork, so `syncs_issued()` stays meaningful at every level and the harness prints it as the
/// check that this held.
///
/// * 0 — untouched fork.
/// * 1 — skip `write_record` / `write_record_new`.
/// * 2 — 1, plus skip the FREE_ID range scan (mint a fresh id unconditionally).
/// * 3 — 2, plus skip the parent envelope lookup and the child-key insert.
#[inline(always)]
pub fn stub_level() -> u8 {
    STUB.load(Ordering::Relaxed)
}

#[inline(always)]
pub fn extra_upserts() -> u64 {
    EXTRA.load(Ordering::Relaxed)
}

/// Start a phase. `None` when the probe is off, so the clock is never read.
#[inline(always)]
pub fn mark() -> Option<Instant> {
    if enabled() { Some(Instant::now()) } else { None }
}

/// End a phase opened by [`mark`].
#[inline(always)]
pub fn record(phase: usize, t: Option<Instant>) {
    if let Some(t) = t {
        let ns = t.elapsed().as_nanos() as u64;
        ACC.with(|a| a.borrow_mut().ns[phase] += ns);
    }
}

#[inline(always)]
pub fn bump_fork() {
    if enabled() {
        ACC.with(|a| a.borrow_mut().forks += 1);
    }
}

/// Push this thread's private accumulators into the shared totals. **A thread that forks and never
/// calls this contributes nothing**, which would silently shrink every number, so the harness
/// asserts `threads` equals the arm's thread count before printing.
pub fn flush_thread() {
    let mine = ACC.with(|a| {
        let mut b = a.borrow_mut();
        let copy = *b;
        *b = Totals::EMPTY;
        copy
    });
    if mine.forks == 0 {
        return;
    }
    let mut g = TOTALS.lock().unwrap();
    for i in 0..NPHASE {
        g.ns[i] += mine.ns[i];
    }
    g.forks += mine.forks;
    g.threads += 1;
}

pub fn reset() {
    *TOTALS.lock().unwrap() = Totals::EMPTY;
}

pub fn snapshot() -> Totals {
    *TOTALS.lock().unwrap()
}
