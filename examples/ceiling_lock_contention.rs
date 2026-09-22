//! ⛔⛔ **CEILING MEASUREMENT SCAFFOLD. MUST NEVER MERGE.** See `src/branch/lockcount.rs`.
//!
//! # Is the 1.9× ceiling the structure, or the mutex? Asked as a COUNT.
//!
//! Pre-registered in `frontier/INVENTION-TRIGGER.md`, section *"IS THE 1.9× CEILING THE STRUCTURE,
//! OR THE MUTEX?"*, with three outcomes and Amendments 1–2. **This harness may not add a fourth.**
//! It may report that the counts cannot discriminate, which is a result about the instrument.
//!
//! The recorded pair this attacks (`bench/d123_serial_attribution.txt` §3b, warm=0, T=64):
//!
//! ```text
//!   stub  forks/sec     S_eff  HOLD_TOT       gap  U(hold)
//!      0     4788.6   0.20883   0.20079   0.00804    96.1%
//!      3     9019.1   0.11088   0.00941   0.10147     8.5%
//! ```
//!
//! HOLD 21× down, throughput 1.9× up, GAP 12.6× up, utilisation 96.1% → 8.5%. The project read
//! *"the handoff is the floor"* off that and retired the section-shortening family. **Only the
//! first half was measured.** Amendment 2's discriminator:
//!
//! * gap is **park/unpark latency** ⇒ threads are still piling onto the lock, so contended
//!   acquisitions per operation stay HIGH even as utilisation collapses.
//! * contention **tracks the collapsing utilisation** instead ⇒ the remaining time is not threads
//!   blocking on each other, and **outcome 1 is dead with no duration measured at all.**
//!
//! # ⚠ EVERY NUMBER CARRIES ITS LAYER (Amendment 1's fourth condition)
//!
//! | mode | layer | locks that layer holds |
//! |------|-------|------------------------|
//! | `direct` | `TableBranchCatalog` driven directly, as `examples/fork_concurrency.rs` does — **the layer the recorded 21×/1.9× pair was measured at** | `logical` only |
//! | `pgwire` | the shipped pgwire server over a real TCP socket, forks issued as SQL — **what production does** | `ServerContext::catalog()` outermost → `AgentRuntime` state → `logical` |
//!
//! E.6 established those are not the same experiment. A number here without its layer named is
//! not citable, and the printer refuses to emit one.
//!
//! # `model` — forcing the detector to fire, in both directions
//!
//! A count that comes back low is worthless until the counter has been made to come back high.
//! `model` runs the SAME wrapper over a bare `Mutex<()>` with no fsync, no tree and no second
//! lock: T threads, each looping *hold H spins inside, then W spins outside*. Sweeping W/H walks
//! the same hold-shrinks/outside-grows transition the stub ladder walks, in a system where the
//! answer is known by construction. It gives:
//!
//! * a **must-fire** cell (H large, W=0 ⇒ contention per op must approach 1.0),
//! * a **must-not-fire** cell (T=1 ⇒ contention per op must be exactly 0.000),
//! * and the **shape a pure mutex produces** when the section shortens, which is what tells a
//!   reading on the real ladder from an instrument artifact.
//!
//! ```text
//! cargo run --release --features lock_census --example ceiling_lock_contention -- <mode> [args]
//!   model                                  the fire-check and the pure-mutex reference shape
//!   direct  [N] [T,T,T] [warm]             layer A: the L0→L3 ladder on TableBranchCatalog
//!   paired  [N] [T,T,T] [warm]             layer A, with D123's phase clock ON beside the count,
//!                                          so U(hold) and contended/op come from ONE run
//!   pgwire  [F] [T,T,T]                    layer B: the L0→L3 ladder through the shipped server
//! ```

use std::collections::BTreeSet;
use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::d123_probe as probe;
use ferrodb::branch::lockcount as lk;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::pgwire::{catalog_acquisitions, catalog_contended, serve, ServerContext};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;
const PROTOCOL_V3: i32 = 196_608;

/// Exit non-zero rather than print a number the run did not earn.
fn refuse(why: &str) -> ! {
    eprintln!("ceiling: REFUSING. {why}");
    std::process::exit(1);
}

/// ⛔ PROVENANCE, PRINTED BY THE RUN ITSELF — not echoed before it.
///
/// This binary links the D123 scaffold, whose stub levels are not a database: L1 never writes the
/// child's record, L2 leaks retired slots, L3 drops the capability envelope and the parent's
/// live-child entry. `stub_level()` reads a static that starts at 0, so a clean run is the
/// overwhelmingly likely case — and that is exactly the shape of this project's three worst
/// measurement failures (D79 measured with durability off, E.6 measured below the production lock,
/// D101 measured a runtime with `storage: None`). Each looked fine until someone checked.
///
/// So the state is READ BACK from the probe and asserted, at process start and again at every
/// rung. `expect` is the stub level this rung means to run; anything else, or any non-zero extra
/// upsert count, refuses instead of printing a number.
fn assert_probe(expect: u8, where_: &str) {
    let stub = probe::stub_level();
    let extra = probe::extra_upserts();
    let new_keys = probe::extra_new_keys();
    if stub != expect {
        refuse(&format!(
            "{where_}: probe::stub_level() reads {stub}, this rung is L{expect}. The fork path \
             being measured is not the one the row is labelled with."
        ));
    }
    if extra != 0 || new_keys {
        refuse(&format!(
            "{where_}: probe::extra_upserts()={extra}, extra_new_keys()={new_keys}. F5's additive \
             axis is on and this harness never asks for it, so the critical section under test is \
             not fork's."
        ));
    }
}

fn stamp(label: &str) {
    let load = std::process::Command::new("uptime")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.split("age").last().unwrap_or("").trim().to_string())
        .unwrap_or_else(|| "unknown".into());
    let when = std::process::Command::new("date")
        .arg("-u")
        .arg("+%Y-%m-%dT%H:%M:%SZ")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into());
    println!("# {label}: {when}  load_at_acquire={load}");
}

// =================================================================================================
// model — the fire-check, and the reference shape a PURE mutex produces
// =================================================================================================

#[inline(never)]
fn spin(n: u64) {
    let mut acc = 0u64;
    for i in 0..n {
        acc = std::hint::black_box(acc.wrapping_add(i ^ 0x9e37_79b9_7f4a_7c15));
    }
    std::hint::black_box(acc);
}

/// ⭐ THE SECOND WITNESS, and it exists because the first fire-check FAILED.
///
/// `try_lock`-failure counts COLLISIONS. It does not count THREADS PARKED. Those come apart
/// whenever a releasing thread re-acquires before any parked waiter can wake — barging, which
/// `pthread_mutex` on macOS does not prevent. Under barging 63 threads can be parked for the whole
/// run while the counter reads ~0, because a parked thread makes ONE failed `try_lock` and then
/// contributes nothing for the thousands of operations the barger completes.
///
/// So the model carries an independent, load-immune gauge of *how many threads are inside the
/// acquire region at the moment one of them gets in*. It is incremented before the acquire and
/// decremented after, and the pre-decrement value (which includes the acquirer itself) is
/// recorded. `1` means nobody else was there. `T` means every thread was queued.
///
/// ⛔ MODEL ONLY. These are process-wide atomics on the acquire path, i.e. exactly the cache-line
/// bouncing `lockcount`'s header refuses to put on the real ladder. Here that is the point: the
/// model has no finding to protect, only a mechanism to expose.
static IN_ACQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SUM_SEEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static MAX_SEEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct ModelCell {
    contended_per_op: f64,
    acq_per_op: f64,
    /// Mean number of threads in the acquire region when an acquisition succeeded, self included.
    queue_mean: f64,
    /// The largest such number seen. `1` proves the threads never overlapped at all.
    queue_max: u64,
    ops: u64,
    secs: f64,
}

/// Spins per second, measured single-threaded, so a cell can be asked for a DURATION instead of a
/// spin count. Printed with the cells that use it — a conversion factor nobody can see is a
/// conversion factor nobody can check.
fn calibrate_spin() -> f64 {
    // Warm, then measure. One untimed pass so the branch predictor and the frequency governor are
    // not part of the constant.
    spin(5_000_000);
    let t0 = Instant::now();
    spin(50_000_000);
    50_000_000.0 / t0.elapsed().as_secs_f64()
}

/// T threads × `iters` iterations: hold the mutex for `hold` spins, then `outside` spins free.
fn model_cell(t: usize, iters: u64, hold: u64, outside: u64) -> ModelCell {
    model_cell_ex(t, iters, hold, outside, None)
}

/// As [`model_cell`], but the work OUTSIDE the section may be a sleep instead of a spin.
///
/// ⭐ WHY THIS EXISTS, AND IT IS NOT A DETAIL. In the real ladder the work outside `logical` is
/// dominated by `durable(seq)` — an fsync. A thread in an fsync is BLOCKED ON I/O: it holds no
/// core, and the other 63 threads run. A thread in `spin()` holds a core. With 64 threads on a
/// ~12-core box the spin version oversubscribes the CPU by 5x, which inflates every queue reading
/// and depresses achieved throughput far below the offered load — the first operating-point
/// reference below achieved 2.0% utilisation when it was asked for 8.5%.
///
/// A sleep is the faithful stand-in: the thread leaves the runnable set for the duration, exactly
/// as an fsync does. It is not an fsync (no I/O, no group commit, and the wake-up has timer
/// granularity), and the reference is labelled accordingly.
fn model_cell_ex(
    t: usize,
    iters: u64,
    hold: u64,
    outside: u64,
    outside_sleep: Option<std::time::Duration>,
) -> ModelCell {
    use std::sync::atomic::Ordering as O;
    let m: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    lk::set_global_mode(false);
    lk::reset();
    IN_ACQ.store(0, O::SeqCst);
    SUM_SEEN.store(0, O::SeqCst);
    MAX_SEEN.store(0, O::SeqCst);
    lk::set_enabled(true);
    let t0 = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..t {
            let m = Arc::clone(&m);
            s.spawn(move || {
                for _ in 0..iters {
                    {
                        IN_ACQ.fetch_add(1, O::SeqCst);
                        let mut g = lk::acquire_unwrap(&m, lk::LK_LOGICAL);
                        // `fetch_sub` returns the value BEFORE the subtraction, so `seen` counts
                        // this thread plus everyone else still waiting to get in.
                        let seen = IN_ACQ.fetch_sub(1, O::SeqCst);
                        SUM_SEEN.fetch_add(seen, O::Relaxed);
                        MAX_SEEN.fetch_max(seen, O::Relaxed);
                        *g += 1;
                        spin(hold);
                    }
                    lk::bump_op();
                    match outside_sleep {
                        Some(d) => std::thread::sleep(d),
                        None => spin(outside),
                    }
                }
                lk::flush_thread();
            });
        }
    });
    let secs = t0.elapsed().as_secs_f64();
    lk::set_enabled(false);
    let c = lk::snapshot();
    if c.threads as usize != t {
        refuse(&format!("model lost a thread's accumulators: {} of {t} flushed", c.threads));
    }
    if c.ops != iters * t as u64 {
        refuse(&format!("model counted {} ops, ran {}", c.ops, iters * t as u64));
    }
    ModelCell {
        contended_per_op: c.contended_per_op(lk::LK_LOGICAL),
        acq_per_op: c.acq_per_op(lk::LK_LOGICAL),
        queue_mean: SUM_SEEN.load(O::Relaxed) as f64 / c.ops.max(1) as f64,
        queue_max: MAX_SEEN.load(O::Relaxed),
        ops: c.ops,
        secs,
    }
}

fn mode_model() {
    println!("CEILING — MODE=model. THE FIRE-CHECK, and the shape a PURE mutex produces.");
    println!("LAYER: none. A bare `Mutex<()>`: no fsync, no tree, no second lock. This cell exists");
    println!("to make the counter fire on purpose and to show what it reads when it should read 0.");
    stamp("model_start");
    println!();
    println!("  queue_mean / queue_max = threads inside the acquire region when one got in, self");
    println!("  included (the SECOND witness; see IN_ACQ). 1 means nobody else was there at all.");
    println!();
    println!("  {:>18} {:>4} {:>9} {:>10} {:>8} {:>8} {:>14} {:>8} {:>10} {:>9}",
        "cell", "T", "hold", "outside", "ops", "secs", "contended/op", "acq/op", "queue_mean", "queue_max");
    let row = |name: &str, t: usize, hold: u64, outside: u64, c: &ModelCell| {
        println!(
            "  {:>18} {:>4} {:>9} {:>10} {:>8} {:>8.3} {:>14.5} {:>8.4} {:>10.2} {:>9}",
            name, t, hold, outside, c.ops, c.secs, c.contended_per_op, c.acq_per_op, c.queue_mean, c.queue_max
        );
    };

    // ── must NOT fire ────────────────────────────────────────────────────────────────────────
    let c = model_cell(1, 20_000, 2_000, 0);
    row("NEG single-thread", 1, 2_000, 0, &c);
    if c.contended_per_op != 0.0 {
        refuse(&format!(
            "NEGATIVE CONTROL FAILED: one thread on a private mutex reported {} contended \
             acquisitions per op. The counter fires spuriously and NOTHING else in this run is \
             interpretable.",
            c.contended_per_op
        ));
    }
    if c.queue_max != 1 {
        refuse(&format!(
            "NEGATIVE CONTROL FAILED on the second witness: one thread reported queue_max={}. \
             The gauge counts something other than concurrent acquirers.",
            c.queue_max
        ));
    }

    // ── must fire ────────────────────────────────────────────────────────────────────────────
    //
    // ⛔ THE GATED POSITIVE CONTROL IS THE ONE WITH `outside > 0`, AND THAT IS A CORRECTION.
    // The first version of this file gated on `outside = 0` — 64 threads doing nothing but take
    // and release one mutex — and it FAILED, reading 0.029 contended per op. The cell below it
    // ("BARGE") reproduces that failure deliberately, with the queue gauge attached, because the
    // reason matters: with no work outside the section a releasing thread re-acquires before any
    // parked waiter can wake, so 63 threads sit blocked while the collision counter reads ~0.
    // That regime does not resemble either rung of the real ladder (both have milliseconds of
    // fsync outside the section), so it is reported as a blind spot rather than used as the gate.
    let c = model_cell(64, 800, 20_000, 20_000);
    row("POS 64t queued", 64, 20_000, 20_000, &c);
    let pos = c.contended_per_op;
    if c.contended_per_op < 0.5 {
        refuse(&format!(
            "POSITIVE CONTROL FAILED: 64 threads offering 32x the lock's capacity reported only \
             {:.4} contended acquisitions per op (queue_mean {:.2}, queue_max {}). The counter \
             does not fire when contention is certain, so a low reading anywhere else in this run \
             means nothing.",
            c.contended_per_op, c.queue_mean, c.queue_max
        ));
    }

    let c = model_cell(64, 800, 20_000, 0);
    row("BARGE 64t no-out", 64, 20_000, 0, &c);
    if c.queue_max < 2 {
        refuse(&format!(
            "The barge cell did not even overlap: queue_max={}. Its low contended count would then \
             be non-overlap rather than barging, and the blind spot this run documents would be \
             unproven.",
            c.queue_max
        ));
    }
    println!();
    println!("  ⇒ BLIND SPOT, MEASURED: the BARGE row has queue_mean/queue_max showing threads ARE");
    println!("    parked, while contended/op reads far below the POS row. `try_lock` failure counts");
    println!("    COLLISIONS, not PARKED THREADS, and the two come apart when the releaser re-takes");
    println!("    the lock. Direction of the error: contended/op UNDER-reports blocking, i.e. it errs");
    println!("    toward killing pre-registered outcome 1. Every low reading below inherits that.");

    // ── the reference shape: shrink the section, grow the outside work ───────────────────────
    println!();
    println!("  REFERENCE SHAPE — 64 threads, total work per iteration held CONSTANT at 20,000");
    println!("  spins, moved out of the section a step at a time. This is the stub ladder's own");
    println!("  transition (hold shrinks, outside grows) in a system with NO other term.");
    println!();
    println!("  {:>18} {:>4} {:>9} {:>10} {:>8} {:>8} {:>14} {:>8} {:>10} {:>9}",
        "cell", "T", "hold", "outside", "ops", "secs", "contended/op", "acq/op", "queue_mean", "queue_max");
    for (hold, outside) in [(20_000u64, 0u64), (10_000, 10_000), (2_000, 18_000), (500, 19_500), (100, 19_900)] {
        let c = model_cell(64, 400, hold, outside);
        row(&format!("work-in {:.0}%", 100.0 * hold as f64 / 20_000.0), 64, hold, outside, &c);
    }

    // ── the operating-point reference: the ladder's OWN two rungs, in a pure mutex ───────────
    //
    // ⭐ THIS IS THE COMPARATOR THE WHOLE RUN TURNS ON. The recorded ladder's two ends are not
    // arbitrary; each has a measured offered load, and a pure mutex driven at the SAME offered
    // load is what "nothing but a mutex" reads there. From `bench/d123_serial_attribution.txt`
    // §3b at T=64, per-thread cycle = 64 / forks-per-sec:
    //
    //   L0: HOLD 0.20079 ms, cycle 64/4788.6 = 13.365 ms  ->  hold/cycle = 0.015023  (rho = 0.961)
    //   L3: HOLD 0.00941 ms, cycle 64/9019.1 =  7.096 ms  ->  hold/cycle = 0.001326  (rho = 0.085)
    //
    // Those two fractions are reproduced below as spin ratios. Nothing about the real system is
    // used except the ratio, so this is a MODEL of the operating point, not a re-measurement.
    println!();
    println!("  OPERATING-POINT REFERENCE (a) — SPIN OUTSIDE. A pure mutex driven at the SAME");
    println!("  hold/cycle fraction as each recorded rung (L0 0.015023, L3 0.001326; d123 §3b,");
    println!("  T=64). ⚠ The outside work is a SPIN, so all 64 threads are runnable at once on a");
    println!("  ~12-core box. Achieved utilisation therefore falls well short of the offered load;");
    println!("  the U_ach column says by how much, and (b) below is the faithful version.");
    println!();
    let spins_per_sec = calibrate_spin();
    println!("  spin calibration: {:.3e} spins/sec single-threaded (hold=2000 spins = {:.4} ms)",
        spins_per_sec, 2_000.0 * 1000.0 / spins_per_sec);
    println!();
    println!("  {:>18} {:>4} {:>9} {:>10} {:>8} {:>8} {:>14} {:>8} {:>10} {:>9} {:>7}",
        "cell", "T", "hold", "outside", "ops", "secs", "contended/op", "acq/op", "queue_mean", "queue_max", "U_ach");
    let rowu = |name: &str, t: usize, hold: u64, outside: String, c: &ModelCell, u: f64| {
        println!(
            "  {:>18} {:>4} {:>9} {:>10} {:>8} {:>8.3} {:>14.5} {:>8.4} {:>10.2} {:>9} {:>6.1}%",
            name, t, hold, outside, c.ops, c.secs, c.contended_per_op, c.acq_per_op, c.queue_mean, c.queue_max, 100.0 * u
        );
    };
    for (name, frac, iters) in [("L0-like rho=.961", 0.015023f64, 400u64), ("L3-like rho=.085", 0.001326, 400)] {
        let hold = 2_000u64;
        let outside = ((hold as f64) * (1.0 / frac - 1.0)).round() as u64;
        let c = model_cell(64, iters, hold, outside);
        let u = (c.ops as f64) * (hold as f64 / spins_per_sec) / c.secs;
        rowu(name, 64, hold, outside.to_string(), &c, u);
    }

    // ── (b) the faithful operating point: the outside work is an I/O WAIT, as fsync is ───────
    println!();
    println!("  OPERATING-POINT REFERENCE (b) — SLEEP OUTSIDE. ⭐ THIS IS THE ONE TO COMPARE THE");
    println!("  REAL LADDER AGAINST. Both the hold and the outside interval are set to the recorded");
    println!("  rung's own WALL-CLOCK values, and the outside interval is a sleep, so a thread");
    println!("  between forks leaves the runnable set exactly as it does inside `durable()`'s fsync.");
    println!("  From d123 §3b at T=64:  L0 hold 0.20079 ms / cycle 13.365 ms");
    println!("                          L3 hold 0.00941 ms / cycle  7.096 ms");
    println!("  It is a MODEL, not an fsync: no I/O, no group commit, and the wake-up carries timer");
    println!("  granularity. What it reproduces is the ARRIVAL PROCESS at each rung.");
    println!();
    println!("  {:>18} {:>4} {:>9} {:>10} {:>8} {:>8} {:>14} {:>8} {:>10} {:>9} {:>7}",
        "cell", "T", "hold", "outside", "ops", "secs", "contended/op", "acq/op", "queue_mean", "queue_max", "U_ach");
    let mut opref: Vec<(f64, f64, u64)> = Vec::new();
    for (name, hold_ms, cycle_ms) in
        [("L0-like 0.201ms", 0.20079f64, 13.365f64), ("L3-like 0.0094ms", 0.00941, 7.096)]
    {
        let hold = (hold_ms / 1000.0 * spins_per_sec).round() as u64;
        let out = std::time::Duration::from_secs_f64((cycle_ms - hold_ms) / 1000.0);
        let c = model_cell_ex(64, 200, hold, 0, Some(out));
        let u = (c.ops as f64) * (hold_ms / 1000.0) / c.secs;
        rowu(name, 64, hold, format!("sleep {:.3}ms", (cycle_ms - hold_ms)), &c, u);
        opref.push((c.contended_per_op, c.queue_mean, c.queue_max));
    }
    println!();
    println!("⭐ READ IT THIS WAY. `contended/op` near {pos:.2} is what a mutex reads when threads are");
    println!("   genuinely queued on it. Reference (b) gives the value a pure mutex reads at each");
    println!("   recorded rung's own arrival process: L0-like {:.5} (queue_mean {:.2}), L3-like {:.5}",
        opref[0].0, opref[0].1, opref[1].0);
    println!("   (queue_mean {:.2}). If the real ladder's L3 lands near the L3-like reference, the", opref[1].1);
    println!("   lock is simply not busy and nothing is queued on it — pre-registered outcome 1 dies.");
    println!("   If it lands near {pos:.2} instead, threads are still piling up and outcome 1 lives.");
}

// =================================================================================================
// direct — LAYER A: TableBranchCatalog, the layer the recorded 21x/1.9x pair was measured at
// =================================================================================================

fn open_catalog(dir: &Path, tag: &str) -> Arc<TableBranchCatalog> {
    let main_path = dir.join(format!("main-{tag}.db"));
    let mf = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&main_path)
        .unwrap();
    let _main_pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(mf).unwrap())));
    let path = dir.join(format!("branches-{tag}.branchcat"));
    let _ = std::fs::remove_file(&path);
    Arc::new(TableBranchCatalog::open_sidecar(&path, 1).expect("open catalog"))
}

struct DirectArm {
    forks: usize,
    secs: f64,
    syncs: u64,
    counts: lk::Counts,
    /// `None` unless the rung ran with D123's phase clock on. See [`mode_paired`].
    phases: Option<probe::Totals>,
}

/// One rung. Mirrors `d123_serial_attribution::run_arm` — same constructor, same warm-up shape,
/// same `n / t` fork-bounded loop — so the ladder is the recorded one and not a lookalike.
#[allow(clippy::too_many_arguments)]
fn run_direct(
    dir: &Path,
    tag: &str,
    n: usize,
    t: usize,
    warm: usize,
    stub: u8,
    global: bool,
    probe_on: bool,
) -> DirectArm {
    let cat = open_catalog(dir, tag);
    let lease = LeaseDeadline(u64::MAX);

    // Warm with the stub OFF and the census OFF, so every rung starts from the same tree shape
    // whatever the timed loop is about to skip, and no warm fork lands in the counts.
    const WARM_THREADS: usize = 32;
    lk::set_enabled(false);
    probe::configure(false, 0, 0);
    let per_warm = warm / WARM_THREADS;
    if per_warm > 0 {
        std::thread::scope(|s| {
            for _ in 0..WARM_THREADS {
                let cat = Arc::clone(&cat);
                s.spawn(move || {
                    for _ in 0..per_warm {
                        cat.fork(BranchId::TRUNK, lease).expect("warm fork");
                    }
                });
            }
        });
    }

    // Probe OFF: this run's evidence is integers, so the clock is never read and the timing
    // instrument's own perturbation is not in the way. `stub` is the only thing that varies.
    probe::configure(probe_on, stub, 0);
    assert_probe(stub, &format!("direct rung {tag} (L{stub})"));
    probe::reset();
    lk::set_global_mode(global);
    lk::reset();
    lk::set_enabled(true);

    let syncs_before = cat.syncs_issued();
    let per = n / t.max(1);
    let total = per * t;

    let t0 = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..t {
            let cat = Arc::clone(&cat);
            s.spawn(move || {
                for _ in 0..per {
                    cat.fork(BranchId::TRUNK, lease).expect("fork");
                }
                lk::flush_thread();
                probe::flush_thread();
            });
        }
    });
    let secs = t0.elapsed().as_secs_f64();
    lk::set_enabled(false);
    probe::configure(false, 0, 0);
    let counts = lk::snapshot();

    if counts.ops as usize != total {
        refuse(&format!(
            "{tag}: census counted {} forks, the harness ran {total}. A per-operation ratio \
             assembled from two disagreeing bookkeepings is not a measurement.",
            counts.ops
        ));
    }
    if !global && counts.threads as usize != t {
        refuse(&format!(
            "{tag}: {} of {t} threads flushed their accumulators. Loss shrinks the contended \
             count in the direction that looks like LESS contention, which is the direction that \
             would falsely kill outcome 1.",
            counts.threads
        ));
    }
    let phases = if probe_on {
        let p = probe::snapshot();
        if p.forks as usize != total {
            refuse(&format!(
                "{tag}: the phase clock counted {} forks against {total} run. HOLD per fork would \
                 be divided by the wrong denominator.",
                p.forks
            ));
        }
        if p.threads as usize != t {
            refuse(&format!(
                "{tag}: {} of {t} threads flushed phase accumulators; HOLD_TOTAL would be short by \
                 the missing threads and U(hold) would read LOW — the direction that manufactures \
                 the answer.",
                p.threads
            ));
        }
        // The identity D123 asserts: HOLD_TOTAL = HOLD + DROPS + UNLOCK, all under the lock.
        let parts = p.ns[probe::PH_HOLD] + p.ns[probe::PH_DROPS] + p.ns[probe::PH_UNLOCK];
        let tot = p.ns[probe::PH_HOLD_TOTAL];
        let rel = (tot as f64 - parts as f64).abs() / tot.max(1) as f64;
        if rel > 0.05 {
            refuse(&format!(
                "{tag}: HOLD_TOTAL {tot} ns != HOLD+DROPS+UNLOCK {parts} ns ({:.1}% apart). The \
                 bracketing is wrong, so the lock-held total is not the lock-held total.",
                100.0 * rel
            ));
        }
        Some(p)
    } else {
        None
    };
    DirectArm { forks: total, secs, syncs: cat.syncs_issued() - syncs_before, counts, phases }
}

// =================================================================================================
// paired — THE COUNT AND THE UTILISATION, IN ONE RUN, ONE PROCESS, ONE MOMENT
// =================================================================================================

/// ⭐ WHY THIS MODE EXISTS, AND IT IS THE ONE THE VERDICT RESTS ON.
///
/// The discriminator is *"contended acquisitions stay high WHILE utilisation collapses"*. That is a
/// statement about TWO quantities, and `mode_direct` measures only one of them — the other comes
/// from `bench/d123_serial_attribution.txt`, a different run on a differently loaded box. Reading a
/// count from today against a utilisation from last week is two instruments and two moments, and
/// this project has a rule about that for a reason.
///
/// So this mode turns D123's phase clock back on and reports, per rung, from the SAME threads in
/// the SAME process:
///
/// ```text
///   contended/op   the census
///   HOLD_TOT ms    per fork, acquire -> unlock, from the phase clock
///   S_eff ms       wall / forks
///   U(hold)        HOLD_TOT / S_eff          <- a ratio of two durations, ONE instrument
///   gap ms         S_eff - HOLD_TOT          <- the lock-idle interval the question is about
/// ```
///
/// ⚠ U(hold), S_eff and gap are RATIOS OR DURATIONS and the box is not quiet. U(hold) is the one
/// that survives load, because numerator and denominator move together inside one run; the
/// absolute ms figures are upper bounds and are stamped as such. The COUNT remains the evidence.
///
/// ⚠ The phase clock perturbs: 16 `Instant::now()` pairs per fork, some of them inside the
/// section. It perturbs every rung the same way, and `mode_direct`'s probe-OFF counts are printed
/// beside these so the perturbation is visible rather than assumed.
fn mode_paired(dir: &Path, n: usize, threads: &[usize], warm: usize) {
    println!("CEILING — MODE=paired. THE COUNT AND THE UTILISATION, MEASURED TOGETHER.");
    println!();
    println!("⚠ LAYER: `TableBranchCatalog` driven directly — the layer the recorded 21x/1.9x pair");
    println!("  was measured at, NOT what any shipped front-end does. See MODE=pgwire.");
    println!();
    println!("  Each rung is run TWICE, back to back, same process:");
    println!("    probe OFF -> contended/op with no clock reads anywhere (the citable count)");
    println!("    probe ON  -> HOLD_TOT, S_eff, U(hold), gap, and the count again under the clock");
    println!();
    println!("  U(hold) is a ratio of two durations taken by ONE instrument inside ONE run, which is");
    println!("  the kind D123 §(F4) argues survives a loaded box. ms columns are UPPER BOUNDS.");
    stamp("paired_start");
    println!();
    println!(
        "  {:>7} {:>5} {:>7} {:>13} {:>13} {:>10} {:>10} {:>9} {:>9} {:>10}",
        "threads", "stub", "forks", "cont/op(off)", "cont/op(on)", "HOLD_TOT", "S_eff", "U(hold)", "gap ms", "forks/sec"
    );
    for &t in threads {
        let mut first: Option<(f64, f64, f64)> = None;
        for stub in 0u8..=3 {
            let off = run_direct(dir, &format!("q{t}_{stub}_off"), n, t, warm, stub, false, false);
            let on = run_direct(dir, &format!("q{t}_{stub}_on"), n, t, warm, stub, false, true);
            let p = on.phases.expect("probe-on rung must carry phases");
            let hold_ms = p.ns[probe::PH_HOLD_TOTAL] as f64 / p.forks.max(1) as f64 / 1e6;
            let s_eff_ms = on.secs * 1000.0 / on.forks.max(1) as f64;
            let u = hold_ms / s_eff_ms;
            let gap_ms = s_eff_ms - hold_ms;
            if off.syncs == 0 || on.syncs == 0 {
                refuse(&format!("T={t} L{stub}: syncs collapsed to 0; the rung lost DURABILITY."));
            }
            println!(
                "  {:>7} {:>5} {:>7} {:>13.5} {:>13.5} {:>10.5} {:>10.5} {:>8.1}% {:>9.5} {:>10.1}",
                t,
                stub,
                off.forks,
                off.counts.contended_per_op(lk::LK_LOGICAL),
                on.counts.contended_per_op(lk::LK_LOGICAL),
                hold_ms,
                s_eff_ms,
                100.0 * u,
                gap_ms,
                off.forks as f64 / off.secs
            );
            if stub == 0 {
                first = Some((off.counts.contended_per_op(lk::LK_LOGICAL), u, gap_ms));
            }
            if stub == 3 {
                let (c0, u0, g0) = first.expect("L0 ran first");
                let c3 = off.counts.contended_per_op(lk::LK_LOGICAL);
                println!(
                    "    L0→L3 at T={t}:  contended/op {c0:.5} → {c3:.5} ({:.2}x)   U(hold) {:.1}% → {:.1}% ({:.2}x)   gap {g0:.5} → {gap_ms:.5} ms ({:.2}x)",
                    c3 / c0.max(f64::MIN_POSITIVE),
                    100.0 * u0,
                    100.0 * u,
                    u / u0.max(f64::MIN_POSITIVE),
                    gap_ms / g0.max(f64::MIN_POSITIVE)
                );
                println!();
            }
        }
    }
    println!("⭐ THE DISCRIMINATOR, NOW SELF-CONTAINED. If U(hold) collapses across L0→L3 while");
    println!("   contended/op stays pinned near its L0 value, threads are still colliding on a lock");
    println!("   that is mostly idle — they are not arriving at random into free time. If instead");
    println!("   contended/op falls in step with U(hold), the lock simply stopped being busy.");
}

fn mode_direct(dir: &Path, n: usize, threads: &[usize], warm: usize) {
    println!("CEILING — MODE=direct. THE L0→L3 KEY-COUNT LADDER, AS A CONTENTION COUNT.");
    println!();
    println!("⚠ LAYER: `TableBranchCatalog` driven directly (open_sidecar + cat.fork), holding");
    println!("  `logical` and NOTHING ELSE. This is the layer `examples/fork_concurrency.rs` drives");
    println!("  and the layer the recorded 21x/1.9x/12.6x pair was measured at. It is NOT what any");
    println!("  shipped front-end does — see MODE=pgwire. (E.6; INVENTION-TRIGGER Amendment 1.)");
    println!();
    println!("⛔ Stubbed levels are NOT a database. L1 writes no child record; L2 leaks retired");
    println!("   slots; L3 also drops the envelope (capability escape) and the parent's live-child");
    println!("   entry (GC hole). Every level KEEPS write_header + stage so >=1 page is dirty and");
    println!("   the fsync stays real — `syncs` is the check that this held.");
    println!();
    println!("   contended/op  = acquisitions of `logical` whose try_lock() failed, per fork.");
    println!("                   UPPER bound on blocking (holder may release in the window), so a");
    println!("                   LOW value is the STRONG direction. See src/branch/lockcount.rs.");
    println!("   acq/op        = acquisitions of `logical` per fork. Free, and it bounds the answer.");
    println!("   forks/sec     = UPPER BOUND ONLY. Box not quiet; stamped below. Counts are the evidence.");
    stamp("direct_start");
    println!();
    println!(
        "  {:>7} {:>5} {:>9} {:>9} {:>14} {:>10} {:>10} {:>8} {:>8}",
        "threads", "stub", "forks", "ops", "contended/op", "acq/op", "cont.frac", "syncs", "f/sync"
    );
    for &t in threads {
        let mut base_ct = f64::NAN;
        let mut base_tp = f64::NAN;
        let mut rows: Vec<(u8, f64, f64, f64)> = Vec::new();
        for stub in 0u8..=3 {
            let a = run_direct(dir, &format!("d{t}_{stub}"), n, t, warm, stub, false, false);
            let c = &a.counts;
            let ct = c.contended_per_op(lk::LK_LOGICAL);
            let tp = a.forks as f64 / a.secs;
            if stub == 0 {
                base_ct = ct;
                base_tp = tp;
            }
            if a.syncs == 0 {
                refuse(&format!(
                    "T={t} L{stub}: syncs collapsed to 0. The rung lost DURABILITY, not serial \
                     work, and D123's own validity gate voids it."
                ));
            }
            println!(
                "  {:>7} {:>5} {:>9} {:>9} {:>14.5} {:>10.4} {:>9.1}% {:>8} {:>8.1}",
                t,
                stub,
                a.forks,
                c.ops,
                ct,
                c.acq_per_op(lk::LK_LOGICAL),
                100.0 * c.contended_frac(lk::LK_LOGICAL),
                a.syncs,
                a.forks as f64 / a.syncs.max(1) as f64
            );
            rows.push((stub, ct, tp, a.secs));
        }
        println!("    L0→L3 at T={t}:  contended/op {:.5} → {:.5}  ({:.2}x)   [forks/sec {:.0} → {:.0}, {:.2}x, UPPER BOUND]",
            base_ct, rows[3].1,
            if base_ct > 0.0 { rows[3].1 / base_ct } else { f64::NAN },
            base_tp, rows[3].2, rows[3].2 / base_tp);
        println!();
    }

    // The cross-check that licenses the pgwire arm's global-atomic mode.
    println!("  CROSS-CHECK — the same rung counted BOTH ways. Thread-local accumulation is used");
    println!("  above; the pgwire arm must use process-wide atomics because the library owns its");
    println!("  connection threads. If the two modes disagree, one of them is losing counts.");
    let t = *threads.last().unwrap_or(&64);
    let tl = run_direct(dir, "xc_tl", n, t, warm, 0, false, false);
    let gl = run_direct(dir, "xc_gl", n, t, warm, 0, true, false);
    let a = tl.counts.contended_per_op(lk::LK_LOGICAL);
    let b = gl.counts.contended_per_op(lk::LK_LOGICAL);
    println!("    T={t} L0  thread-local {a:.5}   global-atomic {b:.5}   ratio {:.4}", b / a.max(f64::MIN_POSITIVE));
    println!("    (acq/op {:.4} vs {:.4}; ops {} vs {})",
        tl.counts.acq_per_op(lk::LK_LOGICAL), gl.counts.acq_per_op(lk::LK_LOGICAL), tl.counts.ops, gl.counts.ops);
    println!();
    println!("⭐ THE DISCRIMINATOR (INVENTION-TRIGGER Amendment 2, pre-registered):");
    println!("   contention per op RISES as the section shortens  ⇒ the gap is park/unpark handoff.");
    println!("   it does NOT rise                                 ⇒ the remaining time is not threads");
    println!("                                                      blocking on each other, and");
    println!("                                                      pre-registered OUTCOME 1 IS DEAD.");
}

// =================================================================================================
// pgwire — LAYER B: what production actually does
// =================================================================================================

struct Client {
    w: TcpStream,
    r: BufReader<TcpStream>,
}

struct Reply {
    texts: Vec<String>,
    error: Option<String>,
}

impl Client {
    fn connect(addr: std::net::SocketAddr) -> std::io::Result<Client> {
        let w = TcpStream::connect(addr)?;
        w.set_nodelay(true)?;
        let r = BufReader::new(w.try_clone()?);
        let mut c = Client { w, r };
        let mut body = Vec::new();
        body.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
        for (k, v) in [("user", "ceiling"), ("database", "ceiling")] {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        c.w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
        c.w.write_all(&body)?;
        c.w.flush()?;
        let hello = c.read_to_ready()?;
        if let Some(e) = hello.error {
            return Err(std::io::Error::other(format!("startup refused: {e}")));
        }
        Ok(c)
    }

    fn query(&mut self, sql: &str) -> std::io::Result<Reply> {
        let mut body = Vec::with_capacity(sql.len() + 1);
        body.extend_from_slice(sql.as_bytes());
        body.push(0);
        self.w.write_all(b"Q")?;
        self.w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
        self.w.write_all(&body)?;
        self.w.flush()?;
        self.read_to_ready()
    }

    fn terminate(&mut self) -> std::io::Result<()> {
        self.w.write_all(b"X")?;
        self.w.write_all(&4i32.to_be_bytes())?;
        self.w.flush()
    }

    fn read_to_ready(&mut self) -> std::io::Result<Reply> {
        let mut out = Reply { texts: Vec::new(), error: None };
        loop {
            let mut tag = [0u8; 1];
            self.r.read_exact(&mut tag)?;
            let mut lenb = [0u8; 4];
            self.r.read_exact(&mut lenb)?;
            let len = i32::from_be_bytes(lenb);
            if len < 4 {
                return Err(std::io::Error::other(format!("backend message length {len}")));
            }
            let mut body = vec![0u8; (len - 4) as usize];
            self.r.read_exact(&mut body)?;
            match tag[0] {
                b'Z' => return Ok(out),
                b'E' => {
                    if out.error.is_none() {
                        out.error = Some(decode_error(&body));
                    }
                }
                b'D' => decode_row(&body, &mut out.texts),
                _ => {}
            }
        }
    }
}

fn decode_error(body: &[u8]) -> String {
    let mut i = 0usize;
    let mut msg = String::new();
    while i < body.len() && body[i] != 0 {
        let code = body[i];
        i += 1;
        let start = i;
        while i < body.len() && body[i] != 0 {
            i += 1;
        }
        let val = String::from_utf8_lossy(&body[start..i]).to_string();
        i += 1;
        if code == b'M' {
            msg = val;
        }
    }
    if msg.is_empty() { "<error with no message field>".into() } else { msg }
}

fn decode_row(body: &[u8], out: &mut Vec<String>) {
    if body.len() < 2 {
        return;
    }
    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut i = 2usize;
    for _ in 0..count {
        if i + 4 > body.len() {
            return;
        }
        let n = i32::from_be_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
        i += 4;
        if n < 0 {
            continue;
        }
        let n = n as usize;
        if i + n > body.len() {
            return;
        }
        out.push(String::from_utf8_lossy(&body[i..i + n]).to_string());
        i += n;
    }
}

fn branch_names(reply: &Reply) -> Vec<String> {
    reply
        .texts
        .iter()
        .filter(|s| {
            s.strip_prefix("b_").is_some_and(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))
        })
        .cloned()
        .collect()
}

struct Rig {
    addr: std::net::SocketAddr,
    _dir: PathBuf,
}

/// The shape `examples/pgserver.rs` ships, lifted from `examples/e6_outer_lock_count.rs::rig` so
/// the two harnesses drive the same server and their counts are comparable.
///
/// **No `LeaseThread`**, for E.6's reason: its periodic scan takes the same outermost mutex, and
/// those acquisitions are not attributable to client statements. Leaving it out makes every outer
/// count a LOWER bound on production, which is the safe direction here.
fn rig(root: &Path, tag: &str) -> Rig {
    let dir = root.join(format!("ceiling-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("rig dir");
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
    let store: Arc<ArenaPageStore> = Arc::new(
        ArenaPageStore::new(bp.clone(), branches.clone() as Arc<dyn BranchCatalog>, base).unwrap(),
    );
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches.clone() as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            store.clone() as Arc<dyn PageStore>,
        )
        .expect("storage-backed runtime"),
    );

    let ctx = Arc::new(ServerContext::new(catalog, bp, txn, runtime));
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    std::thread::spawn(move || {
        let _ = serve(listener, ctx);
    });
    Rig { addr, _dir: dir }
}

fn outer_acq() -> u64 {
    catalog_acquisitions().unwrap_or_else(|| {
        refuse(
            "`pgwire::catalog_acquisitions()` is None: built without `--features lock_census`. \
             That is not zero acquisitions, it is no measurement.",
        )
    })
}

fn outer_cont() -> u64 {
    catalog_contended().unwrap_or_else(|| {
        refuse("`pgwire::catalog_contended()` is None: built without `--features lock_census`.")
    })
}

struct PgArm {
    forks: usize,
    secs: f64,
    outer_acq: u64,
    outer_cont: u64,
    inner: lk::Counts,
}

fn run_pgwire(root: &Path, tag: &str, t: usize, f: usize, stub: u8) -> PgArm {
    let rig = rig(root, tag);
    let addr = rig.addr;

    probe::configure(false, stub, 0);
    assert_probe(stub, &format!("pgwire rung {tag} (L{stub})"));
    // GLOBAL mode: `serve()` owns the connection threads, so no thread-local can be flushed
    // before the counters are read. See `lockcount::GLOBAL_MODE`.
    lk::set_global_mode(true);
    lk::reset();
    lk::set_enabled(true);
    // Snapshot AFTER the rig is built: `Catalog::create` and the runtime's construction are
    // fixture cost, not statement cost.
    let oa0 = outer_acq();
    let oc0 = outer_cont();

    let mut reported: BTreeSet<String> = BTreeSet::new();
    let mut asked = 0usize;
    let t0 = Instant::now();
    std::thread::scope(|s| {
        let mut hs = Vec::with_capacity(t);
        for th in 0..t {
            hs.push(s.spawn(move || {
                let mut names: Vec<String> = Vec::new();
                let mut asked = 0usize;
                let mut c = Client::connect(addr).expect("connect");
                for i in 0..f {
                    let r = c
                        .query(&format!("BEGIN AGENT SESSION AS 'a{th}' RUN 'r{th}_{i}';"))
                        .expect("BEGIN AGENT SESSION round trip");
                    if let Some(e) = &r.error {
                        refuse(&format!(
                            "BEGIN AGENT SESSION returned an error: {e}. A refused fork is a fork \
                             that did not happen; averaging it in reports a mediated path as an \
                             unmediated one."
                        ));
                    }
                    names.extend(branch_names(&r));
                    asked += 1;
                    let r = c.query("ABANDON;").expect("ABANDON round trip");
                    if let Some(e) = &r.error {
                        refuse(&format!("ABANDON returned an error: {e}"));
                    }
                }
                let _ = c.terminate();
                (names, asked)
            }));
        }
        for h in hs {
            let (names, a) = h.join().expect("client thread");
            reported.extend(names);
            asked += a;
        }
    });
    let secs = t0.elapsed().as_secs_f64();
    lk::set_enabled(false);

    if reported.len() != asked {
        refuse(&format!(
            "{tag}: asked for {asked} forks but the server named {} distinct branches. The fork \
             total may not come from this harness's own counter when the two witnesses disagree.",
            reported.len()
        ));
    }
    let inner = lk::snapshot();
    PgArm {
        forks: asked,
        secs,
        outer_acq: outer_acq() - oa0,
        outer_cont: outer_cont() - oc0,
        inner,
    }
}

fn mode_pgwire(root: &Path, f: usize, threads: &[usize]) {
    println!("CEILING — MODE=pgwire. THE SAME L0→L3 LADDER, AT THE LAYER PRODUCTION RUNS.");
    println!();
    println!("⚠ LAYER: the shipped pgwire server over a real TCP socket, one client connection per");
    println!("  thread, forks issued as `BEGIN AGENT SESSION`. Locks held, outermost first:");
    println!("  `ServerContext::catalog()` -> `AgentRuntime` state -> `TableBranchCatalog::logical`.");
    println!("  E.6 measured that this is NOT the same experiment as MODE=direct.");
    println!();
    println!("  outer = ServerContext::catalog(), the per-statement mutex every connection waits on.");
    println!("  inner = TableBranchCatalog::logical, the mutex the 21x/1.9x pair is about.");
    println!("  Counted with process-wide atomics (the library owns these threads) — the `direct`");
    println!("  mode's cross-check is what licenses that.");
    stamp("pgwire_start");
    println!();
    println!(
        "  {:>7} {:>5} {:>7} {:>10} {:>12} {:>10} {:>12} {:>9}",
        "threads", "stub", "forks", "outer/op", "outerCont/op", "inner/op", "innerCont/op", "stmts/sec"
    );
    for &t in threads {
        for stub in 0u8..=3 {
            let a = run_pgwire(root, &format!("p{t}_{stub}"), t, f, stub);
            let ops = a.forks.max(1) as f64;
            if a.inner.ops as usize != a.forks {
                refuse(&format!(
                    "T={t} L{stub}: the inner census counted {} forks against {} the server named. \
                     A per-operation ratio from two disagreeing witnesses is not a measurement.",
                    a.inner.ops, a.forks
                ));
            }
            println!(
                "  {:>7} {:>5} {:>7} {:>10.4} {:>12.5} {:>10.4} {:>12.5} {:>9.1}",
                t,
                stub,
                a.forks,
                a.outer_acq as f64 / ops,
                a.outer_cont as f64 / ops,
                a.inner.acq_per_op(lk::LK_LOGICAL),
                a.inner.contended_per_op(lk::LK_LOGICAL),
                a.forks as f64 / a.secs
            );
        }
        println!();
    }
    println!("⚠ stmts/sec is an UPPER BOUND on a loaded box and is NOT evidence here. Each fork is");
    println!("  two TCP round trips, so this arm's throughput is bounded by the socket, not the lock.");
}

// =================================================================================================

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "model".into());
    let n: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(8000);
    let threads: Vec<usize> = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "1,8,64".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let warm: usize = std::env::args().nth(4).and_then(|s| s.parse().ok()).unwrap_or(0);

    let dir = std::env::temp_dir().join(format!("ferrodb-ceiling-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    println!("⛔ CEILING MEASUREMENT SCAFFOLD — branch CEILING-park-or-structure, MUST NEVER MERGE.");
    println!(
        "   PROVENANCE at process start, read back from the probe: stub_level={} extra_upserts={} \
         extra_new_keys={} probe_clock={}",
        probe::stub_level(),
        probe::extra_upserts(),
        probe::extra_new_keys(),
        probe::enabled()
    );
    assert_probe(0, "process start");
    println!("   Every rung re-asserts this. A non-zero stub or extra count REFUSES, it does not warn.");
    println!();
    match mode.as_str() {
        "model" => mode_model(),
        "direct" => mode_direct(&dir, n, &threads, warm),
        "paired" => mode_paired(&dir, n, &threads, warm),
        "pgwire" => mode_pgwire(&dir, n, &threads),
        other => refuse(&format!("unknown mode `{other}` (model | direct | paired | pgwire)")),
    }
    stamp("end");
    let _ = std::fs::remove_dir_all(&dir);
}
