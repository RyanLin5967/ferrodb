//! W4 — how long does `forget_reaped_branches` block an unrelated statement?
//!
//! `AgentRuntime` holds one `Mutex<State>`, taken by every statement that touches branch state.
//! `forget_reaped_branches` runs in three phases: phase 1 walks `state.workspaces` **holding that
//! lock**, phase 2 asks the catalog about each candidate with **no lock held**, phase 3 re-takes
//! the lock for the survivors. So its *wall* time is dominated by phase 2 (O(candidates) catalog
//! reads) while its *blocking* time is phase 1 + phase 3 — O(open sessions), not O(total
//! branches). Those are different numbers and this harness reports both, because quoting the wall
//! time as the stall is the error this measurement exists to avoid.
//!
//!   statement_lock_sweep [S,comma,separated] [window_ms] [op_period_ms] [gone] [reps]
//!
//! TWO tables, because the sweep has two lock-held phases and they are exercised by different
//! fixtures. Table 1 is the steady state — nothing is reap-eligible, so only phase 1 (the walk
//! over `workspaces`) runs. Table 2 is the case that actually happens when the lease reaper fires:
//! `gone` of the S branches have been reaped in the catalog, so phase 3 runs too. Phase 3 calls
//! `capture_is_protected`, which scans **every** workspace per removed branch, so it is O(gone x S)
//! under the lock — measuring only table 1 would report the smaller, calmer number and miss it.
//!
//! Three arms per S, all driven by one prober thread that does nothing but take the lock and let
//! go (`quarantine_reason` against an empty map — the cheapest public call that acquires it),
//! timing each acquisition in NANOSECONDS. Each arm runs for the same wall window and fires its
//! operation on the same period, so the three sample sets are the same size of draw taken against
//! the same duty cycle.
//!
//!   idle     — the prober alone; the arm operation never takes the lock. The NEGATIVE control:
//!              what the prober reports when there is provably nothing to see. A stall here is
//!              instrument noise, not a wall.
//!   sweep    — `forget_reaped_branches()` once per period. The thing under test.
//!   activity — `run_activity()` once per period. The POSITIVE control, and deliberately something
//!              this work does not touch: it walks every workspace under the lock by construction
//!              (its own doc comment says so), so it must keep separating from `idle` no matter
//!              what happens to `sweep`. A run where `sweep` goes flat and `activity` goes flat
//!              with it has measured a broken prober, not a fix.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::session::AgentSession;
use ferrodb::branch::types::{BranchId, BranchState};

/// Sorted latency samples, in nanoseconds.
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
    /// Mean of the `k` largest samples.
    ///
    /// The readout the structure of this experiment actually calls for. The holder takes the lock
    /// once per op and a waiter blocks for the remainder of that one hold, so there is roughly ONE
    /// blocked acquisition per op and everything else is uncontended — a bimodal draw in which the
    /// mean is meaningless and a lone `max` is one sample. With `k = op_n` this averages exactly
    /// the population that did the waiting, and degrades honestly: if nothing blocked, it reads
    /// the uncontended tail and stays small.
    fn top_mean(&self, k: usize) -> u64 {
        let k = k.min(self.0.len());
        if k == 0 {
            return 0;
        }
        let tail = &self.0[self.0.len() - k..];
        (tail.iter().map(|v| *v as u128).sum::<u128>() / k as u128) as u64
    }
}

/// What one arm produced.
struct Arm {
    probe: Samples,
    /// Per-call wall times of the arm's own operation, nanoseconds.
    op: Samples,
    /// How many of this arm's iterations took LONGER than `period`, so no sleep happened and the
    /// loop ran that iteration back-to-back with the next. See the overrun guard in [`arm`]: past
    /// a small fraction this stops being the design the doc comment above describes.
    overran: usize,
}

/// Run `op` once every `period` for `window`, while a prober thread takes the state lock every
/// 50µs and times each acquisition.
///
/// **`period`, not back-to-back, and that is the whole design.** Fired back to back the holder
/// keeps the lock at a ~100% duty cycle, so what the prober records is a queue behind many holds
/// rather than the length of one — and the duty cycle then varies with S all by itself, which
/// makes the S-to-S comparison measure the harness. Firing on a fixed period leaves the lock free
/// almost all of the time, so `probe_max` reads the length of a single hold, which is the quantity
/// the question is about. It is also what the lease thread actually does: one sweep per interval.
///
/// The prober sleeps between acquisitions for the matching reason: a prober that spins can starve
/// the holder, and a starved holder produces a reassuring flat line for the wrong reason. The
/// `activity` arm is what catches that if it ever happens — it uses this identical protocol, so a
/// protocol that cannot see a stall cannot see that one either.
///
/// ⛔ **CORRECTED 2026-09-18: this loop ABANDONED the period silently, and it did so exactly where
/// the numbers matter.** The sleep is `period.checked_sub(t0.elapsed())`, which is `None` when the
/// op outran the period — and `if let Some(rest)` then simply does not sleep. So once `op` is
/// slower than `period` the loop becomes **back-to-back**, which is the ~100% duty cycle the
/// paragraph above says *"makes the S-to-S comparison measure the harness"*. Nothing reported the
/// switch. It is worst at large S, because that is where the op is slow: at S ≥ 10⁴ a
/// `forget_reaped_branches` that W4 measured in **seconds** cannot fit a millisecond period, so the
/// large-S end of every table was taken in the mode this design explicitly rejects, while the
/// header still described the periodic one.
///
/// ⇒ **The fix is NOT to sleep anyway** — that would silently change the duty cycle instead of
/// silently changing the protocol, which is no better. Overruns are now COUNTED, reported per arm,
/// and **refused** past `MAX_OVERRUN_FRACTION`: a run whose stated premise is false is not a result
/// to be caveated. The operator's remedy is a larger `op_period_ms`, and the refusal says so.
fn arm(rt: &Arc<AgentRuntime>, window: Duration, period: Duration, mut op: impl FnMut()) -> Arm {
    let stop = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicU64::new(0));
    let probe_rt = Arc::clone(rt);
    let probe_stop = Arc::clone(&stop);
    let probe_started = Arc::clone(&started);
    let prober = std::thread::spawn(move || {
        probe_started.store(1, Ordering::SeqCst);
        let mut out: Vec<u64> = Vec::with_capacity(1 << 17);
        while !probe_stop.load(Ordering::Relaxed) {
            let t0 = Instant::now();
            // Reads `quarantine_reasons`, empty in this fixture: as close to a bare
            // acquire/release as the public API gets, so what this times is waiting, not work.
            let _ = probe_rt.quarantine_reason(BranchId::TRUNK);
            out.push(t0.elapsed().as_nanos() as u64);
            std::thread::sleep(Duration::from_micros(50));
        }
        out
    });

    while started.load(Ordering::SeqCst) == 0 {
        std::hint::spin_loop();
    }
    // Let the prober reach steady state before the holder starts competing with it.
    std::thread::sleep(Duration::from_millis(20));

    let deadline = Instant::now() + window;
    let mut walls: Vec<u64> = Vec::new();
    let mut overran = 0usize;
    while Instant::now() < deadline {
        let t0 = Instant::now();
        op();
        walls.push(t0.elapsed().as_nanos() as u64);
        // Sleep the remainder of the period. `checked_sub` is `None` exactly when the op outran
        // the period — that iteration therefore ran back-to-back with the next, and it is COUNTED
        // rather than passed over, because the doc comment above makes the periodic duty cycle a
        // load-bearing part of what the numbers mean.
        match period.checked_sub(t0.elapsed()) {
            Some(rest) => std::thread::sleep(rest),
            None => overran += 1,
        }
    }
    stop.store(true, Ordering::Relaxed);
    Arm { probe: Samples::of(prober.join().unwrap()), op: Samples::of(walls), overran }
}

/// A runtime holding `s` open sessions, and the handles that keep them open.
fn fixture(s: usize) -> (Arc<AgentRuntime>, Vec<AgentSession>) {
    let rt = Arc::new(AgentRuntime::new());
    let sessions: Vec<AgentSession> = (0..s)
        .map(|i| rt.begin_session("prober", Some(&format!("r{i}")), BranchId::TRUNK).expect("fork"))
        .collect();
    let live = rt.run_activity().len();
    assert_eq!(live, s, "wanted {s} open sessions, runtime holds {live}");
    (rt, sessions)
}

/// ONE sweep against a fresh fixture in which `g` branches really have been reaped, timed by the
/// same prober.
///
/// Phase 3 is self-consuming: a sweep removes the branches it found, so a second sweep against one
/// fixture has `gone = 0` and measures nothing. The periodic arm cannot be used here at all, and
/// rebuilding the fixture is the only honest way to take more than one sample. Each rep
/// contributes the prober's largest stall while that single sweep ran.
fn oneshot(
    s: usize,
    g: usize,
    reps: usize,
    targeted: bool,
    probe_ns: u64,
) -> (Samples, Samples, usize) {
    let mut stalls: Vec<u64> = Vec::with_capacity(reps);
    let mut walls: Vec<u64> = Vec::with_capacity(reps);
    // Fixtures whose sweep no probe acquisition overlapped; see the refusal below.
    let mut blind = 0usize;
    for _ in 0..reps {
        let (rt, sessions) = fixture(s);
        // Reap `g` branches behind the runtime's back -- exactly what the lease reaper does, and
        // the reason `forget_reaped_branches` exists at all: nothing tells the runtime.
        for sess in sessions.iter().take(g) {
            let rec = rt.branches().get(sess.branch).expect("live record");
            rt.branches()
                .set_state(sess.branch, rec.state, BranchState::Reaped)
                .expect("mark the record reaped");
        }
        // Exactly what `reap_expired` hands `scan_once`, for the targeted arm to be given.
        let reaped: Vec<BranchId> = sessions.iter().take(g).map(|sess| sess.branch).collect();

        let stop = Arc::new(AtomicBool::new(false));
        let probe_rt = Arc::clone(&rt);
        let probe_stop = Arc::clone(&stop);
        // One clock shared by the prober and the sweep, so a sample can be placed in time
        // relative to the sweep rather than merely counted.
        let base = Instant::now();
        let prober = std::thread::spawn(move || {
            // (offset from `base` when the acquisition STARTED, how long it took), nanoseconds.
            let mut out: Vec<(u64, u64)> = Vec::with_capacity(1 << 14);
            while !probe_stop.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                let _ = probe_rt.quarantine_reason(BranchId::TRUNK);
                out.push((
                    t0.duration_since(base).as_nanos() as u64,
                    t0.elapsed().as_nanos() as u64,
                ));
                std::thread::sleep(Duration::from_nanos(probe_ns));
            }
            out
        });
        std::thread::sleep(Duration::from_millis(20));

        let sweep_start = Instant::now().duration_since(base).as_nanos() as u64;
        let t0 = Instant::now();
        // `recon` is the full reconciliation -- the backstop, O(open sessions). `fast` is
        // `forget_branches(&reaped)`, which is what `scan_once` calls on every successful tick
        // now that it stops re-deriving a list `reap_expired` already handed it.
        let dropped =
            if targeted { rt.forget_branches(&reaped) } else { rt.forget_reaped_branches() };
        walls.push(t0.elapsed().as_nanos() as u64);
        let sweep_end = Instant::now().duration_since(base).as_nanos() as u64;
        assert_eq!(dropped, g, "fixture reaped {g} branches, sweep forgot {dropped}");

        stop.store(true, Ordering::Relaxed);
        // **Only acquisitions that overlap the sweep count.** Taking the max over the prober's
        // whole lifetime attributed to the sweep anything that happened during the 20 ms warm-up
        // above -- when NOTHING held the lock -- or in the tail after it finished. That is not a
        // hypothetical contamination: the idle control in table 1 reaches tens of milliseconds on
        // this machine with the fleet running, which is the same magnitude as the whole AFTER
        // column. An acquisition overlaps if it started before the sweep ended and had not yet
        // returned when the sweep began.
        let samples = prober.join().unwrap();
        let overlapping: Vec<u64> = samples
            .iter()
            .filter(|(off, dur)| off + dur > sweep_start && *off < sweep_end)
            .map(|(_, dur)| *dur)
            .collect();
        // Zero overlapping samples is a fact about the run, not a zero stall: a sweep shorter than
        // the probe interval can finish between two acquisitions. Refusing is the only honest
        // reading -- reporting 0 would say "never blocked" about something never observed.
        //
        // **Refused PER SAMPLE rather than by panicking the process.** This used to assert, which
        // meant one unobservable cell destroyed every row after it: the run that found it lost the
        // S=10^5 row entirely, which is the row the headline quotes. The guard's meaning is
        // unchanged -- no number is invented for a sweep nobody saw -- but the cells that WERE
        // measured survive, and `blind` is carried out so the caller can print "--" instead of a
        // figure and exit non-zero. A blind cell is a fact about the instrument's resolution
        // against this sweep, and at these sizes it is itself a result: the fast path's lock hold
        // is short enough to fall between two probes.
        if overlapping.is_empty() {
            blind += 1;
            eprintln!(
                "# BLIND: no probe overlapped a {} ns sweep (S={s} g={g} targeted={targeted}, \
                 {} samples at {probe_ns} ns spacing)",
                sweep_end - sweep_start,
                samples.len()
            );
        } else {
            stalls.push(Samples::of(overlapping).max());
        }
        drop(sessions);
    }
    (Samples::of(stalls), Samples::of(walls), blind)
}

fn mean(s: &Samples) -> u64 {
    if s.0.is_empty() {
        0
    } else {
        (s.0.iter().map(|v| *v as u128).sum::<u128>() / s.0.len() as u128) as u64
    }
}

/// Past this fraction of iterations outrunning the period, the arm was NOT running the protocol
/// its doc comment describes and the S-to-S comparison measures the harness. 5% is a judgement:
/// it is loose enough that one slow iteration in a short window does not abort a run, and tight
/// enough that the back-to-back regime cannot hide in it.
const MAX_OVERRUN_FRACTION: f64 = 0.05;

/// Refuse a run whose periodic premise has stopped holding, naming the remedy. Refused BEFORE the
/// table is printed rather than footnoted after it, for the same reason the disjointness guard in
/// `bufpool_fault_concurrency.rs` refuses: a run whose stated premise is false is not a result to
/// be caveated.
fn check_overrun(s: usize, name: &str, a: &Arm) {
    let n = a.op.n();
    if n == 0 {
        return;
    }
    let frac = a.overran as f64 / n as f64;
    if frac > MAX_OVERRUN_FRACTION {
        eprintln!(
            "\nPERIOD OVERRUN: S={s} arm={name}: {} of {n} iterations ({:.1}%) took LONGER than the \
             op period, so those ran BACK-TO-BACK and the holder's duty cycle was not the one this \
             harness is built on. Back to back, probe_max reads a queue behind many holds rather \
             than the length of one, and the duty cycle then varies with S by itself — which makes \
             the S-to-S comparison a measurement of the harness. Raise op_period_ms above the \
             observed op wall time (max {} ns for this arm) and re-run.",
            a.overran,
            frac * 100.0,
            a.op.max(),
        );
        std::process::exit(2);
    }
}

fn row(s: usize, name: &str, a: &Arm) {
    check_overrun(s, name, a);
    println!(
        "{:>8} {:>7} {:>9} {:>8} {:>10} {:>11} {:>11} {:>7} {:>11} {:>11} {:>8}",
        s,
        name,
        a.probe.n(),
        a.probe.pct(0.50),
        a.probe.pct(0.999),
        a.probe.top_mean(a.op.n()),
        a.probe.max(),
        a.op.n(),
        mean(&a.op),
        a.op.max(),
        a.overran,
    );
}

fn main() {
    let sweep: Vec<usize> = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "10,100,1000,10000".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let window = Duration::from_millis(
        std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(3000),
    );
    let period = Duration::from_millis(
        std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(2),
    );
    let gone: usize = std::env::args().nth(4).and_then(|s| s.parse().ok()).unwrap_or(64);
    let reps: usize = std::env::args().nth(5).and_then(|s| s.parse().ok()).unwrap_or(7);
    // Prober spacing, nanoseconds. Default 50 us, the value every artifact before this used.
    // Lower it to resolve a sweep short enough to fall between two probes -- at the cost of a
    // busier prober, which is itself contention, so the idle control is what says whether a
    // tighter setting is still honest.
    let probe_ns: u64 = std::env::args().nth(6).and_then(|s| s.parse().ok()).unwrap_or(50_000);

    println!(
        "# W4 statement-lock sweep. os={} window={:?} op_period={:?} gone={} reps={}",
        std::env::consts::OS,
        window,
        period,
        gone,
        reps
    );
    println!("# prober = AgentRuntime::quarantine_reason (acquires the state lock, reads an empty map)");
    println!("# probe_* = one unrelated statement's lock acquisition, NANOSECONDS");
    println!("# stall_mean = mean of the op_n largest probe samples: one acquisition blocks per op,");
    println!("#           so this averages the population that waited. THE HEADLINE NUMBER.");
    println!("# op_*    = the arm's own operation, wall, NANOSECONDS (sweep wall is dominated by");
    println!("#           phase 2, which holds NO lock -- it is not the blocking number)");
    println!("# overran = iterations whose op took LONGER than the period, so no sleep happened and");
    println!("#           that iteration ran BACK-TO-BACK. Must be small: past 5% the run REFUSES,");
    println!("#           because back to back the duty cycle varies with S by itself and the");
    println!("#           S-to-S comparison then measures the harness. Before 2026-09-18 this was");
    println!("#           neither counted nor reported, and at S>=10^4 it was the normal case.");
    println!(
        "{:>8} {:>7} {:>9} {:>8} {:>10} {:>11} {:>11} {:>7} {:>11} {:>11} {:>8}",
        "S", "arm", "probe_n", "p50", "p999", "stall_mean", "probe_max", "op_n", "op_mean",
        "op_max", "overran"
    );

    println!("#");
    println!("# TABLE 1 -- steady state: nothing is reap-eligible, so only PHASE 1 runs.");
    for &s in &sweep {
        let t_open = Instant::now();
        let (rt, sessions) = fixture(s);
        eprintln!("# T1 S={s}: opened {s} sessions in {} ms", t_open.elapsed().as_millis());

        // ARM 1 (under test) first, so it sees the coldest caches: the arm most likely to be
        // flattered by warm state should not be the one that gets it.
        let sweep_rt = Arc::clone(&rt);
        let a_sweep = arm(&rt, window, period, move || {
            let dropped = sweep_rt.forget_reaped_branches();
            assert_eq!(dropped, 0, "nothing is reap-eligible in this fixture");
        });

        // ARM 2 (positive control).
        let act_rt = Arc::clone(&rt);
        let a_act = arm(&rt, window, period, move || {
            std::hint::black_box(act_rt.run_activity().len());
        });

        // ARM 0 (negative control): the prober alone, same window and same period, doing
        // something that never takes the lock at all.
        let a_idle = arm(&rt, window, period, || std::thread::yield_now());

        row(s, "idle", &a_idle);
        row(s, "sweep", &a_sweep);
        row(s, "activ", &a_act);
        drop(sessions);
    }

    println!("#");
    println!("# TABLE 2 -- a reap really happened: {gone} of the S branches are gone from the");
    println!("# catalog, so PHASE 3 runs as well. One sweep per fresh fixture, {reps} fixtures;");
    println!("# stall_* is the prober's largest wait during that single sweep, NANOSECONDS.");
    println!("#");
    println!("# TWO ARMS. `recon` is `forget_reaped_branches` -- the reconciliation, which walks");
    println!("# every open session and is the BACKSTOP the lease thread now runs only when a scan");
    println!("# failed and cannot report what it reaped. `fast` is `forget_branches(&reaped)`,");
    println!("# which is what `scan_once` calls on every successful tick: O(gone), not O(S).");
    println!("#");
    println!("# `blind` counts fixtures whose sweep NO probe acquisition overlapped -- the sweep");
    println!("# finished between two probes {probe_ns} ns apart. Those contribute no stall sample and");
    println!("# stall_* reads `--` when every fixture in the cell was blind. That is a fact about");
    println!("# this instrument's resolution, not a zero: a sweep nobody saw is unmeasured, not");
    println!("# fast. wall_* is timed around the sweep call itself and is never blind.");
    println!(
        "{:>8} {:>7} {:>6} {:>6} {:>6} {:>12} {:>12} {:>12} {:>12}",
        "S", "gone", "reps", "arm", "blind", "stall_med", "stall_max", "wall_med", "wall_max"
    );
    let mut any_blind = false;
    for &s in &sweep {
        let g = gone.min(s);
        for (name, targeted) in [("recon", false), ("fast", true)] {
            let t0 = Instant::now();
            let (stalls, walls, blind) = oneshot(s, g, reps, targeted, probe_ns);
            eprintln!(
                "# T2 S={s} g={g} {name}: {reps} fixtures in {} ms ({blind} blind)",
                t0.elapsed().as_millis()
            );
            any_blind |= blind > 0;
            let (med, max) = if stalls.n() == 0 {
                ("--".to_string(), "--".to_string())
            } else {
                (stalls.pct(0.50).to_string(), stalls.max().to_string())
            };
            println!(
                "{:>8} {:>7} {:>6} {:>6} {:>6} {:>12} {:>12} {:>12} {:>12}",
                s,
                g,
                reps,
                name,
                blind,
                med,
                max,
                walls.pct(0.50),
                walls.max(),
            );
        }
    }
    if any_blind {
        println!("#");
        println!("# EXIT 1: at least one fixture's sweep was never observed by the prober. The wall");
        println!("# columns stand; the affected stall cells are unmeasured and must not be quoted.");
        std::process::exit(1);
    }
}
