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
use ferrodb::branch::types::BranchId;

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
    while Instant::now() < deadline {
        let t0 = Instant::now();
        op();
        walls.push(t0.elapsed().as_nanos() as u64);
        // Sleep the remainder of the period, if the op did not already outrun it.
        if let Some(rest) = period.checked_sub(t0.elapsed()) {
            std::thread::sleep(rest);
        }
    }
    stop.store(true, Ordering::Relaxed);
    Arm { probe: Samples::of(prober.join().unwrap()), op: Samples::of(walls) }
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
fn oneshot(s: usize, g: usize, reps: usize, targeted: bool) -> (Samples, Samples) {
    let mut stalls: Vec<u64> = Vec::with_capacity(reps);
    let mut walls: Vec<u64> = Vec::with_capacity(reps);
    for _ in 0..reps {
        let (rt, sessions) = fixture(s);
        // Reap `g` branches behind the runtime's back -- exactly what the lease reaper does, and
        // the reason `forget_reaped_branches` exists at all: nothing tells the runtime.
        for sess in sessions.iter().take(g) {
            let mut rec = rt.branches().get(sess.branch).expect("live record");
            rec.mark_reaped();
            rt.branches().put(&rec).expect("put reaped record");
        }
        // Exactly what `reap_expired` hands `scan_once`, for the targeted arm to be given.
        let reaped: Vec<BranchId> = sessions.iter().take(g).map(|sess| sess.branch).collect();

        let stop = Arc::new(AtomicBool::new(false));
        let probe_rt = Arc::clone(&rt);
        let probe_stop = Arc::clone(&stop);
        let prober = std::thread::spawn(move || {
            let mut out: Vec<u64> = Vec::with_capacity(1 << 14);
            while !probe_stop.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                let _ = probe_rt.quarantine_reason(BranchId::TRUNK);
                out.push(t0.elapsed().as_nanos() as u64);
                std::thread::sleep(Duration::from_micros(50));
            }
            out
        });
        std::thread::sleep(Duration::from_millis(20));

        let t0 = Instant::now();
        // `recon` is the full reconciliation -- the backstop, O(open sessions). `fast` is
        // `forget_branches(&reaped)`, which is what `scan_once` calls on every successful tick
        // now that it stops re-deriving a list `reap_expired` already handed it.
        let dropped =
            if targeted { rt.forget_branches(&reaped) } else { rt.forget_reaped_branches() };
        walls.push(t0.elapsed().as_nanos() as u64);
        assert_eq!(dropped, g, "fixture reaped {g} branches, sweep forgot {dropped}");

        stop.store(true, Ordering::Relaxed);
        stalls.push(Samples::of(prober.join().unwrap()).max());
        drop(sessions);
    }
    (Samples::of(stalls), Samples::of(walls))
}

fn mean(s: &Samples) -> u64 {
    if s.0.is_empty() {
        0
    } else {
        (s.0.iter().map(|v| *v as u128).sum::<u128>() / s.0.len() as u128) as u64
    }
}

fn row(s: usize, name: &str, a: &Arm) {
    println!(
        "{:>8} {:>7} {:>9} {:>8} {:>10} {:>11} {:>11} {:>7} {:>11} {:>11}",
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
    println!(
        "{:>8} {:>7} {:>9} {:>8} {:>10} {:>11} {:>11} {:>7} {:>11} {:>11}",
        "S", "arm", "probe_n", "p50", "p999", "stall_mean", "probe_max", "op_n", "op_mean",
        "op_max"
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
    println!(
        "{:>8} {:>7} {:>6} {:>6} {:>12} {:>12} {:>12} {:>12}",
        "S", "gone", "reps", "arm", "stall_med", "stall_max", "wall_med", "wall_max"
    );
    for &s in &sweep {
        let g = gone.min(s);
        for (name, targeted) in [("recon", false), ("fast", true)] {
            let t0 = Instant::now();
            let (stalls, walls) = oneshot(s, g, reps, targeted);
            eprintln!(
                "# T2 S={s} g={g} {name}: {reps} fixtures in {} ms",
                t0.elapsed().as_millis()
            );
            println!(
                "{:>8} {:>7} {:>6} {:>6} {:>12} {:>12} {:>12} {:>12}",
                s,
                g,
                reps,
                name,
                stalls.pct(0.50),
                stalls.max(),
                walls.pct(0.50),
                walls.max(),
            );
        }
    }
}
