//! D51 — is a shared relaxed LOAD actually cheaper than a shared RMW, at thread count?
//!
//! `SCALE-DESIGN` D51 chose option C (a per-connection cached schema validated against a global
//! version counter read with a **relaxed load**) over option A (`RwLock<Catalog>`, readers take
//! `.read()`) and option B (RCU, readers `Arc::clone` a published catalog). The whole decision
//! rests on ONE claim, and the entry says so in its own text:
//!
//! > *"A shared LOAD does not bounce a cache line — only an RMW does."*
//!
//! ⚠ That claim was **assumed**, not measured. D49 measured the RMW side (turso's per-statement
//! shared read-mark lock: ×0.308 total at 16 threads, readers doing less total work than one) —
//! the load side had no number at all. Building option C on an unmeasured premise is exactly the
//! move that D48 punished: a careful structural reading predicted turso's hit path would not
//! degrade, and the run said otherwise.
//!
//! So this measures the premise **before** anything is built on it. Three arms, identical except
//! for the one operation, all on a word every thread shares:
//!
//!   LOAD   — `v.load(Ordering::Relaxed)` on a shared `AtomicU64`. Option C's per-statement cost.
//!   ARC    — `Arc::clone` then drop. Option B's per-statement cost: an RMW on one refcount.
//!   RWLOCK — `lock.read()` then drop. Option A's per-statement cost.
//!
//! PREDICTION, recorded before the run so it can fail: LOAD scales ~linearly with threads while
//! ARC and RWLOCK go flat or worse. If LOAD does NOT scale, option C is dead and D51's decision
//! has to be reopened — which is a result, not a setback.
//!
//! Order is rotated per round (Latin square) AND the arm order is rotated too, because D50 found
//! a 2.2× bias sitting between arms that point-rotation cannot cancel. Guards refuse: any arm
//! whose slowest thread did zero work, or whose observed value is wrong, exits non-zero.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, RwLock};
use std::time::{Duration, Instant};

const MEASURE: Duration = Duration::from_millis(500);
const WARMUP: Duration = Duration::from_millis(150);
const ROUNDS: usize = 3;
const POINTS: [usize; 5] = [1, 2, 4, 8, 16];
const SENTINEL: u64 = 0x5EED_1234_ABCD_0001;

#[derive(Clone, Copy, PartialEq)]
enum Arm {
    Load,
    ArcClone,
    RwRead,
}

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Load => "LOAD   (relaxed atomic load -- option C)",
            Arm::ArcClone => "ARC    (Arc::clone, an RMW on a refcount -- option B)",
            Arm::RwRead => "RWLOCK (RwLock::read -- option A, the standard answer)",
        }
    }
}

struct Shared {
    counter: AtomicU64,
    arc: Arc<u64>,
    lock: RwLock<u64>,
}

fn sweep_point(s: &Arc<Shared>, arm: Arm, threads: usize) -> (f64, u64) {
    let start = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let slowest = Arc::new(AtomicU64::new(u64::MAX));
    let wrong = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..threads {
        let (s, start, stop) = (s.clone(), start.clone(), stop.clone());
        let (total, slowest, wrong) = (total.clone(), slowest.clone(), wrong.clone());
        handles.push(std::thread::spawn(move || {
            let mut bad = 0u64;
            let mut one = |s: &Shared| match arm {
                // black_box so the load cannot be hoisted out of the loop -- an optimised-away
                // arm would report an enormous number and read as a win.
                Arm::Load => std::hint::black_box(s.counter.load(Ordering::Relaxed)),
                Arm::ArcClone => *std::hint::black_box(Arc::clone(&s.arc)),
                Arm::RwRead => *std::hint::black_box(&*s.lock.read().unwrap()),
            };
            let t0 = Instant::now();
            while t0.elapsed() < WARMUP {
                if one(&s) != SENTINEL {
                    bad += 1;
                }
            }
            start.wait();
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if one(&s) != SENTINEL {
                    bad += 1;
                }
                n += 1;
            }
            total.fetch_add(n, Ordering::Relaxed);
            slowest.fetch_min(n, Ordering::Relaxed);
            wrong.fetch_add(bad, Ordering::Relaxed);
        }));
    }

    start.wait();
    let t0 = Instant::now();
    std::thread::sleep(MEASURE);
    stop.store(true, Ordering::Relaxed);
    let elapsed = t0.elapsed();
    for h in handles {
        h.join().unwrap();
    }

    let n = total.load(Ordering::Relaxed);
    let slow = slowest.load(Ordering::Relaxed);
    let bad = wrong.load(Ordering::Relaxed);
    if n == 0 || slow == 0 {
        eprintln!("GUARD: {threads} threads produced {n} ops (slowest thread {slow})");
        std::process::exit(2);
    }
    if bad != 0 {
        eprintln!("GUARD: {bad} reads saw a value other than the sentinel; this is not the probe");
        std::process::exit(2);
    }
    (n as f64 / elapsed.as_secs_f64(), slow)
}

fn run_arm(s: &Arc<Shared>, arm: Arm) -> Vec<f64> {
    println!();
    println!("=== ARM {} ===", arm.name());
    let mut samples: Vec<Vec<f64>> = vec![Vec::new(); POINTS.len()];
    for r in 0..ROUNDS {
        let order: Vec<usize> = (0..POINTS.len()).map(|i| (i + r) % POINTS.len()).collect();
        let shown: Vec<String> = order.iter().map(|i| POINTS[*i].to_string()).collect();
        println!("# round {r} order: {}", shown.join(" "));
        for &i in &order {
            let (ops, slow) = sweep_point(s, arm, POINTS[i]);
            samples[i].push(ops);
            println!("#   {}T -> {:.0} ops/s (slowest {slow})", POINTS[i], ops);
        }
    }
    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let meds: Vec<f64> = (0..POINTS.len()).map(|i| med(&mut samples[i])).collect();
    println!();
    println!("threads      total_ops_s   per_thread   total_vs_1T");
    for (i, &t) in POINTS.iter().enumerate() {
        println!(
            "{t:>7}   {:>14.0}   {:>10.0}   {:>11.3}",
            meds[i],
            meds[i] / t as f64,
            meds[i] / meds[0]
        );
    }
    meds
}

fn main() {
    let s = Arc::new(Shared {
        counter: AtomicU64::new(SENTINEL),
        arc: Arc::new(SENTINEL),
        lock: RwLock::new(SENTINEL),
    });
    // Arm order rotates too: D50 found a 2.2x bias BETWEEN arms that point-rotation cannot cancel.
    let order: Vec<Arm> = if std::env::var("D51_ARM_ORDER").map(|v| v == "REV").unwrap_or(false) {
        vec![Arm::RwRead, Arm::ArcClone, Arm::Load]
    } else {
        vec![Arm::Load, Arm::ArcClone, Arm::RwRead]
    };
    println!("# D51 shared-word probe. measure={MEASURE:?} warmup={WARMUP:?} rounds={ROUNDS}");
    println!("# PREDICTION (recorded before the run): LOAD scales ~linearly; ARC and RWLOCK do not.");
    println!("# If LOAD does not scale, SCALE-DESIGN D51 option C is dead and the decision reopens.");

    let mut out: Vec<(Arm, Vec<f64>)> = Vec::new();
    for arm in &order {
        out.push((*arm, run_arm(&s, *arm)));
    }

    println!();
    println!("threads   {:>12}   {:>12}   {:>12}   (total_vs_1T)", "LOAD", "ARC", "RWLOCK");
    let pick = |a: Arm| out.iter().find(|(x, _)| *x == a).map(|(_, v)| v.clone()).unwrap();
    let (l, ar, rw) = (pick(Arm::Load), pick(Arm::ArcClone), pick(Arm::RwRead));
    for (i, &t) in POINTS.iter().enumerate() {
        println!(
            "{t:>7}   {:>12.3}   {:>12.3}   {:>12.3}",
            l[i] / l[0],
            ar[i] / ar[0],
            rw[i] / rw[0]
        );
    }
}
