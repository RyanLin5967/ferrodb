//! ⛔ D123 MEASUREMENT SCAFFOLD — runs only against branch `D123-serial-attribution`.
//!
//! Two questions, one binary. See `bench/d123_PREREGISTRATION.md` for the falsifiers.
//!
//!   Q1  where does fork's 0.228 ms effective serial interval go at 64 threads?
//!   Q2  is that interval the ceiling at all?
//!
//! `bench/serial_section_profile.txt` priced the same operations **one at a time, uncontended**,
//! and summed to 0.0748 ms. Subtracting that from 0.228 ms leaves 0.1532 ms that has never been
//! observed — it is the gap between two regimes, not a measurement. This harness measures the
//! phases IN PLACE, under the contention that is supposed to be producing the gap.
//!
//!   cargo run --release --example d123_serial_attribution -- <mode> <N> <threads,...>
//!
//! Modes: `phases` (F2/F4) · `stub` (F1) · `extra` (F5) · `threads` (F6) · `perturb` (instrument).
use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::d123_probe as probe;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;

/// One arm's outcome. Durations are upper bounds — `load_at_acquire` is stamped by the caller.
struct Arm {
    threads: usize,
    forks: usize,
    secs: f64,
    syncs: u64,
    totals: probe::Totals,
}

impl Arm {
    fn per_fork_ms(&self) -> f64 {
        self.secs * 1000.0 / self.forks as f64
    }
    fn throughput(&self) -> f64 {
        self.forks as f64 / self.secs
    }
    /// Mean nanoseconds per fork in one phase, summed over every thread.
    fn ms(&self, phase: usize) -> f64 {
        if self.totals.forks == 0 {
            return f64::NAN;
        }
        self.totals.ns[phase] as f64 / self.totals.forks as f64 / 1e6
    }
}

fn open_catalog(dir: &std::path::Path, tag: &str) -> Arc<TableBranchCatalog> {
    // Two pools, matching `fork_concurrency.rs` exactly so the numbers are comparable to
    // `bench/fork_concurrency_after.txt`. Any divergence here silently makes this a different
    // experiment measured against that file's numbers.
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

/// Run one arm. `warm` forks first so the tree has real depth — a profile taken on an empty tree
/// measures the best case of every descent.
fn run_arm(dir: &std::path::Path, tag: &str, n: usize, t: usize, warm: usize) -> Arm {
    let cat = open_catalog(dir, tag);
    let lease = LeaseDeadline(u64::MAX);

    // Warm OUTSIDE the probe window and with the stub off, so every arm starts from the same tree
    // shape regardless of what the timed loop is about to skip. A tree with real depth matters:
    // a profile taken on an empty tree measures the best case of every descent.
    //
    // CONCURRENTLY, and that is not a shortcut. Serially this is 20k forks at ~270/sec = 74 s of
    // warm-up per arm, and there are 20-odd arms. The tree that results holds the same 20k
    // branches either way; only the interleaving of ids differs, and no phase below is keyed on id
    // order. What it must NOT do is run under the probe or the stub, hence the save/restore.
    const WARM_THREADS: usize = 32;
    let saved = (probe::enabled(), probe::stub_level(), probe::extra_upserts());
    probe::configure(false, 0, 0);
    let per_warm = warm / WARM_THREADS;
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
    probe::configure(saved.0, saved.1, saved.2);
    probe::reset();
    // Discard the warm-up's own accumulation on this thread.
    probe::flush_thread();
    probe::reset();

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
                // ⛔ Without this the thread's private accumulators are dropped and every phase
                // number silently shrinks. `assert_threads` below is the anti-vacuity check.
                probe::flush_thread();
            });
        }
    });
    let secs = t0.elapsed().as_secs_f64();
    let totals = probe::snapshot();

    if probe::enabled() {
        assert_eq!(
            totals.threads as usize, t,
            "probe lost a thread's accumulators: {} of {} flushed",
            totals.threads, t
        );
        assert_eq!(
            totals.forks as usize, total,
            "probe counted {} forks, harness ran {}",
            totals.forks, total
        );
    }

    Arm { threads: t, forks: total, secs, syncs: cat.syncs_issued() - syncs_before, totals }
}

fn banner(mode: &str) {
    println!("D123 serial-section attribution — {mode}");
    println!("build {}", ferrodb::build_provenance());
    println!(
        "load_at_acquire is stamped by the caller; EVERY duration below is an UPPER BOUND on a box"
    );
    println!("measured at a 46x quiet-vs-loaded spread. Ratios of two durations from THIS process");
    println!("are the trustworthy quantities; absolute milliseconds are not.");
    println!();
}

/// F2 + F4. The whole point: `HOLD` and `WAIT` are measured, so `gap` is observed rather than
/// inferred from a subtraction across two regimes.
fn mode_phases(dir: &std::path::Path, n: usize, threads: &[usize], warm: usize) {
    banner("MODE=phases (F2: does the residual grow with T? F4: hold vs handoff)");
    probe::configure(true, 0, 0);
    let arms: Vec<Arm> =
        threads.iter().map(|&t| run_arm(dir, &format!("ph{t}"), n, t, warm)).collect();

    println!("PER-PHASE, ms per fork, measured IN PLACE under contention.");
    println!("Phases core..extra are a disjoint partition of HOLD. WAIT and DURABLE are OUTSIDE it.");
    print!("  {:<24}", "phase");
    for a in &arms {
        print!("{:>12}", format!("T={}", a.threads));
    }
    println!("{:>12}", "64/1");
    for p in 0..probe::NPHASE {
        print!("  {:<24}", probe::PHASE_NAMES[p]);
        for a in &arms {
            print!("{:>12.5}", a.ms(p));
        }
        let first = arms.first().map(|a| a.ms(p)).unwrap_or(f64::NAN);
        let last = arms.last().map(|a| a.ms(p)).unwrap_or(f64::NAN);
        if first > 0.0 {
            println!("{:>12.2}", last / first);
        } else {
            println!("{:>12}", "-");
        }
    }
    println!();
    println!("THE DECOMPOSITION F4 PRE-REGISTERED. Only one thread can hold `logical`, so");
    println!("S_eff = hold + gap EXACTLY, with no third term and nothing unaccounted.");
    println!(
        "  {:>7} {:>11} {:>11} {:>10} {:>10} {:>9} {:>9} {:>8}",
        "threads", "forks/sec", "S_eff ms", "HOLD ms", "gap ms", "U(hold)", "sum_ph", "resid"
    );
    for a in &arms {
        let hold = a.ms(probe::PH_HOLD);
        let s_eff = a.per_fork_ms();
        let sum_ph: f64 = (probe::PH_CORE..=probe::PH_EXTRA).map(|p| a.ms(p)).sum();
        println!(
            "  {:>7} {:>11.1} {:>11.5} {:>10.5} {:>10.5} {:>8.1}% {:>9.5} {:>8.5}",
            a.threads,
            a.throughput(),
            s_eff,
            hold,
            s_eff - hold,
            100.0 * hold / s_eff,
            sum_ph,
            hold - sum_ph
        );
    }
    println!();
    println!("  U near 33% with hold ~0.075 => H-HANDOFF. U near 100% with hold ~0.228 =>");
    println!("  H-INFLATION, and then `64/1` above names WHICH phase inflated.");
}

/// F1. The decisive one.
fn mode_stub(dir: &std::path::Path, n: usize, threads: &[usize], warm: usize) {
    banner("MODE=stub (F1: drive the serial section toward zero)");
    println!("⛔ Stubbed levels are NOT a database. L1 writes no child record; L2 leaks retired");
    println!("   slots; L3 also drops the envelope (capability escape) and the parent's live-child");
    println!("   entry (GC hole). Every level KEEPS write_header + stage so ≥1 page is dirty and");
    println!("   the fsync stays real — watch `syncs` for the check that this held.");
    println!();
    println!(
        "  {:>7} {:>5} {:>11} {:>11} {:>10} {:>10} {:>8} {:>8} {:>9}",
        "threads", "stub", "forks/sec", "vs L0", "S_eff ms", "HOLD ms", "syncs", "f/sync", "U(hold)"
    );
    for &t in threads {
        let mut base = 0.0;
        for stub in 0u8..=3 {
            probe::configure(true, stub, 0);
            let a = run_arm(dir, &format!("st{t}_{stub}"), n, t, warm);
            if stub == 0 {
                base = a.throughput();
            }
            let hold = a.ms(probe::PH_HOLD);
            println!(
                "  {:>7} {:>5} {:>11.1} {:>10.2}x {:>10.5} {:>10.5} {:>8} {:>8.1} {:>8.1}%",
                t,
                stub,
                a.throughput(),
                a.throughput() / base,
                a.per_fork_ms(),
                hold,
                a.syncs,
                a.forks as f64 / a.syncs.max(1) as f64,
                100.0 * hold / a.per_fork_ms(),
            );
        }
        println!();
    }
    println!("F1 FIRES if L3 is within +/-10% of L0: the section is NOT the ceiling.");
    println!("⛔ ANY ROW WHOSE `syncs` COLLAPSED IS VOID — it lost durability, not serial work.");
}

/// F5. Additive, no stub, no correctness compromise — valid even if every stub level is void.
fn mode_extra(dir: &std::path::Path, n: usize, threads: &[usize], warm: usize) {
    banner("MODE=extra (F5: marginal cost of work added under `logical`)");
    println!("k extra upserts under the lock, on a fixed key set. An upsert costs u = 0.01344 ms");
    println!("UNCONTENDED (bench/serial_section_profile.txt). Fit 1/throughput against k:");
    println!("  slope ~ u      => added work costs what it costs alone; the residual is a FIXED");
    println!("                    per-fork overhead (handoff), not an inflation of the work.");
    println!("  slope ~ 3.05u  => work under the lock is 3.05x dearer under contention, which is");
    println!("                    exactly 0.228/0.0748 — and that ATTRIBUTES the residual.");
    println!("  slope ~ 0      => M-FSYNC, and F1 fires.");
    println!();
    for &t in threads {
        println!(
            "  {:>7} {:>4} {:>11} {:>11} {:>10} {:>10} {:>8}",
            "threads", "k", "forks/sec", "S_eff ms", "HOLD ms", "extra ms", "f/sync"
        );
        let ks = [0u64, 2, 4, 8, 16];
        let mut pts: Vec<(f64, f64)> = Vec::new();
        for k in ks {
            probe::configure(true, 0, k);
            let a = run_arm(dir, &format!("ex{t}_{k}"), n, t, warm);
            pts.push((k as f64, a.per_fork_ms()));
            println!(
                "  {:>7} {:>4} {:>11.1} {:>11.5} {:>10.5} {:>10.5} {:>8.1}",
                t,
                k,
                a.throughput(),
                a.per_fork_ms(),
                a.ms(probe::PH_HOLD),
                a.ms(probe::PH_EXTRA),
                a.forks as f64 / a.syncs.max(1) as f64
            );
        }
        // Least squares on 5 points. Reported with its residuals so a bad fit cannot pass as good.
        let n_p = pts.len() as f64;
        let sx: f64 = pts.iter().map(|p| p.0).sum();
        let sy: f64 = pts.iter().map(|p| p.1).sum();
        let sxx: f64 = pts.iter().map(|p| p.0 * p.0).sum();
        let sxy: f64 = pts.iter().map(|p| p.0 * p.1).sum();
        let slope = (n_p * sxy - sx * sy) / (n_p * sxx - sx * sx);
        let icept = (sy - slope * sx) / n_p;
        let ss_res: f64 =
            pts.iter().map(|p| (p.1 - (icept + slope * p.0)).powi(2)).sum();
        let mean = sy / n_p;
        let ss_tot: f64 = pts.iter().map(|p| (p.1 - mean).powi(2)).sum();
        println!();
        println!(
            "  FIT T={t}:  1/throughput = {icept:.5} + {slope:.5}*k   R^2={:.4}",
            1.0 - ss_res / ss_tot
        );
        println!(
            "     slope/u = {:.2}x  (u = 0.01344 ms uncontended).  intercept is an INDEPENDENT",
            slope / 0.01344
        );
        println!("     estimate of S that never touches the 1-thread profiler.");
        println!();
    }
}

/// F6. The sharpest single discriminator between M-SERIAL and M-FSYNC.
fn mode_threads(dir: &std::path::Path, n: usize, threads: &[usize], warm: usize) {
    banner("MODE=threads (F6: M-FSYNC requires throughput proportional to T)");
    probe::configure(true, 0, 0);
    println!(
        "  {:>7} {:>11} {:>11} {:>10} {:>10} {:>8} {:>8} {:>13} {:>10}",
        "threads", "forks/sec", "S_eff ms", "HOLD ms", "gap ms", "syncs", "f/sync", "M-FSYNC bound", "measured/"
    );
    for &t in threads {
        let a = run_arm(dir, &format!("th{t}"), n, t, warm);
        // The adversary's bound, evaluated at each T: at most T forks durable per fsync round.
        // Round duration is taken from THIS arm (wall/syncs), not from the 3.574 ms residual, so
        // the bound is not inherited from another regime.
        let round_ms = a.secs * 1000.0 / a.syncs.max(1) as f64;
        let bound = t as f64 / (round_ms / 1000.0);
        println!(
            "  {:>7} {:>11.1} {:>11.5} {:>10.5} {:>10.5} {:>8} {:>8.1} {:>13.0} {:>9.2}x",
            t,
            a.throughput(),
            a.per_fork_ms(),
            a.ms(probe::PH_HOLD),
            a.per_fork_ms() - a.ms(probe::PH_HOLD),
            a.syncs,
            a.forks as f64 / a.syncs.max(1) as f64,
            bound,
            a.throughput() / bound
        );
    }
    println!();
    println!("  `measured/bound` near 1.00 => the harness really is sitting on the fsync bound.");
    println!("  Well below 1.00 => it is not, and the bound cannot be what pins the plateau.");
}

/// The instrument measuring itself. Pre-registered: >2% and every duration is quarantined.
fn mode_perturb(dir: &std::path::Path, n: usize, threads: &[usize], warm: usize) {
    banner("MODE=perturb (does the probe itself change the number it reports?)");
    println!("  {:>7} {:>13} {:>13} {:>9}", "threads", "probe OFF/sec", "probe ON/sec", "delta");
    for &t in threads {
        probe::configure(false, 0, 0);
        let off = run_arm(dir, &format!("pf{t}"), n, t, warm);
        probe::configure(true, 0, 0);
        let on = run_arm(dir, &format!("pn{t}"), n, t, warm);
        println!(
            "  {:>7} {:>13.1} {:>13.1} {:>8.2}%",
            t,
            off.throughput(),
            on.throughput(),
            100.0 * (on.throughput() - off.throughput()) / off.throughput()
        );
    }
    println!();
    println!("  |delta| > 2% => the probe perturbs and every duration it produced is quarantined.");
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "phases".into());
    let n: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(4000);
    let threads: Vec<usize> = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "1,8,64".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let warm: usize =
        std::env::var("FERRODB_D123_WARM").ok().and_then(|s| s.parse().ok()).unwrap_or(20_000);

    let dir = std::env::temp_dir().join(format!("ferrodb-d123-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    match mode.as_str() {
        "phases" => mode_phases(&dir, n, &threads, warm),
        "stub" => mode_stub(&dir, n, &threads, warm),
        "extra" => mode_extra(&dir, n, &threads, warm),
        "threads" => mode_threads(&dir, n, &threads, warm),
        "perturb" => mode_perturb(&dir, n, &threads, warm),
        other => {
            eprintln!("unknown mode {other}; expected phases|stub|extra|threads|perturb");
            std::process::exit(2);
        }
    }
    println!();
    println!("warm-up forks per arm = {warm}, N per arm = {n}");
    let _ = std::fs::remove_dir_all(&dir);
}
