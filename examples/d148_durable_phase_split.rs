//! D148 — what share of `DurableEffectLog::append` is the frame scan D138 removes?
//!
//! `cargo run --release --example d148_durable_phase_split`
//!
//! D137's slope-1 law is `MemEffectLog`'s, because the pgwire front-end forces that store.
//! `src/cli/cli.rs:141` ships `DurableEffectLog`, which also encodes, `pwrite`s and **`sync_data`s
//! once per statement**. D115 put ≥97.6% of that append in encode+write+sync — but its
//! "comparisons" counter is `extends()` (ops within one frame), not the `position()` scan across
//! frames. So this measures the thing neither of them measured.
//!
//! ⚠ **WALL-CLOCK, on a shared box, and that is why the headline is a SHARE and a SLOPE rather
//! than a duration.** Every phase is timed inside one `append`, so they all carry the same load
//! and the ratio between them is stable where an absolute ms is not. Every timer is paired with a
//! counter that must be non-zero, and the phases are summed against an independently timed TOTAL
//! so the table has to close. The residual is printed, never absorbed.
//!
//! The workload mirrors D137's PARK axis exactly — sessions of 3 writes, one new key each, never
//! ended — so the two measurements describe the same shape on two stores.

use std::time::Instant;

use ferrodb::branch::types::{BranchId, CommitHash};
use ferrodb::catalog::column::Value;
use ferrodb::tel::ids::{ColId, RowId, TableId};
use ferrodb::tel::log::phase;
use ferrodb::tel::op::{Delta, Op, OpKind};
use ferrodb::tel::{DurableEffectLog, EffectLog, TxnFrame, TxnId};

const WRITES_PER_SESSION: usize = 3;

fn env_usize(k: &str, d: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

/// One session's frame after `n` statements — the shape `stage_all` re-appends: the same key,
/// growing by one op per statement, so statement 1 is a MISS and 2..n are HITs.
fn frame_after(session: usize, n: usize) -> TxnFrame {
    let mut f = TxnFrame::new(
        TxnId(session as u64 + 1),
        BranchId::new(session as u64 + 1, 0),
        CommitHash::ZERO,
        0,
        1,
    );
    for k in 0..n {
        f.push_op(
            Op::new(TableId(1), RowId(k as u64 + 1), Some(ColId(2)), OpKind::Add(Delta::Int(-1)))
                .with_witness(Value::Integer(20)),
        );
    }
    f
}

fn fmt_ns(v: Option<f64>) -> String {
    match v {
        Some(x) if x >= 1000.0 => format!("{:>9.1}us", x / 1000.0),
        Some(x) => format!("{x:>9.0}ns"),
        None => "        -  ".to_string(),
    }
}

/// ⛔ The clock must be shown to resolve what is asked of it, IN THIS BINARY, before any share
/// below is a reading.
///
/// The first cut of this check timed a 250-iteration arithmetic loop and got 42 ns against a 41 ns
/// tick — because the loop was optimised away, and because a single keyed lookup IS sub-tick here.
/// **It failed, and it was right to.** A sub-tick phase truncates toward zero, so it reads as
/// "free" rather than "unresolvable", which is the direction that flatters whoever is measuring.
/// The fix is not a better timer, it is BATCHING: nothing below times a single lookup.
fn clock_tick() -> u64 {
    let mut tick = u64::MAX;
    for _ in 0..500 {
        let a = Instant::now();
        loop {
            let d = a.elapsed().as_nanos() as u64;
            if d > 0 {
                tick = tick.min(d);
                break;
            }
        }
    }
    tick
}

fn main() {
    println!("D148 — phase split of `DurableEffectLog::append` (the store `src/cli/cli.rs:141` ships).");
    println!("{}", ferrodb::build_provenance());
    println!("⚠ WALL-CLOCK. The headline is the SHARE and the SLOPE, not any duration.");
    println!();

    let tick = clock_tick();
    println!("=== FIRE-CHECK 0 — the clock, in this binary ===");
    println!("    smallest non-zero Instant delta: {tick} ns");
    println!("    ⇒ NOTHING below times a single lookup. Per-append phases (encode/pwrite/sync/");
    println!("      TOTAL) are microseconds-to-milliseconds; the LOOKUP is measured by a batched");
    println!("      probe of thousands of repetitions, so the tick is amortised, not resolved.");
    println!();

    let blocks = env_usize("D148_BLOCKS", 8);
    let block = env_usize("D148_BLOCK", 250);
    let dir = std::env::temp_dir().join(format!("d148_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("agent.tel");
    let _ = std::fs::remove_file(&path);
    let log = DurableEffectLog::open(&path).expect("open durable effect log");

    println!("Axis: {blocks} blocks of {block} sessions, {WRITES_PER_SESSION} writes each — the PARK shape.");
    println!(
        "    {:>8} {:>8} {:>11} {:>11} {:>11} {:>12} {:>12} {:>9}",
        "frames", "appends", "encode", "pwrite", "sync_data", "idx/lookup", "scan/lookup", "scan%"
    );

    let mut rows: Vec<(f64, f64, f64)> = Vec::new();      // (frames, SCAN ns/append, io ns/append)
    let mut idx_rows: Vec<(f64, f64, f64)> = Vec::new();  // (frames, INDEX ns/append, total ns/append)
    let mut total_appends = 0u64;
    for b in 0..blocks {
        let before = phase::snapshot();
        let frames_before = log.len() as f64;
        for s in 0..block {
            let session = b * block + s;
            for n in 1..=WRITES_PER_SESSION {
                log.append(&frame_after(session, n)).expect("durable append");
            }
        }
        let d = phase::snapshot().since(&before);
        let appends = d.p[phase::TOTAL].calls;
        total_appends += appends;
        if appends == 0 {
            println!("⛔ a block made ZERO appends. Not a result.");
            std::process::exit(1);
        }
        let per = |i: usize| d.p[i].nanos as f64 / appends as f64;
        let io = per(phase::ENCODE) + per(phase::PWRITE) + per(phase::SYNC);
        let total = per(phase::TOTAL);

        // ⭐ THE LOOKUP, BATCHED — both shapes over the same Vec, under one lock, one moment.
        //
        // ⛔ THE KEY MATTERS MORE THAN THE TIMER. `position()` scans from the FRONT and
        // short-circuits, so probing session 0's key — which sits at position 0 — measures a
        // ONE-element scan and reports a 2000-frame log as 1 ns. That is what the first cut of
        // this harness did. D137 established the real access pattern: the frame for a
        // recently-created txn sits at the BACK, so a hit costs about the whole Vec. This probes
        // the MOST RECENT session's key, and asserts from `examined_per_rep` that the scan really
        // walked the log rather than trusting that it did.
        let reps = env_usize("D148_REPS", 5_000) as u64;
        let newest = (b * block + block - 1) as u64;
        let pr = log.probe_lookup_batch(BranchId::new(newest + 1, 0), TxnId(newest + 1), reps);
        std::hint::black_box(pr.sink);
        let want = pr.frames as f64 * 0.5;
        if pr.examined_per_rep < want {
            println!(
                "⛔ the scan control walked {:.0} elements of {} — it short-circuited instead of \
                 scanning. Not a result.",
                pr.examined_per_rep, pr.frames
            );
            std::process::exit(1);
        }
        if pr.index_ns == 0 || pr.scan_ns == 0 {
            println!("⛔ a batched probe of {reps} reps measured ZERO ns. Not a result.");
            std::process::exit(1);
        }
        let idx_each = pr.index_ns as f64 / reps as f64;
        let scan_each = pr.scan_ns as f64 / reps as f64;
        let frames_now = pr.frames;
        println!(
            "    {:>8} {:>8} {} {} {} {} {} {:>8.3}%",
            frames_now,
            appends,
            fmt_ns(d.p[phase::ENCODE].per_call()),
            fmt_ns(d.p[phase::PWRITE].per_call()),
            fmt_ns(d.p[phase::SYNC].per_call()),
            fmt_ns(Some(idx_each)),
            fmt_ns(Some(scan_each)),
            // Two keyed lookups per durable append: classify_append, then mem.append.
            100.0 * (2.0 * scan_each) / total.max(1e-9),
        );
        println!(
            "             (scan control walked {:.0} of {} frames — verified, not assumed)",
            pr.examined_per_rep, pr.frames
        );
        rows.push((frames_now as f64, 2.0 * scan_each, io));
        idx_rows.push((frames_now as f64, 2.0 * idx_each, total));
    }

    // ── every timer paired with a counter that must be non-zero ───────────────────────────────
    let all = phase::snapshot();
    println!();
    println!("=== THE TABLE MUST CLOSE — every phase, its counter, and the residual ===");
    println!("    units are: lookup* = LOG LENGTH the lookup was over (NOT frames walked — the");
    println!("    indexed arm walks none, which is the point); encode/pwrite = bytes; sync = fsyncs.");
    for i in 0..phase::PHASES {
        println!(
            "    {:<18} calls {:>7}  units {:>12}  total {:>10.3} ms",
            phase::NAMES[i],
            all.p[i].calls,
            all.p[i].units,
            all.p[i].nanos as f64 / 1e6
        );
    }
    let resid = all.residual_nanos();
    let resid_pct = 100.0 * resid as f64 / all.p[phase::TOTAL].nanos.max(1) as f64;
    println!("    residual (TOTAL - parts): {:.3} ms = {resid_pct:+.2}%", resid as f64 / 1e6);
    let mut ok = true;
    for i in 0..phase::PHASES {
        if all.p[i].calls == 0 {
            println!("⛔ phase {} never ran — a zero timer beside a zero counter is no result.", phase::NAMES[i]);
            ok = false;
        }
    }
    if all.p[phase::SYNC].units as u64 != all.p[phase::TOTAL].calls {
        println!(
            "⛔ fsyncs {} != appends {} — this store is supposed to sync once per append.",
            all.p[phase::SYNC].units,
            all.p[phase::TOTAL].calls
        );
        ok = false;
    }
    if resid_pct.abs() > 15.0 {
        println!("⛔ residual {resid_pct:+.2}% — the phases do not account for the append; the split is wrong.");
        ok = false;
    }

    // ── the answer D148 asks for ──────────────────────────────────────────────────────────────
    println!();
    println!("=== D148's QUESTION: the frame scan's share of a DURABLE append ===");
    println!("    Two keyed lookups per durable append: classify_append, then mem.append.");
    let first = rows.first().copied().unwrap_or_default();
    let last = rows.last().copied().unwrap_or_default();
    let ifirst = idx_rows.first().copied().unwrap_or_default();
    let ilast = idx_rows.last().copied().unwrap_or_default();
    println!(
        "    at {:>6} frames  SCAN {:>10.0} ns/append   encode+write+sync {:>10.0} ns   => scan is {:>6.3}% of append",
        first.0, first.1, first.2, 100.0 * first.1 / ilast.2.max(1e-9)
    );
    println!(
        "    at {:>6} frames  SCAN {:>10.0} ns/append   encode+write+sync {:>10.0} ns   => scan is {:>6.3}% of append",
        last.0, last.1, last.2, 100.0 * last.1 / ilast.2.max(1e-9)
    );
    println!(
        "    INDEX, same axis: {:>8.0} ns/append at {:>6} frames -> {:>8.0} ns/append at {:>6} frames (flat = O(1))",
        ifirst.1, ifirst.0, ilast.1, ilast.0
    );
    println!();
    if rows.len() >= 3 {
        // ⛔ LEAST SQUARES over every row, not an endpoint fit. The box is shared, one row came
        // back visibly noisy, and an endpoint fit hands that single row the whole slope. Both
        // fits are printed so the disagreement between them IS the error bar.
        let n = rows.len() as f64;
        let mx = rows.iter().map(|r| r.0).sum::<f64>() / n;
        let my = rows.iter().map(|r| r.1).sum::<f64>() / n;
        let sxy: f64 = rows.iter().map(|r| (r.0 - mx) * (r.1 - my)).sum();
        let sxx: f64 = rows.iter().map(|r| (r.0 - mx) * (r.0 - mx)).sum();
        let a_ls = sxy / sxx;
        let b_ls = my - a_ls * mx;
        let a_ep = (last.1 - first.1) / (last.0 - first.0);
        let io_mean = rows.iter().map(|r| r.2).sum::<f64>() / rows.len() as f64;
        let total_mean = idx_rows.iter().map(|r| r.2).sum::<f64>() / idx_rows.len() as f64;
        println!("    SCAN slope, least squares over all {} rows: {a_ls:.4} ns per frame per append", rows.len());
        println!("    SCAN slope, endpoints only:                 {a_ep:.4} ns per frame per append");
        println!("    encode+pwrite+sync_data mean: {io_mean:.0} ns/append — CONSTANT in log length");
        println!("    whole append mean (indexed arm): {total_mean:.0} ns");
        println!();
        println!("    ⭐ EXTRAPOLATED to the objective's own axis (a fit over 250..2000 frames,");
        println!("       stated as an extrapolation, NOT a measurement at that size):");
        for (label, a) in [("least squares", a_ls), ("endpoints", a_ep)] {
            let at_1e6 = a * 1e6 + b_ls;
            println!(
                "       {label:>14}: scan at 10^6 frames = {:.2} ms/append vs {:.2} ms of I/O  => {:.1}% of the append",
                at_1e6 / 1e6,
                io_mean / 1e6,
                100.0 * at_1e6 / (at_1e6 + io_mean)
            );
        }
        if a_ls > 1e-12 {
            let cross = (io_mean - b_ls) / a_ls;
            println!("    ⇒ the scan EQUALS encode+pwrite+sync at ~{cross:.0} frames (least-squares fit).");
        }
        println!("    ⚠ At 10^6 the frame Vec is ~100 MB, so per-element cost would RISE with cache");
        println!("      misses. The extrapolation is therefore conservative, not optimistic.");
    }
    println!();
    println!("    appends measured: {total_appends}, final log length {} frames", log.len());
    let _ = std::fs::remove_dir_all(&dir);
    if !ok {
        std::process::exit(1);
    }
}
