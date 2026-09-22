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

/// ⛔ The clock must be shown to resolve what is being asked of it, IN THIS BINARY, before any
/// share below is a reading. A sub-tick phase repeats one quantum and looks like a perfect
/// constant; that failure has happened on this project.
fn clock_fire_check() -> bool {
    let mut tick = u64::MAX;
    for _ in 0..200 {
        let a = Instant::now();
        loop {
            let d = a.elapsed().as_nanos() as u64;
            if d > 0 {
                tick = tick.min(d);
                break;
            }
        }
    }
    // A phase the size of the scan at the SMALLEST log length this run reaches.
    let probe = Instant::now();
    let mut acc = 0u64;
    for i in 0..250u64 {
        acc = acc.wrapping_add(i * 7);
    }
    let walked = probe.elapsed().as_nanos() as u64;
    std::hint::black_box(acc);
    println!("=== FIRE-CHECK 0 — the clock, in this binary ===");
    println!("    smallest non-zero Instant delta observed: {tick} ns");
    println!("    a 250-iteration loop measures {walked} ns  ({:.1}x the tick)", walked as f64 / tick.max(1) as f64);
    let ok = tick > 0 && walked > tick * 3;
    println!(
        "    => {} : the scan-sized phase is resolvable, so a small share is a reading and not a \
         rounding artifact",
        if ok { "PASS" } else { "FAIL" }
    );
    println!();
    ok
}

fn main() {
    println!("D148 — phase split of `DurableEffectLog::append` (the store `src/cli/cli.rs:141` ships).");
    println!("{}", ferrodb::build_provenance());
    println!("⚠ WALL-CLOCK. The headline is the SHARE and the SLOPE, not any duration.");
    println!();

    if !clock_fire_check() {
        println!("⛔ the clock cannot resolve a scan-sized phase. No share is reported.");
        std::process::exit(1);
    }

    let blocks = env_usize("D148_BLOCKS", 8);
    let block = env_usize("D148_BLOCK", 250);
    let dir = std::env::temp_dir().join(format!("d148_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("agent.tel");
    let _ = std::fs::remove_file(&path);
    let log = DurableEffectLog::open(&path).expect("open durable effect log");

    println!("Axis: {blocks} blocks of {block} sessions, {WRITES_PER_SESSION} writes each — the PARK shape.");
    println!(
        "    {:>9} {:>9} {:>12} {:>12} {:>12} {:>12} {:>9} {:>9}",
        "frames", "appends", "lookup", "encode", "pwrite", "sync_data", "lookup%", "resid%"
    );

    let mut rows: Vec<(f64, f64, f64)> = Vec::new(); // (frames_before, lookup_ns/append, io_ns/append)
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
        let lookup = per(phase::LOOKUP_CLASSIFY) + per(phase::LOOKUP_APPEND);
        let io = per(phase::ENCODE) + per(phase::PWRITE) + per(phase::SYNC);
        let total = per(phase::TOTAL);
        println!(
            "    {:>9} {:>9} {} {} {} {} {:>8.2}% {:>8.2}%",
            frames_before as u64,
            appends,
            fmt_ns(Some(lookup)),
            fmt_ns(d.p[phase::ENCODE].per_call()),
            fmt_ns(d.p[phase::PWRITE].per_call()),
            fmt_ns(d.p[phase::SYNC].per_call()),
            100.0 * lookup / total.max(1e-9),
            100.0 * (total - lookup - io) / total.max(1e-9),
        );
        rows.push((frames_before, lookup, io));
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
    let first = rows.first().copied().unwrap_or_default();
    let last = rows.last().copied().unwrap_or_default();
    let share = |r: (f64, f64, f64)| 100.0 * r.1 / (r.1 + r.2).max(1e-9);
    println!(
        "    at {:>7} frames: lookup {:>9.0} ns/append vs encode+write+sync {:>9.0} ns  => {:>6.3}%",
        first.0, first.1, first.2, share(first)
    );
    println!(
        "    at {:>7} frames: lookup {:>9.0} ns/append vs encode+write+sync {:>9.0} ns  => {:>6.3}%",
        last.0, last.1, last.2, share(last)
    );
    // Slope of the lookup against log length, and where it would meet the constant I/O term.
    if rows.len() >= 2 && (last.0 - first.0).abs() > 0.0 {
        let a = (last.1 - first.1) / (last.0 - first.0);
        let b = first.1 - a * first.0;
        let io_mean = rows.iter().map(|r| r.2).sum::<f64>() / rows.len() as f64;
        println!("    lookup fit: {a:.4} ns per frame in the log, intercept {b:.0} ns");
        println!("    encode+write+sync mean: {io_mean:.0} ns/append, and it does NOT grow with log length");
        if a > 1e-9 {
            let cross = (io_mean - b) / a;
            println!(
                "    ⇒ CROSSOVER at ~{cross:.0} frames — below it the scan is swamped by the fsync, \
                 above it the scan dominates."
            );
        } else {
            println!("    ⇒ the lookup does not grow with log length (this is the INDEXED arm).");
        }
    }
    println!();
    println!("    appends measured: {total_appends}, final log length {} frames", log.len());
    let _ = std::fs::remove_dir_all(&dir);
    if !ok {
        std::process::exit(1);
    }
}
