//! S20 / D28 — what a SINGLE unrepped sample of `live_count` is worth.
//!
//! # The question this exists to settle
//!
//! `runtime_curve`'s `live_cnt` column is S15's calibration control, and it is taken **once** per
//! checkpoint with no reps:
//!
//! ```text
//!     let t = Instant::now();
//!     let live_n = db.cat.live_count().unwrap_or(0);
//!     let live_ms = t.elapsed().as_secs_f64() * 1000.0;
//! ```
//!
//! Across D28's before and after runs that cell moved 6.098 ms -> 51.459 ms at N=10^5, which reads
//! as an 8.4x regression on a path D28 does not touch. Two explanations were available and only one
//! of them is a finding:
//!
//!   1. D28 made `live_count` slower. Falsifiable from source — see the verdict below — and
//!      falsified: `live_count` has **zero call sites on any statement path**, so nothing D28
//!      changed can reach it.
//!   2. A single sample of `live_count` has a long enough tail that 51 ms is an ordinary draw.
//!      That is a claim about a DISTRIBUTION, and a distribution is not something one sample can
//!      report. Hence this file.
//!
//! **A first hypothesis was written down and killed before either of those.** It said the queries
//! that used to precede `live_count` warmed the buffer pool with every record page and no longer
//! do, so the control had gone cold. It predicts a reproducible slowdown. Re-running the same
//! binary gave 7.434 ms, so it is wrong, and it is recorded here because a mechanism that is
//! plausible and untested is exactly the kind that survives into a ledger.
//!
//! # What it measures
//!
//! Three numbers over the same catalog, in this order:
//!
//!   * `cold`    — one draw, the first call after the forks, which is the draw `runtime_curve` takes.
//!   * `repped`  — mean over `reps` calls, which is what the cell *should* have been.
//!   * `singles` — `draws` independent single samples, reported as min / median / p95 / max. This is
//!     the distribution a one-shot cell is drawn from, and the only thing that can say whether a
//!     given cell was a tail or a regression.
//!
//! Usage: `live_count_reps [N] [reps] [draws] [threads]` (default 100000 200 200 64).
use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;

fn main() {
    let n: u64 = arg(1).unwrap_or(100_000);
    let reps: usize = arg(2).unwrap_or(200);
    let draws: usize = arg(3).unwrap_or(200);
    let threads: u64 = arg(4).unwrap_or(64);

    let dir = std::env::temp_dir().join(format!("ferrodb-lcreps-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("branches.branchcat");
    let _ = std::fs::remove_file(&path);
    // The DURABLE catalog, on a real file, as `runtime_curve` uses. An in-memory stand-in would
    // make every number below a property of a HashMap.
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&path, 1).expect("open catalog"));

    let lease = LeaseDeadline(u64::MAX);
    let per = n / threads.max(1);
    std::thread::scope(|s| {
        for _ in 0..threads {
            let cat = Arc::clone(&cat);
            s.spawn(move || {
                for _ in 0..per {
                    cat.fork(BranchId::TRUNK, lease).expect("fork");
                }
            });
        }
    });
    let forked = per * threads;

    // The cold draw, first, because it can only happen once.
    // The INHERENT `TableBranchCatalog::live_count`, returning `Result`, because that is the one
    // `runtime_curve` times (`db.cat.live_count().unwrap_or(0)` on a concrete `Arc`, where Rust
    // resolves inherent before trait). Timing the trait method instead would be a different
    // function and this file would not be answering the question it was opened for.
    let t = Instant::now();
    let live = cat.live_count().expect("live_count");
    let cold_ms = t.elapsed().as_secs_f64() * 1000.0;
    // Refuse rather than report: a `live_count` that has stopped counting would make every timing
    // below a measurement of an early return.
    assert!(
        live as u64 >= forked,
        "live_count {live} < {forked} forked — this is not measuring a full walk"
    );

    let t = Instant::now();
    for _ in 0..reps {
        std::hint::black_box(cat.live_count().expect("live_count"));
    }
    let repped_ms = t.elapsed().as_secs_f64() * 1000.0 / reps as f64;

    let mut singles: Vec<f64> = Vec::with_capacity(draws);
    for _ in 0..draws {
        let t = Instant::now();
        std::hint::black_box(cat.live_count().expect("live_count"));
        singles.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    singles.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let at = |q: f64| singles[((singles.len() - 1) as f64 * q) as usize];

    println!("S20 / D28: the spread of a SINGLE `live_count` sample, on the durable catalog.");
    println!();
    println!("N = {forked} branches, {reps} reps, {draws} independent single draws, {threads} fork threads.");
    println!();
    println!("  cold (one draw, what runtime_curve's live_cnt cell is) : {cold_ms:.3} ms");
    println!("  repped mean over {reps}                                 : {repped_ms:.3} ms");
    println!();
    println!("  {draws} single draws:");
    println!("    min    {:.3} ms", singles[0]);
    println!("    median {:.3} ms", at(0.50));
    println!("    p95    {:.3} ms", at(0.95));
    println!("    p99    {:.3} ms", at(0.99));
    println!("    max    {:.3} ms", singles[singles.len() - 1]);
    println!("    max/median = {:.1}x", singles[singles.len() - 1] / at(0.50).max(f64::MIN_POSITIVE));
    println!();
    println!("Read the max against the cell being questioned. A single draw is only evidence of a");
    println!("regression if the value sits OUTSIDE this spread.");

    let _ = std::fs::remove_dir_all(&dir);
}

fn arg<T: std::str::FromStr>(i: usize) -> Option<T> {
    std::env::args().nth(i).and_then(|s| s.parse().ok())
}
