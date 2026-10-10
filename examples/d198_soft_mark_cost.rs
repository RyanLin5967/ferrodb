//! What does the D198 soft mark cost a commit? (review 4, C6; pre-registered in
//! `bench/lease_grace/PREREG.md` amendment 12)
//!
//! Every commit of a branch catalog with no last-alive mark writes the soft mark `[0x0A]`: one
//! acquisition of the process lock for the lease clock, and one upsert, inside `logical`, which is
//! fork's serial section. A catalog with a mark writes none. So the two arms below differ by
//! exactly the soft mark, over the same code path, with no switch that disables anything:
//!
//! - `unmarked`: a fresh sidecar, used as an embedder that never resumes uses it;
//! - `marked`: a fresh sidecar marked before the timed loop by `record_lease_alive` then
//!   `resume_leases` at the same reading (downtime 0, so `D` stays 0 and no magic switch runs).
//!
//! Two commit kinds:
//! - `staged`: `fork_staged`, never awaited — the serial section alone, no fsync;
//! - `durable`: `set_root` on trunk — the whole commit, fsync included.
//!
//!   cargo run --release --example d198_soft_mark_cost -- [N_STAGED] [N_DURABLE] [ROUNDS]
//!
//! **The integer beside the timer.** `key_rewrites` per commit is fixed by control flow: the soft
//! mark is exactly one upsert per stage. So unmarked minus marked must be EXACTLY 1 per commit, in
//! both kinds. Anything else means the arms do not differ by the soft mark alone, and no ratio
//! printed after it is quoted (prediction I; the harness exits non-zero).
//!
//! Arms alternate A-B, B-A over the rounds so that drift on a shared machine lands on both.
use std::time::Instant;

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Arm {
    Unmarked,
    Marked,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Staged,
    Durable,
}

/// One timed run: per-commit latencies in nanoseconds, and `key_rewrites` over the loop.
struct Run {
    nanos: Vec<u128>,
    key_rewrites: u64,
}

fn fresh_catalog(dir: &std::path::Path, tag: &str) -> TableBranchCatalog {
    let path = dir.join(format!("{tag}.branchcat"));
    let _ = std::fs::remove_file(&path);
    TableBranchCatalog::open_sidecar(&path, 1).expect("open catalog")
}

fn run(dir: &std::path::Path, arm: Arm, kind: Kind, n: usize, round: usize) -> Run {
    let cat = fresh_catalog(dir, &format!("{arm:?}-{kind:?}-{round}"));
    if arm == Arm::Marked {
        let now = LeaseDeadline::now_millis();
        cat.record_lease_alive(now).expect("mark");
        cat.resume_leases(now).expect("resume");
    }
    let before = cat.key_rewrites();
    let mut nanos = Vec::with_capacity(n);
    for i in 0..n {
        let t = Instant::now();
        match kind {
            Kind::Staged => {
                cat.fork_staged(BranchId::TRUNK, LeaseDeadline(u64::MAX - 1)).expect("fork");
            }
            Kind::Durable => {
                cat.set_root(BranchId::TRUNK, 2 + (i as u32 % 2)).expect("set_root");
            }
        }
        nanos.push(t.elapsed().as_nanos());
    }
    Run { nanos, key_rewrites: cat.key_rewrites() - before }
}

fn quantile(sorted: &[u128], q: f64) -> u128 {
    let i = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[i]
}

fn main() {
    let arg = |i: usize, default: usize| -> usize {
        std::env::args().nth(i).and_then(|s| s.parse().ok()).unwrap_or(default)
    };
    let n_staged = arg(1, 2000);
    let n_durable = arg(2, 200);
    let rounds = arg(3, 6);
    if n_staged == 0 || n_durable == 0 || rounds == 0 {
        eprintln!("refusing: a run of zero commits or zero rounds measures nothing");
        std::process::exit(2);
    }

    let dir = std::env::temp_dir().join(format!("ferrodb-d198-softmark-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    println!(
        "D198 soft-mark cost: staged N={n_staged}, durable N={n_durable}, rounds={rounds}, \
         arms alternate A-B / B-A"
    );

    let mut integer_ok = true;
    for kind in [Kind::Staged, Kind::Durable] {
        let n = if kind == Kind::Staged { n_staged } else { n_durable };
        let mut medians: Vec<(u128, u128)> = Vec::new();
        for round in 0..rounds {
            let order = if round % 2 == 0 {
                [Arm::Unmarked, Arm::Marked]
            } else {
                [Arm::Marked, Arm::Unmarked]
            };
            let mut unmarked = None;
            let mut marked = None;
            for arm in order {
                let r = run(&dir, arm, kind, n, round);
                match arm {
                    Arm::Unmarked => unmarked = Some(r),
                    Arm::Marked => marked = Some(r),
                }
            }
            let (u, m) = (unmarked.unwrap(), marked.unwrap());
            let delta = u.key_rewrites as i128 - m.key_rewrites as i128;
            let exact = delta == n as i128;
            integer_ok &= exact;
            let (mut us, mut ms) = (u.nanos, m.nanos);
            us.sort_unstable();
            ms.sort_unstable();
            let (um, mm) = (quantile(&us, 0.5), quantile(&ms, 0.5));
            medians.push((um, mm));
            println!(
                "{kind:?} round {round}: unmarked median {:.2} us p90 {:.2} us | marked median \
                 {:.2} us p90 {:.2} us | key_rewrites unmarked {} marked {} (difference \
                 {delta}, {} per commit: {})",
                um as f64 / 1e3,
                quantile(&us, 0.9) as f64 / 1e3,
                mm as f64 / 1e3,
                quantile(&ms, 0.9) as f64 / 1e3,
                u.key_rewrites,
                m.key_rewrites,
                if exact { "exactly 1" } else { "NOT 1" },
                if exact { "I holds" } else { "I FAILS" },
            );
        }
        // The pre-registered statistic (PREREG amendment 13, C6): the median over rounds of the
        // per-round ratio of medians. Pairing within a round cancels drift between rounds; an even
        // number of rounds takes the mean of the two middle ratios.
        let mut ratios: Vec<f64> = medians.iter().map(|(u, m)| *u as f64 / *m as f64).collect();
        ratios.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mid = ratios.len() / 2;
        let median_ratio =
            if ratios.len() % 2 == 0 { (ratios[mid - 1] + ratios[mid]) / 2.0 } else { ratios[mid] };
        let (lo, hi) = if kind == Kind::Staged { (1.00, 1.40) } else { (0.95, 1.10) };
        println!(
            "{kind:?}: median over rounds of (unmarked median / marked median) = \
             {median_ratio:.3}; pre-registered interval [{lo:.2}, {hi:.2}]: {}",
            if (lo..=hi).contains(&median_ratio) { "inside" } else { "OUTSIDE (report it)" }
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
    if !integer_ok {
        eprintln!(
            "prediction I failed: the arms did not differ by exactly one key rewrite per commit, \
             so they do not differ by the soft mark alone; no ratio above is quoted"
        );
        std::process::exit(1);
    }
}
