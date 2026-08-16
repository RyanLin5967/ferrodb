//! E16 — the latency instrument, and its refusal to report nonsense.
//!
//! The number itself belongs in a benchmark, not a test — it varies with the machine. What is
//! worth pinning is that the instrument **works and refuses when it should**:
//!
//! - It must run past the automatic checkpoint threshold (256 commits). A run below it passes for
//!   the same reason E4's 40-row replication test passed, which is to say for a reason that does
//!   not generalise: the log has not been truncated yet. That is exactly how the missing
//!   subscription pin was found.
//! - It must refuse to print a measurement when commits produced no events, because a timing loop
//!   that found nothing reports beautifully small numbers — it is timing the cost of looking.

use std::path::{Path, PathBuf};
use std::process::Command;

fn example_bin(name: &str) -> PathBuf {
    let mut p = std::env::current_exe().expect("test exe");
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
    let bin = p.join("examples").join(name);
    let t = std::fs::metadata(&bin)
        .unwrap_or_else(|e| panic!("{} missing ({e}); run: cargo build --examples", bin.display()))
        .modified()
        .unwrap();
    if let Some(src) = walk_newest(Path::new("src")) {
        assert!(t >= src, "{} is older than src/; run: cargo build --examples", bin.display());
    }
    bin
}

fn walk_newest(dir: &Path) -> Option<std::time::SystemTime> {
    let mut newest = None;
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let p = e.path();
        let t = if p.is_dir() { walk_newest(&p) } else { std::fs::metadata(&p).ok().and_then(|m| m.modified().ok()) };
        if let Some(t) = t {
            newest = Some(match newest { Some(c) if c > t => c, _ => t });
        }
    }
    newest
}

/// 300 commits: past the 256-commit checkpoint interval, so the run crosses at least one automatic
/// truncation. Below it, the subscription pin would never be exercised.
const SAMPLES: usize = 300;

#[test]
fn the_latency_run_survives_an_automatic_checkpoint_and_reports() {
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(example_bin("cdc_latency"))
        .arg(dir.path().join("lat.db"))
        .arg(SAMPLES.to_string())
        .output()
        .expect("run cdc_latency");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "the latency run failed across a checkpoint — the consumer's log was truncated out from \
         under it:\nstdout: {stdout}\nstderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let summary = stdout
        .lines()
        .find(|l| l.starts_with("SAMPLES "))
        .unwrap_or_else(|| panic!("no machine-readable summary:\n{stdout}"));
    assert!(summary.contains(&format!("SAMPLES {SAMPLES}")), "{summary}");

    // Every sample accounted for, and the timings are real rather than a zero-valued placeholder.
    let p50: u128 = summary
        .split_whitespace()
        .skip_while(|t| *t != "P50_NS")
        .nth(1)
        .and_then(|v| v.parse().ok())
        .expect("no P50_NS");
    assert!(p50 > 0, "p50 came out as zero, which means nothing was actually timed: {summary}");
    let max: u128 = summary
        .split_whitespace()
        .skip_while(|t| *t != "MAX_NS")
        .nth(1)
        .and_then(|v| v.parse().ok())
        .expect("no MAX_NS");
    assert!(max >= p50, "max {max} is below p50 {p50}, so the distribution is wrong: {summary}");
}

/// The example writes its own database, so running it twice must not accumulate state that changes
/// what the second run measures. A measurement that depends on run order is not a measurement.
#[test]
fn two_runs_over_the_same_path_measure_the_same_thing() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lat.db");
    let run = || {
        let out = Command::new(example_bin("cdc_latency"))
            .arg(&db)
            .arg("300")
            .output()
            .expect("run cdc_latency");
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        let s = String::from_utf8_lossy(&out.stdout).to_string();
        s.lines()
            .find(|l| l.starts_with("SAMPLES "))
            .expect("no summary")
            .split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ")
    };
    assert_eq!(run(), run(), "the second run measured a different number of samples");
}
