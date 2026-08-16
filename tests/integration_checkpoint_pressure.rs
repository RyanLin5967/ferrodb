//! E17 — making a rare condition constant, and finding out what depended on it being rare.
//!
//! The change feed reads the WAL, and a checkpoint throws the WAL away. At the default interval of
//! 256 commits that is invisible: a consumer keeping up reads records long before they are
//! discarded. **Rare is not safe.** Three separate features in this project — the base backup, the
//! schema in the feed, and the latency instrument — each shipped with a test that passed only
//! because it never crossed the threshold, and past it each one failed for real.
//!
//! So rather than hunt a fourth instance, this sets `FERRODB_CHECKPOINT_INTERVAL=1` and makes every
//! commit truncate.
//!
//! The second test is the one worth reading. It asserts that at the **default** interval this same
//! workload cannot tell a pinned consumer from an unpinned one — both succeed. That is the
//! blindness stated as an assertion rather than as a comment: it is why the first test must set the
//! environment variable, and it will fail if someone ever "simplifies" the harness by dropping it.

use std::path::{Path, PathBuf};
use std::process::Command;

fn example_bin(name: &str) -> PathBuf {
    let mut p = std::env::current_exe().expect("test exe");
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
        // `EXE_SUFFIX` is "" on unix and ".exe" on Windows. Hardcoding the unix name made every
    // example-spawning test fail on the Windows runner with "The system cannot find the file
    // specified" - the binary was built, just not under the name being looked for.
    let bin = p.join("examples").join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
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

const COMMITS: usize = 30;

/// Run the probe. `interval` of `None` leaves the default in place.
fn probe(interval: Option<&str>) -> std::collections::HashMap<String, String> {
    let dir = tempfile::tempdir().unwrap();
    let mut cmd = Command::new(example_bin("checkpoint_pressure"));
    cmd.arg(dir.path()).arg(COMMITS.to_string());
    if let Some(v) = interval {
        cmd.env("FERRODB_CHECKPOINT_INTERVAL", v);
    } else {
        cmd.env_remove("FERRODB_CHECKPOINT_INTERVAL");
    }
    let out = cmd.output().expect("run checkpoint_pressure");
    assert!(
        out.status.success(),
        "the probe itself failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    stdout
        .lines()
        .filter_map(|l| l.split_once(' ').map(|(k, v)| (k.to_string(), v.to_string())))
        .collect()
}

fn num(m: &std::collections::HashMap<String, String>, k: &str) -> usize {
    m.get(k).unwrap_or_else(|| panic!("no {k} in probe output: {m:?}"))
        .parse()
        .unwrap_or_else(|_| panic!("{k} is not a number: {:?}", m.get(k)))
}

/// **With every commit truncating, a pinned consumer loses nothing and an unpinned one loses
/// everything.** That difference is the entire argument for the subscription pin.
#[test]
fn under_constant_truncation_only_a_pinned_consumer_survives() {
    let m = probe(Some("1"));

    assert_eq!(
        num(&m, "PINNED_INSERTS"),
        COMMITS,
        "a pinned consumer lost rows even though it held a claim on the log: {m:?}"
    );
    assert_eq!(m.get("PINNED_ERR").map(String::as_str), Some("none"), "the pinned consumer errored: {m:?}");

    // The unpinned consumer must fail, and fail LOUDLY. Silently returning fewer rows would be the
    // worse outcome — a feed missing records is indistinguishable from one that had none.
    assert!(
        num(&m, "UNPINNED_INSERTS") < COMMITS,
        "the unpinned consumer somehow kept up with truncation on every commit: {m:?}"
    );
    let err = m.get("UNPINNED_ERR").cloned().unwrap_or_default();
    assert!(
        err.contains("truncated away"),
        "the unpinned consumer lost rows WITHOUT an error, which is silent data loss: {m:?}"
    );
}

/// **The blindness, asserted.**
///
/// At the default interval this same workload cannot tell the two consumers apart — both get
/// everything. That is precisely why the test above has to force the interval, and why three
/// earlier features shipped with tests that proved less than they appeared to. If this ever starts
/// failing, the default has changed and the test above should be re-examined rather than trusted.
#[test]
fn at_the_default_interval_the_workload_cannot_tell_the_two_apart() {
    let m = probe(None);

    assert_eq!(num(&m, "PINNED_INSERTS"), COMMITS, "{m:?}");
    assert_eq!(
        num(&m, "UNPINNED_INSERTS"),
        COMMITS,
        "an unpinned consumer lost rows at the DEFAULT interval — that is a worse finding than the \
         one this file was written for, and the test above is no longer the interesting one: {m:?}"
    );
    assert_eq!(m.get("UNPINNED_ERR").map(String::as_str), Some("none"), "{m:?}");
}
