//! D8 — kill the process mid-merge, restart, prove there is no torn state.
//!
//! The merge publishes every row in one transaction precisely so that a failure part-way through
//! cannot leave the target half-updated. This test kills the process *inside* that loop, with rows
//! already written and the transaction still open, then reopens the database through the same
//! sequence the CLI uses — `recover()` then `Catalog::open` — and checks what survived.
//!
//! **The invariant:** every row is at its pre-merge value, or every row is at its merged value.
//! A mixture is the failure this is looking for, and it is the state a row-at-a-time publish would
//! produce routinely.
//!
//! # What this does and does not simulate
//!
//! The child dies via `std::process::abort()`: no destructors, no Rust-level flush, no orderly
//! shutdown. That is a faithful model of **the process being killed**.
//!
//! It is *not* power loss. Bytes already passed to `write()` live in the OS page cache and outlive
//! the process, so nothing here exercises what happens when the machine loses power with dirty
//! cache. Calling this "crash safe" without that distinction would claim a durability property
//! that was never tested.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Values the seed writes, and the value the merge sets every row to.
const SEEDED: &str = "1:100,2:200,3:300";
const MERGED: &str = "1:999,2:999,3:999";


/// Refuse to run against a stale example binary.
///
/// `cargo test` does NOT rebuild examples — confirmed by mtime — so a test that spawns one can
/// silently exercise a build from before the change under test. That is not a hypothetical: the
/// first fire-check of this file passed while the injected defect was live, because the replica
/// binary predated it.
///
/// A test that cannot observe the code it claims to test is worse than no test, so this refuses
/// with an instruction instead of passing.
fn assert_example_is_fresh(bin: &Path) {
    let bin_time = std::fs::metadata(bin)
        .unwrap_or_else(|e| panic!("{} is missing ({e}); run: cargo build --examples", bin.display()))
        .modified()
        .expect("mtime");
    let newest_src = walk_newest(Path::new("src"));
    if let Some(src_time) = newest_src {
        assert!(
            bin_time >= src_time,
            "{} is older than src/ — cargo test does not rebuild examples, so this would test a \
             stale binary. Run: cargo build --examples",
            bin.display()
        );
    }
}

fn walk_newest(dir: &Path) -> Option<std::time::SystemTime> {
    let mut newest = None;
    let entries = std::fs::read_dir(dir).ok()?;
    for e in entries.flatten() {
        let p = e.path();
        let t = if p.is_dir() {
            walk_newest(&p)
        } else {
            std::fs::metadata(&p).ok().and_then(|m| m.modified().ok())
        };
        if let Some(t) = t {
            newest = Some(match newest {
                None => t,
                Some(cur) if t > cur => t,
                Some(cur) => cur,
            });
        }
    }
    newest
}

fn bin() -> PathBuf {
    // The integration test binary lives in target/<profile>/deps, so the example is two up.
    let mut p = std::env::current_exe().expect("test exe path");
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
        // `EXE_SUFFIX` is "" on unix and ".exe" on Windows. Hardcoding the unix name made every
    // example-spawning test fail on the Windows runner with "The system cannot find the file
    // specified" - the binary was built, just not under the name being looked for.
    let out = p.join("examples").join(format!("crash_mid_merge{}", std::env::consts::EXE_SUFFIX));
    assert_example_is_fresh(&out);
    out
}

fn run(db: &Path, phase: &str, crash_after: Option<usize>) -> (bool, String) {
    let mut cmd = Command::new(bin());
    cmd.arg(db).arg(phase);
    match crash_after {
        Some(n) => cmd.env("FERRODB_CRASH_AFTER_ROWS", n.to_string()),
        None => cmd.env_remove("FERRODB_CRASH_AFTER_ROWS"),
    };
    let out = cmd.output().expect("failed to spawn the crash harness");
    let text = String::from_utf8_lossy(&out.stdout).to_string();

    // **Clear the single-writer lock, because this harness simulates crashes.** `crash_mid_merge`
    // exits with `process::exit`, which skips destructors, so its lock file survives exactly as it
    // would after a real kill -9 — and this test reuses one database across many runs, so without
    // this the second run of every scenario would be refused.
    //
    // Removing it here is not a workaround for the guard; it is the documented recovery, performed
    // at the one moment it is provably safe. `cmd.output()` has returned, so the child is reaped and
    // no process holds this database. That is precisely the condition the error message asks an
    // operator to establish before deleting the file.
    let mut lock = db.as_os_str().to_os_string();
    lock.push(".lock");
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock));

    (out.status.success(), text)
}

fn state(db: &Path) -> String {
    let (ok, text) = run(db, "read", None);
    assert!(ok, "reading back after recovery failed: {text}");
    text.lines()
        .find_map(|l| l.strip_prefix("STATE "))
        .unwrap_or_else(|| panic!("no STATE line in: {text}"))
        .trim()
        .to_string()
}

fn fresh(tag: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join(format!("{tag}.db"));
    let (ok, text) = run(&db, "seed", None);
    assert!(ok, "seed failed: {text}");
    (dir, db)
}

/// The control. Without it, "the crashed runs are untouched" could equally mean the merge never
/// works at all, and every crash assertion below would be vacuously satisfied.
#[test]
fn a_merge_that_is_not_interrupted_lands_completely() {
    let (_d, db) = fresh("clean");
    assert_eq!(state(&db), SEEDED, "the seed did not persist");

    let (ok, text) = run(&db, "merge", None);
    assert!(ok, "the uninterrupted merge failed: {text}");
    assert_eq!(state(&db), MERGED, "an uninterrupted merge did not land");
}

/// The row itself: killed part-way through publishing, at every point in the loop.
#[test]
fn a_merge_killed_mid_publish_leaves_no_torn_state() {
    for crash_after in 0..3usize {
        let (_d, db) = fresh(&format!("crash{crash_after}"));
        assert_eq!(state(&db), SEEDED, "the seed did not persist");

        let (ok, _) = run(&db, "merge", Some(crash_after));
        assert!(
            !ok,
            "the harness was asked to abort after {crash_after} row(s) and exited cleanly instead, \
             so this iteration tested nothing"
        );

        let after = state(&db);
        assert!(
            after == SEEDED || after == MERGED,
            "TORN STATE after a crash {crash_after} row(s) into the publish: {after}\n\
             expected all-or-nothing, i.e. {SEEDED} or {MERGED}"
        );
    }
}

/// Recovery must be repeatable. A restart that only produces consistent state the first time is a
/// restart nobody can rely on.
#[test]
fn reopening_repeatedly_after_a_crash_keeps_giving_the_same_answer() {
    let (_d, db) = fresh("repeat");
    let (ok, _) = run(&db, "merge", Some(1));
    assert!(!ok, "the harness did not abort, so nothing was tested");

    let first = state(&db);
    assert!(first == SEEDED || first == MERGED, "torn state: {first}");
    for i in 0..3 {
        assert_eq!(state(&db), first, "reopen #{i} disagreed with the first recovery");
    }
}

/// A crashed merge must not take the table with it. Recovering to consistent-but-empty would
/// satisfy an all-or-nothing check while having destroyed the database.
#[test]
fn a_crash_does_not_lose_the_table_or_its_rows() {
    let (_d, db) = fresh("survive");
    let (ok, _) = run(&db, "merge", Some(2));
    assert!(!ok, "the harness did not abort, so nothing was tested");

    let after = state(&db);
    assert_eq!(
        after.split(',').count(),
        3,
        "rows went missing after a crashed merge: {after}"
    );
}
