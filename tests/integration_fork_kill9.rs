//! An acknowledged fork must survive SIGKILL. (D6a)
//!
//! Group commit moved the fsync out from under the logical lock and made it shared. The failure it
//! can introduce is exact and silent: a leader advances the durable watermark past work whose pages
//! were never written, the caller is told its fork succeeded, and a crash loses it. Throughput
//! numbers cannot see that — a catalog that acknowledges without flushing is FASTER — so the only
//! instrument is a real crash.
//!
//! `examples/fork_kill9` prints a branch id only after `fork` returned Ok, flushing each line. So
//! stdout is the list of forks the catalog PROMISED. We SIGKILL it, reopen, and require every one.
//!
//! ⛔ WHAT THIS TEST DOES **NOT** CATCH, STATED HERE BECAUSE A GREEN LINE IS OTHERWISE READ AS
//! COVERAGE IT DOES NOT HAVE.
//!
//! It was fire-checked against the ordering mutant D6a names — taking the commit ticket BEFORE the
//! mutations instead of after — and **it passed 8 runs out of 8**. Twice with a timer-based kill,
//! then again after being rebuilt to kill on quiescence. The mutant is a real durability bug, but
//! its window is sub-millisecond and depends on buffer-pool iteration order, so a crash test cannot
//! reliably discriminate it. Adding runs would buy confidence in proportion to nothing.
//!
//! That invariant is therefore tested where it is deterministically expressible, in
//! `src/branch/group_commit.rs`: `a_ticket_taken_during_a_sync_is_not_claimed_by_that_sync` blocks
//! a leader inside its sync with a channel and asserts the follower is not released on that
//! leader's watermark. That test DOES fail on the mutant, immediately.
//!
//! So what this one is: an end-to-end assertion that the whole path — fork, group commit, fsync,
//! process death, reopen — loses nothing it acknowledged. That is worth having and it is not the
//! ordering guard. The victim forks a fixed count then QUIESCES, so no later sync can rescue the
//! final group; killing on a timer while it was still forking meant a subsequent flush covered
//! nearly every at-risk fork, which is why the first version could not have caught anything.
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::branch::BranchCatalog;

fn example_bin(name: &str) -> PathBuf {
    let mut p = std::env::current_exe().expect("test exe");
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
    let out = p.join("examples").join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    let bin_time = std::fs::metadata(&out)
        .unwrap_or_else(|e| panic!("{} missing ({e}); run: cargo build --examples", out.display()))
        .modified()
        .expect("mtime");
    // Same staleness guard the other example-spawning tests use: `cargo test` does not rebuild
    // examples, and a stale victim would be testing yesterday's durability path.
    if let Ok(src) = std::fs::metadata("src/branch/group_commit.rs").and_then(|m| m.modified()) {
        assert!(
            bin_time >= src,
            "{} is older than src/branch/group_commit.rs — run: cargo build --examples",
            out.display()
        );
    }
    out
}

#[test]
fn an_acknowledged_fork_survives_sigkill() {
    let dir = std::env::temp_dir().join(format!("ferrodb-kill9-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("tmp dir");
    let cat_path = dir.join("branches.branchcat");
    let _ = std::fs::remove_file(&cat_path);

    let mut child = Command::new(example_bin("fork_kill9"))
        .arg(&cat_path)
        .arg("8")  // several threads, so the group actually batches and a leader really exists
        .arg("64") // a FIXED count: the victim then goes quiet, so no later sync can rescue the
                   // final group -- which is the only group a broken commit ordering can lose
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn fork_kill9");

    // Read until the victim says it is QUIESCED, then kill. Killing on a TIMER instead was the
    // first version, and it could not detect a deliberately broken commit ordering in three runs:
    // the victim was still forking, so a later group's sync flushed the pages of nearly every
    // at-risk fork. Waiting for quiescence makes the exposed window deterministic.
    let mut out = child.stdout.take().expect("stdout");
    let mut acked = String::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut buf = [0u8; 4096];
    while !acked.contains("QUIESCED") && std::time::Instant::now() < deadline {
        match out.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => acked.push_str(&String::from_utf8_lossy(&buf[..n])),
            Err(_) => break,
        }
    }
    assert!(acked.contains("QUIESCED"), "victim never quiesced; acks so far: {}", acked.lines().count());
    child.kill().expect("SIGKILL");
    let _ = child.wait();

    let ids: Vec<u64> = acked.lines().filter_map(|l| l.trim().parse().ok()).collect();

    // A run that collected nothing has not passed. Zero acknowledged forks would make every
    // assertion below vacuously true, which is the failure mode this repo keeps finding.
    assert!(
        ids.len() >= 50,
        "only {} acknowledged forks captured — the victim did not run long enough, and a test that \
         checks nothing is not a passing test",
        ids.len()
    );

    // Reopen from disk. Everything the dead process was told is durable must be here.
    let cat = TableBranchCatalog::open_sidecar(Path::new(&cat_path), 1).expect("reopen catalog");
    let mut lost = Vec::new();
    for &id in &ids {
        if cat.get_raw(id).is_err() {
            lost.push(id);
        }
    }
    assert!(
        lost.is_empty(),
        "{} of {} ACKNOWLEDGED forks did not survive SIGKILL (first few: {:?}). The durable \
         watermark advanced past work whose pages were never written.",
        lost.len(),
        ids.len(),
        &lost[..lost.len().min(8)]
    );

    // And the trunk is still readable, i.e. we did not merely fail to find anything at all.
    assert!(cat.get_raw(BranchId::TRUNK.id).is_ok(), "trunk unreadable after reopen");

    let _ = std::fs::remove_dir_all(&dir);
}
