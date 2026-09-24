//! **D212 option (a), exit tests (6) and (7): kill the shipped binary, reopen, REVERT.**
//!
//! Through `CARGO_BIN_EXE_ferrodb` — the CLI, with its durable branch catalog, arena, provenance
//! and effect log, reopened by its own recover-then-rebuild sequence — rather than an example
//! binary, so there is no stale-example trap: cargo builds the bin for every integration test.
//!
//! The child dies by `std::process::abort()` at a crash point armed by an environment variable,
//! which models the process being killed, not power loss (the distinction
//! `integration_crash_safety.rs` spells out).
//!
//! | test | crash point | contract | pre-registered mutant |
//! |---|---|---|---|
//! | (6) | `FERRODB_CRASH_AFTER_PUBLISH_COMMIT`: after the publish commit, before anything in memory records the merge | the reopened database can REVERT the merge | the history is written in a transaction of its own after the publish commits (the sidecar-after-commit shape) |
//! | (7) | `FERRODB_CRASH_MID_REVERT=1`: after the revert's first inverse write, its transaction open | after reopen no row is reverted; a retry reverts once; a further REVERT is refused, across a restart too | inverses commit one at a time; or `write_revert_record` writes no `REVERTED` record |
//!
//! Both are RED at Step 0 (`b2269c9`) at their first assertion: the crash points do not exist
//! there, so the armed process exits cleanly.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

/// Feed SQL to the real binary. Returns whether it exited cleanly, and everything it printed.
fn ferrodb(db: &Path, sql: &str, arm: Option<(&str, &str)>) -> (bool, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ferrodb"));
    cmd.arg(db)
        // Same reason as `integration_cli_row_authorship_durability.rs`: the default arena floor is
        // ~128 MB of real zeroes on a filesystem without sparse files.
        .env("FERRODB_ARENA_HEADROOM", "256")
        .env_remove("FERRODB_CRASH_AFTER_PUBLISH_COMMIT")
        .env_remove("FERRODB_CRASH_MID_REVERT")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some((k, v)) = arm {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("spawn ferrodb");
    child.stdin.take().unwrap().write_all(sql.as_bytes()).expect("write sql");
    let out = child.wait_with_output().expect("wait for ferrodb");
    // **The single-writer lock survives a killed process, exactly as after a real kill -9.**
    // Removed here, once the child has been reaped and so provably holds nothing — the documented
    // recovery, performed at the one moment it is safe (`integration_crash_safety.rs` does the same).
    let mut lock = db.as_os_str().to_os_string();
    lock.push(".lock");
    let _ = std::fs::remove_file(std::path::PathBuf::from(lock));
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

/// What only a COMPLETED revert prints (`AgentOutput::Revert`'s display): "reverted" alone also
/// appears inside the "already reverted" refusal.
const REVERTED: &str = "cascaded:";

/// A process that must run to completion without any statement failing.
fn clean(db: &Path, sql: &str) -> String {
    let (ok, text) = ferrodb(db, sql, None);
    assert!(ok, "ferrodb exited abnormally on:\n{sql}\n--- output ---\n{text}");
    assert!(!text.contains("error:"), "a statement failed, so nothing after it means anything:\n{text}");
    text
}

/// The `(id, qty)` rows a `SELECT id, qty FROM inv;` printed, sorted.
///
/// The CLI prints its `ferrodb=> ` prompt with no newline, so the first line of each statement's
/// output arrives prefixed by it; the id is the last token before the `|`, whatever precedes it.
fn rows(text: &str) -> Vec<(i64, i64)> {
    let mut out: Vec<(i64, i64)> = text
        .lines()
        .filter_map(|l| {
            let mut it = l.split('|').map(|c| c.trim());
            match (it.next(), it.next(), it.next()) {
                (Some(a), Some(b), None) => {
                    Some((a.split_whitespace().last()?.parse().ok()?, b.parse().ok()?))
                }
                _ => None,
            }
        })
        .collect();
    out.sort_unstable();
    out
}

fn seed(db: &Path) {
    clean(
        db,
        "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n\
         INSERT INTO inv VALUES (1, 10);\n\
         INSERT INTO inv VALUES (2, 20);\n",
    );
}

/// **(6)** Killed after the publish commit and before the merge is recorded anywhere in memory.
/// The rows are durable; so must be everything REVERT needs to take them back.
#[test]
fn exit_6_a_merge_killed_after_its_commit_is_revertible_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("d212c6.db");
    seed(&db);

    let (ok, text) = ferrodb(
        &db,
        "BEGIN AGENT SESSION AS 'crasher' RUN 'r_crash';\n\
         UPDATE inv SET qty = qty + 5 WHERE id = 1;\n\
         MERGE;\n",
        Some(("FERRODB_CRASH_AFTER_PUBLISH_COMMIT", "1")),
    );
    assert!(!ok, "the process was armed to die after its publish commit and exited cleanly:\n{text}");

    let before = clean(&db, "SELECT id, qty FROM inv;\n");
    assert_eq!(rows(&before), vec![(1, 15), (2, 20)], "the publish committed before the kill:\n{before}");

    let after = clean(&db, "REVERT MERGE m_1;\nSELECT id, qty FROM inv;\n");
    assert!(after.contains(REVERTED), "REVERT MERGE m_1 did not revert:\n{after}");
    assert_eq!(rows(&after), vec![(1, 10), (2, 20)], "the killed merge was not reverted:\n{after}");
}

/// **(7)** Killed in the middle of a REVERT. The revert is one transaction, so after reopen none of
/// it happened; a retry happens once; and the record of it survives the next restart.
#[test]
fn exit_7_a_revert_killed_part_way_is_all_or_none_and_a_retry_applies_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("d212c7.db");
    seed(&db);
    // Two Adds, so the revert makes two inverse writes and the kill lands between them.
    clean(
        &db,
        "BEGIN AGENT SESSION AS 'adder' RUN 'r_add';\n\
         UPDATE inv SET qty = qty + 5 WHERE id = 1;\n\
         UPDATE inv SET qty = qty + 5 WHERE id = 2;\n\
         MERGE;\n",
    );

    let (ok, text) =
        ferrodb(&db, "REVERT MERGE m_1;\n", Some(("FERRODB_CRASH_MID_REVERT", "1")));
    assert!(!ok, "the process was armed to die mid-revert and exited cleanly:\n{text}");

    let torn = clean(&db, "SELECT id, qty FROM inv;\n");
    assert_eq!(
        rows(&torn),
        vec![(1, 15), (2, 25)],
        "a revert killed after one of its two writes left part of itself behind:\n{torn}"
    );

    let (ok, retry) = ferrodb(
        &db,
        "REVERT MERGE m_1;\nSELECT id, qty FROM inv;\nREVERT MERGE m_1;\nSELECT id, qty FROM inv;\n",
        None,
    );
    assert!(ok, "the retry process exited abnormally:\n{retry}");
    assert_eq!(retry.matches(REVERTED).count(), 1, "the retry did not revert exactly once:\n{retry}");
    assert!(retry.contains("already reverted"), "the second REVERT was not refused:\n{retry}");
    let seen: Vec<(i64, i64)> = rows(&retry);
    assert_eq!(
        seen,
        vec![(1, 10), (1, 10), (2, 20), (2, 20)],
        "the retry did not revert exactly once, or the refused one moved a value:\n{retry}"
    );

    let (ok, later) = ferrodb(&db, "REVERT MERGE m_1;\nSELECT id, qty FROM inv;\n", None);
    assert!(ok, "the process after the restart exited abnormally:\n{later}");
    assert!(!later.contains(REVERTED), "after a restart the revert ran again:\n{later}");
    assert!(
        later.contains("already reverted"),
        "after a restart the revert was accepted again, so each Add moved twice:\n{later}"
    );
    assert_eq!(rows(&later), vec![(1, 10), (2, 20)]);
}
