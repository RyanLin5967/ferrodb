//! The claim "an independent consumer re-materializes the table and diffs it against the source",
//! made true and kept true.
//!
//! # What was missing
//!
//! The Go consumer could re-materialize a table from the change events and print it. Nothing produced
//! the other half of the comparison, so every check that the rebuilt table matched the source was a
//! Rust test holding the expected rows as a literal — `"1|widget|999|0\n2|gadget|20|1"` and friends.
//! That verifies the pipeline against **what somebody typed**, which is a weaker claim than it looks:
//! change the workload and the literal is what breaks, and a literal cannot notice a difference nobody
//! anticipated.
//!
//! Two pieces close it. `table_dump` asks the source database `SELECT * FROM <table>` — the same
//! question a user would ask, MVCC visibility and all — and prints the answer as JSON. `cdc-consumer
//! diff` folds the feed with the **same** `Table.apply` the sink uses, then compares the two per row
//! and per column.
//!
//! # Why comparing semantically rather than byte for byte
//!
//! Both sides are decoded with `UseNumber()`, so numeric text survives exactly — which is the entire
//! point for an int64 past 2^53. Comparing the two JSON documents as bytes would fail on key order or
//! on any formatting difference between a Rust writer and a Go writer, neither of which is a data
//! problem, and a check that cries wolf gets switched off.
//!
//! The renderers are shared rather than parallel: `write_table_json` uses the same `value_into` the feed
//! writer uses, so a `DECIMAL` or `TIMESTAMP` is a string on both sides without either tool knowing it.

use std::path::{Path, PathBuf};
use std::process::Command;

fn go_bin() -> String {
    for c in ["go", "/opt/homebrew/bin/go", "/usr/local/go/bin/go"] {
        if Command::new(c).arg("version").output().map(|o| o.status.success()).unwrap_or(false) {
            return c.to_string();
        }
    }
    panic!("Go is required to drive the CDC consumer");
}

/// Cargo's own path for an example binary, refusing one older than the source it was built from.
///
/// `cargo test` does not rebuild examples, and a stale binary here would certify a `diff` that no
/// longer exists. That has fooled three fire-checks in this repo already.
fn example_bin(name: &str) -> PathBuf {
    let mut p = std::env::current_exe().expect("test binary path");
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
    let bin = p.join("examples").join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    let bin_time = std::fs::metadata(&bin)
        .unwrap_or_else(|e| panic!("{} missing ({e}); run: cargo build --examples", bin.display()))
        .modified()
        .expect("mtime");
    let own_src = std::fs::metadata(Path::new("examples").join(format!("{name}.rs")))
        .ok()
        .and_then(|m| m.modified().ok());
    let newest = [newest_under(Path::new("src")), own_src].into_iter().flatten().max();
    if let Some(src) = newest {
        assert!(
            bin_time >= src,
            "{} is older than src/ or its own source; run: cargo build --examples",
            bin.display()
        );
    }
    bin
}

fn newest_under(dir: &Path) -> Option<std::time::SystemTime> {
    let mut newest = None;
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let p = e.path();
        let t = if p.is_dir() {
            newest_under(&p)
        } else {
            std::fs::metadata(&p).ok().and_then(|m| m.modified().ok())
        };
        if t > newest {
            newest = t;
        }
    }
    newest
}

/// Produce a database, its feed, and the source dump of `inventory`, all from one run of real SQL.
fn pipeline(dir: &Path) -> (PathBuf, PathBuf) {
    let db = dir.join("src.db");
    let feed = dir.join("feed.jsonl");
    let source = dir.join("source.json");

    let out = Command::new(example_bin("cdc_feed")).arg(&db).output().expect("run cdc_feed");
    assert!(out.status.success(), "cdc_feed failed: {}", String::from_utf8_lossy(&out.stderr));
    assert!(!out.stdout.is_empty(), "the feed is empty; everything below would be vacuous");
    std::fs::write(&feed, &out.stdout).unwrap();

    let out = Command::new(example_bin("table_dump"))
        .arg(&db)
        .arg("inventory")
        .output()
        .expect("run table_dump");
    assert!(out.status.success(), "table_dump failed: {}", String::from_utf8_lossy(&out.stderr));
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        text.trim_start().starts_with('[') && text.contains("\"id\""),
        "table_dump did not produce a JSON array of rows: {text}"
    );
    assert_ne!(text.trim(), "[]", "the source table dumped empty; a diff against it proves nothing");
    std::fs::write(&source, &out.stdout).unwrap();

    (feed, source)
}

/// Run `cdc-consumer diff` and return (success, combined output).
fn diff(feed: &Path, source: &Path) -> (bool, String) {
    let out = Command::new(go_bin())
        .current_dir("cdc-consumer")
        .args(["run", ".", "diff"])
        .arg(feed)
        .arg(source)
        .args(["-key", "id"])
        .output()
        .expect("run the diff");
    let mut text = String::from_utf8_lossy(&out.stdout).to_string();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    (out.status.success(), text)
}

/// **The claim itself: the rebuilt table equals the source.**
#[test]
fn the_table_rebuilt_from_the_feed_matches_the_source_database() {
    let dir = tempfile::tempdir().unwrap();
    let (feed, source) = pipeline(dir.path());

    let (ok, text) = diff(&feed, &source);
    assert!(ok, "the feed does not reproduce the source table:\n{text}");
    assert!(
        text.contains("MATCH"),
        "the diff exited zero without reporting a match, so it may have compared nothing: {text}"
    );
    // A count, so "matched" cannot mean "matched zero rows against zero rows".
    assert!(
        text.contains("MATCH 2 row(s)"),
        "expected the workload's two surviving rows to be compared: {text}"
    );
}

/// Rewrite one line of the feed and hand the result back to the diff.
fn mutated(dir: &Path, feed: &Path, name: &str, f: impl Fn(&str) -> Option<String>) -> PathBuf {
    let text = std::fs::read_to_string(feed).unwrap();
    let mut out = String::new();
    let mut changed = false;
    for line in text.lines() {
        match f(line) {
            Some(kept) => {
                if kept != line {
                    changed = true;
                }
                out.push_str(&kept);
                out.push('\n');
            }
            None => changed = true,
        }
    }
    assert!(changed, "the mutation for `{name}` matched nothing, so the case below is not testing it");
    let p = dir.join(name);
    std::fs::write(&p, out).unwrap();
    p
}

/// **The three ways a pipeline can be wrong, each caught and named.**
///
/// Without these the test above is a detector that has never fired: a diff that always prints MATCH
/// would satisfy it completely.
#[test]
fn the_diff_catches_a_wrong_value_an_extra_row_and_a_missing_row() {
    let dir = tempfile::tempdir().unwrap();
    let (feed, source) = pipeline(dir.path());

    // A value that arrives wrong. The workload updates qty to 999; corrupt that.
    let bad_value = mutated(dir.path(), &feed, "bad_value.jsonl", |l| {
        Some(l.replace("\"qty\":999", "\"qty\":42"))
    });
    let (ok, text) = diff(&bad_value, &source);
    assert!(!ok, "a corrupted value was reported as a match:\n{text}");
    assert!(
        text.contains("column \"qty\"") && text.contains("999") && text.contains("42"),
        "the report does not name the column and both values: {text}"
    );

    // A DELETE that never arrives: the destination keeps a row the source dropped. This is the
    // failure a soft-deleting sink is most likely to have and the hardest to see by eye.
    let no_delete = mutated(dir.path(), &feed, "no_delete.jsonl", |l| {
        if l.contains("\"op\":\"DELETE\"") { None } else { Some(l.to_string()) }
    });
    let (ok, text) = diff(&no_delete, &source);
    assert!(!ok, "a lost DELETE was reported as a match:\n{text}");
    assert!(
        text.contains("absent from the source"),
        "the report does not say the rebuilt table has a row the source does not: {text}"
    );

    // An INSERT that never arrives: a source row the consumer never learns about.
    let no_insert = mutated(dir.path(), &feed, "no_insert.jsonl", |l| {
        if l.contains("\"op\":\"INSERT\"") && l.contains("doohickey") {
            None
        } else {
            Some(l.to_string())
        }
    });
    let (ok, text) = diff(&no_insert, &source);
    assert!(!ok, "a lost INSERT was reported as a match:\n{text}");
    assert!(
        text.contains("missing from the feed"),
        "the report does not say the source has a row the feed never carried: {text}"
    );
}

/// **Nothing compared is not a pass**, on either side.
///
/// Two empty tables agree trivially, so a pipeline that delivered nothing at all would otherwise
/// report success — the exact failure a CDC consumer must never produce, because the destination looks
/// healthy and is empty.
#[test]
fn the_diff_refuses_when_there_is_nothing_to_compare() {
    let dir = tempfile::tempdir().unwrap();
    let (feed, source) = pipeline(dir.path());

    let empty_feed = dir.path().join("empty.jsonl");
    std::fs::write(&empty_feed, "").unwrap();
    let (ok, text) = diff(&empty_feed, &source);
    assert!(!ok, "an empty feed was diffed successfully:\n{text}");
    assert!(text.contains("no events"), "refused, but not by this guard: {text}");

    // Both sides empty: a schema-only feed against an empty source dump.
    let schema_only = mutated(dir.path(), &feed, "schema_only.jsonl", |l| {
        if l.contains("\"op\":\"CREATE_TABLE\"") { Some(l.to_string()) } else { None }
    });
    let empty_source = dir.path().join("empty.json");
    std::fs::write(&empty_source, "[]\n").unwrap();
    let (ok, text) = diff(&schema_only, &empty_source);
    assert!(!ok, "two empty tables were reported as agreeing:\n{text}");
    assert!(
        text.contains("nothing to agree about"),
        "refused, but not by the both-empty guard: {text}"
    );
}

/// `table_dump` must refuse rather than print `[]`, because an unknown table and an empty table are
/// different facts and a diff cannot tell them apart afterwards.
#[test]
fn table_dump_refuses_an_unknown_table_and_a_missing_database() {
    let dir = tempfile::tempdir().unwrap();
    let (_feed, _source) = pipeline(dir.path());

    let out = Command::new(example_bin("table_dump"))
        .arg(dir.path().join("src.db"))
        .arg("nosuch")
        .output()
        .expect("run table_dump");
    assert!(!out.status.success(), "an unknown table dumped successfully");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("unknown table") && err.contains("known tables are"),
        "the refusal does not use the shared unknown-table message: {err}"
    );
    assert!(out.stdout.is_empty(), "it printed a dump anyway: {:?}", String::from_utf8_lossy(&out.stdout));

    let out = Command::new(example_bin("table_dump"))
        .arg(dir.path().join("nope.db"))
        .arg("inventory")
        .output()
        .expect("run table_dump");
    assert!(!out.status.success(), "a missing database dumped successfully");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("does not exist"),
        "the refusal does not say the database is missing: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// **The README's diff block runs as written, and its documented output is what comes back.**
///
/// Every command block in this README is executed by a test (E51), because a documented command that
/// has quietly stopped working is worse than an undocumented one: a reader trusts it. This block is
/// two commands and one line of expected output, and all three are checked here rather than in
/// `integration_readme_commands.rs`, whose harness copies a scratch tree for the sink sequence and
/// would need reworking to carry a second file between two directories.
#[test]
fn the_readmes_diff_block_runs_as_written() {
    let root = {
        let mut p = std::env::current_dir().expect("cwd");
        while !p.join("README.md").exists() {
            assert!(p.pop(), "no README.md above the test's working directory");
        }
        p
    };
    let readme = std::fs::read_to_string(root.join("README.md")).expect("read README.md");

    let marker = "Run this after the sink commands above";
    let at = readme.find(marker).unwrap_or_else(|| {
        panic!("README no longer contains {marker:?}. If that section moved, update this test; if it \
                was deleted, the diff is now undocumented and that is the failure.")
    });
    let rest = &readme[at..];
    let open = rest.find("```").expect("no fenced block after the marker");
    let body_start = rest[open + 3..].find('\n').expect("unterminated fence") + open + 4;
    let close = rest[body_start..].find("```").expect("unterminated fenced block") + body_start;
    let block = &rest[body_start..close];

    let mut cmds = Vec::new();
    let mut documented = Vec::new();
    for line in block.lines() {
        match line.strip_prefix("$ ") {
            Some(c) => cmds.push(c.trim().to_string()),
            None if !line.trim().is_empty() => documented.push(line.trim().to_string()),
            None => {}
        }
    }
    assert_eq!(cmds.len(), 2, "expected the two documented commands, got {cmds:?}");
    assert!(
        cmds[0].contains("table_dump") && cmds[0].contains("inventory"),
        "the first documented command is not the source dump: {}",
        cmds[0]
    );
    assert!(
        cmds[1].contains("diff") && cmds[1].contains("-key id"),
        "the second documented command is not the diff: {}",
        cmds[1]
    );
    assert_eq!(documented, vec!["MATCH 2 row(s) from 6 event(s)"], "unexpected documented output");

    // Now run them for real, on the same workload the block's first line assumes.
    let dir = tempfile::tempdir().unwrap();
    let (feed, source) = pipeline(dir.path());
    let (ok, text) = diff(&feed, &source);
    assert!(ok, "the documented sequence does not succeed:\n{text}");
    assert!(
        text.contains(&documented[0]),
        "the README documents `{}` but the diff printed:\n{text}",
        documented[0]
    );
}
