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
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::jsonl::write_feed;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn sqlite_bin() -> String {
    for c in ["sqlite3", "/usr/bin/sqlite3", "/opt/homebrew/bin/sqlite3"] {
        if Command::new(c).arg("--version").output().map(|o| o.status.success()).unwrap_or(false) {
            return c.to_string();
        }
    }
    panic!("sqlite3 is required to read the destination independently of the driver that wrote it");
}

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

// ---------------------------------------------------------------------------------------------
// A commit carries many rows — and that shape was missing from every sink test in the repo.
// ---------------------------------------------------------------------------------------------
//
// `cdc_feed`'s workload is autocommit, so its six events have six distinct commit_lsn values: one
// row per commit. Measured on the feed this file already produces — `{1:1, 203:1, 393:1, 587:1,
// 916:1, 1154:1}`. So no test in the repo, including the diff above, ever landed two rows sharing a
// commit_lsn, and the sink's idempotence key was `commit_lsn` alone.
//
// The consequence, measured on the shipped consumer: a 3-row commit landed ONE row, printed
// `APPLIED 2 SKIPPED 2`, and exited 0. The first row advanced the cursor to its commit and both
// siblings then compared `commit_lsn <= cursor` and were discarded as re-deliveries. That is the
// backfill path too — `snapshot_table` emits its whole scan at one LSN — so the loss scaled with the
// size of the table.
//
// This test exists so the shape is reachable from REAL SQL rather than only from hand-written JSON:
// a `BEGIN; INSERT; INSERT; INSERT; COMMIT;` really does produce three events under one commit_lsn,
// and the sink really does land all three.

struct Src {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn src(dir: &Path) -> Src {
    let path = dir.join("multi.db");
    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.join("multi.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Src {
        _dir: tempfile::tempdir().unwrap(),
        catalog, wal, bp, txn, session: Session::new(),
    }
}

impl Src {
    fn sql(&mut self, sql: &str) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    }
}

#[test]
fn every_row_of_a_multi_row_commit_reaches_the_destination() {
    let dir = tempfile::tempdir().unwrap();
    let mut d = src(dir.path());

    d.sql("CREATE TABLE m (id INTEGER NOT NULL, v INTEGER);");
    d.sql("BEGIN;");
    d.sql("INSERT INTO m VALUES (1, 10);");
    d.sql("INSERT INTO m VALUES (2, 20);");
    d.sql("INSERT INTO m VALUES (3, 30);");
    d.sql("COMMIT;");

    use std::sync::atomic::Ordering;
    d.wal.flush().unwrap();
    let decoder = LogicalDecoder::new(&d.catalog);
    let out = decoder
        .decode(&d.wal, d.wal.base_lsn.load(Ordering::SeqCst), d.wal.next_lsn.load(Ordering::SeqCst))
        .expect("decode");

    let mut buf: Vec<u8> = Vec::new();
    write_feed(&out.events, &mut buf).expect("write feed");
    let text = String::from_utf8(buf).unwrap();

    // The premise: three row events really do share one commit_lsn. If ferrodb ever stopped batching
    // a transaction this way, this test would silently stop testing the defect - so it is asserted.
    let mut per_commit: std::collections::BTreeMap<u64, usize> = Default::default();
    for line in text.lines() {
        if !line.contains("\"op\":\"INSERT\"") {
            continue;
        }
        let c = line
            .split("\"commit_lsn\":").nth(1).expect("commit_lsn")
            .split(|ch: char| !ch.is_ascii_digit()).next().expect("digits")
            .parse::<u64>().expect("parse");
        *per_commit.entry(c).or_default() += 1;
    }
    let widest = per_commit.values().copied().max().unwrap_or(0);
    assert_eq!(
        widest, 3,
        "a BEGIN/COMMIT block did not put three inserts under one commit_lsn ({per_commit:?}), so \
         this test is no longer exercising a multi-row commit"
    );

    let feed = dir.path().join("multi.jsonl");
    std::fs::write(&feed, &text).unwrap();
    let out_db = dir.path().join("multi.sqlite");

    let sink = Command::new(go_bin())
        .current_dir("cdc-consumer")
        .args(["run", ".", "sink"])
        .arg(&feed)
        .arg("-db").arg(&out_db)
        .args(["-key", "id"])
        .output()
        .expect("run the sink");
    assert!(
        sink.status.success(),
        "the sink failed: {}{}",
        String::from_utf8_lossy(&sink.stdout),
        String::from_utf8_lossy(&sink.stderr)
    );

    let rows = Command::new(sqlite_bin())
        .arg(&out_db)
        .arg("SELECT id,v FROM m ORDER BY id;")
        .output()
        .expect("sqlite3");
    let landed = String::from_utf8_lossy(&rows.stdout).replace("\r\n", "\n").trim().to_string();
    assert_eq!(
        landed, "1|10\n2|20\n3|30",
        "not every row of the commit reached the destination. Rows sharing a commit_lsn are being \
         discarded as re-deliveries, and the run still exits 0:\n  landed: {landed}\n  sink said: {}",
        String::from_utf8_lossy(&sink.stdout).trim()
    );
}
