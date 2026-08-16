//! E15 — landing the change feed in SQLite, and surviving replay.
//!
//! A change feed nobody lands anywhere is a demo. This exercises the real pipeline shape — source
//! database, change feed, destination table — where the interesting problem is not writing rows but
//! writing them **idempotently**.
//!
//! The feed is at-least-once by design, so a sink will be handed the same event twice and can be
//! handed a stale one after a newer one. Applying either naively is not a small bug: a re-applied
//! old `UPDATE` overwrites current data with a previous value, and a re-applied `INSERT` after a
//! `DELETE` resurrects a row the source no longer has. Both leave the destination silently wrong
//! *and self-consistent*, which is the worst failure a pipeline can have.
//!
//! **The destination is inspected with the `sqlite3` CLI**, not with the Go driver that wrote it,
//! for the same reason the feed is validated by a separate program: a writer checked with its own
//! reader agrees with itself about any shared misreading.

use std::path::{Path, PathBuf};
use std::process::Command;

fn go_bin() -> String {
    for c in ["go", "/opt/homebrew/bin/go", "/usr/local/go/bin/go"] {
        if Command::new(c).arg("version").output().map(|o| o.status.success()).unwrap_or(false) {
            return c.to_string();
        }
    }
    panic!("Go is required for the CDC sink");
}

fn sqlite_bin() -> String {
    for c in ["sqlite3", "/usr/bin/sqlite3", "/opt/homebrew/bin/sqlite3"] {
        if Command::new(c).arg("--version").output().map(|o| o.status.success()).unwrap_or(false) {
            return c.to_string();
        }
    }
    panic!("sqlite3 is required to verify the destination independently of the Go driver");
}

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
    let bin_time = std::fs::metadata(&bin)
        .unwrap_or_else(|e| panic!("{} missing ({e}); run: cargo build --examples", bin.display()))
        .modified()
        .unwrap();
    if let Some(src) = walk_newest(Path::new("src")) {
        assert!(bin_time >= src, "{} is older than src/; run: cargo build --examples", bin.display());
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

/// Produce a real feed from real SQL.
fn make_feed(dir: &Path) -> PathBuf {
    let feed = dir.join("feed.jsonl");
    let out = Command::new(example_bin("cdc_feed"))
        .arg(dir.join("src.db"))
        .output()
        .expect("run cdc_feed");
    assert!(out.status.success(), "cdc_feed failed: {}", String::from_utf8_lossy(&out.stderr));
    std::fs::write(&feed, &out.stdout).unwrap();
    assert!(!out.stdout.is_empty(), "the feed is empty; the test would be vacuous");
    feed
}

/// Run the Go sink. Returns its stdout summary line.
fn run_sink(feed: &Path, db: &Path) -> String {
    let out = Command::new(go_bin())
        .current_dir("cdc-consumer")
        .args(["run", ".", "sink"])
        .arg(feed)
        .args(["-db"])
        .arg(db)
        .args(["-key", "id"])
        .output()
        .expect("run the Go sink");
    assert!(
        out.status.success(),
        "the sink failed:\nstderr: {}\nstdout: {}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Query the destination with the sqlite3 CLI.
fn query(db: &Path, sql: &str) -> String {
    let out = Command::new(sqlite_bin())
        .arg(db)
        .arg(sql)
        .output()
        .expect("run sqlite3");
    assert!(out.status.success(), "sqlite3 failed: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[test]
fn the_feed_lands_in_sqlite_matching_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let feed = make_feed(dir.path());
    let db = dir.path().join("out.sqlite");

    let summary = run_sink(&feed, &db);
    assert!(summary.contains("APPLIED"), "no summary from the sink: {summary}");

    // The workload cdc_feed runs: insert ids 1..3, update id 1 to qty 999, delete id 2.
    let rows = query(&db, "SELECT id,item,qty,_deleted FROM inventory ORDER BY id;");
    assert_eq!(
        rows,
        "1|widget|999|0\n2|gadget|20|1\n3|doohickey|30|0",
        "the destination does not match the source workload"
    );

    // The delete is a TOMBSTONE, not a missing row. That is what lets a stale re-insert be
    // rejected later; a hard delete would throw away the LSN that does the rejecting.
    assert_eq!(
        query(&db, "SELECT COUNT(*) FROM inventory WHERE _deleted=1;"),
        "1",
        "the delete did not leave a tombstone"
    );
    // And every row records the commit that last wrote it.
    assert_eq!(query(&db, "SELECT COUNT(*) FROM inventory WHERE _commit_lsn <= 0;"), "0");
}

/// **Replay must be a no-op.** The feed is at-least-once, so this is the normal case, not an edge.
#[test]
fn replaying_the_whole_feed_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let feed = make_feed(dir.path());
    let db = dir.path().join("out.sqlite");

    run_sink(&feed, &db);
    let after_first = query(&db, "SELECT id,item,qty,_commit_lsn,_deleted FROM inventory ORDER BY id;");
    assert!(!after_first.is_empty(), "nothing landed, so idempotence would be vacuous");

    let summary = run_sink(&feed, &db);
    let after_second = query(&db, "SELECT id,item,qty,_commit_lsn,_deleted FROM inventory ORDER BY id;");

    assert_eq!(after_first, after_second, "replaying the feed changed the destination");
    // And it must have *recognised* the replay rather than coincidentally rewriting identical rows.
    assert!(
        summary.contains("SKIPPED") && !summary.contains("SKIPPED 0"),
        "the sink did not report skipping any re-delivered events: {summary}"
    );
}

/// **The guard is in the SQL, not in the control flow — tested with genuinely out-of-order
/// delivery.**
///
/// The first version of this test replayed the whole feed with the cursor cleared and asserted the
/// destination was unchanged. It passed with the ordering clause **removed**, which means it was
/// testing nothing: replaying a feed *in order* is naturally idempotent, because each row's last
/// write is still its newest one. The guard only earns its place when a stale event arrives AFTER
/// a newer one.
///
/// So this replays a single early event — the original `INSERT` of id 1 at qty 10, which was later
/// updated to 999, and the `INSERT` of id 2, which was later deleted. Without the clause, id 1
/// reverts and id 2's tombstone is lost. That is the silent corruption a CDC sink exists to
/// prevent, and it is now what fails when the clause goes away.
#[test]
fn a_stale_event_arriving_after_a_newer_one_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let feed = make_feed(dir.path());
    let db = dir.path().join("out.sqlite");
    run_sink(&feed, &db);

    let before = query(&db, "SELECT id,item,qty,_deleted FROM inventory ORDER BY id;");
    assert!(before.contains("1|widget|999|0"), "the update did not land: {before}");
    assert!(before.contains("2|gadget|20|1"), "the delete did not land: {before}");

    // Build a feed of ONLY the early inserts — stale relative to what the destination now holds.
    let text = std::fs::read_to_string(&feed).unwrap();
    let stale_lines: Vec<&str> = text
        .lines()
        .filter(|l| l.contains("\"op\":\"INSERT\"") && (l.contains("\"id\":1,") || l.contains("\"id\":2,")))
        .collect();
    assert_eq!(
        stale_lines.len(),
        2,
        "expected the two early inserts to replay; got {}:\n{text}",
        stale_lines.len()
    );
    let stale = dir.path().join("stale.jsonl");
    std::fs::write(&stale, format!("{}\n", stale_lines.join("\n"))).unwrap();

    // Lose the cursor, so nothing above the SQL statement can reject anything.
    Command::new(sqlite_bin()).arg(&db).arg("DELETE FROM _cdc_checkpoint;").status().unwrap();
    assert_eq!(query(&db, "SELECT COUNT(*) FROM _cdc_checkpoint;"), "0");

    run_sink(&stale, &db);

    let after = query(&db, "SELECT id,item,qty,_deleted FROM inventory ORDER BY id;");
    assert!(
        after.contains("1|widget|999|0"),
        "a stale INSERT reverted id 1 from qty 999 to its original value — the ordering guard is \
         not in the statement.\n  before: {before}\n  after:  {after}"
    );
    assert!(
        after.contains("2|gadget|20|1"),
        "a stale INSERT resurrected a deleted row — the tombstone was overwritten.\n  \
         before: {before}\n  after:  {after}"
    );
    assert_eq!(before, after, "stale events changed the destination");
}
