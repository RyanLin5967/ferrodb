//! E64 — a deleted primary key, used again, all the way to a destination table.
//!
//! # Why this did not exist until now
//!
//! `INSERT k; DELETE k; INSERT k` was **impossible** in this database until E63 was fixed this
//! morning: the uniqueness check read the index, an index entry outlives the row it points at, and so
//! a deleted key was taken forever. No CDC test, no sink and no consumer had ever processed a
//! delete-and-reinsert of one primary key, because no workload could produce one.
//!
//! That makes this a newly reachable path through the whole pipeline, and the shape a CDC pipeline is
//! most likely to get wrong. The destination soft-deletes: `DELETE` writes `_deleted = 1` rather than
//! removing the row, so the row that comes back has to *overwrite a tombstone*. A sink that treats a
//! tombstone as final — or one whose upsert does not reset the flag — leaves a row that exists in the
//! source reading as deleted downstream, forever, and self-consistently. Nothing errors.
//!
//! # What is checked, and where each check lives
//!
//! - the decoder emits `DELETE` then `INSERT` for the same key, in commit order (source side);
//! - the destination ends with the row **live** and carrying the new values (sink side);
//! - a replay of the whole feed leaves it that way (at-least-once);
//! - and a **stale `DELETE` arriving after the resurrect must not re-tombstone it**. That is the
//!   discriminating case: the other three pass against a sink that ignores `_deleted` ordering
//!   entirely, because in-order delivery gets the right answer by accident.
//!
//! The last one is deliberately run through the Go binary rather than in-process, because the
//! resurrect is the case where "the destination is right" and "the destination happens to agree with
//! the last event delivered" differ.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::jsonl::write_feed;
use ferrodb::replication::logical::{ChangeOp, LogicalDecoder};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn go_bin() -> String {
    for c in ["go", "/opt/homebrew/bin/go", "/usr/local/go/bin/go"] {
        if Command::new(c).arg("version").output().map(|o| o.status.success()).unwrap_or(false) {
            return c.to_string();
        }
    }
    panic!("Go is required to drive the CDC sink");
}

fn sqlite_bin() -> String {
    for c in ["sqlite3", "/usr/bin/sqlite3", "/opt/homebrew/bin/sqlite3"] {
        if Command::new(c).arg("--version").output().map(|o| o.status.success()).unwrap_or(false) {
            return c.to_string();
        }
    }
    panic!("sqlite3 is required to read the destination independently of the driver that wrote it");
}

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reuse.db");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("reuse.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { _dir: dir, catalog, wal, bp, txn, session: Session::new() }
}

impl Db {
    fn sql(&mut self, sql: &str) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    }

    fn decode(&self) -> ferrodb::replication::logical::Decoded {
        use std::sync::atomic::Ordering;
        self.wal.flush().unwrap();
        let decoder = LogicalDecoder::new(&self.catalog);
        assert!(
            decoder.known_tables() > 0,
            "the decoder resolved no tables, so every change would decode as unresolved"
        );
        decoder
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode")
    }
}

/// The workload: one key used, deleted, and used again with different values.
///
/// `id = 2` is a control. It is inserted and never touched again, so any claim below about row 1
/// having moved is measured against a row that did not.
fn reuse_workload(d: &mut Db) {
    d.sql("CREATE TABLE inventory (id INTEGER NOT NULL, item VARCHAR(32), qty INTEGER);");
    d.sql("INSERT INTO inventory VALUES (1, 'widget', 10);");
    d.sql("INSERT INTO inventory VALUES (2, 'gadget', 20);");
    d.sql("DELETE FROM inventory WHERE id = 1;");
    d.sql("INSERT INTO inventory VALUES (1, 'resurrected', 77);");
}

/// **Source side: the decoder emits the delete and the reuse as two events on one key, in order.**
#[test]
fn the_decoder_emits_a_delete_then_an_insert_for_the_same_key() {
    let mut d = db();
    reuse_workload(&mut d);
    let out = d.decode();

    assert!(out.is_complete(), "the feed is incomplete, so any sequence claim below is unsound");

    // Only the events for key 1, in the order a consumer would see them.
    let mut seen: Vec<(&'static str, u64)> = Vec::new();
    for e in &out.events {
        // A DELETE describes the row that went away, so its key is in the BEFORE image.
        let row = match &e.op {
            ChangeOp::Insert { new } => Some(new),
            ChangeOp::Update { new, .. } => Some(new),
            ChangeOp::Delete { old } => Some(old),
            _ => None,
        };
        if let Some(row) = row {
            if matches!(row.first(), Some(Value::Integer(1))) {
                seen.push((e.op.name(), e.commit_lsn));
            }
        }
    }

    let ops: Vec<&str> = seen.iter().map(|(t, _)| *t).collect();
    assert_eq!(
        ops,
        vec!["INSERT", "DELETE", "INSERT"],
        "key 1's history did not decode as insert, delete, insert: {seen:?}"
    );
    // Strictly increasing commit positions, or a consumer's ordering guard cannot tell the resurrect
    // from the delete it must overwrite.
    for w in seen.windows(2) {
        assert!(
            w[1].1 > w[0].1,
            "two events on one key share or invert their commit_lsn, so the resurrect cannot be \
             ordered after the delete: {seen:?}"
        );
    }

    // The reinsert carries the NEW values, not a replay of the original row.
    let last = out
        .events
        .iter()
        .filter(|e| matches!(&e.op, ChangeOp::Insert { new }
            if matches!(new.first(), Some(Value::Integer(1)))))
        .next_back()
        .expect("an insert for key 1");
    match &last.op {
        ChangeOp::Insert { new } => assert!(
            format!("{new:?}").contains("resurrected"),
            "the reinsert carries the old row rather than the new one: {new:?}"
        ),
        other => panic!("expected an insert, got {other:?}"),
    }
}

/// Write the workload's feed to a JSONL file.
fn feed_file(dir: &Path) -> PathBuf {
    let mut d = db();
    reuse_workload(&mut d);
    let out = d.decode();
    let path = dir.join("feed.jsonl");
    let mut buf: Vec<u8> = Vec::new();
    let n = write_feed(&out.events, &mut buf).expect("write feed");
    assert!(n > 0 && !buf.is_empty(), "the feed is empty; everything downstream would be vacuous");
    std::fs::write(&path, &buf).unwrap();
    path
}

fn run_sink(feed: &Path, db_path: &Path) -> String {
    let out = Command::new(go_bin())
        .current_dir("cdc-consumer")
        .args(["run", ".", "sink"])
        .arg(feed)
        .arg("-db")
        .arg(db_path)
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

fn query(db_path: &Path, sql: &str) -> String {
    let out = Command::new(sqlite_bin()).arg(db_path).arg(sql).output().expect("run sqlite3");
    assert!(out.status.success(), "sqlite3 failed: {}", String::from_utf8_lossy(&out.stderr));
    // Row separators only - a `\r` inside a value is data and must still read as a difference.
    String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n").trim().to_string()
}

/// **Sink side: the row comes back live, with the new values, over its own tombstone.**
#[test]
fn a_reused_key_lands_live_in_the_destination() {
    let dir = tempfile::tempdir().unwrap();
    let feed = feed_file(dir.path());
    let out = dir.path().join("out.sqlite");

    let summary = run_sink(&feed, &out);
    assert!(summary.contains("APPLIED"), "no summary from the sink: {summary}");

    let rows = query(&out, "SELECT id,item,qty,_deleted FROM inventory ORDER BY id;");
    assert_eq!(
        rows, "1|resurrected|77|0\n2|gadget|20|0",
        "a key that was deleted and used again did not land as a live row carrying its new values"
    );

    // Said directly, because the row above is the whole point: the tombstone was overwritten, not
    // left in place beside a second row.
    assert_eq!(
        query(&out, "SELECT COUNT(*) FROM inventory WHERE id=1;"),
        "1",
        "the resurrect created a second row for one key rather than replacing the tombstone"
    );
    assert_eq!(
        query(&out, "SELECT COUNT(*) FROM inventory WHERE _deleted=1;"),
        "0",
        "the destination still holds a tombstone for a row the source has"
    );
}

/// **The tombstone has to be there before the resurrect can be shown to clear it.**
///
/// Asserting only the final state cannot see the `_deleted` flag at all. Measured: deleting
/// `"_deleted"=excluded."_deleted"` from the upsert leaves every other test in this file green,
/// because the same omission stops the `DELETE` tombstoning *and* stops the resurrect clearing, and
/// in-order delivery then lands on the right final row by symmetry. The destination agreeing with
/// the source is not the same as the pipeline having done the two things it claims to do.
///
/// So this delivers the feed in two batches, cut at the delete, and pins the flag in both
/// directions. Two batches is also how a real consumer meets this: the cursor left by the first run
/// is what admits the second.
#[test]
fn the_delete_tombstones_first_and_the_resurrect_clears_it() {
    let dir = tempfile::tempdir().unwrap();
    let feed = feed_file(dir.path());
    let out = dir.path().join("out.sqlite");

    let text = std::fs::read_to_string(&feed).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    let cut = lines
        .iter()
        .position(|l| l.contains("\"op\":\"DELETE\""))
        .expect("the feed contains no DELETE, so it is not the workload this test needs");
    assert!(
        cut + 1 < lines.len(),
        "the DELETE is the last event, so there is no resurrect to deliver in a second batch"
    );

    // Batch one: everything through the delete.
    let first = dir.path().join("first.jsonl");
    std::fs::write(&first, format!("{}\n", lines[..=cut].join("\n"))).unwrap();
    run_sink(&first, &out);
    assert_eq!(
        query(&out, "SELECT id,item,qty,_deleted FROM inventory WHERE id=1;"),
        "1|widget|10|1",
        "the DELETE did not tombstone the row, so this file cannot show the resurrect clearing one"
    );

    // Batch two: the resurrect, admitted by the cursor batch one left behind.
    let second = dir.path().join("second.jsonl");
    std::fs::write(&second, format!("{}\n", lines[cut + 1..].join("\n"))).unwrap();
    run_sink(&second, &out);
    assert_eq!(
        query(&out, "SELECT id,item,qty,_deleted FROM inventory WHERE id=1;"),
        "1|resurrected|77|0",
        "the resurrect did not clear the tombstone; downstream the row still reads as deleted"
    );
}

/// Replaying the whole feed must not resurrect the tombstone or lose the new row.
#[test]
fn replaying_a_feed_that_reuses_a_key_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let feed = feed_file(dir.path());
    let out = dir.path().join("out.sqlite");

    run_sink(&feed, &out);
    let first = query(&out, "SELECT id,item,qty,_commit_lsn,_deleted FROM inventory ORDER BY id;");
    assert!(first.contains("resurrected"), "nothing landed; idempotence would be vacuous: {first}");

    let summary = run_sink(&feed, &out);
    let second = query(&out, "SELECT id,item,qty,_commit_lsn,_deleted FROM inventory ORDER BY id;");
    assert_eq!(first, second, "replaying a key-reuse feed changed the destination");
    assert!(
        summary.contains("SKIPPED") && !summary.contains("SKIPPED 0"),
        "the sink did not recognise the replay; identical rows may have been rewritten: {summary}"
    );
}

/// **The discriminating case: a stale `DELETE` arriving after the resurrect must not re-tombstone.**
///
/// Every test above delivers the feed in order, and in-order delivery reaches the right destination
/// even for a sink that ignores ordering entirely — the resurrect is simply the last thing written.
/// This one delivers the `DELETE` of key 1 *after* the reinsert that superseded it, with the cursor
/// cleared so nothing above the SQL statement can reject it. Only the `WHERE excluded._commit_lsn >
/// _commit_lsn` clause stands between the feed and a row that vanishes downstream while the source
/// still has it.
///
/// This is the resurrect-specific twin of `a_stale_event_arriving_after_a_newer_one_is_rejected`,
/// and it fails where that one cannot: that test replays stale *inserts*, so it never asks whether a
/// tombstone can come back.
#[test]
fn a_stale_delete_arriving_after_the_resurrect_does_not_re_tombstone_the_row() {
    let dir = tempfile::tempdir().unwrap();
    let feed = feed_file(dir.path());
    let out = dir.path().join("out.sqlite");
    run_sink(&feed, &out);

    let before = query(&out, "SELECT id,item,qty,_deleted FROM inventory ORDER BY id;");
    assert!(before.contains("1|resurrected|77|0"), "the resurrect did not land: {before}");

    // Just the DELETE of key 1 - stale relative to what the destination now holds.
    let text = std::fs::read_to_string(&feed).unwrap();
    let deletes: Vec<&str> = text.lines().filter(|l| l.contains("\"op\":\"DELETE\"")).collect();
    assert_eq!(
        deletes.len(),
        1,
        "expected exactly one DELETE in the feed to replay; got {}:\n{text}",
        deletes.len()
    );
    let stale = dir.path().join("stale.jsonl");
    std::fs::write(&stale, format!("{}\n", deletes[0])).unwrap();

    // Lose the cursor, so the guard under test is the only one left.
    Command::new(sqlite_bin()).arg(&out).arg("DELETE FROM _cdc_checkpoint;").status().unwrap();
    assert_eq!(
        query(&out, "SELECT COUNT(*) FROM _cdc_checkpoint;"),
        "0",
        "the cursor survived, so this test would be checking the cursor rather than the SQL guard"
    );

    run_sink(&stale, &out);

    let after = query(&out, "SELECT id,item,qty,_deleted FROM inventory ORDER BY id;");
    assert!(
        after.contains("1|resurrected|77|0"),
        "a stale DELETE re-tombstoned a row the source has. Downstream the row is gone and nothing \
         errored.\n  before: {before}\n  after:  {after}"
    );
    assert_eq!(before, after, "a stale DELETE changed the destination");
}
