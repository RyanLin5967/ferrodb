//! D236, end to end through the shipped CLI: **an acknowledged COMMIT survives `kill -9` when the
//! eviction gate drained the whole log just before it.**
//!
//! # The defect
//!
//! `WalManager::append` returns where a record STARTS, and `flushed_lsn` is one past the last
//! durable byte. `flush_up_to(lsn)` returned early when `flushed_lsn >= lsn`, so the first record
//! appended after a flush that emptied the buffer, which starts exactly at `flushed_lsn`, was
//! treated as durable and never written. `TxnManager::commit` calls `flush_up_to(commit_lsn)`, so a
//! `Commit` that followed such a drain was acknowledged while it was still only in memory. A crash
//! then undid a transaction whose client had been told it committed.
//!
//! # The schedule, and why every step is there
//!
//! This is the single-session shape (b) from `frontier/d236_adversary.md` §3 in artie-research.
//! 1. `BEGIN`, then insert about 2.5x the buffer pool into one table: 2600 rows, one per heap page.
//!    The pool is `MAX_BUFFER_POOL_PAGES` = 1024 frames at `9aa6968`. Evictions during the load
//!    drain the log, but the load's last records are appended after the last drain.
//! 2. A `SELECT` that scans every page and matches nothing. It appends no record, and faulting the
//!    early pages back in cycles the whole pool. That evicts the load's most recent dirty pages,
//!    whose LSNs are past `flushed_lsn`, so the gate drains the buffer. From here on,
//!    `flushed_lsn == next_lsn`.
//! 3. `COMMIT`. Its `Commit` starts exactly at `flushed_lsn`. Under `>=` it is acknowledged and not
//!    written.
//! 4. `kill -9` as soon as COMMIT is acknowledged, before anything else can flush.
//! 5. Reopen (recovery runs) and look the first and last rows up.
//!
//! # How the test knows the drain happened (the premise)
//!
//! It watches the WAL FILE. The log is written only by `flush`, which appends the drained buffer
//! at its end, so `<db>.wal` grows exactly when a flush writes something. The file's length is
//! read after the load and again after the SELECT, and it MUST have grown: that growth is the
//! drain step 3 depends on. Nothing else in the CLI flushes: the lease thread never touches the
//! WAL, and the SELECT appends nothing. A drain empties the buffer, so it leaves
//! `flushed_lsn == next_lsn`. Without the growth the test refuses, instead of passing or failing
//! for a reason it did not create.
//!
//! The length after COMMIT is also printed in the failure message. Under `>` it grows again (the
//! `Commit` reached the disk); under `>=` it does not.
//!
//! Pre-registered from source, UNBUILT: FAILS at `9aa6968` at the "row is gone" assertion. The
//! premise holds there.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Receiver};
use std::time::Duration;

/// Heap pages this test's table occupies: one row per page (see `PAD`). About 2.5x the pool.
const ROWS: usize = 2600;
/// The pool at `9aa6968` (`MAX_BUFFER_POOL_PAGES` in `src/buffer/buffer_pool.rs`). The scan must
/// fault in more pages than this, or it may not cycle the pool at all.
const POOL_FRAMES: usize = 1024;
/// Two tuples of this size cannot share a 4 KiB page, so each row gets a heap page of its own.
const PAD: usize = 2100;
/// Ordinary-table pages below the arena floor. The table needs about `ROWS` of them plus its
/// directory and index pages, and the floor is a hard ceiling (see `arena_headroom` in the CLI).
const HEADROOM: u32 = 8192;
/// A debug-build CLI loading and scanning a few thousand wide rows is slow. The bound is for a
/// hang, not for speed.
const PATIENCE: Duration = Duration::from_secs(900);

struct Cli {
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<String>,
}

impl Cli {
    fn open(db: &Path) -> Cli {
        let mut child = Command::new(env!("CARGO_BIN_EXE_ferrodb"))
            .arg(db)
            .env("FERRODB_ARENA_HEADROOM", HEADROOM.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn ferrodb");
        let stdin = child.stdin.take().unwrap();
        let (tx, lines) = channel();
        // Both streams are read on their own threads for the whole life of the process. A load
        // written to stdin while nothing reads stdout deadlocks once the pipe fills.
        let out = child.stdout.take().unwrap();
        let tx_out = tx.clone();
        std::thread::spawn(move || {
            for l in BufReader::new(out).lines().map_while(Result::ok) {
                if tx_out.send(l).is_err() {
                    break;
                }
            }
        });
        let err = child.stderr.take().unwrap();
        std::thread::spawn(move || {
            for l in BufReader::new(err).lines().map_while(Result::ok) {
                if tx.send(format!("stderr: {l}")).is_err() {
                    break;
                }
            }
        });
        Cli { child, stdin, lines }
    }

    fn send(&mut self, sql: &str) {
        self.stdin.write_all(sql.as_bytes()).expect("write sql");
        self.stdin.write_all(b"\n").expect("write sql");
        self.stdin.flush().expect("flush stdin");
    }

    /// Read output until `count` lines have ended with `marker`. Any line on stderr is a failed
    /// statement, and everything after one would mean nothing, so it fails the test at once.
    fn expect(&self, marker: &str, count: usize, what: &str) {
        let mut seen = 0;
        while seen < count {
            let line = self
                .lines
                .recv_timeout(PATIENCE)
                .unwrap_or_else(|_| panic!("{what}: no `{marker}` within {PATIENCE:?} ({seen} of {count})"));
            assert!(!line.starts_with("stderr: "), "{what}: the CLI reported {line}");
            if line.trim_end().ends_with(marker) {
                seen += 1;
            }
        }
    }
}

fn wal_len(db: &Path) -> u64 {
    std::fs::metadata(format!("{}.wal", db.display())).map(|m| m.len()).unwrap_or(0)
}

#[test]
fn an_acknowledged_commit_after_the_gate_drained_the_log_survives_kill9() {
    assert!(ROWS > 2 * POOL_FRAMES, "fixture: the scan must fault in more than the pool holds");
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("d236.db");
    let pad = "x".repeat(PAD);

    // ---- the victim ------------------------------------------------------------------------
    let mut cli = Cli::open(&db);
    cli.send("CREATE TABLE t (id INTEGER NOT NULL, pad VARCHAR(2100));");
    cli.expect("ok", 1, "CREATE TABLE");
    cli.send("BEGIN;");
    cli.expect("ok", 1, "BEGIN");
    let mut load = String::with_capacity(ROWS * (PAD + 48));
    for id in 1..=ROWS {
        load.push_str(&format!("INSERT INTO t VALUES ({id}, '{pad}');\n"));
    }
    cli.send(load.trim_end());
    cli.expect("(1 row affected)", ROWS, "the load");

    let after_load = wal_len(&db);
    cli.send("SELECT id FROM t WHERE pad = 'no row has this pad';");
    cli.expect("(0 rows)", 1, "the scan");
    let after_scan = wal_len(&db);
    assert!(
        after_scan > after_load,
        "premise failed: the WAL file did not grow during the scan ({after_load} -> {after_scan} \
         bytes), so the eviction gate never drained the log and COMMIT's record does not start at \
         the flushed point. This run proves nothing either way"
    );

    cli.send("COMMIT;");
    cli.expect("ok", 1, "COMMIT");
    let after_commit = wal_len(&db);
    // Acknowledged. Kill before anything else can flush the log.
    cli.child.kill().expect("SIGKILL");
    let _ = cli.child.wait();
    drop(cli);

    // A SIGKILLed CLI leaves its lock file behind by design (`storage/db_lock.rs`). Removing it is
    // what an operator does once the process is known dead, and `wait` above established that.
    std::fs::remove_file(ferrodb::storage::db_lock::lock_path(&db)).expect("remove the stale lock");

    // ---- the reopen: recovery runs here ---------------------------------------------------
    let mut cli = Cli::open(&db);
    for id in [1, ROWS] {
        cli.send(&format!("SELECT id FROM t WHERE id = {id};"));
        // `(1 row)` if the transaction survived; `(0 rows)` if recovery undid it. Read whichever
        // comes, so the failure says which.
        let line = loop {
            let l = cli.lines.recv_timeout(PATIENCE).expect("the reopened CLI went quiet");
            assert!(!l.starts_with("stderr: "), "the reopened CLI reported {l}");
            if l.trim_end().ends_with("row)") || l.trim_end().ends_with("rows)") {
                break l;
            }
        };
        assert!(
            line.trim_end().ends_with("(1 row)"),
            "row {id} is gone after kill -9: COMMIT was acknowledged, but its Commit record never \
             reached the log, and recovery undid the transaction. WAL bytes after the load \
             {after_load}, after the scan {after_scan}, after COMMIT {after_commit} (equal to the \
             scan's means COMMIT wrote nothing). Got: {line}"
        );
    }
    cli.send(".exit");
    let _ = cli.child.wait();
}
