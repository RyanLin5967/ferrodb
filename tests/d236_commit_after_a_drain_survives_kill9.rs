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
//! 0. A CONTROL row, `c(7)`, committed the ordinary way. Its `Commit` follows its own `Begin` and
//!    `HeapInsert`, so it is written under `>=` as well. After the reopen it must be there at every
//!    commit, which separates "the drained COMMIT was lost" from "the reopen lost committed data".
//! 1. `BEGIN`, then insert 1.5x the buffer pool into one table: 1536 rows, one per heap page.
//!    The pool is `MAX_BUFFER_POOL_PAGES` = 1024 frames at `9aa6968`. The load's first DIRTY
//!    eviction (the first heap page, at about insert 1016) drains the log, and every page loaded
//!    after it stays dirty and unflushed.
//! 2. A `SELECT` that scans every page and matches nothing. It appends no record, and its faults
//!    must evict at least one of those dirty pages, which drains the buffer. The pool is ARC, not
//!    LRU, so "must" is a premise, and the test checks it (below).
//! 3. `COMMIT`. Its `Commit` starts exactly at `flushed_lsn`. Under `>=` it is acknowledged and not
//!    written.
//! 4. `kill -9` as soon as COMMIT is acknowledged, before anything else can flush.
//! 5. Reopen (recovery runs), check the control, and look up the first and last rows.
//!
//! # Why the table is between one and two pools, and not bigger
//!
//! ARC is scan-resistant, and the size is what lets this scan get past that (INFERRED from
//! `src/buffer/arc.rs` at `9aa6968`; `frontier/d236_review.md` W1 and `lane_d236.md` AMENDMENT 1
//! in artie-research). With c frames and N heap pages:
//! * The load promotes every heap page to T2, and its evictions move T2's oldest pages into the ghost
//!   list b2. b2 loses entries only once all four lists together hold 2c, so for N < 2c every
//!   evicted page is still a ghost when the scan starts.
//! * The scan's first page is a b2 ghost. A ghost hit evicts T2's oldest page, which becomes a
//!   ghost in turn, so every page the scan reaches is a ghost hit, and each one evicts the next
//!   oldest load page. After about 2c - N steps it evicts the first dirty page, and the gate drains
//!   (step 496 in a model of `arc.rs` driven by this test's access trace, which counts the setup and
//!   index pages that shift it: `frontier/d236_arc_band_check.md` in artie-research).
//! * For N ≥ 2c the early pages have left b2 before the scan starts. The scan is then a run of
//!   complete misses that recycle ONE T1 frame, the dirty tail in T2 is never touched, and nothing
//!   drains. At 2600 rows this test is predicted to refuse at every commit, and more rows make that
//!   worse, not better.
//! * The setup and index pages also sit in the lists, so the band's real upper edge is a few dozen
//!   pages below 2c (the model drains at N = 2030 and not at 2040). The fixture check below is
//!   therefore necessary, not sufficient: the premise assertion is what decides a run.
//!
//! # The premises, each checked and each refusing rather than passing
//!
//! * **The scan drained the log.** While a transaction is open, `<db>.wal` grows only by `flush`,
//!   which always drains the whole buffer and appends it at the file's end. `new` runs only at
//!   startup, and `truncate` only from a checkpoint, which needs no open transaction. So the file
//!   growing during the scan means a full drain, and the scan appends nothing after it:
//!   `flushed_lsn == next_lsn` at COMMIT.
//! * **No checkpoint ran at COMMIT.** One would flush the `Commit` on its way to truncating, and
//!   erase the red. `FERRODB_CHECKPOINT_INTERVAL` is removed from the child's environment for that
//!   reason, and the file must not have shrunk at COMMIT, because a truncation can only shrink it.
//! * **The reopen works.** The control row must be there.
//!
//! After the row checks, the WAL must also have grown at COMMIT: under `>` the `Commit` reached the
//! disk before the acknowledgement; under `>=` it did not. It is checked last, so a red run fails at
//! "row 1 is gone" first.
//!
//! Pre-registered from source, UNBUILT (`lane_d236.md` AMENDMENT 1): against `9aa6968`'s `src/`
//! (mutant M1), FAILS at "row 1 is gone" with every premise holding. PASSES with the fix. A failed
//! premise is a VOID run, never a red.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::time::Duration;

/// Heap pages this test's table occupies: one row per page (see `PAD`). Strictly between one and
/// two pools, the band where the scan's pages are still ARC ghosts (see the header).
const ROWS: usize = 1536;
/// The pool at `9aa6968`: `MAX_BUFFER_POOL_PAGES` in `src/buffer/buffer_pool.rs`. `ROWS` is sized
/// against it, so a change there has to be re-derived here.
const POOL_FRAMES: usize = 1024;
/// Two tuples of this size cannot share a 4 KiB page, so each row gets a heap page of its own.
const PAD: usize = 2100;
/// Ordinary-table pages below the arena floor. The table needs about `ROWS` of them plus its
/// directory and index pages, and the floor is a hard ceiling (see `arena_headroom` in the CLI).
const HEADROOM: u32 = 4096;
/// A debug-build CLI loading and scanning a few thousand wide rows is slow. The bound is for a
/// hang, not for speed, and it applies to every wait: no write here can block (see `Cli::send`).
const PATIENCE: Duration = Duration::from_secs(900);
/// The control row's key.
const CONTROL: u32 = 7;

struct Cli {
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<String>,
    /// The last lines the CLI printed on either stream, for every failure message.
    recent: VecDeque<String>,
}

impl Cli {
    fn open(db: &Path) -> Cli {
        let mut child = Command::new(env!("CARGO_BIN_EXE_ferrodb"))
            .arg(db)
            .env("FERRODB_ARENA_HEADROOM", HEADROOM.to_string())
            // An inherited interval of 1 checkpoints at COMMIT and erases the red (see the header).
            .env_remove("FERRODB_CHECKPOINT_INTERVAL")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn ferrodb");
        let stdin = child.stdin.take().unwrap();
        let (tx, lines) = channel();
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
        Cli { child, stdin, lines, recent: VecDeque::new() }
    }

    /// One statement at a time. Each is under 8 KiB, well inside a pipe's buffer, so the write
    /// cannot block even on a CLI that has stopped reading. The wait for its output is bounded.
    fn send(&mut self, sql: &str) {
        let wrote = self
            .stdin
            .write_all(sql.as_bytes())
            .and_then(|_| self.stdin.write_all(b"\n"))
            .and_then(|_| self.stdin.flush());
        if let Err(e) = wrote {
            let head: String = sql.chars().take(60).collect();
            self.die(&format!("writing `{head}` to the CLI failed: {e}"));
        }
    }

    fn die(&mut self, msg: &str) -> ! {
        // Whatever is already queued may be the cause: show it.
        while let Ok(l) = self.lines.try_recv() {
            self.remember(l);
        }
        let recent: Vec<&str> = self.recent.iter().map(|s| s.as_str()).collect();
        panic!("{msg}\n--- the CLI's last lines ---\n{}", recent.join("\n"))
    }

    fn remember(&mut self, l: String) {
        self.recent.push_back(l);
        if self.recent.len() > 20 {
            self.recent.pop_front();
        }
    }

    fn next_line(&mut self, what: &str) -> String {
        match self.lines.recv_timeout(PATIENCE) {
            Ok(l) => {
                self.remember(l.clone());
                if failed(&l) {
                    self.die(&format!("{what}: the CLI reported {l}"));
                }
                l
            }
            Err(RecvTimeoutError::Timeout) => {
                self.die(&format!("{what}: no output for {PATIENCE:?}"))
            }
            Err(RecvTimeoutError::Disconnected) => {
                let status = self.child.try_wait();
                self.die(&format!("{what}: the CLI exited ({status:?})"))
            }
        }
    }

    /// Read until a stdout line ends with `marker`.
    fn expect(&mut self, marker: &str, what: &str) {
        loop {
            let l = self.next_line(what);
            if !l.starts_with("stderr: ") && l.trim_end().ends_with(marker) {
                return;
            }
        }
    }

    /// The count line of a SELECT: `(1 row)` or `(N rows)`, whichever comes.
    fn row_count(&mut self, what: &str) -> String {
        loop {
            let l = self.next_line(what);
            let t = l.trim_end();
            if !l.starts_with("stderr: ") && (t.ends_with("row)") || t.ends_with("rows)")) {
                return l;
            }
        }
    }
}

impl Drop for Cli {
    /// A wedged child must not outlive the test (memory: load harnesses must not orphan). After the
    /// explicit kill or `.exit` below, this is a no-op.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A statement failed. The CLI reports failures on stderr as `error: ...`, `parser error: ...` or
/// `fatal error: ...`. A crash's panic text is not caught here, but it ends the stream, and
/// `next_line` reports the exit together with the last lines.
fn failed(line: &str) -> bool {
    line.starts_with("stderr: ") && line.contains("error")
}

fn wal_len(db: &Path) -> u64 {
    std::fs::metadata(format!("{}.wal", db.display())).map(|m| m.len()).unwrap_or(0)
}

#[test]
fn an_acknowledged_commit_after_the_gate_drained_the_log_survives_kill9() {
    assert!(
        POOL_FRAMES < ROWS && ROWS < 2 * POOL_FRAMES,
        "fixture: ROWS must be strictly between one and two pools, where the scan's pages are still \
         ARC ghosts (see the header)"
    );
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("d236.db");
    let pad = "x".repeat(PAD);

    // ---- the victim ------------------------------------------------------------------------
    let mut cli = Cli::open(&db);
    cli.send("CREATE TABLE c (id INTEGER NOT NULL);");
    cli.expect("ok", "CREATE TABLE c");
    cli.send(&format!("INSERT INTO c VALUES ({CONTROL});"));
    cli.expect("(1 row affected)", "the control row");
    cli.send("CREATE TABLE t (id INTEGER NOT NULL, pad VARCHAR(2100));");
    cli.expect("ok", "CREATE TABLE t");
    cli.send("BEGIN;");
    cli.expect("ok", "BEGIN");
    for id in 1..=ROWS {
        cli.send(&format!("INSERT INTO t VALUES ({id}, '{pad}');"));
        cli.expect("(1 row affected)", "the load");
    }

    let after_load = wal_len(&db);
    cli.send("SELECT id FROM t WHERE pad = 'no row has this pad';");
    cli.expect("(0 rows)", "the scan");
    let after_scan = wal_len(&db);
    assert!(
        after_scan > after_load,
        "premise failed: the WAL file did not grow during the scan ({after_load} -> {after_scan} \
         bytes), so the eviction gate never drained the log and COMMIT's record does not start at \
         the flushed point. This run is VOID, neither red nor green. The ARC band in the header is \
         wrong: re-derive it from src/buffer/arc.rs rather than changing ROWS by guesswork"
    );

    cli.send("COMMIT;");
    cli.expect("ok", "COMMIT");
    let after_commit = wal_len(&db);
    // Acknowledged. Kill before anything else can flush the log.
    cli.child.kill().expect("SIGKILL");
    let _ = cli.child.wait();
    drop(cli);
    assert!(
        after_commit >= after_scan,
        "premise failed: the WAL file shrank at COMMIT ({after_scan} -> {after_commit} bytes), so a \
         checkpoint truncated it and flushed the Commit on its way. This run proves nothing"
    );

    // A SIGKILLed CLI leaves its lock file behind by design (`storage/db_lock.rs`). Removing it is
    // what an operator does once the process is known dead, and `wait` above established that.
    std::fs::remove_file(ferrodb::storage::db_lock::lock_path(&db)).expect("remove the stale lock");

    // ---- the reopen: recovery runs here ---------------------------------------------------
    let mut cli = Cli::open(&db);
    cli.send(&format!("SELECT id FROM c WHERE id = {CONTROL};"));
    let control = cli.row_count("the control row after the reopen");
    assert!(
        control.trim_end().ends_with("(1 row)"),
        "premise failed: after the reopen the control row, committed the ordinary way, is gone too. \
         The reopen itself lost committed data, and says nothing about the drained COMMIT. Got: \
         {control}"
    );
    for id in [1, ROWS] {
        cli.send(&format!("SELECT id FROM t WHERE id = {id};"));
        let line = cli.row_count("the victim's rows after the reopen");
        assert!(
            line.trim_end().ends_with("(1 row)"),
            "row {id} is gone after kill -9, while the control row survived: COMMIT was acknowledged, \
             but its Commit record never reached the log, and recovery undid the transaction. WAL \
             bytes after the load {after_load}, after the scan {after_scan}, after COMMIT \
             {after_commit} (equal to the scan's means COMMIT wrote nothing). Got: {line}"
        );
    }
    // Checked AFTER the lookups, so a red run still fails first at "row N is gone": the durable
    // outcome is the claim, and this is how it came about. COMMIT's record starts at the flushed
    // point, so writing it before the acknowledgement is what grows the file.
    assert!(
        after_commit > after_scan,
        "COMMIT was acknowledged without writing its record: the WAL did not grow at COMMIT \
         ({after_scan} -> {after_commit} bytes), yet the rows survived. Something else made them \
         durable, and this run did not test the drained COMMIT"
    );
    cli.send(".exit");
    let _ = cli.child.wait();
}
