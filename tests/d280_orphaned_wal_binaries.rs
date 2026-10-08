//! D280 — a data file whose write-ahead log never issued its pages' LSNs, opened as a primary by
//! the two shipped entry points.
//!
//! `backup::restore` is a file copy (`replication/backup.rs`), and a replica never builds a
//! `WalManager` (`examples/repl_replica.rs`). Either file therefore arrives with pages stamped at the
//! primary's LSNs and no `<db>.wal`. Opened with `ferrodb` or `pgserver`, the log was created at
//! LSN 1, far below those pages. A committed write to one of them that crashed before the page was
//! flushed was then skipped by redo (`page.lsn >= lsn`), and the transaction still had its Commit,
//! so nothing undid or reported it.
//!
//! These tests drive the real binaries, so they state the outcome and nothing about the API:
//! either the open is refused and leaves the file exactly as it found it, or every committed write
//! survives a crash. They use only API that existed before the fix, so the same file is the red
//! run at the base and the green run at the tip.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::replication::backup;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::heap_page::Page;
use ferrodb::wal::log::WalManager;

/// Ordinary-table pages below the arena floor. Small for the reason
/// `integration_cli_effect_log_durability.rs` gives: the default puts the arena ~128 MB in, which is
/// 128 MB of real zeroes on a filesystem without sparse files.
const HEADROOM: u32 = 256;

/// Rows the primary writes before the backup. Enough that the page they share carries an LSN far
/// above the first record a fresh log issues, and few enough that they share one page with room left,
/// so the restored database's next INSERT lands on that same page.
const ROWS: i64 = 20;

/// A generous deadline costs nothing when the binary answers, and a tight one fails on a loaded box.
const DEADLINE: Duration = Duration::from_secs(120);

/// Kills the child when dropped, so a failing assertion never leaves a server or a CLI behind.
struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wal_of(db: &Path) -> PathBuf {
    let mut p = db.as_os_str().to_os_string();
    p.push(".wal");
    PathBuf::from(p)
}

fn lock_of(db: &Path) -> PathBuf {
    let mut p = db.as_os_str().to_os_string();
    p.push(".lock");
    PathBuf::from(p)
}

fn cli(db: &Path) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_ferrodb"));
    c.arg(db)
        .env("FERRODB_ARENA_HEADROOM", HEADROOM.to_string())
        // Neither the lease scan nor the automatic checkpoint may run between the commit and the
        // kill: either one could flush the page and close the window this test aims at.
        .env("FERRODB_LEASE_SCAN_MILLIS", "3600000")
        .env("FERRODB_CHECKPOINT_INTERVAL", "100000")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    c
}

/// One whole CLI session that exits cleanly: its exit status and everything it printed.
fn session(db: &Path, sql: &str) -> (ExitStatus, String) {
    let mut child = cli(db).spawn().expect("spawn ferrodb");
    child.stdin.take().unwrap().write_all(sql.as_bytes()).expect("write sql");
    let out = child.wait_with_output().expect("wait for ferrodb");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status, text)
}

/// The `a | b` rows a SELECT printed, with the REPL's prompts stripped off the front of each line.
fn rows(out: &str) -> Vec<(i64, i64)> {
    let mut found = Vec::new();
    for line in out.lines() {
        let mut l = line;
        while let Some(rest) = l.strip_prefix("ferrodb=> ").or_else(|| l.strip_prefix("     ...? ")) {
            l = rest;
        }
        let cells: Vec<&str> = l.split(" | ").collect();
        if let [a, b] = cells[..] {
            if let (Ok(a), Ok(b)) = (a.trim().parse(), b.trim().parse()) {
                found.push((a, b));
            }
        }
    }
    found
}

/// A primary written by the shipped CLI and closed cleanly, so its pages are flushed with the LSNs
/// its own log issued. Also the control: a second, ordinary open of the same file must work, or a
/// guard that refuses everything would pass every test below.
fn cli_primary(dir: &Path) -> PathBuf {
    let db = dir.join("p.db");
    let mut sql = String::from("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n");
    for i in 1..=ROWS {
        sql.push_str(&format!("INSERT INTO inv VALUES ({i}, {i});\n"));
    }
    let (status, out) = session(&db, &sql);
    assert!(status.success() && !out.contains("error:"), "building the primary failed:\n{out}");

    let (status, out) = session(&db, "SELECT * FROM inv;\n");
    assert!(
        status.success() && !out.contains("error:"),
        "CONTROL: an ordinary reopen of a database the CLI itself wrote failed:\n{out}"
    );
    assert_eq!(rows(&out).len(), ROWS as usize, "CONTROL: the primary does not hold its rows:\n{out}");
    db
}

/// `backup::take` on the primary, then `backup::restore` to `dest`: the file a replica starts from,
/// and the file an operator restoring a backup gets. No `<dest>.wal` exists afterwards.
fn restored_copy(primary: &Path, dir: &Path) -> PathBuf {
    let file = std::fs::OpenOptions::new().read(true).write(true).open(primary).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(wal_of(primary)).unwrap());
    bp.attach_wal(wal.clone());
    let backup_dir = dir.join("backup");
    drop(backup::take(&bp, &wal, &backup_dir).expect("take a base backup"));
    drop(bp);
    drop(wal);

    let dest = dir.join("r.db");
    backup::restore(&backup_dir, &dest).expect("restore the backup");
    assert!(!wal_of(&dest).exists(), "fixture: restore wrote a WAL, so this is not the case under test");

    // The premise, computed from the bytes rather than asked of anything under test: the copy
    // carries heap pages stamped with LSNs, which a log starting at 1 has never issued.
    let bytes = std::fs::read(&dest).unwrap();
    let highest = bytes
        .chunks_exact(PAGE_SIZE)
        .enumerate()
        .filter(|(id, raw)| raw[0] == 0 && raw[1..5] == (*id as u32).to_be_bytes())
        .map(|(_, raw)| Page::deserialize(raw.try_into().unwrap()).unwrap().lsn)
        .max()
        .unwrap_or(0);
    assert!(highest > 0, "fixture: the restored copy carries no page LSN, so nothing can be lost");
    dest
}

/// Lines from a child's pipe, delivered on a channel so the test can wait with a deadline.
fn lines_of(pipe: impl Read + Send + 'static) -> Receiver<String> {
    let (tx, rx) = channel();
    std::thread::spawn(move || {
        for line in BufReader::new(pipe).lines() {
            match line {
                Ok(l) => {
                    if tx.send(l).is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });
    rx
}

/// Wait for a line containing `needle`. `Ok(Some(line))` when it arrives, `Ok(None)` when the pipe
/// closes first (the process is exiting), and a panic naming `what` at the deadline.
fn wait_for(rx: &Receiver<String>, needle: &str, what: &str, seen: &mut Vec<String>) -> Option<String> {
    let until = Instant::now() + DEADLINE;
    loop {
        let left = until.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(line) => {
                seen.push(line.clone());
                if line.contains(needle) {
                    return Some(line);
                }
            }
            Err(RecvTimeoutError::Disconnected) => return None,
            Err(RecvTimeoutError::Timeout) => {
                panic!("no {what} within {DEADLINE:?}; stdout so far:\n{}", seen.join("\n"))
            }
        }
    }
}

/// The refusal, checked by its effects: a non-zero exit, a message naming the missing log, and a
/// directory left exactly as it was — no log created, no lock left behind, the data file untouched.
fn assert_refused_without_trace(db: &Path, before: &[u8], status: ExitStatus, stderr: &str) {
    assert!(!status.success(), "the process exited successfully without opening: {stderr}");
    let wal_name = wal_of(db).file_name().unwrap().to_string_lossy().into_owned();
    assert!(
        stderr.contains(&wal_name),
        "the refusal does not name the missing log {wal_name}: {stderr}"
    );
    assert!(!wal_of(db).exists(), "the refused open created {}", wal_of(db).display());
    assert!(!lock_of(db).exists(), "the refused open left its lock {} behind", lock_of(db).display());
    assert!(
        std::fs::read(db).unwrap() == before,
        "the refused open changed the data file it refused"
    );
}

/// **The red test.** Open the restored copy with the CLI, commit one row on a page that came from
/// the primary, crash, reopen. Before the fix the row is gone: redo compared the new record's LSN,
/// tens of bytes into a fresh log, with the primary's LSN on the page, and skipped it.
///
/// After the fix, the first open is refused. There is no third acceptable outcome: an open that is
/// not refused and happens to keep the row (because something flushed the page first) still fails,
/// under a different message, since the hazard is the open and not this particular crash.
#[test]
fn a_restored_backup_opened_by_the_cli_keeps_every_committed_write_or_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let primary = cli_primary(dir.path());
    let db = restored_copy(&primary, dir.path());
    let before = std::fs::read(&db).unwrap();

    let mut child = Reaped(cli(&db).spawn().expect("spawn ferrodb on the restored copy"));
    let stdout = lines_of(child.0.stdout.take().unwrap());
    let stderr = lines_of(child.0.stderr.take().unwrap());
    let mut seen = Vec::new();

    if wait_for(&stdout, "type .exit", "banner or exit", &mut seen).is_none() {
        let status = child.0.wait().expect("wait for the refused CLI");
        let err: Vec<String> = stderr.iter().collect();
        assert_refused_without_trace(&db, &before, status, &err.join("\n"));
        return;
    }

    // It opened. Commit one row, and die the moment the commit is acknowledged.
    let mut stdin = child.0.stdin.take().unwrap();
    stdin.write_all(b"INSERT INTO inv VALUES (999, 999);\n").expect("send the insert");
    stdin.flush().unwrap();
    if wait_for(&stdout, "row affected", "commit acknowledgement", &mut seen).is_none() {
        let err: Vec<String> = stderr.try_iter().collect();
        panic!("the CLI exited before acknowledging the insert:\n{}\n{}", seen.join("\n"), err.join("\n"));
    }
    child.0.kill().expect("SIGKILL the CLI");
    child.0.wait().unwrap();
    drop(stdin);
    // A SIGKILLed process leaves its lock, and the next open refuses a stale one by design.
    std::fs::remove_file(lock_of(&db)).expect("remove the dead process's lock");

    let (status, out) = session(&db, "SELECT * FROM inv;\n");
    assert!(status.success() && !out.contains("error:"), "the reopen after the crash failed:\n{out}");
    let after = rows(&out);
    for i in 1..=ROWS {
        assert!(
            after.contains(&(i, i)),
            "premise: the primary's row {i} is missing after the reopen, so the table itself is not \
             being read and the check below would mean nothing:\n{out}"
        );
    }
    assert!(
        after.contains(&(999, 999)),
        "COMMITTED ROW LOST: a restored base backup (pages at the primary's LSNs, no WAL) was opened \
         by the CLI as a primary; row (999, 999) was committed and acknowledged, the process was \
         killed, and after recovery the row is gone. Rows after the reopen: {after:?}"
    );
    panic!(
        "the restored copy was opened as a primary and not refused. The committed row survived this \
         crash, but only because its page was flushed first; the open itself is the hazard."
    );
}

/// The same file offered to `pgserver`. Before the fix it printed `LISTENING` and served it; after,
/// it refuses the way the CLI does, and does not leave its lock behind on the way out.
#[test]
fn pgserver_refuses_a_restored_backup_and_leaves_no_trace() {
    let dir = tempfile::tempdir().unwrap();
    let primary = cli_primary(dir.path());
    let db = restored_copy(&primary, dir.path());
    let before = std::fs::read(&db).unwrap();

    let mut child = Reaped(
        Command::new(example_bin("pgserver"))
            .arg(&db)
            .arg("127.0.0.1:0")
            .env("FERRODB_ARENA_HEADROOM", HEADROOM.to_string())
            .env("FERRODB_LEASE_SCAN_MILLIS", "3600000")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn pgserver"),
    );
    let stdout = lines_of(child.0.stdout.take().unwrap());
    let stderr = lines_of(child.0.stderr.take().unwrap());
    let mut seen = Vec::new();

    if let Some(line) = wait_for(&stdout, "LISTENING", "readiness line or exit", &mut seen) {
        panic!(
            "pgserver ACCEPTED a restored base backup (pages at the primary's LSNs, no WAL) as a \
             primary and is serving it: {line}"
        );
    }
    let status = child.0.wait().expect("wait for the refused pgserver");
    let err: Vec<String> = stderr.iter().collect();
    assert_refused_without_trace(&db, &before, status, &err.join("\n"));
}

/// Refuse to run against a stale example binary: `cargo test` does not rebuild examples. The same
/// guard `integration_base_backup.rs` carries, for the same reason.
fn assert_example_is_fresh(bin: &Path) {
    let bin_time = std::fs::metadata(bin)
        .unwrap_or_else(|e| panic!("{} is missing ({e}); run: cargo build --examples", bin.display()))
        .modified()
        .expect("mtime");
    let own_src = bin
        .file_stem()
        .map(|s| Path::new("examples").join(format!("{}.rs", s.to_string_lossy())))
        .and_then(|p| std::fs::metadata(p).ok())
        .and_then(|m| m.modified().ok());
    let newest_source = [walk_newest(Path::new("src")), own_src].into_iter().flatten().max();
    if let Some(src_time) = newest_source {
        assert!(
            bin_time >= src_time,
            "{} is older than src/ or examples/ — cargo test does not rebuild examples, so \
             this would test a stale binary. Run: cargo build --examples",
            bin.display()
        );
    }
}

fn walk_newest(dir: &Path) -> Option<std::time::SystemTime> {
    let mut newest = None;
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let p = e.path();
        // A `src/**/tests_*.rs` file is `#[cfg(test)]` and does not link into an example binary.
        if p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("tests_")) {
            continue;
        }
        let t = if p.is_dir() {
            walk_newest(&p)
        } else {
            std::fs::metadata(&p).ok().and_then(|m| m.modified().ok())
        };
        if let Some(t) = t {
            newest = Some(match newest {
                Some(cur) if cur > t => cur,
                _ => t,
            });
        }
    }
    newest
}

fn example_bin(name: &str) -> PathBuf {
    let mut p = std::env::current_exe().expect("test exe");
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
    let out = p.join("examples").join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert_example_is_fresh(&out);
    out
}
