//! D202, crash half — `pgserver` must rebuild its indexes from the heap after recovery.
//!
//! Index pages are not logged. Recovery (`wal::recovery::recover`) redoes and undoes heap records
//! only, so after a crash the trees on disk are whatever the buffer pool last flushed. The CLI
//! answers this by rebuilding every tree from the recovered heap (`cli::run_cli`: `recover` →
//! `rebuild_indexes` → `checkpoint`). `examples/pgserver.rs` calls `recover` and nothing else,
//! and has since D9 (`f741424`). INFERRED from source, never run; this file is the measurement:
//!
//! - a row committed after the last checkpoint is redone into the heap, but its primary entry
//!   lived only in an unflushed index page. The scan finds it; the lookup by key does not;
//! - the uniqueness check asks the same index, so an INSERT of that key is ADMITTED, giving two
//!   live rows under one primary key;
//! - D202's abort-time index undo is in memory and dies with the process. An index page flushed
//!   while its transaction was still open keeps an entry for a row that recovery then undoes.
//!   That is a separate path to the same state, and the same rebuild closes it.
//!
//! The premise that nothing flushed the index page between the INSERT and the kill is controlled
//! rather than assumed. `FERRODB_CHECKPOINT_INTERVAL` is raised far past the one commit this
//! test makes, and CREATE TABLE's own checkpoint flushes the EMPTY tree before the row exists.
//!
//! Pre-registered: FAILS at the tests commit at the lookup by key (`[]` against `[[1, 10]]`).
//! Passes once `pgserver` rebuilds after `recover`.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

/// Refuse to run against a stale example binary: `cargo test` does not rebuild `examples/`.
/// Verbatim from `tests/integration_server_reaps.rs`.
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
    let newest_src = [walk_newest(Path::new("src")), own_src].into_iter().flatten().max();
    if let Some(src_time) = newest_src {
        assert!(
            bin_time >= src_time,
            "{} is older than src/ or examples/ — cargo test does not rebuild examples, so this would test a \
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
        // `src/**/tests_*.rs` is `#[cfg(test)]` and cannot make an example binary stale.
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
                None => t,
                Some(cur) if t > cur => t,
                Some(cur) => cur,
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

fn require_python_module(module: &str) {
    let out = Command::new("python3")
        .arg("-c")
        .arg(format!("import {module}"))
        .output()
        .expect("python3 is required to drive pgserver");
    assert!(
        out.status.success(),
        "`{module}` is not installed, so this test cannot talk to pgserver, and a skipped check would \
         report success for the wrong reason. Install it with: python3 -m pip install --user {module}\n\
         python3 said: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

struct Server {
    child: Child,
    stderr_path: PathBuf,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(db: &Path, tag: &str) -> Server {
    let stderr_path = PathBuf::from(format!("{}.{tag}.stderr", db.display()));
    let mut child = Command::new(example_bin("pgserver"))
        .arg(db)
        .arg("127.0.0.1:0")
        .env("FERRODB_CHECKPOINT_INTERVAL", "1000000")
        .stdout(Stdio::piped())
        .stderr(Stdio::from(std::fs::File::create(&stderr_path).expect("create stderr sink")))
        .spawn()
        .expect("spawn pgserver");
    let stdout = child.stdout.take().expect("piped stdout");
    let mut lines = BufReader::new(stdout).lines();
    let addr = loop {
        match lines.next() {
            Some(Ok(l)) if l.starts_with("LISTENING ") => break l.trim_start_matches("LISTENING ").to_string(),
            Some(Ok(_)) => continue,
            _ => panic!(
                "pgserver exited before it started listening. Its stderr:\n{}",
                std::fs::read_to_string(&stderr_path).unwrap_or_default()
            ),
        }
    };
    let port: u16 = addr.rsplit(':').next().unwrap().parse().expect("port");
    Server { child, stderr_path, port }
}

impl Server {
    /// SIGKILL, so nothing is flushed on the way out, then clear the `DbLock` file a killed process
    /// cannot release (`integration_server_reaps.rs::kill_and_unlock` gives the reason).
    fn crash(mut self, db: &Path) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(format!("{}.lock", db.display()));
    }
}

/// One autocommit connection. Each statement prints one line: `OK <rows>`, `OK -` for a statement
/// with no result set, or `ERR <message>`.
const CLIENT: &str = r#"
import sys, pg8000.dbapi
c = pg8000.dbapi.connect(host="127.0.0.1", port=int(sys.argv[1]), user="ferro", database="ferro")
c.autocommit = True
cur = c.cursor()
for stmt in sys.argv[2:]:
    try:
        cur.execute(stmt)
        if cur.description is None:
            print("OK -")
        else:
            print("OK " + repr([list(r) for r in cur.fetchall()]))
    except Exception as e:
        print("ERR " + str(e).replace("\n", " "))
"#;

fn sql(server: &Server, stmts: &[&str]) -> Vec<String> {
    let out = Command::new("python3")
        .arg("-c")
        .arg(CLIENT)
        .arg(server.port.to_string())
        .args(stmts)
        .output()
        .expect("run the pg8000 client");
    assert!(
        out.status.success(),
        "the client itself failed:\nstdout: {}\nstderr: {}\nserver stderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
        std::fs::read_to_string(&server.stderr_path).unwrap_or_default()
    );
    let lines: Vec<String> = String::from_utf8_lossy(&out.stdout).lines().map(str::to_string).collect();
    assert_eq!(lines.len(), stmts.len(), "one line per statement, got: {lines:?}");
    lines
}

#[test]
fn a_row_committed_before_a_crash_is_found_by_key_and_its_key_stays_taken() {
    require_python_module("pg8000");
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("crash.db");

    let first = start(&db, "first");
    let setup = sql(
        &first,
        &["CREATE TABLE t (id INTEGER NOT NULL, v INTEGER)", "INSERT INTO t VALUES (1, 10)"],
    );
    assert!(setup.iter().all(|l| l.starts_with("OK")), "setup failed: {setup:?}");
    first.crash(&db);

    let second = start(&db, "second");
    let after = sql(
        &second,
        &[
            "SELECT id, v FROM t",
            "SELECT id, v FROM t WHERE id = 1",
            "INSERT INTO t VALUES (1, 99)",
            "SELECT id, v FROM t",
        ],
    );
    // Premise: recovery redid the committed row into the heap, so this test is about the index and
    // not about the WAL.
    assert_eq!(after[0], "OK [[1, 10]]", "the committed row did not survive the crash at all: {after:?}");
    assert_eq!(
        after[1], "OK [[1, 10]]",
        "after the crash, the scan finds row 1 and the lookup by key does not: {after:?}"
    );
    assert!(
        after[2].starts_with("ERR") && after[2].contains("duplicate primary key"),
        "after the crash, a second row 1 was not refused as a duplicate: {after:?}"
    );
    assert_eq!(after[3], "OK [[1, 10]]", "the table holds more than the one row 1: {after:?}");
}
