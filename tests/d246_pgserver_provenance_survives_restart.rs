//! **D246 — pgserver's provenance survives a restart, and the log it leaves decodes cold.**
//!
//! `examples/pgserver.rs` never called `with_durable_provenance`; only the CLI did. So pgserver ran
//! on `MemProvenanceStore`, which mints a slot as `runs.len() + 1` and forgets every run at exit. A
//! restarted server therefore hands out slot 1 again, and two things follow:
//!
//! * `who_wrote_row` — `ferro_row_authors` over the wire — answers NOTHING for a row an agent merged
//!   before the restart. Never a wrong actor: the map is simply empty.
//! * pgserver's open runs `recover` and no checkpoint, so the old process's `RunIdentity` record
//!   stays in the log beside the new process's, both under slot 1 for two different actors, and
//!   `LogicalDecoder::decode` refuses any range holding both ("declares provenance slot 1 twice with
//!   different actors"). A cold decode from `base_lsn` can never get past those bytes.
//!
//! This drives the REAL `pgserver` binary over the wire rather than a copy of its open sequence: the
//! defect is that one entry point's construction differs from the other's, and a test that rebuilt
//! the construction in Rust would test the copy. The wire client is `tests/pg/pg_client.py` (via
//! `pg_sql_session.py`), the independent reading of the protocol the other wire tests use.
//!
//! # Pre-registered, from the source at `fbfe038`, before this file was ever run
//!
//! | | at `fbfe038` (no fix) | with the fix |
//! |---|---|---|
//! | premise: first process names `agent-a` for row 1 | holds | holds |
//! | premise: second process names `agent-b` for row 2 | holds | holds |
//! | second process names `agent-a` for row 1 | **fails**: the view has no row 1 | holds |
//! | cold decode `[base_lsn, next_lsn)` | **fails**: slot 1 declared for `agent-a` and `agent-b` | Ok, `agent-a` and `agent-b` under two slots |
//! | the runs are in `<db>.provenance`, the file the CLI opens | **fails**: no such store | holds |
//!
//! The claims are collected before anything is asserted, so a red run shows every symptom.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::Ordering;

use ferrodb::provenance::DurableProvenanceStore;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::wal::log::WalManager;

/// `<db>.<suffix>`, the way pgserver names its side files.
fn side(db: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}.{suffix}", db.display()))
}

/// Refuse to run against a stale example binary: `cargo test` does not rebuild `examples/`, so a
/// test that spawns one can exercise a build from before the change under test. The same guard as
/// `tests/integration_server_reaps.rs`, by the convention that each spawning test carries its own.
fn assert_example_is_fresh(bin: &Path) {
    let bin_time = std::fs::metadata(bin)
        .unwrap_or_else(|e| {
            panic!("{} is missing ({e}); run: cargo build --examples", bin.display())
        })
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
            "{} is older than src/ or examples/ — cargo test does not rebuild examples, so this \
             would test a stale binary. Run: cargo build --examples",
            bin.display()
        );
    }
}

fn walk_newest(dir: &Path) -> Option<std::time::SystemTime> {
    let mut newest = None;
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let p = e.path();
        // A `src/**/tests_*.rs` file does not link into an example binary; see
        // `tests/integration_server_reaps.rs` for the 53 failures counting it caused.
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

/// A running pgserver. Its stderr goes to a FILE, not a pipe: nothing drains a pipe until the child
/// is over, and a full pipe buffer would block the server.
struct Server {
    child: Child,
    port: u16,
    stderr_path: PathBuf,
    db: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(db: &Path) -> Server {
    let stderr_path = side(db, "stderr");
    let mut child = Command::new(example_bin("pgserver"))
        .arg(db)
        .arg("127.0.0.1:0")
        .stdout(Stdio::piped())
        .stderr(Stdio::from(std::fs::File::create(&stderr_path).expect("create stderr sink")))
        .spawn()
        .expect("spawn pgserver");
    let stdout = child.stdout.take().expect("piped stdout");
    let mut lines = BufReader::new(stdout).lines();
    let addr = loop {
        match lines.next() {
            Some(Ok(l)) if l.starts_with("LISTENING ") => {
                break l.trim_start_matches("LISTENING ").to_string();
            }
            Some(Ok(_)) => continue,
            _ => panic!(
                "pgserver exited before it started listening. Its stderr:\n{}",
                std::fs::read_to_string(&stderr_path).unwrap_or_default()
            ),
        }
    };
    let port = addr.rsplit(':').next().and_then(|p| p.parse().ok()).expect("a port");
    Server { child, port, stderr_path, db: db.to_path_buf() }
}

impl Server {
    /// Run `statements` in order on ONE connection and return every `ROW` line's cells. Panics
    /// with the client's output and the server's stderr if any statement errored.
    fn session(&self, statements: &[&str]) -> Vec<Vec<String>> {
        let out = Command::new("python3")
            .arg("pg_sql_session.py")
            .arg("127.0.0.1")
            .arg(self.port.to_string())
            .args(statements)
            .current_dir("tests/pg")
            .output()
            .expect("python3 is required to run the independent wire client");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            out.status.success(),
            "a statement failed over the wire:\nstdout: {stdout}\nstderr: {}\npgserver stderr:\n{}",
            String::from_utf8_lossy(&out.stderr),
            std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
        );
        let done = stdout.lines().filter(|l| l.starts_with("DONE\t")).count();
        assert_eq!(done, statements.len(), "the client did not run every statement: {stdout}");
        stdout
            .lines()
            .filter_map(|l| l.strip_prefix("ROW\t"))
            .map(|l| l.split('\t').map(str::to_string).collect())
            .collect()
    }

    /// `(table_name, row_id) -> agent_id` from `ferro_row_authors`, which answers from the same
    /// provenance store `who_wrote_row` reads.
    fn row_authors(&self) -> BTreeMap<(String, String), String> {
        self.session(&["SELECT * FROM ferro_row_authors;"])
            .into_iter()
            .map(|cells| {
                assert!(cells.len() >= 4, "ferro_row_authors changed shape: {cells:?}");
                ((cells[0].clone(), cells[1].clone()), cells[3].clone())
            })
            .collect()
    }

    /// SIGKILL, as a crash would, and clear the lock file a killed process cannot release (the
    /// same step `tests/integration_server_reaps.rs` takes; `DbLock` refuses rather than guessing
    /// that a holder is dead).
    fn kill_and_unlock(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(side(&self.db, "lock"));
    }
}

fn author(authors: &BTreeMap<(String, String), String>, row: &str) -> Option<String> {
    authors.get(&("t".to_string(), row.to_string())).cloned()
}

#[test]
fn pgserver_provenance_survives_a_restart_and_its_log_decodes_cold() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("d246.db");

    // ---- first process: agent A publishes row 1, and the process dies ----
    let first = start(&db);
    first.session(&[
        "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);",
        "BEGIN AGENT SESSION AS 'agent-a' RUN 'run-a' MODEL 'claude-opus-5/2026-05';",
        "INSERT INTO t VALUES (1, 10);",
        "MERGE;",
    ]);
    let before = first.row_authors();
    assert_eq!(
        author(&before, "1").as_deref(),
        Some("agent-a"),
        "premise: A's merge was not attributed even inside the process that made it: {before:?}"
    );
    first.kill_and_unlock();

    // ---- second process on the same database: agent B publishes row 2 ----
    let second = start(&db);
    second.session(&[
        "BEGIN AGENT SESSION AS 'agent-b' RUN 'run-b' MODEL 'claude-opus-5/2026-05';",
        "INSERT INTO t VALUES (2, 20);",
        "MERGE;",
    ]);
    let after = second.row_authors();
    second.kill_and_unlock();
    assert_eq!(
        author(&after, "2").as_deref(),
        Some("agent-b"),
        "premise: B's merge was not attributed: {after:?}"
    );

    // ---- a cold decode of everything the log still holds ----
    let wal = WalManager::new(side(&db, "wal")).expect("open the log the servers left");
    let (base, next) = (wal.base_lsn.load(Ordering::SeqCst), wal.next_lsn.load(Ordering::SeqCst));
    let decoded = LogicalDecoder::blank().decode(&wal, base, next);

    let mut failures = Vec::new();
    if author(&after, "1").as_deref() != Some("agent-a") {
        failures.push(format!(
            "after the restart, ferro_row_authors does not name agent-a for row 1: {after:?}"
        ));
    }
    match &decoded {
        Err(e) => failures.push(format!("a cold decode of [{base}, {next}) was refused: {e}")),
        Ok(d) => {
            let slots: BTreeMap<String, u32> =
                d.runs.iter().map(|(slot, run)| (run.agent_id.clone(), *slot)).collect();
            match (slots.get("agent-a"), slots.get("agent-b")) {
                (Some(a), Some(b)) if a != b => {}
                _ => failures.push(format!(
                    "premise: the decoded range must declare agent-a and agent-b under two slots, \
                     or it proves nothing about slot reuse; it declared {:?}",
                    d.runs
                )),
            }
        }
    }
    // Where the CLI keeps it, `<db>.provenance`, so either binary reopening this database reads the
    // same attribution. A server that kept its runs in some other file would pass every check above
    // and still leave the CLI blind to them.
    let at_cli_path = DurableProvenanceStore::open(side(&db, "provenance"))
        .map(|s| s.runs().unwrap_or_default().into_iter().map(|r| r.agent_id).collect::<Vec<_>>());
    match &at_cli_path {
        Ok(agents) if agents.iter().any(|a| a == "agent-a") && agents.iter().any(|a| a == "agent-b") => {}
        other => failures.push(format!(
            "the runs are not in {}, the file the CLI opens for this database: {other:?}",
            side(&db, "provenance").display()
        )),
    }
    assert!(
        failures.is_empty(),
        "D246: pgserver's provenance did not survive its restart:\n  {}",
        failures.join("\n  ")
    );
}
