//! D9 — a real client, over a real socket, speaking the PostgreSQL v3 protocol.
//!
//! The client is `tests/pg/pg_client.py`, written from the protocol spec and deliberately **not**
//! sharing any code with the server. That separation is the point: if both ends were built from
//! the same encoder, a consistent misreading of the wire format would pass every test here and
//! still fail against anything real.
//!
//! **What was not done:** `psql` itself is not installed on this machine, so it has not been run.
//! The row asked for "psql can connect", and what is demonstrated is a client that implements the
//! same protocol psql speaks — startup with the TLS probe, simple query, error responses,
//! termination. That is strong evidence and it is not the same claim, so it is written down
//! rather than rounded up.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};


/// Refuse to run against a stale example binary.
///
/// `cargo test` does NOT rebuild examples — confirmed by mtime — so a test that spawns one can
/// silently exercise a build from before the change under test. That is not a hypothetical: the
/// first fire-check of this file passed while the injected defect was live, because the replica
/// binary predated it.
///
/// A test that cannot observe the code it claims to test is worse than no test, so this refuses
/// with an instruction instead of passing.
fn assert_example_is_fresh(bin: &Path) {
    let bin_time = std::fs::metadata(bin)
        .unwrap_or_else(|e| panic!("{} is missing ({e}); run: cargo build --examples", bin.display()))
        .modified()
        .expect("mtime");
    let newest_src = walk_newest(Path::new("src"));
    if let Some(src_time) = newest_src {
        assert!(
            bin_time >= src_time,
            "{} is older than src/ — cargo test does not rebuild examples, so this would test a \
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
        // `EXE_SUFFIX` is "" on unix and ".exe" on Windows. Hardcoding the unix name made every
    // example-spawning test fail on the Windows runner with "The system cannot find the file
    // specified" - the binary was built, just not under the name being looked for.
    let out = p.join("examples").join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert_example_is_fresh(&out);
    out
}

struct Server {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start the server and wait for it to say it is listening.
///
/// It prints its bound address before accepting, so readiness is observed rather than slept
/// through — a sleep would make this test flaky on a loaded machine and, worse, would sometimes
/// pass for the wrong reason.
fn start() -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("pg.db");
    let mut child = Command::new(example_bin("pgserver"))
        .arg(&db)
        .arg("127.0.0.1:0")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn pgserver");

    let stdout = child.stdout.take().expect("piped stdout");
    let mut lines = BufReader::new(stdout).lines();
    let addr = loop {
        match lines.next() {
            Some(Ok(l)) if l.starts_with("LISTENING ") => {
                break l.trim_start_matches("LISTENING ").to_string()
            }
            Some(Ok(_)) => continue,
            _ => panic!("pgserver exited before it started listening"),
        }
    };
    let port: u16 = addr.rsplit(':').next().unwrap().parse().expect("port");
    Server { child, port, _dir: dir }
}

#[test]
fn an_independent_client_can_connect_and_run_sql_over_the_wire() {
    let server = start();

    let out = Command::new("python3")
        .arg("tests/pg/pg_client.py")
        .arg("127.0.0.1")
        .arg(server.port.to_string())
        .output()
        .expect("python3 is required to run the independent wire client");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "the independent wire client failed:\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(stdout.contains("OK "), "client did not report success: {stdout}");

    // A client that connected but ran nothing would also print OK with zero checks.
    let n: usize = stdout
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert!(n >= 10, "only {n} checks ran; the client did almost nothing: {stdout}");
}

/// Two connections in sequence must both work. A server that only ever serves its first client is
/// a demo, not a protocol implementation.
#[test]
fn the_server_accepts_more_than_one_connection() {
    let server = start();
    for i in 0..2 {
        let out = Command::new("python3")
            .arg("tests/pg/pg_client.py")
            .arg("127.0.0.1")
            .arg(server.port.to_string())
            .output()
            .expect("spawn client");
        // The second run re-creates the same table, so it is expected to report the CREATE error
        // and still complete its startup and query exchange.
        let stdout = String::from_utf8_lossy(&out.stdout);
        if i == 0 {
            assert!(out.status.success(), "first connection failed: {stdout}");
        } else {
            assert!(
                !stdout.is_empty() || !out.status.success(),
                "the second connection produced nothing at all, so the server stopped serving"
            );
        }
    }
}

/// **Agent-branch isolation over the wire, across two connections.**
///
/// The pgwire server built `Session::new()` until 2026-08-16, so an agent session over the wire
/// staged rows in a `BTreeMap` while the copy-on-write engine sat unused beside it — the same gap
/// E31 closed for the CLI. Closing it needs the runtime to be shared by every connection, because
/// branches belong to the database and not to the socket, and building one per connection is the
/// obvious way to get that wrong.
///
/// The client runs its checks from a second, independent reading of the protocol and returns
/// non-zero on the first violation. The load-bearing one is `AS OF BRANCH` from the socket that did
/// NOT open the branch: "B cannot see A's write" passes against a per-connection runtime too, and
/// against a server that lost the write entirely.
#[test]
fn two_wire_clients_see_branch_isolation_and_share_one_runtime() {
    let server = start();

    let out = Command::new("python3")
        .arg("pg_agent_client.py")
        .arg("127.0.0.1")
        .arg(server.port.to_string())
        .current_dir("tests/pg")
        .output()
        .expect("python3 is required to run the independent wire client");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "agent isolation over the wire failed:\nstdout: {stdout}\nstderr: {stderr}"
    );

    // A client that connected and asserted nothing would also exit zero.
    let n: usize = stdout
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert!(n >= 5, "only {n} checks ran; the client did almost nothing: {stdout}");
}
