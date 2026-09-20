//! B9 — the system views read back through a real client over a real socket.
//!
//! The exit criterion is "`SELECT` over each view returns typed rows, read back through a client
//! rather than asserted in Rust only", and this is the "rather than" half. `tests/pg/pg_views_client.py`
//! implements the PostgreSQL v3 protocol from the spec and shares no code with ferrodb's encoder, so
//! a consistent misreading of the wire format cannot pass on both sides.
//!
//! The Rust side deliberately asserts almost nothing about the views themselves. Everything it says
//! is about the *instrument*: the server started, the client ran, and the client ran enough checks to
//! be worth believing. The view assertions are the client's, because a claim about what crosses a
//! socket cannot be made from inside the process that owns one end of it.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

/// Refuse to run against a stale example binary.
///
/// Lifted from `tests/integration_pgwire.rs` for the reason its own comment gives: **`cargo test`
/// does not rebuild examples**, so a test that spawns one can silently exercise a build from before
/// the change under test. That is not hypothetical — the first fire-check of that file passed while
/// the injected defect was live, because the binary predated it. A test that cannot observe the code
/// it claims to test is worse than no test.
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
        // **A `src/**/tests_*.rs` file does not link into an example binary**, so editing one
        // cannot make that binary stale — and `cargo build --examples` correctly does not rebuild
        // for it, because those modules are `#[cfg(test)]` and are not part of the lib's non-test
        // fingerprint. Counting them made this guard fire on a tree whose examples WERE fresh:
        // one edit to `src/consensus/tests_transport.rs` failed 53 tests across 5 targets.
        // The convention is enforced, not assumed — see `test_only_sources_are_cfg_test_gated`.
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

struct Server {
    child: Child,
    stderr_path: PathBuf,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start the server and wait for it to say it is listening — readiness observed, not slept through.
fn start() -> Server {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("views.db");
    let stderr_path = db.with_extension("stderr");
    let mut child = Command::new(example_bin("pgserver"))
        .arg(&db)
        .arg("127.0.0.1:0")
        .stdout(Stdio::piped())
        // To a FILE, not discarded and not a pipe: a discarded server panic reaches a reader as
        // nothing but `connection refused`, and nothing drains a pipe until the child is over, so a
        // full pipe buffer would block the writer.
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
    let port: u16 = addr.rsplit(':').next().unwrap().parse().expect("port");
    Server { child, port, stderr_path, _dir: dir }
}

/// **Every view over the wire, typed, with the empty and populated halves of each.**
///
/// What the client checks, and why each one needs a client:
///
/// * a `RowDescription` per view whose field names and type OIDs match the declaration exactly —
///   before B9 nothing carried a schema past the executor, so pgwire named columns `column1..N`;
/// * `ferro_quarantine` read with **nothing held**, still announcing all four fields with
///   `SELECT 0`, which is the only thing separating "nothing is quarantined" from "this view is
///   broken" as far as a client can see;
/// * a quarantined branch **with its reason**, reached through B1's read-premise check at merge
///   admission across three connections;
/// * `MERGE` arriving as typed columns rather than one `text` column of `format!("{a:?}")`;
/// * the trunk's `u64::MAX` lease surviving as digits rather than as `-1`, and the trunk's absent
///   parent arriving as SQL NULL (protocol length -1) rather than as the string "NULL".
#[test]
fn the_system_views_are_typed_rows_to_an_independent_client() {
    let server = start();

    let out = Command::new("python3")
        .arg("pg_views_client.py")
        .arg("127.0.0.1")
        .arg(server.port.to_string())
        .current_dir("tests/pg")
        .output()
        .expect("python3 is required to run the independent wire client");

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "the system views failed over the wire:\nstdout: {stdout}\nstderr: {stderr}\nserver stderr: {}",
        std::fs::read_to_string(&server.stderr_path).unwrap_or_default()
    );

    // A client that connected and asserted nothing would also exit zero, and a client whose
    // scenario silently stopped early would exit zero with a smaller number. The count is the guard
    // against both, and it is a floor rather than an equality so adding a check does not fail this.
    let n: usize = stdout
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert!(
        n >= 45,
        "only {n} checks ran; the client did not get through its scenario: {stdout}"
    );
}
