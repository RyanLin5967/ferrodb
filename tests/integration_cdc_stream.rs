//! E11 — following a live CDC source over TCP, from a separate process.
//!
//! The unit tests in `src/replication/stream.rs` drive `pump` directly. This connects a socket to a
//! server that is writing while it serves, which is the only way to exercise the case the cursor
//! rule exists for: a transaction in flight at the moment a pump runs.
//!
//! The second test is the one that matters for a consumer. It disconnects part-way, reconnects with
//! the cursor it recorded, and checks that the two halves join up **exactly** — no gap, which would
//! be silent data loss, and no overlap, which would be a duplicate delivery the consumer has to
//! deduplicate itself.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

/// Refuse to run against a stale example binary: `cargo test` does not rebuild examples, so
/// without this a test that spawns one can silently exercise a build from before the change.
fn assert_example_is_fresh(bin: &Path) {
    let bin_time = std::fs::metadata(bin)
        .unwrap_or_else(|e| panic!("{} is missing ({e}); run: cargo build --examples", bin.display()))
        .modified()
        .expect("mtime");
    if let Some(src_time) = walk_newest(Path::new("src")) {
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
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let p = e.path();
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
        // `EXE_SUFFIX` is "" on unix and ".exe" on Windows. Hardcoding the unix name made every
    // example-spawning test fail on the Windows runner with "The system cannot find the file
    // specified" - the binary was built, just not under the name being looked for.
    let out = p.join("examples").join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert_example_is_fresh(&out);
    out
}

struct Server {
    child: Child,
    /// Where this server's stderr landed, so a failure can quote its last words instead of
    /// reporting an unexplained connection error.
    stderr_path: std::path::PathBuf,
    addr: String,
    _dir: tempfile::TempDir,
}

impl Server {
    /// Whatever the server wrote to stderr. Empty when it wrote nothing, which is
    /// itself information: it means the process did not report why it stopped.
    #[allow(dead_code)]
    fn stderr(&self) -> String {
        std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(rows: u32) -> Server {
    let dir = tempfile::tempdir().unwrap();
    let stderr_path = dir.path().join("cdc.stderr");
    let mut child = Command::new(example_bin("cdc_server"))
        .arg(dir.path().join("cdc.db"))
        .arg("127.0.0.1:0")
        .arg(rows.to_string())
        .stdout(Stdio::piped())
        // Captured to a FILE, not discarded and not a pipe.
        //
        // Discarded is how a server-side panic reaches a reader as nothing but `connection
        // refused`: on 2026-08-16 that cost two passes hypothesising a dial race that did not
        // exist, while the process's own `failed printing to stdout: Broken pipe` sat in
        // /dev/null. A pipe would be worse than a file — nothing reads it until the child is
        // over, and an unread pipe fills its buffer and blocks the writer.
        .stderr(Stdio::from(
            std::fs::File::create(&stderr_path).expect("create stderr sink"),
        ))
        .spawn()
        .expect("spawn cdc_server");
    let stdout = child.stdout.take().expect("piped");
    let mut lines = BufReader::new(stdout).lines();
    let addr = loop {
        match lines.next() {
            Some(Ok(l)) if l.starts_with("LISTENING ") => {
                break l.trim_start_matches("LISTENING ").to_string()
            }
            Some(Ok(_)) => continue,
            _ => panic!(
                // The whole reason stderr is captured: this used to be the entire failure
                // report, and it says everything except why.
                "{} exited before it started listening. Its stderr:\n{}",
                "cdc_server",
                std::fs::read_to_string(&stderr_path).unwrap_or_default()
            ),
        }
    };
    Server { child, addr, stderr_path, _dir: dir }
}

/// Connect from `cursor` and read at most `limit` lines. Returns them.
fn follow(addr: &str, cursor: u64, limit: usize) -> Vec<String> {
    let mut stream = TcpStream::connect(addr).expect("connect");
    writeln!(stream, "{cursor}").expect("send cursor");
    stream.flush().unwrap();
    let reader = BufReader::new(stream);
    let mut out = Vec::new();
    for line in reader.lines() {
        match line {
            Ok(l) if !l.is_empty() => {
                out.push(l);
                if out.len() >= limit {
                    break;
                }
            }
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    out
}

/// Pull one integer field out of a JSON line without a JSON parser. Adequate here because the
/// feed's own format is pinned by `tests/integration_cdc_feed.rs` against a real parser; this only
/// needs the numbers.
fn field(line: &str, key: &str) -> u64 {
    let pat = format!("\"{key}\":");
    let i = line.find(&pat).unwrap_or_else(|| panic!("no {key} in {line}"));
    let rest = &line[i + pat.len()..];
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    rest[..end].parse().unwrap_or_else(|_| panic!("{key} is not a number in {line}"))
}

#[test]
fn a_consumer_follows_a_live_database_over_tcp() {
    let server = start(20);
    let lines = follow(&server.addr, 0, 20);

    assert!(lines.len() >= 20, "only {} events arrived from a 20-row workload", lines.len());
    for l in &lines {
        assert!(l.starts_with('{') && l.ends_with('}'), "not a JSON object: {l}");
        assert!(l.contains("\"table\":\"inventory\""), "wrong table: {l}");
    }

    // Commit order, checked from the wire rather than from the server's claim to provide it.
    let mut last = 0;
    for l in &lines {
        let c = field(l, "commit_lsn");
        assert!(c >= last, "commit_lsn went backwards on the wire: {c} after {last}");
        last = c;
    }
}

/// **Disconnect and resume.** The two halves must join exactly: no gap and no overlap.
#[test]
fn a_consumer_that_disconnects_resumes_without_gap_or_overlap() {
    let server = start(30);

    // First half: take some events, then hang up.
    let first = follow(&server.addr, 0, 10);
    assert!(first.len() >= 10, "only {} events in the first half", first.len());
    let resume_at = field(first.last().unwrap(), "commit_end_lsn");

    // Reconnect where we left off.
    let second = follow(&server.addr, resume_at, 15);
    assert!(!second.is_empty(), "resuming produced nothing at all");

    // No overlap: nothing in the second half may be at or before the last commit of the first.
    let last_commit_of_first = field(first.last().unwrap(), "commit_lsn");
    for l in &second {
        assert!(
            field(l, "commit_lsn") > last_commit_of_first,
            "resuming re-delivered a commit the consumer had already processed: {l}"
        );
    }

    // No gap: the ids seen across both halves must be contiguous from 1. A skipped transaction
    // would show up as a missing id, which is exactly the failure the cursor rule prevents and
    // exactly what a consumer could never detect on its own.
    let mut ids: Vec<u64> = first
        .iter()
        .chain(second.iter())
        .filter(|l| l.contains("\"op\":\"INSERT\""))
        .map(|l| field(l, "id"))
        .collect();
    ids.sort_unstable();
    ids.dedup();
    assert!(ids.len() >= 15, "too few distinct inserts to judge contiguity: {ids:?}");
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(
            *id,
            i as u64 + 1,
            "a gap in the feed: expected id {} but saw {id}. Ids seen: {ids:?}",
            i + 1
        );
    }
}
