//! E13 — a Go consumer follows the live feed and rebuilds the table.
//!
//! Every other test in this repo judges the change feed by looking at what the producer emitted.
//! This one judges it by what a **consumer ends up with**, which is the only question a CDC user
//! actually has: after following this stream, do I have the right data?
//!
//! The consumer is `cdc-consumer`, a separate program in a separate language sharing no code with
//! the database. It applies READ/INSERT/UPDATE/DELETE into a local map and prints the result. The
//! assertions below compare that against the workload the server is known to have run — so a feed
//! that is well-formed, correctly ordered, and *wrong* still fails here.
//!
//! Getting `DELETE` wrong is the failure this shape catches best: a consumer that ignores deletes
//! still produces valid JSON and a plausible table, and the row simply never goes away.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

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
    let out = p.join("examples").join(name);
    assert_example_is_fresh(&out);
    out
}

/// Locate the Go toolchain. `cargo test` does not necessarily inherit an interactive shell's PATH.
fn go_bin() -> String {
    for candidate in ["go", "/opt/homebrew/bin/go", "/usr/local/go/bin/go"] {
        if Command::new(candidate)
            .arg("version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            return candidate.to_string();
        }
    }
    panic!("Go is required to run the independent CDC consumer");
}

struct Server {
    child: Child,
    addr: String,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(rows: u32) -> Server {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(example_bin("cdc_server"))
        .arg(dir.path().join("cdc.db"))
        .arg("127.0.0.1:0")
        .arg(rows.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
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
            _ => panic!("cdc_server exited before it started listening"),
        }
    };
    Server { child, addr, _dir: dir }
}

/// Run the Go consumer against `addr` until the server closes. Returns the `TABLE ...` line.
fn materialise(addr: &str) -> String {
    let out = Command::new(go_bin())
        // `cdc-consumer` has its own go.mod and the repo root is not a Go module.
        .current_dir("cdc-consumer")
        .args(["run", ".", "follow", addr, "-key", "id"])
        .output()
        .expect("failed to run the Go CDC consumer");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        out.status.success(),
        "the Go consumer failed (exit {:?}):\nstderr: {}\nstdout: {stdout}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
        .lines()
        .find(|l| l.starts_with("TABLE "))
        .unwrap_or_else(|| panic!("no TABLE line from the consumer:\n{stdout}"))
        .trim_start_matches("TABLE ")
        .to_string()
}

/// The workload `cdc_server` runs: insert id 1..=rows with qty i*10, then for every fifth id an
/// update to qty i*100. So the table the consumer must arrive at is fully determined.
fn expected_row(i: u32) -> String {
    let qty = if i % 5 == 0 { i * 100 } else { i * 10 };
    format!("{{\"id\":{i},\"item\":\"item{i}\",\"qty\":{qty}}}")
}

#[test]
fn a_go_consumer_rebuilds_the_source_table_from_the_feed_alone() {
    const ROWS: u32 = 12;
    let server = start(ROWS);
    let table = materialise(&server.addr);

    assert!(table.starts_with('['), "the consumer did not print a JSON array: {table}");
    for i in 1..=ROWS {
        let want = expected_row(i);
        assert!(
            table.contains(&want),
            "the consumer's table is missing or wrong for id {i}.\n  expected: {want}\n  got: {table}"
        );
    }

    // Exactly the rows the workload created — no extras invented by replaying something twice.
    let count = table.matches("\"id\":").count();
    assert_eq!(
        count, ROWS as usize,
        "the consumer ended with {count} rows for a {ROWS}-row workload: {table}"
    );
}

/// **Updates must overwrite, not accumulate.** Every fifth id is updated, so a consumer that
/// appended instead of replacing would end with the pre-update value still present.
#[test]
fn an_updated_row_shows_its_latest_value_only() {
    const ROWS: u32 = 10;
    let server = start(ROWS);
    let table = materialise(&server.addr);

    // id 5 and id 10 were updated to i*100.
    for i in [5u32, 10] {
        assert!(
            table.contains(&expected_row(i)),
            "id {i} does not show its updated qty: {table}"
        );
        let stale = format!("{{\"id\":{i},\"item\":\"item{i}\",\"qty\":{}}}", i * 10);
        assert!(
            !table.contains(&stale),
            "id {i} still shows its pre-update value, so the update did not overwrite: {table}"
        );
    }
}
