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
        // `EXE_SUFFIX` is "" on unix and ".exe" on Windows. Hardcoding the unix name made every
    // example-spawning test fail on the Windows runner with "The system cannot find the file
    // specified" - the binary was built, just not under the name being looked for.
    let out = p.join("examples").join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
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
    stderr_path: std::path::PathBuf,
    /// Held for the server's whole life, and that is the point rather than an accident.
    ///
    /// This used to be a local in `start()`, so the read end of the pipe closed the moment `start`
    /// returned. If the server's next `println!` landed after that it died of EPIPE — exit 101,
    /// reproduced deterministically — which is what made this test fail on ubuntu and windows while
    /// macOS won the race. Keeping the reader alive removes the window; the server no longer
    /// panics either, so both ends are fixed rather than one relying on the other.
    _stdout: BufReader<std::process::ChildStdout>,
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
        // Captured to a file, not discarded. On 2026-08-16 this server was proven to die with exit
        // 101 - a panic - and because stderr went to /dev/null the panic's own message was gone.
        // A file rather than a pipe: nothing reads it until the process is over, and an unread pipe
        // fills its buffer and blocks the writer, which would turn a diagnostic into a deadlock.
        .stderr(Stdio::from(
            std::fs::File::create(dir.path().join("server.stderr")).expect("create stderr sink"),
        ))
        .spawn()
        .expect("spawn cdc_server");
    let stdout = child.stdout.take().expect("piped");
    let mut reader = BufReader::new(stdout);
    let addr = loop {
        let mut line = String::new();
        let got = reader.read_line(&mut line);
        match got.map(|n| (n, line.trim_end().to_string())) {
            Ok((n, l)) if n > 0 && l.starts_with("LISTENING ") => {
                break l.trim_start_matches("LISTENING ").to_string()
            }
            Ok((n, _)) if n > 0 => continue,
            _ => panic!("cdc_server exited before it started listening"),
        }
    };
    let stderr_path = dir.path().join("server.stderr");
    Server { child, addr, stderr_path, _stdout: reader, _dir: dir }
}

/// Run the Go consumer once, returning its output and exit status.
fn run_consumer(addr: &str) -> (bool, String, String) {
    let out = Command::new(go_bin())
        // `cdc-consumer` has its own go.mod and the repo root is not a Go module.
        .current_dir("cdc-consumer")
        .args(["run", ".", "follow", addr, "-key", "id"])
        .output()
        .expect("failed to run the Go CDC consumer");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// Run the Go consumer against the server until it closes. Returns the `TABLE ...` line.
///
/// # Why this retries exactly once, and what it refuses to retry
///
/// On 2026-08-16 this failed on windows-latest alone with `dial tcp 127.0.0.1:65402: connectex: No
/// connection could be made because the target machine actively refused it`, and passed on a re-run
/// — eight consecutive green runs on either side of it. `start` already waits for the server to
/// print `LISTENING`, so the port is bound before the consumer is spawned, and `go run` compiles
/// before it dials, which makes the gap between the two large and variable on that runner.
///
/// A connect failure has two very different causes and they must not be conflated:
///
/// - **the server is gone** — it died between binding and being dialled. Retrying is wrong: the
///   next attempt fails the same way and the test spends twice as long saying so, and if it somehow
///   passed it would be hiding a crash. So this case is failed IMMEDIATELY, and the panic reports
///   the exit status, which is the fact the next occurrence needs and the original failure did not
///   record.
/// - **the server is alive and listening** — a transient dial failure. That is the case worth one
///   more attempt, and the retry is announced on stderr so a green run still leaves a trace that it
///   was needed rather than swallowing it.
fn materialise(server: &mut Server) -> String {
    let addr = server.addr.clone();
    let (mut ok, mut stdout, mut stderr) = run_consumer(&addr);

    if !ok {
        // Ask the one question the original failure could not answer.
        let status = server.child.try_wait().expect("query the server process");
        let server_stderr = std::fs::read_to_string(&server.stderr_path).unwrap_or_default();
        assert!(
            status.is_none(),
            "the Go consumer could not reach the server, and the server had already exited \
             ({status:?}). This is not a dial race — the server died between printing LISTENING \
             and being connected to, and retrying would only hide it.\n\
             --- server stderr ---\n{server_stderr}\n\
             --- consumer stderr ---\n{stderr}"
        );
        eprintln!(
            "NOTE: the Go consumer failed to reach a server that is still alive; retrying once. \
             This is the windows-latest dial race. stderr was: {stderr}"
        );
        let again = run_consumer(&addr);
        ok = again.0;
        stdout = again.1;
        stderr = again.2;
    }

    let stdout = stdout;
    assert!(
        ok,
        "the Go consumer failed twice against a live server:\nstderr: {stderr}\nstdout: {stdout}"
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
    let mut server = start(ROWS);
    let table = materialise(&mut server);

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
    let mut server = start(ROWS);
    let table = materialise(&mut server);

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

/// **The retry above must not be able to hide a dead server.** Forces the branch that distinguishes
/// the two causes: kill the server, then ask the consumer to reach it. The failure must name the
/// exit status rather than spending a second `go run` to arrive at the same place.
///
/// Without this, the retry is the kind of accommodation that turns a real crash into a slow, silent
/// one — and the reason the retry exists at all is a failure nobody could diagnose, so the guard
/// that keeps it honest is the part worth pinning.
#[test]
fn a_dead_server_is_reported_as_dead_rather_than_retried() {
    let mut server = start(4);
    server.child.kill().expect("kill the server");
    server.child.wait().expect("reap the server");

    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        materialise(&mut server)
    }));
    let err = panicked.expect_err("a consumer reached a server that had been killed");
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert!(
        msg.contains("had already exited"),
        "the failure did not identify a dead server, so the next occurrence is as undiagnosable \
         as the one that prompted this: {msg}"
    );
}

/// **A server must not die because nobody is reading its stdout.**
///
/// This is the bug behind an intermittent CI failure on ubuntu and windows that macOS never showed.
/// `start()` used to hold its stdout reader in a local, so the read end of the pipe closed the
/// moment `start` returned — and if the server's next `println!` landed after that, it panicked with
/// `failed printing to stdout: Broken pipe (os error 32)` and exit 101. CI reported exactly that
/// status, `unix_wait_status(25856)`, and 25856 >> 8 = 101.
///
/// The race is microseconds wide, so this does not try to hit it. It closes the pipe *before* the
/// server's first write, which makes the failure deterministic: against the old code the server dies
/// every time. Both ends were fixed — the harness now holds the reader open, and the server ignores
/// stdout write errors — and this pins the half that does not depend on the harness behaving.
#[test]
fn the_server_survives_a_consumer_that_stops_reading_its_stdout() {
    use std::io::{Read, Write as _};
    use std::net::{TcpListener, TcpStream};

    // A port to hand the server, so this test never needs to read its stdout for the address.
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();

    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(example_bin("cdc_server"))
        .arg(dir.path().join("cdc.db"))
        .arg(format!("127.0.0.1:{port}"))
        .arg("12")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cdc_server");

    // Close the read end before the server has written anything.
    drop(child.stdout.take().expect("piped"));

    // Give it a moment to reach its first write, then require it to still be serving.
    // A real delay between attempts. The first version of this loop had none, and `connect` to an
    // unbound port fails instantly, so 600 attempts elapsed in 0.02s and the test reported that the
    // server "never accepted a connection" when it simply had not finished starting.
    let mut stream = None;
    for _ in 0..200 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            stream = Some(s);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        if let Some(st) = child.try_wait().expect("query the server") {
            let mut err = String::new();
            let _ = child.stderr.take().unwrap().read_to_string(&mut err);
            panic!(
                "the server died ({st:?}) because its stdout pipe was closed. A closed log pipe \
                 must not be fatal to a server that is otherwise healthy.\nstderr: {err}"
            );
        }
    }
    let mut stream = stream.expect("the server never accepted a connection");

    // Alive is not enough — it has to still deliver a feed.
    stream.write_all(b"0\n").expect("send cursor");
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).expect("read the feed");
    assert!(n > 0, "the server accepted the connection but sent nothing");

    let _ = child.kill();
    let _ = child.wait();
}
