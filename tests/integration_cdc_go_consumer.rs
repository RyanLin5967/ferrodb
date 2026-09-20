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

/// Ask the one question every failure in the test below needs answered first: is the server still
/// alive, and if not, what did it say on the way out?
///
/// The original occurrence of this flake answered neither. It reported "the server never accepted a
/// connection" and nothing else, which is consistent with a dead server, a wedged one, and a port
/// that had been taken by another process — three different bugs — and it took six lanes hitting it
/// to tell them apart. Every panic below routes through here so that never costs that again.
///
/// Kills the server first when it is still running: the read end of a pipe held open by a live
/// child blocks to EOF, and a diagnostic that hangs is worse than no diagnostic.
fn server_epitaph(child: &mut Child) -> String {
    use std::io::Read as _;

    let status = child.try_wait().expect("query the server process");
    if status.is_none() {
        let _ = child.kill();
    }
    // A pipe can only be drained once, so this is single-use per child by construction. Saying so
    // beats printing an empty section, which reads as "the server said nothing on the way out" —
    // the exact wrong conclusion for whoever is reading the second epitaph.
    let err = match child.stderr.take() {
        Some(mut e) => {
            let mut err = String::new();
            let _ = e.read_to_string(&mut err);
            err
        }
        None => "(already drained by an earlier epitaph on this server)".to_string(),
    };
    let _ = child.wait();
    match status {
        Some(st) => format!("The server had ALREADY EXITED ({st:?}).\n--- its stderr ---\n{err}"),
        None => format!("The server was still running when asked.\n--- its stderr ---\n{err}"),
    }
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
///
/// # I16 — the port is the server's, and was never anybody else's
///
/// This test used to open its own `TcpListener` on `127.0.0.1:0` purely to *discover* a free port,
/// drop the listener, and hand the bare number to a server it spawned afterwards. That is a
/// discover-then-race, and the gap is not small: between the drop and the server's own `bind` sit a
/// process spawn, the single-writer lock, opening the database, a buffer pool, a catalog and a
/// `CREATE TABLE`. Anything else on the machine may take the port in that window, after which the
/// server's `bind` fails or it listens somewhere this test is not looking, and the connect loop
/// simply runs out.
///
/// It ran out on six independent lanes. The measurement that settles which mechanism it was:
/// binding took 30ms at the median and 838ms at the worst over 50 spawns at load 91, **with zero
/// failures to bind** — so the server was not merely slow, and the budget being too tight was not
/// the whole story either.
///
/// **The fix is not a retry.** Retrying the connect leaves the port unowned and relabels a stolen
/// port as a slow start, which is the same flake with the evidence removed. Instead the port is
/// never unowned: the server binds `:0` and reports the address it got. Every other harness in this
/// file learns that from the `LISTENING` line, which this test cannot read — closing stdout before
/// the first write is the entire point of it — so the server writes the same address to
/// `FERRODB_LISTEN_FILE`, by atomic rename, before it prints anything.
///
/// What is still being tested is unchanged and not made vacuous by the handshake: the server
/// publishes its address *before* its first stdout write, so arriving at the file proves nothing
/// about EPIPE — but serving a feed afterwards means it went through both writes to a closed pipe
/// and kept running, which is the property.
#[test]
fn the_server_survives_a_consumer_that_stops_reading_its_stdout() {
    use std::io::{Read, Write as _};
    use std::net::TcpStream;

    let dir = tempfile::tempdir().unwrap();
    // The address the server will publish once it has bound it. Inside the tempdir so it is removed
    // with everything else, and so a stale file from a previous run can never be read as this one's.
    let addr_file = dir.path().join("listen.addr");

    let mut child = Command::new(example_bin("cdc_server"))
        .arg(dir.path().join("cdc.db"))
        // `:0` — the server picks, the kernel decides, and it holds the port from that moment until
        // it exits. Nothing here ever owns a port it then gives up.
        .arg("127.0.0.1:0")
        .arg("12")
        .env("FERRODB_LISTEN_FILE", &addr_file)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cdc_server");

    // Close the read end before the server has written anything.
    drop(child.stdout.take().expect("piped"));

    // Wait for the server to say where it is, not for a connection to succeed.
    //
    // **1200 x 50ms = 60s, and the number is measured rather than picked.** The other three tests
    // in this binary shell out to `go run .`, which COMPILES AND STATICALLY LINKS DuckDB on any run
    // whose Go build cache is cold for that package — adding one file under `cdc-consumer/` is
    // enough to make it cold, and content-identical files stay warm, so it happens exactly once per
    // change and then never again. They run in parallel with this test, which does not use Go at
    // all; it just competes with them for the machine.
    //
    // Measured on this machine (18 cores) by forcing a genuine relink five times, this binary took
    // 7.5s, 8.9s, 12.3s, 20.0s and 27.5s wall-clock. The previous budget was 10s — inside that
    // spread rather than outside it — and it failed exactly once, on the first run after a new Go
    // file was added, with `the server never accepted a connection` and the server still alive.
    //
    // The budget is a liveness bound and not an assertion: the loop exits the instant the address
    // appears, so a healthy run costs what it always did, and a server that DIES still fails
    // immediately through the `try_wait` branch below rather than waiting the timeout out. Raising
    // it weakens nothing that is being tested — the property is "a closed stdout pipe is not fatal",
    // and every check of that is below.
    //
    // `read_to_string` is safe against a torn read because the server renames the file into place;
    // the emptiness check is belt and braces for a platform where that is less atomic than it looks.
    let mut published = None;
    for _ in 0..1200 {
        if let Ok(text) = std::fs::read_to_string(&addr_file) {
            let text = text.trim().to_string();
            if !text.is_empty() {
                published = Some(text);
                break;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        if let Some(st) = child.try_wait().expect("query the server") {
            panic!(
                "the server died ({st:?}) because its stdout pipe was closed. A closed log pipe \
                 must not be fatal to a server that is otherwise healthy. {}",
                server_epitaph(&mut child)
            );
        }
    }
    let addr = published.unwrap_or_else(|| {
        panic!(
            "the server never published a listening address within 60s, and it was alive every \
             time it was asked. That is not the failure this test is about — a server killed by a \
             closed stdout pipe is caught by the `try_wait` branch above — so it is wedged before \
             or inside its own `bind`. {}",
            server_epitaph(&mut child)
        )
    });

    // Anti-vacuity. `127.0.0.1:0` is what was ASKED for, so reading it back would mean the server
    // published the request rather than the result, and every connect below would go nowhere.
    let port: u16 = addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .unwrap_or_else(|| panic!("the server published an address this test cannot parse: {addr:?}"));
    assert_ne!(
        port, 0,
        "the server published {addr:?} — the wildcard it was asked for, not the port it was given, \
         so nothing below would be testing a real connection"
    );

    // **One connect, no retry loop, and that is the point of I16.** The port was bound before the
    // address was published and stays bound for the life of the process, so there is no window for
    // anything to take it and nothing here to retry. A failure at this line is a fact about the
    // server, and reporting it as one is what a retry loop would have thrown away.
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap_or_else(|e| {
        panic!(
            "the server published {addr:?} and then refused a connection to it ({e}). The port is \
             the server's own and was never released, so this is not a port race. {}",
            server_epitaph(&mut child)
        )
    });

    // The last unbounded wait in this test, made bounded. `read` on a healthy server returns
    // immediately — it starts writing as soon as it has accepted — so the only thing this changes is
    // that a server which accepts and then wedges is REPORTED after a minute instead of hanging the
    // run until CI's own timeout kills it and says nothing about why. Sixty seconds for the same
    // reason the loop above uses sixty: it is a liveness bound on a machine that can be very busy,
    // not an assertion about how fast the feed should be.
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(60)))
        .expect("set a read timeout");

    // Alive is not enough — it has to still deliver a feed.
    stream.write_all(b"0\n").unwrap_or_else(|e| {
        panic!("the cursor could not be sent to the server ({e}). {}", server_epitaph(&mut child))
    });
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).unwrap_or_else(|e| {
        panic!("the feed could not be read ({e}). {}", server_epitaph(&mut child))
    });
    assert!(
        n > 0,
        "the server accepted the connection but sent nothing, which is what a server that died \
         between accepting and writing looks like. {}",
        server_epitaph(&mut child)
    );

    let _ = child.kill();
    let _ = child.wait();
}
