//! E44 — no server dies because nobody is reading its stdout.
//!
//! Three example servers print a readiness line that a test harness reads to learn their address:
//! `cdc_server`, `pgserver` and `repl_primary`. Every harness that does so builds its reader as a
//! local, so the read end of the pipe closes as soon as the helper returns — and `println!` panics
//! on EPIPE, taking the process with it.
//!
//! That is not a hypothetical. It produced an intermittent CI failure on ubuntu and windows that
//! macOS never showed, and it was pinned down only because a *different* guard reported the server's
//! exit status instead of an unexplained connection refusal: `unix_wait_status(25856)`, and
//! 25856 >> 8 = 101, a Rust panic. Reproducing it deterministically gave the message itself —
//! `failed printing to stdout: Broken pipe (os error 32)`.
//!
//! `cdc_server` was fixed when that was diagnosed. The other two were not checked, and
//! `repl_primary` is the worse case: it writes six lines after `LISTENING`, so a harness stopping at
//! the first leaves five more writes to fail. This file covers the class rather than the instance.
//!
//! # Why the race is not what is tested
//!
//! The real window is microseconds wide, which is why macOS kept winning it and why running the
//! affected test 25 times locally proved nothing. So this does not race: it closes the pipe
//! *before* the server's first write, which makes the failure deterministic. Against the old code
//! every one of these dies every time.

use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};

/// Cargo's own path for an example binary, with the `.exe` suffix Windows needs.
///
/// Hand-built `target/debug/examples/<name>` paths have been wrong here nine times over in one
/// file, always on Windows and always for the same reason.
fn example_bin(name: &str) -> PathBuf {
    let mut p = std::env::current_exe().expect("test binary path");
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
    p.push("examples");
    p.push(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert!(p.exists(), "{} was not built; run cargo build --examples", p.display());
    p
}

/// Spawn `name`, close its stdout before it writes anything, and require it not to die of EPIPE.
///
/// The assertion is deliberately about *how* it died rather than *whether* it is alive: some of
/// these servers finish their workload and exit cleanly, and an exit is only a failure here when it
/// carries a panic. Checking liveness instead would make this test about timing again.
fn survives_a_closed_stdout(name: &str, args: &[String]) {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(example_bin(name))
        .args(args)
        .current_dir(dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {name}: {e}"));

    // Before its first write. This is the whole point — racing it would test the scheduler.
    drop(child.stdout.take().expect("piped stdout"));

    // **Poll to a generous deadline instead of sleeping a fixed 1.5s.** The first version of this
    // slept 1500ms and then asked once, and it PASSED for `pgserver` even with the panicking
    // `println!` restored — pgserver takes a lock, opens the file, recovers and opens the catalog
    // before it prints, so 1.5s expired before it reached the write and the test saw a healthy
    // process that had not yet had the chance to die. A detector that reports "nothing bad
    // happened" because it looked too early is worse than no detector.
    //
    // Measured: a vulnerable server dies within about a second of reaching its first write, so ten
    // is ample rather than arbitrary.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut status = None;
    while std::time::Instant::now() < deadline {
        status = child.try_wait().expect("query the child");
        if status.is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Anti-vacuity: the server must have done real work before being judged. Every one of these
    // creates its database file before it prints, so a missing file means the process never got
    // near the write and this test proved nothing about it.
    assert!(
        std::fs::read_dir(dir.path()).unwrap().count() > 0,
        "{name} created nothing in its working directory, so it never reached the point where a \
         closed stdout could matter and this test is vacuous"
    );
    let mut err = String::new();
    if let Some(mut e) = child.stderr.take() {
        // The child may still be running and holding the pipe open, so this must not block
        // forever. Killing first makes the read finite.
        if status.is_none() {
            let _ = child.kill();
        }
        let _ = e.read_to_string(&mut err);
    }
    let _ = child.wait();

    assert!(
        !err.contains("Broken pipe"),
        "{name} reported a broken pipe, so a closed log pipe is still reaching its writes:\n{err}"
    );
    assert!(
        !err.contains("failed printing to stdout"),
        "{name} panicked printing to stdout. A closed log pipe must not be fatal:\n{err}"
    );
    if let Some(st) = status {
        assert_ne!(
            st.code(),
            Some(101),
            "{name} exited 101 - a Rust panic - after its stdout was closed:\n{err}"
        );
    }
}

#[test]
fn cdc_server_survives_a_closed_stdout() {
    survives_a_closed_stdout(
        "cdc_server",
        &["cdc.db".into(), "127.0.0.1:0".into(), "12".into()],
    );
}

#[test]
fn pgserver_survives_a_closed_stdout() {
    survives_a_closed_stdout("pgserver", &["pg.db".into(), "127.0.0.1:0".into()]);
}

/// The widest exposure of the three: six writes after the line a harness stops at.
#[test]
fn repl_primary_survives_a_closed_stdout() {
    survives_a_closed_stdout(
        "repl_primary",
        &["primary.db".into(), "127.0.0.1:0".into(), "10".into()],
    );
}
