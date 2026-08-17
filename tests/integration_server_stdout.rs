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
    assert_example_is_fresh(&p);
    p
}

/// Refuse to test a binary older than the source it was built from.
///
/// `cargo test` does not rebuild examples, so a test that spawns one can silently exercise a
/// previous build. That fooled three fire-checks in a single session; twice it reported a PASS,
/// because a guard deleted from the source was still present in the stale binary. A check that
/// certifies a guard at the moment it stops existing is worse than no check.
///
/// Both `src/` and `examples/` — watching only `src/` is what made those three invisible, since a
/// fire-check on an example touches neither.
fn assert_example_is_fresh(bin: &std::path::Path) {
    let bin_time = std::fs::metadata(bin)
        .unwrap_or_else(|e| panic!("{} is missing ({e}); run: cargo build --examples", bin.display()))
        .modified()
        .expect("mtime");
    // This example's OWN source, plus `src/` because every example links the library. NOT all of
    // `examples/`: that marked every other example's binary stale whenever any one was edited,
    // because `cargo build --examples` only relinks what changed, and a guard that fails on
    // unrelated edits gets switched off.
    let own_src = bin
        .file_stem()
        .map(|s| std::path::Path::new("examples").join(format!("{}.rs", s.to_string_lossy())))
        .and_then(|p| std::fs::metadata(p).ok())
        .and_then(|m| m.modified().ok());
    let newest = [newest_under("src"), own_src].into_iter().flatten().max();
    if let Some(src_time) = newest {
        assert!(
            bin_time >= src_time,
            "{} is older than src/ or examples/ - cargo test does not rebuild examples, so this \
             would test a stale binary. Run: cargo build --examples",
            bin.display()
        );
    }
}

fn newest_under(dir: &str) -> Option<std::time::SystemTime> {
    fn walk(p: &std::path::Path) -> Option<std::time::SystemTime> {
        let mut newest = None;
        for e in std::fs::read_dir(p).ok()?.flatten() {
            let path = e.path();
            let t = if path.is_dir() {
                walk(&path)
            } else {
                std::fs::metadata(&path).ok().and_then(|m| m.modified().ok())
            };
            if t > newest {
                newest = t;
            }
        }
        newest
    }
    walk(std::path::Path::new(dir))
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

/// **E45 — every binary that opens a user-named database refuses one that is already held.**
///
/// The single-writer lock started life on two entry points, which made it a denylist: it caught the
/// two binaries that happened to be open in the editor that day. Measured before this test existed:
/// with a lock file present the CLI was correctly refused while `cdc_server` opened the same
/// database and began serving.
///
/// Listing them here is what turns it back into an allowlist. A new example that takes a database
/// path and forgets the lock does not fail this test — nothing can make it — but the list is the
/// place a reviewer looks, and every name on it is checked rather than assumed.
#[test]
fn every_user_path_binary_refuses_a_database_that_is_already_open() {
    const BINARIES: &[(&str, &[&str])] = &[
        ("cdc_server", &["127.0.0.1:0", "4"]),
        ("repl_primary", &["127.0.0.1:0", "4"]),
        ("repl_replica", &["127.0.0.1:0"]),
        ("cdc_feed", &[]),
        ("cdc_latency", &[]),
        ("crash_mid_merge", &["seed"]),
    ];

    for (name, extra) in BINARIES {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("held.db");
        // A lock left by a process that is not us. The pid is deliberately one nothing owns.
        std::fs::write(format!("{}.lock", db.display()), "999999\n").unwrap();

        let mut args: Vec<String> = vec![db.display().to_string()];
        args.extend(extra.iter().map(|s| s.to_string()));
        // **Bounded, not `.output()`.** Half of these are servers: if one fails to refuse it starts
        // serving and never exits, and `.output()` waits for that forever. The first version of
        // this test did exactly that — removing the lock from `cdc_server` to check the test could
        // fail made the whole run hang instead, which is the one thing a detector must never do.
        // Still running after the deadline IS the failure, and it is reported as one.
        let mut child = Command::new(example_bin(name))
            .args(&args)
            .current_dir(dir.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {name}: {e}"));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut status = None;
        while std::time::Instant::now() < deadline {
            status = child.try_wait().expect("query the child");
            if status.is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        let still_running = status.is_none();
        if still_running {
            let _ = child.kill();
        }
        let mut text = String::new();
        if let Some(mut e) = child.stderr.take() {
            let _ = e.read_to_string(&mut text);
        }
        if let Some(mut o) = child.stdout.take() {
            let _ = o.read_to_string(&mut text);
        }
        let _ = child.wait();

        assert!(
            !still_running,
            "{name} did not refuse a database that is already held - it was still running after \
             10s, which for a server means it opened it and started serving:\n{text}"
        );
        assert!(
            !status.unwrap().success(),
            "{name} opened a database that is already held. Two writers hand the same arena pages \
             to different branches and every one of them still passes its checksum, so nothing \
             downstream can detect it:\n{text}"
        );
        assert!(
            text.contains("already open"),
            "{name} failed, but not for the reason this test is about - it must refuse because the \
             database is held, not because of an unrelated error:\n{text}"
        );
    }
}
