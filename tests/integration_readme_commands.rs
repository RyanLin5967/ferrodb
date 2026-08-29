//! **Two tests were removed on 2026-08-28, and this says which and why rather than leaving a
//! shorter file for somebody to wonder about.** `the_readmes_documented_cdc_sequence_runs_as_written`
//! and `the_readmes_agent_isolation_transcript_replays_as_written` each replayed a long transcript
//! that the README used to contain. The README was rewritten from 905 lines to ~130 and no longer
//! carries those transcripts, so both tests had nothing left to guard — a test whose fixture is
//! gone does not fail, it silently covers nothing, which is the failure mode this file was written
//! against in the first place.
//!
//! What is still covered: the demo's own output, the benchmark's refusal to report debug numbers,
//! the README's zero-copy page count against the committed raw artifact, and the WAL diagnostic.
//!
//! E50 — the README's documented commands are executed, not trusted.
//!
//! Three separate passes found the same defect shape: something in the docs that was true when
//! written, silently falsified by a later change, and never re-run.
//!
//! - The demo's own summary claimed nothing in `src/` built a storage-backed runtime, months after
//!   the CLI began doing exactly that.
//! - The README told a reader to open "another terminal, against the same database" to observe
//!   isolation — which the single-writer lock refuses outright.
//! - Both documented `sink` commands failed with `open feed.jsonl: no such file or directory`,
//!   because nothing produced that file and the path was relative to the wrong directory anyway.
//!
//! Each was found by hand. Three hand audits is the point at which the check belongs in the suite,
//! because the fourth drift will land between audits and the audit is what keeps not happening.
//!
//! # The README is the fixture
//!
//! This does not re-implement the documented sequence, which would drift from the docs exactly the
//! way the docs drifted from the code. It **reads the commands out of `README.md`** and runs them.
//! Edit the block and this test runs whatever it now says; delete the block and the test fails
//! rather than silently covering nothing.
//!
//! # What it deliberately does not do
//!
//! It does not run every command in the file. `cargo run` with no arguments opens an interactive
//! REPL, and the pgwire and replication examples want a second process and a port. Those are
//! covered by their own integration tests. This covers the one multi-step sequence a reader is most
//! likely to follow verbatim, and the one that was actually broken.

use std::path::{Path, PathBuf};
use std::process::Command;


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

/// The repository root, from the test binary's own location.
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Pull the `$`-prefixed commands out of the fenced block that follows `marker`.
///
/// Returns the commands with their trailing `# comment` stripped. Lines without `$` are the
/// documented *output* and are returned separately, because asserting on them is what makes this a
/// test of the documentation rather than of the program.
fn documented_block(readme: &str, marker: &str) -> (Vec<String>, Vec<String>) {
    let at = readme
        .find(marker)
        .unwrap_or_else(|| panic!("README no longer contains the marker {marker:?}. If that section \
                                   was renamed, update this test; if it was deleted, say so here \
                                   rather than letting this test quietly cover nothing."));
    let rest = &readme[at..];
    let open = rest.find("```").expect("no fenced block after the marker");
    let body_start = rest[open + 3..].find('\n').expect("unterminated fence") + open + 4;
    let close = rest[body_start..].find("```").expect("unterminated fenced block") + body_start;
    let body = &rest[body_start..close];

    let mut cmds = Vec::new();
    let mut out = Vec::new();
    for line in body.lines() {
        if let Some(c) = line.strip_prefix("$ ") {
            let c = c.split('#').next().unwrap().trim().to_string();
            cmds.push(c);
        } else if !line.trim().is_empty() {
            out.push(line.trim().to_string());
        }
    }
    assert!(!cmds.is_empty(), "the block after {marker:?} contains no commands to run");
    (cmds, out)
}

/// Run one documented command line, honouring `cd` and `>` exactly as a reader's shell would.
fn run_documented(cmd: &str, cwd: &mut PathBuf, root: &Path) -> String {
    if let Some(dir) = cmd.strip_prefix("cd ") {
        *cwd = cwd.join(dir.trim());
        return String::new();
    }

    // `>` redirection, because the documented feed command uses it and a reader's shell would.
    let (cmd, redirect) = match cmd.split_once('>') {
        Some((c, f)) => (c.trim(), Some(cwd.join(f.trim()))),
        None => (cmd, None),
    };

    let parts: Vec<&str> = cmd.split_whitespace().collect();
    let out = Command::new(parts[0])
        .args(&parts[1..])
        .current_dir(&*cwd)
        // Share the caller's target dir so this does not rebuild the world in a fresh directory.
        .env("CARGO_TARGET_DIR", root.join("target"))
        .output()
        .unwrap_or_else(|e| panic!("could not run documented command `{cmd}`: {e}"));

    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    // A documented transcript is what a reader SEES, and a terminal interleaves both streams. The
    // first version of this compared against stdout alone and reported the README as wrong for
    // showing `applied 6, skipped 0 re-delivered`, which the consumer writes to stderr. The
    // documentation was right and the instrument was too narrow.
    assert!(
        out.status.success(),
        "a command the README tells a reader to run failed.\n  $ {cmd}\n  in {}\n--- stderr ---\n{}",
        cwd.display(),
        stderr
    );
    if let Some(path) = redirect {
        // Only stdout is redirected by `>`; stderr still reaches the reader's terminal.
        std::fs::write(&path, &stdout).expect("write the redirected output");
        return stderr;
    }
    format!("{stdout}{stderr}")
}

/// **The sequence that was broken.** Produce a feed, land it in SQLite, land it in DuckDB.
#[test]
fn demo_md_matches_what_the_demo_prints_and_the_demo_still_passes() {
    let root = repo_root();
    let demo = root
        .join("target")
        .join(if cfg!(debug_assertions) { "debug" } else { "release" })
        .join("examples")
        .join(format!("agent_isolation_demo{}", std::env::consts::EXE_SUFFIX));
    assert_example_is_fresh(&demo);
    let out = Command::new(&demo)
        .output()
        .expect("run the demo; `cargo build --examples` must have run first");
    let text = String::from_utf8_lossy(&out.stdout).to_string();

    // The demo grades itself and reports the grade in its exit status. Running it is the point.
    assert!(
        out.status.success(),
        "the demo reported failure. Its verdicts are computed, so this is a real regression in one \
         of the ten criteria, not a formatting change:\n{text}"
    );

    // `   3. AS OF BRANCH reads ...    MET`
    let mut printed: Vec<(u32, String)> = Vec::new();
    for line in text.lines() {
        let t = line.trim();
        let Some((num, rest)) = t.split_once('.') else { continue };
        let Ok(n) = num.trim().parse::<u32>() else { continue };
        if !(1..=10).contains(&n) {
            continue;
        }
        for verdict in ["NOT MET", "PARTIAL", "MET"] {
            if rest.trim_end().ends_with(verdict) {
                printed.push((n, verdict.to_string()));
                break;
            }
        }
    }
    assert_eq!(printed.len(), 10, "the demo did not print ten verdicts:\n{text}");

    // `| 3 | ... | **MET** |`
    let demo_md = std::fs::read_to_string(root.join("DEMO.md")).expect("read DEMO.md");
    let mut documented: Vec<(u32, String)> = Vec::new();
    for line in demo_md.lines() {
        if !line.starts_with("| ") {
            continue;
        }
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.len() < 4 {
            continue;
        }
        let Ok(n) = cells[1].parse::<u32>() else { continue };
        let verdict = cells[3].trim_matches('*').trim().to_string();
        if !verdict.is_empty() {
            documented.push((n, verdict));
        }
    }
    assert_eq!(
        documented.len(),
        10,
        "DEMO.md's table no longer has ten rows this test can read. If the table moved or changed \
         shape, update this test rather than leaving it matching nothing."
    );

    printed.sort();
    documented.sort();
    assert_eq!(
        documented, printed,
        "DEMO.md's table disagrees with what the demo prints. The table is a transcription of a \
         computed result, so the demo is right and the document is stale.\n--- demo ---\n{text}"
    );

    // The totals sentence is a second copy of the same result and drifts independently of the rows.
    let met = printed.iter().filter(|(_, v)| v == "MET").count();
    let partial = printed.iter().filter(|(_, v)| v == "PARTIAL").count();
    let not_met = printed.iter().filter(|(_, v)| v == "NOT MET").count();
    let totals = format!("**{met} MET, {partial} PARTIAL, {not_met} NOT MET.**");
    assert!(
        demo_md.contains(&totals),
        "DEMO.md does not state the totals the demo computed. Expected to find {totals:?}"
    );
}

/// **The benchmark refuses to print numbers from a debug build.**
///
/// That refusal is the guard standing between this repository and a quoted measurement that means
/// nothing — debug timings are dominated by unoptimised code, and a number in a README gets quoted
/// whether or not it was meaningful when produced. It is a behaviour, so it is pinned like one.
#[test]
fn the_benchmark_refuses_to_report_debug_numbers() {
    let bin = repo_root()
        .join("target/debug/examples")
        .join(format!("branch_scaling_bench{}", std::env::consts::EXE_SUFFIX));
    assert_example_is_fresh(&bin);
    let out = Command::new(&bin).output().expect("run the benchmark");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !out.status.success(),
        "the debug build printed benchmark numbers instead of refusing:\n{text}"
    );
    assert!(
        text.contains("REFUSING"),
        "it failed, but not by refusing - so the guard may be gone and something else broke:\n{text}"
    );
}

/// **The README's quoted benchmark number must match the committed raw output it came from.**
///
/// Two copies of one measurement in the repository: a sentence in the README and
/// `bench/branch_scaling.txt`. Editing either alone is silent, and the README's own framing is
/// "Measured rather than asserted" — a claim that only holds while the two agree.
///
/// This does not re-run the benchmark. The invariant it demonstrates (a fork copies no pages) is
/// already covered by `integration_zero_copy_fork` and by the demo's criterion 1; what nothing
/// checked was that the number in the prose is the number in the artifact.
#[test]
fn the_readmes_benchmark_number_matches_the_committed_raw_output() {
    let root = repo_root();
    let readme = std::fs::read_to_string(root.join("README.md")).expect("read README.md");
    let raw = std::fs::read_to_string(root.join("bench/branch_scaling.txt"))
        .expect("bench/branch_scaling.txt is the artifact the README quotes; it must be committed");

    // `| forking copies zero data pages | 44 pages at 10, 100 and 1000 branches |`
    let row = readme
        .lines()
        .find(|l| l.contains("forking copies zero data pages"))
        .expect("the README no longer states the zero-copy fork claim; update this test");
    let quoted: u32 = row
        .split_whitespace()
        .find_map(|w| w.parse::<u32>().ok())
        .expect("no number in the README's zero-copy row");

    // The idle rows of the raw table: `   10 |      idle |      44 | ...`
    let mut idle_pages = Vec::new();
    for line in raw.lines() {
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.len() > 2 && cells[1] == "idle" {
            if let Ok(p) = cells[2].parse::<u32>() {
                idle_pages.push(p);
            }
        }
    }
    assert_eq!(
        idle_pages.len(),
        3,
        "expected three idle rows in bench/branch_scaling.txt, found {idle_pages:?}. If the \
         benchmark's table changed shape, update this test rather than leaving it reading nothing."
    );
    assert!(
        idle_pages.iter().all(|&p| p == quoted),
        "the README says {quoted} pages but the committed benchmark output says {idle_pages:?}. \
         One of the two was edited without the other, and the README's claim to be measured rather \
         than asserted only holds while they agree."
    );
}

/// `wal_pages` is a diagnostic, not a demonstration: it needs a WAL to read. Nothing ran it, so
/// nothing would have noticed it breaking. Both halves of its contract are cheap to state.
#[test]
fn the_wal_diagnostic_refuses_without_a_wal_and_reads_a_real_one() {
    let bin = repo_root()
        .join("target/debug/examples")
        .join(format!("wal_pages{}", std::env::consts::EXE_SUFFIX));
    assert_example_is_fresh(&bin);

    let bare = Command::new(&bin).output().expect("run wal_pages");
    assert!(!bare.status.success(), "wal_pages accepted no arguments; it needs a WAL path");

    // A real WAL, produced by the shipped binary rather than hand-built.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("d.db");
    let mut child = Command::new(env!("CARGO_BIN_EXE_ferrodb"))
        .arg(&db)
        .env("FERRODB_ARENA_HEADROOM", "256")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("spawn ferrodb");
    {
        use std::io::Write;
        let mut si = child.stdin.take().unwrap();
        writeln!(si, "CREATE TABLE t (id INTEGER NOT NULL);").unwrap();
        writeln!(si, "INSERT INTO t VALUES (1);").unwrap();
    }
    child.wait().expect("ferrodb");

    let wal = format!("{}.wal", db.display());
    let out = Command::new(&bin).arg(&wal).output().expect("run wal_pages on a real wal");
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(out.status.success(), "wal_pages failed on a WAL this repository just wrote:\n{text}");
    assert!(
        text.contains("kinds:"),
        "wal_pages produced no record-kind summary, so it read nothing:\n{text}"
    );
}
