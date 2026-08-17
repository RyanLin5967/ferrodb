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
fn the_readmes_documented_cdc_sequence_runs_as_written() {
    let root = repo_root();
    let readme = std::fs::read_to_string(root.join("README.md")).expect("read README.md");
    let (cmds, documented_output) = documented_block(&readme, "Produce a feed first");

    // A scratch copy of the two directories the commands touch, so running the docs does not write
    // into the checkout. The commands' relative paths (`../feed.jsonl`) only mean the right thing
    // if the layout is preserved, which is itself part of what is being tested.
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("cdc-consumer")).unwrap();
    for entry in std::fs::read_dir(root.join("cdc-consumer")).unwrap().flatten() {
        let p = entry.path();
        if p.is_file() {
            std::fs::copy(&p, tmp.path().join("cdc-consumer").join(p.file_name().unwrap())).unwrap();
        }
    }
    std::fs::copy(root.join("Cargo.toml"), tmp.path().join("Cargo.toml")).unwrap();

    let mut cwd = tmp.path().to_path_buf();
    let mut transcript = String::new();
    for cmd in &cmds {
        // `cargo run` needs the real manifest; run those from the checkout, writing their output
        // into the scratch tree exactly where the documented path says it goes.
        if cmd.starts_with("cargo ") {
            // `cargo` needs the real manifest, so it runs from the checkout — but its redirected
            // output lands in the scratch tree at exactly the path the documented command names.
            let (c, redirect) = match cmd.split_once('>') {
                Some((c, f)) => (c.trim(), Some(tmp.path().join(f.trim()))),
                None => (cmd.as_str(), None),
            };
            let parts: Vec<&str> = c.split_whitespace().collect();
            let out = Command::new(parts[0])
                .args(&parts[1..])
                .current_dir(&root)
                .output()
                .unwrap_or_else(|e| panic!("could not run `{c}`: {e}"));
            assert!(
                out.status.success(),
                "a command the README tells a reader to run failed.\n  $ {c}\n--- stderr ---\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
            match redirect {
                Some(path) => std::fs::write(&path, &out.stdout).expect("write redirected output"),
                None => transcript.push_str(&String::from_utf8_lossy(&out.stdout)),
            }
            transcript.push_str(&String::from_utf8_lossy(&out.stderr));
            continue;
        }
        transcript.push_str(&run_documented(cmd, &mut cwd, &root));
    }

    // The documented output is part of the documentation. A sequence that runs but prints something
    // else has drifted just as surely as one that fails.
    for line in &documented_output {
        // `<...>` marks a value that legitimately varies — the cursor is a WAL byte offset and
        // moves with whatever the database did before. Pinning it documented noise: the README said
        // CURSOR 1187 and a second run says CURSOR 2406, so a reader matching the number exactly
        // would conclude they had done something wrong. Everything outside the brackets is still
        // matched, in order, so the line is checked rather than waved through.
        let mut rest = transcript.as_str();
        for fragment in line.split(|c| c == '<' || c == '>').step_by(2) {
            if fragment.is_empty() {
                continue;
            }
            match rest.find(fragment) {
                Some(i) => rest = &rest[i + fragment.len()..],
                None => panic!(
                    "the README documents this output line and the commands did not produce it:\n  \
                     {line}\n  (looking for the fragment {fragment:?})\n--- actual ---\n{transcript}"
                ),
            }
        }
    }
    assert!(
        !documented_output.is_empty(),
        "the block documents no output, so this test checked only that the commands exit zero"
    );
}

/// Pull a `ferrodb=> ` REPL transcript out of the fenced block that follows `marker`.
///
/// A different shape from the `$` blocks: `ferrodb=> ` lines are SQL typed at the prompt and
/// everything else is what the prompt printed back.
///
/// The trailing `-- comment` on a documented line is stripped, and that is not cosmetic: the CLI
/// splits input on `;`, so a comment sitting after the semicolon would be prepended to the NEXT
/// statement and break the replay in a way that looks like a database bug.
fn documented_repl(readme: &str, marker: &str) -> (Vec<String>, Vec<String>) {
    let at = readme
        .find(marker)
        .unwrap_or_else(|| panic!("README no longer contains {marker:?}; update or remove this test \
                                   rather than letting it cover nothing"));
    let rest = &readme[at..];
    let open = rest.find("```").expect("no fenced block after the marker");
    let body_start = rest[open + 3..].find('\n').expect("unterminated fence") + open + 4;
    let close = rest[body_start..].find("```").expect("unterminated fenced block") + body_start;

    let mut sql = Vec::new();
    let mut out = Vec::new();
    for line in rest[body_start..close].lines() {
        match line.strip_prefix("ferrodb=> ") {
            Some(stmt) => {
                let stmt = match stmt.find(';') {
                    Some(i) => &stmt[..=i],
                    None => stmt,
                };
                sql.push(stmt.trim().to_string());
            }
            None if !line.trim().is_empty() && !line.starts_with('$') => {
                out.push(line.trim().to_string())
            }
            None => {}
        }
    }
    assert!(!sql.is_empty(), "the block after {marker:?} contains no statements");
    (sql, out)
}

/// **The transcript that carries the project's central claim.** Four blocks, one narrative: an
/// agent's write is visible to itself, invisible to the next session, and lands on trunk only when
/// a session merges it.
///
/// The third block was wrong when this was written, and had been since the prose around it was
/// rewritten: it showed a bare `MERGE;` after the reader had been told to leave *without* merging,
/// which abandons the branch. Run as documented it answered `no agent session in this connection`.
/// Nothing caught it because nothing ran it.
#[test]
fn the_readmes_agent_isolation_transcript_replays_as_written() {
    let root = repo_root();
    let readme = std::fs::read_to_string(root.join("README.md")).expect("read README.md");
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("shop.db");

    // Each block is one CLI session, in order, against the same database — which is the narrative.
    let blocks = [
        // Prose, not the `$` line: that sits INSIDE the fence, so searching from it
        // finds the CLOSING backticks and reads past the block entirely.
        "real transcript, not an illustration",
        "The agent's row is not there",
        "To publish it the agent has to still be in its session",
        "The result survives a restart",
    ];

    for marker in blocks {
        let (sql, expected) = documented_repl(&readme, marker);
        let mut child = Command::new(env!("CARGO_BIN_EXE_ferrodb"))
            .arg(&db)
            // A small arena floor purely so this is cheap on a filesystem without sparse files;
            // the transcript's output does not depend on it.
            .env("FERRODB_ARENA_HEADROOM", "256")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn ferrodb");
        {
            use std::io::Write;
            let mut stdin = child.stdin.take().unwrap();
            for stmt in &sql {
                writeln!(stdin, "{stmt}").expect("write sql");
            }
        }
        let out = child.wait_with_output().expect("wait for ferrodb");
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );

        assert!(
            !text.contains("error:"),
            "a statement in the documented transcript {marker:?} errored:\n{text}"
        );
        let mut rest = text.as_str();
        for line in &expected {
            match rest.find(line.as_str()) {
                Some(i) => rest = &rest[i + line.len()..],
                None => panic!(
                    "the README documents this line in {marker:?} and the session did not print \
                     it, in order:\n  {line}\n--- actual ---\n{text}"
                ),
            }
        }
        assert!(!expected.is_empty(), "block {marker:?} documents no output to check");
    }
}
