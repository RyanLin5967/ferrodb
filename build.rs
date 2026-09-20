//! Stamp the commit the binary was built from into the binary itself.
//!
//! # Why this exists
//!
//! A measurement is a claim about a particular tree, and on 2026-09-20 one in `bench/` was banked
//! against the wrong one: `d69_wal_per_merge.txt` was run in a worktree seven commits behind the
//! branch it was reported against, so it measured a merge whose scans had not yet been removed and
//! was written up as evidence about the code that removed them. Nothing caught it, because nothing
//! could: **120 of the 151 files in `bench/` name no commit at all.** Provenance was a thing each
//! author happened to remember, and the guard rule for this project is that a dangerous state
//! should be unrepresentable rather than documented.
//!
//! So the commit is compiled IN. A harness cannot forget to record it, and cannot record a
//! different one than it ran, because the value travels inside the binary that did the running.
//!
//! # Why `dirty` is not optional
//!
//! A commit id describes uncommitted code incorrectly, and that is the more dangerous half: a
//! clean-looking sha next to a number invites exactly the trust that a half-applied edit does not
//! deserve. `FERRODB_BUILD_DIRTY` is therefore always emitted, and [`ferrodb::build_provenance`]
//! renders it as a loud suffix rather than a flag someone has to look for.
//!
//! # What it cannot do
//!
//! * It says what the tree was at BUILD time, not at RUN time. A run that outlives an edit is
//!   still mis-attributed — `verify-suite.sh` is what compares the tree before and after a run,
//!   and this does not replace it.
//! * It cannot see a dirty SUBMODULE or an untracked file that is not on the build path.
//! * If `git` is absent or this is not a repository it emits `unknown`, which reads as loudly as
//!   it should. It does NOT fail the build: refusing to compile ferrodb because git is missing
//!   would be a worse failure than an honest `unknown`.

use std::process::Command;

fn main() {
    // Re-stamp when the checked-out commit moves. Without these, cargo caches the stamp and a
    // rebuilt binary keeps reporting the commit it was FIRST built at — a stale stamp is worse
    // than none, because it is confidently wrong.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");

    let git = |args: &[&str]| -> Option<String> {
        let out = Command::new("git").args(args).output().ok()?;
        if !out.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
    };

    let commit = git(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".into());

    // `--porcelain` over tracked files only. An untracked file is not what this is about: it
    // cannot change the compiled behaviour unless it is also on the build path, and treating every
    // stray scratch file as "dirty" would make the flag mean nothing within a day.
    let dirty = match git(&["status", "--porcelain", "--untracked-files=no"]) {
        Some(s) => !s.is_empty(),
        // Could not ask. "clean" would be a guess in the direction that invites trust, so it is
        // reported as unknown-and-therefore-suspect instead.
        None => true,
    };

    println!("cargo:rustc-env=FERRODB_BUILD_COMMIT={commit}");
    println!("cargo:rustc-env=FERRODB_BUILD_DIRTY={}", if dirty { "1" } else { "0" });
}
