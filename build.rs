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
//! # When the stamp is taken, and therefore what it can see
//!
//! The stamp is two facts read from git **when this script runs**: the commit `HEAD` resolves to,
//! and whether any tracked file differs from it, staged or not. Cargo re-runs the script only when
//! something it was told to watch has changed, so the stamp is exactly as fresh as the watch list,
//! and the list is the whole of the mechanism:
//!
//! * **The build's inputs**: `src/`, `examples/`, `tests/` and `benches/` (each scanned whole, and
//!   each only if it exists), `build.rs`, `Cargo.toml` and `Cargo.lock`. An edit to any of them is
//!   an edit to what is being compiled, so it re-stamps. This is what makes an unstaged edit say
//!   `+DIRTY`.
//! * **The git state**: this worktree's `HEAD` and index, and the checked-out branch's ref file
//!   (or, on a repository that keeps its refs in reftable, its `reftable/` directories). Each is
//!   resolved by `git rev-parse` (`--git-path`, and `--git-common-dir` for the shared reftable),
//!   because in a linked worktree `HEAD` and the index live in the worktree's own git directory
//!   and the refs in the shared one. A commit moves the branch's ref file and leaves `HEAD`
//!   alone; `git reset --soft` moves it and leaves the index alone too.
//!
//!   `packed-refs` is deliberately NOT watched. While the branch's ref file exists it shadows the
//!   packed value, so a change to `packed-refs` cannot change what `HEAD` resolves to; while the
//!   file does not exist, watching it already re-runs this script on every build (below).
//!   Watching `packed-refs` as well would add no information, and would rebuild every worktree
//!   of the repository whenever that one shared file is rewritten: a packed branch deleted
//!   anywhere (measured by D231's review), `git gc`, `git fetch --prune`.
//!
//! **D231.** The list used to be `.git/HEAD` and `.git/index`, relative to the package. In a main
//! checkout that missed unstaged edits, so a binary said clean for code it was not built from,
//! and branch moves that did not also rewrite the index (`git reset --soft` does not), except
//! when some other change happened to re-run the script first.
//! In a linked worktree `.git` is a file and both paths were missing, and the Cargo FAQ says a
//! missing watched path re-runs the script, and rebuilds the crate, on every build: the stamp came
//! out right there, at the price of a rebuild per cargo invocation. `bench/d231_stamp_check.sh`
//! measures all three at the base and at the fix; its output, not this paragraph, is the
//! evidence.
//!
//! # What it cannot do
//!
//! * It says what the tree was at BUILD time, not at RUN time. A run that outlives an edit is
//!   still mis-attributed — `verify-suite.sh` is what compares the tree before and after a run,
//!   and this does not replace it.
//! * **`+DIRTY` describes the tree as it was at the last re-stamp.** An unstaged edit to a tracked
//!   file that is not a build input (`bench/`, `tools/`, a document) is seen only when something
//!   on the watch list also changes. So a clean stamp means the build's inputs have not changed
//!   since a clean re-stamp, not that nothing in the repository has. Anything the crate compiles
//!   in from outside those paths, such as an `include_str!` of a file elsewhere, must be added to
//!   `INPUTS` or it is invisible here.
//! * Cargo detects a change by mtime alone, against the time this script last ran. A change that
//!   leaves a file's mtime no newer than that is invisible: an edit that preserves the mtime, or
//!   an older copy put back with `cp -p` or `rsync -a`.
//! * It cannot see a dirty SUBMODULE or an untracked file that is not on the build path.
//! * **Every re-stamp rebuilds the crate, and so everything that links it.** The Cargo FAQ lists a
//!   re-running build script as a cause of rebuilds; that the dependents follow is inferred. So an
//!   edit to one test or example rebuilds the library and every target, where cargo alone would
//!   have rebuilt that one target, and so does a file in a watched directory that is never
//!   compiled: `tests/pg/`'s Python clients, or the `__pycache__/` Python writes beside them the
//!   first time they run. That is the price of the stamp living in the library, and it is paid
//!   deliberately: a harness in `examples/` edited and not committed must still say `+DIRTY`.
//! * A watched git path that does not exist makes cargo re-run this script, and therefore rebuild
//!   the crate, on every build until it does exist. The stamp stays right and the rebuild is the
//!   cost. A branch that lives only in `packed-refs` does this until its next commit creates its
//!   ref file, and `git gc` (`pack-refs --all`) makes that true of every branch at once, checked
//!   out or not (measured by D231's review): after a gc, every checkout rebuilds on every build
//!   until its branch moves.
//! * `HEAD` is followed to the branch it names and that branch's file is watched. A symbolic ref
//!   chain (`HEAD` to `foo` to `main`) is followed to its end, so retargeting `foo` changes what
//!   `HEAD` resolves to without touching a watched file (measured by D231's review). Nothing here
//!   builds such a chain.
//! * In a reftable repository the shared table changes on every ref update in every worktree, so
//!   any commit anywhere re-stamps, and rebuilds, every worktree. ferrodb's repository uses the
//!   files store, so this is latent here.
//! * If `git` is absent, or cannot say where this package's repository is (which includes a git
//!   older than 2.31, too old to answer the question the way it is asked), it emits `unknown`,
//!   which reads as loudly as it should. It does NOT fail the build: refusing to compile ferrodb
//!   because git is missing would be a worse failure than an honest `unknown`.
//!
//! It describes the repository at the package root and nothing else: every variable that
//! `git rev-parse --local-env-vars` lists (`GIT_DIR`, `GIT_WORK_TREE`, `GIT_INDEX_FILE`, and the
//! rest) is cleared before git is asked. A git hook exports those for the repository the hook is
//! about, and `tools/prepush.sh` builds the checkout it lives in, which need not be that one.

use std::path::{Path, PathBuf};
use std::process::Command;

/// What the crate is compiled from, relative to the package root. Anything the crate compiles in
/// from elsewhere must be added here, or an edit to it never re-stamps.
///
/// Only the entries that exist are watched: cargo re-runs a script whose watched path is missing
/// on every build (the Cargo FAQ, "Why is Cargo rebuilding my code?"), and this package has no
/// `benches/`. One that appears later is picked up at the next re-stamp, and adding a tracked file
/// to it rewrites the index, which is watched. `build.rs` is listed for completeness: cargo
/// already re-runs a script whose source changed.
const INPUTS: &[&str] =
    &["src", "examples", "tests", "benches", "build.rs", "Cargo.toml", "Cargo.lock"];

fn main() {
    let root = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR")
            .expect("cargo sets CARGO_MANIFEST_DIR for every build script"),
    );
    for input in INPUTS {
        let p = root.join(input);
        if p.exists() {
            watch(&p);
        }
    }

    let local_env = local_git_env();
    let git = |args: &[&str]| -> Option<String> {
        let mut cmd = Command::new("git");
        // `--no-optional-locks`: a plain `git status` refreshes the index and writes it back
        // (git-status(1), BACKGROUND REFRESH). That takes the index lock out from under whoever
        // is committing in this worktree, and it rewrites a file this script watches, so the
        // script would schedule its own re-run.
        //
        // `--no-replace-objects`: the stamped sha names the real commit, so `dirty` must compare
        // against the real commit's tree. With `git replace` honoured, a replacement object
        // changes the comparison without changing any file this script watches (D231 review).
        cmd.args(["--no-optional-locks", "--no-replace-objects"]).args(args).current_dir(&root);
        for var in &local_env {
            cmd.env_remove(var);
        }
        let out = cmd.output().ok()?;
        if !out.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
    };

    // Fail loud. Without a repository there is nothing to watch and nothing to believe.
    let Some(git_paths) = git_state_paths(&git) else {
        stamp("unknown", true);
        return;
    };
    for p in &git_paths {
        watch(p);
    }

    let commit = git(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".into());

    // `--porcelain` over TRACKED files only, and that is a deliberate trade with a real hole in it.
    //
    // ⚠ **AN UNTRACKED FILE ON THE BUILD PATH STAMPS CLEAN, AND THIS WAS DEMONSTRATED, NOT
    // GUESSED.** Proving the flag fires in both directions needed a throwaway `examples/*.rs`; it
    // was untracked, it compiled and ran, and the stamp read clean. So the honest statement is not
    // "untracked files cannot change compiled behaviour" — they plainly can — it is that counting
    // them would mark the tree dirty for every stray scratch file and the flag would mean nothing
    // within a day. A flag nobody believes catches nothing at all.
    //
    // The hole that remains is narrow and worth naming precisely: a NEW file that is on the build
    // path and has never been committed. An edit to any file the crate already tracks IS caught,
    // and that is the case the mechanism exists for.
    let dirty = match git(&["status", "--porcelain", "--untracked-files=no"]) {
        Some(s) => !s.is_empty(),
        // Could not ask. "clean" would be a guess in the direction that invites trust, so it is
        // reported as unknown-and-therefore-suspect instead.
        None => true,
    };

    stamp(&commit, dirty);
}

fn watch(p: &Path) {
    println!("cargo:rerun-if-changed={}", p.display());
}

fn stamp(commit: &str, dirty: bool) {
    println!("cargo:rustc-env=FERRODB_BUILD_COMMIT={commit}");
    println!("cargo:rustc-env=FERRODB_BUILD_DIRTY={}", if dirty { "1" } else { "0" });
}

/// The variables git treats as local to one repository, which this script clears so that git
/// finds the package's own. Asked of git rather than written down, so a variable a later git adds
/// is cleared too. An empty list, if git cannot even answer this, leaves the environment as it
/// was, and the questions that follow fail or answer for the package as they would have anyway.
fn local_git_env() -> Vec<String> {
    Command::new("git")
        .args(["rev-parse", "--local-env-vars"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).lines().map(str::to_string).collect())
        .unwrap_or_default()
}

/// Every git file whose change can change the stamp, as absolute paths, or `None` when git cannot
/// say where this package's repository is.
///
/// `--git-path` rather than names joined onto `--git-dir` by hand: git knows that `HEAD` and the
/// index belong to this worktree and the refs to the common directory. (It would also honour a
/// relocation such as `GIT_INDEX_FILE`, but `main` has cleared those by the time this runs.)
fn git_state_paths(git: &dyn Fn(&[&str]) -> Option<String>) -> Option<Vec<PathBuf>> {
    // The two ref stores are asked different questions. In a reftable repository the `HEAD` file
    // is a fixed placeholder (`ref: refs/heads/.invalid`), every ref lives in a `reftable/`
    // directory (one per worktree, plus the shared one), and `--git-path` for a branch's ref FILE
    // is refused outright with "Not a directory", which would read here as "no repository". A git
    // that cannot answer `--show-ref-format` is taken to be reading the files store: reftable
    // support and that question arrived together, in git 2.45 (from the release notes, not
    // re-checked here).
    let reftable = git(&["rev-parse", "--show-ref-format"]).as_deref() == Some("reftable");
    // `refs/heads/<branch>`, or nothing when HEAD is detached. A detached HEAD holds the commit
    // itself, so its file is then the whole of it.
    let branch = if reftable { None } else { git(&["symbolic-ref", "-q", "HEAD"]) };
    // The branch's ref file is asked for EVEN when it does not exist. Then the branch lives only
    // in `packed-refs`, its next commit creates the file, and until then cargo's re-run on every
    // build for a missing watched path is what keeps the stamp right. (`packed-refs` itself is
    // not watched: the module doc says why.)
    let mut asked = vec!["HEAD", "index"];
    if reftable {
        asked.push("reftable");
    } else if let Some(b) = branch.as_deref() {
        asked.push(b);
    }
    let mut args = vec!["rev-parse", "--path-format=absolute", "--git-common-dir"];
    for name in &asked {
        args.push("--git-path");
        args.push(*name);
    }
    let answer = git(&args)?;
    let paths: Vec<PathBuf> = answer.lines().map(|l| PathBuf::from(l.trim())).collect();
    // One absolute path per question, or it is not an answer. `rev-parse` prints an argument it
    // does not understand back as a line of output and still exits 0 (measured, git 2.50.1), so
    // a git older than `--path-format` (2.31, from the release notes) answers with the wrong
    // lines rather than failing.
    if paths.len() != 1 + asked.len() || !paths.iter().all(|p| p.is_absolute()) {
        return None;
    }

    let mut watched = paths[1..].to_vec();
    if reftable {
        // `--git-path reftable` is this worktree's own table; the branches are in the shared one,
        // which in a main checkout is the same directory.
        let shared = paths[0].join("reftable");
        if !watched.contains(&shared) {
            watched.push(shared);
        }
    }
    Some(watched)
}
