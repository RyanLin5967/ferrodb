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
//! and whether the tree differs from it. "Differs" is three questions, OR-ed: `git status` over
//! the tracked files; the BYTES of every tracked build input against HEAD's blobs; and any
//! untracked file among the build inputs that cargo could compile. Cargo re-runs the script only
//! when something it was told to watch has changed, so the stamp is exactly as fresh as the watch
//! list, and the list is the whole of the mechanism:
//!
//! * **The build's inputs**: `src/`, `examples/`, `tests/` and `benches/` (each scanned whole, and
//!   each only if it exists), `build.rs`, `Cargo.toml` and `Cargo.lock`, and the package root's
//!   `.cargo/`, `rust-toolchain` and `rust-toolchain.toml`, which change the build without changing
//!   a line of source. An edit to any of them is an edit to what is being compiled, so it
//!   re-stamps. This is what makes an unstaged edit say `+DIRTY`.
//! * **The git state**: this worktree's `HEAD` and index, and the ref file of every link from
//!   `HEAD` to the branch it resolves to, normally just the checked-out branch's (or, on a
//!   repository that keeps its refs in reftable, its `reftable/` directories). Each is
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
//! * Submodules: a submodule that is modified, or checked out at another commit, shows in
//!   `git status` and stamps `+DIRTY`. What hides one is `submodule.<name>.ignore = all` in the
//!   tracked `.gitmodules`, or an untracked file inside it (measured by D231's review 5, which
//!   corrected this sentence). A submodule among the build INPUTS stamps `unknown`, because it has
//!   no bytes to compare. ferrodb has no submodules.
//! * The byte comparison assumes a checkout writes each blob's bytes unchanged. A smudge filter,
//!   or line endings converted on checkout (`core.autocrlf`, `eol=crlf`), makes every converted
//!   file differ from its blob, so a clean tree stamps `+DIRTY` on that machine: the safe
//!   direction, but a flag that then means nothing there. ferrodb's `.gitattributes` pins
//!   `* text=auto eol=lf`, which checks out LF on every platform, CI's Windows runner included
//!   (from gitattributes(5); not run on Windows here).
//! * Untracked files that are NOT build inputs, and ignored files among the inputs that do not end
//!   in `.rs`, are not counted: nothing compiles them. A `.cargo/` or `rust-toolchain*` in a parent
//!   directory is not seen at all, and one that appears at the package root where none existed is
//!   seen only at the next re-stamp for another reason, like a new `benches/`.
//! * **Every re-stamp rebuilds the crate, and so everything that links it.** The Cargo FAQ lists a
//!   re-running build script as a cause of rebuilds; that the dependents follow is inferred. So an
//!   edit to one test or example rebuilds the library and every target, where cargo alone would
//!   have rebuilt that one target, and so does a file in a watched directory that is never
//!   compiled: `tests/pg/`'s Python clients, or the `__pycache__/` Python writes beside them the
//!   first time they run. That is the price of the stamp living in the library, and it is paid
//!   deliberately: a harness in `examples/` edited and not committed must still say `+DIRTY`.
//! * A watched git path that does not exist makes cargo re-run this script, and therefore rebuild
//!   the crate, on every build until it does exist. The stamp stays right and the rebuild is the
//!   cost. A branch that lives only in `packed-refs` does this until the next write to its ref
//!   recreates the file. **One `git gc` puts every worktree into that state at once**: its
//!   `pack-refs --all` deletes the loose ref of every branch, the checked-out ones included
//!   (measured by D231's review 2), so each worktree then rebuilds on every build until its own
//!   branch's ref is next written. For a main checkout this is new relative to the old watch list,
//!   whose two files gc does not remove; for a linked worktree it is what the old list did always.
//! * `HEAD` is followed link by link (`git symbolic-ref --no-recurse`), and every link's file is
//!   watched, so retargeting the middle of a chain (`HEAD` to `foo` to `main`) re-stamps. A git
//!   without `--no-recurse` is asked for the chain's end only, and there retargeting a middle link
//!   is invisible (measured by D231's review 2 for the one-question form).
//! * In a reftable repository the shared table changes on every ref update in every worktree, so
//!   any commit anywhere re-stamps, and rebuilds, every worktree. ferrodb's repository uses the
//!   files store, so this is latent here.
//! * With the deprecated `core.preferSymlinkRefs`, `HEAD` is a symbolic LINK, and `--git-path
//!   HEAD` names its target's real path, so retargeting `HEAD` moves no watched file and the stamp
//!   keeps the old branch's sha (measured by D231's review 4). A known limit: the option is not
//!   set on this machine, and git has deprecated it.
//! * The sha and the dirty bit are read before rustc compiles anything. An edit that lands after
//!   this script reads the tree and before rustc reads the file is compiled and not stamped; the
//!   next build re-stamps, on a filesystem with sub-second mtimes. On one with 1 s or 2 s mtimes an
//!   edit in the same tick as the script's start is not newer than its `output`, and is not seen.
//!
//! # When it says `unknown`, which is never a guess in the direction that invites trust
//!
//! The stamp is `unknown`, marked dirty, whenever the two facts cannot be read as a description
//! of the tree cargo is compiling:
//!
//! * `git` is absent, or cannot say where this package's repository is. That includes a git older
//!   than 2.31, too old to answer the path question the way it is asked.
//! * **The repository git finds is not this package's own** (D231 review 4, N1). Git discovery
//!   walks UP from the package root, so a `git archive` copy extracted inside another checkout,
//!   under its `target/` for example, would otherwise be stamped with THAT checkout's sha. It can
//!   read clean, too, because the copy is untracked there. `git rev-parse --show-toplevel` must be
//!   the package root. ferrodb is a single-package repository, so a package below the repository
//!   root is refused in the same way.
//! * **An index entry is told to hide changes** (N2): assume-unchanged (a lowercase `ls-files -v`
//!   tag), skip-worktree (`S`), or `core.ignoreStat`. `git status` skips such entries by design,
//!   so an edited, compiled file would read clean.
//! * **`git status` fails** (N4), or one of the byte comparison's questions does (U2), or a build
//!   input is a submodule, which has no bytes to compare. "Dirty" with the old sha would still
//!   name a commit, and nothing here was able to compare the tree with it.
//! * **HEAD moved while the two facts were being read** (N6). The sha is read, then `status`
//!   and the byte comparison run, then the sha is read again. A commit landing between them would
//!   otherwise pair the parent's sha with the child's tree.
//!
//! None of these fails the build: refusing to compile ferrodb because git is missing or odd would
//! be a worse failure than an honest `unknown`.
//!
//! It describes the repository at the package root and nothing else: every variable that
//! `git rev-parse --local-env-vars` lists (`GIT_DIR`, `GIT_WORK_TREE`, `GIT_INDEX_FILE`, and the
//! rest) is cleared before git is asked. A git hook exports those for the repository the hook is
//! about, and `tools/prepush.sh` builds the checkout it lives in, which need not be that one.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// What the crate is compiled from, relative to the package root. Anything the crate compiles in
/// from elsewhere must be added here, or an edit to it never re-stamps.
///
/// Only the entries that exist are watched: cargo re-runs a script whose watched path is missing
/// on every build (the Cargo FAQ, "Why is Cargo rebuilding my code?"), and this package has no
/// `benches/`. One that appears later is picked up at the next re-stamp, and adding a tracked file
/// to it rewrites the index, which is watched. `build.rs` is listed for completeness: cargo
/// already re-runs a script whose source changed. `.cargo/` and the `rust-toolchain` files change
/// the build without changing a line of source (D231 review 5, R1).
const INPUTS: &[&str] = &[
    "src",
    "examples",
    "tests",
    "benches",
    "build.rs",
    "Cargo.toml",
    "Cargo.lock",
    ".cargo",
    "rust-toolchain",
    "rust-toolchain.toml",
];

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
        let out = git_command(&root, &local_env, args).output().ok()?;
        if !out.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
    };
    // The same, with `input` on stdin. Written from a thread: git writes its answers while it
    // reads, and a caller that wrote everything before reading would deadlock once the answers
    // filled the pipe.
    let git_in = |args: &[&str], input: &str| -> Option<String> {
        let mut child = git_command(&root, &local_env, args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        let mut stdin = child.stdin.take()?;
        let input = input.to_string();
        let writer = std::thread::spawn(move || stdin.write_all(input.as_bytes()));
        let out = child.wait_with_output().ok()?;
        writer.join().ok()?.ok()?;
        if !out.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
    };

    // Fail loud. A repository that is not this package's own describes some other tree, and
    // without a repository there is nothing to watch and nothing to believe.
    if !repository_is_the_package(&git, &root) {
        stamp("unknown", true);
        return;
    }
    let Some(git_paths) = git_state_paths(&git) else {
        stamp("unknown", true);
        return;
    };
    for p in &git_paths {
        watch(p);
    }
    // After the watch list is emitted, so that whatever changes the answer re-stamps.
    if index_hides_changes(&git) {
        stamp("unknown", true);
        return;
    }

    // "Dirty" is three questions, OR-ed. `git status` covers every TRACKED file, build input or
    // not. The byte comparison covers the build inputs without trusting git's opinion of them.
    // The untracked check covers what `status --untracked-files=no` leaves out.
    //
    // The untracked check replaces a trade this comment used to defend: that counting untracked
    // files "would mark the tree dirty for every stray scratch file". D231's review 5 measured it
    // across all 72 ferrodb worktrees on this machine. One had an untracked file among the inputs,
    // and it was a true positive: three adversarial test files that cargo compiled, in a tree whose
    // tracked files were clean, so its stamp read clean over code no commit holds.
    //
    // The sha is read on both sides: separate git processes are separate moments, and a commit
    // landing between them would pair one commit's sha with another tree's answer.
    let before = git(&["rev-parse", "--short=12", "HEAD"]);
    let status = git(&["status", "--porcelain", "--untracked-files=no"]);
    let inputs = inputs_differ_from_head(&git, &git_in, &root);
    let after = git(&["rev-parse", "--short=12", "HEAD"]);
    match (before, status, inputs, after) {
        (Some(sha), Some(changes), Some(differ), Some(again)) if sha == again => {
            stamp(&sha, !changes.is_empty() || differ)
        }
        // No sha, a question that failed, or a HEAD that moved in between. "Dirty" beside a sha
        // would still name a commit nothing here compared the tree with.
        _ => stamp("unknown", true),
    }
}

/// `git`, run at the package root, with the flags and the cleared environment every question
/// here needs.
///
/// `--no-optional-locks`: a plain `git status` refreshes the index and writes it back
/// (git-status(1), BACKGROUND REFRESH). That takes the index lock out from under whoever is
/// committing in this worktree, and it rewrites a file this script watches, so the script would
/// schedule its own re-run.
///
/// `--no-replace-objects`: the stamped sha names the real commit, so `dirty` must compare against
/// the real commit's tree. With `git replace` honoured, a replacement object changes the
/// comparison without changing any file this script watches (D231 review).
fn git_command(root: &Path, local_env: &[String], args: &[&str]) -> Command {
    let mut cmd = Command::new("git");
    cmd.args(["--no-optional-locks", "--no-replace-objects"]).args(args).current_dir(root);
    for var in local_env {
        cmd.env_remove(var);
    }
    cmd
}

/// Whether the build inputs on disk differ from HEAD, by their BYTES (D231 review 5, U1 and U2).
/// `Some(true)` differs, `Some(false)` is identical, `None` could not tell.
///
/// `git status` answers from the index: its stat cache, its assume-unchanged and skip-worktree
/// bits, an fsmonitor's report, clean filters, and whatever configuration supplies them,
/// `GIT_CONFIG_GLOBAL` included, which the environment clearing above does not reach. Review 5
/// made each of those hide an edit that was compiled. This asks none of them. It hashes each
/// tracked input as it is on disk (`hash-object --no-filters`) and compares that with HEAD's blob
/// id (`ls-tree`). Measured by review 5 on this package: 386 files, 9.8 MB, 0.162 s.
///
/// Then the inputs HEAD does not have. An untracked file that is not ignored counts, whatever it
/// is. An ignored one counts only if it ends in `.rs`, because cargo's target discovery compiles
/// `examples/*.rs` and `tests/*.rs` whether git ignores them or not, and ignored noise such as
/// `tests/pg/__pycache__` must not make every stamp dirty.
fn inputs_differ_from_head(
    git: &dyn Fn(&[&str]) -> Option<String>,
    git_in: &dyn Fn(&[&str], &str) -> Option<String>,
    root: &Path,
) -> Option<bool> {
    // HEAD's entries under the inputs, NUL-separated: `<mode> <type> <id>\t<path>`.
    let mut args = vec!["ls-tree", "-r", "-z", "--full-tree", "HEAD", "--"];
    args.extend(INPUTS.iter().copied());
    let tree = git(&args)?;
    let mut paths: Vec<&str> = Vec::new();
    let mut ids: Vec<&str> = Vec::new();
    for entry in tree.split('\0').filter(|e| !e.is_empty()) {
        let (meta, path) = entry.split_once('\t')?;
        let mut fields = meta.split(' ');
        let (_mode, kind, id) = (fields.next()?, fields.next()?, fields.next()?);
        // A submodule has no bytes to compare, and a path with a newline cannot be handed to
        // `--stdin-paths`. Neither exists in ferrodb; either is refused rather than skipped.
        if kind != "blob" || path.contains('\n') {
            return None;
        }
        paths.push(path);
        ids.push(id);
    }
    // A tracked input missing from the disk differs.
    if paths.iter().any(|p| !root.join(p).is_file()) {
        return Some(true);
    }
    if !paths.is_empty() {
        let mut list = paths.join("\n");
        list.push('\n');
        let disk = git_in(&["hash-object", "--no-filters", "--stdin-paths"], &list)?;
        let disk: Vec<&str> = disk.lines().collect();
        if disk.len() != ids.len() {
            return None;
        }
        if disk.iter().zip(&ids).any(|(on_disk, in_head)| on_disk != in_head) {
            return Some(true);
        }
    }

    let present: Vec<&str> = INPUTS.iter().copied().filter(|i| root.join(i).exists()).collect();
    if present.is_empty() {
        return Some(false);
    }
    let mut args = vec!["ls-files", "--others", "--exclude-standard", "-z", "--"];
    args.extend(present.iter().copied());
    if !git(&args)?.is_empty() {
        return Some(true);
    }
    let mut args = vec!["ls-files", "--others", "--ignored", "--exclude-standard", "-z", "--"];
    args.extend(present.iter().copied());
    Some(git(&args)?.split('\0').any(|p| p.ends_with(".rs")))
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

/// Whether the repository git finds from the package root is the package's own (module doc, N1).
///
/// Discovery walks UP, so a copy of the sources extracted inside another checkout finds that
/// checkout. Both sides are canonicalised, because the manifest directory and git's answer can
/// spell one directory differently (a symlinked prefix, `/` against `\` on Windows).
fn repository_is_the_package(git: &dyn Fn(&[&str]) -> Option<String>, root: &Path) -> bool {
    let Some(top) = git(&["rev-parse", "--show-toplevel"]) else {
        return false;
    };
    match (std::fs::canonicalize(&top), std::fs::canonicalize(root)) {
        (Ok(top), Ok(root)) => top == root,
        _ => false,
    }
}

/// Whether the index is told to hide changes from `git status` (module doc, N2): an entry marked
/// assume-unchanged (`ls-files -v` prints its tag in lower case) or skip-worktree (`S`), or
/// `core.ignoreStat`, which marks entries assume-unchanged as they are added. An index that cannot
/// be listed counts as hiding: "clean" is not the answer to a question that could not be asked.
fn index_hides_changes(git: &dyn Fn(&[&str]) -> Option<String>) -> bool {
    if git(&["config", "--bool", "core.ignoreStat"]).as_deref() == Some("true") {
        return true;
    }
    match git(&["ls-files", "-v"]) {
        Some(entries) => {
            entries.lines().any(|l| l.starts_with(|c: char| c.is_ascii_lowercase() || c == 'S'))
        }
        None => true,
    }
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
    // Every link from `HEAD` to the ref that holds the commit, one `--no-recurse` question each:
    // retargeting any link changes what `HEAD` resolves to. Nothing at all when HEAD is detached,
    // because a detached HEAD holds the commit itself. The walk stops collecting after five links.
    // It refuses nothing: git itself resolves a chain of four links and refuses five (measured by
    // D231 review 3, git 2.50.1), so past that `rev-parse HEAD` fails and the stamp is `unknown`.
    let mut links: Vec<String> = Vec::new();
    if !reftable {
        let mut at = "HEAD".to_string();
        while links.len() < 5 {
            let Some(next) = git(&["symbolic-ref", "-q", "--no-recurse", at.as_str()]) else {
                break;
            };
            at = next.clone();
            links.push(next);
        }
        // A git without `--no-recurse` fails the first question; it can still name the end.
        if links.is_empty() {
            if let Some(end) = git(&["symbolic-ref", "-q", "HEAD"]) {
                links.push(end);
            }
        }
    }
    // Each link's ref file is asked for EVEN when it does not exist. Then the branch lives only
    // in `packed-refs`, the next write to it creates the file, and until then cargo's re-run on
    // every build for a missing watched path is what keeps the stamp right. (`packed-refs` itself
    // is not watched: the module doc says why.)
    let mut asked = vec!["HEAD", "index"];
    if reftable {
        asked.push("reftable");
    } else {
        asked.extend(links.iter().map(String::as_str));
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
