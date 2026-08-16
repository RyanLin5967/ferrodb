//! E31 — agent isolation through the **shipped binary**, not through the engine's API.
//!
//! Every other test in this repo constructs an `AgentRuntime` itself and hands it a page store. That
//! proves the engine works. It does not prove the program anyone actually runs uses it, and for most
//! of this project's life it did not: `Session::new()` built a runtime with `storage: None`, so
//! `BEGIN AGENT SESSION` in the CLI staged writes in a `BTreeMap` while the copy-on-write branch
//! engine sat beside it, fully tested and entirely unreachable from SQL. A reader who cloned the
//! repo and typed `INSERT` got none of it.
//!
//! So these tests spawn `target/debug/ferrodb`, pipe SQL to its stdin, and read what it prints.
//! Nothing here can pass by calling an internal constructor the binary does not call.
//!
//! # What makes these tests non-vacuous
//!
//! "The agent's row is not on trunk" passes just as well against a database that dropped the write
//! on the floor, and "isolation works" passes against an engine that isolates by doing nothing. Two
//! things rule that out:
//!
//! - the same row **is** visible inside the session that wrote it, and becomes visible on trunk
//!   after `MERGE` and a restart, so the write was real and reached durable storage;
//! - the database file grows past the arena floor, which is only possible if pages were allocated
//!   in the copy-on-write region. An in-memory runtime leaves the file at its catalog size.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

/// Ordinary-table pages reserved below the arena floor. The default (32736) puts the arena's first
/// page ~128 MB into the file, which is free on a filesystem with sparse files and 128 MB of real
/// zeroes on one without — NTFS, which CI runs. A small floor keeps these tests cheap everywhere
/// while still being a floor.
const HEADROOM: u32 = 256;

/// Feed SQL to the real binary and return everything it printed.
///
/// `CARGO_BIN_EXE_*` rather than a hand-built `target/debug/...` path: it is correct on Windows
/// without a `.exe` suffix that gets forgotten, which has already been a defect here nine times over
/// in one file.
fn ferrodb(db: &Path, sql: &str) -> String {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ferrodb"))
        .arg(db)
        .env("FERRODB_ARENA_HEADROOM", HEADROOM.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ferrodb");

    child.stdin.take().unwrap().write_all(sql.as_bytes()).expect("write sql");
    let out = child.wait_with_output().expect("wait for ferrodb");

    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "ferrodb exited {:?} on:\n{sql}\n--- output ---\n{text}",
        out.status.code()
    );
    text
}

/// Fail loudly on a statement that errored. Without this a typo in the SQL below turns every
/// assertion into "the row was not there", which is exactly what the test is trying to detect, and
/// the test passes for the wrong reason.
fn assert_no_errors(out: &str, what: &str) {
    assert!(
        !out.contains("error:"),
        "{what} reported an error, so nothing after it means anything:\n{out}"
    );
}

fn rows_of(out: &str, marker: &str) -> String {
    let at = out.find(marker).unwrap_or_else(|| panic!("no `{marker}` in output:\n{out}"));
    out[at..].to_string()
}

#[test]
fn an_agent_sessions_write_is_invisible_to_trunk_and_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("iso.db");

    let setup = ferrodb(
        &db,
        "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n\
         INSERT INTO inv VALUES (1, 10);\n",
    );
    assert_no_errors(&setup, "the setup session");
    let before = std::fs::metadata(&db).unwrap().len();

    let first = ferrodb(
        &db,
        "BEGIN AGENT SESSION AS 'pricing' RUN 'r_1';\n\
         INSERT INTO inv VALUES (2, 20);\n\
         SELECT * FROM inv;\n\
         DIFF;\n",
    );
    assert_no_errors(&first, "the first session");
    let after = std::fs::metadata(&db).unwrap().len();

    // The write is real and visible to its own author. If this fails, everything below is testing a
    // database that simply discarded the insert.
    let seen = rows_of(&first, "(1 row affected)");
    assert!(
        seen.contains("2 | 20"),
        "the agent could not see its own write, so `isolation` here means `data loss`:\n{first}"
    );
    assert!(
        first.contains("INSERT inv.row2"),
        "DIFF did not report the branch's pending row:\n{first}"
    );

    assert!(
        db.with_extension("db.arena").exists(),
        "no arena checkpoint was written, so the CLI is not running the page-backed runtime"
    );
    assert!(
        db.with_extension("db.branches").exists(),
        "no branch catalog was written, so branches are not durable"
    );

    // **The assertion that makes this test about shadow paging rather than about any isolation at
    // all.** Verified by reverting `Session::with_runtime(runtime)` to `Session::new()` and re-
    // running: without it this test still passed, because a runtime staging rows in a `BTreeMap`
    // isolates them from trunk just as well. What it cannot do is copy a page. The agent's write
    // must extend the file, and past the arena floor, or it did not go through the branch engine.
    let floor_bytes = HEADROOM as u64 * 4096;
    assert!(
        after > before && after > floor_bytes,
        "the agent's write left the database file at {after} bytes (was {before}, arena floor \
         {floor_bytes}). Isolation without a copied page is isolation by staging the row \
         somewhere else, not by shadow paging."
    );

    // A *different* process, which never saw the agent session, must not see its row.
    let second = ferrodb(&db, "SELECT * FROM inv;\n");
    assert_no_errors(&second, "the second session");
    assert!(second.contains("1 | 10"), "trunk lost the committed row:\n{second}");
    assert!(
        !second.contains("2 | 20"),
        "an unmerged agent write leaked onto trunk and outlived the session that made it:\n{second}"
    );
}

/// The anti-vacuity half of the test above. Isolation that never ends is indistinguishable from
/// discarding the write, so the row has to become visible on trunk once merged — and stay visible
/// across a restart, which is what makes it durable rather than merely reported.
///
/// This one deliberately does **not** discriminate page-backed from in-memory, and reverting the
/// wiring confirms it still passes: `MERGE` publishes into the ordinary heap through the executor,
/// so a merged row is durable by the table storage's own WAL either way. Its job is to rule out the
/// reading of the test above where "not on trunk" means "gone".
#[test]
fn a_merged_write_reaches_trunk_and_is_still_there_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("merge.db");

    let first = ferrodb(
        &db,
        "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n\
         INSERT INTO inv VALUES (1, 10);\n\
         BEGIN AGENT SESSION AS 'a' RUN 'r';\n\
         INSERT INTO inv VALUES (2, 20);\n\
         MERGE;\n",
    );
    assert_no_errors(&first, "the merging session");
    assert!(
        first.contains("Clean"),
        "a merge with no competing write did not come back Clean:\n{first}"
    );

    let after = ferrodb(&db, "SELECT * FROM inv;\n");
    assert_no_errors(&after, "the post-merge session");
    assert!(after.contains("1 | 10"), "the trunk row vanished across the merge:\n{after}");
    assert!(
        after.contains("2 | 20"),
        "the merged row is not on trunk after a restart; the merge was reported but not durable:\n\
         {after}"
    );
}

/// The discriminating check: pages really are allocated in the copy-on-write arena.
///
/// The arena owns `[floor, inf)` and the ordinary allocator everything below, so a file that extends
/// past `floor * PAGE_SIZE` can only have got there by a write to an arena page. A runtime holding
/// its rows in memory leaves the file at the handful of pages the catalog uses, whatever the SQL
/// prints. This is the assertion that fails if `Session::new()` is ever restored.
#[test]
fn the_binary_allocates_real_pages_above_the_arena_floor() {
    const PAGE_SIZE: u64 = 4096;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("pages.db");

    let baseline = ferrodb(&db, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);\n");
    assert_no_errors(&baseline, "the schema-only session");
    let before = std::fs::metadata(&db).unwrap().len();

    let out = ferrodb(
        &db,
        "BEGIN AGENT SESSION AS 'a' RUN 'r';\n\
         INSERT INTO t VALUES (1, 1);\n",
    );
    assert_no_errors(&out, "the agent session");
    let after = std::fs::metadata(&db).unwrap().len();

    // The floor is high-water-at-creation plus HEADROOM; high-water is a few pages for a database
    // this small, so HEADROOM alone is a safe lower bound on it.
    let floor_bytes = HEADROOM as u64 * PAGE_SIZE;

    // A storage-backed runtime allocates the trunk root up in the arena the moment the database is
    // created, so the file is past the floor before any agent has written a row. That is a stronger
    // signal than the one this test was originally written to look for — an in-memory runtime never
    // touches a page up there and leaves the file at the catalog's handful of pages, three orders of
    // magnitude below — but it does mean the floor alone cannot distinguish "the engine is wired in"
    // from "the agent's write went somewhere". Hence the second assertion.
    assert!(
        before > floor_bytes,
        "with only a schema created the file is {before} bytes, below the arena floor at \
         {floor_bytes}. Nothing was allocated in the copy-on-write region, so the CLI is not \
         running the page-backed runtime at all."
    );
    assert!(
        after > before,
        "the agent-session insert did not extend the file ({before} -> {after} bytes). Under \
         shadow paging a write copies the pages it touches, so a write that allocates nothing is a \
         write that never reached a page."
    );
}

/// Reopening must reattach to the arena that is there, not start a new one beside it. A fresh arena
/// on every open would silently orphan every branch page written before the restart, and the symptom
/// would be data that reads correctly right up until it does not.
#[test]
fn reopening_reattaches_to_the_existing_arena_and_branch_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("reattach.db");

    let first = ferrodb(
        &db,
        "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);\n\
         BEGIN AGENT SESSION AS 'a' RUN 'r1';\n",
    );
    assert_no_errors(&first, "the first session");
    assert!(first.contains("b_1"), "the first branch was not b_1:\n{first}");

    let second = ferrodb(&db, "BEGIN AGENT SESSION AS 'b' RUN 'r2';\n");
    assert_no_errors(&second, "the second session");
    assert!(
        second.contains("b_2"),
        "after a restart the branch counter went back to the start, so the branch catalog was \
         recreated rather than reopened and b_1's pages are now unreachable:\n{second}"
    );
}

/// A database whose arena checkpoint is missing must refuse to open rather than invent a floor.
///
/// Tests the *open* path, which is independent of which runtime the session gets — it passes with
/// the wiring reverted, by design.
/// Guessing would put the new arena on top of pages the old one owns, and two allocators handing out
/// the same page is corruption that surfaces long after the mistake.
#[test]
fn a_deleted_arena_checkpoint_is_refused_rather_than_defaulted() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("lost.db");

    let first = ferrodb(&db, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);\n");
    assert_no_errors(&first, "the first session");
    let arena = db.with_extension("db.arena");
    assert!(arena.exists(), "no checkpoint to delete; the test would prove nothing");
    std::fs::remove_file(&arena).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_ferrodb"))
        .arg(&db)
        .env("FERRODB_ARENA_HEADROOM", HEADROOM.to_string())
        .stdin(Stdio::null())
        .output()
        .expect("spawn ferrodb");
    assert!(
        !out.status.success(),
        "opening a database whose arena checkpoint was deleted succeeded. It cannot know where the \
         arena starts, so it either guessed or silently started a second one:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// **E38 — a second process must not be able to open a database that is already open.**
///
/// Two openers both build an `ArenaPageStore` from the same checkpoint, so both read the same
/// `next_extent_start` and hand the same pages to different branches. Every such page still passes
/// its checksum, which is what makes this worth a hard refusal rather than a warning: there is no
/// later point at which the damage announces itself.
///
/// Measured before the lock existed: with a server listening on a database, the CLI opened the same
/// path, read from it and exited cleanly, checkpointing its arena over the server's live view, and
/// neither process printed anything.
#[test]
fn a_database_already_open_is_refused_to_a_second_process() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("locked.db");

    let setup = ferrodb(&db, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);\n");
    assert_no_errors(&setup, "the setup session");

    // Hold the database open by taking the lock file the way a live process would.
    let lock = db.with_extension("db.lock");
    std::fs::write(&lock, "999999\n").unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_ferrodb"))
        .arg(&db)
        .env("FERRODB_ARENA_HEADROOM", HEADROOM.to_string())
        .stdin(Stdio::null())
        .output()
        .expect("spawn ferrodb");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !out.status.success(),
        "a second process opened a database that was already held:\n{text}"
    );
    assert!(text.contains("already open"), "the refusal does not say why:\n{text}");
    assert!(
        text.contains("999999"),
        "the refusal does not name the holder, so an operator cannot tell whether it is stale:\n\
         {text}"
    );

    // And releasing it lets the next open through — otherwise every clean shutdown would brick the
    // database, which is a worse failure than the one being prevented.
    std::fs::remove_file(&lock).unwrap();
    let after = ferrodb(&db, "SELECT * FROM t;\n");
    assert_no_errors(&after, "the session after the lock was released");
}
