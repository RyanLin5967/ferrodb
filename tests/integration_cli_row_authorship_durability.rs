//! E79c — exit criterion 9 **through the API a user calls**, across a real process restart.
//!
//! # The gap this closes, and why the existing tests could not see it
//!
//! `DurableProvenanceStore` has persisted interned runs and `(page, slot) -> ProvId` stamps since
//! E79, and `tests/provenance_e2e.rs` proves it. E79's commit message therefore said attribution
//! "outlives the process", and the first half of that was true.
//!
//! The second half was not. `AgentRuntime::who_wrote_row` — the SQL-facing surface, and the one
//! criterion 9 is written against — read `State::row_author` and `State::runs`, two in-memory maps
//! on the runtime that the durable store never populated. So the stamp survived a restart while the
//! *question* did not: a reopened database had every row intact and `ferro_row_authors` empty.
//!
//! Every provenance test in the repo passed throughout, because each asks
//! `provenance().attribute()` — the layer *underneath* the API. A test of the layer under an API is
//! not a test of the API, which is the lesson this ledger has now recorded three times.
//!
//! # What makes this test non-vacuous
//!
//! Three separate things, because each of the obvious weaker versions passes against the defect:
//!
//! 1. **The question is asked in a different process from the one that answered it.** A single
//!    process passes with the in-memory maps and always did.
//! 2. **It is asked through `SELECT ... FROM ferro_row_authors`**, which reaches `authors_of`,
//!    which is one of the two functions the row is about. Nothing here touches
//!    `provenance().attribute()`.
//! 3. **The unattributed row is asserted absent.** A view that listed every row of every table
//!    would satisfy assertions 1 and 2 while answering nothing; row 1 is inserted by plain SQL
//!    before any agent exists and must come back with no author, for ever.
//!
//! Expected values come from the design, not from running the subject: `DESIGN.md` states `RowId`
//! is "an immutable surrogate minted at insert — the PK is a *constraint*, not identity", and
//! `row_id_of`'s `Integer` arm takes the first column verbatim, so `VALUES (2, 20)` is row `2`.
//!
//! Fire-checked, both directions, by reverting `who_wrote_row`/`authors_of` to the in-memory maps:
//! `the_author_of_a_merged_row_is_answerable_by_the_view_after_a_restart` fails on its post-restart
//! assertion (`restock` present before the restart, absent after), and
//! `dropping_a_table_forgets_its_authors_across_a_restart` fails the other way — the drop is
//! forgotten and the authors come back.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

/// Ordinary-table pages reserved below the arena floor. Same reasoning as
/// `integration_cli_agent_isolation.rs`: the default floor puts the arena's first page ~128 MB in,
/// which is 128 MB of real zeroes on a filesystem without sparse files (NTFS, which CI runs).
const HEADROOM: u32 = 256;

/// Feed SQL to the real binary and return everything it printed.
///
/// `CARGO_BIN_EXE_*` rather than a hand-built `target/debug/...` path: it is correct on Windows
/// without a `.exe` suffix that gets forgotten.
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

/// Fail loudly on a statement that errored. Without this a typo in the SQL turns every assertion
/// below into "the author was not there", which is exactly what this test is trying to detect, and
/// it would then pass for the wrong reason.
fn assert_no_errors(out: &str, what: &str) {
    assert!(
        !out.contains("error:"),
        "{what} reported an error, so nothing after it means anything:\n{out}"
    );
}

/// The view, projected to the four columns this test makes claims about.
///
/// Named columns rather than `SELECT *`: the view has seven, and two of them (`prov_id`, the
/// interning slot, and `model_version`) are free to change without changing the answer to "who
/// wrote this row". Pinning them here would make the test fail for reasons it is not about.
const AUTHORS: &str = "SELECT table_name, row_id, agent_id, run_id FROM ferro_row_authors;\n";

#[test]
fn the_author_of_a_merged_row_is_answerable_by_the_view_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("authors.db");

    // Process 1: a table and a row nobody attributed. This row is the control — it must never
    // acquire an author, in this process or any later one.
    let setup = ferrodb(
        &db,
        "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n\
         INSERT INTO inv VALUES (1, 10);\n",
    );
    assert_no_errors(&setup, "the setup session");

    // Process 2: an agent writes and merges, then asks the view itself. Asking here as well as
    // after the restart is what separates "attribution never worked" from "attribution did not
    // survive" — without it, a total failure and the defect under test look identical.
    let agent = ferrodb(
        &db,
        &format!(
            "BEGIN AGENT SESSION AS 'restock' RUN 'r_7' MODEL 'claude-opus-5/2026-05';\n\
             INSERT INTO inv VALUES (2, 20);\n\
             MERGE;\n\
             {AUTHORS}"
        ),
    );
    assert_no_errors(&agent, "the agent session");
    assert!(
        agent.contains("Clean"),
        "the merge did not come back Clean, so nothing was published to attribute:\n{agent}"
    );
    assert!(
        agent.contains("inv | 2 | restock | r_7"),
        "the view did not answer in the process that did the writing, so this test cannot tell a \
         durability failure from a total one:\n{agent}"
    );

    // Process 3: the restart. Nothing in this process ran an agent, so every in-memory map the old
    // implementation read is empty by construction.
    let after = ferrodb(&db, AUTHORS);
    assert_no_errors(&after, "the post-restart session");
    assert!(
        after.contains("inv | 2 | restock | r_7"),
        "`ferro_row_authors` lost the author of a merged row across a restart — the stamp is \
         durable and the question is not, which is exactly the E79c defect:\n{after}"
    );

    // The control. `row_id` is the first column verbatim for an `Integer` PK, so the plain-SQL row
    // is row 1, and it has no run behind it in any process.
    assert!(
        !after.contains("inv | 1 |"),
        "a row written by plain SQL before any agent existed came back with an author, so the view \
         is listing rows rather than answering about them:\n{after}"
    );

    // And the rows themselves are still there — otherwise "the author survived" could be true of a
    // database that lost the data it describes.
    let rows = ferrodb(&db, "SELECT * FROM inv;\n");
    assert_no_errors(&rows, "the row read-back");
    assert!(rows.contains("2 | 20"), "the attributed row itself did not survive:\n{rows}");
}

/// `DROP TABLE` has to erase authorship, and that erasure has to outlive the process too.
///
/// The key is a hash of the table's **name** (`table_id`), because the catalog mints no table ids
/// and a name hash is stable across processes where an assignment counter would not be. The cost is
/// that a table dropped and recreated under one name is the same table to the store — so a drop
/// that only cleared an in-memory map would hold until the next restart and then hand the new table
/// its predecessor's authors, attributed to an agent that never touched it.
///
/// This is the half of E79c that the append-only log makes non-trivial: the stamps are already in
/// the file and cannot be unwritten, so `forget_table` appends a tombstone that replay applies in
/// file order.
#[test]
fn dropping_a_table_forgets_its_authors_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("forget.db");

    let first = ferrodb(
        &db,
        "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n\
         BEGIN AGENT SESSION AS 'restock' RUN 'r_1' MODEL 'm/1';\n\
         INSERT INTO inv VALUES (7, 70);\n\
         MERGE;\n",
    );
    assert_no_errors(&first, "the merging session");

    // It is there after a restart — the same fact the test above pins, asserted here so the
    // disappearance below is attributable to the DROP and not to the attribution never landing.
    let before_drop = ferrodb(&db, AUTHORS);
    assert_no_errors(&before_drop, "the pre-drop session");
    assert!(
        before_drop.contains("inv | 7 | restock | r_1"),
        "the author was not there before the drop, so the drop proves nothing:\n{before_drop}"
    );

    let dropped = ferrodb(&db, "DROP TABLE inv;\n");
    assert_no_errors(&dropped, "the drop session");

    // A NEW process, and the table recreated under the same name. If the tombstone did not reach
    // the file, replay hands this table the dead one's author.
    let after = ferrodb(
        &db,
        &format!(
            "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n\
             INSERT INTO inv VALUES (7, 70);\n\
             {AUTHORS}"
        ),
    );
    assert_no_errors(&after, "the post-drop session");
    assert!(
        !after.contains("restock"),
        "a table recreated under a dropped table's name inherited its authors across a restart; \
         the drop cleared memory and not the log:\n{after}"
    );
}
