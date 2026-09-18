//! E79c — exit criterion 9 across a **real process boundary**, through the shipped binary and the
//! SQL surface a user actually queries.
//!
//! # Why the in-process test is not enough on its own
//!
//! `tests/agent_sql_surface.rs::row_authorship_survives_a_process_restart_when_provenance_is_durable`
//! drives the public API (`who_wrote_row`) across two runtimes sharing one provenance file, and
//! `src/provenance/durable.rs`'s unit tests prove the file replays. Neither of them crosses an
//! `exec`: both build the second runtime inside the process that wrote the first, so anything a
//! process happens to carry — a static, a cache, a shared `Arc` reached by accident — is still in
//! scope. That is the precise reading under which E79 was recorded as "outlives the process" and
//! turned out to be half of what it claimed, so this row does not get to make the same claim from
//! the same kind of evidence.
//!
//! It also asks a different question. The in-process test calls `who_wrote_row`; **this one asks
//! `ferro_row_authors`**, which is the only way anyone outside the crate can ask at all, and which
//! reaches the store by a third path (`authors_of`, via `catalog::system_views`). A fix to
//! `who_wrote_row` alone would pass there and fail here.
//!
//! # What makes it non-vacuous
//!
//! * The row is written by an ordinary `UPDATE` inside `BEGIN AGENT SESSION` and published by a
//!   real `MERGE`, in a **different process** from the one that created the table.
//! * The view is queried in the writing process first, so a failure after the restart is about
//!   durability and not about attribution never having worked.
//! * A second row is written by **plain SQL with no agent session**, and the view must list
//!   exactly one attributed row after the restart. A store that attributed everything, or a view
//!   that echoed the table's rows, fails that count — "the agent's name appears somewhere in the
//!   output" on its own would not.
//! * The `model` and `model_version` columns are asserted separately, because they are split from
//!   one `MODEL 'name/version'` clause and a reopen that lost the split would still print the name.
//!
//! Fire-checked by reverting `who_wrote_row`/`authors_of` in `src/agent_sql/runtime.rs` to their
//! committed in-memory form: the pre-restart assertion still passes and the post-restart one fails,
//! which is the failure this test exists to catch.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

/// Ordinary-table pages reserved below the arena floor. Same reasoning as
/// `integration_cli_effect_log_durability.rs`: the default floor puts the arena's first page
/// ~128 MB in, which is 128 MB of real zeroes on a filesystem without sparse files (NTFS, which
/// CI runs).
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

/// The view's declared width, from `SystemView::columns` in `src/catalog/system_views.rs`:
/// `table_name, row_id, prov_id, agent_id, run_id, model_name, model_version`.
const VIEW_COLUMNS: usize = 7;

/// Data rows of a `ferro_row_authors` result that name this agent.
///
/// **Filtering the whole transcript on the agent's name does not work, and the first version of
/// this helper did exactly that.** `BEGIN AGENT SESSION` echoes `agent session b_1 on b1@g0
/// (agent=restock-agent run=run-42)`, so in the process that opens the session the name appears on
/// a banner line as well as on the view row. Measured: the count came back 2 for a view holding
/// exactly one row, and the product was right both times.
///
/// A view data row is identified by the view's declared shape — `VIEW_COLUMNS` pipe-separated
/// fields — which no banner or prompt line has. The header line has that shape and is excluded by
/// the agent filter rather than by its position, so this still counts rows and not lines.
fn attributed_lines(out: &str, agent: &str) -> Vec<String> {
    out.lines()
        .filter(|l| l.split('|').count() == VIEW_COLUMNS && l.contains(agent))
        .map(|l| l.to_string())
        .collect()
}

#[test]
fn the_shipped_binary_still_names_the_agent_that_wrote_a_row_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("attribution.db");

    // ---- process one: a table, one seeded row, and one row written by PLAIN SQL ----------------
    //
    // Row 2 is the control. It is a real row in the same table, written with no agent session, so
    // it must never acquire an author — not before the restart and not after one.
    let setup = ferrodb(
        &db,
        "CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);\n\
         INSERT INTO inv VALUES (1, 10);\n\
         INSERT INTO inv VALUES (2, 20);\n",
    );
    assert_no_errors(&setup, "the setup session");

    // ---- process two: an agent publishes row 1 -------------------------------------------------
    //
    // A second process, so the runtime takes `cli.rs`'s `reopen_with_storage` arm and the
    // provenance store is opened from the file the first process left.
    let agent = ferrodb(
        &db,
        "BEGIN AGENT SESSION AS 'restock-agent' RUN 'run-42' MODEL 'claude-opus-5/2026-05';\n\
         UPDATE inv SET qty = qty + 30 WHERE id = 1;\n\
         MERGE;\n\
         SELECT * FROM ferro_row_authors;\n",
    );
    assert_no_errors(&agent, "the agent session");
    assert!(
        agent.contains("Clean"),
        "a merge with no competing write did not come back Clean:\n{agent}"
    );
    // Answers in the writing process. Without this the post-restart failure below could not be
    // told apart from attribution never having happened at all.
    assert_eq!(
        attributed_lines(&agent, "restock-agent").len(),
        1,
        "the view did not attribute the merged row in the process that wrote it:\n{agent}"
    );

    // ---- process three: the whole point. Nothing of the writer survives but the files. ---------
    let after = ferrodb(&db, "SELECT * FROM ferro_row_authors;\n");
    assert_no_errors(&after, "the query after the restart");

    let lines = attributed_lines(&after, "restock-agent");
    assert_eq!(
        lines.len(),
        1,
        "expected exactly one attributed row after the restart; the audit trail this product \
         sells does not survive exit:\n{after}"
    );
    let line = &lines[0];

    // Every column of the answer, not just the agent's name: the run, and the model split into
    // name and version from one `MODEL 'name/version'` clause.
    assert!(line.contains("run-42"), "the run id was lost across the restart:\n{after}");
    assert!(line.contains("claude-opus-5"), "the model was lost across the restart:\n{after}");
    assert!(
        line.contains("2026-05"),
        "the model VERSION was lost across the restart, so the split did not survive:\n{after}"
    );
    assert!(line.contains("inv"), "the attributed row does not name its table:\n{after}");

    // The row it attributes is row 1 — the one the agent updated — and not the control row.
    // Asserted on the row-id column rather than by searching the whole line, because `2026-05`
    // and `run-42` both contain digits that would otherwise satisfy a bare `contains`.
    let cols: Vec<&str> = line.split('|').map(|c| c.trim()).collect();
    assert_eq!(
        cols.first().copied(),
        Some("inv"),
        "first column is not the table name; the view's shape changed:\n{after}"
    );
    assert_eq!(
        cols.get(1).copied(),
        Some("1"),
        "the surviving attribution names the wrong row:\n{after}"
    );

    // And the control row really is in the table, unattributed. A view that listed nothing at all
    // would pass the "row 2 is not attributed" reading of the count above for the wrong reason.
    let rows = ferrodb(&db, "SELECT * FROM inv;\n");
    assert_no_errors(&rows, "the post-restart table read");
    assert!(rows.contains("1 | 40"), "the agent's write did not survive the restart:\n{rows}");
    assert!(rows.contains("2 | 20"), "the control row is not in the table:\n{rows}");
}
