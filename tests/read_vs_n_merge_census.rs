//! READ-VS-N arm 2 — the merge-log counters' own fire-check (`bench/read_vs_n/PREREG.md` A3).
//!
//! `MERGE_APPLIED_VISITED` and `MERGE_CELL_INDEX_VISITED` are process-wide statics, so this file is
//! its OWN test binary with ONE test in it: no other test can run a merge in this process and move
//! them. Each counter is forced to move by an event made to happen, and held still by the case
//! that must not move it. Expected values come from the log's length read beside the merge, never
//! from the counter's own arithmetic.
//!
//! The database is opened by `cli::open_database` — the shipped binary's own sequence — and the
//! statements run through `execution::executor::run` under the statement lock, as `run_cli` runs
//! them.

use std::sync::Arc;
use std::time::Duration;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::merge_log_counters;
use ferrodb::cli::cli::{open_database, OpenDatabase};
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;

fn exec(db: &OpenDatabase, sess: &mut Session, sql: &str) -> Outcome {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new())
        .scan_tokens()
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    assert!(parser.errors.is_empty(), "{sql}: {:?}", parser.errors);
    let mut cat = db.catalog.lock();
    run(stmts.remove(0), &mut cat, db.bp.clone(), db.txn.clone(), sess)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// What one `MERGE;` did to the log and to the two counters.
#[derive(Debug)]
struct Seen {
    applied_before: u64,
    applied_after: u64,
    visited: u64,
    cell_visited: u64,
}

/// One agent task: open a session, run `write`, merge. Only the `MERGE;` is bracketed.
fn cycle(db: &OpenDatabase, agent: &str, write: &str) -> Seen {
    let mut sess = Session::with_runtime(Arc::clone(&db.runtime));
    exec(db, &mut sess, &format!("BEGIN AGENT SESSION AS '{agent}';"));
    exec(db, &mut sess, write);
    let applied_before = db.runtime.state_sizes().applied as u64;
    let (v0, c0) = merge_log_counters();
    let out = exec(db, &mut sess, "MERGE;");
    let (v1, c1) = merge_log_counters();
    let applied_after = db.runtime.state_sizes().applied as u64;
    match out {
        Outcome::Agent(AgentOutput::Merge(report)) => assert!(
            report.applied_to_target,
            "{agent}: the merge did not reach the target, so it measured nothing"
        ),
        _ => panic!("{agent}: MERGE did not return a merge report"),
    }
    Seen { applied_before, applied_after, visited: v1 - v0, cell_visited: c1 - c0 }
}

#[test]
fn the_merge_log_counters_follow_the_log_and_the_cell_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("census.db");
    let db = open_database(path.to_str().unwrap(), Duration::from_secs(86_400)).unwrap();
    let mut plain = Session::with_runtime(Arc::clone(&db.runtime));
    exec(&db, &mut plain, "CREATE TABLE m (id INTEGER NOT NULL, v INTEGER);");
    exec(&db, &mut plain, "INSERT INTO m VALUES (1, 1);");

    // A new row each: the log grows, and no cell has any history.
    let a = cycle(&db, "a", "INSERT INTO m VALUES (2, 2);");
    let b = cycle(&db, "b", "INSERT INTO m VALUES (3, 3);");
    assert_eq!(a.applied_before, 0, "a fresh open starts with an empty log: {a:?}");
    assert!(b.applied_before > 0, "the first merge appended nothing, so nothing below can fire: {b:?}");
    for s in [&a, &b] {
        assert_eq!(
            s.visited, s.applied_before,
            "highest_applied_seq visits the whole log once per merge: {s:?}"
        );
        assert_eq!(s.cell_visited, 0, "a brand-new row's cells have no history to read: {s:?}");
    }
    assert_eq!(
        a.applied_after - a.applied_before,
        b.applied_after - b.applied_before,
        "the same write appends the same number of ops: {a:?} {b:?}"
    );

    // The same cell twice: the second merge's `concurrent_op` has one entry of history to probe.
    let c = cycle(&db, "c", "UPDATE m SET v = 7 WHERE id = 1;");
    let d = cycle(&db, "d", "UPDATE m SET v = 9 WHERE id = 1;");
    assert_eq!(c.visited, c.applied_before, "{c:?}");
    assert_eq!(d.visited, d.applied_before, "{d:?}");
    assert!(
        d.cell_visited >= 1,
        "a cell merged once before was probed through D86's index: {d:?}"
    );

    assert_eq!(db.runtime.state_sizes().workspaces, 0, "every merged session was sealed");
    let (_, closed) = db.close();
    closed.unwrap();
}
