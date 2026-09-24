//! D261 review 1's F4 (lane `lane_d261_relocation_space.md` §4 T5): recovery's undo of a loser ADDS
//! the directory entry the crash lost.
//!
//! The page directory is not logged, so after a crash a page added since the last checkpoint is not
//! listed. Recovery undoes its losers BEFORE its directory repair lists such pages, and since D261
//! every undo tells the directory the page's free space. It does so through
//! `HeapFileManager::set_directory_entry`, which adds the entry when there is none. A plain update
//! would meet `KeyNotFound` there, and count and print a failure for an undo that stands.
//!
//! Its own binary, because `DIRECTORY_UPDATE_FAILURES` is process-wide and another test's count
//! would hide this one's.

use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::db_lock::DbLock;
use ferrodb::wal::recovery::{open_recovered, OpenedDatabase};
use ferrodb::wal::txn::directory_update_failures;

fn exec(sql: &str, o: &mut OpenedDatabase, s: &mut Session) -> Outcome {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let mut stmts = p.parse();
    assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
    run(stmts.remove(0), &mut o.catalog, o.bp.clone(), o.txn.clone(), s).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
}

#[test]
fn a_losers_undo_at_recovery_adds_the_directory_entry_the_crash_lost() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("d261_undo_entry.db");
    {
        let lock = DbLock::acquire(&db).unwrap();
        let mut o = open_recovered(&db, &lock).unwrap();
        let mut plain = Session::new();
        exec("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut o, &mut plain);
        exec("CREATE TABLE s (id INTEGER NOT NULL, v INTEGER);", &mut o, &mut plain);
        // The loser: its insert is `t`'s first row, so it allocates `t`'s first data page. The
        // directory page that lists it is changed only in the pool; the last checkpoint was the
        // CREATE's.
        let mut loser = Session::new();
        exec("BEGIN;", &mut o, &mut loser);
        exec("INSERT INTO t VALUES (1, 10);", &mut o, &mut loser);
        // A committed write elsewhere flushes the log, and with it the loser's record.
        exec("INSERT INTO s VALUES (1, 1);", &mut o, &mut plain);
        // The crash: the loser never ends, and no checkpoint runs.
    }
    let before = directory_update_failures();
    let lock = DbLock::acquire(&db).unwrap();
    let mut o = open_recovered(&db, &lock).expect("the open after the crash failed");
    assert!(o.recovered, "premise failed: the log held nothing to recover, so no loser was undone");
    assert_eq!(
        directory_update_failures(),
        before,
        "recovery's undo of the loser counted a failed directory update: it did not add the entry the crash lost"
    );
    match exec("SELECT id FROM t;", &mut o, &mut Session::new()) {
        Outcome::Rows(rows) => assert!(rows.is_empty(), "the loser's row survived the recovery: {rows:?}"),
        _ => panic!("SELECT did not return rows"),
    }
}
