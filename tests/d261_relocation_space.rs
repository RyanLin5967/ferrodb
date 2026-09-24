//! D261 (lane `lane_d261_relocation_space.md` §2 T2): an ALTER after a rolled-back shrink loses no
//! row.
//!
//! ALTER's rewrite relocates every row that grows past its page through an UNLOGGED heap
//! (`catalog::alter::commit_rewrite`, `txn: None`), and a relocation deletes the row before it inserts
//! it elsewhere. `d257-review1` R2 infers, at `9aa6968`, that a rolled-back in-place shrink leaves the
//! page directory overstating the page. A relocation that then trusted the figure would find its
//! insert refused after the delete, and nothing undoes an unlogged delete, so the row would be lost.
//!
//! **A guard on this branch, stated** (lane §0.1): since D213 a shrink keeps its slot's capacity and
//! its rollback writes back in place, so this route leaves the directory true. The lost row itself is
//! shown with a planted overstated entry, in
//! `storage::heap_file_manager::tests::an_unlogged_relocation_to_a_page_the_directory_overstates_keeps_its_row`.

use std::path::Path;

use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::db_lock::DbLock;
use ferrodb::wal::recovery::{open_recovered, OpenedDatabase};

/// Rows in the table: at 500 bytes of text each, several pages, so the rewrite relocates.
const ROWS: i32 = 24;

fn exec(sql: &str, o: &mut OpenedDatabase, s: &mut Session) -> Outcome {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut p = Parser::new(tokens);
    let mut stmts = p.parse();
    assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
    run(stmts.remove(0), &mut o.catalog, o.bp.clone(), o.txn.clone(), s).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
}

fn rows(sql: &str, o: &mut OpenedDatabase) -> Vec<Vec<Value>> {
    match exec(sql, o, &mut Session::new()) {
        Outcome::Rows(mut r) => {
            r.sort_by_key(|row| format!("{row:?}"));
            r
        }
        _ => panic!("`{sql}` did not return rows"),
    }
}

fn text(id: i32) -> String {
    format!("{id:03}{}", "x".repeat(497))
}

#[test]
fn an_alter_after_a_rolled_back_shrink_loses_no_row() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("d261.db");
    let lock = DbLock::acquire(Path::new(&db)).unwrap();
    let mut o = open_recovered(&db, &lock).unwrap();
    let mut s = Session::new();
    exec("CREATE TABLE t (id INTEGER NOT NULL, v VARCHAR(600));", &mut o, &mut s);
    for id in 1..=ROWS {
        exec(&format!("INSERT INTO t VALUES ({id}, '{}');", text(id)), &mut o, &mut s);
    }
    // The in-place shrink, rolled back.
    for sql in ["BEGIN;", "UPDATE t SET v = '' WHERE id = 1;", "ROLLBACK;"] {
        exec(sql, &mut o, &mut s);
    }
    assert_eq!(
        rows("SELECT id, v FROM t WHERE id = 1;", &mut o),
        vec![vec![Value::Integer(1), Value::Varchar(text(1))]],
        "premise failed: the rolled-back UPDATE did not leave row 1 as it was"
    );

    exec("ALTER TABLE t ADD COLUMN w INTEGER;", &mut o, &mut s);

    let all = rows("SELECT id, v FROM t;", &mut o);
    assert_eq!(all.len(), ROWS as usize, "the ALTER after a rolled-back shrink lost rows: {} of {ROWS} remain", all.len());
    for id in 1..=ROWS {
        assert_eq!(
            rows(&format!("SELECT id, v FROM t WHERE id = {id};"), &mut o),
            vec![vec![Value::Integer(id), Value::Varchar(text(id))]],
            "row {id} is missing or changed after the ALTER, by key"
        );
    }
}
