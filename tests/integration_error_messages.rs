//! D13 — the errors a user can actually trigger from SQL should say what to do.
//!
//! An error that names only the category ("value count != column count") tells someone that
//! something is wrong and leaves them to find out what. These tests assert the *specifics* are
//! present — the offending table, the offending value, the counts — because that is the part that
//! turns a message into an action.
//!
//! The concrete defect that motivated this row: `bind_scan` returned the string
//! `"unknown table: {}"` **as a literal**, with no `format!`. It printed the braces and never named
//! the table. Every other site in the codebase formats it, which is why it survived — it looked
//! right in a grep.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("err.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("err.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    fn exec(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let mut s = Session::with_runtime(self.runtime.clone());
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut s)
    }

    fn ok(&mut self, sql: &str) {
        self.exec(sql).unwrap_or_else(|e| panic!("{sql} failed: {e}"));
    }

    fn err(&mut self, sql: &str) -> String {
        match self.exec(sql) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected an error from: {sql}"),
        }
    }
}

fn seeded() -> Db {
    let mut db = Db::new();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);");
    db.ok("INSERT INTO inventory VALUES (1, 100);");
    db
}

/// The literal-brace defect, pinned so it cannot come back.
#[test]
fn an_unknown_table_is_named_and_the_known_ones_are_listed() {
    let mut db = seeded();
    let msg = db.err("SELECT * FROM invntory;");

    assert!(
        !msg.contains("{}"),
        "the message still contains a literal format placeholder: {msg}"
    );
    assert!(msg.contains("invntory"), "the message must name the table asked for: {msg}");
    assert!(
        msg.contains("inventory"),
        "listing the tables that DO exist is what makes a typo self-correcting: {msg}"
    );
}

#[test]
fn an_unknown_table_in_an_empty_database_says_there_are_none_rather_than_listing_nothing() {
    let mut db = Db::new();
    let msg = db.err("SELECT * FROM anything;");
    assert!(msg.contains("anything"), "must name the table: {msg}");
    assert!(
        msg.contains("no tables") || msg.contains("CREATE TABLE"),
        "an empty database should say so, not print an empty list: {msg}"
    );
}

#[test]
fn a_wrong_value_count_reports_both_counts_and_the_table() {
    let mut db = seeded();
    let msg = db.err("INSERT INTO inventory VALUES (2, 200, 300);");

    assert!(msg.contains("inventory"), "must name the table: {msg}");
    assert!(msg.contains('2'), "must say how many columns the table has: {msg}");
    assert!(msg.contains('3'), "must say how many values were given: {msg}");
}

#[test]
fn a_duplicate_primary_key_names_the_value_and_the_column() {
    let mut db = seeded();
    let msg = db.err("INSERT INTO inventory VALUES (1, 999);");

    assert!(msg.contains("inventory"), "must name the table: {msg}");
    assert!(msg.contains('1'), "must name the offending key value: {msg}");
    assert!(msg.contains("id"), "must name the column that is the key: {msg}");
    assert!(
        msg.to_lowercase().contains("update"),
        "should say what to do instead, not only what went wrong: {msg}"
    );
}

#[test]
fn a_null_into_a_not_null_column_names_the_column_the_table_and_the_constraint() {
    let mut db = seeded();
    let msg = db.err("INSERT INTO inventory VALUES (NULL, 5);");

    assert!(msg.contains("id"), "must name the column: {msg}");
    assert!(msg.contains("inventory"), "must name the table: {msg}");
    assert!(msg.contains("NOT NULL"), "must name the constraint that was violated: {msg}");
}

/// Errors must not cost the connection. A message that arrives on a session you can no longer use
/// is not much of an improvement over a crash.
#[test]
fn the_session_still_works_after_each_of_these_errors() {
    let mut db = seeded();
    for bad in [
        "SELECT * FROM nope;",
        "INSERT INTO inventory VALUES (2, 200, 300);",
        "INSERT INTO inventory VALUES (1, 999);",
        "INSERT INTO inventory VALUES (NULL, 5);",
    ] {
        let _ = db.err(bad);
        match db.exec("SELECT qty FROM inventory WHERE id = 1;") {
            Ok(Outcome::Rows(rows)) => {
                assert_eq!(rows.len(), 1, "the table changed after a rejected statement: {bad}")
            }
            other => panic!("the session was unusable after `{bad}`: {}", match other {
                Err(e) => e.to_string(),
                Ok(_) => "unexpected outcome".into(),
            }),
        }
    }
}

/// The variant and its display string were spelled "contraint", and it reached users on every
/// constraint violation. Pinned, because a typo in an error message is invisible to every test
/// that only checks the message's *contents*.
#[test]
fn constraint_errors_are_spelled_correctly() {
    let mut db = seeded();
    let msg = db.err("INSERT INTO inventory VALUES (1, 999);");
    assert!(msg.contains("constraint error"), "got: {msg}");
    assert!(!msg.contains("contraint"), "the old misspelling is back: {msg}");
}
