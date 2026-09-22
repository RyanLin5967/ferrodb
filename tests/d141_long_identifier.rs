//! D141 — a table name too long for the catalog page's one-byte length field, driven from SQL.
//!
//! The unit tests in `catalog::catalog_page` prove the encoder refuses. This file proves the thing
//! that makes that worth doing: that an **ordinary `CREATE TABLE`** reaches it. D141 claimed there
//! is no identifier-length limit anywhere in `src/`, and a guard at the bottom of the stack is only
//! interesting if the top of the stack lets the value through.
//!
//! Both tests are laws, not error codes: a future build may impose an identifier limit further up,
//! or widen the on-disk format, and either would still satisfy what is asserted here.

use std::fs::OpenOptions;
use std::sync::Arc;

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
            .open(dir.path().join("d141.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d141.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, _dir: dir }
    }

    fn exec(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        let mut s = Session::new();
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut s)
    }
}

/// A 300-byte table name — D141's own example — must not leave a catalog that misreports itself.
///
/// The law: after the statement, whatever it answered, every table the catalog lists must be a
/// table that was actually created. Before the fix the encoder wrote a length prefix of 44 and then
/// all 300 bytes, and everything after that entry was parsed from the wrong offset.
#[test]
fn a_three_hundred_byte_table_name_does_not_corrupt_the_catalog() {
    let mut db = Db::new();
    let long = "a".repeat(300);

    let created = db.exec(&format!("CREATE TABLE {long} (id INTEGER NOT NULL);")).is_ok();

    // Either it was refused, or it is there under its own full name — but not a third thing.
    let listed: Vec<String> = db.catalog.tables.keys().cloned().collect();
    for name in &listed {
        assert!(
            name.len() <= 255 || *name == long,
            "the catalog lists a table nobody created: {:?} ({} bytes)",
            &name[..name.len().min(40)],
            name.len()
        );
    }
    if !created {
        assert!(
            !listed.contains(&long),
            "CREATE TABLE refused, but the refused table is still in the catalog — the next DDL \
             will re-serialize it and fail too, so one bad statement wedges the database"
        );
    }
}

/// The refusal has to be a refusal and not a wedge: the database is still usable afterwards.
///
/// This is the half that separates "refused cleanly" from "poisoned". `Catalog::create_table`
/// inserts into its in-memory map and *then* persists, so a persist that refuses leaves an entry
/// that every later persist trips over — a single over-long `CREATE TABLE` took out every
/// subsequent DDL until restart. Measured, then fixed by `Catalog::persist_or_undo`.
///
/// The first assertion is what stops this passing vacuously: without it the test would be
/// satisfied by the long name being *accepted*, which is the corruption it exists to rule out.
#[test]
fn a_refused_create_table_does_not_wedge_the_next_one() {
    let mut db = Db::new();
    let long = "a".repeat(300);

    let err = db
        .exec(&format!("CREATE TABLE {long} (id INTEGER NOT NULL);"))
        .err()
        .expect("a 300-byte table name must be refused, not written truncated");
    assert!(
        matches!(&err, FerroError::Unrepresentable { len: 300, limit: 255, .. }),
        "expected the encoder's refusal naming the length it could not express, got: {err:?}"
    );

    db.exec("CREATE TABLE ok_after (id INTEGER NOT NULL);")
        .expect("an ordinary CREATE TABLE after a refused one must still work");
    assert!(db.catalog.get_table("ok_after").is_some());
}

/// A refusal is repeatable, not a slow leak.
///
/// `Catalog::persist` returns through `?` without unpinning the catalog page it fetched, so every
/// refusal leaks one pin — a pre-existing shape on every error path there, which D141 made
/// reachable from ordinary SQL for the first time. This bounds what that costs: if the leak
/// mattered, several hundred refusals would break the pool or the page, and the ordinary statement
/// at the end would not get through.
#[test]
fn many_refused_statements_leave_the_database_working() {
    let mut db = Db::new();
    let long = "a".repeat(300);

    for i in 0..300 {
        let err = db.exec(&format!("CREATE TABLE {long}{i} (id INTEGER NOT NULL);")).err();
        assert!(err.is_some(), "refusal {i} did not refuse");
    }

    db.exec("CREATE TABLE still_fine (id INTEGER NOT NULL);")
        .expect("300 refusals must not cost the database its ability to accept a DDL");
    assert!(db.catalog.get_table("still_fine").is_some());
    assert_eq!(db.catalog.tables.len(), 1, "a refused table must not be in the catalog");
}

/// The boundary is a boundary and not a wall: the longest identifier the format CAN hold works
/// end to end, through the parser, the executor and the catalog.
#[test]
fn the_longest_expressible_table_name_still_works_from_sql() {
    let mut db = Db::new();
    let max = "a".repeat(255);

    db.exec(&format!("CREATE TABLE {max} (id INTEGER NOT NULL);"))
        .expect("255 bytes is exactly what the length prefix holds; it must NOT be refused");
    assert!(db.catalog.get_table(&max).is_some(), "the table is not under its own full name");
}
