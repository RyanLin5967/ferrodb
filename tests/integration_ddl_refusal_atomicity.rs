//! A refused DDL statement must not have half-happened.
//!
//! Sibling of `integration_alter_refusal_safety` (I19, a refused `ALTER` that had already destroyed
//! rows) and of the I21 fork hole, and the general case of adversarial fixture
//! `a8_a_refused_create_table_still_creates_the_table_and_logs_nothing`.
//!
//! `CREATE TABLE` and `DROP TABLE` both mutate the catalog and then call `TxnManager::checkpoint`,
//! which refuses while ANY transaction is attached. Done in that order the refusal arrives after
//! the irreversible half: the CREATE returns `Err` over a table that exists, and the DROP returns
//! `Err` over a table that is gone. Neither logs a `Ddl` record — those are written *after* the
//! checkpoint — so `log_ddl` never puts the table into the retained `schema_log` and no later
//! checkpoint re-declares it either. The damage is permanent and invisible to any self-describing
//! consumer.
//!
//! The refusal is reachable from a perfectly ordinary session because the executor's
//! `DDL not allowed in txn` guard is per-SESSION, while checkpoint admissibility is GLOBAL: the
//! transaction that blocks the checkpoint belongs to a different session, so the guard passes and
//! the checkpoint still refuses.

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
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ddl.db");
    let file = std::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true).open(&path).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("ddl.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { _dir: dir, catalog, bp, txn, session: Session::new() }
}

impl Db {
    fn try_sql(&mut self, sql: &str) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse errors in `{sql}`: {:?}", p.errors);
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        let mut session = std::mem::replace(&mut self.session, Session::new());
        let out = run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut session);
        self.session = session;
        out
    }
    fn sql(&mut self, sql: &str) -> Outcome {
        self.try_sql(sql).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }
    /// Open a transaction in a SEPARATE session and leave it attached, which is what makes a
    /// checkpoint inadmissible without tripping the executor's per-session DDL guard.
    fn hold_a_txn_elsewhere(&mut self) -> Session {
        let mut other = Session::new();
        for sql in ["BEGIN;", "INSERT INTO inv VALUES (1, 10);"] {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let mut stmts = p.parse();
            run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut other)
                .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
        }
        other
    }
}

#[test]
fn a_refused_create_table_does_not_create_the_table() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    let _held = d.hold_a_txn_elsewhere();

    let err = match d.try_sql("CREATE TABLE ghost (id INTEGER NOT NULL, v INTEGER);") {
        Err(e) => e,
        Ok(_) => panic!("the CREATE was not refused; this test measures the wrong thing"),
    };
    assert!(
        err.to_string().contains("checkpoint with active txns"),
        "refused for an unexpected reason, so this test is no longer measuring the refusal it was \
         written for: {err}"
    );
    assert!(
        d.catalog.get_table("ghost").is_none(),
        "`CREATE TABLE ghost` returned `{err}` and created the table anyway. A refusal that has \
         already half-happened is worse than a failure: no `Ddl` record is logged for it, so the \
         table is invisible to every self-describing consumer permanently."
    );
    // And the refusal left the database usable, rather than wedged half-way through a checkpoint.
    assert!(d.catalog.get_table("inv").is_some(), "the refused CREATE damaged an unrelated table");
}

#[test]
fn a_refused_drop_table_does_not_drop_the_table() {
    let mut d = db();
    d.sql("CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);");
    d.sql("INSERT INTO inv VALUES (7, 70);");
    let _held = d.hold_a_txn_elsewhere();

    let err = match d.try_sql("DROP TABLE inv;") {
        Err(e) => e,
        Ok(_) => panic!("the DROP was not refused; this test measures the wrong thing"),
    };
    assert!(
        err.to_string().contains("checkpoint with active txns"),
        "refused for an unexpected reason, so this test is no longer measuring the refusal it was \
         written for: {err}"
    );
    assert!(
        d.catalog.get_table("inv").is_some(),
        "`DROP TABLE inv` returned `{err}` and dropped the table anyway. No `DROP_TABLE` record is \
         logged for it either, so a consumer keeps the table in its own schema forever and simply \
         never hears of it again."
    );
    // Still readable: the table is genuinely present, not a husk left behind by a partial drop.
    match d.sql("SELECT id, qty FROM inv;") {
        Outcome::Rows(rows) => assert_eq!(rows.len(), 1, "the refused DROP lost the table's rows"),
        _ => panic!("expected rows back from the surviving table; the refused DROP left it there \
                     but unreadable"),
    }
}
