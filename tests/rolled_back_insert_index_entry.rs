//! A rolled-back INSERT leaves its primary-index entry pointing at a slot the rollback freed.
//!
//! INFERRED from source, never run; this file is the measurement. Found while designing the
//! reused-key fix (`tests/reused_key_old_snapshot.rs`), whose relocation arm inherits it. It is
//! not fixed there.
//!
//! - `TxnManager::abort` undoes heap records only (`HeapInsert` -> `undo_insert` ->
//!   `Page::delete`). Index pages are not logged and nothing on the abort path touches a tree.
//! - So after `BEGIN; INSERT (5, ..); ROLLBACK;` the primary entry for 5 still points at the freed
//!   slot, and `Page::read` answers `SlotDeleted` for it.
//! - `IndexScan::next` propagates a `heap.read` error, so `WHERE id = 5` fails rather than
//!   returning no rows. `Insert::execute` reads the entry's slot before its uniqueness check and
//!   propagates the same error, so key 5 cannot be inserted again.
//!
//! `executor::tests::test_block_rollback_discards_everything` passes regardless: it reads the table
//! with `SELECT id FROM t;`, a sequential scan, which skips the freed slot.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
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
            .open(dir.path().join("rollback.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("rollback.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, _dir: dir }
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }
}

#[test]
fn a_key_whose_insert_rolled_back_is_absent_by_key_and_insertable_again() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut s);
    db.ok("INSERT INTO t VALUES (1, 10);", &mut s);
    match db.ok("EXPLAIN SELECT id, v FROM t WHERE id = 5;", &mut s) {
        Outcome::Explain(plan) => assert!(
            plan.contains("Index scan on t (col 0"),
            "premise failed: a lookup of key 5 does not use the primary index:\n{plan}"
        ),
        _ => panic!("EXPLAIN did not explain"),
    }

    db.ok("BEGIN;", &mut s);
    db.ok("INSERT INTO t VALUES (5, 50);", &mut s);
    db.ok("ROLLBACK;", &mut s);

    match db.exec("SELECT id, v FROM t WHERE id = 5;", &mut s) {
        Ok(Outcome::Rows(r)) => assert!(r.is_empty(), "a rolled-back row is visible by key: {r:?}"),
        Ok(_) => panic!("a SELECT did not return rows"),
        Err(e) => panic!("a lookup of a key whose INSERT rolled back failed instead of finding nothing: {e}"),
    }
    db.exec("INSERT INTO t VALUES (5, 51);", &mut s)
        .unwrap_or_else(|e| panic!("a key whose INSERT rolled back cannot be inserted again: {e}"));
    match db.ok("SELECT id, v FROM t WHERE id = 5;", &mut s) {
        Outcome::Rows(r) => assert_eq!(r, vec![vec![Value::Integer(5), Value::Integer(51)]]),
        _ => panic!("a SELECT did not return rows"),
    }
}
