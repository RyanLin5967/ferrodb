//! D66 — the planner's relation sets are BITMASKS, and a wide join used to alias them.
//!
//! `optimizer::search_algorithm` tracks which base relations a predicate touches as a bitmask,
//! `1 << relation_of(..)`. Those masks were `u32`, so relation 32 aliased onto relation 0. In a
//! debug build that shift panics ("attempt to shift left with overflow"); in RELEASE it is
//! silent and worse — two distinct relations share a bit, the join-order search treats them as
//! one, and the query returns a cross product or quietly drops a predicate. **A wrong answer, not
//! a crash.** `relations_of` runs BEFORE the `MAX_DP_RELATIONS` fallback, so the cheap left-deep
//! path was affected too.
//!
//! Masks are `u64` now and the relation count is refused above `MAX_JOIN_RELATIONS`. Widening
//! alone would only move the cliff; every fixed-width mask has one. A refusal naming the limit is
//! something a caller can act on, an aliased bit is not.
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::optimizer::search_algorithm::MAX_JOIN_RELATIONS;
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
            .open(dir.path().join("p.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, _dir: dir }
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(format!("{:?}", parser.errors)));
        }
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }
}

/// Build `n` single-row tables and join them all.
fn join_query(db: &mut Db, s: &mut Session, n: usize) -> Result<Outcome, FerroError> {
    for i in 0..n {
        db.exec(&format!("CREATE TABLE r{i} (id INTEGER NOT NULL, v INTEGER);"), s)?;
        // SAME key in every table, so the chained predicate r0.id = rN.id actually matches.
        // An earlier version inserted `id = i`, which made every join empty — the test then
        // "passed" against a cross product too, since 0 rows is 0 rows either way.
        db.exec(&format!("INSERT INTO r{i} VALUES (7, {i});"), s)?;
    }
    let mut sql = String::from("SELECT r0.id FROM r0");
    for i in 1..n {
        sql.push_str(&format!(" JOIN r{i} ON r0.id = r{i}.id"));
    }
    sql.push(';');
    db.exec(&sql, s)
}

/// Past the limit: a NAMED refusal. Never a panic, never a silently aliased mask.
///
/// ⚠ Against the u32 masks this input did not return an error at all — it panicked in debug and
/// returned wrong rows in release, which is why the assertion is on the message and not merely on
/// `is_err()`: "some error" would also be satisfied by an unrelated failure.
#[test]
fn more_relations_than_the_mask_can_hold_is_refused_by_name() {
    let mut db = Db::new();
    let mut s = Session::new();
    // `Outcome` has no `Debug`, so this cannot use `expect_err`.
    let msg = match join_query(&mut db, &mut s, MAX_JOIN_RELATIONS + 2) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("a join wider than the mask must be refused, not planned"),
    };
    assert!(
        msg.contains("relations") && msg.contains(&MAX_JOIN_RELATIONS.to_string()),
        "the refusal must name the limit so a caller can act on it; got: {msg}"
    );
}

/// And the other side of the boundary, so a too-eager guard is caught as well as a missing one.
#[test]
fn a_join_at_the_relation_limit_still_plans() {
    let mut db = Db::new();
    let mut s = Session::new();
    join_query(&mut db, &mut s, MAX_JOIN_RELATIONS)
        .unwrap_or_else(|e| panic!("exactly {MAX_JOIN_RELATIONS} relations must still plan: {e}"));
}

/// The aliasing itself: relation 32 must not collide with relation 0.
///
/// 33 relations is past `u32`'s width and well past `MAX_DP_RELATIONS = 12`, so this exercises the
/// LEFT-DEEP path — the one `relations_of` reaches before the DP fallback is even consulted.
#[test]
fn relation_thirty_two_does_not_alias_onto_relation_zero() {
    let mut db = Db::new();
    let mut s = Session::new();
    let out = join_query(&mut db, &mut s, 33)
        .unwrap_or_else(|e| panic!("33 relations must plan and run: {e}"));
    // Every table holds exactly one row and the predicates chain r0.id = rN.id, so the answer is
    // one row. An aliased mask drops predicates and yields a cross product instead.
    let n = match out {
        Outcome::Rows(rows) => rows.len(),
        Outcome::Table(t) => t.rows.len(),
        _ => panic!("expected rows from a SELECT"),
    };
    assert_eq!(n, 1, "33 joined single-row tables must yield exactly one row, not a cross product");
}
