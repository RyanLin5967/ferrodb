//! D181 — an **exact cost tie** between two index candidates must not be broken by typing order.
//!
//! # Why this file exists separately from the order-swap test
//!
//! `tests/d181_conjunct_order_independence.rs` uses a fixture where the two candidates cost 17.01
//! and 4018.0. That margin is enormous and deliberate: it makes that test independent of how ties
//! are resolved, so it measures one thing only. This file measures the other thing, on a fixture
//! built so the two candidates cost **exactly** the same.
//!
//! A strict `if candidate_cost < best_cost` leaves the first-typed candidate holding a tie, so
//! typing order would still decide the plan in that case. Cost is an ESTIMATE — two plans the model
//! scores identically can examine very different numbers of rows — so that residue is not cosmetic,
//! and a counter-based swap test over a tied fixture would fail intermittently rather than never.
//! `build_index_scan` therefore breaks ties on `candidate_key` — `(column, lower, upper)` — which
//! is a property of the candidate and not of where it sat in the predicate.
//!
//! # The fixture makes the tie EXACT, not approximate
//!
//! `a` and `b` are seeded with identical data, so `ANALYZE` gives them identical `ColumnStats`, and
//! `cost` reads nothing else about a column. Both are `column != 0`, so both take the same branch.
//! The two candidates therefore evaluate the SAME formula on the SAME inputs and produce bitwise
//! equal `f64`s. If a future change makes the cost model read something about a column that these
//! two do not share, the tie stops being exact and this test stops testing ties — the assertion in
//! `the_fixture_really_is_a_tie` is what makes that visible instead of silent.
//!
//! Pre-registered in `bench/d181_prereg.txt` case (c) before this test was first run: both typing
//! orders choose column 1. Before the tie-break key landed, `a = 5 AND b = 5` chose column 1 and
//! `b = 5 AND a = 5` chose column 2.
//!
//! These tests read `EXPLAIN` and no counters, so they are safe to run in parallel and more than
//! one may live here.

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

/// 1000 rows with `a = b = id` makes both columns unique, so each equality estimates ONE row and
/// each candidate costs 17.01 — under the sequential scan's `table_pages + 20 >= 21`. Both
/// candidates must beat the incumbent or the tie is never consulted and this test is vacuous;
/// `both_candidates_beat_the_sequential_scan` is what checks that.
const ROWS: i32 = 1000;
const KEY: i32 = 5;

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
            .open(dir.path().join("d181t.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d181t.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, _dir: dir }
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        assert!(parser.errors.is_empty(), "parse errors in `{sql}`: {:?}", parser.errors);
        assert_eq!(stmts.len(), 1, "one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn plan(&mut self, sql: &str, s: &mut Session) -> String {
        match self.ok(&format!("EXPLAIN {sql}"), s) {
            Outcome::Explain(t) => t,
            _ => panic!("EXPLAIN {sql}: expected a plan"),
        }
    }

    fn row_count(&mut self, sql: &str, s: &mut Session) -> usize {
        match self.ok(sql, s) {
            Outcome::Rows(r) => r.len(),
            _ => panic!("{sql}: expected Rows"),
        }
    }
}

/// `t (id, a, b, pad)` where `a` and `b` hold IDENTICAL data, both indexed, `ANALYZE` run.
fn fixture() -> (Db, Session) {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, a INTEGER, b INTEGER, pad VARCHAR(16));", &mut s);
    for i in 0..ROWS {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {i}, {i}, 'row');", ), &mut s);
    }
    db.ok("CREATE INDEX ix_a ON t (a);", &mut s);
    db.ok("CREATE INDEX ix_b ON t (b);", &mut s);
    db.ok("ANALYZE t;", &mut s);
    (db, s)
}

/// The access-path line of a plan — the BOTTOM of the tree, not the top. `EXPLAIN` renders a tree
/// and its first line is always the `Projection`, which says nothing about which index ran.
fn access_path(plan: &str) -> String {
    plan.lines()
        .map(str::trim)
        .find(|l| l.starts_with("Index scan") || l.starts_with("Sequential scan"))
        .unwrap_or_else(|| panic!("no access path in plan:\n{plan}"))
        .to_string()
}

/// The fixture is only a TIE test while the two candidates really cost the same. Asserted directly
/// against the engine's own `EXPLAIN`, which prints each node's cost.
#[test]
fn the_fixture_really_is_a_tie() {
    let (mut db, mut s) = fixture();
    // One conjunct each, so the plan for each is the bare candidate with no residual filter and
    // `EXPLAIN` prints its cost unmixed.
    let only_a = access_path(&db.plan(&format!("SELECT id FROM t WHERE a = {KEY};"), &mut s));
    let only_b = access_path(&db.plan(&format!("SELECT id FROM t WHERE b = {KEY};"), &mut s));

    let cost_of = |line: &str| -> String {
        line.rsplit_once("cost=")
            .unwrap_or_else(|| panic!("no cost in `{line}`"))
            .1
            .trim_end_matches(')')
            .to_string()
    };
    assert_eq!(
        cost_of(&only_a),
        cost_of(&only_b),
        "the two candidates no longer cost the same, so this file is no longer testing a tie:\n  \
         a: {only_a}\n  b: {only_b}\n\
         Something in the cost model started reading a property of a column that `a` and `b` do \
         not share. Re-derive bench/d181_prereg.txt case (c) before trusting the tie assertion."
    );
    assert!(only_a.starts_with("Index scan on t (col 1"), "a = {KEY} did not use a's index: {only_a}");
    assert!(only_b.starts_with("Index scan on t (col 2"), "b = {KEY} did not use b's index: {only_b}");
}

/// Both tied candidates must beat the sequential incumbent, or the tie is never reached and
/// `a_cost_tie_is_broken_the_same_way_whichever_order_it_is_typed` passes for the wrong reason —
/// two sequential scans also agree with each other.
#[test]
fn both_candidates_beat_the_sequential_scan() {
    let (mut db, mut s) = fixture();
    for col in ["a", "b"] {
        let path = access_path(&db.plan(&format!("SELECT id FROM t WHERE {col} = {KEY};"), &mut s));
        assert!(
            path.starts_with("Index scan"),
            "`{col} = {KEY}` chose {path}; if the sequential scan wins on this fixture then the \
             tie between the two index candidates is never consulted"
        );
    }
}

/// The assertion this file is for.
#[test]
fn a_cost_tie_is_broken_the_same_way_whichever_order_it_is_typed() {
    let (mut db, mut s) = fixture();
    let a_first = format!("SELECT id FROM t WHERE a = {KEY} AND b = {KEY};");
    let b_first = format!("SELECT id FROM t WHERE b = {KEY} AND a = {KEY};");

    let path_a = access_path(&db.plan(&a_first, &mut s));
    let path_b = access_path(&db.plan(&b_first, &mut s));

    assert_eq!(
        path_a, path_b,
        "an EXACT cost tie was broken by the order the predicate was typed:\n  \
         `a = {KEY} AND b = {KEY}` -> {path_a}\n  \
         `b = {KEY} AND a = {KEY}` -> {path_b}\n\
         `build_index_scan` must break ties on `candidate_key`, which reads the candidate and not \
         its position in the conjunct list."
    );
    // Pre-registered in bench/d181_prereg.txt case (c): the lower column id wins, so column 1.
    // Asserted as a constant rather than as "whatever the other arm said" — two arms that agree on
    // a wrong answer agree just as well as two that agree on the right one.
    assert!(
        path_a.starts_with("Index scan on t (col 1"),
        "expected the tie to resolve to column 1 as pre-registered, got {path_a}"
    );

    // And the tie-break must not have cost anyone the right answer.
    assert_eq!(db.row_count(&a_first, &mut s), 1, "a_first returned the wrong number of rows");
    assert_eq!(db.row_count(&b_first, &mut s), 1, "b_first returned the wrong number of rows");
}
