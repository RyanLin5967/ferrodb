//! D187 — an index scan must not emit a row whose indexed value is NULL.
//!
//! # The defect
//!
//! `insert.rs` posts a secondary entry unconditionally — `(vals[col].clone(), vals[0].clone())`,
//! with no NULL check — so a row with a NULL indexed value gets an index entry like any other.
//! `catalog/column.rs`'s `type_rank` puts `Value::Null` at rank 0, below every other variant, so
//! that entry sorts at the very front of the tree. Both scans then decide "past the upper bound"
//! with `Value`'s `Ord`:
//!
//! ```text
//! Bound::Included(u) => &key > u,
//! Bound::Excluded(u) => &key >= u,
//! ```
//!
//! `Null > Integer(5)` is **false**, so a NULL entry is never past the bound and the scan emits it.
//! SQL says the opposite: `NULL < 5` is UNKNOWN, and a row whose predicate is UNKNOWN is not in the
//! answer. `executor.rs`'s `compare` gets this right — it returns `Value::Null` the moment either
//! operand is NULL — so correctness today depends entirely on whether a residual `Filter` happens
//! to sit above the scan. When the predicate is fully consumed by the bounds, nothing does.
//!
//! # Why the lower bound is not affected, and is pinned here anyway
//!
//! A lower bound is enforced by the tree descent, not by a comparison in the executor:
//! `range_scan` seeks to the first key `>= lower`, and NULL sorts below every non-NULL literal, so
//! NULL entries are behind the start position and are never read. The asymmetry is real, so the
//! lower-bound direction is a **control** in this file: it must already pass on the unfixed tree.
//! A fix that made it pass "differently" would be a fix aimed at the wrong thing.
//!
//! # Why the tests build plans directly
//!
//! `build_index_scan` costs the index candidate against a filtered sequential scan, and below a few
//! thousand rows the sequential scan wins — correctly. A probe that only ran SQL would be reporting
//! on a heap filter while claiming to test an index, which is the trap
//! `integration_secondary_index_debt.rs` documents at length. So the executor-level cases build the
//! `PhysicalPlan` directly and the one end-to-end SQL case asserts, via `explain_plan`, that an
//! index scan was actually chosen before it believes the row count.

use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use ferrodb::binder::binder::Binder;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::optimizer::optimizer::{explain_plan, lower, optimize, pushdown};
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::planner::physical_plan::PhysicalPlan;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::{ReadView, Snapshot, TxnManager};

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db() -> Db {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("d187.db");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("d187.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { _dir: dir, catalog, bp, txn, session: Session::new() }
}

impl Db {
    fn sql(&mut self, sql: &str) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    }

    fn view(&self) -> Arc<ReadView> {
        Arc::new(ReadView {
            snapshot: Arc::new(Snapshot { high_water: u64::MAX, active: HashSet::new() }),
            txn_id: 0,
        })
    }

    /// Run a hand-built plan, bypassing the cost model, and return the rows it yields.
    fn run_plan(&self, plan: PhysicalPlan) -> Vec<Vec<Value>> {
        let mut exec = lower(plan, &self.catalog, self.bp.clone(), self.view()).expect("lower");
        let mut out = Vec::new();
        while let Some(r) = exec.next() {
            out.push(r.expect("scan").1);
        }
        out
    }

    /// The physical plan the optimizer actually chooses for a SELECT, rendered.
    fn explain(&self, sql: &str) -> String {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        let logical = Binder::new(&self.catalog).bind(stmts.remove(0)).expect("bind");
        let physical = optimize(pushdown(logical), &self.catalog).expect("optimize");
        explain_plan(&physical, &self.catalog)
    }

    fn select_rows(&mut self, sql: &str) -> usize {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        match run(
            stmts.remove(0),
            &mut self.catalog,
            self.bp.clone(),
            self.txn.clone(),
            &mut self.session,
        )
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
        {
            ferrodb::execution::executor::Outcome::Rows(r) => r.len(),
            other => panic!("expected rows, got {:?}", std::mem::discriminant(&other)),
        }
    }
}

/// `t (id, v, w)` with a secondary index on `v`, built BEFORE the inserts so every row is posted.
///
/// - ids 1..=`nulls`      : `v` is NULL          — must never satisfy a range predicate
/// - id  `nulls`+1        : `v` = 1              — the single legitimate `v < 5` match
/// - ids `nulls`+2..      : `v` = 100            — non-matching, so the table is not all-NULL
fn seeded(nulls: i32, highs: i32) -> Db {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER, v INTEGER, w INTEGER);");
    d.sql("CREATE INDEX iv ON t (v);");
    for id in 1..=nulls {
        d.sql(&format!("INSERT INTO t VALUES ({id}, NULL, 1);"));
    }
    d.sql(&format!("INSERT INTO t VALUES ({}, 1, 1);", nulls + 1));
    for k in 0..highs {
        d.sql(&format!("INSERT INTO t VALUES ({}, 100, 1);", nulls + 2 + k));
    }
    d
}

/// A secondary index scan on `v`, with the column-space bounds the optimizer would have derived.
fn sec_scan(lower: Bound<Value>, upper: Bound<Value>) -> PhysicalPlan {
    PhysicalPlan::IndexScan { table: "t".into(), column: 1, lower, upper }
}

/// A primary index scan on `id` (column 0 is always the primary tree).
fn pk_scan(lower: Bound<Value>, upper: Bound<Value>) -> PhysicalPlan {
    PhysicalPlan::IndexScan { table: "p".into(), column: 0, lower, upper }
}

/// `p (id, v)` whose primary key is NULL on exactly one row. A NULL pk is insertable — nothing on
/// this path refuses it — which is what makes the primary scan's copy of this defect reachable.
fn seeded_pk() -> Db {
    let mut d = db();
    d.sql("CREATE TABLE p (id INTEGER, v INTEGER);");
    d.sql("INSERT INTO p VALUES (NULL, 900);");
    for id in 1..=10 {
        d.sql(&format!("INSERT INTO p VALUES ({id}, {});", id * 10));
    }
    d
}

fn v_of(rows: &[Vec<Value>], col: usize) -> Vec<Value> {
    rows.iter().map(|r| r[col].clone()).collect()
}

// ---------------------------------------------------------------------------------------------
// SECONDARY SCAN — upper bound, both directions
// ---------------------------------------------------------------------------------------------

#[test]
fn secondary_excluded_upper_drops_null() {
    let d = seeded(5, 3);
    let rows = d.run_plan(sec_scan(Bound::Unbounded, Bound::Excluded(Value::Integer(5))));
    assert!(
        !v_of(&rows, 1).contains(&Value::Null),
        "`v < 5` emitted a NULL-valued row: NULL < 5 is UNKNOWN, so it is not in the answer. got {:?}",
        v_of(&rows, 1)
    );
    assert_eq!(rows.len(), 1, "exactly one row has v < 5; got {:?}", v_of(&rows, 1));
}

#[test]
fn secondary_included_upper_drops_null() {
    let d = seeded(5, 3);
    let rows = d.run_plan(sec_scan(Bound::Unbounded, Bound::Included(Value::Integer(5))));
    assert!(
        !v_of(&rows, 1).contains(&Value::Null),
        "`v <= 5` emitted a NULL-valued row. got {:?}",
        v_of(&rows, 1)
    );
    assert_eq!(rows.len(), 1, "exactly one row has v <= 5; got {:?}", v_of(&rows, 1));
}

/// A NULL *bound literal* is a second, distinct defect, and it is pinned at the planner.
///
/// `check_comparable` lets a NULL literal through on the stated grounds that "three-valued logic
/// handles it in `compare`". `predicate_to_bounds` then turns it into an ordinary bound, and the
/// index scan's bound test never reaches `compare`. The worst shape is `v >= NULL`, which becomes
/// `(Included(Null), Unbounded)` — a scan starting at the first NULL entry with no upper bound at
/// all, i.e. **the whole table** where SQL answers zero rows. Skipping NULL *entries* does not fix
/// this: `v >= NULL` is UNKNOWN for non-NULL rows too.
///
/// Pinned here rather than through SQL because this is deterministic: `predicate_to_bounds` is a
/// pure function, so it fails before the fix regardless of what the cost model would have chosen.
/// The end-to-end guarantee is `sql_null_literal_predicates_answer_nothing` below.
#[test]
fn null_bound_literal_is_not_turned_into_bounds() {
    use ferrodb::binder::binder::BoundExpr;
    use ferrodb::parser::scanner::TokenType;
    use ferrodb::planner::plan::predicate_to_bounds;

    for op in [
        TokenType::Equal,
        TokenType::Less,
        TokenType::LessEqual,
        TokenType::Greater,
        TokenType::GreaterEqual,
    ] {
        let pred = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Column(0)),
            operator: op,
            right: Box::new(BoundExpr::Literal(Value::Null)),
        };
        assert_eq!(
            predicate_to_bounds(&pred),
            None,
            "`col {op:?} NULL` is UNKNOWN for every row, so it must not become an index bound: \
             turning it into one makes the scan answer a range where SQL answers nothing"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// PRIMARY SCAN — the same class, reachable because a NULL primary key is insertable
// ---------------------------------------------------------------------------------------------

#[test]
fn primary_excluded_upper_drops_null() {
    let d = seeded_pk();
    let rows = d.run_plan(pk_scan(Bound::Unbounded, Bound::Excluded(Value::Integer(5))));
    assert!(
        !v_of(&rows, 0).contains(&Value::Null),
        "`id < 5` emitted a NULL-keyed row. got {:?}",
        v_of(&rows, 0)
    );
    assert_eq!(rows.len(), 4, "ids 1..=4 satisfy id < 5; got {:?}", v_of(&rows, 0));
}

#[test]
fn primary_included_upper_drops_null() {
    let d = seeded_pk();
    let rows = d.run_plan(pk_scan(Bound::Unbounded, Bound::Included(Value::Integer(5))));
    assert!(
        !v_of(&rows, 0).contains(&Value::Null),
        "`id <= 5` emitted a NULL-keyed row. got {:?}",
        v_of(&rows, 0)
    );
    assert_eq!(rows.len(), 5, "ids 1..=5 satisfy id <= 5; got {:?}", v_of(&rows, 0));
}

/// Both scans must also survive a NULL bound reaching them directly — a hand-built plan can still
/// name one even after `predicate_to_bounds` stops producing them, and `lower` does not refuse it.
/// With bounds `(Included(Null), Included(Null))` every key in range is NULL, so the answer is
/// empty once NULL entries are dropped.
#[test]
fn a_null_bound_reaching_either_scan_yields_no_rows() {
    let d = seeded(5, 3);
    let sec = d.run_plan(sec_scan(Bound::Included(Value::Null), Bound::Included(Value::Null)));
    assert_eq!(sec.len(), 0, "secondary: got {:?}", v_of(&sec, 1));

    let p = seeded_pk();
    let pk = p.run_plan(pk_scan(Bound::Included(Value::Null), Bound::Included(Value::Null)));
    assert_eq!(pk.len(), 0, "primary: got {:?}", v_of(&pk, 0));
}

// ---------------------------------------------------------------------------------------------
// CONTROLS — these must pass on the UNFIXED tree. If one of them ever fails first, the fix is
// aimed at the wrong mechanism.
// ---------------------------------------------------------------------------------------------

#[test]
fn control_secondary_lower_bound_already_excludes_null() {
    let d = seeded(5, 3);
    let rows = d.run_plan(sec_scan(Bound::Included(Value::Integer(5)), Bound::Unbounded));
    assert!(
        !v_of(&rows, 1).contains(&Value::Null),
        "the tree descent should already have skipped NULL entries on a lower bound"
    );
    assert_eq!(rows.len(), 3, "the three v=100 rows satisfy v >= 5; got {:?}", v_of(&rows, 1));
}

#[test]
fn control_primary_lower_bound_already_excludes_null() {
    let d = seeded_pk();
    let rows = d.run_plan(pk_scan(Bound::Included(Value::Integer(5)), Bound::Unbounded));
    assert!(
        !v_of(&rows, 0).contains(&Value::Null),
        "the tree descent should already have skipped the NULL key on a lower bound"
    );
    assert_eq!(rows.len(), 6, "ids 5..=10 satisfy id >= 5; got {:?}", v_of(&rows, 0));
}

/// The semantics this fix commits to, pinned so a later blanket skip cannot pass silently.
///
/// A NULL entry is dropped because the row's membership is decided by a **comparison against a
/// bound**, and that comparison is UNKNOWN. With no bound on either end there is no comparison and
/// nothing to be UNKNOWN about, so the NULL rows stay. `predicate_to_bounds` cannot currently
/// produce `(Unbounded, Unbounded)` — every one of its five arms bounds at least one side — so this
/// shape is unreachable from SQL today. It is pinned because the moment a full index scan is added
/// for ordering, a blanket skip would start dropping rows that belong in the answer, and this test
/// is what would say so.
#[test]
fn fully_unbounded_scan_keeps_nulls() {
    let d = seeded(5, 3);
    let rows = d.run_plan(sec_scan(Bound::Unbounded, Bound::Unbounded));
    assert_eq!(
        rows.len(),
        9,
        "an unbounded scan compares against nothing, so all 9 rows stay; got {:?}",
        v_of(&rows, 1)
    );
    assert_eq!(
        v_of(&rows, 1).iter().filter(|v| **v == Value::Null).count(),
        5,
        "the five NULL rows belong in a scan with no bound to be UNKNOWN against"
    );
}

// ---------------------------------------------------------------------------------------------
// END TO END — the reported shape, through real SQL, with the plan asserted first
// ---------------------------------------------------------------------------------------------

/// 500 NULL rows plus one `v = 1` row: `v < 5` must answer 1, and answered 501 before the fix.
///
/// The `explain_plan` assertion is load-bearing. Below a few thousand rows the cost model prefers a
/// filtered sequential scan, whose `Filter` rejects NULLs correctly — so without this assertion a
/// GREEN here would mean "the index was never used", not "the index is right".
#[test]
fn sql_range_predicate_answers_one_row() {
    let mut d = seeded(500, 0);
    d.sql("ANALYZE t;");
    let plan = d.explain("SELECT id FROM t WHERE v < 5;");
    assert!(
        plan.contains("Index scan"),
        "this case only tests the index path if the index path is chosen; plan was:\n{plan}"
    );
    assert_eq!(
        d.select_rows("SELECT id FROM t WHERE v < 5;"),
        1,
        "only the v=1 row satisfies v < 5; the 500 NULL rows are UNKNOWN. plan:\n{plan}"
    );
}

/// The user-facing guarantee behind `null_bound_literal_is_not_turned_into_bounds`.
///
/// Every comparison against a NULL literal is UNKNOWN for every row, so each of these answers
/// nothing. Whether the index or a sequential scan is chosen must not change that — which is the
/// whole point, so this one deliberately does NOT assert a plan.
#[test]
fn sql_null_literal_predicates_answer_nothing() {
    let mut d = seeded(500, 0);
    d.sql("ANALYZE t;");
    for pred in ["v = NULL", "v >= NULL", "v > NULL", "v <= NULL", "v < NULL"] {
        let sql = format!("SELECT id FROM t WHERE {pred};");
        assert_eq!(
            d.select_rows(&sql),
            0,
            "`{pred}` is UNKNOWN for all 501 rows and must answer nothing. plan:\n{}",
            d.explain(&sql)
        );
    }
}

/// The two-conjunct shape. `w = 1` is true for every row, so it changes no answer — it exists to
/// put a residual `Filter` above the scan and prove the fix does not depend on one being there.
#[test]
fn sql_two_conjunct_shape_answers_one_row() {
    let mut d = seeded(500, 0);
    d.sql("ANALYZE t;");
    let plan = d.explain("SELECT id FROM t WHERE w = 1 AND v < 5;");
    assert_eq!(
        d.select_rows("SELECT id FROM t WHERE w = 1 AND v < 5;"),
        1,
        "w = 1 holds for every row, so this is `v < 5` and answers 1. plan:\n{plan}"
    );
}
