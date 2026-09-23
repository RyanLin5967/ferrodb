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

    /// Rows a DML statement reported changing.
    fn affected(&mut self, sql: &str) -> usize {
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
            ferrodb::execution::executor::Outcome::Affected(n) => n,
            other => panic!("expected an affected count, got {:?}", std::mem::discriminant(&other)),
        }
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

/// The strictly-excluded lower bound, which takes a different descent path: `range_scan` uses
/// `leaf.upper_bound(k)` (first key `> k`) rather than `binary_search`. NULL still sorts beneath
/// the start position, so this is a control too.
///
/// Only the PRIMARY scan can be asked this on `main`. `secondary_scan_lower` returns `None` for a
/// strictly excluded bound and `lower` refuses with "lower bound sec index isn't supported", so the
/// secondary equivalent is unbuildable here — it becomes reachable under D179, which enforces the
/// exclusion in the executor instead.
#[test]
fn control_primary_excluded_lower_bound_already_excludes_null() {
    let d = seeded_pk();
    let rows = d.run_plan(pk_scan(Bound::Excluded(Value::Integer(5)), Bound::Unbounded));
    assert!(
        !v_of(&rows, 0).contains(&Value::Null),
        "the tree descent should already have skipped the NULL key on a strict lower bound"
    );
    assert_eq!(rows.len(), 5, "ids 6..=10 satisfy id > 5; got {:?}", v_of(&rows, 0));
}

/// ⚠ **THIS IS A PREMISE TEST, NOT A CODE-PATH TEST. DO NOT DELETE IT AS REDUNDANT.**
///
/// What it guards is a REACHABILITY PREMISE about `predicate_to_bounds`, not the branch it happens
/// to execute. The premise: **no planner-built scan is unbounded on both ends** — all five arms of
/// `predicate_to_bounds` bound at least one side, `build_index_scan` is the only non-test producer
/// of a `PhysicalPlan::IndexScan`, and `IS NULL` does not exist anywhere in the parser, binder or
/// planner. That premise is why `skip_nulls` is `true` for every real query today, and therefore
/// why an unconditional skip would ALSO have been correct.
///
/// The semantics the fix commits to: a NULL entry is dropped because the row's membership is
/// decided by a **comparison against a bound**, and that comparison is UNKNOWN. With no bound on
/// either end there is no comparison, nothing to be UNKNOWN about, and the NULL rows belong.
///
/// So if someone adds a full index scan for ordering, or any arm that yields an unbounded side,
/// **this test is what tells them the premise changed** — and at that moment a blanket skip would
/// silently start dropping rows that belong in the answer. It is also the only exercise of
/// `skip_nulls == false`, so deleting it leaves that branch untested. Both reasons are load-bearing;
/// the first is the one a reader is likely to miss.
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

/// A table the cost model will actually choose the INDEX for. Both dimensions are load-bearing.
///
/// The first attempt at this seeded 500 NULLs plus a single `v = 1` and got a sequential scan:
///
/// ```text
///   Filter (#1 < 5) (rows=125 cost=15.02)
///     Sequential scan on t (rows=501 cost=10.01)
/// ```
///
/// Two independent reasons, both visible in `cost_model.rs`:
///
/// 1. **`analyze` computes min/max over NON-NULL values only** — NULLs are counted into `nulls` and
///    never pushed into `per_col`. With one non-NULL row, `min == max`, and `bound_selectivity`
///    bails to `DEFAULT_RANGE_SELECTIVITY = 0.25`. That is the `rows=125` above: 501 * 0.25. So the
///    `v` values must SPREAD, or the estimate cannot be selective no matter how big the table is.
/// 2. **A sequential scan is genuinely cheaper on a small table**, and correctly so. The table must
///    be big enough that `table_pages + rows * CPU_TUPLE_COST` exceeds the index's
///    `height * RANDOM_PAGE_COST + leaf_pages + rows * RANDOM_PAGE_COST * 2`.
///
/// 500 NULLs + one `v = 1` + 2500 rows spread to `v = 7597` satisfies both, and lands the reported
/// numbers exactly: **501 rows returned where 1 is correct.**
fn seeded_wide() -> Db {
    let mut d = db();
    d.sql("CREATE TABLE t (id INTEGER, v INTEGER, w INTEGER);");
    d.sql("CREATE INDEX iv ON t (v);");
    let mut id = 1;
    for _ in 0..500 {
        d.sql(&format!("INSERT INTO t VALUES ({id}, NULL, 1);"));
        id += 1;
    }
    d.sql(&format!("INSERT INTO t VALUES ({id}, 1, 1);"));
    id += 1;
    for k in 0..2500 {
        d.sql(&format!("INSERT INTO t VALUES ({id}, {}, 1);", 100 + 3 * k));
        id += 1;
    }
    d.sql("ANALYZE t;");
    d
}

/// The reported defect, end to end, through real SQL on a plan that really is an index scan.
///
/// One fixture, because building it is 3001 statements and all four questions are about the same
/// table. The `explain_plan` assertion on the first is load-bearing: below a few thousand rows the
/// cost model prefers a filtered sequential scan whose `Filter` rejects NULLs correctly, so a GREEN
/// without it would mean "the index was never used", not "the index is right". That is not
/// hypothetical — it is what the first version of this test actually did.
/// ⛔ **THE DEFECT IS NOT CONFINED TO READS. THIS IS A WRONG WRITE.**
///
/// D178 stopped `UPDATE`/`DELETE` building their own scan and routed them through the same
/// `optimize`/`lower` path `SELECT` uses — `plan::build_scan` ends in
/// `lower(optimize(pushdown(logical), catalog)?, ..)`, deliberately, so there is exactly ONE
/// planning path. The consequence for D187 is that both scans are reached by DML, and the same
/// predicate that returns 501 rows to a `SELECT` hands 501 rows to a `DELETE`.
///
/// So before the fix, `DELETE FROM t WHERE v < 5` **destroys the 500 NULL-valued rows** alongside
/// the one row that actually matches. That is data loss from a predicate those rows do not satisfy,
/// and it is a strictly more serious failure than the wrong answer this row was opened for.
///
/// The `affected` count is the instrument, not the surviving row count, because it reports what the
/// scan handed the writer — the same number the `SELECT` shapes above measure, taken on the write
/// path. The survivor count is asserted too, so a bug that reports 1 and deletes 501 cannot pass.
#[test]
fn sql_delete_does_not_destroy_null_rows() {
    let mut d = seeded_wide();

    // Same predicate, same table, same statistics as the SELECT case, so `build_index_scan` makes
    // the same choice. Asserted rather than assumed: if DML took a sequential scan here, its
    // `Filter` would answer correctly and this test would be vacuous.
    let plan = d.explain("SELECT id FROM t WHERE v < 5;");
    assert!(
        plan.contains("Index scan"),
        "this case only tests the index path if the index path is chosen; plan was:\n{plan}"
    );

    let deleted = d.affected("DELETE FROM t WHERE v < 5;");
    let survivors = d.select_rows("SELECT id FROM t;");
    assert_eq!(
        (deleted, survivors),
        (1, 3000),
        "`DELETE FROM t WHERE v < 5` must remove exactly the one v=1 row and leave the 500 \
         NULL-valued rows untouched — NULL < 5 is UNKNOWN. plan:\n{plan}"
    );
}

/// The other DML executor. `Update::execute` and `Delete::execute` are different code consuming the
/// same scan, so proving one says nothing about the other — the scan fix is shared, the consumers
/// are not.
///
/// Assigns to `w`, not `id`: `Update` refuses to assign to column 0, so a scan driven by the
/// primary index can never have its own key rewritten underneath it.
///
/// The `w = 9` recount is an INDEPENDENT instrument — `w` carries no index, so that query is a
/// sequential scan whose `Filter` is known-correct for NULLs. If the index scan wrote 501 rows and
/// reported 1, this is what catches it.
#[test]
fn sql_update_does_not_overwrite_null_rows() {
    let mut d = seeded_wide();
    let plan = d.explain("SELECT id FROM t WHERE v < 5;");
    assert!(
        plan.contains("Index scan"),
        "this case only tests the index path if the index path is chosen; plan was:\n{plan}"
    );

    let updated = d.affected("UPDATE t SET w = 9 WHERE v < 5;");
    let carrying = d.select_rows("SELECT id FROM t WHERE w = 9;");
    assert_eq!(
        (updated, carrying),
        (1, 1),
        "`UPDATE t SET w = 9 WHERE v < 5` must touch exactly the one v=1 row; the 500 NULL-valued \
         rows do not satisfy `v < 5`. plan:\n{plan}"
    );
}

/// Every shape is MEASURED FIRST and asserted once at the end, deliberately.
///
/// The first version asserted inline and aborted on the first mismatch, so on the unfixed tree only
/// `v < 5` was ever measured — the two-conjunct shape, the inclusive bound and the NULL-literal
/// predicates all short-circuited and produced no before-number at all. A run that stops at the
/// first failure reports one defect and hides four. Collecting first costs nothing and makes a
/// single run show the whole picture, which is the point of a before-number.
#[test]
fn sql_index_path_answers_correctly() {
    let mut d = seeded_wide();

    // `w = 1` holds for every row, so it changes no answer — it is there to put a residual `Filter`
    // above the scan and prove the fix does not depend on one being present.
    // The NULL-literal predicates answer nothing because every comparison with NULL is UNKNOWN;
    // `v = NULL` reaches the index via `bound_selectivity`'s `lo == hi` branch (`1/distinct`).
    let cases: [(&str, usize); 8] = [
        ("v < 5", 1),
        ("w = 1 AND v < 5", 1),
        ("v <= 4", 1),
        ("v = NULL", 0),
        ("v >= NULL", 0),
        ("v > NULL", 0),
        ("v <= NULL", 0),
        ("v < NULL", 0),
    ];

    let mut wrong = Vec::new();
    for (pred, want) in cases {
        let sql = format!("SELECT id FROM t WHERE {pred};");
        let got = d.select_rows(&sql);
        let plan = d.explain(&sql);
        let kind = if plan.contains("Index scan") { "index" } else { "seq" };
        if got != want {
            wrong.push(format!("  {pred:<18} got {got:<5} want {want:<5} ({kind} scan)"));
        }
    }

    // ⚠ PERMANENT, not scaffolding from the hunt. Below a few thousand rows the cost model prefers
    // a filtered sequential scan, whose `Filter` rejects NULLs correctly — so a GREEN with no index
    // scan anywhere means "the index was never used", NOT "the index is right".
    //
    // This is not hypothetical and it is not a risk that went away with the fix: the FIRST version
    // of this fixture, at 501 rows, silently got
    //
    //     Filter (#1 < 5) (rows=125 cost=15.02)
    //       Sequential scan on t (rows=501 cost=10.01)
    //
    // and would have passed after the fix while testing a heap filter. Anything that shifts the
    // cost model, the row count, the `v` spread or `analyze`'s statistics can put it back there,
    // and every one of those is a change someone would make for an unrelated reason. Deleting this
    // assertion does not make the test weaker in a visible way — it makes it green in a useless
    // one.
    let headline = d.explain("SELECT id FROM t WHERE v < 5;");
    assert!(
        headline.contains("Index scan"),
        "this case only tests the index path if the index path is chosen; plan was:\n{headline}"
    );

    assert!(
        wrong.is_empty(),
        "{} of {} SQL shapes answered wrongly:\n{}\nheadline plan:\n{headline}",
        wrong.len(),
        cases.len(),
        wrong.join("\n")
    );
}
