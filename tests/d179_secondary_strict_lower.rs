//! D179 — `WHERE secondary > v` must use the secondary index, and must EXCLUDE `v`.
//!
//! # What was wrong
//!
//! A secondary tree is keyed `(value, pk)`. `Bound::Included(v)` has an exact start key,
//! `(v, Null)`, because `Null` sorts below every pk. A strictly excluded `v` has none: the scan
//! would have to start just past the LAST key with value `v` and there is no maximum pk to write
//! down. `optimizer::lower` therefore refused outright —
//! `FerroError::Bind("lower bound sec index isn't supported")` — so `secondary > v` could not use
//! the index at all and fell back to a sequential scan of the whole table.
//!
//! D179 puts the exclusion where the upper bound has always lived: open at `(v, Null)` anyway and
//! have `SecondaryIndexScan::next` SKIP the leading run of entries whose value is still `v`.
//!
//! # Why these arms drive `lower` directly instead of running SQL
//!
//! A hand-built `PhysicalPlan::IndexScan` guarantees the executor path under test actually runs.
//! Routing through the optimizer would make every arm conditional on the cost model preferring the
//! index over a sequential scan for that particular fixture — a correctness arm that silently
//! stopped exercising the new code would still pass, which is the one failure mode a test for this
//! must not have. The end-to-end question, *does the optimizer now CHOOSE this plan*, is a
//! different question and is answered with counters in
//! `tests/d179_secondary_strict_lower_counters.rs`.
//!
//! Every expected value below is computed from the seeding rule in Rust. None of them comes from
//! asking the engine.
//!
//! # The boundary the skip exists for
//!
//! `many_rows_share_the_boundary_value` and `every_row_shares_the_boundary_value` are the two arms
//! that would pass trivially on a fixture with distinct values. The second is the sharper: if the
//! skip were written `return None` instead of `continue`, `> v` would answer the empty set whenever
//! ANY row held `v`, and on a fixture where no row does, that bug is invisible.

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::ops::Bound;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Executor, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::optimizer::optimizer::lower;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::planner::physical_plan::PhysicalPlan;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::{ReadView, Snapshot, TxnManager};

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
            .open(dir.path().join("d179.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d179.wal")).unwrap());
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

    fn ok(&mut self, sql: &str, s: &mut Session) {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"));
    }

    /// Run a hand-built plan to exhaustion and return the `v` column of every row, in order.
    fn drain_v(&self, plan: PhysicalPlan) -> Vec<i32> {
        let view = Arc::new(ReadView {
            snapshot: Arc::new(Snapshot { high_water: u64::MAX, active: HashSet::new() }),
            txn_id: 0,
        });
        let mut exec: Box<dyn Executor> =
            lower(plan, &self.catalog, self.bp.clone(), view).expect("lower refused the plan");
        let mut out = Vec::new();
        while let Some(row) = exec.next() {
            match row.expect("row error").1[1] {
                Value::Integer(v) => out.push(v),
                ref other => panic!("column 1 is not an Integer: {other:?}"),
            }
        }
        out
    }
}

/// `t (id, v)` with `v` given by `f(id)`, indexed on `v`. Returns the seeded `(id, v)` pairs so
/// every expected value in a test is computed from the fixture rather than read back out of it.
fn seed(n: i32, f: impl Fn(i32) -> i32) -> (Db, Vec<(i32, i32)>) {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut s);
    let mut seeded = Vec::with_capacity(n as usize);
    for id in 0..n {
        let v = f(id);
        db.ok(&format!("INSERT INTO t VALUES ({id}, {v});"), &mut s);
        seeded.push((id, v));
    }
    db.ok("CREATE INDEX ix_v ON t (v);", &mut s);
    (db, seeded)
}

/// A hand-built secondary-index scan over `t.v` — column 1.
fn sec_scan(lower_bound: Bound<Value>, upper_bound: Bound<Value>) -> PhysicalPlan {
    PhysicalPlan::IndexScan { table: "t".into(), column: 1, lower: lower_bound, upper: upper_bound }
}

/// The `v` values the predicate selects, in index order (`v` ascending), computed from the fixture.
fn expected(seeded: &[(i32, i32)], keep: impl Fn(i32) -> bool) -> Vec<i32> {
    let mut vs: Vec<i32> = seeded.iter().map(|(_, v)| *v).filter(|v| keep(*v)).collect();
    vs.sort_unstable();
    vs
}

// ---------------------------------------------------------------------------------------------
// The two boundary arms the skip exists for.
// ---------------------------------------------------------------------------------------------

/// 800 of 1000 rows carry the boundary value. `> 500` must walk past all 800 and return the 200
/// above it — not stop at the first one, and not return any of them.
#[test]
fn many_rows_share_the_boundary_value() {
    const N: i32 = 1000;
    let boundary = 500;
    // ids 0..799 -> v = 500 (800 rows on the boundary); ids 800..999 -> v = 501..700.
    let (db, seeded) = seed(N, |id| if id < 800 { 500 } else { 500 + (id - 799) });
    assert_eq!(
        seeded.iter().filter(|(_, v)| *v == boundary).count(),
        800,
        "fixture: the boundary run must be long enough that skipping it is observable"
    );

    let strict = db.drain_v(sec_scan(Bound::Excluded(Value::Integer(boundary)), Bound::Unbounded));
    assert_eq!(strict, expected(&seeded, |v| v > boundary), "`v > {boundary}` returned the wrong rows");
    assert_eq!(strict.len(), 200);
    assert!(!strict.contains(&boundary), "the excluded boundary value came back");

    // The inclusive bound over the identical fixture. If the skip were unconditional this would
    // lose the 800-row run, and if it never ran the strict arm above would have gained it.
    let inclusive = db.drain_v(sec_scan(Bound::Included(Value::Integer(boundary)), Bound::Unbounded));
    assert_eq!(inclusive, expected(&seeded, |v| v >= boundary), "`v >= {boundary}` returned the wrong rows");
    assert_eq!(inclusive.len(), 1000);
}

/// EVERY row carries the boundary value, so `> v` selects nothing. The arm that fails if the skip
/// is written `return None`: that spelling would also return nothing here, so this arm is paired
/// with the one above, where returning early loses 200 real rows.
#[test]
fn every_row_shares_the_boundary_value() {
    let (db, seeded) = seed(300, |_| 42);
    let strict = db.drain_v(sec_scan(Bound::Excluded(Value::Integer(42)), Bound::Unbounded));
    assert!(strict.is_empty(), "`v > 42` on a table where every v IS 42 returned {} rows", strict.len());
    let inclusive = db.drain_v(sec_scan(Bound::Included(Value::Integer(42)), Bound::Unbounded));
    assert_eq!(inclusive, expected(&seeded, |_| true), "`v >= 42` must return the whole table");
    assert_eq!(inclusive.len(), 300);
}

// ---------------------------------------------------------------------------------------------
// The no-duplicates case, and the ends of the range.
// ---------------------------------------------------------------------------------------------

/// No row holds the boundary value, so there is no run to skip and `>` and `>=` must agree.
#[test]
fn no_row_holds_the_boundary_value() {
    let (db, seeded) = seed(1000, |id| id * 10);
    // 9905 falls between 9900 and 9910, so no row carries it.
    assert!(!seeded.iter().any(|(_, v)| *v == 9905), "fixture: the boundary must be absent");

    let strict = db.drain_v(sec_scan(Bound::Excluded(Value::Integer(9905)), Bound::Unbounded));
    assert_eq!(strict, expected(&seeded, |v| v > 9905));
    let inclusive = db.drain_v(sec_scan(Bound::Included(Value::Integer(9905)), Bound::Unbounded));
    assert_eq!(strict, inclusive, "with no row on the boundary, `>` and `>=` select the same rows");
    assert_eq!(strict.len(), 9, "v in 9910..=9990 step 10 is 9 rows");
}

/// A boundary at the table's maximum: `>` selects nothing, `>=` selects the maximum's run.
#[test]
fn boundary_at_the_maximum() {
    let (db, seeded) = seed(400, |id| id / 2); // v = 0..199, two rows per value
    let max = seeded.iter().map(|(_, v)| *v).max().unwrap();
    let strict = db.drain_v(sec_scan(Bound::Excluded(Value::Integer(max)), Bound::Unbounded));
    assert!(strict.is_empty(), "`v > max` returned {} rows", strict.len());
    let inclusive = db.drain_v(sec_scan(Bound::Included(Value::Integer(max)), Bound::Unbounded));
    assert_eq!(inclusive, expected(&seeded, |v| v >= max));
    assert_eq!(inclusive.len(), 2, "two rows share the maximum");
}

/// A boundary below the table's minimum: everything is above it, and nothing is skipped.
#[test]
fn boundary_below_the_minimum() {
    let (db, seeded) = seed(200, |id| 1000 + id);
    let strict = db.drain_v(sec_scan(Bound::Excluded(Value::Integer(-5)), Bound::Unbounded));
    assert_eq!(strict, expected(&seeded, |_| true), "`v > -5` must return the whole table");
    assert_eq!(strict.len(), 200);
}

/// A strict lower bound and an upper bound together — the two checks run in the same `next`, and
/// the upper one must still stop the scan after the lower one has skipped a run.
#[test]
fn strict_lower_with_an_upper_bound() {
    let (db, seeded) = seed(600, |id| if id < 300 { 100 } else { 100 + (id - 299) });
    let rows = db.drain_v(sec_scan(Bound::Excluded(Value::Integer(100)), Bound::Included(Value::Integer(150))));
    assert_eq!(rows, expected(&seeded, |v| v > 100 && v <= 150));
    assert_eq!(rows.len(), 50, "v in 101..=150");

    let excl_upper = db.drain_v(sec_scan(Bound::Excluded(Value::Integer(100)), Bound::Excluded(Value::Integer(150))));
    assert_eq!(excl_upper, expected(&seeded, |v| v > 100 && v < 150));
    assert_eq!(excl_upper.len(), 49);
}

// ---------------------------------------------------------------------------------------------
// The refusal that is still real.
// ---------------------------------------------------------------------------------------------

/// `lower` still refuses a hand-built plan naming a secondary column that carries NO index.
///
/// # BAND — read this before re-adding the refusal you will find deleted in `git log`
///
/// A test named `plan::tests::test_index_scan_secondary_rejects_strict_lower` was **removed** by
/// D179. It asserted, in full:
///
/// ```ignore
/// let plan = PhysicalPlan::IndexScan {
///     table: "users".into(), column: 1,
///     lower: Bound::Excluded(Value::Varchar("b".into())), upper: Bound::Unbounded };
/// assert!(lower(plan, &c, bp, ..).is_err()); // todo: composite bound handling
/// ```
///
/// **That trailing `// todo: composite bound handling` is the original author's own note, on the
/// assertion line itself.** It is what makes the deletion safe to reason about: the author labelled
/// the behaviour a LIMITATION, not a guarantee. The test therefore pinned a WALL — and a test that
/// pins a wall has to fail when the wall falls, because that failure IS the signal that the fix
/// worked. It did fail, exactly there, and that was the evidence D179 landed.
///
/// The premise died rather than the coverage. `(value, pk)` still has no start key for `> v` —
/// that part was always true and still is — but `optimizer::secondary_scan_start` now opens at
/// `(v, Null)` and `SecondaryIndexScan::next` skips the leading `sec == v` run, so there is nothing
/// left to refuse. `secondary_scan_lower`'s `Option` and `index_scan_lowerable` went with it: after
/// the fix both could only ever return the permissive answer, and a branch that cannot be false is
/// one no test can hold to account.
///
/// Two things replaced it, and between them `lower`'s error path is still covered:
///
///  * the positive contract, at `plan::tests::test_index_scan_secondary_strict_lower` and its
///    inclusive-bound twin — the same fixture and the same bound, now asserting the ROWS;
///  * **this test**, which pins a refusal whose premise is a fact about the catalog — the tree does
///    not exist — and so cannot be removed by making a bound expressible.
///
/// ⛔ Do not restore the deleted assertion. `lower` builds that plan now; re-adding it would pin a
/// wall that is gone and fail on a correct engine.
#[test]
fn lower_refuses_a_column_with_no_index() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut s);
    db.ok("INSERT INTO t VALUES (1, 10);", &mut s);
    // No `CREATE INDEX`. Column 1 is a secondary column with no tree behind it.
    let view = Arc::new(ReadView {
        snapshot: Arc::new(Snapshot { high_water: u64::MAX, active: HashSet::new() }),
        txn_id: 0,
    });
    let plan = sec_scan(Bound::Excluded(Value::Integer(5)), Bound::Unbounded);
    let err = lower(plan, &db.catalog, db.bp.clone(), view).err().expect("an unindexed column must be refused");
    assert!(
        matches!(&err, FerroError::Bind(m) if m.contains("no index")),
        "expected a bind error naming the missing index, got: {err}"
    );
}
