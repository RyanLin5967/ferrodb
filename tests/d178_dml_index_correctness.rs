//! D178 — routing `UPDATE`/`DELETE` through the optimizer must not change WHICH ROWS they touch.
//!
//! The counter half of D178 lives in `tests/d178_dml_index_counters.rs` and answers "did it stop
//! reading the whole table". This file answers the question that outranks it: **did it still change
//! exactly the rows the predicate names, and nothing else.** A planner change that makes a
//! statement fast and slightly wrong is far worse than the scan it replaced.
//!
//! # How the expected values are obtained
//!
//! From the fixture's own literals, arithmetically — never by asking the engine what it did. The
//! fixture is `id` in `0..N`, `v = id * 10`, `label = 'row'`, and every expectation below is
//! computed from that rule in the test. A test whose expected value comes from calling the subject
//! passes for any implementation, including a broken one.
//!
//! # The shapes covered, and why each is here
//!
//! Each predicate reaches a DIFFERENT access path now that DML is planned by `optimize`, and three
//! of them were **unreachable from a write statement before D178**:
//!
//! | predicate | access path | new since D178? |
//! |---|---|---|
//! | `id = k` | primary `IndexScan`, point | yes |
//! | `id > k` | primary `IndexScan`, `Bound::Excluded` lower | yes |
//! | `v = k` | `SecondaryIndexScan` | yes |
//! | `label = ?` | `Filter` over `SeqScan` | no — the old path |
//! | no `WHERE` | bare `SeqScan` | no — the old path |
//!
//! A row count alone would not catch a wrong-rows bug: "4 rows updated" is equally true of the
//! right four and the wrong four. So every case compares the WHOLE table, sorted, against a table
//! built in the test.

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

const N: i64 = 60;

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
            .open(dir.path().join("d178k.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d178k.wal")).unwrap());
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
        assert_eq!(stmts.len(), 1, "one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn affected(&mut self, sql: &str, s: &mut Session) -> usize {
        match self.ok(sql, s) {
            Outcome::Affected(n) => n,
            _ => panic!("{sql}: expected an affected-row count"),
        }
    }

    /// The whole table as `(id, v)`, **sorted by id**.
    ///
    /// Sorting is load-bearing, not tidiness: a `SeqScan` yields heap order and an `IndexScan`
    /// yields key order, so an unsorted comparison would fail for two plans that agree perfectly
    /// on content. Content is what this file is about.
    fn table(&mut self, s: &mut Session) -> Vec<(i64, i64)> {
        let rows = match self.ok("SELECT id, v FROM t;", s) {
            Outcome::Rows(r) => r,
            _ => panic!("SELECT id, v FROM t: expected rows"),
        };
        let mut out: Vec<(i64, i64)> = rows
            .iter()
            .map(|r| match (&r[0], &r[1]) {
                (Value::Integer(a), Value::Integer(b)) => (*a as i64, *b as i64),
                other => panic!("unexpected row shape {other:?}"),
            })
            .collect();
        out.sort();
        out
    }
}

/// `id` in `0..N`, `v = id * 10`, `label = 'row'`, with a secondary index on `v`.
fn fixture() -> (Db, Session) {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, label VARCHAR(16));", &mut s);
    for i in 0..N {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {}, 'row');", i * 10), &mut s);
    }
    db.ok("CREATE INDEX ix ON t (v);", &mut s);
    (db, s)
}

/// The H1 fixture: **800 rows and `ANALYZE` run**, which is not decoration.
///
/// The defect H1 guards against is only reachable where the optimizer's cost comparison PICKS the
/// index for `v > k`, and that needs a small enough row estimate. Measured against the unfixed tree
/// (`tests/d178_probe_scope.rs`, run under the mutant that removes the guard):
///
/// ```text
///   n=400  analyze=true   v > 3980  ->  OK, 1 rows       <- the defect does NOT fire here
///   n=800  analyze=false  v > 7980  ->  OK, 1 rows       <- nor here
///   n=800  analyze=true   v > 7980  ->  ERROR: lower bound sec index isn't supported
/// ```
///
/// Without `ANALYZE` the cost model has no min/max for `v` and falls back to
/// `DEFAULT_RANGE_SELECTIVITY`, which estimates a quarter of the table and makes the index side
/// lose; with real statistics the estimate collapses to one row and the index side wins. So **a
/// smaller table or a missing `ANALYZE` makes this test pass against the bug** — it was written
/// that way first, and the mutation run is what caught it.
///
/// ⛔ Do not shrink this fixture or drop the `ANALYZE` to make the file faster. That does not speed
/// the test up, it switches it off.
const N_BIG: i64 = 800;

fn big_fixture() -> (Db, Session) {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, label VARCHAR(16));", &mut s);
    for i in 0..N_BIG {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {}, 'row');", i * 10), &mut s);
    }
    db.ok("CREATE INDEX ix ON t (v);", &mut s);
    db.ok("ANALYZE t;", &mut s);
    (db, s)
}

fn big_expected_after_update(matches: impl Fn(i64, i64) -> bool, new_v: i64) -> Vec<(i64, i64)> {
    (0..N_BIG).map(|i| if matches(i, i * 10) { (i, new_v) } else { (i, i * 10) }).collect()
}

fn big_expected_after_delete(matches: impl Fn(i64, i64) -> bool) -> Vec<(i64, i64)> {
    (0..N_BIG).filter(|&i| !matches(i, i * 10)).map(|i| (i, i * 10)).collect()
}

/// The fixture as this test computes it, from the rule above and nothing else.
fn expected_fixture() -> Vec<(i64, i64)> {
    (0..N).map(|i| (i, i * 10)).collect()
}

/// Apply `f` to the fixture's rows to build the table an `UPDATE ... SET v = ?` should leave behind.
fn expected_after_update(matches: impl Fn(i64, i64) -> bool, new_v: i64) -> Vec<(i64, i64)> {
    (0..N).map(|i| if matches(i, i * 10) { (i, new_v) } else { (i, i * 10) }).collect()
}

/// The table a `DELETE` matching `matches` should leave behind.
fn expected_after_delete(matches: impl Fn(i64, i64) -> bool) -> Vec<(i64, i64)> {
    (0..N).filter(|&i| !matches(i, i * 10)).map(|i| (i, i * 10)).collect()
}

#[test]
fn the_fixture_is_what_this_file_thinks_it_is() {
    // If this fails, every expectation in every other test here is measured against the wrong
    // ruler, and their passes would mean nothing.
    let (mut db, mut s) = fixture();
    assert_eq!(db.table(&mut s), expected_fixture());
}

#[test]
fn update_on_a_primary_key_equality_changes_exactly_one_row() {
    let (mut db, mut s) = fixture();
    assert_eq!(db.affected("UPDATE t SET v = 999 WHERE id = 17;", &mut s), 1);
    assert_eq!(db.table(&mut s), expected_after_update(|id, _| id == 17, 999));
}

#[test]
fn update_on_a_primary_key_range_changes_exactly_the_rows_above_the_bound() {
    // `id > 50` lowers to `Bound::Excluded(50)` on the PRIMARY tree, which `range_scan` resolves
    // with `upper_bound` (first key strictly greater). A write statement could not reach this path
    // at all before D178, so an off-by-one here would be a NEW bug, not an inherited one.
    let (mut db, mut s) = fixture();
    let affected = db.affected("UPDATE t SET v = 999 WHERE id > 50;", &mut s);
    assert_eq!(affected, (N - 51) as usize, "id > 50 over 0..{N} is {} rows", N - 51);
    assert_eq!(db.table(&mut s), expected_after_update(|id, _| id > 50, 999));
}

#[test]
fn update_on_a_secondary_index_equality_changes_exactly_one_row() {
    let (mut db, mut s) = fixture();
    // v = 170 is id 17 by the fixture's rule, and no other row carries that value.
    assert_eq!(db.affected("UPDATE t SET v = 999 WHERE v = 170;", &mut s), 1);
    assert_eq!(db.table(&mut s), expected_after_update(|_, v| v == 170, 999));
}

#[test]
fn update_with_no_where_clause_changes_every_row() {
    // Predicate `None`, so `build_scan` hands `optimize` a bare `Scan` with no `Filter` over it.
    let (mut db, mut s) = fixture();
    assert_eq!(db.affected("UPDATE t SET v = 999;", &mut s), N as usize);
    assert_eq!(db.table(&mut s), expected_after_update(|_, _| true, 999));
}

#[test]
fn update_on_an_unindexed_column_still_changes_exactly_the_matching_rows() {
    let (mut db, mut s) = fixture();
    assert_eq!(db.affected("UPDATE t SET v = 999 WHERE label = 'row';", &mut s), N as usize);
    assert_eq!(db.table(&mut s), expected_after_update(|_, _| true, 999));

    let (mut db2, mut s2) = fixture();
    assert_eq!(db2.affected("UPDATE t SET v = 999 WHERE label = 'nope';", &mut s2), 0);
    assert_eq!(db2.table(&mut s2), expected_fixture(), "a predicate matching nothing changed something");
}

#[test]
fn delete_on_a_primary_key_equality_removes_exactly_one_row() {
    let (mut db, mut s) = fixture();
    assert_eq!(db.affected("DELETE FROM t WHERE id = 17;", &mut s), 1);
    assert_eq!(db.table(&mut s), expected_after_delete(|id, _| id == 17));
}

#[test]
fn delete_on_a_primary_key_range_removes_exactly_the_rows_above_the_bound() {
    let (mut db, mut s) = fixture();
    assert_eq!(db.affected("DELETE FROM t WHERE id > 50;", &mut s), (N - 51) as usize);
    assert_eq!(db.table(&mut s), expected_after_delete(|id, _| id > 50));
}

#[test]
fn delete_on_a_secondary_index_equality_removes_exactly_one_row() {
    let (mut db, mut s) = fixture();
    assert_eq!(db.affected("DELETE FROM t WHERE v = 170;", &mut s), 1);
    assert_eq!(db.table(&mut s), expected_after_delete(|_, v| v == 170));
}

#[test]
fn updating_the_indexed_column_itself_is_not_a_moving_target() {
    // The Halloween shape: the scan is driven by the index on `v`, and the statement rewrites `v`.
    // It is safe because `Update::execute` drains its child COMPLETELY before it writes anything,
    // so no tree is mutated while a scanner over it is live — but "safe by construction" is a
    // claim, and this is the case that would expose it if the construction ever changed.
    //
    // `v >= 400 AND v <= 400` is spelled as two conjuncts so the optimizer takes the first
    // lowerable indexed one and leaves the other as a residual `Filter` — the plan shape that has
    // both an index scan and a filter over the column being written.
    let (mut db, mut s) = fixture();
    let affected = db.affected("UPDATE t SET v = 405 WHERE v >= 400 AND v <= 400;", &mut s);
    assert_eq!(affected, 1, "exactly one row has v = 400 in the fixture");
    assert_eq!(db.table(&mut s), expected_after_update(|_, v| v == 400, 405));
}

#[test]
fn a_strict_lower_bound_on_a_secondary_index_answers_rather_than_erroring() {
    // D178 H1. `v > k` is a strictly-excluded lower bound on a secondary index, which `lower`
    // cannot build. Before the fix `build_index_scan` would still CHOOSE it whenever the estimate
    // was small enough, and the statement failed — measured on the SELECT path at `fe40276` in
    // `bench/d178_run1_BEFORE_RAW.txt`:
    //
    //     SELECT id FROM h WHERE v > 9980 ;  ERROR: lower bound sec index isn't supported
    //     SELECT id FROM h WHERE v > 9900 ;  OK, 9 rows
    //
    // Same table, same index; only the row estimate differed. The optimizer now declines to propose
    // a plan it cannot build and falls through to the sequential scan it was already costing
    // against, so the answer is correct rather than absent.
    //
    // ⚠ This asserts that the statement ANSWERS, and answers correctly. It does NOT assert that
    // `v > k` uses the index — it does not, and making it do so is a separate row.
    let (mut db, mut s) = big_fixture();

    // The top of `v`'s range is where the estimate is smallest and the index side of the cost
    // comparison is cheapest — the corner that used to fail. See `N_BIG` for why the fixture has
    // to be this size and has to be ANALYZEd.
    for cutoff in [(N_BIG - 2) * 10, (N_BIG - 5) * 10, (N_BIG / 2) * 10] {
        let rows = match db.exec(&format!("SELECT id FROM t WHERE v > {cutoff};"), &mut s) {
            Ok(Outcome::Rows(r)) => r,
            Ok(_) => panic!("v > {cutoff}: expected rows"),
            Err(e) => panic!("v > {cutoff} must answer, not refuse: {e}"),
        };
        let mut got: Vec<i64> = rows
            .iter()
            .map(|r| match &r[0] {
                Value::Integer(i) => *i as i64,
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        got.sort();
        let want: Vec<i64> = (0..N_BIG).filter(|i| i * 10 > cutoff).collect();
        assert_eq!(got, want, "v > {cutoff} returned the wrong rows");
    }

    // And the same shape on a WRITE statement, which is where D178 would otherwise have introduced
    // this error for the first time.
    let (mut db2, mut s2) = big_fixture();
    let cutoff = (N_BIG - 2) * 10;
    let affected = db2.affected(&format!("UPDATE t SET v = 99999 WHERE v > {cutoff};"), &mut s2);
    assert_eq!(affected, 1, "exactly one row has v > {cutoff}");
    assert_eq!(db2.table(&mut s2), big_expected_after_update(|_, v| v > cutoff, 99999));

    let (mut db3, mut s3) = big_fixture();
    assert_eq!(db3.affected(&format!("DELETE FROM t WHERE v > {cutoff};"), &mut s3), 1);
    assert_eq!(db3.table(&mut s3), big_expected_after_delete(|_, v| v > cutoff));
}

#[test]
fn an_unlowerable_conjunct_does_not_cost_the_statement_its_other_index() {
    // `build_index_scan` passes over a conjunct it cannot lower and considers the next indexed one,
    // rather than abandoning indexes for the whole predicate. Correctness is what is asserted here;
    // that it really does reach the index is asserted by the counter test.
    let (mut db, mut s) = big_fixture();
    let cutoff = (N_BIG - 2) * 10;
    let target = N_BIG - 1;
    let affected =
        db.affected(&format!("UPDATE t SET v = 99999 WHERE v > {cutoff} AND id = {target};"), &mut s);
    assert_eq!(affected, 1);
    assert_eq!(db.table(&mut s), big_expected_after_update(|id, _| id == target, 99999));
}

#[test]
fn a_predicate_the_optimizer_cannot_index_at_all_still_answers() {
    // `!=` has no bound form, so `predicate_to_bounds` returns `None` and the whole predicate falls
    // through to `Filter` over `SeqScan` — the pre-D178 path, which must be unchanged.
    let (mut db, mut s) = fixture();
    assert_eq!(db.affected("DELETE FROM t WHERE id != 17;", &mut s), (N - 1) as usize);
    assert_eq!(db.table(&mut s), vec![(17, 170)]);
}
