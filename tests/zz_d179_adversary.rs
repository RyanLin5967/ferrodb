//! ADVERSARIAL probes against D179/D181. Not for landing — this file exists to try to break the
//! branch, and several arms are expected to be informative rather than green.
//!
//! The central instrument is a DIFFERENTIAL one: the same predicate over the same fixture, run
//! once as a hand-built secondary `IndexScan` and once as `SeqScan + Filter`, with the row sets
//! compared. D181 changes WHICH of those two the optimizer picks, so any disagreement between them
//! is a live correctness defect that D181 can newly expose.

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::ops::Bound;
use std::sync::Arc;

use ferrodb::binder::binder::BoundExpr;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Executor, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::optimizer::optimizer::lower;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::{Scanner, TokenType};
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
            .open(dir.path().join("adv.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("adv.wal")).unwrap());
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

    fn view(&self) -> Arc<ReadView> {
        Arc::new(ReadView {
            snapshot: Arc::new(Snapshot { high_water: u64::MAX, active: HashSet::new() }),
            txn_id: 0,
        })
    }

    /// Run a hand-built plan to exhaustion, returning `(id, v)` with `Null` mapped to `None`.
    fn drain(&self, plan: PhysicalPlan) -> Result<Vec<(Option<i32>, Option<i32>)>, FerroError> {
        let mut exec: Box<dyn Executor> = lower(plan, &self.catalog, self.bp.clone(), self.view())?;
        let mut out = Vec::new();
        while let Some(row) = exec.next() {
            let (_, vals) = row?;
            let cell = |v: &Value| match v {
                Value::Integer(i) => Some(*i),
                Value::Null => None,
                other => panic!("unexpected cell {other:?}"),
            };
            out.push((cell(&vals[0]), cell(&vals[1])));
        }
        Ok(out)
    }
}

fn cmp_expr(col: usize, op: TokenType, v: i32) -> BoundExpr {
    BoundExpr::BinaryOp {
        left: Box::new(BoundExpr::Column(col)),
        operator: op,
        right: Box::new(BoundExpr::Literal(Value::Integer(v))),
    }
}

fn seq_plan(pred: BoundExpr) -> PhysicalPlan {
    PhysicalPlan::Filter { input: Box::new(PhysicalPlan::SeqScan { table: "t".into() }), predicate: pred }
}

fn idx_plan(lower_b: Bound<Value>, upper_b: Bound<Value>) -> PhysicalPlan {
    PhysicalPlan::IndexScan { table: "t".into(), column: 1, lower: lower_b, upper: upper_b }
}

/// The five single-sided comparisons `predicate_to_bounds` understands, as (op, bounds).
fn ops(v: i32) -> Vec<(&'static str, TokenType, Bound<Value>, Bound<Value>)> {
    let lit = || Value::Integer(v);
    vec![
        (">", TokenType::Greater, Bound::Excluded(lit()), Bound::Unbounded),
        (">=", TokenType::GreaterEqual, Bound::Included(lit()), Bound::Unbounded),
        ("<", TokenType::Less, Bound::Unbounded, Bound::Excluded(lit())),
        ("<=", TokenType::LessEqual, Bound::Unbounded, Bound::Included(lit())),
        ("=", TokenType::Equal, Bound::Included(lit()), Bound::Included(lit())),
    ]
}

fn sorted(mut v: Vec<(Option<i32>, Option<i32>)>) -> Vec<(Option<i32>, Option<i32>)> {
    v.sort();
    v
}

/// Build `t (id, v)` indexed on `v` from an explicit list of `(id, v)` literals, so a test can
/// seed NULLs. `id_decl`/`v_decl` carry the nullability.
fn seed_raw(rows: &[(&str, &str)], id_decl: &str, v_decl: &str) -> Db {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok(&format!("CREATE TABLE t (id INTEGER {id_decl}, v INTEGER {v_decl});"), &mut s);
    for (id, v) in rows {
        db.ok(&format!("INSERT INTO t VALUES ({id}, {v});"), &mut s);
    }
    db.ok("CREATE INDEX ix_v ON t (v);", &mut s);
    db
}

// =============================================================================================
// AXIS 3 + AXIS 2 — differential: the index path must agree with the sequential path, everywhere.
// =============================================================================================

/// A fixture with heavy duplication at the boundary, the minimum and the maximum, driven through
/// EVERY comparison at EVERY interesting cutoff, both ways.
#[test]
fn adv_index_and_seq_agree_on_a_duplicate_heavy_fixture() {
    // v: 10 x10, 20 x1, 30 x800-ish (kept small for speed), 40 x2, 50 x1
    let mut rows: Vec<(String, String)> = Vec::new();
    let mut id = 0;
    for (v, n) in [(10, 10), (20, 1), (30, 25), (40, 2), (50, 1)] {
        for _ in 0..n {
            rows.push((id.to_string(), v.to_string()));
            id += 1;
        }
    }
    let refs: Vec<(&str, &str)> = rows.iter().map(|(a, b)| (a.as_str(), b.as_str())).collect();
    let db = seed_raw(&refs, "NOT NULL", "NOT NULL");

    // Cutoffs: below min, exactly min, between, exactly a duplicated value, exactly max, above max,
    // and a value no row holds.
    for cutoff in [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55] {
        for (name, op, lo, hi) in ops(cutoff) {
            let via_index = sorted(db.drain(idx_plan(lo, hi)).expect("index plan refused"));
            let via_seq = sorted(db.drain(seq_plan(cmp_expr(1, op, cutoff))).expect("seq plan refused"));
            assert_eq!(
                via_index, via_seq,
                "`v {name} {cutoff}`: index path and sequential path disagree"
            );
        }
    }
}

/// The same differential, with NULLs present in the INDEXED column. A secondary index posts one
/// entry per row including `(Null, pk)` (`execution::insert`), so a NULL row is physically inside
/// the tree and the scan walks over it.
#[test]
fn adv_index_and_seq_agree_when_the_indexed_column_holds_nulls() {
    let db = seed_raw(
        &[("0", "NULL"), ("1", "10"), ("2", "NULL"), ("3", "20"), ("4", "30"), ("5", "NULL")],
        "NOT NULL",
        "NULL",
    );
    for cutoff in [5, 10, 20, 30, 35] {
        for (name, op, lo, hi) in ops(cutoff) {
            let via_index = sorted(db.drain(idx_plan(lo, hi)).expect("index plan refused"));
            let via_seq = sorted(db.drain(seq_plan(cmp_expr(1, op, cutoff))).expect("seq plan refused"));
            assert_eq!(
                via_index, via_seq,
                "`v {name} {cutoff}` with NULLs in v: index path and sequential path disagree\n\
                 index: {via_index:?}\n  seq: {via_seq:?}"
            );
        }
    }
}

/// The doc on `optimizer::secondary_scan_start` justifies the `(v, Null)` start key with
/// "`Null` sorts below every pk ... and column 0 is `NOT NULL`". The second half is a claim about
/// the ENGINE, not about this function. This arm establishes whether it is true.
#[test]
fn adv_is_column_zero_actually_not_null() {
    let mut db = Db::new();
    let mut s = Session::new();
    // No NOT NULL suffix on column 0. `parser::parse_nullability` documents absent == nullable.
    db.ok("CREATE TABLE t (id INTEGER, v INTEGER);", &mut s);
    let r = db.exec("INSERT INTO t VALUES (NULL, 7);", &mut s);
    match r {
        Ok(_) => panic!(
            "FINDING: a NULL primary key was accepted. The claim \"column 0 is NOT NULL\" in \
             `secondary_scan_start`'s doc is FALSE for a table declared without the suffix."
        ),
        Err(e) => panic!("column 0 refused NULL, doc claim holds. error was: {e}"),
    }
}

/// And the consequence, if a NULL pk is insertable: `(v, Null)` is then a key a real row can
/// occupy, so `Bound::Included((v, Null))` must still include it.
#[test]
fn adv_null_pk_row_is_not_lost_by_the_start_key() {
    let db = seed_raw(&[("NULL", "30"), ("1", "30"), ("2", "40")], "NULL", "NOT NULL");
    for (name, op, lo, hi) in ops(30) {
        let via_index = sorted(db.drain(idx_plan(lo, hi)).expect("index plan refused"));
        let via_seq = sorted(db.drain(seq_plan(cmp_expr(1, op, 30))).expect("seq plan refused"));
        assert_eq!(via_index, via_seq, "`v {name} 30` with a NULL pk: paths disagree");
    }
}

/// An inverted / empty range handed straight to `lower`. Nothing in `build_index_scan` can emit
/// one, but a hand-built plan can, and D179 removed the only bound-shape refusal `lower` had.
#[test]
fn adv_inverted_range_returns_nothing_rather_than_misbehaving() {
    let db = seed_raw(&[("0", "10"), ("1", "20"), ("2", "30")], "NOT NULL", "NOT NULL");
    let rows = db
        .drain(idx_plan(Bound::Excluded(Value::Integer(30)), Bound::Included(Value::Integer(10))))
        .expect("lower refused an inverted range");
    assert!(rows.is_empty(), "inverted range returned {rows:?}");

    // lower == upper, both strict: empty.
    let rows = db
        .drain(idx_plan(Bound::Excluded(Value::Integer(20)), Bound::Excluded(Value::Integer(20))))
        .expect("lower refused");
    assert!(rows.is_empty(), "`v > 20 AND v < 20` returned {rows:?}");
}

/// A strictly-excluded lower bound whose literal is of a DIFFERENT TYPE from the column. Before
/// D179 `lower` returned `Bind("lower bound sec index isn't supported")` for every `Excluded` on a
/// secondary column, so this input ERRORED. It now builds. Does it still agree with a seq scan?
#[test]
fn adv_type_mismatched_strict_bound_still_agrees_with_the_sequential_path() {
    let db = seed_raw(&[("0", "10"), ("1", "20"), ("2", "30")], "NOT NULL", "NOT NULL");
    // Varchar bound on an INTEGER column. `Value`'s type ranks put Varchar(7) above Integer(2),
    // so `sec <= Varchar` is true for every row and the skip should drop them all.
    let via_index = sorted(
        db.drain(idx_plan(Bound::Excluded(Value::Varchar("m".into())), Bound::Unbounded))
            .expect("lower refused a type-mismatched bound"),
    );
    let via_seq = sorted(
        db.drain(seq_plan(BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Column(1)),
            operator: TokenType::Greater,
            right: Box::new(BoundExpr::Literal(Value::Varchar("m".into()))),
        }))
        .expect("seq refused"),
    );
    assert_eq!(via_index, via_seq, "type-mismatched `v > 'm'`: paths disagree");
}
