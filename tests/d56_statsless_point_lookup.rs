//! D56 — a primary-key point lookup must use the index on a table nobody has ANALYZEd.
//!
//! Without statistics the cost model filled the estimate from two constants — `DEFAULT_TABLE_ROWS
//! = 1000` and `DEFAULT_DISTINCT = 100` — and the arithmetic that followed preferred a sequential
//! scan: index `2*4 + 1 + 10*4 = 49` against seq+filter `8 + 1000*0.01 + 10 = 28`. Measured in
//! `bench/d55_explain_before_after_analyze.txt`: `WHERE id = 7` on a 5,000-row table planned
//! `Sequential scan on t (rows=1000)` before `ANALYZE` and `Index scan` after. So every point read
//! on an un-ANALYZEd table was O(table), and at 10^6 agent branches nobody runs `ANALYZE`.
//!
//! The 10 rows was the fabrication. Column 0 is the primary key and is UNIQUE by construction —
//! `execution::insert` refuses a duplicate — so `id = 7` matches at most one row. That is a schema
//! FACT and it may not be overridden by an estimate. This test pins the fact, its SCOPE, and that
//! the plan change does not change the answer.

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
        assert_eq!(stmts.len(), 1, "one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn plan(&mut self, sql: &str, s: &mut Session) -> String {
        match self.ok(&format!("EXPLAIN {sql}"), s) {
            Outcome::Explain(text) => text,
            _ => panic!("EXPLAIN {sql}: expected a plan, not this outcome"),
        }
    }

    fn rows(&mut self, sql: &str, s: &mut Session) -> Vec<Vec<Value>> {
        match self.ok(sql, s) {
            Outcome::Rows(rows) => rows,
            _ => panic!("{sql}: expected rows, not this outcome"),
        }
    }
}

/// `rows=` of the plan's TOP line, which is what the cost model estimated for the whole query.
fn estimated_rows(plan: &str) -> f64 {
    let first = plan.lines().next().unwrap_or_else(|| panic!("empty plan"));
    let after = first.split("rows=").nth(1).unwrap_or_else(|| panic!("no rows= in {first:?}"));
    let num: String = after.chars().take_while(|c| c.is_ascii_digit() || *c == '.').collect();
    num.parse().unwrap_or_else(|_| panic!("unparsable rows= in {first:?}"))
}

fn seeded(db: &mut Db, s: &mut Session, n: i32) {
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", s);
    for i in 1..=n {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {});", i * 10), s);
    }
}

#[test]
fn primary_key_equality_uses_the_index_with_no_statistics() {
    let mut db = Db::new();
    let mut s = Session::new();
    seeded(&mut db, &mut s, 200);

    // No ANALYZE anywhere above. This is the whole point: the plan must not depend on one.
    let plan = db.plan("SELECT v FROM t WHERE id = 7;", &mut s);
    assert!(
        plan.contains("Index scan on t (col 0"),
        "a primary-key point lookup planned without an index on an un-ANALYZEd table:\n{plan}"
    );
    assert!(
        !plan.contains("Sequential scan"),
        "the point lookup still reads the whole table:\n{plan}"
    );
    assert_eq!(
        estimated_rows(&plan),
        1.0,
        "the primary key is unique, so an equality estimate above 1 row is a fabrication:\n{plan}"
    );
}

#[test]
fn the_plan_change_does_not_change_the_answer() {
    let mut db = Db::new();
    let mut s = Session::new();
    seeded(&mut db, &mut s, 200);

    // Every id, plus two that do not exist, compared against the same query AFTER ANALYZE — which
    // is the plan the engine already trusted. Expected values come from the post-ANALYZE run, not
    // from re-implementing the lookup here.
    let queries: Vec<String> = (0..=201).map(|i| format!("SELECT id, v FROM t WHERE id = {i};")).collect();
    let before: Vec<Vec<Vec<Value>>> = queries.iter().map(|q| db.rows(q, &mut s)).collect();

    db.ok("ANALYZE t;", &mut s);
    let after: Vec<Vec<Vec<Value>>> = queries.iter().map(|q| db.rows(q, &mut s)).collect();

    assert_eq!(before, after, "the stats-less plan returned different rows from the ANALYZEd plan");
    assert_eq!(before[7], vec![vec![Value::Integer(7), Value::Integer(70)]]);
    assert!(before[0].is_empty() && before[201].is_empty(), "a missing key must return nothing");
}

#[test]
fn the_fact_is_scoped_to_equality_on_the_unique_column() {
    let mut db = Db::new();
    let mut s = Session::new();
    seeded(&mut db, &mut s, 200);
    db.ok("CREATE INDEX ix ON t (v);", &mut s);

    // A RANGE on the primary key matches many rows; uniqueness says nothing about how many.
    let range = db.plan("SELECT v FROM t WHERE id > 100;", &mut s);
    assert!(
        estimated_rows(&range) > 1.0,
        "uniqueness was applied to a RANGE, where it does not hold:\n{range}"
    );

    // Equality on a SECONDARY indexed column: `v` is not unique by construction, so the estimate
    // must still come from the statistics path, not from the primary key's fact.
    let secondary = db.plan("SELECT id FROM t WHERE v = 70;", &mut s);
    assert!(
        estimated_rows(&secondary) > 1.0,
        "the primary key's uniqueness was applied to a non-unique column:\n{secondary}"
    );
}
