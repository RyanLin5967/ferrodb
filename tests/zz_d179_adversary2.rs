//! ADVERSARIAL probes, part 2 — reachability through the OPTIMIZER (not hand-built plans).
//!
//! Part 1 established that a secondary `IndexScan` with an upper bound returns NULL rows that a
//! sequential scan filters out. That is only interesting if the optimizer can CHOOSE such a plan.
//! These arms ask the optimizer, through SQL, and also measure the cost-model crossover that
//! decides how often it chooses an index at all (D181's reach).

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::ops::Bound;
use std::sync::Arc;

use ferrodb::binder::binder::BoundExpr;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::optimizer::cost_model::cost;
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
            .read(true).write(true).create(true).truncate(true)
            .open(dir.path().join("adv2.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("adv2.wal")).unwrap());
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

    fn rows(&mut self, sql: &str, s: &mut Session) -> Vec<Vec<Value>> {
        match self.ok(sql, s) {
            Outcome::Rows(r) => r,
            _ => panic!("{sql} did not return rows"),
        }
    }

    fn explain(&mut self, sql: &str, s: &mut Session) -> String {
        match self.ok(&format!("EXPLAIN {sql}"), s) {
            Outcome::Explain(t) => t.replace('\n', " | "),
            _ => panic!("EXPLAIN did not return Explain"),
        }
    }
}

fn _unused_view() -> Arc<ReadView> {
    Arc::new(ReadView { snapshot: Arc::new(Snapshot { high_water: u64::MAX, active: HashSet::new() }), txn_id: 0 })
}

// =============================================================================================
// AXIS 5 — where is the cost-model crossover, really?
// =============================================================================================

/// Sweep the cutoff of `v > k` on a 1,000-row table with an index on `v` and `ANALYZE` run, and
/// report the estimated row count at which the secondary index first beats the sequential scan.
/// The lane's disclosure says "~8 rows". This measures it instead of quoting it.
#[test]
fn adv_measure_the_secondary_index_crossover() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER NOT NULL);", &mut s);
    for id in 0..1000 {
        db.ok(&format!("INSERT INTO t VALUES ({id}, {});", id * 10), &mut s);
    }
    db.ok("CREATE INDEX ix_v ON t (v);", &mut s);
    db.ok("ANALYZE t;", &mut s);

    let seq = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::SeqScan { table: "t".into() }),
        predicate: BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Column(1)),
            operator: TokenType::Greater,
            right: Box::new(BoundExpr::Literal(Value::Integer(0))),
        },
    };
    let seq_cost = cost(&seq, &db.catalog).cost;
    println!("SEQ+FILTER cost on 1000 rows = {seq_cost}");

    let mut first_win: Option<(i32, f64, f64)> = None;
    let mut report = Vec::new();
    // Sweep cutoffs from the top of v's range downwards: higher cutoff => fewer estimated rows.
    for k in [9990, 9980, 9950, 9900, 9800, 9500, 9000, 8000, 5000, 0] {
        let idx = PhysicalPlan::IndexScan {
            table: "t".into(), column: 1,
            lower: Bound::Excluded(Value::Integer(k)), upper: Bound::Unbounded,
        };
        let c = cost(&idx, &db.catalog);
        report.push(format!("  v > {k:>5}: est_rows={:>8.2} idx_cost={:>10.2} seq_cost={seq_cost:.2} -> {}",
            c.stats.rows, c.cost, if c.cost < seq_cost { "INDEX" } else { "seq" }));
        if c.cost < seq_cost && first_win.is_none() {
            first_win = Some((k, c.stats.rows, c.cost));
        }
    }
    println!("{}", report.join("\n"));

    // Now binary-search the exact estimated-row crossover by costing a synthetic equality-free
    // range whose estimate we can steer.
    let mut crossover_rows = None;
    for target in 1..=60 {
        // v ranges 0..9990; selectivity for `v > k` is (max-k)/(max-min). Pick k for `target` rows.
        let k = 9990 - (target * 9990 / 1000);
        let idx = PhysicalPlan::IndexScan {
            table: "t".into(), column: 1,
            lower: Bound::Excluded(Value::Integer(k)), upper: Bound::Unbounded,
        };
        let c = cost(&idx, &db.catalog);
        if c.cost >= seq_cost {
            println!("crossover: index stops winning at est_rows={:.2} (idx {:.2} vs seq {seq_cost:.2})", c.stats.rows, c.cost);
            break;
        }
        crossover_rows = Some(c.stats.rows);
    }
    println!("MAX est_rows at which the SECONDARY index still wins: {crossover_rows:?}");

    // EXPLAIN, end to end, so the number is not only arithmetic.
    for k in [9990, 9950, 9900, 9800, 9000] {
        println!("EXPLAIN v > {k}: {}", db.explain(&format!("SELECT id FROM t WHERE v > {k};"), &mut s));
    }
    panic!("REPORTING ARM — read stdout above");
}

/// The crossover is `r < (P + 11) / 8` where `P = table_pages`, so it moves with ROW WIDTH. This
/// arm measures `P` and the crossover for three real fixture shapes, including the ones D178 and
/// D181 actually use, so the "~8 rows" figure can be attributed rather than merely contradicted.
#[test]
fn adv_crossover_moves_with_row_width() {
    for (label, ddl) in [
        ("2 x INTEGER (my fixture)", "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER NOT NULL);"),
        ("2 x INTEGER + VARCHAR(16) (d178 shape)", "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, label VARCHAR(16));"),
        ("3 x INTEGER + VARCHAR(16) (d181 shape)", "CREATE TABLE t (id INTEGER NOT NULL, sel INTEGER, broad INTEGER, pad VARCHAR(16));"),
    ] {
        let mut db = Db::new();
        let mut s = Session::new();
        db.ok(ddl, &mut s);
        let ncols = ddl.matches(',').count() + 1;
        for id in 0..1000 {
            let mut vals = vec![id.to_string(), (id * 10).to_string()];
            while vals.len() < ncols {
                if ddl.contains("VARCHAR") && vals.len() == ncols - 1 { vals.push("'rowrowrowrow'".into()) }
                else { vals.push((id % 2).to_string()) }
            }
            db.ok(&format!("INSERT INTO t VALUES ({});", vals.join(", ")), &mut s);
        }
        db.ok("CREATE INDEX ix_v ON t (v);", &mut s);
        db.ok("ANALYZE t;", &mut s);

        let seq = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::SeqScan { table: "t".into() }),
            predicate: BoundExpr::BinaryOp {
                left: Box::new(BoundExpr::Column(1)),
                operator: TokenType::Greater,
                right: Box::new(BoundExpr::Literal(Value::Integer(0))),
            },
        };
        let seq_cost = cost(&seq, &db.catalog).cost;
        // seq_cost = P + 0.02*1000  =>  P = seq_cost - 20
        let pages = seq_cost - 20.0;
        // index wins iff 2*4 + ceil(r/100) + 8r < seq_cost ; for r < 100 that is 9 + 8r < seq_cost
        let crossover = (seq_cost - 9.0) / 8.0;
        println!("{label}: seq_cost={seq_cost:.2} table_pages={pages:.0} -> SECONDARY index wins up to r < {crossover:.2} rows");
    }
    panic!("REPORTING ARM — read stdout above");
}

// =============================================================================================
// The NULL leak, reached through the OPTIMIZER rather than a hand-built plan.
// =============================================================================================

/// `w = 3 AND v < 5` where BOTH `w` and `v` are indexed and `v` holds NULLs.
///
/// Whichever conjunct becomes the index scan, the other becomes a residual `Filter`. If `v < 5`
/// becomes the scan, the NULL rows enter the pipeline and only `w = 3` is applied to them, so they
/// come back. If `w = 3` becomes the scan, `Filter(v < 5)` rejects them. The two plans answer the
/// same predicate differently, and D181 decides which one runs.
#[test]
fn adv_null_leak_is_reachable_through_the_optimizer() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, w INTEGER NOT NULL, v INTEGER NULL);", &mut s);
    // 200 rows: w cycles 0..4 so `w = 3` selects 40; v is NULL for every 5th row, else id*10.
    for id in 0..200 {
        let w = id % 5;
        if id % 5 == 3 {
            db.ok(&format!("INSERT INTO t VALUES ({id}, {w}, NULL);"), &mut s);
        } else {
            db.ok(&format!("INSERT INTO t VALUES ({id}, {w}, {});", id * 10), &mut s);
        }
    }
    db.ok("CREATE INDEX ix_w ON t (w);", &mut s);
    db.ok("CREATE INDEX ix_v ON t (v);", &mut s);
    db.ok("ANALYZE t;", &mut s);

    // Ground truth computed from the seeding rule, NOT from the engine: w = 3 => id % 5 == 3 =>
    // v IS NULL for every one of them. So `w = 3 AND v < 5` selects ZERO rows under any sane
    // reading of `<` on NULL, and the sequential scan agrees (part 1 measured that).
    let a = db.rows("SELECT id FROM t WHERE w = 3 AND v < 5;", &mut s);
    let b = db.rows("SELECT id FROM t WHERE v < 5 AND w = 3;", &mut s);
    println!("EXPLAIN A: {}", db.explain("SELECT id FROM t WHERE w = 3 AND v < 5;", &mut s));
    println!("EXPLAIN B: {}", db.explain("SELECT id FROM t WHERE v < 5 AND w = 3;", &mut s));
    println!("A rows = {}, B rows = {}", a.len(), b.len());
    assert_eq!(a.len(), 0, "`w = 3 AND v < 5`: every w=3 row has v NULL, so nothing matches");
    assert_eq!(b.len(), 0, "`v < 5 AND w = 3`: same predicate, same answer required");
}

/// REPRODUCTION of the lane's own disclosure at
/// `tests/d179_secondary_strict_lower_counters.rs:46`, on ITS fixture — `VARCHAR(200)`, not the
/// narrow tables measured above. The lane reports the flip sitting between 9900 and 9910:
///
/// ```text
///   v > 9900   9 rows   Filter (#1 > 9900) (rows=9 cost=79.00)          <- sequential
///   v > 9910   8 rows   Index scan on t (col 1, (9910, inf)) cost=73.06 <- index
/// ```
#[test]
fn adv_reproduce_the_lanes_eight_row_crossover() {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, label VARCHAR(200));", &mut s);
    let label = "x".repeat(180);
    for i in 0..1000 {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {}, '{label}');", i * 10), &mut s);
    }
    db.ok("CREATE INDEX ix_v ON t (v);", &mut s);
    db.ok("ANALYZE t;", &mut s);

    for k in [9880, 9890, 9900, 9910, 9920] {
        println!("v > {k}: {}", db.explain(&format!("SELECT id FROM t WHERE v > {k};"), &mut s));
    }
    let seq = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::SeqScan { table: "t".into() }),
        predicate: BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Column(1)),
            operator: TokenType::Greater,
            right: Box::new(BoundExpr::Literal(Value::Integer(0))),
        },
    };
    let seq_cost = cost(&seq, &db.catalog).cost;
    println!("seq_cost={seq_cost:.2}  table_pages={:.0}  => index wins up to r < {:.2}",
        seq_cost - 20.0, (seq_cost - 9.0) / 8.0);
    panic!("REPORTING ARM — read stdout above");
}
