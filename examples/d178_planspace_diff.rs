//! D178 — **did narrowing what the optimizer may PROPOSE shrink SELECT's plan space?**
//!
//! The D178 H1 fix stops `build_index_scan` proposing a secondary `IndexScan` with a strictly
//! excluded lower bound, because `lower` cannot build one. That is a change to SELECT's planning,
//! not just to DML's, and "it only removes plans that could not run" is a CLAIM. This dumps the
//! evidence for it: for a corpus of predicates over three fixtures, the chosen plan and the actual
//! result of running the query.
//!
//! Run it on the fixed tree and on a tree with the guard removed, and diff the two outputs. The
//! claim holds only if **every** line that differs is one where the unguarded tree ERRORS. A line
//! that differs in the PLAN while both trees answer would mean a query that used to work now takes
//! a different access path, which is a behaviour change of a completely different kind.
//!
//! The plan is printed whole, not just its top line: `EXPLAIN` renders a tree and the access path
//! is at the BOTTOM of it. An earlier probe in this row printed only the first line and got
//! `Projection [#0]` for every row, which says nothing about whether an index was used.

use std::fmt::Write as _;
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
            .open(dir.path().join("ps.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("ps.wal")).unwrap());
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

    fn ok(&mut self, sql: &str, s: &mut Session) {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"));
    }
}

/// The access path a plan actually uses: the DEEPEST line of the `EXPLAIN` tree, trimmed.
fn access_path(plan: &str) -> String {
    plan.lines()
        .filter(|l| !l.trim().is_empty())
        .next_back()
        .unwrap_or("<empty plan>")
        .trim()
        .to_string()
}

fn build(n: i64, analyze: bool, secondary: bool) -> (Db, Session) {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, w INTEGER, label VARCHAR(16));", &mut s);
    for i in 0..n {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {}, {}, 'row');", i * 10, i % 7), &mut s);
    }
    if secondary {
        db.ok("CREATE INDEX ixv ON t (v);", &mut s);
        db.ok("CREATE INDEX ixw ON t (w);", &mut s);
    }
    if analyze {
        db.ok("ANALYZE t;", &mut s);
    }
    (db, s)
}

fn main() {
    let mut out = String::new();
    let _ = writeln!(out, "D178 PLAN-SPACE DIFFERENTIAL — access path + result, per predicate.");
    let _ = writeln!(out, "Diff this against the same binary built with the H1 guard REMOVED.");
    let _ = writeln!(out, "CLAIM UNDER TEST: every differing line is one where the unguarded tree ERRORS.\n");

    // Three fixtures, because whether the optimizer picks the index depends on the row estimate,
    // which depends on both size and whether statistics exist. A corpus over one fixture would
    // only probe one corner of the cost comparison.
    for (n, analyze, secondary) in
        [(800i64, true, true), (800, false, true), (200, true, true), (800, true, false)]
    {
        let (mut db, mut s) = build(n, analyze, secondary);
        let _ = writeln!(
            out,
            "== fixture n={n} analyze={analyze} secondary_indexes={secondary} =="
        );
        for p in [
            // primary key — every bound shape; none of these is affected by the guard
            "id = 400",
            "id > 400",
            "id >= 400",
            "id < 400",
            "id <= 400",
            "id != 400",
            // secondary `v` — the EXCLUDED lower bound is the shape the guard touches
            "v = 4000",
            "v > 7980",
            "v > 7000",
            "v > 4000",
            "v > 0",
            "v >= 7980",
            "v < 100",
            "v <= 100",
            // secondary `w`, low cardinality — a different selectivity regime
            "w = 3",
            "w > 5",
            "w >= 5",
            // conjunctions: an unlowerable conjunct beside a lowerable one, both orders
            "v > 7980 AND id = 799",
            "id = 799 AND v > 7980",
            "v > 7980 AND w = 1",
            "v > 7980 AND v < 7995",
            "id > 400 AND v > 7980",
            // unindexed, and mixtures with it
            "label = 'row'",
            "label = 'zz'",
            "label = 'row' AND v > 7980",
            "v > 7980 AND label = 'row'",
        ] {
            let sql = format!("SELECT id FROM t WHERE {p};");
            let plan = match db.exec(&format!("EXPLAIN {sql}"), &mut s) {
                Ok(Outcome::Explain(t)) => access_path(&t),
                Ok(_) => "<not a plan>".to_string(),
                Err(e) => format!("EXPLAIN ERROR: {e}"),
            };
            let res = match db.exec(&sql, &mut s) {
                Ok(Outcome::Rows(r)) => {
                    // Checksum the rows, not just the count: two plans returning the same NUMBER of
                    // different rows is exactly the wrong-answer bug a count cannot see.
                    let mut ids: Vec<i64> =
                        r.iter().filter_map(|x| match &x[0] {
                            Value::Integer(i) => Some(*i as i64),
                            _ => None,
                        }).collect();
                    ids.sort();
                    format!("{} rows sum={}", ids.len(), ids.iter().sum::<i64>())
                }
                Ok(_) => "<not rows>".to_string(),
                Err(e) => format!("ERROR: {e}"),
            };
            let _ = writeln!(out, "  {p:<32} | {plan:<46} | {res}");
        }
        let _ = writeln!(out);
    }
    print!("{out}");
}
