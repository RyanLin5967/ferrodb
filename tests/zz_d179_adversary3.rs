//! ADVERSARIAL part 3 — does D181 newly CHOOSE a plan that gives a different ANSWER?
//!
//! Part 1 measured that a secondary `IndexScan` with an upper bound emits rows whose indexed value
//! is NULL, while `SeqScan + Filter` rejects them. Part 2 measured that the cost model only prefers
//! a secondary index below ~2-3 estimated rows, so reaching the bad plan needs a very selective
//! upper bound.
//!
//! This file builds exactly that: a predicate with TWO indexed conjuncts where
//!   * the LEFTMOST conjunct is a broad equality whose index candidate loses to the sequential scan
//!   * the SECOND conjunct is a very selective `<` on a column that holds NULLs
//!
//! Pre-D181, `conjuncts.iter().position(..)` builds only the leftmost candidate, it loses, and the
//! statement runs a sequential scan — the CORRECT answer. Post-D181 every conjunct is costed, the
//! selective `<` wins, and the NULL rows leak through the residual filter.

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
            .read(true).write(true).create(true).truncate(true)
            .open(dir.path().join("adv3.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("adv3.wal")).unwrap());
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
        match self.ok(sql, s) { Outcome::Rows(r) => r, _ => panic!("{sql} did not return rows") }
    }

    fn explain(&mut self, sql: &str, s: &mut Session) -> String {
        match self.ok(&format!("EXPLAIN {sql}"), s) {
            Outcome::Explain(t) => t.replace('\n', " | "),
            _ => panic!("EXPLAIN did not return Explain"),
        }
    }
}

/// 1,000 rows. `w = id % 2`, so `w = 1` selects 500 — a broad equality whose index candidate is
/// far more expensive than a sequential scan. `v` is NULL for every ODD id (i.e. for every `w = 1`
/// row) and `id * 10` otherwise, so `v`'s non-null range is 0..9980 and `v < 5` estimates 1 row.
///
/// GROUND TRUTH, from the seeding rule and not from the engine: every row with `w = 1` has
/// `v IS NULL`, so `w = 1 AND v < 5` selects ZERO rows.
fn fixture() -> (Db, Session) {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, w INTEGER NOT NULL, v INTEGER NULL, pad VARCHAR(16));", &mut s);
    for id in 0..1000 {
        let w = id % 2;
        if id % 2 == 1 {
            db.ok(&format!("INSERT INTO t VALUES ({id}, {w}, NULL, 'rowrowrowrow');"), &mut s);
        } else {
            db.ok(&format!("INSERT INTO t VALUES ({id}, {w}, {}, 'rowrowrowrow');", id * 10), &mut s);
        }
    }
    db.ok("CREATE INDEX ix_w ON t (w);", &mut s);
    db.ok("CREATE INDEX ix_v ON t (v);", &mut s);
    db.ok("ANALYZE t;", &mut s);
    (db, s)
}

#[test]
fn adv_a_selective_upper_bound_on_a_null_bearing_indexed_column() {
    let (mut db, mut s) = fixture();

    // Control 1: the predicate with NO second conjunct, forced down the sequential path by a
    // predicate the optimizer cannot index (`pad`), establishes the true answer.
    let truth = db.rows("SELECT id FROM t WHERE w = 1 AND v < 5 AND pad = 'rowrowrowrow';", &mut s);
    println!("CONTROL (with an unindexable conjunct forcing seq): {} rows", truth.len());
    println!("CONTROL EXPLAIN: {}", db.explain("SELECT id FROM t WHERE w = 1 AND v < 5 AND pad = 'rowrowrowrow';", &mut s));

    println!("EXPLAIN `w = 1 AND v < 5`: {}", db.explain("SELECT id FROM t WHERE w = 1 AND v < 5;", &mut s));
    let got = db.rows("SELECT id FROM t WHERE w = 1 AND v < 5;", &mut s);
    println!("`w = 1 AND v < 5` returned {} rows", got.len());

    // The SAME predicate typed the other way round. Pre-D181 `position` takes the leftmost usable
    // conjunct, so this order picks `v < 5` where the other order picks `w = 1`.
    println!("EXPLAIN `v < 5 AND w = 1`: {}", db.explain("SELECT id FROM t WHERE v < 5 AND w = 1;", &mut s));
    let swapped = db.rows("SELECT id FROM t WHERE v < 5 AND w = 1;", &mut s);
    println!("`v < 5 AND w = 1` returned {} rows", swapped.len());

    // Single conjunct, for completeness: is `v < 5` alone also served by the index?
    println!("EXPLAIN `v < 5`: {}", db.explain("SELECT id FROM t WHERE v < 5;", &mut s));
    let solo = db.rows("SELECT id FROM t WHERE v < 5;", &mut s);
    println!("`v < 5` alone returned {} rows", solo.len());

    assert_eq!(truth.len(), 0, "ground truth: every w=1 row has v NULL");
    assert_eq!(
        got.len(), 0,
        "`w = 1 AND v < 5` returned {} rows; every row with w = 1 has v IS NULL, so the answer is 0. \
         The index path emitted the NULL entries and the residual Filter(w = 1) kept them.",
        got.len()
    );
    assert_eq!(solo.len(), 0, "`v < 5` alone returned {} rows; no non-null v is below 5", solo.len());
}
