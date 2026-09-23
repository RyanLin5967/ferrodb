//! D179 — does the OPTIMIZER reach the secondary index for `WHERE v > k`, end to end?
//!
//! `tests/d179_secondary_strict_lower.rs` answers the correctness half by driving `lower` with a
//! hand-built plan. This file answers the other half, which no correctness arm can: that a plain
//! `SELECT` now produces that plan instead of a sequential scan. Before D179 it could not — the
//! optimizer either refused (`"lower bound sec index isn't supported"`) or, after D178, declined to
//! propose the plan at all and fell back to a full scan.
//!
//! # Why this file holds exactly ONE test, and must keep holding exactly one
//!
//! `SEQ_SCAN_TUPLES` and `INDEX_SCAN_ENTRIES` are **process-global atomics**, scoped to a phase by
//! reading them twice and subtracting. `cargo test` runs a file's tests inside one binary on a
//! thread pool, so a second test here would run its own SQL inside this one's measurement window
//! and add to the same counters. The subtraction would then be silently wrong — and wrong in the
//! direction that looks like a defect. One test per binary makes that unrepresentable rather than
//! merely discouraged. This is the same rule, and the same reason, as
//! `tests/d178_dml_index_counters.rs`.
//!
//! ⛔ **Do not add a second `#[test]` here.**

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::index_scan::index_scan_counters;
use ferrodb::execution::seq_scan::seq_scan_counters;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Large enough that a full scan is unmistakable against an index scan's handful of entries, small
/// enough to stay quick in a debug build.
const ROWS: i32 = 1000;
/// `v = id * 10`, so the largest `v` is 9990 and `v > 9910` selects the top 8 rows.
///
/// # Why 9910 and not a rounder cutoff
///
/// D179 makes the plan BUILDABLE; the cost model still decides whether to pick it, and on this
/// fixture it prefers a sequential scan until the estimate drops to 8 rows. Measured with `EXPLAIN`
/// over the same fixture, the flip is sharp and sits between 9900 and 9910:
///
/// ```text
///   v > 9900   9 rows   Filter (#1 > 9900) (rows=9 cost=79.00)          <- sequential
///   v > 9910   8 rows   Index scan on t (col 1, (9910, inf)) cost=73.06 <- index
/// ```
///
/// That is the cost model's judgement, not D179's: a secondary `IndexScan` is charged
/// `2 * DEFAULT_RANDOM_PAGE_COST` per row (the heap fetch and the primary-index resolution) against
/// a sequential scan's `DEFAULT_CPU_TUPLE_COST` of 0.01, so a small table is cheap to read whole.
/// Whether that trade is calibrated right is a cost-model question and a separate row. This test
/// asserts only that the plan is now REACHABLE where the cost model wants it, which before D179 it
/// was not at any cutoff.
///
/// The `(9910, inf)` in that plan line is the strict lower bound itself: before D179 no
/// `PhysicalPlan` carrying it could be lowered at all.
const CUTOFF: i32 = 9910;
const EXPECTED_ROWS: usize = 8;

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
            .open(dir.path().join("d179c.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d179c.wal")).unwrap());
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
            _ => panic!("{sql}: expected Rows"),
        }
    }
}

/// `(sequential tuples pulled, index entries walked, index scans)` for one statement, on a FRESH
/// database, together with the rows it returned.
///
/// Fresh per call so an earlier arm cannot change how much a later one reads. The window opens
/// AFTER every bit of setup — the inserts, the `CREATE INDEX` and the `ANALYZE` must not land
/// inside it — and closes after the plan has been dropped, because every scan flushes its count in
/// `Drop`. `rows` returns an owned `Vec`, so the plan is already gone.
fn signature(sql: &str) -> (u64, u64, u64, usize) {
    let mut db = Db::new();
    let mut s = Session::new();
    // `label` is wide on purpose. It carries no index — it is what the fire-check reads — and its
    // width is what puts the table over enough pages for a sequential scan to cost more than an
    // 8-row index scan. A narrow table is so cheap to read whole that the cost model prefers the
    // full scan at every cutoff down to 2 rows, and this test would then be pinning a 2-row plan.
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, label VARCHAR(200));", &mut s);
    let label = "row".repeat(20);
    for i in 0..ROWS {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {}, '{label}');", i * 10), &mut s);
    }
    db.ok("CREATE INDEX ix_v ON t (v);", &mut s);
    // D179's plan has to WIN on cost to be chosen, and `bound_selectivity` needs min/max to tell a
    // 9-row range from a 250-row default guess. Without statistics the engine correctly prefers a
    // sequential scan here, which is a different (and defensible) outcome from the one under test.
    db.ok("ANALYZE t;", &mut s);

    let before = (seq_scan_counters().1, index_scan_counters().1, index_scan_counters().0);
    let rows = db.rows(sql, &mut s);
    let after = (seq_scan_counters().1, index_scan_counters().1, index_scan_counters().0);
    (after.0 - before.0, after.1 - before.1, after.2 - before.2, rows.len())
}

#[test]
fn a_strict_lower_bound_on_a_secondary_column_reaches_the_index() {
    // ---- 1. The FIRE-CHECK first, so a later zero can mean something. -------------------------
    //
    // `label` carries no index, so this statement has no access path but a sequential scan, with
    // the D179 change and without it. If it does not come back at ROWS, the counter is not
    // reporting and every assertion below is vacuous.
    let (fire_seq, fire_idx, fire_scans, fire_rows) = signature("SELECT id FROM t WHERE label = 'zz';");
    assert_eq!(
        fire_seq, ROWS as u64,
        "the counter did not report a full scan for an UNINDEXED predicate ({fire_seq} tuples, \
         expected {ROWS}), so it cannot report the absence of one either"
    );
    assert_eq!(fire_idx, 0, "an unindexed predicate walked {fire_idx} index entries");
    assert_eq!(fire_scans, 0, "an unindexed predicate ran {fire_scans} index scans");
    assert_eq!(fire_rows, 0, "no row has label 'zz'");

    // ---- 2. The treatment. --------------------------------------------------------------------
    //
    // Before D179 this exact statement either errored with
    // `"lower bound sec index isn't supported"` or (after D178) read all ROWS tuples sequentially.
    // Both are recorded in `bench/d178_run1_BEFORE_RAW.txt`.
    let (seq, entries, scans, rows) = signature(&format!("SELECT id FROM t WHERE v > {CUTOFF};"));

    assert_eq!(
        rows, EXPECTED_ROWS,
        "`v > {CUTOFF}` returned {rows} rows, expected {EXPECTED_ROWS} — a statement that stopped \
         returning the right answer is not one that got faster"
    );
    assert_eq!(
        seq, 0,
        "`v > {CUTOFF}` still pulled {seq} tuples off the heap sequentially out of {ROWS}; the \
         fire-check above read {fire_seq}, so the counter works and this is a full scan"
    );
    assert!(scans > 0, "`v > {CUTOFF}` read nothing sequentially AND ran no index scan: it did no work");
    assert!(
        entries > 0 && entries < ROWS as u64,
        "`v > {CUTOFF}` walked {entries} index entries; expected a handful, and strictly fewer \
         than the {ROWS} a full walk of the tree would cost"
    );

    // ---- 3. The strictness itself, end to end. ------------------------------------------------
    //
    // `>` and `>=` differ by exactly the row ON the boundary. A plan that reached the index but
    // dropped the exclusion would satisfy every assertion above and fail here.
    let (_, _, _, ge_rows) = signature(&format!("SELECT id FROM t WHERE v >= {CUTOFF};"));
    assert_eq!(
        ge_rows,
        EXPECTED_ROWS + 1,
        "`v >= {CUTOFF}` returned {ge_rows} rows; it must return exactly one more than `v > {CUTOFF}` \
         returned ({rows}), namely the row whose v IS {CUTOFF}"
    );
}
