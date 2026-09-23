//! D178 — an indexed `UPDATE`/`DELETE` must not read the whole table, and must plan it the way
//! `SELECT` plans the identical predicate.
//!
//! # Why this file holds exactly ONE test, and must keep holding exactly one
//!
//! `SEQ_SCAN_TUPLES` and `INDEX_SCANS` are **process-global atomics**, scoped to a phase by reading
//! them twice and subtracting. `cargo test` runs the tests inside one binary on a thread pool, so a
//! second test in this file would run its own SQL inside this one's measurement window and add to
//! the same counters. The subtraction would then be silently wrong — and wrong in the direction that
//! looks like a defect, which is the worst way for a gate to fail.
//!
//! One test per binary makes that unrepresentable rather than merely discouraged: with nothing else
//! in the file there is nothing to interleave with. The correctness half of D178 — that these
//! statements change the right rows — needs no counters and lives in
//! `tests/d178_dml_index_correctness.rs`, where tests may run in parallel safely.
//!
//! ⛔ **Do not add a second `#[test]` here.** Add it to the correctness file, or to a new file of
//! its own.
//!
//! # What is actually asserted
//!
//! Not a magic number. The property is **one planning path**: after D178 there is a single
//! `optimize` and both `SELECT` and DML reach it, so the same predicate over the same fixture must
//! produce the same access path. The test compares the `(sequentially-scanned tuples, index scans)`
//! signature of an `UPDATE` against the signature of a `SELECT` carrying the identical `WHERE`
//! clause — the invariant, not a constant someone has to keep up to date.
//!
//! The unindexed arm is in the same test on purpose. A pair of zeros is also what a broken counter
//! reports, so the indexed arm's zero means nothing unless a sibling arm in the same process, the
//! same binary and the same run comes back non-zero. Measured before the fix in
//! `bench/d178_run1_BEFORE_RAW.txt`: every one of these arms read `delta * n` tuples.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::index_scan::index_scan_counter;
use ferrodb::execution::seq_scan::seq_scan_counters;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Big enough that a full scan is unmistakable against an index scan's handful of tuples, small
/// enough to stay quick in a debug build.
const ROWS: i64 = 400;

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
            .open(dir.path().join("d178c.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d178c.wal")).unwrap());
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
}

/// `(sequentially-scanned tuples, index scans)` for one statement, on a FRESH database.
///
/// Fresh per call so that an earlier arm's writes cannot change how much a later arm reads — the
/// arms have to be comparable, and a shared fixture that one arm mutates makes them not.
fn signature(sql: &str) -> (u64, u64) {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, label VARCHAR(16));", &mut s);
    for i in 0..ROWS {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {}, 'row');", i * 10), &mut s);
    }
    db.ok("CREATE INDEX ix ON t (v);", &mut s);

    // The window opens AFTER every bit of setup, so index construction and the inserts cannot land
    // inside it. It closes after the statement's plan has been dropped: `SeqScan` flushes its
    // tuple count in `Drop`, so a reading taken while a plan were still alive would miss it.
    let before = (seq_scan_counters().1, index_scan_counter());
    db.ok(sql, &mut s);
    let after = (seq_scan_counters().1, index_scan_counter());
    (after.0 - before.0, after.1 - before.1)
}

#[test]
fn dml_reaches_the_index_and_plans_it_the_way_select_does() {
    // ---- 1. The FIRE-CHECK first, so a later zero can mean something. -----------------------
    //
    // `label` carries no index, so this statement has no access path but a sequential scan, under
    // the fix and without it. If this comes back small, the counter is not reporting and every
    // other assertion in this test is vacuous.
    let (unindexed_tuples, unindexed_index) = signature("UPDATE t SET v = 1 WHERE label = 'zz';");
    assert_eq!(
        unindexed_tuples, ROWS as u64,
        "the counter did not report a full scan for an UNINDEXED predicate, so it cannot report \
         the absence of one either; every assertion below would be vacuous"
    );
    assert_eq!(unindexed_index, 0, "an unindexed predicate must not reach an index");

    // ---- 2. The treatment. --------------------------------------------------------------------
    //
    // Before D178 this read `ROWS` tuples — `build_scan` always built a `SeqScan`. See
    // `bench/d178_run1_BEFORE_RAW.txt`.
    let (update_tuples, update_index) = signature("UPDATE t SET v = 1 WHERE id = 7;");
    assert_eq!(
        update_tuples, 0,
        "an UPDATE on a primary-key equality still read {update_tuples} tuples sequentially, out of \
         a {ROWS}-row table; the unindexed arm above read {unindexed_tuples}, so the counter works"
    );
    assert!(
        update_index > 0,
        "the UPDATE read no tuples sequentially AND used no index — that is a statement that \
         stopped doing work, not one that got faster"
    );

    let (delete_tuples, delete_index) = signature("DELETE FROM t WHERE id = 7;");
    assert_eq!(delete_tuples, 0, "a DELETE on a primary-key equality read {delete_tuples} tuples sequentially");
    assert!(delete_index > 0, "the DELETE used no index and read nothing: it did no work");

    // ---- 3. The actual invariant: ONE planning path. -------------------------------------------
    //
    // This is the assertion that survives a future change to index selection or to the cost model.
    // Whatever `SELECT` decides for a predicate, DML must decide the same, because after D178 there
    // is one `optimize` and both reach it. A constant here would have to be revised every time the
    // cost model moved; this does not.
    for predicate in [
        "id = 7",                 // primary-key equality
        "id > 300",               // primary-key range — `Bound::Excluded` on the primary tree
        "v = 70",                 // secondary-index equality
        "label = 'row'",          // no index at all
        "v > 3990 AND id = 399",  // two indexed conjuncts, one far cheaper (D178 H1, D181)
    ] {
        let select = signature(&format!("SELECT id, v FROM t WHERE {predicate};"));
        let update = signature(&format!("UPDATE t SET label = 'x' WHERE {predicate};"));
        assert_eq!(
            select, update,
            "SELECT and UPDATE planned `WHERE {predicate}` differently — SELECT \
             (seq_tuples, index_scans) = {select:?}, UPDATE = {update:?}. After D178 there is one \
             planning path and they must agree; two answers means a second one has grown back."
        );
    }

    // ---- 4. A second usable conjunct must not cost the statement its index. --------------------
    //
    // `v > 3990` is a strictly-excluded lower bound on a SECONDARY index. Until D179 `lower` could
    // not build one, and `build_index_scan` passed over the conjunct and took `id = 399` instead.
    //
    // ⚠ **BOTH HALVES OF THAT SENTENCE ARE NOW OBSOLETE, AND THIS ASSERTION IS UNCHANGED.** D179
    // makes `v > 3990` buildable, so it is no longer passed over — and D181 stopped
    // `build_index_scan` taking the FIRST usable conjunct, so being no-longer-passed-over does not
    // mean being chosen. Every usable conjunct is costed and the cheapest wins, which on this
    // fixture is the primary-key point lookup `id = 399` by a wide margin.
    //
    // This assertion caught exactly that. With D179 alone it failed at 400 vs 0: `v > 3990` became
    // lowerable, `position` took it, the candidate lost to the sequential scan on cost and dragged
    // the whole statement into a full 400-row scan. D178's gate had been accidentally shielding
    // the plan from D181's first-match defect. The fix was to D181, not to this line.
    let (mixed_tuples, mixed_index) = signature("UPDATE t SET label = 'x' WHERE v > 3990 AND id = 399;");
    assert_eq!(
        mixed_tuples, 0,
        "an unlowerable conjunct beside an indexed one dragged the statement back to a full scan"
    );
    assert!(mixed_index > 0, "the mixed predicate used no index");
}
