//! D181 — **the order a conjunction is typed must not change what the engine reads.**
//!
//! `bench/d181_conjunct_order_BEFORE_RAW.txt` measured that it did, by a factor that GROWS with the
//! table: 400x at n=800 and 800x at n=1600, because the loser reads the whole table while the
//! winner reads two index entries. That is a complexity-class gap, O(n) against O(1), not a
//! constant. This file is the law that gap violated, asserted rather than measured.
//!
//! # A single passing case is not order-independence
//!
//! The fixture is built so the CHEAPER conjunct is typed SECOND in one arm and FIRST in the other.
//! An implementation that takes the leftmost usable conjunct — which is what
//! `conjuncts.iter().position(..)` did — passes the first arm and fails the second. An
//! implementation that costs every candidate and takes the cheapest reports the SAME counters for
//! both, and that identity is what is asserted. Not a threshold, not a ratio: the same integers.
//!
//! # Every expected value here was pre-registered
//!
//! `bench/d181_prereg.txt`, committed before this test was ever run, derives all of them by hand
//! from `cost_model.rs` and `sec_index_scan.rs`: 17.01 for the `sel` candidate against 4018.0 for
//! the `broad` candidate against `table_pages + 20 >= 21` for the sequential scan, hence
//! `seq_tuples = 0, index_scans = 1, index_entries = 2, rows = 1` for both orders. The margins are
//! STRICT on both sides, so this test does not rest on tie-breaking — the exact-tie case is
//! `tests/d181_tie_break.rs`.
//!
//! `index_entries = 2` rather than 1 because the scan must read the first entry past the upper
//! bound in order to learn that it is past it, and that entry is counted where the tree yielded it.
//!
//! # Why this file holds exactly ONE test, and must keep holding exactly one
//!
//! `SEQ_SCAN_TUPLES` and `INDEX_SCAN_ENTRIES` are **process-global atomics**, scoped by reading
//! twice and subtracting. `cargo test` runs a file's tests inside one binary on a thread pool, so a
//! second test here would run its own SQL inside this one's window and corrupt the subtraction —
//! silently, and in the direction that looks like a defect. Same rule and same reason as
//! `tests/d178_dml_index_counters.rs` and `tests/d179_secondary_strict_lower_counters.rs`.
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

/// 1000 rows makes `broad = k%2` match 500 — enough that reading it instead of `sel` is
/// unmistakable — and keeps `tree_height` at 2, which is what the pre-registered arithmetic
/// assumes.
const ROWS: i32 = 1000;
/// In the middle of the table. A prefix or suffix key cannot tell a scan that stops early from one
/// that does not.
const KEY: i32 = 501;

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
            .open(dir.path().join("d181o.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d181o.wal")).unwrap());
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

/// `(seq_scans, seq_tuples, index_scans, index_entries, rows)` for one statement, on a FRESH
/// database.
///
/// Fresh per call so no arm's reading can be changed by another arm's fixture. The window opens
/// AFTER all setup — the inserts, both `CREATE INDEX`es and the `ANALYZE` must not land inside it —
/// and closes after the plan has been dropped, because every scan flushes its count in `Drop`.
/// `rows` returns an owned `Vec`, so the plan is already gone when it returns.
fn signature(sql: &str) -> (u64, u64, u64, u64, usize) {
    let mut db = Db::new();
    let mut s = Session::new();
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, sel INTEGER, broad INTEGER, pad VARCHAR(16));", &mut s);
    for i in 0..ROWS {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {i}, {}, 'row');", i % 2), &mut s);
    }
    db.ok("CREATE INDEX ix_sel ON t (sel);", &mut s);
    db.ok("CREATE INDEX ix_broad ON t (broad);", &mut s);
    // Without statistics both columns get DEFAULT_DISTINCT, both candidates cost the same, and
    // both lose to the sequential scan — the defect is invisible and so is the fix. See the
    // `ANALYZE: no` half of bench/d181_conjunct_order_BEFORE_RAW.txt, where the two orders agree
    // at every size for that reason and not because anything was right.
    db.ok("ANALYZE t;", &mut s);

    let before = (seq_scan_counters(), index_scan_counters());
    let rows = db.rows(sql, &mut s);
    let after = (seq_scan_counters(), index_scan_counters());
    (
        after.0 .0 - before.0 .0,
        after.0 .1 - before.0 .1,
        after.1 .0 - before.1 .0,
        after.1 .1 - before.1 .1,
        rows.len(),
    )
}

#[test]
fn the_two_typing_orders_of_one_predicate_examine_the_same_rows() {
    // ---- 1. The FIRE-CHECK, first, so the zeros below can mean anything. ----------------------
    //
    // `pad` carries no index, so `has_index` is false, `build_index_scan` proposes nothing and the
    // statement has no access path but a full scan — with this fix and without it. Pre-registered
    // at 1000 sequential tuples.
    let (fire_scans, fire_seq, fire_iscans, fire_entries, fire_rows) =
        signature("SELECT id FROM t WHERE pad = 'zz';");
    assert_eq!(
        fire_seq, ROWS as u64,
        "the counter reported {fire_seq} sequential tuples for an UNINDEXED predicate, not {ROWS}; \
         it cannot report a full scan, so it cannot report the absence of one and every assertion \
         below is vacuous"
    );
    assert_eq!(fire_scans, 1, "one sequential scan for the unindexed predicate");
    assert_eq!(fire_iscans, 0, "an unindexed predicate must reach no index");
    assert_eq!(fire_entries, 0, "an unindexed predicate must walk no index entries");
    assert_eq!(fire_rows, 0, "no row has pad = 'zz'");

    // ---- 2. The same predicate, typed both ways. ----------------------------------------------
    //
    // `sel = KEY` matches one row; `broad = KEY % 2` matches 500. The cheap conjunct is typed
    // SECOND in `broad_first`, which is the arm a leftmost-wins implementation gets wrong.
    let b = KEY % 2;
    let sel_first = signature(&format!("SELECT id FROM t WHERE sel = {KEY} AND broad = {b};"));
    let broad_first = signature(&format!("SELECT id FROM t WHERE broad = {b} AND sel = {KEY};"));

    // ---- 3. The pre-registered values, per arm. -----------------------------------------------
    //
    // Asserted as constants, not as "whatever the other arm said" — two arms that agree on a wrong
    // number agree just as well as two that agree on the right one. bench/d181_prereg.txt derives
    // every one of these from cost_model.rs before this test was first run.
    for (name, sig) in [("sel_first", sel_first), ("broad_first", broad_first)] {
        let (scans, seq, iscans, entries, rows) = sig;
        assert_eq!(rows, 1, "{name}: expected the one row with sel = {KEY}, got {rows}");
        assert_eq!(
            seq, 0,
            "{name}: pulled {seq} tuples off the heap sequentially out of {ROWS}; the fire-check \
             read {fire_seq}, so the counter works and this is a full scan"
        );
        assert_eq!(scans, 0, "{name}: ran {scans} sequential scans, expected none");
        assert_eq!(iscans, 1, "{name}: ran {iscans} index scans, expected exactly 1");
        assert_eq!(
            entries, 2,
            "{name}: walked {entries} index entries, expected 2 — the matching entry plus the one \
             past the upper bound that the scan must read to learn it is past it"
        );
    }

    // ---- 4. The law itself. --------------------------------------------------------------------
    //
    // Redundant with §3 by construction, and kept because it is the sentence this file exists for.
    // If a future change moves both arms together, §3 catches it; if it moves only one, this says
    // what broke in the words of the defect.
    assert_eq!(
        sel_first, broad_first,
        "the two typing orders of ONE predicate examined different things — \
         (seq_scans, seq_tuples, index_scans, index_entries, rows) was {sel_first:?} typed one way \
         and {broad_first:?} typed the other. Index selection is reading the predicate's word \
         order again."
    );
}
