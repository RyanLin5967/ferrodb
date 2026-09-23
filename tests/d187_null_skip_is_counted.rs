//! D187 × D181 — **an index entry dropped by the NULL skip is still an entry the scan PULLED.**
//!
//! # What this file enforces, and why nothing did before it
//!
//! `IndexScan::next` and `SecondaryIndexScan::next` each carry two statements that arrived from two
//! branches and meet on the same line:
//!
//! ```text
//! self.examined += 1;                                          // D181: entries PULLED
//! if self.skip_nulls && matches!(key, Value::Null) { continue } // D187: NULL is UNKNOWN vs a bound
//! ```
//!
//! That order is the only correct one. `examined` feeds `INDEX_SCAN_ENTRIES`, which is D181's
//! rows-examined instrument, and the tree really did yield every NULL entry the skip drops. Swap the
//! two lines and the counter under-reports by exactly the number of NULLs skipped — the same work
//! read as a smaller number, i.e. a fabricated improvement, with every answer still correct.
//!
//! Until this file, **no test could see that swap.** Every counter fixture — `d181_*`,
//! `d179_secondary_strict_lower_counters`, `examples/d181_conjunct_order.rs` — inserts integer
//! literals only, so no NULL entry ever reaches `next()` and the skip never fires: those arms read
//! the same integers with the two lines in either order. The merge that put them together cited
//! "D181's arms must still read 2 vs 800" as the check against a wrong resolution; it could not
//! fire (retracted in `artie-research/frontier/LANDING-QUEUE.md`, 2026-09-23T21:35:38Z). This is
//! the check that can.
//!
//! # Every expected value is derived from source, not read off a run
//!
//! **Secondary** — `SELECT id FROM t WHERE v < 5` on [`seed`]'s `t`: bounds `(Unbounded,
//! Excluded(5))`, so `secondary_scan_start` opens the tree at its leftmost entry, which is where the
//! [`SEC_NULLS`] `(Null, pk)` entries live (`type_rank` puts NULL at 0). The scanner is opened with
//! an UNBOUNDED upper, so the executor sees every entry and decides "past" itself:
//!
//! ```text
//!   SEC_NULLS  NULL entries   pulled, counted, skipped
//!   1          (1, pk)        pulled, counted, returned        <- the one v < 5 row
//!   1          (100, pk)      pulled, counted, past the bound  <- terminator, see INDEX_SCAN_ENTRIES
//!   = SEC_NULLS + 2 = 502     (with the lines swapped: 2)
//! ```
//!
//! **Primary** — `SELECT v FROM p WHERE id < 3` on `p`: the primary scanner is opened WITH the
//! upper bound, so `RangeScanner::next` refuses key 3 itself and `IndexScan::next` never sees it:
//!
//! ```text
//!   PK_NULLS   NULL key       pulled, counted, skipped
//!   2          ids 1, 2       pulled, counted, returned
//!   = PK_NULLS + 2 = 3        (with the lines swapped: 2)
//! ```
//!
//! `PK_NULLS` is 1 and cannot be more: a NULL primary key IS insertable (nothing refuses it), but
//! `Value`'s `PartialEq` makes `Null == Null`, so `execution::insert` refuses a second one as a
//! duplicate primary key. One is enough — the assertion is an exact equality, so a shortfall of one
//! fails it as surely as a shortfall of 500.
//!
//! **Why exact equality and not `entries >= K`**: a lower bound alone would pass a scan that pulled
//! the NULLs AND something it should not have. The exact figure pins the whole accounting; its
//! failure message says which term is missing.
//!
//! # Both plans are asserted before any count is believed
//!
//! A counter read off a sequential scan says nothing about an index scan's `next()`. D187's first
//! end-to-end fixture silently got a filtered seq scan: `analyze` computes min/max over NON-NULL
//! values only, so a table that is mostly NULL gives `bound_selectivity` nothing to be selective
//! with. `t` is therefore `tests/d187_null_index_scan.rs::seeded_wide` exactly — 500 NULLs, one
//! `v = 1`, 2500 rows spread to 7597 — which that file proves takes the index. `p` is sized from
//! `cost_model.rs`: index `2*4 + 1 + 2.004*4 = 17.02` against seq+filter `8 + 10.01 + 10.01 =
//! 28.02` (1001 rows, 32-byte rows, 128 per page). The plan line must also show `(-inf, …)`: the
//! whole premise is a scan that STARTS on the NULL prefix, and a scan that started past it would
//! read 2 either way.
//!
//! # Why this file holds exactly ONE test, and must keep holding exactly one
//!
//! `SEQ_SCAN_TUPLES`, `INDEX_SCANS` and `INDEX_SCAN_ENTRIES` are **process-global atomics**, scoped
//! by reading twice and subtracting. `cargo test` runs a file's tests inside one binary on a thread
//! pool, so a second test here would run its own SQL inside this one's window and corrupt the
//! subtraction. Same rule and reason as `tests/d179_secondary_strict_lower_counters.rs`,
//! `tests/d181_conjunct_order_independence.rs` and `tests/d178_dml_index_counters.rs`. Both scans
//! are therefore measured inside this one test, in two windows that do not overlap.
//!
//! ⛔ **Do not add a second `#[test]` here.**

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::binder::binder::Binder;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::index_scan::index_scan_counters;
use ferrodb::execution::seq_scan::seq_scan_counters;
use ferrodb::execution::session::Session;
use ferrodb::optimizer::optimizer::{explain_plan, optimize, pushdown};
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// NULL-valued rows in `t.v`, each of which posts a `(Null, pk)` entry at the front of `iv`.
const SEC_NULLS: u64 = 500;
/// NULL primary keys in `p`. One is the most the engine admits; see the file header.
const PK_NULLS: u64 = 1;
/// Non-NULL primary keys in `p`, `1..=PK_ROWS`.
const PK_ROWS: i32 = 1000;

const SEC_SQL: &str = "SELECT id FROM t WHERE v < 5;";
const SEC_PLAN: &str = "Index scan on t (col 1, (-inf, 5))";
const PK_SQL: &str = "SELECT v FROM p WHERE id < 3;";
const PK_PLAN: &str = "Index scan on p (col 0, (-inf, 3))";

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("d187c.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d187c.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { _dir: dir, catalog, bp, txn, session: Session::new() }
    }

    fn run(&mut self, sql: &str) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        assert_eq!(stmts.len(), 1, "one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    fn sql(&mut self, sql: &str) {
        self.run(sql);
    }

    /// The physical plan the optimizer chooses, rendered. Optimizes only — builds no executor, so
    /// it moves no counter.
    fn explain(&self, sql: &str) -> String {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        let logical = Binder::new(&self.catalog).bind(stmts.remove(0)).expect("bind");
        let physical = optimize(pushdown(logical), &self.catalog).expect("optimize");
        explain_plan(&physical, &self.catalog)
    }
}

/// Both tables in one database. Every insert, index build and `ANALYZE` happens here, before any
/// measurement window opens.
fn seed() -> Db {
    let mut d = Db::new();

    // `t` — `tests/d187_null_index_scan.rs::seeded_wide`, unchanged. The index is built BEFORE the
    // inserts so every row, NULL or not, posts its entry.
    d.sql("CREATE TABLE t (id INTEGER, v INTEGER, w INTEGER);");
    d.sql("CREATE INDEX iv ON t (v);");
    let mut id = 1;
    for _ in 0..SEC_NULLS {
        d.sql(&format!("INSERT INTO t VALUES ({id}, NULL, 1);"));
        id += 1;
    }
    d.sql(&format!("INSERT INTO t VALUES ({id}, 1, 1);"));
    id += 1;
    for k in 0..2500 {
        d.sql(&format!("INSERT INTO t VALUES ({id}, {}, 1);", 100 + 3 * k));
        id += 1;
    }
    d.sql("ANALYZE t;");

    // `p` — one NULL primary key, which sorts to the very front of the primary tree.
    d.sql("CREATE TABLE p (id INTEGER, v INTEGER);");
    d.sql("INSERT INTO p VALUES (NULL, 900);");
    for i in 1..=PK_ROWS {
        d.sql(&format!("INSERT INTO p VALUES ({i}, {});", i * 10));
    }
    d.sql("ANALYZE p;");
    d
}

/// One statement's counters, from a window that opens after `explain` and closes after the plan
/// has been dropped — every scan flushes in `Drop`, and `Outcome::Rows` owns its rows, so by the
/// time `run` returns the executor tree is gone.
struct Window {
    plan: String,
    rows: usize,
    seq_tuples: u64,
    index_scans: u64,
    entries: u64,
}

fn measure(d: &mut Db, sql: &str) -> Window {
    let plan = d.explain(sql);
    let before = (seq_scan_counters().1, index_scan_counters());
    let rows = match d.run(sql) {
        Outcome::Rows(r) => r.len(),
        _ => panic!("{sql}: expected Rows"),
    };
    let after = (seq_scan_counters().1, index_scan_counters());
    Window {
        plan,
        rows,
        seq_tuples: after.0 - before.0,
        index_scans: after.1 .0 - before.1 .0,
        entries: after.1 .1 - before.1 .1,
    }
}

#[test]
fn an_index_entry_the_null_skip_drops_is_still_counted_as_pulled() {
    let mut d = seed();

    // Both windows are measured before anything is asserted, so a failure at one scan still shows
    // the other's numbers.
    let sec = measure(&mut d, SEC_SQL);
    let pk = measure(&mut d, PK_SQL);
    let seen = format!(
        "\n  secondary `{SEC_SQL}`: entries={} rows={} index_scans={} seq_tuples={}\n    plan: {}\
         \n  primary   `{PK_SQL}`: entries={} rows={} index_scans={} seq_tuples={}\n    plan: {}",
        sec.entries, sec.rows, sec.index_scans, sec.seq_tuples, sec.plan.trim(),
        pk.entries, pk.rows, pk.index_scans, pk.seq_tuples, pk.plan.trim(),
    );

    // ---- 1. The plan is the scan under test, and it starts ON the NULL prefix. ------------------
    assert!(
        sec.plan.contains(SEC_PLAN),
        "the secondary arm only tests SecondaryIndexScan::next if the optimizer picks it with an \
         unbounded lower end; expected `{SEC_PLAN}`{seen}"
    );
    assert!(
        pk.plan.contains(PK_PLAN),
        "the primary arm only tests IndexScan::next if the optimizer picks it with an unbounded \
         lower end; expected `{PK_PLAN}`{seen}"
    );

    // ---- 2. The answers are right, so no count below is read off a wrong answer. ---------------
    //
    // This is D187's own correctness (`NULL < k` is UNKNOWN), repeated here only as a precondition.
    // The line swap this file exists to catch does NOT change it — the NULLs are still skipped.
    assert_eq!(sec.rows, 1, "exactly one row has v < 5; the NULL-valued rows are UNKNOWN{seen}");
    assert_eq!(pk.rows, 2, "ids 1 and 2 satisfy id < 3; the NULL key is UNKNOWN{seen}");

    // ---- 3. Each window holds exactly one index scan and no heap walk. -------------------------
    assert_eq!(
        (sec.index_scans, sec.seq_tuples),
        (1, 0),
        "the secondary window must contain exactly one index scan and no sequential one{seen}"
    );
    assert_eq!(
        (pk.index_scans, pk.seq_tuples),
        (1, 0),
        "the primary window must contain exactly one index scan and no sequential one{seen}"
    );

    // ---- 4. THE GUARD: entries PULLED include every NULL entry the skip dropped. ---------------
    let sec_want = SEC_NULLS + 1 + 1;
    assert_eq!(
        sec.entries,
        sec_want,
        "SecondaryIndexScan pulled {SEC_NULLS} NULL entries + 1 match + 1 terminator = {sec_want}, \
         and INDEX_SCAN_ENTRIES must say so. A reading of exactly {} ({SEC_NULLS} short) means \
         `examined += 1` has moved BELOW the NULL skip in `SecondaryIndexScan::next`: the NULLs are \
         still pulled, just no longer counted — a fabricated improvement, not a real one{seen}",
        sec_want - SEC_NULLS,
    );
    let pk_want = PK_NULLS + 2;
    assert_eq!(
        pk.entries,
        pk_want,
        "IndexScan pulled {PK_NULLS} NULL key + 2 matches = {pk_want} (key 3 is refused inside \
         RangeScanner and never reaches next()), and INDEX_SCAN_ENTRIES must say so. A reading of \
         exactly {} means `examined += 1` has moved BELOW the NULL skip in `IndexScan::next`{seen}",
        pk_want - PK_NULLS,
    );
}
