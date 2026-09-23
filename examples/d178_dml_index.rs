//! D178 — **can an `UPDATE`/`DELETE` reach an index?** Answered with an INTEGER, not a stopwatch.
//!
//! # What this measures and why it is a count
//!
//! `build_scan` (`src/planner/plan.rs`) is the only planning site for `Stmt::Update` and
//! `Stmt::Delete`. At `fe40276` it opens the heap, builds a `SeqScan` and wraps it in a `Filter`;
//! it never consults an index. `optimize`, which does the index-selection rewrite, is reached only
//! by `Stmt::Select`. So SELECT may get an `IndexScan` and UPDATE/DELETE structurally cannot, and
//! every one of them is O(table).
//!
//! D176 measured the consequence on the merge path — a `MERGE;` writing `delta` rows pulls
//! `delta * n` tuples off the heap, because publish issues one `UPDATE` per changed row. This run
//! measures the gap at its source, with a plain session `UPDATE`, and it reuses D176's counters
//! rather than inventing new ones: `SEQ_SCANS`/`SEQ_SCAN_TUPLES` (`execution::seq_scan`) and
//! `INDEX_SCANS` (`execution::index_scan`), cherry-picked from `d176-merge-rowcount` as `81173de`.
//!
//! A duration cannot settle this. Two of this project's timing runs have been voided by a shared
//! machine (`bench/d101_rerun_VOID_disk_emergency.txt`, rc=137). An integer has neither failure
//! mode: the count of tuples a statement pulls is the same on a quiet box and on a loaded one.
//!
//! # The four arms
//!
//! | arm | statement | indexed? | what it is for |
//! |---|---|---|---|
//! | A | `UPDATE t SET v = <lit> WHERE id = <k>` | yes, pk | the treatment |
//! | B | `UPDATE t SET v = <lit> WHERE label = 'zz'` | **no** | the CONTROL and the FIRE-CHECK |
//! | C | `DELETE FROM t WHERE id = <k>` | yes, pk | the treatment, other statement |
//! | D | `SELECT v FROM t WHERE id = <k>` | yes, pk | the SELECT control |
//!
//! **ARM B IS THE FIRE-CHECK AND IT IS NOT OPTIONAL.** A flat curve is also what a broken counter
//! produces. ARM B runs in the same process, the same binary and the same run as ARM A, over a
//! column that carries no index, so `has_index` is false, `build_index_scan` returns `None`, and
//! `optimize` falls through to `Filter { SeqScan }` — the identical executor tree `build_scan`
//! builds today. It must come back proportional to `n` in BOTH runs. If ARM A and ARM B are both
//! flat, the instrument is blind and this run reports NO VERDICT rather than a win (F4).
//!
//! ARM B is also the control in the strict sense: the change under test cannot affect it. If it
//! MOVES between the BEFORE and the AFTER run, the two halves did not come from the same base and
//! both are void (F3).
//!
//! **ARM A's flat curve means nothing on its own either.** `INDEX_SCANS` must be non-zero, and the
//! rows-affected count must be exactly 1 per statement. A statement that stopped doing work is
//! also flat, and would otherwise read as a complexity-class win (F2, F6).
//!
//! # H1 — the named hazard, probed rather than assumed
//!
//! Routing DML through `optimize` makes it inherit SELECT's planning defects as well as its wins.
//! At the time of this run `lower` refused a strictly-excluded lower bound on a SECONDARY index
//! (`optimizer.rs`, `"lower bound sec index isn't supported"`), and `predicate_to_bounds` maps
//! `col > v` to exactly that. The probe asked whether the cost model actually CHOOSES that plan on
//! the SELECT path — i.e. whether this was a live, pre-existing SELECT bug or unreachable
//! arithmetic. It is reported, not a pass/fail gate here.
//!
//! ⚠ **D179 REMOVED THE REFUSAL THE PROBE ASKS ABOUT, so re-running this harness today cannot
//! answer the question its own header poses.** `lower` now builds that plan: it opens the scan at
//! `(v, Null)` and skips the leading `sec == v` run. A fresh run prints `OK, N rows` with an
//! `Index scan` plan on every H1 line, which is the correct answer to a different question — *does
//! `v > k` reach the index* — and says nothing about a guard that no longer exists. The H1 lines in
//! `bench/d178_run1_BEFORE_RAW.txt` and `bench/d178_run3_AFTER_RAW.txt` remain valid as a record of
//! the tree at the time they were taken. Index USE is now asserted in
//! `tests/d179_secondary_strict_lower_counters.rs`.
//!
//! Pre-registration: `bench/d178_prereg.txt`, committed before the first number.

use std::fmt::Write as _;
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
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

/// 500 -> 4000 is the 8x span the pre-registration requires.
const SIZES: [i64; 4] = [500, 1000, 2000, 4000];
/// Statements per cell on the size axis. Held CONSTANT — that is what makes it a size axis.
const DELTA: usize = 4;
/// Repeats per cell. Every repeat should produce the IDENTICAL integer; min and max are reported
/// separately so that a spread is visible rather than averaged away.
const REPS: usize = 5;

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
            .open(dir.path().join("d178.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d178.wal")).unwrap());
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

    /// Rows a statement changed. Panics on any other outcome, so a statement that silently became
    /// a no-op cannot pass as a fast one.
    fn affected(&mut self, sql: &str, s: &mut Session) -> usize {
        match self.ok(sql, s) {
            Outcome::Affected(n) => n,
            other => panic!("{sql}: expected Affected, got {}", describe(&other)),
        }
    }

    fn rows(&mut self, sql: &str, s: &mut Session) -> Vec<Vec<Value>> {
        match self.ok(sql, s) {
            Outcome::Rows(r) => r,
            other => panic!("{sql}: expected Rows, got {}", describe(&other)),
        }
    }

    fn plan(&mut self, sql: &str, s: &mut Session) -> String {
        match self.ok(&format!("EXPLAIN {sql}"), s) {
            Outcome::Explain(t) => t,
            other => panic!("EXPLAIN {sql}: expected a plan, got {}", describe(&other)),
        }
    }
}

/// Name an `Outcome` variant without deriving `Debug` on a core enum for a harness's benefit.
/// A panic message has to say what actually came back; adding a derive to `execution::executor`
/// to get one would be the harness editing the engine.
fn describe(o: &Outcome) -> &'static str {
    match o {
        Outcome::Rows(_) => "Rows",
        Outcome::Affected(_) => "Affected",
        Outcome::Explain(_) => "Explain",
        Outcome::Agent(_) => "Agent",
        Outcome::Table(_) => "Table",
        Outcome::Ok => "Ok",
    }
}

/// One counter reading: `(seq scans, tuples pulled, index scans)`.
#[derive(Clone, Copy)]
struct Counts(u64, u64, u64);

fn read_counts() -> Counts {
    let (s, t) = seq_scan_counters();
    Counts(s, t, index_scan_counter())
}

/// Read twice and subtract to scope a phase. Exact here because the harness is single-threaded;
/// it would not be under concurrency, and this run claims nothing about the concurrent case.
fn since(a: Counts, b: Counts) -> Counts {
    Counts(b.0 - a.0, b.1 - a.1, b.2 - a.2)
}

/// `CREATE TABLE t (id, v, label)` with `n` rows, ids `0..n-1`, `v = id * 10`, `label = 'row'`.
///
/// No `ANALYZE`. Column 0 is the primary key and `apply_unique_key_fact` (cost_model.rs, D56)
/// makes its distinct count equal the row count as a SCHEMA fact, so a point lookup on it prefers
/// the index with or without statistics. A cell carried by a statistic a real workload might not
/// have would be measuring the fixture.
fn seed(db: &mut Db, s: &mut Session, n: i64) {
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, label VARCHAR(16));", s);
    for i in 0..n {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {}, 'row');", i * 10), s);
    }
}

/// The keys each arm touches: `DELTA` ids spread across the table so no arm can be served by a
/// prefix of it. A suffix or prefix selection is the one shape that cannot distinguish a scan that
/// stops early from one that does not.
fn keys(n: i64) -> Vec<i64> {
    (0..DELTA as i64).map(|j| (j + 1) * n / (DELTA as i64 + 1)).collect()
}

/// ARM A — `DELTA` indexed point UPDATEs. Returns the counter window and the rows affected.
fn arm_a(n: i64) -> (Counts, Vec<usize>) {
    let mut db = Db::new();
    let mut s = Session::new();
    seed(&mut db, &mut s, n);
    let ks = keys(n);
    let before = read_counts();
    let affected: Vec<usize> =
        ks.iter().map(|k| db.affected(&format!("UPDATE t SET v = 777 WHERE id = {k};"), &mut s)).collect();
    let after = read_counts();
    // Read back OUTSIDE the counter window, and compare against the literal written above — never
    // against a value fetched from the system under test.
    for k in &ks {
        let got = db.rows(&format!("SELECT v FROM t WHERE id = {k};"), &mut s);
        assert_eq!(got, vec![vec![Value::Integer(777)]], "ARM A n={n} id={k}: the UPDATE did not land");
    }
    (since(before, after), affected)
}

/// ARM B — the CONTROL and the FIRE-CHECK. `label` carries no index, so this arm must stay a
/// sequential scan under both trees, and must stay proportional to `n`.
fn arm_b(n: i64) -> (Counts, Vec<usize>) {
    let mut db = Db::new();
    let mut s = Session::new();
    seed(&mut db, &mut s, n);
    let before = read_counts();
    let affected: Vec<usize> = (0..DELTA)
        .map(|_| db.affected("UPDATE t SET v = 888 WHERE label = 'zz';", &mut s))
        .collect();
    let after = read_counts();
    // Nothing is labelled 'zz', so nothing may have changed. This is the assertion that the
    // control arm really is doing the full scan and finding nothing, rather than not running.
    let hits = db.rows("SELECT id FROM t WHERE v = 888;", &mut s);
    assert!(hits.is_empty(), "ARM B n={n}: the control arm wrote {} rows; it must write none", hits.len());
    (since(before, after), affected)
}

/// ARM C — `DELTA` indexed point DELETEs, on distinct keys so each must find exactly one row.
fn arm_c(n: i64) -> (Counts, Vec<usize>) {
    let mut db = Db::new();
    let mut s = Session::new();
    seed(&mut db, &mut s, n);
    let ks = keys(n);
    let before = read_counts();
    let affected: Vec<usize> =
        ks.iter().map(|k| db.affected(&format!("DELETE FROM t WHERE id = {k};"), &mut s)).collect();
    let after = read_counts();
    for k in &ks {
        let got = db.rows(&format!("SELECT v FROM t WHERE id = {k};"), &mut s);
        assert!(got.is_empty(), "ARM C n={n} id={k}: the DELETE did not land, got {got:?}");
    }
    (since(before, after), affected)
}

/// ARM D — the SELECT control. Already indexed at `fe40276`, so it must not move.
fn arm_d(n: i64) -> (Counts, usize) {
    let mut db = Db::new();
    let mut s = Session::new();
    seed(&mut db, &mut s, n);
    let ks = keys(n);
    let before = read_counts();
    let mut found = 0;
    for k in &ks {
        found += db.rows(&format!("SELECT v FROM t WHERE id = {k};"), &mut s).len();
    }
    let after = read_counts();
    (since(before, after), found)
}

/// H1 — is `lower`'s refusal of a strictly-excluded lower bound on a SECONDARY index REACHABLE on
/// the SELECT path in this tree? Reported, not gated.
///
/// ⚠ **After D179 there is no such refusal**, so this now prints which access path the cost model
/// picks for `v > k` rather than whether the plan can be built. See the module header.
fn h1_probe(out: &mut String) {
    let mut db = Db::new();
    let mut s = Session::new();
    let n = 1000i64;
    db.ok("CREATE TABLE h (id INTEGER NOT NULL, v INTEGER);", &mut s);
    for i in 0..n {
        db.ok(&format!("INSERT INTO h VALUES ({i}, {});", i * 10), &mut s);
    }
    db.ok("CREATE INDEX ix ON h (v);", &mut s);
    db.ok("ANALYZE h;", &mut s);

    // A value near the top of `v`'s range, so the estimated row count is small and the index side
    // of the cost comparison is cheap. That is the corner where `build_index_scan` would choose a
    // plan `lower` cannot build.
    for cutoff in [9980i64, 9900, 9000, 5000] {
        let sql = format!("SELECT id FROM h WHERE v > {cutoff};");
        let plan = db.plan(&sql, &mut s);
        let top = plan.lines().next().unwrap_or("").trim().to_string();
        let res = db.exec(&sql, &mut s);
        let verdict = match &res {
            Ok(Outcome::Rows(r)) => format!("OK, {} rows", r.len()),
            Ok(o) => format!("OK, unexpected outcome {}", describe(o)),
            Err(e) => format!("ERROR: {e}"),
        };
        let _ = writeln!(out, "  v > {cutoff:<6} plan={top:<52} run={verdict}");
    }

    // The same predicate shape on the PRIMARY key, which `lower` handles: the contrast says the
    // probe is aimed at the secondary path specifically and not at range predicates in general.
    let sql = "SELECT v FROM h WHERE id > 995;";
    let plan = db.plan(sql, &mut s);
    let top = plan.lines().next().unwrap_or("").trim().to_string();
    let verdict = match db.exec(sql, &mut s) {
        Ok(Outcome::Rows(r)) => format!("OK, {} rows", r.len()),
        Ok(o) => format!("OK, unexpected outcome {}", describe(&o)),
        Err(e) => format!("ERROR: {e}"),
    };
    let _ = writeln!(out, "  id > 995    plan={top:<52} run={verdict}   (primary key, contrast)");
}

/// Collapse `REPS` readings of one cell into `(min, max)` per counter. Equal min and max is the
/// expected outcome for integer counters and is what makes these numbers quotable without a mean.
fn collapse(reps: &[Counts]) -> [(u64, u64); 3] {
    let f = |g: fn(&Counts) -> u64| {
        (reps.iter().map(g).min().unwrap(), reps.iter().map(g).max().unwrap())
    };
    [f(|c| c.0), f(|c| c.1), f(|c| c.2)]
}

fn cell(out: &mut String, label: &str, n: i64, reps: &[Counts], note: &str) {
    let [scans, tuples, idx] = collapse(reps);
    let fmt = |(lo, hi): (u64, u64)| if lo == hi { format!("{lo}") } else { format!("{lo}..{hi}") };
    let _ = writeln!(
        out,
        "| {label:<5} | {n:>5} | {:>10} | {:>7} | {:>7} | {note} |",
        fmt(tuples),
        fmt(scans),
        fmt(idx)
    );
}

fn main() {
    let mut out = String::new();
    let _ = writeln!(out, "D178 — can UPDATE/DELETE reach an index? Counters, not durations.");
    let _ = writeln!(out, "delta = {DELTA} statements per cell, {REPS} repeats per cell, sizes {SIZES:?}");
    let _ = writeln!(out, "counters: SEQ_SCAN_TUPLES / SEQ_SCANS / INDEX_SCANS (D176, cherry-picked 81173de)");
    let _ = writeln!(out, "min..max shown when they differ; a bare number means every repeat agreed.\n");

    let _ = writeln!(out, "| arm   |     n |     tuples |   scans |  index  | rows affected |");
    let _ = writeln!(out, "|-------|-------|------------|---------|---------|---------------|");

    for n in SIZES {
        let mut reps = Vec::new();
        let mut aff = Vec::new();
        for _ in 0..REPS {
            let (c, a) = arm_a(n);
            reps.push(c);
            aff.push(a);
        }
        for a in &aff {
            assert_eq!(a, &vec![1usize; DELTA], "ARM A n={n}: rows affected must be 1 per UPDATE (P7)");
        }
        cell(&mut out, "A", n, &reps, "4 x 1 (indexed UPDATE)");
    }
    for n in SIZES {
        let mut reps = Vec::new();
        for _ in 0..REPS {
            let (c, a) = arm_b(n);
            assert_eq!(a, vec![0usize; DELTA], "ARM B n={n}: the control must match no rows (P7)");
            reps.push(c);
        }
        cell(&mut out, "B-CTL", n, &reps, "4 x 0 (UNindexed UPDATE)");
    }
    for n in SIZES {
        let mut reps = Vec::new();
        for _ in 0..REPS {
            let (c, a) = arm_c(n);
            assert_eq!(a, vec![1usize; DELTA], "ARM C n={n}: rows affected must be 1 per DELETE (P7)");
            reps.push(c);
        }
        cell(&mut out, "C", n, &reps, "4 x 1 (indexed DELETE)");
    }
    for n in SIZES {
        let mut reps = Vec::new();
        for _ in 0..REPS {
            let (c, found) = arm_d(n);
            assert_eq!(found, DELTA, "ARM D n={n}: each SELECT must find its row (P7)");
            reps.push(c);
        }
        cell(&mut out, "D-CTL", n, &reps, "4 x 1 (indexed SELECT)");
    }

    let _ = writeln!(out, "\nH1 probe — access path for a secondary strict lower bound on the SELECT path.");
    let _ = writeln!(out, "  (This asked whether `lower`'s refusal was REACHABLE. D179 removed that refusal;");
    let _ = writeln!(out, "   these lines now report which plan is CHOSEN, not whether it can be built.)");
    h1_probe(&mut out);

    print!("{out}");
}
