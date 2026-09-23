//! D181 — **does the ORDER the user typed a conjunction decide which index runs?** Answered with
//! integers, not a stopwatch.
//!
//! # The mechanism under test
//!
//! `optimizer::build_index_scan` picks the conjunct it will turn into an `IndexScan` with
//!
//! ```text
//!     let chosen = conjuncts.iter().position(|c| ... has_index(entry, col) ...)?;
//! ```
//!
//! `position` returns the FIRST match. `split_and` preserves source order, so with two usable
//! indexed conjuncts exactly ONE candidate plan is ever built — the leftmost one — and it is then
//! costed against a sequential scan and nothing else. The other index is never costed, never
//! compared, and cannot win. Index selection is therefore decided by the order the predicate was
//! typed rather than by the cost model, **even when `ANALYZE` has measured statistics that say the
//! other index is hundreds of times better**.
//!
//! # The arms
//!
//! Two indexed secondary columns of deliberately extreme selectivity contrast, `N` rows:
//!
//! | column | data | equality matches |
//! |---|---|---|
//! | `sel`   | `sel = id`      | exactly 1 row  |
//! | `broad` | `broad = id % 2`| `N/2` rows     |
//!
//! | arm | predicate | what it is for |
//! |---|---|---|
//! | SEL_FIRST   | `sel = k AND broad = k%2` | selective index typed first |
//! | BROAD_FIRST | `broad = k%2 AND sel = k` | the SAME predicate, typed the other way |
//! | CONTROL_SEL | `sel = k` | one conjunct — conjunct order cannot apply |
//! | FIRE        | `pad = 'zz'` | **the fire-check**: no index exists, must read all `N` |
//!
//! SEL_FIRST and BROAD_FIRST are logically identical predicates over an identical fixture and
//! return an identical single row. Every difference between their counters is the typing order and
//! nothing else.
//!
//! **FIRE IS NOT OPTIONAL.** A pair of small numbers is also what a dead counter reports. FIRE runs
//! in the same process, the same binary and the same run, over a column carrying no index, so
//! `has_index` is false and `build_index_scan` returns `None`. It must come back at `N` sequential
//! tuples. If it does not, the instrument is blind and this run reports NO VERDICT.
//!
//! Every arm asserts its rows-returned. An arm that stopped returning the right answer is also an
//! arm that got cheap, and that must not read as a win.
//!
//! # Why both with and without `ANALYZE`
//!
//! They separate two different failures that look the same in one number:
//!
//! - **without stats** both columns get `DEFAULT_DISTINCT`, so the two candidate plans cost the
//!   SAME. Any gap in what they actually examine is pure typing order — the cost model had no
//!   opinion to ignore.
//! - **with stats** the cost model knows `sel` is `N`-distinct and `broad` is 2-distinct, so it
//!   could pick correctly. A gap here is the cost model being asked the wrong question: only one
//!   candidate was ever built for it to cost.
//!
//! # Why a size axis
//!
//! One pair of numbers cannot separate a constant overhead from a complexity class. `broad = k%2`
//! matches `N/2` rows, so if the defect is real the gap must GROW with `N`; a fixed gap would mean
//! something else is going on. Three sizes, each doubling.
//!
//! # The instruments
//!
//! `SEQ_SCANS`/`SEQ_SCAN_TUPLES` (`execution::seq_scan`, D176) and
//! `INDEX_SCANS`/`INDEX_SCAN_ENTRIES` (`execution::index_scan`; the entries half is D181's, added
//! with this run). Rows examined is their sum: every tuple pulled off the heap plus every index
//! entry walked. Both are plain `u64` fields flushed once per scan in `Drop`, never a per-row
//! atomic — the rule at `agent_sql/runtime.rs:115`.
//!
//! A duration would not settle this. Two of this project's timing runs have been voided by a
//! shared machine (`bench/d101_rerun_VOID_disk_emergency.txt`, rc=137). An integer count is the
//! same on a quiet box and a loaded one.

use std::fmt::Write as _;
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

/// Each doubles, so a gap proportional to `N` is distinguishable from a constant one.
const SIZES: [i64; 3] = [400, 800, 1600];

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
            .open(dir.path().join("d181.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d181.wal")).unwrap());
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

/// One counter reading: `(seq scans, seq tuples, index scans, index entries)`.
#[derive(Clone, Copy)]
struct Counts {
    seq_scans: u64,
    seq_tuples: u64,
    index_scans: u64,
    index_entries: u64,
}

impl Counts {
    /// Rows examined, the whole of it: heap tuples pulled plus index entries walked. The two halves
    /// are reported separately as well, because a plan that moved work from one to the other is a
    /// different story from one that did less work.
    fn examined(&self) -> u64 {
        self.seq_tuples + self.index_entries
    }
}

fn read_counts() -> Counts {
    let (seq_scans, seq_tuples) = seq_scan_counters();
    let (index_scans, index_entries) = index_scan_counters();
    Counts { seq_scans, seq_tuples, index_scans, index_entries }
}

/// Read twice and subtract to scope a phase. Exact here because the harness is single-threaded;
/// it would not be under concurrency, and this run claims nothing about the concurrent case.
fn since(a: Counts, b: Counts) -> Counts {
    Counts {
        seq_scans: b.seq_scans - a.seq_scans,
        seq_tuples: b.seq_tuples - a.seq_tuples,
        index_scans: b.index_scans - a.index_scans,
        index_entries: b.index_entries - a.index_entries,
    }
}

/// `CREATE TABLE t (id, sel, broad, pad)` with `n` rows. `sel = id` (n distinct, 1 row per value),
/// `broad = id % 2` (2 distinct, n/2 rows per value), both indexed. `pad` carries no index and is
/// what the fire-check reads.
fn seed(db: &mut Db, s: &mut Session, n: i64, analyze: bool) {
    db.ok("CREATE TABLE t (id INTEGER NOT NULL, sel INTEGER, broad INTEGER, pad VARCHAR(16));", s);
    for i in 0..n {
        db.ok(&format!("INSERT INTO t VALUES ({i}, {i}, {}, 'row');", i % 2), s);
    }
    db.ok("CREATE INDEX ix_sel ON t (sel);", s);
    db.ok("CREATE INDEX ix_broad ON t (broad);", s);
    if analyze {
        db.ok("ANALYZE t;", s);
    }
}

/// A key in the MIDDLE of the table. A prefix or suffix key is the one shape that cannot tell a
/// scan that stops early from one that does not.
fn key(n: i64) -> i64 {
    n / 2 + 1
}

/// Run one statement on a FRESH database and report `(counter window, rows returned, plan)`.
///
/// Fresh per arm so no arm's reading can be changed by another arm's fixture, and the counter
/// window opens AFTER every bit of setup — the inserts, both `CREATE INDEX`es and the optional
/// `ANALYZE` must not land inside it. It closes after the statement's plan has been dropped:
/// every scan flushes its count in `Drop`, so a reading taken while a plan were still alive would
/// miss it. `rows` returns an owned `Vec`, so the plan is gone by the time it returns.
fn arm(n: i64, analyze: bool, sql: &str) -> (Counts, usize, String) {
    let mut db = Db::new();
    let mut s = Session::new();
    seed(&mut db, &mut s, n, analyze);

    let plan = db.plan(sql, &mut s);

    let before = read_counts();
    let rows = db.rows(sql, &mut s);
    let after = read_counts();
    (since(before, after), rows.len(), plan)
}

fn main() {
    let mut out = String::new();
    writeln!(out, "D181 — conjunct order vs index selection").unwrap();
    writeln!(out, "rows examined = seq tuples pulled + index entries walked").unwrap();
    writeln!(out).unwrap();

    let mut any_verdict = false;

    for analyze in [false, true] {
        writeln!(out, "================ ANALYZE: {} ================", if analyze { "yes" } else { "no" }).unwrap();
        writeln!(
            out,
            "{:<14} {:>6} {:>9} {:>10} {:>10} {:>12} {:>9} {:>6}",
            "arm", "n", "examined", "seq_tuples", "seq_scans", "idx_entries", "idx_scans", "rows"
        )
        .unwrap();

        for n in SIZES {
            let k = key(n);
            let b = k % 2;

            let arms: [(&str, String, usize); 4] = [
                ("FIRE", format!("SELECT id FROM t WHERE pad = 'zz';"), 0),
                ("CONTROL_SEL", format!("SELECT id FROM t WHERE sel = {k};"), 1),
                ("SEL_FIRST", format!("SELECT id FROM t WHERE sel = {k} AND broad = {b};"), 1),
                ("BROAD_FIRST", format!("SELECT id FROM t WHERE broad = {b} AND sel = {k};"), 1),
            ];

            let mut examined = Vec::new();
            let mut plans = Vec::new();
            for (name, sql, expect_rows) in &arms {
                let (c, rows, plan) = arm(n, analyze, sql);
                assert_eq!(
                    rows, *expect_rows,
                    "{name} n={n} analyze={analyze} returned {rows} rows, expected {expect_rows} \
                     — an arm that stopped returning the right answer is not a faster arm"
                );
                writeln!(
                    out,
                    "{:<14} {:>6} {:>9} {:>10} {:>10} {:>12} {:>9} {:>6}",
                    name, n, c.examined(), c.seq_tuples, c.seq_scans, c.index_entries, c.index_scans, rows
                )
                .unwrap();
                examined.push((name.to_string(), c));
                plans.push((name.to_string(), sql.clone(), plan));
            }

            // ---- the fire-check, evaluated not just printed ------------------------------------
            let fire = examined.iter().find(|(nm, _)| nm == "FIRE").unwrap().1;
            if fire.seq_tuples != n as u64 {
                writeln!(
                    out,
                    "  !! NO VERDICT at n={n}: the fire-check read {} sequential tuples, not {n}. \
                     The counter cannot report a full scan, so it cannot report the absence of one \
                     either, and every row above is vacuous.",
                    fire.seq_tuples
                )
                .unwrap();
                continue;
            }
            any_verdict = true;

            let sel_first = examined.iter().find(|(nm, _)| nm == "SEL_FIRST").unwrap().1;
            let broad_first = examined.iter().find(|(nm, _)| nm == "BROAD_FIRST").unwrap().1;
            writeln!(
                out,
                "  -> n={n}: SEL_FIRST examined {}, BROAD_FIRST examined {} (delta {}, ratio {:.1}x)",
                sel_first.examined(),
                broad_first.examined(),
                broad_first.examined() as i64 - sel_first.examined() as i64,
                broad_first.examined() as f64 / sel_first.examined().max(1) as f64,
            )
            .unwrap();

            if n == SIZES[SIZES.len() - 1] {
                writeln!(out, "\n  plans at n={n}:").unwrap();
                for (name, sql, plan) in &plans {
                    writeln!(out, "    {name}: {sql}").unwrap();
                    for line in plan.lines() {
                        writeln!(out, "      {line}").unwrap();
                    }
                }
            }
            writeln!(out).unwrap();
        }
    }

    if !any_verdict {
        writeln!(out, "NO VERDICT: the fire-check never fired at any size.").unwrap();
        print!("{out}");
        std::process::exit(1);
    }
    print!("{out}");
}
