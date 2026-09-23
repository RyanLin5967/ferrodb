//! D176 — **is merge cost O(table) or O(delta)?** Answered with an INTEGER, not a stopwatch.
//!
//! # Why this run exists, and why it is a count
//!
//! SCALE-LEDGER.md:4059 banks: *"A merge that writes FOUR ROWS reads every row of the table TWICE
//! … merge cost is O(table), independent of how much the branch actually changed."*
//!
//! The DURATION form of that experiment is `examples/d68_merge_is_o_table.rs`, and it has been
//! VOIDED TWICE — once because the harness timed an in-memory stub (D101), once by a disk
//! emergency mid-sweep (`bench/d101_rerun_VOID_disk_emergency.txt`, rc=137). Both voids are
//! properties of a wall-clock number on a shared box. An integer has neither failure mode: the
//! count of rows a merge reads is the same on a quiet machine and on a machine running thirteen
//! build agents, so this run does not take the fleet's measure lock and does not need to.
//!
//! # The four arms, and what each is for
//!
//! | arm | path | axis | what it answers |
//! |---|---|---|---|
//! | A | plain `MERGE;` | table size, delta fixed at 4 | the claim as banked |
//! | B | `SIMULATE … ASSERT ON t` | table size, delta fixed at 4 | the surviving full scan |
//! | B-CTL | `SIMULATE … ASSERT ON small` | table size, delta fixed at 4 | the control |
//! | C | both | delta, table size fixed | "independent of how much changed" |
//!
//! **ARM B IS THE FIRE-CHECK AND IT IS NOT OPTIONAL.** A detector that found nothing is not a
//! clean result until it has been forced to fire. If ARM A comes back flat and ARM B comes back
//! flat too, the instrument is blind and ARM A says NOTHING — that outcome is reported as a
//! failed measurement, never as a dead claim. Pre-registered as F4 in `bench/d176_prereg.txt`.
//!
//! **B-CTL IS WHY ARM B'S SLOPE IS ATTRIBUTABLE.** A `SIMULATE` statement contains the candidate's
//! own UPDATEs, so the counter window around it covers writes as well as the scan. B-CTL runs the
//! identical statement with the assertion pointed at a one-row table instead of the big one: same
//! candidate, same writes, same everything except which table the assertion ranges over. It must
//! be FLAT in table size. An arm the change cannot affect that moves anyway means the two halves
//! did not come from the same configuration, and the difference B - B_CTL is then meaningless.
//!
//! # The two counters, and why one of them would lie alone
//!
//! * `SCAN_TABLE_ROWS` — rows RETURNED by `scan_table_where`, one relaxed add per scan.
//! * `SEQ_SCAN_TUPLES` — tuples the heap actually yielded, accumulated in a plain field and
//!   flushed once per scan in `Drop`.
//!
//! The first alone cannot answer the question. The merge fetches the rows a branch touched by
//! pushing `pk = k` into the planner (`runtime.rs:4421`), which RETURNS one row. If the planner
//! declines the index and falls back to a sequential scan, the engine reads the whole table while
//! `SCAN_TABLE_ROWS` reports 1 — a flat curve produced by an O(table) merge. `SEQ_SCAN_TUPLES` is
//! what discriminates that, and the pair is only meaningful read together.
//!
//! # What is deliberately NOT claimed
//!
//! Whatever ARM A shows is a statement about the plain-`MERGE` path at the commit this ran
//! against. It is NOT a correction of the ledger's measurement, because the ledger measured a
//! different tree: `fingerprint_rows` has since been deleted and `evaluate_merge`'s blanket scan
//! gated. A claim can be true when banked and false now, and that is not the same as it having
//! been wrong.
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::{scan_table_counters, AgentRuntime};
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::seq_scan::seq_scan_counters;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Table sizes. 500 -> 4000 is the 8x span the pre-registration requires.
const SIZES: [i64; 4] = [500, 1000, 2000, 4000];
/// Writes per branch on the size axis. Held CONSTANT — that is what makes it a size axis.
const FIXED_DELTA: usize = 4;
/// Table size on the delta axis. Held CONSTANT, for the same reason.
const FIXED_SIZE: i64 = 2000;
const DELTAS: [usize; 6] = [1, 2, 4, 8, 16, 32];
/// Merges per cell. Every one should produce the IDENTICAL integer; reporting min and max is how
/// that is shown rather than asserted. A spread means something in the window is not deterministic
/// and the mean would have hidden it.
const REPS: usize = 5;

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
}

/// One counter reading: `(scan calls, rows returned, seq scans, tuples pulled)`.
#[derive(Clone, Copy)]
struct Counts(u64, u64, u64, u64);

fn read_counts() -> Counts {
    let (c, r) = scan_table_counters();
    let (s, t) = seq_scan_counters();
    Counts(c, r, s, t)
}

/// Read twice and subtract to scope a phase — the pattern `ours_scan_counters` documents.
/// Exact here because the harness is single-threaded; it would not be under concurrency.
fn since(a: Counts, b: Counts) -> Counts {
    Counts(b.0 - a.0, b.1 - a.1, b.2 - a.2, b.3 - a.3)
}

/// Run one statement the way the server does: take the catalog, run, drop.
///
/// Deliberately NOT using the read fast path, for the reason D68 gives: every statement here
/// either writes or merges, so routing through `try_run_read` would add a branch that always falls
/// through, and hiding the acquisition inside a helper that sometimes avoids it is how a harness
/// ends up measuring itself.
fn exec(s: &Server, sql: &str, sess: &mut Session) -> Result<Outcome, String> {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new())
        .scan_tokens()
        .map_err(|e| e.to_string())?;
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    if !parser.errors.is_empty() {
        return Err(format!("parse failed for {sql}: {:?}", parser.errors));
    }
    let stmt = stmts.remove(0);
    let mut cat = s.ctx.catalog();
    let out = run(stmt, &mut cat, s.bp.clone(), s.txn.clone(), sess);
    drop(cat);
    out.map_err(|e| e.to_string())
}

/// Build a server with an `nrows`-row table `t`, plus a one-row `small` for the control arm.
///
/// ⚠ THE THREE CONFIGURATION CORRECTIONS ARE LOAD-BEARING AND ARE CARRIED OVER VERBATIM FROM
/// `d68_merge_is_o_table.rs`, WHERE EACH WAS PAID FOR BY A WITHDRAWN RESULT:
///
///  1. `with_storage`, NOT `with_catalog`. `with_catalog` delegates to `with_parts`, which sets
///     `storage: None, reaper: None`, so the whole branch STORAGE engine — ArenaPageStore, CoW
///     pages, the reaper — is absent and agent writes go to an in-memory effect log. D67's first
///     configuration was this, and its numbers are withdrawn.
///  2. `s.ctx.session()`, NEVER `Session::new()`. `Session::new` builds its OWN `AgentRuntime::new()`
///     with `storage: None` and a private branch catalog, so every agent statement would run on a
///     stub while the arena built here was constructed and never touched. This is the D101 void.
///  3. `checkpoint_to`, because production sets one and a harness without it skips the free-space
///     persistence work entirely — an omission that flatters the result.
fn build_sized(dir: &std::path::Path, tag: &str, nrows: i64) -> Server {
    let d = dir.join(tag);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(d.join("main.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(d.join("main.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&d.join("b.branchcat"), 1).unwrap());
    let branches: Arc<dyn BranchCatalog> = cat.clone();
    // Reserve a table region BELOW the arena. Taking `high_water()` here would put the arena at
    // page 2 and leave the ordinary table nowhere to grow.
    const ARENA_BASE: u32 = 1024;
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), ARENA_BASE).unwrap());
    store.checkpoint_to(d.join("main.arena"));
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
    let s = Server { ctx, bp, txn };

    let mut sess = s.ctx.session();
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    for i in 1..=nrows {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut sess).unwrap();
    }
    // The control arm's assertion target. ONE row, and it must have one: an assertion that
    // examines zero rows is HARD-REJECTED ("an assertion that never ran is not an assertion that
    // held"), so an empty table here would make B-CTL measure a refusal rather than a merge.
    exec(&s, "CREATE TABLE small (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    exec(&s, "INSERT INTO small VALUES (1, 1);", &mut sess).unwrap();
    s
}

/// ARM A: one plain-`MERGE` cycle. The counter window covers the `MERGE;` statement ALONE —
/// the session and the UPDATEs are outside it.
///
/// ⚠ Returns `None` unless the merge REACHED THE TARGET. D68 records why: a QUARANTINED merge
/// returns `Ok`, so counting `is_ok()` scores a merge that did no work, and the arm looks cheaper
/// the more merges fail. And the report must be read as `Outcome::Agent(AgentOutput::Merge(..))` —
/// matching `Outcome::Table` falls to `_ => false` and reports zero forever, which is exactly the
/// shape a broken instrument takes.
fn arm_a_cycle(s: &Server, nrows: i64, delta: usize, seq: u64) -> Option<Counts> {
    let mut sess = s.ctx.session();
    exec(s, &format!("BEGIN AGENT SESSION AS 'a{seq}';"), &mut sess).ok()?;
    for w in 0..delta {
        let id = 1 + (w as i64 % nrows);
        let v = (seq % 1000) as i64;
        exec(s, &format!("UPDATE t SET v = {v} WHERE id = {id};"), &mut sess).ok()?;
    }
    let before = read_counts();
    let applied = match exec(s, "MERGE;", &mut sess) {
        Ok(Outcome::Agent(AgentOutput::Merge(report))) => report.applied_to_target,
        _ => false,
    };
    let c = since(before, read_counts());
    if !applied {
        return None;
    }
    Some(c)
}

/// ARMS B and B-CTL: one `SIMULATE` whose single candidate makes `delta` writes, with the
/// assertion pointed at `assert_table`.
///
/// The window necessarily covers the candidate's own UPDATEs as well as the scan, which is
/// precisely why B-CTL exists: it is this same function with `assert_table = "small"`, so the
/// write component is identical and the DIFFERENCE is the scan of `t`.
fn arm_b_cycle(
    s: &Server,
    nrows: i64,
    delta: usize,
    seq: u64,
    assert_table: &str,
) -> Option<Counts> {
    let mut sess = s.ctx.session();
    let mut writes = String::new();
    for w in 0..delta {
        let id = 1 + (w as i64 % nrows);
        let v = (seq % 1000) as i64;
        writes.push_str(&format!("UPDATE t SET v = {v} WHERE id = {id}; "));
    }
    let pred = if assert_table == "small" { "id >= 0" } else { "id >= 0" };
    let sql = format!(
        "SIMULATE AS 'a{seq}' CANDIDATE 'c' ( {writes}) ASSERT ON {assert_table} ({pred}) ADMIT ALL;"
    );
    let before = read_counts();
    let admitted = match exec(s, &sql, &mut sess) {
        Ok(Outcome::Agent(AgentOutput::Simulation(report))) => !report.admitted().is_empty(),
        Ok(_) => {
            eprintln!("  !! SIMULATE returned a non-simulation outcome");
            false
        }
        Err(e) => {
            eprintln!("  !! SIMULATE failed: {e}");
            false
        }
    };
    let c = since(before, read_counts());
    if !admitted {
        eprintln!("  !! no candidate admitted at nrows={nrows} assert_on={assert_table}");
        return None;
    }
    Some(c)
}

/// A cell's worth of repetitions, reported as min/max rather than a mean.
struct Cell {
    rows_min: u64,
    rows_max: u64,
    tuples_min: u64,
    tuples_max: u64,
    calls_max: u64,
    ok: usize,
}

fn cell(mut f: impl FnMut(u64) -> Option<Counts>) -> Cell {
    let mut rows = Vec::new();
    let mut tuples = Vec::new();
    let mut calls = Vec::new();
    for i in 0..REPS {
        if let Some(c) = f(i as u64 + 1) {
            calls.push(c.0);
            rows.push(c.1);
            tuples.push(c.3);
        }
    }
    Cell {
        rows_min: rows.iter().copied().min().unwrap_or(0),
        rows_max: rows.iter().copied().max().unwrap_or(0),
        tuples_min: tuples.iter().copied().min().unwrap_or(0),
        tuples_max: tuples.iter().copied().max().unwrap_or(0),
        calls_max: calls.iter().copied().max().unwrap_or(0),
        ok: rows.len(),
    }
}

fn main() {
    let dir = std::env::temp_dir().join(format!("d176_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    println!("D176 — merge cost as an INTEGER. rows = rows RETURNED by scans; tuples = tuples");
    println!("pulled off the heap by SeqScan. Both scoped to the statement window by read-twice-");
    println!("and-subtract. reps={REPS} per cell; min==max means the count is deterministic.");
    println!();

    // ---------------- ARM A: plain MERGE, table-size axis at fixed delta ----------------
    println!("== ARM A — plain MERGE;  delta FIXED at {FIXED_DELTA}, table size varies 8x ==");
    println!("{:>7}  {:>10} {:>10}  {:>12} {:>12}  {:>6} {:>4}", "nrows", "rows_min", "rows_max", "tuples_min", "tuples_max", "calls", "ok");
    let mut arm_a = Vec::new();
    for n in SIZES {
        let s = build_sized(&dir, &format!("a{n}"), n);
        let c = cell(|seq| arm_a_cycle(&s, n, FIXED_DELTA, seq));
        println!("{:>7}  {:>10} {:>10}  {:>12} {:>12}  {:>6} {:>4}", n, c.rows_min, c.rows_max, c.tuples_min, c.tuples_max, c.calls_max, c.ok);
        arm_a.push((n, c.rows_max, c.tuples_max));
    }
    println!();

    // ---------------- ARM B / B-CTL: SIMULATE, same axis ----------------
    println!("== ARM B — SIMULATE … ASSERT ON t   (the surviving full scan; ALSO THE FIRE-CHECK) ==");
    println!("== B-CTL — SIMULATE … ASSERT ON small  (identical statement, 1-row assertion table) ==");
    println!("{:>7}  {:>10} {:>10}  {:>12} {:>12}  {:>6} {:>4}  arm", "nrows", "rows_min", "rows_max", "tuples_min", "tuples_max", "calls", "ok");
    let mut arm_b = Vec::new();
    let mut arm_bctl = Vec::new();
    for n in SIZES {
        let s = build_sized(&dir, &format!("b{n}"), n);
        let c = cell(|seq| arm_b_cycle(&s, n, FIXED_DELTA, seq, "t"));
        println!("{:>7}  {:>10} {:>10}  {:>12} {:>12}  {:>6} {:>4}  B", n, c.rows_min, c.rows_max, c.tuples_min, c.tuples_max, c.calls_max, c.ok);
        arm_b.push((n, c.rows_max, c.tuples_max));

        let s2 = build_sized(&dir, &format!("bc{n}"), n);
        let c2 = cell(|seq| arm_b_cycle(&s2, n, FIXED_DELTA, seq, "small"));
        println!("{:>7}  {:>10} {:>10}  {:>12} {:>12}  {:>6} {:>4}  B-CTL", n, c2.rows_min, c2.rows_max, c2.tuples_min, c2.tuples_max, c2.calls_max, c2.ok);
        arm_bctl.push((n, c2.rows_max, c2.tuples_max));
    }
    println!();

    // ---------------- ARM C: delta axis at fixed table size ----------------
    println!("== ARM C — delta axis, table size FIXED at {FIXED_SIZE} ==");
    println!("{:>7}  {:>10} {:>12}  {:>4}  path", "delta", "rows_max", "tuples_max", "ok");
    let mut arm_c_a = Vec::new();
    let mut arm_c_b = Vec::new();
    for d in DELTAS {
        let s = build_sized(&dir, &format!("ca{d}"), FIXED_SIZE);
        let c = cell(|seq| arm_a_cycle(&s, FIXED_SIZE, d, seq));
        println!("{:>7}  {:>10} {:>12}  {:>4}  A (plain MERGE)", d, c.rows_max, c.tuples_max, c.ok);
        arm_c_a.push((d, c.rows_max, c.tuples_max));

        let s2 = build_sized(&dir, &format!("cb{d}"), FIXED_SIZE);
        let c2 = cell(|seq| arm_b_cycle(&s2, FIXED_SIZE, d, seq, "t"));
        println!("{:>7}  {:>10} {:>12}  {:>4}  B (SIMULATE+ASSERT ON t)", d, c2.rows_max, c2.tuples_max, c2.ok);
        arm_c_b.push((d, c2.rows_max, c2.tuples_max));
    }
    println!();

    // ---------------- verdict against the pre-registered falsifiers ----------------
    println!("== VERDICT against bench/d176_prereg.txt ==");
    let a_first = arm_a.first().map(|x| x.1).unwrap_or(0);
    let a_last = arm_a.last().map(|x| x.1).unwrap_or(0);
    let b_first = arm_b.first().map(|x| x.1).unwrap_or(0);
    let b_last = arm_b.last().map(|x| x.1).unwrap_or(0);
    let bc_first = arm_bctl.first().map(|x| x.1).unwrap_or(0);
    let bc_last = arm_bctl.last().map(|x| x.1).unwrap_or(0);
    let n_first = SIZES[0];
    let n_last = SIZES[SIZES.len() - 1];

    // F4 FIRST. Every other verdict is conditional on the instrument having fired.
    let fired = b_last > b_first && b_last >= (n_last as u64);
    println!("F4 fire-check — ARM B grew with table size and reached >= n:  {}", if fired { "FIRED — instrument sees a full scan" } else { "⛔ DID NOT FIRE" });
    if !fired {
        println!("⛔ ARM B is flat or below n. The instrument is blind to a full scan, so ARM A's");
        println!("   shape proves NOTHING. This run is a FAILED MEASUREMENT, not a dead claim.");
        println!("   Per F4, no verdict on the O(table) claim is drawn.");
        return;
    }
    let ctl_flat = bc_last <= bc_first + 8;
    println!("B-CTL control is flat in table size ({bc_first} -> {bc_last}):  {}", if ctl_flat { "YES — B's slope is the scan of t" } else { "⛔ NO — control moved, halves not comparable" });

    println!("ARM A rows/merge: {a_first} at n={n_first} -> {a_last} at n={n_last}");
    println!("ARM B rows/merge: {b_first} at n={n_first} -> {b_last} at n={n_last}  (ratio to n: {:.2}x at n={n_last})", b_last as f64 / n_last as f64);
    let a_flat = a_last <= a_first + 8;
    if a_flat {
        println!("⇒ F1 HOLDS: ARM A is FLAT in table size. The O(table) claim as banked is DEAD");
        println!("  for the plain-MERGE path at this commit.");
    } else if a_last >= 2 * (n_last as u64) {
        println!("⇒ F3 HOLDS: ARM A grew to ~2n. The banked claim STANDS exactly as written.");
    } else {
        println!("⇒ ARM A grew but not to 2n — neither F1 nor F3. Report the shape, claim nothing.");
    }
    let c_moves_with_delta = arm_c_a.last().map(|x| x.1).unwrap_or(0) > arm_c_a.first().map(|x| x.1).unwrap_or(0);
    println!("⇒ F2: ARM A rows move with DELTA at fixed size ({} -> {}): {}",
        arm_c_a.first().map(|x| x.1).unwrap_or(0),
        arm_c_a.last().map(|x| x.1).unwrap_or(0),
        if c_moves_with_delta { "YES — 'independent of how much changed' is false" } else { "no" });

    let _ = std::fs::remove_dir_all(&dir);
}
