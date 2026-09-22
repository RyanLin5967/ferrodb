//! D114 — **is the `ours`-side cell scan linear in the ops a branch recorded?**
//!
//! # The row, and why it is only a reading
//!
//! `runtime.rs` computes the two sides of one cell's three-way comparison on consecutive lines.
//! `theirs` goes through `concurrent_op`, which **D86 indexed** down to a `partition_point` over a
//! per-cell position list. `ours`, one line above, filtered **every op the branch had recorded**,
//! once per changed cell. Two sides of the same comparison; only one was fixed.
//!
//! That is read off the source. It has never been measured, and the precedent on this exact code
//! is ugly: the first mechanism proposed for a slope here was wrong **twice** (D68, then
//! D69-REOPEN). "It looks like the cause" is not evidence, so this harness exists to try to
//! falsify it before anything is changed.
//!
//! # PRE-REGISTERED falsifier (SCALE-DESIGN.md D114, recorded before any fix)
//!
//! > D86's own harness with the axis set to **OPS PER BRANCH** rather than merges — nobody has
//! > varied that one. Hold changed cells FIXED, grow the branch's recorded op count.
//! > * **FLAT** in ops per branch ⇒ the reading is WRONG and the row closes as a misreading.
//! > * **LINEAR** at fixed delta ⇒ the row is real, and the fix is D86's index.
//!
//! # ⚠ ONE AXIS IS NOT ENOUGH, AND THAT IS THE LESSON OF D86's OWN ATTEMPT 1
//!
//! The pre-registered axis can come back linear for **two different reasons**, and an index only
//! removes one of them:
//!
//! | mechanism | what grows | does an index fix it? |
//! |---|---|---|
//! | the SCAN walks ops belonging to other cells | `examined` | ✅ yes — they are never looked at |
//! | `compose_ops` FOLDS this cell's own history | `matched` | ⛔ no — the fold must still see them |
//!
//! D86 hit exactly this: its attempt 1 indexed by `(tbl, row, col)` and the drift only fell
//! 1.60x -> 1.25x, because *"an agent workload writes the SAME cells over and over"* so a cell's
//! own history is the same order as the whole log. **Indexing divides by the cell count; it does
//! not change a complexity class.** So this harness runs two arms that differ only in WHERE the
//! ops sit, and reports the `examined` / `matched` counters that tell them apart:
//!
//! * **`REPEAT`** — the literal pre-registered arm. `W` updates round-robin over the SAME 4 cells
//!   that form the delta. Every recorded op is both examined AND matched: `examined = 4W`,
//!   `matched = W`. Both mechanisms are live, so this arm cannot name which one it measured.
//! * **`CHURN`** — 4 changed cells as before, but the bulk of the ops land on a DIFFERENT column
//!   of the same 4 rows, and that column is written back to its base value at the end, so it
//!   contributes ops **without** contributing a changed cell. `examined = 4W`, `matched = 4`.
//!   Only the scan is live here, so a slope in this arm is the scan and nothing else.
//!
//! Reading the pair:
//!
//! | REPEAT | CHURN | what it means |
//! |---|---|---|
//! | flat | flat | the reading is WRONG; row closes (the pre-registered falsifier fires) |
//! | linear | flat | the slope is the FOLD, not the scan. An index would not have fixed it |
//! | linear | linear | the SCAN is real and an index is the right fix |
//! | flat | linear | impossible as written — `CHURN` ⊂ `REPEAT`'s cost. Harness is wrong |
//!
//! # The instrument
//!
//! `ours_scan_counters()` returns `(examined, matched)` — integers taken from inside the iterator
//! that actually walks the ops. **An integer does not move when the build fleet is loaded**, which
//! is the whole reason it is here; every millisecond in this file is an UPPER BOUND on a shared
//! box and is labelled as one. The counters are taken per changed cell (4 adds per merge), so the
//! instrument is ~4 relaxed adds against a scan of thousands and cannot create the slope it reads.
//!
//! # What is deliberately NOT claimed
//!
//! The axis here is the **DELTA side**, not the table. This says nothing about D110's retirement:
//! the merge stays O(delta · log N) in table size and `MERGE;` still reads zero branch-engine
//! pages. **Nothing here revives `merge3`.**
use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::Instant;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::{ours_scan_counters, AgentRuntime};
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Rows in the table. NOT the axis — held small and fixed on purpose, because the table-size
/// question is D68/D69's and it is closed.
const ROWS: i64 = 2000;
/// Changed cells per merge on the OPS axis. THIS IS WHAT THE PRE-REGISTRATION HOLDS FIXED — at 4,
/// the value D86's harness used. `D114_DELTA` overrides it so the ops axis can be re-walked at a
/// LARGE fixed delta, which is the decisive control: the scan costs `delta x ops`, so if it is
/// ever the merge's cost it is there, at the biggest delta and the biggest op count together.
static DELTA_V: std::sync::LazyLock<i64> = std::sync::LazyLock::new(|| {
    std::env::var("D114_DELTA").ok().and_then(|v| v.parse().ok()).unwrap_or(4)
});
#[allow(non_snake_case)]
fn DELTA() -> i64 { *DELTA_V }

/// The row ids carrying the delta, for a delta of `d`. Spread so no two land in one page slot by
/// accident; which rows they are does not matter, only that the set is the same at every point.
fn delta_ids(d: i64) -> Vec<i64> {
    (0..d).map(|k| 11 + k * 7).collect()
}

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
}

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

/// ⚠ `with_storage`, NOT `with_catalog` — copied from `d68_merge_is_o_table.rs` along with the
/// reason it is spelled this way. `with_catalog` sets `storage: None`, so the whole branch storage
/// engine is absent and agent writes go to an in-memory effect log instead of arena pages. D67's
/// first configuration made that mistake and its numbers were withdrawn.
fn build(dir: &std::path::Path, tag: &str) -> Server {
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
    // Reserve a table region BELOW the arena, same fixed floor and same reason as D68: taking
    // high_water() here puts the arena at page 2 and leaves the ordinary table nowhere to grow.
    const ARENA_BASE: u32 = 1024;
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), ARENA_BASE).unwrap());
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

    // ⛔ D101 — `s.ctx.session()`, NEVER `Session::new()`. `Session::new` builds its OWN
    // `AgentRuntime::new()` (`storage: None`, private in-memory branch catalog, private effect
    // log), so every agent statement below would run on a STUB and the arena-backed runtime
    // this harness constructs would be built and never touched. `agent_sql::designated` now
    // refuses such a statement rather than measuring it.
    let mut sess = s.ctx.session();
    // THREE columns, not two. `c` is the churn column: the CHURN arm needs somewhere to put ops
    // that is not the delta, and doing it on a separate ROW would grow the outer loop instead and
    // confound the thing being measured with the row count.
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, c INTEGER);", &mut sess).unwrap();
    for i in 1..=ROWS {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, {}, {});", i * 7, i * 13), &mut sess).unwrap();
    }
    s
}

#[derive(Clone, Copy, PartialEq)]
enum Arm {
    /// Every op lands on a cell that is IN the delta. Scan and fold both grow.
    Repeat,
    /// Every op but the last four lands on a cell that is NOT in the delta. Only the scan grows.
    Churn,
}

struct Cycle {
    writes_ms: f64,
    merge_ms: f64,
    examined: u64,
    matched: u64,
    ops_recorded: u64,
}

/// One agent's whole life at a chosen op count: fork, write `target_ops` ops, merge.
///
/// ⚠ The workspace — and therefore its `TxnFrame`, and therefore `ops` — is created fresh by
/// `BEGIN AGENT SESSION` and destroyed by the `seal` that `MERGE` performs. So "ops per branch"
/// means **ops this session recorded before it merged**, and it does NOT accumulate across merges
/// the way D86's `State::applied` does. That is a real difference from D86 and it is the reason
/// this axis had to be driven deliberately rather than falling out of a long run.
fn one_cycle(s: &Server, seq: u64, target_ops: i64, delta: i64, arm: Arm) -> Option<Cycle> {
    let ids = delta_ids(delta);
    // ⛔ D101 — `s.ctx.session()`, NEVER `Session::new()`. `Session::new` builds its OWN
    // `AgentRuntime::new()` (`storage: None`, private in-memory branch catalog, private effect
    // log), so every agent statement below would run on a STUB and the arena-backed runtime
    // this harness constructs would be built and never touched. `agent_sql::designated` now
    // refuses such a statement rather than measuring it.
    let mut sess = s.ctx.session();
    // ⛔ ONE agent name for every cycle, not `a{seq}`. The provenance slot is declared per branch
    // and REFUSES to be redeclared under a different agent name: `a1` on the slot `a0` declared
    // errors with "provenance slot prov1 is already declared as agent=a0". A per-cycle name made
    // every merge after the first fail, and because the harness only counted merges that applied,
    // it printed n=1 rather than an error — a broken instrument wearing a result's clothes. D68's
    // harness reuses `a{tid}` for exactly this reason.
    if let Err(e) = exec(s, "BEGIN AGENT SESSION AS 'a0';", &mut sess) {
        if std::env::var("D114_DEBUG").is_ok() { eprintln!("  [seq {seq}] BEGIN failed: {e}"); }
        return None;
    }

    let mut ops_recorded = 0u64;
    let t = Instant::now();
    match arm {
        Arm::Repeat => {
            // `target_ops` updates round-robin over the delta cells. The LAST write to each is
            // what makes it a changed cell, so the delta is `delta` whatever `target_ops` is.
            let rounds = (target_ops / delta).max(1);
            for r in 0..rounds {
                for (k, id) in ids.iter().enumerate() {
                    // ⚠ MUST depend on `seq`. Without it cycle 2 writes the values cycle 1 already
                    // published, the changed-column loop skips every cell, the delta is ZERO and
                    // the merge applies nothing — a second way to get n=1 that looks like the first.
                    let v = 1_000_000 + seq as i64 * 4096 + r * 16 + k as i64;
                    if exec(s, &format!("UPDATE t SET v = {v} WHERE id = {id};"), &mut sess).is_err() {
                        return None;
                    }
                    ops_recorded += 1;
                }
            }
        }
        Arm::Churn => {
            // Bulk of the ops go to column `c` of the SAME four rows, then `c` is written back to
            // its base value (`id * 13`, set at INSERT). A cell whose final image equals its base
            // is skipped by the changed-column loop, so these ops are EXAMINED by the scan and
            // never MATCHED by it — which is precisely the cost an index removes and the fold
            // does not pay.
            let rounds = ((target_ops - delta) / delta).max(0);
            for r in 0..rounds {
                for id in ids.iter() {
                    let junk = 9_000_000 + r;
                    if exec(s, &format!("UPDATE t SET c = {junk} WHERE id = {id};"), &mut sess).is_err() {
                        return None;
                    }
                    ops_recorded += 1;
                }
            }
            for id in ids.iter() {
                let base_c = id * 13;
                if exec(s, &format!("UPDATE t SET c = {base_c} WHERE id = {id};"), &mut sess).is_err() {
                    return None;
                }
                ops_recorded += 1;
            }
            // The delta itself: four cells, one op each.
            for (k, id) in ids.iter().enumerate() {
                let v = 2_000_000 + seq as i64 * 16 + k as i64;
                if exec(s, &format!("UPDATE t SET v = {v} WHERE id = {id};"), &mut sess).is_err() {
                    return None;
                }
                ops_recorded += 1;
            }
        }
    }
    let writes_ms = t.elapsed().as_secs_f64() * 1000.0;

    let (e0, m0) = ours_scan_counters();
    let t = Instant::now();
    // ⚠ READ THE REPORT, NOT A RENDERED ROW, and count `applied_to_target` rather than `is_ok()`.
    // Both mistakes are recorded in D67's harness: a MERGE returns `Outcome::Agent`, so matching
    // `Outcome::Table` counts zero forever; and a QUARANTINED merge returns Ok without doing the
    // work being timed.
    let applied = match exec(s, "MERGE;", &mut sess) {
        Ok(Outcome::Agent(AgentOutput::Merge(report))) => {
            if !report.applied_to_target && std::env::var("D114_DEBUG").is_ok() {
                eprintln!("  [seq {seq}] merge did NOT apply: outcome={:?} rows={}",
                          report.outcome, report.rows.len());
            }
            report.applied_to_target
        }
        Ok(_) => { if std::env::var("D114_DEBUG").is_ok() { eprintln!("  [seq {seq}] MERGE returned a NON-merge outcome"); } false }
        Err(e) => { if std::env::var("D114_DEBUG").is_ok() { eprintln!("  [seq {seq}] MERGE errored: {e}"); } false }
    };
    let merge_ms = t.elapsed().as_secs_f64() * 1000.0;
    let (e1, m1) = ours_scan_counters();
    if !applied {
        return None;
    }
    Some(Cycle {
        writes_ms,
        merge_ms,
        examined: e1 - e0,
        matched: m1 - m0,
        ops_recorded,
    })
}

fn med(v: &mut Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// **The arm the row's framing is missing: vary the DELTA at a FIXED op count.**
///
/// The ops axis alone cannot tell a complexity class from a constant, and here it genuinely is a
/// constant unless the delta moves. The merge ALREADY pays O(W) once per merge without any scan:
/// `WorkspaceSnapshot` clones `ws.frame.ops` wholesale (`runtime.rs:4043`). So the scan makes the
/// merge O(delta x W) where it was already O(W) — at a fixed delta of 4 that is a **4x constant**,
/// not a new class. The class only changes if `delta` grows, and `examined = delta x W` is the
/// integer that says so directly.
///
/// ⚠ Read the COUNTER here, not the milliseconds. Growing the delta grows the merge's real work
/// too — more cells to resolve, more ops applied, more rows published — so wall-clock rises in
/// this arm whether or not the scan exists. `examined` does not have that confound.
fn run_delta_arm(dir: &std::path::Path, deltas: &[i64], w: i64, merges: usize) {
    println!();
    println!("=== ARM DELTA — ops per branch held at {w}, axis = CHANGED CELLS per merge");
    println!("    (the arm that separates a 4x CONSTANT from a complexity class; read `examined`)");
    println!("    delta   n |   MERGE ms |  examined/merge  matched/merge | examined/(delta x ops)");
    for &d in deltas {
        let s = build(dir, &format!("d114_delta_{d}"));
        let mut mg = vec![];
        let (mut ex, mut ma, mut rec) = (0u64, 0u64, 0u64);
        for i in 0..merges {
            if let Some(c) = one_cycle(&s, i as u64, w, d, Arm::Churn) {
                mg.push(c.merge_ms);
                ex += c.examined;
                ma += c.matched;
                rec += c.ops_recorded;
            }
        }
        if mg.is_empty() {
            println!("  {d:>7}    0 |  NO MERGE APPLIED — not a result, and not a zero");
            continue;
        }
        let n = mg.len() as u64;
        if (n as usize) < merges {
            println!("  ⛔ delta {d}: only {n} of {merges} merges APPLIED — this row is NOT a measurement");
        }
        let mm = med(&mut mg);
        let (epm, mpm) = (ex as f64 / n as f64, ma as f64 / n as f64);
        let recpm = rec as f64 / n as f64;
        // If this ratio is ~1.0 the scan is EXACTLY `delta x ops` and the product is the cost.
        println!(
            "  {d:>7} {n:>3} | {mm:>10.3} | {epm:>15.1} {mpm:>15.1} | {:>21.3}  (ops recorded/merge = {recpm:.1})",
            epm / (d as f64 * recpm).max(1e-9)
        );
    }
}

fn run_arm(dir: &std::path::Path, arm: Arm, axis: &[i64], merges: usize, label: &str, order: &str) {
    println!();
    println!("=== ARM {label} — delta held at {} changed cells, axis = ops recorded per branch", DELTA());
    println!("    (axis walked {order}; one FRESH server per point — see `run_arm` for why)");
    println!("   ops/br   n |   MERGE ms |  examined/merge  matched/merge | CONTROL writes ms");
    let mut base: Option<(i64, f64, f64)> = None;
    for &w in axis {
        // ⛔ **A FRESH SERVER PER POINT, and this is not tidiness.** `State::applied` grows by one
        // entry per changed cell per merge and is never pruned, which is D86's whole finding. Walk
        // the axis on one server and the number of merges already done rises WITH the axis — a
        // confound pointing in exactly the direction of the hypothesis, which is how you measure a
        // slope and name the wrong mechanism for it. Twice, on this code. A fresh server puts every
        // point at the same `applied` length, so the only thing varying is `ops` per branch.
        let s = build(dir, &format!("d114_{w}"));
        let (mut mg, mut wr) = (vec![], vec![]);
        let (mut ex, mut ma, mut rec) = (0u64, 0u64, 0u64);
        for i in 0..merges {
            if let Some(c) = one_cycle(&s, i as u64, w, DELTA(), arm) {
                mg.push(c.merge_ms);
                wr.push(c.writes_ms);
                ex += c.examined;
                ma += c.matched;
                rec += c.ops_recorded;
            }
        }
        // ⛔ A run that collected nothing has NOT passed. Say so and do not print a zero row that
        // reads like a flat point on the curve — a fabricated flat point falsifies this row.
        if mg.is_empty() {
            println!("  {w:>7}    0 |  NO MERGE APPLIED — not a result, and not a zero");
            continue;
        }
        let n = mg.len() as u64;
        // ⛔ A PARTIAL n IS NOT A SMALLER SAMPLE, IT IS A BROKEN ARM. The first run of this harness
        // silently reported n=1 out of 15 because every merge after the first was refused, and a
        // lone sample printed in the same column as a median reads exactly like a result. Say it
        // on the row itself; the reader cannot be trusted to cross-check a count they did not ask for.
        if (n as usize) < merges {
            println!("  ⛔ {w:>5}: only {n} of {merges} merges APPLIED — this row is NOT a measurement");
        }
        let (mm, mw) = (med(&mut mg), med(&mut wr));
        let (epm, mpm) = (ex as f64 / n as f64, ma as f64 / n as f64);
        println!(
            "  {w:>7} {n:>3} | {mm:>10.3} | {epm:>15.1} {mpm:>15.1} | {mw:>17.3}    (ops actually recorded/merge = {:.1})",
            rec as f64 / n as f64
        );
        if base.is_none() {
            base = Some((w, mm, epm));
        }
        if let Some((w0, m0, e0)) = base {
            if w != w0 {
                println!(
                    "          ^ {:.0}x ops -> {:.2}x MERGE ms,  {:.2}x examined",
                    w as f64 / w0 as f64,
                    mm / m0.max(1e-9),
                    epm / e0.max(1e-9)
                );
            }
        }
    }
}

fn main() {
    let dir = std::env::temp_dir().join(format!("ferrodb-d114-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let axis: Vec<i64> = std::env::var("D114_OPS")
        .unwrap_or_else(|_| "4,16,64,256,1024".to_string())
        .split(',')
        .filter_map(|v| v.parse().ok())
        .collect();
    let merges: usize = std::env::var("D114_MERGES").ok().and_then(|v| v.parse().ok()).unwrap_or(15);

    println!("D114 — merge cost against OPS RECORDED PER BRANCH, at FIXED changed cells ({}).", DELTA());
    println!("PRE-REGISTERED: FLAT in ops/branch => the ours-side-scan reading is WRONG, row closes.");
    println!("                LINEAR at fixed delta => the row is real.");
    println!();
    println!("The counters are the finding; the milliseconds are an UPPER BOUND on a shared box.");
    println!("  examined/merge  = ops the ours-side scan WALKED   -> an index removes these");
    println!("  matched/merge   = ops compose_ops must FOLD       -> an index does NOT remove these");
    println!();
    println!("table rows = {ROWS} (fixed; the table axis is D68/D69's and is closed), merges/point = {merges}");

    // **The axis is walked in BOTH directions.** A machine that drifts monotonically through a run
    // fakes a slope in the ascending pass and an ANTI-slope in the descending one; a real slope
    // survives both. This box is shared and has a measured 46x quiet-vs-loaded spread, so a single
    // monotone pass is not evidence here no matter how clean the curve looks.
    let mut desc: Vec<i64> = axis.clone();
    desc.reverse();
    let rep = "REPEAT (ops land ON the delta cells: scan AND fold grow)";
    let chu = "CHURN  (ops land OFF the delta cells: only the scan grows)";
    run_arm(&dir, Arm::Repeat, &axis, merges, rep, "ASCENDING");
    run_arm(&dir, Arm::Churn, &desc, merges, chu, "DESCENDING");
    run_arm(&dir, Arm::Repeat, &desc, merges, rep, "DESCENDING");
    run_arm(&dir, Arm::Churn, &axis, merges, chu, "ASCENDING");

    // The third axis. Fixed op count, growing delta: the product `delta x ops` is what the scan
    // actually costs, and only this arm moves the left factor.
    let deltas: Vec<i64> = std::env::var("D114_DELTAS")
        .unwrap_or_else(|_| "1,2,4,8,16,32".to_string())
        .split(',')
        .filter_map(|v| v.parse().ok())
        .collect();
    let fixed_w: i64 = std::env::var("D114_DELTA_OPS").ok().and_then(|v| v.parse().ok()).unwrap_or(512);
    run_delta_arm(&dir, &deltas, fixed_w, merges);

    println!();
    println!("HOW TO READ IT:");
    println!("  Each arm appears TWICE, ascending and descending. If the two passes disagree on");
    println!("  the SHAPE, the machine moved and neither pass is a result.");
    println!("  REPEAT linear + CHURN flat  -> the slope is the FOLD. An index would NOT fix it.");
    println!("  REPEAT linear + CHURN linear-> the SCAN is real; D86's index is the right fix.");
    println!("  both flat                   -> the pre-registered falsifier FIRES. Close the row.");
    let _ = std::fs::remove_dir_all(&dir);
}
