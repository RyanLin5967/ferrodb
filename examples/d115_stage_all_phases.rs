//! D115 — **is `stage_all` quadratic in a session's op count, and if so, WHICH term is it?**
//!
//! # The row, and what is only a reading of it
//!
//! `runtime.rs` clones the whole `TxnFrame` once per statement (`ws.frame.clone()`), and the frame
//! accumulates every op the agent session has recorded. `W` statements therefore copy
//! `W(W+1)/2` ops. D114's harness measured the writes phase at delta 32 and saw the exponent climb
//! 1.10 → 1.27 → 1.83 → 1.95 across the decades, which is what that shape predicts.
//!
//! **The shape predicting the curve is not evidence that it CAUSED the curve**, and this is the
//! file where that distinction has cost three rows (D68, D69-REOPEN, D114). D114 is the instructive
//! one: the shape read off the source was exactly right — `examined = delta × ops`, ratio 1.000 —
//! and the term was worth a few hundred microseconds against a merge doing page writes. Right
//! shape, wrong magnitude.
//!
//! # ⚠ The clone is not the only O(ops) term on this path. There are FOUR
//!
//! Every statement, with `n` = ops the session has already recorded:
//!
//! | # | term | where | cost | fires when |
//! |---|---|---|---|---|
//! | 1 | `ws.frame.clone()` | `runtime.rs` | `n` op clones + 1 alloc | always |
//! | 2 | `extends()`'s ops prefix | `tel/log.rs` | `n` `op_eq` calls | always (re-append) |
//! | 3 | `frame_eq()`'s guard compare | `tel/log.rs` | `g` recursive `GuardExpr` walks | only when the statement pushes **no** guard, because `Vec::eq` short-circuits on LENGTH and the op-count check sits *after* it |
//! | 4 | `position()` over the log | `tel/log.rs` | frames held | axis is SESSIONS, not ops |
//! | 5 | dropping the clone | `runtime.rs` | `n` `Op` destructors | always — and it lands after every obvious span |
//!
//! Terms in series are indistinguishable from one term several times as big — D99's lesson — so
//! each is counted on its own axis here rather than inferred from the total. Term 3 in particular
//! is named nowhere in the row and fires under a condition the other two do not share.
//!
//! And a fifth candidate that is not on the frame at all: the **CoW mirror** (`put_row` per staged
//! row). The row does not mention it; it is timed anyway, because "the cost is the thing the row
//! noticed" is precisely the assumption that closed D114 the hard way.
//!
//! # PRE-REGISTERED falsifier (SCALE-DESIGN.md D115, recorded before any code was touched)
//!
//! > The exponent must fall from ~1.95 toward 1.0 on the same ops axis with delta fixed.
//! > **Flat or unchanged ⇒ the clone is not the cause; close the row and say so plainly.**
//!
//! This harness is the BEFORE half. It establishes (a) that the quadratic reproduces here, and
//! (b) which of the five terms carries it. Nothing is changed until it has.
//!
//! # ⭐ The axis is walked in BOTH directions
//!
//! D114's ascending/descending control caught a contamination worth 3.7x on a single point — the
//! same configuration read 36.121 ms ascending and 9.798 ms descending on a loaded box. One
//! monotone pass would have reported that difference as a slope. Every arm here runs ASC then DESC
//! and both are printed; a point whose two readings disagree by more than the effect being claimed
//! is a contaminated point, not a measurement.
//!
//! # The instrument
//!
//! `tel::stage_probe` — six `Instant` pairs and a dozen relaxed `fetch_add`s per statement, a fixed
//! per-statement cost that does not vary with the axis. Every phase reports a nanosecond total
//! (an UPPER BOUND on a shared box, and labelled as one) and an integer that does not move under
//! load. `clone_ops` is the integer form of term 1 and `extends_op_cmp` of term 2; if the
//! milliseconds and the integers disagree about which is growing, believe the integers about the
//! SHAPE and the milliseconds about the MAGNITUDE.
//!
//! ⚠ `sum/stage` in the third table is the instrument auditing ITSELF: the phases must add up to
//! the whole function. The first version of this harness summed to 60% and the missing 40% was
//! term 5 — the drop — which no span covered. A phase table that does not close is an attribution
//! with a hole in it, and the hole is always the term nobody named.
//!
//! Usage:
//!   `D115_OPS=32,128,512,2048,8192 D115_DELTA=32 D115_REPS=3 cargo run --release --example d115_stage_all_phases`
//!
//! Refuses rather than reporting: a cycle whose merge did not apply, a point with zero reps, an
//! ops axis whose `stage_calls` does not match the statements it issued.

use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::Instant;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
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
use ferrodb::tel::stage_probe;
use ferrodb::tel::{DurableEffectLog, EffectLog, MemEffectLog};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Rows in the table. NOT the axis — held fixed, and the same value D114 used so the writes-phase
/// numbers here are comparable with the ones the row quotes.
const ROWS: i64 = 2000;

fn env_list(name: &str, default: &[i64]) -> Vec<i64> {
    match std::env::var(name) {
        Ok(v) => v
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().parse().expect("D115 axis must be integers"))
            .collect(),
        Err(_) => default.to_vec(),
    }
}

fn env_num(name: &str, default: i64) -> i64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Which effect-log store to measure.
///
/// ⛔ **The default is `mem`, and `mem` is NOT what production runs.** `cli.rs:141` builds a
/// `DurableEffectLog`; `MemEffectLog::new()` appears on the SQL path only in tests and harnesses.
/// D114's harness — the one whose writes-phase column produced the D115 row — used `mem`, so the
/// quoted 2.25 s/session is a number for a configuration the shipped binary does not use. That is
/// the same trap D77 flagged ("free-space-map persistence may be OFF in the harnesses that
/// produced this project's 10^6 results") and the one that withdrew D67's numbers.
///
/// `mem` is kept as the default anyway, because reproducing the row's own number is the first
/// thing a before-curve has to do. `D115_LOG=durable` runs the identical axis on the shipped
/// store, and the two together are the answer.
fn log_kind() -> String {
    std::env::var("D115_LOG").unwrap_or_else(|_| "mem".to_string())
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

/// ⚠ `with_storage`, NOT `with_catalog` — carried over from D114 along with the reason.
/// `with_catalog` sets `storage: None`, which switches the whole branch storage engine off: agent
/// writes never reach arena pages, the CoW mirror inside `stage_all` never runs, and the harness
/// would be measuring a configuration production does not use. D67's first run made that mistake
/// and its numbers were withdrawn.
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
    // Reserve a table region BELOW the arena: taking `high_water()` here puts the arena at page 2
    // and leaves the ordinary table nowhere to grow. Same fixed floor and same reason as D68.
    const ARENA_BASE: u32 = 1024;
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), ARENA_BASE).unwrap());
    let log: Arc<dyn EffectLog> = if log_kind() == "durable" {
        Arc::new(DurableEffectLog::open(d.join("effects.tel")).expect("open durable effect log"))
    } else {
        Arc::new(MemEffectLog::new())
    };
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches,
            log,
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
    let s = Server { ctx, bp, txn };

    // ⛔⛔ `Session::with_runtime`, NOT `Session::new()`. THIS IS THE LINE THAT DECIDES WHETHER
    // THE HARNESS MEASURES THE RIG IT BUILT. `Session::new()` constructs its OWN
    // `AgentRuntime::new()` — `storage: None`, its own in-memory branch catalog, its own
    // `MemEffectLog` — so every agent statement misses the arena store, the durable effect log and
    // the sidecar catalog configured above, and the whole `with_storage` rig sits unused while the
    // numbers still look entirely reasonable. `pgwire::serve` uses `with_runtime`
    // (`pgwire/mod.rs:358`), so a harness on `Session::new()` is not running the server's
    // configuration.
    //
    // This is the project's OPEN ROW **D101 (stub-runtime)**, named in
    // `examples/d68_merge_is_o_table.rs:249`. Not a new finding — what is new is that the D115
    // row's own numbers were produced under it, through D114's harness, which has `Session::new()`
    // at both of its session sites.
    //
    // ⭐ The instrument that caught it was `mirror_rows`, an INTEGER that cannot be zero when a
    // page store is attached. It read 0 at every point of the axis. The timer alone showed
    // `mirror` at 0.000 ms, which reads as "the mirror is cheap".
    let mut sess = Session::with_runtime(Arc::clone(&s.ctx.runtime));
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER, c INTEGER);", &mut sess).unwrap();
    for i in 1..=ROWS {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, {}, {});", i * 7, i * 13), &mut sess).unwrap();
    }
    s
}

/// The cells the session writes, for a delta of `d`. Spread so no two land in one page slot by
/// accident; which rows they are does not matter, only that the set is the same at every point.
fn delta_ids(d: i64) -> Vec<i64> {
    (0..d).map(|k| 11 + k * 7).collect()
}

struct Cycle {
    writes_ms: f64,
    statements: u64,
    probe: Vec<u64>,
}

/// One agent's whole life at a chosen op count: fork, issue `target_ops` single-row UPDATEs
/// round-robin over `delta` cells, merge.
///
/// ⚠ The workspace — and therefore its `TxnFrame` — is created fresh by `BEGIN AGENT SESSION` and
/// destroyed by the seal `MERGE` performs. "Ops per session" means ops recorded before the merge;
/// it does not accumulate across cycles, which is why the axis has to be driven deliberately.
///
/// ⛔ ONE agent name for every cycle, not `a{seq}`. The provenance slot is declared per branch and
/// REFUSES redeclaration under a different name, so a per-cycle name makes every merge after the
/// first fail — and a harness that only counts merges that applied then prints `n=1` instead of an
/// error, a broken instrument wearing a result's clothes. D68's harness reuses one name for
/// exactly this reason.
///
/// ⚠ The written value MUST depend on `seq`. Without it cycle 2 writes the values cycle 1 already
/// published, every cell is unchanged, the delta is zero and the merge applies nothing — a second
/// route to `n=1` that looks identical to the first.
fn one_cycle(s: &Server, seq: u64, target_ops: i64, delta: i64) -> Option<Cycle> {
    let ids = delta_ids(delta);
    let mut sess = Session::with_runtime(Arc::clone(&s.ctx.runtime));
    if let Err(e) = exec(s, "BEGIN AGENT SESSION AS 'a0';", &mut sess) {
        eprintln!("  [seq {seq}] BEGIN failed: {e}");
        return None;
    }

    let before = stage_probe::snapshot();
    let mut statements = 0u64;
    let t = Instant::now();
    let rounds = (target_ops / delta).max(1);
    for r in 0..rounds {
        for (k, id) in ids.iter().enumerate() {
            let v = 1_000_000 + seq as i64 * 1_000_000 + r * 64 + k as i64;
            if let Err(e) = exec(s, &format!("UPDATE t SET v = {v} WHERE id = {id};"), &mut sess) {
                eprintln!("  [seq {seq}] UPDATE failed: {e}");
                return None;
            }
            statements += 1;
        }
    }
    let writes_ms = t.elapsed().as_secs_f64() * 1000.0;
    let probe = stage_probe::delta(&before, &stage_probe::snapshot());

    // The merge is not measured here — it is D114's row and it is closed. It is issued because it
    // is what SEALS the workspace, and without it the next cycle's `BEGIN` has nowhere to start.
    // ⚠ Read the report, not `is_ok()`: a QUARANTINED merge returns Ok without doing the work, and
    // a MERGE returns `Outcome::Agent`, so matching `Outcome::Table` counts zero forever.
    let applied = match exec(s, "MERGE;", &mut sess) {
        Ok(Outcome::Agent(AgentOutput::Merge(report))) => report.applied_to_target,
        Ok(_) => {
            eprintln!("  [seq {seq}] MERGE returned a NON-merge outcome");
            false
        }
        Err(e) => {
            eprintln!("  [seq {seq}] MERGE errored: {e}");
            false
        }
    };
    if !applied {
        return None;
    }
    Some(Cycle { writes_ms, statements, probe })
}

fn med(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

struct Point {
    ops: i64,
    n: usize,
    writes_ms: f64,
    statements: u64,
    probe: Vec<u64>,
}

/// One point of the ops axis: `reps` cycles on a fresh server, median wall clock, summed counters.
///
/// The counters are SUMMED over the reps rather than medianed, and divided by `n` where a
/// per-session figure is wanted. They are exact integers, so a sum loses nothing; the wall clock
/// is medianed because on a shared box its outliers are the machine, not the code.
fn run_point(dir: &std::path::Path, ops: i64, delta: i64, reps: usize, tag: &str) -> Point {
    let s = build(dir, &format!("d115_{tag}_{ops}"));
    let mut ms = vec![];
    let mut probe = vec![0u64; stage_probe::FIELDS.len()];
    let mut statements = 0u64;
    for i in 0..reps {
        match one_cycle(&s, i as u64, ops, delta) {
            Some(c) => {
                ms.push(c.writes_ms);
                statements += c.statements;
                for (slot, v) in probe.iter_mut().zip(&c.probe) {
                    *slot += v;
                }
            }
            None => eprintln!("  [{tag} ops={ops}] rep {i} did not complete"),
        }
    }
    if ms.is_empty() {
        eprintln!("⛔ REFUSED: ops={ops} ({tag}) completed ZERO reps. A point with no reps is not a measurement.");
        std::process::exit(2);
    }
    let n = ms.len();
    Point { ops, n, writes_ms: med(&mut ms), statements, probe }
}

/// `log(y2/y1) / log(x2/x1)` — the local exponent between two adjacent points. 1.0 is linear,
/// 2.0 quadratic. Printed per interval rather than as one fitted slope, because the whole claim
/// in the row is that the exponent CLIMBS across the decades.
fn exponent(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    if x1 <= 0.0 || y1 <= 0.0 || x2 <= 0.0 || y2 <= 0.0 {
        return f64::NAN;
    }
    (y2 / y1).ln() / (x2 / x1).ln()
}

fn print_table(tag: &str, pts: &[Point], delta: i64) {
    println!();
    println!("=== {tag} — ops axis, delta fixed at {delta}, {} points", pts.len());
    println!(
        "    {:>6} {:>3} | {:>10} {:>6} | {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} | {:>6}",
        "ops", "n", "writes ms", "exp", "decide", "apply", "clone", "drop", "append", "mirror", "exp(t)"
    );
    println!(
        "    {:>6} {:>3} | {:>10} {:>6} | {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} | {:>6}",
        "", "", "(median)", "", "ms/sess", "ms/sess", "ms/sess", "ms/sess", "ms/sess", "ms/sess", ""
    );
    let mut prev: Option<(f64, f64, f64)> = None;
    for p in pts {
        let n = p.n as f64;
        let g = |f: &str| stage_probe::field(&p.probe, f) as f64 / n / 1e6;
        let total = stage_probe::field(&p.probe, "stage_ns") as f64 / n / 1e6;
        let (exp_w, exp_t) = match prev {
            None => (f64::NAN, f64::NAN),
            Some((px, pw, pt)) => (
                exponent(px, pw, p.ops as f64, p.writes_ms),
                exponent(px, pt, p.ops as f64, total),
            ),
        };
        println!(
            "    {:>6} {:>3} | {:>10.3} {:>6.2} | {:>8.3} {:>8.3} {:>8.3} {:>8.3} {:>8.3} {:>8.3} | {:>6.2}",
            p.ops,
            p.n,
            p.writes_ms,
            exp_w,
            g("decide_ns"),
            g("apply_ns"),
            g("clone_ns"),
            g("drop_ns"),
            g("append_ns"),
            g("mirror_ns"),
            exp_t
        );
        prev = Some((p.ops as f64, p.writes_ms, total));
    }

    println!();
    println!("    integers — per session, exact, immune to fleet load");
    println!(
        "    {:>6} | {:>11} {:>11} {:>11} {:>11} {:>10} {:>9} {:>8} {:>11}",
        "ops", "stage_calls", "clone_ops", "extends_cmp", "eq_guard_cmp", "eq_op_cmp", "tail_ops", "pos_scan", "mirror_rows"
    );
    for p in pts {
        let n = p.n as u64;
        let g = |f: &str| stage_probe::field(&p.probe, f) / n;
        println!(
            "    {:>6} | {:>11} {:>11} {:>11} {:>11} {:>10} {:>9} {:>8} {:>11}",
            p.ops,
            g("stage_calls"),
            g("clone_ops"),
            g("extends_op_cmp"),
            g("eq_guard_cmp"),
            g("eq_op_cmp"),
            g("extend_tail_ops"),
            g("position_scan"),
            g("mirror_rows"),
        );
    }

    println!();
    println!("    share of stage_all, per session (%) — the attribution the row needs");
    println!(
        "    {:>6} | {:>9} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} | {:>12} {:>12}",
        "ops", "stage ms", "decide", "apply", "clone", "drop", "append", "mirror", "stage/writes", "sum/stage"
    );
    for p in pts {
        let n = p.n as f64;
        let g = |f: &str| stage_probe::field(&p.probe, f) as f64 / n / 1e6;
        let stage = g("stage_ns");
        let sum = g("decide_ns")
            + g("apply_ns")
            + g("clone_ns")
            + g("drop_ns")
            + g("append_ns")
            + g("mirror_ns");
        let pc = |v: f64| if stage > 0.0 { 100.0 * v / stage } else { f64::NAN };
        println!(
            "    {:>6} | {:>9.3} {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% | {:>11.1}% {:>11.1}%",
            p.ops,
            stage,
            pc(g("decide_ns")),
            pc(g("apply_ns")),
            pc(g("clone_ns")),
            pc(g("drop_ns")),
            pc(g("append_ns")),
            pc(g("mirror_ns")),
            if p.writes_ms > 0.0 { 100.0 * stage / p.writes_ms } else { f64::NAN },
            pc(sum),
        );
    }
}

/// Every point must have issued exactly the statements the axis asked for. A short point reads as
/// a fast point, which is the one direction of instrument error that looks like a result.
fn check_statements(pts: &[Point], delta: i64) {
    for p in pts {
        let rounds = (p.ops / delta).max(1) as u64;
        let want = rounds * delta as u64 * p.n as u64;
        if p.statements != want {
            eprintln!(
                "⛔ REFUSED: ops={} issued {} statements, expected {}",
                p.ops, p.statements, want
            );
            std::process::exit(2);
        }
        // ⛔ The rig attaches an `ArenaPageStore`, so the CoW mirror MUST have run. Zero
        // mirrored rows means the statements reached some OTHER runtime — the D101 stub-runtime
        // defect — and every number in the run then describes a configuration nobody ships.
        // A refusal, not a warning: this failure produces plausible numbers, which is the only
        // kind that gets quoted.
        if stage_probe::field(&p.probe, "mirror_rows") == 0 {
            eprintln!(
                "⛔ REFUSED: ops={} mirrored ZERO rows with a page store attached. The statements \
                 did not reach the configured runtime (D101 stub-runtime).",
                p.ops
            );
            std::process::exit(2);
        }
        let calls = stage_probe::field(&p.probe, "stage_calls");
        if calls != p.statements {
            eprintln!(
                "⛔ REFUSED: ops={} issued {} statements but stage_all ran {} times — the \
                 instrument is not on the path being driven",
                p.ops, p.statements, calls
            );
            std::process::exit(2);
        }
    }
}

fn main() {
    let ops = env_list("D115_OPS", &[32, 128, 512, 2048, 8192]);
    let delta = env_num("D115_DELTA", 32);
    let reps = env_num("D115_REPS", 3).max(1) as usize;
    let dir = std::env::temp_dir().join(format!("ferrodb-d115-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    println!("D115 — stage_all phase attribution");
    println!("  ops axis        : {ops:?}");
    println!("  delta (cells)   : {delta}");
    println!("  reps per point  : {reps}");
    println!("  table rows      : {ROWS}");
    println!("  effect log      : {}  (⛔ production ships `durable`, cli.rs:141)", log_kind());
    println!("  load at start   : {}", load_avg());
    println!();
    println!("  ⚠ Every millisecond below is an UPPER BOUND — this box runs a build fleet.");
    println!("    The integer columns are the claim; the durations confirm the magnitude.");

    let asc: Vec<Point> = ops.iter().map(|&o| run_point(&dir, o, delta, reps, "asc")).collect();
    let mut desc: Vec<Point> = ops
        .iter()
        .rev()
        .map(|&o| run_point(&dir, o, delta, reps, "desc"))
        .collect();
    desc.reverse();

    check_statements(&asc, delta);
    check_statements(&desc, delta);

    print_table("ASCENDING", &asc, delta);
    print_table("DESCENDING (same axis, walked backwards)", &desc, delta);

    println!();
    println!("=== ASC vs DESC — the contamination control");
    println!("    A point whose two readings disagree by more than the effect being claimed is a");
    println!("    contaminated point. D114 saw 36.121 vs 9.798 ms on one point of this same box.");
    println!("    {:>6} | {:>10} {:>10} {:>7} | {:>10} {:>10} {:>7}", "ops", "asc ms", "desc ms", "ratio", "asc clone", "desc clone", "ratio");
    for (a, d) in asc.iter().zip(&desc) {
        let ac = stage_probe::field(&a.probe, "clone_ns") as f64 / a.n as f64 / 1e6;
        let dc = stage_probe::field(&d.probe, "clone_ns") as f64 / d.n as f64 / 1e6;
        println!(
            "    {:>6} | {:>10.3} {:>10.3} {:>7.2} | {:>10.3} {:>10.3} {:>7.2}",
            a.ops,
            a.writes_ms,
            d.writes_ms,
            a.writes_ms / d.writes_ms,
            ac,
            dc,
            if dc > 0.0 { ac / dc } else { f64::NAN }
        );
    }
    println!();
    println!("  load at end     : {}", load_avg());
    let _ = std::fs::remove_dir_all(&dir);
}

/// The 1/5/15-minute load averages, straight from `uptime`. Printed at both ends of the run
/// because a duration on this box is only interpretable next to what else was on it.
fn load_avg() -> String {
    std::process::Command::new("uptime")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}
