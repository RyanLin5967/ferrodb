//! D102 — **which arm of `cow_page` does an agent merge actually take?**
//!
//! # Why this exists before any encoder does
//!
//! D93 measured that an intra-page delta makes a four-row agent merge cost 211 B instead of
//! 16,384 B, and the plan that follows from it is "wire [`ferrodb::branch::delta::PageDelta`] into
//! [`ArenaPageStore::cow_page`]". That plan rests on a premise nobody had measured: **that an
//! agent merge reaches the arm of `cow_page` that copies a whole page at all.**
//!
//! `cow_page` has two arms. One mutates a page in place, because the page is already private to
//! the writing branch — there is no copy there, so there is nothing for a delta to shrink. The
//! other allocates a fresh page and copies `source[PAGE_HEADER_SIZE..]` into it, and that copy is
//! the entire cost a delta encoder is aimed at. A delta helps on the second arm and cannot help on
//! the first.
//!
//! D90's sweep reports `live_page_count delta` exactly equal to its distinct-page count for every
//! `r` it measured — 1, 3, 5, 9, 17, 33, 65, with no page ever freed. That is the arithmetic
//! signature of **fresh allocation**, not of shadowing: the shadowing arm of `cow_page` allocates
//! a page *and* (when the branch owns the source extent) frees one, and the pages it shadows for a
//! 128-row scattered update would number in the hundreds, not three. So the signature says the
//! copying arm is not where D90's bytes come from. That is an inference from a second-hand
//! instrument, and this file replaces it with the counter itself.
//!
//! # The instrument
//!
//! [`ferrodb::branch::arena::cow_path_census`] — three process-wide integers bumped inside
//! `cow_page` on the exact lines that choose an arm. Counters, not timings, so the fleet's load
//! does not enter. The census is reset immediately before the measured window and read
//! immediately after, with `flush_all()` on both sides for the same reason D90 brackets its own
//! window: dirty frames that land later belong to a different cycle.
//!
//! # ⚠ PRE-REGISTERED, before the first number exists
//!
//! * **If `shadow` is 0 for every `r`, the premise is FALSIFIED**: no agent merge in this shape
//!   copies a whole page, so wiring a delta encoder into `cow_page` cannot change one byte that
//!   D90 measures. Report that, and do not report the encoder's unit-test saving as if it were an
//!   end-to-end one.
//! * **If `shadow` is large and grows with `r`**, the premise holds, the copying arm is the cost,
//!   and `shadow_payload_bytes` is the exact budget a delta encoder is competing against.
//! * **If `shadow` is small but non-zero and FLAT in `r`** — a couple of pages per merge whatever
//!   `r` is — the copying arm is real but is per-merge bookkeeping rather than per-changed-row,
//!   and the saving available is that fixed handful of pages and no more.
//!
//! # The anti-vacuity guard, which is not optional
//!
//! A census over a merge that moved nothing would report a truthful zero about a fiction. So every
//! cycle reads its last written row back through an ordinary `SELECT` against the trunk, after the
//! counters are read, and refuses to print a census unless the trunk holds the value the branch
//! wrote. `D102_FIRECHECK=1` stages no rows while still forking and merging, which must make every
//! cycle refuse and the process exit non-zero — the only way to know the guard is not decorative.
use std::path::Path;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::{cow_path_census, reset_cow_path_census, ArenaPageStore};
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
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

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    store: Arc<ArenaPageStore>,
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
    let out = run(stmt, &mut cat, s.bp.clone(), s.ctx.txn.clone(), sess);
    drop(cat);
    out.map_err(|e| e.to_string())
}

/// D90's `build`, in the one configuration that puts the arena and the CoW pages really in the
/// loop. `with_catalog` would set `storage: None` and send agent writes to an in-memory effect
/// log, which would make every count on this page a fiction.
fn build(dir: &Path, nrows: i64) -> Server {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let disk = DiskManager::new(dir.join("main.db").to_str().unwrap()).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(disk)));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.join("main.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&dir.join("b.branchcat"), 1).unwrap());
    let branches: Arc<dyn BranchCatalog> = cat.clone();
    let arena_base: u32 = ((nrows / 40) as u32 + 4096).next_power_of_two();
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), arena_base).unwrap());
    store.checkpoint_to(dir.join("main.arena"));
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
    let s = Server { ctx, bp, store };

    let mut sess = Session::with_runtime(Arc::clone(&s.ctx.runtime));
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    const BATCH: i64 = 1000;
    let mut i = 1;
    while i <= nrows {
        let hi = (i + BATCH - 1).min(nrows);
        exec(&s, "BEGIN;", &mut sess).unwrap();
        for k in i..=hi {
            exec(&s, &format!("INSERT INTO t VALUES ({k}, {});", k * 7), &mut sess).unwrap();
        }
        exec(&s, "COMMIT;", &mut sess).unwrap();
        i = hi + 1;
    }
    s
}

struct Row {
    r: i64,
    in_place: u64,
    shadow: u64,
    shadow_bytes: u64,
    live_delta: i64,
}

/// One fork / update `r` scattered rows / merge, with the census bracketing exactly that.
fn one_cycle(s: &Server, r: i64, nrows: i64, seq: usize) -> Option<Row> {
    s.bp.flush_all().unwrap();
    reset_cow_path_census();
    let live0 = s.store.live_page_count().unwrap_or(0) as i64;

    // `with_runtime`, never `Session::new()` — D90's header records that the latter builds its own
    // storage-less runtime and measures a stub while looking like it worked.
    let mut sess = Session::with_runtime(Arc::clone(&s.ctx.runtime));
    let (branch, _prov) =
        match exec(s, &format!("BEGIN AGENT SESSION AS 'd102' RUN 'r{seq}';"), &mut sess) {
            Ok(Outcome::Agent(AgentOutput::SessionStarted(a))) => (a.branch, a.prov),
            Ok(_) => {
                eprintln!("  r={r}: BEGIN did not return a SessionStarted — the instrument is wrong");
                return None;
            }
            Err(e) => {
                eprintln!("  r={r}: BEGIN AGENT SESSION failed: {e}");
                return None;
            }
        };
    let fire_check = std::env::var("D102_FIRECHECK").is_ok();
    for i in 0..r {
        if fire_check {
            break;
        }
        // D90's scatter, unchanged: 7919 is prime and does not divide nrows, so this is injective.
        let id = 1 + (i * 7919) % nrows;
        exec(s, &format!("UPDATE t SET v = {} WHERE id = {id};", i + 1), &mut sess).ok()?;
    }
    let applied = match exec(s, "MERGE;", &mut sess) {
        Ok(Outcome::Agent(AgentOutput::Merge(report))) => report.applied_to_target,
        Ok(_) => {
            eprintln!("  r={r}: MERGE returned an outcome that is not a merge report — refusing.");
            return None;
        }
        Err(e) => {
            eprintln!("  r={r}: MERGE failed: {e}");
            return None;
        }
    };
    if !applied {
        eprintln!("  r={r}: merge did NOT apply to target — not a result, and not a zero");
        return None;
    }
    s.bp.flush_all().unwrap();

    let (in_place, shadow, shadow_bytes) = cow_path_census();
    let live1 = s.store.live_page_count().unwrap_or(0) as i64;
    let _ = branch;

    // ---- ANTI-VACUITY: the merge must have actually moved the rows, read after the counters ----
    let last_i = r - 1;
    let check_id = 1 + (last_i * 7919) % nrows;
    let expect = Value::Integer((last_i + 1) as i32);
    let mut verify = Session::with_runtime(Arc::clone(&s.ctx.runtime));
    match exec(s, &format!("SELECT v FROM t WHERE id = {check_id};"), &mut verify) {
        Ok(Outcome::Rows(rows)) => {
            let got = rows.first().and_then(|row| row.first()).cloned();
            if got.as_ref() != Some(&expect) {
                eprintln!(
                    "  r={r}: MERGE reported applied, but trunk row id={check_id} reads {got:?}, \
                     not {expect:?}. The merge published nothing this census can describe. \
                     Refusing to report its counters."
                );
                return None;
            }
        }
        Ok(_) => {
            eprintln!("  r={r}: the read-back SELECT did not return Rows — the check is broken.");
            return None;
        }
        Err(e) => {
            eprintln!("  r={r}: the read-back SELECT failed: {e}. Refusing unverified counters.");
            return None;
        }
    }

    Some(Row { r, in_place, shadow, shadow_bytes, live_delta: live1 - live0 })
}

fn main() {
    let nrows: i64 = std::env::var("D102_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(20_000);
    let rs: Vec<i64> = std::env::var("D102_R")
        .unwrap_or_else(|_| "1,2,4,8,16,32,64,128,256,512".to_string())
        .split(',')
        .filter_map(|v| v.parse().ok())
        .collect();
    assert!(!rs.is_empty(), "an empty sweep is not a measurement");
    assert!(
        *rs.iter().max().unwrap() < nrows,
        "r must stay below the row count or the scattered ids stop being distinct"
    );

    println!("D102 — WHICH ARM OF cow_page DOES AN AGENT MERGE TAKE?");
    println!("  instrument: branch::arena::cow_path_census(), three counters bumped on the two");
    println!("  lines inside cow_page that choose an arm. Counters, not timings.");
    println!();
    println!("  shadow > 0  => cow_page copies whole pages here, and a delta encoder has a target.");
    println!("  shadow == 0 => this workload never copies a page, and no encoder placed in");
    println!("                 cow_page can change one byte of what D90 measures.");
    println!();
    println!("table = {nrows} rows, fixed. sweep r = {rs:?}");

    let dir = std::env::temp_dir().join(format!("ferrodb-d102-{}", std::process::id()));
    let s = build(&dir, nrows);
    s.bp.flush_all().unwrap();
    println!("built; {} live arena pages", s.store.live_page_count().unwrap_or(0));

    let mut seq = 0usize;
    // D90's warm-up, for D90's reason: first touch allocates the arena's first extent and faults
    // in catalog pages, and charging that to the smallest r would manufacture a result.
    let warm: usize = std::env::var("D102_WARMUP").ok().and_then(|v| v.parse().ok()).unwrap_or(3);
    let warm_r = *rs.iter().max().unwrap();
    for _ in 0..warm {
        seq += 1;
        if one_cycle(&s, warm_r, nrows, seq).is_none() {
            eprintln!("REFUSED: a warm-up cycle did not complete; no steady state was reached.");
            let _ = std::fs::remove_dir_all(&dir);
            std::process::exit(1);
        }
    }

    let mut rows: Vec<Row> = Vec::new();
    for &r in rs.iter() {
        seq += 1;
        match one_cycle(&s, r, nrows, seq) {
            Some(c) => rows.push(c),
            None => println!("  r={r}: NO CYCLE — refusing to report a zero as a measurement"),
        }
    }
    if rows.is_empty() {
        eprintln!();
        eprintln!("REFUSED: not one cycle completed. A run that collected nothing has not passed.");
        let _ = std::fs::remove_dir_all(&dir);
        std::process::exit(1);
    }

    println!();
    println!("       r | cow in-place |  cow SHADOW | shadow payload B | live pages delta");
    for c in &rows {
        println!(
            "  {:6} | {:12} | {:11} | {:16} | {:+16}",
            c.r, c.in_place, c.shadow, c.shadow_bytes, c.live_delta
        );
    }

    let total_shadow: u64 = rows.iter().map(|c| c.shadow).sum();
    let total_in_place: u64 = rows.iter().map(|c| c.in_place).sum();
    println!();
    println!("=== VERDICT ===");
    if total_shadow == 0 {
        println!("  shadow == 0 for EVERY r, across {} measured merges.", rows.len());
        println!("  ⇒ PREMISE FALSIFIED. `cow_page` never copies a whole page in this workload, so");
        println!("    it holds no bytes for a delta encoder to remove. The {total_in_place} cows that did");
        println!("    happen all took the in-place arm, where there is no copy. D90's per-merge page");
        println!("    is a FRESHLY ALLOCATED page holding the branch's own changed rows — an");
        println!("    allocation-granularity cost, not a copy cost. A delta against a base cannot");
        println!("    shrink a page that has no base.");
    } else {
        let flat = rows.iter().all(|c| c.shadow == rows[0].shadow);
        println!("  shadow > 0: {total_shadow} whole-page copies across {} merges.", rows.len());
        if flat {
            println!("  ⇒ The copying arm IS reached, but its count is FLAT in r ({} per merge).", rows[0].shadow);
            println!("    The saving available is that fixed handful of pages per merge, not a");
            println!("    saving that scales with how little changed.");
        } else {
            println!("  ⇒ The copying arm is reached and scales with r. This is the target a delta");
            println!("    encoder is aimed at, and shadow payload B is the budget it competes with.");
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
