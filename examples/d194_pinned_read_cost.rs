//! D194 new-wall audit: what a branch's read costs against the merges committed since its pin
//! (term 2), and what the live branches' pinned snapshots retain (term 3).
//!
//! PRE-REGISTERED in `bench/d194_fork_snapshot/rebase_prereg.md`, Amendment 4, before this file
//! was written.
//!
//! # Part 1: read cost against merges since the pin
//!
//! A branch reads main through the snapshot it was pinned at. `resolve_visibility` walks each row's
//! `prev` chain from the newest version down, doing one `tt_heap.read` per version the pinned view
//! cannot see. So a read on an OLD branch pays one hop per version committed to each row it scans
//! since the pin. A FRESH branch forked at the same moment pays none. That branch is the control:
//! same tables, same chains, a pin that is not old.
//!
//! The OLD branch is pinned at k = 0. Each merge is `UPDATE t SET v = v + 1 WHERE id >= 1`, merged
//! from a fresh branch, and adds one version to every row. At each checkpoint k the harness reads:
//!
//! * `wal::visibility::VISIBILITY_HOPS`: an integer that control flow fixes, so load cannot move it.
//!   OLD full scan = ROWS·k, FRESH = 0. OLD point read = k through the primary index, or ROWS·k if
//!   the planner scans; `SEQ_SCAN_TUPLES` says which. Any other value REFUSES the run.
//! * the median latency of each read over REPS repetitions. It is reported, not certified. The
//!   pre-registered shape is a positive slope in k for OLD and a flat line for FRESH.
//!
//! # Part 2: snapshot census
//!
//! OPEN_TXNS plain transactions are held open, so every snapshot carries that many active ids.
//! Then FORKS branches are forked twice over:
//!
//! * SHARED: back to back on one thread, with no transaction begun or ended between forks, so
//!   every fork hits `read_snapshot_cached`'s cache. Census `(FORKS, 1, OPEN_TXNS)`.
//! * CHURN: a main autocommit INSERT between forks, so each fork misses. Census
//!   `(FORKS, FORKS, FORKS·OPEN_TXNS)`.
//!
//! Any other triple REFUSES the run, and so does a census that is not `(0, 0, 0)` once the SHARED
//! arm's branches are abandoned.
//!
//! Run: `timeout 1800 cargo run --release --example d194_pinned_read_cost`.

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::execution::executor::{run, try_run_read, Outcome};
use ferrodb::execution::seq_scan::seq_scan_counters;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;
use ferrodb::wal::visibility::VISIBILITY_HOPS;

const ROWS: u64 = 256;
const CHECKPOINTS: [u64; 4] = [0, 8, 32, 128];
const REPS: usize = 5;
const OPEN_TXNS: usize = 16;
const FORKS: usize = 64;

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
}

/// One statement through the server's own dispatch: the shared read path first, and the
/// exclusive lock only for what `try_run_read` refuses. This is `d55_agent_read_scaling`'s `exec`.
/// A branch's own `SELECT` must take the path the server takes, which is the shared one.
fn exec(s: &Server, sql: &str, sess: &mut Session) -> Result<usize, String> {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    if !parser.errors.is_empty() {
        return Err(format!("parse failed for {sql}: {:?}", parser.errors));
    }
    let stmt = stmts.remove(0);
    let mut cache = None;
    let slot = AtomicBool::new(false);
    let outcome = {
        let shared = s.ctx.read_catalog(&mut cache);
        let attempted = match s.ctx.begin_read(&slot) {
            Some(_pass) => try_run_read(&stmt, shared, s.bp.clone(), s.txn.clone(), sess),
            None => None,
        };
        match attempted {
            Some(read) => read,
            None => {
                let mut cat = s.ctx.catalog();
                let o = run(stmt, &mut cat, s.bp.clone(), s.txn.clone(), sess);
                drop(cat);
                o
            }
        }
    };
    match outcome {
        Ok(Outcome::Rows(r)) => Ok(r.len()),
        Ok(Outcome::Table(t)) => Ok(t.rows.len()),
        Ok(_) => Ok(0),
        Err(e) => Err(format!("{sql} failed: {e}")),
    }
}

/// The shipped wiring, as `d55_agent_read_scaling::build` gives it and for the reasons recorded
/// there (D101): `with_storage` over a real `ArenaPageStore` with persistence armed, and sessions
/// from `ServerContext::session`. A `Session::new()` would bring its own in-memory runtime.
fn build(dir: &std::path::Path, name: &str, rows: u64) -> Result<Server, String> {
    let d = dir.join(name);
    std::fs::create_dir_all(&d).map_err(|e| e.to_string())?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(d.join("main.db"))
        .map_err(|e| e.to_string())?;
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).map_err(|e| e.to_string())?;
    let wal = Arc::new(WalManager::new(d.join("main.wal")).map_err(|e| e.to_string())?);
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&d.join("b.branchcat"), 1).unwrap());
    let branches: Arc<dyn BranchCatalog> = cat;
    let arena_base: u32 = ((rows / 40) as u32 + 4096).next_power_of_two();
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), arena_base).unwrap());
    store.checkpoint_to(d.join("main.arena"));
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .map_err(|e| format!("attach arena storage: {e}"))?,
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
    let s = Server { ctx, bp, txn };
    let mut main = s.ctx.session();
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut main)?;
    for i in 1..=rows {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, 0);"), &mut main)?;
    }
    Ok(s)
}

/// One read on `sess`, REPS times: `(hops, sequentially scanned tuples, median µs)`. Hops and tuples
/// must be the same on every repetition, because control flow fixes them. A difference means
/// something other than the read moved the counters, and the run is refused.
fn measure(s: &Server, sess: &mut Session, sql: &str, rows: usize) -> Result<(u64, u64, f64), String> {
    let mut counts: Option<(u64, u64)> = None;
    let mut us: Vec<f64> = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let hops0 = VISIBILITY_HOPS.load(Ordering::Relaxed);
        let (_, tuples0) = seq_scan_counters();
        let t0 = Instant::now();
        let got = exec(s, sql, sess)?;
        let elapsed = t0.elapsed();
        let hops = VISIBILITY_HOPS.load(Ordering::Relaxed) - hops0;
        let tuples = seq_scan_counters().1 - tuples0;
        if got != rows {
            return Err(format!("{sql} returned {got} rows, want {rows}"));
        }
        match counts {
            None => counts = Some((hops, tuples)),
            Some(c) if c != (hops, tuples) => {
                return Err(format!("{sql}: (hops, tuples) moved between repetitions: {c:?} then {:?}", (hops, tuples)))
            }
            Some(_) => {}
        }
        us.push(elapsed.as_secs_f64() * 1e6);
    }
    us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let (hops, tuples) = counts.ok_or("REPS is 0: nothing was measured")?;
    Ok((hops, tuples, us[REPS / 2]))
}

fn one_merge(s: &Server, i: u64) -> Result<(), String> {
    let mut w = s.ctx.session();
    exec(s, &format!("BEGIN AGENT SESSION AS 'w' RUN 'w{i}';"), &mut w)?;
    exec(s, "UPDATE t SET v = v + 1 WHERE id >= 1;", &mut w)?;
    exec(s, "MERGE;", &mut w)?;
    Ok(())
}

fn part1(dir: &std::path::Path) -> Result<(), String> {
    let s = build(dir, "part1", ROWS)?;
    let mut old = s.ctx.session();
    exec(&s, "BEGIN AGENT SESSION AS 'old' RUN 'old';", &mut old)?;
    let scan = "SELECT id, v FROM t;";
    let point = "SELECT id, v FROM t WHERE id = 1;";
    println!("# part 1: rows={ROWS} reps={REPS} (latency = median of reps, reported not certified)");
    println!("k\told_scan_hops\told_scan_us\tfresh_scan_hops\tfresh_scan_us\told_point_hops\told_point_path\told_point_us\tfresh_point_hops\tfresh_point_us");
    let mut merged = 0u64;
    for &k in &CHECKPOINTS {
        while merged < k {
            one_merge(&s, merged)?;
            merged += 1;
        }
        let (os_h, _, os_us) = measure(&s, &mut old, scan, ROWS as usize)?;
        let (op_h, op_t, op_us) = measure(&s, &mut old, point, 1)?;
        let mut fresh = s.ctx.session();
        exec(&s, &format!("BEGIN AGENT SESSION AS 'fresh' RUN 'fresh{k}';"), &mut fresh)?;
        let (fs_h, _, fs_us) = measure(&s, &mut fresh, scan, ROWS as usize)?;
        let (fp_h, _, fp_us) = measure(&s, &mut fresh, point, 1)?;
        exec(&s, "ABANDON;", &mut fresh)?;

        if os_h != ROWS * k {
            return Err(format!("k={k}: OLD full scan made {os_h} hops, pre-registered {}", ROWS * k));
        }
        if fs_h != 0 || fp_h != 0 {
            return Err(format!("k={k}: FRESH reads made {fs_h} (scan) and {fp_h} (point) hops, pre-registered 0"));
        }
        let path = if op_t == 0 { "index" } else { "seqscan" };
        let want_point = if op_t == 0 { k } else { ROWS * k };
        if op_h != want_point {
            return Err(format!("k={k}: OLD point read made {op_h} hops on the {path} path, pre-registered {want_point}"));
        }
        println!("{k}\t{os_h}\t{os_us:.1}\t{fs_h}\t{fs_us:.1}\t{op_h}\t{path}\t{op_us:.1}\t{fp_h}\t{fp_us:.1}");
    }
    Ok(())
}

fn part2(dir: &std::path::Path) -> Result<(), String> {
    let s = build(dir, "part2", 1)?;
    let rt = s.ctx.runtime.clone();
    let mut holders: Vec<Session> = (0..OPEN_TXNS).map(|_| s.ctx.session()).collect();
    for h in &mut holders {
        exec(&s, "BEGIN;", h)?;
    }
    println!("# part 2: open_txns={OPEN_TXNS} forks={FORKS}; census = (pinned live branches, distinct snapshots, active ids retained)");

    let mut forks: Vec<Session> = Vec::with_capacity(FORKS);
    for i in 0..FORKS {
        let mut f = s.ctx.session();
        exec(&s, &format!("BEGIN AGENT SESSION AS 'c' RUN 'shared{i}';"), &mut f)?;
        forks.push(f);
    }
    let shared = rt.fork_snapshot_census();
    println!("SHARED\t{shared:?}");
    if shared != (FORKS, 1, OPEN_TXNS) {
        return Err(format!("SHARED census {shared:?}, pre-registered {:?}", (FORKS, 1, OPEN_TXNS)));
    }
    for f in &mut forks {
        exec(&s, "ABANDON;", f)?;
    }
    forks.clear();
    let empty = rt.fork_snapshot_census();
    if empty != (0, 0, 0) {
        return Err(format!("census after abandoning every fork is {empty:?}, want (0, 0, 0)"));
    }

    let mut main = s.ctx.session();
    for i in 0..FORKS {
        exec(&s, &format!("INSERT INTO t VALUES ({}, 0);", 1000 + i), &mut main)?;
        let mut f = s.ctx.session();
        exec(&s, &format!("BEGIN AGENT SESSION AS 'c' RUN 'churn{i}';"), &mut f)?;
        forks.push(f);
    }
    let churn = rt.fork_snapshot_census();
    println!("CHURN\t{churn:?}");
    if churn != (FORKS, FORKS, FORKS * OPEN_TXNS) {
        return Err(format!("CHURN census {churn:?}, pre-registered {:?}", (FORKS, FORKS, FORKS * OPEN_TXNS)));
    }
    for f in &mut forks {
        exec(&s, "ABANDON;", f)?;
    }
    for h in &mut holders {
        exec(&s, "COMMIT;", h)?;
    }
    Ok(())
}

fn main() {
    let dir = tempfile::tempdir().expect("temp dir");
    println!("# D194-PINNED-READ-COST  pid={}", std::process::id());
    let result = part1(dir.path()).and_then(|_| part2(dir.path()));
    match result {
        Ok(()) => println!("# OK: every pre-registered integer held"),
        Err(e) => {
            eprintln!("REFUSED: {e}");
            // Destructors do not run under `exit`, so the temp dir is removed first.
            drop(dir);
            std::process::exit(1);
        }
    }
}
