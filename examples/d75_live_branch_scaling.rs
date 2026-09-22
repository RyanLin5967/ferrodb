//! D75 — **does per-statement cost grow with the number of LIVE branches?**
//!
//! # Why this experiment and not another one
//!
//! BranchBench's finding is about *"concurrent branch management"* at scale. Everything this
//! project has measured at 10^6 is about branches **existing**: D61 (10^6 write-bearing branches at
//! exactly 4096 B/branch) and D65 (reopen flat to 10^6, 0.263 -> 0.264 ms). Neither asks what a
//! statement costs **while** that many branches are live.
//!
//! D67 varied THREADS, which at one branch per thread tops out at 16 concurrent branches — three
//! orders of magnitude short of the regime the finding is about.
//!
//! And D74 established that the closest prior art does not answer it either: ForkBase asserts "no
//! restrictions on the number of branches per key" (§3.3) and **runs no branch-count experiment at
//! all**. So this is the measurement nobody has, which is the only reason it is worth running.
//!
//! # The shape
//!
//! Hold the WORKLOAD fixed and vary the number of LIVE branches underneath it. A few threads do
//! ordinary fork -> write -> merge cycles while L other branches sit live and untouched. If a
//! statement's cost is independent of L, branch management here is concurrent in the sense the
//! benchmark means. If it grows with L, something iterates the live set per statement and that is
//! the wall.
//!
//! **PRE-REGISTERED, before the first run:**
//!   * `ms per cycle` FLAT in L            -> concurrent branch management scales; report that.
//!   * `ms per cycle` LINEAR in L          -> the wall, and the next job is to name what iterates.
//!   * `ms per cycle` growing then FLAT    -> a cache/working-set effect, not a complexity one;
//!                                            re-run with the arena base moved before concluding.
//!
//! ⚠ **The background branches must be LIVE, not reaped.** A reaped branch keeps its record, so a
//! sweep that walks `records` would show the same slope either way and the result would say
//! nothing about *live* branch management. `D75_VERIFY=1` asserts the live count matches L before
//! measuring, and the run refuses rather than reporting a number it cannot attribute.
//!
//! ⚠ Absolutes are this box under whatever else it is running. The SHAPE across L is the result.
//!
//! Usage: `D75_LIVE=1000,4000,16000,64000 D75_THREADS=4 d75_live_branch_scaling`
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

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
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const ROWS: i64 = 2000;
const WRITES_PER_BRANCH: usize = 4;
const ARENA_BASE: u32 = 1024;

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    cat: Arc<TableBranchCatalog>,
    arena_path: std::path::PathBuf,
}

/// A session wired to the SHARED runtime, the way `pgwire/mod.rs:358` and `cli/cli.rs:163` build
/// one.
///
/// ⛔ **`Session::new()` IS THE TRAP, AND D67 FELL IN IT.** `Session::new` sets
/// `runtime: Arc::new(AgentRuntime::new())` (`execution/session.rs:20`) — a BRAND-NEW, private,
/// in-memory runtime with no branch catalog, no arena store and no reaper. `run()` reaches the
/// runtime through `session.runtime` and nowhere else, so a harness that builds a `ServerContext`
/// and then calls `Session::new()` never consults the engine it just wired up. Every "agent" gets
/// its own empty database, and a contention benchmark measures N independent single-threaded
/// engines sharing nothing.
///
/// Proved before this was written, not guessed: with `Session::new()`, `BEGIN AGENT SESSION`
/// followed by a write left `cat.scan()` at ONE record (trunk) while `MERGE` still reported
/// `applied_to_target=true`.
fn session(s: &Server) -> Session {
    Session::with_runtime(Arc::clone(&s.ctx.runtime))
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
    run(stmt, &mut cat, s.bp.clone(), s.txn.clone(), sess).map_err(|e| e.to_string())
}

/// The same wiring `cli.rs` uses. `with_storage`, never `with_catalog` — D67's withdrawn first
/// configuration set `storage: None, reaper: None`, so the branch storage engine was absent from a
/// benchmark reporting on branch management.
fn build(dir: &std::path::Path) -> Server {
    std::fs::create_dir_all(dir).unwrap();
    let file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(dir.join("main.db")).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.join("main.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&dir.join("b.branchcat"), 1).unwrap());
    let branches: Arc<dyn BranchCatalog> = cat.clone();
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), ARENA_BASE).unwrap());
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    // **D79: the free-space map is persisted ONLY if `checkpoint_to` is called.**
    //
    // `cli.rs:120` calls it, so the shipped binary pays it. `branch_curve_writes.rs` (D61's 10^6
    // harness), `branch_scaling_bench.rs` and the first version of THIS harness do not — so every
    // 10^6 result this project has published measured a configuration production does not use.
    // `D75_PERSIST=1` turns it on so the two can be compared on one axis.
    let arena_path = dir.join("main.db.arena");
    if std::env::var("D75_PERSIST").map(|v| v == "1").unwrap_or(false) {
        store.checkpoint_to(arena_path.clone());
    }
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
    let s = Server { ctx, bp, txn, cat, arena_path };
    let mut sess = session(&s);
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    for i in 1..=ROWS {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut sess).unwrap();
    }
    s
}

/// Park `n` branches in the LIVE state and leave them there.
///
/// They are opened with `BEGIN AGENT SESSION` and never merged and never ended, which is exactly
/// the state an abandoned agent leaves behind and exactly what "concurrent branches" means here.
fn park_live_branches(s: &Server, n: usize) {
    for i in 0..n {
        let mut sess = session(s);
        if exec(s, &format!("BEGIN AGENT SESSION AS 'bg{i}';"), &mut sess).is_err() {
            panic!("could not park background branch {i}");
        }
        // One write so the branch is WRITE-BEARING, not an empty fork. An empty branch may never
        // allocate an arena, and a scaling result over branches that own nothing would be a result
        // about the catalog alone.
        let id = 1 + (i as i64 % ROWS);
        if exec(s, &format!("UPDATE t SET v = {i} WHERE id = {id};"), &mut sess).is_err() {
            panic!("could not write in background branch {i}");
        }
        // deliberately NO merge and NO end: the branch stays live.
    }
}

/// One measured agent lifetime against the loaded database.
fn one_cycle(s: &Server, tid: usize, seq: u64) -> bool {
    let mut sess = session(s);
    if exec(s, &format!("BEGIN AGENT SESSION AS 'm{tid}_{seq}';"), &mut sess).is_err() {
        return false;
    }
    for w in 0..WRITES_PER_BRANCH {
        let id = 1 + (tid as i64 * 97 + w as i64) % ROWS;
        let v = (seq % 1000) as i64;
        if exec(s, &format!("UPDATE t SET v = {v} WHERE id = {id};"), &mut sess).is_err() {
            return false;
        }
    }
    matches!(
        exec(s, "MERGE;", &mut sess),
        Ok(Outcome::Agent(AgentOutput::Merge(r))) if r.applied_to_target
    )
}

fn main() {
    let lives: Vec<usize> = std::env::var("D75_LIVE")
        .unwrap_or_else(|_| "500,1000,2000,4000,8000".to_string())
        .split(',').filter_map(|v| v.parse().ok()).collect();
    let threads: usize = std::env::var("D75_THREADS").ok().and_then(|v| v.parse().ok()).unwrap_or(4);
    let cycles: usize = std::env::var("D75_CYCLES").ok().and_then(|v| v.parse().ok()).unwrap_or(30);

    println!("D75 — per-statement cost against the number of LIVE branches.");
    println!("{}", ferrodb::build_provenance());
    println!("{threads} threads doing fork/write/merge cycles; L other branches sit LIVE underneath.");
    println!("PRE-REGISTERED: flat in L -> concurrent branch management scales. Linear -> the wall.");
    println!();
    println!("  live L    live_count   median ms/cycle   ms per 1000 live   merges   map bytes   replaces   appends   fsyncs   durable KB");
    println!("  (the four rightmost columns are PER PHASE — snapshotted around each L — not cumulative.)");
    println!("  persistence: {}", if std::env::var("D75_PERSIST").map(|v| v=="1").unwrap_or(false) { "ON (as cli.rs:120 does)" } else { "OFF (as every 10^6 harness here does)" });
    let mut first: Option<(usize, f64)> = None;
    for &l in &lives {
        let dir = std::env::temp_dir().join(format!("ferrodb-d75-{}-{}", std::process::id(), l));
        let _ = std::fs::remove_dir_all(&dir);
        // **D81: snapshot the durability counters around the WHOLE phase**, so each row reports
        // what that L cost rather than what every L before it also cost. The earlier
        // `bench/d81_bytes_vs_fsyncs.txt` printed these cumulatively and subtracted by hand.
        let (reps0, rbytes0) = ferrodb::storage::atomic_file::atomic_replace_counters();
        let (apps0, abytes0) = ferrodb::storage::atomic_file::durable_append_counters();
        let s = build(&dir);
        park_live_branches(&s, l);

        // ⚠ Refuse rather than report a number that cannot be attributed. If the parked branches
        // are not actually live, the whole axis is meaningless and a printed row would hide that.
        let live = s.cat.live_count().unwrap_or(0);
        if live < l {
            println!("  {l:>7}   live_count={live} < L — branches did not stay LIVE. NOT A RESULT.");
            let _ = std::fs::remove_dir_all(&dir);
            continue;
        }

        let s = Arc::new(s);
        let stop = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(threads + 1));
        let mut samples_all: Vec<Vec<f64>> = Vec::new();
        let mut handles = Vec::new();
        for tid in 0..threads {
            let (s, stop, done, barrier) =
                (Arc::clone(&s), Arc::clone(&stop), Arc::clone(&done), Arc::clone(&barrier));
            handles.push(std::thread::spawn(move || {
                let mut mine = Vec::new();
                barrier.wait();
                let mut seq = 0u64;
                while !stop.load(Ordering::Relaxed) && mine.len() < cycles {
                    let t = Instant::now();
                    let ok = one_cycle(&s, tid, seq);
                    let ms = t.elapsed().as_secs_f64() * 1000.0;
                    seq += 1;
                    if ok {
                        mine.push(ms);
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                }
                mine
            }));
        }
        barrier.wait();
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline && done.load(Ordering::Relaxed) < (threads * cycles) as u64 {
            std::thread::sleep(Duration::from_millis(20));
        }
        stop.store(true, Ordering::Relaxed);
        for h in handles {
            samples_all.push(h.join().unwrap());
        }
        let mut flat: Vec<f64> = samples_all.into_iter().flatten().collect();
        if flat.is_empty() {
            println!("  {l:>7}   {live:>10}   NO CYCLE COMPLETED — not a result, and not a zero");
            let _ = std::fs::remove_dir_all(&dir);
            continue;
        }
        flat.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = flat[flat.len() / 2];
        let map_bytes = std::fs::metadata(&s.arena_path).map(|m| m.len()).unwrap_or(0);
        // **D81 falsifier, and then D81's fix measured against it.** Separate the CONSTANT from the
        // GROWING part of the checkpoint cost. Each `replace_atomically` is two fsyncs (file +
        // dir) whatever the image size; each `append_durably` is ONE, over one record. `fsyncs` is
        // therefore the column the falsifier said the penalty tracks, and `durable KB` the one it
        // said it does not — printing both is what lets the after-run be read against the before.
        let (reps1, rbytes1) = ferrodb::storage::atomic_file::atomic_replace_counters();
        let (apps1, abytes1) = ferrodb::storage::atomic_file::durable_append_counters();
        let (reps, rbytes) = (reps1 - reps0, rbytes1 - rbytes0);
        let (apps, abytes) = (apps1 - apps0, abytes1 - abytes0);
        println!("  {l:>7}   {live:>10}   {med:>15.3}   {:>16.4}   {:>6}   {map_bytes:>10}   {reps:>9}   {apps:>7}   {:>6}   {:>10}",
                 med / (l as f64 / 1000.0), flat.len(),
                 2 * reps + apps, (rbytes + abytes) / 1024);
        if first.is_none() { first = Some((l, med)); }
        if let Some((l0, m0)) = first {
            if l != l0 {
                println!("           ^ {:.1}x the live branches, {:.2}x the cycle",
                         l as f64 / l0 as f64, med / m0);
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
    println!();
    println!("FLAT median ms/cycle -> cost is independent of how many branches are live.");
    println!("A flat 'ms per 1000 live' column is the signature of LINEAR in L.");
}
