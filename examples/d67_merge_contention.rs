//! D67 — **does CONCURRENT BRANCH MANAGEMENT scale?** Merges per second against thread count.
//!
//! # Why this run exists
//!
//! BranchBench's finding is about *"concurrent branch management"*, and every scaling number this
//! project has is about something else. D55–D59 optimised the READ path and reached x8.12 at 16
//! threads against a x10.5 private-context ceiling. D61 measured per-branch SPACE to 10^6. D65
//! measured REOPEN to 10^6. **Nothing here has ever measured the write side under concurrency**,
//! and the write side is where the objective's own wording points.
//!
//! # The prediction, recorded before the first number exists
//!
//! `src/pgwire/mod.rs:64` is `pub catalog: Mutex<Catalog>`. Reads were taught to avoid it — that is
//! what the epoch mirror and `try_run_read` are for — but **everything needing `&mut Catalog` takes
//! it**, and `ServerContext::catalog()` additionally DRAINS THE READERS on every acquisition. A
//! `MERGE` needs `&mut Catalog`. So the prediction is:
//!
//!   **merges/sec is FLAT from 1 to 16 threads, because every merge in the system serialises on one
//!   global mutex.** If that holds, "concurrent branch management" is concurrent in name only, and
//!   the wall is structural rather than a constant.
//!
//! # The control, and why it is the whole experiment
//!
//! Two arms that differ ONLY in whether the agents' writes collide:
//!
//! * `DISJOINT` — thread *i* writes its own private id range. No two branches touch the same row,
//!   so nothing a conflict detector could legitimately serialise on.
//! * `SHARED` — every thread writes the SAME ids. Real conflicts, which a merge gate must resolve.
//!
//! Reading the pair is the point, and each outcome says something different:
//!
//! | DISJOINT | SHARED | what it means |
//! |---|---|---|
//! | flat | flat | the wall is the GLOBAL LOCK. Conflict detection is irrelevant; the mechanism has to change |
//! | scales | flat | the wall is CONFLICT DETECTION, and a disjoint workload already works |
//! | flat | scales | the harness is wrong — this ordering is impossible and would mean the measurement is measuring itself |
//! | scales | scales | the prediction is WRONG, merges already scale, and this row closes |
//!
//! ⚠ A flat DISJOINT arm is NOT by itself proof of a lock: a single-threaded bottleneck anywhere —
//! the WAL, the buffer pool, this harness — looks identical from the outside. It localises the wall
//! to "something shared and serial", and naming WHICH shared thing is the next run, not this one.
//!
//! # What is deliberately NOT claimed
//!
//! merges/sec is a FLOOR on a shared machine, exactly as in D61 and D65 — a loaded box makes it
//! smaller, never larger. The number this row turns on is the SHAPE of the column against thread
//! count, which load does not invert.
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::{run, try_run_read, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const ROWS: i64 = 2000;
const WRITES_PER_BRANCH: usize = 4;
const WARMUP: Duration = Duration::from_millis(400);
const MEASURE: Duration = Duration::from_millis(2000);
const POINTS: [usize; 5] = [1, 2, 4, 8, 16];

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
}

/// Run one statement the way the server does: take the catalog, run, drop.
///
/// Deliberately NOT using the read fast path. Every statement in this benchmark either writes or
/// merges, so routing through `try_run_read` would only add a branch that always falls through —
/// and hiding the acquisition being measured inside a helper that sometimes avoids it is how a
/// harness ends up measuring itself.
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
    let runtime = Arc::new(AgentRuntime::with_catalog(cat as Arc<dyn BranchCatalog>));
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
    let s = Server { ctx, bp, txn };

    let mut sess = Session::new();
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    for i in 1..=ROWS {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut sess).unwrap();
    }
    s
}

/// One agent's whole life: fork, write, merge. Returns true if the cycle completed.
fn one_cycle(s: &Server, tid: usize, seq: u64, disjoint: bool) -> bool {
    let mut sess = Session::new();
    if exec(s, &format!("BEGIN AGENT SESSION AS 'a{tid}';"), &mut sess).is_err() {
        return false;
    }
    for w in 0..WRITES_PER_BRANCH {
        // DISJOINT: a private slice of the key space per thread. SHARED: everyone on the same ids.
        let id = if disjoint {
            1 + (tid as i64 * 97 + w as i64) % ROWS
        } else {
            1 + (w as i64) % 16
        };
        let v = (seq % 1000) as i64;
        if exec(s, &format!("UPDATE t SET v = {v} WHERE id = {id};"), &mut sess).is_err() {
            return false;
        }
    }
    // A merge that is REFUSED or quarantined still exercised the whole path, which is what is being
    // timed. Counting only applied merges would make the SHARED arm look faster the more it failed.
    exec(s, "MERGE;", &mut sess).is_ok()
}

/// A reader connection, exactly as the server has them: a REGISTERED slot plus the lock-free read
/// path (`read_catalog` + `begin_read` + `try_run_read`) that D58/D59 built.
///
/// This exists to close a gap the first D67 run could not see. `ServerContext::catalog()` calls
/// `drain_readers()`, which stores SeqCst to a shared `writer_active` word and takes a SECOND
/// global mutex (`self.readers`) on EVERY acquisition — and then SPINS until every registered
/// reader is idle. With no readers registered that is all free, which is why the first run's
/// blocked stacks never showed it. With readers registered it is a shared-word write on the hot
/// path, and D51 measured that exact shape at x0.121 against a relaxed load's x7.823.
fn reader_thread(s: Arc<Server>, stop: Arc<AtomicBool>, reads: Arc<AtomicU64>) {
    let slot = Arc::new(AtomicBool::new(false));
    s.ctx.register_reader(Arc::clone(&slot));
    let mut cache: Option<(u64, Arc<Catalog>)> = None;
    let mut sess = Session::new();
    let sql = "SELECT v FROM t WHERE id = 7;";
    while !stop.load(Ordering::Relaxed) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return;
        }
        let stmt = stmts.remove(0);
        let shared = s.ctx.read_catalog(&mut cache);
        let attempted = match s.ctx.begin_read(&slot) {
            Some(_pass) => try_run_read(&stmt, shared, s.bp.clone(), s.txn.clone(), &mut sess),
            None => None,
        };
        if attempted.is_some() {
            reads.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn sweep(dir: &std::path::Path, disjoint: bool) -> Vec<(usize, f64)> {
    let arm = if disjoint { "disjoint" } else { "shared" };
    let mut out = Vec::new();
    for &threads in POINTS.iter() {
        let s = Arc::new(build(dir, &format!("{arm}_{threads}")));
        let start = Arc::new(Barrier::new(threads + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicU64::new(0));

        let mut hs = Vec::new();
        for tid in 0..threads {
            let (s, start, stop, done) = (s.clone(), start.clone(), stop.clone(), done.clone());
            hs.push(std::thread::spawn(move || {
                let mut seq = 0u64;
                start.wait();
                while !stop.load(Ordering::Relaxed) {
                    if one_cycle(&s, tid, seq, disjoint) {
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                    seq += 1;
                }
            }));
        }
        start.wait();
        std::thread::sleep(WARMUP);
        done.store(0, Ordering::Relaxed);
        let t0 = Instant::now();
        std::thread::sleep(MEASURE);
        let n = done.load(Ordering::Relaxed);
        let secs = t0.elapsed().as_secs_f64();
        stop.store(true, Ordering::Relaxed);
        for h in hs {
            let _ = h.join();
        }
        let rate = n as f64 / secs;
        println!("  {arm:<9} {threads:>3}T -> {rate:>9.1} merges/sec  ({n} in {secs:.2}s)");
        out.push((threads, rate));
    }
    out
}

fn main() {
    let dir = std::env::temp_dir().join(format!("ferrodb-d67-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // D67_POINT=16 D67_SECONDS=30 runs ONE point for a long time, so a sampling profiler has a
    // steady state to look at. The sweep's 2-second windows are too short to profile and the
    // build/teardown between points would dominate the sample.
    if let Ok(n) = std::env::var("D67_POINT") {
        let threads: usize = n.parse().expect("D67_POINT must be a number");
        let secs: u64 = std::env::var("D67_SECONDS").ok().and_then(|v| v.parse().ok()).unwrap_or(30);
        let disjoint = std::env::var("D67_ARM").map(|a| a != "shared").unwrap_or(true);
        println!("SINGLE POINT: {threads} threads, {secs}s, arm={}", if disjoint { "disjoint" } else { "shared" });
        let s = Arc::new(build(&dir, "profile"));
        let start = Arc::new(Barrier::new(threads + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicU64::new(0));
        let mut hs = Vec::new();
        for tid in 0..threads {
            let (s, start, stop, done) = (s.clone(), start.clone(), stop.clone(), done.clone());
            hs.push(std::thread::spawn(move || {
                let mut seq = 0u64;
                start.wait();
                while !stop.load(Ordering::Relaxed) {
                    if one_cycle(&s, tid, seq, disjoint) {
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                    seq += 1;
                }
            }));
        }
        // D67_READERS=N adds N REGISTERED reader connections. They are not counted in the merge
        // rate; they exist so `drain_readers` has something to drain.
        let nreaders: usize = std::env::var("D67_READERS").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
        let reads = Arc::new(AtomicU64::new(0));
        let mut rh = Vec::new();
        for _ in 0..nreaders {
            let (s, stop, reads) = (s.clone(), stop.clone(), reads.clone());
            rh.push(std::thread::spawn(move || reader_thread(s, stop, reads)));
        }
        start.wait();
        println!("pid {} running with {nreaders} registered readers — profile now", std::process::id());
        let t0 = Instant::now();
        std::thread::sleep(Duration::from_secs(secs));
        let n = done.load(Ordering::Relaxed);
        stop.store(true, Ordering::Relaxed);
        for h in hs { let _ = h.join(); }
        for h in rh { let _ = h.join(); }
        println!("{:.1} merges/sec ({} in {:.1}s), readers did {} reads",
                 n as f64 / t0.elapsed().as_secs_f64(), n, t0.elapsed().as_secs_f64(),
                 reads.load(Ordering::Relaxed));
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }

    println!("D67 — CONCURRENT BRANCH MANAGEMENT: merges/sec against thread count.");
    println!("PREDICTION, recorded before the numbers: FLAT in both arms, because every MERGE takes");
    println!("the one `Mutex<Catalog>` at pgwire/mod.rs:64 and drains readers on the way in.");
    println!("⚠ merges/sec is a FLOOR on a shared box; the SHAPE against thread count is the result.");
    println!();

    let disjoint = sweep(&dir, true);
    println!();
    let shared = sweep(&dir, false);

    println!();
    println!("threads   disjoint/sec   vs 1T      shared/sec   vs 1T");
    let d1 = disjoint[0].1.max(1e-9);
    let s1 = shared[0].1.max(1e-9);
    for i in 0..POINTS.len() {
        println!(
            "{:>7}   {:>12.1}   {:>5.2}x   {:>11.1}   {:>5.2}x",
            disjoint[i].0,
            disjoint[i].1,
            disjoint[i].1 / d1,
            shared[i].1,
            shared[i].1 / s1
        );
    }
    let dscale = disjoint[POINTS.len() - 1].1 / d1;
    println!();
    println!("VERDICT at 16T: disjoint x{:.2}, shared x{:.2}", dscale, shared[POINTS.len() - 1].1 / s1);
    if dscale < 1.5 {
        println!("FLAT. Concurrent branch management does not scale, and the disjoint arm rules out");
        println!("conflict detection as the cause: no two branches touched the same row. The wall is");
        println!("something SHARED AND SERIAL on the merge path. Which one is the next run.");
    } else {
        println!("IT SCALES — the prediction was WRONG. Record that before doing anything else.");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
