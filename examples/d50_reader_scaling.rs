//! D50-FERRODB-SNAPSHOT-COST — does ferrodb's read path scale with concurrent readers?
//!
//! The question D48/D49 answered for turso, asked of ferrodb, **in process**.
//!
//! # Why in process, and not over the wire
//!
//! The first three attempts drove the real `pgserver` with a Python wire client, threaded and
//! then multi-process. All three plateaued near 20,000 statements/s in total, and — the fact that
//! refuses them — giving every client its **own server**, and so its own `Mutex<Catalog>`, moved
//! nothing at all. A harness that does not separate when the suspected wall is *removed* has not
//! reached that wall and cannot see it in either direction. Those runs are committed as VOID
//! (`bench/d50_threaded_VOID_harness_ceiling.txt`, `bench/d50_procs_VOID_no_barrier.txt`).
//!
//! This harness deletes the client. Threads call `executor::run` through
//! `ServerContext::catalog()` — the same `MutexGuard<Catalog>` a connection thread takes, the same
//! outermost lock, the same parse-per-statement — with no socket and no Python in the loop.
//!
//! # What is already settled without measuring, and what is not
//!
//! * `src/execution/executor.rs:60` — `pub fn run(stmt: Stmt, catalog: &mut Catalog, ...)`
//! * `src/pgwire/mod.rs:64` — `pub catalog: Mutex<Catalog>`
//! * `src/pgwire/mod.rs:100` — the mutex "is taken **outermost**, for the duration of a single
//!   statement, and released before the next message is read"
//!
//! So ferrodb executes one statement at a time, globally, and the `&mut` is what forces it. That
//! is a type-level and documented fact. **The CURVE it predicts is still a prediction** — and one
//! hour before this file was written, a careful read of turso's source predicted its hit path
//! would not degrade, and the run said otherwise. Predictions from source reading are exactly what
//! got falsified today, which is why this exists.
//!
//! # The arms
//!
//! * **A SHARED** — N threads, ONE `ServerContext`. The real server configuration.
//! * **B PRIVATE (the CONTROL)** — N threads, N `ServerContext`s on N database files. Removes the
//!   shared `Mutex<Catalog>` and changes nothing else. Without it arm A's slope means nothing.
//!
//! Order is rotated per round as a Latin square and medians are taken per point: interleaving
//! cancels drift across a run, but only rotation cancels a within-round position bias, which has
//! constant sign. Guards **refuse**: zero statements, a thread that did nothing, or a SELECT that
//! returned a row count other than one.

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const ROWS: i64 = 200;
const WARMUP: Duration = Duration::from_millis(300);
const MEASURE: Duration = Duration::from_millis(1000);
const ROUNDS: usize = 3;
const POINTS: [usize; 5] = [1, 2, 4, 8, 16];

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
}

fn build(dir: &std::path::Path, n: usize) -> Server {
    let d = dir.join(format!("s{n}"));
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
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut Session::new());
    let mut sess = Session::new();
    for i in 1..=ROWS {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut sess);
    }
    s
}

/// One statement through the server's own path: parse, then `run` under the catalog mutex.
fn exec(s: &Server, sql: &str, sess: &mut Session) -> usize {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    assert!(parser.errors.is_empty(), "parse failed for {sql}: {:?}", parser.errors);
    let stmt = stmts.remove(0);
    // THE LOCK UNDER TEST. `ServerContext::catalog()` is what a connection thread calls, and the
    // guard derefs to the `&mut Catalog` that `run` demands. Taken outermost, held for the whole
    // statement, exactly as src/pgwire/mod.rs documents.
    let mut cat = s.ctx.catalog();
    match run(stmt, &mut cat, s.bp.clone(), s.txn.clone(), sess) {
        Ok(Outcome::Rows(r)) => r.len(),
        Ok(Outcome::Table(t)) => t.rows.len(),
        Ok(_) => 0,
        Err(e) => panic!("{sql} failed: {e}"),
    }
}

fn sweep_point(servers: &[Arc<Server>], shared: bool, threads: usize) -> (f64, u64) {
    let start = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let rows = Arc::new(AtomicU64::new(0));
    let slowest = Arc::new(AtomicU64::new(u64::MAX));

    let mut handles = Vec::new();
    for t in 0..threads {
        let srv = if shared { servers[0].clone() } else { servers[t].clone() };
        let (start, stop) = (start.clone(), stop.clone());
        let (total, rows, slowest) = (total.clone(), rows.clone(), slowest.clone());
        let key = (t as i64 * 97) % ROWS + 1;
        handles.push(std::thread::spawn(move || {
            let sql = format!("SELECT v FROM t WHERE id = {key};");
            let mut sess = Session::new();
            let t0 = Instant::now();
            while t0.elapsed() < WARMUP {
                exec(&srv, &sql, &mut sess);
            }
            start.wait();
            let (mut n, mut r) = (0u64, 0u64);
            while !stop.load(Ordering::Relaxed) {
                r += exec(&srv, &sql, &mut sess) as u64;
                n += 1;
            }
            total.fetch_add(n, Ordering::Relaxed);
            rows.fetch_add(r, Ordering::Relaxed);
            slowest.fetch_min(n, Ordering::Relaxed);
        }));
    }

    start.wait();
    let t0 = Instant::now();
    std::thread::sleep(MEASURE);
    stop.store(true, Ordering::Relaxed);
    let elapsed = t0.elapsed();
    for h in handles {
        h.join().unwrap();
    }

    let n = total.load(Ordering::Relaxed);
    let r = rows.load(Ordering::Relaxed);
    let slow = slowest.load(Ordering::Relaxed);
    if n == 0 || slow == 0 {
        eprintln!("GUARD: {threads} threads produced {n} statements (slowest thread {slow})");
        std::process::exit(2);
    }
    if r != n {
        eprintln!("GUARD: {n} statements returned {r} rows; each must return exactly one");
        std::process::exit(2);
    }
    (n as f64 / elapsed.as_secs_f64(), slow)
}

fn run_arm(servers: &[Arc<Server>], shared: bool, label: &str) -> Vec<f64> {
    println!();
    println!("=== ARM {label} ===");
    println!("# order rotated per round; a within-round position bias has constant sign");
    let mut samples: Vec<Vec<f64>> = vec![Vec::new(); POINTS.len()];
    for r in 0..ROUNDS {
        let order: Vec<usize> = (0..POINTS.len()).map(|i| (i + r) % POINTS.len()).collect();
        let shown: Vec<String> = order.iter().map(|i| POINTS[*i].to_string()).collect();
        println!("# round {r} order: {}", shown.join(" "));
        for &i in &order {
            let (ops, slow) = sweep_point(servers, shared, POINTS[i]);
            samples[i].push(ops);
            println!("#   {}T -> {ops:.0} stmt/s (slowest thread {slow})", POINTS[i]);
        }
    }
    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let meds: Vec<f64> = (0..POINTS.len()).map(|i| med(&mut samples[i])).collect();
    println!();
    println!("threads   total_stmt_s   per_thread   total_vs_1T   per_thread_vs_1T");
    for (i, &t) in POINTS.iter().enumerate() {
        println!(
            "{t:>7}   {:>12.0}   {:>10.0}   {:>11.3}   {:>16.3}",
            meds[i],
            meds[i] / t as f64,
            meds[i] / meds[0],
            (meds[i] / t as f64) / meds[0]
        );
    }
    meds
}

fn main() {
    let dir = std::env::temp_dir().join(format!("ferrodb-d50-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let maxt = *POINTS.iter().max().unwrap();
    println!("# D50-FERRODB-SNAPSHOT-COST  rows={ROWS} warmup={WARMUP:?} measure={MEASURE:?} rounds={ROUNDS}");
    println!("# building {maxt} independent servers (arm A uses only the first)");
    let servers: Vec<Arc<Server>> = (0..maxt).map(|n| Arc::new(build(&dir, n))).collect();

    // ARM ORDER IS ITSELF ROTATED. The first run of this harness measured the SAME
    // configuration -- one thread on servers[0] -- at 52187 stmt/s as arm A's baseline and
    // 23341 as arm B's, a 2.2x gap for identical work. Rotating the POINTS within a round
    // cannot cancel a bias that sits between the ARMS, and a bias with constant sign is not
    // averaged away by repeating it. D50_ARM_ORDER=BA runs the control first.
    let ba = std::env::var("D50_ARM_ORDER").map(|v| v == "BA").unwrap_or(false);
    let (a, b) = if ba {
        let b = run_arm(&servers, false, "PRIVATE (N threads, N ServerContexts) -- the CONTROL");
        let a = run_arm(&servers, true, "SHARED  (N threads, ONE ServerContext -- one Mutex<Catalog>)");
        (a, b)
    } else {
        let a = run_arm(&servers, true, "SHARED  (N threads, ONE ServerContext -- one Mutex<Catalog>)");
        let b = run_arm(&servers, false, "PRIVATE (N threads, N ServerContexts) -- the CONTROL");
        (a, b)
    };
    println!();
    println!("# arm order this run: {}", if ba { "B then A" } else { "A then B" });

    println!();
    println!("threads   shared_vs_1T   private_vs_1T       apart");
    for (i, &t) in POINTS.iter().enumerate() {
        println!("{t:>7}   {:>12.3}   {:>13.3}   {:>9.2}x", a[i] / a[0], b[i] / b[0], b[i] / a[i]);
    }
    let _ = std::fs::remove_dir_all(&dir);
}
