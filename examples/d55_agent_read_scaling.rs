//! D55 — does a BRANCH-PER-AGENT read scale, now that the catalog lock is off the read path?
//!
//! D54 removed the process-wide `Mutex<Catalog>` from reads and measured 46,897 -> 534,639
//! statements/s over 1 -> 16 readers. **That was a PLAIN `SELECT` with no agent session.**
//! BranchBench's finding is about *"concurrent branch management"*, so the number that answers it
//! is this one, and the number that does not is that one.
//!
//! # The prediction, recorded before the run so it can fail
//!
//! `AgentRuntime` holds one `Mutex<State>`. `visible_rows` does **not** hold it across the B+tree
//! scan — that part runs free — but it **does** hold it while walking every row staged on the
//! branch, filtering by table:
//!
//! ```ignore
//! let base = scan_table(table, ctx)?;              // no lock
//! if let Some(b) = branch {
//!     let state = self.state.lock().unwrap();      // taken here
//!     if let Some(ws) = state.workspaces.get(&b.id) {
//!         for ((t, row), st) in &ws.rows {         // O(rows staged on this branch)
//!             if *t != tbl.0 { continue; }
//! ```
//!
//! So the expectation is **contention × a linear walk**, and those are separable — which is why
//! this sweep has TWO axes. A sweep over threads alone, with each agent staging a handful of rows,
//! would measure only the contention term and would then "confirm" the prediction for the wrong
//! reason.
//!
//! * **threads** {1, 2, 4, 8, 16}
//! * **staged rows per branch** {10, 4000} — the upper figure is not invented: `persistent_map.rs`
//!   records D27 measuring a parent holding **4000 staged rows**.
//!
//! If reads stay flat across that 400× change in workspace size, the linear term does not matter
//! and the `PersistentMap::range` hypothesis in `SCALE-DESIGN` D55 is dead. That is a result.
//!
//! # Arms
//!
//! * **SHARED** — N threads, one `ServerContext`. The real configuration.
//! * **PRIVATE (the CONTROL)** — N threads, N `ServerContext`s. Removes every shared structure.
//!   Without it the shared arm's slope means nothing, because a harness can be the wall.
//!
//! Order is rotated per round AND the arm order is rotated, because D50 found a 2.2× bias between
//! arms that point-rotation cannot cancel. Guards **refuse**: zero iterations, a thread that did
//! nothing, or a row count that is not the iteration count.

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

const ROWS: i64 = 5000;
const WARMUP: Duration = Duration::from_millis(300);
const MEASURE: Duration = Duration::from_millis(1000);
const ROUNDS: usize = 3;
const POINTS: [usize; 5] = [1, 2, 4, 8, 16];
const STAGED: [usize; 2] = [10, 4000];

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
}

/// One statement through the server's own dispatch — the shared read path first, the exclusive
/// lock only for what `try_run_read` refuses. This must mirror `pgwire::extended`, or the harness
/// measures a path the server does not run.
fn exec(
    s: &Server,
    sql: &str,
    sess: &mut Session,
    cache: &mut Option<(u64, Arc<Catalog>)>,
    slot: &AtomicBool,
) -> usize {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    assert!(parser.errors.is_empty(), "parse failed for {sql}: {:?}", parser.errors);
    let stmt = stmts.remove(0);

    let outcome = {
        let shared = s.ctx.read_catalog(cache);
        let attempted = match s.ctx.begin_read(slot) {
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
        Ok(Outcome::Rows(r)) => r.len(),
        Ok(Outcome::Table(t)) => t.rows.len(),
        Ok(_) => 0,
        Err(e) => panic!("{sql} failed: {e}"),
    }
}

/// Every `build` gets a FRESH directory.
///
/// Re-using `s{n}` across sweep points truncated `main.db` while leaving the previous point's
/// `b.branchcat` sidecar in place, and the branch catalog then refused with *"page 1 is not a
/// branch-catalog header"* — correctly, because it was not. A counter is cheaper than reasoning
/// about which files a rebuild does and does not clear.
static BUILD_SEQ: AtomicU64 = AtomicU64::new(0);

fn build(dir: &std::path::Path, n: usize) -> Server {
    let seq = BUILD_SEQ.fetch_add(1, Ordering::Relaxed);
    let d = dir.join(format!("s{n}_{seq}"));
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

    let mut cache = None;
    let slot = AtomicBool::new(false);
    let mut sess = Session::new();
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess, &mut cache, &slot);
    for i in 1..=ROWS {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut sess, &mut cache, &slot);
    }
    s
}

fn sweep_point(servers: &[Arc<Server>], shared: bool, threads: usize, staged: usize) -> (f64, u64) {
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
        handles.push(std::thread::spawn(move || {
            let mut sess = Session::new();
            let mut cache: Option<(u64, Arc<Catalog>)> = None;
            let slot = Arc::new(AtomicBool::new(false));
            srv.ctx.register_reader(Arc::clone(&slot));

            // Each thread is its own agent on its own branch. This is the whole point: the plain
            // SELECT D54 measured never touched AgentRuntime's state lock.
            exec(
                &srv,
                &format!("BEGIN AGENT SESSION AS 'a{t}' RUN 'r{t}';"),
                &mut sess,
                &mut cache,
                &slot,
            );
            // Stage rows on THIS branch, so the workspace walk has something to walk.
            for i in 0..staged {
                let id = (i as i64) % ROWS + 1;
                exec(
                    &srv,
                    &format!("UPDATE t SET v = {} WHERE id = {id};", 900000 + i),
                    &mut sess,
                    &mut cache,
                    &slot,
                );
            }

            // Read a row this branch did NOT stage, so the answer comes from the base table and
            // the workspace walk is pure overhead -- which is exactly the cost under test.
            let key = ROWS - (t as i64 % 8);
            let sql = format!("SELECT v FROM t WHERE id = {key};");
            let t0 = Instant::now();
            while t0.elapsed() < WARMUP {
                exec(&srv, &sql, &mut sess, &mut cache, &slot);
            }
            start.wait();
            let (mut n, mut r) = (0u64, 0u64);
            while !stop.load(Ordering::Relaxed) {
                r += exec(&srv, &sql, &mut sess, &mut cache, &slot) as u64;
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

fn run_arm(dir: &std::path::Path, shared: bool, staged: usize, label: &str) -> Vec<f64> {
    println!();
    println!("=== ARM {label}  staged={staged} ===");
    let mut samples: Vec<Vec<f64>> = vec![Vec::new(); POINTS.len()];
    for r in 0..ROUNDS {
        let order: Vec<usize> = (0..POINTS.len()).map(|i| (i + r) % POINTS.len()).collect();
        let shown: Vec<String> = order.iter().map(|i| POINTS[*i].to_string()).collect();
        println!("# round {r} order: {}", shown.join(" "));
        for &i in &order {
            // Fresh servers per point: an agent session and its staged rows are state, and
            // re-using a branch across points would measure the previous point's workspace.
            // Build only what this point uses: the shared arm reads servers[0] and nothing
            // else, so building sixteen of them would be setup cost with no effect on the
            // measurement.
            let want = if shared { 1 } else { POINTS[i] };
            let servers: Vec<Arc<Server>> =
                (0..want).map(|n| Arc::new(build(dir, n))).collect();
            let (ops, slow) = sweep_point(&servers, shared, POINTS[i], staged);
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
    println!("threads   total_stmt_s   per_thread   total_vs_1T");
    for (i, &t) in POINTS.iter().enumerate() {
        println!(
            "{t:>7}   {:>12.0}   {:>10.0}   {:>11.3}",
            meds[i],
            meds[i] / t as f64,
            meds[i] / meds[0]
        );
    }
    meds
}

fn main() {
    let dir = std::env::temp_dir().join(format!("ferrodb-d55-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    println!("# D55-AGENT-READ-PATH  rows={ROWS} warmup={WARMUP:?} measure={MEASURE:?} rounds={ROUNDS}");
    println!("# PREDICTION (before the run): the shared arm stays FLAT, because AgentRuntime holds");
    println!("# one Mutex<State> and visible_rows walks the whole workspace under it. If it scales");
    println!("# anyway, the prediction is wrong and the ledger says so rather than defending it.");
    println!("# staged rows per branch: {STAGED:?} -- the upper figure is D27's measured 4000.");

    let rev = std::env::var("D55_ARM_ORDER").map(|v| v == "BA").unwrap_or(false);

    // D55_QUICK=1: the shared arm at staged=10 only -- the exact block the full baseline sweep
    // produced first, so a before/after on it is same-harness, same-mode. The full sweep is the
    // design-record artifact; this is the falsifier, and it runs in minutes rather than an hour.
    if std::env::var("D55_QUICK").is_ok() {
        println!("# QUICK: shared arm, staged=10 only");
        let a = run_arm(&dir, true, 10, "SHARED  (ONE ServerContext)");
        println!();
        println!("threads   shared_vs_1T   shared_abs");
        for (i, &t) in POINTS.iter().enumerate() {
            println!("{t:>7}   {:>12.3}   {:>10.0}", a[i] / a[0], a[i]);
        }
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }

    let mut out: Vec<(usize, Vec<f64>, Vec<f64>)> = Vec::new();
    for &staged in STAGED.iter() {
        let (a, b) = if rev {
            let b = run_arm(&dir, false, staged, "PRIVATE (N ServerContexts) -- the CONTROL");
            let a = run_arm(&dir, true, staged, "SHARED  (ONE ServerContext)");
            (a, b)
        } else {
            let a = run_arm(&dir, true, staged, "SHARED  (ONE ServerContext)");
            let b = run_arm(&dir, false, staged, "PRIVATE (N ServerContexts) -- the CONTROL");
            (a, b)
        };
        out.push((staged, a, b));
    }

    println!();
    println!("# arm order this run: {}", if rev { "B then A" } else { "A then B" });
    for (staged, a, b) in &out {
        println!();
        println!("staged={staged}");
        println!("threads   shared_vs_1T   private_vs_1T   shared_abs   private_abs");
        for (i, &t) in POINTS.iter().enumerate() {
            println!(
                "{t:>7}   {:>12.3}   {:>13.3}   {:>10.0}   {:>11.0}",
                a[i] / a[0],
                b[i] / b[0],
                a[i],
                b[i]
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
