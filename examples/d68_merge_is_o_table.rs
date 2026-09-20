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
use ferrodb::tel::MemEffectLog;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::BranchCatalog;
use ferrodb::cow::PageStore;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::agent_sql::dispatch::AgentOutput;
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
    // ⚠ `with_storage`, NOT `with_catalog` — D67's first configuration was WRONG and the numbers
    // it produced are withdrawn. `with_catalog` delegates to `with_parts`, which sets
    // `storage: None, reaper: None` (src/agent_sql/runtime.rs:628-629), so the whole branch
    // STORAGE engine — ArenaPageStore, CoW pages, the reaper — was absent from a benchmark
    // reporting on "concurrent branch management". Agent writes went to an in-memory effect log
    // instead of arena pages. Found by an adversarial reviewer reading the harness, not by me.
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&d.join("b.branchcat"), 1).unwrap());
    let branches: Arc<dyn BranchCatalog> = cat.clone();
    // Reserve a table region BELOW the arena. Taking high_water() here would put the arena at
    // page 2 and leave the ordinary table with nowhere to grow — "no free page below the reserved
    // arena region", which is what the first attempt hit. branch_scaling_bench uses the same
    // fixed floor for the same reason.
    const ARENA_BASE: u32 = 1024;
    let arena_base = ARENA_BASE;
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), arena_base).unwrap());
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

    let mut sess = Session::new();
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    for i in 1..=nrows {
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
    // ⚠ COUNT `applied_to_target`, NOT `is_ok()`. The first version of this counted any Ok, and a
    // QUARANTINED merge returns Ok — so the SHARED arm looked FASTER the more merges failed, and
    // its apparent "recovery" to 0.87x at 16 threads was measuring how quickly merges can fail.
    // A merge that did not reach the target did not do the work being timed.
    // ⚠ READ THE REPORT, NOT A RENDERED ROW. The first attempt at this matched
    // `Outcome::Table(t)` and indexed column 4 — but a MERGE returns `Outcome::Agent`, so the
    // match fell to `_ => false` and counted ZERO forever, in every arm. It did not fail; it
    // reported 0.0 merges/sec, which is exactly the shape a broken instrument takes.
    match exec(s, "MERGE;", &mut sess) {
        Ok(Outcome::Agent(AgentOutput::Merge(report))) => report.applied_to_target,
        _ => false,
    }
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


/// D68 — IS MERGE O(TABLE) OR O(DELTA)? One thread, fixed delta, growing table.
///
/// D67 found merges serialising on the global catalog mutex. Reviewing what the lock HOLDER does
/// found something larger: `evaluate_merge` (runtime.rs:2691) scans every row of each touched
/// table, and `fingerprint_tables` (runtime.rs:3459) scans it AGAIN, with `fingerprint_rows`
/// hashing every row to build `base_fingerprint`. A merge that writes four rows would then read
/// the whole table twice — which is a COMPLEXITY CLASS, not a locking constant, and no amount of
/// lock engineering touches it.
///
/// This run is ONE THREAD on purpose. With a single thread there is no contention, so whatever
/// slope appears is the merge's own cost against table size and nothing else.
///
/// PRE-REGISTERED: merge latency grows LINEARLY with table size at fixed delta. If it is FLAT,
/// the O(table) reading is WRONG — the scans exist but something prunes them — and D68 is closed
/// as a misreading of the source.
fn main() {
    let dir = std::env::temp_dir().join(format!("ferrodb-d68-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let sizes: Vec<i64> = std::env::var("D68_SIZES")
        .unwrap_or_else(|_| "1000,2000,4000,8000,16000".to_string())
        .split(',').filter_map(|v| v.parse().ok()).collect();
    let merges: usize = std::env::var("D68_MERGES").ok().and_then(|v| v.parse().ok()).unwrap_or(25);

    println!("D68 — merge latency against TABLE SIZE at FIXED DELTA (4 rows written per branch).");
    println!("One thread: no contention, so the slope is the merge's own cost.");
    println!("PRE-REGISTERED: linear in table size. If FLAT, the O(table) reading is wrong.");
    println!();
    println!("  table rows   merges   median ms   ms per 1000 rows");
    let mut first: Option<(i64, f64)> = None;
    for &rows in &sizes {
        let s = build_sized(&dir, &format!("d68_{rows}"), rows);
        let mut samples = Vec::new();
        for i in 0..merges {
            let t = Instant::now();
            let ok = one_cycle(&s, 0, i as u64, true);
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            if ok { samples.push(ms); }
        }
        if samples.is_empty() {
            println!("  {rows:>10}   {:>6}   NO MERGE APPLIED — not a result", 0);
            continue;
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = samples[samples.len() / 2];
        println!("  {rows:>10}   {:>6}   {med:>9.3}   {:>16.4}", samples.len(), med / (rows as f64 / 1000.0));
        if first.is_none() { first = Some((rows, med)); }
        if let Some((r0, m0)) = first {
            if rows != r0 {
                println!("             ^ {:.1}x the rows, {:.2}x the merge time", rows as f64 / r0 as f64, med / m0);
            }
        }
    }
    println!();
    println!("READ THE LAST COLUMN: flat ms-per-1000-rows means LINEAR in table size (O(table)).");
    println!("A falling ms-per-1000-rows means sublinear; a flat MEDIAN MS column means O(1).");
    let _ = std::fs::remove_dir_all(&dir);
}
