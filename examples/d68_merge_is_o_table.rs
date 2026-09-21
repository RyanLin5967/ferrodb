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
/// Rows written per branch. `D68_DELTA` overrides it.
///
/// **THE ORTHOGONAL AXIS — D69-REOPEN.** The curve against TABLE SIZE at fixed delta says merge is
/// linear in the table. This axis asks the complementary question: at a FIXED table, does the cost
/// scale with how much the branch actually CHANGED? Neither axis alone can separate
///   * cost ~ delta        -> O(delta) achieved, and the table-size slope is something else; from
///   * cost flat in delta  -> a per-merge constant that depends on the TABLE, not on the work.
static WRITES_PER_BRANCH: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::env::var("D68_DELTA").ok().and_then(|v| v.parse().ok()).unwrap_or(4)
});
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
    // D101 — ARM PERSISTENCE, because production does and this harness did not.
    // `ArenaPageStore` persists its free-space map through `persist_if_configured`, which is a
    // NO-OP until a checkpoint path is set. `src/cli/cli.rs:120` and `examples/pgserver.rs:101`
    // both set one, so a run without it measures a configuration nobody ships — and it measures
    // it in the flattering direction, since the persistence work is simply skipped.
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

    // D101 — `s.ctx.session()`, NEVER `Session::new()`. `Session::new` builds its OWN
    // `AgentRuntime::new()` (`storage: None`, private in-memory branch catalog, private
    // effect log), so every agent statement below would run on a stub and the arena/durable
    // catalog built above would be constructed and never touched. `agent_sql::designated`
    // now refuses this rather than measuring it.
    let mut sess = s.ctx.session();
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    for i in 1..=nrows {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut sess).unwrap();
    }
    s
}

/// One agent's whole life: fork, write, merge. Returns true if the cycle completed.
/// One merge cycle, with the SIX commits it contains timed and counted SEPARATELY.
///
/// ⛔ **THE TIMER USED TO WRAP ALL SIX AND THAT IS WHY D68 WAS MISREAD.** `one_cycle` issues
/// `BEGIN AGENT SESSION`, then `*WRITES_PER_BRANCH` UPDATEs, then `MERGE` — and the single
/// wall-clock number around the lot was reported, in the file name and in the prose, as "merge
/// latency". It is not: it is the latency of a session plus N updates plus a merge. If the UPDATEs
/// are what grow with table size then "the merge is O(table)" was never a statement about the
/// merge, and every mechanism proposed for it was aimed at the wrong statement.
///
/// So the phases are separated here, and the fsyncs each one performs are COUNTED rather than
/// inferred from a profile aggregate — an aggregate cannot tell one slow fsync from several fast
/// ones, which is the exact ambiguity left open by the 98.2% `WalManager::flush` reading.
struct Cycle {
    begin_ms: f64,
    writes_ms: f64,
    merge_ms: f64,
    merge_fsyncs: u64,
    merge_fsync_bytes: u64,
    total_fsyncs: u64,
}

fn one_cycle_timed(s: &Server, tid: usize, seq: u64, disjoint: bool) -> Option<Cycle> {
    use ferrodb::wal::log::fsync_counters;
    let mut sess = s.ctx.session();
    let (c0, _) = fsync_counters();

    let t = Instant::now();
    if exec(s, &format!("BEGIN AGENT SESSION AS 'a{tid}';"), &mut sess).is_err() {
        return None;
    }
    let begin_ms = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    for w in 0..*WRITES_PER_BRANCH {
        // DISJOINT: a private slice of the key space per thread. SHARED: everyone on the same ids.
        let id = if disjoint {
            1 + (tid as i64 * 97 + w as i64) % ROWS
        } else {
            1 + (w as i64) % 16
        };
        let v = (seq % 1000) as i64;
        if exec(s, &format!("UPDATE t SET v = {v} WHERE id = {id};"), &mut sess).is_err() {
            return None;
        }
    }
    let writes_ms = t.elapsed().as_secs_f64() * 1000.0;
    let (c1, b1) = fsync_counters();
    // ⚠ COUNT `applied_to_target`, NOT `is_ok()`. The first version of this counted any Ok, and a
    // QUARANTINED merge returns Ok — so the SHARED arm looked FASTER the more merges failed, and
    // its apparent "recovery" to 0.87x at 16 threads was measuring how quickly merges can fail.
    // A merge that did not reach the target did not do the work being timed.
    // ⚠ READ THE REPORT, NOT A RENDERED ROW. The first attempt at this matched
    // `Outcome::Table(t)` and indexed column 4 — but a MERGE returns `Outcome::Agent`, so the
    // match fell to `_ => false` and counted ZERO forever, in every arm. It did not fail; it
    // reported 0.0 merges/sec, which is exactly the shape a broken instrument takes.
    let t = Instant::now();
    let applied = match exec(s, "MERGE;", &mut sess) {
        Ok(Outcome::Agent(AgentOutput::Merge(report))) => report.applied_to_target,
        _ => false,
    };
    let merge_ms = t.elapsed().as_secs_f64() * 1000.0;
    let (c2, b2) = fsync_counters();
    if !applied {
        return None;
    }
    Some(Cycle {
        begin_ms,
        writes_ms,
        merge_ms,
        merge_fsyncs: c2 - c1,
        merge_fsync_bytes: b2 - b1,
        total_fsyncs: c2 - c0,
    })
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
    let mut sess = s.ctx.session();
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
    println!("delta (rows written per branch) = {}", *WRITES_PER_BRANCH);
    println!();
    println!("  table    n   total ms | begin ms  writes ms  MERGE ms | merge fsyncs  bytes/fsync");
    let med = |v: &mut Vec<f64>| { v.sort_by(|a, b| a.partial_cmp(b).unwrap()); v[v.len() / 2] };
    let mut first: Option<(i64, f64, f64)> = None;
    for &rows in &sizes {
        let s = build_sized(&dir, &format!("d68_{rows}"), rows);
        let (mut tot, mut beg, mut wr, mut mg) = (vec![], vec![], vec![], vec![]);
        let (mut fs, mut fb) = (0u64, 0u64);
        for i in 0..merges {
            if let Some(c) = one_cycle_timed(&s, 0, i as u64, true) {
                tot.push(c.begin_ms + c.writes_ms + c.merge_ms);
                beg.push(c.begin_ms);
                wr.push(c.writes_ms);
                mg.push(c.merge_ms);
                fs += c.merge_fsyncs;
                fb += c.merge_fsync_bytes;
                let _ = c.total_fsyncs;
            }
        }
        if tot.is_empty() {
            println!("  {rows:>6}   NO MERGE APPLIED — not a result, and not a zero");
            continue;
        }
        let n = tot.len() as u64;
        // **D86: does a merge get slower the more merges have already happened?**
        //
        // `State::applied` is a never-pruned Vec, and `concurrent_op` scans ALL of it once per
        // changed cell (`runtime.rs:3688`, filtering on tbl/row/col/seq). If that is the cost,
        // merge k pays O(k) and merging N branches is O(N^2). Comparing the FIRST decile of this
        // run against the LAST is a within-run comparison, so machine load cannot produce it.
        let decile = (mg.len() / 10).max(1);
        let mut first_d: Vec<f64> = mg[..decile].to_vec();
        let mut last_d: Vec<f64> = mg[mg.len() - decile..].to_vec();
        // **THE CONTROL.** The UPDATEs in each cycle do not touch `applied`, so their latency must
        // NOT drift within the run. If both drift, it is the machine; if only the merge does, it is
        // the applied log. Without this the comparison is worthless — rising load fakes it exactly.
        let mut first_w: Vec<f64> = wr[..decile].to_vec();
        let mut last_w: Vec<f64> = wr[wr.len() - decile..].to_vec();
        let (fw, lw) = (med(&mut first_w), med(&mut last_w));
        let (f_med, l_med) = (med(&mut first_d), med(&mut last_d));
        let (mt, mb, mw, mm) = (med(&mut tot), med(&mut beg), med(&mut wr), med(&mut mg));
        println!("           merge latency: first {decile} = {f_med:.3} ms, last {decile} = {l_med:.3} ms  -> {:.2}x drift", l_med / f_med.max(1e-9));
        println!("           CONTROL writes: first {decile} = {fw:.3} ms, last {decile} = {lw:.3} ms  -> {:.2}x drift (this one must stay ~1.0 or the machine moved)", lw / fw.max(1e-9));
        println!("  {rows:>6} {:>4}   {mt:>8.3} | {mb:>8.3}  {:>9.3}  {mm:>8.3} | {:>12.2}  {:>11.1}",
                 n, mw, fs as f64 / n as f64,
                 if fs == 0 { 0.0 } else { fb as f64 / fs as f64 });
        if first.is_none() { first = Some((rows, mt, mm)); }
        if let Some((r0, t0, g0)) = first {
            if rows != r0 {
                println!("         ^ {:.1}x rows -> {:.2}x TOTAL, {:.2}x MERGE",
                         rows as f64 / r0 as f64, mt / t0, mm / g0);
            }
        }
    }
    println!();
    println!("READ THE LAST COLUMN: flat ms-per-1000-rows means LINEAR in table size (O(table)).");
    println!("A falling ms-per-1000-rows means sublinear; a flat MEDIAN MS column means O(1).");
    let _ = std::fs::remove_dir_all(&dir);
}
