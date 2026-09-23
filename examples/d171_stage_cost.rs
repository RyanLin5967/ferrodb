//! D171 — separate W (distinct rows staged) from S (statements executed) in the staged write path.
//!
//! Pre-registered in `bench/d171_prereg.txt`, committed BEFORE the first measurement.
//!
//! D170 found a ~35 ns/staged-row residual at HEAD with the overlay probe firing on every
//! statement. Its sweep had `W == S` by construction and took the median of a session growing from
//! 0 to N, so a per-STATEMENT term presents there as a per-staged-row cost. This separates them.
//!
//! Method, after D167: PRE-LOAD the session to the target state untimed, then time a FIXED window
//! of 200 statements on keys that are ALREADY staged, so the axis does not move while timing.
use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::Instant;

use ferrodb::agent_sql::runtime::{d170_overlay_counters, d170_reset_overlay_counters, AgentRuntime};
use ferrodb::agent_sql::AgentOutput;
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

struct Db {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new(rows: i64) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(dir.path().join("p.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        let cat = Arc::new(TableBranchCatalog::open_sidecar(&dir.path().join("b.branchcat"), 1).unwrap());
        let branches: Arc<dyn BranchCatalog> = cat;
        let arena_base: u32 = ((rows / 40) as u32 + 4096).next_power_of_two();
        let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), arena_base).unwrap());
        store.checkpoint_to(dir.path().join("p.arena"));
        let runtime = Arc::new(
            AgentRuntime::with_storage(branches, Arc::new(MemEffectLog::new()), store as Arc<dyn PageStore>)
                .expect("attach arena storage"),
        );
        let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
        Db { ctx, bp, txn, _dir: dir }
    }
    fn exec(&mut self, sql: &str, s: &mut Session) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "{sql}: {:?}", p.errors);
        let mut cat = self.ctx.catalog();
        let out = run(stmts.remove(0), &mut cat, self.bp.clone(), self.txn.clone(), s);
        drop(cat);
        out.unwrap_or_else(|e| panic!("{sql}: {e}"))
    }
}

/// ARM D (amendment A3): the EXACT stub configuration `d71_point_update_curve.txt`'s
/// "second wall" sweep ran on -- `Session::new()`, `storage: None`, and **no `ServerContext`
/// constructed anywhere in the process**, which `designated::check` permits ("Returns `Ok(())`
/// when nothing is designated"). Must never share a process with the arena path.
struct StubDb {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    _dir: tempfile::TempDir,
}

impl StubDb {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(dir.path().join("p.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        StubDb { catalog, bp, txn, _dir: dir }
    }
    fn exec(&mut self, sql: &str, s: &mut Session) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "{sql}: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
    }
}

/// One stub cell, same shape as `cell`: pre-load untimed to (w, s), then time TIMED statements.
fn stub_cell(rows: i64, w: usize, s: usize) -> (f64, u64, u64, u64, u64) {
    let mut db = StubDb::new();
    let mut base = Session::new();
    db.exec("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut base);
    for i in 1..=rows {
        db.exec(&format!("INSERT INTO t VALUES ({i}, 0);"), &mut base);
    }
    let mut a = Session::new();
    db.exec("BEGIN AGENT SESSION AS 'd171';", &mut a);
    let nout = db.exec(&format!("UPDATE t SET v = -1 WHERE id = {};", rows + 1_000_000), &mut a);
    assert_eq!(affected(&nout), Some(0), "D171 STUB negative control failed at w={w} s={s}");
    for i in 0..s {
        let out = db.exec(&format!("UPDATE t SET v = {i} WHERE id = {};", key(i, w, rows)), &mut a);
        assert_eq!(affected(&out), Some(1), "STUB preload no-op at w={w} s={s} i={i}");
    }
    d170_reset_overlay_counters();
    let mut samples = Vec::with_capacity(TIMED);
    for i in 0..TIMED {
        let k = key(i, w, rows);
        let t = Instant::now();
        let out = db.exec(&format!("UPDATE t SET v = {i} WHERE id = {k};"), &mut a);
        samples.push(t.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(affected(&out), Some(1), "STUB timed no-op at w={w} s={s} i={i}");
    }
    let (pf, wu, _wn, mu, mo) = d170_overlay_counters();
    (median(&mut samples), pf, wu, mu, mo)
}

fn affected(o: &Outcome) -> Option<usize> {
    match o {
        Outcome::Agent(AgentOutput::Affected(n)) => Some(*n),
        Outcome::Affected(n) => Some(*n),
        _ => None,
    }
}

fn median(v: &mut Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// The i-th key of a set of `w` distinct keys spread across a `rows`-row table.
/// `7919` is prime, so for `w <= rows` these are distinct.
fn key(i: usize, w: usize, rows: i64) -> i64 {
    1 + ((i % w) as i64 * 7919) % rows
}

const TIMED: usize = 200;

/// Run one cell: pre-load to (w distinct keys, s statements) untimed, then time TIMED statements
/// on keys already staged. Returns (median ms, probe_fired, walk_unprob, max_unprob, max_overlay).
fn cell(rows: i64, w: usize, s: usize) -> (f64, u64, u64, u64, u64) {
    let mut db = Db::new(rows);
    let mut base = db.ctx.session();
    db.exec("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut base);
    for i in 1..=rows {
        db.exec(&format!("INSERT INTO t VALUES ({i}, 0);"), &mut base);
    }
    let mut a = db.ctx.session();
    db.exec("BEGIN AGENT SESSION AS 'd171';", &mut a);

    // Negative control, before anything else: an absent key must report 0, or the `affected`
    // instrument cannot tell a no-op from a write and this cell is void.
    let nout = db.exec(&format!("UPDATE t SET v = -1 WHERE id = {};", rows + 1_000_000), &mut a);
    assert_eq!(affected(&nout), Some(0), "D171 negative control failed at w={w} s={s}");

    // PRE-LOAD, untimed. `s` statements spread over exactly `w` distinct keys.
    for i in 0..s {
        let out = db.exec(&format!("UPDATE t SET v = {i} WHERE id = {};", key(i, w, rows)), &mut a);
        assert_eq!(affected(&out), Some(1), "preload no-op at w={w} s={s} i={i}");
    }

    // TIMED WINDOW. Keys already staged, so the overlay does not grow while timing.
    d170_reset_overlay_counters();
    let mut samples = Vec::with_capacity(TIMED);
    for i in 0..TIMED {
        let k = key(i, w, rows);
        let t = Instant::now();
        let out = db.exec(&format!("UPDATE t SET v = {i} WHERE id = {k};"), &mut a);
        samples.push(t.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(affected(&out), Some(1), "timed no-op at w={w} s={s} i={i}");
    }
    let (pf, wu, _wn, mu, mo) = d170_overlay_counters();
    (median(&mut samples), pf, wu, mu, mo)
}

fn main() {
    println!("D171 — separate W (staged rows) from S (statements). Prereg: bench/d171_prereg.txt");
    println!("{}", ferrodb::build_provenance());
    let rows: i64 = std::env::var("D171_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(20000);
    let axis: Vec<usize> = std::env::var("D171_AXIS")
        .unwrap_or_else(|_| "100,400,1600,6400,12800".to_string())
        .split(',').filter_map(|v| v.parse().ok()).collect();
    let arm = std::env::var("D171_ARM").unwrap_or_else(|_| "A".to_string());
    let s_fixed: usize = std::env::var("D171_S").ok().and_then(|v| v.parse().ok()).unwrap_or(12800);
    let w_fixed: usize = std::env::var("D171_W").ok().and_then(|v| v.parse().ok()).unwrap_or(100);

    println!("table rows = {rows}, timed window = {TIMED} statements on already-staged keys");
    println!("config = {}", if std::env::var("D171_STUB").is_ok() {
        "STUB (Session::new(), storage: None, no ServerContext) — ARM D, amendment A3"
    } else { "ARENA (ArenaPageStore + ServerContext) — the shipped path" });
    println!("ARM {arm}: {}", match arm.as_str() {
        "A" => "W = S, both swept (reproduces D170's axis)".to_string(),
        "B" => format!("W FIXED at {w_fixed}, S swept -- overlay never grows"),
        "C" => format!("S FIXED at {s_fixed}, W swept -- statement count constant"),
        _ => "unknown".to_string(),
    });
    println!("\n      axis        W        S   median ms   us/stmt   probe   walk_unprob   max_unprob   max_overlay");
    let mut first: Option<f64> = None;
    for &x in &axis {
        let (w, s) = match arm.as_str() {
            "A" => (x, x),
            "B" => (w_fixed, x),
            "C" => (x, s_fixed),
            _ => panic!("D171_ARM must be A, B or C"),
        };
        assert!((w as i64) <= rows, "w={w} exceeds table rows={rows}: the overlay would SATURATE");
        let stub = std::env::var("D171_STUB").is_ok();
        let (med, pf, wu, mu, mo) = if stub { stub_cell(rows, w, s) } else { cell(rows, w, s) };

        // Controls, enforced before the number is printed.
        assert_eq!(pf, TIMED as u64, "probe did not fire on every timed statement (w={w} s={s}): {pf}/{TIMED}");
        assert_eq!(wu, 0, "walk_unprobeable non-zero at w={w} s={s}: this is D167's demoted regime, cell VOID");
        assert_eq!(mu, 0, "unprobeable_rows non-zero at w={w} s={s}: cell VOID");
        assert_eq!(mo, w as u64, "max_overlay_len={mo} != intended W={w}: the overlay SATURATED, cell VOID");

        println!("  {x:>8} {w:>8} {s:>8}   {med:>9.4}   {:>7.2}   {pf:>5}   {wu:>11}   {mu:>10}   {mo:>11}",
                 med * 1000.0);
        if first.is_none() { first = Some(med); }
        if let Some(m0) = first {
            if x != axis[0] {
                println!("           ^ {:.1}x the axis, {:.2}x the time", x as f64 / axis[0] as f64, med / m0);
            }
        }
    }
    println!("\nAll cells: probe fired on every timed statement, unprobeable_rows 0, overlay == W,");
    println!("every statement changed exactly 1 row, negative control 0.");
}
