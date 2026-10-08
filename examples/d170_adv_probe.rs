//! D170 ADVERSARY — three questions the D170 re-run cannot answer about itself.
//!
//! The `Db`/`build` below are COPIED VERBATIM from `examples/d71_point_update_curve.rs` at HEAD,
//! so the engine configuration under test is byte-for-byte the one D170 measured: a real
//! `TableBranchCatalog` sidecar, a real `ArenaPageStore`, a `ServerContext` that designates the
//! runtime, and every session built from it.
//!
//! What this adds that the D71 harness cannot do, because it DISCARDS the `Outcome`
//! (`d71_point_update_curve.rs:163` — the returned `Outcome` is dropped):
//!
//!   Q1  DOES THE STAGED UPDATE MUTATE ANYTHING? An `UPDATE` matching 0 rows SUCCEEDS, so a
//!       silent no-op is indistinguishable from a fast write in the D71 output. Every statement's
//!       `AgentOutput::Affected(n)` is asserted `== 1`, and the value is READ BACK through the
//!       agent session and asserted equal to what was written. A NEGATIVE CONTROL runs the same
//!       assertion against a key that cannot exist and requires `Affected(0)`, so the instrument
//!       is proven able to tell 0 from 1 rather than assumed to be.
//!
//!   Q2  IS THE FLATNESS THE ARENA, OR THE D71 PUSHDOWN FIX `e6958d6`? `D170_NOPUSHDOWN=1`
//!       restores the pre-fix call inside `branch_update` in THIS SAME BINARY. If the staged arm
//!       goes linear again on the ARENA path, the linearity was never about the stub.
//!
//!   Q3  IS THE AXIS `D71_N`? `D170_NSWEEP` fixes the table and sweeps the number of staged rows.
//!
//! Durations are printed but the CONCLUSIONS ride on the integer columns (`affected`, `rowsseen`)
//! and on within-run ratios, because this box is not quiet.
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
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::{fsync_counters, WalManager};
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
            AgentRuntime::with_storage(
                branches,
                Arc::new(MemEffectLog::new()),
                store as Arc<dyn PageStore>,
            )
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

/// The EXACT pre-`fc443c4` stub configuration: no `ArenaPageStore`, no `ServerContext`, and every
/// session from `Session::new()` -- whose implicit `AgentRuntime::new()` is `storage: None` over
/// an in-memory branch catalog. `designated::check` returns `Ok(())` when nothing is designated,
/// so a process that never builds a `ServerContext` runs this without touching the D101 guard.
/// **This mode must never share a process with the arena mode**, or the guard would (correctly)
/// refuse it.
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

/// The stub arm of the size sweep, with the same assertions the arena arm carries.
fn stub_sweep(sizes: &[i64], n: usize) {
    println!("\n  STUB CONFIG (Session::new(), storage: None) — the pre-fc443c4 harness");
    println!("  table rows   median ms   ms per 1000   affected_sum   probe/walk_unprob/walk_nopk   max_unprob   max_overlay");
    let mut first: Option<(i64, f64)> = None;
    for &rows in sizes {
        let mut db = StubDb::new();
        let mut s = Session::new();
        db.exec("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut s);
        for i in 1..=rows {
            db.exec(&format!("INSERT INTO t VALUES ({i}, 0);"), &mut s);
        }
        let mut a = Session::new();
        db.exec("BEGIN AGENT SESSION AS 'd170';", &mut a);
        // Same negative control as the arena arm.
        let nout = db.exec(&format!("UPDATE t SET v = -1 WHERE id = {};", rows + 1_000_000), &mut a);
        assert_eq!(affected(&nout), Some(0), "STUB negative control failed at {rows} rows");
        d170_reset_overlay_counters();
        let mut samples = Vec::new();
        let mut affected_sum = 0usize;
        for i in 0..n {
            let id = 1 + (i as i64 * 7919) % rows;
            let t = Instant::now();
            let out = db.exec(&format!("UPDATE t SET v = {i} WHERE id = {id};"), &mut a);
            samples.push(t.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(affected(&out), Some(1), "STUB staged UPDATE is a NO-OP at {rows} rows, i={i}");
            affected_sum += 1;
        }
        let (pf, wu, wn, mu, mo) = d170_overlay_counters();
        let med = median(&mut samples);
        println!("  {rows:>10}   {med:>9.4}   {:>11.4}   {affected_sum:>12}   {pf:>5}/{wu:>5}/{wn:>5}            {mu:>10}   {mo:>11}",
                 med / (rows as f64 / 1000.0));
        if first.is_none() { first = Some((rows, med)); }
        if let Some((r0, m0)) = first {
            if rows != r0 {
                println!("             ^ {:.1}x the rows, {:.2}x the time", rows as f64 / r0 as f64, med / m0);
            }
        }
    }
}

fn build(rows: i64) -> (Db, Session) {
    let mut db = Db::new(rows);
    let mut s = db.ctx.session();
    db.exec("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut s);
    for i in 1..=rows {
        db.exec(&format!("INSERT INTO t VALUES ({i}, 0);"), &mut s);
    }
    (db, s)
}

/// How many rows a statement says it changed. `None` = the outcome carries no count at all,
/// which is itself a finding and must never be silently read as zero or as success.
fn affected(o: &Outcome) -> Option<usize> {
    match o {
        Outcome::Agent(AgentOutput::Affected(n)) => Some(*n),
        Outcome::Affected(n) => Some(*n),
        _ => None,
    }
}

fn rows_of(o: &Outcome) -> Vec<Vec<Value>> {
    match o {
        Outcome::Rows(r) => r.clone(),
        Outcome::Table(t) => t.rows.clone(),
        _ => Vec::new(),
    }
}

fn median(v: &mut Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    println!("D170 ADVERSARY PROBE");
    println!("{}", ferrodb::build_provenance());
    let nopush = std::env::var("D170_NOPUSHDOWN").map(|v| v == "1").unwrap_or(false);
    println!("D170_NOPUSHDOWN = {}  ({})", nopush,
             if nopush { "PRE-e6958d6 unpredicated scan in branch_update" } else { "HEAD, predicate pushed" });
    let staged_arm = std::env::var("D71_ARM").map(|a| a == "staged").unwrap_or(false);
    println!("arm = {}", if staged_arm { "STAGED" } else { "PLAIN" });

    if std::env::var("D170_STUB").is_ok() {
        let sizes: Vec<i64> = std::env::var("D71_SIZES")
            .unwrap_or_else(|_| "1000,2000,4000,8000,16000".to_string())
            .split(',').filter_map(|v| v.parse().ok()).collect();
        let n: usize = std::env::var("D71_N").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
        stub_sweep(&sizes, n);
        return;
    }

    // ---- FIRE-CHECK the overlay-arm detector ------------------------------------------------
    //
    // A counter reading "probe_fired=60, walks=0" is worth nothing until it has been FORCED to
    // report the other two arms. This drives all three on purpose and refuses to continue unless
    // each one is observed, so the main sweep's reading is a measurement and not an assumption.
    if std::env::var("D170_FIRECHECK").is_ok() {
        println!("\nFIRE-CHECK — force each overlay arm and require the counter to see it.");
        let (mut db, _s) = build(200);
        let mut a = db.ctx.session();
        db.exec("BEGIN AGENT SESSION AS 'd170fc';", &mut a);

        d170_reset_overlay_counters();
        db.exec("UPDATE t SET v = 1 WHERE id = 7;", &mut a);
        let (pf, wu, wn, mu, _) = d170_overlay_counters();
        println!("  1. pk= predicate, nothing demoted     -> probe={pf} walk_unprob={wu} walk_nopk={wn} unprob_rows={mu}");
        assert!(pf >= 1 && wu == 0, "the PROBE did not fire on a plain `id = k` staged UPDATE");

        d170_reset_overlay_counters();
        db.exec("UPDATE t SET v = 2 WHERE v = 999999;", &mut a);
        let (pf, wu, wn, mu, _) = d170_overlay_counters();
        println!("  2. NON-pk predicate                   -> probe={pf} walk_unprob={wu} walk_nopk={wn} unprob_rows={mu}");
        assert!(wn >= 1, "a predicate with no `pk = literal` conjunct did not take the WALK arm");

        // D167's door: a PK-MOVING update makes `unprobeable_rows` non-zero, permanently.
        db.exec("UPDATE t SET id = 1000000 WHERE id = 5;", &mut a);
        d170_reset_overlay_counters();
        db.exec("UPDATE t SET v = 3 WHERE id = 7;", &mut a);
        let (pf, wu, wn, mu, _) = d170_overlay_counters();
        println!("  3. same pk= predicate AFTER a PK move -> probe={pf} walk_unprob={wu} walk_nopk={wn} unprob_rows={mu}");
        assert!(wu >= 1 && mu > 0,
            "demotion did not reach the counter: a PK-moving UPDATE should leave unprobeable_rows>0 \
             and force the walk, but probe={pf} walk_unprob={wu} unprob_rows={mu}");

        println!("  ALL THREE ARMS OBSERVED. The detector discriminates.");
        return;
    }

    // ---- Q3: sweep the number of staged rows at a FIXED table size ---------------------------
    if std::env::var("D170_NSWEEP").is_ok() {
        let rows: i64 = std::env::var("D170_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(4000);
        let ns: Vec<usize> = std::env::var("D170_NS")
            .unwrap_or_else(|_| "50,100,200,400,800,1600,3200,6400,12800".to_string())
            .split(',').filter_map(|v| v.parse().ok()).collect();
        println!("\nQ3 — N-SWEEP at a FIXED {rows}-row table. Axis = rows staged in the session.");
        println!("  staged N     median ms   us/update   affected!=1   probe/walk_unprob/walk_nopk   max_unprob   max_overlay");
        for &n in &ns {
            let (mut db, mut s) = build(rows);
            let mut a = db.ctx.session();
            if staged_arm { db.exec("BEGIN AGENT SESSION AS 'd170';", &mut a); }
            let sess: &mut Session = if staged_arm { &mut a } else { &mut s };
            d170_reset_overlay_counters();
            let mut samples = Vec::new();
            let mut bad = 0usize;
            for i in 0..n {
                // Every update hits a DISTINCT key, so the session's staged-row count really is N.
                let id = 1 + (i as i64 * 7919) % rows;
                let t = Instant::now();
                let out = db.exec(&format!("UPDATE t SET v = {i} WHERE id = {id};"), sess);
                samples.push(t.elapsed().as_secs_f64() * 1000.0);
                if affected(&out) != Some(1) { bad += 1; }
            }
            let med = median(&mut samples);
            let (pf, wu, wn, mu, mo) = d170_overlay_counters();
            println!("  {n:>8}     {med:>9.4}   {:>9.2}   {bad:>11}   {pf:>5}/{wu:>5}/{wn:>5}            {mu:>10}   {mo:>11}",
                     med * 1000.0);
        }
        return;
    }

    // ---- Q1 + Q2: the table-size axis, with every statement's effect asserted ----------------
    let sizes: Vec<i64> = std::env::var("D71_SIZES")
        .unwrap_or_else(|_| "1000,2000,4000,8000,16000".to_string())
        .split(',').filter_map(|v| v.parse().ok()).collect();
    let n: usize = std::env::var("D71_N").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
    println!("\n  table rows   median ms   ms per 1000   fsyncs/upd   affected_sum   readback_ok   neg_ctl");
    let mut first: Option<(i64, f64)> = None;
    for &rows in &sizes {
        let (mut db, mut s) = build(rows);
        let mut a = db.ctx.session();
        if staged_arm { db.exec("BEGIN AGENT SESSION AS 'd170';", &mut a); }
        let sess: &mut Session = if staged_arm { &mut a } else { &mut s };

        // NEGATIVE CONTROL, run FIRST and on this same session: a key that cannot exist must
        // report 0. If this ever reports 1, or reports `None`, the `affected` instrument below is
        // not measuring what it claims and NOTHING in this row may be read as a mutation.
        let ncid = rows + 1_000_000;
        let nout = db.exec(&format!("UPDATE t SET v = -1 WHERE id = {ncid};"), sess);
        let neg = affected(&nout);
        assert_eq!(neg, Some(0),
            "NEGATIVE CONTROL FAILED at {rows} rows: an UPDATE on absent key {ncid} reported {neg:?}, \
             so `affected` cannot distinguish a no-op from a write and this run is VOID");

        d170_reset_overlay_counters();
        let (f0, _) = fsync_counters();
        let mut samples = Vec::new();
        let mut affected_sum = 0usize;
        for i in 0..n {
            let id = 1 + (i as i64 * 7919) % rows;
            let t = Instant::now();
            let out = db.exec(&format!("UPDATE t SET v = {i} WHERE id = {id};"), sess);
            samples.push(t.elapsed().as_secs_f64() * 1000.0);
            let got = affected(&out);
            assert_eq!(got, Some(1),
                "STAGED UPDATE IS A NO-OP at {rows} rows, i={i}, id={id}: reported {got:?}, expected 1");
            affected_sum += got.unwrap();
        }
        let (f1, _) = fsync_counters();

        // READ-BACK, outside the timed loop: the value written by the LAST update must be
        // visible to this same session. Asserting the count alone would still admit a write that
        // was counted and then dropped.
        let last = n - 1;
        let last_id = 1 + (last as i64 * 7919) % rows;
        let sel = db.exec(&format!("SELECT v FROM t WHERE id = {last_id};"), sess);
        let got = rows_of(&sel);
        assert_eq!(got.len(), 1, "read-back at {rows} rows returned {} rows, expected 1", got.len());
        let want = Value::Integer(last as i32);
        assert_eq!(got[0][0], want,
            "READ-BACK MISMATCH at {rows} rows, id={last_id}: stored {:?}, wrote {want:?} — the \
             statement was COUNTED but the row did not change", got[0][0]);

        let (pf, wu, wn, mu, mo) = d170_overlay_counters();
        println!("             overlay arms: probe_fired={pf} walk_unprobeable={wu} walk_no_pk_conjunct={wn} max_unprobeable_rows={mu} max_overlay_len={mo}");
        let med = median(&mut samples);
        println!("  {rows:>10}   {med:>9.4}   {:>11.4}   {:>10.2}   {affected_sum:>12}   {:>11}   {:>7}",
                 med / (rows as f64 / 1000.0),
                 (f1 - f0) as f64 / n as f64,
                 "YES",
                 neg.unwrap());
        if first.is_none() { first = Some((rows, med)); }
        if let Some((r0, m0)) = first {
            if rows != r0 {
                println!("             ^ {:.1}x the rows, {:.2}x the time", rows as f64 / r0 as f64, med / m0);
            }
        }
    }
    println!("\nEvery row above asserted: affected==1 per statement, read-back equal, negative control==0.");
}
