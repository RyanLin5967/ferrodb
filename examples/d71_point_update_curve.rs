//! D71 — is `UPDATE t SET v = ? WHERE id = <primary key>` a DESCENT or a SCAN?
//!
//! This is the wall D68/D69 spent three rows attributing to the merge. `bench/d69_fsync_counted.txt`
//! split the merge harness's timer by phase and found the whole table-size slope living in the four
//! UPDATEs, not in the MERGE: 1.110 ms -> 11.566 ms over 16x the rows, and it survived reversing
//! the size order, which the merge column did not.
//!
//! That measurement cannot say WHICH update path is linear, because every write in it ran inside
//! `BEGIN AGENT SESSION` — so it could equally be the agent's staging path. This separates them:
//!
//!   * PLAIN   — an ordinary `UPDATE`, no agent session. The general engine path.
//!   * STAGED  — the same `UPDATE` inside an agent session, which is what the merge harness timed.
//!
//! PRE-REGISTERED, before the run:
//!   * PLAIN flat and STAGED linear  -> the wall is the agent staging path, and it is D71's.
//!   * BOTH linear                   -> the planner is not using the primary index for an equality
//!                                      on the primary key, and it is a general engine defect.
//!   * BOTH flat                     -> the slope is neither, and `d69_fsync_counted.txt` is wrong
//!                                      about where it lives. Report that and reopen.
//!
//! ⚠ ONE fsync per commit dominates each individual UPDATE, so the ABSOLUTES here are fsync, not
//! lookup. The question is only whether the cost GROWS WITH THE TABLE, and an fsync floor is a
//! constant that cannot manufacture a slope.
use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::Instant;

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
use ferrodb::wal::log::{fsync_counters, WalManager};
use ferrodb::wal::txn::TxnManager;

/// ⛔ **D101 — WHAT THE FIRST VERSION OF THIS HARNESS MEASURED, AND WHY IT IS NOT WHAT IT SAID.**
///
/// The banked run in `bench/d71_point_update_curve.txt` built NO `AgentRuntime` at all. It ran
/// every statement on `Session::new()`, which quietly constructs `AgentRuntime::new()` —
/// `with_catalog(LogBranchCatalog::in_memory(..))`, so `storage: None`, no `ArenaPageStore`, no
/// CoW pages, no reaper, and a private in-memory effect log (`execution/session.rs:20`).
///
/// That is a real path, but it is NOT the path the shipped engine takes. `src/cli/cli.rs` builds
/// `with_storage`/`reopen_with_storage` over an `ArenaPageStore`, so the STAGED arm's "agent
/// staging path" was the in-memory overlay rather than the arena the product actually writes to.
/// The PLAIN arm was unaffected — ordinary DML never touches the runtime — which is exactly why
/// the defect was invisible: one arm of a two-arm comparison silently changed meaning.
///
/// This is the `d90_delta_vs_chunk` / `cli.rs` wiring: a real branch-catalog sidecar, a real
/// arena, a `ServerContext` that designates the runtime, and every session built from it.
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
        // The arena floor must sit ABOVE where the ordinary table grows to, or the build runs out
        // of pages below the reserved region. Sized off the row count for the reason D90 records:
        // a copied constant fails as a mid-run allocation error at the largest size only.
        let arena_base: u32 = ((rows / 40) as u32 + 4096).next_power_of_two();
        let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), arena_base).unwrap());
        // `cli.rs:120` does this, so the measured configuration matches the shipped one. ⚠ It can
        // only ADD cost to a staged write (the free-space map is persisted rather than dropped),
        // never remove it, so it cannot flatter the STAGED arm this harness is testing.
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

fn build(rows: i64) -> (Db, Session) {
    let mut db = Db::new(rows);
    // `ctx.session()`, never `Session::new()` — see the block on `Db`. Both this session and the
    // agent session in `main` now come from the same designated runtime, which is what
    // `src/pgwire/mod.rs` does for every connection.
    let mut s = db.ctx.session();
    db.exec("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut s);
    for i in 1..=rows {
        db.exec(&format!("INSERT INTO t VALUES ({i}, 0);"), &mut s);
    }
    (db, s)
}

fn median(v: &mut Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    println!("D71 — point UPDATE on the primary key: descent or scan?");
    println!("{}", ferrodb::build_provenance());
    let sizes: Vec<i64> = std::env::var("D71_SIZES")
        .unwrap_or_else(|_| "1000,2000,4000,8000,16000".to_string())
        .split(',').filter_map(|v| v.parse().ok()).collect();
    let n: usize = std::env::var("D71_N").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
    let staged = std::env::var("D71_ARM").map(|a| a == "staged").unwrap_or(false);
    // `D71_OP=insert` times branch_insert's duplicate-key check instead of an UPDATE.
    let insert_arm = std::env::var("D71_OP").map(|o| o == "insert").unwrap_or(false);
    println!("arm = {}, {n} updates per size\n", if staged { "STAGED (inside an agent session)" } else { "PLAIN" });
    println!("  table rows   median ms   ms per 1000 rows   fsyncs/update");
    let mut first: Option<(i64, f64)> = None;
    for &rows in &sizes {
        let (mut db, mut s) = build(rows);
        let mut a = db.ctx.session();
        if staged {
            db.exec("BEGIN AGENT SESSION AS 'd71';", &mut a);
        }
        let sess: &mut Session = if staged { &mut a } else { &mut s };
        let (f0, _) = fsync_counters();
        let mut samples = Vec::new();
        for i in 0..n {
            // Spread the key across the WHOLE table so a scan cannot be short-circuited by always
            // hitting row 1 — which would make a seq scan look O(1) and is the obvious way to get
            // this measurement wrong.
            let id = 1 + (i as i64 * 7919) % rows;
            let t = Instant::now();
            if insert_arm {
                // Fresh keys ABOVE the built range, so every insert is a genuine non-duplicate and
                // the duplicate-key CHECK is what is being timed, not an early refusal.
                db.exec(&format!("INSERT INTO t VALUES ({}, {i});", rows + 1 + i as i64), sess);
            } else {
                db.exec(&format!("UPDATE t SET v = {i} WHERE id = {id};"), sess);
            }
            samples.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        let (f1, _) = fsync_counters();
        let med = median(&mut samples);
        println!("  {rows:>10}   {med:>9.3}   {:>16.4}   {:>13.2}",
                 med / (rows as f64 / 1000.0), (f1 - f0) as f64 / n as f64);
        if first.is_none() { first = Some((rows, med)); }
        if let Some((r0, m0)) = first {
            if rows != r0 {
                println!("             ^ {:.1}x the rows, {:.2}x the time", rows as f64 / r0 as f64, med / m0);
            }
        }
    }
    println!();
    println!("FLAT median ms  -> a DESCENT (O(log N), the index is used).");
    println!("LINEAR median ms -> a SCAN (O(N)); flat 'ms per 1000 rows' is the signature.");
}
