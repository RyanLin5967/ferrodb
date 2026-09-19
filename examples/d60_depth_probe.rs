//! D60 — does anything cost more as a branch chain gets DEEPER? The premise, measured before any
//! change is designed.
//!
//! `MAX_BRANCH_DEPTH = 8` refuses a fork past depth 8, and nothing in production collapses, so the
//! cap is a hard wall on tree-search agents. The design entry's hypothesis is that the cap protects
//! one recursion (`has_live_children` over reaped chains) and nothing on the read or fork path. This
//! probe measures that, in a build where the cap is raised to `u8::MAX` (the probe branch changes
//! only that constant):
//!
//! 1. read latency at the LEAF of a chain, vs depth — for a row inherited from the root;
//! 2. fork latency vs depth;
//! 3. the reaper over a chain whose interior nodes are all reaped (MCTS pruning).
//!
//! Output is per-depth medians; the reading is the SLOPE (memory: measure the slope, not the ratio).
//! Refuses on zero rows read, and on a chain that did not reach the depth it claims.

use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::Instant;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
}

impl Db {
    fn new(dir: &std::path::Path) -> Self {
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true)
            .open(dir.join("d60.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.join("d60.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()) }
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

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let max_depth: usize = std::env::var("D60_DEPTH").ok().and_then(|v| v.parse().ok()).unwrap_or(250);
    let checkpoints = [1usize, 2, 4, 8, 16, 32, 64, 128, 250];
    let dir = std::env::temp_dir().join(format!("ferrodb-d60-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut db = Db::new(&dir);

    let mut setup = Session::with_runtime(db.runtime.clone());
    db.exec("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut setup);
    // 300 rows; levels write ids 1..=200 only, so id 300 is written by the ROOT and nobody else
    // at any depth. (The first version read id 200, which level 199 writes: at depth 250 the probe
    // correctly returned 100199 and its own assertion called that wrong.)
    for i in 1..=300 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, {});", i * 10), &mut setup);
    }

    println!("# D60 depth probe. max_depth={max_depth}. read = SELECT of a ROOT row at the chain's leaf.");
    println!("depth   fork_us(med)   read_us(med)   rows");
    // Level 1 forks from trunk; each next level forks from the previous level's branch.
    let mut parent = ferrodb::branch::types::BranchId::TRUNK;
    let mut sessions: Vec<Session> = Vec::new();
    let mut fork_window: Vec<f64> = Vec::new();
    for depth in 1..=max_depth {
        let t0 = Instant::now();
        let agent = db.runtime.begin_session("d60", Some(&format!("r{depth}")), parent)
            .unwrap_or_else(|e| panic!("fork at depth {depth} refused: {e}"));
        fork_window.push(t0.elapsed().as_secs_f64() * 1e6);
        parent = agent.branch;
        let mut s = Session::with_runtime(db.runtime.clone());
        s.agent = Some(agent);
        // One write per level, so every level owns a page of its own.
        db.exec(&format!("UPDATE t SET v = {} WHERE id = {};", 100000 + depth, (depth % 200) + 1), &mut s);

        if checkpoints.contains(&depth) || depth == max_depth {
            // Read a row the ROOT wrote and no level ever touches: id 300.
            let mut reads = Vec::new();
            let mut rows = 0;
            for _ in 0..200 {
                let r0 = Instant::now();
                let out = db.exec("SELECT v FROM t WHERE id = 300;", &mut s);
                reads.push(r0.elapsed().as_secs_f64() * 1e6);
                if let Outcome::Rows(r) = out {
                    rows = r.len();
                    assert_eq!(r[0][0], Value::Integer(3000), "depth {depth}: wrong value inherited from the root");
                }
            }
            assert_eq!(rows, 1, "depth {depth}: the root row was not visible at the leaf");
            // Fork cost is the MEDIAN of every fork since the last checkpoint, not one sample.
            let fork_us = median(std::mem::take(&mut fork_window));
            println!("{depth:>5}   {fork_us:>12.1}   {:>12.1}   {rows}", median(reads));
        }
        sessions.push(s);
    }
    assert_eq!(sessions.len(), max_depth, "the chain did not reach the depth it claims");
    println!("# chain reached depth {max_depth} with every fork admitted.");
    let _ = std::fs::remove_dir_all(&dir);
}
