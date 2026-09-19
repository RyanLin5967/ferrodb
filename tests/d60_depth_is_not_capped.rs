//! D60 — a branch chain may be any depth, and the things depth used to protect still hold.
//!
//! `MAX_BRANCH_DEPTH = 8` refused the ninth fork of any chain, and nothing in production called
//! `collapse`, so a tree-search agent (BranchBench's own workload: fork from your own children)
//! hit a wall at eight. Measured before removing it (`bench/d60_depth_premise.txt`): fork and read
//! are FLAT from depth 1 to 250, because a branch's root is its parent's root at fork and a read
//! never walks ancestry.
//!
//! Three things had to survive the removal, and each is a test here:
//!
//! 1. a chain far past the old cap forks, and every level reads what it inherited;
//! 2. reclamation over a chain of REAPED interior nodes — D16's rule that such a node is a pin —
//!    must still answer correctly, and must not recurse per level (it was mutually recursive with
//!    `live_child_at`, bounded only by the cap);
//! 3. the `depth` field widened from `u8` to `u32`, so a core record written by an older build is
//!    three bytes short and must still load.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::record::{BranchRecord, CORE_BYTES};
use ferrodb::branch::types::{BranchId, BranchState, Epoch, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
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
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true)
            .open(dir.path().join("d60.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d60.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
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

/// A chain 64 deep — eight times the old cap — where every level writes, and the leaf still sees
/// what the root wrote. Before D60 the ninth `begin_session` failed with "is at ancestry depth 9".
#[test]
fn a_chain_far_past_the_old_cap_forks_and_still_reads_what_the_root_wrote() {
    const DEPTH: usize = 64;
    let mut db = Db::new();
    let mut setup = Session::with_runtime(db.runtime.clone());
    db.exec("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut setup);
    for i in 1..=80 {
        db.exec(&format!("INSERT INTO t VALUES ({i}, {});", i * 10), &mut setup);
    }

    let mut parent = BranchId::TRUNK;
    let mut sessions = Vec::new();
    for level in 1..=DEPTH {
        let agent = db
            .runtime
            .begin_session("d60", Some(&format!("r{level}")), parent)
            .unwrap_or_else(|e| panic!("fork at depth {level} refused: {e}"));
        parent = agent.branch;
        let mut s = Session::with_runtime(db.runtime.clone());
        s.agent = Some(agent);
        // Each level writes its own row, so every level has state of its own to lose.
        db.exec(&format!("UPDATE t SET v = {} WHERE id = {level};", 900000 + level), &mut s);
        sessions.push(s);
    }

    let deepest = sessions.last_mut().expect("a chain");
    // Row 80 is written by the root and by nobody below it.
    match db.exec("SELECT v FROM t WHERE id = 80;", deepest) {
        Outcome::Rows(r) => assert_eq!(r[0][0], Value::Integer(800), "the leaf lost the root's row"),
        _ => panic!("expected rows"),
    }
    // ...and the leaf sees its own write, and the write of the level above it.
    match db.exec(&format!("SELECT v FROM t WHERE id = {DEPTH};"), deepest) {
        Outcome::Rows(r) => assert_eq!(r[0][0], Value::Integer((900000 + DEPTH) as i32)),
        _ => panic!("expected rows"),
    }
    match db.exec("SELECT v FROM t WHERE id = 1;", deepest) {
        Outcome::Rows(r) => assert_eq!(r[0][0], Value::Integer(900001), "level 1's write is not inherited"),
        _ => panic!("expected rows"),
    }
}

/// D16's rule — a reaped interior node with a live descendant is a PIN — over a chain far longer
/// than any recursion should carry. In `TableBranchCatalog`, `has_live_children` resolves each
/// child's RECORD and, for a reaped one, explores its subtree; that was mutual recursion with
/// `live_child_at`, so its call depth was this chain's length and the cap was the only thing
/// keeping it to 8. It is iterative now, and this is what says so.
///
/// (Deliberately the TABLE catalog, not the log one: `LogBranchCatalog::has_live_children` answers
/// from the parent's stored `live_children` array without resolving anything, so it walks nothing
/// and would pass this test no matter how the walk were written.)
#[test]
fn reclamation_walks_a_long_reaped_chain_without_recursing_per_level() {
    // **2,000 levels, chosen by measurement rather than by feel.** With the pre-D60 recursive walk
    // restored, this test PASSES at depth 500 and dies with `fatal runtime error: stack overflow`
    // at 1,000 — so 2,000 is double the measured death point, and still an eighth of the 5,000 the
    // first version used, which cost 99 s of every suite run to prove the same thing.
    const DEPTH: usize = 2_000;
    let dir = tempfile::tempdir().unwrap();
    let c = TableBranchCatalog::open_sidecar(&dir.path().join("b.branchcat"), 1).unwrap();
    let mut chain = Vec::with_capacity(DEPTH);
    let mut cur = BranchId::TRUNK;
    for level in 1..=DEPTH {
        cur = c
            .fork(cur, LeaseDeadline(u64::MAX))
            .unwrap_or_else(|e| panic!("fork at depth {level} refused: {e}"))
            .branch_id;
        chain.push(cur);
    }
    assert_eq!(c.get(cur).unwrap().depth, DEPTH as u32, "depth stopped counting");

    // Reap every INTERIOR node and leave the leaf alive: MCTS pruning, and the shape that makes a
    // long chain of reaped nodes with one live descendant at the bottom.
    let leaf = *chain.last().unwrap();
    for b in &chain[..chain.len() - 1] {
        c.set_state(*b, BranchState::Live, BranchState::Reaping).unwrap();
        c.set_state(*b, BranchState::Reaping, BranchState::Reaped).unwrap();
    }
    assert_eq!(c.get(leaf).unwrap().state, BranchState::Live, "the leaf must still be live");

    // The question the reaper asks of the root: is anything under me still alive? Every level in
    // between is reaped, so the answer has to come from the leaf, 5,000 levels down.
    assert!(
        c.has_live_children(BranchId::TRUNK.id).unwrap(),
        "a live branch {DEPTH} levels down was not seen through {} reaped interior nodes: D16's \
         pin rule does not survive a deep chain",
        DEPTH - 1
    );

    // And when the leaf goes too, nothing under trunk is alive any more.
    c.set_state(leaf, BranchState::Live, BranchState::Reaping).unwrap();
    c.set_state(leaf, BranchState::Reaping, BranchState::Reaped).unwrap();
    assert!(
        !c.has_live_children(BranchId::TRUNK.id).unwrap(),
        "with every branch reaped the chain still reads as pinning trunk"
    );
}

/// The depth field went from `u8` to `u32`, so a core record written by an older build is three
/// bytes short. It must still load, with its byte read as the depth — the same tolerant-read
/// discipline this module already uses for envelopes.
#[test]
fn a_core_record_written_with_a_one_byte_depth_still_loads() {
    let mut r = BranchRecord::trunk(1, LeaseDeadline(7));
    r.depth = 5;
    r.parent_id = Some(BranchId::new(3, 0));
    r.fork_epoch = Epoch(9);
    let current = r.serialize_core();
    assert_eq!(current.len(), CORE_BYTES);

    // The old shape: everything up to `state`, then ONE byte of depth.
    let mut old = current[..CORE_BYTES - 4].to_vec();
    old.push(5);
    assert_eq!(old.len(), CORE_BYTES - 3, "the old core record was three bytes shorter");

    let decoded = BranchRecord::deserialize_core(&old).expect("an older core record must load");
    assert_eq!(decoded.depth(), 5, "the one-byte depth was not read");
    assert_eq!(decoded.branch_id(), r.branch_id);
    assert_eq!(decoded.fork_epoch(), Epoch(9));

    // And the new shape round-trips a depth no byte could hold.
    r.depth = 100_000;
    let wide = BranchRecord::deserialize_core(&r.serialize_core()).expect("round trip");
    assert_eq!(wide.depth(), 100_000);

    // A length that is neither is still refused, rather than being read as one of them.
    assert!(BranchRecord::deserialize_core(&current[..CORE_BYTES - 1]).is_err());
}
