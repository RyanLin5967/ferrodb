//! D27 — the two properties the fork's deep clone provided, now provided by structural sharing.
//!
//! `AgentRuntime::begin_session_as` used to deep-clone the parent's five workspace maps. That cost
//! O(parent's working set) per fork and O(N·W) at fanout N (`bench/d27_fork_workspace_cost_*.txt`).
//! It is now a `PersistentMap` clone — an `Arc` bump — with writes copying only their own path.
//!
//! The clone's two load-bearing properties, named in the comment at the fork site, are:
//!
//! 1. **The parent's writes AFTER the fork stay invisible to the child.**
//! 2. **A read never walks the parent chain** — the pattern DESIGN.md rules out outright.
//!
//! Under a deep copy both were trivially true because nothing was shared. Under sharing they are
//! true for a different reason (nodes are immutable, and the child addresses its own root), so
//! they need testing directly rather than inheriting the old argument. That is what this file is.
//!
//! `src/agent_sql/persistent_map.rs` tests the same two properties at the data-structure level.
//! These tests exercise them through SQL, against the real runtime, which is where a wiring
//! mistake — a map shared that should have been cloned, or the reverse — would actually show up.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::types::BranchId;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
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
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("d27.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d27.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        match self.exec(sql, s) {
            Ok(o) => o,
            Err(e) => panic!("{sql} failed: {e}"),
        }
    }

    fn seed(&mut self, n: i32) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER, note VARCHAR(32));", &mut s);
        for i in 0..n {
            self.ok(&format!("INSERT INTO inventory VALUES ({i}, 0, 'seed');"), &mut s);
        }
    }

    /// `id -> qty` as `branch_name` sees it. `None` when the branch no longer resolves.
    fn view(&mut self, branch_name: &str) -> Option<BTreeMap<i32, i32>> {
        let mut s = self.session();
        let sql = format!("SELECT id, qty FROM inventory AS OF BRANCH {branch_name};");
        let out = self.exec(&sql, &mut s).ok()?;
        Some(as_map(out))
    }

    fn trunk_view(&mut self) -> BTreeMap<i32, i32> {
        let mut s = self.session();
        as_map(self.ok("SELECT id, qty FROM inventory;", &mut s))
    }
}

fn as_map(out: Outcome) -> BTreeMap<i32, i32> {
    let rows = match out {
        Outcome::Rows(r) => r,
        _ => panic!("expected rows from a SELECT"),
    };
    rows.into_iter()
        .map(|r| match (&r[0], &r[1]) {
            (Value::Integer(id), Value::Integer(q)) => (*id, *q),
            other => panic!("unexpected row shape {other:?}"),
        })
        .collect()
}

/// Invariant 1, through SQL and at a size where the tree really has interior nodes.
///
/// The parent does three things after the fork, and each is a different way sharing could leak:
/// it OVERWRITES every key the child inherited (a payload swap on a shared node), it INSERTS many
/// new keys (which forces rotations, rewriting interior nodes the child also reaches), and it
/// DELETES one (which in this workspace is a `RowState::Deleted` payload, not a removal).
#[test]
fn a_parent_write_after_the_fork_stays_invisible_to_the_child() {
    const W: i32 = 200;
    let mut db = Db::new();
    db.seed(W);

    let mut parent = db.session();
    db.ok("BEGIN AGENT SESSION AS 'planner' RUN 'r1';", &mut parent);
    db.ok("UPDATE inventory SET qty = 1000;", &mut parent);
    let parent_branch: BranchId = parent.agent.as_ref().unwrap().branch;
    let parent_name = parent.agent.as_ref().unwrap().branch_name.clone();

    let child = db.runtime.begin_session("sub", Some("r2"), parent_branch).unwrap();
    let at_fork = db.view(&child.branch_name).expect("child resolves");
    assert_eq!(at_fork.len(), W as usize, "the child inherits the parent's whole working set");
    assert!(at_fork.values().all(|q| *q == 1000), "the child inherits the parent's staged values");

    // ---- now the parent moves, in three different ways ---------------------------------------
    db.ok("UPDATE inventory SET qty = 2000;", &mut parent);
    for i in W..(2 * W) {
        db.ok(&format!("INSERT INTO inventory VALUES ({i}, 3000, 'later');"), &mut parent);
    }
    db.ok("DELETE FROM inventory WHERE id = 7;", &mut parent);

    // The child must be byte-for-byte what it was at fork time.
    let after = db.view(&child.branch_name).expect("child still resolves");
    assert_eq!(
        after, at_fork,
        "the child saw one of the parent's post-fork writes: an overwrite, an insert or a delete"
    );
    assert_eq!(after.len(), W as usize);
    assert!(after.values().all(|q| *q == 1000));
    assert!(after.contains_key(&7), "the child saw the parent's DELETE");

    // **The control.** Every assertion above would also pass if the parent had done nothing at
    // all, so prove the parent really moved. Without this the test is unfalsifiable.
    let p = db.view(&parent_name).expect("parent resolves");
    assert_eq!(p.len(), (2 * W - 1) as usize, "the parent's own writes did not land");
    assert_eq!(p.get(&0), Some(&2000), "the parent's overwrite did not land");
    assert_eq!(p.get(&(2 * W - 1)), Some(&3000), "the parent's inserts did not land");
    assert!(!p.contains_key(&7), "the parent's DELETE did not land");

    // ...and main is untouched by either of them.
    let t = db.trunk_view();
    assert_eq!(t.len(), W as usize);
    assert!(t.values().all(|q| *q == 0));
}

/// The fanout case: N children of ONE live parent. Each must see the parent's fork-point state
/// plus its own writes, and nothing from any sibling.
///
/// This is the shape the O(N·W) cost came from, so it is the shape most likely to be broken by
/// replacing the copy with sharing.
#[test]
fn siblings_forked_from_one_parent_cannot_see_each_other() {
    const W: i32 = 64;
    const N: i32 = 8;
    let mut db = Db::new();
    db.seed(W);

    let mut parent = db.session();
    db.ok("BEGIN AGENT SESSION AS 'planner' RUN 'r1';", &mut parent);
    db.ok("UPDATE inventory SET qty = 1000;", &mut parent);
    let parent_branch: BranchId = parent.agent.as_ref().unwrap().branch;
    let parent_name = parent.agent.as_ref().unwrap().branch_name.clone();

    let kids: Vec<_> = (0..N)
        .map(|i| db.runtime.begin_session("sub", Some(&format!("r_{i}")), parent_branch).unwrap())
        .collect();

    // Each child writes a different row, on its own branch.
    for (i, k) in kids.iter().enumerate() {
        let mut s = db.session();
        s.agent = Some(k.clone());
        db.ok(&format!("UPDATE inventory SET qty = {} WHERE id = {i};", 7000 + i), &mut s);
    }

    for (i, k) in kids.iter().enumerate() {
        let v = db.view(&k.branch_name).expect("child resolves");
        assert_eq!(v.len(), W as usize, "child {i} lost rows");
        assert_eq!(v.get(&(i as i32)), Some(&(7000 + i as i32)), "child {i} lost its own write");
        for j in 0..N as usize {
            if i != j {
                assert_eq!(
                    v.get(&(j as i32)),
                    Some(&1000),
                    "child {i} saw sibling {j}'s write instead of the fork-point value"
                );
            }
        }
    }

    // The parent saw none of its children.
    let p = db.view(&parent_name).expect("parent resolves");
    assert!(p.values().all(|q| *q == 1000), "the parent saw a child's write");
}

/// Invariant 2: a read answers from the branch's OWN workspace, never by walking up to an
/// ancestor's.
///
/// The chain is built far past where the old depth cap stood — there is no deepest chain since
/// D60 — because a read that consulted ancestors would show at 64 levels where 8 might hide it.
/// Every ancestor is abandoned, which removes its workspace from the runtime entirely
/// (`seal` -> `state.workspaces.remove`). If a read consulted the parent chain, the deepest
/// child's inherited rows would vanish with them. They must not.
///
/// The positive control for this test lives in
/// `the_instrument_goes_blind_when_the_workspace_it_reads_is_gone` below, which proves that
/// removing the workspace a read DOES depend on changes this instrument's answer. Without that,
/// "the rows are still there after the ancestors died" would be consistent with the query never
/// having read a workspace at all.
#[test]
fn a_read_on_a_deep_child_does_not_consult_its_ancestors() {
    const W: i32 = 200;
    // **There is no deepest legal chain since D60** — the cap this used to derive from is gone,
    // and fork and read were measured flat to depth 250 (`bench/d60_depth_premise.txt`). So this
    // goes far past where the old ceiling was: if a read ever started consulting ancestors, a
    // 64-deep chain shows it where an 8-deep one might not.
    let depth = 64usize;
    let mut db = Db::new();
    db.seed(W);

    // The root of the chain stages the whole working set; every level below it only forks.
    let mut root = db.session();
    db.ok("BEGIN AGENT SESSION AS 'planner' RUN 'r1';", &mut root);
    db.ok("UPDATE inventory SET qty = 1000;", &mut root);
    let root_branch: BranchId = root.agent.as_ref().unwrap().branch;

    let mut ancestors: Vec<BranchId> = vec![root_branch];
    let mut cur = root_branch;
    for d in 0..depth {
        let s = db
            .runtime
            .begin_session("sub", Some(&format!("d_{d}")), cur)
            .unwrap_or_else(|e| panic!("fork at depth {d} refused: {e}"));
        cur = s.branch;
        ancestors.push(cur);
    }
    assert_eq!(ancestors.len(), depth + 1, "the chain is not as deep as this test claims");
    let deepest = ancestors.pop().expect("the chain has a deepest branch");
    let deepest_name = format!("b_{}", deepest.id);

    let before = db.view(&deepest_name).expect("deepest resolves");
    assert_eq!(before.len(), W as usize, "the deepest child inherited the root's working set");
    assert!(before.values().all(|q| *q == 1000));

    // ---- kill every ancestor -----------------------------------------------------------------
    for a in &ancestors {
        db.runtime.abandon(*a).unwrap_or_else(|e| panic!("abandon {a} failed: {e}"));
    }
    assert_eq!(
        db.runtime.run_activity().len(),
        1,
        "every ancestor workspace should be gone, leaving only the deepest child's"
    );

    let after = db.view(&deepest_name).expect("deepest still resolves with no ancestors alive");
    assert_eq!(
        after, before,
        "the deepest child's read depended on an ancestor's workspace -- that is the parent-chain \
         walk DESIGN.md rules out"
    );
    assert_eq!(after.len(), W as usize);
    assert!(after.values().all(|q| *q == 1000));
}

/// The positive control for the test above: prove this instrument CAN go blind.
///
/// A branch stages rows, the read sees them, the branch is abandoned, and the read must stop
/// seeing them. If this failed — if the query answered the same either way — then the previous
/// test would prove nothing, because its "the rows are still there" would not be evidence that a
/// workspace was read at all.
#[test]
fn the_instrument_goes_blind_when_the_workspace_it_reads_is_gone() {
    const W: i32 = 32;
    let mut db = Db::new();
    db.seed(W);

    let mut victim = db.session();
    db.ok("BEGIN AGENT SESSION AS 'doomed' RUN 'r1';", &mut victim);
    db.ok("UPDATE inventory SET qty = 9999;", &mut victim);
    let branch = victim.agent.as_ref().unwrap().branch;
    let name = victim.agent.as_ref().unwrap().branch_name.clone();

    let seen = db.view(&name).expect("the branch resolves while it is alive");
    assert!(seen.values().all(|q| *q == 9999), "the read is not reading the workspace at all");

    db.runtime.abandon(branch).unwrap();

    // Either the branch no longer resolves, or it resolves to trunk's values. Both are "blind";
    // what must NOT happen is still reporting 9999.
    match db.view(&name) {
        None => {}
        Some(v) => assert!(
            !v.values().any(|q| *q == 9999),
            "the read still reported an abandoned workspace's staged rows, so this instrument \
             cannot tell a live workspace from a dead one and proves nothing about either"
        ),
    }
}

/// `schema_edits` is the one workspace field that is NOT a `PersistentMap`.
///
/// It is an `Arc<Vec<_>>` mutated through `Arc::make_mut`, because the number of ALTERs staged on
/// one branch is bounded by the DDL an agent typed and a tree would be a second data structure
/// for a collection that is never large. `make_mut` clones when the `Arc` is shared, which is what
/// keeps a parent's later `ALTER` out of a child that forked before it — and "make_mut clones
/// when shared" is an argument, not evidence. So here it is as evidence.
#[test]
fn a_parent_schema_edit_after_the_fork_stays_invisible_to_the_child() {
    let mut db = Db::new();
    db.seed(4);

    let mut parent = db.session();
    db.ok("BEGIN AGENT SESSION AS 'planner' RUN 'r1';", &mut parent);
    db.ok("ALTER TABLE inventory ADD COLUMN first VARCHAR(16);", &mut parent);
    let parent_branch: BranchId = parent.agent.as_ref().unwrap().branch;

    let child = db.runtime.begin_session("sub", Some("r2"), parent_branch).unwrap();
    let inherited = db.runtime.pending_schema_edits(child.branch);
    assert_eq!(inherited.len(), 1, "the child must inherit the parent's staged ALTER");

    // The parent alters again, AFTER the fork.
    db.ok("ALTER TABLE inventory ADD COLUMN second VARCHAR(16);", &mut parent);

    // The control first: prove the parent's second ALTER actually landed somewhere, or the
    // assertion below would pass for the wrong reason.
    assert_eq!(
        db.runtime.pending_schema_edits(parent_branch).len(),
        2,
        "the parent's second ALTER did not land, so this test proves nothing"
    );

    let after = db.runtime.pending_schema_edits(child.branch);
    assert_eq!(
        after.len(),
        1,
        "the child saw the parent's post-fork ALTER: Arc::make_mut mutated a shared Vec"
    );
    assert_eq!(after, inherited, "the child's staged edits changed after the fork");
}
