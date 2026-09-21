//! D103 — two sibling branches can be merged, because the fork point is now computed.
//!
//! # The capability that was missing
//!
//! Every merge path in `agent_sql::runtime` opens with `parent_id.unwrap_or(TRUNK)`. That is not a
//! policy choice, it is the only thing those paths could do: without an LCA there is no fork point
//! for any pair other than a branch and its own parent, and `tel::engine::ThreeWayMerger::merge`
//! says so by taking the parameter as `_lca` and never reading it.
//!
//! The consequence is the workload `SIMULATE` actually runs: K candidates forked off one base can
//! be admitted only **one at a time into that base**, and every candidate after the first is
//! re-scored against a base the previous admission moved. Composing candidate into candidate was
//! not slow — it was not expressible.
//!
//! # What is asserted here, and what would make each assertion fail
//!
//! * `merging_two_siblings_was_impossible_before_a_fork_point_could_be_computed` — the control.
//!   It reconstructs, against the shipped API, what the old shape could reach: `MERGE` targets
//!   `parent_id` and there is no other door. It fails if some path to a sibling merge existed all
//!   along, which would mean this row builds something already present.
//! * `two_siblings_merge_and_the_target_carries_both_branches_work` — the capability itself.
//! * `the_fork_point_is_computed_not_assumed` — the LCA of two cousins is their real meeting
//!   point, not trunk and not either parent. An implementation that returned `parent_id` fails it.
//! * `only_one_side_moved_the_cell` / `both_siblings_moved_the_same_cell_and_it_conflicts` — the
//!   two directions of the three-way decision. A merge that ignored the base passes the first and
//!   fails the second; one that treated every shared cell as a conflict does the reverse.
//! * `a_sibling_merge_refuses_an_ancestor_and_refuses_a_stranger` — the two refusals that keep the
//!   fork point from being fabricated.
//! * `the_fork_point_query_costs_fewer_hops_than_the_walk_it_replaces` — the complexity claim,
//!   stated in jump-pointer dereferences rather than in seconds, because this box runs a build
//!   fleet and a wall clock here would measure the fleet.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::{row_id_of, AgentRuntime, ExecCtx, RunIdentity};
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const ARENA_BASE: u32 = 1024;

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
            .open(dir.path().join("pages.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("pages.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);

        let branches = Arc::new(LogBranchCatalog::in_memory(1));
        let store = Arc::new(
            ArenaPageStore::new(
                bp.clone(),
                Arc::clone(&branches) as Arc<dyn ferrodb::branch::BranchCatalog>,
                ARENA_BASE,
            )
            .unwrap(),
        );
        let runtime = Arc::new(
            AgentRuntime::with_storage(
                branches,
                Arc::new(MemEffectLog::new()),
                Arc::clone(&store) as Arc<dyn PageStore>,
            )
            .unwrap(),
        );
        Db { catalog, bp, txn, runtime, _dir: dir }
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
        assert_eq!(stmts.len(), 1, "expected one statement: {}", sql);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    /// The runtime, cloned out so it is not borrowed from `db` while `db.catalog` is.
    fn rt(&self) -> Arc<AgentRuntime> {
        self.runtime.clone()
    }

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER, note INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 100, 0);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 200, 0);", &mut s);
    }

    /// Open an agent session forked from `parent`, returning its branch and a SQL session bound
    /// to it.
    ///
    /// ⚠ **The SQL surface cannot express a nested fork.** `BEGIN AGENT SESSION` refuses when a
    /// session is already open on the connection, so `bind_agent` only ever sees `current == None`
    /// and every session started through SQL forks from trunk. A chain therefore has to be built
    /// through `begin_session_as`, which is not a test-only door: it is exactly the call
    /// `dispatch::exec_agent` makes for the SQL statement, with the parent it would have passed.
    fn fork_from(&mut self, agent: &str, parent: Option<BranchId>) -> (BranchId, Session) {
        let run = format!("r_{agent}");
        let handle = self
            .runtime
            .begin_session_as(
                RunIdentity { agent_id: agent, run_id: Some(&run), model: None, prompt: None },
                parent.unwrap_or(BranchId::TRUNK),
            )
            .unwrap_or_else(|e| panic!("forking {agent} from {parent:?}: {e}"));
        let branch = handle.branch;
        let mut s = self.session();
        s.agent = Some(handle);
        (branch, s)
    }

    /// What the SQL surface reports for a cell on a branch.
    fn qty(&mut self, branch_name: &str, id: i32) -> Option<i32> {
        self.cell("qty", branch_name, id)
    }

    /// What the shared tables hold, with no branch qualifier.
    fn trunk_qty(&mut self, id: i32) -> Option<i32> {
        let mut s = self.session();
        let sql = format!("SELECT qty FROM inventory WHERE id = {id};");
        match self.ok(&sql, &mut s) {
            Outcome::Rows(rows) => rows.first().and_then(|r| match r.first() {
                Some(Value::Integer(i)) => Some(*i),
                _ => None,
            }),
            _ => panic!("expected rows from {sql}"),
        }
    }

    fn cell(&mut self, col: &str, branch_name: &str, id: i32) -> Option<i32> {
        let mut s = self.session();
        let sql =
            format!("SELECT {col} FROM inventory AS OF BRANCH {branch_name} WHERE id = {id};");
        match self.ok(&sql, &mut s) {
            Outcome::Rows(rows) => rows.first().and_then(|r| match r.first() {
                Some(Value::Integer(i)) => Some(*i),
                _ => None,
            }),
            _ => panic!("expected rows from {sql}"),
        }
    }
}

fn rid(id: i32) -> u64 {
    row_id_of(&[Value::Integer(id)]).0
}

fn branch_name(b: BranchId) -> String {
    format!("b_{}", b.id)
}

// -------------------------------------------------------------------------------------------
// The control: what the old shape could reach
// -------------------------------------------------------------------------------------------

/// **Before a fork point could be computed, this merge had no door.**
///
/// The only merge the runtime offered was `merge(ctx, branch)`, whose target is fixed at
/// `parent_id.unwrap_or(TRUNK)` — the branch cannot say where it is going. So the way to get two
/// siblings' work into one place was to merge them one at a time into the shared parent, and the
/// second candidate is then scored against a base the first admission moved.
///
/// That is asserted here rather than described: `b` merges to trunk, and `a`'s subsequent merge of
/// the SAME cell is refused as a conflict, having been scored against a base that moved. The row
/// exists to fail if a sibling-to-sibling path existed all along.
#[test]
fn merging_two_siblings_was_impossible_before_a_fork_point_could_be_computed() {
    let mut db = Db::new();
    db.seed();

    let (a, mut sa) = db.fork_from("agent-a", None);
    let (b, mut sb) = db.fork_from("agent-b", None);

    // Siblings: both forked from trunk, neither is an ancestor of the other.
    let fork = db.runtime.fork_point(a, b).unwrap();
    assert_eq!(fork.branch, BranchId::TRUNK, "two trunk forks meet at trunk");
    assert_ne!(fork.branch, a);
    assert_ne!(fork.branch, b);

    db.ok("UPDATE inventory SET qty = 110 WHERE id = 1;", &mut sa);
    db.ok("UPDATE inventory SET qty = 120 WHERE id = 1;", &mut sb);

    // The only door: each merges into the parent. The first one lands.
    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let first = rt.merge(&mut ctx, b).unwrap();
    assert!(!first.outcome.is_conflict(), "the first candidate must land: {:?}", first.outcome);

    // The second is now scored against a base the first admission moved, and is refused. This is
    // the defect: the two candidates never disagreed with each other about anything except a cell
    // they both wrote, and there was nowhere to compose them.
    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let second = rt.merge(&mut ctx, a).unwrap();
    assert!(
        second.outcome.is_conflict(),
        "the second candidate was expected to be refused against a moved base, got {:?}",
        second.outcome
    );
}

// -------------------------------------------------------------------------------------------
// The capability
// -------------------------------------------------------------------------------------------

/// **Two siblings merge, and the target then carries both branches' work.**
///
/// The same two candidates as the control, composed into each other instead of into the parent.
/// `a`'s row 1 and `b`'s row 2 are disjoint, so the composition is clean and `b` ends up holding
/// both — which is what lets ONE merge reach the parent instead of two serialised admissions.
#[test]
fn two_siblings_merge_and_the_target_carries_both_branches_work() {
    let mut db = Db::new();
    db.seed();

    let (a, mut sa) = db.fork_from("agent-a", None);
    let (b, mut sb) = db.fork_from("agent-b", None);

    db.ok("UPDATE inventory SET qty = 111 WHERE id = 1;", &mut sa);
    db.ok("UPDATE inventory SET qty = 222 WHERE id = 2;", &mut sb);

    // Before the merge, each sibling sees only its own write.
    let (na, nb) = (branch_name(a), branch_name(b));
    assert_eq!(db.qty(&na, 1), Some(111));
    assert_eq!(db.qty(&na, 2), Some(200), "a must not see b's write");
    assert_eq!(db.qty(&nb, 1), Some(100), "b must not see a's write");
    assert_eq!(db.qty(&nb, 2), Some(222));

    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let report = rt.merge_into(&mut ctx, a, b).unwrap();
    assert!(
        !report.outcome.is_conflict(),
        "disjoint rows must compose cleanly, got {:?}",
        report.outcome
    );
    assert!(report.applied, "a clean sibling merge must stage its rows");
    assert_eq!(report.fork.branch, BranchId::TRUNK);
    assert_eq!(report.from, a);
    assert_eq!(report.into, b);

    // b now holds both. This is the whole point: one branch carrying K candidates' work.
    assert_eq!(db.qty(&nb, 1), Some(111), "b did not take a's write");
    assert_eq!(db.qty(&nb, 2), Some(222), "b lost its own write");
    // And a is untouched: the merge is one-directional.
    assert_eq!(db.qty(&na, 2), Some(200), "the source must not have been written to");

    // The composed rows reached the branch's OWN page tree, not just its map — so the page-derived
    // DIFF sees them too. A wiring that only updated the workspace map would pass every assertion
    // above and fail this one.
    let on_pages = db.runtime.get_row(b, "inventory", rid(1)).unwrap();
    assert_eq!(
        on_pages.as_ref().and_then(|r| r.get(1)),
        Some(&Value::Integer(111)),
        "the merged row never reached b's copy-on-write tree"
    );

    // And the single admission the capability exists to enable: b merges once, carrying both.
    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let merged = rt.merge(&mut ctx, b).unwrap();
    assert!(!merged.outcome.is_conflict(), "the combined branch must land: {:?}", merged.outcome);
    // Read trunk back one row at a time: this surface has no ORDER BY, and a test that sorted in
    // Rust would be asserting about its own sort rather than about the merge.
    assert_eq!(db.trunk_qty(1), Some(111), "trunk lost a's write");
    assert_eq!(db.trunk_qty(2), Some(222), "trunk lost b's write");
}

/// **The fork point is computed, not read off `parent_id`.**
///
/// Two cousins: `a1` under `a`, `b1` under `b`, both `a` and `b` under trunk. Their meeting point
/// is trunk; their parents are different branches. An implementation that returned `parent_id`
/// answers `a` here, and an implementation that always answered trunk would pass this and fail the
/// sibling pair below it.
#[test]
fn the_fork_point_is_computed_not_assumed() {
    let mut db = Db::new();
    db.seed();

    let (a, _sa) = db.fork_from("agent-a", None);
    let (b, _sb) = db.fork_from("agent-b", None);
    let (a1, _sa1) = db.fork_from("agent-a1", Some(a));
    let (a2, _sa2) = db.fork_from("agent-a2", Some(a));
    let (b1, _sb1) = db.fork_from("agent-b1", Some(b));

    // Cousins meet at trunk.
    assert_eq!(db.runtime.fork_point(a1, b1).unwrap().branch, BranchId::TRUNK);
    // Siblings under `a` meet at `a`, which is NOT trunk — so the answer is not a constant.
    assert_eq!(db.runtime.fork_point(a1, a2).unwrap().branch, a);
    // A branch and its own parent meet at the parent: the reflexive case every existing merge
    // path already relies on, and it must keep holding.
    assert_eq!(db.runtime.fork_point(a1, a).unwrap().branch, a);
    // Symmetric.
    assert_eq!(db.runtime.fork_point(b1, a1).unwrap().branch, BranchId::TRUNK);
}

/// **The three-way decision, in the direction where the base lets the merge through.**
///
/// Only `a` moved `qty`; `b` moved `note` on the same row. Comparing after-images alone would
/// report both cells as differing between the two branches and conflict on `qty`. The base says
/// `b` never moved `qty`, so there is nothing to disagree with.
#[test]
fn only_one_side_moved_the_cell() {
    let mut db = Db::new();
    db.seed();

    let (a, mut sa) = db.fork_from("agent-a", None);
    let (b, mut sb) = db.fork_from("agent-b", None);

    db.ok("UPDATE inventory SET qty = 150 WHERE id = 1;", &mut sa);
    db.ok("UPDATE inventory SET note = 7 WHERE id = 1;", &mut sb);

    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let report = rt.merge_into(&mut ctx, a, b).unwrap();
    assert!(
        !report.outcome.is_conflict(),
        "different cells of one row must compose, got {:?}",
        report.outcome
    );

    let nb = branch_name(b);
    assert_eq!(db.cell("qty", &nb, 1), Some(150), "b did not take a's cell");
    assert_eq!(db.cell("note", &nb, 1), Some(7), "b lost its own cell");
}

/// **The same decision, in the direction where it must refuse.**
///
/// Both siblings assigned the same cell to different values from the same base. Under the default
/// `REJECT` policy that is a conflict, nothing is staged, and both branches stay alive so the
/// agent can retry against the predicate it was handed.
///
/// ⚠ This is the assertion that makes the previous one mean something. A merge that ignored the
/// base entirely — replaying the source over the target — passes `only_one_side_moved_the_cell`
/// and fails here.
#[test]
fn both_siblings_moved_the_same_cell_and_it_conflicts() {
    let mut db = Db::new();
    db.seed();

    let (a, mut sa) = db.fork_from("agent-a", None);
    let (b, mut sb) = db.fork_from("agent-b", None);

    db.ok("UPDATE inventory SET qty = 150 WHERE id = 1;", &mut sa);
    db.ok("UPDATE inventory SET qty = 160 WHERE id = 1;", &mut sb);

    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let report = rt.merge_into(&mut ctx, a, b).unwrap();
    assert!(
        report.outcome.is_conflict(),
        "two assignments to one cell from one base must conflict, got {:?}",
        report.outcome
    );
    assert!(!report.applied, "a conflicting merge must stage nothing");
    assert!(
        !report.outcome.conflicts().is_empty(),
        "a conflict must carry a report back to the agent"
    );

    // Nothing moved on either side.
    assert_eq!(db.qty(&branch_name(b), 1), Some(160), "the target was written by a refused merge");
    assert_eq!(db.qty(&branch_name(a), 1), Some(150), "the source was written by a refused merge");
}

/// **A row only the source touched lands even though the target never saw it.**
///
/// The target's `base_rows` has no entry for row 2, so the fork-point image has to come from the
/// source's. A merge that required both sides to have a base would drop this row silently.
#[test]
fn a_row_the_target_never_touched_still_carries_its_fork_point_image() {
    let mut db = Db::new();
    db.seed();

    let (a, mut sa) = db.fork_from("agent-a", None);
    let (b, mut sb) = db.fork_from("agent-b", None);

    db.ok("UPDATE inventory SET qty = 250 WHERE id = 2;", &mut sa);
    db.ok("UPDATE inventory SET qty = 101 WHERE id = 1;", &mut sb);

    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let report = rt.merge_into(&mut ctx, a, b).unwrap();
    assert!(!report.outcome.is_conflict(), "{:?}", report.outcome);
    assert_eq!(report.rows.len(), 1, "one row moved, so one row outcome");

    let nb = branch_name(b);
    assert_eq!(db.qty(&nb, 2), Some(250));
    assert_eq!(db.qty(&nb, 1), Some(101));
}

/// **The two refusals that keep the fork point from being fabricated.**
///
/// A branch and its ancestor are not siblings — that pair has a publishing path already, and
/// silently doing something else under the same name is the failure this refuses. A branch merged
/// into itself has no fork to compose across.
#[test]
fn a_sibling_merge_refuses_an_ancestor_and_refuses_itself() {
    let mut db = Db::new();
    db.seed();

    let (a, _sa) = db.fork_from("agent-a", None);
    let (a1, _sa1) = db.fork_from("agent-a1", Some(a));

    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let err = rt.merge_into(&mut ctx, a1, a).unwrap_err().to_string();
    assert!(
        err.contains("ancestor"),
        "a child merged into its own parent must be refused as an ancestor pair, got: {err}"
    );

    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let err = rt.merge_into(&mut ctx, a, a).unwrap_err().to_string();
    assert!(err.contains("itself"), "merging a branch into itself must be refused, got: {err}");

    // And the ancestor direction refuses too, rather than fast-forwarding.
    let rt = db.rt();
    let (bp, txn) = (db.bp.clone(), db.txn.clone());
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let err = rt.merge_into(&mut ctx, a, a1).unwrap_err().to_string();
    assert!(err.contains("ancestor"), "got: {err}");
}

/// **The complexity claim, in the unit it is actually about.**
///
/// A chain of depth D, with two leaves hanging off the deepest branch. The fork-point query climbs
/// jump pointers; the parent-pointer walk it replaces climbs one level per step on each side. Both
/// numbers come back on `ForkPoint`, in the same unit, so they are directly comparable.
///
/// ⚠ Stated in dereferences and not in seconds on purpose. This box runs a build fleet at load
/// 20-60 and a 46x quiet-vs-loaded spread has been measured on it, so a duration here would report
/// the machine. An operation count does not move when the machine is busy.
#[test]
fn the_fork_point_query_costs_fewer_hops_than_the_walk_it_replaces() {
    let mut db = Db::new();
    db.seed();

    // Build a chain. Each link is a real `BEGIN AGENT SESSION ... FROM BRANCH`, so the ancestry
    // index is being asked about branches the production fork path minted.
    const DEPTH: usize = 40;
    let mut chain = Vec::new();
    let mut parent: Option<BranchId> = None;
    for i in 0..DEPTH {
        let (b, _s) = db.fork_from(&format!("chain-{i}"), parent);
        parent = Some(b);
        chain.push(b);
    }
    let deepest = *chain.last().unwrap();
    let (leaf_a, _la) = db.fork_from("leaf-a", Some(deepest));
    let (leaf_b, _lb) = db.fork_from("leaf-b", Some(deepest));

    let fork = db.runtime.fork_point(leaf_a, leaf_b).unwrap();
    assert_eq!(fork.branch, *chain.last().unwrap(), "the two leaves meet at their shared parent");

    // The control, from the same struct: one dereference per level on each side down to the fork
    // point. Both leaves sit one level below it, so this is small HERE — the point of the pair is
    // the next assertion, where the two branches are far apart.
    assert_eq!(fork.walk_hops, 2, "the leaves are one level below their fork point");

    // Now the case the index exists for: a leaf and the top of the chain. The walk is O(depth);
    // the jump-pointer query is O(log depth).
    let top = chain[0];
    let far = db.runtime.fork_point(leaf_a, top).unwrap();
    assert_eq!(far.branch, top, "a branch and its ancestor meet at the ancestor");
    assert_eq!(
        far.walk_hops, DEPTH as u64,
        "the walk control must be the depth difference, or it is not the walk"
    );
    assert!(
        far.hops < far.walk_hops,
        "the jump-pointer query cost {} hops against a walk of {} — no better than the walk it \
         replaces",
        far.hops,
        far.walk_hops
    );
    // log2(40) is under 6, and the query reads a jump entry from each side per level examined.
    assert!(
        far.hops <= 2 * (usize::BITS - DEPTH.leading_zeros()) as u64 + 2,
        "{} hops is not O(log {})",
        far.hops,
        DEPTH
    );
}

/// **The ancestry index answers about branches it was never explicitly told about.**
///
/// Nothing hooks the fork path: the index derives each chain from the branch catalog the first
/// time it is asked. This exercises that door by querying a branch created before any ancestry
/// call was ever made, and then querying it again — the second call must agree with the first, or
/// the hydration is not idempotent.
#[test]
fn the_index_hydrates_from_the_catalog_rather_than_from_a_fork_hook() {
    let mut db = Db::new();
    db.seed();

    let (a, _sa) = db.fork_from("agent-a", None);
    let (b, _sb) = db.fork_from("agent-b", None);
    let (a1, _sa1) = db.fork_from("agent-a1", Some(a));

    // First ask ever, about a branch three levels in.
    let first = db.runtime.fork_point(a1, b).unwrap();
    assert_eq!(first.branch, BranchId::TRUNK);
    // Asked again, the index is warm; the answer must not change.
    let second = db.runtime.fork_point(a1, b).unwrap();
    assert_eq!(first, second, "a warm index disagreed with a cold one");
}
