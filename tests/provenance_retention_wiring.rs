//! Retention at the EDGES OF THE WIRING, end to end through the agent SQL surface.
//!
//! Design authority: DESIGN.md section 2 and exit criterion 10.
//!
//! The region derivation and the version clock are covered by `provenance_scan_cascade.rs`. What
//! this file covers is the set of paths that reach — or fail to reach — that machinery at all:
//!
//! - the scan `UPDATE ... WHERE` and `DELETE ... WHERE` perform, which is the read-modify-write
//!   shape agents actually use and which used to be retained nowhere;
//! - a read that observed an ABSENCE, which has no version to name;
//! - a read whose session was sealed underneath it;
//! - a task that was explicitly discarded, whose reads must stop generating edges.
//!
//! Every test states the input shape that fails without the fix, and every one carries its own
//! anti-vacuity half — a case in the same run that must NOT produce an edge — because a dependency
//! finder that names everything is as useless as one that names nothing.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::MergeReport;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::provenance::revert::RevertPlan;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::ids::TxnId;
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
            .open(dir.path().join("wiring.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("wiring.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    /// A connection sharing this database's agent runtime, so branches are mutually visible. Two of
    /// these is what makes "a second connection seals a live session's branch" expressible.
    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, session: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {}", sql);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), session)
    }

    fn ok(&mut self, sql: &str, session: &mut Session) -> Outcome {
        match self.exec(sql, session) {
            Ok(o) => o,
            Err(e) => panic!("{} failed: {}", sql, e),
        }
    }

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 5);", &mut s);
    }

    /// Every row of the table, read outside any agent session (so nothing is retained).
    fn all(&mut self) -> Vec<Vec<Value>> {
        let mut s = self.session();
        rows(self.ok("SELECT id, qty FROM inventory;", &mut s))
    }

    fn has_row(&mut self, id: i32) -> bool {
        self.all().iter().any(|r| r[0] == Value::Integer(id))
    }

    fn qty_of(&mut self, id: i32) -> i32 {
        let row = self
            .all()
            .into_iter()
            .find(|r| r[0] == Value::Integer(id))
            .unwrap_or_else(|| panic!("row {} missing", id));
        match row[1] {
            Value::Integer(i) => i,
            ref other => panic!("qty is not an integer: {:?}", other),
        }
    }
}

fn rows(out: Outcome) -> Vec<Vec<Value>> {
    match out {
        Outcome::Rows(r) => r,
        _ => panic!("expected rows"),
    }
}

fn affected(out: Outcome) -> usize {
    match out {
        Outcome::Affected(n) => n,
        _ => panic!("expected an affected count"),
    }
}

fn agent(out: Outcome) -> AgentOutput {
    match out {
        Outcome::Agent(a) => a,
        Outcome::Rows(_) => panic!("expected an agent output, got rows"),
        _ => panic!("expected an agent output"),
    }
}

fn report(out: Outcome) -> MergeReport {
    match agent(out) {
        AgentOutput::Merge(m) => m,
        other => panic!("expected a merge report, got {}", other),
    }
}

fn plan(out: Outcome) -> RevertPlan {
    match agent(out) {
        AgentOutput::Revert(p) => p,
        other => panic!("expected a revert plan, got {}", other),
    }
}

/// One agent task that inserts a row and merges it, returning its merge id.
fn insert_and_merge(db: &mut Db, agent_id: &str, run_id: &str, id: i32, qty: i32) -> String {
    let mut s = db.session();
    db.ok(&format!("BEGIN AGENT SESSION AS '{}' RUN '{}';", agent_id, run_id), &mut s);
    db.ok(&format!("INSERT INTO inventory VALUES ({}, {});", id, qty), &mut s);
    let r = report(db.ok("MERGE;", &mut s));
    assert!(r.applied_to_target, "{} failed to merge: {}", agent_id, r);
    r.merge_id
}

// ---- the session was sealed underneath the read ------------------------------------------------

/// **BREAKING SHAPE: a second connection seals a live session's branch by name, and the first
/// connection then reads.** Workspace-absent is the reachable half of the pair of sequential guards
/// on the retention path, and it was a bare `None => return`: the read reported success, returned
/// real rows, and dropped its retention on the floor — the precise failure this lane exists to
/// remove, left in place while the unreachable half beside it was hardened.
#[test]
fn a_read_whose_session_was_sealed_underneath_it_is_refused_rather_than_silently_unretained() {
    let mut db = Db::new();
    db.seed();
    let m1 = insert_and_merge(&mut db, "restock-agent", "r_restock", 7, 30);

    let mut victim = db.session();
    db.ok("BEGIN AGENT SESSION AS 'victim' RUN 'r_victim';", &mut victim);
    let name = victim.agent.as_ref().unwrap().branch_name.clone();

    // The anti-vacuity half, and it has to come first: this fixture must be able to read AND retain
    // while the session is live, or "the read was refused" proves nothing about sealing.
    let before = rows(db.ok(
        "SELECT id, qty FROM inventory WHERE qty >= 20 AND qty < 50;",
        &mut victim,
    ));
    assert_eq!(before.len(), 2, "rows 1 and 7 are in [20, 50): {:?}", before);
    let mut main = db.session();
    let blocked = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert_eq!(
        blocked.blocked_by,
        vec![TxnId(2)],
        "a live session's scan is retained, so it is a dependent: {:?}",
        blocked.blocked_by
    );

    // A DIFFERENT connection seals the branch this session is holding.
    let mut other = db.session();
    db.ok(&format!("ABANDON BRANCH {name};"), &mut other);

    // The read must refuse. Before this it returned `Ok` with two rows.
    let msg = match db.exec("SELECT id, qty FROM inventory WHERE qty >= 20 AND qty < 50;", &mut victim) {
        Err(e) => e.to_string(),
        Ok(o) => panic!(
            "a read on a sealed branch reported success and retained nothing: {} row(s)",
            rows(o).len()
        ),
    };
    assert!(
        msg.contains("no agent session"),
        "the refusal does not name what happened: {msg}"
    );

    // And the rest of that session's surface agrees, which is the point: the read was the one
    // operation still reporting success on a branch every other operation refuses.
    assert!(
        db.exec("UPDATE inventory SET qty = 1 WHERE id = 1;", &mut victim).is_err(),
        "a write on a sealed branch was admitted"
    );
    assert!(db.exec("MERGE;", &mut victim).is_err(), "a merge on a sealed branch was admitted");
}

// ---- the read-modify-write shape ---------------------------------------------------------------

/// **BREAKING SHAPE: the dependent's read is the `WHERE` clause of its own `UPDATE`, with no
/// hand-written `SELECT` anywhere.** This is the shape agents actually use — read some rows, decide,
/// write — and it is the shape this lane exists to protect. `branch_update` calls `visible_rows`,
/// which returns every row, and then evaluates the bound clause against each one: a full scan by the
/// module's own definition, and none of it was retained, so cascade found ZERO dependents.
///
/// Measured before this fix: `blocked_by = []`, the halt-mode revert proceeded, and row 7 —
/// carrying the decider's qty 31 — was deleted with no name in any tree. The identical workload with
/// one extra `SELECT` over the same range gave `blocked_by = [TxnId(2)]`.
#[test]
fn an_update_whose_where_clause_scanned_names_its_dependents() {
    let mut db = Db::new();
    db.seed();

    // (a) restock-agent inserts row 7 INSIDE the range the decider is about to update over. txn 1.
    let m1 = insert_and_merge(&mut db, "restock-agent", "r_restock", 7, 30);
    // (b) bulk-agent inserts row 8 well OUTSIDE it, BEFORE the decider reads — so the anti-vacuity
    //     half below is decided by the region and not by the temporal rule.
    let m_out = insert_and_merge(&mut db, "bulk-agent", "r_bulk", 8, 500);

    // (c) the decider reads by UPDATE and by nothing else. txn 3.
    let mut d = db.session();
    db.ok("BEGIN AGENT SESSION AS 'decider' RUN 'r_decide';", &mut d);
    let touched = affected(db.ok(
        "UPDATE inventory SET qty = qty + 1 WHERE qty >= 20 AND qty < 50;",
        &mut d,
    ));
    assert_eq!(touched, 2, "the clause matches rows 1 and 7; that scan is the read under test");
    let merged = report(db.ok("MERGE;", &mut d));
    assert!(merged.applied_to_target, "{}", merged);
    assert_eq!(db.qty_of(7), 31);
    // Held, not asserted yet. The causal claim is the headline of this test and it has to be the
    // assertion a lost-retention regression hits FIRST — asserting the metric here instead made
    // both fire-check mutants die on the metric and never reach the revert at all.
    let blind = merged.blind_writes.clone();

    // (d) HALT is the default, and the decider is named.
    let mut main = db.session();
    let halted = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(halted.is_blocked(), "the UPDATE's own scan consumed row 7");
    assert_eq!(
        halted.blocked_by,
        vec![TxnId(3)],
        "the decider is txn 3, got {:?}",
        halted.blocked_by
    );
    assert!(db.has_row(7), "a halted revert changes nothing");
    assert_eq!(db.qty_of(7), 31, "the dependent's work survived the revert it blocked");

    // (e) anti-vacuity: a write outside the scanned region is not a dependency, and its revert
    //     proceeds and really happens. Both merges are visible to the decider's snapshot, so the
    //     only thing separating them is the region.
    let free = plan(db.ok(&format!("REVERT MERGE {};", m_out), &mut main));
    assert!(!free.is_blocked(), "qty 500 is outside [20, 50): {:?}", free.blocked_by);
    assert!(!db.has_row(8), "an unblocked revert actually reverts");

    // (f) the second, independent symptom of the same root cause: DESIGN.md section 4's metric
    //     reported the row the WHERE clause had just compared as never-looked-at.
    assert!(
        blind.is_empty(),
        "the WHERE clause compared a VALUE, so these rows were looked at: {blind:?}"
    );
}

/// **BREAKING SHAPE: the same thing for `DELETE ... WHERE`.** A separate test rather than a loop over
/// two statements, because the two go through different arms of the merge engine — `RowDelete`
/// against `Assign` — and the published images a retained region is tested against are built
/// per-arm.
#[test]
fn a_delete_whose_where_clause_scanned_names_its_dependents() {
    let mut db = Db::new();
    db.seed();

    let m1 = insert_and_merge(&mut db, "restock-agent", "r_restock", 7, 30);
    let m_out = insert_and_merge(&mut db, "bulk-agent", "r_bulk", 8, 500);

    let mut d = db.session();
    db.ok("BEGIN AGENT SESSION AS 'pruner' RUN 'r_prune';", &mut d);
    let touched = affected(db.ok("DELETE FROM inventory WHERE qty >= 20 AND qty < 50;", &mut d));
    assert_eq!(touched, 2, "the clause matches rows 1 and 7");
    let merged = report(db.ok("MERGE;", &mut d));
    assert!(merged.applied_to_target, "{}", merged);
    assert!(!db.has_row(7), "the delete published");
    let blind = merged.blind_writes.clone();

    let mut main = db.session();
    let halted = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(halted.is_blocked(), "the DELETE's own scan consumed row 7");
    assert_eq!(halted.blocked_by, vec![TxnId(3)], "got {:?}", halted.blocked_by);

    let free = plan(db.ok(&format!("REVERT MERGE {};", m_out), &mut main));
    assert!(!free.is_blocked(), "qty 500 is outside [20, 50): {:?}", free.blocked_by);
    assert!(!db.has_row(8), "an unblocked revert actually reverts");

    // The metric last, for the same reason as the UPDATE test above.
    assert!(blind.is_empty(), "the WHERE clause compared a VALUE: {blind:?}");
}

/// **BREAKING SHAPE: `UPDATE ... WHERE <pk> = <literal>` — the case where causality and inspection
/// give opposite answers, in one test, because a fix that conflated them would pass one half and
/// fail the other.**
///
/// CAUSALITY: the statement could only write row 7 because its scan found row 7, so a revert of the
/// merge that published row 7 has a dependent to name. Without retention this is `blocked_by = []`.
///
/// INSPECTION: naming a row by primary key looks at no value, so DESIGN.md section 4's metric must
/// still report it blind. A fix that routed every write-path scan into the read-set builder would
/// make no `UPDATE ... WHERE <pk> = <lit>` blind ever again, which is the whole shape that metric
/// exists to catch.
#[test]
fn an_update_that_names_a_row_by_key_names_its_dependent_and_stays_a_blind_write() {
    let mut db = Db::new();
    db.seed();

    let m1 = insert_and_merge(&mut db, "restock-agent", "r_restock", 7, 30);
    // Published BEFORE the setter reads, so the anti-vacuity half at the end is decided by the
    // region rather than by the temporal rule.
    let m_out = insert_and_merge(&mut db, "bulk-agent", "r_bulk", 8, 500);

    let mut d = db.session();
    db.ok("BEGIN AGENT SESSION AS 'setter' RUN 'r_set';", &mut d);
    let branch = d.agent.as_ref().unwrap().branch;
    db.ok("UPDATE inventory SET qty = 99 WHERE id = 7;", &mut d);

    let blind: Vec<u64> =
        db.runtime.blind_writes(branch).unwrap().into_iter().map(|(_, r)| r.0).collect();
    assert_eq!(
        blind,
        vec![7u64],
        "an UPDATE that addressed the row by key inspected nothing, so the metric must still \
         report it: {blind:?}"
    );

    let merged = report(db.ok("MERGE;", &mut d));
    assert!(merged.applied_to_target, "{}", merged);
    assert_eq!(
        merged.blind_writes.iter().map(|(_, r)| r.0).collect::<Vec<_>>(),
        vec![7u64],
        "and the merge report carries the same answer: {:?}",
        merged.blind_writes
    );

    let mut main = db.session();
    let halted = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(halted.is_blocked(), "the UPDATE could only write row 7 because its scan found row 7");
    assert_eq!(halted.blocked_by, vec![TxnId(3)], "got {:?}", halted.blocked_by);
    assert_eq!(db.qty_of(7), 99, "a halted revert changes nothing");

    // Anti-vacuity: `WHERE id = 7` retains the interval [7, 7] over `id`, not the whole table, so a
    // merge that published a different row is free — even though the setter's snapshot saw it.
    let free = plan(db.ok(&format!("REVERT MERGE {};", m_out), &mut main));
    assert!(!free.is_blocked(), "row 8 is not row 7: {:?}", free.blocked_by);
    assert!(!db.has_row(8), "an unblocked revert actually reverts");
}

// ---- an observation with no version to name ----------------------------------------------------

/// **BREAKING SHAPE: `WHERE <pk> = <literal>` that finds NO ROW.** `access_shape` routes it to
/// `IndexLookup` -> `ExactVersions`, `versions` comes back empty, and `ReadSetBuilder::finish` drops
/// an empty exact set entirely — so the observation was retained as nothing at all, not even the
/// table. The engine full-scans for this query regardless, so the declared shape did not match the
/// physical access and the phantom coverage a scan would have earned was discarded.
///
/// Measured before this fix: the revert was not blocked, proceeded, and failed with
/// `constraint error: duplicate primary key Integer(2) in 'inventory'` — a constraint error where
/// the contract promises either a dependency tree or a completed revert. The same absence expressed
/// as a range halted correctly, so the outcome was decided by syntax.
#[test]
fn a_point_lookup_that_observed_an_absence_halts_the_revert_that_would_refill_it() {
    let mut db = Db::new();
    db.seed();

    // (a) pruner deletes row 2 and merges. txn 1.
    let mut p = db.session();
    db.ok("BEGIN AGENT SESSION AS 'pruner' RUN 'r_prune';", &mut p);
    db.ok("DELETE FROM inventory WHERE id = 2;", &mut p);
    let m1 = report(db.ok("MERGE;", &mut p)).merge_id;
    assert!(!db.has_row(2), "the premise of this test is that the pruner removed row 2");

    // (b) bulk-agent publishes an unrelated row BEFORE the two readers look, so the anti-vacuity
    //     half at the end is decided by the region and not by the temporal rule. txn 2.
    let m_out = insert_and_merge(&mut db, "bulk-agent", "r_bulk", 8, 500);

    // (c) filler asks for row 2 BY KEY, sees the absence the pruner caused, and fills it. txn 3.
    let mut f = db.session();
    db.ok("BEGIN AGENT SESSION AS 'filler' RUN 'r_fill';", &mut f);
    let seen = rows(db.ok("SELECT id, qty FROM inventory WHERE id = 2;", &mut f));
    assert!(seen.is_empty(), "row 2 must be absent for this to be the absence case: {seen:?}");
    db.ok("INSERT INTO inventory VALUES (2, 999);", &mut f);
    let merged = report(db.ok("MERGE;", &mut f));
    assert!(merged.applied_to_target, "{}", merged);
    assert_eq!(db.qty_of(2), 999);

    // (d) auditor asks by key for a row nothing has ever touched — the anti-vacuity half, and the
    //     one that keeps this fix from meaning "an empty exact read blocks everything". txn 4.
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'auditor' RUN 'r_audit';", &mut a);
    let none = rows(db.ok("SELECT id, qty FROM inventory WHERE id = 42;", &mut a));
    assert!(none.is_empty(), "row 42 was never seeded: {none:?}");

    // (e) the revert HALTS, and names only the filler.
    let mut main = db.session();
    let halted = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(
        halted.is_blocked(),
        "the filler's decision came from an absence this merge caused; the revert must not proceed \
         and then fail on a duplicate key"
    );
    assert_eq!(
        halted.blocked_by,
        vec![TxnId(3)],
        "the filler is txn 3 and the auditor looked somewhere else, got {:?}",
        halted.blocked_by
    );
    assert_eq!(db.qty_of(2), 999, "a halted revert changes nothing");
    assert!(db.has_row(1), "and nothing else moved");

    // (f) anti-vacuity: neither absence covers row 8, so its merge is free — even though both
    //     readers' snapshots saw it.
    let free = plan(db.ok(&format!("REVERT MERGE {};", m_out), &mut main));
    assert!(
        !free.is_blocked(),
        "an absence observed at key 2 or 42 says nothing about row 8: {:?}",
        free.blocked_by
    );
    assert!(!db.has_row(8), "an unblocked revert actually reverts");
}

// ---- a task that was discarded -----------------------------------------------------------------

/// **BREAKING SHAPE: a task scans, is then explicitly ABANDONed, and a revert is attempted.**
/// Captures are keyed by txn and were never dropped, including on `abandon`, so a discarded task
/// kept generating dependency edges forever. `undo_txn` finds no applied ops for it, so `CASCADE`
/// "reverts" a task that published nothing — which leaves the dangerous mode as the ONLY way past a
/// name that has nothing behind it, training the operator away from the default that protects them.
///
/// Both halves are the SAME reader, before and after `ABANDON`, so this test cannot pass by
/// retention being broken outright: the first half requires it to work.
#[test]
fn an_abandoned_tasks_scan_stops_blocking_the_revert_it_never_depended_on() {
    let mut db = Db::new();
    db.seed();
    let m1 = insert_and_merge(&mut db, "restock-agent", "r_restock", 7, 30);

    let mut ghost = db.session();
    db.ok("BEGIN AGENT SESSION AS 'ghost' RUN 'r_ghost';", &mut ghost);
    let seen = rows(db.ok(
        "SELECT id, qty FROM inventory WHERE qty >= 20 AND qty < 50;",
        &mut ghost,
    ));
    assert_eq!(seen.len(), 2, "rows 1 and 7 are in [20, 50): {seen:?}");

    // The anti-vacuity half, and it comes first: while the task is LIVE its scan is a real
    // dependent and the revert must halt.
    let mut main = db.session();
    let blocked = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(blocked.is_blocked(), "a live scanner must still block the revert");
    assert_eq!(
        blocked.blocked_by,
        vec![TxnId(2)],
        "the ghost is txn 2, got {:?}",
        blocked.blocked_by
    );
    assert!(db.has_row(7), "a halted revert changes nothing");

    // Now discard the task. It published nothing, so there is nothing downstream to protect.
    db.ok("ABANDON;", &mut ghost);

    let free = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(
        !free.is_blocked(),
        "a task that was discarded and published nothing still blocks the revert: {:?}",
        free.blocked_by
    );
    assert!(!db.has_row(7), "and an unblocked revert actually reverts");
}
