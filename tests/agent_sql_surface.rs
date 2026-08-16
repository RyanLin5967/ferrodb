//! End-to-end tests for the agent-session SQL surface.
//!
//! Design authority: DESIGN.md section 5. Each test names the exit criterion it exercises, and
//! the ones it cannot reach are named in the module docs of `agent_sql::runtime` rather than
//! quietly implied here: nothing in this file measures page counts, so criteria 1 and 8 are not
//! touched. What is measured is what the SQL surface is responsible for — isolation of branch
//! writes, reading another branch's uncommitted state, and the structure of what DIFF, MERGE and
//! REVERT hand back.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::{ChangeOutcome, ChangeSet, MergeReport, RowChangeKind};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::merge::MergeOutcome;
use ferrodb::tel::op::{Delta, OpKind};
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
        let path = dir.path().join("agent.db");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("agent.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    /// A connection sharing this database's agent runtime, so branches are mutually visible.
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
}

fn rows(out: Outcome) -> Vec<Vec<Value>> {
    match out {
        Outcome::Rows(r) => r,
        _ => panic!("expected rows"),
    }
}

fn agent(out: Outcome) -> AgentOutput {
    match out {
        Outcome::Agent(a) => a,
        Outcome::Rows(_) => panic!("expected an agent output, got rows"),
        _ => panic!("expected an agent output"),
    }
}

fn changeset(out: Outcome) -> ChangeSet {
    match agent(out) {
        AgentOutput::Diff(d) => d,
        other => panic!("expected a changeset, got {}", other),
    }
}

fn report(out: Outcome) -> MergeReport {
    match agent(out) {
        AgentOutput::Merge(m) => m,
        other => panic!("expected a merge report, got {}", other),
    }
}

/// The error from a statement that must fail. Panics if it unexpectedly succeeded, so a silent
/// success can never read as a passing negative test.
fn err_of(r: Result<Outcome, FerroError>) -> FerroError {
    match r {
        Ok(_) => panic!("expected an error, the statement succeeded"),
        Err(e) => e,
    }
}

fn qty_of(db: &mut Db, id: i32) -> i32 {
    let mut s = db.session();
    let r = rows(db.ok(&format!("SELECT qty FROM inventory WHERE id = {};", id), &mut s));
    assert_eq!(r.len(), 1, "row {} missing", id);
    match r[0][0] {
        Value::Integer(i) => i,
        ref other => panic!("qty is not an integer: {:?}", other),
    }
}

// ---- BEGIN AGENT SESSION -------------------------------------------------------------------

#[test]
fn begin_agent_session_forks_a_branch_and_interns_the_run() {
    // Exit criterion 9: which agent + run wrote here is recorded once per run, not per row.
    let mut db = Db::new();
    db.seed();
    let mut s = db.session();
    let out = agent(db.ok(
        "BEGIN AGENT SESSION AS 'pricing-agent' RUN 'r_8fk2';",
        &mut s,
    ));
    let session = match out {
        AgentOutput::SessionStarted(s) => s,
        other => panic!("expected a session, got {}", other),
    };
    assert_eq!(session.agent_id, "pricing-agent");
    assert_eq!(session.run_id, "r_8fk2");
    assert_eq!(session.branch_name, "b_1");
    assert!(!session.branch.is_trunk());

    let run = db.runtime.run_of(session.branch).expect("run interned");
    assert!(run.describe().contains("pricing-agent"));
    assert!(run.describe().contains("r_8fk2"));

    // and a second BEGIN on the same connection is refused rather than silently nesting
    assert!(db.exec("BEGIN AGENT SESSION AS 'other';", &mut s).is_err());
}

#[test]
fn agent_statements_outside_a_session_name_the_missing_branch() {
    let mut db = Db::new();
    db.seed();
    let mut s = db.session();
    for sql in ["DIFF;", "MERGE;", "ABANDON;"] {
        let err = err_of(db.exec(sql, &mut s));
        assert!(
            err.to_string().contains("no agent session"),
            "{} gave {}",
            sql,
            err
        );
    }
    // an unknown branch is an error, never an empty answer
    assert!(db.exec("DIFF BRANCH b_99;", &mut s).is_err());
    assert!(db.exec("SELECT * FROM inventory AS OF BRANCH b_99;", &mut s).is_err());
}

// ---- exit criterion 2: invisible until merge ------------------------------------------------

#[test]
fn branch_writes_are_invisible_to_main_and_to_siblings() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let mut b = db.session();
    let mut main = db.session();

    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);

    // the writing session sees its own write
    let seen = rows(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut a));
    assert_eq!(seen[0][0], Value::Integer(15));
    // main does not
    let seen = rows(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut main));
    assert_eq!(seen[0][0], Value::Integer(20));
    // and neither does the sibling branch
    let seen = rows(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut b));
    assert_eq!(seen[0][0], Value::Integer(20));
}

#[test]
fn inserts_and_deletes_on_a_branch_are_also_invisible_until_merge() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let mut main = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("INSERT INTO inventory VALUES (3, 7);", &mut a);
    db.ok("DELETE FROM inventory WHERE id = 2;", &mut a);

    assert_eq!(rows(db.ok("SELECT id FROM inventory;", &mut a)).len(), 2);
    assert_eq!(rows(db.ok("SELECT id FROM inventory;", &mut main)).len(), 2);
    assert_eq!(qty_of(&mut db, 2), 5);

    let r = report(db.ok("MERGE;", &mut a));
    assert!(r.applied_to_target, "{}", r);
    let after = rows(db.ok("SELECT id FROM inventory;", &mut main));
    let mut ids: Vec<i32> = after
        .iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i,
            _ => panic!(),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 3]);
}

// ---- exit criterion 3: AS OF BRANCH reaches uncommitted state -------------------------------

#[test]
fn select_as_of_branch_reads_another_branchs_uncommitted_state() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let mut observer = db.session();

    let started = agent(db.ok("BEGIN AGENT SESSION AS 'restock' RUN 'r1';", &mut a));
    let branch_name = match started {
        AgentOutput::SessionStarted(s) => s.branch_name,
        other => panic!("expected a session, got {}", other),
    };
    db.ok("UPDATE inventory SET qty = qty + 30 WHERE id = 1;", &mut a);

    // Never merged, never committed to main — and visible to a different connection on request.
    let sql = format!("SELECT qty FROM inventory AS OF BRANCH {};", branch_name);
    let seen = rows(db.ok(&sql, &mut observer));
    let mut qtys: Vec<i32> = seen
        .iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i,
            _ => panic!(),
        })
        .collect();
    qtys.sort();
    assert_eq!(qtys, vec![5, 50]);

    // The same read without AS OF still sees main.
    assert_eq!(qty_of(&mut db, 1), 20);

    // AS OF composes with WHERE and with projection.
    let sql = format!("SELECT qty FROM inventory AS OF BRANCH {} WHERE id = 1;", branch_name);
    let seen = rows(db.ok(&sql, &mut observer));
    assert_eq!(seen, vec![vec![Value::Integer(50)]]);
}

#[test]
fn as_of_branch_sees_uncommitted_inserts_and_deletes_too() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let mut observer = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("INSERT INTO inventory VALUES (9, 99);", &mut a);
    db.ok("DELETE FROM inventory WHERE id = 2;", &mut a);

    let seen = rows(db.ok("SELECT id FROM inventory AS OF BRANCH b_1;", &mut observer));
    let mut ids: Vec<i32> = seen
        .iter()
        .map(|r| match r[0] {
            Value::Integer(i) => i,
            _ => panic!(),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 9]);
    assert_eq!(rows(db.ok("SELECT id FROM inventory;", &mut observer)).len(), 2);
}

#[test]
fn a_nested_task_forks_from_its_parents_state_and_then_diverges() {
    // The child's visible state is the parent's state at fork time — the row-level equivalent of
    // the child's root page being the parent's root page. Writes the parent makes *after* the
    // fork stay invisible to the child, and the child never walks a parent chain to find them.
    let mut db = Db::new();
    db.seed();
    let mut parent = db.session();
    db.ok("BEGIN AGENT SESSION AS 'planner' RUN 'r1';", &mut parent);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut parent);

    // A sub-task forks from the parent *branch*. SQL allows one session per connection, so a
    // nested fork is asked for through the runtime with an explicit parent.
    let parent_branch = parent.agent.as_ref().unwrap().branch;
    let child = db
        .runtime
        .begin_session("sub", Some("r2"), parent_branch)
        .unwrap();
    let mut observer = db.session();

    let sql = format!("SELECT qty FROM inventory AS OF BRANCH {} WHERE id = 1;", child.branch_name);
    let seen = rows(db.ok(&sql, &mut observer));
    assert_eq!(
        seen[0][0],
        Value::Integer(15),
        "the child inherits the parent's uncommitted row"
    );

    // Now the parent moves again; the child must not see it, and must not go looking up the
    // parent chain for it either.
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut parent);
    let seen = rows(db.ok(&sql, &mut observer));
    assert_eq!(seen[0][0], Value::Integer(15));
    let seen = rows(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut parent));
    assert_eq!(seen[0][0], Value::Integer(10));
    // and main is still untouched by either of them
    assert_eq!(qty_of(&mut db, 1), 20);
}

// ---- exit criterion 4: DIFF is structured ---------------------------------------------------

#[test]
fn diff_returns_a_structured_changeset_with_ops_and_outcomes() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'pricing' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE qty >= 5 AND id = 1;", &mut a);
    db.ok("INSERT INTO inventory VALUES (4, 1);", &mut a);

    let d = changeset(db.ok("DIFF;", &mut a));
    assert_eq!(d.rows.len(), 2, "{}", d);

    let update = d.rows.iter().find(|r| r.kind == RowChangeKind::Update).expect("an update row");
    assert_eq!(update.table, "inventory");
    assert_eq!(update.before, Some(vec![Value::Integer(1), Value::Integer(20)]));
    assert_eq!(update.after, Some(vec![Value::Integer(1), Value::Integer(15)]));
    // the op is the algebra element, not a before/after pair: `qty = qty - 5` is Add(-5)
    assert_eq!(update.ops.len(), 1);
    assert_eq!(update.ops[0].kind, OpKind::Add(Delta::Int(-5)));
    assert_eq!(update.ops[0].witness, Some(Value::Integer(20)));
    // and the guard that admitted it is kept verbatim
    assert_eq!(update.guards.len(), 1);
    assert!(update.guards[0].violated_predicate().contains("qty >= 5"));
    assert_eq!(update.outcome, ChangeOutcome::Pending);

    let insert = d.rows.iter().find(|r| r.kind == RowChangeKind::Insert).expect("an insert row");
    assert!(insert.before.is_none());
    assert!(matches!(insert.ops[0].kind, OpKind::RowCreate(_)));
}

#[test]
fn diff_of_an_untouched_branch_is_empty_but_still_a_changeset() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'idle' RUN 'r1';", &mut a);
    let d = changeset(db.ok("DIFF;", &mut a));
    assert!(d.is_empty());
    assert_eq!(d.to.id, 1);
}

// ---- exit criterion 5: MERGE reports all four outcomes --------------------------------------

#[test]
fn a_solo_merge_is_clean_and_publishes_to_main() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    let r = report(db.ok("MERGE;", &mut a));
    assert_eq!(r.outcome, MergeOutcome::Clean);
    assert!(r.applied_to_target);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.merge_id, "m_1");
    assert_eq!(qty_of(&mut db, 1), 15);
    // the session is over: the branch merged
    assert!(db.exec("DIFF;", &mut a).is_err());
}

#[test]
fn a_multi_row_merge_publishes_every_row_in_one_transaction() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut a);
    db.ok("INSERT INTO inventory VALUES (5, 50);", &mut a);

    let r = report(db.ok("MERGE;", &mut a));
    assert!(r.applied_to_target, "{}", r);
    assert_eq!(r.rows.len(), 3);
    assert_eq!(qty_of(&mut db, 1), 15);
    assert_eq!(qty_of(&mut db, 2), 6);
    assert_eq!(qty_of(&mut db, 5), 50);
}

#[test]
fn two_branches_decrementing_the_same_row_compose_arithmetically() {
    // Exit criterion 6. 20 - 5 - 3 = 12, which is neither branch's own answer.
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 3 WHERE id = 1;", &mut b);

    let first = report(db.ok("MERGE;", &mut a));
    assert_eq!(first.outcome, MergeOutcome::Clean);
    assert_eq!(qty_of(&mut db, 1), 15);

    let second = report(db.ok("MERGE;", &mut b));
    assert!(
        matches!(second.outcome, MergeOutcome::Commuting { .. }),
        "expected Commuting, got {}",
        second
    );
    assert!(second.applied_to_target);
    assert_eq!(qty_of(&mut db, 1), 12);
}

#[test]
fn two_decrements_inside_one_task_are_not_collapsed() {
    // Add is not idempotent: two statements are -10, not -5.
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    let r = report(db.ok("MERGE;", &mut a));
    assert!(r.applied_to_target, "{}", r);
    assert_eq!(qty_of(&mut db, 1), 10);
}

#[test]
fn concurrent_assignments_conflict_rather_than_silently_picking_one() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    db.ok("UPDATE inventory SET qty = 1 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = 2 WHERE id = 1;", &mut b);

    assert!(report(db.ok("MERGE;", &mut a)).applied_to_target);
    let second = report(db.ok("MERGE;", &mut b));
    assert!(second.outcome.is_conflict(), "expected a conflict, got {}", second);
    // and nothing was published: the target still holds the first writer's value
    assert!(!second.applied_to_target);
    assert_eq!(qty_of(&mut db, 1), 1);
    // the branch is still alive, so the agent can retry
    assert!(db.exec("DIFF;", &mut b).is_ok());
}

#[test]
fn an_lww_column_resolves_with_loss_and_never_reports_clean() {
    // The fourth outcome. Reporting a lossy resolution as Clean would tell the agent its write
    // landed when the other agent's write is gone.
    use ferrodb::tel::ids::ColId;
    use ferrodb::tel::merge::MergePolicy;

    let mut db = Db::new();
    db.seed();
    db.runtime.set_policy("inventory", ColId(1), MergePolicy::Lww);

    let mut a = db.session();
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    db.ok("UPDATE inventory SET qty = 111 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = 222 WHERE id = 1;", &mut b);

    assert!(report(db.ok("MERGE;", &mut a)).applied_to_target);
    assert_eq!(qty_of(&mut db, 1), 111);

    let second = report(db.ok("MERGE;", &mut b));
    assert!(second.outcome.lost_a_write(), "expected ResolvedWithLoss, got {}", second);
    assert_ne!(second.outcome.name(), "Clean");
    assert!(second.applied_to_target);
    assert_eq!(second.rows[0].discarded.len(), 1);
    assert_eq!(second.rows[0].discarded[0].policy, MergePolicy::Lww);
    assert_eq!(qty_of(&mut db, 1), 222);
}

#[test]
fn diff_marks_a_row_the_target_moved_under() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 3 WHERE id = 1;", &mut b);

    // before anything merges, b's change is simply pending
    assert_eq!(changeset(db.ok("DIFF;", &mut b)).rows[0].outcome, ChangeOutcome::Pending);
    assert!(report(db.ok("MERGE;", &mut a)).applied_to_target);
    // afterwards the same change is pending *against a target that moved*
    assert_eq!(
        changeset(db.ok("DIFF;", &mut b)).rows[0].outcome,
        ChangeOutcome::PendingConcurrent
    );
}

// ---- exit criterion 7: the violated predicate comes back ------------------------------------

#[test]
fn a_guard_that_fails_against_merged_state_returns_the_predicate_itself() {
    // The bounded-counter case from DESIGN.md: compose the Adds, then re-check the guard.
    // 20 - 12 - 12 = -4, so `qty >= 12` no longer holds after composition.
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    db.ok("UPDATE inventory SET qty = qty - 12 WHERE id = 1 AND qty >= 12;", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 12 WHERE id = 1 AND qty >= 12;", &mut b);

    let first = report(db.ok("MERGE;", &mut a));
    assert!(first.applied_to_target, "{}", first);
    assert_eq!(qty_of(&mut db, 1), 8);

    let second = report(db.ok("MERGE;", &mut b));
    assert!(second.outcome.is_conflict(), "expected a conflict, got {}", second);
    let predicates = second.violated_predicates();
    assert_eq!(predicates.len(), 1, "{:?}", predicates);
    assert!(
        predicates[0].contains("qty >= 12"),
        "the agent must get the predicate back, got {:?}",
        predicates
    );
    // nothing was published, so the counter never went negative
    assert!(!second.applied_to_target);
    assert_eq!(qty_of(&mut db, 1), 8);
}

// ---- ABANDON ---------------------------------------------------------------------------------

#[test]
fn abandon_drops_the_branch_and_leaves_main_untouched() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let mut observer = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 20 WHERE id = 1;", &mut a);
    db.ok("ABANDON;", &mut a);

    assert_eq!(qty_of(&mut db, 1), 20);
    // the branch name no longer resolves, and its id is a hard error rather than stale data
    assert!(db.exec("SELECT qty FROM inventory AS OF BRANCH b_1;", &mut observer).is_err());
    let branch = ferrodb::branch::types::BranchId::new(1, 0);
    assert!(db.runtime.branches().get(branch).is_err());
    // the connection is free to start another task
    assert!(db.exec("BEGIN AGENT SESSION AS 'a2' RUN 'r2';", &mut a).is_ok());
}

// ---- exit criterion 10: REVERT ... CASCADE over read-sets -----------------------------------

#[test]
fn revert_merge_halts_on_a_downstream_dependent_then_cascades_on_request() {
    let mut db = Db::new();
    db.seed();

    // Agent A changes row 1 and merges.
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    let first = report(db.ok("MERGE;", &mut a));
    assert!(first.applied_to_target, "{}", first);
    assert_eq!(qty_of(&mut db, 1), 15);

    // Agent B *reads* the version A produced (a point lookup, so the read-set is exact), then
    // writes somewhere else on the strength of it.
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    let seen = rows(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut b));
    assert_eq!(seen[0][0], Value::Integer(15));
    db.ok("UPDATE inventory SET qty = qty + 2 WHERE id = 2;", &mut b);
    let second = report(db.ok("MERGE;", &mut b));
    assert!(second.applied_to_target, "{}", second);
    assert_eq!(qty_of(&mut db, 2), 7);

    // Halt is the default: the dependent is found through the retained read-set and nothing moves.
    let mut main = db.session();
    let plan = match agent(db.ok("REVERT MERGE m_1;", &mut main)) {
        AgentOutput::Revert(p) => p,
        other => panic!("expected a revert plan, got {}", other),
    };
    assert!(plan.is_blocked(), "expected the revert to halt: {:?}", plan);
    assert_eq!(plan.blocked_by.len(), 1);
    assert_eq!(qty_of(&mut db, 1), 15);
    assert_eq!(qty_of(&mut db, 2), 7);

    // CASCADE undoes the dependent first, then the target.
    let plan = match agent(db.ok("REVERT MERGE m_1 CASCADE;", &mut main)) {
        AgentOutput::Revert(p) => p,
        other => panic!("expected a revert plan, got {}", other),
    };
    assert!(!plan.is_blocked());
    assert_eq!(plan.cascade.len(), 1);
    assert_eq!(qty_of(&mut db, 1), 20);
    assert_eq!(qty_of(&mut db, 2), 5);
}

#[test]
fn reverting_an_unknown_merge_is_an_error_not_a_no_op() {
    let mut db = Db::new();
    db.seed();
    let mut s = db.session();
    let err = err_of(db.exec("REVERT MERGE m_99 CASCADE;", &mut s));
    assert!(err.to_string().contains("m_99"), "got {}", err);
}

#[test]
fn a_merge_with_no_downstream_reader_reverts_without_cascading() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    assert!(report(db.ok("MERGE;", &mut a)).applied_to_target);
    assert_eq!(qty_of(&mut db, 1), 15);

    let mut main = db.session();
    let plan = match agent(db.ok("REVERT MERGE m_1;", &mut main)) {
        AgentOutput::Revert(p) => p,
        other => panic!("expected a revert plan, got {}", other),
    };
    assert!(!plan.is_blocked());
    assert!(plan.cascade.is_empty());
    assert_eq!(qty_of(&mut db, 1), 20);
}

// ---- the captured Typed Effect Log ----------------------------------------------------------

#[test]
fn captured_frames_reach_the_effect_log_as_one_frame_per_task() {
    use ferrodb::agent_sql::SurfaceMerger;
    use ferrodb::branch::types::BranchId;
    use ferrodb::tel::merge::Merger;

    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let branch = match agent(db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a)) {
        AgentOutput::SessionStarted(s) => s.branch,
        other => panic!("expected a session, got {}", other),
    };
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1 AND qty >= 5;", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 1 WHERE id = 2;", &mut a);

    // One agent task is one frame, however many statements it ran: the unit of isolation is the
    // task, and that is what keeps the causal edges on the task rather than on each statement.
    let frames = db.runtime.log().frames_for(branch, 0).unwrap();
    assert_eq!(frames.len(), 1, "expected one frame per task");
    assert_eq!(frames[0].ops.len(), 2);
    assert_eq!(frames[0].guards.len(), 2);
    assert_eq!(frames[0].branch, branch);

    // and the trait-shaped differ reads the same log
    let differ = SurfaceMerger::new(db.runtime.log().clone());
    let d = differ.diff(BranchId::TRUNK, branch).unwrap();
    assert_eq!(d.ops.len(), 2);
    assert!(d
        .guards
        .iter()
        .any(|g| g.violated_predicate().contains("qty >= 5")));
}

// ---- ordinary SQL keeps working --------------------------------------------------------------

#[test]
fn ordinary_statements_are_unaffected_by_the_agent_surface() {
    let mut db = Db::new();
    db.seed();
    let mut s = db.session();
    assert_eq!(rows(db.ok("SELECT * FROM inventory;", &mut s)).len(), 2);
    db.ok("BEGIN;", &mut s);
    db.ok("INSERT INTO inventory VALUES (7, 3);", &mut s);
    db.ok("COMMIT;", &mut s);
    assert_eq!(rows(db.ok("SELECT * FROM inventory;", &mut s)).len(), 3);
    // and an agent session may not open inside a transaction block
    db.ok("BEGIN;", &mut s);
    assert!(db.exec("BEGIN AGENT SESSION AS 'a';", &mut s).is_err());
    db.ok("ROLLBACK;", &mut s);
}

// ---- wide and narrow numeric literals on a branch --------------------------------------------

/// **The third write site.**
///
/// A numeric literal is bound against the declared type of the column it lands in, and there are
/// two forms of that: `literal_for_column`, shared with comparisons, and
/// `literal_for_written_column`, which adds the `FLOAT` widening that only makes sense for a write.
/// Three places write a literal into a typed column — INSERT's `bind_row_against`, trunk UPDATE in
/// the planner, and agent-branch UPDATE here. The first two were switched to the write-only form;
/// this one was still calling the comparison form.
///
/// The symptom is not subtle. `UPDATE ... SET amount = 5` against a FLOAT column is ordinary SQL
/// that every dialect accepts, and a whole-numbered literal reads as `Integer`. Without the
/// widening, `value_fits` refuses it — `serialize` would lay down four bytes where `deserialize`
/// reads eight and shift every column after it — so the statement is a hard error on a branch
/// while the identical statement on trunk succeeds.
///
/// Trunk is exercised in the same test, so a failure says which of the two paths broke rather than
/// leaving that to be guessed.
#[test]
fn a_whole_numbered_literal_updates_a_float_column_on_a_branch_and_on_trunk() {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("CREATE TABLE prices (id INTEGER NOT NULL, amount FLOAT);", &mut s);
    db.ok("INSERT INTO prices VALUES (1, 2.5);", &mut s);
    db.ok("INSERT INTO prices VALUES (2, 2.5);", &mut s);

    // Trunk: the planner's UPDATE path.
    db.ok("UPDATE prices SET amount = 7 WHERE id = 2;", &mut s);
    let trunk = rows(db.ok("SELECT amount FROM prices WHERE id = 2;", &mut s));
    // The VARIANT, not just the number. `Value`'s `PartialEq` is numeric, so
    // `Integer(7) == Float(7.0)` — an `assert_eq!` against `Value::Float(7.0)` passes just as
    // happily on an `Integer` that was never widened, and would report a green result for exactly
    // the bug this test exists to catch.
    assert!(
        matches!(trunk[0][0], Value::Float(v) if v == 7.0),
        "trunk UPDATE lost the FLOAT widening: {:?}",
        trunk[0][0]
    );

    // Branch: the agent runtime's UPDATE path, which is the one that was missed.
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'repricer' RUN 'r1';", &mut a);
    db.ok("UPDATE prices SET amount = 5 WHERE id = 1;", &mut a);
    let seen = rows(db.ok("SELECT amount FROM prices WHERE id = 1;", &mut a));
    assert!(
        matches!(seen[0][0], Value::Float(v) if v == 5.0),
        "an agent branch stored a whole-numbered literal into a FLOAT column as {:?} rather than \
         widening it to Float, though trunk widens the identical statement. A page-backed branch \
         does not run the tuple encoder's width check, so the wrong variant is written silently \
         and only surfaces when the branch merges back into a heap tuple.",
        seen[0][0]
    );

    // A negative literal takes the same path through `signed_numeric_text`.
    db.ok("UPDATE prices SET amount = -3 WHERE id = 1;", &mut a);
    let neg = rows(db.ok("SELECT amount FROM prices WHERE id = 1;", &mut a));
    assert!(
        matches!(neg[0][0], Value::Float(v) if v == -3.0),
        "a negative whole literal was not widened on a branch: {:?}",
        neg[0][0]
    );

    // And the branch's value must survive the trip back to trunk, where the tuple encoder DOES
    // enforce the column's width. This is where a wrong variant stops being invisible.
    db.ok("MERGE;", &mut a);
    let merged = rows(db.ok("SELECT amount FROM prices WHERE id = 1;", &mut s));
    assert!(
        matches!(merged[0][0], Value::Float(v) if v == -3.0),
        "the branch's FLOAT did not survive the merge onto trunk: {:?}",
        merged[0][0]
    );
}

/// The other half of the same split, on the branch: a **comparison** must NOT get the widening.
///
/// 2^53+1 is not representable as any f64, so no stored float can equal it and the honest answer
/// is the empty set. If the branch's WHERE clause bound its literal through the FLOAT widening it
/// would round to 2^53 and match the row below — returning a row it can prove is unequal, with no
/// error anywhere. `Value::cmp` compares an i64 against an f64 exactly for precisely this reason.
#[test]
fn a_comparison_on_a_branch_does_not_round_the_literal() {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("CREATE TABLE prices (id INTEGER NOT NULL, amount FLOAT);", &mut s);
    db.ok("INSERT INTO prices VALUES (1, 9007199254740992);", &mut s); // 2^53

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'auditor' RUN 'r1';", &mut a);

    let exact = rows(db.ok("SELECT id FROM prices WHERE amount = 9007199254740992;", &mut a));
    assert_eq!(exact.len(), 1, "2^53 is an f64 and must still match: {exact:?}");

    let past = rows(db.ok("SELECT id FROM prices WHERE amount = 9007199254740993;", &mut a));
    assert!(
        past.is_empty(),
        "2^53+1 is not representable as an f64, so nothing can equal it; the literal was rounded \
         down to 2^53 and matched a row it is provably unequal to: {past:?}"
    );

    // And an UPDATE guarded by that predicate must touch nothing, or the rounding would silently
    // rewrite a row the statement never named.
    db.ok("UPDATE prices SET amount = 1 WHERE amount = 9007199254740993;", &mut a);
    let after = rows(db.ok("SELECT amount FROM prices WHERE id = 1;", &mut a));
    assert_eq!(
        after[0][0],
        Value::Float(9007199254740992.0),
        "an UPDATE matched on a rounded literal and overwrote an untouched row"
    );
}

// ---- exit criterion 9: which agent + run + model wrote a given row ---------------------------

#[test]
fn row_authorship_survives_the_merge_that_published_it() {
    // The failure this pins: `run_of` reads the branch's workspace and `seal` drops that
    // workspace the instant a merge succeeds, so authorship used to become unanswerable at
    // exactly the moment the row became visible to anyone else.
    use ferrodb::tel::ids::RowId;

    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok(
        "BEGIN AGENT SESSION AS 'restock-agent' RUN 'run-42' MODEL 'claude-opus-5/2026-05';",
        &mut a,
    );
    db.ok("UPDATE inventory SET qty = qty + 30 WHERE id = 1;", &mut a);
    assert!(report(db.ok("MERGE;", &mut a)).applied_to_target);

    // The branch is gone. The question is about the row, so it must still answer.
    let who = db
        .runtime
        .who_wrote_row("inventory", RowId(1))
        .expect("row 1 must still be attributed after its branch merged");
    assert_eq!(who.agent_id, "restock-agent");
    assert_eq!(who.run_id, "run-42");
    assert_eq!(who.model, "claude-opus-5");
    assert_eq!(who.model_version, "2026-05");

    // A row no agent ever wrote is unattributed, never a guess at the nearest run.
    assert!(db.runtime.who_wrote_row("inventory", RowId(2)).is_none());
    assert!(db.runtime.who_wrote_row("inventory", RowId(999)).is_none());
    // and an unknown table does not resolve to some other table's rows
    assert!(db.runtime.who_wrote_row("no_such_table", RowId(1)).is_none());
}

#[test]
fn two_runs_are_attributed_separately_and_interned_once_each() {
    use ferrodb::tel::ids::RowId;

    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'restock' RUN 'r1' MODEL 'claude-opus-5/2026-05';", &mut a);
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 1;", &mut a);
    assert!(report(db.ok("MERGE;", &mut a)).applied_to_target);

    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'audit' RUN 'r2' MODEL 'gpt-9/turbo';", &mut b);
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut b);
    assert!(report(db.ok("MERGE;", &mut b)).applied_to_target);

    let one = db.runtime.who_wrote_row("inventory", RowId(1)).unwrap();
    let two = db.runtime.who_wrote_row("inventory", RowId(2)).unwrap();
    assert_eq!(one.agent_id, "restock");
    assert_eq!(two.agent_id, "audit");
    assert_eq!(two.model, "gpt-9");
    assert_ne!(one.prov_id, two.prov_id, "two runs must not share one interned slot");

    let table = db.runtime.authors_of("inventory");
    assert_eq!(table.len(), 2, "only the two written rows are attributed: {:?}", table);
}

#[test]
fn a_conflicting_merge_attributes_nothing() {
    // A merge that publishes nothing must not claim authorship of a row it failed to write.
    use ferrodb::tel::ids::RowId;

    let mut db = Db::new();
    db.seed();
    let (mut a, mut b) = (db.session(), db.session());
    db.ok("BEGIN AGENT SESSION AS 'winner' RUN 'r1' MODEL 'm/1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'loser' RUN 'r2' MODEL 'm/2';", &mut b);
    db.ok("UPDATE inventory SET qty = 1 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = 2 WHERE id = 1;", &mut b);

    assert!(report(db.ok("MERGE;", &mut a)).applied_to_target);
    let second = report(db.ok("MERGE;", &mut b));
    assert!(second.outcome.is_conflict(), "expected a conflict, got {}", second);
    assert!(!second.applied_to_target);

    // The row belongs to the agent whose write actually landed.
    let who = db.runtime.who_wrote_row("inventory", RowId(1)).unwrap();
    assert_eq!(who.agent_id, "winner", "a rejected merge must not take authorship");
    assert_eq!(who.run_id, "r1");
}

#[test]
fn a_session_declaring_no_model_is_recorded_as_unspecified_not_guessed() {
    use ferrodb::tel::ids::RowId;

    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'no-model-agent' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 1;", &mut a);
    assert!(report(db.ok("MERGE;", &mut a)).applied_to_target);

    let who = db.runtime.who_wrote_row("inventory", RowId(1)).unwrap();
    assert_eq!(who.agent_id, "no-model-agent");
    assert_eq!(who.model, "unspecified");
    assert_eq!(who.model_version, "unspecified");
}

#[test]
fn a_malformed_model_is_refused_rather_than_recorded_blank() {
    let mut db = Db::new();
    db.seed();
    let mut s = db.session();
    for sql in [
        "BEGIN AGENT SESSION AS 'a' MODEL '';",
        "BEGIN AGENT SESSION AS 'a' MODEL '/2026-05';",
        "BEGIN AGENT SESSION AS 'a' MODEL 'claude/';",
    ] {
        assert!(db.exec(sql, &mut s).is_err(), "{} must not begin a session", sql);
    }
    // and a well-formed one still works on the same connection
    assert!(db.exec("BEGIN AGENT SESSION AS 'a' MODEL 'claude-opus-5/2026-05';", &mut s).is_ok());
}
