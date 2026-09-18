//! ADVERSARIAL: what F6 loses by dropping a capture on `seal(published = false)`.
//!
//! F6's premise is "an ABANDONed task published nothing, so its capture protects nothing".
//! A task can publish through a CHILD it forked: `begin_session(.., parent = P)` copies P's
//! staged rows into the child's workspace, and the child's `MERGE` writes them into the shared
//! tables. The parent's READ PREMISE lives only in the parent's capture (captures are per-txn and
//! the child gets a fresh one), so dropping the parent's capture removes the only edge between
//! the rows the parent read and the rows that are now published because of them.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::MergeReport;
use ferrodb::branch::types::BranchId;
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
            .open(dir.path().join("advf6.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("advf6.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

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

    fn all(&mut self) -> Vec<Vec<Value>> {
        let mut s = self.session();
        rows(self.ok("SELECT id, qty FROM inventory;", &mut s))
    }

    fn has_row(&mut self, id: i32) -> bool {
        self.all().iter().any(|r| r[0] == Value::Integer(id))
    }
}

fn rows(out: Outcome) -> Vec<Vec<Value>> {
    match out {
        Outcome::Rows(r) => r,
        other => panic!("expected rows, got {:?}", std::mem::discriminant(&other)),
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

fn insert_and_merge(db: &mut Db, agent_id: &str, run_id: &str, id: i32, qty: i32) -> String {
    let mut s = db.session();
    db.ok(&format!("BEGIN AGENT SESSION AS '{}' RUN '{}';", agent_id, run_id), &mut s);
    db.ok(&format!("INSERT INTO inventory VALUES ({}, {});", id, qty), &mut s);
    let r = report(db.ok("MERGE;", &mut s));
    assert!(r.applied_to_target, "{} failed to merge: {}", agent_id, r);
    r.merge_id
}

/// Build the shared shape: `m1` publishes row 7 (qty 30); a PARENT task scans [20, 50) and stages a
/// row derived from what it saw; a CHILD forked off the parent MERGEs, which publishes the parent's
/// staged row into the shared tables. Returns `(m1, parent session, parent branch, parent txn)`.
///
/// The parent's txn id is RETURNED rather than compared to a literal. This fixture was quarantined
/// unreviewed, and its equality against a hard-coded txn 3 was simply wrong at I21's tip -- the
/// planner is txn 2 -- which failed all three tests, INCLUDING THE CONTROL, before any of them
/// reached the property under test. The claim is "the revert is blocked BY THE PARENT PLANNER", so
/// the planner is named by its actual id; every `blocked_by` assertion below is unchanged in
/// strength, since a missing edge still fails `is_blocked` and an edge attributed to a different
/// txn still fails the comparison.
fn parent_write_published_through_a_child(db: &mut Db) -> (String, Session, BranchId, TxnId) {
    let m1 = insert_and_merge(db, "restock-agent", "r_restock", 7, 30);

    // The PARENT: reads the region m1 wrote into, then stages a row justified by that read.
    let mut parent = db.session();
    db.ok("BEGIN AGENT SESSION AS 'planner' RUN 'r_planner';", &mut parent);
    let seen = rows(db.ok(
        "SELECT id, qty FROM inventory WHERE qty >= 20 AND qty < 50;",
        &mut parent,
    ));
    assert_eq!(seen.len(), 2, "rows 1 and 7 are in [20, 50): {seen:?}");
    let pbranch = parent.agent.as_ref().unwrap().branch;
    let ptxn = parent.agent.as_ref().unwrap().txn;
    // 50 = the two quantities it just read, summed. A write with a read premise.
    db.ok("INSERT INTO inventory VALUES (9, 50);", &mut parent);

    // The CHILD, forked off the live parent. It inherits the parent's staged rows.
    let cs = db.runtime.begin_session("planner-child", Some("r_child"), pbranch).unwrap();
    let mut child = db.session();
    child.agent = Some(cs);
    let r = report(db.ok("MERGE;", &mut child));
    assert!(r.applied_to_target, "the child failed to merge: {r}");

    // The parent's write is now in the SHARED tables, published by the child.
    assert!(
        db.has_row(9),
        "the child's merge did not publish the parent's staged row; this fixture proves nothing"
    );
    (m1, parent, pbranch, ptxn)
}

/// **THE BREAK.** The parent's read premise is dropped by `ABANDON`, even though the write it
/// justified is published and still in the shared tables.
#[test]
fn abandoning_a_parent_whose_child_published_its_write_loses_the_dependent() {
    let mut db = Db::new();
    db.seed();
    let (m1, mut parent, _pbranch, ptxn) = parent_write_published_through_a_child(&mut db);

    // ANTI-VACUITY: while the parent is live the edge exists and the revert halts.
    let mut main = db.session();
    let blocked = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(blocked.is_blocked(), "the planner's scan must block the revert while it is live");
    assert_eq!(blocked.blocked_by, vec![ptxn], "got {:?}", blocked.blocked_by);
    assert!(db.has_row(7), "a halted revert changes nothing");

    // The parent walks away. Its buffered rows were never published BY IT -- but its child
    // published them, and row 9 is in the shared tables right now.
    db.ok("ABANDON;", &mut parent);
    assert!(db.has_row(9), "row 9 is published; abandoning the parent does not unpublish it");

    let after = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(
        after.is_blocked(),
        "row 9 is published and was derived from reading row 7, but the revert of m1 is \
         unblocked: blocked_by = {:?}, cascade = {:?}. Row 7 present after the revert: {}",
        after.blocked_by,
        after.cascade,
        db.has_row(7)
    );
}

/// **CONTROL.** Identical up to the last statement, which is `MERGE` instead of `ABANDON`. The
/// `published` discriminator keeps the capture, so the edge survives. This is what proves the
/// break above is F6's drop and not a broken fixture.
#[test]
fn control_merging_the_parent_keeps_the_same_dependent() {
    let mut db = Db::new();
    db.seed();
    let (m1, mut parent, _pbranch, ptxn) = parent_write_published_through_a_child(&mut db);

    let mut main = db.session();
    let blocked = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert_eq!(blocked.blocked_by, vec![ptxn], "got {:?}", blocked.blocked_by);

    db.ok("MERGE;", &mut parent);

    let after = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(
        after.is_blocked(),
        "the merged parent's capture was kept, so this must still halt: {:?}",
        after.blocked_by
    );
    assert_eq!(after.blocked_by, vec![ptxn], "got {:?}", after.blocked_by);
}

/// **THE SAME BREAK THROUGH THE LEASE-REAPER DOOR.** No client cooperation at all: the parent's
/// lease runs out, the reaper takes its record, and `forget_reaped_branches` drops the capture.
#[test]
fn reaping_a_parent_whose_child_published_its_write_loses_the_dependent() {
    let mut db = Db::new();
    db.seed();
    let (m1, _parent, pbranch, ptxn) = parent_write_published_through_a_child(&mut db);

    let mut main = db.session();
    let blocked = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert_eq!(blocked.blocked_by, vec![ptxn], "got {:?}", blocked.blocked_by);

    // Exactly what `TwoTierReaper::reap` does to the catalog record, with no page store in the
    // way: mark reaped, which bumps the generation and makes the old id an error.
    use ferrodb::branch::types::BranchState;
    let rec = db.runtime.branches().get(pbranch).unwrap();
    db.runtime
        .branches()
        .set_state(pbranch, rec.state, BranchState::Reaped)
        .unwrap();
    let dropped = db.runtime.forget_reaped_branches();
    assert_eq!(dropped, 1, "the sweep did not see the reaped parent; the fixture proves nothing");

    let after = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(
        after.is_blocked(),
        "the reaper took the parent's bookkeeping and with it the premise for a row that is \
         still published: blocked_by = {:?}. Row 9 still there: {}",
        after.blocked_by,
        db.has_row(9)
    );
}
