//! D4: quarantine — a branch a gate declined stays **unmerged but still queryable**.
//!
//! Both halves are the point, and each is worthless without the other. Not merging is what
//! declining means. Staying queryable is what separates quarantine from rejection: a branch that
//! tripped a *heuristic* has not been shown to be wrong, and discarding it destroys the evidence
//! someone needs in order to decide whether it was. A hold you cannot read is a deletion with
//! extra steps.
//!
//! Quarantine here is a **mechanism, not a policy**. Nothing decides on its own to invoke it — in
//! particular the blind-write tier still reports without deciding, which `integration_blind_writes`
//! pins. Wiring "which findings warrant a hold" is the gate's business and is not this row.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::types::{BranchId, BranchState, LeaseDeadline};
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::BranchCatalog;
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
            .open(dir.path().join("q.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("q.wal")).unwrap());
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
        assert_eq!(stmts.len(), 1, "expected one statement: {}", sql);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 100);", &mut s);
    }

    fn qty_on(&mut self, branch_name: &str, id: i32) -> Option<i32> {
        let mut s = self.session();
        let sql = format!("SELECT qty FROM inventory AS OF BRANCH {branch_name} WHERE id = {id};");
        match self.ok(&sql, &mut s) {
            Outcome::Rows(rows) => rows.first().and_then(|r| match r.first() {
                Some(Value::Integer(i)) => Some(*i),
                _ => None,
            }),
            _ => panic!("expected rows from: {sql}"),
        }
    }

    fn main_qty(&mut self, id: i32) -> Option<i32> {
        let mut s = self.session();
        let sql = format!("SELECT qty FROM inventory WHERE id = {id};");
        match self.ok(&sql, &mut s) {
            Outcome::Rows(rows) => rows.first().and_then(|r| match r.first() {
                Some(Value::Integer(i)) => Some(*i),
                _ => None,
            }),
            _ => panic!("expected rows from: {sql}"),
        }
    }
}

/// Sets up a branch with an uncommitted write, then holds it.
fn held() -> (Db, BranchId, String) {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'suspect-agent' RUN 'r_s';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;
    let name = a.agent.as_ref().unwrap().branch_name.clone();
    db.ok("UPDATE inventory SET qty = 42 WHERE id = 1;", &mut a);
    db.runtime.quarantine(branch, "blind write on inventory row 1").unwrap();
    (db, branch, name)
}

#[test]
fn a_quarantined_branch_is_not_merged_and_main_is_untouched() {
    let (mut db, _branch, _name) = held();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'x' RUN 'r_x';", &mut a);
    // main must still read its original value: nothing from the held branch reached it.
    assert_eq!(db.main_qty(1), Some(100), "a held branch's write reached main");
}

#[test]
fn a_quarantined_branch_is_still_queryable() {
    let (mut db, branch, name) = held();

    // The whole distinction from rejection: the evidence is still there to look at.
    assert_eq!(
        db.qty_on(&name, 1),
        Some(42),
        "a quarantined branch is not readable, which makes the hold a deletion with extra steps"
    );
    assert!(
        db.runtime.branches().get(branch).is_ok(),
        "the branch record itself became unreadable"
    );
}

/// A hold that a merge can walk through is advisory, and an advisory hold is not a hold.
#[test]
fn merging_a_quarantined_branch_is_refused_with_the_reason() {
    let (mut db, branch, _name) = held();
    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ferrodb::agent_sql::runtime::ExecCtx { catalog: &mut db.catalog, bp, txn };

    let e = db.runtime.merge(&mut ctx, branch).unwrap_err();
    let msg = format!("{e}");
    assert!(msg.contains("quarantined"), "merge was not refused for quarantine: {msg}");
    assert!(
        msg.contains("blind write on inventory row 1"),
        "the refusal must name why the branch is held, or the operator has to go looking: {msg}"
    );
}

#[test]
fn a_held_branch_is_listed_with_its_reason_and_distinguishable_from_live() {
    let (db, branch, _name) = held();

    assert_eq!(db.runtime.quarantined_branches().unwrap(), vec![branch]);
    assert_eq!(
        db.runtime.quarantine_reason(branch).as_deref(),
        Some("blind write on inventory row 1")
    );
    assert_eq!(db.runtime.branches().get(branch).unwrap().state, BranchState::Quarantined);
}

#[test]
fn releasing_a_branch_returns_it_to_service_and_it_merges() {
    let (mut db, branch, _name) = held();
    db.runtime.release_from_quarantine(branch).unwrap();

    assert!(db.runtime.quarantined_branches().unwrap().is_empty());
    assert_eq!(db.runtime.branches().get(branch).unwrap().state, BranchState::Live);
    assert_eq!(db.runtime.quarantine_reason(branch), None, "the reason outlived the hold");

    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ferrodb::agent_sql::runtime::ExecCtx { catalog: &mut db.catalog, bp, txn };
    let report = db.runtime.merge(&mut ctx, branch).expect("a released branch must merge");
    assert!(report.applied_to_target, "released branch did not publish: {:?}", report.outcome);
    assert_eq!(db.main_qty(1), Some(42), "the released branch's write did not reach main");
}

#[test]
fn releasing_a_branch_that_is_not_held_is_refused() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'z' RUN 'r_z';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;
    assert!(
        db.runtime.release_from_quarantine(branch).is_err(),
        "releasing a live branch silently succeeded, so release cannot be trusted to mean anything"
    );
}

/// The state has to survive the record log, or a restart quietly frees every held branch.
#[test]
fn the_quarantined_state_round_trips_through_the_record_log() {
    let catalog = LogBranchCatalog::in_memory(1);
    let child = catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
    let mut rec = catalog.get(child.branch_id).unwrap();
    rec.state = BranchState::Quarantined;

    let bytes = rec.serialize();
    let back = ferrodb::branch::record::BranchRecord::deserialize(&bytes).unwrap();
    assert_eq!(back.state, BranchState::Quarantined, "the hold did not survive serialization");
    assert_eq!(back.branch_id, child.branch_id);
}

/// Appending the new tag must not have moved the existing ones, or every record already on disk
/// changes meaning on upgrade.
#[test]
fn the_existing_state_tags_kept_their_values() {
    assert_eq!(BranchState::Live.as_u8(), 0);
    assert_eq!(BranchState::Reaping.as_u8(), 1);
    assert_eq!(BranchState::Reaped.as_u8(), 2);
    assert_eq!(BranchState::Quarantined.as_u8(), 3, "the new state must be appended, not inserted");
    assert!(BranchState::from_u8(4).is_err(), "an unknown tag must be refused, not defaulted");
}
