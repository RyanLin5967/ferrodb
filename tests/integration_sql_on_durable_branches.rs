//! The agent SQL surface running on the **durable** branch engine.
//!
//! `tests/agent_sql_surface.rs` exercises the surface on `MemBranchCatalog`, the in-memory
//! stand-in the SQL agent wrote for itself, and the branch engine's own tests exercise
//! `LogBranchCatalog` with no SQL above it. Neither proves the pieces compose, so this file runs
//! the exit-criteria demo path — isolation, arithmetic composition, guard rejection, abandon —
//! against the real catalog, with its durable record log, generation-guarded ids and
//! append-only fork-epoch arrays.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::MergeReport;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::BranchId;
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
use ferrodb::tel::merge::MergeOutcome;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    branches: Arc<LogBranchCatalog>,
    _dir: tempfile::TempDir,
}

impl Db {
    /// The only difference from the surface tests' harness: the runtime is built over the
    /// durable `LogBranchCatalog` instead of `MemBranchCatalog`.
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

        let branches =
            Arc::new(LogBranchCatalog::open(&dir.path().join("branches.log"), 1).unwrap());
        let runtime = Arc::new(AgentRuntime::with_catalog(
            Arc::clone(&branches) as Arc<dyn BranchCatalog>
        ));
        Db { catalog, bp, txn, runtime, branches, _dir: dir }
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
}

fn rows(out: Outcome) -> Vec<Vec<Value>> {
    match out {
        Outcome::Rows(r) => r,
        other => panic!("expected rows, got {:?}", std::mem::discriminant(&other)),
    }
}

fn report(out: Outcome) -> MergeReport {
    match out {
        Outcome::Agent(AgentOutput::Merge(m)) => m,
        other => panic!("expected a merge report, got {}", agent_str(other)),
    }
}

fn agent_str(out: Outcome) -> String {
    match out {
        Outcome::Agent(a) => a.to_string(),
        _ => "<non-agent outcome>".into(),
    }
}

fn qty_of(db: &mut Db, id: i64) -> i32 {
    let mut s = db.session();
    let r = rows(db.ok(&format!("SELECT qty FROM inventory WHERE id = {};", id), &mut s));
    match r[0][0] {
        Value::Integer(n) => n,
        ref v => panic!("qty is not an integer: {:?}", v),
    }
}

#[test]
fn branch_writes_stay_invisible_to_main_on_the_durable_catalog() {
    // Exit criterion 2, against the real branch engine.
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    assert_eq!(qty_of(&mut db, 1), 20, "the branch write leaked to main before MERGE");

    let m = report(db.ok("MERGE;", &mut a));
    assert_eq!(m.outcome, MergeOutcome::Clean);
    assert_eq!(qty_of(&mut db, 1), 15);
}

#[test]
fn two_branches_decrementing_compose_arithmetically_on_the_durable_catalog() {
    // Exit criterion 6. 20 - 5 - 3 = 12, which is neither branch's own answer.
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 3 WHERE id = 1;", &mut b);

    assert_eq!(report(db.ok("MERGE;", &mut a)).outcome, MergeOutcome::Clean);
    assert_eq!(qty_of(&mut db, 1), 15);

    let second = report(db.ok("MERGE;", &mut b));
    assert!(
        matches!(second.outcome, MergeOutcome::Commuting { .. }),
        "expected Commuting, got {}",
        second
    );
    assert_eq!(qty_of(&mut db, 1), 12);
}

#[test]
fn a_guard_violation_is_rejected_with_its_predicate_on_the_durable_catalog() {
    // Exit criterion 7: the violated predicate comes back verbatim and nothing is published.
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b);
    // Row 2 holds 5. Each branch legally takes 4 against `qty >= 4`; together they overdraw.
    db.ok("UPDATE inventory SET qty = qty - 4 WHERE id = 2 AND qty >= 4;", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 4 WHERE id = 2 AND qty >= 4;", &mut b);

    assert_eq!(report(db.ok("MERGE;", &mut a)).outcome, MergeOutcome::Clean);
    assert_eq!(qty_of(&mut db, 2), 1);

    let second = report(db.ok("MERGE;", &mut b));
    assert!(
        matches!(second.outcome, MergeOutcome::Conflict { .. }),
        "expected Conflict, got {}",
        second
    );
    assert!(!second.applied_to_target, "a conflicting merge published anyway");
    assert_eq!(qty_of(&mut db, 2), 1, "state moved despite the conflict");
    assert!(
        second.to_string().contains("qty >= 4"),
        "the violated predicate was not returned: {}",
        second
    );
}

#[test]
fn abandon_marks_the_branch_reaped_in_the_durable_record_log() {
    // The surface's ABANDON goes through the BranchCatalog trait only. Against the real engine
    // that must bump the generation, so the old id is a hard error rather than stale data, and
    // must drop the fork epoch from the parent's live-children array — the array the interval
    // rule reads to decide what is reclaimable.
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let branch = match db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a) {
        Outcome::Agent(AgentOutput::SessionStarted(s)) => s.branch,
        other => panic!("expected a session, got {}", agent_str(other)),
    };
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);

    assert_eq!(
        db.branches.get(BranchId::TRUNK).unwrap().live_children.len(),
        1,
        "the fork was not recorded in the parent"
    );

    db.ok("ABANDON;", &mut a);

    assert!(
        db.branches.get(branch).is_err(),
        "reading an abandoned branch returned data instead of a hard error"
    );
    assert!(
        db.branches.get(BranchId::TRUNK).unwrap().live_children.is_empty(),
        "the abandoned branch still pins its parent's pages"
    );
    assert_eq!(qty_of(&mut db, 1), 20, "an abandoned branch's writes reached main");
}
