//! ADVERSARIAL PROBE for I21/F5 — "record_read refuses when the reading branch has no workspace".
//! Not a deliverable test file; it exists to measure which flows the refusal breaks.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::AgentOutput;
use ferrodb::agent_sql::MergeReport;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::LeaseDeadline;
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::provenance::revert::RevertPlan;
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
    reaper: Arc<TwoTierReaper>,
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
            .open(dir.path().join("probe.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("probe.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        let branches = Arc::new(LogBranchCatalog::in_memory(1));
        let store =
            Arc::new(ArenaPageStore::new(bp.clone(), Arc::clone(&branches), ARENA_BASE).unwrap());
        let reaper = Arc::new(TwoTierReaper::new(Arc::clone(&branches), Arc::clone(&store)));
        let runtime = Arc::new(
            AgentRuntime::with_storage(
                Arc::clone(&branches) as Arc<dyn BranchCatalog>,
                Arc::new(MemEffectLog::new()),
                Arc::clone(&store) as Arc<dyn PageStore>,
            )
            .unwrap(),
        );
        Db { catalog, bp, txn, runtime, reaper, _dir: dir }
    }

    /// Same runtime, no reaper attached, so `seal` does not also reclaim pages.
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
        self.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 5);", &mut s);
    }
}

fn rows(o: Outcome) -> Vec<Vec<Value>> {
    match o {
        Outcome::Rows(r) => r,
        other => panic!("expected rows, got {:?}", std::mem::discriminant(&other)),
    }
}

fn agent(o: Outcome) -> AgentOutput {
    match o {
        Outcome::Agent(a) => a,
        _ => panic!("expected agent output"),
    }
}

fn report(o: Outcome) -> MergeReport {
    match agent(o) {
        AgentOutput::Merge(m) => m,
        other => panic!("expected merge report, got {}", other),
    }
}

fn plan(o: Outcome) -> RevertPlan {
    match agent(o) {
        AgentOutput::Revert(p) => p,
        other => panic!("expected revert plan, got {}", other),
    }
}

fn started(o: Outcome) -> String {
    match agent(o) {
        AgentOutput::SessionStarted(s) => s.branch_name,
        other => panic!("expected a session, got {}", other),
    }
}

// ------------------------------------------------------------------------------------------------
// P1: a NON-agent connection reading AS OF BRANCH — reader must be None, nothing retained, Ok.
// ------------------------------------------------------------------------------------------------
#[test]
fn p1_non_agent_as_of_branch_reads_fine_and_retains_nothing() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let name = started(db.ok("BEGIN AGENT SESSION AS 'restock' RUN 'r1';", &mut a));
    db.ok("UPDATE inventory SET qty = qty + 30 WHERE id = 1;", &mut a);

    // Non-agent connection.
    let mut obs = db.session();
    let seen = rows(db.ok(&format!("SELECT qty FROM inventory AS OF BRANCH {name};"), &mut obs));
    println!("P1 non-agent AS OF -> {seen:?}");
    assert_eq!(seen.len(), 2);

    // CONTROL: the same connection AFTER the branch is abandoned by someone else. The name is gone
    // from the runtime, so the binder refuses -- that is the shape a non-agent reader hits.
    let mut other = db.session();
    db.ok(&format!("ABANDON BRANCH {name};"), &mut other);
    match db.exec(&format!("SELECT qty FROM inventory AS OF BRANCH {name};"), &mut obs) {
        Ok(o) => println!("P1 control after abandon -> OK {:?}", rows(o)),
        Err(e) => println!("P1 control after abandon -> ERR {e}"),
    }
}

// ------------------------------------------------------------------------------------------------
// P2: an agent session reading ANOTHER live branch AS OF.
// ------------------------------------------------------------------------------------------------
#[test]
fn p2_agent_session_reads_another_live_branch_as_of() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let a_name = started(db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a));
    db.ok("UPDATE inventory SET qty = qty + 30 WHERE id = 1;", &mut a);

    let mut b = db.session();
    started(db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r2';", &mut b));
    let seen = rows(db.ok(&format!("SELECT qty FROM inventory AS OF BRANCH {a_name};"), &mut b));
    println!("P2 agent b reading {a_name} -> {seen:?}");
    assert_eq!(seen.len(), 2);

    // Now b's OWN branch is sealed by a third connection, and b reads A's STILL-LIVE branch.
    let b_name = b.agent.as_ref().unwrap().branch_name.clone();
    let mut c = db.session();
    db.ok(&format!("ABANDON BRANCH {b_name};"), &mut c);
    let r = db.exec(&format!("SELECT qty FROM inventory AS OF BRANCH {a_name};"), &mut b);
    match &r {
        Ok(_) => println!("P2b reading a LIVE branch after MY branch died -> Ok"),
        Err(e) => println!("P2b reading a LIVE branch after MY branch died -> ERR {e}"),
    }
    assert!(r.is_err(), "expected the F5 refusal here; if this fires the probe is stale");
}

// ------------------------------------------------------------------------------------------------
// P3: a SUPERVISOR merges an agent's branch BY NAME. The agent's connection stays open with
// `session.agent` still set. What can it still do?
// ------------------------------------------------------------------------------------------------
#[test]
fn p3_supervisor_merge_by_name_leaves_the_agent_connection_inert() {
    let mut db = Db::new();
    db.seed();
    let mut worker = db.session();
    let name = started(db.ok("BEGIN AGENT SESSION AS 'worker' RUN 'r_w';", &mut worker));
    db.ok("UPDATE inventory SET qty = qty + 30 WHERE id = 1;", &mut worker);

    // Anti-vacuity: while the session is live, the plain SELECT works.
    let live = rows(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut worker));
    println!("P3 live plain SELECT -> {live:?}");
    assert_eq!(live.len(), 1);

    // A supervisor connection merges the worker's branch BY NAME. This is a first-class statement.
    let mut sup = db.session();
    let r = report(db.ok(&format!("MERGE BRANCH {name};"), &mut sup));
    assert!(r.applied_to_target, "the supervisor's merge did not apply: {r}");
    println!("P3 supervisor merged {name}: {r}");
    // The worker's rows ARE published now.
    let mut plainconn = db.session();
    let pub_rows = rows(db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut plainconn));
    println!("P3 published state on a fresh connection -> {pub_rows:?}");
    assert_eq!(pub_rows[0][0], Value::Integer(50));

    // Now: EVERY statement the worker connection can issue.
    for sql in [
        "SELECT qty FROM inventory WHERE id = 1;",
        "SELECT qty FROM inventory;",
        "UPDATE inventory SET qty = 1 WHERE id = 1;",
        "MERGE;",
        "DIFF;",
        "BEGIN AGENT SESSION AS 'worker2' RUN 'r_w2';",
        "COMMIT;",
        "ROLLBACK;",
    ] {
        match db.exec(sql, &mut worker) {
            Ok(o) => println!("P3   {sql:55} -> OK   {:?}", std::mem::discriminant(&o)),
            Err(e) => println!("P3   {sql:55} -> ERR  {e}"),
        }
    }
    match db.exec(&format!("ABANDON BRANCH {name};"), &mut worker) {
        Ok(_) => println!("P3   ABANDON BRANCH {name} -> OK"),
        Err(e) => println!("P3   ABANDON BRANCH {name} -> ERR {e}"),
    }
    println!("P3 worker session.agent still set? {}", worker.agent.is_some());
}

// ------------------------------------------------------------------------------------------------
// P4: criterion 8 — the LEASE REAPER takes the branch with NO client cooperation. F5's guard reads
// `state.workspaces`, which the reaper does not touch. Does the read refuse? And what does the
// retention it keeps do to REVERT?
// ------------------------------------------------------------------------------------------------
#[test]
fn p4_a_read_on_a_branch_the_reaper_took_is_not_refused_and_pins_revert_forever() {
    let mut db = Db::new();
    db.seed();

    // A published merge to revert later.
    let m1 = {
        let mut s = db.session();
        db.ok("BEGIN AGENT SESSION AS 'restock' RUN 'r_restock';", &mut s);
        db.ok("INSERT INTO inventory VALUES (7, 30);", &mut s);
        let r = report(db.ok("MERGE;", &mut s));
        assert!(r.applied_to_target);
        r.merge_id
    };

    // A ghost task: it scans the region, then its lease expires and the reaper takes it.
    let mut ghost = db.session();
    let name = started(db.ok("BEGIN AGENT SESSION AS 'ghost' RUN 'r_ghost';", &mut ghost));
    let before = rows(db.ok(
        "SELECT id, qty FROM inventory WHERE qty >= 20 AND qty < 50;",
        &mut ghost,
    ));
    assert_eq!(before.len(), 2, "anti-vacuity: the scan must see rows 1 and 7: {before:?}");

    // No client cooperation at all: the lease expires and the reaper reclaims it.
    let taken = db
        .reaper
        .reap_expired(LeaseDeadline::now_millis() + 16 * 60 * 1000)
        .expect("reap");
    println!("P4 reaper took {taken:?} (ghost branch was {name})");
    assert!(!taken.is_empty(), "the reaper took nothing; the probe measured nothing");

    // The read on a branch that IS gone.
    let after = db.exec("SELECT id, qty FROM inventory WHERE qty >= 20 AND qty < 50;", &mut ghost);
    match &after {
        Ok(o) => println!(
            "P4 read on a REAPED branch -> OK with {} row(s)  <-- F5 did NOT fire",
            match o { Outcome::Rows(r) => r.len(), _ => 0 }
        ),
        Err(e) => println!("P4 read on a REAPED branch -> ERR {e}"),
    }

    // Every other operation on that session, for comparison.
    for sql in ["UPDATE inventory SET qty = 1 WHERE id = 1;", "MERGE;"] {
        match db.exec(sql, &mut ghost) {
            Ok(_) => println!("P4   {sql:45} -> OK"),
            Err(e) => println!("P4   {sql:45} -> ERR {e}"),
        }
    }

    // And what the retention it kept does to a revert.
    let mut main = db.session();
    let p = plan(db.ok(&format!("REVERT MERGE {m1};"), &mut main));
    println!("P4 REVERT MERGE {m1} blocked_by = {:?}", p.blocked_by);
    println!("P4 forget_reaped_branches() = {}", db.runtime.forget_reaped_branches());
    let p2 = plan(db.ok(&format!("REVERT MERGE {m1};"), &mut main));
    println!("P4 after forget, blocked_by = {:?}", p2.blocked_by);
}

// ------------------------------------------------------------------------------------------------
// P5: message accuracy. F1 moved `record_write_scan` in front of `stage_all`, whose refusal was
// deliberately ordered first. What does an UPDATE on a sealed branch now say?
// ------------------------------------------------------------------------------------------------
#[test]
fn p5_an_update_on_a_sealed_branch_now_reports_a_read_error() {
    let mut db = Db::new();
    db.seed();
    let mut victim = db.session();
    let name = started(db.ok("BEGIN AGENT SESSION AS 'victim' RUN 'r_v';", &mut victim));
    let mut other = db.session();
    db.ok(&format!("ABANDON BRANCH {name};"), &mut other);

    // A WHERE that is a scan (goes through record_write_scan -> record_read).
    for sql in [
        "UPDATE inventory SET qty = qty + 1 WHERE qty >= 20 AND qty < 50;",
        "UPDATE inventory SET qty = 3 WHERE id = 1;",
        "DELETE FROM inventory WHERE qty >= 20;",
        "DELETE FROM inventory WHERE id = 2;",
        "INSERT INTO inventory VALUES (9, 1);",
    ] {
        match db.exec(sql, &mut victim) {
            Ok(_) => println!("P5   {sql:60} -> OK (admitted!)"),
            Err(e) => println!("P5   {sql:60} -> ERR {e}"),
        }
    }
}
