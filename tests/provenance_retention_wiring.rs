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
