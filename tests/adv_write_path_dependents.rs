//! ADVERSARIAL PROBE (not for merge as-is): dependents the write-path retention still misses.
//!
//! Lens: F1 retains the region an `UPDATE`/`DELETE` `WHERE` clause scanned. Every test here is a
//! workload where a revert SHOULD halt. Each carries its CONTROL: the identical workload with an
//! explicit `SELECT` of the same region first.

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


// =================================================================================================
// P1: an INSERT's own uniqueness scan
// =================================================================================================

/// The BARE insert. The pruner deletes row 2 and merges; the filler inserts (2, 999) — which is only
/// admitted because `branch_insert`'s duplicate-key check scanned `visible_rows` and found key 2
/// absent. Reverting the pruner puts row 2 back. Should halt.
#[test]
fn p1_bare_insert_after_a_delete_caused_absence() {
    let mut db = Db::new();
    db.seed();

    let mut p = db.session();
    db.ok("BEGIN AGENT SESSION AS 'pruner' RUN 'r_prune';", &mut p);
    db.ok("DELETE FROM inventory WHERE id = 2;", &mut p);
    let m1 = report(db.ok("MERGE;", &mut p)).merge_id;
    assert!(!db.has_row(2), "premise: the pruner removed row 2");

    let mut f = db.session();
    db.ok("BEGIN AGENT SESSION AS 'filler' RUN 'r_fill';", &mut f);
    // NO SELECT. The insert's own uniqueness check is the scan.
    db.ok("INSERT INTO inventory VALUES (2, 999);", &mut f);
    let merged = report(db.ok("MERGE;", &mut f));
    assert!(merged.applied_to_target, "{}", merged);
    assert_eq!(db.qty_of(2), 999);

    let mut main = db.session();
    let halted = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    eprintln!("P1 bare-insert blocked_by = {:?}", halted.blocked_by);
    assert!(
        halted.is_blocked(),
        "the insert was admitted BY the absence this merge caused; the revert must not proceed"
    );
}

/// CONTROL for P1: identical, plus an explicit `SELECT ... WHERE id = 2` first.
#[test]
fn p1_control_insert_preceded_by_an_explicit_select() {
    let mut db = Db::new();
    db.seed();

    let mut p = db.session();
    db.ok("BEGIN AGENT SESSION AS 'pruner' RUN 'r_prune';", &mut p);
    db.ok("DELETE FROM inventory WHERE id = 2;", &mut p);
    let m1 = report(db.ok("MERGE;", &mut p)).merge_id;

    let mut f = db.session();
    db.ok("BEGIN AGENT SESSION AS 'filler' RUN 'r_fill';", &mut f);
    let seen = rows(db.ok("SELECT id, qty FROM inventory WHERE id = 2;", &mut f));
    assert!(seen.is_empty(), "{seen:?}");
    db.ok("INSERT INTO inventory VALUES (2, 999);", &mut f);
    assert!(report(db.ok("MERGE;", &mut f)).applied_to_target);

    let mut main = db.session();
    let halted = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    eprintln!("P1 control blocked_by = {:?}", halted.blocked_by);
    assert!(halted.is_blocked(), "the control must halt, or the harness proves nothing");
}

/// What the unblocked revert in P1 actually DOES: the contract promises a dependency tree or a
/// completed revert, and this is the third thing.
#[test]
fn p1_consequence_the_unblocked_revert_is_executed() {
    let mut db = Db::new();
    db.seed();

    let mut p = db.session();
    db.ok("BEGIN AGENT SESSION AS 'pruner' RUN 'r_prune';", &mut p);
    db.ok("DELETE FROM inventory WHERE id = 2;", &mut p);
    let m1 = report(db.ok("MERGE;", &mut p)).merge_id;

    let mut f = db.session();
    db.ok("BEGIN AGENT SESSION AS 'filler' RUN 'r_fill';", &mut f);
    db.ok("INSERT INTO inventory VALUES (2, 999);", &mut f);
    assert!(report(db.ok("MERGE;", &mut f)).applied_to_target);
    assert_eq!(db.qty_of(2), 999, "the filler's row is live");

    let mut main = db.session();
    let outcome = db.exec(&format!("REVERT MERGE {};", m1), &mut main);
    match &outcome {
        Ok(o) => eprintln!("P1 consequence: revert returned Ok({o:?})"),
        Err(e) => eprintln!("P1 consequence: revert returned Err({e})"),
    }
    let after: Vec<Vec<Value>> = db.all();
    eprintln!("P1 consequence: table after = {after:?}");
    // The filler's 999 must survive: either the revert halted, or it must not have clobbered it.
    assert_eq!(
        db.qty_of(2),
        999,
        "the revert overwrote the filler's row without ever naming it as a dependent"
    );
}

// =================================================================================================
// P2: a write statement that matched ZERO rows
// =================================================================================================

/// `DELETE ... WHERE qty >= 20 AND id > 1000` matches nothing (no id exceeds 1000), but the region
/// the summary keeps is `qty >= 20` — which covers the row the earlier merge published. The task
/// then publishes an unrelated insert, so its capture is not the "published nothing" case.
#[test]
fn p2_zero_row_write_scan_still_names_its_dependency() {
    let mut db = Db::new();
    db.seed();
    let m1 = insert_and_merge(&mut db, "restock-agent", "r_restock", 7, 30);

    let mut t = db.session();
    db.ok("BEGIN AGENT SESSION AS 'sweeper' RUN 'r_sweep';", &mut t);
    let n = affected(db.ok("DELETE FROM inventory WHERE qty >= 20 AND id > 1000;", &mut t));
    assert_eq!(n, 0, "premise: the sweep matched nothing");
    db.ok("INSERT INTO inventory VALUES (9, 1);", &mut t);
    assert!(report(db.ok("MERGE;", &mut t)).applied_to_target);

    let mut main = db.session();
    let halted = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    eprintln!("P2 zero-row blocked_by = {:?}", halted.blocked_by);
    assert!(halted.is_blocked(), "a scan that matched no rows still scanned the region");
}

/// CONTROL for P2.
#[test]
fn p2_control_select_of_the_same_region() {
    let mut db = Db::new();
    db.seed();
    let m1 = insert_and_merge(&mut db, "restock-agent", "r_restock", 7, 30);

    let mut t = db.session();
    db.ok("BEGIN AGENT SESSION AS 'sweeper' RUN 'r_sweep';", &mut t);
    let seen = rows(db.ok("SELECT id, qty FROM inventory WHERE qty >= 20 AND id > 1000;", &mut t));
    assert!(seen.is_empty(), "{seen:?}");
    db.ok("INSERT INTO inventory VALUES (9, 1);", &mut t);
    assert!(report(db.ok("MERGE;", &mut t)).applied_to_target);

    let mut main = db.session();
    let halted = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    eprintln!("P2 control blocked_by = {:?}", halted.blocked_by);
    assert!(halted.is_blocked(), "the control must halt");
}

// =================================================================================================
// P3: shapes the lens listed, run through UPDATE/DELETE, as a sanity sweep
// =================================================================================================

/// Each arm: restock publishes (7, 30) and merges; a second task runs ONE write statement whose
/// clause covers row 7; the revert of the restock must halt.
fn sweep(sql: &str) -> Vec<TxnId> {
    let mut db = Db::new();
    db.seed();
    let m1 = insert_and_merge(&mut db, "restock-agent", "r_restock", 7, 30);
    let mut t = db.session();
    db.ok("BEGIN AGENT SESSION AS 'rmw' RUN 'r_rmw';", &mut t);
    db.ok(sql, &mut t);
    let r = report(db.ok("MERGE;", &mut t));
    assert!(r.applied_to_target, "{sql} failed to merge: {r}");
    let mut main = db.session();
    plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main)).blocked_by
}

#[test]
fn p3_shape_sweep() {
    let cases: Vec<(&str, &str)> = vec![
        ("no WHERE at all", "UPDATE inventory SET qty = qty + 1;"),
        ("non-pk column =", "UPDATE inventory SET qty = qty + 1 WHERE qty = 30;"),
        ("top-level OR", "UPDATE inventory SET qty = qty + 1 WHERE qty = 30 OR qty = 20;"),
        ("column vs column", "UPDATE inventory SET qty = qty + 1 WHERE qty > id;"),
        ("pk = literal", "UPDATE inventory SET qty = qty + 1 WHERE id = 7;"),
        ("range", "UPDATE inventory SET qty = qty + 1 WHERE qty >= 20 AND qty < 50;"),
        ("!= on non-pk", "UPDATE inventory SET qty = qty + 1 WHERE qty != 5;"),
        ("delete no WHERE", "DELETE FROM inventory;"),
        ("delete pk = literal", "DELETE FROM inventory WHERE id = 7;"),
        ("delete range", "DELETE FROM inventory WHERE qty >= 20;"),
    ];
    let mut misses: Vec<&str> = Vec::new();
    for (label, sql) in cases {
        let blocked = sweep(sql);
        eprintln!("P3 [{label}] {sql} -> blocked_by = {blocked:?}");
        if blocked.is_empty() {
            misses.push(label);
        }
    }
    assert!(misses.is_empty(), "these shapes named no dependent: {misses:?}");
}
