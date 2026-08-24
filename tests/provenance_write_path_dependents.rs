//! Adversarial sweep of the WRITE-PATH retention F1 added: which `UPDATE`/`DELETE` shapes name
//! their causal dependents, and which do not, measured against the CONTROL of the identical
//! workload with an explicit `SELECT` of the same region first.
//!
//! Every probe runs the whole workload from a fresh database, once per revert target, because an
//! unblocked revert mutates the state the next probe would read.

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
            .open(dir.path().join("wp.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("wp.wal")).unwrap());
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

    fn rows_of(&mut self, sql: &str) -> Vec<Vec<Value>> {
        let mut s = self.session();
        match self.ok(sql, &mut s) {
            Outcome::Rows(r) => r,
            _ => panic!("expected rows from {sql}, got a different Outcome variant"),
        }
    }
}

fn report(out: Outcome) -> MergeReport {
    match out {
        Outcome::Agent(AgentOutput::Merge(m)) => m,
        _ => panic!("expected a merge report, got a different Outcome variant"),
    }
}

fn plan(out: Outcome) -> RevertPlan {
    match out {
        Outcome::Agent(AgentOutput::Revert(p)) => p,
        _ => panic!("expected a revert plan, got a different Outcome variant"),
    }
}

/// Which of the three earlier merges a probe reverts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Which {
    /// The merge the dependent's scan really did consume. MUST be blocked.
    Target,
    /// A merge into a DIFFERENT TABLE, published before the dependent looked. MUST NOT be blocked,
    /// whatever the shape — this is the universal anti-vacuity arm.
    Offsite,
    /// A same-table merge far outside any bounded region, published before the dependent looked.
    Far,
}

struct Workload {
    /// The task whose merge is `Which::Target`.
    upstream: &'static [&'static str],
    /// An explicit `SELECT` over the same region as the dependent's clause. `None` is the BARE arm.
    control: Option<&'static str>,
    /// The dependent's statements, run in its own agent session before its `MERGE`.
    dependent: &'static [&'static str],
}

/// Run the whole workload from scratch and return the `blocked_by` txn numbers of one revert.
fn run_once(w: &Workload, which: Which) -> Result<Vec<u64>, String> {
    let mut db = Db::new();
    {
        let mut s = db.session();
        db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER, note VARCHAR(16));", &mut s);
        db.ok("INSERT INTO inventory VALUES (1, 20, 'a');", &mut s);
        db.ok("INSERT INTO inventory VALUES (2, 5, 'b');", &mut s);
        db.ok("CREATE TABLE other (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        db.ok("INSERT INTO other VALUES (1, 1);", &mut s);
    }

    // txn 1: the merge under test.
    let m_target = {
        let mut s = db.session();
        db.ok("BEGIN AGENT SESSION AS 'upstream' RUN 'r_up';", &mut s);
        for sql in w.upstream {
            db.ok(sql, &mut s);
        }
        let r = report(db.ok("MERGE;", &mut s));
        assert!(r.applied_to_target, "upstream failed to merge: {r}");
        r.merge_id
    };
    // txn 2: same table, far outside any bounded region.
    let m_far = {
        let mut s = db.session();
        db.ok("BEGIN AGENT SESSION AS 'far' RUN 'r_far';", &mut s);
        db.ok("INSERT INTO inventory VALUES (8, 500, 'z');", &mut s);
        let r = report(db.ok("MERGE;", &mut s));
        assert!(r.applied_to_target, "far failed to merge: {r}");
        r.merge_id
    };
    // txn 3: a different table entirely.
    let m_offsite = {
        let mut s = db.session();
        db.ok("BEGIN AGENT SESSION AS 'offsite' RUN 'r_off';", &mut s);
        db.ok("INSERT INTO other VALUES (9, 30);", &mut s);
        let r = report(db.ok("MERGE;", &mut s));
        assert!(r.applied_to_target, "offsite failed to merge: {r}");
        r.merge_id
    };

    // txn 4: the dependent.
    {
        let mut s = db.session();
        db.ok("BEGIN AGENT SESSION AS 'dep' RUN 'r_dep';", &mut s);
        if let Some(sel) = w.control {
            db.ok(sel, &mut s);
        }
        for sql in w.dependent {
            match db.exec(sql, &mut s) {
                Ok(_) => {}
                Err(e) => return Err(format!("dependent stmt `{sql}` failed: {e}")),
            }
        }
        match db.exec("MERGE;", &mut s) {
            Ok(o) => {
                let r = report(o);
                if !r.applied_to_target {
                    return Err(format!("dependent's MERGE was not applied: {r}"));
                }
            }
            Err(e) => return Err(format!("dependent's MERGE failed: {e}")),
        }
    }

    let target = match which {
        Which::Target => m_target,
        Which::Far => m_far,
        Which::Offsite => m_offsite,
    };
    let mut main = db.session();
    let p = plan(db.ok(&format!("REVERT MERGE {target};"), &mut main));
    Ok(p.blocked_by.iter().map(|t| t.0).collect())
}

/// The three answers for one workload, plus what the table looks like afterwards.
fn probe(w: &Workload) -> String {
    let t = run_once(w, Which::Target);
    let f = run_once(w, Which::Far);
    let o = run_once(w, Which::Offsite);
    format!("target={t:?} far={f:?} offsite={o:?}")
}

const UP_INSERT_IN: &[&str] = &["INSERT INTO inventory VALUES (7, 30, 'a');"];

/// Exploratory: prints every shape's answer. Never fails; the per-shape tests below are the
/// assertions.
#[test]
fn sweep() {
    let shapes: Vec<(&str, &'static [&'static str], Option<&'static str>, &'static [&'static str])> = vec![
        (
            "S2 range on non-pk col (known-good baseline)",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE qty >= 20 AND qty < 50;"),
            &["UPDATE inventory SET qty = qty + 1 WHERE qty >= 20 AND qty < 50;"],
        ),
        (
            "S1 UPDATE with NO WHERE at all",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory;"),
            &["UPDATE inventory SET note = 'k';"],
        ),
        (
            "S1d DELETE with NO WHERE at all",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory;"),
            &["DELETE FROM inventory;"],
        ),
        (
            "S3 top-level OR",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE qty = 30 OR qty = 31;"),
            &["UPDATE inventory SET note = 'k' WHERE qty = 30 OR qty = 31;"],
        ),
        (
            "S4 column compared to column",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE qty > id;"),
            &["UPDATE inventory SET note = 'k' WHERE qty > id;"],
        ),
        (
            "S5 equality on a NON-pk integer column",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE qty = 30;"),
            &["UPDATE inventory SET note = 'k' WHERE qty = 30;"],
        ),
        (
            "S6 equality on the pk",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE id = 7;"),
            &["UPDATE inventory SET note = 'k' WHERE id = 7;"],
        ),
        (
            "S13 equality on a VARCHAR non-pk column",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE note = 'a';"),
            &["UPDATE inventory SET qty = 1 WHERE note = 'a';"],
        ),
        (
            "S8 multi-column SET",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE qty >= 20 AND qty < 50;"),
            &["UPDATE inventory SET qty = qty + 1, note = 'k' WHERE qty >= 20 AND qty < 50;"],
        ),
        (
            "S9 DELETE by pk",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE id = 7;"),
            &["DELETE FROM inventory WHERE id = 7;"],
        ),
        (
            "S11 two sequential writes, first one scans the region",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE qty >= 20 AND qty < 50;"),
            &[
                "UPDATE inventory SET note = 'k' WHERE qty >= 20 AND qty < 50;",
                "INSERT INTO inventory VALUES (11, 7, 'x');",
            ],
        ),
        (
            "S14 open upper bound",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE qty >= 25;"),
            &["UPDATE inventory SET note = 'k' WHERE qty >= 25 AND note = 'a';"],
        ),
        (
            "S15 pk range spelling",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE id >= 7 AND id <= 7;"),
            &["UPDATE inventory SET note = 'k' WHERE id >= 7 AND id <= 7;"],
        ),
        (
            "S16 UPDATE of ONE cell upstream, scanned on a column it did not change",
            &["UPDATE inventory SET note = 'x' WHERE id = 1;"],
            Some("SELECT id FROM inventory WHERE qty >= 20 AND qty < 50;"),
            &["UPDATE inventory SET qty = qty + 1 WHERE qty >= 20 AND qty < 50;"],
        ),
        (
            "S17 literal on the LEFT of the comparison",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE 7 = id;"),
            &["UPDATE inventory SET note = 'k' WHERE 7 = id;"],
        ),
        (
            "S18 residual-only clause (note <> 'zz'), region unbounded",
            UP_INSERT_IN,
            Some("SELECT id FROM inventory WHERE note <> 'zz';"),
            &["UPDATE inventory SET qty = 1 WHERE note <> 'zz';"],
        ),
    ];

    let mut lines = Vec::new();
    for (name, up, ctl, dep) in shapes {
        let bare = probe(&Workload { upstream: up, control: None, dependent: dep });
        let ctrl = probe(&Workload { upstream: up, control: ctl, dependent: dep });
        lines.push(format!("{name}\n    BARE    {bare}\n    CONTROL {ctrl}"));
    }
    println!("\n=== WRITE-PATH DEPENDENT SWEEP (dependent is txn 4) ===\n{}", lines.join("\n"));
}
