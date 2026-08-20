//! Probe: is `SELECT` inside a governed agent session actually governed by the envelope?
//!
//! The sentence under test (tests/integration_capability_envelope.rs:638-641) says the four verbs
//! SELECT / INSERT / UPDATE / DELETE are "Governed" and that "every one of them funnels into
//! AgentRuntime::stage_all". This drives a real SELECT against a table the envelope forbids.

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::{table_id, AgentRuntime};
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::branch::{BranchCatalog, CapabilityEnvelope, ColumnCapability, Verb};
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
use ferrodb::wal::recovery::recover;
use ferrodb::wal::txn::TxnManager;

const ID: u32 = 0;
const QTY: u32 = 1;

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let (catalog, bp, txn, runtime) = Self::attach(dir.path().to_path_buf());
        Db { _dir: dir, catalog, bp, txn, runtime }
    }

    fn attach(dir: PathBuf) -> (Catalog, Arc<BufferPoolManager>, Arc<TxnManager>, Arc<AgentRuntime>) {
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(dir.join("probe.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.join("probe.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        recover(&txn).unwrap();
        let catalog = Catalog::create(bp.clone()).unwrap();
        let branches = Arc::new(LogBranchCatalog::open(&dir.join("branches.log"), 1).unwrap());
        let runtime = Arc::new(AgentRuntime::with_catalog(Arc::clone(&branches) as Arc<dyn BranchCatalog>));
        (catalog, bp, txn, runtime)
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
        self.ok("CREATE TABLE payroll (id INTEGER NOT NULL, salary INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);
        self.ok("INSERT INTO payroll VALUES (1, 1000);", &mut s);
    }
}

fn inventory_only() -> CapabilityEnvelope {
    CapabilityEnvelope::new(Verb::ALL, 1_000).allow(
        table_id("inventory").0,
        vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
    )
}

#[test]
fn select_on_a_forbidden_table_is_not_governed() {
    let mut db = Db::new();
    db.seed();
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'probe' RUN 'r_probe';", &mut a);

    // Anti-vacuity: the envelope must really be in force and payroll must really be forbidden.
    let err = match db.exec("UPDATE payroll SET salary = -1 WHERE id = 1;", &mut a) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("payroll must be forbidden or this probe proves nothing"),
    };
    assert!(err.contains("may not write table `payroll`"), "unexpected refusal: {err}");
    println!("ANTI-VACUITY: governed UPDATE payroll refused: {err}");

    // And a permitted write must still succeed, so the session is live rather than broken.
    db.ok("UPDATE inventory SET qty = 5 WHERE id = 1;", &mut a);
    println!("ANTI-VACUITY: governed UPDATE inventory succeeded");

    // The claim under test.
    let out = db.exec("SELECT salary FROM payroll WHERE id = 1;", &mut a);
    match out {
        Ok(Outcome::Rows(rows)) => {
            println!("RESULT: governed SELECT salary FROM payroll -> Ok rows={rows:?}");
            assert_eq!(
                rows,
                vec![vec![Value::Integer(1000)]],
                "SELECT returned the forbidden table's contents"
            );
        }
        Ok(_) => panic!("SELECT returned a non-Rows outcome"),
        Err(e) => panic!("SELECT WAS GOVERNED after all -- claim would be correct: {e}"),
    }
}
