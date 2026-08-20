//! Probe: does the capability envelope have any READ dimension?
//!
//! Replays the exact setup of no_ddl_verb_is_governed_by_the_envelope_and_this_is_a_known_gap
//! and asks what the governed session can READ *before* any full-text index exists.

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::{table_id, AgentRuntime};
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::branch::{BranchCatalog, CapabilityEnvelope, ColumnCapability, Verb};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
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
        let (catalog, bp, txn, runtime) = Self::attach(dir.path().to_path_buf(), true);
        Db { _dir: dir, catalog, bp, txn, runtime }
    }

    fn attach(
        dir: PathBuf,
        fresh: bool,
    ) -> (Catalog, Arc<BufferPoolManager>, Arc<TxnManager>, Arc<AgentRuntime>) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(fresh)
            .open(dir.join("envelope.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.join("envelope.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        recover(&txn).unwrap();
        let catalog = if fresh {
            Catalog::create(bp.clone()).unwrap()
        } else {
            Catalog::open(bp.clone(), 1).unwrap()
        };
        let branches = Arc::new(LogBranchCatalog::open(&dir.join("branches.log"), 1).unwrap());
        let runtime =
            Arc::new(AgentRuntime::with_catalog(Arc::clone(&branches) as Arc<dyn BranchCatalog>));
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

    fn begin(&mut self, agent: &str, s: &mut Session) -> BranchId {
        self.ok(&format!("BEGIN AGENT SESSION AS '{agent}' RUN 'r_{agent}';"), s);
        s.agent.as_ref().unwrap().branch
    }
}

fn inventory_only() -> CapabilityEnvelope {
    CapabilityEnvelope::new(Verb::ALL, 1_000).allow(
        table_id("inventory").0,
        vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
    )
}

fn show(label: &str, r: Result<Outcome, FerroError>) -> bool {
    match r {
        Ok(Outcome::Rows(rows)) => {
            println!("PROBE {label}: Ok rows={rows:?}");
            true
        }
        Ok(o) => {
            let _ = &o; println!("PROBE {label}: Ok (non-rows)");
            true
        }
        Err(e) => {
            println!("PROBE {label}: Err({e})");
            false
        }
    }
}

#[test]
fn hole3_probe() {
    let mut db = Db::new();
    db.seed();
    {
        let mut s = db.session();
        db.ok("CREATE TABLE payroll_notes (id INTEGER NOT NULL, note VARCHAR(64));", &mut s);
        db.ok("INSERT INTO payroll_notes VALUES (1, 'severance clause');", &mut s);
    }
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();

    let mut a = db.session();
    db.begin("ddl", &mut a);

    // 1. writes to both forbidden tables are refused
    let w1 = db.exec("UPDATE payroll SET salary = 0 WHERE id = 1;", &mut a);
    assert!(!show("governed UPDATE payroll", w1), "write was NOT refused");
    let w2 = db.exec("UPDATE payroll_notes SET note = 'redacted' WHERE id = 1;", &mut a);
    assert!(!show("governed UPDATE payroll_notes", w2), "write was NOT refused");

    // 2. READS of the same forbidden tables, BEFORE any full-text index exists
    let r1 = db.exec("SELECT note FROM payroll_notes;", &mut a);
    let read_notes = show("governed SELECT note FROM payroll_notes (no ft index yet)", r1);
    let r2 = db.exec("SELECT salary FROM payroll;", &mut a);
    let read_payroll = show("governed SELECT salary FROM payroll (no index yet)", r2);

    // 3. SEARCH before the index exists, from a plain session
    let mut plain = db.session();
    let s0 = db.exec("SEARCH payroll_notes (note) FOR 'severance';", &mut plain);
    let searchable_before = show("plain SEARCH before index", s0);

    // 4. can the GOVERNED session itself use SEARCH after the index is built?
    db.ok("CREATE FULLTEXT INDEX ix_note ON payroll_notes (note);", &mut a);
    let s1 = db.exec("SEARCH payroll_notes (note) FOR 'severance';", &mut a);
    show("governed SEARCH after index", s1);
    let mut plain2 = db.session();
    let s2 = db.exec("SEARCH payroll_notes (note) FOR 'severance';", &mut plain2);
    show("plain SEARCH after index", s2);

    // 5. after DROP, is the pre-drop SELECT non-vacuous?
    let mut pre = db.session();
    let p = db.exec("SELECT id FROM payroll;", &mut pre);
    show("plain SELECT id FROM payroll BEFORE drop", p);

    println!(
        "SUMMARY: governed session could read payroll_notes={read_notes} payroll={read_payroll}; \
         SEARCH worked before index={searchable_before}"
    );
    assert!(
        read_notes && read_payroll,
        "the governed session could NOT read the forbidden tables — the finding is refuted"
    );
}
