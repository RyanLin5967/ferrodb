//! D2: `write-set \ read-set` — rows an agent changed without ever looking at them.
//!
//! DESIGN.md section 4 calls this the cheap novel metric and says to implement it first: nobody
//! else in the data-quality literature has it, for the mundane reason that nobody else retains
//! read-sets. One set difference, no threshold to tune.
//!
//! The first thing these tests establish is that it can **fire at all**. A metric wired into a
//! path that always records a read would report an empty set forever and look like a clean bill of
//! health, which is the failure mode that matters here — a detector nobody has forced to fire is
//! not evidence of anything.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::MergeReport;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
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
            .open(dir.path().join("blind.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("blind.wal")).unwrap());
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
        self.ok("INSERT INTO inventory VALUES (2, 200);", &mut s);
        self.ok("INSERT INTO inventory VALUES (3, 300);", &mut s);
    }

    fn report(&mut self, o: Outcome) -> MergeReport {
        match o {
            Outcome::Agent(a) => match a {
                ferrodb::agent_sql::AgentOutput::Merge(r) => r,
                _ => panic!("expected a merge report"),
            },
            _ => panic!("expected an agent outcome"),
        }
    }
}

/// The forcing test. If this ever reports an empty set, the metric is dead and every other test in
/// this file is vacuously green.
#[test]
fn a_row_written_without_being_read_is_reported_blind() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'blind-agent' RUN 'r_b';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;

    db.ok("UPDATE inventory SET qty = 7 WHERE id = 1;", &mut a);

    let blind = db.runtime.blind_writes(branch).unwrap();
    assert!(
        !blind.is_empty(),
        "the agent wrote a row it never read and the metric reported nothing; either reads are \
         being recorded by the write path, or the metric is not wired in"
    );
    assert_eq!(blind.len(), 1, "expected exactly the one written row, got {blind:?}");
    assert_eq!(blind[0].1 .0, 1, "wrong row reported blind: {blind:?}");
}

/// The other half: looking first must clear it. Without this, "reports blind" could just mean
/// "reports every write".
#[test]
fn a_row_read_before_being_written_is_not_blind() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'careful-agent' RUN 'r_c';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;
    let name = a.agent.as_ref().unwrap().branch_name.clone();

    db.ok(&format!("SELECT qty FROM inventory AS OF BRANCH {name} WHERE id = 1;"), &mut a);
    db.ok("UPDATE inventory SET qty = 7 WHERE id = 1;", &mut a);

    assert!(
        db.runtime.blind_writes(branch).unwrap().is_empty(),
        "a row the agent read before writing was still reported blind"
    );
}

/// Mixed: one row looked at, one not. The metric has to separate them rather than answering
/// all-or-nothing for the session.
#[test]
fn only_the_unread_row_is_reported_when_a_session_does_both() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'mixed-agent' RUN 'r_m';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;
    let name = a.agent.as_ref().unwrap().branch_name.clone();

    db.ok(&format!("SELECT qty FROM inventory AS OF BRANCH {name} WHERE id = 1;"), &mut a);
    db.ok("UPDATE inventory SET qty = 7 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = 9 WHERE id = 2;", &mut a);

    let blind = db.runtime.blind_writes(branch).unwrap();
    let rows: Vec<u64> = blind.iter().map(|(_, r)| r.0).collect();
    assert_eq!(rows, vec![2], "expected only the unread row 2 to be blind, got {blind:?}");
}

/// The metric reaches the merge report, which is where a gate tier can act on it.
#[test]
fn the_merge_report_carries_the_blind_write_set() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'blind-agent' RUN 'r_b';", &mut a);
    db.ok("UPDATE inventory SET qty = 7 WHERE id = 1;", &mut a);
    let out = db.ok("MERGE;", &mut a);
    let report = db.report(out);

    assert_eq!(
        report.blind_writes.iter().map(|(_, r)| r.0).collect::<Vec<_>>(),
        vec![1],
        "the merge report did not carry the blind write"
    );
    // A heuristic tier reports; it does not decide. The merge must still have applied.
    assert!(
        report.applied_to_target,
        "a blind write blocked the merge; the tier is a heuristic and quarantine does not exist \
         yet, so it must report without deciding"
    );
}

#[test]
fn a_session_that_read_and_wrote_nothing_blind_reports_an_empty_set() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'clean-agent' RUN 'r_k';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;
    let name = a.agent.as_ref().unwrap().branch_name.clone();

    db.ok(&format!("SELECT qty FROM inventory AS OF BRANCH {name} WHERE id = 1;"), &mut a);
    db.ok(&format!("SELECT qty FROM inventory AS OF BRANCH {name} WHERE id = 2;"), &mut a);
    db.ok("UPDATE inventory SET qty = 7 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = 8 WHERE id = 2;", &mut a);

    assert!(db.runtime.blind_writes(branch).unwrap().is_empty());
}
