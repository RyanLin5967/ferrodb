//! TEMPORARY PROBE (not committed): D194 questions answered by running rather than reading.
use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
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

struct Db { catalog: Catalog, bp: Arc<BufferPoolManager>, txn: Arc<TxnManager>, runtime: Arc<AgentRuntime>, _dir: tempfile::TempDir }
impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(dir.path().join("p.db")).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("p.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }
    fn session(&self) -> Session { Session::with_runtime(self.runtime.clone()) }
    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty());
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }
    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome { self.exec(sql, s).unwrap_or_else(|e| panic!("{sql}: {e}")) }
    fn rows(&mut self, sql: &str, s: &mut Session) -> Vec<Vec<Value>> { match self.ok(sql, s) { Outcome::Rows(r) => r, _ => panic!("rows") } }
}

/// Probe 1: the nested-task variant where the CHILD point-reads row 1 itself. Prints why the merge
/// did not publish.
#[test]
fn probe_nested_child_point_read_reason() {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
    db.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);
    let mut parent = db.session();
    db.ok("BEGIN AGENT SESSION AS 'planner' RUN 'r1';", &mut parent);
    let mut other = db.session();
    db.ok("BEGIN AGENT SESSION AS 'other' RUN 'r2';", &mut other);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut other);
    db.ok("MERGE;", &mut other);
    let pb = parent.agent.as_ref().unwrap().branch;
    let child = db.runtime.begin_session("sub", Some("r3"), pb).unwrap();
    let cb = child.branch;
    let mut c = db.session();
    c.agent = Some(child);
    println!("PROBE1 child point read: {:?}", db.rows("SELECT qty FROM inventory WHERE id = 1;", &mut c));
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 1;", &mut c);
    let out = db.ok("MERGE;", &mut c);
    println!("PROBE1 merge: {:?}", matches!(out, Outcome::Agent(_)));
    println!("PROBE1 quarantine reason: {:?}", db.runtime.quarantine_reason(cb));
    let mut m = db.session();
    println!("PROBE1 main row 1: {:?}", db.rows("SELECT qty FROM inventory WHERE id = 1;", &mut m));
}

/// Probe 2: E63 key reuse. Main DELETEs row 3 and INSERTs it again after the fork. The branch's
/// full scan and its point lookup of row 3 must agree (both: the fork-time row (3, 7)).
#[test]
fn probe_key_reuse_after_fork_scan_vs_point() {
    let mut db = Db::new();
    let mut main = db.session();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut main);
    db.ok("INSERT INTO inventory VALUES (1, 20);", &mut main);
    db.ok("INSERT INTO inventory VALUES (3, 7);", &mut main);
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r1';", &mut a);
    db.ok("DELETE FROM inventory WHERE id = 3;", &mut main);
    db.ok("INSERT INTO inventory VALUES (3, 8);", &mut main);
    println!("PROBE2 main scan: {:?}", db.rows("SELECT id, qty FROM inventory;", &mut main));
    println!("PROBE2 branch scan: {:?}", db.rows("SELECT id, qty FROM inventory;", &mut a));
    println!("PROBE2 branch point id=3: {:?}", db.rows("SELECT id, qty FROM inventory WHERE id = 3;", &mut a));
    println!("PROBE2 branch update id=3 -> rows affected: {:?}", match db.ok("UPDATE inventory SET qty = 70 WHERE id = 3;", &mut a) { Outcome::Affected(n) => n as i64, _ => -1 });
    println!("PROBE2 branch insert id=3: {:?}", db.exec("INSERT INTO inventory VALUES (3, 9);", &mut a).map(|_| "admitted").map_err(|e| e.to_string()));
}

/// Probe 3: the same key-reuse sequence under a plain explicit transaction, with no agent at all —
/// is the anomaly pre-existing for any old snapshot?
#[test]
fn probe_key_reuse_under_explicit_transaction() {
    let mut db = Db::new();
    let mut main = db.session();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut main);
    db.ok("INSERT INTO inventory VALUES (1, 20);", &mut main);
    db.ok("INSERT INTO inventory VALUES (3, 7);", &mut main);
    let mut t = db.session();
    db.ok("BEGIN;", &mut t);
    println!("PROBE3 txn first scan: {:?}", db.rows("SELECT id, qty FROM inventory;", &mut t));
    db.ok("DELETE FROM inventory WHERE id = 3;", &mut main);
    db.ok("INSERT INTO inventory VALUES (3, 8);", &mut main);
    println!("PROBE3 txn scan: {:?}", db.rows("SELECT id, qty FROM inventory;", &mut t));
    println!("PROBE3 txn point id=3: {:?}", db.rows("SELECT id, qty FROM inventory WHERE id = 3;", &mut t));
    db.ok("COMMIT;", &mut t);
}
