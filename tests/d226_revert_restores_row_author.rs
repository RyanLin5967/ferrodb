//! **D226 — a REVERT hands each row back to the author it had before the reverted merge.**
//!
//! `record_applied` stamps every row a merge publishes with the merge's run, and `who_wrote_row` /
//! `ferro_row_authors` answer from that stamp. REVERT's inverses carried no author and nothing
//! restored the stamp, so after a revert the database went on naming the reverted run as the author
//! of a value that run no longer wrote — criterion 9's question answered confidently and wrongly.
//!
//! Three shapes, because "the prior author" means three different things:
//! * a row another merge had already attributed goes back to THAT run;
//! * a row nobody was on record for (seeded by plain SQL) goes back to nobody;
//! * a row the reverted merge CREATED is gone, and so is its attribution.
//!
//! Every test compiles against the base `9aa6968` (`who_wrote_row` is public there) and is RED
//! there: after the revert `who_wrote_row` still names the reverted run.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
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
use ferrodb::tel::ids::RowId;
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
            .open(dir.path().join("d226.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d226.wal")).unwrap());
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
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        match self.exec(sql, s) {
            Ok(o) => o,
            Err(e) => panic!("{sql} failed: {e}"),
        }
    }

    /// One agent task that runs `sql` and merges, returning the merge id.
    fn merged_by(&mut self, agent: &str, sql: &str) -> String {
        let mut s = self.session();
        self.ok(&format!("BEGIN AGENT SESSION AS '{agent}' RUN 'r_{agent}';"), &mut s);
        self.ok(sql, &mut s);
        match self.ok("MERGE;", &mut s) {
            Outcome::Agent(AgentOutput::Merge(m)) => {
                assert!(m.applied_to_target, "{agent}'s merge did not land: {m}");
                m.merge_id
            }
            _ => panic!("MERGE did not return a merge report"),
        }
    }

    fn revert(&mut self, merge_id: &str) {
        let mut s = self.session();
        match self.ok(&format!("REVERT MERGE {merge_id};"), &mut s) {
            Outcome::Agent(AgentOutput::Revert(p)) => {
                assert!(!p.is_blocked(), "nothing read the rows, so the revert proceeds: {p:?}")
            }
            _ => panic!("REVERT did not return a revert plan"),
        }
    }

    fn author(&self, row: u64) -> Option<String> {
        self.runtime.who_wrote_row("inventory", RowId(row)).map(|r| r.agent_id)
    }

    fn qty_of(&mut self, id: i32) -> Option<i32> {
        let mut s = self.session();
        match self.ok("SELECT id, qty FROM inventory;", &mut s) {
            Outcome::Rows(rows) => rows.into_iter().find(|r| r[0] == Value::Integer(id)).map(|r| {
                match r[1] {
                    Value::Integer(q) => q,
                    ref other => panic!("qty of row {id} is {other:?}"),
                }
            }),
            _ => panic!("SELECT did not return rows"),
        }
    }
}

fn seeded() -> Db {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
    db.ok("INSERT INTO inventory VALUES (1, 10);", &mut s);
    db.ok("INSERT INTO inventory VALUES (2, 20);", &mut s);
    db
}

/// A row another run already published goes back to that run.
#[test]
fn a_reverted_update_hands_the_row_back_to_the_run_that_wrote_it_before() {
    let mut db = seeded();
    db.merged_by("pricer", "UPDATE inventory SET qty = 11 WHERE id = 1;");
    assert_eq!(db.author(1).as_deref(), Some("pricer"), "the fixture needs a prior author");

    let m = db.merged_by("clobberer", "UPDATE inventory SET qty = 12 WHERE id = 1;");
    assert_eq!(db.author(1).as_deref(), Some("clobberer"));

    db.revert(&m);
    assert_eq!(db.qty_of(1), Some(11), "the revert did not restore the value");
    assert_eq!(
        db.author(1).as_deref(),
        Some("pricer"),
        "after reverting the clobberer's merge the row still names the wrong run"
    );
}

/// A row nobody was on record for goes back to nobody — not to the reverted run.
#[test]
fn a_reverted_update_of_an_unattributed_row_leaves_nobody_on_record() {
    let mut db = seeded();
    assert_eq!(db.author(2), None, "plain SQL wrote row 2, so nobody is on record");

    let m = db.merged_by("bumper", "UPDATE inventory SET qty = 21 WHERE id = 2;");
    assert_eq!(db.author(2).as_deref(), Some("bumper"));

    db.revert(&m);
    assert_eq!(db.qty_of(2), Some(20));
    assert_eq!(db.author(2), None, "the reverted run is still named as row 2's author");
}

/// A row the reverted merge created is gone, and nobody is named as having written it.
#[test]
fn a_reverted_insert_leaves_no_author_for_the_row_it_removed() {
    let mut db = seeded();
    let m = db.merged_by("creator", "INSERT INTO inventory VALUES (3, 30);");
    assert_eq!(db.author(3).as_deref(), Some("creator"));

    db.revert(&m);
    assert_eq!(db.qty_of(3), None, "the revert did not remove the inserted row");
    assert_eq!(db.author(3), None, "a row that no longer exists is still attributed to its creator");
}
