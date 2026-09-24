//! **D212 option (a): a history write that fails inside the publish poisons the REVERT history,
//! and nothing merges or reverts until the database is reopened.**
//!
//! In its own test binary, and with one test in it, because the failure is injected by
//! `FERRODB_FAIL_HISTORY_WRITE` — process-wide state that a sibling test would read too.
//!
//! The shape is the one `plan_history` cannot refuse up front: the user's rows are already written
//! in the publish transaction when the history write fails, as a full disk would make it. The
//! transaction aborts, but this runtime's counters were advanced for it, so continuing would key
//! the next records and version upserts off a picture of the file that is no longer true.
//!
//! Pre-registered mutants (in `src/agent_sql/runtime.rs`), each RED here:
//! - `poison_history` does nothing: the second MERGE is accepted.
//! - `publish_evaluation_as` skips `history_usable()`: the second MERGE is accepted.
//! - `revert_merge` skips `history_usable()`: the REVERT answers about `m_1` (an unpublished id),
//!   not about the history.
//! Also RED at `0185f06` (option (a) before this fix), which has no injection point: the first
//! MERGE lands.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::catalog::LogBranchCatalog;
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
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::{rebuild_indexes, recover};
use ferrodb::wal::txn::TxnManager;

/// What `history_usable` says while the history is poisoned.
const POISONED: &str = "could not be written consistently";

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    dir: PathBuf,
}

impl Db {
    fn open(dir: &Path) -> Db {
        let path = dir.join("d212p.db");
        let existed = path.exists();
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.join("d212p.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        let recovered = recover(&txn).unwrap();
        let mut catalog = if existed {
            Catalog::open(bp.clone(), 1).unwrap()
        } else {
            Catalog::create(bp.clone()).unwrap()
        };
        if recovered {
            rebuild_indexes(&mut catalog, &bp).unwrap();
            txn.checkpoint().unwrap();
        }
        let branches = LogBranchCatalog::open(&dir.join("d212p.branches"), 1).unwrap();
        let runtime =
            Arc::new(AgentRuntime::with_catalog(Arc::new(branches) as Arc<dyn BranchCatalog>));
        Db { catalog, bp, txn, runtime, dir: dir.to_path_buf() }
    }

    fn restart(self) -> Db {
        self.txn.checkpoint().unwrap();
        let dir = self.dir.clone();
        drop(self);
        Db::open(&dir)
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        assert!(parser.errors.is_empty(), "parse errors in {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    /// One agent task that adds one to row `id`, then `MERGE`'s result.
    fn bump_and_merge(&mut self, name: &str, id: i32) -> Result<Outcome, FerroError> {
        let mut a = Session::with_runtime(self.runtime.clone());
        self.ok(&format!("BEGIN AGENT SESSION AS '{name}' RUN 'r_{name}';"), &mut a);
        self.ok(&format!("UPDATE inventory SET qty = qty + 1 WHERE id = {id};"), &mut a);
        self.exec("MERGE;", &mut a)
    }

    fn qty_of(&mut self, id: i32) -> i32 {
        let mut s = Session::with_runtime(self.runtime.clone());
        let rows = match self.ok("SELECT id, qty FROM inventory;", &mut s) {
            Outcome::Rows(r) => r,
            _ => panic!("SELECT did not return rows"),
        };
        match rows.into_iter().find(|r| r[0] == Value::Integer(id)).map(|r| r[1].clone()) {
            Some(Value::Integer(q)) => q,
            other => panic!("row {id}: {other:?}"),
        }
    }
}

#[test]
fn a_failed_history_write_refuses_merge_and_revert_until_the_database_is_reopened() {
    // SAFETY: the only test in this binary, set before any runtime reads it. One injected failure.
    unsafe { std::env::set_var("FERRODB_FAIL_HISTORY_WRITE", "1") };

    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    let mut s = Session::with_runtime(db.runtime.clone());
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
    for id in 1..=2 {
        db.ok(&format!("INSERT INTO inventory VALUES ({id}, 10);"), &mut s);
    }
    drop(s);

    // The failure itself: the MERGE fails, and the rows it had written are rolled back with it.
    match db.bump_and_merge("first", 1) {
        Err(e) => assert!(
            e.to_string().contains("FERRODB_FAIL_HISTORY_WRITE"),
            "failed, but not at the injection: {e}"
        ),
        Ok(Outcome::Agent(AgentOutput::Merge(m))) => {
            panic!("the MERGE whose history write failed was reported, not refused: {m}")
        }
        Ok(_) => panic!("MERGE returned something other than a report"),
    }
    assert_eq!(db.qty_of(1), 10, "the aborted publish left its row write behind");

    // Poisoned: the next MERGE is refused before it publishes — the injection is spent, so a
    // refusal here is the poison's, not a second injected failure.
    match db.bump_and_merge("second", 2) {
        Err(e) => assert!(e.to_string().contains(POISONED), "refused, but not for the poison: {e}"),
        Ok(Outcome::Agent(AgentOutput::Merge(m))) => {
            panic!("a MERGE was accepted after the history write failed: {m}")
        }
        Ok(_) => panic!("MERGE returned something other than a report"),
    }
    assert_eq!(db.qty_of(2), 10, "the refused MERGE published its row");

    // ...and so is every REVERT, whatever it names.
    let mut s = Session::with_runtime(db.runtime.clone());
    match db.exec("REVERT MERGE m_1;", &mut s) {
        Err(e) => {
            assert!(e.to_string().contains(POISONED), "REVERT refused, but not for the poison: {e}")
        }
        Ok(Outcome::Agent(AgentOutput::Revert(p))) => {
            panic!("a REVERT was accepted after the history write failed: {p:?}")
        }
        Ok(_) => panic!("REVERT returned something other than a plan"),
    }
    drop(s);

    // Reopening reads the truth from disk, and both work again. Anti-vacuity: the refusals above
    // are the poison's, not a database that cannot merge.
    let mut db = db.restart();
    let merge_id = match db.bump_and_merge("third", 2) {
        Ok(Outcome::Agent(AgentOutput::Merge(m))) => {
            assert!(m.applied_to_target, "the merge after the reopen did not land: {m}");
            m.merge_id
        }
        Ok(_) => panic!("MERGE after the reopen returned something other than a report"),
        Err(e) => panic!("MERGE after the reopen failed: {e}"),
    };
    assert_eq!(db.qty_of(2), 11);
    let mut s = Session::with_runtime(db.runtime.clone());
    match db.ok(&format!("REVERT MERGE {merge_id};"), &mut s) {
        Outcome::Agent(AgentOutput::Revert(p)) => {
            assert!(!p.is_blocked(), "{merge_id} was blocked: {p:?}")
        }
        _ => panic!("REVERT MERGE {merge_id} did not return a plan"),
    }
    drop(s);
    assert_eq!(db.qty_of(2), 10, "the REVERT after the reopen did not undo its merge");
}
