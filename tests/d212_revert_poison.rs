//! **D212 option (a): a history write that fails inside its transaction poisons the REVERT
//! history, and nothing merges or reverts until the database is reopened.**
//!
//! In its own test binary, and with one test in it, because the failures are injected by
//! `FERRODB_FAIL_HISTORY_WRITE` — process-wide state that a sibling test would read too. Each named
//! site fails once, in the order the scenario reaches them: the merge-id reservation, the publish,
//! the REVERT's record.
//!
//! A failure there is the shape nothing can refuse up front: the transaction is open, and at the
//! publish and the REVERT the user's rows are already written in it. The transaction aborts, but
//! this runtime's counters were advanced for it, so continuing would key the next records and
//! version upserts off a picture of the file that is no longer true.
//!
//! Pre-registered mutants (in `src/agent_sql/runtime.rs`), each RED here:
//! - `poison_history` does nothing: the MERGE after the reservation failure is accepted.
//! - `next_merge_id` skips `history_usable()`: the same MERGE reserves again and is accepted.
//! - `reserve_merge_ids`, or the publish path after `write_history`, does not poison on the write
//!   failure: the MERGE after it is accepted.
//! - `revert_merge` does not poison when `write_revert_record` fails: the second REVERT is accepted.
//! - `revert_merge` skips `history_usable()`: the first REVERT answers about `m_1` (an id never
//!   minted), not about the history.
//!
//! Not covered: the three sites that poison on a `commit` error, and the one on a `record_applied`
//! error after the commit. No injection reaches inside `TxnManager::commit` or the provenance store.
//!
//! RED at `0185f06` (no injection point) and at `8a0b934` (whose injection took a count, not site
//! names): the first MERGE lands.

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

    fn revert(&mut self, merge_id: &str) -> Result<Outcome, FerroError> {
        let mut s = Session::with_runtime(self.runtime.clone());
        self.exec(&format!("REVERT MERGE {merge_id};"), &mut s)
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

/// `Err` whose text names `what`, or a panic saying which statement was accepted instead.
fn refused(result: Result<Outcome, FerroError>, what: &str, statement: &str) {
    match result {
        Err(e) => assert!(e.to_string().contains(what), "{statement} refused, but not for {what}: {e}"),
        Ok(Outcome::Agent(AgentOutput::Merge(m))) => panic!("{statement} was accepted: {m}"),
        Ok(Outcome::Agent(AgentOutput::Revert(p))) => panic!("{statement} was accepted: {p:?}"),
        Ok(_) => panic!("{statement} returned something other than a report or a plan"),
    }
}

#[test]
fn a_failed_history_write_refuses_merge_and_revert_until_the_database_is_reopened() {
    // SAFETY: the only test in this binary, set before any runtime reads it.
    unsafe { std::env::set_var("FERRODB_FAIL_HISTORY_WRITE", "reserve,publish,revert") };

    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    let mut s = Session::with_runtime(db.runtime.clone());
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
    for id in 1..=2 {
        db.ok(&format!("INSERT INTO inventory VALUES ({id}, 10);"), &mut s);
    }
    drop(s);

    // ---- 1. the merge-id reservation's write fails ------------------------------------------
    let r = db.bump_and_merge("first", 1);
    refused(r, "FERRODB_FAIL_HISTORY_WRITE at reserve", "the MERGE whose reservation failed");
    assert_eq!(db.qty_of(1), 10, "a MERGE whose id was never minted published its row");
    // Poisoned: the injection is spent, so a refusal from here on is the poison's.
    let r = db.bump_and_merge("second", 2);
    refused(r, POISONED, "a MERGE after the reservation failed");
    assert_eq!(db.qty_of(2), 10, "the refused MERGE published its row");
    let r = db.revert("m_1");
    refused(r, POISONED, "a REVERT after the reservation failed");

    // ---- 2. the publish's history write fails, after the rows are written in its txn --------
    let mut db = db.restart();
    let r = db.bump_and_merge("third", 1);
    refused(r, "FERRODB_FAIL_HISTORY_WRITE at publish", "the MERGE whose history write failed");
    assert_eq!(db.qty_of(1), 10, "the aborted publish left its row write behind");
    let r = db.bump_and_merge("fourth", 2);
    refused(r, POISONED, "a MERGE after the history write failed");
    assert_eq!(db.qty_of(2), 10, "the refused MERGE published its row");

    // ---- 3. a REVERT's record fails, after its inverses are written in its txn ---------------
    let mut db = db.restart();
    let merge_id = match db.bump_and_merge("fifth", 1) {
        Ok(Outcome::Agent(AgentOutput::Merge(m))) => {
            assert!(m.applied_to_target, "the merge after the reopen did not land: {m}");
            m.merge_id
        }
        Ok(_) => panic!("MERGE after the reopen returned something other than a report"),
        Err(e) => panic!("MERGE after the reopen failed: {e}"),
    };
    assert_eq!(db.qty_of(1), 11);
    let r = db.revert(&merge_id);
    refused(r, "FERRODB_FAIL_HISTORY_WRITE at revert", "the REVERT whose record failed");
    assert_eq!(db.qty_of(1), 11, "the aborted REVERT left its inverse behind");
    let r = db.revert(&merge_id);
    refused(r, POISONED, "a REVERT after its record failed");
    assert_eq!(db.qty_of(1), 11, "the refused REVERT inverted the row");

    // Anti-vacuity: after a reopen the same REVERT goes through, so every refusal above was the
    // poison's and not a database that cannot merge or revert.
    let mut db = db.restart();
    match db.revert(&merge_id) {
        Ok(Outcome::Agent(AgentOutput::Revert(p))) => {
            assert!(!p.is_blocked(), "{merge_id} was blocked: {p:?}")
        }
        Ok(_) => panic!("REVERT MERGE {merge_id} did not return a plan"),
        Err(e) => panic!("REVERT MERGE {merge_id} after the reopen failed: {e}"),
    }
    assert_eq!(db.qty_of(1), 10, "the REVERT after the reopen did not undo its merge");
}
