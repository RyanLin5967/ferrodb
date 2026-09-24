//! **D212 option (a), exit test (8): a REVERT older than the retention window is refused, and the
//! refusal names the window.**
//!
//! In its own test binary, and with one test in it, because the window is configured by
//! `FERRODB_REVERT_RETENTION_MERGES` — process-wide state that a sibling test in the same binary
//! would read too.
//!
//! Pre-registered mutant: `revert_merge` skips its `n < oldest` refusal. Then `REVERT MERGE m_1`
//! reverts from memory before the restart, and after it the pruned history answers "no merge was
//! published", which names no window — RED at both assertions. Also RED at Step 0 (`b2269c9`), which
//! ignores the variable and reverts `m_1`.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::{rebuild_indexes, recover};
use ferrodb::wal::txn::TxnManager;

/// The window this test runs under. Small, so that the pruning it forces is a handful of merges.
const W: u64 = 2;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    dir: PathBuf,
}

impl Db {
    fn open(dir: &Path) -> Db {
        let path = dir.join("d212w.db");
        let existed = path.exists();
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.join("d212w.wal")).unwrap());
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
        let branches = LogBranchCatalog::open(&dir.join("d212w.branches"), 1).unwrap();
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

    fn revert_err(&mut self, id: &str) -> String {
        let mut s = Session::with_runtime(self.runtime.clone());
        match self.exec(&format!("REVERT MERGE {id};"), &mut s) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("REVERT MERGE {id} was accepted; it is outside a window of {W}"),
        }
    }

    fn revert_ok(&mut self, id: &str) {
        let mut s = Session::with_runtime(self.runtime.clone());
        match self.ok(&format!("REVERT MERGE {id};"), &mut s) {
            Outcome::Agent(AgentOutput::Revert(p)) => {
                assert!(!p.is_blocked(), "{id} was blocked: {p:?}")
            }
            _ => panic!("REVERT MERGE {id} did not return a plan"),
        }
    }
}

#[test]
fn exit_8_a_revert_older_than_the_window_is_refused_and_names_it() {
    // SAFETY: the only test in this binary, set before any runtime reads it.
    unsafe { std::env::set_var("FERRODB_REVERT_RETENTION_MERGES", W.to_string()) };

    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    let mut s = Session::with_runtime(db.runtime.clone());
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
    for id in 1..=5 {
        db.ok(&format!("INSERT INTO inventory VALUES ({id}, 10);"), &mut s);
    }
    drop(s);
    // Five independent merges, one row each, so no one of them depends on another.
    let mut ids = Vec::new();
    for id in 1..=5 {
        let mut a = Session::with_runtime(db.runtime.clone());
        db.ok(&format!("BEGIN AGENT SESSION AS 'w{id}' RUN 'r{id}';"), &mut a);
        db.ok(&format!("UPDATE inventory SET qty = qty + 1 WHERE id = {id};"), &mut a);
        match db.ok("MERGE;", &mut a) {
            Outcome::Agent(AgentOutput::Merge(m)) => {
                assert!(m.applied_to_target, "{m}");
                ids.push(m.merge_id);
            }
            _ => panic!("MERGE did not return a report"),
        }
    }
    assert_eq!(ids, ["m_1", "m_2", "m_3", "m_4", "m_5"], "the fixture assumes a fresh database");

    let msg = db.revert_err("m_1");
    assert!(msg.contains("retention window"), "refused, but not for the window: {msg}");
    assert!(msg.contains(&format!("last {W} published merges")), "the refusal does not name W: {msg}");
    // Anti-vacuity: the newest merge is inside the window and reverts.
    db.revert_ok("m_5");

    // The same answer from the durable history alone.
    let mut db = db.restart();
    let msg = db.revert_err("m_2");
    assert!(msg.contains("retention window"), "after a restart, refused but not for the window: {msg}");
    assert!(msg.contains(&format!("last {W} published merges")), "the refusal does not name W: {msg}");
    db.revert_ok("m_4");
}
