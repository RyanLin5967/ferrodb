//! **D212 option (a) — REVERT's substrate survives a restart.** Exit tests (1)–(5) of
//! `frontier/d212_design.md` §6, plus (3b) for the capture of an ancestor published through its
//! child. (6) and (7) kill a real process and live in `d212_revert_crash.rs`; (8) sets an
//! environment variable and lives alone in `d212_revert_retention.rs`.
//!
//! # The contract each test holds, and the structure whose removal must turn it red
//!
//! | test | structure | pre-registered mutant (in `src/agent_sql/runtime.rs` / `revert_store.rs`) |
//! |---|---|---|
//! | (1) | `versions` | `attach_history` skips the `state.versions` fill from `read_versions` |
//! | (2) | `apply_seq` | `attach_history` skips `state.apply_seq = max(..)` |
//! | (3) | captures | `persist_publish` writes no `CAPTURE` records |
//! | (3b) | inherited captures | `persist_publish` writes the merging task's capture only |
//! | (4) | `next_txn` | `attach_history` skips `state.next_txn = max(..)` |
//! | (5) | `next_merge`, `merges`, `applied`, revert markers | skip `state.next_merge = max(..)`; or no `MERGES_TABLE` row; or no `APPLIED` record; or `write_revert` writes no `REVERTED` record |
//!
//! Every restart here is real: the files are checkpointed, every in-process object — sessions
//! first — is dropped, and a new `Catalog`, WAL, buffer pool, branch catalog and `AgentRuntime` are
//! built from the files alone, in `cli.rs`'s order.
//!
//! All of these compile against Step 0 (`b2269c9`), whose REVERT refuses every pre-restart merge as
//! an earlier run's, so each is RED there at its REVERT.

use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::MergeReport;
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
use ferrodb::provenance::revert::RevertPlan;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::ids::TxnId;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::{rebuild_indexes, recover};
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    dir: PathBuf,
}

impl Db {
    fn open(dir: &Path) -> Db {
        let path = dir.join("d212a.db");
        let existed = path.exists();
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let wal = Arc::new(WalManager::new(dir.join("d212a.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        let recovered = recover(&txn).unwrap();
        let mut catalog = if existed {
            Catalog::open(bp.clone(), FIRST_CATALOG_PAGE_ID).unwrap()
        } else {
            Catalog::create(bp.clone()).unwrap()
        };
        if recovered {
            rebuild_indexes(&mut catalog, &bp).unwrap();
            txn.checkpoint().unwrap();
        }
        let branches = LogBranchCatalog::open(&dir.join("d212a.branches"), 1).unwrap();
        let runtime =
            Arc::new(AgentRuntime::with_catalog(Arc::new(branches) as Arc<dyn BranchCatalog>));
        Db { catalog, bp, txn, runtime, dir: dir.to_path_buf() }
    }

    /// Checkpoint, drop everything this process built, reopen from the files.
    fn restart(self) -> Db {
        self.txn.checkpoint().unwrap();
        let dir = self.dir.clone();
        drop(self);
        Db::open(&dir)
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

    fn seed(&mut self, rows: &[(i32, i32)]) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        for (id, qty) in rows {
            self.ok(&format!("INSERT INTO inventory VALUES ({id}, {qty});"), &mut s);
        }
    }

    fn agent(&mut self, name: &str) -> (Session, TxnId) {
        let mut s = self.session();
        let txn = match self.ok(&format!("BEGIN AGENT SESSION AS '{name}' RUN 'r_{name}';"), &mut s)
        {
            Outcome::Agent(AgentOutput::SessionStarted(a)) => a.txn,
            _ => panic!("BEGIN AGENT SESSION did not report a session"),
        };
        (s, txn)
    }

    fn merge(&mut self, s: &mut Session) -> MergeReport {
        match self.ok("MERGE;", s) {
            Outcome::Agent(AgentOutput::Merge(m)) => {
                assert!(m.applied_to_target, "the merge did not land: {m}");
                m
            }
            _ => panic!("MERGE did not return a merge report"),
        }
    }

    /// One agent task that runs `sqls` and merges; returns the merge id and the task's txn.
    fn task(&mut self, name: &str, sqls: &[&str]) -> (String, TxnId) {
        let (mut s, txn) = self.agent(name);
        for sql in sqls {
            self.ok(sql, &mut s);
        }
        (self.merge(&mut s).merge_id, txn)
    }

    fn revert(&mut self, sql: &str) -> RevertPlan {
        let mut s = self.session();
        match self.ok(sql, &mut s) {
            Outcome::Agent(AgentOutput::Revert(p)) => p,
            _ => panic!("{sql} did not return a revert plan"),
        }
    }

    fn qty_of(&mut self, id: i32) -> i32 {
        let mut s = self.session();
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

/// **(1) `versions`.** A post-restart point read of a row a pre-restart merge wrote must be stamped
/// with THAT merge's version, or the exact edge from it is lost.
///
/// The only edge from A to B is the point read: B writes a different row, by key, so its
/// write-targeting region (`id = 2`) cannot cover anything A wrote.
#[test]
fn exit_1_a_point_read_after_a_restart_names_the_pre_restart_merge_it_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 10), (2, 20)]);
    let (ma, _) = db.task("a", &["UPDATE inventory SET qty = 11 WHERE id = 1;"]);

    let mut db = db.restart();
    let (mb, b_txn) = db.task(
        "b",
        &["SELECT qty FROM inventory WHERE id = 1;", "UPDATE inventory SET qty = 22 WHERE id = 2;"],
    );
    assert_ne!(ma, mb);

    let halted = db.revert(&format!("REVERT MERGE {ma};"));
    assert_eq!(
        halted.blocked_by,
        vec![b_txn],
        "B read the row A published, after a restart, and the revert of A does not name it"
    );
    assert_eq!((db.qty_of(1), db.qty_of(2)), (11, 22), "a halted revert changed a row");
}

/// **(2) `apply_seq`.** A post-restart SCAN over a range that holds a value a pre-restart merge
/// wrote must come out AFTER that write on the version clock, or the temporal rule
/// (`begin_ts < observed_at`) drops the edge.
///
/// B's read is a scan, so it retains a region and no exact versions: the only edge is predicate-
/// derived, and it rests on the clock.
#[test]
fn exit_2_a_scan_after_a_restart_is_later_than_the_pre_restart_write_it_covers() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 10), (2, 20)]);
    let (ma, _) = db.task("a", &["UPDATE inventory SET qty = 150 WHERE id = 1;"]);

    let mut db = db.restart();
    let (_mb, b_txn) = db.task(
        "b",
        &[
            "SELECT id, qty FROM inventory WHERE qty >= 100 AND qty < 200;",
            "UPDATE inventory SET qty = 22 WHERE id = 2;",
        ],
    );

    let halted = db.revert(&format!("REVERT MERGE {ma};"));
    assert_eq!(
        halted.blocked_by,
        vec![b_txn],
        "B scanned a range holding A's value after a restart, and the revert of A does not name it"
    );
    assert_eq!(db.qty_of(1), 150, "a halted revert changed a row");
}

/// **(3) captures.** A dependent that read and published BEFORE the restart is still named after it.
#[test]
fn exit_3_a_dependent_published_before_the_restart_still_blocks_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 10), (2, 20)]);
    let (ma, _) = db.task("a", &["UPDATE inventory SET qty = 11 WHERE id = 1;"]);
    let (_mb, b_txn) = db.task(
        "b",
        &["SELECT qty FROM inventory WHERE id = 1;", "UPDATE inventory SET qty = 22 WHERE id = 2;"],
    );
    // Anti-vacuity: the edge exists before the restart.
    assert_eq!(db.revert(&format!("REVERT MERGE {ma};")).blocked_by, vec![b_txn]);

    let mut db = db.restart();
    let halted = db.revert(&format!("REVERT MERGE {ma};"));
    assert_eq!(halted.blocked_by, vec![b_txn], "the dependent's capture did not survive the restart");
}

/// **(3b) inherited captures.** A parent reads A's row and stages a row; its CHILD merges and so
/// publishes the parent's staged row. The parent's read is the premise of a published row, so after
/// a restart — when the parent's workspace is long gone — the revert of A must still name it.
#[test]
fn exit_3b_an_ancestor_published_through_its_child_still_blocks_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 10), (2, 20)]);
    let (ma, _) = db.task("a", &["UPDATE inventory SET qty = 11 WHERE id = 1;"]);

    let (mut parent, p_txn) = db.agent("parent");
    db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut parent);
    db.ok("INSERT INTO inventory VALUES (9, 90);", &mut parent);
    let pbranch = parent.agent.as_ref().unwrap().branch;
    let cs = db.runtime.begin_session("child", Some("r_child"), pbranch).unwrap();
    let mut child = db.session();
    child.agent = Some(cs);
    db.merge(&mut child);
    assert_eq!(db.qty_of(9), 90, "the child did not publish the parent's staged row");
    assert_eq!(
        db.revert(&format!("REVERT MERGE {ma};")).blocked_by,
        vec![p_txn],
        "anti-vacuity: the parent blocks the revert before the restart"
    );

    drop((parent, child));
    let mut db = db.restart();
    let halted = db.revert(&format!("REVERT MERGE {ma};"));
    assert_eq!(
        halted.blocked_by,
        vec![p_txn],
        "the parent's read is the premise of published row 9 and was lost at the restart"
    );
}

/// **(4) `next_txn`.** The first task after a restart must not take a pre-restart task's txn id.
/// If it did, the pre-restart merge's capture and the new task's would share a key, the graph would
/// read the new task's read as the merge reading its own write, and the edge would vanish.
#[test]
fn exit_4_a_task_after_a_restart_never_takes_a_pre_restart_txn_id() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 10), (2, 20)]);
    let (ma, a_txn) = db.task("a", &["UPDATE inventory SET qty = 11 WHERE id = 1;"]);

    let mut db = db.restart();
    // The FIRST session after the restart, which is the one a reset counter would give `a_txn`.
    let (_mc, c_txn) = db.task(
        "c",
        &["SELECT qty FROM inventory WHERE id = 1;", "UPDATE inventory SET qty = 22 WHERE id = 2;"],
    );
    assert!(c_txn > a_txn, "the first post-restart task took txn {c_txn}, not above {a_txn}");

    let halted = db.revert(&format!("REVERT MERGE {ma};"));
    assert_eq!(halted.blocked_by, vec![c_txn], "the post-restart dependent is not named");
}

/// **(5) `next_merge`, `merges`, `applied` and the revert marker.** Merge, restart, merge: the
/// pre-restart id reverts the PRE-restart merge — and, being an `Add`, only once, however many
/// restarts later it is asked again.
#[test]
fn exit_5_a_pre_restart_id_reverts_the_pre_restart_merge_once_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(dir.path());
    db.seed(&[(1, 10), (2, 20)]);
    let (ma, a_txn) = db.task("a", &["UPDATE inventory SET qty = qty + 1 WHERE id = 1;"]);
    assert_eq!(ma, "m_1", "the fixture assumes a fresh database's first id");

    let mut db = db.restart();
    let (mb, _) = db.task("b", &["UPDATE inventory SET qty = 22 WHERE id = 2;"]);
    assert_ne!(mb, "m_1", "the post-restart merge re-used a pre-restart id");

    let plan = db.revert("REVERT MERGE m_1;");
    assert_eq!(plan.target, a_txn, "REVERT MERGE m_1 did not plan the pre-restart merge");
    assert!(!plan.is_blocked(), "{plan:?}");
    assert_eq!((db.qty_of(1), db.qty_of(2)), (10, 22), "m_1's Add was not inverted, or m_2 moved");

    let mut s = db.session();
    let again = db.exec("REVERT MERGE m_1;", &mut s).err().map(|e| e.to_string());
    assert!(
        again.as_deref().is_some_and(|m| m.contains("already reverted")),
        "a second REVERT in the same run was not refused: {again:?}"
    );
    drop(s);

    let mut db = db.restart();
    let mut s = db.session();
    let after = db.exec("REVERT MERGE m_1;", &mut s).err().map(|e| e.to_string());
    assert!(
        after.as_deref().is_some_and(|m| m.contains("already reverted")),
        "after a restart the revert of m_1 was accepted again, so its Add moved twice: {after:?}"
    );
    drop(s);
    assert_eq!(db.qty_of(1), 10, "the Add was inverted more than once");
}
