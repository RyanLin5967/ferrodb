//! **D212 (a') AMENDED 3 — REVERT's history reached through the one open path.**
//!
//! Every test here opens its database the way the CLI and pgserver do: `DbLock`, then
//! `wal::recovery::open_recovered`, then a runtime through `OpenedDatabase::attach_runtime` (D250's
//! door). SCALE-DESIGN "D212 (a') AMENDED 3" says where the history store lives on that path; each
//! test names its item and the mutant that must turn it red.
//!
//! | test | item | pre-registered mutant |
//! |---|---|---|
//! | `a_merge_through_the_open_path_is_revertible_after_a_crash` | 4: the store is opened inside `open_recovered`, before `recover` | `open_recovered` registers no store; or registers it after `recover` |
//! | `a_runtime_attached_to_one_database_refuses_another_databases_history` | 4: the door hands the runtime its database's store | `attach_history` skips the same-store check |
//! | `a_failed_open_checkpoint_leaves_the_queue_and_the_next_publish_takes_fresh_ids` | 1: the runtime reads history only through store ∪ queue | `HistoryStore::records` returns the durable window only |
//! | `a_fresh_log_moves_the_old_history_aside` | 10: a fresh log is a new database, and the history goes aside | `WalManager::with_storage` skips the history move |
//!
//! A "crash" here drops every handle with no checkpoint, so the history queued in memory is lost
//! and the log is its only copy.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::db_lock::DbLock;
use ferrodb::wal::recovery::{open_recovered, OpenedDatabase};

/// One database, opened through the production path. Dropped field by field in this order, so the
/// lock goes last.
struct Db {
    runtime: Arc<AgentRuntime>,
    o: OpenedDatabase,
    path: PathBuf,
    _lock: DbLock,
}

impl Db {
    fn open(path: &Path) -> Db {
        Db::try_open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()))
    }

    fn try_open(path: &Path) -> Result<Db, FerroError> {
        Db::try_open_locked(path, DbLock::acquire(path)?)
    }

    /// The open, with the lock already taken, so a test can change the directory between the two.
    fn try_open_locked(path: &Path, lock: DbLock) -> Result<Db, FerroError> {
        let o = open_recovered(path, &lock)?;
        let mut branches = path.as_os_str().to_os_string();
        branches.push(".branches");
        let branches = LogBranchCatalog::open(Path::new(&branches), 1)?;
        let runtime =
            o.attach_runtime(AgentRuntime::with_catalog(Arc::new(branches) as Arc<dyn BranchCatalog>));
        Ok(Db { runtime, o, path: path.to_path_buf(), _lock: lock })
    }

    /// Drop every handle with no checkpoint, then open again.
    fn crash_and_reopen(self) -> Db {
        let path = self.path.clone();
        drop(self);
        Db::open(&path)
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
        run(stmts.remove(0), &mut self.o.catalog, self.o.bp.clone(), self.o.txn.clone(), s)
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

    /// One agent task that runs `sql` and merges; returns the merge id.
    fn task(&mut self, name: &str, sql: &str) -> String {
        let mut s = self.session();
        self.ok(&format!("BEGIN AGENT SESSION AS '{name}' RUN 'r_{name}';"), &mut s);
        self.ok(sql, &mut s);
        match self.ok("MERGE;", &mut s) {
            Outcome::Agent(AgentOutput::Merge(m)) => {
                assert!(m.applied_to_target, "the merge did not land: {m}");
                m.merge_id
            }
            _ => panic!("MERGE did not return a merge report"),
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

/// **Item 4.** A merge made through the production open path is revertible after a crash: the store
/// is opened inside `open_recovered` and registered BEFORE `recover`, so the open's catch-up puts the
/// crashed process's history into it, and the runtime reaches it through the door.
///
/// RED before item 4's code: `open_recovered` opened no store, so the merge carried no history and
/// the REVERT is refused as an earlier run's merge with none.
#[test]
fn a_merge_through_the_open_path_is_revertible_after_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::open(&dir.path().join("reach4a.db"));
    db.seed(&[(1, 10), (2, 20)]);
    let id = db.task("a", "UPDATE inventory SET qty = 11 WHERE id = 1;");
    assert_eq!(db.qty_of(1), 11, "premise: the merge published");

    let mut db = db.crash_and_reopen();
    let mut s = db.session();
    match db.exec(&format!("REVERT MERGE {id};"), &mut s) {
        Ok(Outcome::Agent(AgentOutput::Revert(plan))) => {
            assert!(!plan.is_blocked(), "nothing depends on the merge, and the revert halted: {plan:?}")
        }
        Ok(_) => panic!("REVERT did not return a revert plan"),
        Err(e) => panic!("a merge made before a crash, through the open path, could not be reverted: {e}"),
    }
    assert_eq!(db.qty_of(1), 10, "the revert did not restore the row");
}

/// **Item 4.** The door hands a runtime ITS database's history store, and a runtime so attached is
/// refused against a database whose log carries another store, before anything is written: its
/// counters and window describe the first database, and the second's history would be written
/// under them.
///
/// RED before item 4's code: the door handed nothing, the runtime read whatever store the statement's
/// log had, and the agent session began.
#[test]
fn a_runtime_attached_to_one_database_refuses_another_databases_history() {
    let dir = tempfile::tempdir().unwrap();
    let a = Db::open(&dir.path().join("reach4b_a.db"));
    let mut b = Db::open(&dir.path().join("reach4b_b.db"));
    b.seed(&[(1, 10)]);
    let mut s = Session::with_runtime(a.runtime.clone());
    let err = match b.exec("BEGIN AGENT SESSION AS 'x' RUN 'r_x';", &mut s) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("a runtime attached to database A began an agent session over database B's log"),
    };
    assert!(err.contains("another database"), "refused, but not for the history: {err}");
    assert_eq!(b.qty_of(1), 10);
}

/// **Item 1 (the round-3 adversary's A4).** The open's checkpoint fails its first history write,
/// so the store's queue still holds the crashed process's merge when the runtime attaches. The
/// runtime reads history only through the store's union of its durable window and its queue, so
/// the next publish takes the next `hseq` and version stamps after BOTH, and a reopen can REVERT
/// both merges. A runtime that read the durable window alone would reuse the queued merge's `hseq`,
/// and the reader's dedup by `hseq` would drop one of the two.
///
/// The failure is real: the database's directory is made read-only for the open, so the first
/// write (a full replace, which creates a temporary beside the file) cannot create its file.
///
/// RED before item 1's code: a failed hook write failed `open_recovered` itself.
#[cfg(unix)]
#[test]
fn a_failed_open_checkpoint_leaves_the_queue_and_the_next_publish_takes_fresh_ids() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reach1.db");
    let mut db = Db::open(&path);
    db.seed(&[(1, 10), (2, 20)]);
    let first = db.task("a", "UPDATE inventory SET qty = 11 WHERE id = 1;");
    let history = {
        let mut p = path.as_os_str().to_os_string();
        p.push(".history");
        PathBuf::from(p)
    };
    assert!(!history.exists(), "premise: nothing drained the first merge into the store before the crash");
    drop(db);

    let lock = DbLock::acquire(&path).unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
    let opened = Db::try_open_locked(&path, lock);
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut db = opened.unwrap_or_else(|e| panic!("an open whose history write failed did not open: {e}"));
    assert!(
        !history.exists(),
        "premise: the open's checkpoint wrote the store, so its first replace did not fail (running as root?)"
    );

    let second = db.task("b", "UPDATE inventory SET qty = 22 WHERE id = 2;");
    let mut db = db.crash_and_reopen();
    for id in [&second, &first] {
        let mut s = db.session();
        match db.exec(&format!("REVERT MERGE {id};"), &mut s) {
            Ok(Outcome::Agent(AgentOutput::Revert(plan))) => {
                assert!(!plan.is_blocked(), "{id}: the two merges are independent, and the revert halted: {plan:?}")
            }
            Ok(_) => panic!("REVERT did not return a revert plan"),
            Err(e) => panic!("REVERT {id} failed: one of two merges lost its history: {e}"),
        }
    }
    assert_eq!((db.qty_of(1), db.qty_of(2)), (10, 20), "a revert did not restore its row");
}

/// **Item 10.** A fresh log is a new database at this path (#16's rule for its release quarantine),
/// so the REVERT history beside it is moved aside rather than inherited: an old incarnation's merge
/// ids would otherwise be revertible against the new database's rows.
///
/// RED before item 10's code: the old `<db>.history` stays and is read as this database's.
#[test]
fn a_fresh_log_moves_the_old_history_aside() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reach10.db");
    let mut db = Db::open(&path);
    db.seed(&[(1, 10)]);
    let _ = db.task("a", "UPDATE inventory SET qty = 11 WHERE id = 1;");
    db.o.txn.checkpoint().unwrap();
    drop(db);
    let with_suffix = |suffix: &str| {
        let mut p = path.as_os_str().to_os_string();
        p.push(suffix);
        PathBuf::from(p)
    };
    let history = with_suffix(".history");
    let old = std::fs::read(&history).expect("premise: the checkpoint wrote the store");
    std::fs::remove_file(with_suffix(".wal")).unwrap();

    let _db = Db::open(&path);
    assert_ne!(
        std::fs::read(&history).ok(),
        Some(old.clone()),
        "a fresh log kept the earlier database's REVERT history as its own"
    );
    let aside: Vec<Vec<u8>> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with("reach10.db.history.before-"))
        .map(|p| std::fs::read(p).unwrap())
        .collect();
    assert_eq!(aside, vec![old], "the earlier history was not moved aside whole");
}
