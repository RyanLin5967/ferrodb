//! F10 — the durable Typed Effect Log, driven through the SQL surface a user actually calls.
//!
//! `src/tel/tests_durable_log.rs` proves the store: the format, the refusals, the aimed crashes.
//! This file proves the *wiring* — that an `AgentRuntime` built over `DurableEffectLog` captures an
//! agent task's frames exactly as one built over `MemEffectLog` does, and that the merge computed
//! from them after a full restart is the same merge.
//!
//! **What "a merge" means here, stated because a comment in `runtime.rs` says otherwise.**
//! `AgentRuntime::merge`, `evaluate_merge` and `diff` read `Workspace.frame` — the live session's
//! accumulating frame — and never call `EffectLog::frames_for`. `self.log` appears exactly twice in
//! `src/agent_sql/runtime.rs`: at the accessor, and at the one `append` inside `stage_all`. So the
//! log is write-only on the runtime's own path, and the doc comment on `AgentRuntime::log` ("MERGE
//! and DIFF both read from here through the shared traits, so the log is on the live path") is
//! stale. The path that *does* read the log is a `Merger` over it, which is what
//! `tests/agent_sql_surface.rs` uses and what this file uses.
//!
//! A restart is also the only place that distinction can be seen, and it is the whole of F10: a
//! `Workspace` does not survive one, so afterwards the log is the only thing left to compute a
//! merge from. On a cluster that is not a restart but a promotion, and the frames belong to a node
//! that is gone.

use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::SurfaceMerger;
use ferrodb::branch::types::BranchId;
use ferrodb::branch::{BranchCatalog, LogBranchCatalog};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::log::DurableEffectLog;
use ferrodb::tel::merge::{Diff, Merger};
use ferrodb::tel::op::{Delta, OpKind};
use ferrodb::tel::{EffectLog, MemEffectLog, TxnFrame};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// `TRUNK_ROOT_PAGE` in `agent_sql::runtime` is private and is 1. Named rather than inlined so the
/// reason it is 1 is visible: it is a placeholder id for a map-backed runtime, not a B+tree page.
const TRUNK_ROOT: u32 = 1;

/// One process's worth of database, over a directory.
struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
}

fn branch_catalog() -> Arc<dyn BranchCatalog> {
    Arc::new(LogBranchCatalog::in_memory(TRUNK_ROOT))
}

impl Db {
    /// `durable` picks the store under test. Both arms are exercised: the in-memory one is this
    /// file's anti-vacuity half and must fail the assertions the durable one passes.
    fn open(dir: &Path, durable: bool) -> Self {
        let path = dir.join("agent.db");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.join("agent.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);

        let log: Arc<dyn EffectLog> = if durable {
            DurableEffectLog::default_for_database(path.to_str().unwrap()).unwrap()
        } else {
            Arc::new(MemEffectLog::new())
        };
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::with_parts(branch_catalog(), log)) }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, session: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), session)
    }

    fn ok(&mut self, sql: &str, session: &mut Session) -> Outcome {
        match self.exec(sql, session) {
            Ok(o) => o,
            Err(e) => panic!("{sql} failed: {e}"),
        }
    }

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 5);", &mut s);
    }

    fn begin_agent(&mut self, as_: &str, run_id: &str, s: &mut Session) -> BranchId {
        match self.ok(&format!("BEGIN AGENT SESSION AS '{as_}' RUN '{run_id}';"), s) {
            Outcome::Agent(AgentOutput::SessionStarted(st)) => st.branch,
            _ => panic!("BEGIN AGENT SESSION did not start a session"),
        }
    }

    /// One agent task of two statements, which is one frame re-appended twice.
    fn run_one_agent_task(&mut self) -> BranchId {
        let mut a = self.session();
        let branch = self.begin_agent("restock", "r1", &mut a);
        self.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1 AND qty >= 5;", &mut a);
        self.ok("UPDATE inventory SET qty = qty - 1 WHERE id = 2;", &mut a);
        branch
    }
}

/// **The restart.** A new runtime over the effect log that survived, with no catalog and no page
/// store — which is the honest instrument rather than a shortcut: after a restart the log really is
/// the only thing left, because a `Workspace` does not survive one.
fn reopen_log_only(dir: &Path, durable: bool) -> Arc<AgentRuntime> {
    let db = dir.join("agent.db");
    let log: Arc<dyn EffectLog> = if durable {
        DurableEffectLog::default_for_database(db.to_str().unwrap()).unwrap()
    } else {
        Arc::new(MemEffectLog::new())
    };
    Arc::new(AgentRuntime::with_parts(branch_catalog(), log))
}

fn diff_from(runtime: &Arc<AgentRuntime>, branch: BranchId) -> Diff {
    SurfaceMerger::new(runtime.log().clone())
        .diff(BranchId::TRUNK, branch)
        .unwrap()
}

/// Compose every integer `Add` a set of frames carries — the units the Cassandra counter trap is
/// stated in. A re-appended frame stored twice shows up here as a decrement applied twice.
fn composed_delta(frames: &[TxnFrame]) -> i64 {
    frames
        .iter()
        .flat_map(|f| f.ops.iter())
        .filter_map(|op| match op.kind {
            OpKind::Add(Delta::Int(n)) => Some(n),
            _ => None,
        })
        .sum()
}

/// **Exit criterion, through the surface: frames survive a restart, and the merge computed after
/// one agrees with the merge computed before it.**
///
/// The workload is the shape `stage_all` actually produces: one frame per task, re-appended after
/// every statement, so the second append is a *growth* of the first. A log that stored each whole
/// re-append and concatenated them on replay would come back with three ops from a two-statement
/// task — and `dedup_by_txn` would not catch it, because there is one frame and the doubling is
/// inside it.
#[test]
fn an_agent_tasks_frames_and_its_merge_survive_a_process_restart() {
    let dir = tempfile::tempdir().unwrap();

    // ---- process one --------------------------------------------------------------------------
    let (branch, before, frames_before) = {
        let mut db = Db::open(dir.path(), true);
        db.seed();
        let branch = db.run_one_agent_task();

        let frames = db.runtime.log().frames_for(branch, 0).unwrap();
        assert_eq!(frames.len(), 1, "one agent task is one frame");
        assert_eq!(frames[0].ops.len(), 2, "two statements, two ops");
        assert_eq!(frames[0].guards.len(), 2, "each UPDATE's WHERE is a guard");
        assert_eq!(composed_delta(&frames), -6, "qty fell by 5 and by 1");

        let d = diff_from(&db.runtime, branch);
        assert_eq!(d.ops.len(), 2);
        assert!(
            d.guards.iter().any(|g| g.violated_predicate().contains("qty >= 5")),
            "the guard the first UPDATE ran under never reached the diff"
        );
        (branch, d, frames)
    };
    assert!(dir.path().join("agent.db.tel").exists(), "no <db>.tel was written");

    // ---- process two --------------------------------------------------------------------------
    let runtime = reopen_log_only(dir.path(), true);
    let after = runtime.log().frames_for(branch, 0).unwrap();
    assert_eq!(after.len(), 1, "the frame did not survive the restart");
    assert_eq!(
        after[0].ops.len(),
        2,
        "the frame came back with {} ops; the task ran two statements",
        after[0].ops.len()
    );
    assert_eq!(
        composed_delta(&after),
        -6,
        "the composed decrement changed across the restart: a re-append was replayed twice"
    );
    assert_eq!(
        format!("{after:?}"),
        format!("{frames_before:?}"),
        "a frame did not survive byte for byte"
    );
    assert_eq!(
        diff_from(&runtime, branch),
        before,
        "the merge computed after the restart disagrees with the one computed before it"
    );

    // ---- the anti-vacuity half ----------------------------------------------------------------
    //
    // The identical script over `MemEffectLog` loses everything at the restart, which is what F10
    // exists to close. Asserted rather than commented, so the assertions above cannot pass for a
    // reason that has nothing to do with the file.
    let mem_dir = tempfile::tempdir().unwrap();
    let mem_branch = {
        let mut db = Db::open(mem_dir.path(), false);
        db.seed();
        let branch = db.run_one_agent_task();
        assert_eq!(db.runtime.log().frames_for(branch, 0).unwrap().len(), 1);
        assert!(!diff_from(&db.runtime, branch).ops.is_empty());
        branch
    };
    let mem_runtime = reopen_log_only(mem_dir.path(), false);
    assert!(
        mem_runtime.log().frames_for(mem_branch, 0).unwrap().is_empty(),
        "MemEffectLog kept an agent task's frames across a restart, so this test proves nothing"
    );
    assert_eq!(
        diff_from(&mem_runtime, mem_branch),
        Diff { from: BranchId::TRUNK, to: mem_branch, ops: Vec::new(), guards: Vec::new() },
        "the in-memory store answered with effects after a restart"
    );
}

/// A runtime over the durable log is a drop-in: the same statements capture the same frames as one
/// over `MemEffectLog`, byte for byte.
///
/// The regression half. The durable store refuses more than the in-memory one does — a string past
/// its length prefix, a guard nested past the decoder's cap — and that is deliberate; it must not
/// change anything about an ordinary task.
#[test]
fn the_durable_store_captures_exactly_what_the_in_memory_one_captures() {
    let captured: Vec<String> = [true, false]
        .into_iter()
        .map(|durable| {
            let dir = tempfile::tempdir().unwrap();
            let mut db = Db::open(dir.path(), durable);
            db.seed();

            // One task, three statements: two UPDATEs (each an op plus a guard) and an INSERT,
            // whose op carries a full row image and no guard. So the frame grows three times and
            // covers both op shapes.
            let mut a = db.session();
            let branch = db.begin_agent("restock", "r1", &mut a);
            db.ok("UPDATE inventory SET qty = qty - 5 WHERE id = 1 AND qty >= 5;", &mut a);
            db.ok("INSERT INTO inventory VALUES (3, 7);", &mut a);
            db.ok("UPDATE inventory SET qty = qty - 1 WHERE id = 2;", &mut a);

            // A second, independent task on its own branch, so ordering across frames is compared
            // too and not just one frame's contents.
            let mut c = db.session();
            let other = db.begin_agent("audit", "r2", &mut c);
            db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 1;", &mut c);

            let mut all = db.runtime.log().frames_for(branch, 0).unwrap();
            all.extend(db.runtime.log().frames_for(other, 0).unwrap());
            let text = format!("{all:?}");
            // Branch ids are minted per process and both processes mint the same ones, but assert
            // the shape rather than trusting that: three ops on the first frame, one on the second.
            assert_eq!(all.len(), 2, "expected one frame per task");
            assert_eq!(all[0].ops.len(), 3);
            assert_eq!(all[1].ops.len(), 1);
            text
        })
        .collect();
    assert_eq!(captured[0], captured[1], "the durable store captured something different");
    assert!(captured[0].contains("Add"), "neither store captured anything");
    assert!(captured[0].contains("RowCreate"), "the INSERT's row image was not captured");
}

/// The values the encoder has to be byte-exact about, driven from SQL rather than constructed:
/// a DECIMAL whose trailing zero is information, and a VARCHAR. `Value`'s equality is numeric —
/// `Decimal("1.50") == Decimal("1.5")` — so the assertion is on the text.
#[test]
fn wide_typed_values_survive_the_restart_with_their_bytes_intact() {
    let dir = tempfile::tempdir().unwrap();
    let (branch, before) = {
        let mut db = Db::open(dir.path(), true);
        let mut s = db.session();
        db.ok(
            "CREATE TABLE prices (id INTEGER NOT NULL, amount DECIMAL, label VARCHAR(32), \
             ts TIMESTAMP, big BIGINT);",
            &mut s,
        );
        db.ok("INSERT INTO prices VALUES (1, 1.50, 'x', 0, 1);", &mut s);

        let mut a = db.session();
        let branch = db.begin_agent("pricer", "r1", &mut a);
        db.ok("UPDATE prices SET amount = 2.50, label = 'y' WHERE id = 1;", &mut a);
        let frames = db.runtime.log().frames_for(branch, 0).unwrap();
        assert!(!frames.is_empty(), "the UPDATE captured nothing");
        (branch, format!("{frames:?}"))
    };

    let runtime = reopen_log_only(dir.path(), true);
    let frames = runtime.log().frames_for(branch, 0).unwrap();
    assert_eq!(frames.len(), 1);
    let after = format!("{frames:?}");
    assert_eq!(after, before, "a wide-typed value did not survive byte for byte");
    assert!(
        after.contains("2.50"),
        "the decimal did not come back with the scale it was written with: {after}"
    );
    assert!(after.contains("\"y\""), "the varchar did not come back: {after}");
    // The witness — the pre-op value a merge needs for LWW and for equality detection — survives
    // too, and it is the one that carries the OTHER decimal scale.
    assert!(after.contains("1.50"), "the witness did not survive: {after}");
}
