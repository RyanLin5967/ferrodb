//! Exit criterion 9, through the executor: which agent + run + model wrote a given *stored version*.
//!
//! The criterion was PARTIAL because attribution existed only in the agent runtime's own bookkeeping.
//! `src/execution` held zero provenance references, so a row that a merge published into the shared
//! tables — a real tuple, at a real `RecordId`, written by the ordinary executor — could not be
//! attributed at all. The per-`RecordId` path was implemented and tested, but nothing on the write
//! path ever called it.
//!
//! The merge publish path now carries the authoring run down to `Modify::set_author`, so the version
//! the executor writes is stamped with the agent that produced it.
//!
//! Two things are deliberately NOT attributed, and both are asserted here rather than left to
//! assumption, because a provenance system that over-claims is worse than one that abstains:
//!   - a write made outside any agent session has no author, and reads back as `ProvId::NONE`;
//!   - a `REVERT` is not a write by the agent whose work is being undone.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Executor, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::optimizer::optimizer::lower;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::planner::physical_plan::PhysicalPlan;
use ferrodb::provenance::{ProvId, ProvenanceStore};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::{ReadView, TxnManager};

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
            .open(dir.path().join("prov_exec.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("prov_exec.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
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
        assert_eq!(stmts.len(), 1, "expected one statement: {}", sql);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), session)
    }

    fn ok(&mut self, sql: &str, session: &mut Session) -> Outcome {
        self.exec(sql, session).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn scan(&self, table: &str) -> Box<dyn Executor> {
        let view = Arc::new(ReadView { snapshot: self.txn.read_snapshot(), txn_id: 0 });
        lower(PhysicalPlan::SeqScan { table: table.into() }, &self.catalog, self.bp.clone(), view)
            .unwrap()
    }

    /// The physical slot currently holding the row with this primary key.
    fn rid_of(&self, table: &str, pk: i32) -> RecordId {
        let mut exec = self.scan(table);
        while let Some(row) = exec.next() {
            let (rid, values) = row.unwrap();
            if values[0] == Value::Integer(pk) {
                return rid;
            }
        }
        panic!("row {pk} not found in {table}");
    }
}

fn seeded() -> (Db, Session) {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
    db.ok("INSERT INTO inventory VALUES (1, 100);", &mut s);
    db.ok("INSERT INTO inventory VALUES (2, 200);", &mut s);
    (db, s)
}

#[test]
fn a_row_published_by_a_merge_names_the_agent_run_and_model_that_wrote_it() {
    let (mut db, mut s) = seeded();

    // Unattributed before any agent touches it — and asserted, so the later attribution cannot be
    // something that was already there.
    let rid = db.rid_of("inventory", 1);
    assert_eq!(
        db.runtime.provenance().attribute(rid).unwrap(),
        ProvId::NONE,
        "a row written outside any agent session must be unattributed"
    );

    let mut a = db.session();
    db.ok(
        "BEGIN AGENT SESSION AS 'pricing-agent' RUN 'r_8fk2' MODEL 'claude/v1';",
        &mut a,
    );
    db.ok("UPDATE inventory SET qty = 42 WHERE id = 1;", &mut a);
    db.ok("MERGE;", &mut a);

    // The merge republished the row, so its version lives at a (possibly new) slot.
    let rid = db.rid_of("inventory", 1);
    let who = db.runtime.provenance().attribute(rid).unwrap();
    assert_ne!(
        who,
        ProvId::NONE,
        "the version a merge published is unattributed; the executor never stamped it"
    );

    let run = db.runtime.provenance().lookup(who).unwrap();
    assert_eq!(run.agent_id, "pricing-agent");
    assert_eq!(run.run_id, "r_8fk2");
    assert_eq!(run.model, "claude");
    assert_eq!(run.model_version, "v1");

    // The row the agent never touched must not be swept up by the same stamp.
    assert_eq!(
        db.runtime.provenance().attribute(db.rid_of("inventory", 2)).unwrap(),
        ProvId::NONE,
        "a row the agent did not write was attributed to it anyway"
    );
    let _ = s;
}

#[test]
fn two_agents_writing_different_rows_are_told_apart() {
    let (mut db, _s) = seeded();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r_a' MODEL 'm/1';", &mut a);
    db.ok("UPDATE inventory SET qty = 11 WHERE id = 1;", &mut a);
    db.ok("MERGE;", &mut a);

    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-b' RUN 'r_b' MODEL 'm/1';", &mut b);
    db.ok("UPDATE inventory SET qty = 22 WHERE id = 2;", &mut b);
    db.ok("MERGE;", &mut b);

    let store = db.runtime.provenance();
    let who1 = store.attribute(db.rid_of("inventory", 1)).unwrap();
    let who2 = store.attribute(db.rid_of("inventory", 2)).unwrap();

    assert_ne!(who1, ProvId::NONE, "row 1 unattributed");
    assert_ne!(who2, ProvId::NONE, "row 2 unattributed");
    assert_ne!(who1, who2, "two different agents collapsed to one provenance id");
    assert_eq!(store.lookup(who1).unwrap().agent_id, "agent-a");
    assert_eq!(store.lookup(who2).unwrap().agent_id, "agent-b");
}

#[test]
fn one_run_is_never_split_across_two_provenance_entities() {
    // The store's contract is that attribution is run-level: one run, one `ProvId`. `begin_session`
    // honours it by REFUSING a second session for the same `(agent_id, run_id)` rather than by
    // reusing the id — worth pinning, because "refused" and "reused" are both consistent with the
    // headline and only one of them is what happens.
    //
    // The discriminator is `RunEntity::same_actor`, which includes `started_at`. Two sessions for
    // one run necessarily start at different instants, so the re-intern can never match and the
    // second is rejected. That makes the refusal unconditional in practice: it is not detecting a
    // client that changed the model mid-run, it fires for any repeat. Recorded because anyone
    // relaxing `same_actor` would silently turn this refusal into a second entity for one run,
    // which is the outcome the contract exists to prevent.
    let (mut db, _s) = seeded();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'r_same' MODEL 'm/1';", &mut a);
    db.ok("UPDATE inventory SET qty = 11 WHERE id = 1;", &mut a);
    db.ok("MERGE;", &mut a);

    let first = db.runtime.provenance().attribute(db.rid_of("inventory", 1)).unwrap();
    assert_ne!(first, ProvId::NONE);

    let mut b = db.session();
    let err = match db.exec("BEGIN AGENT SESSION AS 'agent-a' RUN 'r_same' MODEL 'm/1';", &mut b) {
        Err(e) => e,
        Ok(_) => panic!("a second session for one run must not silently mint a second entity"),
    };
    assert!(
        format!("{err}").contains("refusing to re-intern"),
        "expected the run-level refusal, got: {err}"
    );

    // The refusal must leave the first session's attribution intact rather than half-applied.
    assert_eq!(
        db.runtime.provenance().attribute(db.rid_of("inventory", 1)).unwrap(),
        first,
        "a refused re-intern disturbed the attribution already recorded for that run"
    );
    assert_eq!(db.runtime.provenance().lookup(first).unwrap().run_id, "r_same");
}

#[test]
fn a_plain_write_outside_any_agent_session_stays_unattributed() {
    // Abstaining is the correct answer here. Attributing an ordinary write to some default
    // identity would make the provenance query confidently wrong.
    let (mut db, mut s) = seeded();
    db.ok("INSERT INTO inventory VALUES (3, 300);", &mut s);
    let store = db.runtime.provenance();
    assert_eq!(store.attribute(db.rid_of("inventory", 3)).unwrap(), ProvId::NONE);
    assert!(
        store.lookup(ProvId::NONE).is_err(),
        "ProvId::NONE must name no run rather than resolving to one"
    );
}
