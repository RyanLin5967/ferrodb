//! I15 PROBE — throwaway. Does B3's envelope govern B11's `ALTER TABLE` on a branch?
//!
//! # The gap this closes
//!
//! Every policy verb in this runtime lives in an in-memory `Mutex<State>`: merge policy, escrow
//! claims, quarantine reasons. Restarting the database silently un-governs every running agent,
//! and there was no scope at all on WHAT a session may touch — an agent session could write any
//! table in the database.
//!
//! The envelope is a default-deny allowlist — writable tables, writable columns, a floor per
//! column, permitted verbs, and a row-write budget — held in the branch's **own durable record**
//! and enforced at `AgentRuntime::stage_all`, which is already the single funnel every branch
//! write passes through and is already statement-atomic. The refusal inherits that atomicity.
//!
//! # Everything here is decided on the after-image
//!
//! Not on the shape of the op, and not on the SQL keyword. That is the rule the funnel already
//! learned the expensive way: a bound keyed on `Add(negative)` let `SET qty = -100` walk past the
//! floor as a plain `Assign`, and a float decrement slipped through the same gap. Two tests below
//! name the shape that would fail without it —
//! `an_assignment_that_lowers_a_bounded_value_is_refused_not_just_a_decrement` and
//! `an_insert_cannot_write_a_column_the_branch_was_never_granted`, the latter because an INSERT's
//! `Op` carries `col: None` and therefore names no column at all.

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::{table_id, AgentRuntime};
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::branch::{BranchCatalog, CapabilityEnvelope, ColumnCapability, Verb};
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
use ferrodb::wal::recovery::recover;
use ferrodb::wal::txn::TxnManager;

/// `inventory (id INTEGER, qty INTEGER)` — column indices as the envelope names them.
const ID: u32 = 0;
const QTY: u32 = 1;

/// A database whose branch metadata, heap and WAL all live in one directory, so the whole thing
/// can be dropped and reopened from the files alone.
struct Db {
    dir: tempfile::TempDir,
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
}

impl Db {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let (catalog, bp, txn, runtime) = Self::attach(dir.path().to_path_buf(), true);
        Db { dir, catalog, bp, txn, runtime }
    }

    fn attach(
        dir: PathBuf,
        fresh: bool,
    ) -> (Catalog, Arc<BufferPoolManager>, Arc<TxnManager>, Arc<AgentRuntime>) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(fresh)
            .open(dir.join("envelope.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));

        let wal = Arc::new(WalManager::new(dir.join("envelope.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        // The sequence the CLI uses on open: recover the WAL BEFORE reading the catalog. Skipping
        // it reattaches to a heap whose committed rows were never replayed, and every table reads
        // back empty — measured, and the reason this line is here rather than assumed.
        recover(&txn).unwrap();
        let catalog = if fresh {
            Catalog::create(bp.clone()).unwrap()
        } else {
            Catalog::open(bp.clone(), 1).unwrap()
        };
        let branches = Arc::new(LogBranchCatalog::open(&dir.join("branches.log"), 1).unwrap());
        let runtime =
            Arc::new(AgentRuntime::with_catalog(Arc::clone(&branches) as Arc<dyn BranchCatalog>));
        (catalog, bp, txn, runtime)
    }

    /// **Close the database and open it again from the files.** Every in-memory structure — the
    /// runtime and its `Mutex<State>`, the branch catalog and its record index, the buffer pool —
    /// is dropped. Anything that survives came off disk.
    fn reopen(self) -> Db {
        self.bp.flush_all().unwrap();
        let Db { dir, catalog, bp, txn, runtime } = self;
        drop(catalog);
        drop(txn);
        drop(runtime);
        drop(bp);
        let (catalog, bp, txn, runtime) = Self::attach(dir.path().to_path_buf(), false);
        Db { dir, catalog, bp, txn, runtime }
    }

    /// Drop the runtime, its `Mutex<State>` and the branch catalog, and reopen the catalog from
    /// `branches.log`. The heap, WAL and SQL catalog stay attached, so the write path still works
    /// end to end while everything the envelope could have been cached in is gone.
    fn restart_branch_metadata(&mut self) {
        let path = self.dir.path().join("branches.log");
        self.runtime = Arc::new(AgentRuntime::new()); // drops the old runtime and its catalog
        let branches = Arc::new(LogBranchCatalog::open(&path, 1).unwrap());
        self.runtime =
            Arc::new(AgentRuntime::with_catalog(Arc::clone(&branches) as Arc<dyn BranchCatalog>));
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

    /// The refusal text, or a panic naming the statement that was let through.
    fn refused(&mut self, sql: &str, s: &mut Session) -> String {
        match self.exec(sql, s) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("the envelope let `{sql}` through"),
        }
    }

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("CREATE TABLE payroll (id INTEGER NOT NULL, salary INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 20);", &mut s);
        self.ok("INSERT INTO inventory VALUES (3, 20);", &mut s);
        self.ok("INSERT INTO payroll VALUES (1, 1000);", &mut s);
    }

    fn qty(&mut self, id: i32) -> i32 {
        let mut s = self.session();
        match self.ok(&format!("SELECT qty FROM inventory WHERE id = {id};"), &mut s) {
            Outcome::Rows(rows) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Integer(i)) => *i,
                other => panic!("unexpected qty: {other:?}"),
            },
            _ => panic!("expected rows"),
        }
    }

    fn salary(&mut self, id: i32) -> i32 {
        let mut s = self.session();
        match self.ok(&format!("SELECT salary FROM payroll WHERE id = {id};"), &mut s) {
            Outcome::Rows(rows) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Integer(i)) => *i,
                other => panic!("unexpected salary: {other:?}"),
            },
            _ => panic!("expected rows"),
        }
    }

    fn count(&mut self, table: &str) -> usize {
        let mut s = self.session();
        match self.ok(&format!("SELECT id FROM {table};"), &mut s) {
            Outcome::Rows(rows) => rows.len(),
            _ => panic!("expected rows"),
        }
    }

    /// Open an agent session and return its branch.
    fn begin(&mut self, agent: &str, s: &mut Session) -> BranchId {
        self.ok(&format!("BEGIN AGENT SESSION AS '{agent}' RUN 'r_{agent}';"), s);
        s.agent.as_ref().unwrap().branch
    }
}

/// Everything on `inventory`, nothing anywhere else, no floor, ample budget.
fn inventory_only() -> CapabilityEnvelope {
    CapabilityEnvelope::new(Verb::ALL, 1_000).allow(
        table_id("inventory").0,
        vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
    )
}


/// **The measurement.** An agent on a branch whose envelope allows only `inventory` runs
/// `ALTER TABLE payroll ADD COLUMN`. The `UPDATE payroll` above it is the anti-vacuity control:
/// without it a refusal of the ALTER could mean the envelope was never installed.
#[test]
fn i15_does_the_envelope_govern_an_alter_on_a_table_it_never_granted() {
    let mut db = Db::new();
    db.seed();
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();

    let mut a = db.session();
    let branch = db.begin("ddl_probe", &mut a);

    // Control: the envelope IS in force on this session, for rows.
    let refusal = db.refused("UPDATE payroll SET salary = 0 WHERE id = 1;", &mut a);
    assert!(
        refusal.contains("not on the allowlist"),
        "the row-write control refused for the wrong reason: {refusal}"
    );

    // The measurement itself.
    // `Outcome` has no `Debug`, so the result is rendered by hand — Ok carries its display form.
    let alter = db
        .exec("ALTER TABLE payroll ADD COLUMN note VARCHAR(16);", &mut a)
        .map(|o| match o {
            Outcome::Agent(a) => format!("Ok(Agent({a}))"),
            Outcome::Affected(n) => format!("Ok(Affected({n}))"),
            Outcome::Ok => "Ok(Ok)".to_string(),
            Outcome::Rows(r) => format!("Ok(Rows({}))", r.len()),
            Outcome::Explain(e) => format!("Ok(Explain({e}))"),
            // Merge fixup: B9 added `Outcome::Table` after this probe was written, and the match
            // is exhaustive. Rendered like `Rows`, by count.
            Outcome::Table(t) => format!("Ok(Table({}))", t.len()),
        })
        .map_err(|e| format!("Err({e})"));
    let pending = db.runtime.pending_schema_edits(branch);
    eprintln!("I15-PROBE alter_result = {alter:?}");
    eprintln!("I15-PROBE pending_schema_edits({branch}) = {pending:?}");
    eprintln!(
        "I15-PROBE envelope_after = {:?}",
        db.runtime.envelope_of(branch).unwrap()
    );

    // Pinned to what this merge MEASURES, not to what the design intends: `stage_schema_edit`
    // reads the catalog and the workspace and never consults `self.branches`, so no envelope
    // check runs on the DDL path at all.
    assert!(alter.is_ok(), "the ALTER was refused: {alter:?}");
    assert!(
        pending.iter().any(|(t, _)| t == "payroll"),
        "the branch is not carrying a pending edit on payroll: {pending:?}"
    );
}

/// **The consequence.** A pending edit is only half the question: what the envelope is for is
/// keeping a branch's authority off tables it was never granted, and the edit is published to the
/// SHARED catalog at `MERGE`. So the branch merges, and `payroll`'s shared shape is read back
/// through plain SQL on a session that is not the agent's.
#[test]
fn i15_does_the_ungoverned_alter_reach_the_shared_table_at_merge() {
    let mut db = Db::new();
    db.seed();
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();

    let mut a = db.session();
    db.begin("ddl_probe_merge", &mut a);
    // A write the envelope DOES allow, so the merge has a row to carry and cannot be dismissed as
    // an empty merge that happened to publish DDL.
    db.ok("UPDATE inventory SET qty = 7 WHERE id = 1;", &mut a);
    let alter = db.exec("ALTER TABLE payroll ADD COLUMN note VARCHAR(16);", &mut a);
    assert!(alter.is_ok(), "the ALTER was refused before the merge could be measured");

    // **Anti-vacuity: the detector, fired on purpose.** `SELECT note FROM payroll` is the whole
    // instrument, so it is run BEFORE the merge, where the column provably does not exist in the
    // shared catalog. If this passed, the assertion after the merge would prove nothing.
    {
        let mut probe = db.session();
        let before = db.exec("SELECT note FROM payroll WHERE id = 1;", &mut probe);
        assert!(
            before.is_err(),
            "the detector does not fire: `note` resolved against payroll before any merge"
        );
        eprintln!("I15-PROBE detector_fires_before_merge = true");
    }

    let merged = db.exec("MERGE;", &mut a).map_err(|e| e.to_string());
    eprintln!("I15-PROBE merge = {}", if merged.is_ok() { "Ok" } else { "Err" });
    if let Err(e) = &merged {
        eprintln!("I15-PROBE merge_err = {e}");
    }

    let mut s = db.session();
    let after = db.exec("SELECT note FROM payroll WHERE id = 1;", &mut s);
    eprintln!(
        "I15-PROBE payroll_has_note_column_in_shared_catalog = {}",
        after.is_ok()
    );
    if let Err(e) = &after {
        eprintln!("I15-PROBE select_err = {e}");
    }
    assert!(
        after.is_ok(),
        "the column never reached the shared table, so the envelope stopped it somewhere later"
    );
}
