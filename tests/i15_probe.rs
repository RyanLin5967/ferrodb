//! B3 — the durable capability envelope at the write funnel.
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


// ================= I15 review probes =================

/// PROBE 1: REVERT MERGE inside a governed agent session rewrites rows of a table the envelope
/// forbids. This is a DATA write to the shared catalog, not a schema change.
#[test]
fn probe_revert_merge_rewrites_a_forbidden_table() {
    let mut db = Db::new();
    db.seed();

    // An ungoverned session merges a change to payroll, producing a merge id.
    let mut w = db.session();
    db.begin("writer", &mut w);
    db.ok("UPDATE payroll SET salary = 4242 WHERE id = 1;", &mut w);
    let merge_id = match db.ok("MERGE;", &mut w) {
        Outcome::Agent(a) => format!("{a}"),
        _ => panic!("merge"),
    };
    println!("merge output: {merge_id}");
    assert_eq!(db.salary(1), 4242);

    // Now govern trunk: inventory only.
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();
    let mut a = db.session();
    let branch = db.begin("reverter", &mut a);
    let err = db.refused("UPDATE payroll SET salary = 0 WHERE id = 1;", &mut a);
    assert!(err.contains("may not write table `payroll`"), "got {err}");

    // find the merge id string (m_1 etc.)
    let id = merge_id
        .split_whitespace()
        .find(|t| t.starts_with("m_"))
        .map(|s| s.trim_end_matches(|c: char| !c.is_alphanumeric()).to_string())
        .expect("no merge id in report");
    println!("reverting {id}");
    let out = db.exec(&format!("REVERT MERGE {id} CASCADE;"), &mut a);
    println!("revert result: {:?}", out.as_ref().map(|_| "ok").map_err(|e| e.to_string()));
    assert!(out.is_ok(), "revert refused: {:?}", out.err().map(|e| e.to_string()));

    let after = db.salary(1);
    println!("payroll.salary after revert from a governed session = {after}");
    assert_eq!(
        after, 1000,
        "REVERT from a governed session did NOT change the forbidden table"
    );
    // and nothing was charged
    let env = db.runtime.envelope_of(branch).unwrap().unwrap();
    println!("row_writes after the revert = {}", env.row_writes());
    assert_eq!(env.row_writes(), 0, "the revert was charged after all");
}

/// PROBE 2: the tautology. `assert_eq!(table_id(x), table_id(x))` holds for a name that never
/// existed, so it says nothing about the drop/recreate.
#[test]
fn probe_table_id_assertion_is_tautological() {
    assert_eq!(table_id("never_existed_anywhere"), table_id("never_existed_anywhere"));
}

/// PROBE 3: DROP+CREATE with the columns REORDERED strips a floor off the column it guarded.
#[test]
fn probe_drop_create_strips_a_floor() {
    let mut db = Db::new();
    db.seed();
    // qty is floored at 0; id is open.
    let env = CapabilityEnvelope::new(Verb::ALL, 1_000).allow(
        table_id("inventory").0,
        vec![ColumnCapability::open(ID), ColumnCapability::floored(QTY, 0)],
    );
    db.runtime.restrict_branch(BranchId::TRUNK, env).unwrap();

    let mut a = db.session();
    db.begin("floorbreaker", &mut a);
    // The floor is live.
    let err = db.refused("UPDATE inventory SET qty = -999 WHERE id = 1;", &mut a);
    println!("floor refusal: {err}");
    assert!(err.contains("floor"), "got {err}");

    // Two ungoverned verbs: same table name, same column names, swapped order.
    db.ok("DROP TABLE inventory;", &mut a);
    db.ok("CREATE TABLE inventory (qty INTEGER NOT NULL, id INTEGER);", &mut a);
    db.ok("INSERT INTO inventory VALUES (-999, 1);", &mut a);
    match db.ok("SELECT qty, id FROM inventory;", &mut a) {
        Outcome::Rows(rows) => println!("after substitution, branch sees {rows:?}"),
        _ => panic!("rows"),
    }
    // If this passes, the floor granted over `qty` no longer guards `qty`.
}

/// PROBE 4: a DELETE that the column allowlist refused becomes admissible once the table is
/// recreated with only the granted column.
#[test]
fn probe_drop_create_unlocks_a_refused_delete() {
    let mut db = Db::new();
    db.seed();
    // DELETE granted, but only column 0 is on the allowlist -> every delete is refused.
    let env = CapabilityEnvelope::new(Verb::ALL, 1_000)
        .allow(table_id("inventory").0, vec![ColumnCapability::open(ID)]);
    db.runtime.restrict_branch(BranchId::TRUNK, env).unwrap();

    let mut a = db.session();
    db.begin("deleter", &mut a);
    let err = db.refused("DELETE FROM inventory WHERE id = 1;", &mut a);
    println!("delete refusal: {err}");
    assert!(err.contains("column 1"), "got {err}");

    db.ok("DROP TABLE inventory;", &mut a);
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL);", &mut a);
    db.ok("INSERT INTO inventory VALUES (1);", &mut a);
    let out = db.exec("DELETE FROM inventory WHERE id = 1;", &mut a);
    println!("delete after substitution: {:?}", out.as_ref().map(|_| "OK").map_err(|e| e.to_string()));
    assert!(out.is_ok(), "still refused: {:?}", out.err().map(|e| e.to_string()));
}

/// PROBE 5: a governed session can MERGE and ABANDON a branch it does not own, publishing or
/// destroying another agent's work — neither is on the DDL pin's list.
#[test]
fn probe_governed_session_merges_a_foreign_branch() {
    let mut db = Db::new();
    db.seed();

    // An ungoverned worker stages a change to payroll and leaves it unmerged.
    let mut w = db.session();
    let victim = db.begin("victim", &mut w);
    db.ok("UPDATE payroll SET salary = 1 WHERE id = 1;", &mut w);
    let victim_name = format!("b_{}", victim.id);
    assert_eq!(db.salary(1), 1000, "the victim's write must still be private");

    // Now govern trunk and open a governed session.
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();
    let mut a = db.session();
    db.begin("thief", &mut a);
    let err = db.refused("UPDATE payroll SET salary = 0 WHERE id = 1;", &mut a);
    assert!(err.contains("may not write table `payroll`"), "got {err}");

    let out = db.exec(&format!("MERGE BRANCH {victim_name};"), &mut a);
    println!("foreign merge: {:?}", out.as_ref().map(|_| "OK").map_err(|e| e.to_string()));
    assert!(out.is_ok(), "refused: {:?}", out.err().map(|e| e.to_string()));
    let after = db.salary(1);
    println!("payroll.salary after a governed session merged a foreign branch = {after}");
    assert_eq!(after, 1, "the foreign merge did not publish");
}
