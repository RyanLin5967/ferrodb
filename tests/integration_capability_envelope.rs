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

// ---- exit criterion: a policy survives a reopen ---------------------------------------------

/// **A policy survives a reopen — proven by closing and reopening, not by asserting the setter
/// worked.**
///
/// The breaking shape is a policy held in `Mutex<State>`: the setter returns Ok, every assertion
/// inside the process passes, and the next process starts with an ungoverned agent. So the whole
/// database is dropped — runtime, branch catalog, buffer pool, heap, WAL — and the envelope is
/// then read back AND enforced through SQL on a session that did not exist when it was installed.
///
/// The reopen replays the WAL before reading the catalog, which is the sequence the CLI uses.
/// Without it the reattached heap reads every table as empty and this test would have "passed" a
/// refusal against a database with nothing in it to refuse.
#[test]
fn an_envelope_survives_closing_and_reopening_the_database() {
    let mut db = Db::new();
    db.seed();
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();

    let mut db = db.reopen();

    let recovered = db
        .runtime
        .envelope_of(BranchId::TRUNK)
        .unwrap()
        .expect("the envelope did not survive the reopen; it was only ever in memory");
    assert_eq!(recovered, inventory_only());

    // And it is still ENFORCED, which is the half a round-trip through the record does not prove.
    let mut a = db.session();
    let branch = db.begin("post_restart", &mut a);
    let err = db.refused("UPDATE payroll SET salary = 0 WHERE id = 1;", &mut a);
    assert!(err.contains("not on the allowlist"), "refused for the wrong reason: {err}");

    // Anti-vacuity: a write to the ALLOWED table is admitted, charged against the recovered
    // budget, and lands in main. Without all three the envelope could have come back as "refuse
    // everything" and this test would still have been green.
    db.ok("UPDATE inventory SET qty = 7 WHERE id = 1;", &mut a);
    assert_eq!(db.runtime.envelope_of(branch).unwrap().unwrap().row_writes(), 1);
    db.ok("MERGE;", &mut a);
    assert_eq!(db.qty(1), 7);
    assert_eq!(db.salary(1), 1000, "a refused write reached the shared table");
}

/// The same claim with the write carried all the way through `MERGE` into the shared table.
///
/// Here only the branch metadata layer restarts — the runtime, its `Mutex<State>` and the branch
/// catalog are dropped and the catalog is reopened from `branches.log` — while the heap stays
/// attached. That is the layer the envelope lives in, and it is the layer whose loss un-governs a
/// running agent. Two tests rather than one because neither reopen alone proves both halves.
#[test]
fn an_envelope_survives_a_restart_of_the_branch_metadata_layer() {
    let mut db = Db::new();
    db.seed();
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();
    db.restart_branch_metadata();

    let recovered = db.runtime.envelope_of(BranchId::TRUNK).unwrap().expect("envelope lost");
    assert_eq!(recovered, inventory_only());

    let mut a = db.session();
    db.begin("post_restart", &mut a);
    let err = db.refused("INSERT INTO payroll VALUES (5, 5);", &mut a);
    assert!(err.contains("may not write table `payroll`"), "got {err}");

    // Anti-vacuity, all the way to main.
    db.ok("UPDATE inventory SET qty = 7 WHERE id = 1;", &mut a);
    db.ok("MERGE;", &mut a);
    assert_eq!(db.qty(1), 7);
    // The refused statement here was an INSERT, so the row count is what sees it.
    assert_eq!(db.count("payroll"), 1, "the refused INSERT reached the shared table");
}

/// The budget's SPENT half has to survive too, or a restart hands a governed agent its quota back
/// — which is the same defect one level down.
#[test]
fn budget_already_spent_is_not_handed_back_by_a_restart() {
    let mut db = Db::new();
    db.seed();
    let envelope = CapabilityEnvelope::new(Verb::ALL, 2).allow(
        table_id("inventory").0,
        vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
    );
    db.runtime.restrict_branch(BranchId::TRUNK, envelope).unwrap();

    let branch = {
        let mut a = db.session();
        let branch = db.begin("spender", &mut a);
        db.ok("UPDATE inventory SET qty = 19 WHERE id = 1;", &mut a);
        assert_eq!(db.runtime.envelope_of(branch).unwrap().unwrap().row_writes(), 1);
        branch
    };

    let db = db.reopen();
    let after = db.runtime.envelope_of(branch).unwrap().expect("envelope lost");
    assert_eq!(after.row_writes(), 1, "a restart handed the spent budget back");
    assert_eq!(after.remaining(), 1);
}

// ---- exit criterion: default-deny, with the anti-vacuity half -------------------------------

/// **Default-deny: a table not on the allowlist is refused. Anti-vacuity: an allowed table still
/// writes.** Both halves in one test on purpose — a refusal with no admitted case beside it is
/// satisfied by a guard that refuses everything.
#[test]
fn a_table_off_the_allowlist_is_refused_and_one_on_it_still_writes() {
    let mut db = Db::new();
    db.seed();
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();

    let mut a = db.session();
    db.begin("scoped", &mut a);

    for sql in [
        "UPDATE payroll SET salary = 0 WHERE id = 1;",
        "INSERT INTO payroll VALUES (2, 5);",
        "DELETE FROM payroll WHERE id = 1;",
    ] {
        let err = db.refused(sql, &mut a);
        assert!(err.contains("may not write table `payroll`"), "`{sql}` gave {err}");
    }

    db.ok("UPDATE inventory SET qty = 11 WHERE id = 1;", &mut a);
    db.ok("INSERT INTO inventory VALUES (4, 4);", &mut a);
    db.ok("DELETE FROM inventory WHERE id = 3;", &mut a);
    db.ok("MERGE;", &mut a);
    assert_eq!(db.qty(1), 11);
    assert_eq!(db.count("inventory"), 3, "one added, one deleted, three seeded");
    // The refused statements were an UPDATE, an INSERT and a DELETE; a row count alone cannot see
    // the UPDATE, so the salary is checked too.
    assert_eq!(db.count("payroll"), 1, "the refused INSERT or DELETE reached the shared table");
    assert_eq!(db.salary(1), 1000, "the refused UPDATE reached the shared table");
}

/// An UPDATE that matches no row is still an attempt to write a table. Authority is not a function
/// of whether a WHERE happened to select anything, and a refusal that depends on the data is a
/// probe oracle.
#[test]
fn a_forbidden_table_is_refused_even_when_the_statement_matches_no_row() {
    let mut db = Db::new();
    db.seed();
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();
    let mut a = db.session();
    db.begin("prober", &mut a);
    let err = db.refused("UPDATE payroll SET salary = 0 WHERE id = 99999;", &mut a);
    assert!(err.contains("may not write table `payroll`"), "got {err}");
}

/// A branch with no envelope behaves exactly as it did before this field existed. Stated and
/// asserted rather than implied: this is the compatibility default every branch already on disk
/// loads with, and if it ever changes, every existing database changes with it.
#[test]
fn an_ungoverned_branch_writes_exactly_as_before() {
    let mut db = Db::new();
    db.seed();
    assert_eq!(db.runtime.envelope_of(BranchId::TRUNK).unwrap(), None);

    let mut a = db.session();
    db.begin("free", &mut a);
    db.ok("UPDATE payroll SET salary = -1 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = -100 WHERE id = 1;", &mut a);
    db.ok("MERGE;", &mut a);
    assert_eq!(db.qty(1), -100);
}

// ---- exit criterion: the after-image rule ---------------------------------------------------

/// **An assignment that lowers a bounded value is refused, not just a decrement.**
///
/// The breaking shape is `SET qty = -100`. It is an `Assign`, not an `Add`, so a bound keyed on
/// the op shape sees no decrement and admits it — which is exactly how the counter reached -100
/// against a floor of 0 the last time this was got wrong. The floor here never sees the op: it
/// compares the value the write would leave behind.
///
/// Unlike escrow, whose claims live in `Mutex<State>`, this floor is in the durable record.
#[test]
fn an_assignment_that_lowers_a_bounded_value_is_refused_not_just_a_decrement() {
    let mut db = Db::new();
    db.seed();
    let floored = CapabilityEnvelope::new(Verb::ALL, 1_000).allow(
        table_id("inventory").0,
        vec![ColumnCapability::open(ID), ColumnCapability::floored(QTY, 0)],
    );
    db.runtime.restrict_branch(BranchId::TRUNK, floored).unwrap();

    let mut a = db.session();
    db.begin("sneaky", &mut a);

    // The shape a bound keyed on `Add(negative)` would miss.
    let err = db.refused("UPDATE inventory SET qty = -100 WHERE id = 1;", &mut a);
    assert!(err.contains("the floor is 0"), "an assignment past the floor: {err}");
    // The shape it would catch, so the two are shown to reach the same refusal.
    let err = db.refused("UPDATE inventory SET qty = qty - 100 WHERE id = 1;", &mut a);
    assert!(err.contains("the floor is 0"), "a decrement past the floor: {err}");
    // Removing the row removes the bounded value, which no comparison on a surviving cell sees.
    let err = db.refused("DELETE FROM inventory WHERE id = 1;", &mut a);
    assert!(err.contains("the row was removed"), "a delete around the floor: {err}");

    // Anti-vacuity, three ways: at the floor, above it, and a raise.
    db.ok("UPDATE inventory SET qty = 0 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = qty + 5 WHERE id = 1;", &mut a);
    db.ok("UPDATE inventory SET qty = 999 WHERE id = 2;", &mut a);
    db.ok("MERGE;", &mut a);
    assert_eq!(db.qty(1), 5);
    assert_eq!(db.qty(2), 999);
}

/// **The breaking shape for the column allowlist is an INSERT**: its `Op` carries `col: None`, so
/// a check reading the ops sees it name no column while it writes every one of them. Reading the
/// before/after images instead reports every column the insert gave a value to.
#[test]
fn an_insert_cannot_write_a_column_the_branch_was_never_granted() {
    let mut db = Db::new();
    db.seed();
    // `qty` only. `id` is deliberately NOT granted.
    let qty_only = CapabilityEnvelope::new(Verb::ALL, 1_000).allow(
        table_id("inventory").0,
        vec![ColumnCapability::open(QTY)],
    );
    db.runtime.restrict_branch(BranchId::TRUNK, qty_only).unwrap();

    let mut a = db.session();
    db.begin("inserter", &mut a);
    let err = db.refused("INSERT INTO inventory VALUES (9, 1);", &mut a);
    assert!(err.contains("column 0"), "an INSERT wrote an ungranted column: {err}");

    // The same allowlist refuses an UPDATE of that column...
    let err = db.refused("UPDATE inventory SET id = 42 WHERE id = 1;", &mut a);
    assert!(err.contains("column 0"), "got {err}");
    // ...and admits an UPDATE of the granted one. Anti-vacuity.
    db.ok("UPDATE inventory SET qty = 3 WHERE id = 1;", &mut a);
    db.ok("MERGE;", &mut a);
    assert_eq!(db.qty(1), 3);
    assert_eq!(db.count("inventory"), 3, "the refused INSERT landed anyway");
}

/// The verb is derived from what happened to the row, not from the SQL keyword.
#[test]
fn a_verb_off_the_allowlist_is_refused_and_one_on_it_still_writes() {
    let mut db = Db::new();
    db.seed();
    let no_delete = CapabilityEnvelope::new(Verb::INSERT | Verb::UPDATE, 1_000).allow(
        table_id("inventory").0,
        vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
    );
    db.runtime.restrict_branch(BranchId::TRUNK, no_delete).unwrap();

    let mut a = db.session();
    db.begin("nodelete", &mut a);
    let err = db.refused("DELETE FROM inventory WHERE id = 1;", &mut a);
    assert!(err.contains("may not DELETE"), "got {err}");

    // Anti-vacuity: the two verbs that ARE granted work.
    db.ok("INSERT INTO inventory VALUES (7, 7);", &mut a);
    db.ok("UPDATE inventory SET qty = 8 WHERE id = 1;", &mut a);
    db.ok("MERGE;", &mut a);
    assert_eq!(db.count("inventory"), 4);
    assert_eq!(db.qty(1), 8);
}

// ---- the row budget --------------------------------------------------------------------------

/// **The breaking shape is one statement over many rows.** A budget checked and charged per row
/// admits rows until it runs out and leaves those rows written — the same defect the escrow batch
/// check exists for, one level up. The budget is decided for the whole statement before any of it
/// is recorded, so an over-budget statement leaves nothing behind and spends nothing.
///
/// The seed writes three rows per table for exactly this reason: a workload that only ever
/// produced one row per statement would never have the shape that fails.
#[test]
fn a_statement_that_overruns_the_row_budget_is_refused_whole() {
    let mut db = Db::new();
    db.seed();
    let two = CapabilityEnvelope::new(Verb::ALL, 2).allow(
        table_id("inventory").0,
        vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
    );
    db.runtime.restrict_branch(BranchId::TRUNK, two).unwrap();

    let mut a = db.session();
    let branch = db.begin("budgeted", &mut a);

    // Three rows against a budget of two.
    let err = db.refused("UPDATE inventory SET qty = 1;", &mut a);
    assert!(err.contains("row-write budget"), "got {err}");
    assert_eq!(
        db.runtime.envelope_of(branch).unwrap().unwrap().row_writes(),
        0,
        "a refused statement charged the budget anyway"
    );
    db.ok("MERGE;", &mut a);
    for id in [1, 2, 3] {
        assert_eq!(db.qty(id), 20, "a refused statement reached the shared table");
    }

    // Anti-vacuity: two rows fit, and the third write then does not.
    let mut b = db.session();
    let branch = db.begin("budgeted2", &mut b);
    db.ok("UPDATE inventory SET qty = 1 WHERE id = 1;", &mut b);
    db.ok("UPDATE inventory SET qty = 1 WHERE id = 2;", &mut b);
    assert_eq!(db.runtime.envelope_of(branch).unwrap().unwrap().remaining(), 0);
    let err = db.refused("UPDATE inventory SET qty = 1 WHERE id = 3;", &mut b);
    assert!(err.contains("row-write budget"), "got {err}");
    db.ok("MERGE;", &mut b);
    assert_eq!((db.qty(1), db.qty(2), db.qty(3)), (1, 1, 20));
}

/// A statement that changes nothing is not a write: it must not cost budget, or a no-op could
/// exhaust an agent's quota.
#[test]
fn a_write_that_changes_nothing_costs_no_budget() {
    let mut db = Db::new();
    db.seed();
    let one = CapabilityEnvelope::new(Verb::ALL, 1).allow(
        table_id("inventory").0,
        vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
    );
    db.runtime.restrict_branch(BranchId::TRUNK, one).unwrap();

    let mut a = db.session();
    let branch = db.begin("noop", &mut a);
    db.ok("UPDATE inventory SET qty = 20 WHERE id = 1;", &mut a); // already 20
    assert_eq!(db.runtime.envelope_of(branch).unwrap().unwrap().row_writes(), 0);
    // Anti-vacuity: a real change does cost one.
    db.ok("UPDATE inventory SET qty = 21 WHERE id = 1;", &mut a);
    assert_eq!(db.runtime.envelope_of(branch).unwrap().unwrap().row_writes(), 1);
}

// ---- inheritance and attenuation -------------------------------------------------------------

/// An envelope installed on trunk governs every agent session forked out of it. Without this the
/// envelope would be one `BEGIN AGENT SESSION` away from irrelevant, because every agent session
/// in this system runs on a forked child.
#[test]
fn an_agent_session_inherits_the_envelope_of_the_branch_it_forked_from() {
    let mut db = Db::new();
    db.seed();
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();

    let mut a = db.session();
    let branch = db.begin("child", &mut a);
    assert!(
        db.runtime.envelope_of(branch).unwrap().is_some(),
        "a child of a governed branch was forked ungoverned"
    );
    let err = db.refused("UPDATE payroll SET salary = 0 WHERE id = 1;", &mut a);
    assert!(err.contains("may not write table `payroll`"), "got {err}");
}

/// A governed branch cannot vote itself more authority. An envelope a governed party can widen is
/// a suggestion, not a capability.
#[test]
fn a_branch_cannot_widen_its_own_envelope() {
    let mut db = Db::new();
    db.seed();
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();

    let mut a = db.session();
    let branch = db.begin("greedy", &mut a);

    let wider = CapabilityEnvelope::new(Verb::ALL, 1_000)
        .allow(
            table_id("inventory").0,
            vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
        )
        .allow(
            table_id("payroll").0,
            vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
        );
    let err = db.runtime.restrict_branch(branch, wider).unwrap_err().to_string();
    assert!(err.contains("may only be narrowed"), "got {err}");

    // And the write it wanted is still refused.
    let err = db.refused("UPDATE payroll SET salary = 0 WHERE id = 1;", &mut a);
    assert!(err.contains("may not write table `payroll`"), "got {err}");

    // Anti-vacuity: narrowing IS accepted, so `restrict_branch` is not simply refusing everything.
    let narrower = CapabilityEnvelope::new(Verb::UPDATE, 5).allow(
        table_id("inventory").0,
        vec![ColumnCapability::floored(QTY, 0)],
    );
    db.runtime.restrict_branch(branch, narrower).expect("a narrowing must be accepted");
    let err = db.refused("DELETE FROM inventory WHERE id = 1;", &mut a);
    assert!(err.contains("may not DELETE"), "the narrowing did not take effect: {err}");
}


// ---- stated boundaries -----------------------------------------------------------------------

/// **Scope boundary, asserted rather than implied.** The envelope governs AGENT-SESSION writes,
/// because `stage_all` is the funnel those pass through and nothing else does. A plain `UPDATE`
/// outside any session goes straight through the executor and is not governed.
///
/// This is the same boundary escrow states for itself in
/// `escrow_governs_agent_writes_only_and_a_direct_write_is_not_charged`, and it is left as a
/// boundary for the same reason: the envelope is branch-scoped and a direct write has no branch,
/// so closing it needs someone to say whether the operator gets an implicit unlimited envelope,
/// trunk's, or a refusal. Inventing one of those quietly would be worse than saying it is open.
/// What is NOT acceptable is the claim "a session cannot write outside its envelope" without this
/// qualifier.
#[test]
fn the_envelope_governs_agent_session_writes_only() {
    let mut db = Db::new();
    db.seed();
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();

    let mut s = db.session(); // no BEGIN AGENT SESSION
    db.ok("UPDATE payroll SET salary = -1 WHERE id = 1;", &mut s);
    assert_eq!(
        db.salary(1),
        -1,
        "the ungoverned direct write must actually have landed, or this test says nothing"
    );

    let mut a = db.session();
    db.begin("scoped", &mut a);
    let err = db.refused("UPDATE payroll SET salary = -2 WHERE id = 1;", &mut a);
    assert!(
        err.contains("may not write table `payroll`"),
        "if a direct write is now governed too, this boundary has moved and the module docs must \
         move with it: {err}"
    );
}

/// **The most important gap in this feature, asserted so it is visible, so closing it trips a
/// test, and so nobody reads the envelope as covering more than it does.** Not one verb that
/// mutates schema is governed by the envelope. This test names each of them and drives each one
/// either against a table the envelope refuses by name (`payroll`, `payroll_notes`) or — for
/// `CREATE TABLE`, where there is no prior table to forbid — against a table it never granted.
///
/// # What is governed, and what is not
///
/// **Governed:** the after-image of every row an `INSERT` / `UPDATE` / `DELETE` writes on the
/// session's branch. Those three funnel into `AgentRuntime::stage_all`, where the envelope is read
/// from the branch's own durable record, and `AgentRuntime::write` accepts no other verb.
///
/// **Routed, but not governed: `SELECT`.** It is the fourth statement diverted onto
/// `run_in_session` — `src/execution/executor.rs:57-61` matches exactly those four — but
/// `run_in_session` splits them again (`src/agent_sql/dispatch.rs:186-192`): `SELECT` takes
/// `AgentRuntime::select`, which never stages and never reads the envelope. There is no read verb
/// for it to check against; `Verb` is `Insert | Update | Delete` (`src/branch/record.rs:402-406`).
/// **A governed branch may read every row of a table it may not write.** That is the envelope's
/// design and not a defect — it is a write allowlist — but it has to be said here, because the rest
/// of this comment is about things the envelope cannot see and a reader must not come away
/// believing reads are among the things it can.
///
/// **Not governed, tier one — the agent verbs.** `is_agent_stmt` diverts `MERGE`, `DIFF`,
/// `ABANDON`, `REVERT MERGE`, `SIMULATE`, `BEGIN AGENT SESSION` and any `SELECT ... AS OF` at
/// `src/execution/executor.rs:52-54`, which is **above** the routing described above, so these
/// never reach either the DML divert or the `match`. Two of them rewrite the rows of a forbidden
/// table: `merge_and_revert_rewrite_a_forbidden_tables_rows_and_this_is_a_known_gap`. This tier is
/// named here because the first version of this comment walked one `match` and concluded it had
/// enumerated everything.
///
/// **Not governed, tier two — the DDL that reaches the `match`.** `ANALYZE`, `CREATE INDEX`,
/// `CREATE FULLTEXT INDEX`, `CREATE TABLE`, `DROP TABLE`: each falls through to the **shared
/// catalog**, with no branch and no `MERGE`, so a governed agent that may not write one row of
/// `payroll` can still index it, analyse it, and drop it, and every other connection sees the
/// result immediately. That is what this test demonstrates.
///
/// # This pin used to be a strict subset of the hole it claimed to name
///
/// It was `ddl_inside_a_session_bypasses_the_envelope_and_this_is_a_known_gap`, and it named four
/// verbs in prose while demonstrating exactly one of them. Both halves of that were a problem:
///
/// - **Three of the four named verbs were never exercised.** A verb that is only named is a verb
///   nobody has checked. `ANALYZE`, `CREATE INDEX` and `CREATE TABLE` are each run here against a
///   table the envelope forbids, and each one's effect is read back out of the catalog rather than
///   inferred from an `Ok` — a bypass that returns `Ok` and writes nothing is not the same defect
///   and must not be allowed to stand in for this one.
/// - **`CREATE FULLTEXT INDEX` (B8) is a fifth verb, added after the pin was written**, on the same
///   fall-through (`src/execution/executor.rs:96`). It opens a B+tree, scans the *entire heap* of
///   the forbidden table and posts every token of the indexed column into it
///   (`src/catalog/catalog.rs:174-188`) — the same shape as `CREATE INDEX` above
///   (`src/catalog/catalog.rs:115-124`), and like it a change to shared **structure**.
///
///   An earlier draft of this comment called it a content exposure, and that was wrong in the
///   direction this whole commit exists to correct: the envelope has no read dimension at all, so
///   this branch could already `SELECT` every row of `payroll_notes` before any index existed, and
///   the governed session cannot even use `SEARCH`. Nothing became readable that was not readable
///   before. What makes this the verb that forced the pin to widen is only that it **arrived after
///   the pin was written**: a pin that enumerates verbs by name and is not widened when a verb is
///   added decays from a warning into a false reassurance, silently, while still passing green.
///
/// Adjacent, and deliberately not pinned here: `BEGIN` / `COMMIT` / `ROLLBACK` are also admitted
/// inside an agent session (`src/execution/executor.rs:63-83`), opening a shared WAL transaction,
/// while `BEGIN AGENT SESSION` refuses the mirror case. No row can land that way — I/U/D still
/// divert at `:57` — and no table the envelope forbids is reached, so it is not this test's
/// business. It is named so the list above is not read as "everything else is refused". `SEARCH`
/// and `EXPLAIN` genuinely are refused or read-only inside a session.
///
/// `ALTER TABLE` is a sixth case and a structurally different one — it reaches *branch* state
/// rather than the shared catalog — so it is pinned separately, by
/// `the_envelope_is_enforced_at_one_funnel_and_branch_scoped_alter_table_arrives_through_another`.
///
/// # Why this is pinned rather than closed
///
/// Because closing it is a design decision that is recorded and owned elsewhere, not an oversight
/// left lying here — `LEDGER-INTEGRATION.md` row I15 and `INTEGRATION.md`, both in the
/// `artie-research` repository and not in this one, so do not go looking for them here. The fix is
/// in the executor's statement routing, and it needs an answer to what
/// DDL on a branch *means* — a branch-scoped `CREATE TABLE` has to say what a sibling sees and what
/// `MERGE` does with it. B11 has already built one answer, for `ALTER` alone. **Anyone reading the
/// envelope as "a session cannot touch what it was not granted" must read this test first.**
#[test]
fn no_ddl_verb_is_governed_by_the_envelope_and_this_is_a_known_gap() {
    let mut db = Db::new();
    db.seed();

    // A second forbidden table, carrying a VARCHAR column so the full-text verb has something to
    // tokenize: `create_fulltext_index` refuses a non-VARCHAR column (`src/catalog/catalog.rs:164`)
    // and `payroll` is all integers. Built before any envelope exists and outside any session,
    // which is the ungoverned direct-write path `the_envelope_governs_agent_session_writes_only`
    // already documents.
    {
        let mut s = db.session();
        db.ok("CREATE TABLE payroll_notes (id INTEGER NOT NULL, note VARCHAR(64));", &mut s);
        db.ok("INSERT INTO payroll_notes VALUES (1, 'severance clause');", &mut s);
    }
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();

    let mut a = db.session();
    db.begin("ddl", &mut a);

    // Anti-vacuity, and it has to cover BOTH forbidden tables: without this the rest of the test
    // could be describing a branch that was never governed at all.
    for sql in [
        "UPDATE payroll SET salary = 0 WHERE id = 1;",
        "UPDATE payroll_notes SET note = 'redacted' WHERE id = 1;",
    ] {
        let err = db.refused(sql, &mut a);
        assert!(err.contains("may not write table"), "got {err}");
    }

    // ---- 1. ANALYZE reads every row of a table the branch may not write ------------------------
    //
    // Asserted on `catalog.stats` and NOT across a reopen, deliberately: `analyze` never calls
    // `persist` (`src/catalog/catalog.rs:368-398`), so its effect is shared in-process state rather
    // than durable. It is also the only one of these five arms with no `session.current` guard, so
    // it is the one verb here that also runs inside an open transaction.
    assert!(
        !db.catalog.stats.contains_key("payroll"),
        "`payroll` already carries stats, so the ANALYZE below would prove nothing"
    );
    db.ok("ANALYZE payroll;", &mut a);
    assert_eq!(
        db.catalog.stats.get("payroll").map(|s| s.row_count),
        Some(1),
        "ANALYZE returned Ok without actually scanning the forbidden table, so what this test \
         measured is not the bypass it claims to measure"
    );

    // ---- 2. CREATE INDEX allocates a shared structure over a forbidden table ------------------
    assert!(
        db.catalog.tables["payroll"].indexes.is_empty(),
        "`payroll` is already indexed, so the CREATE INDEX below would prove nothing"
    );
    db.ok("CREATE INDEX ix_salary ON payroll (salary);", &mut a);
    assert!(
        db.catalog.tables["payroll"].indexes.iter().any(|i| i.column_name == "salary"),
        "CREATE INDEX returned Ok without landing an index on the forbidden table"
    );

    // ---- 3. CREATE FULLTEXT INDEX (B8) scans a forbidden table's heap into a shared index -----
    //
    // The verb B3's pin could not have named, because B8 added it afterwards. This is the reason
    // the pin had to be widened rather than reworded: the hole grew, and the test that was
    // supposed to be the alarm did not notice.
    let mut before = db.session();
    assert!(
        db.exec("SEARCH payroll_notes (note) FOR 'severance';", &mut before).is_err(),
        "a full-text index already exists on the forbidden table; the assertion below is then \
         about a pre-existing index rather than one this session created"
    );
    db.ok("CREATE FULLTEXT INDEX ix_note ON payroll_notes (note);", &mut a);
    assert!(
        db.catalog.tables["payroll_notes"].fulltext_indexes.iter().any(|i| i.column_name == "note"),
        "CREATE FULLTEXT INDEX returned Ok without landing an index on the forbidden table"
    );
    // And the postings are real, not an empty tree: the DDL genuinely scanned the heap of a table
    // this branch may not write. Not an access escalation — see the doc comment — a shared
    // structure the branch was never granted authority over. Read from a plain session because `SEARCH` is
    // refused inside an agent session (`src/execution/executor.rs:107-120`).
    let mut plain = db.session();
    match db.ok("SEARCH payroll_notes (note) FOR 'severance';", &mut plain) {
        Outcome::Rows(rows) => assert_eq!(
            rows.len(),
            1,
            "the index the governed session built over a forbidden table returned nothing, so \
             CREATE FULLTEXT INDEX wrote a catalog entry without scanning the heap, and this \
             measured the catalog write rather than the heap scan it claims to. It does not \
             measure access: the envelope has no read dimension, and this branch could SELECT \
             these rows before the index existed"
        ),
        _ => panic!("SEARCH answered with something other than rows"),
    }

    // ---- 4. CREATE TABLE puts a table nobody granted into the SHARED catalog -------------------
    //
    // Not on the branch, and not awaiting `MERGE`: a different connection can read it at once,
    // and abandoning this agent's branch would not take it away.
    db.ok("CREATE TABLE contraband (id INTEGER NOT NULL, note VARCHAR(32));", &mut a);
    let mut reader = db.session();
    match db.exec("SELECT id FROM contraband;", &mut reader) {
        Ok(Outcome::Rows(rows)) => assert!(rows.is_empty(), "a fresh table returned rows"),
        Ok(_) => panic!("SELECT on the new table answered with something other than rows"),
        Err(e) => panic!(
            "the table a governed agent created is not readable from a plain session, so it did \
             not reach the shared catalog and this is a different finding from the one described: \
             {e}"
        ),
    }

    // ---- 5. DROP TABLE destroys a table and its rows ------------------------------------------
    //
    // Last, because it takes `payroll` away from the two arms above that use it. This is the verb
    // B3's pin
    // did demonstrate, and it is still the sharpest statement of the gap: the branch may not write
    // one row of `payroll`, and it just deleted all of them.
    assert_eq!(db.salary(1), 1000, "payroll must hold a row for the drop to destroy");
    db.ok("DROP TABLE payroll;", &mut a);
    assert!(
        db.exec("SELECT id FROM payroll;", &mut a).is_err(),
        "if DDL is now governed by the envelope, this gap has been closed and the module docs, \
         the summary and this test must say so"
    );
    // Visible outside the session, so it was the shared catalog and not the branch that changed.
    let mut outside = db.session();
    assert!(
        db.exec("SELECT id FROM payroll;", &mut outside).is_err(),
        "the drop was somehow scoped to the branch, which would mean DDL now has branch semantics \
         and this whole test needs rewriting"
    );
}

/// **The gap is not confined to schema: two agent verbs rewrite the ROWS of a forbidden table.**
/// `REVERT MERGE ... CASCADE` and `MERGE BRANCH <other>` both return `Ok` on a branch whose
/// envelope allows only `inventory`, both change `payroll`, and both charge nothing.
///
/// # Why the sibling test's enumeration could not see this
///
/// `executor::run` has **two** diversions before the `match`, not one, and the sibling pin only
/// described the second. `is_agent_stmt` (`src/execution/executor.rs:52-54`,
/// `src/agent_sql/dispatch.rs:72-83`) diverts `MERGE`, `DIFF`, `ABANDON`, `REVERT MERGE`,
/// `SIMULATE`, `BEGIN AGENT SESSION` and any `SELECT ... AS OF` **one tier above** the
/// `matches!(Select | Insert | Update | Delete)` at `:57-61`. So those verbs never reach the `match`
/// at all, and "everything that is not one of those four falls through to the shared catalog" was
/// never true of them. A pin that enumerates by walking one `match` will miss a whole tier.
///
/// # Why this is worse than the DDL gap, not another instance of it
///
/// Every verb in the sibling test changes shared *structure* — a table, an index, a statistic. These
/// two change shared *content*: rows in a table the operator explicitly refused this branch. And
/// the envelope's defence against exactly this is what makes it defensible: B3's design rests on
/// "everything that publishes went through `stage_all` first, so the publish path needs no check of
/// its own". That holds for `MERGE;` — a branch publishing what it staged itself. It does not hold
/// for either verb here, because neither one is publishing this branch's own staged rows:
///
/// - `REVERT MERGE` replays a *previous* merge's writes backwards through
///   `PendingWrite::apply` (`AgentRuntime::revert_merge` → `undo_txn`), so the rows it writes were
///   never staged by this branch at all.
/// - `MERGE BRANCH <name>` takes the branch from the statement, not from the session
///   (`src/agent_sql/dispatch.rs:118`, `BoundAgentStmt::Merge { branch }`), so a governed session
///   can publish a *different* agent's private workspace. The other branch's writes were checked
///   against the *other* branch's envelope — which here is none — and this session's envelope is
///   never consulted.
///
/// Measured, both of them, and `row_writes` stays 0 in each case: the envelope did not refuse them,
/// it never saw them.
///
/// Pinned and not closed, for the same reason as its sibling: this is the design decision about
/// what the envelope governs, recorded in the integration ledger (`LEDGER-INTEGRATION.md` row I15
/// and `INTEGRATION.md` in the `artie-research` repository, not in this one). Closing it means
/// deciding whether a capability is authority over *rows* or authority over *statements* — and
/// `REVERT MERGE` makes that concrete, because the rows it writes are somebody else's.
#[test]
fn merge_and_revert_rewrite_a_forbidden_tables_rows_and_this_is_a_known_gap() {
    // ---- REVERT MERGE: undo somebody else's published change to a forbidden table -------------
    {
        let mut db = Db::new();
        db.seed();

        // An UNGOVERNED session publishes a payroll change, so there is a merge to revert.
        let mut w = db.session();
        db.begin("writer", &mut w);
        db.ok("UPDATE payroll SET salary = 4242 WHERE id = 1;", &mut w);
        db.ok("MERGE;", &mut w);
        assert_eq!(db.salary(1), 4242, "the setup merge did not land, so there is nothing to revert");

        db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();
        let mut a = db.session();
        let branch = db.begin("reverter", &mut a);

        // Anti-vacuity: this branch cannot write one row of payroll by any governed route.
        let err = db.refused("UPDATE payroll SET salary = 7 WHERE id = 1;", &mut a);
        assert!(err.contains("may not write table `payroll`"), "got {err}");

        // ...and it rewrites every row of it anyway.
        db.ok("REVERT MERGE m_1 CASCADE;", &mut a);
        assert_eq!(
            db.salary(1),
            1000,
            "if REVERT MERGE is now governed by the envelope, this half of the gap has closed and \
             the module docs, the summary and this test must say so"
        );
        assert_eq!(
            db.runtime.envelope_of(branch).unwrap().unwrap().row_writes(),
            0,
            "the revert was CHARGED, which would mean it passes through `stage_all` after all and \
             this test is describing the wrong mechanism"
        );
    }

    // ---- MERGE BRANCH: publish a DIFFERENT agent's private writes ------------------------------
    {
        let mut db = Db::new();
        db.seed();

        // An ungoverned agent stages a payroll change on its own branch and does not publish it.
        let mut w = db.session();
        db.begin("foreign", &mut w);
        db.ok("UPDATE payroll SET salary = 1 WHERE id = 1;", &mut w);
        let foreign = w.agent.as_ref().unwrap().branch_name.clone();
        assert_eq!(db.salary(1), 1000, "the foreign branch published early; it must still be private");

        db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();
        let mut a = db.session();
        let branch = db.begin("merger", &mut a);
        let err = db.refused("UPDATE payroll SET salary = 7 WHERE id = 1;", &mut a);
        assert!(err.contains("may not write table `payroll`"), "got {err}");

        // The governed session publishes the other branch's forbidden write.
        db.ok(&format!("MERGE BRANCH {foreign};"), &mut a);
        assert_eq!(
            db.salary(1),
            1,
            "if a governed session can no longer publish a foreign branch, this half of the gap \
             has closed and the docs must say so"
        );
        assert_eq!(
            db.runtime.envelope_of(branch).unwrap().unwrap().row_writes(),
            0,
            "the foreign publish was charged to this branch's budget, which would mean the \
             envelope saw it"
        );
    }
}

/// **The ungoverned DDL is not only an escape from the envelope — it is a way to change what the
/// envelope MEANS.** A branch granted `inventory` can drop `inventory` and build a different table
/// under that name, and its grant silently follows the name onto the new columns.
///
/// # The mechanism, which is two design choices meeting
///
/// `table_id` is FNV-1a over the table *name* (`src/agent_sql/runtime.rs:87-94`) — the catalog mints
/// no ids, and hashing the name is what makes an id stable across processes. `ColumnCapability` keys
/// a column by its *index* (`src/branch/record.rs:546-547`). Neither is wrong on its own. Together
/// they mean the pair (name, column index) is the whole of a grant's identity, and `DROP TABLE` +
/// `CREATE TABLE` — both ungoverned, per
/// `no_ddl_verb_is_governed_by_the_envelope_and_this_is_a_known_gap` — let a governed branch choose
/// what that pair points at.
///
/// So this is worse than "DDL is not governed". A reader could accept that gap and still believe an
/// envelope means what it said when it was installed. It does not: the branch below is granted
/// `inventory` columns 0 and 1, writes `qty` under that grant, and then makes the same grant admit a
/// column called `secret` in a table with entirely different contents — and the envelope charges the
/// write to its budget, because from `stage_all`'s side nothing happened at all.
///
/// Pinned, not fixed. The fix is not in the envelope: it is either governing DDL (the same design
/// decision the sibling test records) or giving tables an identity that a name cannot forge.
#[test]
fn dropping_and_recreating_a_granted_table_repoints_the_grant_at_different_columns() {
    let mut db = Db::new();
    db.seed();
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();

    let mut a = db.session();
    let branch = db.begin("confuser", &mut a);

    // The key the operator's grant actually holds, read before anything is dropped.
    let granted_key = db
        .runtime
        .envelope_of(branch)
        .unwrap()
        .unwrap()
        .table(table_id("inventory").0)
        .expect("the fixture must grant `inventory`")
        .table;

    // The grant is live, and it means `inventory.qty` — column 1 of the table seeded above.
    db.ok("UPDATE inventory SET qty = 5 WHERE id = 1;", &mut a);
    assert_eq!(db.runtime.envelope_of(branch).unwrap().unwrap().row_writes(), 1);
    let err = db.refused("UPDATE payroll SET salary = 0 WHERE id = 1;", &mut a);
    assert!(err.contains("may not write table `payroll`"), "got {err}");

    // Two ungoverned verbs, and the grant now names something else entirely.
    db.ok("DROP TABLE inventory;", &mut a);
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, secret INTEGER);", &mut a);
    assert_eq!(
        db.catalog.tables["inventory"].schema.columns[1].name, "secret",
        "the recreated table did not take the shape this test needs it to take"
    );
    // The grant's key, captured BEFORE the substitution and compared against the table that
    // exists after it. An earlier version of this test asserted
    // `table_id("inventory") == table_id("inventory")` here, which is `f(x) == f(x)` over a pure
    // function: it could not fail under any change to `table_id`, the catalog or the envelope, and
    // it was captioned as the evidence for the mechanism the test is named after. Found by a
    // fresh-context review. What carries the claim is that the LIVE envelope still resolves the
    // substituted table through the key it was granted.
    assert_eq!(granted_key, table_id("inventory").0, "the substituted table took a different id");
    assert!(
        db.runtime
            .envelope_of(branch)
            .unwrap()
            .unwrap()
            .table(granted_key)
            .is_some(),
        "the grant no longer resolves the recreated table, so table identity is not the name any \
         more and this whole test needs rewriting"
    );

    // And the envelope admits a write to it — through `stage_all`, against the same allowlist
    // entry, charged to the same budget. Nothing here can tell that `inventory` is not the table
    // the operator granted.
    // The branch's workspace still holds the row staged by the UPDATE above, and the substituted
    // table's schema calls column 1 `secret`, so the value written as `qty` reads back under the
    // new name: the branch reads its own old data through a schema it swapped underneath itself.
    // Derived from the mechanism, not read off a run — the workspace is keyed by row and `DROP
    // TABLE` touches the shared catalog, not the workspace. Measured `[[Integer(1), Integer(5)]]`.
    match db.ok("SELECT id, secret FROM inventory;", &mut a) {
        Outcome::Rows(rows) => assert_eq!(
            rows,
            vec![vec![Value::Integer(1), Value::Integer(5)]],
            "the staged row did not survive the substitution, so the confusion this test describes \
             takes a different shape than the comment claims and the comment must be rewritten"
        ),
        _ => panic!("the branch read answered with something other than rows"),
    }
    // A fresh key: the staged row above still occupies id 1 on this branch.
    db.ok("INSERT INTO inventory VALUES (7, 42);", &mut a);
    assert_eq!(
        db.runtime.envelope_of(branch).unwrap().unwrap().row_writes(),
        2,
        "the write to the substituted table was not charged, so it did not go through the funnel \
         and this test is measuring something other than what it claims"
    );

    // Anti-vacuity for the claim that the ENVELOPE is what admitted it, and it has to be at the
    // COLUMN dimension: a table-level refusal on some other table would only show default-deny.
    // An earlier version of this test did exactly that — it created `payroll_2` and asserted "may
    // not write table", which is the table gate firing before any column is examined — while its
    // comment claimed a column index had been tested. Found by a fresh-context review.
    //
    // So: substitute a THIRD shape with a column the grant never covered, and show index 2 refused
    // while the granted indices still admit. The column allowlist is being consulted; it is simply
    // consulting it against a table nobody granted.
    db.ok("DROP TABLE inventory;", &mut a);
    db.ok(
        "CREATE TABLE inventory (id INTEGER NOT NULL, secret INTEGER, ungranted INTEGER);",
        &mut a,
    );
    let err = db.refused("INSERT INTO inventory VALUES (9, 1, 1);", &mut a);
    assert!(
        err.contains("column 2") || err.contains("not on the allowlist"),
        "an INSERT writing a column index the grant never covered was admitted, so the column \
         dimension is not being consulted against the substituted table and this test proves \
         something weaker than it claims: {err}"
    );
}

/// **The substitution is not merely confusion about what a grant points at — it is a WIDENING, the
/// one thing the envelope's own design exists to make impossible.** `BranchRecord::restrict` refuses
/// any envelope that grants more than the one it replaces, because "an envelope its holder can widen
/// is a suggestion" — and `a_branch_cannot_widen_its_own_envelope` asserts that. `DROP TABLE` +
/// `CREATE TABLE` walks around it without touching the envelope at all: the envelope bytes never
/// change, so nothing is ever offered to `restrict`; what changes is the table those bytes describe.
///
/// Two consequences, and both are the *value-level* guards rather than the table-level ones, which
/// is why the sibling test could not show them — its fixture grants `Verb::ALL` over every column
/// with no floor, the one envelope under which neither can appear.
///
/// Found by a fresh-context review of the sibling test, which observed that a fixture chosen to make
/// the mechanism visible had also been chosen to make its sharpest consequences invisible.
#[test]
fn the_substitution_strips_a_column_floor_and_unlocks_a_refused_delete() {
    // ---- a floor is keyed by column INDEX, so reordering the columns detaches it ---------------
    {
        let mut db = Db::new();
        db.seed();
        let floored = CapabilityEnvelope::new(Verb::ALL, 1_000).allow(
            table_id("inventory").0,
            vec![ColumnCapability::open(ID), ColumnCapability::floored(QTY, 0)],
        );
        db.runtime.restrict_branch(BranchId::TRUNK, floored).unwrap();

        let mut a = db.session();
        db.begin("floor_stripper", &mut a);

        // The floor is live: this is the envelope's only value-level guard.
        let err = db.refused("UPDATE inventory SET qty = -999 WHERE id = 1;", &mut a);
        assert!(err.contains("the floor is 0"), "the floor was not the reason: {err}");

        // Same name, same column names, INDICES SWAPPED. Nothing was offered to `restrict`.
        db.ok("DROP TABLE inventory;", &mut a);
        db.ok("CREATE TABLE inventory (qty INTEGER NOT NULL, id INTEGER);", &mut a);
        db.ok("INSERT INTO inventory VALUES (-999, 1);", &mut a);

        // Read on the BRANCH: the insert is staged there and is invisible to a plain session
        // until MERGE, which is the whole point of an agent session.
        match db.ok("SELECT qty FROM inventory;", &mut a) {
            Outcome::Rows(rows) => assert_eq!(
                rows,
                vec![vec![Value::Integer(-999)]],
                "if the value below the floor did not land, the floor survived the substitution \\
                 and this half of the finding is wrong"
            ),
            _ => panic!("expected rows"),
        }
    }

    // ---- a verb refused only because its column set was withheld becomes permitted -------------
    {
        let mut db = Db::new();
        db.seed();
        // `Verb::DELETE` granted, but only column 0. A DELETE authors EVERY cell of the row — the
        // rule `changed_columns` documents — so granting the verb without the columns refuses every
        // delete. An operator can rely on that: it is how you grant INSERT/UPDATE on one column
        // without granting the power to remove rows.
        let narrow = CapabilityEnvelope::new(Verb::ALL, 1_000)
            .allow(table_id("inventory").0, vec![ColumnCapability::open(ID)]);
        db.runtime.restrict_branch(BranchId::TRUNK, narrow).unwrap();

        let mut a = db.session();
        db.begin("delete_unlocker", &mut a);
        let err = db.refused("DELETE FROM inventory WHERE id = 1;", &mut a);
        assert!(
            err.contains("column 1"),
            "the delete was refused for some other reason, so the standing refusal this test is \\
             about does not exist: {err}"
        );

        // Rebuild the table with ONLY the granted column. The row now has one cell, so the delete
        // authors only what was granted.
        db.ok("DROP TABLE inventory;", &mut a);
        db.ok("CREATE TABLE inventory (id INTEGER NOT NULL);", &mut a);
        db.ok("INSERT INTO inventory VALUES (1);", &mut a);
        db.ok("DELETE FROM inventory WHERE id = 1;", &mut a);
        assert_eq!(
            db.count("inventory"),
            0,
            "the delete reported Ok without removing the row, so this measured the refusal \\
             disappearing rather than the delete succeeding"
        );
    }
}

/// **The envelope is enforced at exactly one funnel, and B11's branch-scoped `ALTER TABLE` reaches
/// branch state through a second one — so the envelope structurally cannot see it.** This test
/// pins that premise, and fails the moment the premise stops holding.
///
/// # Why this one is a structural check and not a SQL statement
///
/// Every other case in this file drives real SQL. This one cannot: `ALTER TABLE` does not exist in
/// this tree — there is no `Stmt::Alter`, and the scanner has no `ALTER` keyword. It arrives with
/// **B11**, unmerged as of this commit, as `src/agent_sql/dispatch.rs::run_agent_alter` calling
/// `AgentRuntime::stage_schema_edit`, which pushes onto `state.workspaces[branch].schema_edits` and
/// publishes at `MERGE`. That function performs no envelope read and no charge.
///
/// # Measured, not inferred
///
/// B11 was merged onto this commit's parent in a throwaway worktree — branch
/// `I15-b11-alter-probe`, merge `622a17b`, probe `7581498` — and the question was put to the merged
/// tree rather than to B11's report. **Two probe tests, not one run**, and they are separated here
/// because an earlier version of this list ran them together and the numbers do not belong to one
/// session.
///
/// `i15_does_the_envelope_govern_an_alter_on_a_table_it_never_granted`, on a session whose envelope
/// allows only `inventory`:
///
/// - `UPDATE payroll SET salary = 0 WHERE id = 1;` → refused, "not on the allowlist"
/// - `ALTER TABLE payroll ADD COLUMN note VARCHAR(16);` → **`Ok`**
/// - `pending_schema_edits(branch)` → `[("payroll", AddColumn(note VARCHAR(16)))]`
/// - the envelope afterwards → `row_writes: 0`. Nothing was charged, because nothing was seen.
///   This test never merges.
///
/// `i15_does_the_ungoverned_alter_reach_the_shared_table_at_merge`, a separate session that first
/// makes one *allowed* write (`UPDATE inventory SET qty = 7`) so the merge has a row to carry — so
/// `row_writes` is 1 there, not 0:
///
/// - `MERGE` → `Ok`, and `payroll` then carries `note` in the **shared** catalog. The assertion is
///   on the post-merge `SELECT note FROM payroll`; the merge's own `Ok` is printed, not asserted.
///   The same SELECT fails before the merge, so the check is proven to fire rather than assumed to.
///
/// That merge is not part of this commit and is not proposed by it; it existed to answer the
/// question. One caveat, stated because it changes how much the result is worth: the staging half
/// above is B11's code verbatim, but the merge-publish half rests on the probe's own conflict
/// resolution of B11 against B6's split `evaluate_merge` / `publish_evaluation`.
///
/// Writing a test that *executes* that verb would mean merging B11 here, which is a separate,
/// ordered integration step (`src/agent_sql/runtime.rs` goes B4 → B6 → B11 → B9) and is not this
/// commit's business. So what is pinned instead is the premise B3's design rests on and B11
/// falsifies: **`stage_all` is the only way into a branch's write state, which is what makes one
/// enforcement point sufficient.**
///
/// # It is a count, not a list of names — and the first draft of that count did not work
///
/// Asserting "`run_agent_alter` is absent" alone would be a denylist, and a denylist only catches
/// the one bypass somebody already thought of. So the load-bearing assertion is that the funnel is
/// *singular*: exactly one site reads the envelope, exactly one site charges the budget, exactly
/// one site mutates a workspace's staged writes, and all three are inside `stage_all`.
///
/// **The counts run on whitespace-stripped text, because the raw token is a fact about rustfmt and
/// not about the code.** rustfmt breaks a chain that overruns the line width onto one element per
/// line, and B11's `stage_schema_edit` is one of those: it writes `let ws = state` / `.workspaces`
/// / `.get_mut(&branch.id)`, so the needle `workspaces.get_mut(` appears in that function **zero**
/// times. Splice the function alone onto this tree and the raw count is 1 — green — while the dense
/// count is 2.
///
/// **An earlier version of this paragraph overstated that, and the correction belongs here rather
/// than only in a commit message.** It said the raw check "was silent on the exact merge it was
/// written to catch". It was not. Counted on the real merge —
/// `git show I15-b11-alter-probe:src/agent_sql/runtime.rs` — the raw count is **2**, not 1, because
/// `stage_schema_edit` calls `note_base_shape` on every path and that function's chain does fit on
/// one line. So the raw check would have fired on B11, for a reason adjacent to the real one. What
/// is true, and is the reason to count dense, is narrower: the raw needle cannot see the staging
/// site itself, so it would have been reporting a formatting coincidence rather than the funnel —
/// and a needle whose behaviour depends on line width is not a guard. Found by a fresh-context
/// review of this file twice: once for the formatter, once for the overstatement about it.
///
/// Four blind spots remain, stated rather than left to be found.
///
/// 1. It reads text, so it cannot tell whether a second funnel *consults* the envelope. It refuses
///    both cases and says so, because guessing is worse.
/// 2. It reads only `src/agent_sql/runtime.rs`, so a funnel built in another module is invisible.
///    The two field allowlists below are what make the in-`State` and in-`Workspace` versions
///    visible.
/// 3. **The needles are call shapes, not properties.** `self.branches.envelope_of(` is not the only
///    way to read an envelope — `AgentRuntime::envelope_of` itself does it as
///    `self.branches.get(branch)?.envelope`, outside `stage_all`, and a second funnel spelled that
///    way passes the count. Likewise `workspaces.get_mut(` is one way to reach a workspace:
///    `state.workspaces.insert(...)` populates one at fork, and `entry()`, `values_mut()`,
///    `iter_mut()` or a helper handed `&mut Workspace` would all pass. So "any second funnel trips
///    this" — which an earlier version of this comment claimed — is false. What is true is
///    narrower: the three shapes that exist today are pinned to one function, and the field lists
///    catch the state a new funnel would have to add.
/// 4. It says nothing about the publish path. `MERGE` and `REVERT MERGE` write rows without
///    consulting the envelope at all, which is a different hole and has its own test.
///
/// This test fires on the merge that *creates* the hole rather than on the one that fixes it, which
/// is the only ordering that helps: by the time somebody is looking for why the envelope missed an
/// `ALTER`, the branch has already published it.
#[test]
fn the_envelope_is_enforced_at_one_funnel_and_branch_scoped_alter_table_arrives_through_another() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let runtime = std::fs::read_to_string(root.join("src/agent_sql/runtime.rs")).unwrap();
    let dispatch = std::fs::read_to_string(root.join("src/agent_sql/dispatch.rs")).unwrap();

    // Anti-vacuity for the instrument itself. A moved or renamed file would otherwise make every
    // assertion below pass against an empty string, and this test would report "one funnel" about
    // a file it never read — the exact failure mode a zero-result check is supposed to refuse.
    let funnel_at = runtime.find("fn stage_all(").expect(
        "`stage_all` is not in src/agent_sql/runtime.rs any more: either the funnel was renamed, \
         in which case update this test, or this test is reading the wrong file and has been \
         asserting nothing",
    );
    assert!(
        dispatch.contains("pub fn run_agent_stmt("),
        "src/agent_sql/dispatch.rs does not contain run_agent_stmt, so this test is not reading \
         the dispatcher it thinks it is"
    );

    // The span of `stage_all`, from its signature to the next item at the same indentation.
    let body = &runtime[funnel_at..];
    let end = body[1..]
        .find("\n    fn ")
        .into_iter()
        .chain(body[1..].find("\n    pub fn "))
        .min()
        .map(|i| i + 1)
        .unwrap_or(body.len());
    let stage_all = &body[..end];

    // Whitespace-stripped, because the raw token is a fact about rustfmt and not about the code.
    // See the doc comment: counted raw, `workspaces.get_mut(` appears ZERO times in B11's own
    // `stage_schema_edit`, and this test was green on the merge it exists to catch.
    let dense: String = runtime.chars().filter(|c| !c.is_whitespace()).collect();
    let dense_funnel: String = stage_all.chars().filter(|c| !c.is_whitespace()).collect();

    // Exactly one of each, and each one inside `stage_all`. Counted over the whole file so a
    // second call site anywhere in it trips this, wherever somebody puts it.
    for (needle, what) in [
        ("self.branches.envelope_of(", "reads the capability envelope"),
        ("self.branches.charge_row_writes(", "charges the row-write budget"),
        ("workspaces.get_mut(", "mutates a branch workspace's staged writes"),
    ] {
        assert_eq!(
            dense.matches(needle).count(),
            1,
            "a second site in src/agent_sql/runtime.rs {what}. The envelope is enforced at ONE \
             funnel, `stage_all`, and that is only sufficient while nothing else reaches branch \
             write state ({needle}). This check reads text, so it CANNOT tell whether the new \
             site consults the envelope. If it does not — B11's `stage_schema_edit` does not — \
             the envelope has a hole it cannot see, and a branch whose envelope forbids `payroll` \
             can ADD, RENAME or RETYPE a `payroll` column and publish it at MERGE. If it does, \
             the single-funnel premise this test pins is simply gone. Either way this test must \
             stop being a text check: replace it with one that drives the new verb against a \
             table the envelope forbids, and record which case it was in the integration ledger \
             (LEDGER-INTEGRATION.md / INTEGRATION.md live in the `artie-research` repository, not \
             in this one)."
        );
        assert!(
            dense_funnel.contains(needle),
            "`{needle}` has moved out of `stage_all`. Whatever now holds it is a second funnel, \
             and the envelope only governs the one."
        );
    }

    // **A funnel can stage BESIDE the workspace, or INSIDE it without going through the funnel.**
    // Two field lists are pinned, because the two shapes are different and the first version of
    // this guard covered only one of them — and covered it with the wrong claim. It said "a
    // schema-edit map is the live example, and B11 needs one"; B11 needs no `State` field at all.
    // It adds `schema_edits` and `base_shapes` to `struct Workspace`. Found by a fresh-context
    // review, which is also why `Workspace` is pinned here now.
    //
    // `State` covers the sibling-map shape: `escrow`, `quarantine_reasons` and `row_author` are
    // already per-branch maps, so a schema-edit map next to them is the established pattern.
    // `Workspace` covers B11's actual shape: per-branch state added to the workspace itself, which
    // a statement can then write without ever entering `stage_all`.
    let field_names = |decl: &str, what: &str| -> Vec<String> {
        let at = runtime
            .find(decl)
            .unwrap_or_else(|| panic!("`{decl}` is gone from src/agent_sql/runtime.rs; this test \
                 is reading the wrong file and has been asserting nothing"));
        let block = &runtime[at..at + runtime[at..].find("\n}\n").expect("unterminated struct")];
        let mut out = Vec::new();
        for line in block.lines().skip(1) {
            let line = line.trim();
            if line.is_empty() || line.starts_with("//") || line.starts_with("#[") {
                continue;
            }
            // Strip a visibility modifier before parsing. Without this, `pub schema_edits: T`
            // yielded "pub schema_edits", failed the identifier test, and was silently DROPPED —
            // so a `pub` field could be added and this allowlist stayed green. A guard that
            // cannot parse its own input must refuse, not fall through to allow.
            let line = line
                .strip_prefix("pub(crate) ")
                .or_else(|| line.strip_prefix("pub(super) "))
                .or_else(|| line.strip_prefix("pub "))
                .unwrap_or(line);
            let name = line.split(':').next().unwrap_or("");
            assert!(
                !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_'),
                "this test cannot parse a line of `{what}` as a field declaration: {line:?}. It \
                 refuses rather than skipping it, because a line it silently dropped is a field \
                 that never had to come here and declare itself."
            );
            out.push(name.to_string());
        }
        out
    };

    assert_eq!(
        field_names("struct State {", "State"),
        [
            "workspaces", "names", "runs", "next_txn", "next_merge", "apply_seq", "applied",
            "merges", "quarantine_reasons", "escrow", "row_author", "versions", "captures",
            "policy",
        ],
        "the fields of `AgentRuntime`'s `State` have changed. If a new one holds per-branch state \
         that a statement can write, it is a second funnel and the envelope does not govern it. \
         Add the field here only after deciding which it is."
    );
    assert_eq!(
        field_names("struct Workspace {", "Workspace"),
        [
            "name", "prov", "txn", "fork_seq", "fork_root", "rows", "base_rows", "tables", "frame",
        ],
        "the fields of `Workspace` have changed. This is B11's shape: it adds `schema_edits` and \
         `base_shapes` here, and `stage_schema_edit` writes them without passing through \
         `stage_all`. A new field here is branch state the envelope will not see unless the write \
         path that fills it is governed."
    );

    // Belt and braces on top of the counts: B11's two symbols, matched with `fn` adjacent to the
    // name so prose that merely mentions them does not trip this. Two limits, stated because the
    // matcher does not have the robustness the previous version of this comment claimed: after
    // whitespace stripping there is no code/comment boundary left, so a doc block quoting the
    // signature DOES trip it (fails safe); and a definition that grew a generic parameter —
    // `fn stage_schema_edit<T>(` — would NOT. The counts above are the load-bearing half.
    let dense_dispatch: String = dispatch.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(
        !dense_dispatch.contains("fnrun_agent_alter("),
        "B11's branch-scoped ALTER TABLE has landed in src/agent_sql/dispatch.rs. It reaches the \
         runtime without passing through `stage_all`, so the capability envelope cannot see it. \
         Measured on a throwaway merge of B11 (branch `I15-b11-alter-probe`): the ALTER returns \
         Ok, nothing is charged, and MERGE puts the column in the shared catalog. Widen \
         `no_ddl_verb_is_governed_by_the_envelope_and_this_is_a_known_gap` to demonstrate it with \
         real SQL, or close the gap — and record which in the integration ledger, which lives in \
         the `artie-research` repository and not in this one."
    );
    assert!(
        !dense.contains("fnstage_schema_edit("),
        "`AgentRuntime::stage_schema_edit` (B11) is a second funnel into branch state and performs \
         no envelope check. See the message above."
    );
}

/// **The envelope is granted at fork, so installing one does not reach sessions already open.**
///
/// A capability is what you were handed when you were created; changing it under a running holder
/// is *revocation*, which is a separate design decision — it has to say what happens to a
/// statement already in flight and whether a branch can be narrowed below what it has already
/// written. The existing answer for a misbehaving live agent is `quarantine`, which leaves the
/// branch readable and blocks its `MERGE`, and that is asserted here rather than assumed.
#[test]
fn installing_an_envelope_does_not_reach_a_session_that_is_already_open() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    let branch = db.begin("already_running", &mut a); // forked while trunk is ungoverned
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();

    assert_eq!(
        db.runtime.envelope_of(branch).unwrap(),
        None,
        "if a live session now inherits an envelope installed after its fork, revocation has been \
         built and this test should describe it"
    );
    db.ok("UPDATE payroll SET salary = 0 WHERE id = 1;", &mut a);

    // The operator's actual lever over a running agent: hold it, so nothing it wrote can publish.
    db.runtime.quarantine(branch, "writing outside its remit").unwrap();
    let err = db.refused("MERGE;", &mut a);
    assert!(err.contains("quarantined"), "got {err}");
    assert_eq!(db.salary(1), 1000, "a quarantined branch published anyway");

    // Anti-vacuity: a session forked AFTER the install is governed.
    let mut b = db.session();
    db.begin("forked_after", &mut b);
    let err = db.refused("UPDATE payroll SET salary = 0 WHERE id = 1;", &mut b);
    assert!(err.contains("may not write table `payroll`"), "got {err}");
}

/// Reading the branch record to find its envelope also applies that record's readability rule, so
/// a branch mid-reap can no longer write even though its workspace is still around. That is a
/// strengthening rather than an accident, so it gets a test.
#[test]
fn a_branch_being_reaped_cannot_write_even_with_its_workspace_intact() {
    use ferrodb::branch::types::BranchState;

    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    let branch = db.begin("doomed", &mut a);

    // Anti-vacuity first: while it is Live the write is admitted.
    db.ok("UPDATE inventory SET qty = 5 WHERE id = 1;", &mut a);

    let mut rec = db.runtime.branches().get(branch).unwrap();
    rec.state = BranchState::Reaping;
    db.runtime.branches().put(&rec).unwrap();

    let err = db.refused("UPDATE inventory SET qty = 6 WHERE id = 1;", &mut a);
    assert!(err.contains("being reaped"), "got {err}");
}

/// **A statement the escrow ledger refuses must not spend envelope budget.**
///
/// The breaking shape is the ORDER of two whole-statement checks at one funnel. The envelope
/// charge used to be written — and fsynced — before `EscrowLedger::check_all` ran, so a statement
/// escrow then refused permanently consumed row-writes it never used. That is worse than it
/// sounds: the escrow refusal's own text tells the client to claim more and retry, so an ordinary
/// claim-and-retry loop burned the entire envelope budget on statements that wrote zero rows.
///
/// Both checks now decide before anything is charged, which is the same rule each of them already
/// applies within itself.
#[test]
fn a_statement_refused_by_escrow_spends_no_envelope_budget() {
    use ferrodb::tel::ids::{ColId, RowId};

    let mut db = Db::new();
    db.seed();
    db.runtime.restrict_branch(BranchId::TRUNK, inventory_only()).unwrap();
    db.runtime.open_escrow("inventory", RowId(1), ColId(1), 20).unwrap();

    let mut a = db.session();
    let branch = db.begin("retrier", &mut a);
    db.runtime.claim_escrow(branch, "inventory", RowId(1), ColId(1), 1).unwrap();

    // Ten retries of a statement escrow refuses. The envelope's budget is 1000, so if each one
    // charged, this would still not exhaust it — the assertion is that NONE of them charged.
    for _ in 0..10 {
        let err = db.refused("UPDATE inventory SET qty = qty - 5 WHERE id = 1;", &mut a);
        assert!(err.contains("remaining escrow"), "refused for the wrong reason: {err}");
    }
    assert_eq!(
        db.runtime.envelope_of(branch).unwrap().unwrap().row_writes(),
        0,
        "escrow-refused statements spent envelope budget on rows they never wrote"
    );

    // Anti-vacuity: a write escrow DOES admit charges exactly one.
    db.ok("UPDATE inventory SET qty = qty - 1 WHERE id = 1;", &mut a);
    assert_eq!(db.runtime.envelope_of(branch).unwrap().unwrap().row_writes(), 1);
}
