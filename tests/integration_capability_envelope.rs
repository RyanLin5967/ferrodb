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
/// mutates schema is governed by the envelope. This test names each of them and reaches a table
/// the envelope forbids with every one.
///
/// # What is governed, and what is not
///
/// **Governed:** the after-image of every row a `SELECT` / `INSERT` / `UPDATE` / `DELETE` writes on
/// the session's branch. Those four, and only those four, are routed onto `run_in_session`
/// (`src/execution/executor.rs:56-60`), and every one of them funnels into
/// `AgentRuntime::stage_all`, where the envelope is read from the branch's own durable record.
///
/// **Not governed:** every other statement. Each falls through that `match` to the **shared
/// catalog**, with no branch and no `MERGE` — so a governed agent that may not write one row of
/// `payroll` can still index it, analyse it, and drop it, and every other connection sees the
/// result immediately.
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
///   fall-through (`src/execution/executor.rs:96`). It is the worst of the five to leave off a pin:
///   it opens a B+tree, scans the *entire heap* of the forbidden table and posts every token of the
///   indexed column into it (`src/catalog/catalog.rs:174-188`). So it does not merely mutate
///   schema — it makes the contents of a table the branch was never granted retrievable. A pin that
///   enumerates verbs by name and is not widened when a verb is added decays from a warning into a
///   false reassurance, silently, while still passing green.
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
/// left lying here: the fix is in the executor's statement routing, and it needs an answer to what
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

    // ---- 3. CREATE FULLTEXT INDEX (B8) makes a forbidden table's TEXT retrievable -------------
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
    // And the postings are real, not an empty tree: the whole heap of a table this branch may not
    // write is now searchable from any connection. Read from a plain session because `SEARCH` is
    // refused inside an agent session (`src/execution/executor.rs:107-120`).
    let mut plain = db.session();
    match db.ok("SEARCH payroll_notes (note) FOR 'severance';", &mut plain) {
        Outcome::Rows(rows) => assert_eq!(
            rows.len(),
            1,
            "the index the governed session built over a forbidden table returned nothing, so \
             this measured the schema change and not the content exposure"
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
    // Last, because it takes `payroll` away from the four checks above. This is the verb B3's pin
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
    assert_eq!(
        table_id("inventory"),
        table_id("inventory"),
        "table identity is the name, which is the whole mechanism"
    );

    // And the envelope admits a write to it — through `stage_all`, against the same allowlist
    // entry, charged to the same budget. Nothing here can tell that `inventory` is not the table
    // the operator granted.
    // PREDICTION, stated before it was run and then confirmed by running it
    // (`[[Integer(1), Integer(5)]]`): the branch's workspace still holds the row staged by the
    // UPDATE above, so the value written as `qty` reads back under the name `secret`. The branch
    // reads its own old data through the substituted table's schema.
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

    // Anti-vacuity for the claim that the ENVELOPE is what admitted it: a column index the grant
    // never covered is still refused on the substituted table, so the allowlist is being consulted
    // and is simply consulting the wrong table.
    db.ok("CREATE TABLE payroll_2 (id INTEGER NOT NULL, salary INTEGER);", &mut a);
    let err = db.refused("INSERT INTO payroll_2 VALUES (1, 1);", &mut a);
    assert!(
        err.contains("may not write table"),
        "a table created inside the session was writable, which would mean the allowlist is not \
         being consulted at all and this test proves something weaker: {err}"
    );
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
/// tree rather than to B11's report. On a session whose envelope allows only `inventory`:
///
/// - `UPDATE payroll SET salary = 0 WHERE id = 1;` → refused, "not on the allowlist"
/// - `ALTER TABLE payroll ADD COLUMN note VARCHAR(16);` → **`Ok`**
/// - `pending_schema_edits(branch)` → `[("payroll", AddColumn(note VARCHAR(16)))]`
/// - the envelope afterwards → `row_writes: 0`. Nothing was charged, because nothing was seen.
/// - `MERGE` → `Ok`, and `payroll` carries `note` in the **shared** catalog. The same
///   `SELECT note FROM payroll` fails before that merge and succeeds after it, so the check is
///   proven to fire rather than assumed to.
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
/// # It is a count, not a list of names
///
/// Asserting "`run_agent_alter` is absent" alone would be a denylist, and a denylist only catches
/// the one bypass somebody already thought of. So the load-bearing assertion is that the funnel is
/// *singular*: exactly one site reads the envelope, exactly one site charges the budget, exactly
/// one site mutates a workspace's staged writes, and all three are inside `stage_all`. Any second
/// funnel — B11's, or one not yet written — trips this regardless of what it is called.
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

    // Exactly one of each, and each one inside `stage_all`. Counted over the whole file so a
    // second call site anywhere trips this, wherever somebody puts it.
    for (needle, what) in [
        ("self.branches.envelope_of(", "reads the capability envelope"),
        ("self.branches.charge_row_writes(", "charges the row-write budget"),
        ("workspaces.get_mut(", "mutates a branch workspace's staged writes"),
    ] {
        assert_eq!(
            runtime.matches(needle).count(),
            1,
            "`{needle}` — the site that {what} — occurs more than once in \
             src/agent_sql/runtime.rs. The envelope is enforced at ONE funnel, `stage_all`, and \
             that is only sufficient while nothing else reaches branch write state. If this is \
             B11's `stage_schema_edit`, the envelope now has a hole it cannot see: widen this \
             test to drive `ALTER TABLE` against a table the envelope forbids, or govern the new \
             funnel. Either way this test must stop being a text check."
        );
        assert!(
            stage_all.contains(needle),
            "`{needle}` has moved out of `stage_all`. Whatever now holds it is a second funnel, \
             and the envelope only governs the one."
        );
    }

    // Belt and braces on top of the count: B11's two symbols by name, so the failure message can
    // say exactly which merge did it instead of leaving the next reader to work it out.
    assert!(
        !dispatch.contains("run_agent_alter"),
        "B11's branch-scoped ALTER TABLE has landed in src/agent_sql/dispatch.rs. It reaches the \
         runtime without passing through `stage_all`, so the capability envelope cannot see it: a \
         branch whose envelope forbids `payroll` can ADD, RENAME or RETYPE a `payroll` column and \
         publish it at MERGE. Widen \
         `no_ddl_verb_is_governed_by_the_envelope_and_this_is_a_known_gap` to demonstrate it with \
         real SQL, or close the gap — and record which in INTEGRATION.md."
    );
    assert!(
        !runtime.contains("stage_schema_edit"),
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
