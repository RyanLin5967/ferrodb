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

use ferrodb::agent_sql::runtime::{table_capability, AgentRuntime};
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
        table_capability("inventory", vec![]).table,
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
    assert_eq!(db.runtime.envelope_of(branch).unwrap().unwrap().row_writes, 1);
    db.ok("MERGE;", &mut a);
    assert_eq!(db.qty(1), 7);
    assert_eq!(db.count("payroll"), 1, "a refused write reached the shared table");
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
    assert_eq!(db.count("payroll"), 1, "a refused write reached the shared table");
}

/// The budget's SPENT half has to survive too, or a restart hands a governed agent its quota back
/// — which is the same defect one level down.
#[test]
fn budget_already_spent_is_not_handed_back_by_a_restart() {
    let mut db = Db::new();
    db.seed();
    let envelope = CapabilityEnvelope::new(Verb::ALL, 2).allow(
        table_capability("inventory", vec![]).table,
        vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
    );
    db.runtime.restrict_branch(BranchId::TRUNK, envelope).unwrap();

    let branch = {
        let mut a = db.session();
        let branch = db.begin("spender", &mut a);
        db.ok("UPDATE inventory SET qty = 19 WHERE id = 1;", &mut a);
        assert_eq!(db.runtime.envelope_of(branch).unwrap().unwrap().row_writes, 1);
        branch
    };

    let db = db.reopen();
    let after = db.runtime.envelope_of(branch).unwrap().expect("envelope lost");
    assert_eq!(after.row_writes, 1, "a restart handed the spent budget back");
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
    assert_eq!(db.count("payroll"), 1, "a refused write reached the shared table");
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
        table_capability("inventory", vec![]).table,
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
        table_capability("inventory", vec![]).table,
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
        table_capability("inventory", vec![]).table,
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
        table_capability("inventory", vec![]).table,
        vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
    );
    db.runtime.restrict_branch(BranchId::TRUNK, two).unwrap();

    let mut a = db.session();
    let branch = db.begin("budgeted", &mut a);

    // Three rows against a budget of two.
    let err = db.refused("UPDATE inventory SET qty = 1;", &mut a);
    assert!(err.contains("row-write budget"), "got {err}");
    assert_eq!(
        db.runtime.envelope_of(branch).unwrap().unwrap().row_writes,
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
        table_capability("inventory", vec![]).table,
        vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
    );
    db.runtime.restrict_branch(BranchId::TRUNK, one).unwrap();

    let mut a = db.session();
    let branch = db.begin("noop", &mut a);
    db.ok("UPDATE inventory SET qty = 20 WHERE id = 1;", &mut a); // already 20
    assert_eq!(db.runtime.envelope_of(branch).unwrap().unwrap().row_writes, 0);
    // Anti-vacuity: a real change does cost one.
    db.ok("UPDATE inventory SET qty = 21 WHERE id = 1;", &mut a);
    assert_eq!(db.runtime.envelope_of(branch).unwrap().unwrap().row_writes, 1);
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
            table_capability("inventory", vec![]).table,
            vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
        )
        .allow(
            table_capability("payroll", vec![]).table,
            vec![ColumnCapability::open(ID), ColumnCapability::open(QTY)],
        );
    let err = db.runtime.restrict_branch(branch, wider).unwrap_err().to_string();
    assert!(err.contains("may only be narrowed"), "got {err}");

    // And the write it wanted is still refused.
    let err = db.refused("UPDATE payroll SET salary = 0 WHERE id = 1;", &mut a);
    assert!(err.contains("may not write table `payroll`"), "got {err}");

    // Anti-vacuity: narrowing IS accepted, so `restrict_branch` is not simply refusing everything.
    let narrower = CapabilityEnvelope::new(Verb::UPDATE, 5).allow(
        table_capability("inventory", vec![]).table,
        vec![ColumnCapability::floored(QTY, 0)],
    );
    db.runtime.restrict_branch(branch, narrower).expect("a narrowing must be accepted");
    let err = db.refused("DELETE FROM inventory WHERE id = 1;", &mut a);
    assert!(err.contains("may not DELETE"), "the narrowing did not take effect: {err}");
}

