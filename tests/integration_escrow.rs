//! D5: escrow at fork, through the SQL surface.
//!
//! This is the row that closes the finding the demo has been carrying since C5. Two agents each
//! take 12 from a counter of 20 under `WHERE qty >= 0`, both merges are admitted, and main lands
//! at **−4** — because a guard is a *precondition* re-evaluated against merged state before the
//! ops apply, so the second merge tests `8 >= 0` and passes. No amount of care at merge time fixes
//! that; the check is simply too late.
//!
//! Escrow moves the failure earlier. The slack is partitioned when it is claimed, and the write
//! that would overdraw is refused **while the agent is still writing** and can do something about
//! it. The first test here is the unescrowed case, kept deliberately, so the fix is measured
//! against the defect rather than asserted on its own.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::types::BranchId;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::ids::{ColId, RowId};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// `qty` is the second column, and columns are 1-indexed in `ColId`.
const QTY: ColId = ColId(1);

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
            .open(dir.path().join("escrow.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("escrow.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
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

    fn seed(&mut self, start: i32) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok(&format!("INSERT INTO inventory VALUES (1, {start});"), &mut s);
    }

    fn qty(&mut self) -> i32 {
        let mut s = self.session();
        match self.ok("SELECT qty FROM inventory WHERE id = 1;", &mut s) {
            Outcome::Rows(rows) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Integer(i)) => *i,
                other => panic!("unexpected qty: {other:?}"),
            },
            _ => panic!("expected rows"),
        }
    }
}

/// The defect, kept so the fix has something to be measured against. Without escrow the counter
/// really does go under, and a test that only showed escrow working would not prove escrow was
/// what did it.
#[test]
fn without_escrow_two_agents_drive_the_counter_below_its_floor() {
    let mut db = Db::new();
    db.seed(20);

    for (agent, run_id) in [("a", "r_a"), ("b", "r_b")] {
        let mut s = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS '{agent}' RUN '{run_id}';"), &mut s);
        db.ok("UPDATE inventory SET qty = qty - 12 WHERE id = 1 AND qty >= 0;", &mut s);
        db.ok("MERGE;", &mut s);
    }

    assert_eq!(
        db.qty(),
        -4,
        "the unescrowed case is supposed to reach -4; if this changed, the premise of this whole \
         file changed with it"
    );
}

/// The same scenario with the slack partitioned first.
#[test]
fn with_escrow_the_second_agent_is_refused_while_it_is_still_writing() {
    let mut db = Db::new();
    db.seed(20);
    // 20 units of headroom above a floor of 0.
    db.runtime.open_escrow("inventory", RowId(1), QTY, 20).unwrap();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r_a';", &mut a);
    let a_branch = a.agent.as_ref().unwrap().branch;
    db.runtime.claim_escrow(a_branch, "inventory", RowId(1), QTY, 12).unwrap();

    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'b' RUN 'r_b';", &mut b);
    let b_branch = b.agent.as_ref().unwrap().branch;

    // Agent b cannot even reserve 12: only 8 are left, and it finds out now rather than after
    // doing the work.
    let err = db
        .runtime
        .claim_escrow(b_branch, "inventory", RowId(1), QTY, 12)
        .expect_err("both agents reserved 12 out of 20, which is how the counter reached -4");
    assert!(format!("{err}").contains("exceeds the 8"), "got {err}");

    // It takes what actually exists instead.
    db.runtime.claim_escrow(b_branch, "inventory", RowId(1), QTY, 8).unwrap();

    db.ok("UPDATE inventory SET qty = qty - 12 WHERE id = 1 AND qty >= 0;", &mut a);
    db.ok("MERGE;", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 8 WHERE id = 1 AND qty >= 0;", &mut b);
    db.ok("MERGE;", &mut b);

    assert_eq!(db.qty(), 0, "12 + 8 = 20 out of 20 should land exactly on the floor");
}

/// The write-time half, through SQL: a claim that is not enforced on write is a suggestion.
#[test]
fn a_write_beyond_the_claim_fails_at_write_time_not_at_merge() {
    let mut db = Db::new();
    db.seed(20);
    db.runtime.open_escrow("inventory", RowId(1), QTY, 20).unwrap();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'greedy' RUN 'r_g';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;
    db.runtime.claim_escrow(branch, "inventory", RowId(1), QTY, 5).unwrap();

    // Within the claim.
    db.ok("UPDATE inventory SET qty = qty - 3 WHERE id = 1;", &mut a);
    assert_eq!(db.runtime.remaining_escrow(branch, "inventory", RowId(1), QTY), Some(2));

    // Over it — and the failure lands on the UPDATE, not on the MERGE.
    let err = match db.exec("UPDATE inventory SET qty = qty - 9 WHERE id = 1;", &mut a) {
        Err(e) => e,
        Ok(_) => panic!("a write of 9 against a remaining claim of 2 was allowed"),
    };
    let msg = format!("{err}");
    assert!(msg.contains("remaining escrow of 2"), "the error must say what is left: {msg}");

    // The refused statement left nothing behind: the claim is untouched and main is unchanged.
    assert_eq!(
        db.runtime.remaining_escrow(branch, "inventory", RowId(1), QTY),
        Some(2),
        "a refused write still consumed part of the claim"
    );
    assert_eq!(db.qty(), 20, "a refused write reached main");
}

/// An agent that walks away must not strand the resource — the failure mode that makes
/// reservation schemes unusable in practice.
#[test]
fn abandoning_a_branch_returns_its_claim_to_the_pool() {
    let mut db = Db::new();
    db.seed(20);
    db.runtime.open_escrow("inventory", RowId(1), QTY, 20).unwrap();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'quitter' RUN 'r_q';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;
    db.runtime.claim_escrow(branch, "inventory", RowId(1), QTY, 15).unwrap();
    assert_eq!(db.runtime.unclaimed_escrow("inventory", RowId(1), QTY), Some(5));

    db.ok("ABANDON;", &mut a);
    assert_eq!(
        db.runtime.unclaimed_escrow("inventory", RowId(1), QTY),
        Some(20),
        "an abandoned agent stranded its reservation, so the resource shrinks with every crash"
    );
}

/// Escrow governs only what was declared. Policing every column silently would make the mechanism
/// impossible to reason about, and would break every table that is not a bounded resource.
#[test]
fn a_column_with_no_declared_bound_is_not_escrowed() {
    let mut db = Db::new();
    db.seed(20);

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r_a';", &mut a);
    // No open_escrow call at all: this must behave exactly as it did before escrow existed.
    db.ok("UPDATE inventory SET qty = qty - 500 WHERE id = 1;", &mut a);
    db.ok("MERGE;", &mut a);
    assert_eq!(db.qty(), -480);
    assert_eq!(db.runtime.unclaimed_escrow("inventory", RowId(1), QTY), None);
}

/// Escrow must bound agents that run one AFTER another, not only concurrent ones.
///
/// This was a real hole. `seal` released the claim on merge as well as on abandon, so a MERGED
/// branch handed its spend back as though the resource had not been consumed. Five sequential
/// agents each claimed 12 from a pool of 20, each merged, and the counter reached **-40** — the
/// same failure escrow exists to prevent, arrived at the slow way. Merge now settles instead.
#[test]
fn escrow_bounds_sequential_agents_not_just_concurrent_ones() {
    let mut db = Db::new();
    db.seed(20);
    db.runtime.open_escrow("inventory", RowId(1), QTY, 20).unwrap();

    let mut granted = 0;
    for i in 0..5 {
        let mut s = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'a{i}' RUN 'r{i}';"), &mut s);
        let br = s.agent.as_ref().unwrap().branch;
        if db.runtime.claim_escrow(br, "inventory", RowId(1), QTY, 12).is_ok() {
            granted += 1;
            db.ok("UPDATE inventory SET qty = qty - 12 WHERE id = 1;", &mut s);
            db.ok("MERGE;", &mut s);
        }
    }

    assert_eq!(granted, 1, "a 20-unit pool granted {granted} claims of 12");
    assert_eq!(db.qty(), 8, "the counter left its floor: {}", db.qty());
    assert!(db.qty() >= 0, "escrow let the counter go below its floor");
}

/// D15: escrow governs the CHANGE TO THE CELL, not the shape of the op.
///
/// The hook used to match `Add(Int(d < 0))`, which looked equivalent and was not. `SET qty = -100`
/// is an `Assign`: it walked past the bound and put the counter at -100 against a floor of 0.
#[test]
fn a_direct_assignment_below_the_floor_is_refused_not_waved_through() {
    let mut db = Db::new();
    db.seed(20);
    db.runtime.open_escrow("inventory", RowId(1), QTY, 20).unwrap();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'sneaky' RUN 'r_s';", &mut a);
    let br = a.agent.as_ref().unwrap().branch;
    db.runtime.claim_escrow(br, "inventory", RowId(1), QTY, 1).unwrap();

    let err = match db.exec("UPDATE inventory SET qty = -100 WHERE id = 1;", &mut a) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("an assignment straight through the floor was accepted"),
    };
    assert!(err.contains("escrow"), "refused for the wrong reason: {err}");
    // The implied drop is 20 -> -100, i.e. 120 units, and the message should say so.
    assert!(err.contains("120"), "the error should name the implied drop: {err}");

    db.ok("MERGE;", &mut a);
    assert_eq!(db.qty(), 20, "a refused write still reached main");
}

/// A raise is always safe and must not be charged, or an agent could not undo its own decrement.
#[test]
fn raising_the_value_costs_no_escrow() {
    let mut db = Db::new();
    db.seed(20);
    db.runtime.open_escrow("inventory", RowId(1), QTY, 20).unwrap();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'giver' RUN 'r_g';", &mut a);
    let br = a.agent.as_ref().unwrap().branch;
    db.runtime.claim_escrow(br, "inventory", RowId(1), QTY, 5).unwrap();

    db.ok("UPDATE inventory SET qty = 999 WHERE id = 1;", &mut a);
    assert_eq!(
        db.runtime.remaining_escrow(br, "inventory", RowId(1), QTY),
        Some(5),
        "raising the value consumed headroom"
    );
}

/// An unbounded column is untouched by any of this.
#[test]
fn an_assignment_on_an_unbounded_cell_is_not_governed() {
    let mut db = Db::new();
    db.seed(20);
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'free' RUN 'r_f';", &mut a);
    db.ok("UPDATE inventory SET qty = -100 WHERE id = 1;", &mut a);
    db.ok("MERGE;", &mut a);
    assert_eq!(db.qty(), -100, "an undeclared cell must behave exactly as before");
}

/// **Known limit, asserted rather than left to be discovered.** Deleting the row removes the
/// bounded cell without spending against the claim: there is no "after" value to compare, so the
/// before/after rule has nothing to charge. Recorded so the gap is visible; closing it means
/// deciding what deleting a bounded resource even means, which is a design question.
#[test]
fn deleting_the_row_is_not_governed_by_escrow_and_this_is_a_known_gap() {
    let mut db = Db::new();
    db.seed(20);
    db.runtime.open_escrow("inventory", RowId(1), QTY, 20).unwrap();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'deleter' RUN 'r_d';", &mut a);
    let br = a.agent.as_ref().unwrap().branch;
    db.runtime.claim_escrow(br, "inventory", RowId(1), QTY, 1).unwrap();

    db.ok("DELETE FROM inventory WHERE id = 1;", &mut a);
    assert_eq!(
        db.runtime.remaining_escrow(br, "inventory", RowId(1), QTY),
        Some(1),
        "if a delete now spends escrow, this gap has been closed and the test should say so"
    );
}

/// **Scope boundary, asserted rather than implied.** Escrow governs AGENT-SESSION writes. A plain
/// `UPDATE` outside any session goes straight through the executor, never reaches `stage()`, and is
/// therefore not charged against any pool — it drives the counter to -100 here.
///
/// This is the third instance of one pattern found in three passes: a safety check attached to a
/// SHAPE or a PATH rather than to the outcome. D14 was merge-vs-abandon, D15 was Add-vs-Assign,
/// this is agent-vs-direct.
///
/// It is left as a boundary rather than "fixed" because closing it needs a decision that is not an
/// implementation detail: escrow claims are branch-scoped and a direct write has no branch, so
/// someone has to say whether the operator gets an implicit unlimited claim, a shared pool, or a
/// refusal. Inventing one of those quietly would be worse than saying it is open. What is NOT
/// acceptable is the claim "a bounded counter cannot go below its floor" without this qualifier,
/// and the README now carries it.
#[test]
fn escrow_governs_agent_writes_only_and_a_direct_write_is_not_charged() {
    let mut db = Db::new();
    db.seed(20);
    db.runtime.open_escrow("inventory", RowId(1), QTY, 20).unwrap();

    let mut s = db.session(); // no BEGIN AGENT SESSION
    db.ok("UPDATE inventory SET qty = -100 WHERE id = 1;", &mut s);

    assert_eq!(
        db.qty(),
        -100,
        "if a direct write is now charged against escrow, this boundary has moved and the README \
         and the escrow module doc must be updated to match"
    );
    assert_eq!(
        db.runtime.unclaimed_escrow("inventory", RowId(1), QTY),
        Some(20),
        "the pool should be untouched by a write it never saw"
    );
}

/// Changing the primary key while lowering the cell must still be charged: row identity comes from
/// the row as it was, so the resource cannot be moved out from under its own pool.
#[test]
fn changing_the_primary_key_does_not_move_the_cell_out_of_its_pool() {
    let mut db = Db::new();
    db.seed(20);
    db.runtime.open_escrow("inventory", RowId(1), QTY, 20).unwrap();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'mover' RUN 'r_m';", &mut a);
    let br = a.agent.as_ref().unwrap().branch;
    db.runtime.claim_escrow(br, "inventory", RowId(1), QTY, 1).unwrap();

    let err = match db.exec("UPDATE inventory SET id = 2, qty = -100 WHERE id = 1;", &mut a) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("a primary-key change carried the bounded cell past its pool"),
    };
    assert!(err.contains("escrow"), "refused for the wrong reason: {err}");
}

/// Writing a bounded cell with no claim at all is refused, so escrow cannot be skipped by simply
/// never asking for headroom.
#[test]
fn a_bounded_cell_cannot_be_written_without_claiming_first() {
    let mut db = Db::new();
    db.seed(20);
    db.runtime.open_escrow("inventory", RowId(1), QTY, 20).unwrap();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'noclaim' RUN 'r_n';", &mut a);
    let err = match db.exec("UPDATE inventory SET qty = qty - 1 WHERE id = 1;", &mut a) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("a bounded cell was written with no claim"),
    };
    assert!(err.contains("escrow"), "refused for the wrong reason: {err}");
    assert_eq!(db.qty(), 20);
}
