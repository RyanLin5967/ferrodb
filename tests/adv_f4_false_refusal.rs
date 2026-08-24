//! ADVERSARIAL (I21 / F4): can `fresh_reservation` refuse a LEGITIMATE merge?
//!
//! The guard errors when `base < highest`, where `base = State::apply_seq` at reservation time and
//! `highest = max` over `State::applied`'s `seq`. A false refusal turns a valid MERGE into
//! `refusing to publish: this merge would stamp version sequences from ...`.
//!
//! Every test here drives the real SQL surface and asserts the merge SUCCEEDED, naming the guard's
//! message if it did not.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::agent_sql::MergeReport;
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
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const GUARD: &str = "would stamp version sequences";

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
            .open(dir.path().join("adv.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("adv.wal")).unwrap());
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
        match self.exec(sql, session) {
            Ok(o) => o,
            Err(e) => panic!("{} failed: {}", sql, e),
        }
    }

    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 5);", &mut s);
    }

    fn all(&mut self) -> Vec<Vec<Value>> {
        let mut s = self.session();
        rows(self.ok("SELECT id, qty FROM inventory;", &mut s))
    }

    fn has_row(&mut self, id: i32) -> bool {
        self.all().iter().any(|r| r[0] == Value::Integer(id))
    }

    fn qty_of(&mut self, id: i32) -> i32 {
        let row = self
            .all()
            .into_iter()
            .find(|r| r[0] == Value::Integer(id))
            .unwrap_or_else(|| panic!("row {} missing", id));
        match row[1] {
            Value::Integer(i) => i,
            ref other => panic!("qty is not an integer: {:?}", other),
        }
    }

    /// MERGE, asserting it landed, and naming the F4 guard explicitly if it refused.
    fn merge_must_land(&mut self, s: &mut Session, what: &str) -> MergeReport {
        match self.exec("MERGE;", s) {
            Ok(o) => {
                let r = report(o);
                assert!(
                    r.applied_to_target,
                    "{what}: the merge did not reach the target: {r}"
                );
                r
            }
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    !msg.contains(GUARD),
                    "FALSE REFUSAL by the F4 reservation guard after {what}: {msg}"
                );
                panic!("{what}: MERGE failed for another reason: {msg}");
            }
        }
    }
}

fn rows(out: Outcome) -> Vec<Vec<Value>> {
    match out {
        Outcome::Rows(r) => r,
        other => panic!("expected rows, got {:?}", std::mem::discriminant(&other)),
    }
}

fn agent(out: Outcome) -> AgentOutput {
    match out {
        Outcome::Agent(a) => a,
        _ => panic!("expected an agent output"),
    }
}

fn report(out: Outcome) -> MergeReport {
    match agent(out) {
        AgentOutput::Merge(m) => m,
        other => panic!("expected a merge report, got {}", other),
    }
}

fn plan(out: Outcome) -> RevertPlan {
    match agent(out) {
        AgentOutput::Revert(p) => p,
        other => panic!("expected a revert plan, got {}", other),
    }
}

/// One agent task that inserts a row and merges it, returning its merge id.
fn insert_and_merge(db: &mut Db, agent_id: &str, run_id: &str, id: i32, qty: i32) -> String {
    let mut s = db.session();
    db.ok(&format!("BEGIN AGENT SESSION AS '{}' RUN '{}';", agent_id, run_id), &mut s);
    db.ok(&format!("INSERT INTO inventory VALUES ({}, {});", id, qty), &mut s);
    let r = db.merge_must_land(&mut s, &format!("insert_and_merge({agent_id})"));
    r.merge_id
}

// ---------------------------------------------------------------------------------------------
// 1. REVERT then MERGE
// ---------------------------------------------------------------------------------------------

#[test]
fn revert_then_merge() {
    let mut db = Db::new();
    db.seed();
    let m1 = insert_and_merge(&mut db, "a1", "r1", 7, 30);
    assert!(db.has_row(7));

    let mut main = db.session();
    let p = plan(db.ok(&format!("REVERT MERGE {};", m1), &mut main));
    assert!(!p.is_blocked(), "nothing read the row, so the revert must proceed: {:?}", p);
    assert!(!db.has_row(7), "the revert did not happen; the rest of this test is vacuous");

    // The merge after the revert must land.
    let mut s = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a2' RUN 'r2';", &mut s);
    db.ok("UPDATE inventory SET qty = qty + 3 WHERE id = 1;", &mut s);
    db.merge_must_land(&mut s, "REVERT then MERGE");
    assert_eq!(db.qty_of(1), 23);

    // And a second one, in case the first only worked because the counter had slack.
    let mut s2 = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a3' RUN 'r3';", &mut s2);
    db.ok("UPDATE inventory SET qty = qty + 4 WHERE id = 1;", &mut s2);
    db.merge_must_land(&mut s2, "REVERT then two MERGEs");
    assert_eq!(db.qty_of(1), 27);
}

// ---------------------------------------------------------------------------------------------
// 2. REVERT CASCADE then MERGE (a cascade undoes MORE than one txn)
// ---------------------------------------------------------------------------------------------

#[test]
fn revert_cascade_then_merge() {
    let mut db = Db::new();
    db.seed();

    // (a) a merge that lands a row inside a range someone is about to scan.
    let m1 = insert_and_merge(&mut db, "restock", "r_restock", 7, 30);

    // (b) a scanning task that then writes, so it becomes a dependent.
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'reporter' RUN 'r_rep';", &mut b);
    let seen = rows(db.ok("SELECT id, qty FROM inventory WHERE qty >= 20 AND qty < 50;", &mut b));
    assert_eq!(seen.len(), 2, "the fixture's scan did not see the rows it must: {:?}", seen);
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut b);
    db.merge_must_land(&mut b, "the dependent's own merge");

    // (c) cascade: two txns undone in one statement.
    let mut main = db.session();
    let c = plan(db.ok(&format!("REVERT MERGE {} CASCADE;", m1), &mut main));
    assert!(!c.is_blocked(), "{:?}", c);
    assert_eq!(c.cascade.len(), 1, "the cascade must actually name a dependent: {:?}", c);
    assert!(!db.has_row(7));
    assert_eq!(db.qty_of(2), 5);

    // (d) merges after a cascade.
    for (i, who) in ["c1", "c2", "c3"].iter().enumerate() {
        let mut s = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS '{who}' RUN 'rc{i}';"), &mut s);
        db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 1;", &mut s);
        db.merge_must_land(&mut s, &format!("REVERT CASCADE then MERGE #{i}"));
    }
    assert_eq!(db.qty_of(1), 23);
}

// ---------------------------------------------------------------------------------------------
// 3. SIMULATE then MERGE. SIMULATE publishes admitted candidates and reaps the losers, so it is
//    the shape most likely to leave the counter and the applied log disagreeing.
// ---------------------------------------------------------------------------------------------

#[test]
fn simulate_then_merge() {
    let mut db = Db::new();
    db.seed();
    let mut s = db.session();
    let sim = "SIMULATE AS 'pricer' RUN 'r_9' \
               CANDIDATE 'a' ( UPDATE inventory SET qty = qty - 8 WHERE id = 1; ) \
               CANDIDATE 'b' ( UPDATE inventory SET qty = qty - 8 WHERE id = 1; ) \
               CANDIDATE 'c' ( UPDATE inventory SET qty = qty - 8 WHERE id = 1; ) \
               ASSERT ON inventory (qty >= 0) \
               ADMIT ALL;";
    match db.ok(sim, &mut s) {
        Outcome::Agent(AgentOutput::Simulation(r)) => {
            assert_eq!(r.candidates.len(), 3, "{r}");
            assert!(!r.admitted().is_empty(), "no candidate was admitted, so nothing published: {r}");
        }
        _ => panic!("expected a simulation report"),
    }

    for i in 0..3 {
        let mut a = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'post' RUN 'rp{i}';"), &mut a);
        db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut a);
        db.merge_must_land(&mut a, &format!("SIMULATE then MERGE #{i}"));
    }
    assert_eq!(db.qty_of(2), 8);
}

// ---------------------------------------------------------------------------------------------
// 4. A merge that FAILS, then a merge that must succeed. Three distinct failure shapes.
// ---------------------------------------------------------------------------------------------

/// The gate declines (an assertion the merge would violate), then a good merge.
#[test]
fn a_gate_refusal_then_merge() {
    let mut db = Db::new();
    db.seed();
    // A candidate that violates its own assertion is refused inside SIMULATE, which reaches
    // `publish_evaluation_as` for the admitted ones only. Use an explicit escrow overdraw instead:
    // that is a guard conflict, refused after composition.
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'over' RUN 'r_over';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 999 WHERE id = 1;", &mut a);
    let refused = db.exec("MERGE;", &mut a);
    // Whether this particular shape refuses is not the point; what matters is that a merge which
    // did NOT publish is followed by one that must.
    let refused_desc = match &refused {
        Ok(o) => format!("landed: {:?}", std::mem::discriminant(o)),
        Err(e) => e.to_string(),
    };
    assert!(
        !refused_desc.contains(GUARD),
        "the F4 guard refused the FIRST merge of a fresh database: {refused_desc}"
    );

    for i in 0..3 {
        let mut s = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'after' RUN 'ra{i}';"), &mut s);
        db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut s);
        db.merge_must_land(&mut s, &format!("after a refused merge #{i} ({refused_desc})"));
    }
}

/// A conflicting merge (refused BEFORE the reservation), then a good merge.
#[test]
fn a_conflicting_merge_then_merge() {
    let mut db = Db::new();
    db.seed();

    // Two branches assign the same cell: the second conflicts.
    let mut x = db.session();
    db.ok("BEGIN AGENT SESSION AS 'x' RUN 'rx';", &mut x);
    db.ok("UPDATE inventory SET qty = 11 WHERE id = 1;", &mut x);
    let mut y = db.session();
    db.ok("BEGIN AGENT SESSION AS 'y' RUN 'ry';", &mut y);
    db.ok("UPDATE inventory SET qty = 77 WHERE id = 1;", &mut y);

    db.merge_must_land(&mut x, "the first of two racing assigns");
    let second = db.exec("MERGE;", &mut y);
    let desc = match &second {
        Ok(_) => "landed".to_string(),
        Err(e) => e.to_string(),
    };
    assert!(!desc.contains(GUARD), "the F4 guard refused the racing merge: {desc}");

    for i in 0..3 {
        let mut s = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'z{i}' RUN 'rz{i}';"), &mut s);
        db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut s);
        db.merge_must_land(&mut s, &format!("after a conflicting merge #{i} ({desc})"));
    }
}

/// A merge that fails AFTER the reservation was taken: the branch's schema edit is refused by the
/// catalog once the rows are already published. F7 says this leaks the reserved range; the question
/// is whether the leak makes the NEXT merge refuse.
#[test]
fn a_merge_that_fails_after_reserving_then_merge() {
    let mut db = Db::new();
    db.seed();

    // Branch 1 adds a column; branch 2 stages the SAME column. BOTH stage before EITHER merges,
    // which is what the doc comment above describes and what the fixture as quarantined did not do:
    // it opened session 'sch2' AFTER branch 1 had already merged, so branch 2's ALTER was refused
    // at statement time by a column it could already see ("the table already has a column called
    // 'note'"), and the post-reservation merge failure this test exists to exercise never happened.
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'sch1' RUN 'rs1';", &mut a);
    db.ok("ALTER TABLE inventory ADD COLUMN note VARCHAR(32);", &mut a);
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 1;", &mut a);

    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'sch2' RUN 'rs2';", &mut b);
    db.ok("ALTER TABLE inventory ADD COLUMN note VARCHAR(32);", &mut b);
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut b);

    let first = db.exec("MERGE;", &mut a);
    let first_desc = match &first {
        Ok(_) => "landed".to_string(),
        Err(e) => e.to_string(),
    };
    eprintln!("[adv-f4] first schema merge: {first_desc}");

    let second = db.exec("MERGE;", &mut b);
    let second_desc = match &second {
        Ok(_) => "landed".to_string(),
        Err(e) => e.to_string(),
    };
    eprintln!("[adv-f4] duplicate-column merge: {second_desc}");
    assert!(
        !second_desc.contains(GUARD),
        "the F4 guard refused the duplicate-column merge: {second_desc}"
    );

    // Whatever happened above, ordinary merges must still work.
    for i in 0..4 {
        let mut s = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'q{i}' RUN 'rq{i}';"), &mut s);
        db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 1;", &mut s);
        db.merge_must_land(
            &mut s,
            &format!("after a post-reservation failure #{i} (first={first_desc}, second={second_desc})"),
        );
    }
}

// ---------------------------------------------------------------------------------------------
// 5. quarantine, release, merge
// ---------------------------------------------------------------------------------------------

#[test]
fn quarantine_release_then_merge() {
    let mut db = Db::new();
    db.seed();
    let _ = insert_and_merge(&mut db, "pre", "r_pre", 7, 30);

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'suspect' RUN 'r_s';", &mut a);
    let branch = a.agent.as_ref().unwrap().branch;
    db.ok("UPDATE inventory SET qty = 42 WHERE id = 1;", &mut a);
    db.runtime.quarantine(branch, "adversarial hold").unwrap();
    assert!(db.runtime.quarantine_reason(branch).is_some(), "the hold did not take");
    db.runtime.release_from_quarantine(branch).unwrap();
    assert!(db.runtime.quarantine_reason(branch).is_none());

    db.merge_must_land(&mut a, "quarantine then release then MERGE");
    assert_eq!(db.qty_of(1), 42);

    let mut s = db.session();
    db.ok("BEGIN AGENT SESSION AS 'after-release' RUN 'r_ar';", &mut s);
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut s);
    db.merge_must_land(&mut s, "a merge after a released branch merged");
}

// ---------------------------------------------------------------------------------------------
// 6. ABANDON, then a new session, then MERGE
// ---------------------------------------------------------------------------------------------

#[test]
fn abandon_then_new_session_then_merge() {
    let mut db = Db::new();
    db.seed();
    let _ = insert_and_merge(&mut db, "pre", "r_pre", 7, 30);

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'quitter' RUN 'r_q';", &mut a);
    db.ok("INSERT INTO inventory VALUES (8, 8);", &mut a);
    db.ok("UPDATE inventory SET qty = qty + 5 WHERE id = 1;", &mut a);
    db.ok("ABANDON;", &mut a);
    assert!(!db.has_row(8), "ABANDON published rows");
    assert_eq!(db.runtime.forget_reaped_branches(), 0, "nothing should be reap-eligible yet");

    for i in 0..3 {
        let mut s = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'fresh{i}' RUN 'rf{i}';"), &mut s);
        db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 1;", &mut s);
        db.merge_must_land(&mut s, &format!("ABANDON then a new session then MERGE #{i}"));
    }
    assert_eq!(db.qty_of(1), 23);
}

// ---------------------------------------------------------------------------------------------
// 7. Two connections merging, interleaved. `ExecCtx` holds `&mut Catalog`, so two merges cannot be
//    in flight simultaneously by construction; what CAN interleave is fork/write/merge across
//    sessions, which is what decides the base each reservation takes.
// ---------------------------------------------------------------------------------------------

#[test]
fn two_interleaved_connections_merging() {
    let mut db = Db::new();
    db.seed();

    let mut c1 = db.session();
    let mut c2 = db.session();
    db.ok("BEGIN AGENT SESSION AS 'c1' RUN 'r1';", &mut c1);
    db.ok("BEGIN AGENT SESSION AS 'c2' RUN 'r2';", &mut c2);

    // Both branches fork before either merges, then write disjoint rows, then merge in order.
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 1;", &mut c1);
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut c2);
    db.merge_must_land(&mut c1, "the first of two forked-together connections");
    db.merge_must_land(&mut c2, "the second of two forked-together connections");
    assert_eq!(db.qty_of(1), 21);
    assert_eq!(db.qty_of(2), 6);

    // Now the reverse: fork c4 before c3 merges, so c4's fork_seq is behind the counter.
    let mut c3 = db.session();
    let mut c4 = db.session();
    db.ok("BEGIN AGENT SESSION AS 'c3' RUN 'r3';", &mut c3);
    db.ok("UPDATE inventory SET qty = qty + 10 WHERE id = 1;", &mut c3);
    db.ok("BEGIN AGENT SESSION AS 'c4' RUN 'r4';", &mut c4);
    db.ok("UPDATE inventory SET qty = qty + 10 WHERE id = 2;", &mut c4);
    db.merge_must_land(&mut c3, "c3, with c4 already forked behind it");
    db.merge_must_land(&mut c4, "c4, forked behind c3's merge");
    assert_eq!(db.qty_of(1), 31);
    assert_eq!(db.qty_of(2), 16);
}

/// A multi-op merge, which is where the reservation's arithmetic actually has width: one merge that
/// stamps several sequences, followed by merges that must take a base at the top of that range.
#[test]
fn multi_op_merges_in_sequence() {
    let mut db = Db::new();
    db.seed();
    for i in 0..6 {
        let mut s = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'wide{i}' RUN 'rw{i}';"), &mut s);
        // Three rows and a two-column write, so one merge reserves several sequences.
        db.ok(&format!("INSERT INTO inventory VALUES ({}, {});", 100 + i, 7), &mut s);
        db.ok(&format!("INSERT INTO inventory VALUES ({}, {});", 200 + i, 8), &mut s);
        db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 1;", &mut s);
        db.ok("UPDATE inventory SET id = id WHERE id = 2;", &mut s);
        db.merge_must_land(&mut s, &format!("wide merge #{i}"));
    }
    assert_eq!(db.qty_of(1), 26);
}

// ---------------------------------------------------------------------------------------------
// 8. Many merges in a row, on one runtime.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_hundred_merges_in_a_row() {
    let mut db = Db::new();
    db.seed();
    for i in 0..100 {
        let mut s = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'm' RUN 'r{i}';"), &mut s);
        db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 1;", &mut s);
        db.merge_must_land(&mut s, &format!("merge #{i} of 100"));
    }
    assert_eq!(db.qty_of(1), 120);
}

/// Mixed: revert every third merge, so the applied log grows while the counter is also moved by
/// reverts' own writes. This is the shape where a decrementing counter would show up.
#[test]
fn forty_merges_with_reverts_interleaved() {
    let mut db = Db::new();
    db.seed();
    let mut ids: Vec<String> = Vec::new();
    for i in 0..40 {
        let mut s = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'mix' RUN 'rm{i}';"), &mut s);
        db.ok(&format!("INSERT INTO inventory VALUES ({}, {});", 500 + i, 4), &mut s);
        let r = db.merge_must_land(&mut s, &format!("mixed merge #{i}"));
        ids.push(r.merge_id);
        if i % 3 == 2 {
            let victim = ids.pop().unwrap();
            let mut main = db.session();
            let p = plan(db.ok(&format!("REVERT MERGE {} CASCADE;", victim), &mut main));
            assert!(!p.is_blocked(), "revert #{i} was blocked: {:?}", p);
        }
    }
    assert!(db.has_row(500), "the fixture published nothing");
}

/// PROBE, added during the E77 re-derivation of this fixture (not part of the original F4 attack):
/// the test above reports BOTH duplicate-column merges as "landed". That is only correct if the
/// second edit is deduplicated. If both are applied, `inventory` ends up with two columns called
/// `note`, which is a corrupt schema no `ALTER` could produce directly -- the statement-time path
/// refuses it with "the table already has a column called 'note'".
#[test]
fn two_branches_adding_the_same_column_do_not_leave_two_columns_of_that_name() {
    let mut db = Db::new();
    db.seed();

    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'sch1' RUN 'rs1';", &mut a);
    db.ok("ALTER TABLE inventory ADD COLUMN note VARCHAR(32);", &mut a);
    let mut b = db.session();
    db.ok("BEGIN AGENT SESSION AS 'sch2' RUN 'rs2';", &mut b);
    db.ok("ALTER TABLE inventory ADD COLUMN note VARCHAR(32);", &mut b);

    let first = db.exec("MERGE;", &mut a);
    let second = db.exec("MERGE;", &mut b);

    let cols: Vec<String> =
        db.catalog.get_table("inventory").unwrap().schema.columns.iter().map(|c| c.name.clone()).collect();
    let notes = cols.iter().filter(|n| n.as_str() == "note").count();
    eprintln!("[adv-f4-probe] first={:?} second={:?} columns={:?}", first.is_ok(), second.is_ok(), cols);
    assert_eq!(
        notes, 1,
        "`inventory` has {} columns named `note` after two branches each added it and both merges \
         reported success: {:?}",
        notes, cols
    );
}
