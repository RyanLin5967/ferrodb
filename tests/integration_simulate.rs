//! `SIMULATE`: K candidate branches off one base, scored, the winners admitted, the losers reaped.
//!
//! Design authority: DESIGN.md sections 1 and 4, exit criteria 1, 5, 7 and 8.
//!
//! **What every measurement in this file is guarded against.** A page count that does not move is
//! meaningless if nothing ever writes a page, and an assertion that holds is meaningless if it ran
//! over no rows. Both are facts about the test's scope that read as passes, so each of them is
//! forced to be non-vacuous before anything is read into it: the trunk is loaded with enough rows
//! to occupy many pages before a fork is credited with copying none, and every assertion here is
//! checked against a table with rows in it.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::gate::GateOutcome;
use ferrodb::agent_sql::runtime::{row_id_of, AgentRuntime, ExecCtx};
use ferrodb::agent_sql::simulate::{AdmitPolicy, SimulationPlan, SimulationReport};
use ferrodb::agent_sql::AgentOutput;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::{Expr, Parser, Stmt};
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Above anything the ordinary heap/index allocator will want; the partition is deliberate.
const ARENA_BASE: u32 = 1024;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    branches: Arc<LogBranchCatalog>,
    store: Arc<ArenaPageStore>,
    reaper: Arc<TwoTierReaper>,
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
            .open(dir.path().join("pages.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("pages.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);

        let branches = Arc::new(LogBranchCatalog::in_memory(1));
        let store =
            Arc::new(ArenaPageStore::new(bp.clone(), Arc::clone(&branches) as std::sync::Arc<dyn ferrodb::branch::BranchCatalog>, ARENA_BASE).unwrap());
        let reaper =
            Arc::new(TwoTierReaper::new(Arc::clone(&branches) as std::sync::Arc<dyn ferrodb::branch::BranchCatalog>, Arc::clone(&store)));
        let runtime = Arc::new(
            AgentRuntime::with_storage(
                Arc::clone(&branches) as Arc<dyn BranchCatalog>,
                Arc::new(MemEffectLog::new()),
                Arc::clone(&store) as Arc<dyn PageStore>,
            )
            .unwrap()
            // A merged branch's shadow pages are garbage the instant its rows are in the shared
            // tables. Without this the winners' pages sit allocated until their leases expire and
            // "page count returns to baseline" would be false by exactly that much.
            .with_reaper(Arc::clone(&reaper) as Arc<dyn Reaper>),
        );
        Db { catalog, bp, txn, runtime, branches, store, reaper, _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let mut stmts = parse(sql)?;
        assert_eq!(stmts.len(), 1, "expected one statement: {}", sql);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    /// `inventory` in the heap, which is where the candidates' merges publish to.
    fn seed(&mut self) {
        let mut s = self.session();
        self.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        self.ok("INSERT INTO inventory VALUES (1, 20);", &mut s);
        self.ok("INSERT INTO inventory VALUES (2, 5);", &mut s);
    }

    /// Rows on the TRUNK's own copy-on-write tree, so the trunk occupies real pages.
    ///
    /// Without this the page store holds one empty root and "the fork copied zero pages" would be
    /// a statement about an empty tree. These rows are deliberately in a table the SQL surface
    /// never touches: their whole job is to make the trunk's tree deep enough that a branch's
    /// first write copies a root-to-leaf PATH rather than a single page.
    fn ballast(&mut self, rows: u64) {
        for r in 0..rows {
            self.runtime
                .put_row(
                    BranchId::TRUNK,
                    "ballast",
                    r,
                    &[Value::Integer(r as i32), Value::Varchar(format!("widget-{r}"))],
                )
                .unwrap();
        }
    }

    fn pages(&self) -> u32 {
        self.store.live_page_count().unwrap()
    }

    fn qty(&mut self, id: i32) -> i32 {
        let mut s = self.session();
        match self.ok(&format!("SELECT qty FROM inventory WHERE id = {id};"), &mut s) {
            Outcome::Rows(rows) => match rows.first().and_then(|r| r.first()) {
                Some(Value::Integer(i)) => *i,
                other => panic!("row {id} has no integer qty: {other:?}"),
            },
            _ => panic!("expected rows from a SELECT"),
        }
    }

    fn simulate(&mut self, plan: &SimulationPlan) -> Result<SimulationReport, FerroError> {
        let rt = self.runtime.clone();
        let bp = self.bp.clone();
        let txn = self.txn.clone();
        let mut ctx = ExecCtx { catalog: &mut self.catalog, bp, txn };
        rt.simulate(&mut ctx, BranchId::TRUNK, plan)
    }
}

fn parse(sql: &str) -> Result<Vec<Stmt>, FerroError> {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
    let mut parser = Parser::new(tokens);
    let stmts = parser.parse();
    if !parser.errors.is_empty() {
        return Err(FerroError::SqlParseError(
            parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
        ));
    }
    Ok(stmts)
}

/// `Outcome` is not `Debug`, so `unwrap_err` cannot be used on a statement that must fail.
fn expect_err(r: Result<Outcome, FerroError>) -> FerroError {
    match r {
        Ok(_) => panic!("the statement was accepted; it must be refused"),
        Err(e) => e,
    }
}

fn stmts(sql: &str) -> Vec<Stmt> {
    parse(sql).unwrap_or_else(|e| panic!("{sql} failed to parse: {e}"))
}

/// A predicate, borrowed out of a WHERE clause. The parser has no public entry point for a bare
/// expression and this needs no production code to exist.
fn predicate(sql: &str) -> Expr {
    match stmts(&format!("SELECT * FROM inventory WHERE {sql};")).remove(0) {
        Stmt::Select { where_clause: Some(e), .. } => e,
        other => panic!("expected a WHERE clause, got {other:?}"),
    }
}

/// K candidates that each take `amounts[i]` off row 1, with one declared invariant.
fn plan_taking(amounts: &[i32], assertion: &str) -> SimulationPlan {
    let mut plan = SimulationPlan::new("pricing-agent").run("r_9");
    for (i, n) in amounts.iter().enumerate() {
        plan = plan.candidate(
            format!("take-{n}-{i}"),
            stmts(&format!("UPDATE inventory SET qty = qty - {n} WHERE id = 1;")),
        );
    }
    plan.assert_on("inventory", predicate(assertion)).admit(AdmitPolicy::All)
}

// -------------------------------------------------------------------------------------------
// Exit criterion: K forks with the live page count unchanged at fork time.
// -------------------------------------------------------------------------------------------

/// **Breaking shape:** a fork that copies the base's pages. It would be invisible at K = 1 on an
/// empty tree and ruinous at K = 12 on a real one — which is why the trunk is loaded first and the
/// measurement is refused as vacuous if it is not.
#[test]
fn forking_k_candidates_copies_zero_pages_and_the_trunk_really_had_pages_to_copy() {
    let mut db = Db::new();
    db.seed();
    let empty = db.pages();
    db.ballast(600);
    let trunk_pages = db.pages();
    assert!(
        trunk_pages > empty,
        "the page counter did not move while writing 600 rows ({empty} -> {trunk_pages}); it is \
         not measuring anything and 'the fork copied zero' would be a fact about the counter"
    );
    assert!(
        trunk_pages > 1,
        "the trunk occupies {trunk_pages} page(s), so 'K forks copied zero' would be vacuous"
    );

    // ADMIT 0: fork, run and score everything, publish nothing.
    let plan = plan_taking(&[1, 2, 3, 4, 5], "qty >= 0").admit(AdmitPolicy::AtMost(0));
    let report = db.simulate(&plan).expect("simulation");

    assert_eq!(report.candidates.len(), 5);
    assert_eq!(report.pages_before_fork, Some(trunk_pages));
    assert_eq!(
        report.pages_after_fork,
        Some(trunk_pages),
        "forking 5 candidate branches copied {} page(s); the criterion is zero",
        report.pages_after_fork.unwrap_or(0) as i64 - trunk_pages as i64
    );

    println!(
        "MEASURED (this machine, this run): live pages {} before 5 forks, {} after",
        report.pages_before_fork.unwrap(),
        report.pages_after_fork.unwrap()
    );

    // Zero-copy is only correct if the children can still READ the base. A fork that copies
    // nothing and sees nothing would satisfy the count while failing the point.
    for c in &report.candidates {
        assert_eq!(
            db.runtime.get_row(c.branch, "ballast", 599).unwrap(),
            Some(vec![Value::Integer(599), Value::Varchar("widget-599".into())]),
            "candidate {} cannot see the base it forked from",
            c.name
        );
    }
}

/// The other half of the cost model: K candidates cost K root-to-leaf PATH copies, not K copies of
/// the database. `bench/branch_scaling.txt` measured that path at 2 pages on a 2,000-row trunk and
/// 3 on a 40,000-row trunk; this asserts the shape rather than those constants, because the shape
/// is what makes running twelve variants affordable.
///
/// **Breaking shape:** a branch write that materialises the branch instead of shadowing a path.
/// The total would then track K × trunk_pages, and at 5 candidates on a 600-row trunk that is the
/// difference between a handful of pages and six copies of the database.
#[test]
fn k_candidates_cost_k_path_copies_not_k_database_copies() {
    let mut db = Db::new();
    db.seed();
    db.ballast(600);
    let trunk_pages = db.pages();
    assert!(trunk_pages > 4, "trunk occupies {trunk_pages} page(s); the comparison would be weak");

    let plan = plan_taking(&[1, 2, 3, 4, 5], "qty >= 0").admit(AdmitPolicy::AtMost(0));
    let report = db.simulate(&plan).expect("simulation");
    let written = report.pages_written_by_candidates().expect("page-backed runtime");

    assert!(
        written > 0,
        "5 candidates each wrote a row and allocated no pages; nothing was mirrored onto their \
         trees and this test would prove nothing"
    );
    // Printed, not only asserted: the number is the claim, and a claim nobody can read is one
    // nobody can check. `cargo test --test integration_simulate -- --nocapture` shows it.
    println!(
        "MEASURED (this machine, this run): trunk = {trunk_pages} pages; 5 candidates each \
         writing 1 row cost {written} pages in total, {:.1} per candidate; one full copy of the \
         trunk would be {trunk_pages} and five would be {}",
        written as f64 / 5.0,
        trunk_pages * 5
    );
    assert!(
        written < trunk_pages,
        "5 candidates cost {written} page(s) against a {trunk_pages}-page trunk. One copy of the \
         database is {trunk_pages}; five would be {}. This is supposed to be five path copies.",
        trunk_pages * 5
    );
}

// -------------------------------------------------------------------------------------------
// Hard part 1: scoring one candidate must not move the base under the next.
// -------------------------------------------------------------------------------------------

/// **Breaking shape:** scoring by merging. Candidate 1 lands on the target, and candidates 2..K
/// are then scored against a database candidate 1 already changed — so the report compares five
/// candidates that were never compared against the same thing. With `qty -= n` that is directly
/// visible: candidate 5 would be scored against 20−1−2−3−4 rather than against 20.
#[test]
fn every_candidate_is_scored_against_one_identical_base() {
    let mut db = Db::new();
    db.seed();
    let before = db.qty(1);
    assert_eq!(before, 20);

    let plan = plan_taking(&[1, 2, 3, 4, 5], "qty >= 0").admit(AdmitPolicy::AtMost(0));
    let report = db.simulate(&plan).expect("simulation");

    let fingerprints: Vec<u64> = report
        .candidates
        .iter()
        .map(|c| c.scored.as_ref().expect("every candidate is scored").base_fingerprint)
        .collect();
    assert_eq!(fingerprints.len(), 5);
    assert!(
        fingerprints.windows(2).all(|w| w[0] == w[1]),
        "the candidates were scored against different bases: {fingerprints:x?}"
    );

    // And the base really is untouched, which is the claim the fingerprints are evidence for.
    assert_eq!(db.qty(1), before, "scoring published something");
    assert_eq!(db.qty(2), 5);
    assert!(report.admitted().is_empty(), "ADMIT 0 published a candidate");
    for c in &report.candidates {
        assert!(c.rechecked.is_none(), "{} was re-checked with no admission budget", c.name);
        assert!(c.scored.as_ref().unwrap().admissible, "{} should have scored admissible", c.name);
    }
}

/// The guard that makes the split safe: an evaluation is an optimistic read, so publishing one
/// against a base that has since moved is the TOCTOU DESIGN.md section 4 names.
///
/// **Breaking shape:** two candidates evaluated, the first published, the second published from
/// its stale evaluation. Its pending writes were computed against `qty = 20`, so publishing them
/// applies a merge that never composed with the first — 20−12 and 20−12 both land as 8 instead of
/// composing to −4, and the report says `Clean` about a database it never looked at.
#[test]
fn publishing_an_evaluation_whose_base_moved_is_refused_and_publishing_a_fresh_one_is_not() {
    let mut db = Db::new();
    db.seed();

    let a = db.runtime.begin_session("agent-a", Some("r_a"), BranchId::TRUNK).unwrap();
    let b = db.runtime.begin_session("agent-b", Some("r_b"), BranchId::TRUNK).unwrap();

    let rt = db.runtime.clone();
    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    for (branch, n) in [(a.branch, 3), (b.branch, 4)] {
        let stmt = stmts(&format!("UPDATE inventory SET qty = qty - {n} WHERE id = 1;")).remove(0);
        rt.write(&mut ctx, branch, stmt).unwrap();
    }

    let eval_a = rt.evaluate_merge(&mut ctx, a.branch, &[]).unwrap();
    let stale_b = rt.evaluate_merge(&mut ctx, b.branch, &[]).unwrap();
    assert_eq!(
        eval_a.base_fingerprint(),
        stale_b.base_fingerprint(),
        "the two evaluations were computed against different bases before anything was published"
    );

    rt.publish_evaluation(&mut ctx, eval_a).expect("the first publication is fine");

    let err = rt.publish_evaluation(&mut ctx, stale_b).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("moved") && msg.contains("fingerprint"),
        "a stale evaluation was published, or refused for the wrong reason: {msg}"
    );

    // ANTI-VACUITY: the refusal must be about staleness and not about publishing twice. A FRESH
    // evaluation of the same branch, against the base as it now stands, publishes.
    let fresh_b = rt.evaluate_merge(&mut ctx, b.branch, &[]).unwrap();
    let report = rt.publish_evaluation(&mut ctx, fresh_b).expect("a fresh evaluation must publish");
    assert!(report.applied_to_target);
    drop(ctx);
    assert_eq!(db.qty(1), 20 - 3 - 4, "the two decrements did not compose");
}

/// The other refusal: an evaluation the gate declined must not be publishable by a caller who did
/// not look at the verdict.
///
/// **Breaking shape:** a caller that evaluates, ignores `gate`, and publishes — which is exactly
/// what splitting a function into decide-then-do invites. The decision has to stay with the gate.
#[test]
fn publishing_an_evaluation_the_gate_declined_is_refused() {
    let mut db = Db::new();
    db.seed();

    let a = db.runtime.begin_session("agent-a", Some("r_a"), BranchId::TRUNK).unwrap();
    let rt = db.runtime.clone();
    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let stmt = stmts("UPDATE inventory SET qty = qty - 30 WHERE id = 1;").remove(0);
    rt.write(&mut ctx, a.branch, stmt).unwrap();

    // Declared invariant the candidate breaks: 20 − 30 = −10.
    let asserts = [ferrodb::agent_sql::Assertion::new("inventory", predicate("qty >= 0"))];
    let eval = rt.evaluate_merge(&mut ctx, a.branch, &asserts).unwrap();
    assert!(!eval.gate.is_pass(), "the assertion did not fire; nothing is being tested");
    assert!(!eval.is_admissible());

    let err = rt.publish_evaluation(&mut ctx, eval).unwrap_err();
    assert!(
        err.to_string().contains("verification gate"),
        "an evaluation the gate declined was published: {err}"
    );
    drop(ctx);
    assert_eq!(db.qty(1), 20, "the refused merge published anyway");
}

// -------------------------------------------------------------------------------------------
// Hard part 2: admission is greedy and re-checked against each admitted SET.
// -------------------------------------------------------------------------------------------

/// **THE headline case. Breaking shape: three candidates whose every PAIR composes and whose
/// TRIPLE does not.**
///
/// Three agents each take 8 from a counter of 20 under `qty >= 0`. Every candidate is legal
/// against the base (12), every pair is legal (4), and the triple is not (−4). A simulation that
/// scores once and admits everything that scored well publishes all three and leaves the invariant
/// broken with three `Clean` verdicts — which is the whole reason admission re-evaluates rather
/// than consulting the score.
///
/// This is also the case DESIGN.md section 3 says a *guard* cannot catch: `qty >= 0` as a
/// precondition tests the pre-op image (4 >= 0) and passes, which is how the demo ends at −4. The
/// assertion is evaluated against the state the merge would produce, so it fires.
#[test]
fn a_third_candidate_that_composes_with_either_of_a_pair_is_refused_against_both() {
    let mut db = Db::new();
    db.seed();

    let report = db.simulate(&plan_taking(&[8, 8, 8], "qty >= 0")).expect("simulation");

    // Every candidate scored admissible against the untouched base: 20 − 8 = 12.
    for c in &report.candidates {
        let scored = c.scored.as_ref().expect("scored");
        assert!(
            scored.admissible,
            "{} was not admissible against the base, so the test is not about composition: {:?}",
            c.name,
            scored.failed_assertions()
        );
        assert_eq!(scored.score, Some(1.0));
    }

    // And exactly two were admitted.
    let admitted: Vec<&str> = report.admitted().iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        admitted.len(),
        2,
        "expected two admissions and got {}: {:?}",
        admitted.len(),
        report
    );
    assert_eq!(db.qty(1), 4, "the two admitted decrements did not compose arithmetically");

    // The third was refused, and refused BY THE RE-CHECK rather than by its score.
    let loser = report.losers()[0];
    assert!(
        loser.refused_after_recheck(),
        "{} was not refused by the re-evaluation: scored {:?}, rechecked {:?}",
        loser.name,
        loser.scored.as_ref().map(|v| v.admissible),
        loser.rechecked.as_ref().map(|v| v.admissible)
    );
    let rechecked = loser.rechecked.as_ref().expect("the loser was re-evaluated");
    assert_eq!(
        rechecked.failed_assertions(),
        vec!["qty >= 0".to_string()],
        "the refusal did not name the violated predicate"
    );
    // Exit criterion 7's shape: the caller is handed the predicate AND the value that broke it.
    let detail = rechecked.gate.findings()[0].detail.clone();
    assert!(detail.contains("qty = -4"), "the finding does not say what the value would be: {detail}");
}

/// The re-check must work in BOTH directions, or "re-evaluate" would just mean "be stricter".
///
/// **Breaking shape:** a candidate that is illegal against the base and legal against the base
/// plus an earlier admission. Under `qty <= 10` from 20: taking 5 leaves 15 and is refused, but
/// after another candidate has taken 12 it leaves 3 and is fine. A simulation that filtered on the
/// scoring pass would drop it forever.
#[test]
fn a_candidate_the_first_score_refused_is_admitted_once_the_base_has_moved() {
    let mut db = Db::new();
    db.seed();

    let mut plan = SimulationPlan::new("pricing-agent").run("r_9");
    plan = plan.candidate("take-12", stmts("UPDATE inventory SET qty = qty - 12 WHERE id = 1;"));
    plan = plan.candidate("take-5", stmts("UPDATE inventory SET qty = qty - 5 WHERE id = 1;"));
    let plan = plan.assert_on("inventory", predicate("qty <= 10")).admit(AdmitPolicy::All);

    let report = db.simulate(&plan).expect("simulation");

    let twelve = report.get("take-12").unwrap();
    let five = report.get("take-5").unwrap();
    assert!(twelve.scored.as_ref().unwrap().admissible, "20 - 12 = 8 satisfies qty <= 10");
    assert!(
        !five.scored.as_ref().unwrap().admissible,
        "20 - 5 = 15 breaks qty <= 10, so the scoring pass must refuse it"
    );
    assert!(
        five.admitted,
        "the candidate the scoring pass refused was never re-evaluated: rechecked {:?}",
        five.rechecked.as_ref().map(|v| v.admissible)
    );
    assert!(twelve.admitted);
    assert_eq!(db.qty(1), 3, "20 - 12 - 5 did not compose");
}

// -------------------------------------------------------------------------------------------
// Exit criterion: each candidate scored by the SAME gate a production merge uses.
// -------------------------------------------------------------------------------------------

/// A candidate is refused by a gate check that has nothing to do with assertions, and an ordinary
/// `MERGE` in the same situation is refused by the same named check.
///
/// **Breaking shape:** a simulation with its own scoring path. It would score on assertions alone
/// and admit a candidate whose *premise* moved — the hospital case, where two agents each read
/// that one physician remains on call, each releases a different one, and neither write overlaps.
/// Here candidate `reader` reads row 1, candidate `writer` publishes row 1, and the read-premise
/// check refuses `reader` afterwards.
#[test]
fn a_candidate_is_scored_by_the_same_gate_a_production_merge_uses() {
    let mut db = Db::new();
    db.seed();

    let mut plan = SimulationPlan::new("ward-agent").run("r_ward");
    plan = plan.candidate("writer", stmts("UPDATE inventory SET qty = qty - 1 WHERE id = 1;"));
    plan = plan.candidate(
        "reader",
        stmts(
            "SELECT qty FROM inventory WHERE id = 1; UPDATE inventory SET qty = qty - 1 WHERE id = 2;",
        ),
    );
    let plan = plan.assert_on("inventory", predicate("qty >= 0")).admit(AdmitPolicy::All);

    let report = db.simulate(&plan).expect("simulation");

    let writer = report.get("writer").unwrap();
    let reader = report.get("reader").unwrap();
    assert!(writer.admitted, "the writer was not admitted, so no premise moved: {report}");
    assert!(
        !reader.admitted,
        "a candidate whose read premise moved was admitted anyway: {report}"
    );
    let gate = &reader.rechecked.as_ref().expect("re-evaluated").gate;
    assert!(!gate.is_pass(), "the gate passed a moved premise");
    assert_eq!(
        gate.findings()[0].check,
        "read-premise",
        "refused by something other than the read-premise check: {:?}",
        gate.findings()
    );
    assert!(
        gate.findings()[0].detail.contains("changed in the base"),
        "the finding does not explain the premise: {:?}",
        gate.findings()[0]
    );
    // Its assertions all held — this refusal is the gate's, not the assertions'.
    assert!(
        reader.rechecked.as_ref().unwrap().failed_assertions().is_empty(),
        "the assertion fired too, so this test does not isolate the gate check"
    );

    // THE CONTROL: an ordinary agent session in the same situation, through `MERGE`, is refused by
    // the same named check. Same gate, reached from the other statement.
    let mut s = db.session();
    db.ok("BEGIN AGENT SESSION AS 'ward-agent' RUN 'r_solo';", &mut s);
    db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut s);
    db.ok("UPDATE inventory SET qty = qty - 1 WHERE id = 2;", &mut s);
    let branch = s.agent.as_ref().unwrap().branch;

    // Move the premise under it, exactly as the admitted candidate did.
    let mut other = db.session();
    db.ok("BEGIN AGENT SESSION AS 'other-agent' RUN 'r_other';", &mut other);
    db.ok("UPDATE inventory SET qty = qty - 1 WHERE id = 1;", &mut other);
    match db.ok("MERGE;", &mut other) {
        Outcome::Agent(AgentOutput::Merge(m)) => assert!(m.applied_to_target),
        _ => panic!("expected a merge report"),
    }

    match db.ok("MERGE;", &mut s) {
        Outcome::Agent(AgentOutput::Merge(m)) => {
            assert!(!m.applied_to_target, "the stale-premise merge published")
        }
        _ => panic!("expected a merge report"),
    }
    let reason = db.runtime.quarantine_reason(branch).expect("the branch was held");
    assert!(
        reason.contains("read-premise"),
        "MERGE and SIMULATE were refused by different checks: {reason}"
    );
}

// -------------------------------------------------------------------------------------------
// Vacuity: an assertion that examined nothing is not a pass.
// -------------------------------------------------------------------------------------------

/// **Breaking shape:** an assertion over a table with no rows in it — the empty audit table nobody
/// has written to yet. Every predicate holds over the empty set, so every candidate scores 1.00
/// and the simulation admits work that nothing checked.
#[test]
fn an_assertion_that_examined_no_rows_blocks_admission_instead_of_passing() {
    let mut db = Db::new();
    db.seed();
    {
        let mut s = db.session();
        db.ok("CREATE TABLE audit (id INTEGER NOT NULL, note VARCHAR(32));", &mut s);
    }

    let mut plan = SimulationPlan::new("pricing-agent");
    plan = plan.candidate("take-1", stmts("UPDATE inventory SET qty = qty - 1 WHERE id = 1;"));
    // The predicate is true of every row of `audit` — there are none.
    let plan = plan.assert_on("audit", predicate("id >= 0")).admit(AdmitPolicy::All);

    let report = db.simulate(&plan).expect("simulation");
    let c = report.get("take-1").unwrap();
    assert!(!c.admitted, "a candidate was admitted on an assertion that ran over nothing");
    let scored = c.scored.as_ref().unwrap();
    assert!(
        matches!(scored.gate, GateOutcome::HardReject(_)),
        "an assertion that examined no rows produced {}",
        scored.gate.name()
    );
    assert_eq!(scored.assertions[0].rows_checked, 0);
    assert_eq!(db.qty(1), 20, "nothing may have been published");

    // ANTI-VACUITY: the same assertion over the same table, once it has a row, admits.
    {
        let mut s = db.session();
        db.ok("INSERT INTO audit VALUES (1, 'seeded');", &mut s);
    }
    let mut plan = SimulationPlan::new("pricing-agent");
    plan = plan.candidate("take-1", stmts("UPDATE inventory SET qty = qty - 1 WHERE id = 1;"));
    let plan = plan.assert_on("audit", predicate("id >= 0")).admit(AdmitPolicy::All);
    let report = db.simulate(&plan).expect("simulation");
    assert!(
        report.get("take-1").unwrap().admitted,
        "the assertion still blocks with a row present, so the refusal was not about vacuity: {report}"
    );
    assert_eq!(db.qty(1), 19);
}

/// A simulation with nothing declared scores every candidate perfectly against no evidence.
#[test]
fn a_simulation_refuses_to_run_with_no_assertions_no_candidates_or_a_duplicate_name() {
    let mut db = Db::new();
    db.seed();

    let bare = SimulationPlan::new("a")
        .candidate("c", stmts("UPDATE inventory SET qty = qty - 1 WHERE id = 1;"));
    let err = db.simulate(&bare).unwrap_err();
    assert!(err.to_string().contains("at least one ASSERT"), "got {err}");

    let empty = SimulationPlan::new("a").assert_on("inventory", predicate("qty >= 0"));
    let err = db.simulate(&empty).unwrap_err();
    assert!(err.to_string().contains("at least one CANDIDATE"), "got {err}");

    let dup = SimulationPlan::new("a")
        .candidate("same", stmts("UPDATE inventory SET qty = qty - 1 WHERE id = 1;"))
        .candidate("same", stmts("UPDATE inventory SET qty = qty - 2 WHERE id = 1;"))
        .assert_on("inventory", predicate("qty >= 0"));
    let err = db.simulate(&dup).unwrap_err();
    assert!(err.to_string().contains("both named"), "got {err}");

    assert_eq!(db.qty(1), 20, "a refused simulation published something");
}

// -------------------------------------------------------------------------------------------
// Exit criterion 8, through SIMULATE: the losers go back with no client cooperation.
// -------------------------------------------------------------------------------------------

/// **Breaking shape, and the reason the baseline has a healthy branch in it:** a reaper that
/// reclaims unconditionally passes "page count returned to baseline" while destroying live work.
/// So the baseline is measured with a long-lease branch that has written pages, and over-reaping
/// takes the count BELOW it — failing the same assertion the under-reaping case fails.
#[test]
fn the_losers_are_reaped_on_lease_expiry_with_no_client_cooperation_and_pages_return_to_baseline() {
    let mut db = Db::new();
    db.seed();
    db.ballast(400);

    // A healthy branch that is nobody's candidate: long lease, real pages, still live at the end.
    let keeper = db.runtime.begin_session("long-runner", Some("r_keep"), BranchId::TRUNK).unwrap();
    for r in 0..40u64 {
        db.runtime
            .put_row(keeper.branch, "ballast", r, &[Value::Integer(-1), Value::Varchar("kept".into())])
            .unwrap();
    }
    db.branches.renew_lease(keeper.branch, LeaseDeadline(u64::MAX)).unwrap();

    let baseline = db.pages();
    assert!(baseline > 1, "the baseline is {baseline} page(s); the measurement would be vacuous");

    // Six candidates each taking 8 from 20 under `qty >= 0`, admitting as many as compose. Two
    // fit; the other four are re-evaluated, REFUSED, and left exactly where they are. Nobody ever
    // calls ABANDON on any of them.
    //
    // The shape matters. An earlier version of this test used `ADMIT 1`, which meant the five
    // losers were never re-evaluated at all — so a mutant that routed a refused candidate to
    // quarantine (taking it out of `live_branches`, and therefore out of the lease scan, forever)
    // left this test green. Every loser here is one the admission pass actually refused.
    let plan = plan_taking(&[8, 8, 8, 8, 8, 8], "qty >= 0")
        .admit(AdmitPolicy::All)
        .lease_millis(1_000);
    let report = db.simulate(&plan).expect("simulation");
    assert_eq!(report.admitted().len(), 2, "{report}");
    assert_eq!(report.losers().len(), 4);
    assert!(
        report.losers().iter().all(|l| l.refused_after_recheck()),
        "a loser here must have been refused BY the re-check, or the reclamation claim below is \
         only about candidates nothing ever looked at"
    );

    let during = db.pages();
    assert!(
        during > baseline,
        "the candidates wrote rows and allocated no pages ({baseline} -> {during}); there would \
         be nothing for the reaper to reclaim and this test would prove nothing"
    );

    println!(
        "MEASURED (this machine, this run): baseline {baseline} pages (including a healthy \
         long-lease branch), {during} with 6 candidate branches live"
    );

    // The negative control FIRST: an unexpired lease is left alone.
    let untouched = db.reaper.reap_expired(LeaseDeadline::now_millis()).unwrap();
    assert!(untouched.is_empty(), "the reaper fired before any lease expired: {untouched:?}");
    assert_eq!(db.pages(), during, "pages were reclaimed before the leases expired");

    // No cooperation of any kind: the leases simply run out.
    let reaped = db.reaper.reap_expired(LeaseDeadline::now_millis() + 60_000).unwrap();
    db.reaper.drain_pending().ok();
    for l in report.losers() {
        assert!(reaped.contains(&l.branch), "loser {} was not reaped: {reaped:?}", l.name);
    }
    assert!(
        !reaped.contains(&keeper.branch),
        "the long-lease branch was reaped; the lease scan is reclaiming unconditionally"
    );

    assert_eq!(
        db.pages(),
        baseline,
        "pages did not return to the pre-simulation baseline. Below it means the healthy \
         long-lease branch was reaped too; above it means a candidate's pages are still charged."
    );

    // The healthy branch is not merely counted, it still answers.
    assert_eq!(
        db.runtime.get_row(keeper.branch, "ballast", 7).unwrap(),
        Some(vec![Value::Integer(-1), Value::Varchar("kept".into())]),
        "the long-lease branch lost its rows to the scan that reclaimed the losers"
    );
    // And the trunk is intact.
    // `.count()` would be wrong here: it counts `Err` items too, so a failing scan would still
    // report 400. Collecting through the `Result` keeps the assertion as strong as it was.
    assert_eq!(
        db.runtime
            .scan_rows(BranchId::TRUNK, "ballast")
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        400
    );
    assert_eq!(db.qty(1), 4, "the two admitted candidates did not compose (20 - 8 - 8)");
}

/// A losing candidate must NOT be quarantined, even though a production `MERGE` quarantines a
/// branch the gate declines.
///
/// **Breaking shape:** routing losers to quarantine "for inspection". `BranchState::Quarantined`
/// is not `Live`, and the lease scan walks live branches — so every loser of every simulation ever
/// run would pin its pages forever, and exit criterion 8 would fail for the most common workload
/// this database has.
#[test]
fn a_losing_candidate_is_left_for_the_reaper_rather_than_quarantined() {
    let mut db = Db::new();
    db.seed();

    let report = db.simulate(&plan_taking(&[8, 8, 8], "qty >= 0")).expect("simulation");
    let loser = report.losers()[0];

    assert!(
        db.runtime.quarantined_branches().unwrap().is_empty(),
        "a losing candidate was quarantined, which takes it out of the lease scan for good"
    );
    assert!(db.runtime.quarantine_reason(loser.branch).is_none());
    assert_eq!(
        db.branches.get(loser.branch).unwrap().state,
        ferrodb::branch::types::BranchState::Live,
        "a loser must stay live and leased"
    );

    let reaped = db.reaper.reap_expired(LeaseDeadline::now_millis() + 60 * 60 * 1000).unwrap();
    assert!(reaped.contains(&loser.branch), "the loser did not reach the lease scan: {reaped:?}");
}

// -------------------------------------------------------------------------------------------
// Provenance, through a simulation.
// -------------------------------------------------------------------------------------------

/// Exit criterion 9 for admitted work: the row names the CANDIDATE that wrote it, not the
/// simulation. Two candidates are two tasks; interning them under one run id would make the answer
/// "some candidate of r_9", which is not an answer.
#[test]
fn an_admitted_candidates_rows_are_attributed_to_that_candidate() {
    let mut db = Db::new();
    db.seed();

    let mut plan = SimulationPlan::new("pricing-agent").run("r_9");
    plan = plan.candidate("gentle", stmts("UPDATE inventory SET qty = qty - 1 WHERE id = 1;"));
    let plan = plan
        .assert_on("inventory", predicate("qty >= 0"))
        .admit(AdmitPolicy::AtMost(1));

    let report = db.simulate(&plan).expect("simulation");
    assert!(report.get("gentle").unwrap().admitted);

    let who = db
        .runtime
        .who_wrote_row("inventory", row_id_of(&[Value::Integer(1)]))
        .expect("the published row has no author");
    assert_eq!(who.agent_id, "pricing-agent");
    assert_eq!(
        who.run_id, "r_9/gentle",
        "the row is attributed to the simulation rather than to the candidate that wrote it"
    );
}

// -------------------------------------------------------------------------------------------
// The statement itself.
// -------------------------------------------------------------------------------------------

#[test]
fn the_simulate_statement_runs_end_to_end_through_the_sql_surface() {
    let mut db = Db::new();
    db.seed();
    let mut s = db.session();

    let sql = "SIMULATE AS 'pricing-agent' RUN 'r_9' MODEL 'claude-opus-5/2026-05' \
               CANDIDATE 'cut-8a' ( UPDATE inventory SET qty = qty - 8 WHERE id = 1; ) \
               CANDIDATE 'cut-8b' ( UPDATE inventory SET qty = qty - 8 WHERE id = 1; ) \
               CANDIDATE 'cut-8c' ( UPDATE inventory SET qty = qty - 8 WHERE id = 1; ) \
               ASSERT ON inventory (qty >= 0) \
               ADMIT ALL;";
    match db.ok(sql, &mut s) {
        Outcome::Agent(AgentOutput::Simulation(report)) => {
            assert_eq!(report.candidates.len(), 3);
            assert_eq!(report.admitted().len(), 2, "{report}");
            let rendered = format!("{report}");
            assert!(rendered.contains("ADMITTED"), "{rendered}");
            assert!(rendered.contains("qty >= 0"), "the report does not name the predicate that refused a candidate: {rendered}");
        }
        _ => panic!("expected a simulation report"),
    }
    assert_eq!(db.qty(1), 4);

    // The model reached provenance, which is criterion 9's other half.
    let who = db.runtime.who_wrote_row("inventory", row_id_of(&[Value::Integer(1)])).unwrap();
    assert_eq!(who.model, "claude-opus-5");
    assert_eq!(who.model_version, "2026-05");
}

#[test]
fn simulate_is_refused_inside_an_agent_session() {
    let mut db = Db::new();
    db.seed();
    let mut s = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r';", &mut s);

    let err = expect_err(db.exec(
        "SIMULATE AS 'b' CANDIDATE 'c' ( UPDATE inventory SET qty = qty - 1 WHERE id = 1; ) \
         ASSERT ON inventory (qty >= 0) ADMIT ALL;",
        &mut s,
    ));
    assert!(err.to_string().contains("cannot run inside an agent session"), "got {err}");
}

#[test]
fn an_assertion_on_an_unknown_table_or_column_is_refused_at_bind_time() {
    let mut db = Db::new();
    db.seed();
    let mut s = db.session();

    let err = expect_err(db.exec(
        "SIMULATE AS 'a' CANDIDATE 'c' ( UPDATE inventory SET qty = qty - 1 WHERE id = 1; ) \
         ASSERT ON nosuch (qty >= 0) ADMIT ALL;",
        &mut s,
    ));
    assert!(err.to_string().contains("unknown table in ASSERT ON"), "got {err}");

    let err = expect_err(db.exec(
        "SIMULATE AS 'a' CANDIDATE 'c' ( UPDATE inventory SET qty = qty - 1 WHERE id = 1; ) \
         ASSERT ON inventory (nosuchcol >= 0) ADMIT ALL;",
        &mut s,
    ));
    assert!(err.to_string().contains("nosuchcol"), "got {err}");
}

/// A candidate whose body fails is reported and left alone, rather than taking the simulation down
/// with it. The other candidates still run, are still scored, and can still be admitted.
#[test]
fn a_candidate_whose_body_errors_is_reported_and_never_admitted() {
    let mut db = Db::new();
    db.seed();

    let mut plan = SimulationPlan::new("pricing-agent");
    plan = plan.candidate("broken", stmts("INSERT INTO inventory VALUES (1, 99);"));
    plan = plan.candidate("fine", stmts("UPDATE inventory SET qty = qty - 1 WHERE id = 1;"));
    let plan = plan.assert_on("inventory", predicate("qty >= 0")).admit(AdmitPolicy::All);

    let report = db.simulate(&plan).expect("one broken candidate must not fail the simulation");
    let broken = report.get("broken").unwrap();
    assert!(broken.error.is_some(), "a duplicate-key INSERT was accepted");
    assert!(broken.scored.is_none(), "a candidate that did not run was scored anyway");
    assert!(!broken.admitted);
    assert!(report.get("fine").unwrap().admitted, "one broken candidate blocked the others");
    assert_eq!(db.qty(1), 19);
}

// -------------------------------------------------------------------------------------------
// Findings from the adversarial review of this diff, each pinned by the test that would have
// caught it.
// -------------------------------------------------------------------------------------------

/// **Breaking shape:** `BEGIN; SIMULATE …; ROLLBACK;`. Agent statements are dispatched before the
/// executor reaches its transaction arms, and admitting a candidate publishes it in its own
/// transaction — so the admissions commit and the ROLLBACK cannot undo them. The client sees a
/// block it rolled back that permanently changed the database.
#[test]
fn simulate_is_refused_inside_a_transaction_block_because_it_would_escape_the_rollback() {
    let mut db = Db::new();
    db.seed();
    let mut s = db.session();
    db.ok("BEGIN;", &mut s);

    let err = expect_err(db.exec(
        "SIMULATE AS 'a' CANDIDATE 'c' ( UPDATE inventory SET qty = qty - 8 WHERE id = 1; ) \
         ASSERT ON inventory (qty >= 0) ADMIT ALL;",
        &mut s,
    ));
    assert!(
        err.to_string().contains("cannot SIMULATE inside a transaction block"),
        "got {err}"
    );
    db.ok("ROLLBACK;", &mut s);
    assert_eq!(db.qty(1), 20, "the simulation published past the transaction block");

    // ANTI-VACUITY: outside a block the same statement runs and publishes.
    let mut s2 = db.session();
    db.ok(
        "SIMULATE AS 'a' CANDIDATE 'c' ( UPDATE inventory SET qty = qty - 8 WHERE id = 1; ) \
         ASSERT ON inventory (qty >= 0) ADMIT ALL;",
        &mut s2,
    );
    assert_eq!(db.qty(1), 12);
}

/// **Breaking shape:** a branch that READS one table and WRITES another, whose premise moves
/// between evaluation and publication. The fingerprint covered only the tables the branch wrote
/// (a read does not put its table into the workspace's table map), so the read-premise check's
/// `Pass` went stale while the fingerprint still matched — and that is the exact case the check
/// exists for: two agents each read that one physician remains on call, each releases a different
/// one, and neither write overlaps.
#[test]
fn an_evaluation_whose_read_premise_moved_is_refused_even_though_it_wrote_a_different_table() {
    let mut db = Db::new();
    db.seed();
    {
        let mut s = db.session();
        db.ok("CREATE TABLE roster (id INTEGER NOT NULL, staff INTEGER);", &mut s);
        db.ok("INSERT INTO roster VALUES (1, 3);", &mut s);
    }

    // The reader: reads `inventory` row 1 by exact version, writes `roster`.
    let reader = db.runtime.begin_session("ward-a", Some("r_a"), BranchId::TRUNK).unwrap();
    // The mover: writes `inventory` row 1 and publishes it.
    let mover = db.runtime.begin_session("ward-b", Some("r_b"), BranchId::TRUNK).unwrap();

    let rt = db.runtime.clone();
    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };

    let select = stmts("SELECT qty FROM inventory WHERE id = 1;").remove(0);
    rt.select(&ctx.read(), reader.branch, &select, Some(reader.branch)).unwrap();
    rt.write(&mut ctx, reader.branch, stmts("UPDATE roster SET staff = 2 WHERE id = 1;").remove(0))
        .unwrap();
    rt.write(
        &mut ctx,
        mover.branch,
        stmts("UPDATE inventory SET qty = qty - 1 WHERE id = 1;").remove(0),
    )
    .unwrap();

    // Scored while the premise still holds: the gate passes.
    let stale = rt.evaluate_merge(&mut ctx, reader.branch, &[]).unwrap();
    assert!(stale.gate.is_pass(), "the premise had not moved yet: {:?}", stale.gate);
    assert!(stale.is_admissible());

    // The premise moves: `inventory` row 1 gets a published version.
    let moved = rt.evaluate_merge(&mut ctx, mover.branch, &[]).unwrap();
    rt.publish_evaluation(&mut ctx, moved).expect("the mover publishes");

    let err = rt.publish_evaluation(&mut ctx, stale).unwrap_err();
    assert!(
        err.to_string().contains("moved"),
        "an evaluation whose read premise moved was published: {err}"
    );
    drop(ctx);

    // ANTI-VACUITY: re-evaluated now, the branch is REFUSED BY THE GATE rather than by the
    // fingerprint — which is what the fingerprint was protecting.
    let bp = db.bp.clone();
    let txn = db.txn.clone();
    let mut ctx = ExecCtx { catalog: &mut db.catalog, bp, txn };
    let fresh = rt.evaluate_merge(&mut ctx, reader.branch, &[]).unwrap();
    assert!(!fresh.gate.is_pass(), "the read-premise check did not fire on re-evaluation");
    assert_eq!(fresh.gate.findings()[0].check, "read-premise");
}

/// `rows_written` must mean rows written. Folding a `SELECT`'s row count into it made every
/// reading candidate report rows it never wrote, and `rows_written == 0` unusable as "this
/// candidate changed nothing".
#[test]
fn a_read_only_candidate_reports_no_rows_written() {
    let mut db = Db::new();
    db.seed();

    let mut plan = SimulationPlan::new("pricing-agent").run("r_9");
    plan = plan.candidate("look", stmts("SELECT qty FROM inventory WHERE id = 1;"));
    plan = plan.candidate(
        "act",
        stmts("SELECT qty FROM inventory WHERE id = 1; UPDATE inventory SET qty = qty - 1 WHERE id = 1;"),
    );
    let plan = plan.assert_on("inventory", predicate("qty >= 0")).admit(AdmitPolicy::AtMost(0));

    let report = db.simulate(&plan).expect("simulation");
    let look = report.get("look").unwrap();
    assert_eq!(look.rows_written, 0, "a candidate that only reads reported writes");
    assert_eq!(look.rows_read, 1, "the read was not counted at all");
    let act = report.get("act").unwrap();
    assert_eq!(act.rows_written, 1);
    assert_eq!(act.rows_read, 1);
}

/// The builder must default to the value that publishes nothing. A caller who forgets `.admit()`
/// gets a scored report, not every admissible candidate merged into the shared tables.
#[test]
fn a_plan_that_never_says_admit_publishes_nothing() {
    let mut db = Db::new();
    db.seed();

    let plan = SimulationPlan::new("pricing-agent")
        .candidate("take-1", stmts("UPDATE inventory SET qty = qty - 1 WHERE id = 1;"))
        .assert_on("inventory", predicate("qty >= 0"));

    let report = db.simulate(&plan).expect("simulation");
    assert!(report.get("take-1").unwrap().scored.is_some(), "it must still be scored");
    assert!(report.admitted().is_empty(), "the default admitted a candidate");
    assert_eq!(db.qty(1), 20);
}

/// The lease reaper reclaims a loser's pages without telling the runtime, so its workspace, its
/// `b_N` name and any escrow it held would stay in memory for the life of the process. A server
/// running simulations all day is the workload where that matters, and it is the workload this
/// feature is for.
#[test]
fn state_for_branches_the_reaper_took_is_dropped_rather_than_retained_forever() {
    let mut db = Db::new();
    db.seed();

    let plan = plan_taking(&[8, 8, 8], "qty >= 0").admit(AdmitPolicy::AtMost(0)).lease_millis(1_000);
    let first = db.simulate(&plan).expect("simulation");
    assert_eq!(first.candidates.len(), 3);
    // Every branch is still live, so nothing may be forgotten yet.
    assert_eq!(
        db.runtime.forget_reaped_branches(),
        0,
        "a live branch's state was dropped; a loser stays queryable while its lease runs"
    );
    for c in &first.candidates {
        assert!(db.runtime.run_of(c.branch).is_some(), "{} lost its workspace early", c.name);
    }

    // No cooperation: the leases expire and the reaper takes them.
    let reaped = db.reaper.reap_expired(LeaseDeadline::now_millis() + 60_000).unwrap();
    assert_eq!(reaped.len(), 3);

    assert_eq!(
        db.runtime.forget_reaped_branches(),
        3,
        "the runtime kept the workspaces of branches the reaper had already reclaimed"
    );
    for c in &first.candidates {
        assert!(db.runtime.run_of(c.branch).is_none(), "{}'s workspace survived", c.name);
    }
    // Idempotent, and the second call finds nothing left to do.
    assert_eq!(db.runtime.forget_reaped_branches(), 0);
}

/// **A stated limit, pinned so it cannot become a silent one.** Escrow and SIMULATE do not
/// compose: `claim_escrow` is per branch, a candidate's branch is created inside `simulate`, and
/// nothing can claim for it. A candidate that writes an escrow-bounded cell therefore fails at
/// WRITE time — which is escrow working exactly as designed — and the simulation reports the
/// candidate as errored rather than admitting it or silently overdrawing.
#[test]
fn a_candidate_writing_an_escrow_bounded_cell_errors_visibly_rather_than_overdrawing() {
    let mut db = Db::new();
    db.seed();
    db.runtime
        .open_escrow("inventory", row_id_of(&[Value::Integer(1)]), ferrodb::tel::ids::ColId(1), 20)
        .unwrap();

    let report = db.simulate(&plan_taking(&[8, 8], "qty >= 0")).expect("simulation");
    for c in &report.candidates {
        let err = c.error.as_ref().unwrap_or_else(|| {
            panic!("{} wrote a bounded cell with no escrow claim and was not refused", c.name)
        });
        assert!(err.contains("escrow"), "refused for the wrong reason: {err}");
        assert!(!c.admitted);
    }
    assert_eq!(db.qty(1), 20, "an unclaimed overdraw reached the shared tables");
}

/// `with_reaper` takes a `Reaper` it cannot check: the trait exposes no catalog to compare against
/// the runtime's own. What it CAN do is check the result — after a successful reap, this runtime's
/// catalog must no longer hold the branch as live.
///
/// **Breaking shape:** two runtimes over two catalogs, each of which mints branch id 2. A reaper
/// built over the wrong one reaps a record that is not ours, reports success, and our branch is
/// retired in name only with its pages still charged to it. Silent, and permanent.
#[test]
fn a_reaper_wired_to_a_different_catalog_is_caught_at_the_first_seal() {
    let dir = tempfile::tempdir().unwrap();
    let mk = |name: &str| {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join(name))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let branches = Arc::new(LogBranchCatalog::in_memory(1));
        let store =
            Arc::new(ArenaPageStore::new(bp.clone(), Arc::clone(&branches) as std::sync::Arc<dyn ferrodb::branch::BranchCatalog>, ARENA_BASE).unwrap());
        (bp, branches, store)
    };
    let (bp_a, cat_a, store_a) = mk("a.db");
    let (_bp_b, cat_b, store_b) = mk("b.db");

    // The reaper belongs to catalog B; the runtime to catalog A.
    let wrong = Arc::new(TwoTierReaper::new(Arc::clone(&cat_b) as std::sync::Arc<dyn ferrodb::branch::BranchCatalog>, Arc::clone(&store_b)));
    let rt = AgentRuntime::with_storage(
        Arc::clone(&cat_a) as Arc<dyn BranchCatalog>,
        Arc::new(MemEffectLog::new()),
        Arc::clone(&store_a) as Arc<dyn PageStore>,
    )
    .unwrap()
    .with_reaper(Arc::clone(&wrong) as Arc<dyn Reaper>);

    // Both catalogs mint the same id for their first fork, which is what makes the mis-wiring
    // reap something rather than simply error.
    let mine = rt.begin_session("a", Some("r"), BranchId::TRUNK).unwrap();
    let theirs = cat_b
        .fork(BranchId::TRUNK, ferrodb::branch::types::LeaseDeadline::from_now(60_000))
        .unwrap();
    assert_eq!(mine.branch.id, theirs.branch_id.id, "the two catalogs did not agree on the id");

    let err = rt.abandon(mine.branch).unwrap_err();
    assert!(
        err.to_string().contains("different branch catalog"),
        "a reaper over the wrong catalog was not caught: {err}"
    );
    // And the branch is still live here, which is the state the message describes.
    assert_eq!(
        cat_a.get(mine.branch).unwrap().state,
        ferrodb::branch::types::BranchState::Live
    );
    let _ = bp_a;
}
