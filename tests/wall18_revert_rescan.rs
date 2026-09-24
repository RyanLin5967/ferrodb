//! Wall #18 — `REVERT` rescans the applied log and rebuilds the dependency graph, per call.
//!
//! # The defect
//!
//! `AgentRuntime::revert_merge` does two things whose cost is set by how many merges have EVER
//! happened, not by the merge being reverted:
//!
//! * `undo_txn` finds one transaction's ops with `state.applied.iter().filter(|a| a.txn == txn)`.
//!   `State::applied` is never pruned and gains one entry per cell any `MERGE` published, so this
//!   is O(|applied|) per reverted transaction — D86's defect, on one of the two readers D86's index
//!   said it did not serve.
//! * `dependency_graph_of(&state.captures)` clones every retained capture and hands them to
//!   `DependencyGraphBuilder::build`, which joins every write against every read with nested
//!   loops. `captures` keeps every PUBLISHED transaction for the life of the process, so for N
//!   merged tasks of one fixed shape that is Θ(N) captures and Θ(N²) comparisons per revert.
//!
//! # Why counts and not a stopwatch
//!
//! Every quantity here is an integer that control flow fixes: one fork, one single-cell `UPDATE`
//! and one `MERGE` per task, then one `REVERT` of the FIRST merge, which nothing read and which
//! therefore reverts without cascading. A loaded box cannot move any of them.
//!
//! # What is pre-registered (lane report `frontier/lane_wall18_revert.md` in `artie-research`)
//!
//! Per `REVERT MERGE m_1` after N merges, one op each:
//!
//! | quantity                       | before the index | after the index | N = 8 → 64          |
//! |--------------------------------|------------------|-----------------|---------------------|
//! | applied entries examined       | N                | 1               | 8 → 64, then 1 → 1  |
//! | applied entries matched        | 1                | 1               | control, never moves|
//! | captures folded into the graph | N                | N               | 8 → 64 (unfixed)    |
//! | graph pairs compared           | 3N²              | 3N²             | 192 → 12,288 (unfixed) |
//!
//! `3N²` is N captures × 3 valued writes each (`record_applied` records the post image's two
//! cells and the pre image's changed `qty`) against N predicate reads (one row-targeting read per
//! `UPDATE ... WHERE id = i`), with no exact reads at all. The exponent, 64 = (64 / 8)², does not
//! depend on the constant 3; the constant is a reading of `record_applied`, not a measurement.
//!
//! The first test is the failing-first one for this lane's index. The second is the failing-first
//! one for the graph term, which this lane designed and did not build, so it is `#[ignore]`d with
//! its reason; its assertion is the property the design must deliver.

use std::fs::OpenOptions;
use std::sync::{Arc, Mutex};

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::{revert_applied_counters, revert_graph_captures, AgentRuntime};
use ferrodb::agent_sql::MergeReport;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::provenance::revert::graph_build_pairs;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// The counters are process-wide statics and the test harness runs a file's tests on parallel
/// threads, so two tests reading deltas at once would each see the other's `REVERT`. One lock,
/// held for a whole reading, makes a delta mean one statement.
static SERIAL: Mutex<()> = Mutex::new(());

/// The two history sizes. Far enough apart that a linear term and a flat one cannot be confused
/// (8x), small enough that the quadratic arm stays cheap in a debug build.
const SMALL: usize = 8;
const LARGE: usize = 64;

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
            .open(dir.path().join("wall18.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("wall18.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        Db { catalog, bp, txn, runtime: Arc::new(AgentRuntime::new()), _dir: dir }
    }

    /// A connection sharing this database's agent runtime, so branches are mutually visible.
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

    fn qty_of(&mut self, id: usize) -> Value {
        let mut s = self.session();
        match self.ok(&format!("SELECT qty FROM inventory WHERE id = {};", id), &mut s) {
            Outcome::Rows(r) => {
                assert_eq!(r.len(), 1, "row {} missing", id);
                r[0][0].clone()
            }
            _ => panic!("expected rows"),
        }
    }
}

fn report(out: Outcome) -> MergeReport {
    match out {
        Outcome::Agent(AgentOutput::Merge(m)) => m,
        Outcome::Agent(other) => panic!("expected a merge report, got {}", other),
        _ => panic!("expected an agent output"),
    }
}

/// What one `REVERT MERGE m_1` cost, after `n` merges, and whether it did its job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Reading {
    n: usize,
    applied_examined: u64,
    applied_matched: u64,
    graph_captures: u64,
    graph_pairs: u64,
}

/// `n` tasks, each forking, decrementing ITS OWN row by primary key and merging; then one revert of
/// the first merge, with the counters read around that one statement and nothing else.
///
/// Every task touches a different row and reads nothing by value, so the dependency graph has no
/// edge out of `m_1`: the revert is not blocked and cascades through nothing. That is what makes
/// `matched` a constant and the undo a single transaction at every `n`.
fn reading(n: usize) -> Reading {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
    for i in 1..=n {
        db.ok(&format!("INSERT INTO inventory VALUES ({}, 100);", i), &mut s);
    }
    for i in 1..=n {
        let mut a = db.session();
        db.ok(&format!("BEGIN AGENT SESSION AS 'a{i}' RUN 'r{i}';"), &mut a);
        db.ok(&format!("UPDATE inventory SET qty = qty - 1 WHERE id = {i};"), &mut a);
        let m = report(db.ok("MERGE;", &mut a));
        assert!(m.applied_to_target, "merge {} of {} did not land: {}", i, n, m);
    }
    assert_eq!(db.qty_of(1), Value::Integer(99), "m_1's write is not visible before the revert");

    let mut main = db.session();
    let (e0, m0) = revert_applied_counters();
    let (c0, p0) = (revert_graph_captures(), graph_build_pairs());
    let plan = match db.ok("REVERT MERGE m_1;", &mut main) {
        Outcome::Agent(AgentOutput::Revert(p)) => p,
        Outcome::Agent(other) => panic!("expected a revert plan, got {}", other),
        _ => panic!("expected an agent output"),
    };
    let (e1, m1) = revert_applied_counters();
    let (c1, p1) = (revert_graph_captures(), graph_build_pairs());

    // The revert must have DONE the thing whose cost is being counted. A blocked plan reverts
    // nothing, calls no `undo_txn`, and would read as a perfectly flat `examined` of 0.
    assert!(!plan.is_blocked(), "n={n}: m_1 has no reader, yet the revert halted: {plan:?}");
    assert!(plan.cascade.is_empty(), "n={n}: m_1 has no reader, yet it cascaded: {plan:?}");
    assert_eq!(db.qty_of(1), Value::Integer(100), "n={n}: m_1 was not undone");
    // And ONLY that: the other tasks' rows are untouched.
    assert_eq!(db.qty_of(n), Value::Integer(99), "n={n}: the revert touched row {n}");

    Reading {
        n,
        applied_examined: e1 - e0,
        applied_matched: m1 - m0,
        graph_captures: c1 - c0,
        graph_pairs: p1 - p0,
    }
}

fn readings() -> (Reading, Reading) {
    let small = reading(SMALL);
    let large = reading(LARGE);
    // Printed unconditionally: the graph arm is a pre-registered CONTROL for this lane's commit
    // (the index cannot move it), and a control that is not recorded cannot be checked.
    eprintln!("wall18 {:?}", small);
    eprintln!("wall18 {:?}", large);
    (small, large)
}

/// **The failing-first test for this lane's index.** Red on the unfixed tree with
/// `examined = 8` and `64` against `matched = 1` and `1`; green once `undo_txn` asks an index.
#[test]
fn revert_finds_one_txns_ops_without_walking_the_whole_applied_log() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (small, large) = readings();

    // The fixture's own premise, and the anti-vacuity half: one single-cell UPDATE per task means
    // exactly one op for the reverted transaction, at every history size. If this moves, the
    // fixture changed shape and nothing below means what it says.
    assert_eq!(
        (small.applied_matched, large.applied_matched),
        (1, 1),
        "the reverted transaction should own exactly one applied op at both sizes: {small:?} {large:?}"
    );
    assert_eq!(
        (small.applied_examined, large.applied_examined),
        (small.applied_matched, large.applied_matched),
        "REVERT walked applied entries that are not the reverted transaction's: it examined {} \
         at N={} and {} at N={} to find {} and {} ops. Examined should equal matched; growing \
         with N is the whole-log rescan (wall #18). {small:?} {large:?}",
        small.applied_examined,
        small.n,
        large.applied_examined,
        large.n,
        small.applied_matched,
        large.applied_matched,
    );
}

/// **The failing-first test for the graph term, which is DESIGNED, NOT BUILT.**
///
/// Pre-registered red on this tree: captures 8 → 64 and pairs 192 → 12,288. The design in the lane
/// report answers `REVERT` from indexes maintained as captures change, walking only the target and
/// its dependents; for this fixture that is the target alone at every N, so both readings must be
/// the same at N = 8 and N = 64. `captures >= 1` keeps a rewrite that stops counting from passing
/// vacuously: any correct planner consults at least the target's own capture.
#[test]
#[ignore = "wall #18 graph term is OPEN: designed in artie-research frontier/lane_wall18_revert.md, \
            not built. Run with --ignored to see the pre-registered red (captures 8 -> 64, pairs \
            192 -> 12288)"]
fn revert_graph_work_does_not_grow_with_merge_history() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (small, large) = readings();
    assert!(
        small.graph_captures >= 1,
        "the planner consulted no capture at all, not even the target's: {small:?}"
    );
    assert_eq!(
        (small.graph_captures, small.graph_pairs),
        (large.graph_captures, large.graph_pairs),
        "REVERT's dependency work grew with merge history: captures {} -> {}, pairs {} -> {} \
         from N={} to N={}, for a revert whose answer is the same at both",
        small.graph_captures,
        large.graph_captures,
        small.graph_pairs,
        large.graph_pairs,
        small.n,
        large.n,
    );
}
