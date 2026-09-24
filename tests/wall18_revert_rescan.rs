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
//! | quantity                          | full-graph planner | walk (`CaptureSet`) | N = 8 / 64      |
//! |-----------------------------------|--------------------|---------------------|-----------------|
//! | applied entries examined          | 1 (index, `5c15873`) | 1                 | 1 / 1           |
//! | applied entries matched           | 1                  | 1                   | control         |
//! | captures the planner consulted    | N (all, folded)    | 1 (the target)      | 8/64, then 1/1  |
//! | graph pairs compared (`build`)    | 3N²                | 0                   | 192/12,288, then 0/0 |
//! | index candidates examined (walk)  | 0                  | 1                   | 0/0, then 1/1   |
//!
//! `3N²` is N captures × 3 valued writes each (`record_applied` records the post image's two
//! cells and the pre image's changed `qty`) against N predicate reads (one row-targeting read per
//! `UPDATE ... WHERE id = i`), with no exact reads at all. The exponent, 64 = (64 / 8)², does not
//! depend on the constant 3; the constant is a reading of `record_applied`, not a measurement.
//! The walk's single candidate is the target's own `id = 1` targeting read, found in the point
//! bucket for its col-0 write and rejected as itself.
//!
//! The first test is the failing-first one for the applied-log index. The second is the
//! failing-first one for the graph term. It was `#[ignore]`d while the walk was only designed; it is
//! now live, and it compares the candidate count as well, because captures alone cannot tell an
//! indexed walk from one that scans every read in the table. The last three pin the ANSWER REVERT
//! gives for the three kinds of read the walk indexes (an exact version, a point on the key, an
//! unbounded scan), including a second hop. They pass on the full-graph planner first, which is
//! what makes them a fair check of its replacement.

use std::fs::OpenOptions;
use std::sync::{Arc, Mutex};

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::{
    revert_applied_counters, revert_graph_candidates, revert_graph_captures, AgentRuntime,
};
use ferrodb::agent_sql::MergeReport;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::provenance::revert::{graph_build_pairs, RevertPlan};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::ids::TxnId;
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

fn plan_of(out: Outcome) -> RevertPlan {
    match out {
        Outcome::Agent(AgentOutput::Revert(p)) => p,
        Outcome::Agent(other) => panic!("expected a revert plan, got {}", other),
        _ => panic!("expected an agent output"),
    }
}

/// `inventory (id, qty)` holding rows `1..=rows`, every `qty` 100, written outside any agent
/// session so nothing is retained.
fn seeded(rows: usize) -> Db {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
    for i in 1..=rows {
        db.ok(&format!("INSERT INTO inventory VALUES ({}, 100);", i), &mut s);
    }
    db
}

/// A new connection with an agent session open on it. Sessions are txn 1, 2, 3, ... in the order
/// they begin — the numbering `provenance_scan_cascade.rs` asserts on.
fn agent_session(db: &mut Db, name: &str) -> Session {
    let mut a = db.session();
    db.ok(&format!("BEGIN AGENT SESSION AS '{name}' RUN 'r_{name}';"), &mut a);
    a
}

/// `MERGE` the session's task, insisting it landed, and return the merge id.
fn merge(db: &mut Db, a: &mut Session) -> String {
    let m = report(db.ok("MERGE;", a));
    assert!(m.applied_to_target, "merge did not land: {}", m);
    m.merge_id
}

/// What one `REVERT MERGE m_1` cost, after `n` merges, and whether it did its job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Reading {
    n: usize,
    applied_examined: u64,
    applied_matched: u64,
    graph_captures: u64,
    graph_pairs: u64,
    graph_candidates: u64,
}

/// `n` tasks, each forking, decrementing ITS OWN row by primary key and merging; then one revert of
/// the first merge, with the counters read around that one statement and nothing else.
///
/// Every task touches a different row and reads nothing by value, so the dependency graph has no
/// edge out of `m_1`: the revert is not blocked and cascades through nothing. That is what makes
/// `matched` a constant and the undo a single transaction at every `n`.
fn reading(n: usize) -> Reading {
    let mut db = seeded(n);
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
    let (c0, p0, k0) = (revert_graph_captures(), graph_build_pairs(), revert_graph_candidates());
    let plan = plan_of(db.ok("REVERT MERGE m_1;", &mut main));
    let (e1, m1) = revert_applied_counters();
    let (c1, p1, k1) = (revert_graph_captures(), graph_build_pairs(), revert_graph_candidates());

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
        graph_candidates: k1 - k0,
    }
}

fn readings() -> (Reading, Reading) {
    let small = reading(SMALL);
    let large = reading(LARGE);
    // Printed unconditionally: every field is pre-registered at each commit of this lane, including
    // the ones a given commit cannot move, and a control that is not recorded cannot be checked.
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
        "the reverted transaction should own exactly one applied op at both sizes: \
         {small:?} {large:?}"
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

/// **The failing-first test for the graph term.**
///
/// Pre-registered (lane report Amendment 1): RED on the full-graph planner, (captures, pairs,
/// candidates) = (8, 192, 0) at N = 8 against (64, 12288, 0) at N = 64; GREEN on the walk, (1, 0, 1)
/// at both. For this fixture the revert's answer is the target alone at every N, so a planner whose
/// work depends on history rather than on the answer fails here.
///
/// The candidate count is what makes this discriminate. Counting captures alone, a walk that visits
/// only the target but then compares its writes against every read in the table reads 1 at both
/// sizes and passes; its candidates read 8 and 64. `captures >= 1` keeps a planner that stops
/// counting from passing vacuously: any correct planner consults at least the target's capture.
#[test]
fn revert_graph_work_does_not_grow_with_merge_history() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (small, large) = readings();
    assert!(
        small.graph_captures >= 1,
        "the planner consulted no capture at all, not even the target's: {small:?}"
    );
    assert_eq!(
        (small.graph_captures, small.graph_pairs, small.graph_candidates),
        (large.graph_captures, large.graph_pairs, large.graph_candidates),
        "REVERT's dependency work grew with merge history: captures {} -> {}, pairs {} -> {}, \
         candidates {} -> {} from N={} to N={}, for a revert whose answer is the same at both",
        small.graph_captures,
        large.graph_captures,
        small.graph_pairs,
        large.graph_pairs,
        small.graph_candidates,
        large.graph_candidates,
        small.n,
        large.n,
    );
}

/// **The answer through an exact read, one hop and two.**
///
/// A (txn 1) writes row 1 and merges. B (txn 2) reads row 1 BY KEY — an exact read of A's version —
/// then writes row 2 and merges. C (txn 3) reads row 2 by key, which is B's version, and stays open.
/// C read nothing A wrote, so it depends on A only THROUGH B: reverting A must name both, and
/// reverting B must name C alone. A planner that stops after the first hop names `[2]` for A.
#[test]
fn a_two_hop_chain_is_named_through_its_middle() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut db = seeded(3);

    let mut a = agent_session(&mut db, "a");
    db.ok("UPDATE inventory SET qty = qty - 1 WHERE id = 1;", &mut a);
    let m_a = merge(&mut db, &mut a);

    let mut b = agent_session(&mut db, "b");
    db.ok("SELECT qty FROM inventory WHERE id = 1;", &mut b);
    db.ok("UPDATE inventory SET qty = qty - 1 WHERE id = 2;", &mut b);
    let m_b = merge(&mut db, &mut b);

    let mut c = agent_session(&mut db, "c");
    db.ok("SELECT qty FROM inventory WHERE id = 2;", &mut c);

    let mut main = db.session();
    let from_b = plan_of(db.ok(&format!("REVERT MERGE {m_b};"), &mut main));
    assert_eq!(from_b.blocked_by, vec![TxnId(3)], "C read B's version of row 2: {from_b:?}");
    let from_a = plan_of(db.ok(&format!("REVERT MERGE {m_a};"), &mut main));
    assert_eq!(
        from_a.blocked_by,
        vec![TxnId(2), TxnId(3)],
        "B read A's row 1 and C read B's row 2, so both are downstream of A: {from_a:?}"
    );
    assert_eq!(db.qty_of(1), Value::Integer(99), "a halted revert changes nothing");
}

/// **The answer through a point on the key.**
///
/// A (txn 1) writes row 1 and merges. D (txn 2) then runs `UPDATE ... WHERE id = 1`: it named the
/// row A last wrote, and that row-targeting read is retained as a point on column 0. G (txn 3) does
/// the same to row 2, which A never touched — the control. Both stay open.
#[test]
fn a_write_that_names_a_row_by_key_depends_on_the_merge_that_last_wrote_it() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut db = seeded(2);

    let mut a = agent_session(&mut db, "a");
    db.ok("UPDATE inventory SET qty = qty - 1 WHERE id = 1;", &mut a);
    let m_a = merge(&mut db, &mut a);

    let mut d = agent_session(&mut db, "d");
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 1;", &mut d);
    let mut g = agent_session(&mut db, "g");
    db.ok("UPDATE inventory SET qty = qty + 1 WHERE id = 2;", &mut g);

    let mut main = db.session();
    let p = plan_of(db.ok(&format!("REVERT MERGE {m_a};"), &mut main));
    assert_eq!(p.blocked_by, vec![TxnId(2)], "D named row 1 after A wrote it; G named row 2: {p:?}");
}

/// **The answer through an unbounded scan, on both sides of the write.**
///
/// F (txn 1) scans the whole table before anything is published, at `observed_at` 1. A (txn 2)
/// writes row 1 and merges, stamping `begin_ts` 1. E (txn 3) scans the whole table afterwards, at
/// `observed_at` 2. Both scans cover row 1; only E's snapshot admitted A's version
/// (`begin_ts < observed_at`). Both stay open.
#[test]
fn a_full_scan_depends_on_every_merge_published_before_it_and_no_later_one() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut db = seeded(2);

    let mut f = agent_session(&mut db, "f");
    db.ok("SELECT id, qty FROM inventory;", &mut f);

    let mut a = agent_session(&mut db, "a");
    db.ok("UPDATE inventory SET qty = qty - 1 WHERE id = 1;", &mut a);
    let m_a = merge(&mut db, &mut a);

    let mut e = agent_session(&mut db, "e");
    db.ok("SELECT id, qty FROM inventory;", &mut e);

    let mut main = db.session();
    let p = plan_of(db.ok(&format!("REVERT MERGE {m_a};"), &mut main));
    assert_eq!(p.blocked_by, vec![TxnId(3)], "E scanned after A published, F before: {p:?}");
}
