//! D174 — does the cluster merge gate have ANY branch-locality?
//!
//! Pre-registration: `artie-research/bench/d174_prereg.txt`, cut before any measurement here, plus
//! Amendment 1 appended before the first run. Read it first; this file is its instrument.
//!
//! # The claim on trial
//!
//! `BranchLedger::merge_verdict` (src/agent_sql/cluster.rs:617) re-evaluates a merge when
//! `self.last_base_move > base_round`. `last_base_move` is ONE GLOBAL SCALAR, set to `round` on
//! every applied merge (cluster.rs:544) and on every other base-moving command (cluster.rs:461).
//! So a merge is re-evaluated because *some other* merge committed — whether or not the two
//! touched anything in common.
//!
//! # The instrument
//!
//! `ClusterAgents::cost().reevaluations` — `ConsensusCost.reevaluations` (cluster.rs:804),
//! incremented at cluster.rs:1177. An INTEGER COUNTER out of a deterministic state machine. It is
//! not a timer, it does not move with machine load, and it needs neither a measure lock nor a
//! quiet box. Said here so the next reader does not "correct" this into a timed run.
//!
//! # How concurrency is realised, and why that is not a distortion
//!
//! `ClusterAgents::merge` takes `&mut ExecCtx` holding `&mut Catalog`, so two merges cannot run in
//! parallel threads in one process — cluster.rs:1150 says so itself. Concurrency is SCRIPTED,
//! which is the method `tests/integration_cluster_agents.rs:13-18` defends for exactly this
//! window. The independent variable is one line:
//!
//! ```text
//! cluster.rs:1120   let base_round = self.repl.committed_head();
//! ```
//!
//! C merges are concurrent exactly when their C gates read the same committed head. `Scripted`
//! below returns a stale head R to each merge's FIRST attempt and the true head to every retry —
//! which is what the code does, and what a node whose view of the log lags the leader really sees.
//! Nothing else is altered.
//!
//! # Why arm A vs arm C would be a tautology on its own
//!
//! `BranchOp::Merge { branch, base_round }` carries NO table. Arms A and C therefore produce
//! byte-identical logs and A == C is guaranteed by the command's type alone. That is why
//! `fine_check_separates_disjoint_from_overlapping_merges` is in this file: it asks whether a
//! locality signal exists ANYWHERE in the engine. Without it, "no locality" is a statement about a
//! struct definition rather than a measurement.

use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use ferrodb::agent_sql::cluster::{
    BranchLedger, ClusterAgents, ClusterBranchId, ClusterSession, Replicated,
};
use ferrodb::agent_sql::runtime::{AgentRuntime, ExecCtx, RunIdentity};
use ferrodb::branch::types::BranchId;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::consensus::{BranchOp, Command, Entry, NodeId, Round};
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

const N1: NodeId = NodeId(1);

/// The C values the pre-registration fixes. Not a sweep I chose after seeing anything.
const CS: [usize; 4] = [2, 4, 8, 16];

fn entry(round: Round, command: Command) -> Entry {
    Entry { term: 1, round, command }
}

fn merge_op(branch: ClusterBranchId, base_round: Round) -> Command {
    Command::Branch { op: BranchOp::Merge { branch: branch.0, base_round } }
}

// =================================================================================================
// The database. Copied from tests/integration_cluster_agents.rs:549 rather than reinvented; the
// only difference is a seed that takes an arm, because the arms differ in nothing else.
// =================================================================================================

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Db {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("d174.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("d174.wal")).unwrap());
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
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("{sql} failed: {e}"))
    }

    fn on_branch(&mut self, cs: &ClusterSession, sql: &str) {
        let mut s = self.session();
        s.agent = Some(cs.session.clone());
        self.ok(sql, &mut s);
    }

    fn qty(&mut self, table: &str, id: i64) -> Option<i64> {
        let mut s = self.session();
        let sql = format!("SELECT qty FROM {table} WHERE id = {id};");
        match self.ok(&sql, &mut s) {
            Outcome::Rows(rows) => rows.first().and_then(|r| match r.first() {
                Some(Value::Integer(i)) => Some(*i as i64),
                _ => None,
            }),
            _ => panic!("expected rows from: {sql}"),
        }
    }

    fn ctx(&mut self) -> ExecCtx<'_> {
        let bp = self.bp.clone();
        let txn = self.txn.clone();
        ExecCtx { catalog: &mut self.catalog, bp, txn }
    }
}

// =================================================================================================
// The scripted log. Copied from tests/integration_cluster_agents.rs:630. ONE addition: a one-shot
// stale head, which is the arms' independent variable and nothing else.
// =================================================================================================

struct Scripted {
    node: NodeId,
    ledger: Arc<Mutex<BranchLedger>>,
    log: Mutex<Vec<Entry>>,
    applied: Mutex<usize>,
    inject: Mutex<Vec<Command>>,
    sticky: Mutex<bool>,
    leader: Mutex<Option<NodeId>>,
    /// **The concurrency knob, and the whole of it.** When set, the NEXT call to
    /// `committed_head()` returns this round instead of the true head, and clears it. A merge's
    /// first attempt therefore reads the head as it stood before its siblings committed; every
    /// retry reads the true head, exactly as `merge()`'s loop does. This is a node whose view of
    /// the log lags the leader — an ordinary condition, not a fiction.
    stale_head: Mutex<Option<Round>>,
    /// How many times a stale head was actually served. A stale head that was never consumed is a
    /// harness that did not create the concurrency it claims, and would read as a clean zero.
    stale_served: Mutex<u64>,
    /// Sibling branches whose merges land in the window, ONE PER PROPOSAL.
    ///
    /// Each is turned into a `BranchOp::Merge` at propose time, carrying the head as it stands at
    /// that instant — which is what the sibling's own gate would have read. Built late rather than
    /// queued as fixed commands for a reason the first run found the hard way: a merge command
    /// whose `base_round` is already behind `last_base_move` is itself RE-EVALUATED, and a
    /// re-evaluated merge moves nothing (cluster.rs:544 moves the base only on `Applied`). A queue
    /// of stale sibling commands therefore invalidates nothing and reads as "the gate has
    /// locality after all".
    sibling_merges: Mutex<VecDeque<ClusterBranchId>>,
    /// How many siblings were actually served. Zero here means the starvation arm never ran.
    siblings_served: Mutex<u64>,
}

impl Scripted {
    fn new(node: NodeId, ledger: Arc<Mutex<BranchLedger>>) -> Arc<Scripted> {
        Arc::new(Scripted {
            node,
            ledger,
            log: Mutex::new(Vec::new()),
            applied: Mutex::new(0),
            inject: Mutex::new(Vec::new()),
            sticky: Mutex::new(false),
            leader: Mutex::new(Some(node)),
            stale_head: Mutex::new(None),
            stale_served: Mutex::new(0),
            sibling_merges: Mutex::new(VecDeque::new()),
            siblings_served: Mutex::new(0),
        })
    }

    fn stale_head_once(&self, r: Round) {
        *lock(&self.stale_head) = Some(r);
    }

    fn stale_served(&self) -> u64 {
        *lock(&self.stale_served)
    }

    /// Land one sibling's merge immediately before each of the next `ids.len()` proposals.
    fn sibling_merges_before_next_proposals(&self, ids: Vec<ClusterBranchId>) {
        *lock(&self.sibling_merges) = ids.into();
    }

    fn siblings_served(&self) -> u64 {
        *lock(&self.siblings_served)
    }

    /// The whole committed log, as the ledger sees it. Used to settle the question a count cannot:
    /// whether the two arms were ever DISTINGUISHABLE by anything reading this log.
    fn log_commands(&self) -> Vec<String> {
        lock(&self.log).iter().map(|e| format!("{}:{:?}", e.round, e.command)).collect()
    }

    fn settle(&self) {
        loop {
            let (a, n) = (*lock(&self.applied), lock(&self.log).len());
            if a >= n {
                return;
            }
            self.pump().unwrap();
        }
    }
}

impl Replicated for Scripted {
    fn propose(&self, c: Command) -> Result<Round, FerroError> {
        if *lock(&self.leader) != Some(self.node) {
            return Err(FerroError::NotLeader { leader: None });
        }
        let injected: Vec<Command> = if *lock(&self.sticky) {
            lock(&self.inject).clone()
        } else {
            lock(&self.inject).drain(..).collect()
        };
        let sibling = lock(&self.sibling_merges).pop_front();
        let mut log = lock(&self.log);
        for i in injected {
            let r = log.len() as u64 + 1;
            log.push(entry(r, i));
        }
        if let Some(b) = sibling {
            // `base_round` is the head as it stands right now: what this sibling's own gate would
            // have read a moment ago. That is what makes it APPLY rather than be re-evaluated.
            let base = log.len() as u64;
            let r = base + 1;
            log.push(entry(r, merge_op(b, base)));
            *lock(&self.siblings_served) += 1;
        }
        let r = log.len() as u64 + 1;
        log.push(entry(r, c));
        Ok(r)
    }

    fn committed_head(&self) -> Round {
        if let Some(r) = lock(&self.stale_head).take() {
            *lock(&self.stale_served) += 1;
            return r;
        }
        lock(&self.log).len() as u64
    }

    fn pump(&self) -> Result<(), FerroError> {
        let mut applied = lock(&self.applied);
        let next = {
            let log = lock(&self.log);
            if *applied >= log.len() {
                return Ok(());
            }
            log[*applied].clone()
        };
        lock(&self.ledger).apply(&next);
        *applied += 1;
        Ok(())
    }

    fn leader(&self) -> Option<NodeId> {
        *lock(&self.leader)
    }
}

// =================================================================================================
// The arms
// =================================================================================================

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Arm {
    /// A: each branch touches a table NO other branch touches.
    Disjoint,
    /// C: every branch touches the SAME table (a different row each, so the branches do not
    /// conflict on a row and every merge stays admissible — the overlap is at TABLE granularity,
    /// which is the granularity the fine check works at).
    Overlap,
}

impl Arm {
    fn table_of(&self, i: usize) -> String {
        match self {
            Arm::Disjoint => format!("t{i}"),
            Arm::Overlap => "shared".to_string(),
        }
    }
    fn row_of(&self, i: usize) -> i64 {
        match self {
            Arm::Disjoint => 1,
            Arm::Overlap => i as i64 + 1,
        }
    }
}

struct ArmResult {
    reevaluations: u64,
    proposals: u64,
    /// Per-merge outcome. A bare total hides an arm whose merges stopped being admissible for a
    /// reason that has nothing to do with the gate.
    outcomes: Vec<String>,
    applied: usize,
    errored: usize,
    stale_served: u64,
    rows_landed: usize,
    /// The committed log this arm produced.
    log: Vec<String>,
}

/// Run one arm at one C.
///
/// `concurrent == false` is the B-SERIAL control: the same number of merges, each gate reading the
/// true current head. It separates "concurrency causes reevaluations" from "more merges cause
/// them", which arm B as cut (one merge) cannot.
fn run_arm(arm: Arm, c: usize, concurrent: bool) -> ArmResult {
    let mut db = Db::new();

    // Seed. Disjoint: C tables of one row. Overlap: one table of C rows.
    {
        let mut s = db.session();
        match arm {
            Arm::Disjoint => {
                for i in 0..c {
                    db.ok(
                        &format!("CREATE TABLE t{i} (id INTEGER NOT NULL, qty INTEGER);"),
                        &mut s,
                    );
                    db.ok(&format!("INSERT INTO t{i} VALUES (1, 100);"), &mut s);
                }
            }
            Arm::Overlap => {
                db.ok("CREATE TABLE shared (id INTEGER NOT NULL, qty INTEGER);", &mut s);
                for i in 0..c {
                    db.ok(&format!("INSERT INTO shared VALUES ({}, 100);", i + 1), &mut s);
                }
            }
        }
    }

    let ledger = Arc::new(Mutex::new(BranchLedger::new()));
    let repl = Scripted::new(N1, ledger.clone());
    let agents = ClusterAgents::new(N1, db.runtime.clone(), repl.clone(), ledger);

    let mut sessions = Vec::new();
    for _ in 0..c {
        let id = RunIdentity { agent_id: "d174", run_id: Some("r_1"), ..RunIdentity::default() };
        sessions.push(agents.fork(id, BranchId::TRUNK).unwrap());
    }
    repl.settle();

    for (i, cs) in sessions.iter().enumerate() {
        let sql = format!(
            "UPDATE {} SET qty = 42 WHERE id = {};",
            arm.table_of(i),
            arm.row_of(i)
        );
        db.on_branch(cs, &sql);
    }

    // The head every concurrent gate reads. Forks do not move the base (`moves_base`,
    // cluster.rs:230), so this is also `last_base_move == 0` territory: the first merge cannot be
    // re-evaluated by construction, and every later one is re-evaluated only because a SIBLING
    // merge committed.
    let r0 = repl.committed_head();

    let mut outcomes = Vec::new();
    let mut applied = 0usize;
    let mut errored = 0usize;
    for (i, cs) in sessions.iter().enumerate() {
        if concurrent {
            repl.stale_head_once(r0);
        }
        let branch = cs.branch();
        let mut ctx = db.ctx();
        match agents.merge(&mut ctx, branch) {
            Ok(rep) => {
                if rep.report.applied_to_target {
                    applied += 1;
                }
                outcomes.push(format!(
                    "m{i}: applied={} reeval={} base={} round={:?}",
                    rep.report.applied_to_target, rep.reevaluations, rep.base_round, rep.merge_round
                ));
            }
            Err(e) => {
                errored += 1;
                let msg = format!("{e}");
                outcomes.push(format!("m{i}: ERR {}", msg.chars().take(90).collect::<String>()));
            }
        }
    }

    // Did the rows actually land? An arm whose merges "applied" but published nothing would give a
    // count about a database that never changed.
    let mut rows_landed = 0usize;
    for i in 0..c {
        if db.qty(&arm.table_of(i), arm.row_of(i)) == Some(42) {
            rows_landed += 1;
        }
    }

    let cost = agents.cost();
    ArmResult {
        reevaluations: cost.reevaluations,
        proposals: cost.proposals,
        outcomes,
        applied,
        errored,
        stale_served: repl.stale_served(),
        rows_landed,
        log: repl.log_commands(),
    }
}

// =================================================================================================
// P1 — the zero-control. If this is not zero, every other number in this file is void.
// =================================================================================================

#[test]
fn p1_a_single_merge_with_no_concurrency_is_never_re_evaluated() {
    println!("\n=== D174 / P1 — ARM B (control): ONE merge, no concurrency ===");
    let r = run_arm(Arm::Disjoint, 1, false);
    println!("  B(1)  reevaluations={} proposals={} applied={} rows_landed={}", r.reevaluations, r.proposals, r.applied, r.rows_landed);
    for o in &r.outcomes {
        println!("        {o}");
    }
    assert_eq!(
        r.reevaluations, 0,
        "P1 FAILED. A single merge against a base nothing moved was re-evaluated. The counter is \
         not measuring what the pre-registration thinks it is, and every other number in this run \
         is void."
    );
    assert_eq!(r.applied, 1, "the control merge did not publish, so it measured nothing");
    assert_eq!(r.rows_landed, 1, "the control merge's row never reached the target");
    println!("  P1 HELD: B == 0.");
}

// =================================================================================================
// The row: arms A and C at every C, plus the B-SERIAL control at every C.
// =================================================================================================

#[test]
fn d174_merge_gate_locality_table() {
    println!("\n=== D174 — cluster merge gate branch-locality ===");
    println!("Instrument: ClusterAgents::cost().reevaluations (ConsensusCost, cluster.rs:804).");
    println!("An integer counter out of a deterministic state machine. NOT a timer: no measure");
    println!("lock was taken and no quiet box was waited for, on purpose.\n");

    println!("{:<4} {:>10} {:>10} {:>12} {:>10}", "C", "A-DISJ", "C-OVERLAP", "B-SERIAL(C)", "A/OVLP");
    println!("{}", "-".repeat(50));

    let mut rows = Vec::new();
    for &c in CS.iter() {
        let a = run_arm(Arm::Disjoint, c, true);
        let o = run_arm(Arm::Overlap, c, true);
        let bs = run_arm(Arm::Disjoint, c, false);
        let ratio = if o.reevaluations == 0 {
            "n/a".to_string()
        } else {
            format!("{:.2}", a.reevaluations as f64 / o.reevaluations as f64)
        };
        println!(
            "{:<4} {:>10} {:>10} {:>12} {:>10}",
            c, a.reevaluations, o.reevaluations, bs.reevaluations, ratio
        );
        rows.push((c, a, o, bs));
    }

    println!("\n--- per-merge detail, and the guards that say the arms really ran ---");
    for (c, a, o, bs) in &rows {
        println!(
            "\nC={c}  A-DISJOINT : reeval={} proposals={} applied={}/{c} err={} rows_landed={}/{c} stale_heads_served={}",
            a.reevaluations, a.proposals, a.applied, a.errored, a.rows_landed, a.stale_served
        );
        for x in &a.outcomes {
            println!("           {x}");
        }
        println!(
            "C={c}  C-OVERLAP  : reeval={} proposals={} applied={}/{c} err={} rows_landed={}/{c} stale_heads_served={}",
            o.reevaluations, o.proposals, o.applied, o.errored, o.rows_landed, o.stale_served
        );
        for x in &o.outcomes {
            println!("           {x}");
        }
        println!(
            "C={c}  B-SERIAL   : reeval={} proposals={} applied={}/{c} err={} rows_landed={}/{c} stale_heads_served={}",
            bs.reevaluations, bs.proposals, bs.applied, bs.errored, bs.rows_landed, bs.stale_served
        );
    }

    println!("\n--- PREDICTIONS, as registered ---");
    let at = |c: usize| -> &ArmResult { &rows.iter().find(|r| r.0 == c).unwrap().1 };
    let ov = |c: usize| -> &ArmResult { &rows.iter().find(|r| r.0 == c).unwrap().2 };

    // P1 extended: B-SERIAL is the second zero-control (Amendment 1 C1).
    let bserial_all_zero = rows.iter().all(|(_, _, _, bs)| bs.reevaluations == 0);
    println!(
        "P1  B == 0 and B-SERIAL(C) == 0 at every C .......... {}",
        if bserial_all_zero { "HELD" } else { "MISSED" }
    );

    let a_grows = CS.windows(2).all(|w| at(w[1]).reevaluations > at(w[0]).reevaluations);
    let a_positive = CS.iter().all(|&c| at(c).reevaluations > 0);
    println!(
        "P2  A > 0 and A grows with C ........................ {}",
        if a_positive && a_grows { "HELD" } else { "MISSED" }
    );

    let p3 = CS.iter().all(|&c| {
        let (x, y) = (at(c).reevaluations as f64, ov(c).reevaluations as f64);
        y > 0.0 && x <= 2.0 * y && y <= 2.0 * x
    });
    println!(
        "P3  A within 2x of OVERLAP at every C ............... {}",
        if p3 { "HELD" } else { "MISSED" }
    );

    println!(
        "P4  at C=16, A >= 8 ................................. {}  (A(16) = {})",
        if at(16).reevaluations >= 8 { "HELD" } else { "MISSED" },
        at(16).reevaluations
    );

    // ---------------------------------------------------------------------------------------
    // The guards. These are what make the numbers above admissible at all.
    // ---------------------------------------------------------------------------------------
    assert!(
        bserial_all_zero,
        "P1 FAILED on the B-SERIAL control: C merges run strictly serially, with every gate \
         reading the true head, were re-evaluated. Then the counter is counting merges, not \
         conflicts, and the whole row is void."
    );
    // The positive control. A zero from arm A is only meaningful if the counter has been FORCED to
    // fire elsewhere; arm C is what forces it. If C is also zero the harness never created any
    // concurrency and the run collected nothing.
    assert!(
        CS.iter().all(|&c| ov(c).reevaluations > 0),
        "POSITIVE CONTROL FAILED: arm C (OVERLAP) is also zero. The harness never created \
         concurrency at all and this run collected nothing. It is REFUSED, not a clean zero."
    );
    // And the concurrency really was served, rather than silently skipped.
    for (c, a, o, _) in &rows {
        assert_eq!(a.stale_served, *c as u64, "arm A at C={c} did not serve C stale heads");
        assert_eq!(o.stale_served, *c as u64, "arm C at C={c} did not serve C stale heads");
    }

    // ---------------------------------------------------------------------------------------
    // Why A == C is a PROOF and not a coincidence.
    //
    // A null result is worth nothing until you know the instrument could have answered the other
    // way. Here the decisive evidence is not a mutant, because no mutant of `merge_verdict` could
    // pass: `merge_verdict` is a function of the committed log alone, and the two arms' logs are
    // BYTE-IDENTICAL. `BranchOp::Merge { branch, base_round }` carries no table, so the
    // information the gate would need to be local is not merely unused — it is not present.
    // ---------------------------------------------------------------------------------------
    println!("\n--- are the arms distinguishable AT ALL by anything reading the log? ---");
    for (c, a, o, _) in &rows {
        let same = a.log == o.log;
        println!(
            "C={c}: arm A log ({} entries) vs arm C log ({} entries) -> {}",
            a.log.len(),
            o.log.len(),
            if same { "BYTE-IDENTICAL" } else { "DIFFERENT" }
        );
        assert_eq!(
            a.log, o.log,
            "the two arms produced different logs at C={c}. Then A == C was NOT forced by the \
             command's type and this run has found something the analysis did not predict — \
             report it rather than the null."
        );
    }
    println!(
        "  ⇒ A == C is not an observation about this gate. NO function of this log can separate\n         \x20   the arms: the read-set is absent from `BranchOp::Merge`, which is exactly the\n         \x20   constraint the pre-registration names (a fix must put the read-set IN THE LOG).\n         \x20   Sample, C=2, arm A: {:?}",
        rows.iter().find(|r| r.0 == 2).unwrap().1.log
    );

    // And the arms really did touch what the arm names say: arm A's writes landed in C distinct
    // tables, arm C's in C rows of one. Without this the log identity could mean the arms were
    // never different in the first place.
    for (c, a, o, _) in &rows {
        assert_eq!(a.rows_landed, *c, "arm A at C={c} did not land C rows in C distinct tables");
        assert_eq!(o.rows_landed, *c, "arm C at C={c} did not land C rows in the one shared table");
    }
}

// =================================================================================================
// The column that makes A == C a MEASUREMENT rather than a restatement of a struct definition.
// =================================================================================================

/// **Does a locality signal exist anywhere in the engine?**
///
/// The publish-time check (`runtime.rs:4865`) re-fingerprints `eval.tables_read`. It is TABLE
/// granular. So an evaluation held across another branch's publish should survive when the two
/// touched different tables, and be refused when they touched the same one.
///
/// If that holds, the finding is exact: the engine already computes a per-branch read-set, and the
/// cluster gate declines to use it. If it does not hold, "no locality" is a far weaker claim and
/// must be reported as such.
#[test]
fn fine_check_separates_disjoint_from_overlapping_merges() {
    println!("\n=== D174 / FINE — is there a locality signal one layer down? ===");

    for arm in [Arm::Disjoint, Arm::Overlap] {
        let mut db = Db::new();
        {
            let mut s = db.session();
            match arm {
                Arm::Disjoint => {
                    db.ok("CREATE TABLE t0 (id INTEGER NOT NULL, qty INTEGER);", &mut s);
                    db.ok("INSERT INTO t0 VALUES (1, 100);", &mut s);
                    db.ok("CREATE TABLE t1 (id INTEGER NOT NULL, qty INTEGER);", &mut s);
                    db.ok("INSERT INTO t1 VALUES (1, 100);", &mut s);
                }
                Arm::Overlap => {
                    db.ok("CREATE TABLE shared (id INTEGER NOT NULL, qty INTEGER);", &mut s);
                    db.ok("INSERT INTO shared VALUES (1, 100);", &mut s);
                    db.ok("INSERT INTO shared VALUES (2, 100);", &mut s);
                }
            }
        }

        let ledger = Arc::new(Mutex::new(BranchLedger::new()));
        let repl = Scripted::new(N1, ledger.clone());
        let agents = ClusterAgents::new(N1, db.runtime.clone(), repl.clone(), ledger);
        let id = RunIdentity { agent_id: "d174", run_id: Some("r_1"), ..RunIdentity::default() };
        let x = agents.fork(id, BranchId::TRUNK).unwrap();
        let id = RunIdentity { agent_id: "d174", run_id: Some("r_2"), ..RunIdentity::default() };
        let y = agents.fork(id, BranchId::TRUNK).unwrap();
        repl.settle();

        let (tx, rx) = (arm.table_of(0), arm.row_of(0));
        let (ty, ry) = (arm.table_of(1), arm.row_of(1));
        db.on_branch(&x, &format!("UPDATE {tx} SET qty = 42 WHERE id = {rx};"));
        db.on_branch(&y, &format!("UPDATE {ty} SET qty = 77 WHERE id = {ry};"));

        // Score X against the base as it stands now, and HOLD the evaluation.
        let xb = x.branch();
        let rt = db.runtime.clone();
        let held = {
            let mut ctx = db.ctx();
            rt.evaluate_merge(&mut ctx, xb, &[]).expect("X did not evaluate")
        };

        // Y publishes underneath it. This is the base move, and it is a REAL publication of rows —
        // not an injected log command.
        let yb = y.branch();
        {
            let mut ctx = db.ctx();
            agents.merge(&mut ctx, yb).expect("Y's merge did not land");
        }

        // Now try to publish X's held verdict.
        let out = {
            let mut ctx = db.ctx();
            rt.publish_evaluation(&mut ctx, held)
        };
        let verdict = match &out {
            Ok(r) => format!("PUBLISHED (applied_to_target={})", r.applied_to_target),
            Err(e) => {
                let m = format!("{e}");
                format!("REFUSED: {}", m.chars().take(110).collect::<String>())
            }
        };
        println!("  {arm:?}: X evaluated, Y published underneath it, X then published -> {verdict}");

        match arm {
            Arm::Disjoint => assert!(
                out.is_ok(),
                "the fine check refused two merges that share NO table. Then it has no locality \
                 either, and the D174 finding is much weaker than 'the gate ignores a signal the \
                 engine computes'. Got: {verdict}"
            ),
            Arm::Overlap => assert!(
                out.is_err(),
                "the fine check ALLOWED a merge whose table another merge had published to. Then \
                 there is no locality signal anywhere and A == C says nothing. Got: {verdict}"
            ),
        }
    }
    println!(
        "  FINE SEPARATES the arms: a per-branch read-set exists and is enforced one layer below \
         the gate."
    );
}

// =================================================================================================
// The consequence worth more than a count: a DISJOINT merge REFUSED outright.
// =================================================================================================

/// **Starvation by strangers.** A merge that touches only `t0` is refused — `DEFAULT_MAX_REEVALUATIONS`
/// (cluster.rs:868, = 8) — because merges touching only `t1..t9` kept committing. Nothing the
/// refused branch read or wrote was touched by any of them.
///
/// What is scripted and what is real, stated plainly: the other branches' `BranchOp::Merge`
/// commands are PLACED in the window rather than produced by a second node, because one process
/// holds one `&mut Catalog` (cluster.rs:1150). They are real commands, for real branches the
/// cluster agreed exist, applied by the real `BranchLedger`. Their rows do not publish — and the
/// ledger, which is the only thing `merge_verdict` reads, cannot tell the difference. That is the
/// finding, not a limitation of it.
#[test]
fn a_merge_touching_one_table_is_refused_because_merges_of_other_tables_kept_committing() {
    println!("\n=== D174 / STARVATION — a disjoint merge refused outright ===");
    const N: usize = 10;

    let mut db = Db::new();
    {
        let mut s = db.session();
        for i in 0..N {
            db.ok(&format!("CREATE TABLE t{i} (id INTEGER NOT NULL, qty INTEGER);"), &mut s);
            db.ok(&format!("INSERT INTO t{i} VALUES (1, 100);"), &mut s);
        }
    }

    let ledger = Arc::new(Mutex::new(BranchLedger::new()));
    let repl = Scripted::new(N1, ledger.clone());
    let agents = ClusterAgents::new(N1, db.runtime.clone(), repl.clone(), ledger.clone());

    let mut sessions = Vec::new();
    for _ in 0..N {
        let id = RunIdentity { agent_id: "d174", run_id: Some("r_1"), ..RunIdentity::default() };
        sessions.push(agents.fork(id, BranchId::TRUNK).unwrap());
    }
    repl.settle();
    for (i, cs) in sessions.iter().enumerate() {
        db.on_branch(cs, &format!("UPDATE t{i} SET qty = 42 WHERE id = 1;"));
    }

    // Before every one of branch 0's proposals, ONE DISTINCT sibling's merge commits. Each
    // sibling touches a table branch 0 never read or wrote. Distinct matters: a second merge of an
    // already-sealed branch is Refused, and a Refused verdict moves nothing (cluster.rs:544), so
    // repeating one sibling invalidates only the first attempt.
    //
    // Nine of them, because the bound refuses at `reevaluations > DEFAULT_MAX_REEVALUATIONS` = 8.
    let siblings: Vec<ClusterBranchId> = (1..N)
        .map(|i| ClusterBranchId::of(N1, sessions[i].branch()).unwrap())
        .collect();
    assert_eq!(siblings.len(), 9, "the bound needs 9 distinct invalidations to be reached");
    repl.sibling_merges_before_next_proposals(siblings.clone());

    let b0 = sessions[0].branch();
    let err = {
        let mut ctx = db.ctx();
        match agents.merge(&mut ctx, b0) {
            Ok(r) => panic!(
                "branch 0 merged (reeval={}, applied_to_target={}); expected the bound to refuse \
                 it after 9 strangers committed",
                r.reevaluations, r.report.applied_to_target
            ),
            Err(e) => e,
        }
    };

    let msg = format!("{err}");
    println!("  branch 0 touched ONLY t0. Nine merges, each touching only t1..t9, committed.");
    println!("  siblings actually served: {}", repl.siblings_served());
    println!("  branch 0 was REFUSED:\n    {msg}");
    println!("  cost: {:?}", agents.cost());
    println!(
        "  t0 in the target after the refusal: {:?} (100 = branch 0's work never landed)",
        db.qty("t0", 1)
    );
    // The strangers' rows are not in the target either: their merge COMMANDS were placed, their
    // publications were not. Stated so nobody reads this as "9 merges succeeded and one starved".
    println!(
        "  t1 in the target: {:?} (100 = the siblings' log commands moved the base without \
         publishing rows; the ledger is all `merge_verdict` reads)",
        db.qty("t1", 1)
    );

    assert_eq!(repl.siblings_served(), 9, "the strangers never reached the log");
    assert!(
        msg.contains("re-evaluated"),
        "expected the re-evaluation bound to refuse the merge, got: {msg}"
    );
    assert!(
        agents.cost().reevaluations > 8,
        "the bound was not reached: reevaluations={}",
        agents.cost().reevaluations
    );
    assert_eq!(
        db.qty("t0", 1),
        Some(100),
        "a refused merge published anyway; then the refusal is not what was measured"
    );
    println!("  STARVATION CONFIRMED: no branch that touched t0 ever committed.");
}
