//! A6 — does the DEMO PATH carry the R1/R2 defect classes?
//!
//! A code review found two HIGH defects in `tel`'s merge engine: a WHERE clause is a PRE-state
//! precondition, but it was captured and then re-checked against POST-merge state, so a write that
//! falsifies its own guard rejected itself with zero concurrency (R1); and a guarded DELETE was
//! always `GuardUnevaluable` because the deleted row's cells cannot be read (R2).
//!
//! Those findings were filed against `src/tel/{capture,engine}.rs`, which A5 established is NOT
//! what the database runs — `agent_sql::runtime` has its own merge and its own guard capture.
//! So the findings say nothing about the product until someone probes the real path. This is that
//! probe.
//!
//! The discriminating case is the one the existing suite happens to miss. `qty = qty - 5 WHERE
//! qty >= 5` on a base of 20 lands on 15, which still satisfies `qty >= 5`, so it passes under
//! BOTH the correct and the broken reading. Only a row that lands at or below the bound tells
//! them apart. The seed's `id = 2` has `qty = 5`, so it lands exactly on 0.

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
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

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
        let path = dir.path().join("agent.db");
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(&path).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("agent.wal")).unwrap());
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
}

fn report(out: Outcome) -> MergeReport {
    match out {
        Outcome::Agent(AgentOutput::Merge(m)) => m,
        _ => panic!("expected a merge report"),
    }
}

fn qty_of(db: &mut Db, id: i32) -> i32 {
    let mut s = db.session();
    let out = db.ok(&format!("SELECT qty FROM inventory WHERE id = {};", id), &mut s);
    match out {
        Outcome::Rows(r) => {
            assert_eq!(r.len(), 1, "row {} missing", id);
            match r[0][0] {
                Value::Integer(i) => i,
                ref other => panic!("qty is not an integer: {:?}", other),
            }
        }
        _ => panic!("expected rows"),
    }
}

/// R1 on the demo path. A solo merge whose own effect lands the row exactly on the bound.
/// Under the correct reading (guard = precondition, checked against the target as it stands
/// before this branch's ops) this merges and qty becomes 0. Under the broken reading it is a
/// Conflict with nobody to conflict with.
#[test]
fn a_solo_merge_that_lands_on_the_bound_is_not_a_conflict() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'pricing' RUN 'r1';", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE qty >= 5 AND id = 2;", &mut a);

    let m = report(db.ok("MERGE;", &mut a));
    assert!(
        !m.to_string().to_lowercase().contains("conflict"),
        "a solo merge conflicted with nothing: {}",
        m
    );
    assert_eq!(qty_of(&mut db, 2), 0, "the decrement did not publish");
}

/// The control. If the probe above ever passes because guards are not evaluated at all, this
/// fails — a genuinely violated precondition must still be caught and must still hand back the
/// predicate. Two branches each take 5 from a stock of 5; the second cannot be admitted.
#[test]
fn a_genuinely_violated_precondition_is_still_rejected() {
    let mut db = Db::new();
    db.seed();
    let (mut a, mut b) = (db.session(), db.session());
    db.ok("BEGIN AGENT SESSION AS 'agent-a' RUN 'ra';", &mut a);
    db.ok("BEGIN AGENT SESSION AS 'agent-b' RUN 'rb';", &mut b);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE qty >= 5 AND id = 2;", &mut a);
    db.ok("UPDATE inventory SET qty = qty - 5 WHERE qty >= 5 AND id = 2;", &mut b);

    let first = report(db.ok("MERGE;", &mut a));
    assert!(!first.to_string().to_lowercase().contains("conflict"), "first merge: {}", first);
    assert_eq!(qty_of(&mut db, 2), 0);

    let second = report(db.ok("MERGE;", &mut b));
    let text = second.to_string();
    assert!(
        text.to_lowercase().contains("conflict"),
        "the second taker was admitted against a stock of 0: {}",
        text
    );
    assert!(
        text.contains("qty >= 5"),
        "the violated predicate was not handed back verbatim: {}",
        text
    );
    assert_eq!(qty_of(&mut db, 2), 0, "a conflicting merge published anyway");
}

/// A7 evidence: **where the precondition/postcondition decision actually lives.**
///
/// `tests/integration_merge_agreement.rs` shows the two `Merger` impls agree on six cases. But
/// both are handed the merged `CellState` by their *caller* — neither picks it. So the difference
/// between "solo merge succeeds" and "solo merge conflicts with nothing" is not an engine
/// property at all; it is `runtime.rs:816` choosing `now.or(before)`.
///
/// This pins that: hand BOTH engines the post-op state for a solo merge, and both call it a
/// Conflict. The engines are not what makes the demo path correct — the runtime's state selection
/// is. Anyone tempted to "fix R1 in tel" or to route the runtime through tel should read this
/// first: the bug was never in the comparison.
#[test]
fn both_engines_conflict_on_post_state_so_correctness_lives_in_the_caller() {
    use ferrodb::agent_sql::merge_engine::{CellState, PolicyTable, SurfaceMerger};
    use ferrodb::branch::record::BranchRecord;
    use ferrodb::branch::types::{BranchId, CommitHash, LeaseDeadline};
    use ferrodb::tel::engine::ThreeWayMerger;
    use ferrodb::tel::frame::TxnFrame;
    use ferrodb::tel::guard::{CmpOp, Guard, GuardExpr};
    use ferrodb::tel::ids::{ColId, RowId, TableId, TxnId};
    use ferrodb::tel::merge::{MergePolicy, Merger};
    use ferrodb::tel::op::{Delta, Op, OpKind};
    use ferrodb::tel::MemEffectLog;

    const TBL: TableId = TableId(1);
    const ROW: RowId = RowId(1);
    const QTY: ColId = ColId(1);

    // One agent takes 5 from a stock of 5. Nobody else touched the row.
    let mut f = TxnFrame::new(TxnId(1), BranchId::new(1, 0), CommitHash::ZERO, 0, 1);
    let mut op = Op::new(TBL, ROW, Some(QTY), OpKind::Add(Delta::Int(-5)));
    op.witness = Some(Value::Integer(5));
    f.push_op(op);
    f.push_guard(
        Guard::holds(GuardExpr::cmp(
            GuardExpr::col(TBL, ROW, QTY),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(5)),
        ))
        .with_source("qty >= 5"),
    );
    let ours = vec![f];

    let mut policy = PolicyTable::new();
    policy.set(TBL, QTY, MergePolicy::Additive);

    // The POST-op state: what the row becomes. This is the state the runtime deliberately does
    // NOT judge the guard against.
    let mut post = CellState::new();
    post.set(TBL, ROW, QTY, Value::Integer(0));

    let lca = BranchRecord::trunk(1, LeaseDeadline(u64::MAX));
    let surface = SurfaceMerger::new(std::sync::Arc::new(MemEffectLog::new()));
    let tel = ThreeWayMerger::new();

    let name = |r: Result<_, FerroError>| -> String {
        match r { Ok(o) => o, Err(e) => format!("Err({})", e) }
    };
    let a = name(surface.merge(&lca, &ours, &[], &policy, &post).map(|o| o.name().to_string()));
    let b = name(tel.merge(&lca, &ours, &[], &policy, &post).map(|o| o.name().to_string()));
    assert_eq!(a, b, "the two engines disagreed: surface={} tel={}", a, b);
    assert_eq!(
        a,
        "Conflict",
        "given post-op state both engines reject a solo merge - which is exactly why the runtime \
         must hand them the PRE-op state, and does"
    );

    // And the pre-op state, which is what the runtime actually supplies, is admitted by both.
    let mut pre = CellState::new();
    pre.set(TBL, ROW, QTY, Value::Integer(5));
    let a2 = name(surface.merge(&lca, &ours, &[], &policy, &pre).map(|o| o.name().to_string()));
    let b2 = name(tel.merge(&lca, &ours, &[], &policy, &pre).map(|o| o.name().to_string()));
    assert_eq!(a2, b2, "engines disagreed on pre-op state: {} vs {}", a2, b2);
    assert_ne!(a2, "Conflict", "the pre-op reading must admit a solo merge");
}

/// R2 on the demo path: a DELETE carrying a WHERE clause. The guard refers to cells of the row
/// the merge is removing, so a naive re-check cannot read them and reports `GuardUnevaluable`,
/// turning every guarded delete into a conflict with nothing.
#[test]
fn a_guarded_delete_merges_solo() {
    let mut db = Db::new();
    db.seed();
    let mut a = db.session();
    db.ok("BEGIN AGENT SESSION AS 'reaper' RUN 'r1';", &mut a);
    db.ok("DELETE FROM inventory WHERE qty >= 5 AND id = 2;", &mut a);

    let m = report(db.ok("MERGE;", &mut a));
    assert!(
        !m.to_string().to_lowercase().contains("conflict"),
        "a solo guarded delete conflicted with nothing: {}",
        m
    );
    let mut s = db.session();
    let out = db.ok("SELECT qty FROM inventory WHERE id = 2;", &mut s);
    match out {
        Outcome::Rows(r) => assert!(r.is_empty(), "the delete did not publish: {:?}", r),
        _ => panic!("expected rows"),
    }
}
