//! Differential test: the two independent three-way merge engines must agree.
//!
//! After the merge there are two implementations of `tel::Merger` in the tree —
//! `tel::ThreeWayMerger`, written by the module that owns merge, and
//! `agent_sql::SurfaceMerger`, written by the SQL surface against the same trait. Only the SQL
//! surface's is anywhere near the demo path (and the runtime's own per-row merge shares its
//! core), so tel's engine and its ~50 tests currently prove nothing about what the database
//! actually does.
//!
//! Rather than assume the duplicate is harmless, this pins the thing that matters: given the same
//! frames, the same column policies and the same merged state, both engines must select the same
//! one of the four outcomes. A disagreement here is a real defect in whichever is wrong, and the
//! four outcomes are exactly what exit criterion 5 is stated in.

use std::sync::Arc;

use ferrodb::agent_sql::merge_engine::{CellState, PolicyTable, SurfaceMerger};
use ferrodb::branch::record::BranchRecord;
use ferrodb::branch::types::{BranchId, CommitHash, LeaseDeadline};
use ferrodb::catalog::column::Value;
use ferrodb::tel::engine::ThreeWayMerger;
use ferrodb::tel::guard::{CmpOp, Guard, GuardExpr};
use ferrodb::tel::ids::{ColId, RowId, TableId, TxnId};
use ferrodb::tel::merge::{MergePolicy, Merger};
use ferrodb::tel::op::{Delta, Op, OpKind};
use ferrodb::tel::{MemEffectLog, TxnFrame};

const TBL: TableId = TableId(1);
const ROW: RowId = RowId(1);
const QTY: ColId = ColId(2);

fn frame(txn: u64, branch: u64, kind: OpKind, witness: Option<Value>) -> TxnFrame {
    let mut f = TxnFrame::new(TxnId(txn), BranchId::new(branch, 0), CommitHash::ZERO, 0, 1);
    let mut op = Op::new(TBL, ROW, Some(QTY), kind);
    op.witness = witness;
    f.push_op(op);
    f
}

fn guarded(mut f: TxnFrame, at_least: i32, src: &str) -> TxnFrame {
    f.push_guard(
        Guard::holds(GuardExpr::cmp(
            GuardExpr::col(TBL, ROW, QTY),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(at_least)),
        ))
        .with_source(src),
    );
    f
}

fn state(qty: i32) -> CellState {
    let mut s = CellState::new();
    s.set(TBL, ROW, QTY, Value::Integer(qty));
    s
}

fn lca() -> BranchRecord {
    BranchRecord::trunk(1, LeaseDeadline(u64::MAX))
}

/// Run both engines over identical inputs and return `(surface, tel)` outcome names.
fn both(
    ours: &[TxnFrame],
    theirs: &[TxnFrame],
    policy: &PolicyTable,
    merged: &CellState,
) -> (String, String) {
    let surface = SurfaceMerger::new(Arc::new(MemEffectLog::new()));
    let tel = ThreeWayMerger::new();
    let a = surface
        .merge(&lca(), ours, theirs, policy, merged)
        .map(|o| o.name().to_string())
        .unwrap_or_else(|e| format!("Err({})", e));
    let b = tel
        .merge(&lca(), ours, theirs, policy, merged)
        .map(|o| o.name().to_string())
        .unwrap_or_else(|e| format!("Err({})", e));
    (a, b)
}

fn additive() -> PolicyTable {
    let mut p = PolicyTable::new();
    p.set(TBL, QTY, MergePolicy::Additive);
    p
}

#[test]
fn both_engines_agree_that_two_decrements_commute() {
    // Exit criterion 6, at the engine level: Add(-5) + Add(-3).
    let ours = vec![frame(1, 1, OpKind::Add(Delta::Int(-5)), Some(Value::Integer(20)))];
    let theirs = vec![frame(2, 2, OpKind::Add(Delta::Int(-3)), Some(Value::Integer(20)))];
    let (a, b) = both(&ours, &theirs, &additive(), &state(12));
    assert_eq!(a, b, "surface said {}, tel said {}", a, b);
    assert_eq!(a, "Commuting");
}

#[test]
fn both_engines_agree_that_a_one_sided_write_is_clean() {
    let ours = vec![frame(1, 1, OpKind::Add(Delta::Int(-5)), Some(Value::Integer(20)))];
    let (a, b) = both(&ours, &[], &additive(), &state(15));
    assert_eq!(a, b, "surface said {}, tel said {}", a, b);
    assert_eq!(a, "Clean");
}

#[test]
fn both_engines_agree_that_a_guard_failing_against_merged_state_is_a_conflict() {
    // Exit criterion 7. Each side legally took 4 against `qty >= 4` from a base of 5; composed
    // they overdraw, so the guard must be re-evaluated against the *merged* state and fail.
    let ours = vec![guarded(
        frame(1, 1, OpKind::Add(Delta::Int(-4)), Some(Value::Integer(5))),
        4,
        "qty >= 4",
    )];
    let theirs = vec![guarded(
        frame(2, 2, OpKind::Add(Delta::Int(-4)), Some(Value::Integer(5))),
        4,
        "qty >= 4",
    )];
    let (a, b) = both(&ours, &theirs, &additive(), &state(-3));
    assert_eq!(a, b, "surface said {}, tel said {}", a, b);
    assert_eq!(a, "Conflict");
}

#[test]
fn both_engines_agree_that_an_unannotated_column_forbids_concurrent_writes() {
    // AntidoteSQL's default, which DESIGN.md section 3 adopts: no modifier means concurrent
    // updates are forbidden, NOT last-writer-wins.
    let ours = vec![frame(1, 1, OpKind::Assign(Value::Integer(7)), Some(Value::Integer(20)))];
    let theirs = vec![frame(2, 2, OpKind::Assign(Value::Integer(9)), Some(Value::Integer(20)))];
    let (a, b) = both(&ours, &theirs, &PolicyTable::new(), &state(9));
    assert_eq!(a, b, "surface said {}, tel said {}", a, b);
    assert_eq!(a, "Conflict");
}

#[test]
fn both_engines_agree_that_a_discarding_lww_never_reads_as_clean() {
    // The most dangerous thing this system can do to an agent is report a discarded write as
    // Clean. Both engines must call it ResolvedWithLoss.
    let mut policy = PolicyTable::new();
    policy.set(TBL, QTY, MergePolicy::Lww);
    let ours = vec![frame(1, 1, OpKind::Assign(Value::Integer(7)), Some(Value::Integer(20)))];
    let theirs = vec![frame(2, 2, OpKind::Assign(Value::Integer(9)), Some(Value::Integer(20)))];
    let (a, b) = both(&ours, &theirs, &policy, &state(9));
    assert_eq!(a, b, "surface said {}, tel said {}", a, b);
    assert_eq!(a, "ResolvedWithLoss");
}

#[test]
fn both_engines_deduplicate_a_replayed_add_by_txn_id() {
    // Add is not idempotent: a retried frame must not double-count. Same frame twice on our
    // side must read the same as it appearing once.
    let f = frame(1, 1, OpKind::Add(Delta::Int(-5)), Some(Value::Integer(20)));
    let once = vec![f.clone()];
    let twice = vec![f.clone(), f];
    let (a1, b1) = both(&once, &[], &additive(), &state(15));
    let (a2, b2) = both(&twice, &[], &additive(), &state(15));
    assert_eq!(a1, a2, "surface double-counted a replayed Add");
    assert_eq!(b1, b2, "tel double-counted a replayed Add");
    assert_eq!(a1, b1);
}
