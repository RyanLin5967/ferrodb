//! E30 — property tests for the merge OUTCOMES, not the operation algebra.
//!
//! `prop_merge_algebra.rs` generates over compose and apply: associativity, commutativity, the
//! identity, the non-idempotence of `Add`. All of that is about what an operation *is*. None of it
//! says anything about the four outcomes a merge reports, and the words `Clean`, `Commuting`,
//! `Conflict` and `ResolvedWithLoss` do not appear in that file once.
//!
//! The outcomes are where the design's safety claim lives. README calls reporting
//! `ResolvedWithLoss` as `Clean` "the most dangerous thing this system could do to an agent",
//! because an agent told `Clean` has no reason to look, and the write it lost is gone with no
//! record. That claim was pinned by a single unit test which constructs the enum **by hand** and
//! checks its accessor — it never runs a merge. So the property held about the type and was
//! untested about the engine.
//!
//! These drive the real `SurfaceMerger` and assert the classification is honest, whatever the
//! generator produces.

use std::sync::Arc;

use proptest::prelude::*;

use ferrodb::agent_sql::{CellState, PolicyTable, SurfaceMerger};
use ferrodb::branch::record::BranchRecord;
use ferrodb::branch::types::{BranchId, CommitHash, LeaseDeadline};
use ferrodb::catalog::column::Value;
use ferrodb::tel::ids::{ColId, RowId, TableId, TxnId};
use ferrodb::tel::merge::{MergeOutcome, MergePolicy, Merger};
use ferrodb::tel::op::{Delta, Op, OpKind};
use ferrodb::tel::{MemEffectLog, TxnFrame};

const TBL: TableId = TableId(1);
const ROW: RowId = RowId(1);
const COL: ColId = ColId(2);
/// A second cell, so a single merge can both COMPOSE one cell and DISCARD another. With one cell
/// the two outcomes are mutually exclusive and the ordering between them is untestable - which a
/// fire-check proved: swapping `Commuting` ahead of `ResolvedWithLoss` in the engine passed every
/// property here until this existed.
const COL2: ColId = ColId(3);

/// Operations two branches might each apply to one cell.
fn op_kind() -> impl Strategy<Value = OpKind> {
    prop_oneof![
        (-50i64..50).prop_map(|d| OpKind::Add(Delta::Int(d))),
        (-50i32..50).prop_map(|v| OpKind::Assign(Value::Integer(v))),
        (-50i32..50).prop_map(|v| OpKind::Max(Value::Integer(v))),
        (-50i32..50).prop_map(|v| OpKind::Min(Value::Integer(v))),
    ]
}

fn policy() -> impl Strategy<Value = MergePolicy> {
    prop_oneof![
        Just(MergePolicy::Reject),
        Just(MergePolicy::Lww),
        Just(MergePolicy::MultiValue),
        Just(MergePolicy::Additive),
    ]
}

/// Run a real merge of one op from each side against one cell.
fn merge_one(ours: OpKind, theirs: OpKind, pol: MergePolicy, witness: i32) -> MergeOutcome {
    // The merger reads frames back through an `EffectLog`; these tests hand it frames directly,
    // so an empty in-memory log is never consulted and no stub is needed.
    let m = SurfaceMerger::new(Arc::new(MemEffectLog::new()));
    let lca = BranchRecord::trunk(0, LeaseDeadline(0));

    let mut a = TxnFrame::new(TxnId(1), BranchId::new(1, 0), CommitHash::ZERO, 0, 1);
    a.push_op(Op::new(TBL, ROW, Some(COL), ours).with_witness(Value::Integer(witness)));

    let mut b = TxnFrame::new(TxnId(2), BranchId::new(2, 0), CommitHash::ZERO, 0, 1);
    b.push_op(Op::new(TBL, ROW, Some(COL), theirs).with_witness(Value::Integer(witness)));

    let mut table = PolicyTable::new();
    table.set(TBL, COL, pol);

    let mut merged = CellState::new();
    merged.set(TBL, ROW, COL, Value::Integer(witness));

    m.merge(&lca, &[a], &[b], &table, &merged).expect("merge")
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 400, ..ProptestConfig::default() })]

    /// **The safety property.** A merge that discarded a write must say so. `Clean` and `Commuting`
    /// both tell an agent nothing was lost, so neither may carry a discard.
    #[test]
    fn a_discarded_write_is_never_reported_as_clean_or_commuting(
        ours in op_kind(), theirs in op_kind(), pol in policy(), w in -50i32..50,
    ) {
        let out = merge_one(ours, theirs, pol, w);
        match &out {
            MergeOutcome::Clean => prop_assert!(
                !out.lost_a_write(),
                "Clean reported lost_a_write - an agent told Clean has no reason to look"
            ),
            MergeOutcome::Commuting { .. } => prop_assert!(
                !out.lost_a_write(),
                "Commuting reported lost_a_write; composition is supposed to keep both writes"
            ),
            MergeOutcome::ResolvedWithLoss { discarded, .. } => prop_assert!(
                !discarded.is_empty(),
                "ResolvedWithLoss carried an EMPTY discard list, so it is reporting loss that did \
                 not happen - the mirror image of the dangerous case and just as wrong"
            ),
            MergeOutcome::Conflict(reports) => prop_assert!(
                !reports.is_empty(),
                "Conflict carried no reports, so the agent is refused with nothing to act on"
            ),
        }
    }

    /// `lost_a_write()` is the accessor every caller branches on, so it must agree with the variant
    /// exactly - true for `ResolvedWithLoss` and false for the other three.
    #[test]
    fn lost_a_write_agrees_with_the_variant(
        ours in op_kind(), theirs in op_kind(), pol in policy(), w in -50i32..50,
    ) {
        let out = merge_one(ours, theirs, pol, w);
        let is_lossy = matches!(out, MergeOutcome::ResolvedWithLoss { .. });
        prop_assert_eq!(
            out.lost_a_write(), is_lossy,
            "lost_a_write() disagrees with the variant for {}", out.name()
        );
    }

    /// The four outcomes must be distinguishable by name. A caller logging `name()` and a caller
    /// matching the variant have to reach the same conclusion.
    #[test]
    fn the_name_identifies_the_variant(
        ours in op_kind(), theirs in op_kind(), pol in policy(), w in -50i32..50,
    ) {
        let out = merge_one(ours, theirs, pol, w);
        // Read from `MergeOutcome::name`, not invented: my first version guessed SCREAMING_CASE
        // and the property caught it, which is the cheapest possible demonstration that these
        // assertions are load-bearing rather than restatements of the code.
        let expected = match out {
            MergeOutcome::Clean => "Clean",
            MergeOutcome::Commuting { .. } => "Commuting",
            MergeOutcome::Conflict(_) => "Conflict",
            MergeOutcome::ResolvedWithLoss { .. } => "ResolvedWithLoss",
        };
        prop_assert_eq!(out.name(), expected);
    }

    /// **Determinism.** The same two writes merged twice must classify the same way. A merge whose
    /// outcome depends on iteration order would make `Clean` a coin flip, and every property above
    /// would be testing one flip of it.
    #[test]
    fn the_same_merge_classifies_the_same_way_twice(
        ours in op_kind(), theirs in op_kind(), pol in policy(), w in -50i32..50,
    ) {
        let first = merge_one(ours.clone(), theirs.clone(), pol, w);
        let second = merge_one(ours, theirs, pol, w);
        prop_assert_eq!(first.name(), second.name());
        prop_assert_eq!(first.lost_a_write(), second.lost_a_write());
    }

    /// **Loss dominates composition.** When one cell composes cleanly and another is discarded in
    /// the same merge, the outcome must be `ResolvedWithLoss`. Reporting `Commuting` would be true
    /// of one cell and would hide the other, and an agent reads one outcome, not a per-cell report.
    ///
    /// This is the ordering of the three returns at the end of the engine's `merge`, and it is only
    /// observable when both conditions hold at once.
    #[test]
    fn a_discard_beside_a_composition_still_reports_loss(
        d1 in -20i64..20, d2 in -20i64..20, x in -50i32..50, y in -50i32..50, w in -50i32..50,
    ) {
        prop_assume!(x != y);
        let m = SurfaceMerger::new(Arc::new(MemEffectLog::new()));
        let lca = BranchRecord::trunk(0, LeaseDeadline(0));

        // COL composes (Add + Add). COL2 is a genuine concurrent Assign under LWW, so one side of
        // it is thrown away.
        let mut a = TxnFrame::new(TxnId(1), BranchId::new(1, 0), CommitHash::ZERO, 0, 1);
        a.push_op(Op::new(TBL, ROW, Some(COL), OpKind::Add(Delta::Int(d1)))
            .with_witness(Value::Integer(w)));
        a.push_op(Op::new(TBL, ROW, Some(COL2), OpKind::Assign(Value::Integer(x)))
            .with_witness(Value::Integer(w)));

        let mut b = TxnFrame::new(TxnId(2), BranchId::new(2, 0), CommitHash::ZERO, 0, 1);
        b.push_op(Op::new(TBL, ROW, Some(COL), OpKind::Add(Delta::Int(d2)))
            .with_witness(Value::Integer(w)));
        b.push_op(Op::new(TBL, ROW, Some(COL2), OpKind::Assign(Value::Integer(y)))
            .with_witness(Value::Integer(w)));

        let mut table = PolicyTable::new();
        table.set(TBL, COL, MergePolicy::Additive);
        table.set(TBL, COL2, MergePolicy::Lww);

        let mut merged = CellState::new();
        merged.set(TBL, ROW, COL, Value::Integer(w));
        merged.set(TBL, ROW, COL2, Value::Integer(w));

        let out = m.merge(&lca, &[a], &[b], &table, &merged).expect("merge");
        prop_assume!(!matches!(out, MergeOutcome::Conflict(_)));
        prop_assert!(
            out.lost_a_write(),
            "one cell composed and another was discarded, and the merge reported {} - the discard \
             is invisible to an agent that reads a single outcome",
            out.name()
        );
    }

    /// **The policy that discards by design must admit it.** LWW resolves two concurrent writes by
    /// keeping one, so when both sides genuinely write different values it cannot report `Clean`.
    /// Restricted to `Assign`, because `Add` composes and is legitimately not a loss.
    #[test]
    fn lww_on_two_different_assigns_never_reports_clean(
        x in -50i32..50, y in -50i32..50, w in -50i32..50,
    ) {
        prop_assume!(x != y);
        let out = merge_one(
            OpKind::Assign(Value::Integer(x)),
            OpKind::Assign(Value::Integer(y)),
            MergePolicy::Lww,
            w,
        );
        prop_assert!(
            !matches!(out, MergeOutcome::Clean),
            "LWW kept one of two different writes and reported {} - the discarded one is gone with \
             no record and the agent has no reason to look for it",
            out.name()
        );
    }
}
