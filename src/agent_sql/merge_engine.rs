//! Cell-level three-way resolution, shared by `MERGE` at the SQL surface and by the
//! trait-shaped [`SurfaceMerger`].
//!
//! Design authority: DESIGN.md section 3.
//!
//! The order is fixed and cannot be rearranged:
//! 1. de-duplicate frames by `TxnId` — `Add` is **not** idempotent, and a replayed frame that is
//!    not de-duplicated double-counts (the Cassandra counter trap);
//! 2. compose the ops on each cell against the fork-point value;
//! 3. **then** re-evaluate every captured guard against the *composed* state;
//! 4. only then choose between `Clean`, `Commuting`, `Conflict` and `ResolvedWithLoss`.
//!
//! Step 3 cannot move earlier: a guard checked against pre-merge state is checked against a state
//! that will not exist after the merge. This is also why bounded counters need no special merge
//! logic — compose the `Add`s, then re-check `qty >= 0`.
//!
//! **This is a surface-local implementation of the shared `Merger` contract**, written so the SQL
//! statements have working semantics now. It is behind `crate::tel::Merger`, so the Typed Effect
//! Log's own engine replaces it without the SQL layer changing.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::branch::record::BranchRecord;
use crate::branch::types::{BranchId, CommitHash};
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::tel::frame::TxnFrame;
use crate::tel::guard::{Guard, GuardContext};
use crate::tel::ids::{ColId, RowId, TableId, TxnId};
use crate::tel::merge::{
    ColumnPolicyLookup, ConflictKind, ConflictReport, Diff, DiscardedWrite, MergeOutcome,
    MergePolicy, Merger,
};
use crate::tel::op::{Delta, Op, OpKind};
use crate::tel::EffectLog;

/// Per-column merge policy, defaulting to [`MergePolicy::Reject`].
///
/// AntidoteSQL's choice and the safe one: a column with no declared modifier makes concurrent
/// writes *forbidden* rather than silently last-writer-wins. Schema-declared policy is the
/// catalog's job; until the catalog carries the modifier, overrides are set here.
#[derive(Debug, Clone, Default)]
pub struct PolicyTable {
    overrides: BTreeMap<(TableId, ColId), MergePolicy>,
}

impl PolicyTable {
    pub fn new() -> Self {
        PolicyTable::default()
    }

    pub fn set(&mut self, tbl: TableId, col: ColId, policy: MergePolicy) {
        self.overrides.insert((tbl, col), policy);
    }
}

impl ColumnPolicyLookup for PolicyTable {
    fn policy(&self, tbl: TableId, col: ColId) -> MergePolicy {
        self.overrides.get(&(tbl, col)).copied().unwrap_or_default()
    }
}

/// A concrete cell map, used as the state a guard is re-evaluated against.
///
/// `column` returns `Err` for a cell it does not hold. That is deliberate and must not be
/// collapsed into `Value::Null`: "could not be evaluated" is a hard reject, "evaluated false" is
/// a retry, and the gate treats them differently.
#[derive(Debug, Clone, Default)]
pub struct CellState {
    cells: BTreeMap<(TableId, RowId, ColId), Value>,
}

impl CellState {
    pub fn new() -> Self {
        CellState::default()
    }

    pub fn set(&mut self, tbl: TableId, row: RowId, col: ColId, v: Value) {
        self.cells.insert((tbl, row, col), v);
    }

    pub fn get(&self, tbl: TableId, row: RowId, col: ColId) -> Option<&Value> {
        self.cells.get(&(tbl, row, col))
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }
}

impl GuardContext for CellState {
    fn column(&self, tbl: TableId, row: RowId, col: ColId) -> Result<Value, FerroError> {
        self.cells
            .get(&(tbl, row, col))
            .cloned()
            .ok_or_else(|| FerroError::Merge(format!("no such cell {}.{}[{}]", tbl, col, row)))
    }
}

/// Frames de-duplicated by `TxnId`, keeping the first occurrence.
///
/// `Add` is not idempotent, so a frame replayed twice must contribute its increments once. Note
/// this is a *frame*-level de-duplication: two separate `qty -= 5` statements inside one agent
/// task legitimately compose to -10 and must not be collapsed.
pub fn dedup_frames(frames: &[TxnFrame]) -> Vec<&TxnFrame> {
    let mut seen: Vec<TxnId> = Vec::new();
    let mut out = Vec::new();
    for f in frames {
        if !seen.contains(&f.txn_id) {
            seen.push(f.txn_id);
            out.push(f);
        }
    }
    out
}

/// Compose a cell's ops, in capture order, into one effect.
///
/// `Add`s sum. `Max`/`Min` fold monotonically. An `Assign` supersedes everything before it, which
/// is what makes "set it, then bump it twice" resolve to a single value plus a delta.
pub fn compose_ops(ops: &[OpKind]) -> Result<OpKind, FerroError> {
    let mut acc: Option<OpKind> = None;
    for op in ops {
        acc = Some(match (acc.take(), op) {
            (None, k) => k.clone(),
            (Some(OpKind::Add(a)), OpKind::Add(b)) => OpKind::Add(a.compose(b)?),
            (Some(OpKind::Max(a)), OpKind::Max(b)) => {
                OpKind::Max(if b > &a { b.clone() } else { a })
            }
            (Some(OpKind::Min(a)), OpKind::Min(b)) => {
                OpKind::Min(if b < &a { b.clone() } else { a })
            }
            (Some(OpKind::Assign(v)), OpKind::Add(d)) => OpKind::Assign(d.apply(&v)?),
            (Some(_), k) => k.clone(),
        });
    }
    acc.ok_or_else(|| FerroError::Merge("composing an empty op list".into()))
}

/// Apply a composed op to a value.
pub fn apply_op(base: Option<&Value>, kind: &OpKind) -> Result<Value, FerroError> {
    Ok(match kind {
        OpKind::Assign(v) => v.clone(),
        OpKind::Add(d) => {
            let b = base.ok_or_else(|| FerroError::Merge("Add against a missing cell".into()))?;
            d.apply(b)?
        }
        OpKind::Max(v) => match base {
            Some(b) if b > v => b.clone(),
            _ => v.clone(),
        },
        OpKind::Min(v) => match base {
            Some(b) if b < v => b.clone(),
            _ => v.clone(),
        },
        other => {
            return Err(FerroError::Merge(format!(
                "{} is a whole-row op and has no cell value",
                other.name()
            )))
        }
    })
}

/// One contended cell at merge time.
#[derive(Debug, Clone)]
pub struct CellMerge {
    pub tbl: TableId,
    pub row: RowId,
    pub col: ColId,
    /// The value at the fork point (the LCA). Three-way merge is against this, which is what
    /// makes per-replica version vectors unnecessary.
    pub base: Option<Value>,
    /// The value on the target *now*.
    pub target: Option<Value>,
    /// The merging branch's composed effect.
    pub ours: OpKind,
    /// The target's composed effect since the fork, when the target moved under us.
    pub theirs: Option<OpKind>,
}

/// How one cell resolved.
#[derive(Debug, Clone, PartialEq)]
pub enum CellResolution {
    /// Only we wrote. The value to publish, and the op that did it.
    Clean { value: Value, op: Op },
    /// Both wrote and the ops compose. This is exit criterion 6: two branches' `qty -= n`
    /// arrive at `base - n1 - n2`, not at whichever landed last.
    Commuting { value: Value, op: Op },
    /// A policy succeeded while throwing a write away.
    Lossy { value: Value, op: Op, discarded: DiscardedWrite },
    /// Contradictory, under a policy that forbids it.
    Conflict(ConflictReport),
}

/// Resolve one cell. Guards are **not** consulted here — they are re-checked against the composed
/// state afterwards, which is the whole reason step 3 comes after step 2.
pub fn resolve_cell(
    c: &CellMerge,
    ours_branch: BranchId,
    policy: &dyn ColumnPolicyLookup,
) -> Result<CellResolution, FerroError> {
    let our_op = Op::new(c.tbl, c.row, Some(c.col), c.ours.clone());
    let theirs = match &c.theirs {
        // Nobody else touched it: compose against the fork value and publish.
        None => {
            let value = apply_op(c.base.as_ref().or(c.target.as_ref()), &c.ours)?;
            return Ok(CellResolution::Clean { value, op: our_op });
        }
        Some(t) => t.clone(),
    };

    // Both sides wrote. Compose when the algebra allows it.
    if c.ours.commutes_with(&theirs) {
        // The target already carries their effect, so ours composes on top of the target value.
        let value = apply_op(c.target.as_ref(), &c.ours)?;
        return Ok(CellResolution::Commuting { value, op: our_op });
    }

    // Two writes of the same value are not a conflict, whatever the policy.
    if let (OpKind::Assign(a), OpKind::Assign(b)) = (&c.ours, &theirs) {
        if a == b {
            return Ok(CellResolution::Clean { value: a.clone(), op: our_op });
        }
    }

    match policy.policy(c.tbl, c.col) {
        MergePolicy::Lww => {
            let value = apply_op(c.target.as_ref(), &c.ours)?;
            Ok(CellResolution::Lossy {
                value,
                op: our_op,
                discarded: DiscardedWrite {
                    branch: ours_branch,
                    op: Op::new(c.tbl, c.row, Some(c.col), theirs),
                    policy: MergePolicy::Lww,
                    reason: format!(
                        "LWW on {}.{}: the concurrent write on the target was overwritten",
                        c.tbl, c.col
                    ),
                },
            })
        }
        p @ (MergePolicy::Reject | MergePolicy::MultiValue | MergePolicy::Additive) => {
            Ok(CellResolution::Conflict(ConflictReport {
                kind: ConflictKind::ContradictoryAssign,
                tbl: c.tbl,
                row: c.row,
                col: Some(c.col),
                violated_guard: None,
                ours: Some(our_op),
                theirs: Some(Op::new(c.tbl, c.row, Some(c.col), theirs)),
                detail: format!(
                    "concurrent {} on {}.{} under policy {}",
                    c.ours.name(),
                    c.tbl,
                    c.col,
                    p
                ),
            }))
        }
    }
}

/// Re-evaluate guards against the composed state.
///
/// Three outcomes, and they are not the same thing:
/// - holds -> no report;
/// - evaluates false -> `GuardFailed`, **carrying the predicate back** so the agent can retry;
/// - cannot be evaluated -> `GuardUnevaluable`, which is not retryable.
pub fn check_guards(guards: &[Guard], ctx: &dyn GuardContext) -> Vec<ConflictReport> {
    let mut out = Vec::new();
    for g in guards {
        let (tbl, row, col) = match g.expr.referenced_cells().first() {
            Some((t, r, c)) => (*t, *r, Some(*c)),
            None => (TableId::default(), RowId::default(), None),
        };
        match g.check(ctx) {
            Ok(true) => {}
            Ok(false) => out.push(ConflictReport::guard_failed(g.clone(), tbl, row, col)),
            Err(e) => out.push(ConflictReport {
                kind: ConflictKind::GuardUnevaluable,
                tbl,
                row,
                col,
                violated_guard: Some(g.clone()),
                ours: None,
                theirs: None,
                detail: format!("guard could not be evaluated against the merged state: {}", e),
            }),
        }
    }
    out
}

/// Collect every cell effect a set of frames produced, composed per cell, with the fork-point
/// witness that the first op on that cell observed.
pub fn composed_cells(
    frames: &[&TxnFrame],
) -> Result<BTreeMap<(TableId, RowId, ColId), (OpKind, Option<Value>)>, FerroError> {
    let mut per_cell: BTreeMap<(TableId, RowId, ColId), (Vec<OpKind>, Option<Value>)> =
        BTreeMap::new();
    for f in frames {
        for op in &f.ops {
            let col = match op.col {
                Some(c) => c,
                None => continue, // whole-row ops are handled by the row layer
            };
            let e = per_cell.entry((op.tbl, op.row, col)).or_insert((Vec::new(), None));
            if e.1.is_none() {
                e.1 = op.witness.clone();
            }
            e.0.push(op.kind.clone());
        }
    }
    let mut out = BTreeMap::new();
    for (k, (ops, witness)) in per_cell {
        out.insert(k, (compose_ops(&ops)?, witness));
    }
    Ok(out)
}

/// The surface's three-way merge engine, in the shape of the shared [`Merger`] trait.
pub struct SurfaceMerger {
    log: Arc<dyn EffectLog>,
}

impl SurfaceMerger {
    pub fn new(log: Arc<dyn EffectLog>) -> Self {
        SurfaceMerger { log }
    }
}

impl Merger for SurfaceMerger {
    fn merge(
        &self,
        lca: &BranchRecord,
        ours: &[TxnFrame],
        theirs: &[TxnFrame],
        policy: &dyn ColumnPolicyLookup,
        merged_state: &dyn GuardContext,
    ) -> Result<MergeOutcome, FerroError> {
        // 1. de-duplicate by TxnId, because Add is not idempotent.
        let ours_frames = dedup_frames(ours);
        let theirs_frames = dedup_frames(theirs);
        for f in ours_frames.iter().chain(theirs_frames.iter()) {
            if let Some(first) = ours_frames.first() {
                if f.schema_ver != first.schema_ver {
                    return Ok(MergeOutcome::Conflict(vec![ConflictReport {
                        kind: ConflictKind::SchemaMismatch,
                        tbl: TableId::default(),
                        row: RowId::default(),
                        col: None,
                        violated_guard: None,
                        ours: None,
                        theirs: None,
                        detail: format!(
                            "frames written against schema versions {} and {}",
                            first.schema_ver, f.schema_ver
                        ),
                    }]));
                }
            }
        }

        // 2. compose per cell.
        let our_cells = composed_cells(&ours_frames)?;
        let their_cells = composed_cells(&theirs_frames)?;

        let mut applied: Vec<Op> = Vec::new();
        let mut composed: Vec<Op> = Vec::new();
        let mut discarded: Vec<DiscardedWrite> = Vec::new();
        let mut conflicts: Vec<ConflictReport> = Vec::new();

        for ((tbl, row, col), (ours_kind, witness)) in &our_cells {
            let theirs_entry = their_cells.get(&(*tbl, *row, *col));
            let target = match theirs_entry {
                Some((k, w)) => apply_op(w.as_ref().or(witness.as_ref()), k).ok(),
                None => witness.clone(),
            };
            let cell = CellMerge {
                tbl: *tbl,
                row: *row,
                col: *col,
                base: witness.clone(),
                target,
                ours: ours_kind.clone(),
                theirs: theirs_entry.map(|(k, _)| k.clone()),
            };
            let branch = ours_frames.first().map(|f| f.branch).unwrap_or(lca.branch_id);
            match resolve_cell(&cell, branch, policy)? {
                CellResolution::Clean { op, .. } => applied.push(op),
                CellResolution::Commuting { op, .. } => {
                    applied.push(op.clone());
                    composed.push(op);
                }
                CellResolution::Lossy { op, discarded: d, .. } => {
                    applied.push(op);
                    discarded.push(d);
                }
                CellResolution::Conflict(c) => conflicts.push(c),
            }
        }

        // 3. only now re-check the guards, against the merged state.
        let all_guards: Vec<Guard> = ours_frames
            .iter()
            .chain(theirs_frames.iter())
            .flat_map(|f| f.guards.iter().cloned())
            .collect();
        conflicts.extend(check_guards(&all_guards, merged_state));

        // 4. and only now pick the outcome.
        if !conflicts.is_empty() {
            return Ok(MergeOutcome::Conflict(conflicts));
        }
        if !discarded.is_empty() {
            return Ok(MergeOutcome::ResolvedWithLoss { applied, discarded });
        }
        if !composed.is_empty() {
            return Ok(MergeOutcome::Commuting { composed });
        }
        Ok(MergeOutcome::Clean)
    }

    fn diff(&self, from: BranchId, to: BranchId) -> Result<Diff, FerroError> {
        let frames = self.log.frames_for(to, 0)?;
        let mut ops = Vec::new();
        let mut guards = Vec::new();
        for f in dedup_frames(&frames) {
            ops.extend(f.ops.iter().cloned());
            guards.extend(f.guards.iter().cloned());
        }
        Ok(Diff { from, to, ops, guards })
    }
}

/// A negated effect, for `REVERT`.
///
/// `Add` inverts exactly. `Assign` inverts only to its witness — reverting an assign with no
/// recorded before-image is refused rather than guessed.
pub fn invert(kind: &OpKind, witness: Option<&Value>) -> Result<OpKind, FerroError> {
    Ok(match kind {
        OpKind::Add(d) => OpKind::Add(d.negate()),
        OpKind::Assign(_) => match witness {
            Some(w) => OpKind::Assign(w.clone()),
            None => {
                return Err(FerroError::Merge(
                    "cannot revert an Assign with no recorded before-image".into(),
                ))
            }
        },
        other => {
            return Err(FerroError::Merge(format!(
                "cannot invert {} at cell granularity",
                other.name()
            )))
        }
    })
}

/// Turn an arithmetic delta into the value it would produce, for reporting.
pub fn delta_of(kind: &OpKind) -> Option<Delta> {
    match kind {
        OpKind::Add(d) => Some(*d),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::types::LeaseDeadline;
    use crate::tel::guard::{CmpOp, GuardExpr};

    fn cell(ours: OpKind, theirs: Option<OpKind>, base: i32, target: i32) -> CellMerge {
        CellMerge {
            tbl: TableId(1),
            row: RowId(1),
            col: ColId(2),
            base: Some(Value::Integer(base)),
            target: Some(Value::Integer(target)),
            ours,
            theirs,
        }
    }

    #[test]
    fn two_decrements_compose_arithmetically() {
        // exit criterion 6: base 20, they took 5, we take 3 -> 12, not 17 and not 15.
        let c = cell(OpKind::Add(Delta::Int(-3)), Some(OpKind::Add(Delta::Int(-5))), 20, 15);
        let policy = PolicyTable::new();
        match resolve_cell(&c, BranchId::new(1, 0), &policy).unwrap() {
            CellResolution::Commuting { value, .. } => assert_eq!(value, Value::Integer(12)),
            other => panic!("expected Commuting, got {:?}", other),
        }
    }

    #[test]
    fn concurrent_assigns_are_forbidden_by_default_not_silently_lww() {
        let c = cell(OpKind::Assign(Value::Integer(1)), Some(OpKind::Assign(Value::Integer(2))), 0, 2);
        let policy = PolicyTable::new();
        match resolve_cell(&c, BranchId::new(1, 0), &policy).unwrap() {
            CellResolution::Conflict(r) => assert_eq!(r.kind, ConflictKind::ContradictoryAssign),
            other => panic!("expected Conflict, got {:?}", other),
        }
    }

    #[test]
    fn lww_succeeds_but_reports_the_discarded_write() {
        let mut policy = PolicyTable::new();
        policy.set(TableId(1), ColId(2), MergePolicy::Lww);
        let c = cell(OpKind::Assign(Value::Integer(1)), Some(OpKind::Assign(Value::Integer(2))), 0, 2);
        match resolve_cell(&c, BranchId::new(1, 0), &policy).unwrap() {
            CellResolution::Lossy { value, discarded, .. } => {
                assert_eq!(value, Value::Integer(1));
                assert_eq!(discarded.policy, MergePolicy::Lww);
            }
            other => panic!("expected Lossy, got {:?}", other),
        }
    }

    #[test]
    fn identical_concurrent_writes_are_not_a_conflict() {
        let c = cell(OpKind::Assign(Value::Integer(7)), Some(OpKind::Assign(Value::Integer(7))), 0, 7);
        let policy = PolicyTable::new();
        assert!(matches!(
            resolve_cell(&c, BranchId::new(1, 0), &policy).unwrap(),
            CellResolution::Clean { .. }
        ));
    }

    #[test]
    fn composing_two_identical_adds_does_not_collapse_them() {
        // Two separate `qty -= 5` statements in one task are -10. Only *frames* de-duplicate.
        let composed = compose_ops(&[OpKind::Add(Delta::Int(-5)), OpKind::Add(Delta::Int(-5))]).unwrap();
        assert_eq!(composed, OpKind::Add(Delta::Int(-10)));
    }

    #[test]
    fn a_replayed_frame_contributes_its_increments_once() {
        let mut f = TxnFrame::new(TxnId(7), BranchId::new(1, 0), CommitHash::ZERO, 0, 1);
        f.push_op(Op::new(TableId(1), RowId(1), Some(ColId(2)), OpKind::Add(Delta::Int(-5))));
        let frames = vec![f.clone(), f];
        assert_eq!(dedup_frames(&frames).len(), 1);
    }

    #[test]
    fn a_guard_that_fails_against_merged_state_returns_its_predicate() {
        // exit criterion 7: compose the Adds first, then re-check `qty >= 0`.
        let g = Guard::holds(GuardExpr::cmp(
            GuardExpr::col(TableId(1), RowId(1), ColId(2)),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(0)),
        ))
        .with_source("qty >= 0");
        let mut merged = CellState::new();
        merged.set(TableId(1), RowId(1), ColId(2), Value::Integer(-3));
        let reports = check_guards(&[g], &merged);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].kind, ConflictKind::GuardFailed);
        assert!(reports[0].feedback().contains("qty >= 0"));
    }

    #[test]
    fn an_unevaluable_guard_is_not_reported_as_a_failed_one() {
        let g = Guard::holds(GuardExpr::cmp(
            GuardExpr::col(TableId(9), RowId(9), ColId(9)),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(0)),
        ));
        let reports = check_guards(&[g], &CellState::new());
        assert_eq!(reports[0].kind, ConflictKind::GuardUnevaluable);
    }

    #[test]
    fn merger_trait_reports_commuting_for_two_branch_decrements() {
        struct NoLog;
        impl EffectLog for NoLog {
            fn append(&self, _f: &TxnFrame) -> Result<(), FerroError> {
                Ok(())
            }
            fn frames_for(&self, _b: BranchId, _s: u64) -> Result<Vec<TxnFrame>, FerroError> {
                Ok(Vec::new())
            }
        }
        let m = SurfaceMerger::new(Arc::new(NoLog));
        let lca = BranchRecord::trunk(0, LeaseDeadline(0));
        let mut ours = TxnFrame::new(TxnId(1), BranchId::new(1, 0), CommitHash::ZERO, 0, 1);
        ours.push_op(
            Op::new(TableId(1), RowId(1), Some(ColId(2)), OpKind::Add(Delta::Int(-3)))
                .with_witness(Value::Integer(20)),
        );
        let mut theirs = TxnFrame::new(TxnId(2), BranchId::new(2, 0), CommitHash::ZERO, 0, 1);
        theirs.push_op(
            Op::new(TableId(1), RowId(1), Some(ColId(2)), OpKind::Add(Delta::Int(-5)))
                .with_witness(Value::Integer(20)),
        );
        let mut merged = CellState::new();
        merged.set(TableId(1), RowId(1), ColId(2), Value::Integer(12));
        let out = m
            .merge(&lca, &[ours], &[theirs], &PolicyTable::new(), &merged)
            .unwrap();
        assert!(matches!(out, MergeOutcome::Commuting { .. }));
    }

    #[test]
    fn invert_refuses_an_assign_with_no_before_image() {
        assert!(invert(&OpKind::Assign(Value::Integer(1)), None).is_err());
        assert_eq!(
            invert(&OpKind::Add(Delta::Int(-5)), None).unwrap(),
            OpKind::Add(Delta::Int(5))
        );
    }
}
