//! What `DIFF` and `MERGE` hand back.
//!
//! Design authority: DESIGN.md exit criteria 4, 5 and 7.
//!
//! Both are **structured**, never rendered text. `DIFF` is rows, each with the typed ops that
//! produced it and the outcome it is currently headed for; `MERGE` is per-row outcomes, each one
//! of the four (`Clean`, `Commuting`, `Conflict`, `ResolvedWithLoss`), and a conflicting row
//! carries the violated predicate itself so the agent can retry with real feedback rather than a
//! boolean.

use std::fmt::{Display, Formatter};

use crate::branch::types::BranchId;
use crate::catalog::column::Value;
use crate::tel::guard::Guard;
use crate::tel::ids::{RowId, TableId};
use crate::tel::merge::{ConflictReport, DiscardedWrite, MergeOutcome};
use crate::tel::op::Op;

/// What happened to a row on a branch, at row granularity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowChangeKind {
    Insert,
    Update,
    Delete,
}

impl Display for RowChangeKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            RowChangeKind::Insert => "INSERT",
            RowChangeKind::Update => "UPDATE",
            RowChangeKind::Delete => "DELETE",
        })
    }
}

/// Where a change stands relative to the branch it would merge into.
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeOutcome {
    /// Not merged yet, and nothing on the target has touched this row since the fork.
    Pending,
    /// Not merged yet, and the target *has* moved under this row. Whether that is a conflict is
    /// only knowable at merge time, after composition and the guard re-check — which is why this
    /// is not called `Conflict`.
    PendingConcurrent,
}

// There is deliberately no `Merged` variant: a merged branch has no workspace left to diff, so a
// variant nothing can produce would be a claim the type does not back.

impl Display for ChangeOutcome {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeOutcome::Pending => write!(f, "pending"),
            ChangeOutcome::PendingConcurrent => write!(f, "pending (target moved)"),
        }
    }
}

/// One row of a structured changeset: identity, the typed ops, before/after images, the guards
/// that admitted it, and the outcome.
#[derive(Debug, Clone, PartialEq)]
pub struct RowChange {
    pub table: String,
    pub tbl: TableId,
    pub row: RowId,
    pub kind: RowChangeKind,
    /// The typed effects on this row, in capture order. `Add` here is what makes two branches'
    /// `qty -= n` compose arithmetically instead of clobbering.
    pub ops: Vec<Op>,
    /// The image at the fork point. `None` for an insert.
    pub before: Option<Vec<Value>>,
    /// The image on the branch now. `None` for a delete.
    pub after: Option<Vec<Value>>,
    /// The predicates that made these ops legal, kept verbatim.
    pub guards: Vec<Guard>,
    pub outcome: ChangeOutcome,
}

/// The answer to `DIFF`. Exit criterion 4.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeSet {
    pub from: BranchId,
    pub to: BranchId,
    pub rows: Vec<RowChange>,
}

impl ChangeSet {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

impl Display for ChangeSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "diff {} -> {}: {} row(s)", self.from, self.to, self.rows.len())?;
        for r in &self.rows {
            write!(f, "\n  {} {}.{} [{}]", r.kind, r.table, r.row, r.outcome)?;
            for op in &r.ops {
                write!(f, "\n    op {} on {}", op.kind.name(), match op.col {
                    Some(c) => c.to_string(),
                    None => "<row>".to_string(),
                })?;
            }
            for g in &r.guards {
                write!(f, "\n    guard {}", g.violated_predicate())?;
            }
        }
        Ok(())
    }
}

/// One row's merge result. A `Conflict` here carries the predicate that failed — exit criterion 7
/// is not satisfied by a boolean.
#[derive(Debug, Clone, PartialEq)]
pub struct RowMergeOutcome {
    pub table: String,
    pub tbl: TableId,
    pub row: RowId,
    pub outcome: MergeOutcome,
    /// Ops that were (or would be) applied to the target for this row.
    pub applied: Vec<Op>,
    /// Writes a policy threw away to reach a result. Non-empty implies `ResolvedWithLoss`.
    pub discarded: Vec<DiscardedWrite>,
    pub conflicts: Vec<ConflictReport>,
}

impl RowMergeOutcome {
    /// The violated predicates for this row, verbatim, for handing back to the agent.
    pub fn violated_predicates(&self) -> Vec<String> {
        self.conflicts
            .iter()
            .filter_map(|c| c.violated_guard.as_ref().map(|g| g.violated_predicate()))
            .collect()
    }
}

/// The answer to `MERGE`. Exit criterion 5.
///
/// `outcome` is the aggregate over `rows`, and it is deliberately pessimistic in one direction:
/// any row that lost a write makes the whole merge `ResolvedWithLoss`, never `Clean`.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeReport {
    pub merge_id: String,
    pub from: BranchId,
    pub into: BranchId,
    pub outcome: MergeOutcome,
    pub rows: Vec<RowMergeOutcome>,
    /// True when the merge was published to the target. False means the merge was rejected and
    /// the target was left **untouched** — which is the only honest report for a conflict, since
    /// a half-applied merge is exactly what a merge exists to prevent.
    pub applied_to_target: bool,
}

impl MergeReport {
    /// Aggregate the per-row outcomes. Order matters: a conflict beats a loss beats a
    /// composition beats clean.
    pub fn aggregate(rows: &[RowMergeOutcome]) -> MergeOutcome {
        let mut conflicts: Vec<ConflictReport> = Vec::new();
        let mut discarded: Vec<DiscardedWrite> = Vec::new();
        let mut applied: Vec<Op> = Vec::new();
        let mut composed: Vec<Op> = Vec::new();
        for r in rows {
            conflicts.extend(r.conflicts.iter().cloned());
            discarded.extend(r.discarded.iter().cloned());
            applied.extend(r.applied.iter().cloned());
            if let MergeOutcome::Commuting { composed: c } = &r.outcome {
                composed.extend(c.iter().cloned());
            }
        }
        if !conflicts.is_empty() {
            return MergeOutcome::Conflict(conflicts);
        }
        if !discarded.is_empty() {
            return MergeOutcome::ResolvedWithLoss { applied, discarded };
        }
        if !composed.is_empty() {
            return MergeOutcome::Commuting { composed };
        }
        MergeOutcome::Clean
    }

    /// Every violated predicate across the merge, verbatim.
    pub fn violated_predicates(&self) -> Vec<String> {
        self.rows.iter().flat_map(|r| r.violated_predicates()).collect()
    }
}

impl Display for MergeReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} -> {}: {}{}",
            self.merge_id,
            self.from,
            self.into,
            self.outcome.name(),
            if self.applied_to_target { "" } else { " (target unchanged)" }
        )?;
        for r in &self.rows {
            write!(f, "\n  {}.{}: {}", r.table, r.row, r.outcome.name())?;
            for c in &r.conflicts {
                write!(f, "\n    conflict {:?}: {}", c.kind, c.feedback())?;
            }
            for d in &r.discarded {
                write!(f, "\n    DISCARDED under {}: {}", d.policy, d.reason)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tel::guard::{CmpOp, GuardExpr};
    use crate::tel::ids::ColId;
    use crate::tel::merge::MergePolicy;
    use crate::tel::op::{Delta, OpKind};

    fn row(outcome: MergeOutcome, discarded: Vec<DiscardedWrite>, conflicts: Vec<ConflictReport>) -> RowMergeOutcome {
        RowMergeOutcome {
            table: "inventory".into(),
            tbl: TableId(1),
            row: RowId(1),
            outcome,
            applied: Vec::new(),
            discarded,
            conflicts,
        }
    }

    #[test]
    fn a_lossy_row_makes_the_whole_merge_lossy_never_clean() {
        let d = DiscardedWrite {
            branch: BranchId::new(2, 0),
            op: Op::new(TableId(1), RowId(1), Some(ColId(2)), OpKind::Assign(Value::Integer(1))),
            policy: MergePolicy::Lww,
            reason: "older writer".into(),
        };
        let agg = MergeReport::aggregate(&[
            row(MergeOutcome::Clean, vec![], vec![]),
            row(MergeOutcome::ResolvedWithLoss { applied: vec![], discarded: vec![d] }, vec![DiscardedWrite {
                branch: BranchId::new(2, 0),
                op: Op::new(TableId(1), RowId(1), Some(ColId(2)), OpKind::Assign(Value::Integer(1))),
                policy: MergePolicy::Lww,
                reason: "older writer".into(),
            }], vec![]),
        ]);
        assert!(agg.lost_a_write());
        assert_ne!(agg.name(), "Clean");
    }

    #[test]
    fn a_conflicting_row_outranks_a_commuting_one() {
        let g = Guard::holds(GuardExpr::cmp(
            GuardExpr::col(TableId(1), RowId(1), ColId(2)),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(0)),
        ))
        .with_source("qty >= 0");
        let c = ConflictReport::guard_failed(g, TableId(1), RowId(1), Some(ColId(2)));
        let agg = MergeReport::aggregate(&[
            row(
                MergeOutcome::Commuting {
                    composed: vec![Op::new(TableId(1), RowId(1), Some(ColId(2)), OpKind::Add(Delta::Int(-5)))],
                },
                vec![],
                vec![],
            ),
            row(MergeOutcome::Conflict(vec![c]), vec![], vec![ConflictReport::guard_failed(
                Guard::holds(GuardExpr::cmp(
                    GuardExpr::col(TableId(1), RowId(1), ColId(2)),
                    CmpOp::Ge,
                    GuardExpr::Literal(Value::Integer(0)),
                ))
                .with_source("qty >= 0"),
                TableId(1),
                RowId(1),
                Some(ColId(2)),
            )]),
        ]);
        assert!(agg.is_conflict());
        assert!(agg.conflicts()[0].feedback().contains("qty >= 0"));
    }

    #[test]
    fn an_empty_merge_is_clean() {
        assert_eq!(MergeReport::aggregate(&[]), MergeOutcome::Clean);
    }
}
