//! Merge outcomes and policy.
//!
//! Design authority: DESIGN.md section 3 ("Merge").
//!
//! Three-way against the LCA (the fork point). Having an LCA is strictly stronger than being a
//! CRDT replica: the result is `l + (v1 - l) + (v2 - l)` with **no per-replica vectors that grow
//! forever**. A PN-counter carries O(#replicas) metadata that never shrinks; ephemeral agents
//! would grow it without bound.

use std::fmt::{Display, Formatter};

use crate::branch::record::BranchRecord;
use crate::branch::types::BranchId;
use crate::error::FerroError;
use crate::tel::frame::TxnFrame;
use crate::tel::guard::{Guard, GuardContext};
use crate::tel::ids::{ColId, RowId, TableId};
use crate::tel::op::Op;

/// Per-column concurrent-write policy, declared in schema. Prior art: AntidoteSQL 2019.
///
/// **The default is `Reject`, not `Lww`.** AntidoteSQL's choice: a column with no modifier makes
/// concurrent updates *forbidden*. Defaulting to last-writer-wins would silently discard an
/// agent's work on every unannotated column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MergePolicy {
    /// Concurrent writes are forbidden. Any concurrent pair is a `Conflict`.
    #[default]
    Reject,
    /// Last writer wins. Succeeding under this policy *discards a write*, so it yields
    /// `ResolvedWithLoss`, never `Clean`.
    Lww,
    /// Keep both values and surface them to the caller.
    MultiValue,
    /// Compose numerically. Only meaningful for `Add`/`Max`/`Min`.
    Additive,
}

impl Display for MergePolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            MergePolicy::Reject => "REJECT",
            MergePolicy::Lww => "LWW",
            MergePolicy::MultiValue => "MULTI_VALUE",
            MergePolicy::Additive => "ADDITIVE",
        })
    }
}

/// Where a column's policy comes from. Implemented over the catalog.
pub trait ColumnPolicyLookup: Send + Sync {
    /// Must return [`MergePolicy::Reject`] for any column with no declared modifier.
    fn policy(&self, tbl: TableId, col: ColId) -> MergePolicy;
}

/// Why a merge could not proceed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConflictKind {
    /// Two branches assigned different values to a cell whose policy is `Reject`.
    ContradictoryAssign,
    /// A guard failed when re-evaluated against the merged state. This covers bounded counters:
    /// the `Add`s composed fine, then `qty >= 0` stopped holding.
    GuardFailed,
    /// One side deleted the row the other side wrote to.
    DeleteVsWrite,
    /// The two frames were written against different schema versions.
    SchemaMismatch,
    /// A guard could not be evaluated at all. Distinct from `GuardFailed`: not retryable.
    GuardUnevaluable,
}

/// Everything the agent needs in order to retry rather than guess.
#[derive(Debug, Clone, PartialEq)]
pub struct ConflictReport {
    pub kind: ConflictKind,
    pub tbl: TableId,
    pub row: RowId,
    pub col: Option<ColId>,
    /// **The violated predicate itself**, when a guard was involved. Exit criterion 7 is not
    /// satisfied by a boolean; the agent has to be handed the predicate back.
    pub violated_guard: Option<Guard>,
    /// The two contending ops, when the conflict was op-vs-op.
    pub ours: Option<Op>,
    pub theirs: Option<Op>,
    pub detail: String,
}

impl ConflictReport {
    pub fn guard_failed(guard: Guard, tbl: TableId, row: RowId, col: Option<ColId>) -> Self {
        let detail = format!("guard no longer holds against merged state: {}", guard.violated_predicate());
        ConflictReport {
            kind: ConflictKind::GuardFailed,
            tbl,
            row,
            col,
            violated_guard: Some(guard),
            ours: None,
            theirs: None,
            detail,
        }
    }

    /// The string a retrying agent is shown.
    pub fn feedback(&self) -> String {
        match &self.violated_guard {
            Some(g) => format!("{}: {}", self.detail, g.violated_predicate()),
            None => self.detail.clone(),
        }
    }
}

impl Display for ConflictReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} at {}.{}: {}", self.kind, self.tbl, self.row, self.feedback())
    }
}

/// A write that a policy threw away in order to succeed.
#[derive(Debug, Clone, PartialEq)]
pub struct DiscardedWrite {
    pub branch: BranchId,
    pub op: Op,
    pub policy: MergePolicy,
    pub reason: String,
}

/// **Four outcomes, not three.**
///
/// `ResolvedWithLoss` is mandatory. Reporting a lossy resolution as `Clean` is the most dangerous
/// thing this system can do to an agent: it tells the agent its write landed when the write is
/// gone.
#[derive(Debug, Clone, PartialEq)]
pub enum MergeOutcome {
    /// Main untouched. Only knowable **after** the constraint pass has run, not before it.
    Clean,
    /// Both sides wrote and the ops compose: `Add`+`Add`, `SetInsert` ∪ `SetInsert`, and so on.
    Commuting { composed: Vec<Op> },
    /// Contradictory writes, or a guard that failed against the merged state.
    Conflict(Vec<ConflictReport>),
    /// A policy succeeded **while discarding a write**.
    ResolvedWithLoss { applied: Vec<Op>, discarded: Vec<DiscardedWrite> },
}

impl MergeOutcome {
    pub fn is_conflict(&self) -> bool {
        matches!(self, MergeOutcome::Conflict(_))
    }

    /// True iff a write was thrown away. Callers must surface this to the agent.
    pub fn lost_a_write(&self) -> bool {
        matches!(self, MergeOutcome::ResolvedWithLoss { .. })
    }

    pub fn conflicts(&self) -> &[ConflictReport] {
        match self {
            MergeOutcome::Conflict(v) => v,
            _ => &[],
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            MergeOutcome::Clean => "Clean",
            MergeOutcome::Commuting { .. } => "Commuting",
            MergeOutcome::Conflict(_) => "Conflict",
            MergeOutcome::ResolvedWithLoss { .. } => "ResolvedWithLoss",
        }
    }
}

impl Display for MergeOutcome {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeOutcome::Clean => write!(f, "Clean"),
            MergeOutcome::Commuting { composed } => {
                write!(f, "Commuting ({} composed ops)", composed.len())
            }
            MergeOutcome::Conflict(v) => {
                write!(f, "Conflict ({} report(s))", v.len())?;
                for c in v {
                    write!(f, "\n  {}", c)?;
                }
                Ok(())
            }
            MergeOutcome::ResolvedWithLoss { applied, discarded } => write!(
                f,
                "ResolvedWithLoss ({} applied, {} DISCARDED)",
                applied.len(),
                discarded.len()
            ),
        }
    }
}

/// A structured changeset between two branch states. Exit criterion 4.
#[derive(Debug, Clone, PartialEq)]
pub struct Diff {
    pub from: BranchId,
    pub to: BranchId,
    pub ops: Vec<Op>,
    pub guards: Vec<Guard>,
}

/// The three-way merge engine.
///
/// Implementations must, in this order:
/// 1. de-duplicate `Add` ops by `TxnId` (they are not idempotent);
/// 2. compose commuting ops against the LCA;
/// 3. **then** re-evaluate every guard from both sides against the composed state;
/// 4. only then decide between `Clean`, `Commuting`, `Conflict` and `ResolvedWithLoss`.
///
/// Step 3 cannot move earlier: a guard checked against pre-merge state is checked against a state
/// that will not exist after the merge.
pub trait Merger: Send + Sync {
    fn merge(
        &self,
        lca: &BranchRecord,
        ours: &[TxnFrame],
        theirs: &[TxnFrame],
        policy: &dyn ColumnPolicyLookup,
        merged_state: &dyn GuardContext,
    ) -> Result<MergeOutcome, FerroError>;

    fn diff(&self, from: BranchId, to: BranchId) -> Result<Diff, FerroError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Value;
    use crate::tel::guard::{CmpOp, GuardExpr};

    #[test]
    fn default_policy_forbids_concurrent_writes() {
        // AntidoteSQL's choice, and the safe one: an unannotated column is not silently LWW.
        assert_eq!(MergePolicy::default(), MergePolicy::Reject);
    }

    #[test]
    fn lossy_resolution_never_reads_as_clean() {
        let lossy = MergeOutcome::ResolvedWithLoss { applied: vec![], discarded: vec![] };
        assert!(lossy.lost_a_write());
        assert_ne!(lossy.name(), MergeOutcome::Clean.name());
        assert!(!MergeOutcome::Clean.lost_a_write());
    }

    #[test]
    fn conflict_carries_the_predicate_back_to_the_agent() {
        let g = Guard::holds(GuardExpr::cmp(
            GuardExpr::col(TableId(1), RowId(1), ColId(2)),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(0)),
        ))
        .with_source("qty >= 0");
        let report = ConflictReport::guard_failed(g, TableId(1), RowId(1), Some(ColId(2)));
        assert!(report.feedback().contains("qty >= 0"));
        assert_eq!(report.kind, ConflictKind::GuardFailed);
    }
}
