//! Provenance: which agent, run and model wrote a row.
//!
//! Design authority: DESIGN.md section 2 and exit criterion 9.
//!
//! **Provenance is an interned slot, not a fat header.** The actor tuple has *run-level*
//! cardinality — it is constant across every row a run writes — so storing it literally per
//! version costs roughly 3.4x density for nothing. Each version carries a small [`ProvId`] into a
//! page-local dictionary that points at one reified [`RunEntity`].

pub mod readset;
pub mod revert;

pub use readset::{
    blind_writes, AccessShape, Bound, PredicateSummary, ReadSet, ReadSetBuilder, ReadSetForm,
    VersionRef,
};
pub use revert::{
    DependencyEdge, DependencyGraph, DependencyGraphBuilder, RevertMode, RevertPlan,
};

use std::fmt::{Display, Formatter};

use crate::branch::types::BranchId;
use crate::error::FerroError;
use crate::storage::heap_file_manager::RecordId;

/// A dictionary slot, not a value. Stored per version; resolved through the provenance store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ProvId(pub u32);

impl ProvId {
    /// Reserved: "no provenance recorded" (writes that predate the agent layer).
    pub const NONE: ProvId = ProvId(0);

    pub fn is_none(&self) -> bool {
        *self == ProvId::NONE
    }
}

impl Display for ProvId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "prov{}", self.0)
    }
}

/// The reified actor behind a set of writes. One per agent run, referenced by every version that
/// run produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEntity {
    pub prov_id: ProvId,
    /// Stable identity of the agent across runs.
    pub agent_id: String,
    /// This particular invocation.
    pub run_id: String,
    pub model: String,
    pub model_version: String,
    /// Hash of the prompt that produced the run. Hashed rather than stored so a prompt containing
    /// customer data does not become a durable copy of it.
    pub prompt_hash: [u8; 32],
    /// Unix epoch milliseconds.
    pub started_at: u64,
    /// The branch this run was forked onto.
    pub parent_branch: BranchId,
}

impl RunEntity {
    pub fn new(
        prov_id: ProvId,
        agent_id: impl Into<String>,
        run_id: impl Into<String>,
        model: impl Into<String>,
        model_version: impl Into<String>,
        prompt_hash: [u8; 32],
        started_at: u64,
        parent_branch: BranchId,
    ) -> Self {
        RunEntity {
            prov_id,
            agent_id: agent_id.into(),
            run_id: run_id.into(),
            model: model.into(),
            model_version: model_version.into(),
            prompt_hash,
            started_at,
            parent_branch,
        }
    }

    /// The one-line answer to "which agent + run + model wrote this row".
    pub fn describe(&self) -> String {
        format!(
            "agent={} run={} model={}/{} branch={}",
            self.agent_id, self.run_id, self.model, self.model_version, self.parent_branch
        )
    }
}

/// Interning store for run entities plus per-version attribution.
pub trait ProvenanceStore: Send + Sync {
    /// Intern a run, returning its slot. Interning the same run twice must return the same
    /// `ProvId` — attribution is run-level, so a second call is a lookup, not a new entity.
    fn intern(&self, run: &RunEntity) -> Result<ProvId, FerroError>;

    fn lookup(&self, id: ProvId) -> Result<RunEntity, FerroError>;

    /// Which run wrote the version in this slot. `ProvId::NONE` when unattributed.
    fn attribute(&self, rid: RecordId) -> Result<ProvId, FerroError>;

    /// Stamp a version with its author. Called on the write path, once per version, one `u32`.
    fn stamp(&self, rid: RecordId, id: ProvId) -> Result<(), FerroError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_entity_answers_exit_criterion_nine() {
        let r = RunEntity::new(
            ProvId(3),
            "restock-agent",
            "run-42",
            "claude-opus",
            "2026-05",
            [0u8; 32],
            1_700_000_000_000,
            BranchId::new(4, 0),
        );
        let d = r.describe();
        assert!(d.contains("restock-agent"));
        assert!(d.contains("run-42"));
        assert!(d.contains("claude-opus/2026-05"));
    }

    #[test]
    fn prov_id_zero_means_unattributed() {
        assert!(ProvId::NONE.is_none());
        assert!(!ProvId(1).is_none());
    }
}
