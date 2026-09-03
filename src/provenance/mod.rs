//! Provenance: which agent, run and model wrote a row.
//!
//! Design authority: DESIGN.md section 2 and exit criterion 9.
//!
//! **Provenance is an interned slot, not a fat header.** The actor tuple has *run-level*
//! cardinality — it is constant across every row a run writes — so storing it literally per
//! version is pure waste. Each version carries a small [`ProvId`] into a page-local dictionary that
//! points at one reified [`RunEntity`].
//!
//! The cost is measured rather than quoted, by
//! `store::tests::the_density_numbers_the_docs_quote_are_the_numbers_this_computes`: the tuple is
//! **101 bytes** against a **1-byte** slot, so 200 versions cost 20,200 bytes literal against 204
//! interned — **99x**. This header previously said "roughly 3.4x density", which was the
//! row-inflation figure for an unstated ~40-byte row, and was measured nowhere.

pub mod capture;
pub mod durable;
pub mod readset;
pub mod revert;
pub mod sha256;
pub mod store;

pub use capture::{
    CapturingScan, ProvenanceLog, RowIdSource, SurrogateColumn, TimedPredicate, TxnCapture,
    TxnProvenance, VersionSource, WriteRecord,
};
pub use readset::{
    blind_writes, AccessShape, Bound, PredicateSummary, ReadSet, ReadSetBuilder, ReadSetForm,
    VersionRef,
};
pub use revert::{
    DependencyEdge, DependencyGraph, DependencyGraphBuilder, RevertMode, RevertPlan,
};
pub use durable::DurableProvenanceStore;
pub use sha256::{prompt_digest, sha256 as sha256_of, to_hex, Sha256};
pub use store::{MemProvenanceStore, PageProvDict, MAX_PAGE_DICT_ENTRIES, PROV_SLOT_BYTES};

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

    /// Whether two entities describe the same actor. `prov_id` is excluded deliberately: it is
    /// the store's assigned slot, not part of the run's identity, so a caller may present an
    /// entity carrying [`ProvId::NONE`] and still be recognised.
    /// Is `other` the same actor as this one — same agent, same run, same model and prompt?
    ///
    /// **`started_at` is deliberately excluded, and that exclusion is a bug fix rather than a
    /// simplification.** It is when a particular session began, not part of who the actor is. While
    /// it was included, `intern` behaved differently depending on whether the system clock happened
    /// to advance between two calls: a second session for one run was REFUSED when the clock moved
    /// (its `started_at` differed) and silently ACCEPTED when it did not. Same input, two
    /// behaviours, decided by clock granularity — and the refusal blamed "a different actor tuple"
    /// when nothing about the actor had differed.
    ///
    /// CI found it: the test asserting the refusal passed on macOS and failed on an Ubuntu runner
    /// where both sessions landed inside one tick. It was never a platform difference, only a
    /// faster machine making the coincidence likely.
    ///
    /// With `started_at` out, the contract is what it always claimed to be and is now decidable
    /// from the values alone: one run is one entity, a repeat with the same actor reuses its id,
    /// and only a genuine change of model, prompt or parent is refused.
    pub fn same_actor(&self, other: &RunEntity) -> bool {
        self.agent_id == other.agent_id
            && self.run_id == other.run_id
            && self.model == other.model
            && self.model_version == other.model_version
            && self.prompt_hash == other.prompt_hash
            && self.parent_branch == other.parent_branch
    }

    /// Bytes this tuple would cost if it were written literally into every version header — the
    /// thing the interned slot exists to avoid. Strings counted as their bytes plus a 2-byte
    /// length prefix each.
    pub fn literal_footprint(&self) -> usize {
        let s = |x: &String| x.len() + 2;
        s(&self.agent_id)
            + s(&self.run_id)
            + s(&self.model)
            + s(&self.model_version)
            + self.prompt_hash.len()
            + std::mem::size_of::<u64>()
            + std::mem::size_of::<BranchId>()
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

    // ── Logical row attribution ─────────────────────────────────────────────────────────────────
    //
    // `stamp` / `attribute` above are keyed by the PHYSICAL `(page_id, slot_num)`, because that is
    // what the executor knows at the moment it writes a version. "Which agent wrote this row" is
    // asked about the LOGICAL row: DESIGN.md is explicit that `RowId` is the immutable surrogate
    // and that physical position is not identity, so a row that moves pages must keep its author.
    //
    // These four lived on `AgentRuntime`'s `State` as two in-memory `BTreeMap`s until row E79c,
    // and that is exactly how criterion 9 came to hold for one process and not one moment longer:
    // the physical stamps survived a restart while the map that turned them into an *answer* did
    // not, so a reopened database showed rows that still looked attributed and could name nobody.
    // They belong on the store because the store is the layer that outlives the process — a
    // `MemProvenanceStore` still forgets, which is the honest behaviour for an in-memory store and
    // is what the anti-vacuity half of E79c's test asserts.
    //
    // The key is `(u32, u64)` — `table_id`'s FNV hash of the table NAME, and `RowId` — rather than
    // typed ids, because `TableId`/`RowId` live in `tel` and this module sits below it.

    /// Record that the run `id` published the logical row `(table, row)`.
    ///
    /// **`ProvId::NONE` clears the attribution rather than being refused**, which is the one place
    /// this differs from [`ProvenanceStore::stamp`]. A publish carrying no run is a plain write, and
    /// "nobody is on record for this row any more" is a fact that has to be recordable: the
    /// alternative leaves the previous run named as the author of a version it did not write, which
    /// is a confident wrong answer where `None` was available.
    fn stamp_row(&self, table: u32, row: u64, id: ProvId) -> Result<(), FerroError>;

    /// Which run last published the logical row. `ProvId::NONE` when nobody is on record — never a
    /// guess, and never the author of a neighbouring row.
    fn row_author(&self, table: u32, row: u64) -> Result<ProvId, FerroError>;

    /// Every attributed row of one table, as `(row, run)`, ordered by row id.
    fn attributed_rows(&self, table: u32) -> Result<Vec<(u64, ProvId)>, FerroError>;

    /// Forget every row attribution for one table, because the TABLE itself is gone.
    ///
    /// Deliberately NOT called for a `DELETE`: authorship of a deleted row is the audit record
    /// criterion 9 exists to keep, and it outliving the row is the point. A dropped table is a
    /// different question, because `table_id` hashes the table's NAME and that name can come back
    /// attached to entirely different data.
    fn forget_table(&self, table: u32) -> Result<(), FerroError>;
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
