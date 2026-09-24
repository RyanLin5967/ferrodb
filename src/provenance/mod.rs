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
pub mod deferred;
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
pub use deferred::ProvenanceFlush;
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

    /// Stamp a version with its author in the index NOW — with every guard `stamp` applies, so a
    /// refusal still happens at the write that caused it — and leave its durable record PENDING:
    /// written only by `flush`, or ahead of whatever the store's next durable write carries.
    ///
    /// **D219.** A MERGE stamps every version it publishes, and `stamp` syncs once per call, so a
    /// merge of δ versions paid δ fsyncs on its publish loop before its row authorship paid one
    /// more. Its stamps come here instead, and the MERGE's single sync carries them.
    ///
    /// A caller that stamps through this must make the records durable before it acknowledges
    /// anything. [`ProvenanceFlush`] is the guard that makes that hold on every exit — early
    /// returns and panics included — and it is the only intended way in.
    fn stamp_pending(&self, rid: RecordId, id: ProvId) -> Result<(), FerroError>;

    /// Make every pending record durable: ONE append and ONE sync for all of them. Nothing pending
    /// is not a write: no sync, and no refusal even from a store that is refusing writes (a lock
    /// poisoned by a panicking writer is the one exception: the durable store refuses it rather
    /// than panic inside the `Drop`s that call this).
    fn flush(&self) -> Result<(), FerroError>;

    /// Intern a run in the index NOW, with every guard `intern` applies, so a refused re-intern is
    /// refused here, and leave its durable record PENDING until [`Self::await_run`].
    ///
    /// **D246.** `BEGIN AGENT SESSION` interned through `intern`, which syncs a new run's record,
    /// inside `begin_session_as_staged`: under the runtime's `state` lock and inside pgwire's
    /// catalog guard, so every statement on the server waited behind that fsync. This is the half
    /// that belongs under the guard; `await_run` is the half that does not.
    ///
    /// A caller that interns through this must call `await_run` for the returned id before it tells
    /// anyone the run exists. `ForkDurability` is the value that makes that hold on every exit, and
    /// it is the only intended way in.
    fn intern_pending(&self, run: &RunEntity) -> Result<ProvId, FerroError>;

    /// Return once run `id`'s record is durable. That is at once when it already is (a run interned
    /// by `intern`, recovered from the file, or made durable by an earlier call), and otherwise after
    /// writing it and awaiting a sync that covers it.
    ///
    /// **Call it outside any wide lock.** The durable store writes pending run records under its
    /// file lock and waits for their sync OUTSIDE it, group-committed: forks completing together
    /// share one sync, and a fork being staged meanwhile does not wait for it.
    ///
    /// A publish calls it too, before the log is told the run exists (`bind_run`): the WAL must
    /// never declare a slot the provenance file could lose (D246 A3).
    fn await_run(&self, id: ProvId) -> Result<(), FerroError>;

    /// Refuse NOW if this store would refuse a write now.
    ///
    /// For a caller about to make a change it cannot undo and will have to record here afterwards:
    /// an ALTER's rewrite moves rows and must then re-stamp them at their new rids. Asked before the
    /// change, a store that is refusing writes (a durable store poisoned by a failed append) stops
    /// the change instead of leaving it made and unrecorded (D219, PREREG A1).
    ///
    /// Advisory, not a reservation: the write itself still checks under its own lock, so a store
    /// that starts refusing between this and the write is refused there. **Required, with no
    /// default**, for the reason `page_dictionary_lens` is: a default `Ok` would claim every store
    /// writable.
    fn check_writable(&self) -> Result<(), FerroError>;

    /// Every page that carries attribution, as `(page_id, distinct runs in its dictionary)`.
    ///
    /// The per-page dictionary refuses past [`MAX_PAGE_DICT_ENTRIES`], so *how close a workload
    /// runs to that cap* is a property of the workload rather than of any one page. Nothing else
    /// on this trait can answer it: `attribute` speaks about one slot, and
    /// `MemProvenanceStore::page_dictionary_len` about one page whose id you already knew. Those
    /// cannot tell a single hot page from a saturated page population — which are opposite
    /// findings — because both require enumerating the pages that exist.
    ///
    /// Deliberately a **required** method with no default. A default returning an empty `Vec`
    /// would let an implementation report "no page is anywhere near the cap" for a store that had
    /// simply never been asked, which is the one wrong answer that reads exactly like a clean one.
    fn page_dictionary_lens(&self) -> Result<Vec<(u32, usize)>, FerroError>;

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

    /// Record that the run `id` published every row in `rows`, in order, as ONE durable unit.
    ///
    /// **D219.** `AgentRuntime::record_applied` called `stamp_row` once per applied op while
    /// holding the runtime's `state` lock, and a durable store syncs once per call, so a merge of δ
    /// ops held that lock across δ fsyncs. One merge's authorship is one decision, so it is
    /// recorded as one batch: a durable store makes it one append and one fsync, whatever δ is.
    ///
    /// The same outcome as calling `stamp_row` for each entry in order — the same guards, the same
    /// final state, `ProvId::NONE` clearing — and the same records in a durable file, repeats
    /// included. The one difference is the point of a batch: a refusal happens before ANY row is
    /// attributed, never after some of them.
    ///
    /// An empty `rows` records nothing and is not a write, so it succeeds even on a store that is
    /// refusing writes — exactly as making no `stamp_row` call at all would.
    fn stamp_rows(&self, rows: &[(u32, u64)], id: ProvId) -> Result<(), FerroError>;

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

    /// fsyncs this store has issued so far, by the kind of record each one made durable.
    ///
    /// An observing instrument: reading it changes nothing it counts. **Required, with no
    /// default**, for the reason `page_dictionary_lens` is: a default of zero would let a store
    /// that does fsync report that it never does, which is the one wrong answer that reads exactly
    /// like a right one for an in-memory store. `MemProvenanceStore` answers zero because it has no
    /// file, and that is a fact about it rather than a default.
    fn sync_counts(&self) -> SyncCounts;
}

/// fsyncs a provenance store has issued, split by the kind of record each one made durable.
///
/// Split rather than totalled for the reason [`durable::RecoveryReport`] is: a total hides one
/// write path behind another. D219 is the case that needed it. A `MERGE` against the durable store
/// used to pay syncs on two paths — the executor's physical `stamp`, once per published VERSION,
/// and `record_applied`'s logical row authorship, once per applied OP under `AgentRuntime`'s
/// `state` lock — and one number could not say which of the two moved.
///
/// **Each sync is booked once, under the write path that ISSUED it**, so the fields sum to the
/// syncs issued and `total()` is that count. Since D219 a sync also carries every PENDING record
/// (`stamp_pending`) ahead of its own, so a MERGE's physical stamps ride in the one sync its row
/// authorship issues and are booked under `row_authors`; a merge with no row to attribute (a
/// schema-only merge whose rewrite re-stamped moved rows) makes them durable with `flush`, booked
/// under `stamps`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SyncCounts {
    /// Syncs that made a newly interned run durable: `intern`'s own, and since D246 the sync
    /// `await_run` issues for pending run records. A repeat intern is a lookup and syncs nothing.
    pub runs: u64,
    /// Syncs issued by `stamp`, and by `flush` of pending physical stamps.
    pub stamps: u64,
    /// Syncs issued by logical `(table, row)` authorship: one per `stamp_row` call, and one per
    /// `stamp_rows` batch however many rows it carries — plus whatever physical stamps were
    /// pending when it was issued.
    pub row_authors: u64,
    /// Syncs that made a `DROP TABLE`'s forget durable.
    pub forgets: u64,
}

impl SyncCounts {
    /// Every sync issued, whatever it carried: each is booked under exactly one field. D219's exit
    /// — one sync per MERGE, physical and logical together — is stated in this number.
    pub fn total(&self) -> u64 {
        self.runs + self.stamps + self.row_authors + self.forgets
    }
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
