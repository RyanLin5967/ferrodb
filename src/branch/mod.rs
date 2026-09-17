//! Branch engine: the unit of isolation is an agent task, not a transaction.
//!
//! Design authority: DESIGN.md section 1.
//!
//! Invariants every implementor of the traits below must preserve:
//!
//! 1. **Fork copies zero data pages.** `fork` writes one `BranchRecord` and appends one epoch to
//!    the parent's `live_children`. Nothing else.
//! 2. **The read path never walks the parent chain.** The child's root *is* the parent's root at
//!    fork time, so ordinary B+tree descent already reaches parent data. Any "not found here,
//!    ask my parent" step is a spec violation (BranchBench, arXiv:2604.17180, measured that
//!    pattern at up to 4000x read degradation in Neon, DoltgreSQL, Xata and Tiger Data).
//! 3. **No refcounts, no content addressing, no compaction.** Liveness is answered by the
//!    epoch interval rule in [`record::reclaimable`], never by a global reachability question.
//! 4. **Reading a reaped branch is a hard error, never stale data.**
//! 5. **Leases are non-cooperative.** Every branch has a deadline; the reaper does not wait for
//!    a client to close anything.

pub mod arena;
pub mod catalog;
pub mod lease_thread;
pub mod reaper;
pub mod record;
pub mod types;

pub use arena::{privacy_barrier, ArenaPageStore};
pub use catalog::{LogBranchCatalog, TRUNK_LEASE};
pub mod tree_keys;
mod group_commit;
pub mod table_catalog;
pub use table_catalog::TableBranchCatalog;
pub use lease_thread::{CatalogLock, LeaseStats, LeaseThread, RuntimeLock};
pub use reaper::{PageLinks, TwoTierReaper};
pub use record::{CoreRecord, 
    changed_columns, reclaimable, ArenaExtent, BranchRecord, CapabilityEnvelope,
    CapabilityRefusal, ColumnCapability, PendingFree, RowEffect, RowImage, TableCapability, Verb,
};
pub use types::{
    ArenaId, BranchError, BranchId, BranchState, CommitHash, Epoch, LeaseDeadline, PageId,
    ARENA_EXTENT_PAGES, MAX_BRANCH_DEPTH,
};

use crate::error::FerroError;

/// Durable store of branch metadata. One record per branch; this is the whole branch.
///
/// Implementations must make `fork` atomic with respect to the parent's `live_children` update:
/// a child that exists but is not listed in its parent is a GC correctness hole.
pub trait BranchCatalog: Send + Sync {
    /// Allocate the next epoch. Strictly monotonic across the whole store.
    fn next_epoch(&self) -> Epoch;

    /// Current epoch without advancing it.
    fn current_epoch(&self) -> Epoch;

    /// Create a child of `parent`. Must copy **zero data pages**.
    fn fork(&self, parent: BranchId, lease: LeaseDeadline) -> Result<BranchRecord, FerroError>;

    /// Load a record. Returns `BranchError::Reaped` for a stale generation.
    fn get(&self, branch: BranchId) -> Result<BranchRecord, FerroError>;

    /// Durably replace a record. The caller is responsible for having read the current one.
    fn put(&self, record: &BranchRecord) -> Result<(), FerroError>;

    /// Publish a new root for a branch. This is the commit point of shadow paging: until the
    /// root pointer moves, a writing branch's pages are invisible to everyone (exit criterion 2).
    fn set_root(&self, branch: BranchId, root: PageId) -> Result<(), FerroError>;

    /// Branches whose lease has expired at or before `now_millis`: `Live`, not trunk, expired.
    ///
    /// **The result is output-sized, not database-sized, and that is the entire point.** This runs
    /// every 30 seconds for the life of the process and the answer is almost always empty. It
    /// replaced a call that cloned *every* record in the catalog so the caller could filter it —
    /// unnoticeable at 10³ branches, fatal at 10⁶, and invisible to any measurement of `fork`.
    /// An implementation that walks every record to answer this has not implemented it, it has
    /// spelled it differently; see `SCALE-DESIGN.md` D2.
    ///
    /// Trunk is excluded here rather than left to the caller, because trunk holds a lease nobody
    /// may act on and a caller that forgot the check would reap the root of the database.
    fn expired_before(&self, now_millis: u64) -> Result<Vec<BranchRecord>, FerroError>;

    /// Every branch in `state`, in branch-id order. Output-sized, for the same reason.
    ///
    /// This answers the two questions that used to demand the whole catalog: which branches were
    /// mid-reap when the process died, and which are being held for inspection. Trunk is **not**
    /// filtered out — the two callers disagree about whether they want it, so each says.
    fn in_state(&self, state: BranchState) -> Result<Vec<BranchRecord>, FerroError>;

    /// Every record the catalog holds, whatever its state, **streamed in branch-id order**.
    ///
    /// This one is genuinely O(N) and no index changes that: its callers are a full system view
    /// and a full snapshot, and their answer *is* the whole catalog. What it must not do is
    /// materialise a second copy of the database — hence an iterator rather than a `Vec`. And
    /// hence "in branch-id order", which deletes the sort each caller performed afterwards.
    ///
    /// The `Result` is per item rather than only around the iterator because a streaming
    /// implementation reads pages as it goes and can fail partway. An infallible item type would
    /// force it to either swallow that or buffer the whole catalog first, and buffering the whole
    /// catalog is the thing being removed.
    fn scan(&self)
        -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError>;

    /// The **latest** fork epoch among this id slot's live children, or `None` if it has none.
    ///
    /// Generation-blind, like `LogBranchCatalog::get_raw`, and takes a raw `u64` to say so: the
    /// question is about an id SLOT's children, and the slot's owner may be mid-reap or already
    /// reaped while its children still pin pages.
    ///
    /// Replaces `rec.live_children.last()`. The array it replaces is unbounded — trunk's would hold
    /// 10⁶ epochs, 8 MB, in a record that must fit a 2 KB leaf page — and every question anyone
    /// asks of it is a range query over `(parent, fork_epoch)`. See `SCALE-DESIGN.md` D2b.
    fn max_live_child(&self, parent_id: u64) -> Result<Option<Epoch>, FerroError>;

    /// Does this id slot have **any** live child forked in `[lo, hi)`?
    ///
    /// **This is the reclamation rule.** A page born at `lo` and freed at `hi` may be reclaimed
    /// exactly when the answer is `false`: no child forked in the window, so nobody but the owner
    /// can see it. Half-open deliberately — a child that forked at the instant the page was freed
    /// never saw it.
    fn live_child_in_epoch_range(
        &self,
        parent_id: u64,
        lo: Epoch,
        hi: Epoch,
    ) -> Result<bool, FerroError>;

    /// Does this id slot have any live children at all? Replaces `rec.live_children.is_empty()`.
    fn has_live_children(&self, parent_id: u64) -> Result<bool, FerroError>;

    /// Number of branches in state `Live`, trunk included.
    ///
    /// On the trait because it is how every harness in the repo asserts that a reap actually
    /// happened, and those harnesses hold the catalog through this trait now.
    fn live_count(&self) -> usize;

    /// Fetch a record **ignoring the generation guard**, by id slot.
    ///
    /// Only the reaper and the page-reclamation path may use this. They have to read the record of
    /// a branch that is mid-reap or already reaped, because its children are still the authority
    /// over pages parked under its name — a generation-checked read would refuse exactly when the
    /// answer matters most.
    ///
    /// On the trait rather than inherent on one implementation because `ArenaPageStore` and
    /// `TwoTierReaper` held `Arc<LogBranchCatalog>` *concretely* in order to reach it, which meant
    /// no other catalog could ever be installed under them.
    fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError>;

    /// Mark an id slot reusable by a later `fork`.
    ///
    /// **Refuses silently while the slot still has live children**, because that set is what
    /// decides the fate of pages parked under this branch's name: handing the id out again while a
    /// child still points at it makes the parent of those pages ambiguous. Trunk (`id == 0`) is
    /// never released.
    fn release_id(&self, id: u64);

    /// Add one child to `parent_id`'s live set.
    ///
    /// The counterpart of [`Self::detach_child`], and it exists for the same reason: `collapse`
    /// re-parents a branch with `trunk.add_live_child(epoch)` followed by `put(&trunk)`, which is a
    /// RECORD mutation. A catalog that keeps children in an index does not write the child span
    /// from `put` — it cannot, because the records it hands out carry an empty live set — so the
    /// re-parented branch would never appear among trunk's children and trunk's pages would look
    /// unreferenced by it.
    ///
    /// `child_id` is stored so a reader can resolve the child and check whether it is still live;
    /// see the implementations for why an entry is a hint rather than an answer.
    fn attach_child(
        &self,
        parent_id: u64,
        fork_epoch: Epoch,
        child_id: u64,
    ) -> Result<(), FerroError>;

    /// Remove one child from `parent_id`'s live set. Returns whether anything was removed.
    ///
    /// **An explicit operation, not a record mutation followed by `put`.** The reaper used to do
    /// `get_raw(parent)`, `remove_live_child(epoch)`, `put(&parent)` — which works only while the
    /// live set travels inside the record. A catalog that keeps children in an index returns
    /// records with an empty set, so `remove_live_child` would report "nothing to do", the write
    /// would be skipped, and the index entry would survive its branch **for ever**: the parent's
    /// pages would then be pinned by a child that no longer exists and could never be reclaimed.
    /// Silent, permanent, and invisible to every existing test. Hence a method with a return value.
    fn detach_child(&self, parent_id: u64, fork_epoch: Epoch) -> Result<bool, FerroError>;

    /// Extend a lease. Purely advisory to the holder — expiry does not require cooperation.
    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError>;

    /// What this branch is permitted to write, without cloning the rest of the record.
    ///
    /// The write funnel asks this on **every** statement, governed or not, so the default's clone
    /// of arenas and live-children is worth overriding.
    fn envelope_of(&self, branch: BranchId) -> Result<Option<CapabilityEnvelope>, FerroError> {
        Ok(self.get(branch)?.envelope)
    }

    /// Spend `n` row-writes against this branch's capability envelope, **atomically with respect
    /// to every other mutation of that record.**
    ///
    /// No default implementation, deliberately. The obvious one — `get`, mutate, `put` — writes
    /// back a whole stale record snapshot, so a `set_root` publishing a copy-on-write root, a
    /// `renew_lease`, or a `fork` appending a child's epoch that landed in the window is silently
    /// discarded. The last of those is a GC correctness hole, which is exactly why `fork` puts
    /// both halves in one write. An implementor has to decide how it makes this atomic rather
    /// than inherit a race.
    ///
    /// Refuses if the charge does not fit, or if the branch has no envelope — a caller that got a
    /// charge from [`CapabilityEnvelope::admit`] and finds no envelope here is racing an operator
    /// installing or removing one, and admitting the write on the strength of a policy that no
    /// longer exists is the fail-open answer.
    fn charge_row_writes(&self, branch: BranchId, n: u64) -> Result<(), FerroError>;
}

/// The two-tier reaper. Fast path is the overwhelming majority of abandoned agent branches.
pub trait Reaper: Send + Sync {
    /// Reap one branch. Fast path (`BranchRecord::is_childless_leaf`) frees its arenas wholesale;
    /// slow path applies [`record::reclaimable`] and parks the rest as `PendingFree`.
    /// Returns the number of pages actually returned to the free space map.
    fn reap(&self, branch: BranchId) -> Result<u32, FerroError>;

    /// Scan all live branches and hard-reap everything past its lease deadline, with no client
    /// cooperation whatsoever. This is exit criterion 8. Returns the branches reaped.
    fn reap_expired(&self, now_millis: u64) -> Result<Vec<BranchId>, FerroError>;

    /// Re-examine the pending-free log against current `live_children` arrays and release what
    /// has since become reclaimable.
    fn drain_pending(&self) -> Result<u32, FerroError>;

    /// Materialise a branch's visible state to a fresh root and re-parent it to trunk, resetting
    /// depth to 1. Invoked when a fork would exceed `MAX_BRANCH_DEPTH`.
    fn collapse(&self, branch: BranchId) -> Result<BranchRecord, FerroError>;
}
