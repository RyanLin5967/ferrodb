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
//!    ask my parent" step is a spec violation (BranchBench measured that pattern at up to 5400x
//!    read degradation).
//! 3. **No refcounts, no content addressing, no compaction.** Liveness is answered by the
//!    epoch interval rule in [`record::reclaimable`], never by a global reachability question.
//! 4. **Reading a reaped branch is a hard error, never stale data.**
//! 5. **Leases are non-cooperative.** Every branch has a deadline; the reaper does not wait for
//!    a client to close anything.

pub mod catalog;
pub mod record;
pub mod types;

pub use catalog::{LogBranchCatalog, TRUNK_LEASE};
pub use record::{reclaimable, ArenaExtent, BranchRecord, PendingFree};
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

    /// Every live branch, for the lease scan.
    fn live_branches(&self) -> Result<Vec<BranchRecord>, FerroError>;

    /// Extend a lease. Purely advisory to the holder — expiry does not require cooperation.
    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError>;
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
