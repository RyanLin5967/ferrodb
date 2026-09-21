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
pub mod attest;
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
pub use reaper::TwoTierReaper;
pub use record::{CoreRecord, 
    changed_columns, reclaimable, ArenaExtent, BranchRecord, CapabilityEnvelope,
    CapabilityRefusal, ColumnCapability, PendingFree, RowEffect, RowImage, TableCapability, Verb,
};
pub use types::{
    ArenaId, BranchError, BranchId, BranchState, CommitHash, Epoch, LeaseDeadline, PageId,
    ARENA_EXTENT_PAGES,
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

    // **D41 — THERE IS NO `put`, AND THAT IS THE POINT.**
    //
    // `fn put(&self, record: &BranchRecord)` used to sit here: "durably replace a record, the
    // caller is responsible for having read the current one". Every one of its sixteen callers
    // was a read-modify-write — `get` (or `get_raw`), mutate one or two fields, write the whole
    // record back — and a whole-record write is the **necessary condition** for the defect class
    // D20, D29, D33 and D34 are all instances of: a field the caller never read travels back
    // into the store as a stale snapshot, silently discarding whatever landed in the window. A
    // `set_root` publishing a copy-on-write root, a `renew_lease` keepalive, a
    // `charge_row_writes` spend, a `fork` appending a child's epoch — any of them.
    //
    // Narrowing it was not enough, because the caller's *read* is the racy half and no signature
    // can force a caller to hold a lock it does not know about. So the operations below each take
    // the branch id and the new value, and every implementation performs the read-modify-write
    // inside its OWN lock. Nothing can clobber a field a caller never named.
    //
    // **None of them has a default body, for the same reason `charge_row_writes` refuses to have
    // one: the obvious default IS the race.** A new implementation has to decide how it makes
    // each of these atomic rather than inherit a `get`/mutate/write-back that looks correct.
    //
    // A concrete catalog may still keep a whole-record writer of its own — `LogBranchCatalog`
    // needs one internally, and both durable catalogs' format and replay tests write records the
    // engine would never produce on purpose. What it may not do is put one on this trait, where
    // any holder of a `dyn BranchCatalog` reaches it.

    /// Move `branch` under a new parent, publishing its new root in the same atomic write.
    ///
    /// **NO PRODUCTION CALLER, AND THINLY TESTED — D63.** Its only caller was `collapse`, which
    /// D63 deleted. It is kept because the narrowness is the point: any future re-parent must move
    /// these four fields this way, and re-deriving that is how D20/D29/D33/D34 happened. Treat
    /// what follows as the contract a caller would have to meet, not a description of one that
    /// exists.
    ///
    /// ⚠ **What actually covers it, measured — not what you might assume.**
    /// `grep -rn '\.reparent(' src tests examples` returns one real invocation
    /// (`table_catalog.rs`, a `TableBranchCatalog` unit test) plus three trait forwards that no
    /// test drives. `tests/d41_narrow_ops_close_the_window.rs` does **not** call `reparent` — its
    /// two reproductions are `restrict_envelope` and `charge_row_writes`, and its `reparent` is a
    /// bare forward satisfying the trait. `LogBranchCatalog::reparent` lost its only driver when
    /// D63 deleted the collapse suite, which `reaper_suite!(log_catalog, …)` had run against it;
    /// `catalog.rs`'s `reparent_moves_the_four_position_fields_and_nothing_else` was added by D63
    /// to replace it. Do not assume more coverage than those two tests.
    ///
    /// **A RE-PARENT IS MORE THAN THIS CALL, and the rest lives only here now.** On
    /// `TableBranchCatalog` — what production opens — this write goes through `write_record`,
    /// which rewrites the record, state, deadline, envelope and arena keys and **never touches the
    /// children index**. So a caller must also detach the branch from its old parent and
    /// `attach_child` it to the new one; `collapse` did exactly that around this call. A caller
    /// that follows only the paragraphs below gets a branch missing from its new parent's live set
    /// and still listed under its old one: the new parent reads as childless, the interval rule
    /// frees pages the branch is still reading, and the old parent is pinned for ever.
    ///
    /// **D41, site 1.** The four fields move together or not at all — `parent_id`, `fork_epoch`,
    /// `depth` and `root_page_id` describe one position in the tree, and a reader that saw three
    /// of them would see a branch whose root belongs to an ancestry it no longer has. `depth` is
    /// not a parameter: it is `parent.depth + 1` by definition, and a caller permitted to state
    /// it could contradict the tree it just asked for.
    ///
    /// **UNCONDITIONAL, not a compare-and-swap, and that is a decision rather than an omission.**
    /// The standard answer to a lost update is a version check and a caller retry (ZooKeeper's
    /// `BadVersionException`, etcd's `Compare(ModRevision)`, a conditional write). Its premise is
    /// that the caller *can* retry. `collapse`'s could not: by the time it reached here it had
    /// copied up to 65,536 pages (256 MiB) into fresh extents, and a refusal left it the choice of
    /// copying a quarter-gigabyte again or abandoning the extents it had already claimed. So this
    /// write wins, and it wins **narrowly**: it touches four fields and reads nothing else, so a
    /// lease renewal, an envelope charge or an arena claimed concurrently survives it untouched.
    /// That is the whole difference from the `put` it replaced.
    ///
    /// Returns the record as written, because the caller needs it and the implementation has just
    /// built it — a second `get` would be a second chance to read something else.
    fn reparent(
        &self,
        branch: BranchId,
        parent: BranchId,
        fork_epoch: Epoch,
        root: PageId,
    ) -> Result<BranchRecord, FerroError>;

    /// Narrow what `branch` may write, **atomically with respect to every other mutation.**
    ///
    /// **D41, site 2: `AgentRuntime::restrict_branch`.** Narrow, never widen: an envelope already
    /// in force refuses anything wider than itself ([`BranchRecord::restrict`]), and a branch with
    /// no envelope is ungoverned, so the first call installs freely.
    ///
    /// The atomicity is the capability property, not a performance one. Through the old
    /// `get`/`restrict`/`put` the comparison was made against a snapshot: two restrictions racing
    /// meant the later write put back the envelope *it* had compared against, so a branch could
    /// end up wider than a restriction that had already been accepted — a widening reached by
    /// losing a write rather than by being granted one. Here the comparison and the write happen
    /// under one lock, so the second restriction is measured against the first and refused.
    ///
    /// Only the envelope moves. A `set_root`, a `renew_lease` or an `add_arena` that lands in the
    /// window is not part of this write and cannot be discarded by it.
    fn restrict_envelope(
        &self,
        branch: BranchId,
        envelope: CapabilityEnvelope,
    ) -> Result<(), FerroError>;

    /// Move `branch` from state `expect` to state `to`, **atomically**, or refuse.
    ///
    /// **D41, sites 3 and 4: quarantine/release, and both of `reap`'s state marks.** Seven call
    /// sites were hand-rolling this as `get` → assign → `put`, six of them spelled
    /// `get` → [`BranchRecord::mark_reaped`] → `put`.
    ///
    /// **A compare-and-swap here, unlike [`Self::reparent`], because every caller can act on a
    /// refusal and none of them has done irreversible work first.** `expect` is what the caller
    /// read; a mismatch means the branch moved underneath it, and continuing would publish a
    /// transition from a state that no longer holds — releasing a branch from a quarantine that
    /// was already lifted, or re-reaping one somebody else is reaping. `expect == to` is a
    /// no-op and writes nothing.
    ///
    /// **`to == Reaped` carries `mark_reaped`'s full meaning: the generation is bumped and the
    /// arena list is cleared.** That is not an extra service, it is what the state means — a
    /// reaped id slot must never answer to the handle that used to own it, and the reaper frees
    /// exactly `record.arenas` before it gets here, so leaving them listed would name extents
    /// that are already back in the free-space map. Implementations must do both.
    ///
    /// Generation-checked but **not** `check_readable`-checked: `Reaping` is unreadable by
    /// design, and the transition out of it — the second half of every reap — has to be
    /// expressible. A stale handle is still refused, for the reason `add_arena` refuses one
    /// (D33): a recycled slot must not be driven by the branch that used to live in it.
    fn set_state(
        &self,
        branch: BranchId,
        expect: BranchState,
        to: BranchState,
    ) -> Result<(), FerroError>;

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
    /// Branches whose lease expired at or before `now_millis`.
    ///
    /// **Returns CORE records, not whole ones.** The only consumer is the reaper, which reads
    /// `branch_id`, `depth` and `fork_epoch` — all core — and never touches `arenas` or `envelope`.
    /// Returning whole records forced the table catalog to `hydrate` EVERY ANSWER ROW: an arena
    /// range-scan plus an envelope lookup, both discarded. Measured at **24.3 us per answer row**
    /// against ~5 us for a core descent (`bench/s13_calibration_smoke.txt`). This is `SCALE-DESIGN`
    /// D2 — *the linear scan is in the TRAIT, not the implementation* — recurring at ANSWER scale,
    /// where an answer can be 10^6 rows during a mass expiry.
    fn expired_before(&self, now_millis: u64) -> Result<Vec<CoreRecord>, FerroError>;

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

    /// [`Self::scan`] restricted to the **inclusive** branch-id range `[lo, hi]`, same order.
    ///
    /// A narrowing the caller may ask for and an implementation may decline: the **default
    /// implementation filters a full scan**, which returns exactly the right records and is not one
    /// instruction faster. That is deliberate. It means adding this method broke no implementation
    /// and, more importantly, that a caller cannot tell a narrowing catalog from a non-narrowing one
    /// by its answer — only by its clock. An implementation whose records are already keyed by
    /// branch id (`tree_keys::tag::RECORD` is `[0x00][id big-endian]`, so byte order *is* id order)
    /// overrides it with a range descent and turns O(catalog) into O(matching + log N).
    ///
    /// `lo > hi` is an empty range and yields nothing. It is reachable rather than hypothetical:
    /// `branch_id >= 9 AND branch_id <= 3` intersects to exactly that, and the honest answer to it
    /// is no rows, not every row.
    fn scan_ids(
        &self,
        lo: u64,
        hi: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        Ok(Box::new(self.scan()?.filter(move |r| match r {
            Ok(rec) => rec.branch_id.id >= lo && rec.branch_id.id <= hi,
            // An error is never filtered out. Dropping it here would turn a partway read failure
            // into a short result that looks like a narrow one.
            Err(_) => true,
        })))
    }

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
    /// The counterpart of [`Self::detach_child`], and it exists for the same reason. The case that
    /// forced it was the since-deleted `collapse` (D63), which re-parented a branch with
    /// `trunk.add_live_child(epoch)` followed by `put(&trunk)` — a RECORD mutation. A catalog that
    /// keeps children in an index does not write the child span from `put` — it cannot, because
    /// the records it hands out carry an empty live set — so the re-parented branch would never
    /// appear among trunk's children and trunk's pages would look unreferenced by it.
    ///
    /// Its live caller today is `TableBranchCatalog::migrate_from` (verified by
    /// `grep -rn '\.attach_child(' src` after D63). `fork` does NOT come through here — each
    /// catalog writes the child entry inside its own fork path.
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

    /// Record that `branch` owns `arena`, **atomically**.
    ///
    /// **D20.** This exists because `ArenaPageStore::alloc_arena` used to do it as
    /// `get_raw` (UNLOCKED) -> push -> `put` (LOCKED) -- a read-modify-write whose READ raced
    /// every concurrent writer. With no latch protocol under the B+tree, a reader descending
    /// during a split returned a record with the wrong arena list, and that list was written
    /// straight back; the reaper then freed exactly `record.arenas` and the rest leaked.
    /// Measured: 0 pages leaked at 1 thread, 24 at 8 threads, 0 on the log catalog
    /// (`bench/d20_race_control.txt`).
    ///
    /// The whole read-modify-write must happen inside the implementation's own lock. An
    /// implementation that derives `arenas` from an index can satisfy this with a single key
    /// write and no read at all.
    fn add_arena(&self, branch: BranchId, arena: ArenaId) -> Result<(), FerroError>;

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
}
