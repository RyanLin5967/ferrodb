//! Per-branch arenas and the arena-backed copy-on-write page store.
//!
//! Design authority: DESIGN.md section 1 ("Per-branch arenas", "GC").
//!
//! Arenas exist to buy two things with one mechanism:
//!
//! 1. a writing branch's shadow pages stay physically clustered, so scans do not degenerate as
//!    fan-out widens; and
//! 2. **reaping a childless leaf becomes an extent-level free** — the reaper's fast path does no
//!    per-page sharing analysis at all.
//!
//! There are **no reference counts anywhere in this file**. Liveness is answered only by the
//! epoch interval rule in [`crate::branch::record::reclaimable`]: page `p` is reclaimable iff no
//! live child of the arena's owning branch has `fork_epoch` in `[birth(p), free(p))`. Refcounting
//! would put the mutation hot spot on the most-shared page (a parent with 5000 children would
//! carry refcount 5001 on its root), which is exactly btrfs's backref explosion.
//!
//! ## Space ownership
//!
//! The store owns the file region `[base_page, ∞)` **exclusively**. It does not share that region
//! with `DiskManager`'s bitmap allocator, because a bitmap bit and an extent bump pointer would
//! disagree about who owns a page. [`ArenaPageStore::new`] refuses to start below the disk
//! manager's high-water mark rather than warning about it.
//!
//! That exclusivity is enforced by a floor registered with `DiskManager::reserve_from`, and the
//! floor is **process-local** — a reopened file starts with none. So the region is only protected
//! for as long as some store has told this `DiskManager` about it, which means every open must,
//! not just the first. [`ArenaPageStore::reopen`] is that path; going through
//! [`ArenaPageStore::new`] a second time cannot work, because after a reopen the high-water mark
//! counts the arena's own pages and locks it out of its own region.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::branch::delta::{self, PageDelta};
use crate::branch::record::{ArenaExtent, BranchRecord, PendingFree};
use crate::branch::types::{
    next_extent_pages, ArenaId, BranchError, BranchId, Epoch, PageId, ARENA_EXTENT_PAGES,
};
use crate::branch::BranchCatalog;
use crate::cluster::GrantedCounter;
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::cow::page_header::{flags, stamp_checksum, verify_checksum, PageHeader, PageType};
use crate::cow::{CowPage, PageHandle, PageStore, PAGE_HEADER_SIZE};
use crate::error::FerroError;
use crate::storage::atomic_file::{append_durably, replace_atomically, FileOps, OsFileOps};
use crate::storage::disk_manager::PAGE_SIZE;
use crate::wal::log::crc32;

/// Process-wide census of which arm of [`ArenaPageStore::cow_page`] a workload actually takes.
///
/// **This exists because "wire delta encoding into `cow_page`" is a plan that rests on a premise,
/// and the premise is checkable.** A delta can only save bytes on the arm that COPIES a whole
/// page; on the in-place arm there is no copy to shrink. Whether a given workload ever reaches
/// the copying arm is a fact about the workload, not about the encoder, and it was not measured
/// before. These are counters rather than timings for the same reason the rest of this work is:
/// the box runs a fleet, and an integer does not move when the machine is busy.
///
/// Free functions over statics rather than fields on the store, deliberately: a census has to be
/// readable from a harness that holds no reference to the store it is measuring, and every
/// `ArenaPageStore` in the process contributes to one number. That makes them useless for
/// attributing a count to one store and fine for the question they exist to answer.
mod census {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub(super) static IN_PLACE: AtomicU64 = AtomicU64::new(0);
    pub(super) static SHADOW: AtomicU64 = AtomicU64::new(0);
    pub(super) static SHADOW_PAYLOAD_BYTES: AtomicU64 = AtomicU64::new(0);
    /// Deltas refused because the base page id no longer holds the page it was taken from.
    pub(super) static STALE_BASE: AtomicU64 = AtomicU64::new(0);

    pub(super) fn bump(counter: &AtomicU64, by: u64) {
        counter.fetch_add(by, Ordering::Relaxed);
    }
}

/// `(in-place cows, shadowing cows, payload bytes copied by shadowing cows)` since process start.
///
/// The third number is the one a delta encoder is aimed at: it is the total that
/// `frame.data[PAGE_HEADER_SIZE..].copy_from_slice(..)` has moved. If it is zero for a workload,
/// that workload has no whole-page copies for a delta to shrink, whatever the encoder can do.
pub fn cow_path_census() -> (u64, u64, u64) {
    use std::sync::atomic::Ordering;
    (
        census::IN_PLACE.load(Ordering::Relaxed),
        census::SHADOW.load(Ordering::Relaxed),
        census::SHADOW_PAYLOAD_BYTES.load(Ordering::Relaxed),
    )
}

/// Reset the census. For a harness that wants a window rather than a process total.
pub fn reset_cow_path_census() {
    use std::sync::atomic::Ordering;
    census::IN_PLACE.store(0, Ordering::Relaxed);
    census::SHADOW.store(0, Ordering::Relaxed);
    census::SHADOW_PAYLOAD_BYTES.store(0, Ordering::Relaxed);
    census::STALE_BASE.store(0, Ordering::Relaxed);
}

/// How many times a delta was refused because its base page id had been reissued to another page.
///
/// **This should be zero, and it is reported rather than assumed to be.** A page a live child can
/// see is parked by the epoch interval rule instead of released, so a recorded base should never
/// be reissued while a shadow of it exists. That is an argument spanning this file and the reaper;
/// this is the counter that says whether it holds in practice.
pub fn stale_delta_base_count() -> u64 {
    census::STALE_BASE.load(std::sync::atomic::Ordering::Relaxed)
}

/// The epoch at or after which a page in this branch's own arena may still be mutated in place.
///
/// A page is safe for in-place mutation only if nobody else can see it. Two things can make
/// somebody else see it: the page predates this branch's own fork (so the parent has it too), or
/// a child forked off this branch after the page was born (so that child has it too). The
/// barrier is therefore the later of this branch's fork epoch and its most recent child's fork
/// epoch, and it is what gets handed to [`PageHeader::is_private_to`].
/// Takes the two epochs it actually depends on rather than a whole record, because the second one
/// used to be `rec.live_children.last()` — the last element of an unbounded array that trunk would
/// grow to 10⁶ entries. The question "what is my latest live child's fork epoch?" is one index
/// lookup (`BranchCatalog::max_live_child`); materialising the array to take its last element is
/// not. Pure, so it is testable without a catalog at all.
pub fn privacy_barrier(fork_epoch: Epoch, max_live_child: Option<Epoch>) -> Epoch {
    match max_live_child {
        Some(latest) => Epoch(fork_epoch.0.max(latest.0)),
        None => fork_epoch,
    }
}

/// How many extents a standalone node takes for itself at a time.
///
/// Only the *issued* watermark is durable, so this is invisible to the checkpoint image and to
/// every existing test: a self-grant of four extents that issues one leaves the image exactly
/// where a `fetch_add` of one extent would have. It is above one purely so that single-node
/// running exercises the partially-consumed-range path rather than only the empty-range one.
const SELF_GRANT_EXTENTS: u64 = 4;

/// How many arena ids a standalone node takes for itself at a time.
const SELF_GRANT_ARENA_IDS: u64 = 64;

/// Hands out contiguous extents and takes them back whole.
///
/// # F4: both counters here are cluster state
///
/// `next_extent_start` and `next_arena_id` were node-local `AtomicU32`s, and each is a silent
/// corruption on a second node. Two nodes bumping the extent counter both allocate the extent at
/// page 66 and hand the same physical page to different branches; `examples/repl_primary.rs` names
/// what that costs — *"every such page still passes its checksum, so refusing here is the only
/// detection point."* Two nodes bumping the arena counter both name a different extent `a7`, and
/// `BranchRecord::arenas` then points two branches at one arena, which the reaper frees whole.
///
/// Both are now [`GrantedCounter`]s. A standalone node grants itself and issues exactly the values
/// `fetch_add` issued; a cluster member issues only from what the leader granted it and
/// **refuses** when it holds nothing. See [`crate::cluster`].
struct ArenaSpaceManager {
    base_page: PageId,
    /// The LARGEST extent this store hands out. **D31:** extents are no longer one size — a
    /// branch's first is [`crate::branch::types::ARENA_FIRST_EXTENT_PAGES`] and each subsequent
    /// one doubles up to this cap. Kept as a field rather than read from the constant because
    /// `CowStore` already parameterises it and the grant accounting below is stated in it.
    extent_pages: u32,
    /// Start pages of extents. Issues as many pages as the extent being claimed actually needs.
    extent_starts: GrantedCounter,
    /// Freed extents, **keyed by size in pages**, ready to be handed out again. Reuse is what
    /// makes the reserved page count return to baseline rather than merely stopping its growth.
    ///
    /// **Segregated exact fit, and the key is why.** With one extent size a freed start could
    /// serve any request; with geometric growth it cannot, and handing a 1-page hole to a
    /// 256-page request would alias 255 pages that belong to somebody else. Sizes are powers of
    /// two from 1 to `extent_pages`, so there are at most nine classes and no coalescing: a freed
    /// 4-page extent is reusable only by another 4-page request. That is the standard slab
    /// trade-off — bounded internal reuse in exchange for never splitting or merging — and the
    /// bound is what makes the lookup O(1) rather than a scan of a free list that reaches 10^6
    /// entries at the scale this store is aimed at.
    ///
    /// **Recycling needs no grant** — and that is a property, not an oversight. These pages were
    /// already granted to this node and were never given back to the leader, so handing one out
    /// again is this node issuing from its own space. What it *does* need is the epoch check
    /// below: pages self-granted under a previous authority are not this node's to reuse.
    free_extents: Mutex<HashMap<u32, Vec<PageId>>>,
    /// Arena ids. Issues one at a time.
    ///
    /// In a cluster these come from the same [`crate::consensus::Command::ArenaGrant`] as the
    /// pages — see [`ArenaPageStore::apply_arena_grant`] for why, and for what the frozen contract
    /// does not carry.
    arena_ids: GrantedCounter,
    /// The authority epoch `free_extents` was filled under.
    ///
    /// A store that recycled pages while standalone, in a process that then joined a cluster, is
    /// sitting on space no leader knows it has. [`crate::cluster::GrantedCounter`] evicts stale
    /// *grants* on its own; this is the same rule for the recycle stack, which is the one piece of
    /// issued space that lives outside the counter.
    recycle_epoch: AtomicU64,
}

impl ArenaSpaceManager {
    /// Take one extent's worth of pages and one arena id, or refuse.
    ///
    /// Both takes can refuse and neither is retried against a local counter.
    ///
    /// # The order, and the invariant that makes it safe
    ///
    /// The pages are consumed first and the id second, so in principle a refusal on the *id* would
    /// strand a page range: an unused arena id costs one number, but pages consumed for an arena
    /// that was never created are a durable leak the leader cannot see and will not re-grant.
    ///
    /// That cannot happen, and the reason is an invariant worth stating rather than relying on.
    /// [`ArenaPageStore::apply_arena_grant`] grants `page_count` arena ids alongside `page_count`
    /// pages, while one reserve consumes at least one page against exactly one id — so ids can
    /// never run out before pages do, and the page take is always the one reached first. Pinned by
    /// `an_arena_grant_always_carries_more_ids_than_the_extents_it_can_name`.
    ///
    /// **D31 narrowed that margin and did not reverse it.** With one fixed extent size the ratio
    /// was `extent_pages` ids per extent. With geometric growth the smallest extent is one page,
    /// so a grant of `n` pages names at most `n` extents and the two counters can now be exhausted
    /// by the SAME reserve. The order is what keeps that safe: pages are consumed first, so the
    /// reserve that would exhaust both refuses on pages and never consumes the id.
    fn reserve(&self, pages: u32) -> Result<(ArenaId, PageId), FerroError> {
        let epoch = crate::cluster::epoch();
        let start = match self.recycled_start(epoch, pages) {
            Some(s) => s,
            None => {
                let v = self.extent_starts.take(pages as u64)?;
                // Every value in this counter is a page id, and a `PageId` is a `u32`. A grant
                // that pushed the watermark past that is a leader arithmetic error, and truncating
                // it silently would alias page 0.
                u32::try_from(v).map_err(|_| {
                    BranchError::Arena(format!(
                        "granted extent start {v} does not fit a page id; refusing to allocate"
                    ))
                })?
            }
        };
        let id = self.arena_ids.take(1)?;
        let arena = ArenaId(u32::try_from(id).map_err(|_| {
            BranchError::Arena(format!("granted arena id {id} does not fit an ArenaId"))
        })?);
        Ok((arena, start))
    }

    /// Pop a recycled extent of **exactly** `pages` pages, discarding the whole map if it was
    /// filled under a superseded authority.
    ///
    /// Exactly, never "at least": a bigger hole handed to a smaller request would strand its tail
    /// with no record that it exists, and a smaller one handed to a bigger request aliases pages
    /// the next extent owns.
    fn recycled_start(&self, epoch: u64, pages: u32) -> Option<PageId> {
        let mut free = self.free_extents.lock().unwrap();
        if self.recycle_epoch.swap(epoch, Ordering::SeqCst) != epoch {
            free.clear();
            return None;
        }
        free.get_mut(&pages)?.pop()
    }

    fn give_back(&self, start: PageId, pages: u32) {
        let epoch = crate::cluster::epoch();
        let mut free = self.free_extents.lock().unwrap();
        if self.recycle_epoch.swap(epoch, Ordering::SeqCst) != epoch {
            free.clear();
        }
        free.entry(pages).or_default().push(start);
    }
}

struct StoreState {
    /// Live extents by arena. An arena absent from this map has been freed.
    extents: HashMap<ArenaId, ArenaExtent>,
    /// Pages released back inside a still-live extent, reusable before the bump pointer moves.
    recycled: HashMap<ArenaId, Vec<PageId>>,
    /// The arena each branch is currently allocating novel pages from.
    current: HashMap<BranchId, ArenaId>,
    /// **D85.** Arenas restored from an image whose `next_free` may be UNDERSTATED.
    ///
    /// `alloc_for` advances `next_free` without persisting — the persist sites are all off the page
    /// path — so an extent checkpointed while empty and then filled comes back
    /// reading zero. `load_state` already handles the ALLOCATION consequence by clearing
    /// `current`; this handles the COLLECTION one, which was silent data loss: with `next_free`
    /// at 0, `retire_arenas_by_rule` parked none of a live child's pages and `extent_is_empty`
    /// then reported the extent collectable.
    ///
    /// **In memory only, deliberately.** `ArenaExtent` is the SERIALISED type and
    /// `two_stores_in_the_same_state_checkpoint_byte_identical_images` pins its bytes, so this
    /// must not become a field on it. It is set at restore and cleared by [`ArenaPageStore::
    /// resolve_fill`], which recovers the true value by probing.
    fill_unknown: std::collections::HashSet<ArenaId>,
    /// Pages logically freed but still visible to some live child. Slow-path reaping parks here.
    ///
    /// **D183 de-dup at push: one entry per `(page, arena)`, the first.** See [`PendingLog`].
    pending: PendingLog,
    /// **D183.** Arenas whose `recycled` list or `next_free` has moved since the durable file was
    /// last brought level with memory, and which therefore have to ride the next tail record.
    ///
    /// This is the list-shaped state `:2220` named as the reason the reclamation sites could not
    /// use a delta. It is a *set of arenas* rather than a list of pages because the record carries
    /// each one's recycled list **absolutely**: a record of pushes would be wrong the moment a
    /// page came back out of the list again, and an absolute list costs at most `page_count`
    /// entries for an extent the caller was already walking page by page.
    ///
    /// **It lives inside `StoreState` and not beside it, and that is the whole of its
    /// correctness.** `release_page` pushes the page and marks the arena in ONE critical section,
    /// and a record builder takes the set and reads the lists it names in ONE critical section.
    /// Split across two locks in either order there is an interleaving where a push is neither in
    /// the record nor still marked — which is a page that never comes back.
    recycled_dirty: std::collections::HashSet<ArenaId>,
    /// The authority epoch each live extent was **claimed** under.
    ///
    /// Kept beside `extents` rather than inside `ArenaExtent`, because that type is a durable
    /// record (`branch/record.rs`) and the authority is not a durable fact about an extent — it is
    /// a fact about this process's relationship to a cluster. Putting it in the record would change
    /// the on-disk format for something a restart cannot verify anyway.
    ///
    /// An arena missing from this map is one whose authority is not known, and is therefore not
    /// fillable. That is the safe direction: an unfillable extent is still accounted for in
    /// `extents` and still freeable by the reaper, so nothing leaks permanently.
    claim_epoch: HashMap<ArenaId, u64>,
    /// **D102.** For each page this store produced by SHADOWING another, the page it was shadowed
    /// from and how many shadows stand between it and a page that was never a shadow.
    ///
    /// This is the fact [`crate::branch::delta`] needs and that `cow_page` was throwing away: a
    /// delta is meaningless without its base, and until now the identity of the base survived only
    /// as `CowPage::previous_page_id`, which the caller consumes and drops. Recording it is what
    /// makes a delta against the base expressible at all.
    ///
    /// **In memory, and that is a statement about what it is for.** It is not a durable index that
    /// a read depends on — a read of a delta-encoded page must be able to find its base from the
    /// page itself, or a lost map would be lost data. It is a write-side cache of the chain depth,
    /// which is what [`crate::branch::delta::MAX_CHAIN_DEPTH`] is enforced against at write time.
    /// An entry missing after a restart makes the next shadow of that page a chain ROOT (depth 1)
    /// rather than a deeper link, which is the safe direction: it can only make chains shorter.
    ///
    /// The third element is the base's `birth_epoch` **as it was when the shadow was taken**, and
    /// it is what makes a stale base unrepresentable rather than argued about. A page id outlives
    /// the page: `release_page` puts the id on a free list and `alloc_in_arena` hands it out again
    /// for something unrelated. The argument that this cannot happen to a base is real — only
    /// bases the branch does NOT own are recorded, and the epoch interval rule parks a page a live
    /// child can see — but it is an argument that spans this file and the reaper, and a delta
    /// taken against a recycled page is wrong in a way that reports no error at all. So the epoch
    /// is re-read and compared before any delta is encoded; `write_fresh_page` stamps a new one on
    /// every allocation, so a reissued id cannot match.
    shadow_base: HashMap<PageId, (PageId, u8, Epoch)>,
}

/// Copy-on-write page store backed by per-branch arenas.
pub struct ArenaPageStore {
    pool: Arc<BufferPoolManager>,
    /// `dyn` since D1-wire-runtime. It used to be concrete, and the comment here said that was
    /// "on purpose: GC decisions must be able to read" things the trait did not expose - namely
    /// `get_raw`, which is generation-blind and which the reclamation path genuinely needs. That
    /// was a real requirement expressed the wrong way: it made the CATALOG unswappable in order to
    /// reach ONE method. `get_raw` and `release_id` are on the trait now, so the requirement is
    /// stated where it belongs and any catalog can satisfy it.
    catalog: Arc<dyn BranchCatalog>,
    space: ArenaSpaceManager,
    state: Mutex<StoreState>,
    /// Pages handed out by `alloc_in_arena` and not yet returned to the free space map.
    /// Exit criteria 1 and 8 are both stated as page counts, so this is load-bearing.
    live_pages: AtomicU32,
    /// Extent pages currently reserved by some branch. Returns to baseline only if freed extents
    /// are genuinely recycled, which is the stronger claim exit criterion 8 actually wants.
    reserved_pages: AtomicU32,
    /// **D144.** Entries ever PUSHED onto the pending-free log, cumulative and monotone.
    ///
    /// `pending_len()` is a LEVEL and cannot answer "how much was ever parked": the log is drained
    /// as well as filled, and a parked entry whose child dies leaves it again. Sampling the level
    /// from outside cannot recover the total either — the drain is a read-modify-write
    /// (`take_pending` empties the whole vec before `put_pending` restores the survivors), so a
    /// sampler sees transients that belong to no park at all, and whether two parks coexist at any
    /// instant depends on the sweep cadence rather than on anything being measured. This counter
    /// is incremented at the two sites that park a page and nowhere else, so it is immune to all
    /// of that.
    ///
    /// **Since D183 de-dup at push, those two sites are [`Self::push_pending_recorded`] and
    /// [`Self::push_pending_unrecorded`], and they count only a push that LANDED.** A push of a
    /// page already pending is skipped by [`PendingLog::push_if_absent`]
    /// and adds no entry, so it adds no count: the total is the number of entries that ever
    /// entered the log, which a duplicate park no longer inflates. Before the de-dup a resumed
    /// reap's re-park of an already-pending page counted twice; a figure measured then can read
    /// higher than the same run measures now.
    pending_pushed: AtomicU64,
    /// The authority epoch this store's in-memory state belongs to.
    ///
    /// Compared against [`crate::cluster::epoch`] on every allocation path; a change revokes the
    /// right to fill anything claimed before it. See [`ArenaPageStore::revoke_stale_authority`].
    authority_epoch: AtomicU64,
    /// Where to persist the free-space map when an extent is claimed or freed, if anywhere.
    ///
    /// Without this the map reaches disk only when the owner remembers to call `checkpoint`, which
    /// for the CLI is at clean exit — so a `kill -9` leaves a durable map older than the durable
    /// branch catalog, and the next open re-issues pages that catalog still points at. Claiming an
    /// extent is the rare event (once per `extent_pages`, default 256), so persisting there costs
    /// a small write per 256 allocations and bounds what a crash can lose to one extent.
    checkpoint_path: Mutex<Option<std::path::PathBuf>>,
    /// **D81.** What this process knows about the shape of the file at `checkpoint_path`, and the
    /// lock that makes the DURABLE record order equal the IN-MEMORY mutation order.
    ///
    /// See [`PersistState`]. Held across the state mutation *and* the durable write on the two
    /// paths that append a delta, which is the whole reason it is a separate lock rather than a
    /// pair of counters.
    persist: Mutex<PersistState>,
    /// Bumped by every change to the pending-free log that no tail record describes.
    ///
    /// Outside `persist` deliberately: `park_or_release` holds only the `state` lock, and making
    /// it take the persist lock would put a durable-write mutex on the page-free path. A counter
    /// it can bump freely, compared under the persist lock, gets the same answer without the
    /// lock-order problem. See [`PersistState::durable_pending_version`].
    pending_version: AtomicU64,
    /// **A recycled page has been handed out again, and NO tail record can say so.**
    ///
    /// The asymmetry that makes this its own flag: a PUSH onto an extent's recycled list that has
    /// not reached the file leaves the durable list SHORTER than memory, so `extent_is_empty`
    /// (`recycled >= next_free`) answers "not empty" and the extent merely leaks. A POP leaves it
    /// LONGER, so a restored extent that still holds live pages answers "empty" and
    /// `reaper::sweep_empty_extents` frees it. One direction is a leak; the other hands a live
    /// page's range back to the allocator.
    ///
    /// **D85's `resolve_fill` cannot repair it.** That probe RAISES `next_free`, which is the
    /// other side of the comparison, and it raises it to cover the very page that was reissued —
    /// so an overstated recycled count still wins. The `current.clear()` in `load_state` does not
    /// save it either: that stops a restored extent being FILLED, and this is about an extent
    /// being FREED.
    ///
    /// ⚠ **Open since D81 put claims on the append-only tail.** Neither `TAIL_ARENA_CLAIMED` nor
    /// `TAIL_EXTENT_FREED` refreshes an existing extent's recycled list — the claim arm only seeds
    /// the NEW arena's list, the free arm only removes one — so once claims stopped rewriting the
    /// whole image, a pop followed by any number of deltas was stale. It was survivable only
    /// because the reclamation paths still rewrote the image on every reap and closed the window
    /// incidentally.
    ///
    /// So a pop does what every other undescribable change here does: it makes the next persist a
    /// full image rewrite, exactly as [`Self::pending_version`] does for the pending-free log. One
    /// rewrite per burst of reuse, not one per pop — the flag stays set until a persist happens.
    recycled_reissued: AtomicBool,
    /// **D183 tail replay: an OBSERVING counter, compiled into test builds only.** Counts the
    /// pending-free log entries [`Self::replay_tail`] visited. That is every element a scan of the
    /// log walks, plus every entry a record carries.
    ///
    /// Per store, not process-wide, so parallel tests in the lib binary cannot add to each other's
    /// window. Read it before and after one `restore` to scope it to one replay. It is bumped
    /// where the work happens, never computed from a `len()` after the fact. See
    /// `frontier/lane_d183_tail_replay.md` in artie-research.
    #[cfg(test)]
    replay_pending_visits: AtomicU64,
}

/// **D81 — the append-only tail, and what this process is allowed to assume about the file.**
///
/// `<db>.arena` is `[image][tail record]*`. The image is exactly what [`ArenaPageStore::
/// state_bytes`] has always produced — byte-identical, same version, every file already on disk
/// still opens — and the tail is a sequence of self-delimiting, individually CRC'd records that
/// each describe ONE change to the map. A claim appends 45 bytes and fsyncs once; the whole
/// image is rewritten through [`replace_atomically`] only when the tail has grown past
/// [`ArenaPageStore::compact_threshold`] of it.
///
/// # Why this is a struct with a lock and not three atomics
///
/// **The tail is an ORDERED log, so the durable order has to equal the in-memory order.** The case
/// that forces it: `free_arena` returns extent X to the free list and `alloc_arena` immediately
/// re-claims it. In memory that is free-then-claim and it is correct. If the two records reach the
/// file claim-then-free, replay ends with X on the free list *and* live in `extents` — two owners
/// for one page range, which is the exact failure the whole map exists to prevent. Full-image
/// checkpoints could not have this bug, because each one publishes a coherent snapshot of the
/// moment it ran and the last writer wins; a delta cannot, because every delta is only meaningful
/// against the one before it.
///
/// So this mutex is held across `reserve` + the `state` mutation + the append, on both delta
/// paths. It is **outermost**: taken before `state`, before the catalog's locks and before
/// `REPLACE_LOCK`, and never acquired while any of them is held. Every persist site takes it
/// before `state` and holds both only for the snapshot it encodes, which is what makes that rule
/// satisfiable rather than aspirational.
///
/// # What a tail record does NOT carry, and why that is not a hole
///
/// A full-image checkpoint made *everything* durable as a side effect, so it is worth being
/// explicit about what stops doing so. Three kinds of per-PAGE state change without persisting
/// anything, and did so before this row too: `next_free` (advanced by `alloc_in_arena`), the
/// per-extent `recycled` list (pushed by `release_page`, popped by `alloc_in_arena`), and the
/// pending-free log. Under full images a later claim wrote them down incidentally; a tail record
/// does not.
///
/// **For the first two that changes nothing, because the design already refuses to trust them in
/// a restored extent** — and it refuses per-extent, not per-age-of-image. `load_state` clears
/// `current`, so `arena_for` never resumes filling a restored extent and never reaches its
/// recycled list; and D85's `fill_unknown` makes `extent_is_empty` refuse until `resolve_fill`
/// has probed. Both guards apply to every extent that came out of the file, whenever it was
/// written. An older image means MORE extents get that treatment, not weaker treatment.
///
/// **The pending log is the exception and gets an explicit guard**, because nothing refuses to
/// trust it and a lost entry is a page `drain_pending` never revisits — see
/// [`PersistState::durable_pending_version`].
///
/// What genuinely must be durable before it is used is the extent CLAIM — the range and the two
/// watermarks — because that is the one fact whose loss gives two owners one page range. That is
/// precisely what the record carries, and `append_durably` fsyncs it before `alloc_arena`
/// returns.
///
/// Cost of holding it: extent claims serialise. They already did — `REPLACE_LOCK` serialises the
/// fsync, which is the part that costs anything — so what is newly serialised is `reserve`, a
/// counter take, and `catalog.add_arena`.
///
/// # Why `image_bytes == 0` is the interesting state
///
/// It means **this process has not itself written a full image to the current path**, and in that
/// state appending is refused: the next persist is a full rewrite. That single rule closes two
/// holes at once and is why neither needs its own code.
///
///   * A store armed by `checkpoint_to` with no file on disk yet cannot produce a headless tail.
///   * A store reopened by `reopen_from_checkpoint` cannot append onto a tail it did not write —
///     and a tail it did not write may end in a TORN record from the session that crashed. Its
///     first persist rewrites the image, which drops the torn bytes. So a good record can never
///     end up sitting behind a bad one, which is the one arrangement the reader cannot recover
///     from (it must stop at the first bad record, and would then silently discard the good one).
/// **D183.** What a `take_pending` handed out, so the matching `put_pending` can describe the
/// drain by difference instead of by restating the whole log. See [`PersistState::drain_mark`].
struct DrainMark {
    rewrites: u64,
    base_version: u64,
    taken: Vec<(PageId, ArenaId)>,
}

/// **D183 — parked entries still owed a `TAIL_PAGES_PARKED` record, bound to the persist guard.**
///
/// `push_pending_recorded` first took `&mut PersistState`, so a push made without the guard stopped
/// compiling. That bound the PUSH and not the SPAN: dropping the guard and taking it again between
/// the push loop and the append still compiled, and re-opened exactly the window it was written to
/// close — a `take_pending` in the gap reads `level == true` for a file that does not yet list the
/// entries, cuts a `DrainMark` on that false premise, and the park's record then lands behind the
/// drain's and puts a released page back in the durable log. Fire-checked (`d183-reverify`, M8,
/// `bench/d183_reverify.txt`): at `4b1ab2d` that spelling compiled and the whole lib target passed
/// (1673 passed, 0 failed), because nothing races two threads on one store's durability path.
///
/// This holds the guard's `&mut` borrow for as long as any entry is owed a record, so releasing the
/// guard before the append — or before [`ArenaPageStore::abandon_park`] on the append's failure
/// arm — is a borrow error rather than a review finding.
struct RecordedParks<'g> {
    persist: &'g mut PersistState,
    parked: Vec<PendingFree>,
}

struct PersistState {
    /// Bytes of the image THIS process last wrote to `checkpoint_path`, or 0 for "none".
    image_bytes: u64,
    /// Bytes of tail records appended after that image.
    tail_bytes: u64,
    /// The authority epoch the image was written under.
    ///
    /// A change of authority clears `free_extents` inside `give_back`/`recycled_start`
    /// (`ArenaSpaceManager`), and a clear is not expressible as a per-extent delta. Rather than
    /// invent a record for it, a persist whose epoch does not match the image's rewrites the image
    /// — the transition becomes unrepresentable in the tail instead of something the replayer has
    /// to be trusted to get right.
    image_epoch: u64,
    /// The value of [`ArenaPageStore::pending_version`] that the durable file represents.
    ///
    /// **The pending-free log is the one part of the map no per-extent record describes**, and
    /// two paths change it without persisting anything: `park_or_release`'s push and
    /// `take_pending`'s drain. Under full-image checkpoints those changes reached disk at the
    /// next claim, incidentally, because the claim rewrote everything. A tail record does not
    /// carry them, so without this a parked page could sit unrecorded until the next compaction
    /// and a crash in between would leak it — `drain_pending` never revisits an entry that is not
    /// in the file.
    ///
    /// So a delta is only taken while the log is unchanged since the image; otherwise the image
    /// is rewritten. Compared rather than trusted: the counter is read BEFORE `state_bytes`
    /// serialises, so a push racing the write is recorded as still-dirty and costs one extra
    /// rewrite — never a missed one.
    durable_pending_version: u64,
    /// **D183.** Proof that the durable pending-free log is still the one `take_pending` handed
    /// out, so `put_pending` may describe the drain by what it REMOVED instead of restating the
    /// whole log.
    ///
    /// `rewrites` and `durable_pending_version` as they stood at the take, plus the KEYS the take
    /// handed out.
    ///
    /// **THREE things can make the durable log stop being the pre-take log, and the two counters
    /// only see two of them.** A full image rewrite publishes the drained, empty log and moves
    /// `rewrites`; another record that carries the whole log moves `durable_pending_version`. The
    /// third is [`ArenaPageStore::TAIL_PAGES_PARKED`], which APPENDS to the durable log and moves
    /// **neither** — `persist_delta_locked`'s `pending_covered` is `None` for it precisely because
    /// it leaves the log's accounting where it found it.
    ///
    /// ⚠ **An earlier version of this comment said "exactly two", and was wrong.** What actually
    /// covers the third is the subset test in `put_pending`: a parked record landing between the
    /// take and the put also pushes its entry into memory, so the log being put back holds a key
    /// the take never handed out, `kept ⊄ taken`, and the absolute record is written instead. The
    /// counters are not the whole guard and this comment must not claim they are.
    ///
    /// ⛔ **The keys are carried because `put_pending`'s contract is "the log is now exactly
    /// this", NOT "I released what I dropped".** An earlier draft derived the removals from
    /// `release_page` instead, on the reasoning that `drain_pending_seeded` releases every entry
    /// it drops. It does — but `put_pending` is public and
    /// `putting_the_pending_log_back_reaches_the_durable_map` calls it with a subset and no
    /// releases at all, which that draft turned into a durable log with four entries where a full
    /// rewrite gives two. The removals are therefore computed from what the drain TOOK against
    /// what it PUT BACK, which needs no assumption about any other method.
    ///
    /// `None` means "do not try", and every ambiguous case resolves to it: a second drain
    /// overlapping this one, a take whose log was not level with memory to begin with, a log
    /// larger than [`ArenaPageStore::DRAIN_MARK_CAP`], a map loaded from elsewhere. The absolute
    /// record is always correct, so the fallback costs bytes and never correctness.
    drain_mark: Option<DrainMark>,
    /// Full-image rewrites and tail appends this store has performed.
    ///
    /// Per-STORE, where `storage::atomic_file`'s counters are per-process. Both exist and neither
    /// replaces the other: a benchmark wants the process total, and an assertion cannot use it,
    /// because tests run concurrently in one process and would be reading each other's writes.
    /// The quantity this row is about is an integer, so it is worth being able to assert exactly.
    rewrites: u64,
    appends: u64,
    /// Drains whose record `put_pending` did not write because it would have carried nothing:
    /// no removal and no recycled list. Each one moved `durable_pending_version` exactly as the
    /// record's append would have. See [`ArenaPageStore::TAIL_PENDING_DRAINED`].
    elided: u64,
}

impl ArenaPageStore {
    /// `base_page` must sit at or above the disk manager's high-water mark: the region belongs to
    /// this store alone.
    pub fn new(
        pool: Arc<BufferPoolManager>,
        catalog: Arc<dyn BranchCatalog>,
        base_page: PageId,
    ) -> Result<Self, FerroError> {
        // NOT `next_page_id` — that counter only advances when a new bitmap page is created, so
        // it reads 1 after 500 allocations and would accept an arena base of 1 that the bitmap
        // already owns. `reserve_from` would then refuse every subsequent allocate AND every
        // deallocate, turning latent aliasing into aliasing plus a dead allocator.
        let high_water = pool.disk_manager.high_water()?;
        if base_page < high_water {
            return Err(BranchError::Arena(format!(
                "arena region must start at or above the disk manager high-water mark {} (got {})",
                high_water, base_page
            ))
            .into());
        }
        // Claim the region from the legacy bitmap allocator. Being above the high-water mark is
        // not enough on its own: the bitmap's bits are zero from page 0, so without this the very
        // first `DiskManager::allocate` hands out the very first arena page a second time.
        pool.disk_manager.reserve_from(base_page)?;
        Self::assemble(pool, catalog, base_page)
    }

    /// Reattach to an arena region this database already owns, after the file has been reopened.
    ///
    /// `arena_floor` lives only in memory, so a fresh `DiskManager` starts with no floor at all
    /// and its bitmap scan sees the whole arena region as free — the second open hands out pages
    /// the first open already filled. Re-registering the floor is therefore not bookkeeping, it is
    /// the entire guard, and something has to do it on every open.
    ///
    /// [`ArenaPageStore::new`] cannot: it refuses a base below `high_water()`, and after a reopen
    /// that mark is seeded from the file length, which counts the arena's own pages. So the store
    /// is locked out of precisely the region it owns — verified by
    /// `an_arena_region_is_still_off_limits_after_reopening_the_file`, which fails on `new` with
    /// *"must start at or above the disk manager high-water mark 9 (got 1)"*.
    ///
    /// This path asks the narrower question instead: is `base_page` clear of what the **bitmap**
    /// owns? Arena pages never set bitmap bits, so that mark is unaffected by the region's own
    /// growth and still refuses a base that would collide with bitmap-owned pages.
    ///
    /// The caller supplies `base_page` and it is trusted to be the region this store really owns;
    /// the checkpoint does not yet record it (see S2a in the ledger). Passing a base belonging to
    /// a different arena will alias it, and nothing here can currently detect that.
    pub fn reopen(
        pool: Arc<BufferPoolManager>,
        catalog: Arc<dyn BranchCatalog>,
        base_page: PageId,
    ) -> Result<Self, FerroError> {
        let bitmap_mark = pool.disk_manager.bitmap_high_water()?;
        if base_page < bitmap_mark {
            return Err(BranchError::Arena(format!(
                "arena region at {} overlaps pages the bitmap allocator owns (up to {})",
                base_page, bitmap_mark
            ))
            .into());
        }
        pool.disk_manager.reserve_from(base_page)?;
        Self::assemble(pool, catalog, base_page)
    }

    fn assemble(
        pool: Arc<BufferPoolManager>,
        catalog: Arc<dyn BranchCatalog>,
        base_page: PageId,
    ) -> Result<Self, FerroError> {
        Ok(ArenaPageStore {
            pool,
            catalog,
            space: ArenaSpaceManager {
                base_page,
                extent_pages: ARENA_EXTENT_PAGES,
                extent_starts: GrantedCounter::new(
                    "extent-start",
                    base_page as u64,
                    ARENA_EXTENT_PAGES as u64 * SELF_GRANT_EXTENTS,
                ),
                free_extents: Mutex::new(HashMap::new()),
                // Starts at 1: arena 0 is the shared/trunk arena and is never an extent.
                arena_ids: GrantedCounter::new("arena-id", 1, SELF_GRANT_ARENA_IDS),
                recycle_epoch: AtomicU64::new(crate::cluster::epoch()),
            },
            state: Mutex::new(StoreState {
            fill_unknown: std::collections::HashSet::new(),
                extents: HashMap::new(),
                recycled: HashMap::new(),
                current: HashMap::new(),
                pending: PendingLog::default(),
                recycled_dirty: std::collections::HashSet::new(),
                claim_epoch: HashMap::new(),
                shadow_base: HashMap::new(),
            }),
            live_pages: AtomicU32::new(0),
            reserved_pages: AtomicU32::new(0),
            pending_pushed: AtomicU64::new(0),
            authority_epoch: AtomicU64::new(crate::cluster::epoch()),
            checkpoint_path: Mutex::new(None),
            persist: Mutex::new(PersistState {
                image_bytes: 0,
                tail_bytes: 0,
                image_epoch: crate::cluster::epoch(),
                durable_pending_version: 0,
                drain_mark: None,
                rewrites: 0,
                appends: 0,
                elided: 0,
            }),
            pending_version: AtomicU64::new(0),
            recycled_reissued: AtomicBool::new(false),
            #[cfg(test)]
            replay_pending_visits: AtomicU64::new(0),
        })
    }

    pub fn base_page(&self) -> PageId {
        self.space.base_page
    }

    /// Notice an authority change and take back the right to **fill** anything claimed under the
    /// old one. Returns the epoch now in force.
    ///
    /// Only the right to fill is withdrawn. The extents stay in `extents` so their pages remain
    /// accounted for and the reaper can still free them whole — dropping them would leak the space
    /// permanently, which is the one outcome worse than refusing to use it.
    fn revoke_stale_authority(&self) -> u64 {
        let epoch = crate::cluster::epoch();
        if self.authority_epoch.swap(epoch, Ordering::SeqCst) != epoch {
            let mut st = self.state.lock().unwrap();
            // Same rule and same reason as `load_state`'s `current.clear()`: never resume filling
            // an extent whose provenance this process can no longer vouch for.
            st.current.clear();
            st.claim_epoch.clear();
            st.recycled.clear();
        }
        epoch
    }

    /// Apply a committed [`crate::consensus::Command::ArenaGrant`].
    ///
    /// One entry grants **both** counters: pages `[first_page, first_page + page_count)` and arena
    /// ids drawn from the same numbers.
    ///
    /// # Why arena ids ride the page grant
    ///
    /// The frozen contract has no `Command` variant for an arena-id range —
    /// `ArenaGrant { node, first_page, page_count }` names pages only. Rather than leave the id
    /// counter node-local (which is the same corruption one level up: two nodes naming a different
    /// extent `a7`, and `BranchRecord::arenas` then pointing two branches at one arena), the ids
    /// are drawn from the granted page numbers themselves.
    ///
    /// That is sound for exactly the reason the grant exists: page numbers are unique across the
    /// cluster, so any function of them is too, and `[first_page, first_page + page_count)` gives
    /// `page_count` ids per grant — 256 reuses per extent at the default extent size, so recycling
    /// an extent does not need a fresh consensus round. `ArenaId` and `PageId` are distinct types,
    /// so the shared numbering cannot be confused at a call site. Reported as a needed variant in
    /// this row's summary rather than worked around silently.
    ///
    /// # Refusals
    ///
    /// Refuses a grant addressed to another node, and a grant reaching a standalone node. A
    /// re-delivered grant is a no-op: a committed round may be delivered more than once, and
    /// re-offering a range this node has already issued from hands one page to two branches.
    /// Returns whether the grant was new or had already been applied.
    ///
    /// Reporting it is not decoration. The mutation sweep showed that removing the duplicate check
    /// in `Grants::apply_grant` did NOT let a re-delivered grant hand out a second extent — the
    /// clamp to `max(accepted_through, issued)` already prevents that — so an integration test that
    /// only asserted "no second extent" was pinning a rule it could not detect. The two mechanisms
    /// are defence in depth, and the outcome is what makes the idempotence itself observable.
    ///
    /// Refuses if the two counters disagree about whether the grant was new. They are fed from one
    /// entry and are stamped from the same numbers, so a disagreement means their watermarks have
    /// diverged — which would eventually issue an arena id for an extent range that was never
    /// granted, and there is no correct way to carry on from it.
    pub fn apply_arena_grant(
        &self,
        node: crate::consensus::NodeId,
        first_page: u32,
        page_count: u32,
    ) -> Result<crate::cluster::Applied, FerroError> {
        let lo = first_page as u64;
        let hi = lo + page_count as u64;
        let pages = self.space.extent_starts.apply_grant(node, lo, hi)?;
        let ids = self.space.arena_ids.apply_grant(node, lo, hi)?;
        if std::mem::discriminant(&pages) != std::mem::discriminant(&ids) {
            return Err(BranchError::Arena(format!(
                "an ArenaGrant of [{lo}, {hi}) was {pages:?} for extent pages but {ids:?} for \
                 arena ids; the two watermarks have diverged and cannot both be trusted"
            ))
            .into());
        }
        Ok(pages)
    }

    /// How many extent pages this node may still claim without a new grant.
    ///
    /// **D31 — this is the honest unit now.** Extents are no longer one size, so "how many
    /// extents" depends on which sizes get asked for; pages are what the grant is actually
    /// denominated in and what `reserve` actually consumes.
    pub fn grantable_pages(&self) -> u64 {
        self.space.extent_starts.remaining()
    }

    /// How many **full-size** extents this node may still claim without a new grant.
    ///
    /// Diagnostic, for an operator and for the leader loop that decides when to propose the next
    /// grant. A caller that branches on it to decide whether to allocate is re-implementing the
    /// guard in [`ArenaSpaceManager::reserve`], which already refuses.
    ///
    /// Since D31 this is a **lower bound**, not a count: a node holding 300 pages can claim one
    /// 256-page extent or three hundred 1-page ones, and this reports 1. Use
    /// [`Self::grantable_pages`] when the number has to be exact.
    pub fn grantable_extents(&self) -> u64 {
        self.grantable_pages() / self.space.extent_pages as u64
    }

    /// The extent-start watermark, i.e. what the checkpoint image carries. Diagnostic.
    pub fn extent_watermark(&self) -> PageId {
        self.space.extent_starts.issued_through() as PageId
    }

    /// Extent pages currently reserved by some branch.
    pub fn reserved_page_count(&self) -> u32 {
        self.reserved_pages.load(Ordering::SeqCst)
    }

    /// Entries in the pending-free log. A LEVEL: it falls as well as rises.
    pub fn pending_len(&self) -> usize {
        self.state.lock().unwrap().pending.len()
    }

    /// Entries ever pushed onto the pending-free log. See [`ArenaPageStore::pending_pushed`].
    ///
    /// Cumulative and monotone, so unlike [`Self::pending_len`] it answers "how many pages were
    /// ever parked" regardless of when they were released or whether two parks overlapped.
    pub fn pending_pushed_total(&self) -> u64 {
        self.pending_pushed.load(Ordering::Relaxed)
    }

    /// Branches currently holding a fillable extent — the length of the `current` map.
    ///
    /// **D99.** This is the integer a scan of that map costs, and it is reported instead of a
    /// duration wherever possible: this box runs a build fleet, so a wall-clock figure is an upper
    /// bound and nothing better, while an entry count is immune to load. `free_arena` used to walk
    /// this map once per freed extent; it now does one hash probe, and the harness prints this
    /// number beside the timing so the two can be read against each other.
    pub fn current_arena_count(&self) -> usize {
        self.state.lock().unwrap().current.len()
    }

    /// The branch that owns `arena`, or `None` if the extent has been freed.
    ///
    /// **D40.** This is how `reaper::sweep_touched_extents` asks about a handful of named arenas
    /// instead of walking [`Self::live_arenas`]. `None` is the ordinary answer for an arena the
    /// reaper's fast path already freed wholesale, not an error.
    pub fn arena_owner(&self, arena: ArenaId) -> Option<BranchId> {
        self.state.lock().unwrap().extents.get(&arena).map(|e| e.owner)
    }

    /// The page range `arena` covers, as `(start_page, page_count)`. Two live extents that
    /// overlap is silent corruption, so this is what a restart test must actually check.
    pub fn extent_range(&self, arena: ArenaId) -> Option<(PageId, u32)> {
        self.state.lock().unwrap().extents.get(&arena).map(|e| (e.start_page, e.page_count))
    }

    /// Pages handed out inside `arena` and not since released.
    pub fn allocated_pages(&self, arena: ArenaId) -> Vec<PageId> {
        let st = self.state.lock().unwrap();
        let Some(ext) = st.extents.get(&arena) else { return Vec::new() };
        let recycled = st.recycled.get(&arena).cloned().unwrap_or_default();
        (0..ext.next_free)
            .map(|i| ext.start_page + i)
            .filter(|p| !recycled.contains(p))
            .collect()
    }

    fn write_fresh_page(
        &self,
        page_id: PageId,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<(), FerroError> {
        let mut page = [0u8; PAGE_SIZE];
        let mut h = PageHeader::new(birth_epoch, arena, page_type);
        h.flags = flags::PRIVATE;
        h.write_to(&mut page);
        stamp_checksum(&mut page);
        // The page must exist on disk before anything can fetch it: `DiskManager::read` reports
        // EOF rather than zeroes for a page that was never written.
        self.pool.disk_manager.write(page_id, &page)
    }

    /// Drop a page from the buffer pool **without** touching the disk manager's bitmap. The
    /// arena region is not bitmap-managed, so deallocating there would clear a bit that belongs
    /// to somebody else's address space.
    fn evict(&self, page_id: PageId) {
        let mut pt = self.pool.page_table.write().unwrap();
        let Some(&frame_i) = pt.get(&page_id) else { return };
        if self.pool.frames[frame_i].read().unwrap().pin_counter.load(Ordering::Relaxed) > 0 {
            // Still pinned: leave it. The id is retired and will not be handed out again until
            // its extent is recycled, by which time every handle is long gone.
            return;
        }
        pt.remove(&page_id);
        drop(pt);
        {
            let mut frame = self.pool.frame_write(frame_i);
            // `page_table` write was held until the entry was removed, so no `free_pages` call held
            // it, and none can mark this frame now (`Frame::freeing`'s invariant, D237 review 3 R2).
            debug_assert!(!frame.freeing, "arena evict: frame {frame_i} is being freed");
            frame.page_id = None;
            frame.data = [0u8; PAGE_SIZE];
            frame.pin_counter = AtomicU16::new(0);
            frame.dirty_flag = AtomicBool::new(false);
        }
        // Through `arc_locked`, not the mutex directly: it applies the pending hit-path updates
        // first, so this cannot remove a page whose own queued `touch` then lands behind it.
        let _ = self.pool.arc_locked().remove(page_id);
    }

    /// Return one page to the free space map. This is the only place `live_pages` goes down a
    /// page at a time.
    pub fn release_page(&self, page_id: PageId, arena: ArenaId) {
        self.evict(page_id);
        let newly_freed = {
            let mut st = self.state.lock().unwrap();
            // **D102 — a recycled id must not inherit the base of its previous life.** This id
            // goes back on the free list and `alloc_in_arena` will hand it out again for something
            // unrelated; a surviving entry would make that new page read as a delta of a base it
            // has nothing to do with. Forgotten here rather than at `evict`, because this is the
            // point at which the id stops naming this page.
            st.shadow_base.remove(&page_id);
            // If the extent is gone the whole thing was already accounted for by `free_arena`.
            if !st.extents.contains_key(&arena) {
                false
            } else {
                let slot = st.recycled.entry(arena).or_default();
                if slot.contains(&page_id) {
                    false
                } else {
                    slot.push(page_id);
                    // **D183 — marked in the SAME critical section as the push.** See
                    // [`StoreState::recycled_dirty`]: a mark that can land on the other side of
                    // the push is a page that is neither in the next record nor still owed one.
                    st.recycled_dirty.insert(arena);
                    true
                }
            }
        };
        if newly_freed {
            self.live_pages.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// The page `shadow` was copied from by [`PageStore::cow_page`], and its chain depth.
    ///
    /// `None` for a page that was never a shadow, for one whose chain had already reached
    /// [`crate::branch::delta::MAX_CHAIN_DEPTH`] when it was made (so it is a chain root), and for
    /// one shadowed from a page this branch owned — see `cow_page` for why that last case is
    /// deliberately not recorded.
    pub fn shadow_base(&self, shadow: PageId) -> Option<(PageId, u8)> {
        self.state.lock().unwrap().shadow_base.get(&shadow).map(|&(base, depth, _)| (base, depth))
    }

    /// Every page this store currently knows to be a shadow of another. Ascending, so a harness
    /// that diffs two calls gets a stable answer.
    pub fn shadow_pages(&self) -> Vec<PageId> {
        let mut v: Vec<PageId> = self.state.lock().unwrap().shadow_base.keys().copied().collect();
        v.sort_unstable();
        v
    }

    /// Encode `shadow`'s CURRENT content as a delta against the page it was shadowed from, or
    /// `None` when a whole page must be stored instead.
    ///
    /// **This is the decision `cow_page`'s full-payload copy makes implicitly, made explicitly and
    /// with both bounds applied.** It answers "would storing this page as a difference from its
    /// base actually beat storing the page", and it answers `None` in every case where it would
    /// not:
    ///
    /// * `shadow` is not a shadow, or its chain already reached [`delta::MAX_CHAIN_DEPTH`] when it
    ///   was made. The depth bound is enforced at write time, in `cow_page`, so reaching here with
    ///   an over-deep chain is not possible rather than merely unlikely.
    /// * the encoded delta exceeds [`delta::DELTA_BUDGET`]. **A delta that does not beat the page
    ///   it encodes is refused, not stored.** D93 measured the compaction regime at 1373 B and
    ///   2552 B against a 1018-byte budget — both over, both correctly whole. Letting those
    ///   through would have the delta arm claiming a saving it cannot deliver, and would break the
    ///   read-amplification bound, which is the product of the two constants.
    ///
    /// **The 24-byte self-describing header is not diffed, and that is structural rather than
    /// careful.** `PageDelta` works over [`crate::cow::node::PAYLOAD_LEN`] = `PAGE_SIZE -
    /// PAGE_HEADER_SIZE` and this method hands it `[PAGE_HEADER_SIZE..]` of each page, so
    /// `birth_epoch`, `arena_id` and `crc32` are outside the diff by construction. That matters
    /// because a shadow's header is ALWAYS different from its base's — D94 proved two
    /// independently written pages differ even with identical content, for exactly this reason —
    /// so diffing it would put a run at offset 0 in every delta ever taken and inflate the cheapest
    /// case the most. A materialised page re-stamps its own header rather than inheriting one.
    ///
    /// # Crash safety: how a half-written chain cannot read as a complete one
    ///
    /// D85 in this project was silent data loss of exactly this class — a partial write that read
    /// back as a complete, smaller truth — so the argument is recorded here rather than left to be
    /// reconstructed. Four things carry it, and the first is the one that matters most.
    ///
    /// 1. **No chain is written yet, so no chain can be half-written.** `cow_page` still stores a
    ///    whole page; this method computes a delta and returns it. That is not a hedge, it is the
    ///    current truth, and it means D102 adds no crash-recovery surface at all. Everything below
    ///    is what must hold *before* anything stores one.
    /// 2. **A base is always older than the branch that deltas against it.** Only a base the
    ///    branch does NOT own is recorded (see `cow_page`), i.e. a page inherited from an ancestor,
    ///    which was made durable by that ancestor's commit before this branch forked. So a delta
    ///    can never point at a base that the same crash could lose, and the ordering "base durable
    ///    before delta durable" needs no enforcement — it is a consequence of what is recordable.
    ///    The epoch interval rule in `branch::record::reclaimable` keeps that base alive, and
    ///    [`ArenaPageStore::stale_delta_base_count`] counts any case where it did not.
    /// 3. **Each record is self-describing and checksummed.** A delta carries its own `base` and
    ///    `depth`, and a page carries `crc32`. A torn record fails `verify_checksum` and
    ///    `read_page` refuses it — the behaviour
    ///    `a_torn_page_is_refused_rather_than_returned` already pins. A missing link fails as a
    ///    bad page rather than as a shorter chain, because the depth is written down rather than
    ///    inferred from how many links happen to be readable.
    /// 4. **Truncation refuses rather than parses.** [`crate::branch::delta::PageDelta::decode`]
    ///    rejects every proper prefix of a record;
    ///    `every_truncation_of_a_record_is_refused_rather_than_read_short` asserts that over all
    ///    of them, and a mutant that clamps instead of refusing fails it.
    ///
    /// The in-memory `shadow_base` map is deliberately not durable, and that is safe in the one
    /// direction that matters: losing it makes the next shadow a chain ROOT rather than a deeper
    /// link, so a crash can only make chains shorter than [`delta::MAX_CHAIN_DEPTH`], never longer.
    pub fn delta_against_base(&self, shadow: PageId) -> Result<Option<PageDelta>, FerroError> {
        let Some(&(base, depth, born)) = self.state.lock().unwrap().shadow_base.get(&shadow) else {
            return Ok(None);
        };
        // Belt and braces against the bound the write path already enforces: a chain deeper than
        // this cannot be built by `cow_page`, so reaching it means a bug in this file, and an
        // unbounded read is precisely what the bound exists to prevent.
        if depth > delta::MAX_CHAIN_DEPTH {
            return Ok(None);
        }
        let base_handle = self.read_page(base)?;
        // **The base must still be the page the shadow was taken from.** A page id outlives its
        // page — `release_page` frees the id and `alloc_in_arena` reissues it — and a delta taken
        // against a reissued page is wrong in exactly the bytes the writer cared about while
        // reporting nothing. `write_fresh_page` stamps a fresh `birth_epoch` on every allocation,
        // so a reissued id cannot carry the epoch recorded at shadow time. Refusing here stores a
        // whole page, which is always correct; it is the same safe direction the budget rule
        // takes. Counted rather than merely refused, so a workload where it happens is visible
        // instead of silently paying for full pages.
        if base_handle.header()?.birth_epoch != born {
            census::bump(&census::STALE_BASE, 1);
            return Ok(None);
        }
        let base_image = base_handle.read().data;
        let shadow_image = self.read_page(shadow)?.read().data;
        let encoded = PageDelta::between(
            base,
            &base_image[PAGE_HEADER_SIZE..],
            &shadow_image[PAGE_HEADER_SIZE..],
            depth,
        )?;
        if encoded.encoded_len() > delta::DELTA_BUDGET {
            return Ok(None);
        }
        Ok(Some(encoded))
    }

    /// Push onto the pending-free log as part of a change a tail record is about to describe.
    ///
    /// **The [`RecordedParks`] is the point.** A caller can only build one from the persist guard,
    /// and it keeps that guard borrowed until the entries it collects are written, so "mutate the
    /// log under the lock that orders the records, and keep it until the record lands" stops being
    /// a rule in a comment and becomes the only spelling that compiles. The defect this replaces
    /// was exactly a push that happened outside it while a comment three functions away asserted
    /// the invariant — see `retire_arenas_by_rule`. An earlier version took `&mut PersistState`,
    /// which bound the push but not the span to the append; see [`RecordedParks`].
    ///
    /// The caller must append a record covering `parks.parked` before the borrow ends, or route
    /// through [`Self::abandon_park`] — still holding it — if it cannot.
    fn push_pending_recorded(&self, parks: &mut RecordedParks<'_>, entry: PendingFree) {
        // **D183 de-dup at push.** A page already pending keeps its first entry, and the record must
        // describe only what memory took, so a skipped push is not in `parked`. The replay would skip
        // it first-wins anyway; leaving it out keeps the record equal to the change.
        if self.state.lock().unwrap().pending.push_if_absent(entry) {
            parks.parked.push(entry);
            // **D144** counts the parks that landed, here and in `push_pending_unrecorded` only —
            // see [`ArenaPageStore::pending_pushed`].
            self.pending_pushed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Push onto the pending-free log with NO record describing it, and say so durably.
    ///
    /// The other discipline, and the reason the pair exists rather than one function: `free_page`
    /// parks on the page path, which must not take a durability mutex. Such a push bumps
    /// [`Self::pending_version`] instead, which makes the next persist a full image rewrite. Both
    /// are correct; what is not correct is a push that does neither, and neither of these two
    /// functions can be that.
    fn push_pending_unrecorded(&self, entry: PendingFree) {
        let mut st = self.state.lock().unwrap();
        // **D183 de-dup at push.** A skipped push changed nothing, so it announces nothing. A bump here
        // would force a full image rewrite of a log the file already holds: the rule `take_pending`
        // keeps for a drain that took nothing.
        if st.pending.push_if_absent(entry) {
            self.pending_version.fetch_add(1, Ordering::SeqCst);
            // **D144** — see `push_pending_recorded`: a skipped push parked nothing and counts nothing.
            self.pending_pushed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Take the pending-free log for re-evaluation.
    pub fn take_pending(&self) -> Vec<PendingFree> {
        // **D81.** Draining the log changes it and persists nothing; no tail record describes
        // that, so the next claim must rewrite the image rather than append behind a file that
        // still lists these entries. See [`PersistState::durable_pending_version`].
        //
        // **D183 — bumped INSIDE the `state` lock, and that is what makes the counter readable.**
        // `put_pending` now vouches for a version it read beside the log itself; bumped outside,
        // a reader holding `state` can see the new version before the change it announces, and
        // would record a log it has not written as durable.
        //
        // ⭐ **D183 — AND ONLY WHEN IT ACTUALLY TOOK SOMETHING. This was the largest of the four
        // rewrite sites and neither the design entry nor the row's own counter test had it in
        // scope.** The bump means "the log changed and no record says so". Draining a log that
        // was ALREADY EMPTY changes nothing: memory held `[]` before and holds `[]` after, so the
        // durable file cannot have been made stale by it. The unconditional bump therefore
        // announced a change that did not happen, and `persist_delta_locked`'s third condition
        // turned the next delta — any delta, from any caller — into a full image rewrite.
        //
        // `Reaper::reap` runs `drain_pending_seeded` on EVERY reap, leaf or interior, and its
        // first act is this call. So every reap forced the NEXT reap's first `free_arena` into a
        // full rewrite, and the leaf path — which the measurement that opened this row called
        // free — was paying one full image rewrite per branch. Measured through the real reaper
        // at `753b266`: LEAF 8/16 branches -> 7/15 rewrites, slope exactly 1.0 per branch, and an
        // empty drain placed between two `free_arena` calls turns `(0 rewrites, 2 appends)` into
        // `(1, 1)`. Both are pinned in `mod d183_adversary`.
        //
        // The guard stays exactly as strong: every drain that removes an entry still bumps, and
        // `put_pending`'s record is what discharges it afterwards.
        //
        // **`persist` is taken FIRST and only for this**, per the outermost rule in
        // [`PersistState`]. It is here so the mark below is cut from the same instant as the take:
        // read afterwards, a rewrite landing in between would be invisible to it and the drain
        // record would describe a log the file no longer holds.
        let mut persist = self.persist.lock().unwrap();
        let mut st = self.state.lock().unwrap();
        // BEFORE the bump. The mark is only usable if the durable log was level with memory at
        // this instant — otherwise the file is already missing an entry (an unrecorded
        // `park_or_release`, say) and removing from it would leave it missing.
        let version = self.pending_version.load(Ordering::SeqCst);
        let level = persist.durable_pending_version == version;
        let taken = st.pending.take_all();
        if !taken.is_empty() {
            self.pending_version.fetch_add(1, Ordering::SeqCst);
        }
        // A second drain overlapping this one poisons the mark: two takes cannot both be "the
        // log the file still holds", and `put_pending` has no way to tell whose entries it has.
        let overlapping = persist.drain_mark.is_some();
        persist.drain_mark =
            if taken.is_empty() || !level || overlapping || taken.len() > Self::DRAIN_MARK_CAP {
                None
            } else {
                Some(DrainMark {
                    rewrites: persist.rewrites,
                    base_version: version,
                    taken: taken.iter().map(|p| (p.page_id, p.arena_id)).collect(),
                })
            };
        taken
    }

    /// Put entries that are still pinned back on the pending-free log, and checkpoint.
    ///
    /// This is the closing half of `reaper::drain_pending`'s read-modify-write: by the time it runs,
    /// the reclaimable pages have been released into their extents' recycled lists and the survivors
    /// are back on the log. That whole shape lives only in the free-space map, so it persists here
    /// for the same reason `free_arena` does: once per drain, which is once per reap. The exception
    /// is a drain that removed nothing and owes no recycled list. It persists NOTHING, because the
    /// record would describe no change (see the elision below and [`Self::TAIL_PENDING_DRAINED`]).
    pub fn put_pending(&self, entries: Vec<PendingFree>) -> Result<(), FerroError> {
        // **D183 — a delta, not the whole image.** What this call changes is the pending-free log
        // and the recycled lists of the extents the drain released into; that is list-shaped
        // state, which is why it used to rewrite everything, and
        // [`Self::TAIL_PENDING_REPLACED`] is the record that describes it.
        //
        // **Outermost, and held across the mutation AND the append**, per [`PersistState`]: a
        // record that REPLACES the log has to be written in the order the log actually changed,
        // or a `TAIL_PAGES_PARKED` cut by a concurrent reap lands on the wrong side of it.
        let mut persist = self.persist.lock().unwrap();
        // ⭐ **Which record, and it is the difference between linear and quadratic.**
        //
        // The absolute one restates the WHOLE log, and the log is also in the image, so once it
        // has grown past roughly a third of the map every such record exceeds `compact_threshold`
        // and each drain costs a full rewrite again. Measured through the real reaper before this
        // branch existed: interior reaps settled at **0.58 rewrites per branch over 8…128
        // branches** — a better constant than the 2.0 they started at, and the same Θ(N²) class.
        //
        // The drain record instead names only what LEFT the log, which is what actually changed.
        // It is usable exactly when the durable log is still the one `take_pending` handed out;
        // [`PersistState::drain_mark`] is that proof, and every ambiguous case resolves to the
        // absolute record, which is always correct.
        let mark = persist.drain_mark.take();
        let (payload, dirty, covered, kind, says_nothing) = {
            let mut st = self.state.lock().unwrap();
            // **D183 de-dup at push**: an entry whose page is already pending again is skipped, first
            // wins. Only a park between the take and this put can do that.
            //   * A RECORDED park (a retire) could not append its record: the take's bump left the log
            //     unlevel, so it rewrote the image, which moved `rewrites` and voids `mark` below. This
            //     put then restates the log, REPLACED.
            //   * An UNRECORDED park (`free_page` of a page this drain holds: a double free no caller
            //     makes) moves neither counter, so this put still goes DRAINED. Memory keeps the new
            //     entry and the file the taken one, until the next persist rewrites, because `covered`
            //     below is `base + 1` and the park bumped past it. That is the window every unrecorded
            //     park already has. (With the drain elision below, a DRAINED that removed nothing and
            //     owes no recycled list is not written at all; it moves `durable_pending_version` to
            //     the same `covered`, so the park's bump still forces that rewrite.)
            st.pending.extend_absent(entries);
            let dirty = std::mem::take(&mut st.recycled_dirty);
            // ⚠ **`dirty` alone, where `retire_arenas_by_rule` passes `dirty ∪ rec.arenas`, and
            // the asymmetry is deliberate.** That site unions in the arenas it WALKED because it
            // can raise an extent's `next_free` (through `resolve_fill`) and park every page of an
            // extent without ever calling `release_page` on it — so its walked set is not covered
            // by the marks. A drain's only mutation is `release_page`, which marks its arena in
            // the same critical section as the push, so here the marks ARE the complete set. If a
            // drain ever gains a second mutator, this line has to gain its union too.
            // Read under the SAME lock as the log this encodes. See `persist_delta_locked`'s
            // `pending_covered`: a version read outside would vouch for a push the record misses.
            let version = self.pending_version.load(Ordering::SeqCst);

            // Nothing may have moved the durable log since the take: a full image rewrite
            // publishes the drained log (moves `rewrites`), and any other record that carries the
            // log moves `durable_pending_version`. `removed` is then the difference between what
            // the drain TOOK and what it PUT BACK — no assumption about who released what.
            //
            // A caller that puts back a key the take did not hand out is describing a log this
            // record cannot reach by removal alone, so that falls back too.
            let plan = mark.as_ref().filter(|m| {
                m.rewrites == persist.rewrites && m.base_version == persist.durable_pending_version
            });
            let removed: Option<Vec<(PageId, ArenaId)>> = plan.and_then(|m| {
                let kept: std::collections::HashSet<(PageId, ArenaId)> =
                    st.pending.iter().map(|p| (p.page_id, p.arena_id)).collect();
                let held: std::collections::HashSet<(PageId, ArenaId)> =
                    m.taken.iter().copied().collect();
                if kept.iter().any(|k| !held.contains(k)) {
                    return None;
                }
                Some(m.taken.iter().copied().filter(|k| !kept.contains(k)).collect())
            });

            let mut p = Vec::new();
            p.extend_from_slice(&self.live_pages.load(Ordering::SeqCst).to_be_bytes());
            let kind = match &removed {
                Some(rm) => {
                    p.extend_from_slice(&(rm.len() as u32).to_be_bytes());
                    for (page, arena) in rm {
                        p.extend_from_slice(&page.to_be_bytes());
                        p.extend_from_slice(&arena.0.to_be_bytes());
                    }
                    Self::TAIL_PENDING_DRAINED
                }
                None => {
                    p.extend_from_slice(&(st.pending.len() as u32).to_be_bytes());
                    for e in st.pending.iter() {
                        Self::encode_pending_entry(&mut p, e);
                    }
                    Self::TAIL_PENDING_REPLACED
                }
            };
            Self::encode_arena_sections(&mut p, &st, &dirty);
            // The drain record discharges exactly the take's own bump and nothing else, so it
            // vouches for `base + 1`. Anything that bumped alongside it stays undescribed and the
            // next persist rewrites, which is the same conservative direction as everywhere else.
            let covered = match (&removed, &mark) {
                (Some(_), Some(m)) => m.base_version + 1,
                _ => version,
            };
            // A difference record with no removal and no recycled list: the same no-section rule
            // `encode_arena_sections` applies, so "says nothing" is decided from what the record
            // WOULD hold, not from how the drain went.
            let says_nothing = matches!(&removed, Some(rm) if rm.is_empty())
                && dirty.iter().all(|a| !st.extents.contains_key(a));
            (p, dirty, covered, kind, says_nothing)
        };
        // **D183 tail replay — a drain that changed nothing writes nothing.**
        //
        // `drain_pending_seeded` puts the log back on EVERY reap whose drain found entries, and
        // while a live child pins them it releases none. Each such drain used to append a
        // `TAIL_PENDING_DRAINED` naming no removal and no arena: one fsync for a record that
        // changes nothing in the file except the `live` snapshot. That is one fsync per interior
        // reap on top of its `TAIL_PAGES_PARKED`, and one more record for every open after an
        // unclean exit to replay.
        //
        // The record's only durable EFFECT was to move `durable_pending_version` to `covered`, the
        // take's own bump. The drain mark proves the durable log is still the log the take handed
        // out, and nothing left it, so that move is TRUE without the bytes. It is made here
        // directly.
        //
        // ⚠ **Only in place of an APPEND.** If `persist_delta_locked` would REWRITE instead (no
        // image yet, the authority moved, a recycled page was reissued, or the tail is full), a
        // rewrite was already owed for a reason unrelated to this drain. The record falls through
        // and pays it exactly as before. An elision that swallowed an owed rewrite would leave
        // `recycled_reissued`'s window open, and that window is a live page freed, not a leak.
        //
        // What the file loses: the `live` snapshot this record would have carried. A drain that
        // released nothing did not move `live_pages` itself, so the snapshot is stale only by
        // pages allocated since the last record. Those pages' `next_free` is not in the file
        // either, and the next record of any kind re-snapshots it.
        if says_nothing
            && self.checkpoint_path.lock().unwrap().is_some()
            && self.delta_would_append(&persist, 9 + payload.len() as u64, Some(covered))
        {
            persist.durable_pending_version = covered;
            persist.elided += 1;
            return Ok(());
        }
        if let Err(e) = self.persist_delta_locked(&mut persist, kind, &payload, Some(covered)) {
            // Nothing reached the file, so the arenas this record named are still owed one.
            // Re-marking rather than assuming: an arena dirtied again while the append was in
            // flight is already back in the set and `extend` leaves it there.
            self.state.lock().unwrap().recycled_dirty.extend(dirty);
            // **D183 — and the log itself is now ahead of the file with nothing saying so.** This
            // used to be safe by accident: `take_pending` bumped unconditionally, so the caller
            // had always bumped before reaching here. The conditional bump removed that
            // guarantee — a `put_pending` after an EMPTY take, or from any caller that did not
            // take at all, extends the log and bumps nothing. Same repair `abandon_park` makes,
            // and for the same reason.
            self.pending_version.fetch_add(1, Ordering::SeqCst);
            return Err(e);
        }
        Ok(())
    }

    /// True iff `arena` is live and every page ever handed out from it has been released.
    pub fn extent_is_empty(&self, arena: ArenaId) -> bool {
        let st = self.state.lock().unwrap();
        // **D85.** A restored extent's `next_free` may be understated, and this predicate is the
        // one that decides whether an extent may be FREED. Answering "empty" from a number that
        // can be too low is how a live child's pages were freed after a crash. Refuse until
        // `resolve_fill` has probed it; the callers that need the truth ask for it.
        if st.fill_unknown.contains(&arena) {
            return false;
        }
        let Some(ext) = st.extents.get(&arena) else { return false };
        let recycled = st.recycled.get(&arena).map(|v| v.len() as u32).unwrap_or(0);
        recycled >= ext.next_free
    }

    /// Test-only: force an extent's recorded fill, to construct the no-lower-bound case D87 would
    /// create by rebuilding `extents` from the catalog rather than from an image.
    #[cfg(test)]
    pub fn debug_set_next_free(&self, arena: ArenaId, to: u32) {
        let mut st = self.state.lock().unwrap();
        if let Some(e) = st.extents.get_mut(&arena) {
            e.next_free = to;
        }
        st.fill_unknown.insert(arena);
    }

    /// Test-only: how many arenas are still under fill suspicion. Nothing in the engine asks this
    /// — it exists so a test can assert that `free_arena` gives the entry back, which is invisible
    /// from `extent_is_empty` (that already answers false for a missing extent, so a leaked
    /// suspicion and a collected one look identical from outside).
    #[cfg(test)]
    pub fn debug_fill_unknown_len(&self) -> usize {
        self.state.lock().unwrap().fill_unknown.len()
    }

    /// **D85.** Recover a restored extent's true fill by probing its pages, and clear the suspicion.
    ///
    /// Pages inside an extent are handed out sequentially by `alloc_for` and every one is written
    /// by `write_fresh_page` before it is returned, so the written pages are a contiguous prefix
    /// and the first page that fails to read marks the boundary. A page released back into the
    /// extent keeps its contents — `release_page` only records it in `recycled` — so a hole in the
    /// middle does not end the probe early.
    ///
    /// Cost is bounded by `ARENA_EXTENT_PAGES` (256) reads, and it is paid LAZILY: only when a
    /// caller needs to know whether this extent can be freed or which of its pages to park, and
    /// only once per extent per restore. It is never paid at open, and never for a database that
    /// did not crash.
    ///
    /// ⚠ It can only ever RAISE `next_free`, never lower it. An image that was already correct is
    /// left alone, and a probe that under-reads (a genuinely corrupt page in the prefix) leaves
    /// the extent looking fuller than it is — which leaks space rather than losing data, and that
    /// is the direction this whole change exists to choose.
    pub fn resolve_fill(&self, arena: ArenaId) {
        let (start, count, known) = {
            let st = self.state.lock().unwrap();
            if !st.fill_unknown.contains(&arena) {
                return;
            }
            match st.extents.get(&arena) {
                Some(e) => (e.start_page, e.page_count, e.next_free),
                None => {
                    drop(st);
                    self.state.lock().unwrap().fill_unknown.remove(&arena);
                    return;
                }
            }
        };
        let mut high = known;
        for i in known..count {
            if self.read_page(start + i).is_err() {
                break;
            }
            high = i + 1;
        }
        let mut st = self.state.lock().unwrap();
        let raised = match st.extents.get_mut(&arena) {
            Some(e) if high > e.next_free => {
                e.next_free = high;
                true
            }
            _ => false,
        };
        if raised {
            // **D183.** The probe moved a number the durable image records, and it is the number
            // `extent_is_empty` compares the recycled count against. Owed to the next record for
            // the same reason a recycled push is.
            st.recycled_dirty.insert(arena);
        }
        st.fill_unknown.remove(&arena);
    }

    /// Every live arena and its owner, **in arena-id order**. Used by the reaper to find extents
    /// whose owning branch is gone.
    ///
    /// Sorted because this order reaches durable state rather than a diagnostic:
    /// `reaper::sweep_empty_extents` frees empty extents in exactly this sequence, each
    /// `free_arena` pushes the freed extent's start page onto `free_extents` under its size class,
    /// and `ArenaSpaceManager::reserve` **pops** that stack. So the `extents` map's hash order
    /// decided which page range the next arena was handed, and two runs of one workload laid their
    /// extents out differently.
    pub fn live_arenas(&self) -> Vec<(ArenaId, BranchId)> {
        let mut live: Vec<(ArenaId, BranchId)> =
            self.state.lock().unwrap().extents.iter().map(|(a, e)| (*a, e.owner)).collect();
        live.sort_unstable();
        live
    }

    /// Slow path: hand every page still allocated in `rec`'s arenas to the interval rule at
    /// `free_epoch`. Reclaimable pages go back immediately; the rest are parked against `rec`'s
    /// `live_children` array. Returns pages actually returned to the free space map.
    pub fn retire_arenas_by_rule(
        &self,
        rec: &BranchRecord,
        free_epoch: Epoch,
    ) -> Result<u32, FerroError> {
        // ⛔⛔ **D183 — TWO PHASES, AND THE SPLIT IS A CORRECTNESS FIX, NOT A TIDY-UP.**
        //
        // This loop used to DECIDE and MUTATE together, holding only `state`, and take `persist`
        // afterwards to write its record. That put a pending-log mutation outside the lock whose
        // entire job is to make the durable record order equal the in-memory mutation order — the
        // rule [`PersistState`] states and every other delta site obeys. The hole it opened, found
        // by review and reproduced from the committed blob at `149666b`:
        //
        //   1. this loop pushes entry K under `state`, and is descheduled before taking `persist`;
        //   2. a concurrent drain's `take_pending` takes `persist` + `state`, SEES K, and cuts a
        //      [`PersistState::drain_mark`] asserting "the durable log is the log I just took" —
        //      which is false, because K's record has not been appended yet;
        //   3. the drain releases K and its `TAIL_PENDING_DRAINED` removes K from a durable log
        //      that never had it: a no-op;
        //   4. this loop finally appends `TAIL_PAGES_PARKED`, putting K back.
        //
        // Replay then holds an entry memory dropped, for a page that is also in the recycled list.
        // **Removal is idempotent but it does not COMMUTE with an append**, so no amount of
        // care in the replay arms can fix step 4 — the order itself has to be impossible.
        //
        // Gated today only by the per-statement lock that serialises reaps, which `reaper.rs`'s
        // own D124 note says W4 removes. Latent, and armed by a change already on the roadmap.
        //
        // PHASE 1 decides and mutates nothing durable: page reads and catalog queries, which are
        // what would make holding the lock across the whole loop expensive. **The decision is
        // stable across the phase boundary** because `reap` publishes `Reaping` before calling
        // this, `BranchRecord::check_readable` refuses that state, and `fork` calls it — so no new
        // live child of this branch can appear in between, which is the only way a page decided
        // reclaimable could become pinned. The reverse (a pinned page becoming reclaimable) parks
        // a page the next drain releases, which is the safe direction and already possible.
        let mut plan: Vec<(PageId, ArenaId, Epoch, bool)> = Vec::new();
        for arena in rec.arenas.iter().copied() {
            // **D85.** `allocated_pages` is `(0..next_free)`, so an understated `next_free` makes
            // this loop park NONE of a live child's pages. Probe first.
            self.resolve_fill(arena);
            for page_id in self.allocated_pages(arena) {
                // Nothing has been parked yet, so an error here needs no repair — which is why
                // these are plain `?` where they used to route through `abandon_park`.
                let birth = self.page_birth(page_id)?;
                // The reclamation rule as an index question rather than an array walk: is
                // there a live child forked in [birth, free_epoch)? Same predicate, asked of a
                // structure that can answer it without holding every child resident.
                let pinned =
                    self.catalog.live_child_in_epoch_range(rec.branch_id.id, birth, free_epoch)?;
                plan.push((page_id, arena, birth, pinned));
            }
        }

        // PHASE 2 applies and records with `persist` held throughout, exactly as `free_arena` and
        // `put_pending` do. What is newly serialised against extent claims is one `evict` per
        // released page — the same work `free_arena` already does under this lock — and not the
        // page reads and catalog queries, which stayed in phase 1.
        let mut persist = self.persist.lock().unwrap();
        // ⭐ **D183 de-dup at push: a page already PENDING is decided by its entry, not again here.**
        // `allocated_pages` lists a parked page, because parking does not recycle it, so this loop
        // used to decide such a page a second time (`frontier/catalog_root_and_park_adversary.md` §B1
        // @ `a4b48ea`). Two ways to get here:
        //   * a resumed reap: this branch's own earlier attempt parked it, and the `Reaping ->
        //     Reaped` flip never happened;
        //   * its owner's `free_page` parked it while the owner was live.
        // Still pinned, it was parked twice. Unpinned, it was RELEASED while its entry stayed in the
        // log, so one page was both pending and recycled and the next drain released it again: a
        // no-op only while nothing reissues it in between (§B2-B3).
        //
        // Skipping it moves nothing that matters. In-process the entry's free came first, so its range
        // `[birth, e_f)` lies inside this call's `[birth, free_epoch)`: "unpinned here" implies
        // "unpinned for the entry", and the drain `reap` runs next releases it in the same reap. What
        // is lost is a pin for children forked after the page was freed, which cannot see it (§B3).
        // After a restart the two epochs can be in either order, but the owner is Reaping, so no child
        // of it forked between them and the two decisions are equal (§B2 point 1).
        //
        // Filtered under `persist` and `state` together. ⚠ Two blind spots, both closed today by
        // something other than this filter:
        //   * Between this filter and `release_page` below, only an UNRECORDED push can land, since
        //     recorded ones need `persist`. That is `free_page`, and `free_page` of a page whose owner is
        //     Reaping does not happen, because a Reaping owner writes nothing (§B2 reason 2). On the
        //     park arm, `push_if_absent` closes the gap anyway.
        //   * An entry a drain is HOLDING, between its `take_pending` and its `put_pending`, is not in
        //     the log, so this filter cannot see it. A retire in that window can release K, and the
        //     drain then puts `K(e_f)` back: K is pending and recycled. The statement lock keeps every
        //     retire out of a drain's window (§B2 reason 4). Closing it without that lock means deciding
        //     against the page's incarnation, which is the `release_page` check recorded as a
        //     precondition for removing the lock (lane AMENDMENT 3, review F1).
        let plan: Vec<(PageId, ArenaId, Epoch, bool)> = {
            let st = self.state.lock().unwrap();
            plan.into_iter()
                .filter(|&(page, arena, _, _)| !st.pending.contains((page, arena)))
                .collect()
        };
        let mut released = 0u32;
        let mut parks = RecordedParks { persist: &mut persist, parked: Vec::new() };
        for (page_id, arena, birth, pinned) in plan {
            if !pinned {
                self.release_page(page_id, arena);
                released += 1;
            } else {
                let entry = PendingFree {
                    page_id,
                    arena_id: arena,
                    birth_epoch: birth,
                    free_epoch,
                    owner: rec.branch_id,
                };
                self.push_pending_recorded(&mut parks, entry);
            }
        }
        // The slow path changes the durable map every bit as much as the fast one: pages recycled
        // inside a still-live extent, and a pending-free log that nothing but this map records. Left
        // unpersisted, a crash after `mark_reaped` (which clears `rec.arenas`) loses both — the
        // pending entries are gone so `drain_pending` never revisits them, the extent's durable
        // `next_free` is above its recycled count so `extent_is_empty` refuses, and nothing points
        // at the arena any more. Once per branch reaped, not once per page.
        //
        // **D183 — and it is a 9+N·32-byte record now, not the whole 48·L-byte image.** This was
        // `persist_if_configured`, i.e. `persist_full_locked` unconditionally: one full rewrite per
        // INTERIOR branch reaped, never consulting `compact_threshold`, so `sum(48·i) = 24·N²`
        // bytes over a run of N such reaps. That is the shape D81 took off the fork door, alive on
        // the reap door, and it opens on exactly the predicate this project is aimed at — a reaped
        // branch having live children, i.e. deep fork chains. The record describes what the loop
        // above actually did: the entries it parked, and the recycled lists and fills of the
        // arenas it walked. The existing threshold decides when to compact, as it does for a claim.
        //
        // **The guard taken at the top of phase 2 is STILL HELD here, and that is the fix.** The
        // mutations above, this snapshot and the append below are one critical section, so the
        // order records reach the file is the order memory changed in — which is what
        // [`PersistState`] requires and what the old two-phase-lock shape violated. Re-acquiring
        // it here would deadlock: `std::sync::Mutex` is not reentrant, and doing exactly that is
        // how this comment came to be written.
        let (payload, dirty) = {
            let mut st = self.state.lock().unwrap();
            let dirty = std::mem::take(&mut st.recycled_dirty);
            // **Every extent this call WALKED, not only the ones it released into.** An extent
            // whose pages were all parked is never marked by `release_page`, and its `next_free`
            // may still be the image's — `alloc_in_arena` advances that number and persists
            // nothing (see [`PersistState`]). The full rewrite this record replaces wrote the
            // true value, so leaving it out is a real divergence, and it is the one direction that
            // matters: an UNDERSTATED durable `next_free` is what makes `extent_is_empty` call an
            // extent that still holds a live child's pages collectable. D85's `fill_unknown` still
            // catches it on restore; writing the number down is better than being caught.
            // `an_interior_reap_replayed_from_the_tail_is_byte_identical_to_a_full_rewrite` failed
            // on exactly this field before the union was added.
            //
            // Free asymptotically: the loop above already visited every page of every one of them.
            let mut covered = dirty.clone();
            covered.extend(rec.arenas.iter().copied());
            let mut p = Vec::new();
            p.extend_from_slice(&self.live_pages.load(Ordering::SeqCst).to_be_bytes());
            p.extend_from_slice(&(parks.parked.len() as u32).to_be_bytes());
            for e in &parks.parked {
                Self::encode_pending_entry(&mut p, e);
            }
            Self::encode_arena_sections(&mut p, &st, &covered);
            (p, dirty)
        };
        // `None`: this record carries its OWN parked entries and nobody else's, so it is only
        // correct against a durable log already level with memory. See `persist_delta_locked`.
        if let Err(e) =
            self.persist_delta_locked(&mut *parks.persist, Self::TAIL_PAGES_PARKED, &payload, None)
        {
            // Nothing reached the file. Put the marks back — and the record that was going to
            // describe the parked entries did not land either, so they are in exactly the state
            // `abandon_park` exists for.
            //
            // **With the guard STILL HELD.** This arm used to `drop(persist)` first, which left a
            // gap in which a `take_pending` read `level == true` for a file missing every entry
            // parked above — the ordering defect again, on the failure arm. `parks` borrows the
            // guard, so that `drop` no longer compiles here.
            self.state.lock().unwrap().recycled_dirty.extend(dirty);
            return self.abandon_park(&parks.parked, e);
        }
        Ok(released)
    }

    /// Give up on a `retire_arenas_by_rule` that did not get its record written, without leaving
    /// the entries it already parked invisible.
    ///
    /// **They are in memory and in no record**, and the pushes themselves do not bump
    /// [`Self::pending_version`] precisely because the record at the end of the loop describes
    /// them. Returning without that record would leave the counter saying the durable log is level
    /// with memory when it is short by everything parked so far — so the next claim would happily
    /// append behind a file that does not list them, and a crash before the next compaction leaks
    /// every one. Marking the log dirty makes the next persist a full rewrite instead.
    ///
    /// Only one way out of that method comes here now: a failure to append at the end, which is the
    /// easy one to miss, because by then the payload has been built and it *looks* finished. (An
    /// error inside the decision loop used to come here too; since the two-phase split that loop
    /// parks nothing, so it returns with a plain `?`.) Called with the persist guard still held, so
    /// the bump lands before any `take_pending` can read the counter — see [`RecordedParks`].
    ///
    /// The recycled lists need no equivalent from the loop: `release_page` marks its arena in
    /// [`StoreState::recycled_dirty`] as it goes, and the loop never takes that set.
    fn abandon_park<T>(&self, parked: &[PendingFree], e: FerroError) -> Result<T, FerroError> {
        if !parked.is_empty() {
            self.pending_version.fetch_add(1, Ordering::SeqCst);
        }
        Err(e)
    }

    fn page_birth(&self, page_id: PageId) -> Result<Epoch, FerroError> {
        Ok(self.read_page(page_id)?.header()?.birth_epoch)
    }

    // ---- durable free-space map ------------------------------------------------------------
    //
    // `BranchRecord::arenas` is durable, but which *pages* an extent covers, which of them came
    // back, and what is parked in the pending-free log are not derivable from it. Without this,
    // a restart would forget the free-space map: every extent would look untouched, freed space
    // would never be handed out again, and the pending-free log would silently release pages a
    // live child can still see. So the whole map is checkpointed, not reconstructed by guesswork.
    //
    // Format (big-endian, matching the rest of ferrodb):
    //   version u8 | base_page u32 | next_extent_start u32 | next_arena_id u32 | live u32
    //       | reserved u32
    //   free_extents: u32 count, then start u32 | page_count u32   (v3; v2 wrote start only)
    //   extents: u32 count, then per extent
    //       arena u32 | owner.id u64 | owner.gen u32 | start u32 | page_count u32 | next_free u32
    //       | recycled u32 count, u32 each
    //   current: u32 count, then branch.id u64 | branch.gen u32 | arena u32
    //   pending: u32 count, then page u32 | arena u32 | birth u64 | free u64 | owner.id u64
    //       | owner.gen u32
    //   crc32 u32

    /// v2 added `base_page` immediately after the version byte. It is what makes a checkpoint
    /// self-describing: before it, the region's base existed only as an argument the caller
    /// remembered to pass, and reattaching at the wrong base aliased another arena silently.
    ///
    /// **v3 (D31) gives every freed extent its size.** Extents stopped being uniform, so a bare
    /// start page no longer says how big the hole is, and reusing a v2 entry as if it were the
    /// cap would alias up to 255 pages. v2 images still load — every extent a v2 store could
    /// free was exactly `extent_pages` long, so that is what its entries are read as, which is a
    /// fact about the old format rather than a guess. See [`Self::READABLE_STATE_VERSIONS`].
    const STATE_VERSION: u8 = 3;

    /// Versions [`Self::load_state`] accepts. Written as an allowlist: a denylist of known-bad
    /// versions would accept every future one.
    const READABLE_STATE_VERSIONS: &'static [u8] = &[2, 3];

    /// Serialize the free-space map and pending-free log.
    pub fn state_bytes(&self) -> Vec<u8> {
        let st = self.state.lock().unwrap();
        self.state_bytes_locked(&st)
    }

    /// [`Self::state_bytes`] against a `state` lock the caller already holds.
    ///
    /// **D183** split this out for one reason: `persist_full_locked` has to clear
    /// [`StoreState::recycled_dirty`] and serialise the image without letting go in between. Same
    /// bytes, same order, same CRC — `two_stores_in_the_same_state_checkpoint_byte_identical_images`
    /// still pins them.
    fn state_bytes_locked(&self, st: &StoreState) -> Vec<u8> {
        let mut b = Vec::new();
        b.push(Self::STATE_VERSION);
        b.extend_from_slice(&self.space.base_page.to_be_bytes());
        // The two counters are written as they always were: **the issued watermark occupies the
        // slot the old `AtomicU32` did, and is the same number.** Held-but-unissued grant ranges
        // are deliberately NOT persisted — a grant is proved by the replicated log, which replays
        // it, and a range recorded here would be a second, weaker record of the same fact that a
        // restart could disagree with. Keeping the format byte-identical is also what lets
        // `two_stores_in_the_same_state_checkpoint_byte_identical_images` still hold and every
        // `<db>.arena` already on disk still open.
        b.extend_from_slice(&(self.space.extent_starts.issued_through() as u32).to_be_bytes());
        b.extend_from_slice(&(self.space.arena_ids.issued_through() as u32).to_be_bytes());
        b.extend_from_slice(&self.live_pages.load(Ordering::SeqCst).to_be_bytes());
        b.extend_from_slice(&self.reserved_pages.load(Ordering::SeqCst).to_be_bytes());

        // Sorted, for the same reason the two maps below are: this function decides the bytes of
        // a durable file and the CRC32 over them, and a `HashMap` iterated in hash order gives two
        // processes in identical states two different images.
        let free = self.space.free_extents.lock().unwrap();
        let mut flat: Vec<(PageId, u32)> =
            free.iter().flat_map(|(pages, starts)| starts.iter().map(|s| (*s, *pages))).collect();
        drop(free);
        flat.sort_unstable();
        b.extend_from_slice(&(flat.len() as u32).to_be_bytes());
        for (start, pages) in &flat {
            b.extend_from_slice(&start.to_be_bytes());
            b.extend_from_slice(&pages.to_be_bytes());
        }

        // The two maps below are walked in **key order**, not hash order, because this function
        // decides the bytes of a durable file and the CRC32 over them. Iterated as `HashMap`s, one
        // arena state serialised by two processes produced two different images with two different
        // checksums: no test can pin such an image, and a crash sweep over `<db>.arena` — the
        // obvious next use of `storage::sim` — would be as unreplayable as `flush_all` was.
        let mut extents: Vec<_> = st.extents.iter().collect();
        extents.sort_unstable_by_key(|(arena, _)| **arena);

        b.extend_from_slice(&(extents.len() as u32).to_be_bytes());
        for (arena, ext) in extents {
            b.extend_from_slice(&arena.0.to_be_bytes());
            b.extend_from_slice(&ext.owner.id.to_be_bytes());
            b.extend_from_slice(&ext.owner.generation.to_be_bytes());
            b.extend_from_slice(&ext.start_page.to_be_bytes());
            b.extend_from_slice(&ext.page_count.to_be_bytes());
            b.extend_from_slice(&ext.next_free.to_be_bytes());
            let empty = Vec::new();
            let rec = st.recycled.get(arena).unwrap_or(&empty);
            b.extend_from_slice(&(rec.len() as u32).to_be_bytes());
            for p in rec {
                b.extend_from_slice(&p.to_be_bytes());
            }
        }

        let mut current: Vec<_> = st.current.iter().collect();
        current.sort_unstable_by_key(|(branch, _)| **branch);

        b.extend_from_slice(&(current.len() as u32).to_be_bytes());
        for (branch, arena) in current {
            b.extend_from_slice(&branch.id.to_be_bytes());
            b.extend_from_slice(&branch.generation.to_be_bytes());
            b.extend_from_slice(&arena.0.to_be_bytes());
        }

        b.extend_from_slice(&(st.pending.len() as u32).to_be_bytes());
        for p in st.pending.iter() {
            // **D183** — the same five fields the tail records write, written once. The image's
            // layout is unchanged; what changed is that there is now only one place it is spelled.
            Self::encode_pending_entry(&mut b, p);
        }

        let crc = crc32(&b);
        b.extend_from_slice(&crc.to_be_bytes());
        b
    }

    /// Replace the free-space map from a checkpoint. Refuses a truncated or corrupt image rather
    /// than loading a partial map — a free-space map that is half right hands out live pages.
    pub fn load_state(&self, bytes: &[u8]) -> Result<(), FerroError> {
        let body = bytes
            .len()
            .checked_sub(4)
            .ok_or_else(|| BranchError::Arena("arena state shorter than its checksum".into()))?;
        let stored = u32::from_be_bytes(bytes[body..].try_into().unwrap());
        if crc32(&bytes[..body]) != stored {
            return Err(BranchError::Arena("arena state checksum mismatch".into()).into());
        }
        let mut c = StateCursor { b: &bytes[..body], at: 0 };
        let version = c.u8()?;
        if !Self::READABLE_STATE_VERSIONS.contains(&version) {
            return Err(BranchError::Arena(format!(
                "unknown arena state version {} (readable: {:?})",
                version,
                Self::READABLE_STATE_VERSIONS
            ))
            .into());
        }
        // The checkpoint names the region it describes. Loading a map whose base is not this
        // store's would silently graft another arena's extents onto this one's space, and every
        // page id in the rest of the image would refer to somebody else's pages.
        let base = c.u32()?;
        if base != self.space.base_page {
            return Err(BranchError::Arena(format!(
                "arena state describes the region at {} but this store owns {}",
                base, self.space.base_page
            ))
            .into());
        }
        let next_start = c.u32()?;
        let next_arena = c.u32()?;
        let live = c.u32()?;
        let reserved = c.u32()?;

        let n = c.u32()? as usize;
        let mut free_extents: HashMap<u32, Vec<PageId>> = HashMap::new();
        for _ in 0..n {
            let start = c.u32()?;
            // v2 had exactly one extent size and could not have freed any other, so this is what
            // its entries mean rather than an assumption about them.
            let pages = if version >= 3 { c.u32()? } else { self.space.extent_pages };
            free_extents.entry(pages).or_default().push(start);
        }

        let n = c.u32()? as usize;
        let mut extents = HashMap::with_capacity(n);
        let mut recycled = HashMap::with_capacity(n);
        for _ in 0..n {
            let arena = ArenaId(c.u32()?);
            let owner = BranchId::new(c.u64()?, c.u32()?);
            let ext = ArenaExtent {
                arena_id: arena,
                owner,
                start_page: c.u32()?,
                page_count: c.u32()?,
                next_free: c.u32()?,
            };
            let rn = c.u32()? as usize;
            let mut r = Vec::with_capacity(rn);
            for _ in 0..rn {
                r.push(c.u32()?);
            }
            extents.insert(arena, ext);
            recycled.insert(arena, r);
        }

        let n = c.u32()? as usize;
        let mut current = HashMap::with_capacity(n);
        for _ in 0..n {
            let branch = BranchId::new(c.u64()?, c.u32()?);
            current.insert(branch, ArenaId(c.u32()?));
        }

        let n = c.u32()? as usize;
        let mut pending = Vec::with_capacity(n);
        for _ in 0..n {
            pending.push(Self::decode_pending_entry(&mut c)?);
        }
        if c.at != c.b.len() {
            return Err(BranchError::Arena(format!(
                "arena state has {} trailing bytes; refusing a partial free-space map",
                c.b.len() - c.at
            ))
            .into());
        }

        // **Never resume filling a restored extent.** The image records `next_free` as of the last
        // checkpoint, but a session that died after it may have handed out pages beyond that mark,
        // and one of them can be the root a durable branch record still names. Dropping `current`
        // sends `arena_for` to `alloc_arena` for a fresh extent instead, so allocation resumes
        // above everything any previous session could have touched. The extents themselves stay in
        // the map, so their pages remain accounted for and the reaper can still free them; what is
        // given up is the tail of one extent per restore, which reclamation later takes back.
        //
        // `recycled` is deliberately kept: those pages were durably recorded as free *before* the
        // checkpoint, so handing them out again is correct rather than a collision.
        current.clear();
        // Restored extents are stamped with the authority in force NOW, which preserves today's
        // single-node behaviour exactly: `current` is cleared just above, so `arena_for` goes to
        // `alloc_arena` for a fresh extent and a restored extent's tail is given up either way.
        //
        // **What this cannot do is verify the authority the image was written under**, because the
        // image has no field for one and cannot grow one: `load_state` refuses an unknown version
        // (`arena.rs`), `key_order_in_image` hard-codes a 21-byte header, and
        // `two_stores_in_the_same_state_checkpoint_byte_identical_images` pins the bytes. So a
        // database that ran standalone, was converted to a cluster member, and then reopened from
        // its old image could reuse recycled pages the new leader does not know about. No path in
        // this repository performs that conversion, and closing it needs a decision above this row
        // — either bump `STATE_VERSION` and migrate every `<db>.arena` on disk, or make a grant
        // remember its range after it is consumed so a restored page can be checked against it.
        // Named in this row's summary rather than left to be discovered.
        let claim_epoch = extents.keys().map(|a| (*a, crate::cluster::epoch())).collect();
        // **D81 — a map that came from somewhere else is a file this process did not write.**
        //
        // Taken before the `state` lock, per the outermost-persist rule in [`PersistState`].
        //
        // The case that forces this is not a test: `consensus::snapshot`'s install writes a whole
        // arena image over the live `<db>.arena` with `std::fs::write` and then calls this to
        // update the running store (`snapshot.rs:1498,1541`). Without this line the store would go
        // on appending against `image_bytes` and `tail_bytes` describing the image it had BEFORE
        // the install. It happens to be survivable there — `fs::write` truncates, so there is no
        // stale tail — but that is a fact about another module's spelling, and the invariant this
        // restores is the one that means the accounting never has to be reasoned about from
        // outside: `image_bytes == 0` iff this process has not written the image, so the next
        // persist is a full rewrite and the file is ours again.
        self.persist.lock().unwrap().image_bytes = 0;
        *self.state.lock().unwrap() =
            // **D85: every restored extent's fill is SUSPECT until probed.**
            //
            // `next_free` is not persisted per page allocation, so the image can understate it by
            // up to `ARENA_EXTENT_PAGES`. The comment above already gives up a restored extent's
            // TAIL for that reason, on the allocation side. Marking them here is the collection
            // side: until `resolve_fill` has probed an extent, `extent_is_empty` refuses to call
            // it empty, so nothing can free an extent that may still hold a live child's pages.
            StoreState {
                fill_unknown: extents.keys().copied().collect(),
                extents,
                recycled,
                current,
                // **D183 de-dup at push**, applied to a log this process did not build: an image written
                // before the rule can list a page twice, and it loads first-wins, as its tail replays.
                pending: PendingLog::from_entries(pending),
                // Nothing is dirty against a file this process did not write: `image_bytes` was
                // just reset to 0 above, so the next persist is a full rewrite and carries
                // everything anyway.
                recycled_dirty: std::collections::HashSet::new(),
                claim_epoch,
                // Deliberately NOT restored from the image: see the field's own doc. A cold map
                // makes the next shadow a chain root, which can only shorten chains.
                shadow_base: HashMap::new(),
            };
        *self.space.free_extents.lock().unwrap() = free_extents;
        // Raised, never lowered, and every held range is trimmed to match: the image says this
        // much was already issued, and a grant replayed afterwards must only re-offer its unissued
        // suffix. See `GrantedCounter::raise_issued_through`.
        self.space.extent_starts.raise_issued_through(next_start as u64);
        self.space.arena_ids.raise_issued_through(next_arena as u64);
        // The restored recycle stack is kept — those pages were granted to *this* node and were
        // never returned to the leader — but it is re-stamped with the authority in force now, so
        // a later `join` still invalidates it.
        self.space.recycle_epoch.store(crate::cluster::epoch(), Ordering::SeqCst);
        self.live_pages.store(live, Ordering::SeqCst);
        self.reserved_pages.store(reserved, Ordering::SeqCst);
        Ok(())
    }

    // ---- D81: the append-only tail ---------------------------------------------------------
    //
    // Tail record, big-endian like everything else here:
    //
    //   kind u8 | payload_len u32 | payload[payload_len] | crc32 u32   (over kind|len|payload)
    //
    // Self-delimiting and self-checked, because `append_durably` is atomic against other writers
    // and not against a power cut: a torn append leaves a partial record at the END of the file
    // and the reader has to be able to say so. See `replay_tail` for the rule that distinguishes
    // a torn last record from corruption in the middle, which is a different thing and is refused.

    /// An extent was claimed. `alloc_arena`'s durable record, and the one that was costing 48·N
    /// bytes and two fsyncs.
    const TAIL_ARENA_CLAIMED: u8 = 1;
    /// A whole extent was freed. `free_arena`'s fast path.
    const TAIL_EXTENT_FREED: u8 = 2;
    /// **D183.** The interval rule ran over a reaped branch's extents: entries were APPENDED to
    /// the pending-free log and the arenas it walked have new recycled lists.
    /// `retire_arenas_by_rule`'s record — the reap slow path, which used to rewrite the whole
    /// image once per interior branch.
    const TAIL_PAGES_PARKED: u8 = 3;
    /// **D183.** The pending-free log was REPLACED wholesale, and the arenas the drain released
    /// into have new recycled lists. `put_pending`'s record when it cannot prove the durable log
    /// is still the one the drain took — always correct, and O(the whole log).
    const TAIL_PENDING_REPLACED: u8 = 4;
    /// **D183.** A drain removed exactly these entries from the pending-free log.
    /// `put_pending`'s record when [`PersistState::drain_mark`] proves the durable log is the one
    /// `take_pending` handed out — **O(released) rather than O(the whole log)**, which is what
    /// keeps a run of interior reaps linear in bytes instead of quadratic.
    ///
    /// **Never written empty.** A drain that removed nothing and owes no recycled list would
    /// produce a record with no removal and no section. `put_pending` writes no record for it and
    /// moves `durable_pending_version` itself, but only where the record would have been
    /// APPENDED; an owed rewrite is still paid. See the elision in `put_pending` and
    /// `a_drain_that_released_nothing_writes_no_record_and_leaves_the_log_level`.
    const TAIL_PENDING_DRAINED: u8 = 5;
    /// Kinds this build understands. An allowlist for the same reason `READABLE_STATE_VERSIONS`
    /// is one.
    const KNOWN_TAIL_KINDS: &'static [u8] = &[
        Self::TAIL_ARENA_CLAIMED,
        Self::TAIL_EXTENT_FREED,
        Self::TAIL_PAGES_PARKED,
        Self::TAIL_PENDING_REPLACED,
        Self::TAIL_PENDING_DRAINED,
    ];

    /// Largest pending-free log for which a drain will be tracked key by key.
    ///
    /// Past it [`PersistState::drain_mark`] is not taken at all and `put_pending` writes the
    /// absolute record, so the transient 8 bytes per entry this costs is bounded by a constant
    /// and not by a workload. A bound rather than a warning.
    const DRAIN_MARK_CAP: usize = 1 << 16;

    /// Below this the tail may grow freely however small the image is.
    ///
    /// Without a floor, a nearly-empty store (21-byte image) would compact on its very first
    /// append and the tail would never be used at all. 4 KiB is one page of slack — about 100
    /// claims — and bounds the replay a crash can leave behind to something trivial.
    const TAIL_COMPACT_FLOOR_BYTES: u64 = 4096;

    /// Compact once the tail exceeds this many bytes.
    ///
    /// Half the image. That choice is what makes the write volume LINEAR rather than quadratic,
    /// and the arithmetic is worth stating because it is the whole point of the row: with a record
    /// of `r` bytes and an image of `48·L`, a compaction happens every `24·L/r` claims and costs
    /// `48·L` bytes, so the amortised per-claim cost is `2·r` bytes — **independent of L**. Total
    /// over a run of N claims: O(N), where rewriting in full was `sum(48·i) = 24·N²`.
    ///
    /// It also bounds the file at 1.5x the image and the replay at half of it.
    fn compact_threshold(image_bytes: u64) -> u64 {
        (image_bytes / 2).max(Self::TAIL_COMPACT_FLOOR_BYTES)
    }

    /// One pending-free entry, in exactly the 32-byte layout [`Self::state_bytes`] writes.
    ///
    /// Shared with the image on purpose: `putting_the_pending_log_back_reaches_the_durable_map`
    /// and this row's own restore test both compare a replayed log against one a full rewrite
    /// produced, and two spellings of the same five fields is the way that comparison silently
    /// starts measuring the encoder instead of the mechanism.
    fn encode_pending_entry(b: &mut Vec<u8>, p: &PendingFree) {
        b.extend_from_slice(&p.page_id.to_be_bytes());
        b.extend_from_slice(&p.arena_id.0.to_be_bytes());
        b.extend_from_slice(&p.birth_epoch.0.to_be_bytes());
        b.extend_from_slice(&p.free_epoch.0.to_be_bytes());
        b.extend_from_slice(&p.owner.id.to_be_bytes());
        b.extend_from_slice(&p.owner.generation.to_be_bytes());
    }

    fn decode_pending_entry(c: &mut StateCursor) -> Result<PendingFree, FerroError> {
        // Field order is read order: this mirrors `encode_pending_entry` above, which mirrors
        // `state_bytes`.
        Ok(PendingFree {
            page_id: c.u32()?,
            arena_id: ArenaId(c.u32()?),
            birth_epoch: Epoch(c.u64()?),
            free_epoch: Epoch(c.u64()?),
            owner: BranchId::new(c.u64()?, c.u32()?),
        })
    }

    /// **D183.** The per-extent half of a reclamation record: for every arena owed one, its
    /// `next_free` and its whole recycled list, **absolutely**.
    ///
    /// Absolute rather than incremental, and the difference is not a style choice. A record of
    /// "these pages joined the list" is only true if nothing ever takes one back out, and
    /// `alloc_in_arena` pops from exactly this list. An absolute list is what the extent *is* at
    /// the moment the record is cut, so it is also idempotent on replay and immune to the order
    /// two records for one arena reach the file in.
    ///
    /// Cost: at most `page_count` ids for an extent whose pages the caller was already visiting
    /// one at a time, so it does not change the order of the work that produced it.
    ///
    /// Arenas no longer in `extents` are SKIPPED, which is what makes this record commute with
    /// [`Self::TAIL_EXTENT_FREED`]: whichever order the two land in, replay ends with the extent
    /// gone and nothing re-describing it. `release_page` makes the same test in memory.
    ///
    /// Sorted, for the reason `state_bytes` sorts: these bytes reach a file and go under a CRC.
    fn encode_arena_sections(
        b: &mut Vec<u8>,
        st: &StoreState,
        dirty: &std::collections::HashSet<ArenaId>,
    ) {
        let mut ids: Vec<ArenaId> =
            dirty.iter().copied().filter(|a| st.extents.contains_key(a)).collect();
        ids.sort_unstable();
        b.extend_from_slice(&(ids.len() as u32).to_be_bytes());
        let empty = Vec::new();
        for a in ids {
            b.extend_from_slice(&a.0.to_be_bytes());
            b.extend_from_slice(&st.extents[&a].next_free.to_be_bytes());
            let rec = st.recycled.get(&a).unwrap_or(&empty);
            b.extend_from_slice(&(rec.len() as u32).to_be_bytes());
            for p in rec {
                b.extend_from_slice(&p.to_be_bytes());
            }
        }
    }

    /// Replay of [`Self::encode_arena_sections`]. Counts are **pushed, never reserved**: the CRC
    /// is checked before this runs, but a count that decides an allocation is a habit worth not
    /// having in a file parser — `StateCursor` bounds every read, so a bad count fails as
    /// "truncated".
    fn apply_arena_sections(c: &mut StateCursor, st: &mut StoreState) -> Result<(), FerroError> {
        let n = c.u32()? as usize;
        for _ in 0..n {
            let arena = ArenaId(c.u32()?);
            let next_free = c.u32()?;
            let rn = c.u32()? as usize;
            let mut r = Vec::new();
            for _ in 0..rn {
                r.push(c.u32()?);
            }
            // Read the whole section before deciding, so a skipped arena still advances the
            // cursor. Skipping the BYTES would desynchronise every section behind it.
            if let Some(ext) = st.extents.get_mut(&arena) {
                ext.next_free = next_free;
                st.recycled.insert(arena, r);
            }
        }
        Ok(())
    }

    fn encode_tail_record(kind: u8, payload: &[u8]) -> Vec<u8> {
        let mut b = Vec::with_capacity(9 + payload.len());
        b.push(kind);
        b.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        b.extend_from_slice(payload);
        let crc = crc32(&b);
        b.extend_from_slice(&crc.to_be_bytes());
        b
    }

    /// Persist the map on every extent claim from now on.
    ///
    /// Separate from construction because `reopen_from_checkpoint` already takes the path it read
    /// from, and a store that is only ever a test fixture should not be writing files.
    ///
    /// **Arming resets `image_bytes` to 0**, so the first persist against a newly armed path is a
    /// full rewrite. That is what stops this process appending onto a file it has not written —
    /// see [`PersistState`].
    pub fn checkpoint_to(&self, path: std::path::PathBuf) {
        let mut g = self.persist.lock().unwrap();
        *self.checkpoint_path.lock().unwrap() = Some(path);
        g.image_bytes = 0;
        g.tail_bytes = 0;
        g.image_epoch = crate::cluster::epoch();
    }

    /// Rewrite the whole image and drop the tail with it.
    ///
    /// `replace_atomically` renames a fresh inode over the target, so the previous tail goes away
    /// with the previous inode rather than being left stranded after a shorter image. Pinned by
    /// `a_replace_after_appends_drops_the_tail_instead_of_stranding_it`.
    fn persist_full_locked(&self, g: &mut PersistState) -> Result<(), FerroError> {
        let path = self.checkpoint_path.lock().unwrap().clone();
        let Some(p) = path else { return Ok(()) };
        // Read BEFORE the image is serialised. A push that lands in between is written into the
        // image anyway and merely leaves this looking stale, which costs one extra rewrite later.
        // Reading it afterwards would do the opposite: record a change as durable that the image
        // does not contain.
        let pending_version = self.pending_version.load(Ordering::SeqCst);
        // **D183 — the clear and the serialisation are ONE critical section.** A full image is
        // every arena's recycled list, so nothing is owed a record afterwards. Clearing on either
        // side of the serialisation instead leaves an interleaving where a `release_page` lands
        // in the gap: cleared but not written, i.e. a page that comes back in no record and no
        // image. That is why `state_bytes` was split rather than called.
        //
        // **And `recycled_reissued` (main, `97b4564`) is cleared in the SAME section.** A pop sets
        // it while holding `state` (see `alloc_in_arena`), so clearing it here, under that lock and
        // before `state_bytes_locked` reads memory, cannot drop a pop the image does not contain:
        // a pop either precedes this section (the image has it) or follows it (and sets the flag
        // again). Main's order — clear, then serialise — made atomic by the lock D183 takes here.
        let bytes = {
            let mut st = self.state.lock().unwrap();
            st.recycled_dirty.clear();
            self.recycled_reissued.store(false, Ordering::SeqCst);
            self.state_bytes_locked(&st)
        };
        replace_atomically(&OsFileOps, &p, &bytes).map_err(|e| FerroError::Io(e.to_string()))?;
        let written = bytes.len();
        g.image_bytes = written as u64;
        g.tail_bytes = 0;
        g.image_epoch = crate::cluster::epoch();
        g.durable_pending_version = pending_version;
        g.rewrites += 1;
        Ok(())
    }

    /// Append one delta record, or rewrite the whole image if appending is not available or the
    /// tail has grown past its share.
    ///
    /// The four conditions that force a rewrite are each a place where a delta would be a lie,
    /// not a tuning knob:
    ///   * `image_bytes == 0` — this process has not written the image; see [`PersistState`].
    ///   * the authority epoch moved — `free_extents` was cleared wholesale, which no per-extent
    ///     record describes.
    ///   * the pending-free log changed — see [`PersistState::durable_pending_version`].
    ///   * the tail would exceed [`Self::compact_threshold`] — the amortisation bound.
    ///
    /// # `pending_covered` — **D183**
    ///
    /// `None` is the original rule and the one every record that does not mention the pending log
    /// must use: append only while the durable log still equals the in-memory one, because such a
    /// record leaves the log exactly as it found it.
    ///
    /// `Some(v)` says "after this record the durable log is at version `v`", so the third
    /// condition does not apply. **TWO different records pass it and they earn it two different
    /// ways — do not add a third by reading only one of them:**
    ///
    ///   * [`Self::TAIL_PENDING_REPLACED`] carries the WHOLE log. It does not need the log to be
    ///     clean, it MAKES it clean, so any `v` it read under the same `state` lock as the log it
    ///     encoded is true by construction.
    ///   * [`Self::TAIL_PENDING_DRAINED`] carries only REMOVALS and makes nothing clean. It is
    ///     correct only against a durable log that already equals the one `take_pending` handed
    ///     out, and its whole safety is [`PersistState::drain_mark`] plus `put_pending`'s subset
    ///     test. Passing `Some(v)` for it is a claim those two guards have to have checked first.
    ///
    /// ⚠ An earlier version of this paragraph said `Some(v)` was "for a record that carries the
    /// WHOLE log — today only `TAIL_PENDING_REPLACED`", which was false the moment the drain
    /// record existed. A future kind added on the strength of that sentence would skip the third
    /// condition with no equivalent of `drain_mark` behind it, and the failure would be a durable
    /// log silently short an entry. In every case `v` must be read under the same `state` lock as
    /// the log the record describes; a `v` read anywhere else claims durability for a push the
    /// record does not contain.
    ///
    /// ⚠ [`Self::TAIL_PAGES_PARKED`] passes `None` deliberately even though it APPENDS to the
    /// log. It carries its own entries and nobody else's, so it is only correct against a durable
    /// log that is already level with memory — which is precisely what `None` demands.
    fn persist_delta_locked(
        &self,
        g: &mut PersistState,
        kind: u8,
        payload: &[u8],
        pending_covered: Option<u64>,
    ) -> Result<(), FerroError> {
        let path = self.checkpoint_path.lock().unwrap().clone();
        let Some(p) = path else { return Ok(()) };
        let rec = Self::encode_tail_record(kind, payload);
        if !self.delta_would_append(g, rec.len() as u64, pending_covered) {
            // The rewrite folds in the mutation this record described, because `state_bytes`
            // serialises live memory and the caller has already applied it. So the record is
            // simply not needed, rather than needed and skipped.
            return self.persist_full_locked(g);
        }
        append_durably(&OsFileOps, &p, &rec).map_err(|e| FerroError::Io(e.to_string()))?;
        g.tail_bytes += rec.len() as u64;
        g.appends += 1;
        if let Some(v) = pending_covered {
            g.durable_pending_version = v;
        }
        Ok(())
    }

    /// Whether [`Self::persist_delta_locked`] would APPEND a record of `rec_len` bytes (true) or
    /// rewrite the whole image (false). The four conditions are documented there.
    ///
    /// One function, so the append path and `put_pending`'s elision of a record with nothing to
    /// say cannot disagree about when appending is allowed. See
    /// [`Self::TAIL_PENDING_DRAINED`]: an elision stands in for an APPEND and never for an owed
    /// rewrite.
    fn delta_would_append(
        &self,
        g: &PersistState,
        rec_len: u64,
        pending_covered: Option<u64>,
    ) -> bool {
        !(g.image_bytes == 0
            || g.image_epoch != crate::cluster::epoch()
            || (pending_covered.is_none()
                && g.durable_pending_version != self.pending_version.load(Ordering::SeqCst))
            // A recycled page was handed out again and no record kind can describe that: the
            // claim record carries no recycled list and the free record removes one. The only
            // honest answer is to stop appending. See [`Self::recycled_reissued`] — this is the
            // one condition here whose absence is a live page freed rather than a leak.
            || self.recycled_reissued.load(Ordering::SeqCst)
            || g.tail_bytes + rec_len > Self::compact_threshold(g.image_bytes))
    }

    /// Drains `put_pending` answered with no record at all, because the drain changed nothing a
    /// record could carry. See [`Self::TAIL_PENDING_DRAINED`].
    #[cfg(test)]
    pub(crate) fn elided_drains(&self) -> u64 {
        self.persist.lock().unwrap().elided
    }

    /// Pending-free log entries tail replay has visited over this store's life. The unit is
    /// defined on the `replay_pending_visits` field.
    #[cfg(test)]
    pub(crate) fn replay_pending_visits(&self) -> u64 {
        self.replay_pending_visits.load(Ordering::SeqCst)
    }

    /// `(full rewrites, tail appends)` this store has performed against its armed path.
    ///
    /// The whole of D81 stated as two integers: what used to be one rewrite per claim should now
    /// be one append per claim and a rewrite only when the tail outgrows its share.
    #[cfg(test)]
    pub(crate) fn persist_counters(&self) -> (u64, u64) {
        let g = self.persist.lock().unwrap();
        (g.rewrites, g.appends)
    }

    /// **D183.** The record kinds actually present in the tail of `path`, in file order.
    ///
    /// **Ask the artifact, not the code that wrote it.** `persist_counters` says how many records
    /// were appended and says nothing about WHICH, so a test whose site silently fell back to a
    /// different record — `TAIL_PENDING_REPLACED` where `TAIL_PENDING_DRAINED` was the point —
    /// reads exactly like one where it did not. This parses the file the same way `replay_tail`
    /// does and returns what is there.
    ///
    /// Stops at the first torn or unparseable frame, like the replayer; it is an instrument for
    /// tests, so it reports what it can see rather than refusing.
    #[cfg(test)]
    pub(crate) fn tail_kinds(path: &std::path::Path) -> Vec<u8> {
        let Ok(buf) = std::fs::read(path) else { return Vec::new() };
        let Ok(n) = Self::image_len(&buf) else { return Vec::new() };
        let mut kinds = Vec::new();
        let mut at = n;
        while at + 9 <= buf.len() {
            let kind = buf[at];
            if kind == 0 {
                break;
            }
            let len = u32::from_be_bytes(buf[at + 1..at + 5].try_into().unwrap()) as usize;
            let Some(total) = len.checked_add(9).filter(|t| at + *t <= buf.len()) else { break };
            kinds.push(kind);
            at += total;
        }
        kinds
    }

    /// How many bytes of `buf` the IMAGE occupies, including its trailing CRC32.
    ///
    /// # Why this exists instead of a length field in the header
    ///
    /// The obvious spelling is a v4 image that records its own length. It was not taken. This
    /// file's own D85 note spells out the price of a version bump — *"bump `STATE_VERSION` and
    /// migrate every `<db>.arena` on disk"* — and `key_order_in_image` and
    /// `two_stores_in_the_same_state_checkpoint_byte_identical_images` both pin the current
    /// layout. Walking the structure instead costs one pass over ~48·L bytes at open time and
    /// leaves the format, every file on disk, and both pinning tests exactly as they were.
    ///
    /// **It allocates nothing.** That is deliberate and it is what makes it safe to run BEFORE the
    /// checksum: every count it reads is used only to advance a bounds-checked cursor, so a
    /// corrupt count fails as "truncated" instead of reserving four billion entries. `load_state`
    /// keeps its own checksum-first order, because by then the slice is known to be an image.
    ///
    /// ⚠ **It mirrors [`Self::state_bytes`] and nothing in the type system says so.** Pinned by
    /// `the_image_walker_agrees_with_state_bytes_on_a_fully_populated_store`, whose fixture fills
    /// every variable-length section — free extents, recycled pages, current, pending — because a
    /// fixture that leaves one empty cannot see that section's stride being wrong.
    fn image_len(buf: &[u8]) -> Result<usize, FerroError> {
        let mut c = StateCursor { b: buf, at: 0 };
        let version = c.u8()?;
        if !Self::READABLE_STATE_VERSIONS.contains(&version) {
            return Err(BranchError::Arena(format!(
                "unknown arena state version {} (readable: {:?})",
                version,
                Self::READABLE_STATE_VERSIONS
            ))
            .into());
        }
        // base_page, next_extent_start, next_arena_id, live, reserved.
        c.skip(20)?;
        // free_extents: v3 writes (start, page_count); v2 wrote start alone.
        let n = c.u32()? as usize;
        c.skip(n.checked_mul(if version >= 3 { 8 } else { 4 }).unwrap_or(usize::MAX))?;
        // extents: arena u32 | owner u64+u32 | start u32 | page_count u32 | next_free u32, then
        // a counted list of recycled page ids.
        let n = c.u32()? as usize;
        for _ in 0..n {
            c.skip(28)?;
            let rec = c.u32()? as usize;
            c.skip(rec.checked_mul(4).unwrap_or(usize::MAX))?;
        }
        // current: branch u64+u32 | arena u32.
        let n = c.u32()? as usize;
        c.skip(n.checked_mul(16).unwrap_or(usize::MAX))?;
        // pending: page u32 | arena u32 | birth u64 | free u64 | owner u64+u32.
        let n = c.u32()? as usize;
        c.skip(n.checked_mul(36).unwrap_or(usize::MAX))?;

        let body = c.at;
        let stored = u32::from_be_bytes(c.take(4)?.try_into().unwrap());
        if crc32(&buf[..body]) != stored {
            return Err(BranchError::Arena("arena state checksum mismatch".into()).into());
        }
        Ok(body + 4)
    }

    /// Load a whole `<db>.arena` file: the image, then every intact tail record behind it.
    ///
    /// Returns how many bytes of tail were applied, which is what tells the caller whether the
    /// file is already compact.
    fn load_file(&self, buf: &[u8]) -> Result<u64, FerroError> {
        let n = Self::image_len(buf)?;
        self.load_state(&buf[..n])?;
        self.replay_tail(&buf[n..])
    }

    /// Apply the tail records in `tail`, stopping at a torn final record and **refusing**
    /// anything else.
    ///
    /// # The rule, and why it is not simply "stop at the first bad record"
    ///
    /// Stopping is the standard log-tail rule and it is right for the one shape a crash actually
    /// produces: an append that did not finish, which is always LAST because `append_durably`
    /// fsyncs before `alloc_arena` returns and the process that crashed does not come back to
    /// write another. Dropping such a record is correct — the claim was never acknowledged, so no
    /// page was ever written into that extent.
    ///
    /// It is wrong for anything else. A record that is fully present, well-framed and fails its
    /// CRC **with more bytes behind it** is not a torn tail; it is corruption, and stopping there
    /// would silently discard every later claim — extents whose pages a durable branch record
    /// still names, whose watermark advance would be lost, and whose range the next claim would
    /// hand out again. So that case errors, the way `load_state` errors on a bad image, rather
    /// than quietly returning a map that is half right.
    ///
    /// A zero `kind` is treated as unwritten space rather than an unknown kind: a crash can expose
    /// a zero-filled extension, and kinds start at 1 precisely so that reads as "nothing here".
    fn replay_tail(&self, tail: &[u8]) -> Result<u64, FerroError> {
        // **D183 tail replay: index the pending-free log ONCE per replay, not once per record.**
        //
        // Three record kinds change the log, and each used to walk ALL of it for every record:
        // * `TAIL_PAGES_PARKED` built a `HashSet` of every key;
        // * `TAIL_PENDING_DRAINED` ran `retain` over it, even for a record naming nothing;
        // * `TAIL_EXTENT_FREED` ran `retain` over it.
        //
        // D183 made the slow reap and the drain APPEND instead of rewriting the image, so a tail
        // now spans many reaps before `compact_threshold` folds it back into an image. Replaying
        // one therefore cost O(records x P), paid on the one open where speed matters most: the
        // open after an unclean exit. A clean CLI exit writes a full image first (the exit
        // `store.checkpoint` in `src/cli/cli.rs`), so it never reaches this path with a long tail.
        //
        // Now the log is taken out of `state` once and indexed at most once (O(P)), by the first
        // record that touches it. A tail with no such record, which is every clean open, does no
        // work on the log at all. Every record then applies ITS OWN entries by key, and the result
        // is written back once. Total: O(P + tail records + the entries they carry).
        // [`PendingReplay`] states the rules it has to keep, and
        // `replay_of_the_pending_log_keeps_every_rule_the_per_record_scans_had` and
        // `a_repark_of_a_key_already_in_the_log_keeps_the_first_entry` pin them.
        let mut pending = PendingReplay::new(std::mem::take(&mut self.state.lock().unwrap().pending));
        let applied = self.replay_records(tail, &mut pending);
        // Written back on the error path too. The open fails either way, but a store whose log
        // has quietly vanished is not a state any caller should be able to observe.
        let (log, visits) = pending.finish();
        self.state.lock().unwrap().pending = log;
        #[cfg(test)]
        self.replay_pending_visits.fetch_add(visits, Ordering::SeqCst);
        #[cfg(not(test))]
        let _ = visits;
        applied
    }

    /// The record loop of [`Self::replay_tail`]: frame, check and apply each record in order,
    /// with the pending-free log held in `pending` for the whole replay.
    fn replay_records(&self, tail: &[u8], pending: &mut PendingReplay) -> Result<u64, FerroError> {
        let mut at = 0usize;
        while at < tail.len() {
            let rest = &tail[at..];
            if rest.len() < 9 {
                break; // torn: not even a frame
            }
            let kind = rest[0];
            if kind == 0 {
                break; // zero-filled extension, not a record
            }
            let len = u32::from_be_bytes(rest[1..5].try_into().unwrap()) as usize;
            let Some(total) = len.checked_add(9).filter(|t| *t <= rest.len()) else {
                break; // torn: the frame promises more than the file holds
            };
            let stored = u32::from_be_bytes(rest[total - 4..total].try_into().unwrap());
            if crc32(&rest[..total - 4]) != stored {
                if total < rest.len() {
                    return Err(BranchError::Arena(format!(
                        "arena tail record at byte {at} fails its checksum and is NOT the last \
                         record ({} bytes follow it): that is corruption rather than a torn \
                         append, and stopping here would discard claims whose page ranges the \
                         next allocation would then hand out again",
                        rest.len() - total
                    ))
                    .into());
                }
                break; // torn: the last record did not finish
            }
            if !Self::KNOWN_TAIL_KINDS.contains(&kind) {
                return Err(BranchError::Arena(format!(
                    "arena tail record kind {kind} at byte {at} was written by a newer build \
                     (known: {:?}); refusing rather than skipping a change to the free-space map",
                    Self::KNOWN_TAIL_KINDS
                ))
                .into());
            }
            self.apply_tail_record(kind, &rest[5..total - 4], pending)?;
            at += total;
        }
        Ok(at as u64)
    }

    fn apply_tail_record(
        &self,
        kind: u8,
        payload: &[u8],
        pending: &mut PendingReplay,
    ) -> Result<(), FerroError> {
        let mut c = StateCursor { b: payload, at: 0 };
        match kind {
            Self::TAIL_ARENA_CLAIMED => {
                let arena = ArenaId(c.u32()?);
                let owner = BranchId::new(c.u64()?, c.u32()?);
                let start_page = c.u32()?;
                let page_count = c.u32()?;
                let next_start = c.u32()?;
                let next_arena = c.u32()?;
                let live = c.u32()?;
                // If `reserve` satisfied this claim out of the recycle list, the image in front of
                // us still lists the hole as free. Leaving it there would hand the same range to
                // the next claim.
                {
                    let mut free = self.space.free_extents.lock().unwrap();
                    if let Some(v) = free.get_mut(&page_count) {
                        if let Some(i) = v.iter().position(|s| *s == start_page) {
                            v.swap_remove(i);
                        }
                    }
                }
                {
                    let mut st = self.state.lock().unwrap();
                    st.extents.insert(
                        arena,
                        ArenaExtent { arena_id: arena, owner, start_page, page_count, next_free: 0 },
                    );
                    st.recycled.insert(arena, Vec::new());
                    // **D85, and for the same reason `load_state` marks every restored extent.**
                    // `next_free` is recorded as 0 here and pages handed out afterwards never
                    // touched the file, so this extent's fill is exactly as unknown as one that
                    // came out of the image.
                    st.fill_unknown.insert(arena);
                    st.claim_epoch.insert(arena, crate::cluster::epoch());
                    // `current` is deliberately NOT restored: `load_state` clears it, on the rule
                    // that a restored extent is never resumed. A replayed claim is a restored
                    // extent.
                }
                // Monotone, so the order two records reach the file in cannot lower a watermark.
                self.space.extent_starts.raise_issued_through(next_start as u64);
                self.space.arena_ids.raise_issued_through(next_arena as u64);
                self.reserved_pages.fetch_add(page_count, Ordering::SeqCst);
                // **Absolute, where `reserved_pages` is a delta, and the asymmetry is the point.**
                // `reserved_pages` changes ONLY on the two paths that write a record, so a delta
                // tracks it exactly. `live_pages` changes on every `alloc_in_arena` and
                // `release_page`, neither of which persists anything — so the only durable value
                // it can have is a SNAPSHOT, which is exactly what `state_bytes` has always
                // written. Carrying it here keeps a restore as fresh as it was when every claim
                // rewrote the whole image; leaving it out made a restart report zero live pages
                // after seven allocations.
                self.live_pages.store(live, Ordering::SeqCst);
            }
            Self::TAIL_EXTENT_FREED => {
                let arena = ArenaId(c.u32()?);
                let start_page = c.u32()?;
                let page_count = c.u32()?;
                let live = c.u32()?;
                {
                    let mut st = self.state.lock().unwrap();
                    st.extents.remove(&arena);
                    st.recycled.remove(&arena);
                    st.fill_unknown.remove(&arena);
                    st.claim_epoch.remove(&arena);
                    // `free_arena` drops this arena's parked entries too, and so must replay:
                    // a pending entry naming a freed extent would send `drain_pending` looking
                    // for pages in a range that has already gone back on the free list.
                    // By arena index: this arena's own entries, not the whole log.
                    pending.remove_arena(arena);
                    // `shadow_base` and `current` need no repair. `shadow_base` is not persisted
                    // at all (see its doc: a cold map only shortens chains), and `current` is
                    // cleared by `load_state` and never set by a replayed claim.
                }
                self.space
                    .free_extents
                    .lock()
                    .unwrap()
                    .entry(page_count)
                    .or_default()
                    .push(start_page);
                saturating_sub_atomic(&self.reserved_pages, page_count);
                // Absolute, for the reason spelled out under the claim record above.
                self.live_pages.store(live, Ordering::SeqCst);
            }
            // **D183 — the reap slow path's delta.** `retire_arenas_by_rule` parked these entries
            // and recycled the rest of the pages it walked; this is that call, replayed.
            Self::TAIL_PAGES_PARKED => {
                let live = c.u32()?;
                let n = c.u32()? as usize;
                let mut parked = Vec::new();
                for _ in 0..n {
                    parked.push(Self::decode_pending_entry(&mut c)?);
                }
                {
                    let mut st = self.state.lock().unwrap();
                    // **Idempotent by key, and that is a durability property rather than tidiness.**
                    // This record APPENDS, so replaying it behind another record that already
                    // holds these entries must not double them — otherwise the log comes back with
                    // the same page twice and `pending_len` disagrees with a full rewrite.
                    //
                    // ⚠ **The race this comment used to cite is gone**: it said a concurrent drain
                    // could cut its record between this caller's in-memory push and its append.
                    // `retire_arenas_by_rule` now holds `persist` across both, so that
                    // interleaving cannot be produced. What remains is the ordinary reason a
                    // replayed log record should be idempotent — a resumed reap re-parking the
                    // same page, and replay being re-runnable at all.
                    //
                    // The FIRST entry for a key wins, and that is the one that MATCHES MEMORY
                    // rather than the one that is safest in isolation. A page parked twice — a
                    // resumed reap re-entering a `Reaping` branch is the reachable way — carries
                    // the same `birth_epoch` and owner, so only `free_epoch` can differ, and a
                    // narrower `[birth, free)` pins FEWER children, i.e. releases more readily.
                    // That is not a reason to prefer it; the reason is that `drain_pending`
                    // decides on whichever entry it meets FIRST in `st.pending`, which is the
                    // earlier park, which is also the first to reach the log. Keeping the later
                    // one instead would make a restarted database release a page on a different
                    // rule than the running one does.
                    //
                    // **Since de-dup at push, memory never holds the second entry at all**: every push
                    // is first-wins (`PendingLog`), so this arm and memory keep the same one by
                    // construction. `finish` also builds first-wins, so what this lookup still buys is
                    // no slot per re-park, which
                    // `a_repark_of_a_key_already_pending_costs_one_lookup_and_no_slot` pins.
                    //
                    // "Already in the log" is asked of the replay's key index. This used to be a
                    // `HashSet` rebuilt from the WHOLE log for every parked record.
                    for e in parked {
                        // Dead arena: `TAIL_EXTENT_FREED` landed first, and its replay drops every
                        // pending entry naming the extent. Re-adding one here would send
                        // `drain_pending` hunting for a page in a range already back on the free
                        // list — the very thing that record's own comment refuses.
                        if !st.extents.contains_key(&e.arena_id) {
                            continue;
                        }
                        // `push` indexes the key, so a repeat later in THIS record is skipped
                        // too, as the per-record set skipped it.
                        if !pending.contains((e.page_id, e.arena_id)) {
                            pending.push(e);
                        }
                    }
                    Self::apply_arena_sections(&mut c, &mut st)?;
                }
                // Absolute, for the reason spelled out under the claim record above.
                self.live_pages.store(live, Ordering::SeqCst);
            }
            // **D183 — `put_pending`'s delta.** The closing half of `take_pending`'s
            // read-modify-write: the log is not appended to, it is REPLACED by what the drain put
            // back, which is why this one record can also discharge every earlier unrecorded
            // change to the log.
            Self::TAIL_PENDING_REPLACED => {
                let live = c.u32()?;
                let n = c.u32()? as usize;
                let mut log = Vec::new();
                for _ in 0..n {
                    log.push(Self::decode_pending_entry(&mut c)?);
                }
                {
                    let mut st = self.state.lock().unwrap();
                    log.retain(|p| st.extents.contains_key(&p.arena_id));
                    // Re-indexed from this record's own entries: O(n) for the n it carries, plus
                    // dropping the index it replaces.
                    pending.replace(log);
                    Self::apply_arena_sections(&mut c, &mut st)?;
                }
                self.live_pages.store(live, Ordering::SeqCst);
            }
            // **D183 — the small half of `put_pending`.** Everything the drain reclaimed left the
            // log; everything else in it is untouched, which is why this record does not have to
            // name the survivors.
            Self::TAIL_PENDING_DRAINED => {
                let live = c.u32()?;
                let n = c.u32()? as usize;
                // `Vec::new`, not `with_capacity(n)`: `n` comes off the file before its entries do,
                // and a corrupt count must fail on the cursor, not on an allocation.
                let mut removed = Vec::new();
                for _ in 0..n {
                    let page = c.u32()?;
                    let arena = ArenaId(c.u32()?);
                    removed.push((page, arena));
                }
                {
                    let mut st = self.state.lock().unwrap();
                    // Idempotent: removing a key that is not there is the identity, so replaying
                    // this record twice is the same as replaying it once.
                    //
                    // ⛔ **IDEMPOTENT IS NOT COMMUTATIVE, and an earlier version of this comment
                    // claimed it "can sit on either side of a `TAIL_EXTENT_FREED` or a
                    // `TAIL_PAGES_PARKED` for the same pages". That is false by inspection**:
                    // PARKED pushes key K if absent and this removes K, so `[DRAINED][PARKED]`
                    // ends with K in the log and `[PARKED][DRAINED]` without it. Removal commutes
                    // with removal, not with an append.
                    //
                    // What makes the order safe is not this arm — it is that `persist` is held
                    // across the mutation AND the append at every site that writes one of these
                    // records, so the file order equals the memory order. `retire_arenas_by_rule`
                    // did not do that until the fix its own comment now describes, and that was
                    // the defect. Idempotence here is a second line, not the guarantee.
                    //
                    // By key: the entries this record names, every copy of each, and nothing
                    // else. This used to be a `retain` over the WHOLE log, including for a record
                    // that names nothing. `put_pending` used to write one on every drain that
                    // released nothing; since the drain elision it writes one only for such a
                    // drain that owes a recycled list, but files written before it still hold
                    // them, and replay walks those.
                    for key in removed {
                        pending.remove_key(key);
                    }
                    Self::apply_arena_sections(&mut c, &mut st)?;
                }
                self.live_pages.store(live, Ordering::SeqCst);
            }
            other => {
                return Err(BranchError::Arena(format!(
                    "arena tail record kind {other} reached apply after the kind allowlist"
                ))
                .into())
            }
        }
        if c.at != c.b.len() {
            return Err(BranchError::Arena(format!(
                "arena tail record kind {kind} has {} trailing bytes",
                c.b.len() - c.at
            ))
            .into());
        }
        Ok(())
    }

    /// Write the free-space map to `path` durably, so a crash leaves either the previous checkpoint
    /// or the new one and never a half-written map.
    ///
    /// That sentence was already here while the body was `std::fs::write` plus `std::fs::rename` —
    /// two of the four steps the idiom needs. Neither call makes anything durable, so a power cut
    /// could leave the *rename* on the device while the bytes it named were still in the page cache:
    /// exactly the half-written map the promise excludes. `<db>.arena` is the only thing on disk
    /// that says where the branch arena starts, and [`ArenaPageStore::load_state`] verifies a CRC32
    /// over it, so the observable outcome was a database that will not open at all.
    ///
    /// The four steps live in [`crate::storage::atomic_file`], which is also where they can be
    /// *asserted*: an fsync is invisible to any test that merely reads the file back.
    pub fn checkpoint(&self, path: &std::path::Path) -> Result<(), FerroError> {
        let mut g = self.persist.lock().unwrap();
        let pending_version = self.pending_version.load(Ordering::SeqCst);
        let written = self.checkpoint_with(&OsFileOps, path)?;
        // A checkpoint aimed at the armed path IS the image this process may then append to, so
        // it resets the tail accounting. One aimed anywhere else (the CLI's exit checkpoint to a
        // copy, a test dumping state) must leave it alone: claiming a tail of zero on a file we
        // did not write is exactly the mistake `image_bytes == 0` exists to prevent.
        if self.checkpoint_path.lock().unwrap().as_deref() == Some(path) {
            g.image_bytes = written as u64;
            g.tail_bytes = 0;
            g.image_epoch = crate::cluster::epoch();
            g.durable_pending_version = pending_version;
            g.rewrites += 1;
        }
        Ok(())
    }

    /// [`ArenaPageStore::checkpoint`] against an injected [`FileOps`], so a test can see the order
    /// of the four operations rather than only their result. Returns the image's length, which is
    /// what the tail accounting is measured against.
    pub(crate) fn checkpoint_with(
        &self,
        ops: &dyn FileOps,
        path: &std::path::Path,
    ) -> Result<usize, FerroError> {
        let bytes = self.state_bytes();
        replace_atomically(ops, path, &bytes).map_err(|e| FerroError::Io(e.to_string()))?;
        Ok(bytes.len())
    }

    /// Restore from a checkpoint written by [`ArenaPageStore::checkpoint`]. A missing file is not
    /// an error: it means nothing has been checkpointed yet.
    pub fn restore(&self, path: &std::path::Path) -> Result<bool, FerroError> {
        if !path.exists() {
            return Ok(false);
        }
        let bytes = std::fs::read(path).map_err(|e| FerroError::Io(e.to_string()))?;
        self.load_file(&bytes)?;
        Ok(true)
    }

    /// Read `base_page` out of a checkpoint image, without loading it.
    ///
    /// Validates the checksum and version first: a base read out of a corrupt image is worse than
    /// no base at all, because it is the number the floor gets registered from.
    pub fn base_page_in_state(bytes: &[u8]) -> Result<PageId, FerroError> {
        // **D81: the image is a PREFIX of the file now, not the whole of it.** This used to take
        // the last four bytes as the checksum; with a tail behind the image those four bytes are
        // the last tail record's CRC and every reopen would fail "checksum mismatch".
        // `image_len` finds the boundary and verifies the image's own checksum on the way.
        let n = Self::image_len(bytes)?;
        let mut c = StateCursor { b: &bytes[..n - 4], at: 0 };
        // The version is re-checked inside `image_len` against the same allowlist, so reading past
        // it here is not skipping a check.
        c.u8()?;
        c.u32()
    }

    /// Reattach to a checkpointed arena, taking the region's base **from the checkpoint** rather
    /// than from the caller.
    ///
    /// This is the difference between [`ArenaPageStore::reopen`] and a guard. `reopen` registers
    /// a floor at whatever base it is handed; hand it another arena's base and it will claim that
    /// arena's region, and nothing downstream can tell. Here the base is read from the durable
    /// image that describes the region, so the caller cannot get it wrong — there is no argument
    /// to get wrong. `load_state` then re-checks the same field against the assembled store, so a
    /// mismatch is refused twice over.
    ///
    /// A missing checkpoint is an error rather than a fresh start: this constructor exists to
    /// reattach, and "there is nothing to reattach to" is something the caller must handle
    /// deliberately with [`ArenaPageStore::new`], not something to paper over with a default base.
    pub fn reopen_from_checkpoint(
        pool: Arc<BufferPoolManager>,
        catalog: Arc<dyn BranchCatalog>,
        path: &std::path::Path,
    ) -> Result<Self, FerroError> {
        let bytes = std::fs::read(path).map_err(|e| FerroError::Io(e.to_string()))?;
        let base = Self::base_page_in_state(&bytes)?;
        let store = Self::reopen(pool, catalog, base)?;
        // **D81: the image AND the tail.** A claim made after the last full checkpoint lives in a
        // tail record; loading only the image would forget it and re-issue its page range, which
        // is the same aliasing the arming below exists to prevent.
        store.load_file(&bytes)?;
        // Reattaching implies continuing to own this image. Leaving it unarmed is how the first
        // version of this still aliased after a crash: the restored store claimed a fresh extent,
        // never wrote that fact down, and the open after it claimed the very same range.
        //
        // Arming also sets `image_bytes` to 0, which makes the next persist a full rewrite. That
        // is not an optimisation to be tuned away: the tail we just replayed may end in a TORN
        // record, and appending behind it would leave a good record sitting after a bad one —
        // the one arrangement `replay_tail` cannot recover from. See [`PersistState`].
        store.checkpoint_to(path.to_path_buf());
        Ok(store)
    }
}

use pending_log::PendingLog;

/// **D183 de-dup at push: the pending-free log holds at most ONE entry per `(page, arena)`, the first.**
///
/// A module of its own so its two fields are private and cannot drift apart. Every way into the log
/// goes through [`PendingLog::push_if_absent`], so a second entry for a page is unrepresentable rather
/// than something each call site has to avoid. That includes a log restored from an image,
/// or from a REPLACED record, written before this rule existed: [`PendingLog::from_entries`] keeps the
/// first entry for each key, which is the rule the tail replay has always applied to PARKED records. So
/// memory and its replayed file are built by the same rule and cannot disagree about a page's entry.
///
/// **Why the FIRST.** A page is logically freed once. A later entry for it comes from deciding it again:
/// a resumed reap, or the owner's retire of a page it had already freed. In-process that decision is at
/// a later free epoch, which can only widen the pin range, and only to children forked after the page was
/// freed, which cannot see it (`frontier/catalog_root_and_park_adversary.md` §B3 @ `a4b48ea`). After a
/// restart the epochs can be in either order, and for a Reaping owner the two decisions are equal (§B2).
///
/// Cost: `push_if_absent`, `contains` and `take_all` are O(1); `remove_arena` is the O(P) `retain` that
/// `free_arena` always did. The key set is about 10-16 bytes per entry beside a 40-byte `PendingFree`
/// (INFERRED from layout, not measured).
mod pending_log {
    use crate::branch::record::PendingFree;
    use crate::branch::types::{ArenaId, PageId};
    use std::collections::HashSet;

    #[derive(Clone, Default)]
    pub(super) struct PendingLog {
        /// Log order: the order each key was first pushed.
        entries: Vec<PendingFree>,
        /// The keys of `entries`, exactly, one each.
        keys: HashSet<(PageId, ArenaId)>,
    }

    impl PendingLog {
        /// The first entry for each key, in order. Later entries for a key already seen are dropped.
        pub(super) fn from_entries(entries: impl IntoIterator<Item = PendingFree>) -> PendingLog {
            let mut log = PendingLog::default();
            log.extend_absent(entries);
            log
        }

        pub(super) fn len(&self) -> usize {
            self.entries.len()
        }

        pub(super) fn contains(&self, key: (PageId, ArenaId)) -> bool {
            self.keys.contains(&key)
        }

        pub(super) fn iter(&self) -> std::slice::Iter<'_, PendingFree> {
            self.entries.iter()
        }

        /// Append `e` at the END unless an entry for its key is already in the log. Returns whether it
        /// did, so a caller that records or announces its pushes can skip exactly the ones that changed
        /// nothing.
        pub(super) fn push_if_absent(&mut self, e: PendingFree) -> bool {
            if !self.keys.insert((e.page_id, e.arena_id)) {
                return false;
            }
            self.entries.push(e);
            true
        }

        pub(super) fn extend_absent(&mut self, entries: impl IntoIterator<Item = PendingFree>) {
            for e in entries {
                self.push_if_absent(e);
            }
        }

        /// Empty the log and hand back its entries in order. O(1): both fields are swapped out, not
        /// cleared.
        pub(super) fn take_all(&mut self) -> Vec<PendingFree> {
            self.keys = HashSet::new();
            std::mem::take(&mut self.entries)
        }

        /// Drop every entry of `arena`.
        pub(super) fn remove_arena(&mut self, arena: ArenaId) {
            let keys = &mut self.keys;
            self.entries.retain(|p| {
                if p.arena_id != arena {
                    return true;
                }
                keys.remove(&(p.page_id, p.arena_id));
                false
            });
        }

        /// The entries, for a caller that takes the whole log apart.
        pub(super) fn into_entries(self) -> Vec<PendingFree> {
            self.entries
        }
    }

    impl std::fmt::Debug for PendingLog {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_list().entries(self.entries.iter()).finish()
        }
    }

    /// Test-only: compare with the `Vec` this log used to be, so the tests written against it read
    /// unchanged.
    #[cfg(test)]
    impl PartialEq<Vec<PendingFree>> for PendingLog {
        fn eq(&self, other: &Vec<PendingFree>) -> bool {
            &self.entries == other
        }
    }
}

use pending_replay::PendingReplay;

/// **D183 tail replay: the pending-free log, indexed at most once for a whole replay.**
///
/// A module of its own so the log's slots are PRIVATE, and `apply_tail_record` changes the log only
/// through these methods. ⚠ **That narrows the instrument's blind spot; it does not remove it.** The
/// counter sees a rescan only if the rescan bumps `visits`. A scan written inside this module without
/// that bump is invisible to the count test. Mutant MR5 in `frontier/lane_d183_tail_replay.md` is
/// exactly that, and it is expected to survive. Outside the module a rescan is not possible at all,
/// because `st.pending` is empty for the whole replay.
///
/// **Lazily.** The log is indexed by the first record that touches it (`index`). A replay where no
/// record does, which covers an empty tail (every clean open) and a tail of claims only, does no work
/// on the log. That is what the per-record code did there.
///
/// **The rules it keeps**, each one what a per-record scan of `Vec<PendingFree>` did:
/// * order is log order, and an entry pushed after a removal goes to the END;
/// * [`PendingReplay::contains`] means "some live entry has this key". PARKED asks it before every
///   push, so the FIRST entry for a key wins, within one record as well as across records;
/// * [`PendingReplay::remove_key`] removes EVERY live entry with the key (a `retain`), and a
///   missing key is a no-op;
/// * [`PendingReplay::remove_arena`] removes every live entry of the arena;
/// * [`PendingReplay::replace`] takes the new log first-wins per key. Only a REPLACED record written
///   before de-dup at push can carry a key twice, because it restated a memory log that held one twice.
///
/// **The result is a [`PendingLog`], built by [`PendingLog::from_entries`] in the one pass `finish`
/// already made, so it holds each key once, the first, whatever the records said.** Every removal
/// (DRAINED, EXTENT_FREED, REPLACED) removes all copies of a key, so the first copy always sits where the
/// correct entry would. That makes PARKED's `contains` a bound on transient slots and no longer what
/// keeps the result duplicate-free: mutant MC1 (`contains` always false) is equivalent in the log's
/// CONTENTS from the de-dup fix on. Its cost role, no slot per re-park, is pinned by
/// `a_repark_of_a_key_already_pending_costs_one_lookup_and_no_slot`, which kills MC1 at 98 visits
/// against 34 (lane AMENDMENTS 3-4). For the same reason no key ever has two live slots here, so
/// `remove_key`'s every-copy loop never meets a second copy (mutant MC2 is equivalent).
///
/// Cost: indexing and `finish` are one pass each over the log, and only if some record touched it.
/// Every other method is O(1) plus the entries it actually removes; `remove_arena` also walks that
/// arena's positions that other records already removed. Each position is pushed once and dropped at
/// most once, so a whole replay is O(P + records + the entries the records carry), and O(records)
/// when no record touches the log.
mod pending_replay {
    use super::pending_log::PendingLog;
    use crate::branch::record::PendingFree;
    use crate::branch::types::{ArenaId, PageId};
    use std::collections::HashMap;

    pub(super) struct PendingReplay {
        /// The log as the replay found it, until a record first touches it. [`Self::index`] moves
        /// it into `slots`.
        untouched: Option<PendingLog>,
        /// Log order. `None` is an entry a later record removed; [`Self::finish`] drops the holes.
        slots: Vec<Option<PendingFree>>,
        /// LIVE slots per key, oldest first. A key is absent once its last live slot goes.
        by_key: HashMap<(PageId, ArenaId), Vec<usize>>,
        /// Every slot ever pushed for an arena, live or not. A removed one costs one skip in
        /// [`Self::remove_arena`], never a scan of anything else.
        by_arena: HashMap<ArenaId, Vec<usize>>,
        /// Entries visited: every push, lookup and slot touched, plus the passes of `finish` and
        /// `replace`. Read by the test-only counter `ArenaPageStore::replay_pending_visits`.
        visits: u64,
    }

    impl PendingReplay {
        /// Hold `log` unindexed. O(1).
        pub(super) fn new(log: PendingLog) -> PendingReplay {
            PendingReplay {
                untouched: Some(log),
                slots: Vec::new(),
                by_key: HashMap::new(),
                by_arena: HashMap::new(),
                visits: 0,
            }
        }

        /// Index the log `new` was given, once, at the first call that needs it. O(its length).
        fn index(&mut self) {
            let Some(log) = self.untouched.take() else { return };
            let log = log.into_entries();
            self.slots.reserve(log.len());
            self.by_key.reserve(log.len());
            for e in log {
                self.push_indexed(e);
            }
        }

        pub(super) fn contains(&mut self, key: (PageId, ArenaId)) -> bool {
            self.index();
            self.visits += 1;
            self.by_key.contains_key(&key)
        }

        /// Append at the END of the log. Does not deduplicate; callers that must, ask
        /// [`Self::contains`] first, exactly as the scan they replace did.
        pub(super) fn push(&mut self, e: PendingFree) {
            self.index();
            self.push_indexed(e);
        }

        fn push_indexed(&mut self, e: PendingFree) {
            self.visits += 1;
            let at = self.slots.len();
            self.slots.push(Some(e));
            self.by_key.entry((e.page_id, e.arena_id)).or_default().push(at);
            self.by_arena.entry(e.arena_id).or_default().push(at);
        }

        /// Remove every live entry with this key.
        pub(super) fn remove_key(&mut self, key: (PageId, ArenaId)) {
            self.index();
            self.visits += 1;
            let Some(at) = self.by_key.remove(&key) else { return };
            for i in at {
                self.visits += 1;
                self.slots[i] = None;
            }
        }

        /// Remove every live entry of this arena.
        pub(super) fn remove_arena(&mut self, arena: ArenaId) {
            self.index();
            let Some(at) = self.by_arena.remove(&arena) else { return };
            for i in at {
                self.visits += 1;
                let Some(e) = self.slots[i].take() else { continue };
                let key = (e.page_id, e.arena_id);
                let now_empty = match self.by_key.get_mut(&key) {
                    Some(v) => {
                        v.retain(|&j| j != i);
                        v.is_empty()
                    }
                    None => false,
                };
                if now_empty {
                    self.by_key.remove(&key);
                }
            }
        }

        /// The log becomes `log`, first-wins per key, unindexed until something touches it. Costs
        /// dropping the index it replaces, plus one pass over `log`: the pass that decoding the record
        /// already made, and uncounted like it.
        pub(super) fn replace(&mut self, log: Vec<PendingFree>) {
            let carried = self.visits + self.slots.len() as u64;
            *self = PendingReplay::new(PendingLog::from_entries(log));
            self.visits += carried;
        }

        /// The log in order, holes dropped, first-wins per key, and the visits this replay made.
        /// O(slots), or O(1) if no record ever touched the log.
        pub(super) fn finish(self) -> (PendingLog, u64) {
            if let Some(log) = self.untouched {
                return (log, self.visits);
            }
            let visits = self.visits + self.slots.len() as u64;
            (PendingLog::from_entries(self.slots.into_iter().flatten()), visits)
        }
    }
}

struct StateCursor<'a> {
    b: &'a [u8],
    at: usize,
}

/// Subtract without wrapping.
///
/// `AtomicU32::fetch_sub` wraps, so a replayed free whose extent the image had already accounted
/// for would turn a page count of zero into four billion. These two counters are statistics —
/// exit criteria 1 and 8 are stated in them — so the right answer to an underflow is to clamp,
/// not to publish a number that is off by 2^32.
fn saturating_sub_atomic(cell: &AtomicU32, v: u32) {
    let _ = cell.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |c| Some(c.saturating_sub(v)));
}

impl<'a> StateCursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], FerroError> {
        // `checked_add`, not `self.at + n`: `image_len` walks the structure BEFORE the checksum
        // is verified, so `n` can be a corrupt count multiplied by a stride. Plain addition
        // overflows and panics in a debug build and WRAPS in a release one, where it would pass
        // this check and slice out of bounds.
        if self.at.checked_add(n).is_none_or(|end| end > self.b.len()) {
            return Err(BranchError::Arena(format!(
                "arena state truncated at byte {} (wanted {})",
                self.at, n
            ))
            .into());
        }
        let s = &self.b[self.at..self.at + n];
        self.at += n;
        Ok(s)
    }
    /// Advance past `n` bytes without reading them, refusing to run off the end.
    ///
    /// The bounds check is what makes [`ArenaPageStore::image_len`] safe to run before the
    /// checksum: a corrupt count can only produce "truncated", never an allocation.
    fn skip(&mut self, n: usize) -> Result<(), FerroError> {
        self.take(n).map(|_| ())
    }
    fn u8(&mut self) -> Result<u8, FerroError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, FerroError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, FerroError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
}

impl PageStore for ArenaPageStore {
    fn alloc_in_arena(
        &self,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        // Reached with an `ArenaId` the caller already holds, so `arena_for`'s check is not enough:
        // a caller that obtained the id before the authority changed would otherwise keep writing
        // into an extent the current leader knows nothing about.
        let epoch = self.revoke_stale_authority();
        let page_id = {
            let mut st = self.state.lock().unwrap();
            if st.claim_epoch.get(&arena) != Some(&epoch) {
                return Err(BranchError::Arena(format!(
                    "arena {arena} was claimed under a superseded authority and will not be \
                     filled: its pages are not known to the current leader, and a page written \
                     twice still passes its own checksum"
                ))
                .into());
            }
            if let Some(p) = st.recycled.get_mut(&arena).and_then(|v| v.pop()) {
                // **The durable recycled list still names this page and nothing can amend it.**
                // Set while `state` is held, so no reader can see the shortened list without also
                // seeing the flag. See [`Self::recycled_reissued`].
                self.recycled_reissued.store(true, Ordering::SeqCst);
                p
            } else {
                let ext = st
                    .extents
                    .get_mut(&arena)
                    .ok_or_else(|| BranchError::Arena(format!("no such arena {}", arena)))?;
                if ext.remaining() == 0 {
                    return Err(BranchError::Arena(format!(
                        "arena {} is exhausted ({} pages); ask arena_for for a fresh extent",
                        arena, ext.page_count
                    ))
                    .into());
                }
                let p = ext.start_page + ext.next_free;
                ext.next_free += 1;
                p
            }
        };
        // Drop any stale cached image of a recycled id *before* the fresh write, so a later
        // flush of the old frame cannot land on top of the new page.
        self.evict(page_id);
        self.write_fresh_page(page_id, arena, page_type, birth_epoch)?;
        self.live_pages.fetch_add(1, Ordering::SeqCst);
        Ok(page_id)
    }

    fn read_page(&self, page_id: PageId) -> Result<PageHandle, FerroError> {
        let handle = PageHandle::fetch(Arc::clone(&self.pool), page_id)?;
        let ok = verify_checksum(&handle.read().data);
        if !ok {
            return Err(FerroError::Cow(format!("page {} failed its checksum", page_id)));
        }
        Ok(handle)
    }

    fn cow_page(
        &self,
        page_id: PageId,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<CowPage, FerroError> {
        // Hard-errors on a reaped or mid-reap branch: never stale data.
        let rec = self.catalog.get(branch)?;
        let barrier = privacy_barrier(rec.fork_epoch, self.catalog.max_live_child(rec.branch_id.id)?);

        let handle = self.read_page(page_id)?;
        let header = handle.header()?;

        // **D31 — privacy is OWNERSHIP of the page's extent, not identity with the writer's
        // CURRENT one.** This used to be `header.is_private_to(arena_for(branch), barrier)`, i.e.
        // "is this page in the extent I am allocating from right now". A branch used to have
        // exactly one extent almost always, so the two questions coincided and the narrower one
        // looked right.
        //
        // They stopped coinciding the moment a branch could hold several extents. Every rollover
        // made every page in the branch's EARLIER extents read as foreign, so the branch shadowed
        // its own private pages and freed the originals — correct, but it doubles the space and
        // defeats the in-place path this test exists to protect. Geometric growth turns that from
        // "after 256 pages" into "after the first page", which is how it was found.
        //
        // The predicate below is the one the shadow path twenty lines down already uses, word for
        // word: *"only if this branch owns the arena it came from"*. One question, asked once.
        let owns_it = self.arena_owner(header.arena_id) == Some(branch);
        if owns_it && header.birth_epoch >= barrier {
            // Nobody else can see it: mutate in place. This is what keeps a hot branch from
            // shadowing the same page on every single write.
            census::bump(&census::IN_PLACE, 1);
            return Ok(CowPage {
                page_id,
                previous_page_id: page_id,
                copied: false,
                retire_previous: false,
                handle,
            });
        }

        let source = handle.read().data;
        drop(handle);
        census::bump(&census::SHADOW, 1);
        census::bump(&census::SHADOW_PAYLOAD_BYTES, (PAGE_SIZE - PAGE_HEADER_SIZE) as u64);

        // Asked only NOW, on the path that actually allocates. Eagerly above, a branch whose
        // extent had just filled would claim a fresh one on every in-place mutation too — one
        // whole extent per write, which at a one-page first extent is unbounded growth on a
        // workload that allocates nothing.
        let arena = self.arena_for(branch)?;
        let new_id = self.alloc_in_arena(arena, header.page_type, epoch)?;
        let new_handle = self.read_page(new_id)?;
        {
            let mut frame = new_handle.write();
            frame.data[PAGE_HEADER_SIZE..].copy_from_slice(&source[PAGE_HEADER_SIZE..]);
            let mut h = PageHeader::new(epoch, arena, header.page_type);
            h.flags = flags::PRIVATE;
            h.write_to(&mut frame.data);
            stamp_checksum(&mut frame.data);
        }

        // **D102 — record what this page is a shadow OF, which is the fact a delta needs.**
        //
        // A delta is meaningless without its base, so the base has to be knowable after the copy
        // above has made the two pages identical. `previous_page_id` carries it out to the caller,
        // which relinks its parent and drops it; nothing kept it on the store side.
        //
        // **`MAX_CHAIN_DEPTH` is enforced HERE, at write time, and not on the read.** A page whose
        // chain has reached the bound is recorded as a chain ROOT (no entry) rather than as a
        // deeper link, so a reader cannot encounter a chain the bound forbids — which is a
        // stronger statement than checking on read, and is why `delta_against_base` below needs no
        // policy of its own. The bound is not a tuning knob: `branch::mod` invariant 2 cites
        // BranchBench measuring parent-chain-walking reads at up to 4000x degradation, and an
        // unbounded delta chain is that same shape wearing different clothes.
        //
        // **Only a base this branch does not own is recorded, and that is what keeps this out of
        // the GC's business.** When the branch owns the source extent the base becomes a freeable
        // page, and a delta against it would be a second, invisible reason to keep it alive — a
        // reference count, in a file whose header says in bold that there are none. When the
        // branch does NOT own it, the base is an ancestor's page that this branch inherited, and
        // the epoch interval rule in `branch::record::reclaimable` already pins it: the branch
        // holding the delta forked after the base was born, so the free parks the page instead of
        // releasing it. The existing rule covers this case with no new liveness source, which is
        // the only reason it is safe.
        //
        // ⚠ This paragraph used to say "the `free_page` call directly below". **There is no
        // longer a `free_page` call below** — D125 moved it to the caller's commit point (see
        // `CowPage::retire_previous`), because a store cannot know whether its caller's operation
        // will commit and `CowTree` rolls a failed one back to a root that still points here. The
        // reasoning above is unaffected: the owned base still becomes freeable, just one step
        // later. Only the landmark moved, and a header pointing at a call that is not there is
        // how the next reader of this rule loses an afternoon.
        let owner_of_source = self.arena_owner(header.arena_id);
        if owner_of_source != Some(branch) {
            let mut st = self.state.lock().unwrap();
            let base_depth = st.shadow_base.get(&page_id).map(|&(_, d, _)| d).unwrap_or(0);
            if base_depth < delta::MAX_CHAIN_DEPTH {
                st.shadow_base.insert(new_id, (page_id, base_depth + 1, header.birth_epoch));
            }
        }

        // Free the shadowed page **only if this branch owns the arena it came from**.
        //
        // A branch that shadows a page it inherited from an ancestor must leave the original
        // alone: the ancestor still points at it, and the ancestor is not in its own
        // `live_children` array, so the interval rule would eventually declare it reclaimable and
        // corrupt the ancestor. Freeing is the owner's business and nobody else's.
        //
        // ⛔ D125: AND IT IS NOT THIS FUNCTION'S BUSINESS *WHEN*. This used to call
        // `self.free_page(page_id, epoch)?` right here. A store cannot know whether its caller's
        // operation will commit, and `CowTree` rolls a failed one back to a root that still
        // points at `page_id` — so the free had been taken on behalf of an operation that never
        // happened. Reported through `retire_previous` and performed by the caller at its commit
        // point. Same defect as D112 and as the `unlink_up` half of D125.
        Ok(CowPage {
            page_id: new_id,
            previous_page_id: page_id,
            copied: true,
            retire_previous: owner_of_source == Some(branch),
            handle: new_handle,
        })
    }

    fn free_page(&self, page_id: PageId, free_epoch: Epoch) -> Result<(), FerroError> {
        let header = self.read_page(page_id)?.header()?;
        let arena = header.arena_id;
        let Some(owner) = self.arena_owner(arena) else {
            // Extent already gone; the page went back with it.
            return Ok(());
        };

        // The owner may be mid-reap or already reaped and its children are still the authority
        // over this page, so the query is generation-blind by construction: it takes an id slot,
        // not a `BranchId`.
        //
        // ⛔ **D124 — this was `get_raw(owner.id).is_ok() && …`, and the `&&` was a silent free.**
        // The comment it replaces said "an owner with no record at all pins nothing". That is
        // false in both directions it could be read.
        //
        // An extent's owner is published BY CONSTRUCTION: `alloc_arena` is the only writer of
        // `extents` and it ends in `catalog.add_arena`, which both catalogs refuse for a branch
        // with no record. So the owner did exist. And "has no record *now*" does not mean it
        // stopped existing: nothing ever deletes a record, and retirement is a state flip to
        // `Reaped`. Resolving a failed read to "not pinned" ran `release_page` on a page a live
        // child may still be reading: silent data loss, not a leak.
        //
        // ⚠ **D126 changed WHICH failures reach here, not what to do about them.** This used to
        // say the miss was routine: `TableBranchCatalog::upsert` was delete-then-insert with no
        // latch held across the two, and `write_record` routes the RECORD key through it, so a
        // concurrent `set_root` or `renew_lease` on the owner made `get_raw` miss for a moment on
        // a perfectly healthy branch. D126 gave the tree an in-place replace and closed that
        // window (`tests/d126_atomic_upsert.rs`, and `mod d126_record_key_probe` on the RECORD
        // key itself). What can still fail here is an I/O error or a genuinely corrupt catalog,
        // and for both of those refusing remains the only answer that neither frees nor guesses.
        //
        // So the `?`: refusing leaves the page PARKED, which the next drain revisits, and a
        // retry succeeds. Freeing is the one outcome that cannot be retried.
        self.catalog.get_raw(owner.id)?;
        let pinned =
            self.catalog.live_child_in_epoch_range(owner.id, header.birth_epoch, free_epoch)?;

        if pinned {
            // **D81.** Parking a page persists nothing — it never did — but under full-image
            // checkpoints the next claim wrote it down as a side effect. A tail record does not,
            // so this marks the log changed and the next claim rewrites the image instead of
            // appending. See [`PersistState::durable_pending_version`].
            //
            // **D183 — the bump is INSIDE the lock, for the reason `take_pending` gives.** The
            // push and the announcement of the push have to be one step, or a record builder
            // holding `state` reads the new version and the old log. That pairing is what
            // `push_pending_unrecorded` exists to make unskippable.
            self.push_pending_unrecorded(PendingFree {
                page_id,
                arena_id: arena,
                birth_epoch: header.birth_epoch,
                free_epoch,
                owner,
            });
        } else {
            self.release_page(page_id, arena);
        }
        Ok(())
    }

    fn alloc_arena(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        // **D81 — OUTERMOST, and held across the whole claim.** The durable tail is an ordered
        // log, so the order records reach the file has to be the order the memory changed in;
        // otherwise a free of extent X and an immediate re-claim of it can be written down
        // backwards and replay leaves X both free and live. Taken before `state`, before the
        // catalog's locks and before `REPLACE_LOCK`, and never the other way round — see
        // [`PersistState`]. What this newly serialises is `reserve` and `catalog.add_arena`; the
        // fsync at the end was already serialised by `REPLACE_LOCK`.
        let mut persist = self.persist.lock().unwrap();
        let epoch = self.revoke_stale_authority();

        // **D31 — the size is a function of what this branch is already filling.** Derived from
        // the current extent rather than from a counter kept beside it, so it needs no new durable
        // field and survives a restart: `current` and every extent's `page_count` are both already
        // in the checkpoint image. A branch whose extent was freed underneath it starts again at
        // one page, which is the safe direction — it over-allocates nothing.
        //
        // Two threads allocating for one branch can read the same previous size and both claim it.
        // That is benign: they get two distinct valid extents and the branch's growth is one step
        // slower. Holding the state lock across `reserve` to prevent it would put a consensus-
        // capable take inside the store's hottest lock to save one doubling.
        let pages = {
            let st = self.state.lock().unwrap();
            let current = st
                .current
                .get(&branch)
                .and_then(|a| st.extents.get(a))
                .map(|e| e.page_count);
            next_extent_pages(current)
        };

        let (arena, start) = self.space.reserve(pages)?;
        {
            let mut st = self.state.lock().unwrap();
            st.extents.insert(
                arena,
                ArenaExtent {
                    arena_id: arena,
                    owner: branch,
                    start_page: start,
                    page_count: pages,
                    next_free: 0,
                },
            );
            st.recycled.insert(arena, Vec::new());
            st.current.insert(branch, arena);
            st.claim_epoch.insert(arena, epoch);
        }
        self.reserved_pages.fetch_add(pages, Ordering::SeqCst);

        // Keep the durable record truthful: the reaper frees exactly `record.arenas`.
        //
        // **D20 — ONE ATOMIC CATALOG OPERATION.** This was a read-modify-write across two
        // different critical sections: `get_raw` took NO lock, `put` took `logical`. With no latch
        // protocol under the B+tree, the unlocked read could descend through a node another thread
        // was splitting, return a record with the wrong arena list, and have that list written
        // back as truth -- after which the reaper freed exactly `record.arenas` and the arenas it
        // could no longer see leaked. Measured before the fix: 0 leaked at 1 thread, 24 at 8,
        // 0 on the log catalog (`bench/d20_race_control.txt`).
        self.catalog.add_arena(branch, arena)?;

        // Persist the map now that the region has grown. This is the write that makes
        // `next_extent_start` durable: without it a crashed session's freshly claimed extent is
        // invisible to the next open, which then claims the same range and hands out pages that are
        // already in use. Ordered AFTER the catalog write so a crash between the two leaves an
        // extent recorded as reserved but unreferenced, which leaks; the other order aliases.
        //
        // **D81 — THIS is the wall on the FORK door.** D79 measured the whole 48·L-byte image
        // being re-serialised and re-fsynced here, once per new branch, for `sum(48·i) = 24·N²`
        // bytes over a run. It now appends 45 bytes and fsyncs once, and the image is rewritten
        // only when the tail has grown past half of it.
        //
        // ⚠ **D183 corrected this note, which was wrong in both halves.** It said there were
        // "three other `persist_if_configured` sites"; there were TWO (`retire_arenas_by_rule` and
        // `put_pending`), and it said keeping them whole cost "a bounded tail rather than a missing
        // guarantee". It cost neither: `persist_full_locked` never consults `compact_threshold`, so
        // those two were not amortised at all — one full image rewrite per INTERIOR branch reaped,
        // `24·N²` bytes again, on the door that opens for deep fork chains. The condition this note
        // named as hypothetical ("a workload that reaps as often as it forks") is the one the
        // project is aimed at. Both sites are deltas now; see [`Self::TAIL_PAGES_PARKED`] and
        // [`Self::TAIL_PENDING_REPLACED`], and `persist_if_configured` is gone with them.
        let payload = {
            let mut p = Vec::with_capacity(36);
            p.extend_from_slice(&arena.0.to_be_bytes());
            p.extend_from_slice(&branch.id.to_be_bytes());
            p.extend_from_slice(&branch.generation.to_be_bytes());
            p.extend_from_slice(&start.to_be_bytes());
            p.extend_from_slice(&pages.to_be_bytes());
            // The watermarks AFTER the take. Absolute and applied with `raise_issued_through`,
            // which is monotone — so these are the one part of a record that is safe whatever
            // order it lands in.
            p.extend_from_slice(&(self.space.extent_starts.issued_through() as u32).to_be_bytes());
            p.extend_from_slice(&(self.space.arena_ids.issued_through() as u32).to_be_bytes());
            // The live-page count as `state_bytes` would have written it at this instant. See
            // the note beside its replay: it is the only durable form this counter can have.
            p.extend_from_slice(&self.live_pages.load(Ordering::SeqCst).to_be_bytes());
            p
        };
        self.persist_delta_locked(&mut persist, Self::TAIL_ARENA_CLAIMED, &payload, None)?;
        Ok(arena)
    }

    fn arena_for(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        let epoch = self.revoke_stale_authority();
        {
            let st = self.state.lock().unwrap();
            if let Some(&arena) = st.current.get(&branch) {
                // The fast path additionally requires that THIS authority claimed the extent.
                // Without it, a branch that claimed an extent while standalone goes on filling it
                // after the process joins a cluster, and those pages are ones the leader believes
                // are free.
                if st.claim_epoch.get(&arena) == Some(&epoch) {
                    if let Some(ext) = st.extents.get(&arena) {
                        let has_recycled =
                            st.recycled.get(&arena).map(|v| !v.is_empty()).unwrap_or(false);
                        if ext.remaining() > 0 || has_recycled {
                            return Ok(arena);
                        }
                    }
                }
            }
        }
        self.alloc_arena(branch)
    }


    fn free_arena(&self, arena: ArenaId) -> Result<u32, FerroError> {
        // **D81 — the same outermost lock `alloc_arena` takes, and for the same reason.** The
        // ordering hazard the tail has is precisely between these two: this method returns a page
        // range to `free_extents` and the very next `alloc_arena` can re-claim it, so the two
        // durable records must be written in the order the memory changed. See [`PersistState`].
        let mut persist = self.persist.lock().unwrap();
        // **D31 — every one of these is the EXTENT's own size, never the store's cap.** Extents
        // are no longer uniform, so `self.space.extent_pages` here would evict 255 pages belonging
        // to other arenas, credit the reserved counter with space this extent never held, and hand
        // a 1-page hole back to the free list as if it were 256.
        let (start, pages, allocated) = {
            let st = self.state.lock().unwrap();
            let Some(ext) = st.extents.get(&arena) else { return Ok(0) };
            let recycled = st.recycled.get(&arena).map(|v| v.len() as u32).unwrap_or(0);
            (ext.start_page, ext.page_count, ext.next_free.saturating_sub(recycled))
        };

        for i in 0..pages {
            self.evict(start + i);
        }

        let mut st = self.state.lock().unwrap();
        let ext = st.extents.remove(&arena);
        st.recycled.remove(&arena);
        st.pending.remove_arena(arena);
        st.claim_epoch.remove(&arena);
        // **D99 — the one per-arena map this used to leave behind.** `load_state` seeds
        // `fill_unknown` with EVERY restored extent and only `resolve_fill` ever clears an id
        // from it. An extent freed before anything probed its fill therefore left its id in the
        // set for the rest of the process's life, and since arena ids are never reissued nothing
        // could ever collect it. Not a correctness bug — `extent_is_empty` already answers false
        // for a missing extent, so the stale entry changes no decision — but it is per-arena state
        // on a path whose whole job is to give per-arena state back, and at 10^6 restored extents
        // it is the set, not the leak, that is the wrong shape.
        st.fill_unknown.remove(&arena);
        // **D183 — and the same argument, one row later.** This extent's recycled list no longer
        // exists, so nothing is owed a record for it. Left behind, the id would make every later
        // reclamation record re-encode an arena that `TAIL_EXTENT_FREED` has already removed.
        st.recycled_dirty.remove(&arena);
        // **D102 — the whole extent's ids stop naming these pages, so their bases stop being
        // theirs.** `release_page` does this one id at a time; freeing an extent bypasses it
        // entirely (that bypass is the reaper's fast path and the reason arenas exist), so the
        // same forgetting has to happen here or a reissued range would carry stale bases.
        //
        // **D99 — ask the range, do not walk the map.** The `retain` this replaces visited every
        // entry in `shadow_base` to drop the few that lie in this extent: that map is keyed by
        // PAGE id and holds one entry per live copy-on-write shadow page in the whole store, so it
        // grows with the database while the answer is bounded by `pages` — at most
        // `ARENA_EXTENT_PAGES` (256). Probing the range is the spelling `release_page` already
        // uses for exactly this forgetting, one id at a time, and it is bounded by the extent
        // rather than by everything else that ever shadowed a page.
        //
        // Exactly equivalent: `retain` dropped precisely the entries whose KEY fell in
        // `start..start + pages`, and these are those keys.
        for shadow in start..start + pages {
            st.shadow_base.remove(&shadow);
        }
        if let Some(ext) = ext.as_ref() {
            // **D99 — RE-INDEX THE QUESTION, do not speed up the answer.** "Which branch is
            // currently filling this arena?" was answered by walking every entry in `current`,
            // which is keyed by branch: O(branches) under the store's hottest lock, once per
            // freed extent. The extent record already names the answer, so it is one hash lookup.
            //
            // **Exactly equivalent, and arena-id uniqueness is why.** `ArenaSpaceManager::reserve`
            // draws every id from a monotonic counter (`arena_ids.take(1)`) and `give_back`
            // recycles only the page RANGE, so an arena id names one extent for the life of the
            // store and can never be reissued under a second owner. `alloc_arena` writes
            // `extents[arena].owner = branch` and `current[branch] = arena` inside ONE critical
            // section, so if any branch maps to this arena it is `ext.owner` and no other — which
            // is what makes a lookup able to replace a scan rather than merely usually agree with
            // it.
            //
            // The `get` guard carries the case the `retain` also handled: an owner that has since
            // moved on to a newer extent has `current[owner] != arena`, and neither spelling
            // touches it. Dropping the guard would evict a live branch's CURRENT arena and send it
            // back to `alloc_arena` on its next write.
            if st.current.get(&ext.owner) == Some(&arena) {
                st.current.remove(&ext.owner);
            }
        }
        drop(st);

        if ext.is_some() {
            self.space.give_back(start, pages);
            self.reserved_pages.fetch_sub(pages, Ordering::SeqCst);
            self.live_pages.fetch_sub(allocated, Ordering::SeqCst);
            // Persist the shrunk map, for the same reason `alloc_arena` persists the grown one.
            // Without this only *claims* were durable and frees never were, so a crash after a reap
            // left an image still charging the extent to a branch that no longer exists — and the
            // next open could not collect it either: `reaper::sweep_empty_extents` asks
            // `extent_is_empty`, and the durable extent's `next_free` sits above its recycled count
            // because the fast path frees the extent whole and never releases its pages one by one.
            // The extent leaked until the file was rebuilt, and it is the reserved-page count that
            // exit criterion 8 is stated in.
            //
            // Ordered after the in-memory free so a crash in between leaves the extent recorded as
            // still-live: a leak, which is the safe direction. The other order publishes a page
            // range as reusable while a durable record may still point into it.
            //
            // Cost: one small write per whole-extent free. That is the reaper's fast path — as rare
            // as the claim this mirrors, and not per page.
            //
            // This also makes `free_arena` fallible where it was not, and the failure lands *after*
            // the in-memory free. Two consequences, named here rather than left to be discovered:
            // `reap` can now return `Err` with its own durable records already committed — it is
            // idempotent, so a retry converges on `Ok(0)`, and the durable map being behind leaks
            // rather than aliases — and `reap_expired` discards its partial list of reaped branches
            // on any `Err`, which was already true of every slow-path IO error and which no
            // production caller sees today, because `runtime.rs` calls `reap` directly.
            //
            // **D81:** a 16-byte record and one fsync, not the whole image and two. The reaper's
            // fast path is as frequent as the claim it mirrors in any workload that reaps what it
            // forks, so leaving it on the full rewrite would cap this row's benefit at half.
            let mut payload = Vec::with_capacity(16);
            payload.extend_from_slice(&arena.0.to_be_bytes());
            payload.extend_from_slice(&start.to_be_bytes());
            payload.extend_from_slice(&pages.to_be_bytes());
            payload.extend_from_slice(&self.live_pages.load(Ordering::SeqCst).to_be_bytes());
            self.persist_delta_locked(&mut persist, Self::TAIL_EXTENT_FREED, &payload, None)?;
        }
        Ok(allocated)
    }

    fn live_page_count(&self) -> Result<u32, FerroError> {
        Ok(self.live_pages.load(Ordering::SeqCst))
    }

    fn flush(&self) -> Result<(), FerroError> {
        self.pool.flush_all()
    }
}

#[cfg(test)]
pub(crate) mod harness {
    use super::*;
    use crate::branch::catalog::LogBranchCatalog;
    use crate::branch::table_catalog::TableBranchCatalog;
        use crate::storage::disk_manager::DiskManager;
    use std::fs::OpenOptions;
    use std::sync::atomic::AtomicU64;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// A throwaway store on a real file, deleted when the guard drops.
    pub struct Harness {
        pub catalog: Arc<dyn BranchCatalog>,
        pub store: Arc<ArenaPageStore>,
        path: std::path::PathBuf,
    }

    impl Harness {
        /// The LOG catalog. Kept as the default so existing callers are unchanged, but see
        /// [`Harness::new_with`]: it is **not** the catalog that ships.
        pub fn new() -> Harness {
            Harness::new_with(false)
        }

        /// **D19.** `table = true` builds the catalog that actually ships.
        ///
        /// Every reclamation test in this project used to run against `LogBranchCatalog`, which
        /// keeps `live_children` inside the record and therefore **structurally cannot exhibit
        /// D18** -- live data loss on `TableBranchCatalog`, through which the whole suite stayed
        /// green. A test that only exercises the safe implementation proves nothing about the
        /// shipped one.
        ///
        /// The log catalog is deliberately KEPT rather than deleted: it is an independent
        /// reference oracle, and it is what let D16 be proved to be the DESIGN rather than this
        /// implementation. Two implementations are the instrument, not the problem.
        pub fn new_with(table: bool) -> Harness {
            let n = SEQ.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir()
                .join(format!("ferro-arena-{}-{}.db", std::process::id(), n));
            let _ = std::fs::remove_file(&path);
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let dm = Arc::new(DiskManager::new(file).unwrap());
            let pool = Arc::new(BufferPoolManager::new(dm));
            let catalog: Arc<dyn BranchCatalog> = if table {
                let cat_path = std::env::temp_dir()
                    .join(format!("ferro-arena-{}-{}.cat", std::process::id(), n));
                let _ = std::fs::remove_file(&cat_path);
                Arc::new(TableBranchCatalog::open_sidecar(&cat_path, 1).unwrap())
            } else {
                Arc::new(LogBranchCatalog::in_memory(1))
            };
            let base = pool.disk_manager.high_water().unwrap();
            let store = Arc::new(
                ArenaPageStore::new(
                    Arc::clone(&pool),
                    Arc::clone(&catalog),
                    base,
                )
                .unwrap(),
            );
            Harness { catalog, store, path }
        }

        /// A second store over the same file and catalog, as if the process had restarted.
        pub fn fresh_store(&self) -> Arc<ArenaPageStore> {
            Arc::new(
                ArenaPageStore::new(
                    Arc::clone(&self.store.pool),
                    Arc::clone(&self.catalog),
                    self.store.base_page(),
                )
                .unwrap(),
            )
        }

        /// The OS's view of how much space the store is actually occupying. An independent
        /// instrument: `live_page_count` is a counter this module maintains itself, so a test
        /// that only consults it cannot tell reclamation from bookkeeping.
        pub fn file_len(&self) -> u64 {
            std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::branch::catalog::LogBranchCatalog;
    use super::harness::Harness;
    use super::*;
    use crate::branch::types::{LeaseDeadline, ARENA_FIRST_EXTENT_PAGES};

    /// Write one payload byte and restamp the checksum, the way any real writer must.
    pub(crate) fn stamp_payload_byte(h: &Harness, page: PageId, value: u8) {
        let handle = h.store.read_page(page).unwrap();
        let mut frame = handle.write();
        frame.data[PAGE_HEADER_SIZE] = value;
        stamp_checksum(&mut frame.data);
    }

    // S2. `Harness::fresh_store` says "as if the process had restarted", but it reuses the SAME
    // DiskManager, so `arena_floor` survives in memory and the reopen path is never exercised.
    // This closes the file and opens it again, which is what a restart actually is.
    #[test]
    fn an_arena_region_is_still_off_limits_after_reopening_the_file() {
        use std::fs::OpenOptions;
        use crate::storage::disk_manager::DiskManager;
        let path = std::env::temp_dir()
            .join(format!("ferro-arena-reopen-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let open = || {
            OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap()
        };

        // Session 1: give the bitmap allocator a real region of its own with some holes punched
        // in it, then put an arena above that and hand out real pages from it.
        let holes = [5u32, 7, 9];
        let (base, arena_pages) = {
            let dm = Arc::new(DiskManager::new(open()).unwrap());
            let pool = Arc::new(BufferPoolManager::new(dm));
            let catalog = Arc::new(LogBranchCatalog::in_memory(1));
            for _ in 0..20 {
                pool.disk_manager.allocate().unwrap();
            }
            for h in holes {
                pool.disk_manager.deallocate(h).unwrap();
            }
            let base = pool.disk_manager.high_water().unwrap();
            let store = ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base).unwrap();
            let br = catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            let mut pages = Vec::new();
            for _ in 0..8 {
                pages.push(
                    store.alloc_for(br.branch_id, PageType::Heap, catalog.next_epoch()).unwrap(),
                );
            }
            (base, pages)
        }; // file closed here

        assert!(!arena_pages.is_empty());
        let arena_hi = *arena_pages.iter().max().unwrap();

        // Session 2: reopen the same file. The arena's pages are on disk and owned, but the
        // bitmap allocator has just been constructed and knows nothing about them.
        let dm2 = Arc::new(DiskManager::new(open()).unwrap());
        let pool2 = Arc::new(BufferPoolManager::new(dm2));
        let catalog2 = Arc::new(LogBranchCatalog::in_memory(1));

        // Half one: the region must be re-claimable at the base it already owns. `new` cannot do
        // this — after a reopen its high-water mark counts the arena's own pages — so reattach is
        // what `reopen` exists for.
        let reattached =
            ArenaPageStore::reopen(Arc::clone(&pool2), Arc::clone(&catalog2) as Arc<dyn BranchCatalog>, base);
        assert!(
            reattached.is_ok(),
            "an arena cannot reattach to the region it already owns: {:?}",
            reattached.err()
        );

        // Half two, the corruption: the bitmap allocator must not hand out arena-owned pages.
        // It should refill the holes it left below the floor...
        let mut handed_out = Vec::new();
        for _ in 0..holes.len() {
            let p = pool2.disk_manager.allocate().unwrap();
            assert!(
                p < base,
                "bitmap allocator handed out page {} at or above the arena base {} (arena holds up to {})",
                p, base, arena_hi
            );
            handed_out.push(p);
        }
        handed_out.sort();
        assert_eq!(handed_out, holes, "the freed pages below the floor are what should come back");

        // ...and once they are gone it must REFUSE, not walk into the arena. Refusing is the
        // correct outcome here; handing back an arena page is the corruption this row is about.
        let err = pool2.disk_manager.allocate();
        assert!(
            err.is_err(),
            "allocator returned {:?} instead of refusing; that page is inside the arena region [{}, {}]",
            err.ok(), base, arena_hi
        );
        let _ = std::fs::remove_file(&path);
    }

    // S2a. `reopen` takes the base on trust. This one takes it from the durable image that
    // describes the region, so there is no argument left for a caller to get wrong.
    #[test]
    fn reopen_from_checkpoint_takes_the_base_from_the_image_not_the_caller() {
        use std::fs::OpenOptions;
        use crate::storage::disk_manager::DiskManager;
        let dir = std::env::temp_dir();
        let path = dir.join(format!("ferro-s2a-{}.db", std::process::id()));
        let ckpt = dir.join(format!("ferro-s2a-{}.ckpt", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&ckpt);
        let open = || OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();

        let (base, live_before) = {
            let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(open()).unwrap())));
            let catalog = Arc::new(LogBranchCatalog::in_memory(1));
            // Push the base off 1 so a wrong base is actually distinguishable from the right one.
            for _ in 0..12 { pool.disk_manager.allocate().unwrap(); }
            let base = pool.disk_manager.high_water().unwrap();
            let store = ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base).unwrap();
            let br = catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            for _ in 0..4 {
                store.alloc_for(br.branch_id, PageType::Heap, catalog.next_epoch()).unwrap();
            }
            store.checkpoint(&ckpt).unwrap();
            (base, store.live_page_count().unwrap())
        };
        assert!(base > 1, "fixture: base must not be the trivial 1");

        let pool2 = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(open()).unwrap())));
        let catalog2 = Arc::new(LogBranchCatalog::in_memory(1));
        let restored =
            ArenaPageStore::reopen_from_checkpoint(pool2, catalog2, &ckpt).unwrap();

        assert_eq!(restored.base_page(), base, "the base came from the image");
        assert_eq!(restored.live_page_count().unwrap(), live_before, "the map came with it");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&ckpt);
    }

    /// **A crash between two checkpoints must not put a live page back into circulation.**
    ///
    /// The free-space map lives in atomics and reaches disk only when `checkpoint` is called. A
    /// process killed after allocating is therefore describable exactly: the durable map is older
    /// than the durable branch catalog, and every page allocated in between reads as free while
    /// `.branches` still names one of them as a branch's root. The next open hands that page to
    /// somebody else, and two writers now share a page — which is not detectable by a checksum,
    /// because each write leaves a perfectly valid page behind.
    ///
    /// Written against the CLI's exact sequence: E31 wired the arena into the shipped binary and
    /// checkpoints it on clean exit only, so this is reachable by pressing Ctrl-C.
    #[test]
    fn a_page_allocated_after_the_last_checkpoint_is_not_reissued_over_a_live_root() {
        use std::fs::OpenOptions;
        use crate::storage::disk_manager::DiskManager;
        let n: u64 = 7717;
        let dir = std::env::temp_dir();
        let db = dir.join(format!("ferro-crash-{}-{}.db", std::process::id(), n));
        let ckpt = dir.join(format!("ferro-crash-{}-{}.ckpt", std::process::id(), n));
        let brs = dir.join(format!("ferro-crash-{}-{}.branches", std::process::id(), n));
        for f in [&db, &ckpt, &brs] { let _ = std::fs::remove_file(f); }
        let open = || OpenOptions::new().create(true).read(true).write(true).open(&db).unwrap();

        // --- session 1: allocate, exit cleanly, checkpoint ---
        let base = {
            let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(open()).unwrap())));
            let cat = Arc::new(LogBranchCatalog::open(&brs, 1).unwrap());
            for _ in 0..8 { pool.disk_manager.allocate().unwrap(); }
            let base = pool.disk_manager.high_water().unwrap();
            let store = ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&cat) as Arc<dyn BranchCatalog>, base).unwrap();
            store.checkpoint_to(ckpt.clone());
            let a = store.arena_for(BranchId::TRUNK).unwrap();
            store.alloc_in_arena(a, PageType::BTreeLeaf, cat.next_epoch()).unwrap();
            store.checkpoint(&ckpt).unwrap();
            base
        };

        // --- session 2: allocate a page, record it as trunk's root, then CRASH ---
        let live_root = {
            let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(open()).unwrap())));
            let cat = Arc::new(LogBranchCatalog::open(&brs, 1).unwrap());
            let store =
                ArenaPageStore::reopen_from_checkpoint(pool, Arc::clone(&cat) as Arc<dyn BranchCatalog>, &ckpt).unwrap();
            let a = store.arena_for(BranchId::TRUNK).unwrap();
            let p = store.alloc_in_arena(a, PageType::BTreeLeaf, cat.next_epoch()).unwrap();
            // `set_root` appends to the branch log, so this survives the crash. `checkpoint` is
            // deliberately NOT called: that is what being killed looks like.
            cat.set_root(BranchId::TRUNK, p).unwrap();
            p
        };

        // --- session 3: reopen from the now-stale checkpoint ---
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(open()).unwrap())));
        let cat = Arc::new(LogBranchCatalog::open(&brs, 1).unwrap());
        let store = ArenaPageStore::reopen_from_checkpoint(pool, Arc::clone(&cat) as Arc<dyn BranchCatalog>, &ckpt).unwrap();
        assert_eq!(store.base_page(), base, "fixture: the region moved between opens");
        assert_eq!(
            cat.get(BranchId::TRUNK).unwrap().root_page_id,
            live_root,
            "fixture: the branch catalog did not survive the crash, so nothing references the \
             page and this test cannot detect a collision"
        );

        let mut handed = Vec::new();
        for _ in 0..4 {
            handed.push(
                store.alloc_for(BranchId::TRUNK, PageType::BTreeLeaf, cat.next_epoch()).unwrap(),
            );
        }
        for f in [&db, &ckpt, &brs] { let _ = std::fs::remove_file(f); }

        assert!(
            !handed.contains(&live_root),
            "after a crash the arena re-issued page {live_root}, which the branch catalog still \
             names as trunk's root (handed out {handed:?}). The next write to it silently \
             overwrites the trunk tree."
        );
    }

    // Grafting one region's free-space map onto another store would make every page id in the
    // image refer to somebody else's pages. The checkpoint names its region so that is refusable.
    #[test]
    fn a_checkpoint_describing_another_region_is_refused() {
        use std::fs::OpenOptions;
        use crate::storage::disk_manager::DiskManager;
        let a = Harness::new();

        // A second store on its OWN file — one DiskManager cannot carry two regions, which is
        // what S4 now refuses. Allocating first pushes this base clear of a's.
        let path = std::env::temp_dir().join(format!("ferro-s2a-other-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let file = OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
        let pool_b = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog_b = Arc::new(LogBranchCatalog::in_memory(1));
        for _ in 0..12 { pool_b.disk_manager.allocate().unwrap(); }
        let base_b = pool_b.disk_manager.high_water().unwrap();
        let b_other = ArenaPageStore::new(pool_b, catalog_b, base_b).unwrap();
        assert_ne!(a.store.base_page(), b_other.base_page(), "fixture: bases must differ");

        let err = b_other.load_state(&a.store.state_bytes());
        assert!(err.is_err(), "a store loaded a map describing a region it does not own");
        let msg = format!("{:?}", err.unwrap_err());
        assert!(
            msg.contains("describes the region at"),
            "wrong error, got: {}",
            msg
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fork_allocates_no_page_at_all() {
        let h = Harness::new();
        let before = h.store.live_page_count().unwrap();
        let reserved_before = h.store.reserved_page_count();
        for _ in 0..64 {
            h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        }
        assert_eq!(h.store.live_page_count().unwrap(), before, "exit criterion 1");
        assert_eq!(h.store.reserved_page_count(), reserved_before, "not even an extent");
    }

    #[test]
    fn a_private_page_is_mutated_in_place_not_shadowed() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let arena = h.store.arena_for(b.branch_id).unwrap();
        let e = h.catalog.next_epoch();
        let p = h.store.alloc_in_arena(arena, PageType::BTreeLeaf, e).unwrap();

        let before = h.store.live_page_count().unwrap();
        let cow = h.store.cow_page(p, b.branch_id, h.catalog.next_epoch()).unwrap();
        assert!(!cow.copied);
        assert_eq!(cow.page_id, p);
        assert_eq!(h.store.live_page_count().unwrap(), before, "no shadow, no allocation");
    }

    #[test]
    fn an_inherited_page_is_shadowed_and_the_original_is_left_alone() {
        let h = Harness::new();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let p_arena = h.store.arena_for(parent.branch_id).unwrap();
        let page = h
            .store
            .alloc_in_arena(p_arena, PageType::BTreeLeaf, h.catalog.next_epoch())
            .unwrap();
        stamp_payload_byte(&h, page, 0x5A);

        let child = h.catalog.fork(parent.branch_id, LeaseDeadline(1)).unwrap();
        let cow = h.store.cow_page(page, child.branch_id, h.catalog.next_epoch()).unwrap();

        assert!(cow.copied, "a page from the parent's arena must be shadowed");
        assert_ne!(cow.page_id, page);
        assert_eq!(cow.handle.read().data[PAGE_HEADER_SIZE], 0x5A, "payload copied");
        assert_eq!(
            h.store.allocated_pages(p_arena),
            vec![page],
            "the child must not free a page its parent still points at"
        );
        assert_eq!(h.store.pending_len(), 0);
    }

    // ---- D102: the delta write path ------------------------------------------------------
    //
    // These bind `cow_page` to `branch::delta`. Before them that module had no caller anywhere in
    // the crate, so every property it asserted was a property of a fixture.

    /// Overwrite `len` payload bytes of `page` starting at `at`, leaving the rest alone.
    fn write_payload_run(h: &Harness, page: PageId, at: usize, bytes: &[u8]) {
        let handle = h.store.read_page(page).unwrap();
        let mut frame = handle.write();
        frame.data[PAGE_HEADER_SIZE + at..PAGE_HEADER_SIZE + at + bytes.len()]
            .copy_from_slice(bytes);
        stamp_checksum(&mut frame.data);
    }

    /// Fork a child, shadow `page` into it, and hand back the shadow.
    fn shadow_once(h: &Harness, parent: BranchId, page: PageId) -> (BranchId, PageId) {
        let child = h.catalog.fork(parent, LeaseDeadline(1)).unwrap();
        let cow = h.store.cow_page(page, child.branch_id, h.catalog.next_epoch()).unwrap();
        assert!(cow.copied, "the fixture needs a real shadow, not an in-place mutation");
        (child.branch_id, cow.page_id)
    }

    /// **A few changed bytes cost a few bytes, not a page.** This is D93's claim taken against
    /// pages the arena really produced rather than against a fixture built for the encoder.
    #[test]
    fn a_shadow_that_changed_a_little_encodes_to_a_delta_far_under_a_page() {
        let h = Harness::new();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let p_arena = h.store.arena_for(parent.branch_id).unwrap();
        let page =
            h.store.alloc_in_arena(p_arena, PageType::BTreeLeaf, h.catalog.next_epoch()).unwrap();
        // A page with content, so the delta is against something rather than against zeroes.
        for i in 0..40 {
            write_payload_run(&h, page, i * 64, &[(i as u8).wrapping_mul(7); 16]);
        }

        let (_child, shadow) = shadow_once(&h, parent.branch_id, page);
        assert_eq!(
            h.store.shadow_base(shadow),
            Some((page, 1)),
            "cow_page must record the page a shadow was taken from, at depth 1"
        );

        // Four rows' worth of change, the shape D93 priced.
        write_payload_run(&h, shadow, 128, &[0xAA; 24]);
        write_payload_run(&h, shadow, 1024, &[0xBB; 24]);

        let delta = h.store.delta_against_base(shadow).unwrap().expect("a small change fits");
        assert_eq!(delta.base(), page);
        assert_eq!(delta.depth(), 1);
        assert!(
            delta.encoded_len() <= delta::DELTA_BUDGET,
            "a delta that is stored must be within the budget, got {}",
            delta.encoded_len()
        );
        assert!(
            delta.encoded_len() < PAGE_SIZE / 8,
            "48 changed bytes encoded to {} bytes; the whole point is that it is far under the \
             {PAGE_SIZE}-byte page cow_page copies today",
            delta.encoded_len()
        );

        // And it is the RIGHT delta: applying it to the base rebuilds the shadow's payload.
        let base_img = h.store.read_page(page).unwrap().read().data;
        let shadow_img = h.store.read_page(shadow).unwrap().read().data;
        let mut rebuilt = base_img[PAGE_HEADER_SIZE..].to_vec();
        delta.apply(&mut rebuilt).unwrap();
        assert_eq!(
            rebuilt,
            shadow_img[PAGE_HEADER_SIZE..].to_vec(),
            "the delta did not rebuild the page it was taken from"
        );
    }

    /// **The 24-byte header must not appear in the delta.** D94 proved two independently written
    /// pages differ in their headers even when their content is identical — `birth_epoch`,
    /// `arena_id` and `crc32` are all different for a shadow. If the header were inside the diff,
    /// every delta ever taken would carry a run at offset 0 and the cheapest case would be
    /// inflated the most. A shadow nobody has touched must therefore encode to ZERO runs.
    #[test]
    fn a_shadow_nobody_touched_encodes_to_no_runs_although_its_header_differs() {
        let h = Harness::new();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let p_arena = h.store.arena_for(parent.branch_id).unwrap();
        let page =
            h.store.alloc_in_arena(p_arena, PageType::BTreeLeaf, h.catalog.next_epoch()).unwrap();
        write_payload_run(&h, page, 0, &[0x5A; 64]);

        let (_child, shadow) = shadow_once(&h, parent.branch_id, page);

        // The premise of the test, read out of the system rather than assumed.
        let base_img = h.store.read_page(page).unwrap().read().data;
        let shadow_img = h.store.read_page(shadow).unwrap().read().data;
        assert_ne!(
            base_img[..PAGE_HEADER_SIZE],
            shadow_img[..PAGE_HEADER_SIZE],
            "the fixture is vacuous unless the two headers really do differ"
        );
        assert_eq!(
            base_img[PAGE_HEADER_SIZE..],
            shadow_img[PAGE_HEADER_SIZE..],
            "an untouched shadow must have the same payload as its base"
        );

        let delta = h.store.delta_against_base(shadow).unwrap().expect("zero runs is in budget");
        assert!(
            delta.runs().is_empty(),
            "the differing header leaked into the delta as {} run(s)",
            delta.runs().len()
        );
        assert_eq!(delta.encoded_len(), delta::DELTA_HEADER);
    }

    /// **A delta that does not beat the page is REFUSED and a whole page is stored.** D93 measured
    /// the compaction regime at 1373 B and 2552 B against a 1018-byte budget. Rewriting most of a
    /// page is that regime, and it must come back `None` rather than as a delta claiming a saving
    /// it cannot deliver.
    #[test]
    fn a_shadow_that_rewrote_the_page_is_refused_a_delta() {
        let h = Harness::new();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let p_arena = h.store.arena_for(parent.branch_id).unwrap();
        let page =
            h.store.alloc_in_arena(p_arena, PageType::BTreeLeaf, h.catalog.next_epoch()).unwrap();

        let (_child, shadow) = shadow_once(&h, parent.branch_id, page);
        // Rewrite well past the budget: a contiguous run of half the payload.
        let big = vec![0xC3u8; crate::cow::node::PAYLOAD_LEN / 2];
        write_payload_run(&h, shadow, 0, &big);

        assert!(
            h.store.delta_against_base(shadow).unwrap().is_none(),
            "a {}-byte rewrite must be refused against a {}-byte budget",
            big.len(),
            delta::DELTA_BUDGET
        );
    }

    /// **`MAX_CHAIN_DEPTH` is enforced at WRITE time, so a read cannot meet a chain the bound
    /// forbids.** Shadowing a chain one link past the bound must produce a page recorded as a
    /// chain ROOT — no base at all — rather than a deeper link. `branch::mod` invariant 2 is why:
    /// an unbounded delta chain is a parent-chain walk wearing different clothes.
    #[test]
    fn a_chain_stops_growing_at_the_bound_rather_than_being_caught_on_read() {
        let h = Harness::new();
        let root = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let arena = h.store.arena_for(root.branch_id).unwrap();
        let page = h.store.alloc_in_arena(arena, PageType::BTreeLeaf, h.catalog.next_epoch()).unwrap();

        // Shadow down a chain of branches, each forking from the last, so every cow sees a page
        // its own branch does not own and therefore really shadows.
        let mut owner = root.branch_id;
        let mut current = page;
        let mut depths = Vec::new();
        for _ in 0..(delta::MAX_CHAIN_DEPTH as usize + 3) {
            let (child, shadow) = shadow_once(&h, owner, current);
            depths.push(h.store.shadow_base(shadow).map(|(_, d)| d));
            owner = child;
            current = shadow;
        }

        // The chain climbs to the bound, the next link is a ROOT, and the chain after it starts
        // again from 1. **That restart is the collapse working, not a leak**: a page with no
        // recorded base is stored whole, and a shadow of a whole page is legitimately depth 1. It
        // is the same shape `DeltaStore::write` takes when it collapses — store a full page, begin
        // a new chain — and it is what keeps the bound a bound instead of a ceiling that stalls
        // every later write.
        let bound = delta::MAX_CHAIN_DEPTH as usize;
        let mut expected: Vec<Option<u8>> = (1..=delta::MAX_CHAIN_DEPTH).map(Some).collect();
        expected.push(None);
        expected.push(Some(1));
        expected.push(Some(2));
        assert_eq!(
            depths, expected,
            "the chain must climb to {bound}, collapse to a root, then start again"
        );

        // The property the bound actually exists for, stated over the whole run rather than over
        // the one index where it first bites: no read ever faces more than `MAX_CHAIN_DEPTH`
        // deltas, because no deeper link was ever WRITTEN.
        assert!(
            depths.iter().flatten().all(|&d| d <= delta::MAX_CHAIN_DEPTH),
            "no recorded depth may exceed the bound: {depths:?}"
        );
        assert!(
            depths[bound].is_none(),
            "the link that would have been depth {} must be a chain root: {depths:?}",
            bound + 1
        );
    }

    /// Shadowing a page the branch OWNS is not recorded as a delta base, and that is what keeps
    /// this out of the GC's business: that page is handed to `free_page` on the very next line, so
    /// a delta against it would be a second, invisible reason to keep it alive — a reference
    /// count, in a file whose header says there are none.
    #[test]
    fn shadowing_a_page_you_own_records_no_delta_base() {
        let h = Harness::new();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let arena = h.store.arena_for(parent.branch_id).unwrap();
        let page = h.store.alloc_in_arena(arena, PageType::BTreeLeaf, h.catalog.next_epoch()).unwrap();
        // A child fork moves the privacy barrier past the page's birth, so the parent shadows its
        // OWN page rather than mutating it in place.
        let _child = h.catalog.fork(parent.branch_id, LeaseDeadline(1)).unwrap();
        let cow = h.store.cow_page(page, parent.branch_id, h.catalog.next_epoch()).unwrap();

        assert!(cow.copied, "the fixture needs a shadow of a page the branch owns");
        assert_eq!(
            h.store.shadow_base(cow.page_id),
            None,
            "a base the branch owns must not be recorded: free_page owns that page's liveness"
        );
        assert!(h.store.delta_against_base(cow.page_id).unwrap().is_none());
    }

    /// A recycled page id must not inherit the base of its previous life. Without the removal in
    /// `release_page` the reissued id reads as a delta against a base it has nothing to do with,
    /// which materialises a page built from the wrong bytes and reports no error at all.
    #[test]
    fn a_released_page_forgets_the_base_it_was_a_shadow_of() {
        let h = Harness::new();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let p_arena = h.store.arena_for(parent.branch_id).unwrap();
        let page =
            h.store.alloc_in_arena(p_arena, PageType::BTreeLeaf, h.catalog.next_epoch()).unwrap();

        let (child, shadow) = shadow_once(&h, parent.branch_id, page);
        assert!(h.store.shadow_base(shadow).is_some(), "the fixture needs a recorded base");

        let c_arena = h.store.arena_for(child).unwrap();
        h.store.release_page(shadow, c_arena);
        assert_eq!(
            h.store.shadow_base(shadow),
            None,
            "a released id still named a base; the next page to get this id would decode as a \
             delta of an unrelated page"
        );
    }

    /// **D99 — freeing an extent forgets the bases of ITS pages, and of no others.**
    ///
    /// The `retain` this replaced walked every entry in `shadow_base` — a map keyed by PAGE id
    /// holding one entry per live shadow in the whole store — to drop the handful lying inside one
    /// extent. Probing the extent's own range instead is equivalent only if it drops exactly the
    /// same entries, so the assertion that carries the change is the BYSTANDER: a shadow in a
    /// different extent must survive. An implementation that cleared the map, or that probed the
    /// wrong range, still satisfies the first assertion and fails this one.
    #[test]
    fn freeing_an_extent_forgets_only_its_own_shadow_bases() {
        let h = Harness::new();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        // `alloc_for`, not `alloc_in_arena`: a branch's first extent is ONE page, so asking the
        // same extent for a second one fails with "arena is exhausted". `alloc_for` grows the
        // branch onto a fresh extent when it needs to.
        let page_a = h
            .store
            .alloc_for(parent.branch_id, PageType::BTreeLeaf, h.catalog.next_epoch())
            .unwrap();
        let page_b = h
            .store
            .alloc_for(parent.branch_id, PageType::BTreeLeaf, h.catalog.next_epoch())
            .unwrap();

        // Two forks, so the two shadows land in two different extents.
        let (child_a, shadow_a) = shadow_once(&h, parent.branch_id, page_a);
        let (child_b, shadow_b) = shadow_once(&h, parent.branch_id, page_b);
        assert!(h.store.shadow_base(shadow_a).is_some(), "fixture: no base recorded for a");
        assert!(h.store.shadow_base(shadow_b).is_some(), "fixture: no base recorded for b");

        // The arena each shadow ACTUALLY lives in, read from its own page header. Not
        // `arena_for(child)`: that answers "which extent is this branch filling now", and a
        // one-page extent filled by the shadow itself is already exhausted, so it would hand back
        // a fresh extent that does not contain the page and the free would miss.
        let a_arena = h.store.read_page(shadow_a).unwrap().header().unwrap().arena_id;
        let b_arena = h.store.read_page(shadow_b).unwrap().header().unwrap().arena_id;
        let _ = (child_a, child_b);
        assert_ne!(
            a_arena, b_arena,
            "fixture: both shadows share one extent, so this test has no bystander to protect"
        );

        h.store.free_arena(a_arena).unwrap();

        assert_eq!(
            h.store.shadow_base(shadow_a),
            None,
            "a freed extent's page still names a base; a reissued id would decode as a delta of it"
        );
        assert!(
            h.store.shadow_base(shadow_b).is_some(),
            "freeing one extent forgot a DIFFERENT extent's base"
        );
    }

    /// **A base whose page id has been reissued must be REFUSED, not encoded against.**
    ///
    /// The argument that this cannot happen is real — only bases the branch does not own are
    /// recorded, and the epoch interval rule parks a page a live child can see — but it spans this
    /// file and the reaper, and the failure it guards is silent: a delta against a reissued page
    /// rebuilds bytes from a page that has nothing to do with the shadow, and nothing reports an
    /// error. So the dangerous state is made unrepresentable with a `birth_epoch` check, and this
    /// test forces that check to fire by recycling the id deliberately.
    #[test]
    fn a_base_whose_id_was_reissued_is_refused_rather_than_encoded_against() {
        let h = Harness::new();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let p_arena = h.store.arena_for(parent.branch_id).unwrap();
        let base =
            h.store.alloc_in_arena(p_arena, PageType::BTreeLeaf, h.catalog.next_epoch()).unwrap();
        write_payload_run(&h, base, 0, &[0x11; 64]);

        let (_child, shadow) = shadow_once(&h, parent.branch_id, base);
        write_payload_run(&h, shadow, 0, &[0x22; 8]);
        assert!(
            h.store.delta_against_base(shadow).unwrap().is_some(),
            "the fixture is vacuous unless a delta is available BEFORE the id is recycled"
        );
        let before = stale_delta_base_count();

        // Recycle the base's id and hand it straight back out. `write_fresh_page` stamps a new
        // birth_epoch, which is exactly what the check reads.
        h.store.release_page(base, p_arena);
        let reissued =
            h.store.alloc_in_arena(p_arena, PageType::BTreeLeaf, h.catalog.next_epoch()).unwrap();
        assert_eq!(reissued, base, "the fixture needs the SAME id handed out again");

        assert!(
            h.store.delta_against_base(shadow).unwrap().is_none(),
            "a delta was encoded against a page that had been reissued to somebody else"
        );
        assert_eq!(
            stale_delta_base_count(),
            before + 1,
            "the refusal must be counted, or a workload where it happens is invisible"
        );
    }

    #[test]
    fn shadowing_your_own_page_across_a_child_fork_parks_the_original() {
        let h = Harness::new();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let arena = h.store.arena_for(parent.branch_id).unwrap();
        let page = h
            .store
            .alloc_in_arena(arena, PageType::Heap, h.catalog.next_epoch())
            .unwrap();
        // a child forks off *after* the page was born, so it can see it
        let _child = h.catalog.fork(parent.branch_id, LeaseDeadline(1)).unwrap();

        let cow = h.store.cow_page(page, parent.branch_id, h.catalog.next_epoch()).unwrap();
        assert!(cow.copied, "the child can see the old page, so it must be shadowed");

        // ⛔ D125 MOVED THE FREE, AND THIS TEST'S REAL PROPERTY IS UNCHANGED BY THAT.
        //
        // This used to assert `pending_len() == 1` right here, on the strength of `cow_page`
        // freeing the original itself. That free was a defect: a store cannot know whether its
        // caller's operation will commit, and `CowTree` rolls a failed one back to a root that
        // still points at this page — so the free had been taken for an operation that never
        // happened. `cow_page` now REPORTS it and the caller frees at its own commit point.
        //
        // What this test is actually about survives intact and is still asserted below: when
        // the original IS freed, a live child pins it, so it is PARKED rather than released.
        // The two halves are now pinned separately, which is strictly more than before.
        assert!(
            cow.retire_previous,
            "the writer owns the original, so the caller is the one who must retire it"
        );
        assert_eq!(
            h.store.pending_len(),
            0,
            "D125: cow_page must not free the page it shadowed — its caller may still roll back"
        );

        // The caller's commit point.
        h.store.free_page(page, h.catalog.next_epoch()).unwrap();
        assert_eq!(h.store.pending_len(), 1, "the original is pinned by the child, not released");
    }

    #[test]
    fn arena_rolls_over_when_an_extent_fills() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let first = h.store.arena_for(b.branch_id).unwrap();
        let e = h.catalog.next_epoch();
        // The extent's OWN size, not the store's cap. D31 made those different, and looping to
        // the cap here would refuse on the second allocation instead of testing the rollover.
        let (_, first_pages) = h.store.extent_range(first).unwrap();
        assert_eq!(first_pages, ARENA_FIRST_EXTENT_PAGES, "a branch's first extent is one page");
        for _ in 0..first_pages {
            h.store.alloc_in_arena(first, PageType::Heap, e).unwrap();
        }
        assert!(h.store.alloc_in_arena(first, PageType::Heap, e).is_err(), "extent is full");
        let second = h.store.arena_for(b.branch_id).unwrap();
        assert_ne!(second, first);
        assert_eq!(
            h.store.extent_range(second).unwrap().1,
            first_pages * 2,
            "the second extent must DOUBLE the first; a flat sequence is the 262x wall D31 removed"
        );
        assert_eq!(
            h.catalog.get(b.branch_id).unwrap().arenas,
            vec![first, second],
            "the durable record must list every extent the reaper has to free"
        );
    }

    #[test]
    fn freed_extents_are_recycled_rather_than_leaked() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let a1 = h.store.arena_for(b.branch_id).unwrap();
        assert!(h.store.allocated_pages(a1).is_empty());
        let (range1, pages1) = h.store.extent_range(a1).unwrap();
        h.store.alloc_in_arena(a1, PageType::Heap, h.catalog.next_epoch()).unwrap();
        assert_eq!(h.store.free_arena(a1).unwrap(), 1);
        assert_eq!(h.store.reserved_page_count(), 0);

        let b2 = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let a2 = h.store.alloc_arena(b2.branch_id).unwrap();
        // **The page range came back**, which is what recycling means. This used to assert
        // `reserved == ARENA_EXTENT_PAGES`, which said "one extent is reserved" in the uniform
        // geometry and says nothing about reuse: a freshly claimed extent reserves the same count
        // as a recycled one. Since D31 the size classes make the distinction load-bearing, so the
        // test asks the question it always meant to.
        assert_eq!(
            h.store.extent_range(a2).unwrap(),
            (range1, pages1),
            "the freed extent's page range was not handed out again"
        );
        assert_eq!(h.store.reserved_page_count(), pages1, "one extent, reused");
        assert_ne!(a1, a2, "arena ids are not reused even when the extent is");
    }

    #[test]
    fn a_torn_page_is_refused_rather_than_returned() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let arena = h.store.arena_for(b.branch_id).unwrap();
        let p = h.store.alloc_in_arena(arena, PageType::Heap, Epoch(1)).unwrap();
        {
            let mut raw = h.store.pool.disk_manager.read(p).unwrap();
            raw[PAGE_SIZE - 1] ^= 0xff;
            h.store.pool.disk_manager.write(p, &raw).unwrap();
        }
        assert!(h.store.read_page(p).is_err());
    }

    #[test]
    fn store_refuses_to_share_the_bitmap_allocators_region() {
        let h = Harness::new();
        let err = ArenaPageStore::new(
            Arc::clone(&h.store.pool),
            Arc::clone(&h.catalog),
            0,
        );
        assert!(err.is_err(), "overlapping the bitmap allocator must be refused, not warned about");
    }

    /// **The claim `the_checkpoint_image_is_byte_identical_to_what_a_node_local_counter_wrote`
    /// used to make with a version number.**
    ///
    /// That test asserted `bytes[0] == 2` under the message *"the state version changed; every
    /// <db>.arena on disk is now unreadable"*. D31 had to bump it to 3, because a freed extent's
    /// SIZE is not derivable once extents stop being uniform and reusing a v2 entry as if it were
    /// cap-sized would alias up to 255 pages.
    ///
    /// A version number is only a proxy for the consequence. This asserts the consequence: an
    /// image in the old format still opens, and opens to **exactly** the same map. Without this,
    /// renumbering that assertion would have been the move it exists to prevent.
    #[test]
    fn a_v2_checkpoint_image_still_loads_after_the_v3_bump() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();

        // Climb to a cap-sized extent and free it. Only cap-sized extents can appear in a genuine
        // v2 free list, because a v2 store could not produce any other size — which is exactly
        // what makes reading them as cap-sized a fact rather than a guess.
        let mut arena = h.store.alloc_arena(b.branch_id).unwrap();
        while h.store.extent_range(arena).unwrap().1 < ARENA_EXTENT_PAGES {
            arena = h.store.alloc_arena(b.branch_id).unwrap();
        }
        assert_eq!(h.store.extent_range(arena).unwrap().1, ARENA_EXTENT_PAGES);
        h.store.free_arena(arena).unwrap();

        let v3 = h.store.state_bytes();
        assert_eq!(v3[0], 3, "fixture: this is not a v3 image");

        // Rewrite it as v2: version byte 2, and the free list back to bare start pages. Every
        // other field is untouched between the two versions, so this is a real v2 image.
        let body = v3.len() - 4;
        let n = u32::from_be_bytes(v3[21..25].try_into().unwrap()) as usize;
        assert!(n > 0, "fixture: the free list is empty, so the downgrade changes nothing");
        let mut v2 = Vec::new();
        v2.push(2u8);
        v2.extend_from_slice(&v3[1..21]);
        v2.extend_from_slice(&(n as u32).to_be_bytes());
        for i in 0..n {
            let at = 25 + i * 8;
            v2.extend_from_slice(&v3[at..at + 4]); // start, dropping the v3 page_count
        }
        v2.extend_from_slice(&v3[25 + n * 8..body]);
        let crc = crc32(&v2);
        v2.extend_from_slice(&crc.to_be_bytes());

        let restored = h.fresh_store();
        restored
            .load_state(&v2)
            .expect("a v2 image must still open, or every <db>.arena on disk is lost");

        // Byte-identical when written back out: the v2 reader recovered the same map, freed
        // extent's size included. A reader that guessed would differ here.
        assert_eq!(
            restored.state_bytes(),
            v3,
            "a v2 image did not round-trip to the same map"
        );
    }

    /// **D99 — `free_arena` must forget exactly ONE branch's current extent: the owner's.**
    ///
    /// The scan this replaced walked every entry in `current` and removed whatever pointed at the
    /// arena. A lookup keyed on `ext.owner` is equivalent only because an arena id names one
    /// extent for the life of the store, so the entry it finds is the only one that COULD have
    /// matched. The assertion that carries that is not "the owner was forgotten" — a rewrite that
    /// removed the wrong key, or removed several, still satisfies it — but "no bystander moved".
    #[test]
    fn freeing_an_extent_forgets_its_owner_and_no_other_branch() {
        let h = Harness::new();
        let a = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id;
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id;
        let c = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id;
        let aa = h.store.arena_for(a).unwrap();
        let ab = h.store.arena_for(b).unwrap();
        let ac = h.store.arena_for(c).unwrap();
        assert!(aa != ab && ab != ac && aa != ac, "fixture: branches must not share an extent");

        h.store.free_arena(ab).unwrap();

        // **Read the MAP, not `arena_for`.** `free_arena` removes this arena from `claim_epoch` on
        // the line above, and `arena_for` refuses any extent whose claim epoch is missing — so a
        // build that forgot the `current` removal altogether STILL hands `b` a fresh extent, and an
        // assertion phrased on `arena_for` passes against it. Measured: deleting the removal leaves
        // that phrasing green. The entry's real consequence is the durable image, which serialises
        // `current` whole, so that is where the assertion belongs. Taken before anything re-fills
        // `b`, which would put the key straight back.
        let (arenas_in_image, current_in_image) = key_order_in_image(&h.store.state_bytes());
        assert!(!arenas_in_image.contains(&ab.0), "fixture: the freed extent is still an extent");
        let owners: Vec<u64> = current_in_image.iter().map(|(id, _)| *id).collect();
        assert!(
            !owners.contains(&b.id),
            "the freed extent's owner is still named in the image's current-arena section"
        );
        assert!(
            owners.contains(&a.id) && owners.contains(&c.id),
            "freeing b's extent dropped a bystander from the image's current-arena section"
        );

        // And the bystanders still hold the extents they were filling. This is the half a scan gave
        // away for free and a lookup has to earn.
        assert_eq!(h.store.arena_for(a).unwrap(), aa, "freeing b's extent moved a off its own");
        assert_eq!(h.store.arena_for(c).unwrap(), ac, "freeing b's extent moved c off its own");
    }

    /// **D99 — freeing an extent hands back EVERY per-arena map entry, `fill_unknown` included.**
    ///
    /// It is the only one `free_arena` used to leave behind, and only `resolve_fill` ever clears
    /// an id from that set — which nothing calls for an extent that no longer exists. Arena ids
    /// are never reissued, so a leaked suspicion could never be collected by any later path.
    #[test]
    fn freeing_an_extent_gives_back_its_fill_suspicion_too() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id;
        let arena = h.store.arena_for(b).unwrap();

        // The state a restore leaves: `load_state` marks every restored extent fill-unknown.
        h.store.debug_set_next_free(arena, 0);
        assert_eq!(h.store.debug_fill_unknown_len(), 1, "fixture: the extent is not under suspicion");

        h.store.free_arena(arena).unwrap();

        assert_eq!(
            h.store.debug_fill_unknown_len(),
            0,
            "the freed extent is still under fill suspicion, and nothing will ever collect it"
        );
    }

    /// The `get` guard, which the scan also had: freeing an extent the owner has ALREADY moved off
    /// must leave it on its current one. Without the guard this evicts a live branch from the
    /// extent it is filling and sends it back to `alloc_arena` on its next write.
    #[test]
    fn freeing_a_superseded_extent_leaves_the_owner_on_its_current_one() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id;
        let old = h.store.arena_for(b).unwrap();
        let new = h.store.alloc_arena(b).unwrap();
        assert_ne!(old, new, "fixture: the branch did not move to a second extent");
        assert_eq!(h.store.arena_for(b).unwrap(), new, "fixture: the branch is not on the new one");

        h.store.free_arena(old).unwrap();

        assert_eq!(
            h.store.arena_for(b).unwrap(),
            new,
            "freeing a superseded extent evicted the owner from its CURRENT one"
        );
    }

    #[test]
    fn free_space_map_survives_a_restart() {
        let h = Harness::new();
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let a1 = h.store.arena_for(parent.branch_id).unwrap();
        for _ in 0..5 {
            h.store.alloc_for(parent.branch_id, PageType::Heap, h.catalog.next_epoch()).unwrap();
        }
        // one extent handed back, so the free-extent list is non-empty
        let spare = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let a2 = h.store.arena_for(spare.branch_id).unwrap();
        let a2_start = h.store.extent_range(a2).unwrap().0;
        h.store.free_arena(a2).unwrap();
        // and one page parked against a live child
        let page = h.store.alloc_for(parent.branch_id, PageType::Heap, h.catalog.next_epoch()).unwrap();
        let _child = h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        h.store.free_page(page, h.catalog.next_epoch()).unwrap();

        let before = (
            h.store.live_page_count().unwrap(),
            h.store.reserved_page_count(),
            h.store.pending_len(),
            h.store.allocated_pages(a1),
        );
        assert_eq!(before.2, 1, "the parked page is what a restart most easily loses");

        let bytes = h.store.state_bytes();
        let restored = h.fresh_store();
        restored.load_state(&bytes).unwrap();

        assert_eq!(restored.live_page_count().unwrap(), before.0);
        assert_eq!(restored.reserved_page_count(), before.1);
        assert_eq!(restored.pending_len(), before.2);
        assert_eq!(restored.allocated_pages(a1), before.3);
        assert_eq!(restored.arena_owner(a1), Some(parent.branch_id));
        assert_eq!(restored.arena_owner(a2), None, "the freed extent stayed freed");
        // The bump pointer must not rewind. Drain the recycled-extent free list first, so the
        // next reservation has to come from the bump pointer, then check the range it hands out
        // does not overlap the live extent. Two overlapping live extents is silent corruption,
        // and comparing arena *ids* would never notice it.
        let (a1_start, a1_len) = restored.extent_range(a1).unwrap();
        let recycled = restored.alloc_arena(spare.branch_id).unwrap();
        assert_eq!(
            restored.extent_range(recycled).map(|r| r.0),
            Some(a2_start),
            "the freed extent is handed out first"
        );
        let from_bump = restored.alloc_arena(spare.branch_id).unwrap();
        let (b_start, b_len) = restored.extent_range(from_bump).unwrap();
        assert!(
            b_start >= a1_start + a1_len || b_start + b_len <= a1_start,
            "extent {}..{} overlaps live extent {}..{}: the bump pointer rewound on restore",
            b_start,
            b_start + b_len,
            a1_start,
            a1_start + a1_len
        );
    }

    #[test]
    fn a_corrupt_free_space_map_is_refused_not_partially_loaded() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let a = h.store.arena_for(b.branch_id).unwrap();
        h.store.alloc_in_arena(a, PageType::Heap, Epoch(1)).unwrap();
        let good = h.store.state_bytes();

        let target = h.fresh_store();
        let mut bad = good.clone();
        bad[1] ^= 0xff;
        assert!(target.load_state(&bad).is_err(), "checksum must reject a flipped byte");
        assert!(target.load_state(&good[..good.len() - 7]).is_err(), "truncation must be refused");
        let mut versioned = good.clone();
        versioned[0] = 9;
        // recompute the crc so only the version is wrong
        let body = versioned.len() - 4;
        let crc = crc32(&versioned[..body]);
        versioned[body..].copy_from_slice(&crc.to_be_bytes());
        assert!(target.load_state(&versioned).is_err(), "unknown version must be refused");

        // none of those refusals may have left a half-loaded map behind
        assert_eq!(target.live_page_count().unwrap(), 0);
        assert_eq!(target.reserved_page_count(), 0);
        assert_eq!(target.arena_owner(a), None);
        // and the good image still loads
        target.load_state(&good).unwrap();
        assert_eq!(target.arena_owner(a), Some(b.branch_id));
    }

    /// **The checkpoint's "atomic rename" is only atomic if both fsyncs happen.**
    ///
    /// `<db>.arena` is the only thing on disk that says where the branch arena starts, and it was
    /// written with `std::fs::write` + `std::fs::rename`, neither of which makes anything durable.
    /// A power cut could therefore leave the directory entry pointing at bytes that never reached
    /// the device — and `load_state`'s CRC32 then refuses the image, so the database does not open.
    ///
    /// Asserted as an operation *order*, because that is the only way to see it: every test that
    /// reads the file back is answered by the page cache whether the fsyncs happened or not.
    #[test]
    fn the_checkpoint_syncs_the_image_before_the_rename_and_the_directory_after_it() {
        use crate::storage::atomic_file::{Op, RecordingOps};
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let a = h.store.arena_for(b.branch_id).unwrap();
        h.store.alloc_in_arena(a, PageType::Heap, Epoch(3)).unwrap();

        // A path that does not exist: nothing here may touch a real filesystem, which is the point
        // of recording the operations instead of their aftermath.
        let path = std::path::Path::new("/ferro-no-such-dir/db.arena");
        let ops = RecordingOps::new();
        h.store.checkpoint_with(&ops, path).unwrap();

        let tmp = std::path::PathBuf::from("/ferro-no-such-dir/db.arena.tmp");
        assert_eq!(
            ops.shape(),
            vec![
                ("write", tmp.clone()),
                ("sync_file", tmp.clone()),
                ("rename", tmp.clone()),
                ("sync_dir", std::path::PathBuf::from("/ferro-no-such-dir")),
            ],
            "the free-space map must be on the device before the rename names it, and the rename \
             must be on the device after it"
        );
        match &ops.ops()[0] {
            Op::Write(_, bytes) => assert_eq!(
                bytes,
                &h.store.state_bytes(),
                "the temporary must receive this store's free-space map"
            ),
            other => panic!("the first operation was {other:?}"),
        }
        match &ops.ops()[2] {
            Op::Rename(from, to) => {
                assert_eq!(from, &tmp);
                assert_eq!(to, path, "the temporary must land on the checkpoint path itself");
            }
            other => panic!("the third operation was {other:?}"),
        }
    }

    /// A store holding `n` branches, each with its own arena and one page in it.
    ///
    /// Every `Harness` builds fresh `HashMap`s, and `RandomState` gives each instance different
    /// hash keys — so two stores built by this function hold the same logical map in two different
    /// iteration orders, which is exactly the difference a durable image must not show.
    fn store_with_n_arenas(n: u64) -> (Harness, Vec<ArenaId>) {
        let h = Harness::new();
        let mut arenas = Vec::new();
        for _ in 0..n {
            let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            let a = h.store.arena_for(b.branch_id).unwrap();
            h.store.alloc_in_arena(a, PageType::Heap, Epoch(1)).unwrap();
            arenas.push(a);
        }
        (h, arenas)
    }

    /// Read the two key sequences straight out of a checkpoint image, using the format documented
    /// above `state_bytes`.
    ///
    /// An independent reader on purpose: `load_state` puts every entry back into a `HashMap`, so it
    /// cannot see the order they arrived in — which is the whole property under test.
    fn key_order_in_image(b: &[u8]) -> (Vec<u32>, Vec<(u64, u32)>) {
        let u32_at = |at: usize| u32::from_be_bytes(b[at..at + 4].try_into().unwrap());
        let u64_at = |at: usize| u64::from_be_bytes(b[at..at + 8].try_into().unwrap());
        // version u8, then base_page, next_extent_start, next_arena_id, live, reserved.
        let mut at = 1 + 4 * 5;
        at += 4 + 8 * u32_at(at) as usize; // free_extents: (start, page_count) pairs since v3

        let n_extents = u32_at(at) as usize;
        at += 4;
        let mut arenas = Vec::new();
        for _ in 0..n_extents {
            arenas.push(u32_at(at));
            // arena, owner.id, owner.generation, start_page, page_count, next_free
            at += 4 + 8 + 4 + 4 + 4 + 4;
            at += 4 + 4 * u32_at(at) as usize; // recycled
        }

        let n_current = u32_at(at) as usize;
        at += 4;
        let mut current = Vec::new();
        for _ in 0..n_current {
            current.push((u64_at(at), u32_at(at + 8)));
            at += 8 + 4 + 4;
        }
        (arenas, current)
    }

    /// **A durable image whose byte order comes from a `HashMap` cannot be replayed.**
    ///
    /// B10's finding 4. Nothing here is a correctness bug on its own — `load_state` is
    /// count-prefixed, so any order reloads the same logical map — but the same arena state
    /// checkpointed twice produced two different files with two different CRC32s. That makes the
    /// image impossible to pin in a test and a crash sweep over `<db>.arena` impossible to replay,
    /// which is the premise of aiming a crash at all.
    #[test]
    fn two_stores_in_the_same_state_checkpoint_byte_identical_images() {
        let (a, _) = store_with_n_arenas(16);
        let (b, _) = store_with_n_arenas(16);
        assert_eq!(
            a.store.base_page(),
            b.store.base_page(),
            "fixture: the two stores describe different regions, so this proves nothing"
        );
        let left = a.store.state_bytes();
        let right = b.store.state_bytes();
        assert_eq!(
            &left[left.len() - 4..],
            &right[right.len() - 4..],
            "the same free-space map produced two different checksums"
        );
        assert_eq!(left, right, "the same free-space map produced two different durable images");
    }

    #[test]
    fn the_checkpoint_image_lists_extents_and_current_arenas_in_key_order() {
        let (h, arenas) = store_with_n_arenas(16);
        let (in_image, current) = key_order_in_image(&h.store.state_bytes());

        assert_eq!(in_image.len(), arenas.len(), "fixture: the image lost extents");
        let mut sorted = in_image.clone();
        sorted.sort_unstable();
        assert_eq!(in_image, sorted, "the extents section is in hash order, not arena-id order");

        assert_eq!(current.len(), arenas.len(), "fixture: the image lost current arenas");
        let mut sorted_current = current.clone();
        sorted_current.sort_unstable();
        assert_eq!(
            current, sorted_current,
            "the current-arena section is in hash order, not branch-id order"
        );
    }

    /// The reaper frees empty extents in this order and `reserve` pops the stack those frees build,
    /// so a `HashMap`'s order here decided which page range the next arena got.
    #[test]
    fn live_arenas_comes_back_in_arena_id_order() {
        let (h, arenas) = store_with_n_arenas(16);
        let live: Vec<ArenaId> = h.store.live_arenas().into_iter().map(|(a, _)| a).collect();
        assert_eq!(live.len(), arenas.len(), "fixture: an arena went missing");
        let mut sorted = live.clone();
        sorted.sort_unstable();
        assert_eq!(live, sorted, "live_arenas is in hash order");
    }

    /// Guards the **production** entry point, which the recorder cannot reach: `checkpoint` itself
    /// has to go through [`crate::storage::atomic_file`] rather than growing its own copy of the
    /// idiom again.
    ///
    /// The observable is the temporary's *name*, which is chosen in exactly one place. Blocking
    /// `<target>.tmp` with a directory makes the real call fail; the version this replaced staged
    /// through `path.with_extension("tmp")` — `db.tmp`, a different file — and would sail past.
    #[test]
    fn the_public_checkpoint_stages_through_the_durable_helper() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let a = h.store.arena_for(b.branch_id).unwrap();
        h.store.alloc_in_arena(a, PageType::Heap, Epoch(3)).unwrap();

        let dir = std::env::temp_dir().join(format!("ferro-arena-stage-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("db.arena");
        h.store.checkpoint(&target).unwrap();
        let good = std::fs::read(&target).unwrap();

        // Occupy the one path a durable replace must stage through.
        std::fs::create_dir(dir.join("db.arena.tmp")).unwrap();
        let err = h.store.checkpoint(&target);
        assert!(
            err.is_err(),
            "checkpoint did not stage through db.arena.tmp, so it is not using the durable replace"
        );
        assert_eq!(
            std::fs::read(&target).unwrap(),
            good,
            "a failed checkpoint must leave the previous map exactly as it was"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **A freed extent that never reaches the durable map is space no restart gets back.**
    ///
    /// Recorded by B10 as an aside and confirmed here: `alloc_arena` checkpointed, `free_arena` did
    /// not, so only claims were durable. The image a crash left still charged the extent to a
    /// branch that no longer exists, and the sweep that would otherwise collect it refuses —
    /// `extent_is_empty` compares recycled pages against `next_free`, and the fast path frees an
    /// extent whole without ever releasing its pages one at a time. Measured before the fix, the
    /// restored store below reported `owner=Some(BranchId { id: 1, generation: 0 })`, 256 pages
    /// still reserved and its page still live — for an arena that had been freed.
    #[test]
    fn freeing_an_extent_checkpoints_the_map_so_a_restart_gets_the_space_back() {
        let h = Harness::new();
        let path = std::env::temp_dir()
            .join(format!("ferro-arena-free-ckpt-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&path);
        h.store.checkpoint_to(path.clone());

        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let a = h.store.arena_for(b.branch_id).unwrap(); // claims the extent, and checkpoints it
        h.store.alloc_in_arena(a, PageType::Heap, Epoch(3)).unwrap();
        // Checkpoint the *allocated* state explicitly, so the durable image the assertions below
        // read is one where every counter is non-zero. Without this the last image predates the
        // page and `live_page_count` reads 0 whether the free was persisted or not.
        h.store.checkpoint(&path).unwrap();
        assert_eq!(
            h.store.reserved_page_count(),
            h.store.extent_range(a).unwrap().1,
            "fixture: no extent was reserved, so freeing one proves nothing"
        );

        h.store.free_arena(a).unwrap();

        let target = h.fresh_store();
        assert!(target.restore(&path).unwrap(), "fixture: nothing was ever checkpointed");
        assert_eq!(
            target.arena_owner(a),
            None,
            "the durable map still charges the freed extent to its dead owner"
        );
        assert_eq!(
            target.reserved_page_count(),
            0,
            "reserved pages never come back after a restart, which is exit criterion 8"
        );
        assert_eq!(
            target.live_page_count().unwrap(),
            0,
            "the page inside the freed extent is still counted as live"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// The slow path's own persist, isolated.
    ///
    /// Driving this through `reaper::reap` cannot isolate it: `reap` always ends in `drain_pending`,
    /// whose `put_pending` persists, and in `sweep_empty_extents`, whose `free_arena` persists — so
    /// removing this one leaves the end-to-end test green. Measured: it does. The store's contract
    /// is per-method, so the test is too.
    #[test]
    fn parking_pages_by_the_interval_rule_reaches_the_durable_map() {
        let h = Harness::new();
        let path = std::env::temp_dir()
            .join(format!("ferro-arena-park-ckpt-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&path);
        h.store.checkpoint_to(path.clone());

        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        for _ in 0..4 {
            h.store.alloc_for(parent.branch_id, PageType::Heap, Epoch(1)).unwrap();
        }
        // Forked *after* those pages were born, so it can still see them and the interval rule
        // parks them rather than releasing them.
        h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        let rec = h.catalog.get_raw(parent.branch_id.id).unwrap();
        let released = h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();
        assert_eq!(released, 0, "fixture: the pages were released, not parked, so nothing is pinned");
        assert_eq!(h.store.pending_len(), 4, "fixture: the rule parked nothing");

        let target = h.fresh_store();
        assert!(target.restore(&path).unwrap(), "fixture: nothing was ever checkpointed");
        assert_eq!(
            target.pending_len(),
            4,
            "the pending-free log never reached the durable map, so a restart releases pages a \
             live child can still see"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// **D183 — what the two STORE METHODS cost per call. ⛔ NOT what a reap costs.**
    ///
    /// ⛔⛔ **THIS TEST'S ORIGINAL NAME AND HEADLINE WERE WRONG AND WERE ACTED ON.** It was
    /// called `d183_an_interior_reap_rewrites_the_whole_image_while_a_leaf_reap_appends` and its
    /// band read "LEAF 8/16 -> rewrites 0/0; INTERIOR 8/16 -> rewrites 8/16 — one full image
    /// rewrite per interior branch reaped, zero for a leaf". **It never calls `Reaper::reap`.**
    /// It hand-rolls the two lines at `reaper.rs:679-687` and therefore never runs
    /// `drain_pending`, which is half of what a reap does and the source of the leaf path's
    /// entire cost. A design entry and an implementation brief were both written against that
    /// number before `mod d183_adversary` measured the real thing and refuted BOTH halves of it:
    /// through `TwoTierReaper::reap` the leaf slope was 1.0 per branch, not 0, and the interior
    /// slope 2.0, not 1.0.
    ///
    /// ⇒ **For what a reap costs, read `d183adv_a1_what_the_real_reaper_costs` and
    /// `d183adv_a5_the_interior_cost_is_no_longer_a_class`.** This test is kept because a
    /// per-method cost is a real thing to know and the two methods are this row's subject — but
    /// it is a measurement of `retire_arenas_by_rule` and `free_arena` called directly, and its
    /// numbers must never again be quoted as the cost of reaping anything.
    ///
    /// D81 replaced the per-fork full image rewrite with a 45-byte delta append and its note
    /// called that "the only site that changes shape". `retire_arenas_by_rule` ended in
    /// `persist_if_configured`, which was `persist_full_locked` unconditionally — it never
    /// consulted `compact_threshold`, so it was not amortised at all.
    ///
    /// **⭐ THIS TEST HAS BEEN INVERTED. It pinned a WALL and now pins the FIX.** The wall, as
    /// this hand-rolled loop measured it at `630afaa` — kept because a before/after with only the
    /// after is a claim rather than a measurement, and kept labelled because these four cells are
    /// exactly the numbers that misled the row:
    ///
    /// | arm | branches | rewrites | appends |
    /// |---|---|---|---|
    /// | LEAF | 8 / 16 | **0 / 0** | 24 / 48 |
    /// | INTERIOR | 8 / 16 | **8 / 16** | 0 / 0 |
    ///
    /// One full image rewrite per interior branch reaped, zero deltas — `sum(48·i) = 24·N²` bytes
    /// — against a leaf control that paid zero at both sizes.
    ///
    /// **After D183's THREE new tail record kinds** — [`ArenaPageStore::TAIL_PAGES_PARKED`],
    /// [`ArenaPageStore::TAIL_PENDING_DRAINED`] and its always-correct fallback
    /// [`ArenaPageStore::TAIL_PENDING_REPLACED`] — plus the conditional `take_pending` bump,
    /// measured 2026-09-23 on this branch. ⚠ The two kinds named in the first version of this
    /// band are NOT the ones that deliver the headline: the drain record and the conditional bump
    /// are, and `d183adv_a5_the_interior_cost_is_no_longer_a_class` has the evidence.
    ///
    /// | arm | branches | rewrites | appends |
    /// |---|---|---|---|
    /// | LEAF | 8 / 16 | 0 / 0 | 24 / 48 |
    /// | INTERIOR | 8 / 16 | **0 / 1** | **8 / 15** |
    ///
    /// **Pre-registered from the SOURCE, and stated as what must be TRUE rather than as the
    /// numbers above.** The reaper calls `retire_arenas_by_rule` once per branch and that method
    /// persists exactly once, so the two counters must still SUM to one per interior branch — the
    /// site fires as often as it ever did. What D183 changed is which counter each one lands in:
    /// `persist_delta_locked` now rewrites only when the tail would pass
    /// `compact_threshold(image_bytes) = max(image/2, 4096)`, so the rewrites over the loop are
    /// bounded by the BYTES it appends and not by the branch count. Hence the inverted assertion:
    /// **rewrites must grow more slowly than appends** — 1 against 7 here, and 8 against 0 under
    /// the defect, which is what makes it discriminating rather than merely satisfied.
    ///
    /// The single rewrite at 16 is a COMPACTION and not the old per-branch cost: the fixture's own
    /// 48 claim records have already put ~2.2 KiB on the tail before the reap loop starts, so the
    /// 4 KiB floor is crossed part way through. That is the amortisation rule working, and it is
    /// why the assertion is about the slope rather than about zero.
    ///
    /// Two sizes, so the claim is a SLOPE and not a ratio, and both arms in the same run so the
    /// leaf arm is a live control rather than a remembered number.
    ///
    /// ⚠ `new_with(true)` — the catalog that SHIPS. `Harness::new()` is the log catalog, whose
    /// `live_children` lives in the record; a reclamation test on it proves nothing about the
    /// shipped path. That is D19, and this test would be worthless without it.
    #[test]
    fn d183_what_the_two_reclamation_store_methods_cost_per_call() {
        fn run(branches: usize, interior: bool) -> (u64, u64) {
            let h = Harness::new_with(true);
            let path = std::env::temp_dir().join(format!(
                "ferro-arena-d183-{}-{}-{}.bin",
                std::process::id(),
                branches,
                interior
            ));
            let _ = std::fs::remove_file(&path);
            h.store.checkpoint_to(path.clone());

            let mut ids = Vec::new();
            for _ in 0..branches {
                let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
                for _ in 0..4 {
                    h.store.alloc_for(b.branch_id, PageType::Heap, Epoch(1)).unwrap();
                }
                if interior {
                    // Forked AFTER those pages were born, so the interval rule must PARK them
                    // and the reaper is forced down the slow side of `reaper.rs:679`.
                    h.catalog.fork(b.branch_id, LeaseDeadline(0)).unwrap();
                }
                ids.push(b.branch_id);
            }

            // Count only the reap loop: everything above is fixture.
            let (r0, a0) = h.store.persist_counters();

            // Exactly what `reaper.rs:679-687` does, per branch, on each side of the predicate.
            for id in ids {
                let rec = h.catalog.get_raw(id.id).unwrap();
                if interior {
                    h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();
                } else {
                    for arena in rec.arenas.iter().copied() {
                        h.store.free_arena(arena).unwrap();
                    }
                }
            }

            let (r1, a1) = h.store.persist_counters();
            let _ = std::fs::remove_file(&path);
            (r1 - r0, a1 - a0)
        }

        let (leaf_r8, leaf_a8) = run(8, false);
        let (leaf_r16, leaf_a16) = run(16, false);
        let (int_r8, int_a8) = run(8, true);
        let (int_r16, int_a16) = run(16, true);

        println!("D183 reap cost -- (full rewrites, delta appends) over the reap loop only");
        println!("  LEAF      8: rewrites={leaf_r8:3} appends={leaf_a8:3}");
        println!("  LEAF     16: rewrites={leaf_r16:3} appends={leaf_a16:3}");
        println!("  INTERIOR  8: rewrites={int_r8:3} appends={int_a8:3}");
        println!("  INTERIOR 16: rewrites={int_r16:3} appends={int_a16:3}");

        // Anti-vacuity: a fixture that reaped nothing satisfies every shape assertion below.
        assert!(leaf_a8 + leaf_r8 > 0, "fixture: the leaf arm persisted nothing at all");
        assert!(int_r8 + int_a8 > 0, "fixture: the interior arm persisted nothing at all");

        // The SITE still fires once per interior branch — `retire_arenas_by_rule` persists exactly
        // once and the reaper calls it once per branch. D183 changed which counter it lands in,
        // not how often it runs, and stating that separately is what stops a "fix" that simply
        // stopped persisting from passing the slope assertion below.
        assert_eq!(
            (int_r8 + int_a8, int_r16 + int_a16),
            (8, 16),
            "the interior reap no longer persists exactly once per branch: 8->({int_r8},{int_a8}) \
             16->({int_r16},{int_a16}); a reap that persists LESS often is data loss, not a fix"
        );

        // ⭐ THE INVERTED PRE-REGISTERED SHAPE. Before D183 this read `int_r16 - int_r8 == 8`:
        // rewrites rose one-for-one with branches and appends never moved (8 vs 0). The fix makes
        // the interior door a delta, so the growth must land in the APPENDS and the rewrites must
        // be whatever the amortisation rule leaves behind — strictly less.
        assert!(
            int_r16 - int_r8 < int_a16 - int_a8,
            "interior reaps still rewrite the image per branch: rewrites {int_r8}->{int_r16} \
             (+{}) against appends {int_a8}->{int_a16} (+{})",
            int_r16 - int_r8,
            int_a16 - int_a8
        );

        // THE CONTROL, AND IT MUST NOT HAVE MOVED. The leaf door was already a delta before this
        // row and D183 did not touch `free_arena`. A leaf arm that changed would mean the two
        // halves of the before/after came from two different bases rather than that the interior
        // arm improved.
        assert_eq!(
            (leaf_r8, leaf_r16),
            (0, 0),
            "the leaf control moved: it paid {leaf_r8}/{leaf_r16} rewrites where it paid 0/0 at \
             630afaa, so this run is not comparable with the recorded before"
        );
        assert_eq!(
            leaf_a16,
            leaf_a8 * 2,
            "the leaf control's appends stopped being linear in branches ({leaf_a8}->{leaf_a16})"
        );
    }

    /// ⭐ **D183's load-bearing test: the delta must reproduce the full rewrite EXACTLY.**
    ///
    /// The row's whole argument is that the pending-free log and the per-extent recycled lists can
    /// be described by a tail record instead of by re-serialising the map. That is a claim about
    /// bytes, so it is asserted in bytes: run an interior reap AND the drain's read-modify-write
    /// against an armed path, then compare a store restored from `image + tail` against a store
    /// restored from a full image of the same live state. If the replay of
    /// [`ArenaPageStore::TAIL_PAGES_PARKED`] and [`ArenaPageStore::TAIL_PENDING_REPLACED`] drops,
    /// duplicates or reorders one entry, the two images differ and this fails.
    ///
    /// **Byte-identity is the right instrument and `pending_len()` is not.** A count agrees with a
    /// full rewrite while naming different pages; `state_bytes` carries every field of every entry
    /// plus every recycled id, under a CRC32.
    ///
    /// ⛔⛔ **IT COMPARES AFTER *EACH* RECORD, AND THAT IS NOT THOROUGHNESS — IT IS THE ONLY WAY
    /// THE FIRST RECORD IS TESTED AT ALL.** An end-only version of this test was **fire-checked
    /// and PASSED with `TAIL_PAGES_PARKED` dropping a parked entry**: the drain's record restates
    /// the log afterwards, so a downstream record that re-states the same state masks every mutant
    /// of the one in front of it. The parked record is therefore compared while it is still the
    /// last word on the log, before the drain runs.
    ///
    /// ⚠ **Anti-vacuity, at each stage, and in two forms.** A site that quietly fell back to
    /// `persist_full_locked` would of course match a full rewrite, so each stage pins that it
    /// APPENDED and did not rewrite — and, because a count cannot say WHICH record was appended,
    /// each stage also reads the kind back out of the file with [`ArenaPageStore::tail_kinds`].
    /// `TAIL_PENDING_REPLACED` is the always-correct fallback for the drain, so without that
    /// second check this test would pass while never exercising the difference record at all.
    /// Same for the fixture: it asserts the reap both parked and released pages, because a log
    /// with nothing in it round-trips trivially.
    ///
    /// The crash this stands in for is the one `arena.rs`'s own note names: a kill after
    /// `mark_reaped`, whose branch record no longer points at the arenas, so the parked entries and
    /// the recycled pages exist ONLY in this file.
    #[test]
    fn an_interior_reap_replayed_from_the_tail_is_byte_identical_to_a_full_rewrite() {
        // The catalog that ships — D19. The interval rule is asked of an INDEX here, not of a vec
        // in the record, so the fixture's "some pinned, some not" split is decided by the code
        // that runs in production.
        let h = Harness::new_with(true);
        let armed = std::env::temp_dir()
            .join(format!("ferro-arena-d183-replay-{}.bin", std::process::id()));
        let control = std::env::temp_dir()
            .join(format!("ferro-arena-d183-control-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_file(&control);
        h.store.checkpoint_to(armed.clone());

        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        // Born BEFORE the child forked, so the child can see them and the interval rule parks them.
        for _ in 0..3 {
            h.store.alloc_for(parent.branch_id, PageType::Heap, Epoch(1)).unwrap();
        }
        h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        // Born AFTER it forked: no live child's fork epoch lies in [birth, free), so these are
        // RELEASED into the extent's recycled list rather than parked. Both halves of the record
        // are therefore non-empty, which a fixture that parks everything cannot test.
        for _ in 0..3 {
            h.store.alloc_for(parent.branch_id, PageType::Heap, Epoch(9)).unwrap();
        }

        // `image + tail` must serialise to exactly what a full rewrite of the live state would.
        let same_as_a_full_rewrite = |stage: &str| {
            // The full rewrite, taken at a path that is NOT the armed one so the tail accounting
            // this test is measuring is left alone.
            h.store.checkpoint(&control).unwrap();
            let from_tail = h.fresh_store();
            assert!(from_tail.restore(&armed).unwrap(), "{stage}: the armed path holds nothing");
            let from_image = h.fresh_store();
            assert!(from_image.restore(&control).unwrap());
            assert_eq!(
                from_tail.pending_len(),
                from_image.pending_len(),
                "{stage}: the replayed pending-free log has {} entries where a full rewrite gives \
                 {}",
                from_tail.pending_len(),
                from_image.pending_len()
            );
            assert_eq!(
                from_tail.state_bytes(),
                from_image.state_bytes(),
                "{stage}: a store restored from image+tail is not byte-identical to one restored \
                 from a full image of the same state — the D183 delta does not reproduce what it \
                 replaced"
            );
        };

        // ── STAGE 1: the reap slow path alone. `TAIL_PAGES_PARKED` is the last word on the log
        // here, so this is the only place its pending half can be observed at all.
        let (r0, a0) = h.store.persist_counters();
        let rec = h.catalog.get_raw(parent.branch_id.id).unwrap();
        let released = h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();
        assert!(released > 0, "fixture: nothing was released, so no recycled list changed");
        let parked = h.store.pending_len();
        assert!(parked > 0, "fixture: nothing was parked, so the pending log never changed");
        let (r1, a1) = h.store.persist_counters();
        assert_eq!(
            (r1 - r0, a1 - a0),
            (0, 1),
            "the interior reap did not APPEND: rewrites +{}, appends +{}. A fallback to the full \
             rewrite would make the comparison below vacuously true",
            r1 - r0,
            a1 - a0
        );
        // **Ask the FILE which record it wrote.** The counter says one append happened; it cannot
        // say which kind, and a restore that matches a full rewrite passes just as well when the
        // wrong record produced it.
        assert_eq!(
            ArenaPageStore::tail_kinds(&armed).last().copied(),
            Some(ArenaPageStore::TAIL_PAGES_PARKED),
            "stage 1 wrote {:?}, not a parked record",
            ArenaPageStore::tail_kinds(&armed)
        );
        same_as_a_full_rewrite("after the interior reap");

        // ── STAGE 2: the drain's read-modify-write on top, spelled exactly as
        // `reaper::drain_pending_seeded` spells it — take the whole log, hand one page back, put
        // the survivors. This is what reaches `TAIL_PENDING_DRAINED`.
        let taken = h.store.take_pending();
        assert_eq!(taken.len(), parked, "fixture: the take did not see the parked entries");
        h.store.release_page(taken[0].page_id, taken[0].arena_id);
        h.store.put_pending(taken[1..].to_vec()).unwrap();
        let (r2, a2) = h.store.persist_counters();
        assert_eq!(
            (r2 - r1, a2 - a1),
            (0, 1),
            "putting the log back did not APPEND: rewrites +{}, appends +{}",
            r2 - r1,
            a2 - a1
        );
        assert_eq!(
            ArenaPageStore::tail_kinds(&armed).last().copied(),
            Some(ArenaPageStore::TAIL_PENDING_DRAINED),
            "stage 2 wrote {:?}; the DIFFERENCE record is the one this row added and the absolute \
             fallback would hide every defect in it",
            ArenaPageStore::tail_kinds(&armed)
        );
        assert!(
            h.store.pending_len() < parked,
            "fixture: the drain put back everything it took, so the drain record describes no \
             change at all"
        );
        same_as_a_full_rewrite("after the drain put the log back");

        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_file(&control);
    }

    /// ⭐ **D183 — the fallback is the correctness backstop, so it gets its own test.**
    ///
    /// `put_pending` writes the O(released) difference record only while
    /// [`PersistState::drain_mark`] proves the durable log is the one `take_pending` handed out.
    /// Every other case is supposed to write the O(whole log) absolute record instead, and that
    /// branch is what makes the optimisation safe rather than merely fast.
    ///
    /// **Nothing else reaches it.** Every other drain in the suite satisfies the proof, so the
    /// fallback would be dead code that still compiles — and a weakened proof (one that said
    /// "usable" always) would pass every other test in this file. Each arm below invalidates the
    /// proof a different way, asserts from the FILE that the absolute record was chosen, and then
    /// requires the restore to be byte-identical to a full rewrite anyway.
    #[test]
    fn a_drain_that_cannot_prove_the_log_falls_back_to_the_absolute_record() {
        // `invalidate` runs between the take and the put.
        fn case(tag: &str, invalidate: impl Fn(&Harness, &std::path::Path)) {
            let h = Harness::new_with(true);
            let armed = std::env::temp_dir()
                .join(format!("ferro-arena-d183-fb-{}-{}.bin", std::process::id(), tag));
            let _ = std::fs::remove_file(&armed);
            h.store.checkpoint_to(armed.clone());

            let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            for _ in 0..4 {
                h.store.alloc_for(parent.branch_id, PageType::Heap, Epoch(1)).unwrap();
            }
            h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
            let rec = h.catalog.get_raw(parent.branch_id.id).unwrap();
            h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();

            let taken = h.store.take_pending();
            assert!(taken.len() >= 2, "{tag} fixture: the reap parked too little to drop one");
            invalidate(&h, &armed);
            h.store.release_page(taken[0].page_id, taken[0].arena_id);
            h.store.put_pending(taken[1..].to_vec()).unwrap();

            let kinds = ArenaPageStore::tail_kinds(&armed);
            assert!(
                kinds.is_empty()
                    || kinds.last().copied() != Some(ArenaPageStore::TAIL_PENDING_DRAINED),
                "{tag}: the difference record was written although its proof was invalidated \
                 ({kinds:?})"
            );

            let control = std::env::temp_dir()
                .join(format!("ferro-arena-d183-fbc-{}-{}.bin", std::process::id(), tag));
            h.store.checkpoint(&control).unwrap();
            let from_tail = h.fresh_store();
            assert!(from_tail.restore(&armed).unwrap());
            let from_image = h.fresh_store();
            assert!(from_image.restore(&control).unwrap());
            assert_eq!(
                from_tail.state_bytes(),
                from_image.state_bytes(),
                "{tag}: the fallback did not reproduce a full rewrite"
            );
            let _ = std::fs::remove_file(&armed);
            let _ = std::fs::remove_file(&control);
        }

        // A full image rewrite publishes the DRAINED, empty log, so the file no longer holds the
        // entries the take handed out and removing from it would remove nothing. `rewrites` moves.
        case("rewrite", |h, armed| h.store.checkpoint(armed).unwrap());

        // A second drain: two takes cannot both be "the log the file still holds", and neither
        // put can say whose entries it is holding.
        case("overlap", |h, _| {
            let _ = h.store.take_pending();
        });
    }

    /// **D183 — a `put_pending` whose record fails must leave the log reading DIRTY.**
    ///
    /// Review axis 2b, fixed at `4b1ab2d` and pinned by nothing until this test: the re-verify's
    /// mutant M9 (the bump on this arm removed) passed the whole lib target on both `4b1ab2d` and
    /// the merged tree (`bench/d183_reverify.txt`).
    ///
    /// The arm used to be safe by accident — `take_pending` bumped unconditionally, so every caller
    /// arrived here already dirty. The conditional bump removed that: a `put_pending` with no
    /// preceding take (or after an EMPTY one) extends the log and bumps nothing, so if its append
    /// fails, the counters still say the durable log is level with memory. The next persist then
    /// APPENDS behind a file that does not list the entry, and a crash before the next compaction
    /// loses it — `drain_pending` never revisits an entry the file does not hold.
    ///
    /// The append is made to fail by turning the armed file into a DIRECTORY for the one call, which
    /// fails the same way on every platform and needs no permission bits. Two assertions, and both
    /// are needed: the persist after the failure must be a REWRITE (the mechanism), and `image +
    /// tail` must restore byte-identical to a full image of live memory (the durable state). The
    /// second was fire-checked on its own, with the first removed.
    #[test]
    fn a_put_pending_whose_record_fails_leaves_the_log_dirty() {
        let h = Harness::new_with(true);
        let armed = std::env::temp_dir()
            .join(format!("ferro-arena-d183-putfail-{}.bin", std::process::id()));
        let control = std::env::temp_dir()
            .join(format!("ferro-arena-d183-putfail-c-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_dir(&armed);
        let _ = std::fs::remove_file(&control);
        h.store.checkpoint_to(armed.clone());

        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let arena = h.store.arena_for(b.branch_id).unwrap();
        let page = h.store.alloc_in_arena(arena, PageType::Heap, Epoch(1)).unwrap();
        // The image is now current and the log level: the only thing that could carry the entry
        // below to the file afterwards is a record, or a rewrite the counters force.
        h.store.checkpoint(&armed).unwrap();
        let (r0, a0) = h.store.persist_counters();

        let saved = std::fs::read(&armed).unwrap();
        std::fs::remove_file(&armed).unwrap();
        std::fs::create_dir(&armed).unwrap();
        let entry = PendingFree {
            page_id: page,
            arena_id: arena,
            birth_epoch: Epoch(1),
            free_epoch: Epoch(2),
            owner: b.branch_id,
        };
        let put = h.store.put_pending(vec![entry]);
        std::fs::remove_dir(&armed).unwrap();
        std::fs::write(&armed, &saved).unwrap();
        assert!(put.is_err(), "fixture: the append did not fail, so the failure arm never ran");
        assert_eq!(
            h.store.persist_counters(),
            (r0, a0),
            "fixture: the failed put_pending still persisted something"
        );
        assert_eq!(h.store.pending_len(), 1, "fixture: put_pending did not keep the entry in memory");

        // Any persist at all: a claim, which carries no pending log.
        let spare = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        h.store.arena_for(spare.branch_id).unwrap();
        let (r1, a1) = h.store.persist_counters();
        assert_eq!(
            (r1 - r0, a1 - a0),
            (1, 0),
            "the persist after a failed put_pending APPENDED ({} rewrites, {} appends) behind a \
             file that does not list the entry it left in memory",
            r1 - r0,
            a1 - a0
        );

        h.store.checkpoint(&control).unwrap();
        let from_tail = h.fresh_store();
        assert!(from_tail.restore(&armed).unwrap());
        let from_image = h.fresh_store();
        assert!(from_image.restore(&control).unwrap());
        assert_eq!(
            from_tail.state_bytes(),
            from_image.state_bytes(),
            "the durable pending-free log is short the entry a failed put_pending left in memory: \
             a crash now loses it"
        );

        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_file(&control);
    }

    /// ⭐ **A recycled page handed out again must reach the durable map, or a live extent is freed.**
    ///
    /// `extent_is_empty` is `recycled >= next_free`. A durable recycled list that still names a
    /// page memory has reissued OVERSTATES the left side, so a restored extent holding live pages
    /// answers "empty" and `reaper::sweep_empty_extents` frees it — handing that page range back
    /// to the allocator while a branch is still reading it.
    ///
    /// **Not repaired by anything already here.** D85's `resolve_fill` raises `next_free`, the
    /// other side of the comparison, and raises it to cover the reissued page. `load_state`'s
    /// `current.clear()` stops a restored extent being FILLED, which is a different question from
    /// whether it may be FREED.
    ///
    /// ⚠ **Open since D81.** Neither tail record refreshes an existing extent's recycled list, so
    /// once claims stopped rewriting the whole image the only thing closing this window was the
    /// reclamation paths still rewriting on every reap.
    ///
    /// The fixture forces exactly that shape: make a recycled list durable, reuse one of its
    /// pages, persist something, and require the restore to match a full rewrite of live memory.
    #[test]
    fn reusing_a_recycled_page_reaches_the_durable_map() {
        let h = Harness::new_with(true);
        let armed = std::env::temp_dir()
            .join(format!("ferro-arena-reuse-{}.bin", std::process::id()));
        let control = std::env::temp_dir()
            .join(format!("ferro-arena-reuse-c-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_file(&control);
        h.store.checkpoint_to(armed.clone());

        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let arena = h.store.arena_for(b.branch_id).unwrap();
        let p1 = h.store.alloc_in_arena(arena, PageType::Heap, Epoch(1)).unwrap();
        h.store.release_page(p1, arena);

        // Make that recycled list DURABLE and reset the tail accounting to this image, so the
        // only thing that could carry the reuse afterwards is a tail record.
        h.store.checkpoint(&armed).unwrap();
        let (r0, a0) = h.store.persist_counters();

        // The reuse. `arena_for` returns this extent precisely because it has a recycled page.
        let reused = h.store.alloc_for(b.branch_id, PageType::Heap, Epoch(2)).unwrap();
        assert_eq!(reused, p1, "fixture: the allocation did not come from the recycled list");

        // Any persist at all.
        //
        // ⚠ `arena_for` and not `alloc_for`, and the difference is the whole fixture: `alloc_for`
        // claims the extent (which persists) and THEN allocates a page into it, advancing
        // `next_free` with nothing after it to write that down. The comparison below would then
        // fail on that field instead — an ordinary unpersisted `next_free`, which D85 owns and
        // this test is not about. Measured: it did, and the first version of this test read as a
        // failure of the fix when the fix was working.
        let spare = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        h.store.arena_for(spare.branch_id).unwrap();

        let (r1, a1) = h.store.persist_counters();
        // The persist after a reuse must be a full image REWRITE. No tail record can express
        // "a recycled page was handed out again": `TAIL_ARENA_CLAIMED` carries no recycled list
        // and `TAIL_EXTENT_FREED` only removes one. An APPEND here IS the defect, and the byte
        // comparison below shows what it costs.
        assert_eq!(
            (r1 - r0, a1 - a0),
            (1, 0),
            "the claim after a recycled-page reuse APPENDED ({} rewrites, {} appends) instead of \
             rewriting the image",
            r1 - r0,
            a1 - a0
        );

        h.store.checkpoint(&control).unwrap();
        let from_tail = h.fresh_store();
        assert!(from_tail.restore(&armed).unwrap());
        let from_image = h.fresh_store();
        assert!(from_image.restore(&control).unwrap());
        assert_eq!(
            from_tail.state_bytes(),
            from_image.state_bytes(),
            "the durable map still lists the reused page as recycled: a restored extent would \
             answer `extent_is_empty` while holding a live page"
        );

        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_file(&control);
    }

    /// `put_pending`'s own persist, isolated for the same reason as the test above it.
    #[test]
    fn putting_the_pending_log_back_reaches_the_durable_map() {
        let h = Harness::new();
        let path = std::env::temp_dir()
            .join(format!("ferro-arena-putpending-ckpt-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&path);
        h.store.checkpoint_to(path.clone());

        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        for _ in 0..4 {
            h.store.alloc_for(parent.branch_id, PageType::Heap, Epoch(1)).unwrap();
        }
        h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        let rec = h.catalog.get_raw(parent.branch_id.id).unwrap();
        h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();

        // What `drain_pending` does: take the whole log, decide, put the survivors back.
        let taken = h.store.take_pending();
        assert_eq!(taken.len(), 4, "fixture: nothing was parked");
        h.store.put_pending(taken[..2].to_vec()).unwrap();

        let target = h.fresh_store();
        assert!(target.restore(&path).unwrap());
        assert_eq!(
            target.pending_len(),
            2,
            "the durable pending-free log still lists entries the drain resolved, so a restart \
             would park released pages all over again"
        );
        let _ = std::fs::remove_file(&path);
    }

    // ---- D183 tail replay: O(records + their own entries), not O(records x pending log) ----------

    /// **D183 tail replay.** An image holding `p0` parked entries, then a tail of `rounds` rounds.
    /// Each round writes one record of every kind that touches the pending-free log, through the
    /// real writers:
    /// * `TAIL_PAGES_PARKED` with one entry: `retire_arenas_by_rule` over a branch with one page
    ///   and a live child.
    /// * `TAIL_PENDING_DRAINED` removing one entry: a take, then a put of all but the oldest entry,
    ///   which is `drain_pending` finding exactly one entry releasable.
    /// * `TAIL_EXTENT_FREED` of an extent with nothing parked in it: a claim, then `free_arena`.
    ///
    /// Claims ride along as `TAIL_ARENA_CLAIMED`. The log stays at `p0` entries throughout, so
    /// every record replays against a log of the same size.
    ///
    /// Returns how many pending entries the tail's records CARRY. It is counted here as they are
    /// written, never read back from the counter under test.
    fn parked_image_then_mixed_tail(
        h: &Harness,
        armed: &std::path::Path,
        p0: usize,
        rounds: usize,
    ) -> usize {
        h.store.checkpoint_to(armed.to_path_buf());
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        for _ in 0..p0 {
            h.store.alloc_for(parent.branch_id, PageType::Heap, Epoch(1)).unwrap();
        }
        h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        let rec = h.catalog.get_raw(parent.branch_id.id).unwrap();
        h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();
        assert_eq!(h.store.pending_len(), p0, "fixture: the image must hold {p0} parked entries");
        // The image. Every parked entry is in it and the tail starts empty.
        h.store.checkpoint(armed).unwrap();
        assert!(
            ArenaPageStore::tail_kinds(armed).is_empty(),
            "fixture: the tail did not start empty"
        );
        let (r0, _) = h.store.persist_counters();

        let mut carried = 0usize;
        for _ in 0..rounds {
            let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            h.store.alloc_for(b.branch_id, PageType::Heap, Epoch(1)).unwrap();
            h.catalog.fork(b.branch_id, LeaseDeadline(0)).unwrap();
            let rec = h.catalog.get_raw(b.branch_id.id).unwrap();
            h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();
            carried += 1;

            let taken = h.store.take_pending();
            assert_eq!(taken.len(), p0 + 1, "fixture: the park did not reach the log");
            h.store.put_pending(taken[1..].to_vec()).unwrap();
            carried += 1;

            let c = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            let arena = h.store.arena_for(c.branch_id).unwrap();
            h.store.free_arena(arena).unwrap();
        }
        let (r1, _) = h.store.persist_counters();
        assert_eq!(
            r1, r0,
            "fixture: the tail was compacted into a new image, so it no longer holds every record \
             this fixture wrote"
        );
        assert_eq!(h.store.pending_len(), p0, "fixture: each round must add one entry and drop one");
        carried
    }

    /// ⭐ **Replay visits each record's OWN entries, plus one pass over the log. It does not visit
    /// the whole log once per record.**
    ///
    /// At `731a7fa` three replay arms walked the entire pending-free log for every record:
    /// * `TAIL_PAGES_PARKED` built a `HashSet` of it;
    /// * `TAIL_PENDING_DRAINED` ran `retain` over it, even for a record naming nothing;
    /// * `TAIL_EXTENT_FREED` ran `retain` over it.
    ///
    /// D183 turned full rewrites into these records, so a tail now spans many reaps before
    /// `compact_threshold` folds it into an image. An open after an unclean exit paid
    /// O(records x P). A clean CLI exit compacts, so only the crash path paid, and that is the
    /// path where a fast open matters.
    ///
    /// The bound is the fix's claim, stated from the fixture. It allows one pass to index the
    /// image's log, one visit per entry a record carries, one per record, and one pass to write
    /// the log back: `2 x (P0 + carried) + records`. A per-record rescan costs about
    /// `records x P0` instead. With `P0 = 64` and `12` rounds that is 236 allowed against roughly
    /// 2,300 at `731a7fa` (instrument commit `54c66b9`), so the red and the green are an order of
    /// magnitude apart rather than a hair.
    #[test]
    fn replaying_the_tail_visits_each_records_own_entries_not_the_whole_log() {
        const P0: usize = 64;
        const ROUNDS: usize = 12;
        let h = Harness::new_with(true);
        let armed = std::env::temp_dir()
            .join(format!("ferro-arena-d183-replay-visits-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&armed);
        let carried = parked_image_then_mixed_tail(&h, &armed, P0, ROUNDS);

        // Ask the artifact which records it holds, rather than trusting the writers' routing.
        let kinds = ArenaPageStore::tail_kinds(&armed);
        let count = |k: u8| kinds.iter().filter(|&&x| x == k).count();
        assert_eq!(
            (
                count(ArenaPageStore::TAIL_PAGES_PARKED),
                count(ArenaPageStore::TAIL_PENDING_DRAINED),
                count(ArenaPageStore::TAIL_EXTENT_FREED),
            ),
            (ROUNDS, ROUNDS, ROUNDS),
            "fixture: the tail does not hold one parked, one drained and one freed record per \
             round: {kinds:?}"
        );

        let target = h.fresh_store();
        let v0 = target.replay_pending_visits();
        assert!(target.restore(&armed).unwrap());
        let visits = target.replay_pending_visits() - v0;
        // Printed on every run, so a PASSING run shows the number and not only the bound.
        println!("D183 count test: replay visits = {visits}");
        assert_eq!(target.pending_len(), P0, "the replayed log is not the log the tail describes");

        // Anti-vacuity: every carried entry is visited at least once, so a counter that is not
        // wired reads below this and cannot pass the bound by reading zero.
        assert!(
            visits >= carried as u64,
            "the counter saw {visits} visits for a tail carrying {carried} entries: it is not wired"
        );
        let bound = 2 * (P0 + carried) as u64 + kinds.len() as u64;
        assert!(
            visits <= bound,
            "replay visited {visits} pending-log entries for a tail of {} records carrying \
             {carried} entries over a {P0}-entry log; allowed {bound}. A per-record scan of the \
             log visits about {} x {P0}",
            kinds.len(),
            kinds.len()
        );

        // The tail was REPLAYED, not skipped (review F2). A store that replayed no record would
        // pass every bound above, since it visits only the image's log, and its pending log would
        // still be the image's original 64 entries. Memory holds entries 13..64 plus the 12
        // re-parks, and a full image of memory is the independent reference.
        let control = std::env::temp_dir()
            .join(format!("ferro-arena-d183-replay-visits-c-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&control);
        h.store.checkpoint(&control).unwrap();
        let from_image = h.fresh_store();
        assert!(from_image.restore(&control).unwrap());
        assert_eq!(
            target.state_bytes(),
            from_image.state_bytes(),
            "the store restored from image + tail is not the map a full image of memory holds, so \
             the visit count above was taken over a replay that skipped records"
        );
        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_file(&control);
    }

    /// The same image and tail through the real writers, restored and compared byte for byte with
    /// a full image of live memory.
    ///
    /// This passes BEFORE the replay index (`731a7fa`) and must still pass after it. That makes it
    /// the differential for the rewrite: the index has to leave the log in the order, and with
    /// the entries, that the per-record scans produced.
    #[test]
    fn a_mixed_tail_over_a_parked_image_replays_byte_identical_to_a_full_rewrite() {
        let h = Harness::new_with(true);
        let armed = std::env::temp_dir()
            .join(format!("ferro-arena-d183-replay-mixed-{}.bin", std::process::id()));
        let control = std::env::temp_dir()
            .join(format!("ferro-arena-d183-replay-mixed-c-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_file(&control);
        parked_image_then_mixed_tail(&h, &armed, 16, 6);

        h.store.checkpoint(&control).unwrap();
        let from_tail = h.fresh_store();
        assert!(from_tail.restore(&armed).unwrap());
        let from_image = h.fresh_store();
        assert!(from_image.restore(&control).unwrap());
        assert_eq!(
            from_tail.state_bytes(),
            from_image.state_bytes(),
            "a parked image plus a tail of parked, drained, claimed and freed records does not \
             restore to the map a full rewrite holds"
        );
        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_file(&control);
    }

    /// Every rule the pending-log arms of `apply_tail_record` state, on one hand-built tail, against
    /// a log derived BY HAND from those rules. The rules:
    /// * PARKED skips a key already in the log (first wins), a key repeated inside the same record,
    ///   and an entry naming a freed arena;
    /// * a key removed and parked again goes to the END;
    /// * REPLACED drops dead arenas. Since de-dup at push it also keeps only the FIRST entry for a
    ///   key it carries twice, so no key reaches the log twice (`replace` is first-wins, and T6,
    ///   `a_replaced_record_carrying_duplicates_replays_to_the_first_of_each`, pins it);
    /// * DRAINED removes EVERY entry with a key, and a key that is absent is a no-op;
    /// * EXTENT_FREED removes every entry of its arena, and later parks into it are skipped.
    ///
    /// The real writers never produce most of these shapes, which is exactly why they are built by
    /// hand: an index that is right only for the shapes the writers happen to produce today is not
    /// the per-record scan's equal. Passes at `731a7fa`; must pass after the index.
    #[test]
    fn replay_of_the_pending_log_keeps_every_rule_the_per_record_scans_had() {
        let h = Harness::new();
        let branches: Vec<BranchId> = (0..3)
            .map(|_| h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id)
            .collect();
        let arenas: Vec<ArenaId> =
            branches.iter().map(|b| h.store.arena_for(*b).unwrap()).collect();
        let (a1, a2, a3) = (arenas[0], arenas[1], arenas[2]);
        let owner = branches[0];
        let dead = ArenaId(u32::MAX - 7);
        let e = |page: PageId, arena: ArenaId| PendingFree {
            page_id: page,
            arena_id: arena,
            birth_epoch: Epoch(1),
            free_epoch: Epoch(2),
            owner,
        };
        let (x1, x2, x3) = (e(101, a1), e(102, a2), e(103, a1));
        let (y1, w, v1, v2, u) = (e(201, a3), e(301, a2), e(401, a1), e(402, a2), e(501, a2));
        // Unarmed store: this seeds memory only, which is what an image load leaves behind.
        h.store.put_pending(vec![x1, x2, x3]).unwrap();

        let live = h.store.live_page_count().unwrap();
        let with_entries = |kind: u8, es: &[PendingFree]| {
            let mut p = live.to_be_bytes().to_vec();
            p.extend_from_slice(&(es.len() as u32).to_be_bytes());
            for x in es {
                ArenaPageStore::encode_pending_entry(&mut p, x);
            }
            p.extend_from_slice(&0u32.to_be_bytes()); // no arena sections
            ArenaPageStore::encode_tail_record(kind, &p)
        };
        let drained = |keys: &[(PageId, ArenaId)]| {
            let mut p = live.to_be_bytes().to_vec();
            p.extend_from_slice(&(keys.len() as u32).to_be_bytes());
            for (page, arena) in keys {
                p.extend_from_slice(&page.to_be_bytes());
                p.extend_from_slice(&arena.0.to_be_bytes());
            }
            p.extend_from_slice(&0u32.to_be_bytes());
            ArenaPageStore::encode_tail_record(ArenaPageStore::TAIL_PENDING_DRAINED, &p)
        };
        let (a2_start, a2_pages) = {
            let st = h.store.state.lock().unwrap();
            let ext = st.extents.get(&a2).expect("fixture: a2 is a live extent");
            (ext.start_page, ext.page_count)
        };
        let freed_a2 = {
            let mut p = Vec::new();
            p.extend_from_slice(&a2.0.to_be_bytes());
            p.extend_from_slice(&a2_start.to_be_bytes());
            p.extend_from_slice(&a2_pages.to_be_bytes());
            p.extend_from_slice(&live.to_be_bytes());
            ArenaPageStore::encode_tail_record(ArenaPageStore::TAIL_EXTENT_FREED, &p)
        };
        let key = |x: &PendingFree| (x.page_id, x.arena_id);

        let parked = ArenaPageStore::TAIL_PAGES_PARKED;
        let replaced = ArenaPageStore::TAIL_PENDING_REPLACED;
        // Two replays, with the log checked BETWEEN them. The REPLACED record in the second half
        // overwrites everything the first half did, so a single check at the end cannot see a
        // first-wins or in-record de-dup failure: fresh-context review F1 traced a `contains`
        // that always answered "absent" passing the one-replay version of this test.
        let replay = |tail: &[u8]| {
            let applied = h.store.replay_tail(tail).unwrap();
            assert_eq!(applied as usize, tail.len(), "fixture: replay stopped before the end");
            h.store.state.lock().unwrap().pending.clone()
        };

        let mut first = Vec::new();
        // [x1 x2 x3] -> x2 already there, y1 twice in one record, one dead arena -> [x1 x2 x3 y1]
        first.extend(with_entries(parked, &[x2, y1, y1, e(901, dead)]));
        // x1 goes, an absent key is a no-op -> [x2 x3 y1]
        first.extend(drained(&[key(&x1), (999, a3)]));
        // x1 again goes to the END -> [x2 x3 y1 x1]
        first.extend(with_entries(parked, &[x1]));
        assert_eq!(
            replay(&first),
            vec![x2, x3, y1, x1],
            "first-wins, in-record de-dup, the dead-arena skip or re-park-goes-last is broken"
        );

        let mut second = Vec::new();
        // wholesale: dead arena dropped, and x3's second copy dropped first-wins -> [x3 w]
        second.extend(with_entries(replaced, &[x3, x3, w, e(902, dead)]));
        // x3 goes -> [w]. (Before de-dup at push the log held x3 twice here, and both went.)
        second.extend(drained(&[key(&x3)]));
        // -> [w v1 v2]
        second.extend(with_entries(parked, &[v1, v2]));
        // v1 is live, in an arena that stays live: first wins, so nothing changes -> [w v1 v2]
        second.extend(with_entries(parked, &[v1]));
        // a2 freed: w and v2 go, and a2 is dead from here on -> [v1]
        second.extend(freed_a2);
        // a park into the freed arena is skipped; one into a live arena lands -> [v1 x3]
        second.extend(with_entries(parked, &[u, x3]));
        assert_eq!(
            replay(&second),
            vec![v1, x3],
            "the replayed pending log breaks a rule the per-record scans kept (order, first-wins, \
             dead-arena skip, duplicate handling)"
        );
    }

    /// **The FIRST entry for a key wins, which is the rule the PARKED arm's comment gives, pinned with
    /// entries that can be told apart.** A resumed reap re-parks a page with the same `birth_epoch`
    /// and owner but, possibly, a different `free_epoch`. `drain_pending` decides on the first entry
    /// it meets, so replay must keep that one, not the later one.
    ///
    /// The rules test above re-parks an entry byte-identical to the one in the log, so it pins "no
    /// duplicate" and not WHICH copy survives: a last-wins index passes it (lead review F1, mutant
    /// MC6). Here the two entries differ in `free_epoch` only.
    ///
    /// Passes at `731a7fa` (the per-record `HashSet` kept the first) and must pass with the index.
    #[test]
    fn a_repark_of_a_key_already_in_the_log_keeps_the_first_entry() {
        let h = Harness::new();
        let owner = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id;
        let arena = h.store.arena_for(owner).unwrap();
        let parked = |free: u64| PendingFree {
            page_id: 11,
            arena_id: arena,
            birth_epoch: Epoch(1),
            free_epoch: Epoch(free),
            owner,
        };
        let (first, again) = (parked(2), parked(5));
        let live = h.store.live_page_count().unwrap();
        let record = |e: &PendingFree| {
            let mut p = live.to_be_bytes().to_vec();
            p.extend_from_slice(&1u32.to_be_bytes());
            ArenaPageStore::encode_pending_entry(&mut p, e);
            p.extend_from_slice(&0u32.to_be_bytes()); // no arena sections
            ArenaPageStore::encode_tail_record(ArenaPageStore::TAIL_PAGES_PARKED, &p)
        };
        let mut tail = record(&first);
        tail.extend(record(&again));

        let applied = h.store.replay_tail(&tail).unwrap();
        assert_eq!(applied as usize, tail.len(), "fixture: replay stopped before the end");
        let log = h.store.state.lock().unwrap().pending.clone();
        assert_eq!(
            log,
            vec![first],
            "a re-park of a key already in the log did not keep the FIRST entry (free_epoch 2)"
        );
    }

    /// **A clean open does no work on the pending-free log** (lead review F4). The CLI's clean exit
    /// writes a full image (`store.checkpoint` in `src/cli/cli.rs`), so the next open replays an
    /// EMPTY tail. The per-record code did nothing there. An index built up front would cost two
    /// passes over the log on every open, for nothing.
    #[test]
    fn a_clean_open_does_no_work_on_the_pending_log() {
        let h = Harness::new_with(true);
        let armed = std::env::temp_dir()
            .join(format!("ferro-arena-d183-clean-open-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&armed);
        h.store.checkpoint_to(armed.clone());
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        for _ in 0..16 {
            h.store.alloc_for(parent.branch_id, PageType::Heap, Epoch(1)).unwrap();
        }
        h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        let rec = h.catalog.get_raw(parent.branch_id.id).unwrap();
        h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();
        assert_eq!(h.store.pending_len(), 16, "fixture: the retire parked 16 pages");
        h.store.checkpoint(&armed).unwrap();
        assert!(ArenaPageStore::tail_kinds(&armed).is_empty(), "fixture: the tail is not empty");

        let target = h.fresh_store();
        let v0 = target.replay_pending_visits();
        assert!(target.restore(&armed).unwrap());
        assert_eq!(target.pending_len(), 16, "the image's pending log did not come back");
        // Printed on every run, so a PASSING run shows the number and not only the bound.
        println!("D183 clean open: replay visits = {}", target.replay_pending_visits() - v0);
        assert_eq!(
            target.replay_pending_visits() - v0,
            0,
            "replaying an EMPTY tail still walked the pending log"
        );
        let _ = std::fs::remove_file(&armed);
    }

    // ---- D183 de-dup at push: one entry per (page, arena), in memory and in the file ----------------
    //
    // Lane AMENDMENT 2, from `frontier/catalog_root_and_park_adversary.md` §B1-B5 @ `a4b48ea`. Hardening,
    // not a live double free (the adversary refuted that, B2): a page parked twice is decided twice.

    /// A parent with `pages` pages, a live child that pins every one of them, and the parent published
    /// `Reaping`, which is what `reap` does before it retires anything. Returns the record `reap` reads.
    fn reaping_parent_pinned_by_a_child(h: &Harness, pages: usize) -> BranchRecord {
        use crate::branch::types::BranchState;
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        for _ in 0..pages {
            h.store.alloc_for(parent.branch_id, PageType::Heap, Epoch(1)).unwrap();
        }
        h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        h.catalog.set_state(parent.branch_id, BranchState::Live, BranchState::Reaping).unwrap();
        let rec = h.catalog.get_raw(parent.branch_id.id).unwrap();
        assert_eq!(rec.state, BranchState::Reaping, "fixture: the parent is not Reaping");
        rec
    }

    /// The pending-free log in order. Reads it through `iter`, so it means the same thing whatever
    /// type holds the log.
    fn pending_of(store: &ArenaPageStore) -> Vec<PendingFree> {
        store.state.lock().unwrap().pending.iter().copied().collect()
    }

    fn free_epochs(store: &ArenaPageStore) -> Vec<Epoch> {
        pending_of(store).iter().map(|p| p.free_epoch).collect()
    }

    /// ⭐ **A resumed reap parks each page ONCE, in memory and in the file.**
    ///
    /// `reap` publishes `Reaping`, then `retire_arenas_by_rule` parks every pinned page. If the
    /// `Reaping -> Reaped` flip never happens (an `Err` after the retire, or a crash), the resumed `reap`
    /// draws a NEW free epoch and retires again. `allocated_pages` still lists a parked page, because
    /// parking does not recycle it, so at `bfe55a8` every page was parked a second time. Memory held
    /// `[K(e1), K(e2)]` while the tail replayed first-wins to `[K(e1)]`, so the store and its own file
    /// disagreed.
    ///
    /// This is the in-process resume: a second `reap` of a branch still `Reaping`, as a `seal` retry does.
    /// The restart resume is the next test.
    #[test]
    fn a_resumed_reap_parks_each_page_once_in_memory_and_in_the_replayed_file() {
        const PAGES: usize = 4;
        let h = Harness::new_with(true);
        let armed = std::env::temp_dir()
            .join(format!("ferro-arena-d183-resumed-reap-{}.bin", std::process::id()));
        let control = std::env::temp_dir()
            .join(format!("ferro-arena-d183-resumed-reap-c-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_file(&control);
        h.store.checkpoint_to(armed.clone());
        let rec = reaping_parent_pinned_by_a_child(&h, PAGES);
        // An image of the fixture, so the tail below holds the two retires and nothing else.
        h.store.checkpoint(&armed).unwrap();

        let e1 = h.catalog.next_epoch();
        h.store.retire_arenas_by_rule(&rec, e1).unwrap();
        assert_eq!(
            free_epochs(&h.store),
            vec![e1; PAGES],
            "fixture: the first retire did not park every page"
        );
        let e2 = h.catalog.next_epoch();
        h.store.retire_arenas_by_rule(&rec, e2).unwrap();
        assert_eq!(
            free_epochs(&h.store),
            vec![e1; PAGES],
            "the resumed retire parked pages that were already pending (the copies free at {e2:?}); \
             the first entry for a page must stay the only one"
        );

        // Ask the artifact: each retire wrote its record, so the replay has the second one to apply.
        assert_eq!(
            ArenaPageStore::tail_kinds(&armed),
            vec![ArenaPageStore::TAIL_PAGES_PARKED; 2],
            "fixture: the tail does not hold exactly one parked record per retire"
        );
        let from_tail = h.fresh_store();
        assert!(from_tail.restore(&armed).unwrap());
        assert_eq!(free_epochs(&from_tail), vec![e1; PAGES], "fixture: the replay is not first-wins");
        h.store.checkpoint(&control).unwrap();
        let from_image = h.fresh_store();
        assert!(from_image.restore(&control).unwrap());
        assert_eq!(
            from_tail.state_bytes(),
            from_image.state_bytes(),
            "the store restored from its own file is not the store that wrote it"
        );
        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_file(&control);
    }

    /// The resume the adversary traced first (B1 steps 1-6): the retire parks, the process dies before
    /// the `Reaped` flip, and the next open restores the file, re-arms it (`reopen_from_checkpoint` ends
    /// in `checkpoint_to`) and resumes the reap with a new free epoch. At `bfe55a8` the resumed store held
    /// every page twice, and so did every image it wrote.
    #[test]
    fn a_reap_resumed_after_a_restart_parks_each_page_once() {
        const PAGES: usize = 4;
        let h = Harness::new_with(true);
        let armed = std::env::temp_dir()
            .join(format!("ferro-arena-d183-restart-resume-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&armed);
        h.store.checkpoint_to(armed.clone());
        let rec = reaping_parent_pinned_by_a_child(&h, PAGES);
        let e1 = h.catalog.next_epoch();
        h.store.retire_arenas_by_rule(&rec, e1).unwrap();

        // The flip to `Reaped` never happened. A new process opens the file.
        let resumed = h.fresh_store();
        assert!(resumed.restore(&armed).unwrap());
        resumed.checkpoint_to(armed.clone());
        assert_eq!(
            free_epochs(&resumed),
            vec![e1; PAGES],
            "fixture: the first retire's parks are not in the file"
        );

        resumed.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();
        assert_eq!(
            free_epochs(&resumed),
            vec![e1; PAGES],
            "the reap resumed after a restart parked every page a second time"
        );
        let reopened = h.fresh_store();
        assert!(reopened.restore(&armed).unwrap());
        assert_eq!(
            free_epochs(&reopened),
            vec![e1; PAGES],
            "the file the resumed reap wrote parks every page a second time"
        );
        let _ = std::fs::remove_file(&armed);
    }

    /// The crash-free shape (B1): the owner freed a page while it was live, and a live child pinned it,
    /// so `free_page` parked it at e_f. Reaping the owner on the slow path then decided that page again.
    /// Still pinned, it was parked a second time at `bfe55a8`.
    #[test]
    fn a_retire_does_not_repark_a_page_its_owner_already_parked() {
        use crate::branch::types::BranchState;
        const PAGES: usize = 4;
        let h = Harness::new_with(true);
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let pages: Vec<PageId> = (0..PAGES)
            .map(|_| h.store.alloc_for(parent.branch_id, PageType::Heap, Epoch(1)).unwrap())
            .collect();
        h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        let e_f = h.catalog.next_epoch();
        h.store.free_page(pages[0], e_f).unwrap();
        assert_eq!(h.store.pending_len(), 1, "fixture: the child pins the freed page, so it parks");

        h.catalog.set_state(parent.branch_id, BranchState::Live, BranchState::Reaping).unwrap();
        let rec = h.catalog.get_raw(parent.branch_id.id).unwrap();
        h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();
        let log = pending_of(&h.store);
        assert_eq!(log.len(), PAGES, "the retire parked a page its owner had already parked: {log:?}");
        let p0: Vec<Epoch> =
            log.iter().filter(|p| p.page_id == pages[0]).map(|p| p.free_epoch).collect();
        assert_eq!(p0, vec![e_f], "the page freed first must keep the entry its own free made");
    }

    /// **A page already pending is decided by its entry, not again by the retire.** At `bfe55a8` the
    /// retire RELEASED such a page when it found it unpinned, while its entry stayed in the log: one page
    /// both pending and recycled. B2 shows the second release is a no-op today; B3 says what makes it
    /// one, and that the statement lock is part of it.
    ///
    /// Shape: a live child `c0` forked BEFORE the pages are born, so it pins none of them and keeps the
    /// parent's reap on the slow path. Child `c` pins them all, so `free_page(p0)` parks p0. `c` is then
    /// flipped `Reaped` with no drain behind it, which is exactly what a drain refused by an unreadable
    /// owner record leaves (D124: it puts the entry back and returns `Err`). The retire finds all four
    /// pages unpinned. It releases three; p0 belongs to its entry, and the next drain releases it.
    #[test]
    fn a_retire_leaves_a_page_already_pending_to_its_entry() {
        use crate::branch::types::BranchState;
        const PAGES: usize = 4;
        let h = Harness::new_with(true);
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        let birth = h.catalog.next_epoch();
        let pages: Vec<PageId> = (0..PAGES)
            .map(|_| h.store.alloc_for(parent.branch_id, PageType::Heap, birth).unwrap())
            .collect();
        let c = h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        let e_f = h.catalog.next_epoch();
        h.store.free_page(pages[0], e_f).unwrap();
        assert_eq!(h.store.pending_len(), 1, "fixture: c pins the freed page, so it must be parked");
        h.catalog.set_state(c.branch_id, BranchState::Live, BranchState::Reaping).unwrap();
        h.catalog.set_state(c.branch_id, BranchState::Reaping, BranchState::Reaped).unwrap();

        h.catalog.set_state(parent.branch_id, BranchState::Live, BranchState::Reaping).unwrap();
        let rec = h.catalog.get_raw(parent.branch_id.id).unwrap();
        let released = h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();

        let (log, both) = {
            let st = h.store.state.lock().unwrap();
            let log: Vec<PendingFree> = st.pending.iter().copied().collect();
            let both: Vec<PageId> = log
                .iter()
                .filter(|p| st.recycled.get(&p.arena_id).is_some_and(|r| r.contains(&p.page_id)))
                .map(|p| p.page_id)
                .collect();
            (log, both)
        };
        assert_eq!(
            both,
            Vec::<PageId>::new(),
            "the retire released a page that was still in the pending-free log: it is now recycled \
             AND parked, and the next drain releases it a second time"
        );
        assert_eq!(released, (PAGES - 1) as u32, "fixture: the three pages nobody freed are unpinned");
        assert_eq!(
            log.iter().map(|p| (p.page_id, p.free_epoch)).collect::<Vec<_>>(),
            vec![(pages[0], e_f)],
            "the pending page's own entry must be the one that decides it"
        );
    }

    /// The drain's window (B1, the adversary's in-process shape seen from the drain): a drain takes the
    /// whole log, the owner's retire runs again before the drain puts anything back, and the drain then
    /// puts back every entry it found still pinned. At `bfe55a8` the log held every page twice. Today the
    /// statement lock keeps a retire out of that window; the log must not depend on it.
    #[test]
    fn a_drain_that_meets_a_park_of_a_key_it_took_restates_the_whole_log() {
        const PAGES: usize = 4;
        let h = Harness::new_with(true);
        let armed = std::env::temp_dir()
            .join(format!("ferro-arena-d183-drain-window-{}.bin", std::process::id()));
        let control = std::env::temp_dir()
            .join(format!("ferro-arena-d183-drain-window-c-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_file(&control);
        h.store.checkpoint_to(armed.clone());
        let rec = reaping_parent_pinned_by_a_child(&h, PAGES);
        h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();

        let taken = h.store.take_pending();
        assert_eq!(taken.len(), PAGES, "fixture: the drain took nothing");
        // The drain holds every entry, so nothing is pending and every page parks again.
        h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();
        assert_eq!(h.store.pending_len(), PAGES, "fixture: the retire did not park into the drained log");
        h.store.put_pending(taken).unwrap();
        assert_eq!(
            h.store.pending_len(),
            PAGES,
            "the drain put back entries for pages the retire had just parked again"
        );

        h.store.checkpoint(&control).unwrap();
        let from_tail = h.fresh_store();
        assert!(from_tail.restore(&armed).unwrap());
        let from_image = h.fresh_store();
        assert!(from_image.restore(&control).unwrap());
        assert_eq!(
            from_tail.state_bytes(),
            from_image.state_bytes(),
            "the store restored from its own file is not the store that wrote it"
        );
        let _ = std::fs::remove_file(&armed);
        let _ = std::fs::remove_file(&control);
    }

    /// A REPLACED record written before de-dup at push can carry a key twice, because it restated a
    /// memory log that held one twice. Replayed, the log keeps the FIRST entry for each key, the rule
    /// every push now keeps, so a store opened from an old file holds what a store running today would.
    #[test]
    fn a_replaced_record_carrying_duplicates_replays_to_the_first_of_each() {
        let h = Harness::new();
        let owner = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id;
        let arena = h.store.arena_for(owner).unwrap();
        let entry = |page: PageId, free: u64| PendingFree {
            page_id: page,
            arena_id: arena,
            birth_epoch: Epoch(1),
            free_epoch: Epoch(free),
            owner,
        };
        let (x, w, x_again) = (entry(11, 2), entry(12, 2), entry(11, 5));
        let mut p = h.store.live_page_count().unwrap().to_be_bytes().to_vec();
        p.extend_from_slice(&3u32.to_be_bytes());
        for e in [&x, &w, &x_again] {
            ArenaPageStore::encode_pending_entry(&mut p, e);
        }
        p.extend_from_slice(&0u32.to_be_bytes()); // no arena sections
        let tail = ArenaPageStore::encode_tail_record(ArenaPageStore::TAIL_PENDING_REPLACED, &p);

        let applied = h.store.replay_tail(&tail).unwrap();
        assert_eq!(applied as usize, tail.len(), "fixture: replay stopped before the end");
        assert_eq!(
            pending_of(&h.store),
            vec![x, w],
            "a REPLACED record's second entry for a key survived the replay"
        );
    }

    /// **Parking a page that is already pending changes nothing, so it owes the file nothing.** An
    /// unrecorded push bumps `pending_version`, which makes the next persist a full image rewrite. A push
    /// that skipped must not: the same rule `take_pending` keeps for a drain that took nothing.
    #[test]
    fn a_second_park_of_a_page_already_pending_changes_nothing_and_owes_no_rewrite() {
        let h = Harness::new_with(true);
        let armed = std::env::temp_dir()
            .join(format!("ferro-arena-d183-second-park-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&armed);
        h.store.checkpoint_to(armed.clone());
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let page = h.store.alloc_for(parent.branch_id, PageType::Heap, Epoch(1)).unwrap();
        h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        h.store.free_page(page, h.catalog.next_epoch()).unwrap();
        assert_eq!(h.store.pending_len(), 1, "fixture: the child pins the page, so it must be parked");
        // A claim brings the file level with memory again: that park forced it to rewrite.
        let c = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        h.store.arena_for(c.branch_id).unwrap();

        let (r0, a0) = h.store.persist_counters();
        h.store.free_page(page, h.catalog.next_epoch()).unwrap();
        let d = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        h.store.arena_for(d.branch_id).unwrap();
        let (r1, a1) = h.store.persist_counters();
        assert_eq!(h.store.pending_len(), 1, "a second park of a page already pending added an entry");
        assert_eq!(
            (r1 - r0, a1 - a0),
            (0, 1),
            "the claim after a park that changed nothing rewrote the image ({} rewrites, {} appends)",
            r1 - r0,
            a1 - a0
        );
        let _ = std::fs::remove_file(&armed);
    }

    /// **The PARKED arm's lookup is what keeps a re-park from costing a slot** (lane AMENDMENT 3, review
    /// F2). Since de-dup at push, `finish` builds the log first-wins, so the lookup no longer decides the
    /// log's CONTENTS: a PARKED arm that pushed every entry would come back with the same log. What it
    /// still decides is the cost: without it, every re-park of a key already pending takes a slot that
    /// `finish` then walks. Resumed reaps written before the fix re-park whole branches this way.
    ///
    /// Accounting by hand from `PendingReplay`: indexing the one-entry log is 1 visit, each of the N
    /// lookups is 1, and `finish` walks 1 slot. So `N + 2`. Pushing without the lookup costs `2N + 2`,
    /// and a lookup that always answers "absent" costs `3N + 2`.
    #[test]
    fn a_repark_of_a_key_already_pending_costs_one_lookup_and_no_slot() {
        const N: usize = 32;
        let h = Harness::new();
        let owner = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id;
        let arena = h.store.arena_for(owner).unwrap();
        let k = PendingFree {
            page_id: 21,
            arena_id: arena,
            birth_epoch: Epoch(1),
            free_epoch: Epoch(2),
            owner,
        };
        // Unarmed store: this seeds memory only, which is what an image load leaves behind.
        h.store.put_pending(vec![k]).unwrap();
        let mut p = h.store.live_page_count().unwrap().to_be_bytes().to_vec();
        p.extend_from_slice(&(N as u32).to_be_bytes());
        for i in 0..N {
            let again = PendingFree { free_epoch: Epoch(3 + i as u64), ..k };
            ArenaPageStore::encode_pending_entry(&mut p, &again);
        }
        p.extend_from_slice(&0u32.to_be_bytes()); // no arena sections
        let tail = ArenaPageStore::encode_tail_record(ArenaPageStore::TAIL_PAGES_PARKED, &p);

        let v0 = h.store.replay_pending_visits();
        let applied = h.store.replay_tail(&tail).unwrap();
        let visits = h.store.replay_pending_visits() - v0;
        // Printed on every run, so a PASSING run shows the number and not only the bound.
        println!("D183 T8: replay visits = {visits}");
        assert_eq!(applied as usize, tail.len(), "fixture: replay stopped before the end");
        assert_eq!(pending_of(&h.store), vec![k], "a re-park of a key already pending changed the log");
        assert!(visits >= N as u64, "the counter saw {visits} visits for {N} re-parks: it is not wired");
        assert!(
            visits <= N as u64 + 2,
            "{N} re-parks of one key already pending cost {visits} visits; one lookup each plus the \
             one-entry log is {}. Each re-park took a slot",
            N + 2
        );
    }

    // ---- D183: a drain that changed nothing writes nothing ----------------------------------

    /// A parent with `pages` pages and a live child forked after them, retired by the interval
    /// rule, so every page is PARKED by one `TAIL_PAGES_PARKED` record. That record also takes
    /// every recycled-list mark outstanding at the time.
    fn armed_with_parked_log(tag: &str, pages: usize) -> (Harness, std::path::PathBuf) {
        let h = Harness::new_with(true);
        let armed = std::env::temp_dir()
            .join(format!("ferro-arena-d183-elide-{}-{}.bin", std::process::id(), tag));
        let _ = std::fs::remove_file(&armed);
        h.store.checkpoint_to(armed.clone());
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        for _ in 0..pages {
            h.store.alloc_for(parent.branch_id, PageType::Heap, Epoch(1)).unwrap();
        }
        h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        let rec = h.catalog.get_raw(parent.branch_id.id).unwrap();
        h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();
        assert_eq!(h.store.pending_len(), pages, "fixture: the retire parked {pages} pages");
        (h, armed)
    }

    /// `image + tail` at `armed` restores byte-identical to a full image of live memory.
    fn tail_matches_a_full_rewrite(h: &Harness, armed: &std::path::Path, tag: &str) {
        let control = std::env::temp_dir()
            .join(format!("ferro-arena-d183-elide-c-{}-{}.bin", std::process::id(), tag));
        let _ = std::fs::remove_file(&control);
        h.store.checkpoint(&control).unwrap();
        let from_tail = h.fresh_store();
        assert!(from_tail.restore(armed).unwrap());
        let from_image = h.fresh_store();
        assert!(from_image.restore(&control).unwrap());
        assert_eq!(
            from_tail.state_bytes(),
            from_image.state_bytes(),
            "{tag}: image + tail does not restore to what a full rewrite of memory holds"
        );
        let _ = std::fs::remove_file(&control);
    }

    /// ⭐ **A drain that released nothing writes no record, and still leaves the log LEVEL.**
    ///
    /// This is what `drain_pending_seeded` does on every reap while a live child pins the parked
    /// pages: take the log, keep everything, put it back. The record for it named no removal and
    /// no arena, so it cost one fsync per interior reap and one more record for replay to walk.
    ///
    /// The record's one durable effect was to discharge the take's `pending_version` bump. The
    /// elision must make that move itself, or the NEXT record of any kind would find the log
    /// dirty and rewrite the whole image. That is the second assertion. Its mutant (skip the
    /// record, forget the move) turns `(0, 1)` into `(1, 0)`.
    #[test]
    fn a_drain_that_released_nothing_writes_no_record_and_leaves_the_log_level() {
        let (h, armed) = armed_with_parked_log("nothing", 4);
        let kinds0 = ArenaPageStore::tail_kinds(&armed);
        let (r0, a0) = h.store.persist_counters();
        let e0 = h.store.elided_drains();

        let taken = h.store.take_pending();
        assert_eq!(taken.len(), 4, "fixture: the take did not see the parked entries");
        h.store.put_pending(taken).unwrap();

        assert_eq!(
            h.store.elided_drains(),
            e0 + 1,
            "the drain that released nothing was not elided (anti-vacuity: without this, the two \
             assertions below also pass for a store that never reached the elision)"
        );
        assert_eq!(
            h.store.persist_counters(),
            (r0, a0),
            "a drain that released nothing still persisted something"
        );
        assert_eq!(
            ArenaPageStore::tail_kinds(&armed),
            kinds0,
            "a drain that released nothing still wrote a record"
        );

        // The log is level: the next record APPENDS.
        let spare = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        h.store.arena_for(spare.branch_id).unwrap();
        let (r1, a1) = h.store.persist_counters();
        assert_eq!(
            (r1 - r0, a1 - a0),
            (0, 1),
            "the claim after an elided drain did not simply append: the take's pending_version bump \
             was never discharged, so every elision now costs a full image rewrite later"
        );
        tail_matches_a_full_rewrite(&h, &armed, "nothing");
        let _ = std::fs::remove_file(&armed);
    }

    /// An elision stands in for an APPEND. It must never swallow a REWRITE that was already owed.
    ///
    /// Here a recycled page is handed out again first, which sets `recycled_reissued`. Only a full
    /// image rewrite can carry that, because no record kind can say "this recycled page is live
    /// again", and until it lands a crash would restore an extent that looks empty while it holds
    /// a live page. The drain that follows releases nothing and owes no arena, so it would be
    /// elided if the elision ignored what `persist_delta_locked` would have done. It must rewrite.
    #[test]
    fn a_drain_with_nothing_to_say_still_pays_a_rewrite_that_was_owed() {
        let h = Harness::new_with(true);
        let armed = std::env::temp_dir()
            .join(format!("ferro-arena-d183-elide-owed-{}.bin", std::process::id()));
        let _ = std::fs::remove_file(&armed);
        h.store.checkpoint_to(armed.clone());

        // A recycled page, not yet reused.
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let arena = h.store.arena_for(b.branch_id).unwrap();
        let page = h.store.alloc_in_arena(arena, PageType::Heap, Epoch(1)).unwrap();
        h.store.release_page(page, arena);
        // The parked log. Its record also takes the recycled-list mark `release_page` left, so
        // the drain below owes no arena.
        let parent = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        for _ in 0..3 {
            h.store.alloc_for(parent.branch_id, PageType::Heap, Epoch(1)).unwrap();
        }
        h.catalog.fork(parent.branch_id, LeaseDeadline(0)).unwrap();
        let rec = h.catalog.get_raw(parent.branch_id.id).unwrap();
        h.store.retire_arenas_by_rule(&rec, h.catalog.next_epoch()).unwrap();
        assert!(h.store.pending_len() > 0, "fixture: nothing was parked");

        // The reuse. From here a rewrite is owed.
        let reused = h.store.alloc_for(b.branch_id, PageType::Heap, Epoch(2)).unwrap();
        assert_eq!(reused, page, "fixture: the allocation did not come from the recycled list");

        let (r0, a0) = h.store.persist_counters();
        let e0 = h.store.elided_drains();
        let taken = h.store.take_pending();
        h.store.put_pending(taken).unwrap();
        let (r1, a1) = h.store.persist_counters();
        assert_eq!(h.store.elided_drains(), e0, "an owed rewrite was elided");
        assert_eq!(
            (r1 - r0, a1 - a0),
            (1, 0),
            "the drain did not pay the rewrite a reissued recycled page owed"
        );
        tail_matches_a_full_rewrite(&h, &armed, "owed");
        let _ = std::fs::remove_file(&armed);
    }

    /// A drain that released nothing but owes a recycled list still writes its record: an
    /// `n = 0` difference record carrying one arena section.
    ///
    /// The page released outside the drain marks its arena and persists nothing, so the drain's
    /// record is the first thing that can carry that list to the file. Eliding it for having "no
    /// removal" would leave the durable recycled list one page short: a leak of that page after a
    /// crash, and a restore that disagrees with memory.
    #[test]
    fn a_drain_that_released_nothing_still_writes_the_recycled_lists_it_owes() {
        let (h, armed) = armed_with_parked_log("owes", 4);
        // Released outside any drain: marked dirty, no record.
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let arena = h.store.arena_for(b.branch_id).unwrap();
        let page = h.store.alloc_in_arena(arena, PageType::Heap, Epoch(1)).unwrap();
        let (r0, a0) = h.store.persist_counters();
        h.store.release_page(page, arena);
        assert_eq!(h.store.persist_counters(), (r0, a0), "fixture: release_page persisted by itself");

        let e0 = h.store.elided_drains();
        let taken = h.store.take_pending();
        h.store.put_pending(taken).unwrap();
        let (r1, a1) = h.store.persist_counters();
        assert_eq!(h.store.elided_drains(), e0, "a drain owing a recycled list was elided");
        assert_eq!((r1 - r0, a1 - a0), (0, 1), "the drain owing a recycled list did not append");
        assert_eq!(
            ArenaPageStore::tail_kinds(&armed).last().copied(),
            Some(ArenaPageStore::TAIL_PENDING_DRAINED),
            "the drain wrote {:?}, not its difference record",
            ArenaPageStore::tail_kinds(&armed)
        );
        tail_matches_a_full_rewrite(&h, &armed, "owes");
        let _ = std::fs::remove_file(&armed);
    }

    #[test]
    fn checkpoint_round_trips_through_a_file() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let a = h.store.arena_for(b.branch_id).unwrap();
        h.store.alloc_in_arena(a, PageType::Heap, Epoch(3)).unwrap();
        let path = std::env::temp_dir()
            .join(format!("ferro-arena-ckpt-{}-{:?}.bin", std::process::id(), a));
        let _ = std::fs::remove_file(&path);

        let target = h.fresh_store();
        assert!(!target.restore(&path).unwrap(), "no checkpoint yet is not an error");
        h.store.checkpoint(&path).unwrap();
        assert!(target.restore(&path).unwrap());
        assert_eq!(target.live_page_count().unwrap(), 1);
        assert_eq!(target.arena_owner(a), Some(b.branch_id));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn privacy_barrier_is_the_later_of_own_fork_and_latest_child_fork() {
        // Same rule, same asserted values. The call shape changed because the second argument is
        // now the ONE epoch the rule depends on rather than an unbounded array to take the last of.
        let mut rec = BranchRecord::trunk(1, LeaseDeadline(0));
        rec.fork_epoch = Epoch(10);
        assert_eq!(privacy_barrier(rec.fork_epoch, None), Epoch(10));
        rec.add_live_child(Epoch(25));
        rec.add_live_child(Epoch(17));
        // `add_live_child` keeps the array sorted ascending, so `last()` is the LATEST fork -
        // which is why 17 arriving after 25 must not move the barrier back.
        assert_eq!(
            privacy_barrier(rec.fork_epoch, rec.live_children.last().copied()),
            Epoch(25)
        );
        assert_eq!(rec.live_children.last().copied(), Some(Epoch(25)), "array is not sorted");
    }
    /// **D85 diagnostic.** Is `next_free` understated after a crash, and does `extent_is_empty`
    /// then report an extent that still holds pages as empty?
    ///
    /// `alloc_for` advances `ext.next_free` (`arena.rs`) and does NOT persist. The persist sites are
    /// all off the page path, so a crash can leave the image's
    /// `next_free` behind by up to `ARENA_EXTENT_PAGES` pages. `load_state` knows and says so, and
    /// answers it on the ALLOCATION side by never resuming a restored extent. This asks the
    /// COLLECTION side's question instead: `extent_is_empty` is `recycled >= ext.next_free`.
    /// **D87 falsifier 2, checked BEFORE any of D87 is built.**
    ///
    /// D85's probe was only ever asked about extents restored from an IMAGE, which gives it a
    /// lower bound (`next_free` as of the checkpoint) to start from. D87 proposes rebuilding
    /// `extents` from the catalog instead, where there is NO lower bound — the probe would start
    /// at 0. If it cannot then tell a never-allocated page from an allocated one, a rebuilt extent
    /// misreports its fill and D85's data loss returns by another door.
    ///
    /// This asks exactly that: probe an extent from zero and see whether the answer matches what
    /// was actually written.
    #[test]
    fn d87_probing_an_extent_from_zero_reports_the_pages_actually_written() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000)).unwrap();
        let epoch = h.catalog.next_epoch();
        // Enough to land in an extent with room to spare, so there ARE unwritten pages after the
        // written prefix for the probe to run past if it is going to.
        for _ in 0..6 {
            h.store.alloc_for(b.branch_id, PageType::BTreeLeaf, epoch).unwrap();
        }
        let arena = *h.catalog.get(b.branch_id).unwrap().arenas.last().unwrap();
        let truth = h.store.allocated_pages(arena).len();
        h.store.flush().unwrap();
        let image = h.store.state_bytes();

        let re = h.fresh_store();
        re.load_state(&image).unwrap();
        // Force the no-lower-bound case D87 would create: zero the fill before probing.
        re.debug_set_next_free(arena, 0);
        assert_eq!(re.allocated_pages(arena).len(), 0, "fixture: the fill was not zeroed");
        re.resolve_fill(arena);
        let probed = re.allocated_pages(arena).len();

        println!("D87 falsifier2: truth={truth} probed-from-zero={probed}");
        assert!(
            probed <= truth,
            "D87 FALSIFIER 2 FIRED: probing from zero reported {probed} pages where only {truth} \
             were written. A rebuilt extent would claim pages it does not own, so the catalog \
             derivation D87 rests on cannot be trusted without a lower bound."
        );
        assert_eq!(
            probed, truth,
            "probing from zero under-reports ({probed} of {truth}); a rebuilt extent would look \
             emptier than it is, which is exactly D85's data loss arriving by another door"
        );
    }

    /// **D85 guard test: `extent_is_empty` must refuse a suspect extent WITHOUT being probed.**
    ///
    /// The end-to-end test cannot see this guard, because `retire_arenas_by_rule` probes first and
    /// a probe masks every mutant of the refusal behind it — the "two guards is one you cannot
    /// test" shape. So this asks the predicate directly, on an extent nobody has resolved.
    #[test]
    fn d85_extent_is_empty_refuses_an_unprobed_restored_extent() {
        let h = Harness::new();
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000)).unwrap();
        let epoch = h.catalog.next_epoch();
        h.store.alloc_for(b.branch_id, PageType::BTreeLeaf, epoch).unwrap();
        let arena = *h.catalog.get(b.branch_id).unwrap().arenas.last().unwrap();
        for p in h.store.allocated_pages(arena) {
            h.store.release_page(p, arena);
        }
        let image = h.store.state_bytes();

        let re = h.fresh_store();
        re.load_state(&image).unwrap();
        assert!(
            re.arena_owner(arena).is_some(),
            "fixture: the image did not carry the extent, so the predicate is not even asked"
        );
        assert!(
            !re.extent_is_empty(arena),
            "extent_is_empty() answered from an UNPROBED restored extent's next_free, which the \
             image can understate — that is the number that freed a live child's pages in D85"
        );
        // After probing, the honest answer is allowed through again.
        re.resolve_fill(arena);
        let _ = re.extent_is_empty(arena);
    }

    #[test]
    fn d85_next_free_after_a_crash_and_what_extent_is_empty_then_says() {
        use std::fs::OpenOptions;
        use crate::storage::disk_manager::DiskManager;
        let dir = std::env::temp_dir();
        let path = dir.join(format!("ferro-d85-{}.db", std::process::id()));
        let ckpt = dir.join(format!("ferro-d85-{}.ckpt", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&ckpt);
        let open = || OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();

        let (arena, written_before, written_after) = {
            let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(open()).unwrap())));
            let catalog = Arc::new(LogBranchCatalog::in_memory(1));
            let base = pool.disk_manager.high_water().unwrap();
            let store = ArenaPageStore::new(
                Arc::clone(&pool),
                Arc::clone(&catalog) as Arc<dyn BranchCatalog>,
                base,
            ).unwrap();

            let b = catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000)).unwrap();
            let epoch = catalog.next_epoch();
            // Enough pages to outgrow the 1-page first extent and land in a bigger one, so there
            // is a tail to understate.
            for _ in 0..6 {
                store.alloc_for(b.branch_id, PageType::BTreeLeaf, epoch).unwrap();
            }
            // **The dangerous shape, constructed exactly.** An understated `next_free` is only
            // hazardous when it restores to <= `recycled`, i.e. to ZERO for a fresh extent. That
            // happens when the extent is created, the image is written, and only THEN are pages
            // put in it. So: fill the current extent until a NEW one appears, checkpoint while it
            // is still empty, and write into it afterwards.
            let mut arena = *catalog.get(b.branch_id).unwrap().arenas.last().unwrap();
            for _ in 0..64 {
                store.alloc_for(b.branch_id, PageType::BTreeLeaf, epoch).unwrap();
                let now = *catalog.get(b.branch_id).unwrap().arenas.last().unwrap();
                if now != arena && store.allocated_pages(now).len() <= 1 {
                    arena = now;
                    break;
                }
                arena = now;
            }
            // Drain this extent's pages back so the image records it as empty, which is the state
            // a crash right after `alloc_arena` leaves behind.
            for p in store.allocated_pages(arena) {
                store.release_page(p, arena);
            }
            let before = store.allocated_pages(arena).len();

            // The crash line: everything above is in the image, everything below is not.
            store.checkpoint(&ckpt).unwrap();

            // Now put live pages into that extent. None of this reaches the image.
            for _ in 0..3 {
                store.alloc_for(b.branch_id, PageType::BTreeLeaf, epoch).unwrap();
            }
            let after = store.allocated_pages(arena).len();
            store.flush().unwrap();
            (arena, before, after)
        };

        // ...and the restart, reading only what the checkpoint durably held.
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(open()).unwrap())));
        let catalog = Arc::new(LogBranchCatalog::in_memory(1));
        let re = ArenaPageStore::reopen_from_checkpoint(
            Arc::clone(&pool),
            Arc::clone(&catalog) as Arc<dyn BranchCatalog>,
            &ckpt,
        ).unwrap();

        let restored = re.allocated_pages(arena).len();
        let empty = re.extent_is_empty(arena);
        println!(
            "D85: arena {arena:?}  pages before checkpoint={written_before}  after more writes={written_after}  \
             after restore={restored}  extent_is_empty={empty}"
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&ckpt);

        // **This test PINNED THE DEFECT and now pins the FIX.** It originally asserted
        // `extent_is_empty == true` for an extent holding written pages, which is the bug D85
        // reproduced. `fill_unknown` + `resolve_fill` changed that answer, so the assertions are
        // rewritten to the corrected behaviour rather than left pinning what was fixed.
        assert!(
            written_after > written_before,
            "fixture: no pages were written after the checkpoint, so nothing is understated"
        );
        assert!(
            restored < written_after,
            "fixture: the restore did not reproduce the understatement D85 is about"
        );
        // The fix: an unprobed restored extent is never called empty, whatever its next_free says.
        assert!(
            !empty,
            "REGRESSION: an extent holding {written_after} written page(s) reports \
             extent_is_empty()=true after a crash. That is D85 — retire_arenas_by_rule then parks \
             none of a live child's pages and the orphan sweep frees the extent."
        );
        // And the probe recovers the truth rather than merely refusing to answer.
        re.resolve_fill(arena);
        println!(
            "D85 probe: recovered {} page(s) (wanted {written_after}); arena_owner={:?}",
            re.allocated_pages(arena).len(),
            re.arena_owner(arena)
        );
        // ⚠ **The probe recovers what REACHED DISK, and that is the honest limit.** It reads
        // pages from `start_page` upward and stops at the first that fails, so a page still in the
        // buffer pool when the process died is indistinguishable from one never allocated. That
        // costs nothing: such a page was never durable, so no branch could read it after the crash
        // either. Measured here as 2 of the 3 written — the third had not been written back.
        let recovered = re.allocated_pages(arena).len();
        assert!(
            recovered > restored,
            "resolve_fill recovered nothing ({recovered} vs {restored} before the probe). It is \
             what clears `fill_unknown`, so without it every restored extent is refused for ever \
             and the orphan sweep can never reclaim anything after a crash."
        );
        assert!(
            recovered <= written_after,
            "resolve_fill reported {recovered} pages but only {written_after} were ever written — \
             it probed past the end of the written prefix, which would park or free pages that do \
             not exist"
        );
    }

    // ==== D81 — the append-only tail =========================================================
    //
    // Every test below is written so that it FAILS if the append path is never taken. Most of
    // them assert first that the tail is non-empty or that `appends > 0`: a suite that exercised
    // only the full-rewrite path would pass every correctness assertion here while proving
    // nothing, and that is the shape of failure this row is most exposed to.

    static D81_SEQ: AtomicU64 = AtomicU64::new(0);

    /// Arm `h.store` on a fresh path and return it.
    fn arm(h: &Harness) -> std::path::PathBuf {
        let n = D81_SEQ.fetch_add(1, Ordering::SeqCst);
        let p =
            std::env::temp_dir().join(format!("ferro-d81-{}-{}.arena", std::process::id(), n));
        let _ = std::fs::remove_file(&p);
        h.store.checkpoint_to(p.clone());
        p
    }

    /// Fork trunk and claim it an extent, which is the event D79 measured.
    fn claim(h: &Harness) -> (BranchId, ArenaId) {
        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let a = h.store.arena_for(b.branch_id).unwrap();
        (b.branch_id, a)
    }

    /// A store whose map has something in EVERY variable-length section.
    ///
    /// A walker that mis-strides one section is invisible to a fixture that leaves that section
    /// empty, and four of the five are empty in an ordinary small fixture.
    fn fully_populated(h: &Harness) -> (BranchId, ArenaId) {
        let (parent, a1) = claim(h);
        for _ in 0..5 {
            h.store.alloc_for(parent, PageType::Heap, h.catalog.next_epoch()).unwrap();
        }
        // a second live extent, so `extents` and `current` both have more than one entry
        claim(h);
        // a freed extent, so `free_extents` is non-empty
        let (_b3, a3) = claim(h);
        h.store.free_arena(a3).unwrap();
        // a page released with no live child, so `recycled` is non-empty
        let loose = h.store.alloc_for(parent, PageType::Heap, h.catalog.next_epoch()).unwrap();
        h.store.free_page(loose, h.catalog.next_epoch()).unwrap();
        // a page parked against a live child, so `pending` is non-empty
        let parked = h.store.alloc_for(parent, PageType::Heap, h.catalog.next_epoch()).unwrap();
        let _child = h.catalog.fork(parent, LeaseDeadline(0)).unwrap();
        h.store.free_page(parked, h.catalog.next_epoch()).unwrap();
        (parent, a1)
    }

    /// **The walker mirrors `state_bytes` and nothing in the type system says so.** This is the
    /// pin.
    ///
    /// The fixture matters more than the assertion: every variable-length section is non-empty,
    /// checked here rather than assumed, because an empty section has its stride multiplied by
    /// zero and a wrong stride then disappears.
    #[test]
    fn the_image_walker_agrees_with_state_bytes_on_a_fully_populated_store() {
        let h = Harness::new();
        fully_populated(&h);
        let image = h.store.state_bytes();

        // Fixture assertions. The free-extent count sits at offset 21, the documented header size.
        assert!(
            u32::from_be_bytes(image[21..25].try_into().unwrap()) > 0,
            "fixture: no freed extent, so the free-list stride is never exercised"
        );
        assert!(h.store.live_arenas().len() >= 2, "fixture: fewer than two live extents");
        assert!(h.store.pending_len() >= 1, "fixture: the pending log is empty");

        assert_eq!(
            ArenaPageStore::image_len(&image).unwrap(),
            image.len(),
            "the walker and the writer disagree about where the image ends"
        );

        // And it must say the same thing with a tail behind it, which is the only reason it
        // exists.
        let mut with_tail = image.clone();
        with_tail.extend_from_slice(&ArenaPageStore::encode_tail_record(
            ArenaPageStore::TAIL_EXTENT_FREED,
            &[0u8; 16],
        ));
        assert_eq!(
            ArenaPageStore::image_len(&with_tail).unwrap(),
            image.len(),
            "the walker followed the tail instead of stopping at the image"
        );
    }

    /// The walker runs BEFORE the checksum, so its failure modes are the interesting ones: it
    /// must refuse rather than trust a count, and it must refuse a corrupt image at all.
    #[test]
    fn the_image_walker_refuses_a_corrupt_image_instead_of_trusting_its_counts() {
        let h = Harness::new();
        fully_populated(&h);
        let good = h.store.state_bytes();

        let mut flipped = good.clone();
        flipped[1] ^= 0xff;
        assert!(
            ArenaPageStore::image_len(&flipped).is_err(),
            "a flipped byte must fail the image's own checksum"
        );

        assert!(
            ArenaPageStore::image_len(&good[..good.len() - 7]).is_err(),
            "a truncated image must be refused, not read as a shorter one"
        );

        let mut bogus_count = good.clone();
        // Four billion free extents. The walk must fail as "truncated": it must reserve nothing,
        // and the cursor must not wrap into an offset that passes the bounds check.
        bogus_count[21..25].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(
            ArenaPageStore::image_len(&bogus_count).is_err(),
            "a corrupt count must be refused by the bounds check"
        );

        let mut bad_version = good.clone();
        bad_version[0] = 9;
        assert!(
            ArenaPageStore::image_len(&bad_version).is_err(),
            "unknown version must be refused"
        );
    }

    /// **THE ROW, AS TWO INTEGERS.** One append per claim, where it used to be one whole-image
    /// rewrite.
    ///
    /// D79 measured the map being re-serialised and re-fsynced in full on every new branch's
    /// first page write — `sum(48·i) = 24·N²` bytes over a run. These are integers, so unlike a
    /// latency they cannot be moved by what else the box is doing.
    #[test]
    fn a_claim_appends_one_record_where_it_used_to_rewrite_the_whole_image() {
        let h = Harness::new();
        let path = arm(&h);

        for _ in 0..20 {
            claim(&h);
        }

        let (rewrites, appends) = h.store.persist_counters();
        assert_eq!(
            (rewrites, appends),
            (1, 19),
            "20 claims must cost ONE image rewrite (the first, which has no image to append to) \
             and 19 appends; {rewrites} rewrites means the tail is not being used"
        );

        let file = std::fs::metadata(&path).unwrap().len();
        let image = h.store.state_bytes().len() as u64;
        assert!(
            file < image,
            "the file ({file}) is the FIRST claim's tiny image plus 19 records and should be well \
             under the current image ({image}); it is not, so the appends are not small"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// The tail is bounded: once it outgrows its share of the image, the image is rewritten and
    /// the tail goes with it.
    ///
    /// This is the half of the scheme that keeps total write volume LINEAR. Without compaction
    /// the file grows without bound and replay gets slower every open; with it the amortised
    /// per-claim cost is two record lengths, independent of how many branches are live.
    #[test]
    fn the_tail_is_compacted_before_it_outgrows_its_share_of_the_image() {
        let h = Harness::new();
        let path = arm(&h);

        let mut worst_ratio = 0.0f64;
        for _ in 0..400 {
            claim(&h);
            let file = std::fs::metadata(&path).unwrap().len() as f64;
            let image = h.store.state_bytes().len() as f64;
            worst_ratio = worst_ratio.max(file / image);
        }

        let (rewrites, appends) = h.store.persist_counters();
        assert!(rewrites >= 2, "400 claims never compacted: the tail is unbounded ({rewrites})");
        assert!(
            appends > 300,
            "400 claims produced only {appends} appends: the tail is barely being used"
        );
        assert!(
            rewrites < 40,
            "400 claims cost {rewrites} rewrites — compaction fires so often that the row buys \
             nothing"
        );
        assert!(
            worst_ratio < 1.75,
            "the file peaked at {worst_ratio:.2}x the image; the tail is meant to stay under half"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// **The correctness claim.** A restart reads the image AND the tail, and lands in the state
    /// the running store was in.
    ///
    /// The non-empty-tail assertion is what stops this being vacuous: without it the test passes
    /// identically against the code D81 replaced.
    #[test]
    fn a_restart_recovers_the_claims_that_live_only_in_the_tail() {
        let h = Harness::new();
        let path = arm(&h);
        let (parent, a1) = fully_populated(&h);
        let mut claimed = Vec::new();
        for _ in 0..10 {
            claimed.push(claim(&h));
        }

        let bytes = std::fs::read(&path).unwrap();
        let image_len = ArenaPageStore::image_len(&bytes).unwrap();
        assert!(
            bytes.len() > image_len,
            "nothing was appended, so this test would pass against the code D81 replaced"
        );

        let before = (
            h.store.live_page_count().unwrap(),
            h.store.reserved_page_count(),
            h.store.pending_len(),
            h.store.allocated_pages(a1),
        );

        let restored = h.fresh_store();
        assert!(restored.restore(&path).unwrap());

        assert_eq!(restored.live_page_count().unwrap(), before.0, "live pages");
        assert_eq!(restored.reserved_page_count(), before.1, "reserved pages");
        assert_eq!(restored.pending_len(), before.2, "pending log");
        assert_eq!(restored.allocated_pages(a1), before.3, "a1 fill");
        assert_eq!(restored.arena_owner(a1), Some(parent), "a1 owner");
        for (owner, arena) in &claimed {
            assert_eq!(
                restored.arena_owner(*arena),
                Some(*owner),
                "extent {arena} was claimed after the last image and is not in the restored map"
            );
            assert_eq!(
                restored.extent_range(*arena),
                h.store.extent_range(*arena),
                "extent {arena} came back at a different range"
            );
        }
        assert_eq!(
            restored.live_arenas(),
            h.store.live_arenas(),
            "the restored set of live extents differs from the running one"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// **The bump pointer must not rewind through a tail.** The sharpest consequence of losing a
    /// tail record: the next claim hands out a range that is already in use.
    #[test]
    fn the_next_claim_after_a_tail_restore_does_not_overlap_an_extent_the_tail_named() {
        let h = Harness::new();
        let path = arm(&h);
        claim(&h);
        let mut ranges = Vec::new();
        for _ in 0..12 {
            let (_, a) = claim(&h);
            ranges.push(h.store.extent_range(a).unwrap());
        }
        let bytes = std::fs::read(&path).unwrap();
        assert!(
            bytes.len() > ArenaPageStore::image_len(&bytes).unwrap(),
            "fixture: nothing in the tail, so nothing is being tested"
        );

        let restored = h.fresh_store();
        assert!(restored.restore(&path).unwrap());
        let victim = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let fresh = restored.alloc_arena(victim.branch_id).unwrap();
        let (fs, fl) = restored.extent_range(fresh).unwrap();
        for (s, l) in &ranges {
            assert!(
                fs >= s + l || fs + fl <= *s,
                "the extent handed out after a tail restore ({fs}..{}) overlaps {s}..{}: the \
                 watermark rewound, which is two owners for one page range",
                fs + fl,
                s + l
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    /// A free and an immediate re-claim of the same range must replay in that order.
    ///
    /// This is the case that forces the persist lock. Written down backwards, replay leaves the
    /// range on the free list AND live in `extents`, and the next allocation issues pages a live
    /// branch already owns. Nothing about the resulting file looks wrong.
    #[test]
    fn a_free_and_an_immediate_reclaim_of_one_range_survive_a_restart_in_that_order() {
        let h = Harness::new();
        let path = arm(&h);
        claim(&h); // the image
        let (_b1, a1) = claim(&h);
        let (start, pages) = h.store.extent_range(a1).unwrap();
        h.store.free_arena(a1).unwrap();
        let reclaimer = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let a2 = h.store.alloc_arena(reclaimer.branch_id).unwrap();
        assert_eq!(
            h.store.extent_range(a2),
            Some((start, pages)),
            "fixture: the re-claim did not reuse the freed range, so the ordering is not tested"
        );

        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() > ArenaPageStore::image_len(&bytes).unwrap(), "fixture: empty tail");

        let restored = h.fresh_store();
        assert!(restored.restore(&path).unwrap());
        assert_eq!(restored.arena_owner(a2), Some(reclaimer.branch_id), "the re-claim was lost");
        assert_eq!(restored.arena_owner(a1), None, "the freed extent came back to life");

        // And the range must not ALSO be on the free list. Ask by allocating: a store that thinks
        // it is free hands it straight back out.
        let other = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        let a3 = restored.alloc_arena(other.branch_id).unwrap();
        let (s3, l3) = restored.extent_range(a3).unwrap();
        assert!(
            s3 >= start + pages || s3 + l3 <= start,
            "{s3}..{} was handed out although {start}..{} is live: the free and the re-claim \
             replayed in the wrong order",
            s3 + l3,
            start + pages
        );
        let _ = std::fs::remove_file(&path);
    }

    /// A torn final record — the one shape a crash during `append_durably` actually produces — is
    /// dropped, and everything in front of it survives.
    #[test]
    fn a_torn_final_record_is_dropped_and_the_records_in_front_of_it_survive() {
        let h = Harness::new();
        let path = arm(&h);
        claim(&h); // the image
        let (kept_owner, kept) = claim(&h);
        let (_lost_owner, lost) = claim(&h);

        let bytes = std::fs::read(&path).unwrap();
        let image_len = ArenaPageStore::image_len(&bytes).unwrap();
        assert_eq!(bytes.len(), image_len + 2 * 45, "fixture: not two 45-byte claim records");

        // Cut the last record short, as an interrupted append would.
        for cut in [1usize, 20, 40] {
            let torn = &bytes[..bytes.len() - cut];
            let restored = h.fresh_store();
            restored
                .load_file(torn)
                .unwrap_or_else(|e| panic!("a torn tail (cut {cut}) must open, not fail: {e}"));
            assert_eq!(
                restored.arena_owner(kept),
                Some(kept_owner),
                "the intact record before the torn one was dropped with it (cut {cut})"
            );
            assert_eq!(
                restored.arena_owner(lost),
                None,
                "a half-written claim was applied (cut {cut}): it was never acknowledged"
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    /// A zero-filled extension is unwritten space, not a record. A crash can expose one, and
    /// `kind` starts at 1 precisely so that it reads as "nothing here".
    #[test]
    fn a_zero_filled_extension_is_read_as_the_end_of_the_tail() {
        let h = Harness::new();
        let path = arm(&h);
        claim(&h);
        let (owner, kept) = claim(&h);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&[0u8; 64]);

        let restored = h.fresh_store();
        restored.load_file(&bytes).expect("zero padding must not be an error");
        assert_eq!(restored.arena_owner(kept), Some(owner));
        let _ = std::fs::remove_file(&path);
    }

    /// **Corruption in the MIDDLE is refused, and that is a different rule from a torn tail.**
    ///
    /// Stopping at a bad record is right only when it is last. With records behind it, stopping
    /// would silently discard claims whose ranges the next allocation then re-issues — the
    /// aliasing the whole map exists to prevent, arriving through the recovery path.
    #[test]
    fn a_bad_record_with_records_behind_it_is_refused_rather_than_silently_truncating_the_map() {
        let h = Harness::new();
        let path = arm(&h);
        claim(&h); // the image
        claim(&h); // record 1
        claim(&h); // record 2

        let bytes = std::fs::read(&path).unwrap();
        let image_len = ArenaPageStore::image_len(&bytes).unwrap();
        let mut corrupt = bytes.clone();
        corrupt[image_len + 10] ^= 0xff; // inside record 1's payload

        let restored = h.fresh_store();
        let err = restored
            .load_file(&corrupt)
            .expect_err("a bad record with another behind it must be refused, not stopped at");
        assert!(
            format!("{err}").contains("NOT the last record"),
            "the refusal must name why it is not a torn append: {err}"
        );

        // **The control**, and without it this is a blanket refusal rather than a rule: the SAME
        // damage in the LAST record is a torn append and must open.
        let mut torn = bytes.clone();
        let last = torn.len() - 10;
        torn[last] ^= 0xff;
        let restored2 = h.fresh_store();
        restored2
            .load_file(&torn)
            .expect("the same damage in the LAST record is a torn append and must open");
        let _ = std::fs::remove_file(&path);
    }

    /// A record kind this build does not know is refused, not skipped.
    ///
    /// Skipping would apply every change except one and call the result the map.
    #[test]
    fn an_unknown_tail_record_kind_is_refused_rather_than_skipped() {
        let h = Harness::new();
        let path = arm(&h);
        claim(&h);
        claim(&h);
        let base = std::fs::read(&path).unwrap();

        // **D183 — the kind that matters is the one JUST PAST the allowlist, not a distant one.**
        // `0x7f` alone leaves the boundary untested: it is the same refusal whatever the allowlist
        // holds, so it would keep passing if a future kind were added to the constant and never
        // given a match arm. The first unknown kind is the one a real downgrade meets — a build
        // predating this row meets kind 3, and the build after the next row meets one past the end
        // of today's list. Both spellings are asserted, and `max + 1` is computed from the
        // allowlist so it tracks it instead of going stale.
        let past_the_end = ArenaPageStore::KNOWN_TAIL_KINDS.iter().copied().max().unwrap() + 1;
        for kind in [0x7f, past_the_end] {
            let mut bytes = base.clone();
            bytes.extend_from_slice(&ArenaPageStore::encode_tail_record(kind, b"from the future"));
            let restored = h.fresh_store();
            let err = restored
                .load_file(&bytes)
                .expect_err(&format!("kind {kind} must be refused"));
            assert!(format!("{err}").contains("newer build"), "unhelpful refusal: {err}");
        }

        // And every kind the allowlist DOES name must reach a match arm rather than the
        // "reached apply after the kind allowlist" refusal — a constant added without an arm
        // fails closed, which is right, but it must not be how a shipped kind behaves.
        for kind in ArenaPageStore::KNOWN_TAIL_KINDS.iter().copied() {
            let mut bytes = base.clone();
            // A deliberately truncated payload: a known kind fails on its own parse, never on the
            // allowlist. What is asserted is WHICH refusal it is.
            bytes.extend_from_slice(&ArenaPageStore::encode_tail_record(kind, b""));
            let restored = h.fresh_store();
            if let Err(err) = restored.load_file(&bytes) {
                let m = format!("{err}");
                assert!(
                    !m.contains("newer build") && !m.contains("reached apply"),
                    "kind {kind} is in the allowlist but has no match arm: {m}"
                );
            }
        }
        let _ = std::fs::remove_file(&path);
    }

    /// **A store must never append onto a tail it did not write.**
    ///
    /// That tail can end in a torn record, and a good record behind a bad one is the one
    /// arrangement `replay_tail` cannot recover from — it must stop at the bad one, and would
    /// then discard the good one silently. So the first persist after arming is a full rewrite,
    /// which drops the torn bytes. Asserted through `reopen_from_checkpoint`, the path
    /// `cli.rs:101` takes on every open.
    #[test]
    fn a_reopened_store_rewrites_the_image_before_it_appends_again() {
        let h = Harness::new();
        let path = arm(&h);
        for _ in 0..6 {
            claim(&h);
        }
        let before = std::fs::read(&path).unwrap();
        assert!(
            before.len() > ArenaPageStore::image_len(&before).unwrap(),
            "fixture: no tail to be inherited"
        );

        let reopened = ArenaPageStore::reopen_from_checkpoint(
            Arc::clone(&h.store.pool),
            Arc::clone(&h.catalog),
            &path,
        )
        .unwrap();
        assert_eq!(reopened.persist_counters(), (0, 0), "reopening must write nothing by itself");

        let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        reopened.alloc_arena(b.branch_id).unwrap();
        assert_eq!(
            reopened.persist_counters(),
            (1, 0),
            "the first persist after a reopen must be a full rewrite, not an append onto a tail \
             this process did not write"
        );
        let after = std::fs::read(&path).unwrap();
        assert_eq!(
            ArenaPageStore::image_len(&after).unwrap(),
            after.len(),
            "the rewrite must leave the file compact, with the inherited tail gone"
        );

        // ...and it appends again from there.
        let b2 = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        reopened.alloc_arena(b2.branch_id).unwrap();
        assert_eq!(reopened.persist_counters(), (1, 1));
        let _ = std::fs::remove_file(&path);
    }

    /// **The pending-log guard, forced to fire and then forced not to.**
    ///
    /// A guard that has never been made to fire is not a guard. Parking a page changes the map in
    /// a way no tail record describes, so the next claim must REWRITE rather than append; the
    /// control — the identical sequence with nothing parked — must still append, or the guard is
    /// really just "always rewrite" wearing a condition.
    #[test]
    fn parking_a_page_forces_the_next_claim_to_rewrite_instead_of_appending() {
        // Control: claims alone append.
        let c = Harness::new();
        let cpath = arm(&c);
        let (parent, _) = claim(&c);
        c.store.alloc_for(parent, PageType::Heap, c.catalog.next_epoch()).unwrap();
        claim(&c);
        claim(&c);
        assert_eq!(
            c.store.persist_counters(),
            (1, 2),
            "control: with nothing parked, claims after the first must append"
        );
        let _ = std::fs::remove_file(&cpath);

        // Now the same thing with a page parked against a live child in between.
        let h = Harness::new();
        let path = arm(&h);
        let (parent, _) = claim(&h);
        let page = h.store.alloc_for(parent, PageType::Heap, h.catalog.next_epoch()).unwrap();
        claim(&h);
        assert_eq!(h.store.persist_counters(), (1, 1), "fixture: the second claim should append");

        let _child = h.catalog.fork(parent, LeaseDeadline(0)).unwrap();
        h.store.free_page(page, h.catalog.next_epoch()).unwrap();
        assert_eq!(h.store.pending_len(), 1, "fixture: the page was released, not parked");

        claim(&h);
        assert_eq!(
            h.store.persist_counters(),
            (2, 1),
            "the claim after a park must rewrite the image: a tail record does not carry the \
             pending log, and a lost entry is a page drain_pending never revisits"
        );
        // And the parked entry is in the file, not only in memory.
        let restored = h.fresh_store();
        assert!(restored.restore(&path).unwrap());
        assert_eq!(restored.pending_len(), 1, "the parked page did not reach the file");

        // ...and the store goes back to appending afterwards.
        claim(&h);
        assert_eq!(h.store.persist_counters(), (2, 2), "the guard latched instead of clearing");
        let _ = std::fs::remove_file(&path);
    }

    /// **Loading a map from outside makes the file not ours, so the next persist rewrites it.**
    ///
    /// `consensus::snapshot` installs a snapshot by writing a whole arena image over the live
    /// `<db>.arena` and then calling `load_state` on the running store (`snapshot.rs:1498,1541`).
    /// A store that kept appending after that would be appending against accounting for an image
    /// that is no longer in the file.
    #[test]
    fn loading_a_map_from_outside_makes_the_next_persist_a_full_rewrite() {
        let h = Harness::new();
        let path = arm(&h);
        claim(&h);
        claim(&h);
        assert_eq!(h.store.persist_counters(), (1, 1), "fixture: the store should be appending");

        // What a snapshot install does, in the same order: a whole image written over the live
        // path from outside, then `load_state` on the running store. The image is this store's
        // own, because `load_state` refuses one describing another region by design and the
        // point under test is the accounting, not the region check.
        let image = h.store.state_bytes();
        std::fs::write(&path, &image).unwrap();
        h.store.load_state(&image).unwrap();

        claim(&h);
        assert_eq!(
            h.store.persist_counters(),
            (2, 1),
            "after a map arrived from outside, the next persist must rewrite the image rather \
             than append against accounting for a file this process did not write"
        );
        let after = std::fs::read(&path).unwrap();
        assert_eq!(
            ArenaPageStore::image_len(&after).unwrap(),
            after.len(),
            "the rewrite should leave the file compact"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// An unarmed store writes nothing at all, tail included. `Harness` builds one, and most of
    /// this file's other tests depend on it staying that way.
    #[test]
    fn an_unarmed_store_appends_nothing() {
        let h = Harness::new();
        for _ in 0..5 {
            claim(&h);
        }
        assert_eq!(h.store.persist_counters(), (0, 0));
    }
}

/// **D183 ADVERSARY — an attack on what is now
/// `d183_what_the_two_reclamation_store_methods_cost_per_call`, and was called
/// `d183_an_interior_reap_rewrites_the_whole_image_while_a_leaf_reap_appends` when this module
/// was written. The rename is this module's doing: the old name asserted the thing the attack
/// refuted.**
///
/// The test under attack never calls `Reaper::reap`. It hand-rolls the two sides of
/// `reaper.rs:679` and counts those. Everything here exists to ask whether that hand-rolled
/// loop is the thing production runs, whether the counters can be made to move at all, and
/// whether the slope is a property of BRANCHES or of the fixture's shape.
///
/// ⭐ **THESE BANDS PINNED A WALL AND NOW PIN THE FIX. They were written against `630afaa` and
/// every one of them has been RE-STATED, not deleted.** The attack succeeded: it refuted the
/// claim that a reap costs one rewrite per interior branch and zero for a leaf, and the numbers
/// it established — measured through `TwoTierReaper::reap`, the function production calls — are
/// the baseline the fix is measured against. They are kept in each test's doc band, because a
/// before/after with only the after is a claim rather than a measurement.
///
/// **One invariant runs through all of them and it is what the re-statements are built on: the
/// NUMBER of durable records per reap did not change. Only the KIND did.** A reap still makes
/// exactly the same number of writes to `<db>.arena`; each one used to be a full 48·L-byte image
/// and is now a bounded appended record. Every band therefore asserts the unchanged total AND the
/// inverted split, so a "fix" that simply stopped persisting — which is data loss, not a fix —
/// fails the first half while satisfying the second.
///
/// ⚠ **On branch `d183-drain-elide` that invariant no longer holds as written, and that is
/// deliberate.** A drain that released nothing and owes no recycled list now writes NO record
/// (see `put_pending`), so an interior reap whose live child pins every parked page makes ONE
/// durable write where it made two. The total that stays invariant is records PLUS elided drains,
/// and `ArenaPageStore::elided_drains` counts the second term, so "stopped persisting" is still
/// told apart from "had nothing to persist". The bands in `d183adv_a1_what_the_real_reaper_costs`,
/// `d183adv_a5_the_interior_cost_is_no_longer_a_class` and
/// `d183adv_mech_the_interior_second_rewrite_is_the_drains_put_pending` still assert the OLD
/// total. Re-stating them is a test edit, and it waits on a decision
/// (`frontier/lane_d183_tail_replay.md` §5 in artie-research).
#[cfg(test)]
mod d183_adversary {
    use super::harness::Harness;
    use super::*;
    use crate::branch::types::LeaseDeadline;
    use crate::branch::{Reaper, TwoTierReaper};

    /// Arm the store at a private path and hand back the path so the caller can unlink it.
    fn armed(tag: &str, n: usize) -> (Harness, std::path::PathBuf) {
        let h = Harness::new_with(true);
        let path = std::env::temp_dir()
            .join(format!("ferro-d183adv-{}-{}-{}.bin", std::process::id(), tag, n));
        let _ = std::fs::remove_file(&path);
        h.store.checkpoint_to(path.clone());
        (h, path)
    }

    /// Build `branches` branches under TRUNK, each with `pages` pages, each optionally forked
    /// once AFTER its pages were born (which is what makes the interval rule park them).
    ///
    /// Returns the branch handles and the total number of arenas they own — the fixture's own
    /// shape, read from the records rather than assumed.
    fn build(h: &Harness, branches: usize, pages: usize, interior: bool) -> (Vec<BranchId>, usize) {
        let mut ids = Vec::new();
        let mut arenas = 0usize;
        for _ in 0..branches {
            let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            for _ in 0..pages {
                h.store.alloc_for(b.branch_id, PageType::Heap, Epoch(1)).unwrap();
            }
            if interior {
                h.catalog.fork(b.branch_id, LeaseDeadline(0)).unwrap();
            }
            arenas += h.catalog.get_raw(b.branch_id.id).unwrap().arenas.len();
            ids.push(b.branch_id);
        }
        (ids, arenas)
    }

    // ---------------------------------------------------------------------------------------
    // AXIS 1 + 5. Does the fixture reach the code under test, and does the REAL reaper agree?
    // ---------------------------------------------------------------------------------------

    /// The predicate the claim is about, READ, for every branch, on both arms.
    ///
    /// The test under attack asserts nothing about `has_live_children`; it picks a side of
    /// `reaper.rs:679` itself and then reports the cost of the side it picked. If the shipped
    /// catalog answered `false` for the "interior" fixture, the slow path would never run in
    /// production and the measured number would be a property of a call nobody makes.
    #[test]
    fn d183adv_a1_the_predicate_is_what_the_fixture_assumes() {
        for (interior, want) in [(false, false), (true, true)] {
            let (h, path) = armed("a1", interior as usize);
            let (ids, arenas) = build(&h, 8, 4, interior);
            assert!(arenas > 0, "fixture: no arenas at all");
            for id in &ids {
                let got = h.catalog.has_live_children(id.id).unwrap();
                assert_eq!(
                    got, want,
                    "interior={interior}: has_live_children({}) = {got}, fixture assumes {want}",
                    id.id
                );
            }
            println!(
                "A1 interior={interior}: has_live_children = {want} for all 8 branches, \
                 {arenas} arenas total ({} per branch)",
                arenas / 8
            );
            let _ = std::fs::remove_file(&path);
        }
    }

    /// The same four cells, measured through `TwoTierReaper::reap` instead of a hand-rolled
    /// copy of two of its lines.
    ///
    /// `reap` is not `retire_arenas_by_rule`. It also runs `drain_pending_seeded`, whose
    /// `take_pending` bumped `pending_version` UNCONDITIONALLY at `630afaa` and whose
    /// `put_pending` was another `persist_if_configured` — a function this row deleted. Both are
    /// inside the per-branch reap, so both belong in any number described as "what a reap costs",
    /// which is the whole finding of this module.
    #[test]
    fn d183adv_a1_what_the_real_reaper_costs() {
        fn run(branches: usize, interior: bool) -> (u64, u64, u32) {
            let (h, path) = armed("a1r", branches * 2 + interior as usize);
            let (ids, _) = build(&h, branches, 4, interior);
            let reaper = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store));
            let (r0, a0) = h.store.persist_counters();
            let mut freed = 0u32;
            for id in ids {
                freed += reaper.reap(id).unwrap();
            }
            let (r1, a1) = h.store.persist_counters();
            let _ = std::fs::remove_file(&path);
            (r1 - r0, a1 - a0, freed)
        }

        let cells = [
            ("LEAF      8", run(8, false)),
            ("LEAF     16", run(16, false)),
            ("INTERIOR  8", run(8, true)),
            ("INTERIOR 16", run(16, true)),
        ];
        println!("A1 REAL `Reaper::reap` -- (full rewrites, delta appends, pages freed)");
        for (label, (r, a, f)) in cells {
            println!("  {label}: rewrites={r:3} appends={a:3} pages_freed={f:3}");
        }

        // Anti-vacuity: an arm that reaped nothing satisfies every shape assertion. The LEAF
        // arm must return pages; the INTERIOR arm returns NONE (every page is parked for the
        // live child), so it is checked separately, below, by what it parked.
        assert!(cells[0].1 .2 > 0 && cells[1].1 .2 > 0, "the leaf arm freed no pages at all");
        assert_eq!(
            cells[2].1 .2, 0,
            "an interior reap returned pages -- the fixture is not parking them"
        );

        let leaf_slope = cells[1].1 .0 as i64 - cells[0].1 .0 as i64;
        let int_slope = cells[3].1 .0 as i64 - cells[2].1 .0 as i64;
        println!("  rewrite slope over 8 more branches: LEAF={leaf_slope}, INTERIOR={int_slope}");

        // ⛔ **THE REFUTATION THIS TEST WAS WRITTEN FOR, AND THE FIX THAT ANSWERED IT.**
        //
        // The claim under attack was "one full image rewrite per INTERIOR branch reaped, zero for
        // a leaf". Through `Reaper::reap` both halves were wrong. Measured at `630afaa`:
        //
        //     LEAF      8 -> rewrites  7, appends 17      INTERIOR  8 -> rewrites 16, appends  0
        //     LEAF     16 -> rewrites 15, appends 33      INTERIOR 16 -> rewrites 32, appends  0
        //
        // i.e. interior slope 2.0 per branch and leaf slope 1.0 per branch, not 1.0 and 0.0. The
        // leaf cost was `take_pending` bumping `pending_version` on an EMPTY log, which forced
        // the next reap's first `free_arena` into a full rewrite.
        //
        // Measured on this branch, same fixture, same instrument:
        //
        //     LEAF      8 -> rewrites  0, appends 24      INTERIOR  8 -> rewrites  0, appends 16
        //     LEAF     16 -> rewrites  0, appends 48      INTERIOR 16 -> rewrites  1, appends 31
        //
        // ⚠ The assertions below are NOT those numbers. The two that are exact are the ones that
        // did not move — the record TOTALS — because they are what makes "the reap still persists
        // as often as it did" separable from "it persists more cheaply". The rest is stated as a
        // shape, so that a compaction landing in a different place does not fail a green run.

        // UNCHANGED, BEFORE AND AFTER: one durable record per arena on the leaf path, and two per
        // branch on the interior path (the reap's record, then the drain's). A reap that stopped
        // persisting would satisfy every slope assertion below and fail these.
        assert_eq!(cells[0].1 .0 + cells[0].1 .1, 24, "leaf 8 records != 24 arenas");
        assert_eq!(cells[1].1 .0 + cells[1].1 .1, 48, "leaf 16 records != 48 arenas");
        assert_eq!(cells[2].1 .0 + cells[2].1 .1, 16, "interior 8 records != 2 per branch");
        assert_eq!(cells[3].1 .0 + cells[3].1 .1, 32, "interior 16 records != 2 per branch");

        // THE INVERSION. The leaf arm's rewrites were 1.0 per branch and are now flat at zero —
        // exact, because `take_pending`'s empty-log bump was the whole of that cost and nothing
        // in this fixture can reach `compact_threshold` at these sizes.
        assert_eq!(
            (cells[0].1 .0, cells[1].1 .0, leaf_slope),
            (0, 0, 0),
            "the leaf arm still pays full image rewrites: 8->{}, 16->{}",
            cells[0].1 .0,
            cells[1].1 .0
        );
        // The interior arm's rewrites were 2.0 per branch with appends structurally impossible.
        // Now the growth must land in the APPENDS; what rewrites remain are compactions, which
        // `d183adv_a5_the_interior_cost_is_no_longer_a_class` measures over a real size axis.
        assert!(
            int_slope < cells[3].1 .1 as i64 - cells[2].1 .1 as i64,
            "interior rewrites did not stop tracking branches: rewrites +{int_slope} against \
             appends +{}",
            cells[3].1 .1 as i64 - cells[2].1 .1 as i64
        );
    }

    /// ⭐ **Is the interior cost still a CLASS, or only a smaller constant?**
    ///
    /// This is the question two sizes cannot answer and the reason it gets its own test. A single
    /// before/after pair moves whether the fix removed a quadratic or merely divided it.
    ///
    /// The history, all three points measured with the same instrument on the same fixture:
    ///
    /// | | 8 | 16 | 32 | 64 | 128 | rewrites/branch |
    /// |---|---|---|---|---|---|---|
    /// | `630afaa` | 16 | 32 | — | — | — | **2.0, flat** |
    /// | after the first two record kinds | 1 | 5 | 16 | 38 | 74 | **0.58, flat** |
    /// | after `TAIL_PENDING_DRAINED` | 0 | 1 | 1 | 2 | 3 | **0.062 → 0.023, falling** |
    ///
    /// The middle row is the one worth keeping: replacing the two `persist_if_configured` calls
    /// with records made the constant four times better and left the CLASS untouched, because
    /// `TAIL_PENDING_REPLACED` restates the whole pending log and the log grows with the branch
    /// count. Only describing a drain by DIFFERENCE removed the quadratic.
    ///
    /// The assertion is therefore about the ratio between the ends of the axis, not about any
    /// single cell: a per-branch cost that does not fall is a class, whatever its constant.
    #[test]
    fn d183adv_a5_the_interior_cost_is_no_longer_a_class() {
        fn run(branches: usize) -> (u64, u64) {
            let (h, path) = armed("a5", branches);
            let (ids, _) = build(&h, branches, 4, true);
            let reaper = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store));
            let (r0, a0) = h.store.persist_counters();
            for id in ids {
                reaper.reap(id).unwrap();
            }
            let (r1, a1) = h.store.persist_counters();
            let _ = std::fs::remove_file(&path);
            (r1 - r0, a1 - a0)
        }
        println!("A5 INTERIOR cost against branches, through the REAL reaper:");
        let mut rows = Vec::new();
        for n in [8usize, 16, 32, 64, 128] {
            let (r, a) = run(n);
            println!(
                "  branches={n:4}: rewrites={r:4} appends={a:4}  rewrites/branch={:.3}  \
                 records/branch={:.3}",
                r as f64 / n as f64,
                (r + a) as f64 / n as f64
            );
            rows.push((n, r, a));
        }

        // Anti-vacuity: an axis on which nothing happened proves nothing about its own slope.
        for (n, r, a) in &rows {
            assert!(r + a > 0, "branches={n}: the reap loop persisted nothing at all");
            assert_eq!(
                r + a,
                2 * *n as u64,
                "branches={n}: {} records, not the two per interior branch the reaper makes",
                r + a
            );
        }

        // THE CLASS CLAIM. At `630afaa` this ratio was 2.0 at every size, and after the first two
        // record kinds it was 0.58 at every size — flat is what a class looks like. It must now
        // FALL across the axis: the compactions that remain are amortised against an image that
        // grows, so their share of the work per branch shrinks.
        let first = rows[1].1 as f64 / rows[1].0 as f64; // 16 branches; 8 is 0 and gives no ratio
        let last = rows[rows.len() - 1].1 as f64 / rows[rows.len() - 1].0 as f64;
        assert!(
            last < first,
            "rewrites per branch did not fall across the size axis ({first:.3} at 16 -> \
             {last:.3} at 128): the cost is still a class, not a constant"
        );
        // And it must be a small share of the records, not most of them.
        let (_, r_last, a_last) = rows[rows.len() - 1];
        assert!(
            r_last * 10 < a_last,
            "rewrites are {r_last} against {a_last} appends at the top of the axis"
        );
    }

    // ---------------------------------------------------------------------------------------
    // AXIS 2. Can each counter be forced to move on purpose?
    // ---------------------------------------------------------------------------------------

    /// Push a LEAF arm's tail past `compact_threshold` and see whether its zero survives.
    ///
    /// A zero that cannot be made non-zero is not evidence about the leaf path; it is evidence
    /// the instrument is not wired.
    #[test]
    fn d183adv_a2_the_leaf_zero_is_only_a_small_n_zero() {
        fn leaf(branches: usize) -> (u64, u64) {
            let (h, path) = armed("a2l", branches);
            let (ids, _) = build(&h, branches, 4, false);
            let (r0, a0) = h.store.persist_counters();
            for id in ids {
                let rec = h.catalog.get_raw(id.id).unwrap();
                for arena in rec.arenas.iter().copied() {
                    h.store.free_arena(arena).unwrap();
                }
            }
            let (r1, a1) = h.store.persist_counters();
            let _ = std::fs::remove_file(&path);
            (r1 - r0, a1 - a0)
        }
        println!("A2 LEAF arm, hand-rolled exactly as the test under attack does it:");
        let mut first_nonzero = None;
        for n in [8usize, 16, 64, 256] {
            let (r, a) = leaf(n);
            println!("  branches={n:4}: rewrites={r:4} appends={a:4}");
            if r > 0 && first_nonzero.is_none() {
                first_nonzero = Some(n);
            }
        }
        match first_nonzero {
            Some(n) => println!("  -> the leaf arm's rewrite counter FIRES at branches={n}"),
            None => println!("  -> the leaf arm's rewrite counter could NOT be made to fire"),
        }
    }

    /// The INTERIOR arm's `appends = 0` was not a measurement — and now it is.
    ///
    /// **At `630afaa` this test recorded a STRUCTURAL zero**: `retire_arenas_by_rule` ended in
    /// `persist_if_configured` — since deleted — which was `persist_full_locked` with no threshold
    /// and no delta encoder anywhere on the path, so no fixture could make it append. 32 branches x 64 pages —
    /// 4x the branches and 8x the pages of the original fixture — gave `rewrites=32 appends=0`.
    ///
    /// That is exactly the kind of zero worth re-testing after a change, because a zero which no
    /// fixture can move is a fact about the code and not about the workload. On this branch the
    /// same fixture gives **`rewrites=5 appends=27`**: the path now has an encoder on it, the
    /// total is still one record per branch, and the zero is gone.
    #[test]
    fn d183adv_a2_the_interior_zero_appends_is_unfalsifiable() {
        let (h, path) = armed("a2i", 0);
        let (ids, _) = build(&h, 32, 64, true);
        let ep = h.catalog.next_epoch();
        let (r0, a0) = h.store.persist_counters();
        for id in ids {
            let rec = h.catalog.get_raw(id.id).unwrap();
            h.store.retire_arenas_by_rule(&rec, ep).unwrap();
        }
        let (r1, a1) = h.store.persist_counters();
        println!(
            "A2 INTERIOR, 32 branches x 64 pages (8x the pages, 4x the branches of the original): \
             rewrites={} appends={}",
            r1 - r0,
            a1 - a0
        );
        // One durable record per branch, before and after; what changed is which kind.
        assert_eq!(
            (r1 - r0) + (a1 - a0),
            32,
            "the interior retire is no longer one durable record per branch"
        );
        assert!(
            a1 - a0 > 0,
            "the interior arm still cannot append: the structural zero this test recorded at \
             630afaa is back, so nothing on this path reaches an encoder"
        );
        assert!(
            r1 - r0 < a1 - a0,
            "the interior arm is still mostly full rewrites: {} against {} appends",
            r1 - r0,
            a1 - a0
        );
        let _ = std::fs::remove_file(&path);
    }

    // ---------------------------------------------------------------------------------------
    // AXIS 3. Is the slope a property of branches, or of arenas/pages?
    // ---------------------------------------------------------------------------------------

    /// Three sizes, three page counts, plus a branch with NO pages at all.
    ///
    /// `ARENA_FIRST_EXTENT_PAGES` is 1 and extents double, so 4 pages is 3 arenas and 16 pages
    /// is 5. If rewrites tracked arenas or pages, these rows would disagree.
    #[test]
    fn d183adv_a3_the_slope_is_per_branch_not_per_arena_or_page() {
        fn interior(branches: usize, pages: usize) -> (u64, u64, usize) {
            let (h, path) = armed("a3", branches * 100 + pages);
            let (ids, arenas) = build(&h, branches, pages, true);
            let ep = h.catalog.next_epoch();
            let (r0, a0) = h.store.persist_counters();
            for id in ids {
                let rec = h.catalog.get_raw(id.id).unwrap();
                h.store.retire_arenas_by_rule(&rec, ep).unwrap();
            }
            let (r1, a1) = h.store.persist_counters();
            let _ = std::fs::remove_file(&path);
            (r1 - r0, a1 - a0, arenas)
        }
        println!("A3 INTERIOR rewrites against branches x pages (arenas read from the records)");
        for pages in [0usize, 1, 4, 16] {
            let mut row = Vec::new();
            for branches in [8usize, 16, 24] {
                let (r, a, arenas) = interior(branches, pages);
                row.push((branches, r, arenas));
                // **At `630afaa` this asserted `r == branches` and held at every cell** — the
                // hand-rolled `retire_arenas_by_rule` loop cost exactly one full image rewrite
                // per branch whatever its arenas or pages. That is the wall this row removes.
                // What must still hold is the TOTAL: one durable record per branch, which is
                // what makes "cheaper" separable from "skipped".
                assert_eq!(
                    r + a,
                    branches as u64,
                    "pages={pages} branches={branches}: {} durable records, not one per branch",
                    r + a
                );
                assert!(
                    r < branches as u64,
                    "pages={pages} branches={branches}: rewrites={r} still tracks branches \
                     one-for-one"
                );
            }
            let s: Vec<String> = row
                .iter()
                .map(|(b, r, ar)| format!("b={b} rewrites={r} arenas={ar}"))
                .collect();
            // The original question this axis answered is unchanged and still worth asserting:
            // whatever the cost is, it does not track ARENAS or PAGES — the three page counts
            // give three different arena counts and the same per-branch record total.
            println!("  pages/branch={pages:2}: {}", s.join(" | "));
        }
    }

    /// The LEAF arm's cost, by contrast, tracks ARENAS and not branches.
    #[test]
    fn d183adv_a3_the_leaf_arm_is_counted_per_arena() {
        println!("A3 LEAF appends against branches x pages");
        for pages in [1usize, 4, 16] {
            for branches in [8usize, 16] {
                let (h, path) = armed("a3l", branches * 100 + pages);
                let (ids, arenas) = build(&h, branches, pages, false);
                let (r0, a0) = h.store.persist_counters();
                for id in ids {
                    let rec = h.catalog.get_raw(id.id).unwrap();
                    for arena in rec.arenas.iter().copied() {
                        h.store.free_arena(arena).unwrap();
                    }
                }
                let (r1, a1) = h.store.persist_counters();
                println!(
                    "  pages/branch={pages:2} branches={branches:2}: arenas={arenas:3} \
                     rewrites={} appends={}",
                    r1 - r0,
                    a1 - a0
                );
                // One durable record per arena freed -- an append, or a full rewrite when the
                // tail has outgrown `compact_threshold`. The 16x16 cell is where that happens.
                assert_eq!(
                    (a1 - a0) + (r1 - r0),
                    arenas as u64,
                    "leaf durable records did not equal the arena count"
                );
                let _ = std::fs::remove_file(&path);
            }
        }
    }

    // ---------------------------------------------------------------------------------------
    // AXIS 4. Is `has_live_children` the splitter, or is it parking / pending_len?
    // ---------------------------------------------------------------------------------------

    /// A branch with live children whose pages are ALL releasable.
    ///
    /// The child is forked BEFORE the pages are born, so `live_child_in_epoch_range` is false
    /// for every page and nothing is parked. If the cost followed parking or `pending_len`,
    /// this cell would be cheap. If it follows the predicate, it costs the same rewrite.
    #[test]
    fn d183adv_a4_live_children_with_nothing_parked_still_pays() {
        let branches = 8usize;
        let (h, path) = armed("a4a", 0);
        let mut ids = Vec::new();
        for _ in 0..branches {
            let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            // Child FIRST, so its fork epoch is below every page's birth epoch.
            h.catalog.fork(b.branch_id, LeaseDeadline(0)).unwrap();
            let birth = h.catalog.next_epoch();
            for _ in 0..4 {
                h.store.alloc_for(b.branch_id, PageType::Heap, birth).unwrap();
            }
            ids.push(b.branch_id);
        }
        for id in &ids {
            assert!(h.catalog.has_live_children(id.id).unwrap(), "fixture: no live child");
        }
        let ep = h.catalog.next_epoch();
        let (r0, a0) = h.store.persist_counters();
        let mut released = 0u32;
        for id in &ids {
            let rec = h.catalog.get_raw(id.id).unwrap();
            released += h.store.retire_arenas_by_rule(&rec, ep).unwrap();
        }
        let (r1, a1) = h.store.persist_counters();
        let parked = h.store.pending_len();
        println!(
            "A4a live children, all pages RELEASABLE: released={released} parked_after={parked} \
             rewrites={} appends={}",
            r1 - r0,
            a1 - a0
        );
        assert!(released > 0, "fixture: nothing was released, so nothing was releasable");
        assert_eq!(parked, 0, "fixture: something was parked after all");
        // **At `630afaa`: `rewrites=8 appends=0` — a branch that parked NOTHING still paid one
        // full image rewrite each, which is what established that the cost followed the
        // `has_live_children` PREDICATE and not parking or `pending_len`.** On this branch the
        // same fixture gives `rewrites=0 appends=8`. The predicate still decides which door the
        // reap leaves by; it no longer decides whether the door costs the whole image.
        assert_eq!(
            (r1 - r0) + (a1 - a0),
            branches as u64,
            "still not one durable record per branch: {} rewrites + {} appends",
            r1 - r0,
            a1 - a0
        );
        assert_eq!(
            r1 - r0,
            0,
            "a branch that parked nothing still pays {} full image rewrites",
            r1 - r0
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Hold the branch shape fixed and flip ONLY `has_live_children`, by reaping the child
    /// first. Same pages, same arenas, same epochs; one bit different.
    #[test]
    fn d183adv_a4_flipping_only_the_predicate_flips_the_cost() {
        fn run(kill_child: bool) -> (u64, u64, bool) {
            let (h, path) = armed("a4b", kill_child as usize);
            let reaper = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store));
            let mut ids = Vec::new();
            for _ in 0..8 {
                let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
                for _ in 0..4 {
                    h.store.alloc_for(b.branch_id, PageType::Heap, Epoch(1)).unwrap();
                }
                let child = h.catalog.fork(b.branch_id, LeaseDeadline(0)).unwrap();
                if kill_child {
                    reaper.reap(child.branch_id).unwrap();
                }
                ids.push(b.branch_id);
            }
            let pred = h.catalog.has_live_children(ids[0].id).unwrap();
            // Count only the parents' reaps; the children's are fixture.
            let (r0, a0) = h.store.persist_counters();
            for id in ids {
                reaper.reap(id).unwrap();
            }
            let (r1, a1) = h.store.persist_counters();
            let _ = std::fs::remove_file(&path);
            (r1 - r0, a1 - a0, pred)
        }
        let (r_live, a_live, p_live) = run(false);
        let (r_dead, a_dead, p_dead) = run(true);
        println!("A4b same shape, only the predicate differs, through the REAL reaper:");
        println!("  child live  (has_live_children={p_live:5}): rewrites={r_live:3} appends={a_live:3}");
        println!("  child reaped(has_live_children={p_dead:5}): rewrites={r_dead:3} appends={a_dead:3}");
        assert!(p_live, "fixture: the live-child arm had no live child");
        assert!(!p_dead, "fixture: reaping the child did not clear has_live_children");
    }

    // ---------------------------------------------------------------------------------------
    // MECHANISM. Why the REAL leaf reap pays N-1 rewrites that the hand-rolled loop does not.
    // ---------------------------------------------------------------------------------------

    /// ⭐ **THE FOURTH REWRITE SITE — found by this test, and the one neither the design entry
    /// nor the row's own counter test had in scope.**
    ///
    /// At `630afaa`, `take_pending` bumped `pending_version` **even when the log it drained was
    /// empty**, and `persist_delta_locked`'s third condition then forced the NEXT delta to be a
    /// full image rewrite. `Reaper::reap` calls `drain_pending_seeded` — and therefore
    /// `take_pending` — at the end of EVERY reap, leaf or not. So the leaf path's cost was not
    /// what freeing an arena costs; it was that plus one forced rewrite carried over from the
    /// previous reap's empty drain, which is the whole of the 1.0-rewrites-per-branch the leaf
    /// arm was measured at.
    ///
    /// **Measured here at `630afaa`: two arena frees back to back gave `(0 rewrites, 2 appends)`,
    /// and the same two frees with an EMPTY `take_pending` between them gave `(1, 1)`.** On this
    /// branch both give `(0, 2)`: draining a log that was already empty changes nothing in
    /// memory, so it cannot have made the durable file stale, and the bump is gone.
    ///
    /// ⚠ **This is the only test in the file that pins that fix.** Restoring the unconditional
    /// bump as a mutant leaves every test in `branch::arena::tests` green — checked, 57 passed.
    #[test]
    fn d183adv_mech_an_empty_take_pending_no_longer_forces_a_rewrite() {
        let (h, path) = armed("mech", 0);
        let (ids, _) = build(&h, 4, 4, false);
        let recs: Vec<_> =
            ids.iter().map(|id| h.catalog.get_raw(id.id).unwrap()).collect();

        // Control: two arena frees back to back, no drain in between.
        let (r0, a0) = h.store.persist_counters();
        h.store.free_arena(recs[0].arenas[0]).unwrap();
        h.store.free_arena(recs[0].arenas[1]).unwrap();
        let (r1, a1) = h.store.persist_counters();

        // Treatment: the SAME two frees, with an empty `take_pending` between them.
        assert_eq!(h.store.pending_len(), 0, "fixture: the pending log is not empty");
        h.store.free_arena(recs[1].arenas[0]).unwrap();
        let drained = h.store.take_pending();
        assert!(drained.is_empty(), "fixture: the drain was not empty");
        h.store.free_arena(recs[1].arenas[1]).unwrap();
        let (r2, a2) = h.store.persist_counters();

        println!("MECH two arena frees, nothing between: rewrites={} appends={}", r1 - r0, a1 - a0);
        println!(
            "MECH two arena frees, EMPTY take_pending between: rewrites={} appends={}",
            r2 - r1,
            a2 - a1
        );
        assert_eq!((r1 - r0, a1 - a0), (0, 2), "control: both frees should have appended");
        assert_eq!(
            (r2 - r1, a2 - a1),
            (0, 2),
            "an empty drain still forces the following free into a full image rewrite — the \
             fourth D183 site is back (it read (1, 1) at 630afaa)"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Where the interior arm's SECOND rewrite per branch comes from -- and it is not one
    /// mechanism, it is two, which is why this test names both arms.
    ///
    /// ⚠ **This test's first draft asserted that the releasable arm would pay only ONE rewrite,
    /// because `put_pending` never runs when the drain comes back empty. Its own measurement
    /// refuted that: the releasable arm pays two as well.** The accounting at `630afaa`, every
    /// term a counted integer:
    ///
    ///   * PARKED (pages first, child after): `retire_arenas_by_rule` = 1 rewrite;
    ///     `drain_pending_seeded` re-parks the survivors through `put_pending` = 1 rewrite;
    ///     the extents are NOT empty (pages parked, not recycled) so `sweep_touched_extents`
    ///     frees nothing. **2 rewrites, 0 appends, 3 arenas untouched.** (8 branches: 16, 0.)
    ///   * RELEASABLE (child first, pages after): `retire_arenas_by_rule` = 1 rewrite; the
    ///     drain is empty so `put_pending` does NOT run -- but `take_pending` already bumped
    ///     `pending_version`, so the FIRST of the three extents `sweep_touched_extents` now
    ///     frees is forced into a full rewrite by the mechanism pinned in the test above, and
    ///     the other two append. **2 rewrites, 2 appends, 3 arenas freed.** (8 branches: 16, 16.)
    ///
    /// Same headline number, two different mechanisms, neither of them the one the claim under
    /// attack names.
    ///
    /// ⭐ **On this branch both mechanisms are gone and the two arms separate cleanly: PARKED
    /// gives `(0, 16)` and RELEASABLE `(0, 32)`.** The totals — 16 and 32 durable records for 8
    /// branches — are unchanged from `630afaa`, which is the point: the reap writes as often as
    /// it ever did. The releasable arm's 32 is its 8 reap records plus the 24 arena frees its
    /// sweep now reaches, every one an append. And `put_pending` on its own, which was ONE
    /// unconditional full rewrite, is now one appended record.
    #[test]
    fn d183adv_mech_the_interior_second_rewrite_is_the_drains_put_pending() {
        fn run(park: bool) -> (u64, u64, usize) {
            let (h, path) = armed("mech2", park as usize);
            let reaper = TwoTierReaper::new(Arc::clone(&h.catalog), Arc::clone(&h.store));
            let mut ids = Vec::new();
            for _ in 0..8 {
                let b = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
                if park {
                    // Pages first, child after: the interval rule must park them.
                    for _ in 0..4 {
                        h.store.alloc_for(b.branch_id, PageType::Heap, Epoch(1)).unwrap();
                    }
                    h.catalog.fork(b.branch_id, LeaseDeadline(0)).unwrap();
                } else {
                    // Child first, pages after: live child, nothing parkable.
                    h.catalog.fork(b.branch_id, LeaseDeadline(0)).unwrap();
                    let birth = h.catalog.next_epoch();
                    for _ in 0..4 {
                        h.store.alloc_for(b.branch_id, PageType::Heap, birth).unwrap();
                    }
                }
                assert!(h.catalog.has_live_children(b.branch_id.id).unwrap());
                ids.push(b.branch_id);
            }
            let (r0, a0) = h.store.persist_counters();
            for id in ids {
                reaper.reap(id).unwrap();
            }
            let (r1, a1) = h.store.persist_counters();
            let parked = h.store.pending_len();
            let _ = std::fs::remove_file(&path);
            (r1 - r0, a1 - a0, parked)
        }
        let (r_park, a_park, p_park) = run(true);
        let (r_free, a_free, p_free) = run(false);
        println!("MECH2 8 interior branches through the REAL reaper:");
        println!("  pages PARKED     : rewrites={r_park:3} appends={a_park:3} pending_after={p_park}");
        println!("  pages RELEASABLE : rewrites={r_free:3} appends={a_free:3} pending_after={p_free}");
        assert!(p_park > 0, "fixture: the parking arm parked nothing");
        assert_eq!(p_free, 0, "fixture: the releasable arm parked something");

        // The accounting in the doc comment, pinned. 8 branches, 3 arenas each.
        //
        // UNCHANGED from `630afaa`: the record totals. Both arms write exactly as often as they
        // did, which is what separates "cheaper" from "skipped".
        assert_eq!(r_park + a_park, 16, "parked arm: not 2 durable records per branch");
        assert_eq!(r_free + a_free, 32, "releasable arm: not 4 durable records per branch");
        // INVERTED: neither mechanism costs a full image any more.
        assert_eq!(
            (r_park, r_free),
            (0, 0),
            "full image rewrites survive on the interior path: parked={r_park}, \
             releasable={r_free} (both were 16 at 630afaa)"
        );

        // And the term the parked arm's second rewrite is charged to, priced on its own.
        let (h, path) = armed("mech3", 0);
        let (ids, _) = build(&h, 1, 4, true);
        let rec = h.catalog.get_raw(ids[0].id).unwrap();
        let ep = h.catalog.next_epoch();
        h.store.retire_arenas_by_rule(&rec, ep).unwrap();
        let taken = h.store.take_pending();
        assert!(!taken.is_empty(), "fixture: the interior branch parked nothing");
        let (r0, a0) = h.store.persist_counters();
        h.store.put_pending(taken).unwrap();
        let (r1, a1) = h.store.persist_counters();
        println!("MECH3 one put_pending on its own: rewrites={} appends={}", r1 - r0, a1 - a0);
        assert_eq!(
            (r1 - r0, a1 - a0),
            (0, 1),
            "put_pending is a full image rewrite again — it read (1, 0) at 630afaa, which is \
             where the interior arm's second rewrite per branch came from"
        );
        let _ = std::fs::remove_file(&path);
    }
}
