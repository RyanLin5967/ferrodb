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
    /// `alloc_for` advances `next_free` without persisting — the `persist_if_configured` sites are
    /// all off the page path — so an extent checkpointed while empty and then filled comes back
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
    pending: Vec<PendingFree>,
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
/// `REPLACE_LOCK`, and never acquired while any of them is held. Every `persist_if_configured`
/// call site already drops the `state` guard before persisting, which is what makes that rule
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
    /// Full-image rewrites and tail appends this store has performed.
    ///
    /// Per-STORE, where `storage::atomic_file`'s counters are per-process. Both exist and neither
    /// replaces the other: a benchmark wants the process total, and an assertion cannot use it,
    /// because tests run concurrently in one process and would be reading each other's writes.
    /// The quantity this row is about is an integer, so it is worth being able to assert exactly.
    rewrites: u64,
    appends: u64,
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
                pending: Vec::new(),
                claim_epoch: HashMap::new(),
                shadow_base: HashMap::new(),
            }),
            live_pages: AtomicU32::new(0),
            reserved_pages: AtomicU32::new(0),
            authority_epoch: AtomicU64::new(crate::cluster::epoch()),
            checkpoint_path: Mutex::new(None),
            persist: Mutex::new(PersistState {
                image_bytes: 0,
                tail_bytes: 0,
                image_epoch: crate::cluster::epoch(),
                durable_pending_version: 0,
                rewrites: 0,
                appends: 0,
            }),
            pending_version: AtomicU64::new(0),
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

    /// Entries in the pending-free log.
    pub fn pending_len(&self) -> usize {
        self.state.lock().unwrap().pending.len()
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

    /// Take the pending-free log for re-evaluation.
    pub fn take_pending(&self) -> Vec<PendingFree> {
        // **D81.** Draining the log changes it and persists nothing; no tail record describes
        // that, so the next claim must rewrite the image rather than append behind a file that
        // still lists these entries. See [`PersistState::durable_pending_version`].
        self.pending_version.fetch_add(1, Ordering::SeqCst);
        std::mem::take(&mut self.state.lock().unwrap().pending)
    }

    /// Put entries that are still pinned back on the pending-free log, and checkpoint.
    ///
    /// This is the closing half of `reaper::drain_pending`'s read-modify-write: by the time it runs,
    /// the reclaimable pages have been released into their extents' recycled lists and the survivors
    /// are back on the log. That whole shape lives only in the free-space map, so it persists here
    /// for the same reason `free_arena` does — once per drain, which is once per reap.
    pub fn put_pending(&self, entries: Vec<PendingFree>) -> Result<(), FerroError> {
        self.state.lock().unwrap().pending.extend(entries);
        self.persist_if_configured()
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
        if let Some(e) = st.extents.get_mut(&arena) {
            if high > e.next_free {
                e.next_free = high;
            }
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
        let mut released = 0u32;
        for arena in rec.arenas.iter().copied() {
            // **D85.** `allocated_pages` is `(0..next_free)`, so an understated `next_free` makes
            // this loop park NONE of a live child's pages. Probe first.
            self.resolve_fill(arena);
            for page_id in self.allocated_pages(arena) {
                let birth = self.page_birth(page_id)?;
                // The reclamation rule as an index question rather than an array walk: is
                // there a live child forked in [birth, free_epoch)? Same predicate, asked of a
                // structure that can answer it without holding every child resident.
                if !self.catalog.live_child_in_epoch_range(
                    rec.branch_id.id,
                    birth,
                    free_epoch,
                )? {
                    self.release_page(page_id, arena);
                    released += 1;
                } else {
                    self.state.lock().unwrap().pending.push(PendingFree {
                        page_id,
                        arena_id: arena,
                        birth_epoch: birth,
                        free_epoch,
                        owner: rec.branch_id,
                    });
                }
            }
        }
        // The slow path changes the durable map every bit as much as the fast one: pages recycled
        // inside a still-live extent, and a pending-free log that nothing but this map records. Left
        // unpersisted, a crash after `mark_reaped` (which clears `rec.arenas`) loses both — the
        // pending entries are gone so `drain_pending` never revisits them, the extent's durable
        // `next_free` is above its recycled count so `extent_is_empty` refuses, and nothing points
        // at the arena any more. Once per branch reaped, not once per page.
        self.persist_if_configured()?;
        Ok(released)
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
            b.extend_from_slice(&p.page_id.to_be_bytes());
            b.extend_from_slice(&p.arena_id.0.to_be_bytes());
            b.extend_from_slice(&p.birth_epoch.0.to_be_bytes());
            b.extend_from_slice(&p.free_epoch.0.to_be_bytes());
            b.extend_from_slice(&p.owner.id.to_be_bytes());
            b.extend_from_slice(&p.owner.generation.to_be_bytes());
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
            pending.push(PendingFree {
                page_id: c.u32()?,
                arena_id: ArenaId(c.u32()?),
                birth_epoch: Epoch(c.u64()?),
                free_epoch: Epoch(c.u64()?),
                owner: BranchId::new(c.u64()?, c.u32()?),
            });
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
                pending,
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
    /// Kinds this build understands. An allowlist for the same reason `READABLE_STATE_VERSIONS`
    /// is one.
    const KNOWN_TAIL_KINDS: &'static [u8] = &[Self::TAIL_ARENA_CLAIMED, Self::TAIL_EXTENT_FREED];

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

    fn persist_if_configured(&self) -> Result<(), FerroError> {
        let mut g = self.persist.lock().unwrap();
        self.persist_full_locked(&mut g)
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
        let written = self.checkpoint_with(&OsFileOps, &p)?;
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
    fn persist_delta_locked(
        &self,
        g: &mut PersistState,
        kind: u8,
        payload: &[u8],
    ) -> Result<(), FerroError> {
        let path = self.checkpoint_path.lock().unwrap().clone();
        let Some(p) = path else { return Ok(()) };
        let rec = Self::encode_tail_record(kind, payload);
        if g.image_bytes == 0
            || g.image_epoch != crate::cluster::epoch()
            || g.durable_pending_version != self.pending_version.load(Ordering::SeqCst)
            || g.tail_bytes + rec.len() as u64 > Self::compact_threshold(g.image_bytes)
        {
            // The rewrite folds in the mutation this record described, because `state_bytes`
            // serialises live memory and the caller has already applied it. So the record is
            // simply not needed, rather than needed and skipped.
            return self.persist_full_locked(g);
        }
        append_durably(&OsFileOps, &p, &rec).map_err(|e| FerroError::Io(e.to_string()))?;
        g.tail_bytes += rec.len() as u64;
        g.appends += 1;
        Ok(())
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
            self.apply_tail_record(kind, &rest[5..total - 4])?;
            at += total;
        }
        Ok(at as u64)
    }

    fn apply_tail_record(&self, kind: u8, payload: &[u8]) -> Result<(), FerroError> {
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
                    st.pending.retain(|p| p.arena_id != arena);
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
            return Ok(CowPage { page_id, previous_page_id: page_id, copied: false, handle });
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
        // the GC's business.** The `free_page` call directly below runs only when the branch owns
        // the source extent; in that case the base becomes a freeable page and a delta against it
        // would be a second, invisible reason to keep it alive — a reference count, in a file
        // whose header says in bold that there are none. When the branch does NOT own it, the base
        // is an ancestor's page that this branch inherited, and the epoch interval rule in
        // `branch::record::reclaimable` already pins it: the branch holding the delta forked after
        // the base was born, so `free_page` parks the page instead of releasing it. The existing
        // rule covers this case with no new liveness source, which is the only reason it is safe.
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
        if owner_of_source == Some(branch) {
            self.free_page(page_id, epoch)?;
        }

        Ok(CowPage { page_id: new_id, previous_page_id: page_id, copied: true, handle: new_handle })
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
        // stopped existing: `TableBranchCatalog::upsert` is delete-then-insert with no latch held
        // across the two, and `write_record` routes the RECORD key through it, so a concurrent
        // `set_root` or `renew_lease` on the owner makes `get_raw` miss for a moment on a
        // perfectly healthy branch. Resolving that to "not pinned" ran `release_page` on a page a
        // live child may still be reading: silent data loss, not a leak.
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
            self.pending_version.fetch_add(1, Ordering::SeqCst);
            self.state.lock().unwrap().pending.push(PendingFree {
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
        // **D81 — THIS is the wall, and it is the only site that changes shape.** D79 measured the
        // whole 48·L-byte image being re-serialised and re-fsynced here, once per new branch, for
        // `sum(48·i) = 24·N²` bytes over a run. It now appends 45 bytes and fsyncs once, and the
        // image is rewritten only when the tail has grown past half of it.
        //
        // The other three `persist_if_configured` sites keep the full rewrite deliberately. They
        // fire once per REAP, not once per branch created, and each one changes list-shaped state
        // — the pending-free log, per-extent recycled lists — that a per-extent record does not
        // describe. They double as compaction points, so leaving them whole costs a bounded tail
        // rather than a missing guarantee. ⚠ The condition under which that stops being the right
        // call, stated rather than discovered: a workload that reaps as often as it forks pays a
        // full rewrite per reap and only half of this row's benefit.
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
        self.persist_delta_locked(&mut persist, Self::TAIL_ARENA_CLAIMED, &payload)?;
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
        st.pending.retain(|p| p.arena_id != arena);
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
            self.persist_delta_locked(&mut persist, Self::TAIL_EXTENT_FREED, &payload)?;
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
    /// `alloc_for` advances `ext.next_free` (`arena.rs`) and does NOT persist. The four
    /// `persist_if_configured` sites are all off the page path, so a crash can leave the image's
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
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&ArenaPageStore::encode_tail_record(0x7f, b"from the future"));

        let restored = h.fresh_store();
        let err = restored.load_file(&bytes).expect_err("an unknown kind must be refused");
        assert!(format!("{err}").contains("newer build"), "unhelpful refusal: {err}");
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
