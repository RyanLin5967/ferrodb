//! Reachability GC at page granularity: the collector for the leak class the interval rule
//! cannot see.
//!
//! # The premise this file was built to check, and what the check found
//!
//! D96 was opened as "garbage collection at chunk granularity", on the reasoning that
//! content-addressed stores (Dolt, ForkBase, git) must collect chunks that no version
//! references, and that ferrodb reclaims only at arena/extent granularity. Two halves of that
//! premise were measured before any of this was written, and they did not survive in the form
//! they were posed.
//!
//! **Half one — "extent granularity is the space wall" is a FIXED BUG, not a live one.** The
//! 262x space amplification is real and is banked in `bench/d31_before.txt`: 4000 branches
//! writing one page each produced 4193.3 MB of data file holding 16.4 MB of data, because
//! extents were one fixed size and a branch's first 4 KiB page reserved `ARENA_EXTENT_PAGES`
//! = 256 of them. That is the **before** measurement. D31 replaced the fixed size with
//! geometric growth from `ARENA_FIRST_EXTENT_PAGES` = 1 ([`crate::branch::types::next_extent_pages`]),
//! and `bench/d31_after.txt` reads 4097 bytes/branch against a 4096-byte page — *"VERDICT —
//! AMPLIFICATION GONE"*. So reclamation granularity is **not** the space wall today. A brief
//! that cites it as live is citing the pre-D31 allocator.
//!
//! **Half two — "unreachable chunks" presupposes content addressing, which this store
//! deliberately does not have.** [`crate::cow`]'s module brief lists it as a non-goal with a
//! reason: content addressing "forces a global liveness question; you cannot free a chunk
//! without a global statement about who else references it. This is why Dolt needs copying
//! mark-and-sweep GC." Every page here belongs to exactly one arena owned by exactly one
//! branch, stamped in its own header. There is no chunk shared by hash between versions, so
//! there is no chunk that "no version references" in the Dolt sense.
//!
//! **What IS live, and is why this file exists anyway.** Reclamation is answered entirely by
//! the epoch interval rule ([`crate::branch::record::reclaimable`]) applied to pages the owning
//! branch *told* the store about via `free_page`, plus wholesale extent frees at reap. Neither
//! mechanism asks whether a page is reachable from a root. So a page that is allocated in a
//! **live** branch's arena and linked into no tree is invisible to both: the interval rule never
//! sees it because it was never freed, and the reaper's fast path only frees it when the entire
//! branch dies. A long-lived trunk that leaks one page per failed split leaks unboundedly, and
//! nothing in the engine can currently say so — a `grep` over `src/` for reachability marking
//! finds nothing but `unreachable!` macros.
//!
//! That is the class this collects: **allocated, unreachable, never freed, owner still alive.**
//! It is a leak auditor and collector, not a replacement reclaimer. The interval rule remains
//! the authority for pages that were freed properly.
//!
//! # The standard answers, named
//!
//! None of the three is new, and this file invents none of them:
//!
//! * **Reference counting.** Rejected upstream of this file, and for a stated reason worth not
//!   relitigating: a parent with 5000 children would carry refcount 5001 on its root, putting
//!   the mutation hot spot on the most-shared page in the store — btrfs's backref explosion.
//! * **Stop-the-world mark-and-sweep from the branch roots.** The textbook answer and what Dolt
//!   uses. Disqualified in the stop-the-world form: W4 in this project's ledger is a 39.3 s hold
//!   of the per-statement lock across O(arenas) work, and D83/D88 exist to undo exactly that
//!   shape. A mark that holds any lock across O(chunks) reintroduces it.
//! * **Epoch / generational reclamation.** Already the resident mechanism — Fraser 2004 and
//!   ZFS's birth-time intervals are both already cited in this codebase, and the interval rule
//!   *is* a birth/death interval test.
//!
//! This file is the second one built in the **incremental, non-blocking** form, using the third
//! one's epoch stamp as its concurrency barrier. That combination is also not new: "allocate
//! black" is Dijkstra-style concurrent collection, and using an allocation timestamp instead of
//! a write barrier is what generational collectors do when they can order allocations globally.
//!
//! # Why there is no write barrier
//!
//! A concurrent mark needs to not miss a page that becomes reachable *during* the mark. The
//! textbook mechanisms are write barriers — Dijkstra shade-on-store, or Yuasa snapshot-at-the-
//! beginning. Both require instrumenting the allocator and the pointer-store path.
//!
//! This collector uses neither, because the store already stamps every page with the epoch it
//! was born at, and epochs are globally ordered by the catalog. So:
//!
//! > **The allocate-black rule.** A cycle records `start_epoch` before it reads a single root.
//! > Any page whose `birth_epoch >= start_epoch` is **categorically uncollectable by that
//! > cycle**, marked or not.
//!
//! That is sound against every way a page can become reachable mid-mark, and the argument is
//! short enough to check:
//!
//! * A page written during the mark is a *new* page (shadow paging never mutates a shared page
//!   in place), so it is born at `>= start_epoch` and is black.
//! * A branch forked during the mark gets its parent's root byte-identical — that is the entire
//!   fork operation. So it reaches no page its parent did not already reach at fork time, and
//!   its parent was either in the root snapshot (marked) or itself born `>= start_epoch`.
//! * A root pointer swapped during the mark publishes a tree whose *changed* pages are all born
//!   `>= start_epoch` (black) and whose *unchanged* subtrees are shared page-identical with the
//!   old root, which is in the snapshot and therefore marked.
//!
//! The cost of having no barrier is **floating garbage**: anything that dies during a cycle is
//! collected by the next one, not this one. That is the standard trade and it is the safe
//! direction — this collector is conservative by construction, never precise.
//!
//! # Why the pause cannot grow with the heap
//!
//! Every unit of work this file performs is bounded before it starts:
//!
//! * The mark pops at most `budget` pages per slice and holds **no** store lock between slices.
//!   Each `read_page` takes and releases its own latch.
//! * The sweep examines at most `budget` pages per slice, resuming part-way into an extent
//!   across slices. It reads one extent's page list per slice — `allocated_pages` is an O(256)
//!   hold of the arena state mutex, bounded by
//!   [`crate::branch::types::ARENA_EXTENT_PAGES`] regardless of how many pages the store holds
//!   in total — and then touches only `budget` of them.
//!
//!   The resume cursor is not a refinement; it is what makes `budget` mean anything on this
//!   side. Sweeping a whole extent per slice pinned the pause to the extent size however small
//!   a budget the caller asked for, and the first run of `d96_pause_curve` measured that at
//!   **72 ms** for one 256-page extent at 10k chunks: bounded in pages, long in wall time, and
//!   not tunable. Each swept page costs a `read_page` and, when collected, a `release_page`
//!   that takes the page-table write lock, resets the frame and updates the ARC — so the
//!   per-page constant is large and the only lever on the pause is how many pages a slice may
//!   touch.
//!
//! The trap this shape exists to avoid is [`crate::branch::arena::ArenaPageStore::live_arenas`],
//! which collects and sorts every live extent under the state mutex. It is O(arenas), and arena
//! count grows with heap size, so calling it *per slice* would make pause grow with total
//! chunks — W4 again, wearing a different hat. It is called **once per cycle**, at construction,
//! and that one call is reported separately in [`GcStats::snapshot_nanos`] rather than folded
//! into the pause number it would flatter.
//!
//! **The consequence of snapshotting arenas once, stated plainly.** An extent created *after*
//! the cycle opened is not swept by that cycle. This is a second source of deferral alongside
//! floating garbage, and it is in the safe direction for the same reason: every page in a new
//! extent was necessarily born after the snapshot, so the allocate-black rule would have
//! spared it anyway. It matters for testing rather than for safety — a test that plants garbage
//! *after* opening a cycle is not testing the epoch barrier, because extents grow geometrically
//! from one page and a handful of allocations rolls over into an extent the snapshot never saw.
//! The page would survive because nothing looked at it. `a_page_born_during_the_cycle_is_never_collected`
//! plants into a snapshotted arena for exactly this reason, and the first version of it passed
//! vacuously with `examined: 1, spared: 0` until that was fixed.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use crate::branch::arena::ArenaPageStore;
use crate::branch::record::BranchRecord;
use crate::branch::types::{
    ArenaId, BranchId, BranchState, Epoch, PageId, ARENA_EXTENT_PAGES,
};
use crate::branch::BranchCatalog;
use crate::cow::node::Node;
use crate::cow::page_header::{PageHeader, PageType};
use crate::cow::PageStore;
use crate::error::FerroError;

/// Depth guard for the mark walk, mirroring `btree::MAX_DESCENT`.
///
/// A cycle in the page graph would otherwise make the mark non-terminating. The marked set
/// already prevents revisiting, so this is the second line and fires only on a tree deeper than
/// any legal one — corruption, not depth.
const MAX_MARK_DEPTH: usize = 64;

/// A root the mark starts from: one branch's B+tree root as of the snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootRef {
    pub branch: BranchId,
    pub root: PageId,
}

/// Branch states whose root still pins pages, and which therefore must be marked from.
///
/// Deliberately everything except [`BranchState::Reaped`]. `Quarantined` is documented as
/// "unmerged but still queryable", and `Reaping` is mid-reap with children that are still the
/// authority for its pages. Treating any of them as dead would collect a page somebody can
/// still read, and the safe direction here is to over-mark.
fn state_pins_pages(state: BranchState) -> bool {
    !matches!(state, BranchState::Reaped)
}

/// Page types that can be reached from a branch root, and are therefore the only ones a
/// reachability argument says anything about.
///
/// This is the **single** authority for what may be collected, not a second guard behind an
/// arena-ownership check — a redundant downstream check would mask every mutant of the one in
/// front of it. The header byte is the right place to ask, and the type byte exists for exactly
/// this: `PageType` is documented as stored as a single byte "so a scavenger can classify any
/// page without a catalog".
///
/// `Meta`, `BranchCatalog`, `FreeLog` and `Provenance` are store infrastructure. They are
/// structurally unreachable from any branch root, so a reachability sweep would collect every
/// one of them and destroy the database. `Free` is allocated-but-not-yet-typed — a page in
/// flight, whose owner has not linked it yet.
fn is_collectable_type(ty: PageType) -> bool {
    matches!(
        ty,
        PageType::BTreeInternal | PageType::BTreeLeaf | PageType::Heap | PageType::Overflow
    )
}

/// What a collector must be able to ask of a heap: enumerate it, and give one page back.
///
/// A local trait rather than methods on [`PageStore`] because the trait is not the right place
/// for them — `PageStore` is the allocation and read interface that both stores implement, and
/// neither `CowStore` nor any future store owes a collector an enumeration. Implemented here
/// for [`ArenaPageStore`], which is the store that ships.
pub trait ChunkHeap: Send + Sync {
    /// Every live extent and its owner. **O(arenas), and expected to lock**: call once per
    /// cycle, never per slice. See the module brief.
    fn arenas(&self) -> Vec<(ArenaId, BranchId)>;

    /// Pages handed out inside `arena` and not since released. Bounded by the extent size.
    fn pages_in(&self, arena: ArenaId) -> Vec<PageId>;

    /// Return one page to the free space map. Must be idempotent: see
    /// [`GcCycle::step`] on the overlap with the pending-free log.
    fn release(&self, page: PageId, arena: ArenaId);
}

impl ChunkHeap for ArenaPageStore {
    fn arenas(&self) -> Vec<(ArenaId, BranchId)> {
        self.live_arenas()
    }

    fn pages_in(&self, arena: ArenaId) -> Vec<PageId> {
        self.allocated_pages(arena)
    }

    fn release(&self, page: PageId, arena: ArenaId) {
        self.release_page(page, arena)
    }
}

/// Snapshot every root that pins pages, in one streaming pass over the catalog.
///
/// Streams via [`BranchCatalog::scan`] rather than materialising the catalog: `scan` is
/// documented as genuinely O(N) with an iterator item type precisely so a caller need not
/// buffer a second copy of the database. The `Vec` this returns is O(branches), not O(pages) —
/// the distinction the pause measurement holds fixed.
pub fn snapshot_roots(catalog: &dyn BranchCatalog) -> Result<Vec<RootRef>, FerroError> {
    let mut roots = Vec::new();
    for rec in catalog.scan()? {
        let rec: BranchRecord = rec?;
        if state_pins_pages(rec.state) {
            roots.push(RootRef { branch: rec.branch_id, root: rec.root_page_id });
        }
    }
    Ok(roots)
}

/// What one cycle did. Every field is a count or a duration the caller can check a claim against.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcStats {
    /// Distinct pages the mark reached.
    pub marked: u64,
    /// Pages examined by the sweep.
    pub examined: u64,
    /// Pages actually released.
    pub reclaimed: u64,
    /// Pages the sweep declined to collect **only** because of the allocate-black rule.
    ///
    /// This is the floating-garbage meter, and it is the number that says whether the
    /// concurrency barrier did any work in a given run. Zero here in a test that forks during
    /// the mark means the test did not construct the race it claims to.
    pub spared_born_during_cycle: u64,
    /// Slices executed.
    pub slices: u64,
    /// Longest single slice, nanoseconds. **This is the pause number.**
    pub max_slice_nanos: u128,
    /// Most pages touched in any one slice. Must never exceed the budget.
    pub max_slice_pages: u64,
    /// Nanoseconds spent in the once-per-cycle root and arena snapshot.
    ///
    /// Reported apart from `max_slice_nanos` because it is the one O(branches + arenas) step,
    /// and folding it into the pause figure would flatter it at small heaps and hide it at
    /// large ones.
    pub snapshot_nanos: u128,
}

/// Which half of the cycle is running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Marking,
    Sweeping,
    Done,
}

/// One incremental, non-blocking collection cycle.
///
/// Construct it, then call [`GcCycle::step`] until it returns `true`. The cycle holds no lock
/// between steps, so a caller may interleave it with ordinary traffic, abandon it, or run it
/// from a background tick.
pub struct GcCycle {
    /// The allocate-black line. Captured **before** the root snapshot, so that a page born
    /// concurrently with the snapshot itself falls on the protected side.
    start_epoch: Epoch,
    stack: Vec<(PageId, usize)>,
    marked: HashSet<PageId>,
    arenas: Vec<(ArenaId, BranchId)>,
    sweep_idx: usize,
    /// How far into `arenas[sweep_idx]`'s page list the sweep has got.
    ///
    /// **This is what makes `budget` mean something for the sweep.** Without it a slice was one
    /// whole extent, so the pause was pinned to `ARENA_EXTENT_PAGES` however small a budget the
    /// caller asked for — measured at **72 ms** for a 256-page extent at 10k chunks, because
    /// each swept page costs a `read_page` and a `release_page` (page-table write lock, frame
    /// reset, ARC update). Bounded in pages but a long pause in wall time, and not tunable.
    sweep_page_idx: usize,
    phase: Phase,
    stats: GcStats,
}

impl GcCycle {
    /// Open a cycle over `heap`, marking from every root `catalog` currently pins.
    ///
    /// The epoch is read first and the snapshots second, which is the ordering the safety
    /// argument depends on: a page allocated between the two reads is born at `>= start_epoch`
    /// and is therefore black, whereas the reverse order would leave a window where a page is
    /// both absent from the snapshot and below the line.
    pub fn open(
        catalog: &dyn BranchCatalog,
        heap: &dyn ChunkHeap,
    ) -> Result<GcCycle, FerroError> {
        let t0 = Instant::now();
        let start_epoch = catalog.current_epoch();
        let roots = snapshot_roots(catalog)?;
        let arenas = heap.arenas();
        let snapshot_nanos = t0.elapsed().as_nanos();

        let stack = roots.iter().map(|r| (r.root, 0usize)).collect();
        Ok(GcCycle {
            start_epoch,
            stack,
            marked: HashSet::new(),
            arenas,
            sweep_idx: 0,
            sweep_page_idx: 0,
            phase: Phase::Marking,
            stats: GcStats { snapshot_nanos, ..GcStats::default() },
        })
    }

    /// Open a cycle from an explicit root set, for a caller that already has one.
    pub fn open_with_roots(
        start_epoch: Epoch,
        roots: &[RootRef],
        heap: &dyn ChunkHeap,
    ) -> GcCycle {
        let t0 = Instant::now();
        let arenas = heap.arenas();
        let snapshot_nanos = t0.elapsed().as_nanos();
        GcCycle {
            start_epoch,
            stack: roots.iter().map(|r| (r.root, 0usize)).collect(),
            marked: HashSet::new(),
            arenas,
            sweep_idx: 0,
            sweep_page_idx: 0,
            phase: Phase::Marking,
            stats: GcStats { snapshot_nanos, ..GcStats::default() },
        }
    }

    pub fn stats(&self) -> GcStats {
        self.stats
    }

    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// The set of pages the mark reached. Exposed so a test can assert reachability directly
    /// rather than inferring it from what the sweep did or did not free.
    pub fn marked(&self) -> &HashSet<PageId> {
        &self.marked
    }

    /// Run one bounded slice. Returns `true` when the cycle is complete.
    ///
    /// `budget` caps pages touched in this slice. No lock is held across the call; the mark
    /// takes a per-page latch through `read_page` and drops it before the next page, and the
    /// sweep takes the arena state mutex once per arena for an O(extent) read.
    ///
    /// **On the overlap with the pending-free log.** A page that was properly freed and is
    /// parked awaiting the interval rule is still "allocated" from the extent's point of view,
    /// so the sweep can see it. If it is still visible to a child, that child's root is in the
    /// snapshot and the mark reached it, so it is marked and skipped. If it is visible to
    /// nobody it is genuinely dead, and either this sweep or `drain_pending` returns it —
    /// whichever arrives first. That race is benign because
    /// [`ArenaPageStore::release_page`] is idempotent: it refuses to push a page onto a
    /// recycled list that already contains it, and only decrements `live_pages` on a state
    /// change. `a_concurrent_drain_does_not_double_count` pins that.
    pub fn step(
        &mut self,
        store: &dyn PageStore,
        heap: &dyn ChunkHeap,
        budget: u64,
    ) -> Result<bool, FerroError> {
        if self.phase == Phase::Done {
            return Ok(true);
        }
        let t0 = Instant::now();
        let mut touched = 0u64;

        match self.phase {
            Phase::Marking => {
                while touched < budget {
                    let Some((pid, depth)) = self.stack.pop() else {
                        self.phase = Phase::Sweeping;
                        break;
                    };
                    touched += 1;
                    if depth > MAX_MARK_DEPTH {
                        return Err(FerroError::Cow(
                            "gc mark exceeded the depth guard".into(),
                        ));
                    }
                    if !self.marked.insert(pid) {
                        continue;
                    }
                    // A page that cannot be read right now is not evidence of anything. It may
                    // have been freed by its owner between the snapshot and this read. Skipping
                    // it leaves it marked, which is the conservative direction: a marked page is
                    // never collected.
                    let Ok(handle) = store.read_page(pid) else { continue };
                    let frame = handle.read();
                    let Ok(header) = PageHeader::read_from(&frame.data) else { continue };
                    if header.page_type == PageType::BTreeInternal {
                        for child in Node::new(&frame.data).all_children()? {
                            self.stack.push((child, depth + 1));
                        }
                    }
                }
            }
            Phase::Sweeping => {
                // Exactly one arena per slice. An extent is bounded by ARENA_EXTENT_PAGES, so
                // this is the step that keeps the pause independent of total heap size.
                if self.sweep_idx >= self.arenas.len() {
                    self.phase = Phase::Done;
                } else {
                    let (arena, _owner) = self.arenas[self.sweep_idx];
                    // One bounded read of this extent's page list. `allocated_pages` is O(extent)
                    // under the arena state mutex and the extent is capped, so this hold does not
                    // grow with the heap however many arenas exist.
                    let pages = heap.pages_in(arena);
                    let start = self.sweep_page_idx.min(pages.len());
                    let end = pages.len().min(start + budget as usize);
                    // Pages this slice examined and did NOT collect. The cursor advances by this,
                    // not by the number examined — see the note below the loop.
                    let mut survivors = 0usize;
                    for pid in pages[start..end].iter().copied() {
                        touched += 1;
                        self.stats.examined += 1;
                        if self.marked.contains(&pid) {
                            survivors += 1;
                            continue;
                        }
                        let Ok(handle) = store.read_page(pid) else {
                            survivors += 1;
                            continue;
                        };
                        let header = {
                            let frame = handle.read();
                            match PageHeader::read_from(&frame.data) {
                                Ok(h) => h,
                                Err(_) => {
                                    survivors += 1;
                                    continue;
                                }
                            }
                        };
                        drop(handle);
                        if !is_collectable_type(header.page_type) {
                            survivors += 1;
                            continue;
                        }
                        // The allocate-black rule. Counted separately so a test can prove the
                        // barrier actually engaged rather than assuming it did.
                        if header.birth_epoch >= self.start_epoch {
                            self.stats.spared_born_during_cycle += 1;
                            survivors += 1;
                            continue;
                        }
                        heap.release(pid, arena);
                        self.stats.reclaimed += 1;
                    }
                    // **The cursor advances by SURVIVORS, not by pages examined.**
                    //
                    // `allocated_pages` filters out released pages, so collecting one does not
                    // just mutate the list this slice is walking — it SHORTENS it, and every
                    // later element shifts down by one. Advancing the cursor by the number
                    // examined therefore steps over exactly as many unexamined pages as this
                    // slice collected. With a budget of 64 against extents of 256 that skips
                    // every other batch, and `d96_pause_curve` measured it: **10,000 planted,
                    // 5,072 collected, 5,098 examined.** Half the garbage, and a number plausible
                    // enough to have been banked if the harness had asserted "collected > 0"
                    // instead of the exact count.
                    //
                    // The survivors of `pages[start..end]` are still in the list, occupying
                    // `start .. start + survivors`, and everything not yet examined follows them.
                    // So that is where the next slice resumes.
                    //
                    // The unit tests all missed this because each planted fewer pages than one
                    // budget, so a single slice drained each extent and the cursor never moved.
                    // `garbage_larger_than_one_budget_is_fully_collected` is the regression.
                    if end >= pages.len() {
                        self.sweep_idx += 1;
                        self.sweep_page_idx = 0;
                    } else {
                        self.sweep_page_idx = start + survivors;
                    }
                    if self.sweep_idx >= self.arenas.len() {
                        self.phase = Phase::Done;
                    }
                }
            }
            Phase::Done => {}
        }

        self.stats.marked = self.marked.len() as u64;
        self.stats.slices += 1;
        let elapsed = t0.elapsed().as_nanos();
        if elapsed > self.stats.max_slice_nanos {
            self.stats.max_slice_nanos = elapsed;
        }
        if touched > self.stats.max_slice_pages {
            self.stats.max_slice_pages = touched;
        }
        Ok(self.phase == Phase::Done)
    }

    /// Drive the cycle to completion at a fixed slice budget.
    ///
    /// `slice_cap` is a refusal, not a timeout: a cycle that has not finished within it has hit
    /// a bug (a non-terminating mark, an arena list that grows faster than the sweep consumes
    /// it), and returning a partial result labelled complete is how that bug would reach a
    /// bench file as a small, calm number.
    pub fn run_to_completion(
        &mut self,
        store: &dyn PageStore,
        heap: &dyn ChunkHeap,
        budget: u64,
        slice_cap: u64,
    ) -> Result<GcStats, FerroError> {
        for _ in 0..slice_cap {
            if self.step(store, heap, budget)? {
                return Ok(self.stats);
            }
        }
        Err(FerroError::Cow(format!(
            "gc cycle did not finish within {} slices (marked {}, swept {} of {} arenas)",
            slice_cap,
            self.marked.len(),
            self.sweep_idx,
            self.arenas.len()
        )))
    }
}

/// Collect once, from the catalog's current roots, and return what it did.
pub fn collect_once(
    catalog: &dyn BranchCatalog,
    store: &dyn PageStore,
    heap: &dyn ChunkHeap,
    budget: u64,
) -> Result<GcStats, FerroError> {
    let mut cycle = GcCycle::open(catalog, heap)?;
    // Slice cap sized off the work actually queued: every page is popped at most once and every
    // extent is swept in `budget`-sized bites, so anything beyond this is a loop that is not
    // making progress.
    //
    // Read off the cycle's OWN arena snapshot rather than calling `heap.arenas()` a second time.
    // That second call was a duplicate O(arenas) traversal under the state mutex — outside any
    // slice, so it never touched the pause figure, but it doubled the per-cycle cost that
    // `snapshot_nanos` reports and would have made that column understate reality by half.
    let arena_count = cycle.arenas.len() as u64;
    let cap = 16
        + arena_count * (1 + (ARENA_EXTENT_PAGES as u64 / budget.max(1)))
        + 1_000_000 / budget.max(1);
    cycle.run_to_completion(store, heap, budget, cap)
}

/// Convenience for the shipped store, which is both a [`PageStore`] and a [`ChunkHeap`].
pub fn collect_once_arena(
    catalog: &dyn BranchCatalog,
    store: &Arc<ArenaPageStore>,
    budget: u64,
) -> Result<GcStats, FerroError> {
    collect_once(catalog, store.as_ref(), store.as_ref(), budget)
}

#[cfg(test)]
mod tests {
    //! Every test here is written so that a collector which did nothing would FAIL it.
    //!
    //! A sweep that collects nothing is not a clean result, so the two directions are pinned
    //! separately: [`plants_unreachable_pages_and_collects_every_one`] proves the detector
    //! fires, and [`never_collects_a_page_reachable_from_a_root`] proves it does not fire
    //! spuriously. The concurrency cases each assert on `spared_born_during_cycle` as well as
    //! on survival, because a test that forks during the mark but never actually races would
    //! pass vacuously.

    use super::*;

    use crate::branch::catalog::LogBranchCatalog;
    use crate::branch::types::{LeaseDeadline, ARENA_EXTENT_PAGES};
    use crate::buffer::buffer_pool::BufferPoolManager;
    use crate::cow::btree::CowTree;
    use crate::storage::disk_manager::DiskManager;

    struct Env {
        catalog: Arc<LogBranchCatalog>,
        store: Arc<ArenaPageStore>,
        tree: CowTree,
        path: std::path::PathBuf,
    }

    impl Drop for Env {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    impl Env {
        fn new(tag: &str) -> Env {
            let path =
                std::env::temp_dir().join(format!("d96-gc-{}-{}.db", std::process::id(), tag));
            let _ = std::fs::remove_file(&path);
            let file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
            let catalog = Arc::new(LogBranchCatalog::in_memory(0));
            let base = pool.disk_manager.high_water().unwrap();
            let store = Arc::new(
                ArenaPageStore::new(pool, Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base)
                    .unwrap(),
            );
            let tree = CowTree::new(Arc::clone(&store) as Arc<dyn PageStore>);
            Env { catalog, store, tree, path }
        }

        fn cat(&self) -> &dyn BranchCatalog {
            self.catalog.as_ref()
        }

        /// Build a real tree of `n` keys on `branch` and publish its root.
        fn grow_tree(&self, branch: BranchId, n: u32) -> PageId {
            let e = self.catalog.next_epoch();
            let mut root = self.tree.create(branch, e).unwrap();
            for i in 0..n {
                let e = self.catalog.next_epoch();
                let k = format!("k{:06}", i);
                let v = format!("v{:06}", i);
                root = self.tree.insert(root, branch, e, k.as_bytes(), v.as_bytes()).unwrap();
            }
            self.catalog.set_root(branch, root).unwrap();
            root
        }

        /// Allocate `n` pages in `branch`'s arena and link them into nothing. This is the
        /// garbage the collector exists to find.
        fn plant_garbage(&self, branch: BranchId, n: u32, ty: PageType) -> Vec<PageId> {
            (0..n)
                .map(|_| {
                    let e = self.catalog.next_epoch();
                    self.store.alloc_for(branch, ty, e).unwrap()
                })
                .collect()
        }

        fn collect(&self, budget: u64) -> GcStats {
            collect_once_arena(self.cat(), &self.store, budget).unwrap()
        }
    }

    // ---- the detector fires ---------------------------------------------------------------

    /// **The forced-fire case.** Pages allocated in a live branch's arena and linked into no
    /// tree are exactly the class the interval rule cannot see: never freed, so never tested;
    /// owner still alive, so never swept wholesale. Every one must come back.
    #[test]
    fn plants_unreachable_pages_and_collects_every_one() {
        let env = Env::new("plant");
        env.grow_tree(BranchId::TRUNK, 200);

        let planted = env.plant_garbage(BranchId::TRUNK, 9, PageType::BTreeLeaf);
        // Push the allocate-black line above every planted page, so this test is about
        // reachability and not about the epoch barrier.
        let _ = env.catalog.next_epoch();

        let live_before = env.store.live_page_count().unwrap();
        let stats = env.collect(64);

        assert_eq!(
            stats.reclaimed,
            planted.len() as u64,
            "planted {} unreachable pages, collected {} — a sweep that collects nothing is not \
             a clean result. marked={} examined={} spared={}",
            planted.len(),
            stats.reclaimed,
            stats.marked,
            stats.examined,
            stats.spared_born_during_cycle
        );
        assert_eq!(
            stats.spared_born_during_cycle, 0,
            "nothing was allocated during this cycle, so the epoch barrier must not have engaged"
        );
        assert_eq!(
            live_before - env.store.live_page_count().unwrap(),
            9,
            "live page count must drop by exactly the number reclaimed"
        );
    }

    /// The same, planted in a **child** branch's own arena, so the garbage is not in whichever
    /// arena the mark's roots happen to live in.
    #[test]
    fn collects_garbage_in_a_child_branch_arena() {
        let env = Env::new("child");
        env.grow_tree(BranchId::TRUNK, 100);
        let child = env.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
        let child_id = child.branch_id;
        let child_root = env.grow_tree(child_id, 60);
        env.plant_garbage(child_id, 5, PageType::BTreeLeaf);
        let _ = env.catalog.next_epoch();

        let stats = env.collect(64);
        assert_eq!(stats.reclaimed, 5, "child-arena garbage not collected: {:?}", stats);

        assert_eq!(
            env.tree.get(child_root, b"k000042").unwrap().as_deref(),
            Some(&b"v000042"[..]),
            "child tree damaged by the sweep"
        );
    }

    /// **Regression for the sweep cursor.** Every other test above plants fewer pages than one
    /// slice budget, so a single slice drained each extent and the resume cursor never moved.
    /// That is exactly the blind spot that let a 50% collector look green: `d96_pause_curve`
    /// planted 10,000 and collected 5,072, because releasing a page removes it from
    /// `allocated_pages` and the cursor was advancing past pages that had shifted down.
    ///
    /// So this one plants many budgets' worth and asserts the exact count. A budget well below
    /// `ARENA_EXTENT_PAGES` is the whole point — raise it above 256 and this passes against the
    /// broken code.
    #[test]
    fn garbage_larger_than_one_budget_is_fully_collected() {
        let env = Env::new("cursor");
        env.grow_tree(BranchId::TRUNK, 100);
        const N: u32 = 3_000;
        env.plant_garbage(BranchId::TRUNK, N, PageType::BTreeLeaf);
        let _ = env.catalog.next_epoch();

        let budget = 8u64;
        assert!(
            budget < ARENA_EXTENT_PAGES as u64,
            "the cursor is only exercised when a budget is smaller than an extent"
        );
        let live_before = env.store.live_page_count().unwrap();
        let stats = env.collect(budget);

        assert_eq!(
            stats.reclaimed, N as u64,
            "planted {} across many extents at budget {}, collected {} — the sweep cursor is \
             skipping pages that shifted down when earlier ones were released: {:?}",
            N, budget, stats.reclaimed, stats
        );
        assert_eq!(live_before - env.store.live_page_count().unwrap(), N);
    }

    // ---- the detector does not fire spuriously ----------------------------------------------

    /// Nothing reachable may ever be collected, and the tree must still read correctly
    /// afterwards. Reading it back matters: a collector that freed a page the tree still points
    /// at would leave `reclaimed` looking healthy and the database broken.
    #[test]
    fn never_collects_a_page_reachable_from_a_root() {
        let env = Env::new("reach");
        let root = env.grow_tree(BranchId::TRUNK, 400);
        let live_before = env.store.live_page_count().unwrap();

        let stats = env.collect(64);

        assert_eq!(
            stats.reclaimed, 0,
            "collected {} reachable pages — this is the unsafe direction",
            stats.reclaimed
        );
        assert!(stats.marked > 1, "a 400-key tree must mark more than one page: {:?}", stats);
        assert_eq!(env.store.live_page_count().unwrap(), live_before);
        for i in 0..400u32 {
            let k = format!("k{:06}", i);
            let v = format!("v{:06}", i);
            assert_eq!(
                env.tree.get(root, k.as_bytes()).unwrap().as_deref(),
                Some(v.as_bytes()),
                "key {} lost after a collection that claimed to free nothing",
                k
            );
        }
    }

    /// Store infrastructure is structurally unreachable from any branch root. A reachability
    /// sweep with no type filter would collect all of it and destroy the database, so the
    /// filter is load-bearing rather than defensive.
    #[test]
    fn infrastructure_pages_are_never_collected() {
        let env = Env::new("infra");
        env.grow_tree(BranchId::TRUNK, 50);

        let mut infra = Vec::new();
        for ty in
            [PageType::Meta, PageType::BranchCatalog, PageType::FreeLog, PageType::Provenance]
        {
            infra.extend(env.plant_garbage(BranchId::TRUNK, 2, ty));
        }
        // Ordinary garbage too, so this test cannot pass by collecting nothing at all.
        let collectable = env.plant_garbage(BranchId::TRUNK, 3, PageType::BTreeLeaf);
        let _ = env.catalog.next_epoch();

        let stats = env.collect(64);
        assert_eq!(
            stats.reclaimed,
            collectable.len() as u64,
            "exactly the tree-typed garbage should go, not the {} infrastructure pages: {:?}",
            infra.len(),
            stats
        );
    }

    // ---- the concurrency barrier ------------------------------------------------------------

    /// **The allocate-black rule, directly.** A page born after the cycle opened is
    /// uncollectable by that cycle even though no root reaches it, because the mark may already
    /// have walked past the place it would have been linked.
    #[test]
    fn a_page_born_during_the_cycle_is_never_collected() {
        let env = Env::new("black");
        env.grow_tree(BranchId::TRUNK, 100);

        // Stamped at an epoch ABOVE the one the cycle will open at, which is exactly how a page
        // allocated mid-cycle looks to the sweep — but planted BEFORE the cycle opens, so it
        // lands in an arena the cycle's snapshot contains and is therefore actually examined.
        //
        // Planting it after `open` instead would land it in a fresh extent the arena snapshot
        // never saw (extents grow geometrically from ONE page, so a handful of allocations rolls
        // over), and the test would pass because nothing ever looked at it rather than because
        // the barrier held. That is the vacuous-pass this construction exists to avoid, and the
        // first version of this test failed exactly that way: `examined: 1, spared: 0`.
        let here = env.catalog.current_epoch();
        let future = Epoch(here.0 + 50);
        let born_during: Vec<PageId> = (0..6)
            .map(|_| env.store.alloc_for(BranchId::TRUNK, PageType::BTreeLeaf, future).unwrap())
            .collect();

        let mut cycle = GcCycle::open(env.cat(), env.store.as_ref()).unwrap();
        // One slice of marking, so the cycle is genuinely in flight.
        cycle.step(env.store.as_ref(), env.store.as_ref(), 4).unwrap();

        let stats =
            cycle.run_to_completion(env.store.as_ref(), env.store.as_ref(), 64, 100_000).unwrap();

        assert_eq!(
            stats.reclaimed, 0,
            "collected a page born during the cycle — the epoch barrier did not hold"
        );
        assert_eq!(
            stats.spared_born_during_cycle,
            born_during.len() as u64,
            "the barrier must be the reason all {} survived, not an accident of marking: {:?}",
            born_during.len(),
            stats
        );

        // And the next cycle, opened after they were born, does collect them. Floating garbage
        // is deferred by one cycle, not leaked forever.
        while env.catalog.current_epoch() <= future {
            let _ = env.catalog.next_epoch();
        }
        let next = env.collect(64);
        assert_eq!(
            next.reclaimed,
            born_during.len() as u64,
            "the following cycle must collect what the barrier spared: {:?}",
            next
        );
    }

    /// **The race the whole design is for.** A branch forked *during* the mark reaches pages
    /// through a root the snapshot never contained. Nothing it can see may be collected.
    #[test]
    fn a_branch_forked_during_the_mark_keeps_every_page_it_can_reach() {
        let env = Env::new("forkrace");
        let trunk_root = env.grow_tree(BranchId::TRUNK, 300);

        let mut cycle = GcCycle::open(env.cat(), env.store.as_ref()).unwrap();
        // Mark only a sliver, so the fork lands squarely mid-mark.
        cycle.step(env.store.as_ref(), env.store.as_ref(), 2).unwrap();
        assert!(!cycle.is_done(), "the mark must still be in flight for this to be the race");

        // Fork mid-mark. The child's root IS the trunk's root at this instant.
        let child = env.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
        let child_id = child.branch_id;
        // And write in it, which allocates fresh pages and republishes a root the snapshot
        // never saw.
        let child_root = env.grow_tree(child_id, 40);

        let stats =
            cycle.run_to_completion(env.store.as_ref(), env.store.as_ref(), 64, 100_000).unwrap();

        assert_eq!(
            stats.reclaimed, 0,
            "collected {} pages while a branch forked mid-mark — a correctness bug, not a \
             performance one: {:?}",
            stats.reclaimed, stats
        );
        // Deliberately NOT asserting `spared_born_during_cycle > 0` here. A child gets its own
        // extents, so its pages land in arenas created after the cycle's arena snapshot and are
        // not examined this cycle at all — they are protected by the snapshot, not by the epoch
        // barrier. Asserting the barrier fired here would be claiming a mechanism that did not
        // run; `a_page_born_during_the_cycle_is_never_collected` is where the barrier is the
        // thing under test, and it plants into a snapshotted arena precisely so it is.
        assert!(
            stats.examined > 0,
            "the sweep must have examined the trunk's arenas even so: {:?}",
            stats
        );

        for i in 0..40u32 {
            let k = format!("k{:06}", i);
            assert!(
                env.tree.get(child_root, k.as_bytes()).unwrap().is_some(),
                "child lost {} to a collection that ran across its fork",
                k
            );
        }
        assert!(env.tree.get(trunk_root, b"k000299").unwrap().is_some(), "trunk damaged");
    }

    /// The guard is load-bearing, shown rather than asserted: at the moment the barrier spared
    /// those pages, a collector **without** the rule had a non-empty set to collect. If this
    /// count were zero the two tests above would be passing vacuously.
    #[test]
    fn without_the_epoch_barrier_there_would_be_something_to_collect() {
        let env = Env::new("firecheck");
        env.grow_tree(BranchId::TRUNK, 100);

        let mut cycle = GcCycle::open(env.cat(), env.store.as_ref()).unwrap();
        cycle.step(env.store.as_ref(), env.store.as_ref(), 4).unwrap();
        let born_during = env.plant_garbage(BranchId::TRUNK, 6, PageType::BTreeLeaf);
        let stats =
            cycle.run_to_completion(env.store.as_ref(), env.store.as_ref(), 64, 100_000).unwrap();

        // What a barrier-less sweep would have taken: allocated, tree-typed, and unmarked.
        let marked = cycle.marked().clone();
        let mut would_have_taken = 0u64;
        for (arena, _) in env.store.live_arenas() {
            for pid in env.store.allocated_pages(arena) {
                if marked.contains(&pid) {
                    continue;
                }
                let Ok(h) = env.store.read_page(pid) else { continue };
                let ty = PageHeader::read_from(&h.read().data).unwrap().page_type;
                if is_collectable_type(ty) {
                    would_have_taken += 1;
                }
            }
        }

        assert!(
            would_have_taken >= born_during.len() as u64,
            "a barrier-less collector would have taken {} pages; if this is 0 the barrier is \
             untested",
            would_have_taken
        );
        assert_eq!(stats.reclaimed, 0, "the real collector must take none of them");
    }

    /// The third bullet of the safety argument, which was asserted in prose and nowhere else:
    /// a branch's root pointer **swapped during the mark** must not cost it the pages the old
    /// root reached.
    ///
    /// This is the case shadow paging makes subtle. The new root shares every unchanged subtree
    /// with the old one page-identically, so those pages are protected only if the mark reached
    /// them through the root it snapshotted. If the cycle read roots lazily instead of
    /// snapshotting them, it would walk the NEW tree and free everything the write replaced that
    /// the old root still needs.
    #[test]
    fn a_root_swapped_during_the_mark_keeps_the_pages_the_old_root_reached() {
        let env = Env::new("rootswap");
        let old_root = env.grow_tree(BranchId::TRUNK, 400);

        // Fork a child FIRST, so the trunk's pages stop being private to it. Without this the
        // trunk mutates its own pages in place and the root never moves at all — `cow_page`
        // returns `copied == false` for a page born after the branch's own privacy barrier, so a
        // childless trunk rewriting 200 keys ends on the SAME root. That is the D31 fast path
        // working, and it silently made the first version of this test vacuous: it asserted a
        // root swap that had not happened.
        let child = env.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
        let child_id = child.branch_id;

        let mut cycle = GcCycle::open(env.cat(), env.store.as_ref()).unwrap();
        cycle.step(env.store.as_ref(), env.store.as_ref(), 2).unwrap();
        assert!(!cycle.is_done(), "the mark must still be in flight for this to be the case");

        // Overwrite a large slice of the tree and publish a new root, mid-mark.
        let mut root = old_root;
        for i in 0..200u32 {
            let e = env.catalog.next_epoch();
            let k = format!("k{:06}", i);
            let v = format!("REWRITTEN{:06}", i);
            root = env.tree.insert(root, BranchId::TRUNK, e, k.as_bytes(), v.as_bytes()).unwrap();
        }
        env.catalog.set_root(BranchId::TRUNK, root).unwrap();
        assert_ne!(
            root, old_root,
            "the write must actually have moved the root, or this test proves nothing"
        );
        // The child still points at the old root, which is what makes those pages genuinely
        // still-live rather than merely unreferenced garbage the sweep happens to miss.
        assert_eq!(env.catalog.get(child_id).unwrap().root_page_id, old_root);

        let stats =
            cycle.run_to_completion(env.store.as_ref(), env.store.as_ref(), 64, 200_000).unwrap();

        assert_eq!(
            stats.reclaimed, 0,
            "collected {} pages across a root swap: {:?}",
            stats.reclaimed, stats
        );

        // The NEW tree reads correctly...
        for i in 0..400u32 {
            let k = format!("k{:06}", i);
            assert!(
                env.tree.get(root, k.as_bytes()).unwrap().is_some(),
                "new root lost {}",
                k
            );
        }
        // ...and so does the OLD one, which is the half a lazy root read would have broken.
        for i in 0..400u32 {
            let k = format!("k{:06}", i);
            assert!(
                env.tree.get(old_root, k.as_bytes()).unwrap().is_some(),
                "old root lost {} — the snapshot did not protect what it reached",
                k
            );
        }
    }

    // ---- pause and interference --------------------------------------------------------------

    /// The pause bound, stated exactly: **no slice touches more than `budget` pages**, on either
    /// side of the cycle. It contains no term in total pages and no term in extent size.
    ///
    /// The bound used to be `max(budget, ARENA_EXTENT_PAGES)`, because the sweep took a whole
    /// extent per slice. That is what `d96_pause_curve` measured at 72 ms and what
    /// `GcCycle::sweep_page_idx` now removes, so this asserts the tighter bound — a test that
    /// still allowed 256 would pass just as well against the old code and would not be pinning
    /// the fix.
    #[test]
    fn no_slice_exceeds_the_budget() {
        let env = Env::new("pause");
        env.grow_tree(BranchId::TRUNK, 2_000);
        env.plant_garbage(BranchId::TRUNK, 40, PageType::BTreeLeaf);
        let _ = env.catalog.next_epoch();

        let budget = 32u64;
        let stats = env.collect(budget);

        assert!(
            stats.max_slice_pages <= budget,
            "a slice touched {} pages against a budget of {}: {:?}",
            stats.max_slice_pages,
            budget,
            stats
        );
        assert!(
            (ARENA_EXTENT_PAGES as u64) > budget,
            "this test only pins the sweep cursor while an extent is bigger than the budget"
        );
        assert!(
            stats.slices > 1,
            "a 2000-key tree at budget 32 must take many slices: {:?}",
            stats
        );
        assert_eq!(stats.reclaimed, 40);
    }

    /// The collector and the existing pending-free drain may both reach a page. `release_page`
    /// is idempotent, so the overlap must not double-decrement the live count.
    #[test]
    fn a_concurrent_drain_does_not_double_count() {
        let env = Env::new("drain");
        env.grow_tree(BranchId::TRUNK, 150);
        let planted = env.plant_garbage(BranchId::TRUNK, 8, PageType::BTreeLeaf);
        let _ = env.catalog.next_epoch();

        // The OWNING arena of each planted page, read from its own header while it is still
        // readable. A racing drain releases against the arena that owns the page;
        // `release_page` does not verify ownership and will happily push a page onto a
        // stranger's recycled list and decrement `live_pages` for it, so releasing against
        // every arena is not "what a drain would do", it is a different bug. The first version
        // of this test did that and drove the count to 4294967273.
        let owners: Vec<(PageId, ArenaId)> = planted
            .iter()
            .map(|p| {
                let h = env.store.read_page(*p).unwrap();
                let a = PageHeader::read_from(&h.read().data).unwrap().arena_id;
                (*p, a)
            })
            .collect();

        let before = env.store.live_page_count().unwrap();
        let stats = env.collect(64);
        assert_eq!(stats.reclaimed, 8);

        // Release every collected page a second time, exactly as a racing drain would.
        for (pid, arena) in &owners {
            env.store.release_page(*pid, *arena);
        }
        assert_eq!(
            env.store.live_page_count().unwrap(),
            before - 8,
            "a second release moved the live count: the overlap with drain_pending is not benign"
        );
    }

    /// A cycle that cannot finish must say so rather than return a partial result that reads
    /// like a completed one.
    #[test]
    fn an_unfinished_cycle_refuses_rather_than_reporting_a_small_number() {
        let env = Env::new("refuse");
        env.grow_tree(BranchId::TRUNK, 500);
        let mut cycle = GcCycle::open(env.cat(), env.store.as_ref()).unwrap();
        let err =
            cycle.run_to_completion(env.store.as_ref(), env.store.as_ref(), 1, 3).unwrap_err();
        assert!(
            format!("{:?}", err).contains("did not finish"),
            "expected a refusal, got {:?}",
            err
        );
    }

    // ---- the measurement ---------------------------------------------------------------------

    /// Free bytes on the volume holding `path`, via `df -Pk`.
    fn free_bytes(path: &std::path::Path) -> u64 {
        let out = std::process::Command::new("df")
            .arg("-Pk")
            .arg(path)
            .output()
            .expect("df");
        let text = String::from_utf8_lossy(&out.stdout);
        let line = text.lines().nth(1).unwrap_or("");
        let avail_kb: u64 =
            line.split_whitespace().nth(3).and_then(|s| s.parse().ok()).unwrap_or(0);
        avail_kb * 1024
    }

    /// **The pause curve.** `cargo test --lib cow::gc::tests::d96_pause_curve -- --ignored --nocapture`
    ///
    /// Ignored by default because it writes several GB. The claim under test is narrow and is
    /// stated as a slope, not a ratio: **the pause does not grow with total chunks.** So the
    /// reachable tree is held CONSTANT across the axis and only the garbage count moves —
    /// otherwise a growing mark set would be confounded with a growing heap and the flat result
    /// would be unearned.
    ///
    /// What is expected to grow, and is reported separately rather than hidden: total cycle
    /// time (the work really is O(chunks)) and `snapshot_nanos` (one `live_arenas` call per
    /// cycle, O(arenas)). The second one is the honest cost of this design and the number a
    /// reader should check hardest.
    #[test]
    #[ignore]
    fn d96_pause_curve() {
        use crate::storage::disk_manager::PAGE_SIZE;
        use std::io::Write;

        // Refuse rather than fill the disk. The repo's own 10^6 harness carries a floor for the
        // same reason; this machine has been at 98% capacity.
        const DISK_FLOOR_BYTES: u64 = 12 * 1024 * 1024 * 1024;
        const REACHABLE_KEYS: u32 = 2_000;
        const BUDGET: u64 = 64;

        // Wall-clock budget. The plant phase, not the collection, is what costs: this box runs a
        // ten-agent fleet and the 10k row's plant was measured at 0.90 s on a quiet machine and
        // 41.4 s on a loaded one, a 46x spread. A row that cannot start inside the budget is
        // declined in the file rather than discovered by a SIGKILL.
        const WALL_BUDGET_S: f64 = 2_100.0;

        let tmp = std::env::temp_dir();
        let mut rows: Vec<String> = Vec::new();
        let mut refused_at: Option<(u32, &str)> = None;
        let t_start = Instant::now();

        // **The file is opened and its header written BEFORE the first row, and every row is
        // flushed as it lands.** The first version of this harness built the whole file at the
        // end, so the run that was killed part-way through the 10^6 row banked nothing at all —
        // including the two rows it had already earned. A measurement that survives only if the
        // largest point completes is not a curve, it is a single fragile reading.
        let mut f = std::fs::File::create("bench/d96_chunk_gc.txt").unwrap();
        writeln!(f, "D96 — REACHABILITY GC AT PAGE GRANULARITY: THE PAUSE CURVE.").unwrap();
        writeln!(f, "[HERE] {}", crate::build_provenance()).unwrap();
        writeln!(f).unwrap();
        writeln!(
            f,
            "⚠ MEASURED ON A LOADED BOX: a ten-agent fleet was building and testing concurrently."
        )
        .unwrap();
        writeln!(
            f,
            "max_slice_ns is therefore a CEILING under contention, not a quiet-machine figure. The"
        )
        .unwrap();
        writeln!(
            f,
            "claim it is asked to support is a SLOPE across the chunk axis, and load inflates every"
        )
        .unwrap();
        writeln!(f, "row rather than tilting the axis, so the slope survives what the level does not.")
            .unwrap();
        writeln!(f).unwrap();
        writeln!(
            f,
            "     chunks     arenas  max_slice_ns  snapshot_ns  max_pages       slices   cycle_s   plant_s   bytes_recl"
        )
        .unwrap();
        f.flush().unwrap();

        for n in [10_000u32, 100_000, 1_000_000] {
            let need = (n as u64 + 4_096) * PAGE_SIZE as u64;
            let free = free_bytes(&tmp);
            if free.saturating_sub(need) < DISK_FLOOR_BYTES {
                refused_at = Some((n, "disk floor"));
                break;
            }
            // Project this row off the last one's measured plant rate rather than a guess.
            let elapsed = t_start.elapsed().as_secs_f64();
            if elapsed > 0.0 && !rows.is_empty() {
                let projected = elapsed * (n as f64 / 10_000.0f64.max(1.0));
                if elapsed + projected > WALL_BUDGET_S {
                    refused_at = Some((n, "wall-clock budget"));
                    break;
                }
            }

            let env = Env::new(&format!("curve{}", n));
            // Constant reachable set.
            env.grow_tree(BranchId::TRUNK, REACHABLE_KEYS);

            let t_plant = Instant::now();
            for _ in 0..n {
                let e = env.catalog.next_epoch();
                env.store.alloc_for(BranchId::TRUNK, PageType::BTreeLeaf, e).unwrap();
            }
            let plant_s = t_plant.elapsed().as_secs_f64();
            let _ = env.catalog.next_epoch();

            let arenas_at_open = env.store.live_arenas().len();
            let live_before = env.store.live_page_count().unwrap();

            let t_cycle = Instant::now();
            let stats = collect_once_arena(env.cat(), &env.store, BUDGET).unwrap();
            let cycle_s = t_cycle.elapsed().as_secs_f64();

            let live_after = env.store.live_page_count().unwrap();
            let bytes = stats.reclaimed * PAGE_SIZE as u64;

            assert_eq!(
                stats.reclaimed, n as u64,
                "planted {} garbage pages, collected {} — the curve is only meaningful if the \
                 collector actually found them: {:?}",
                n, stats.reclaimed, stats
            );
            assert_eq!(live_before - live_after, n, "live page count disagrees with reclaimed");

            rows.push(format!(
                "{:>10}  {:>9}  {:>12}  {:>13}  {:>10}  {:>11}  {:>10.2}  {:>9.2}  {:>10}",
                n,
                arenas_at_open,
                stats.max_slice_nanos,
                stats.snapshot_nanos,
                stats.max_slice_pages,
                stats.slices,
                cycle_s,
                plant_s,
                bytes
            ));
            // Banked immediately, so a kill after this point cannot take the row with it.
            writeln!(f, "{}", rows.last().unwrap()).unwrap();
            f.flush().unwrap();
            println!("{}", rows.last().unwrap());
        }

        writeln!(f).unwrap();
        writeln!(
            f,
            "PREMISE CHECK FIRST — the row as briefed was aimed at a wall that D31 already removed."
        )
        .unwrap();
        writeln!(
            f,
            "  The 262x space amplification is REAL and is bench/d31_before.txt: 4000 branches"
        )
        .unwrap();
        writeln!(
            f,
            "  writing one page each -> 4193.3 MB of data file holding 16.4 MB (1,048,316 B/branch)."
        )
        .unwrap();
        writeln!(
            f,
            "  That is the BEFORE number. bench/d31_after.txt reads 4097 B/branch against a 4096 B"
        )
        .unwrap();
        writeln!(
            f,
            "  page -- 'VERDICT -- AMPLIFICATION GONE' -- because ARENA_FIRST_EXTENT_PAGES is 1 and"
        )
        .unwrap();
        writeln!(
            f,
            "  next_extent_pages() grows geometrically. Extent granularity is NOT the space wall today."
        )
        .unwrap();
        writeln!(
            f,
            "  The brief cited bench/d32_write_curve.txt, which does not exist in this tree."
        )
        .unwrap();
        writeln!(f).unwrap();
        writeln!(
            f,
            "  And 'unreachable chunks' presupposes content addressing, which src/cow/mod.rs lists"
        )
        .unwrap();
        writeln!(
            f,
            "  as a NON-GOAL with a reason. There are no hash-shared chunks here, so there is no"
        )
        .unwrap();
        writeln!(f, "  chunk that 'no version references' in the Dolt sense.").unwrap();
        writeln!(f).unwrap();
        writeln!(
            f,
            "WHAT IS ACTUALLY MISSING, and what this measures: reclamation is the epoch interval"
        )
        .unwrap();
        writeln!(
            f,
            "rule over pages an owner FREED, plus wholesale extent frees at reap. Neither asks"
        )
        .unwrap();
        writeln!(
            f,
            "whether a page is REACHABLE. A page allocated in a live branch's arena and linked into"
        )
        .unwrap();
        writeln!(
            f,
            "no tree is invisible to both until the whole branch dies. grep over src/ finds no"
        )
        .unwrap();
        writeln!(f, "reachability marking of any kind. That is the class collected here.").unwrap();
        writeln!(f).unwrap();
        writeln!(
            f,
            "CLAIM UNDER TEST (slope, not ratio): max_slice_nanos does NOT grow with total chunks."
        )
        .unwrap();
        writeln!(
            f,
            "Reachable tree held CONSTANT at {} keys across the axis, so a growing mark set cannot",
            REACHABLE_KEYS
        )
        .unwrap();
        writeln!(f, "be confounded with a growing heap. Slice budget {}.", BUDGET).unwrap();
        writeln!(f).unwrap();
        if let Some((n, why)) = refused_at {
            writeln!(f, "REFUSED at chunks={}: {}.", n, why).unwrap();
            writeln!(
                f,
                "  disk floor = {} GiB free; wall-clock budget = {} s.",
                DISK_FLOOR_BYTES / (1024 * 1024 * 1024),
                WALL_BUDGET_S as u64
            )
            .unwrap();
            writeln!(
                f,
                "The stopping point IS the result for that row. It is NOT a pass, and the rows"
            )
            .unwrap();
            writeln!(
                f,
                "above do not become a 10^6 result by sitting next to a refusal for 10^6."
            )
            .unwrap();
        } else {
            writeln!(f, "Every row in the axis completed; nothing was refused.").unwrap();
        }
        writeln!(f).unwrap();
        writeln!(
            f,
            "READING IT: max_slice_nanos is the pause and must be flat. snapshot_ns is the one"
        )
        .unwrap();
        writeln!(
            f,
            "term that DOES scale -- a single live_arenas() call per cycle, O(arenas) under the"
        )
        .unwrap();
        writeln!(
            f,
            "state mutex. It is reported apart from the pause rather than folded into it, because"
        )
        .unwrap();
        writeln!(
            f,
            "folding it in would flatter the number at small heaps and hide it at large ones. It"
        )
        .unwrap();
        writeln!(
            f,
            "is paid once per cycle and never per slice: per slice it would be W4 again (39.3 s"
        )
        .unwrap();
        writeln!(f, "holding the per-statement lock across O(arenas) work).").unwrap();
        writeln!(f).unwrap();
        writeln!(f, "cycle_s grows linearly and is SUPPOSED to: total work is O(chunks). Only the")
            .unwrap();
        writeln!(f, "pause is claimed flat.").unwrap();
        writeln!(f).unwrap();
        writeln!(
            f,
            "WHAT THE FIRST RUN OF THIS HARNESS FOUND, kept because it is the reason the code"
        )
        .unwrap();
        writeln!(
            f,
            "changed: the sweep took ONE WHOLE EXTENT per slice, so the pause was pinned to"
        )
        .unwrap();
        writeln!(
            f,
            "ARENA_EXTENT_PAGES however small a budget was asked for. Measured at chunks=10000:"
        )
        .unwrap();
        writeln!(
            f,
            "  max_slice_ns = 72,082,584 (72 ms), max_slice_pages = 256, arenas = 47, slices = 48."
        )
        .unwrap();
        writeln!(
            f,
            "Bounded in pages and independent of heap size -- the claim held -- but 72 ms is a long"
        )
        .unwrap();
        writeln!(
            f,
            "pause and the budget knob did nothing on that side. Each swept page costs a read_page"
        )
        .unwrap();
        writeln!(
            f,
            "and a release_page (page-table write lock, frame reset, ARC update), so the per-page"
        )
        .unwrap();
        writeln!(
            f,
            "constant is large. GcCycle::sweep_page_idx resumes part-way into an extent, which is"
        )
        .unwrap();
        writeln!(f, "what makes budget the actual pause lever. The rows above are from after that.")
            .unwrap();

        assert!(!rows.is_empty(), "a curve with no rows has not measured anything");
    }

    /// A reaped branch's root must not pin pages, or a reap would never reclaim anything.
    #[test]
    fn only_a_reaped_branch_stops_being_a_root() {
        assert!(state_pins_pages(BranchState::Live));
        assert!(state_pins_pages(BranchState::Quarantined), "quarantined is still queryable");
        assert!(state_pins_pages(BranchState::Reaping), "mid-reap children are the authority");
        assert!(!state_pins_pages(BranchState::Reaped));
    }
}
