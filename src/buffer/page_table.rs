//! `page_id -> frame index`, with a lock-free mirror in front of the map.
//!
//! # Why this is not just a `RwLock<HashMap>`
//!
//! D35 measured the buffer pool's RESIDENT hit path and found the binding constraint was not the
//! replacement policy — deleting `arc_cache.lock().touch()` outright, which is the upper bound on
//! BP-Wrapper or any other batching scheme, moved single-thread throughput 47% and left the
//! **sign of the slope unchanged**. What holds the slope is the page table itself: the hit loop is
//! `fetch_page` + `unpin_page`, and between them each iteration took `page_table.read()` twice.
//!
//! A Rust `RwLock`'s reader count is one process-wide cache line that **every reader atomically
//! RMWs**. It contends exactly like a `Mutex`; it is simply not spelled `Mutex`. Two readers of the
//! same page table on different cores serialise on that line whether or not they conflict.
//!
//! So the resident path resolves `page_id -> frame` through [`PageTable::lookup`], which takes no
//! lock at all: one `Acquire` load of one `AtomicU64`.
//!
//! # The mirror is a hint, and the frame latch is still the arbiter
//!
//! This is the same contract `buffer_pool.rs` already states about the map: *"A page table lookup
//! only ever produces a candidate frame."* Every caller re-checks `frame.page_id` under that
//! frame's own latch before pinning it. The mirror produces the same candidate by a cheaper route,
//! so a mirror entry that has gone stale costs a retry and **cannot hand out a wrong frame**.
//!
//! [`PageTable::lookup`] is therefore allowed to be wrong in exactly two ways and no others:
//!
//! * **Absent when the map has an entry** (a collision evicted it). The caller falls back to the
//!   map and gets today's answer at today's cost.
//! * **Naming a frame the page has since left.** The caller's `frame.page_id` check rejects it.
//!
//! It may **not** name a *different live* frame for a page that is resident elsewhere, and it
//! cannot: every mirror write happens inside the same `page_table` write-lock critical section as
//! the map write it mirrors (see [`PageTableWriteGuard`]), so the two can never be reordered
//! against each other. A slot that names `p` was last written by the insert that made `p`
//! resident in that frame.
//!
//! # Direct-mapped and TAGGED, which is the difference from the measurement scaffold
//!
//! The D35 scaffold on branch `D35-gate-stubtouch` indexed a `Vec<AtomicUsize>` by page id
//! directly. That is O(max page id) and is exactly what the design entry named as a falsifier for
//! a merge-ready version: page ids here are `u32`, so a directly-indexed mirror is a 32 GiB
//! allocation in the limit, and arena pages are handed out from extents far above the heap.
//!
//! This is the same structure with a **tag**: a fixed number of slots, indexed by
//! `page_id & (slots - 1)`, each slot one `AtomicU64` packing `(page_id, frame_i + 1)`. Memory is
//! constant and independent of the page id space. A slot collision is *detectable* — the tag does
//! not match — so it degrades to the map lookup instead of returning someone else's frame.
//!
//! `frame_i + 1` with `0` meaning absent is kept from the scaffold: it makes "empty" a single
//! zero-word test on the whole slot rather than a sentinel that has to be compared against.
//!
//! # Why the map is still here
//!
//! The mirror is lossy by construction, so it cannot be the authority: it answers "which frame, if
//! you are lucky", and the pool needs "which pages are resident" for `flush_all`, `invalidate_all`
//! and the replacement policy's `is_pinned`. The map is the authority and the mirror is a cache of
//! it. Prior art for the split is PostgreSQL's partitioned buffer mapping table, which solves the
//! same contention with a different trade: N locks instead of none, and no lossy fast path.
//!
//! # How this type makes the mirror impossible to desynchronise
//!
//! The obvious version of this change is a `Vec<AtomicU64>` field next to the existing `pub
//! page_table`, maintained by hand at every write site. There are six of those and one of them is
//! in another module (`branch::arena::evict`), so "maintained by hand" means the next person to
//! add a seventh has to know. A missed site is a mirror entry that outlives its page, and the
//! failure it produces is a wrong frame handed to a reader — the one failure a storage engine
//! cannot apologise for, arriving by omission.
//!
//! So the map is **private** and the only way to mutate it is [`PageTableWriteGuard`], which
//! updates both halves in one call. The guard derefs to `&HashMap` so every read site keeps
//! working unchanged, and deliberately does **not** implement `DerefMut`: `HashMap`'s own
//! `insert`/`remove`/`clear` need `&mut self`, so they are unreachable through the guard and the
//! inherent methods here are the only way in. Adding a seventh write site cannot miss the mirror,
//! because there is no way to express a write that does.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LockResult, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// An unoccupied mirror slot. `frame_i + 1` is never zero, so the whole word being zero is the
/// only "empty" and it costs one comparison against zero rather than an unpack.
const EMPTY: u64 = 0;

/// Pack a mapping into one word: page id in the high half, `frame_i + 1` in the low half.
#[inline]
fn pack(page_id: u32, frame_i: usize) -> u64 {
    ((page_id as u64) << 32) | (frame_i as u64 + 1)
}

/// The page id a slot is tagged with, or `None` if the slot is empty.
#[inline]
fn tag_of(slot: u64) -> Option<u32> {
    if slot == EMPTY { None } else { Some((slot >> 32) as u32) }
}

/// The frame a non-empty slot names.
#[inline]
fn frame_of(slot: u64) -> usize {
    (slot & 0xffff_ffff) as usize - 1
}

/// `page_id -> frame index`, authoritative in a `HashMap` and mirrored lock-free.
///
/// See the module doc. The short version: [`PageTable::lookup`] is the hit path and takes no lock;
/// [`PageTable::read`] and [`PageTable::write`] are the authority and behave like the `RwLock`
/// this replaced.
pub struct PageTable {
    /// The authority. Private — see the module doc's last section.
    map: RwLock<HashMap<u32, usize>>,
    /// Direct-mapped, tagged mirror of `map`. Length is a power of two so the index is a mask.
    ///
    /// **Only [`PageTableWriteGuard`] writes this**, and it holds `map`'s write lock for the whole
    /// of every update. Mirror writes are therefore totally ordered with respect to each other and
    /// to the map writes they accompany, which is what lets the maintenance below use plain loads
    /// and stores rather than a CAS loop.
    mirror: Box<[AtomicU64]>,
}

impl PageTable {
    /// A table whose mirror has at least `min_slots` slots, rounded up to a power of two.
    ///
    /// The rounding is not a convenience: the index is computed as `page_id & (slots - 1)`, which
    /// is only a valid index for a power-of-two length. Rounding here means a caller cannot pass a
    /// size that would make the mask wrong, rather than a comment asking it not to.
    ///
    /// Sizing is the caller's call because the right number is a function of the pool: at most
    /// `frames` pages can be resident at once, so slots comfortably above the frame count keeps
    /// collisions rare for any page id distribution that is not adversarial. A collision is a
    /// slower lookup and never a wrong one, so this is a throughput knob and not a correctness one.
    pub fn new(min_slots: usize) -> Self {
        let slots = min_slots.max(1).next_power_of_two();
        PageTable {
            map: RwLock::new(HashMap::new()),
            mirror: (0..slots).map(|_| AtomicU64::new(EMPTY)).collect(),
        }
    }

    /// Which frame holds `page_id`, resolved **without taking any lock** — or `None`.
    ///
    /// `None` means "ask [`PageTable::read`]", never "not resident": a collision evicts a live
    /// entry from its slot and the page is still in the map. Callers must fall back.
    ///
    /// A `Some` is a **candidate frame** on exactly the same terms as a map lookup, and must be
    /// re-checked against `frame.page_id` under that frame's latch before it is used.
    #[inline]
    pub fn lookup(&self, page_id: u32) -> Option<usize> {
        // Acquire pairs with the Release store in `PageTableWriteGuard::insert`, so a thread that
        // sees the mapping also sees everything the publishing thread did before it.
        let slot = self.mirror[self.slot_of(page_id)].load(Ordering::Acquire);
        if tag_of(slot) == Some(page_id) { Some(frame_of(slot)) } else { None }
    }

    /// Read the authoritative map. Same shape as the `RwLock` this type replaced, so every
    /// existing `page_table.read().unwrap()` site is unchanged.
    #[inline]
    pub fn read(&self) -> LockResult<RwLockReadGuard<'_, HashMap<u32, usize>>> {
        self.map.read()
    }

    /// Write the authoritative map **and its mirror**, through [`PageTableWriteGuard`].
    pub fn write(&self) -> LockResult<PageTableWriteGuard<'_>> {
        match self.map.write() {
            Ok(map) => Ok(PageTableWriteGuard { map, mirror: &self.mirror }),
            Err(poisoned) => Err(std::sync::PoisonError::new(PageTableWriteGuard {
                map: poisoned.into_inner(),
                mirror: &self.mirror,
            })),
        }
    }

    /// How many mirror slots there are. For tests and for the pool's own diagnostics.
    #[inline]
    pub fn mirror_slots(&self) -> usize {
        self.mirror.len()
    }

    /// What the mirror currently says, without the tag check. **Tests only** — a caller that wants
    /// the mapping wants [`PageTable::lookup`], which is the one that cannot lie.
    #[doc(hidden)]
    pub fn mirror_slot_raw(&self, page_id: u32) -> Option<(u32, usize)> {
        let slot = self.mirror[self.slot_of(page_id)].load(Ordering::Acquire);
        tag_of(slot).map(|tag| (tag, frame_of(slot)))
    }

    #[inline]
    fn slot_of(&self, page_id: u32) -> usize {
        // `mirror.len()` is a power of two by construction in `new`.
        page_id as usize & (self.mirror.len() - 1)
    }
}

/// The only way to mutate a [`PageTable`], and it maintains the mirror on every path.
///
/// Derefs to `&HashMap` so reads through it — `get`, `contains_key`, `iter`, `len` — are the map's
/// own. It does **not** implement `DerefMut`, which is what makes `HashMap::insert`/`remove`/
/// `clear` unreachable from here: those need `&mut self`, and the inherent methods below shadow
/// them. A write that forgets the mirror is not expressible.
pub struct PageTableWriteGuard<'a> {
    map: RwLockWriteGuard<'a, HashMap<u32, usize>>,
    mirror: &'a [AtomicU64],
}

impl Deref for PageTableWriteGuard<'_> {
    type Target = HashMap<u32, usize>;
    #[inline]
    fn deref(&self) -> &HashMap<u32, usize> {
        &self.map
    }
}

impl PageTableWriteGuard<'_> {
    /// Map `page_id` to `frame_i`, in the map and in the mirror.
    ///
    /// The mirror store is **unconditional**: the newest mapping owns the slot. Whatever page was
    /// tagged there loses its fast path and falls back to the map, which is the designed
    /// degradation and not a loss of information — the map still has it.
    pub fn insert(&mut self, page_id: u32, frame_i: usize) -> Option<usize> {
        // The packing gives the frame index 32 bits, which is four million times the largest pool
        // anyone has configured. Asserted rather than assumed because the failure mode of an
        // overflow here is a silently wrong frame, and this runs only on the miss path.
        assert!(
            frame_i < u32::MAX as usize,
            "frame index {frame_i} does not fit the page table mirror's 32-bit frame field"
        );
        let prev = self.map.insert(page_id, frame_i);
        // Release: a reader that sees this mapping through `lookup`'s Acquire load also sees
        // everything this thread did before publishing it.
        self.mirror[self.slot_of(page_id)].store(pack(page_id, frame_i), Ordering::Release);
        prev
    }

    /// Unmap `page_id` from the map, and from the mirror **if the mirror still names it**.
    ///
    /// The tag check is the whole of the correctness argument for `remove`: if some other page has
    /// since taken this slot, clearing it would evict *that* page's fast path — harmless but
    /// wasteful — and, worse, a later `remove` of this page id would have no way to tell the two
    /// cases apart. Only the tagged owner may clear a slot.
    ///
    /// A plain load-then-store is enough rather than a compare-exchange: every mirror write in the
    /// process happens under `map`'s write lock, which this guard holds, so no other writer can be
    /// running between the load and the store.
    pub fn remove(&mut self, page_id: &u32) -> Option<usize> {
        let prev = self.map.remove(page_id);
        let slot = &self.mirror[self.slot_of(*page_id)];
        if tag_of(slot.load(Ordering::Relaxed)) == Some(*page_id) {
            slot.store(EMPTY, Ordering::Release);
        }
        prev
    }

    /// Drop every mapping, in the map and the mirror.
    ///
    /// Every slot is cleared rather than only the ones the map named, because a slot can be tagged
    /// with a page whose map entry was already gone — `remove` leaves a slot alone when another
    /// page owns it, and the owner may itself have been removed later. Sweeping the whole mirror
    /// is the only way to be sure nothing survives, and this runs once per snapshot install.
    pub fn clear(&mut self) {
        self.map.clear();
        for slot in self.mirror {
            slot.store(EMPTY, Ordering::Release);
        }
    }

    #[inline]
    fn slot_of(&self, page_id: u32) -> usize {
        page_id as usize & (self.mirror.len() - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mirror_answers_what_the_map_answers() {
        let pt = PageTable::new(64);
        pt.write().unwrap().insert(7, 3);
        assert_eq!(pt.lookup(7), Some(3));
        assert_eq!(pt.read().unwrap().get(&7).copied(), Some(3));
    }

    #[test]
    fn a_removed_page_is_gone_from_both_halves() {
        let pt = PageTable::new(64);
        pt.write().unwrap().insert(7, 3);
        pt.write().unwrap().remove(&7);
        assert_eq!(pt.lookup(7), None);
        assert_eq!(pt.read().unwrap().get(&7).copied(), None);
        assert_eq!(pt.mirror_slot_raw(7), None, "the slot was not cleared");
    }

    #[test]
    fn min_slots_is_rounded_up_to_a_power_of_two() {
        // A non-power-of-two length would make `page_id & (len - 1)` produce indices that alias
        // wrongly or exceed the slice, so the constructor must not accept one.
        assert_eq!(PageTable::new(1000).mirror_slots(), 1024);
        assert_eq!(PageTable::new(1024).mirror_slots(), 1024);
        assert_eq!(PageTable::new(0).mirror_slots(), 1);
    }

    /// The collision case, which is the one the tag exists for. Two page ids that land in the same
    /// slot must never be confused for one another; the loser simply has no fast path.
    #[test]
    fn a_collision_costs_the_fast_path_and_never_returns_the_wrong_frame() {
        let pt = PageTable::new(64); // 64 slots, so 5 and 69 collide
        assert_eq!(5usize & 63, 69usize & 63, "precondition: these ids must share a slot");

        pt.write().unwrap().insert(5, 1);
        pt.write().unwrap().insert(69, 2);

        // 69 owns the slot now. 5 is still resident and the MAP still knows where.
        assert_eq!(pt.lookup(69), Some(2));
        assert_eq!(pt.lookup(5), None, "a collision must report absent, never page 69's frame");
        assert_eq!(pt.read().unwrap().get(&5).copied(), Some(1), "the map is still the authority");
    }

    /// `remove` must not clear a slot another page has taken over — doing so would silently strip
    /// the live page's fast path, and is the bug the tag check prevents.
    #[test]
    fn removing_a_collided_out_page_leaves_the_slot_owner_alone() {
        let pt = PageTable::new(64);
        pt.write().unwrap().insert(5, 1);
        pt.write().unwrap().insert(69, 2); // 69 takes the slot from 5

        pt.write().unwrap().remove(&5); // 5 no longer owns the slot

        assert_eq!(pt.lookup(69), Some(2), "removing page 5 stole page 69's mirror entry");
        assert_eq!(pt.read().unwrap().get(&69).copied(), Some(2));
    }

    /// `clear` must empty both halves, including the slot of a page that was collided out of the
    /// mirror and is therefore only in the map.
    #[test]
    fn clear_empties_both_halves() {
        let pt = PageTable::new(64);
        pt.write().unwrap().insert(5, 1);
        pt.write().unwrap().insert(69, 2); // 69 takes 5's slot; 5 lives only in the map
        assert_eq!(pt.lookup(5), None, "precondition: 5 was collided out of its slot");

        pt.write().unwrap().clear();

        assert_eq!(pt.lookup(69), None);
        assert_eq!(pt.mirror_slot_raw(69), None, "clear left a tagged slot behind");
        assert!(pt.read().unwrap().is_empty());
    }

    /// **The invariant the whole design rests on, stated as a test.** A tagged slot always agrees
    /// with the map, for every sequence of writes — the mirror may be missing an entry the map
    /// has, and never the other way round. Driven over a schedule that exercises collisions,
    /// re-mapping, and removing a page that no longer owns its slot.
    #[test]
    fn a_tagged_slot_always_agrees_with_the_map() {
        let pt = PageTable::new(16);
        // A deterministic pseudo-random schedule; `state` is an LCG so the sequence is fixed.
        let mut state: u32 = 12345;
        let mut next = || {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            state
        };
        for step in 0..20_000 {
            let page = next() % 64; // 64 ids over 16 slots: collisions on purpose
            let frame = (next() % 32) as usize;
            if step % 3 == 0 {
                pt.write().unwrap().remove(&page);
            } else {
                pt.write().unwrap().insert(page, frame);
            }
            // Check every id, not just the one touched: a write must not corrupt a bystander.
            for id in 0..64u32 {
                if let Some((tag, frame_i)) = pt.mirror_slot_raw(id) {
                    assert_eq!(
                        pt.read().unwrap().get(&tag).copied(),
                        Some(frame_i),
                        "step {step}: mirror slot for id {id} is tagged {tag} -> frame {frame_i}, \
                         which the map does not agree with"
                    );
                }
            }
        }
        assert!(pt.read().unwrap().len() > 0, "the schedule left the table empty - it proves nothing");
    }

    /// Force the failure this design is for: a mirror maintained by hand can go stale. Here the
    /// mirror is maintained by the guard, so re-mapping a page to a different frame must move the
    /// mirror with it rather than leaving the old frame behind.
    #[test]
    fn remapping_a_page_moves_its_mirror_entry() {
        let pt = PageTable::new(64);
        pt.write().unwrap().insert(7, 3);
        pt.write().unwrap().remove(&7);
        pt.write().unwrap().insert(7, 11);
        assert_eq!(pt.lookup(7), Some(11), "the mirror still names the page's OLD frame");
    }

    /// Page id 0 in frame 0 packs to the smallest non-empty word there is. If `EMPTY` were tested
    /// against the tag rather than the whole word, this mapping would read as absent.
    #[test]
    fn page_zero_in_frame_zero_is_not_mistaken_for_an_empty_slot() {
        let pt = PageTable::new(64);
        pt.write().unwrap().insert(0, 0);
        assert_eq!(pt.lookup(0), Some(0));
    }

    #[test]
    fn the_largest_page_id_round_trips() {
        let pt = PageTable::new(64);
        pt.write().unwrap().insert(u32::MAX, 63);
        assert_eq!(pt.lookup(u32::MAX), Some(63));
        assert_eq!(pt.read().unwrap().get(&u32::MAX).copied(), Some(63));
    }
}
