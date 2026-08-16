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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use crate::branch::record::{reclaimable, ArenaExtent, BranchRecord, PendingFree};
use crate::branch::types::{
    ArenaId, BranchError, BranchId, Epoch, PageId, ARENA_EXTENT_PAGES,
};
use crate::branch::catalog::LogBranchCatalog;
use crate::branch::BranchCatalog;
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::cow::page_header::{flags, stamp_checksum, verify_checksum, PageHeader, PageType};
use crate::cow::{CowPage, PageHandle, PageStore, PAGE_HEADER_SIZE};
use crate::error::FerroError;
use crate::storage::disk_manager::PAGE_SIZE;

/// The epoch at or after which a page in this branch's own arena may still be mutated in place.
///
/// A page is safe for in-place mutation only if nobody else can see it. Two things can make
/// somebody else see it: the page predates this branch's own fork (so the parent has it too), or
/// a child forked off this branch after the page was born (so that child has it too). The
/// barrier is therefore the later of this branch's fork epoch and its most recent child's fork
/// epoch, and it is what gets handed to [`PageHeader::is_private_to`].
pub fn privacy_barrier(rec: &BranchRecord) -> Epoch {
    match rec.live_children.last() {
        Some(latest) => Epoch(rec.fork_epoch.0.max(latest.0)),
        None => rec.fork_epoch,
    }
}

/// Hands out contiguous extents and takes them back whole.
struct ArenaSpaceManager {
    base_page: PageId,
    extent_pages: u32,
    next_extent_start: AtomicU32,
    /// Extent start pages returned by `free_arena`, ready to be handed out again. Reuse is what
    /// makes the reserved page count return to baseline rather than merely stopping its growth.
    free_extent_starts: Mutex<Vec<PageId>>,
    next_arena_id: AtomicU32,
}

impl ArenaSpaceManager {
    fn reserve(&self) -> Result<(ArenaId, PageId), FerroError> {
        let start = match self.free_extent_starts.lock().unwrap().pop() {
            Some(s) => s,
            None => self.next_extent_start.fetch_add(self.extent_pages, Ordering::SeqCst),
        };
        let arena = ArenaId(self.next_arena_id.fetch_add(1, Ordering::SeqCst));
        Ok((arena, start))
    }

    fn give_back(&self, start: PageId) {
        self.free_extent_starts.lock().unwrap().push(start);
    }
}

struct StoreState {
    /// Live extents by arena. An arena absent from this map has been freed.
    extents: HashMap<ArenaId, ArenaExtent>,
    /// Pages released back inside a still-live extent, reusable before the bump pointer moves.
    recycled: HashMap<ArenaId, Vec<PageId>>,
    /// The arena each branch is currently allocating novel pages from.
    current: HashMap<BranchId, ArenaId>,
    /// Pages logically freed but still visible to some live child. Slow-path reaping parks here.
    pending: Vec<PendingFree>,
}

/// Copy-on-write page store backed by per-branch arenas.
pub struct ArenaPageStore {
    pool: Arc<BufferPoolManager>,
    /// Concrete rather than `dyn BranchCatalog` on purpose: GC decisions must be able to read the
    /// record of a branch that is mid-reap or already reaped (`get_raw`), because that record's
    /// `live_children` array is still the authority over its parked pages. The trait's `get` is
    /// generation-guarded and correctly refuses those, so it cannot answer a GC question.
    catalog: Arc<LogBranchCatalog>,
    space: ArenaSpaceManager,
    state: Mutex<StoreState>,
    /// Pages handed out by `alloc_in_arena` and not yet returned to the free space map.
    /// Exit criteria 1 and 8 are both stated as page counts, so this is load-bearing.
    live_pages: AtomicU32,
    /// Extent pages currently reserved by some branch. Returns to baseline only if freed extents
    /// are genuinely recycled, which is the stronger claim exit criterion 8 actually wants.
    reserved_pages: AtomicU32,
}

impl ArenaPageStore {
    /// `base_page` must sit at or above the disk manager's high-water mark: the region belongs to
    /// this store alone.
    pub fn new(
        pool: Arc<BufferPoolManager>,
        catalog: Arc<LogBranchCatalog>,
        base_page: PageId,
    ) -> Result<Self, FerroError> {
        let high_water = pool.disk_manager.next_page_id.load(Ordering::SeqCst);
        if base_page < high_water {
            return Err(BranchError::Arena(format!(
                "arena region must start at or above the disk manager high-water mark {} (got {})",
                high_water, base_page
            ))
            .into());
        }
        Ok(ArenaPageStore {
            pool,
            catalog,
            space: ArenaSpaceManager {
                base_page,
                extent_pages: ARENA_EXTENT_PAGES,
                next_extent_start: AtomicU32::new(base_page),
                free_extent_starts: Mutex::new(Vec::new()),
                next_arena_id: AtomicU32::new(1), // arena 0 is the shared/trunk arena
            },
            state: Mutex::new(StoreState {
                extents: HashMap::new(),
                recycled: HashMap::new(),
                current: HashMap::new(),
                pending: Vec::new(),
            }),
            live_pages: AtomicU32::new(0),
            reserved_pages: AtomicU32::new(0),
        })
    }

    pub fn base_page(&self) -> PageId {
        self.space.base_page
    }

    /// Extent pages currently reserved by some branch.
    pub fn reserved_page_count(&self) -> u32 {
        self.reserved_pages.load(Ordering::SeqCst)
    }

    /// Entries in the pending-free log.
    pub fn pending_len(&self) -> usize {
        self.state.lock().unwrap().pending.len()
    }

    /// The branch that owns `arena`, or `None` if the extent has been freed.
    pub fn arena_owner(&self, arena: ArenaId) -> Option<BranchId> {
        self.state.lock().unwrap().extents.get(&arena).map(|e| e.owner)
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
            let mut frame = self.pool.frames[frame_i].write().unwrap();
            frame.page_id = None;
            frame.data = [0u8; PAGE_SIZE];
            frame.pin_counter = AtomicU16::new(0);
            frame.dirty_flag = AtomicBool::new(false);
        }
        let _ = self.pool.arc_cache.lock().unwrap().remove(page_id);
    }

    /// Return one page to the free space map. This is the only place `live_pages` goes down a
    /// page at a time.
    pub fn release_page(&self, page_id: PageId, arena: ArenaId) {
        self.evict(page_id);
        let newly_freed = {
            let mut st = self.state.lock().unwrap();
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

    /// Take the pending-free log for re-evaluation.
    pub fn take_pending(&self) -> Vec<PendingFree> {
        std::mem::take(&mut self.state.lock().unwrap().pending)
    }

    /// Put entries that are still pinned back on the pending-free log.
    pub fn put_pending(&self, entries: Vec<PendingFree>) {
        self.state.lock().unwrap().pending.extend(entries);
    }

    /// True iff `arena` is live and every page ever handed out from it has been released.
    pub fn extent_is_empty(&self, arena: ArenaId) -> bool {
        let st = self.state.lock().unwrap();
        let Some(ext) = st.extents.get(&arena) else { return false };
        let recycled = st.recycled.get(&arena).map(|v| v.len() as u32).unwrap_or(0);
        recycled >= ext.next_free
    }

    /// Every live arena and its owner. Used by the reaper to find extents whose owning branch is
    /// gone.
    pub fn live_arenas(&self) -> Vec<(ArenaId, BranchId)> {
        self.state.lock().unwrap().extents.iter().map(|(a, e)| (*a, e.owner)).collect()
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
            for page_id in self.allocated_pages(arena) {
                let birth = self.page_birth(page_id)?;
                if reclaimable(&rec.live_children, birth, free_epoch) {
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
        Ok(released)
    }

    fn page_birth(&self, page_id: PageId) -> Result<Epoch, FerroError> {
        Ok(self.read_page(page_id)?.header()?.birth_epoch)
    }
}

impl PageStore for ArenaPageStore {
    fn alloc_in_arena(
        &self,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        let page_id = {
            let mut st = self.state.lock().unwrap();
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
        let arena = self.arena_for(branch)?;
        let barrier = privacy_barrier(&rec);

        let handle = self.read_page(page_id)?;
        let header = handle.header()?;
        if header.is_private_to(arena, barrier) {
            // Nobody else can see it: mutate in place. This is what keeps a hot branch from
            // shadowing the same page on every single write.
            return Ok(CowPage { page_id, previous_page_id: page_id, copied: false, handle });
        }

        let source = handle.read().data;
        drop(handle);

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

        // Free the shadowed page **only if this branch owns the arena it came from**.
        //
        // A branch that shadows a page it inherited from an ancestor must leave the original
        // alone: the ancestor still points at it, and the ancestor is not in its own
        // `live_children` array, so the interval rule would eventually declare it reclaimable and
        // corrupt the ancestor. Freeing is the owner's business and nobody else's.
        if self.arena_owner(header.arena_id) == Some(branch) {
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

        // The owner may be mid-reap or already reaped, and its `live_children` array is still
        // the authority over this page, so read it raw rather than through the generation guard.
        let owner_children = match self.catalog.get_raw(owner.id) {
            Ok(rec) => Some(rec.live_children),
            Err(_) => None,
        };

        match owner_children {
            Some(children) if !reclaimable(&children, header.birth_epoch, free_epoch) => {
                self.state.lock().unwrap().pending.push(PendingFree {
                    page_id,
                    arena_id: arena,
                    birth_epoch: header.birth_epoch,
                    free_epoch,
                    owner,
                });
            }
            _ => self.release_page(page_id, arena),
        }
        Ok(())
    }

    fn alloc_arena(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        let (arena, start) = self.space.reserve()?;
        {
            let mut st = self.state.lock().unwrap();
            st.extents.insert(
                arena,
                ArenaExtent {
                    arena_id: arena,
                    owner: branch,
                    start_page: start,
                    page_count: self.space.extent_pages,
                    next_free: 0,
                },
            );
            st.recycled.insert(arena, Vec::new());
            st.current.insert(branch, arena);
        }
        self.reserved_pages.fetch_add(self.space.extent_pages, Ordering::SeqCst);

        // Keep the durable record truthful: the reaper frees exactly `record.arenas`.
        if let Ok(mut rec) = self.catalog.get_raw(branch.id) {
            if !rec.arenas.contains(&arena) {
                rec.arenas.push(arena);
                self.catalog.put(&rec)?;
            }
        }
        Ok(arena)
    }

    fn arena_for(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        {
            let st = self.state.lock().unwrap();
            if let Some(&arena) = st.current.get(&branch) {
                if let Some(ext) = st.extents.get(&arena) {
                    let has_recycled = st.recycled.get(&arena).map(|v| !v.is_empty()).unwrap_or(false);
                    if ext.remaining() > 0 || has_recycled {
                        return Ok(arena);
                    }
                }
            }
        }
        self.alloc_arena(branch)
    }

    fn free_arena(&self, arena: ArenaId) -> Result<u32, FerroError> {
        let (start, allocated) = {
            let st = self.state.lock().unwrap();
            let Some(ext) = st.extents.get(&arena) else { return Ok(0) };
            let recycled = st.recycled.get(&arena).map(|v| v.len() as u32).unwrap_or(0);
            (ext.start_page, ext.next_free.saturating_sub(recycled))
        };

        for i in 0..self.space.extent_pages {
            self.evict(start + i);
        }

        let mut st = self.state.lock().unwrap();
        let ext = st.extents.remove(&arena);
        st.recycled.remove(&arena);
        st.pending.retain(|p| p.arena_id != arena);
        if let Some(ext) = ext.as_ref() {
            let _ = ext;
            st.current.retain(|_, a| *a != arena);
        }
        drop(st);

        if ext.is_some() {
            self.space.give_back(start);
            self.reserved_pages.fetch_sub(self.space.extent_pages, Ordering::SeqCst);
            self.live_pages.fetch_sub(allocated, Ordering::SeqCst);
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
    use crate::storage::disk_manager::DiskManager;
    use std::fs::OpenOptions;
    use std::sync::atomic::AtomicU64;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// A throwaway store on a real file, deleted when the guard drops.
    pub struct Harness {
        pub catalog: Arc<LogBranchCatalog>,
        pub store: Arc<ArenaPageStore>,
        path: std::path::PathBuf,
    }

    impl Harness {
        pub fn new() -> Harness {
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
            let catalog = Arc::new(LogBranchCatalog::in_memory(1));
            let base = pool.disk_manager.next_page_id.load(Ordering::SeqCst);
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
    use super::harness::Harness;
    use super::*;
    use crate::branch::types::LeaseDeadline;

    /// Write one payload byte and restamp the checksum, the way any real writer must.
    pub(crate) fn stamp_payload_byte(h: &Harness, page: PageId, value: u8) {
        let handle = h.store.read_page(page).unwrap();
        let mut frame = handle.write();
        frame.data[PAGE_HEADER_SIZE] = value;
        stamp_checksum(&mut frame.data);
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
        for _ in 0..ARENA_EXTENT_PAGES {
            h.store.alloc_in_arena(first, PageType::Heap, e).unwrap();
        }
        assert!(h.store.alloc_in_arena(first, PageType::Heap, e).is_err(), "extent is full");
        let second = h.store.arena_for(b.branch_id).unwrap();
        assert_ne!(second, first);
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
        let start1 = h.store.allocated_pages(a1);
        assert!(start1.is_empty());
        h.store.alloc_in_arena(a1, PageType::Heap, h.catalog.next_epoch()).unwrap();
        assert_eq!(h.store.free_arena(a1).unwrap(), 1);
        assert_eq!(h.store.reserved_page_count(), 0);

        let b2 = h.catalog.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let a2 = h.store.alloc_arena(b2.branch_id).unwrap();
        assert_eq!(h.store.reserved_page_count(), ARENA_EXTENT_PAGES, "one extent, reused");
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

    #[test]
    fn privacy_barrier_is_the_later_of_own_fork_and_latest_child_fork() {
        let mut rec = BranchRecord::trunk(1, LeaseDeadline(0));
        rec.fork_epoch = Epoch(10);
        assert_eq!(privacy_barrier(&rec), Epoch(10));
        rec.add_live_child(Epoch(25));
        rec.add_live_child(Epoch(17));
        assert_eq!(privacy_barrier(&rec), Epoch(25));
    }
}
