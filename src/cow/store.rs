//! [`CowStore`] — the concrete copy-on-write page store.
//!
//! Design authority: DESIGN.md section 1.
//!
//! Three mechanisms, and every one of them exists to avoid a named failure mode:
//!
//! 1. **Per-branch arenas.** A writing branch allocates novel pages from private ~1MB extents.
//!    Shadow pages stay physically clustered (so a scan does not degenerate as branches fan out)
//!    *and* reaping a childless leaf becomes [`CowStore::free_arena`] — one extent-level free with
//!    no per-page sharing analysis.
//! 2. **Self-describing headers.** `birth_epoch` and `arena_id` live in the page. A side table
//!    mapping page -> birth would itself be COW'd metadata needing its own reclamation, which is
//!    the recursive trap btrfs fell into.
//! 3. **Epoch interval liveness.** No refcounts and no content addressing. A page is released iff
//!    [`crate::branch::reclaimable`] says no live child of its owning branch forked inside
//!    `[birth, free)`.
//!
//! ## Who may free what
//!
//! A branch may only free pages **it owns**. When a child shadows one of its parent's pages the
//! parent's root still points at the original, so the child must not touch it — the child simply
//! stops referencing it. The interval rule then protects that page when the *parent* eventually
//! overwrites it. Getting this backwards would let a child delete data out from under its parent.
//!
//! ## Liveness input
//!
//! Deciding whether a page is still visible needs each branch's fork epoch and its live children's
//! fork epochs. The [`crate::cow::PageStore`] trait passes neither, so the store keeps its own
//! mirror, maintained through [`CowStore::register_branch`] / [`CowStore::forget_branch`]. The
//! branch engine must mirror every fork and every reap into it. An unregistered branch is a hard
//! error, never a guess: guessing here silently frees live data.
//!
//! ## Space ownership
//!
//! The store reserves contiguous extents through `DiskManager::allocate`, which marks the on-disk
//! bitmap, so it coexists with the rest of ferrodb's allocator. It does **not** return extent
//! pages to the disk manager on free — an extent is recycled inside the store, which is what makes
//! `free_arena` O(1).
//!
//! **Known gap:** the arena table and the pending-free log are in memory only. Page *contents* and
//! their headers are durable, but a restart loses the arena bookkeeping. Persisting it is a
//! `PageType::Meta` / `PageType::FreeLog` chain that nothing on the demo path needs yet.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::branch::record::{reclaimable, ArenaExtent, PendingFree};
use crate::branch::types::{ArenaId, BranchError, BranchId, Epoch, PageId, ARENA_EXTENT_PAGES};
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::cow::page_header::{stamp_checksum, verify_checksum, PageHeader, PageType};
use crate::cow::{CowPage, PageHandle, PageStore};
use crate::error::FerroError;
use crate::storage::disk_manager::PAGE_SIZE;

/// What the store knows about one branch. A mirror of the parts of `BranchRecord` that liveness
/// depends on — deliberately not a second source of truth for anything else.
#[derive(Debug, Clone, Default)]
struct BranchMeta {
    parent: Option<BranchId>,
    fork_epoch: Epoch,
    /// Fork epochs of live children, sorted ascending. The reclamation rule is a range-emptiness
    /// query over this array.
    live_children: Vec<Epoch>,
    arenas: Vec<ArenaId>,
    current_arena: Option<ArenaId>,
}

struct ExtentState {
    extent: ArenaExtent,
    /// Pages released inside a still-live extent, available for reuse by the same branch.
    free_pages: Vec<PageId>,
}

impl ExtentState {
    fn live_pages(&self) -> u32 {
        self.extent.next_free.saturating_sub(self.free_pages.len() as u32)
    }
}

struct Inner {
    extents: HashMap<u32, ExtentState>,
    /// Whole extents released by `free_arena`, ready to be handed to another branch.
    free_extents: Vec<(PageId, u32)>,
    branches: HashMap<BranchId, BranchMeta>,
    pending: Vec<PendingFree>,
    next_arena: u32,
}

impl Inner {
    fn meta(&self, branch: BranchId) -> Result<&BranchMeta, FerroError> {
        self.branches.get(&branch).ok_or_else(|| {
            FerroError::Cow(format!(
                "branch {} is not registered with the page store; the branch engine must call \
                 register_branch on every fork",
                branch
            ))
        })
    }

    fn owner_of(&self, arena: ArenaId) -> Option<BranchId> {
        self.extents.get(&arena.0).map(|e| e.extent.owner)
    }

    /// Fork epochs of `owner`'s live children. A branch the store has already forgotten (reaped)
    /// pins nothing.
    fn live_children_of(&self, owner: BranchId) -> &[Epoch] {
        self.branches.get(&owner).map(|m| m.live_children.as_slice()).unwrap_or(&[])
    }

    fn take_page(&mut self, arena: ArenaId) -> Result<PageId, FerroError> {
        let st = self.extents.get_mut(&arena.0).ok_or_else(|| {
            FerroError::Branch(BranchError::Arena(format!("no such arena {}", arena)).to_string())
        })?;
        if let Some(p) = st.free_pages.pop() {
            return Ok(p);
        }
        if st.extent.next_free >= st.extent.page_count {
            return Err(FerroError::Branch(
                BranchError::Arena(format!("arena {} is exhausted", arena)).to_string(),
            ));
        }
        let p = st.extent.start_page + st.extent.next_free;
        st.extent.next_free += 1;
        Ok(p)
    }

    fn release_page(&mut self, arena: ArenaId, page_id: PageId) -> Result<(), FerroError> {
        let st = self.extents.get_mut(&arena.0).ok_or_else(|| {
            FerroError::Branch(
                BranchError::Arena(format!("release into unknown arena {}", arena)).to_string(),
            )
        })?;
        if !st.extent.contains(page_id) {
            return Err(FerroError::Branch(
                BranchError::Arena(format!("page {} is not inside arena {}", page_id, arena))
                    .to_string(),
            ));
        }
        if st.free_pages.contains(&page_id) {
            return Err(FerroError::Branch(
                BranchError::Arena(format!("double free of page {}", page_id)).to_string(),
            ));
        }
        st.free_pages.push(page_id);
        Ok(())
    }

    fn has_capacity(&self, arena: ArenaId) -> bool {
        self.extents
            .get(&arena.0)
            .map(|st| !st.free_pages.is_empty() || st.extent.next_free < st.extent.page_count)
            .unwrap_or(false)
    }

    fn live_page_count(&self) -> u32 {
        self.extents.values().map(|e| e.live_pages()).sum()
    }
}

/// The copy-on-write page store.
pub struct CowStore {
    pool: Arc<BufferPoolManager>,
    inner: Mutex<Inner>,
    extent_pages: u32,
}

impl CowStore {
    /// Build a store over `pool`. The trunk is registered automatically at `Epoch::ZERO`.
    pub fn new(pool: Arc<BufferPoolManager>) -> Self {
        Self::with_extent_pages(pool, ARENA_EXTENT_PAGES)
    }

    /// Same, with a custom extent size. Tests use a small value to exercise extent rollover
    /// without writing a megabyte per arena.
    pub fn with_extent_pages(pool: Arc<BufferPoolManager>, extent_pages: u32) -> Self {
        let mut branches = HashMap::new();
        branches.insert(BranchId::TRUNK, BranchMeta::default());
        CowStore {
            pool,
            inner: Mutex::new(Inner {
                extents: HashMap::new(),
                free_extents: Vec::new(),
                branches,
                pending: Vec::new(),
                next_arena: 1, // ArenaId(0) is the shared sentinel and is never an extent
            }),
            extent_pages: extent_pages.max(1),
        }
    }

    pub fn pool(&self) -> &Arc<BufferPoolManager> {
        &self.pool
    }

    /// Mirror a fork into the store. `parent` must already be registered; `fork_epoch` is appended
    /// to the parent's sorted live-children array, which is the only thing that keeps the parent's
    /// pages from being reclaimed out from under the child.
    pub fn register_branch(
        &self,
        branch: BranchId,
        parent: Option<BranchId>,
        fork_epoch: Epoch,
    ) -> Result<(), FerroError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.branches.contains_key(&branch) {
            return Err(FerroError::Cow(format!("branch {} is already registered", branch)));
        }
        if let Some(p) = parent {
            let pm = inner.branches.get_mut(&p).ok_or_else(|| {
                FerroError::Cow(format!("parent branch {} is not registered", p))
            })?;
            let at = pm.live_children.partition_point(|e| *e < fork_epoch);
            pm.live_children.insert(at, fork_epoch);
        }
        inner.branches.insert(
            branch,
            BranchMeta { parent, fork_epoch, ..BranchMeta::default() },
        );
        Ok(())
    }

    /// Mirror a reap. Removes `branch` from its parent's live-children array — which is what makes
    /// previously-pinned pending frees become reclaimable — and frees the branch's own arenas
    /// wholesale. Returns the number of pages returned to the free space map.
    ///
    /// This is the reaper's fast path expressed at the page-store level.
    pub fn forget_branch(&self, branch: BranchId) -> Result<u32, FerroError> {
        if branch.is_trunk() {
            return Err(FerroError::Cow("the trunk cannot be forgotten".into()));
        }
        let (arenas, parent, fork_epoch) = {
            let inner = self.inner.lock().unwrap();
            let m = inner.meta(branch)?;
            (m.arenas.clone(), m.parent, m.fork_epoch)
        };
        let mut reclaimed = 0;
        for a in arenas {
            reclaimed += self.free_arena(a)?;
        }
        {
            let mut inner = self.inner.lock().unwrap();
            inner.branches.remove(&branch);
            if let Some(p) = parent {
                if let Some(pm) = inner.branches.get_mut(&p) {
                    let at = pm.live_children.partition_point(|e| *e < fork_epoch);
                    if at < pm.live_children.len() && pm.live_children[at] == fork_epoch {
                        pm.live_children.remove(at);
                    }
                }
            }
        }
        reclaimed += self.drain_pending_free()?;
        Ok(reclaimed)
    }

    /// Re-examine the pending-free log against the current live-children arrays and release
    /// everything that has since become reclaimable. Returns pages released.
    pub fn drain_pending_free(&self) -> Result<u32, FerroError> {
        let mut inner = self.inner.lock().unwrap();
        let pending = std::mem::take(&mut inner.pending);
        let mut still_pending = Vec::with_capacity(pending.len());
        let mut released = 0u32;
        for pf in pending {
            let clear = reclaimable(inner.live_children_of(pf.owner), pf.birth_epoch, pf.free_epoch);
            if !clear {
                still_pending.push(pf);
            } else if inner.extents.contains_key(&pf.arena_id.0) {
                inner.release_page(pf.arena_id, pf.page_id)?;
                released += 1;
            }
            // else: the whole extent already went back to the free pool, nothing left to release
        }
        inner.pending = still_pending;
        Ok(released)
    }

    pub fn pending_free_len(&self) -> usize {
        self.inner.lock().unwrap().pending.len()
    }

    pub fn arenas_of(&self, branch: BranchId) -> Result<Vec<ArenaId>, FerroError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.meta(branch)?.arenas.clone())
    }

    pub fn owner_of_arena(&self, arena: ArenaId) -> Option<BranchId> {
        self.inner.lock().unwrap().owner_of(arena)
    }

    /// Fork epochs of `branch`'s live children, sorted ascending.
    pub fn live_children_of(&self, branch: BranchId) -> Result<Vec<Epoch>, FerroError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.meta(branch)?.live_children.clone())
    }

    /// True iff `branch` may mutate `page_id` in place at `epoch`: it owns the page, the page was
    /// born at or after its own fork, and no live child of the branch forked while the page was
    /// alive. Anything else must be shadowed.
    pub fn is_private(
        &self,
        page_id: PageId,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<bool, FerroError> {
        let hdr = self.header_of(page_id)?;
        let inner = self.inner.lock().unwrap();
        Ok(Self::privacy(&inner, &hdr, branch, epoch)?)
    }

    fn privacy(
        inner: &Inner,
        hdr: &PageHeader,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<bool, FerroError> {
        let meta = inner.meta(branch)?;
        if inner.owner_of(hdr.arena_id) != Some(branch) {
            return Ok(false);
        }
        if hdr.birth_epoch < meta.fork_epoch {
            return Ok(false);
        }
        // A child forked at epoch e sees every page live at e. Mutating in place is exactly
        // freeing the old contents at `epoch`, so the same interval rule applies.
        Ok(reclaimable(&meta.live_children, hdr.birth_epoch, epoch))
    }

    fn header_of(&self, page_id: PageId) -> Result<PageHeader, FerroError> {
        let h = self.read_page(page_id)?;
        let f = h.read();
        PageHeader::read_from(&f.data)
    }

    /// Reserve a contiguous run of `count` pages.
    ///
    /// `DiskManager::allocate` hands out the lowest free bitmap bit, so successive calls are
    /// contiguous as long as it does not have to chain a new bitmap page mid-run. When it does,
    /// the run is restarted from the break rather than silently accepting a non-contiguous
    /// "extent" — `ArenaExtent::contains` is a range test and a hole in it would misroute frees.
    fn reserve_extent(&self, count: u32) -> Result<PageId, FerroError> {
        let dm = &self.pool.disk_manager;
        let mut start = dm.allocate()?;
        let mut have = 1u32;
        let mut attempts = 0;
        while have < count {
            let next = dm.allocate()?;
            if next == start + have {
                have += 1;
            } else {
                attempts += 1;
                if attempts > 4 {
                    return Err(FerroError::Cow(format!(
                        "could not reserve a contiguous {}-page extent",
                        count
                    )));
                }
                start = next;
                have = 1;
            }
        }
        // Extend the file so every page of the extent is readable; the interior is a hole and
        // reads back as zeros.
        self.pool.disk_manager.write(start + count - 1, &[0u8; PAGE_SIZE])?;
        Ok(start)
    }

    /// Overwrite a page with a fresh header and a zeroed payload, in the buffer pool.
    ///
    /// Doing it through the pool rather than straight to disk matters: a recycled page may still
    /// be cached from its previous life, and handing that stale frame back would resurrect data
    /// the store has already freed.
    fn format_page(
        &self,
        page_id: PageId,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<(), FerroError> {
        let h = PageHandle::fetch(self.pool.clone(), page_id)?;
        let mut f = h.write();
        f.data = [0u8; PAGE_SIZE];
        PageHeader::new(birth_epoch, arena, page_type).write_to(&mut f.data);
        stamp_checksum(&mut f.data);
        Ok(())
    }
}

impl PageStore for CowStore {
    fn alloc_in_arena(
        &self,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        let page_id = {
            let mut inner = self.inner.lock().unwrap();
            inner.take_page(arena)?
        };
        self.format_page(page_id, arena, page_type, birth_epoch)?;
        Ok(page_id)
    }

    fn read_page(&self, page_id: PageId) -> Result<PageHandle, FerroError> {
        let h = PageHandle::fetch(self.pool.clone(), page_id)?;
        {
            let f = h.read();
            if !verify_checksum(&f.data) {
                return Err(FerroError::Cow(format!(
                    "page {} failed its checksum; refusing to return a torn page",
                    page_id
                )));
            }
        }
        Ok(h)
    }

    fn cow_page(
        &self,
        page_id: PageId,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<CowPage, FerroError> {
        let handle = self.read_page(page_id)?;
        let hdr = {
            let f = handle.read();
            PageHeader::read_from(&f.data)?
        };

        let (private, owned_by_writer) = {
            let inner = self.inner.lock().unwrap();
            (
                Self::privacy(&inner, &hdr, branch, epoch)?,
                inner.owner_of(hdr.arena_id) == Some(branch),
            )
        };

        if private {
            return Ok(CowPage {
                page_id,
                previous_page_id: page_id,
                copied: false,
                handle,
            });
        }

        let arena = self.arena_for(branch)?;
        let new_id = self.alloc_in_arena(arena, hdr.page_type, epoch)?;
        let new_handle = PageHandle::fetch(self.pool.clone(), new_id)?;
        {
            let src = handle.read();
            let mut dst = new_handle.write();
            dst.data = src.data;
            PageHeader::new(epoch, arena, hdr.page_type).write_to(&mut dst.data);
            stamp_checksum(&mut dst.data);
        }
        drop(handle);

        // A branch may only free pages it owns. If this page belongs to an ancestor, that
        // ancestor's root still points at it; the child simply stops referencing it.
        if owned_by_writer {
            self.free_page(page_id, epoch)?;
        }

        Ok(CowPage {
            page_id: new_id,
            previous_page_id: page_id,
            copied: true,
            handle: new_handle,
        })
    }

    fn free_page(&self, page_id: PageId, free_epoch: Epoch) -> Result<(), FerroError> {
        let hdr = self.header_of(page_id)?;
        let mut inner = self.inner.lock().unwrap();
        let owner = inner.owner_of(hdr.arena_id).ok_or_else(|| {
            FerroError::Cow(format!(
                "page {} claims arena {} which the store does not know",
                page_id, hdr.arena_id
            ))
        })?;
        if reclaimable(inner.live_children_of(owner), hdr.birth_epoch, free_epoch) {
            inner.release_page(hdr.arena_id, page_id)?;
        } else {
            inner.pending.push(PendingFree {
                page_id,
                arena_id: hdr.arena_id,
                birth_epoch: hdr.birth_epoch,
                free_epoch,
                owner,
            });
        }
        Ok(())
    }

    fn alloc_arena(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        // Reserve outside the lock: reserving touches the disk manager.
        let recycled = {
            let mut inner = self.inner.lock().unwrap();
            inner.meta(branch)?; // reject an unregistered branch before taking any space
            inner.free_extents.pop()
        };
        let (start, count) = match recycled {
            Some(e) => e,
            None => (self.reserve_extent(self.extent_pages)?, self.extent_pages),
        };
        let mut inner = self.inner.lock().unwrap();
        let id = ArenaId(inner.next_arena);
        inner.next_arena += 1;
        inner.extents.insert(
            id.0,
            ExtentState {
                extent: ArenaExtent {
                    arena_id: id,
                    owner: branch,
                    start_page: start,
                    page_count: count,
                    next_free: 0,
                },
                free_pages: Vec::new(),
            },
        );
        let m = inner.branches.get_mut(&branch).ok_or_else(|| {
            FerroError::Cow(format!("branch {} was forgotten while allocating its arena", branch))
        })?;
        m.arenas.push(id);
        m.current_arena = Some(id);
        Ok(id)
    }

    fn arena_for(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        {
            let inner = self.inner.lock().unwrap();
            let m = inner.meta(branch)?;
            if let Some(a) = m.current_arena {
                if inner.has_capacity(a) {
                    return Ok(a);
                }
            }
        }
        self.alloc_arena(branch)
    }

    fn free_arena(&self, arena: ArenaId) -> Result<u32, FerroError> {
        let mut inner = self.inner.lock().unwrap();
        let st = match inner.extents.remove(&arena.0) {
            Some(st) => st,
            None => return Ok(0),
        };
        let reclaimed = st.live_pages();
        inner.free_extents.push((st.extent.start_page, st.extent.page_count));
        if let Some(m) = inner.branches.get_mut(&st.extent.owner) {
            m.arenas.retain(|a| *a != arena);
            if m.current_arena == Some(arena) {
                m.current_arena = None;
            }
        }
        inner.pending.retain(|pf| pf.arena_id != arena);
        Ok(reclaimed)
    }

    fn live_page_count(&self) -> Result<u32, FerroError> {
        Ok(self.inner.lock().unwrap().live_page_count())
    }

    fn flush(&self) -> Result<(), FerroError> {
        self.pool.flush_all()
    }
}
