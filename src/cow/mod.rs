//! Copy-on-write page store: shadow paging over fixed 4KB pages, LMDB-shaped.
//!
//! Design authority: DESIGN.md section 1.
//!
//! This module replaces the in-place mutation in `storage::index::BPlusTreeManager::insert`,
//! which is precisely why branching does not work today.
//!
//! Deliberate non-goals, each one a decision with a reason:
//! - **No content addressing.** It forces a global liveness question; you cannot free a chunk
//!   without a global statement about who else references it. This is why Dolt needs copying
//!   mark-and-sweep GC.
//! - **No immutable segments, no compaction.** Lookup cost would scale with segment count.
//! - **No reference counts.** One parent with 5000 children would put refcount 5001 on the most
//!   shared page in the store — btrfs's backref explosion.
//!
//! Liveness is answered instead by the epoch interval rule in `branch::record::reclaimable`.

pub mod btree;
pub mod node;
pub mod page_header;
pub mod store;

#[cfg(test)]
mod tests_isolation;

pub use btree::CowTree;
pub use page_header::{
    flags, stamp_checksum, verify_checksum, PageHeader, PageType, PAGE_HEADER_SIZE,
};
pub use store::CowStore;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLockReadGuard, RwLockWriteGuard};

use crate::branch::types::{ArenaId, BranchId, Epoch, PageId};
use crate::buffer::buffer_pool::{BufferPoolManager, Frame};
use crate::error::FerroError;

/// A pinned page. Unpins on drop, so no caller can leak a pin by taking an early return.
///
/// Obtain one with [`PageHandle::fetch`] or from [`PageStore::read_page`]. Dirtiness is tracked
/// here rather than passed to `unpin` by hand, because every historical pin leak in this codebase
/// came from a hand-written unpin on an error path.
pub struct PageHandle {
    pool: Arc<BufferPoolManager>,
    pub page_id: PageId,
    pub frame_idx: usize,
    dirty: AtomicBool,
}

impl PageHandle {
    /// Pin `page_id` and wrap it. The pin is released when the handle drops.
    pub fn fetch(pool: Arc<BufferPoolManager>, page_id: PageId) -> Result<Self, FerroError> {
        let frame_idx = pool.fetch_page(page_id)?;
        Ok(PageHandle { pool, page_id, frame_idx, dirty: AtomicBool::new(false) })
    }

    /// Wrap a frame that the caller has **already pinned**. Ownership of that pin transfers to
    /// the handle.
    pub fn from_pinned(pool: Arc<BufferPoolManager>, page_id: PageId, frame_idx: usize) -> Self {
        PageHandle { pool, page_id, frame_idx, dirty: AtomicBool::new(false) }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, Frame> {
        self.pool.frames[self.frame_idx].read().unwrap()
    }

    /// Take a write guard and mark the page dirty. Use [`PageHandle::read`] if you are not
    /// modifying anything.
    pub fn write(&self) -> RwLockWriteGuard<'_, Frame> {
        self.dirty.store(true, Ordering::Release);
        self.pool.frames[self.frame_idx].write().unwrap()
    }

    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Parse this page's self-describing header.
    pub fn header(&self) -> Result<PageHeader, FerroError> {
        let frame = self.read();
        PageHeader::read_from(&frame.data)
    }

    /// Recompute the checksum over the current contents. Call before the page can be flushed.
    pub fn stamp(&self) {
        let mut frame = self.write();
        stamp_checksum(&mut frame.data);
    }

    pub fn pool(&self) -> &Arc<BufferPoolManager> {
        &self.pool
    }
}

impl Drop for PageHandle {
    fn drop(&mut self) {
        self.pool.unpin_page(self.page_id, self.dirty.load(Ordering::Acquire));
    }
}

/// Result of [`PageStore::cow_page`].
///
/// `copied == false` means the page was already private to the writing branch at this epoch and
/// was handed back for in-place mutation. That is the fast path and it is what keeps a hot
/// branch from shadowing the same page on every write.
pub struct CowPage {
    /// The page the caller should now write to.
    pub page_id: PageId,
    /// The page it was shadowed from. Equals `page_id` when `copied` is false.
    pub previous_page_id: PageId,
    pub copied: bool,
    pub handle: PageHandle,
}

impl CowPage {
    pub fn parent_link_changed(&self) -> bool {
        self.copied
    }
}

/// The copy-on-write page store.
///
/// Every method is epoch-aware: `birth_epoch` on allocation and `free_epoch` on release are the
/// two endpoints of the interval that `branch::record::reclaimable` tests. A `free_page` that
/// forgets its epoch stamp silently breaks GC correctness, which is why the epoch is a required
/// parameter rather than something the store reads from a clock.
pub trait PageStore: Send + Sync {
    /// Allocate a fresh page inside `arena`, stamping its header with `birth_epoch` and
    /// `page_type`. Novel pages for a writing branch always come from that branch's private
    /// extents so shadow pages stay physically clustered.
    fn alloc_in_arena(
        &self,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<PageId, FerroError>;

    /// Pin and return a page. Must verify the page checksum and fail rather than return a torn
    /// page.
    fn read_page(&self, page_id: PageId) -> Result<PageHandle, FerroError>;

    /// Obtain a writable version of `page_id` for `branch` at `epoch`.
    ///
    /// If the page header reports `is_private_to(branch's arena, branch's fork epoch)` the same
    /// page is returned with `copied == false`. Otherwise a new page is allocated in the
    /// branch's arena, the contents are copied, the new header is stamped with `epoch`, and the
    /// old page is handed to `free_page` at the same epoch. The caller must relink the parent
    /// when `copied` is true.
    fn cow_page(
        &self,
        page_id: PageId,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<CowPage, FerroError>;

    /// Release a page as of `free_epoch`. The store records `[birth_epoch, free_epoch)` and only
    /// returns the page to the free space map once no live child forked inside that interval.
    fn free_page(&self, page_id: PageId, free_epoch: Epoch) -> Result<(), FerroError>;

    /// Give `branch` a fresh private extent (~`ARENA_EXTENT_PAGES` pages).
    fn alloc_arena(&self, branch: BranchId) -> Result<ArenaId, FerroError>;

    /// The arena `branch` is currently allocating from, creating one if it has none.
    fn arena_for(&self, branch: BranchId) -> Result<ArenaId, FerroError>;

    /// Free an entire extent at once. This is the reaper's fast path for a childless leaf, and
    /// the reason arenas exist at all. Returns the number of pages reclaimed.
    fn free_arena(&self, arena: ArenaId) -> Result<u32, FerroError>;

    /// Total pages currently allocated in the store. Exit criteria 1 and 8 are both stated as
    /// page counts, so this is load-bearing rather than diagnostic.
    fn live_page_count(&self) -> Result<u32, FerroError>;

    /// Flush everything dirty. Shadow paging's commit point is the root pointer swap in
    /// `BranchCatalog::set_root`; this must have completed before that swap.
    fn flush(&self) -> Result<(), FerroError>;
}

/// Per-branch in-memory write buffer, probed before B+tree descent.
///
/// A branch that dies before flushing allocates **zero pages** — the common case for an
/// abandoned agent task, and the reason exit criterion 8 is cheap.
pub struct WriteBuffer {
    pub branch: BranchId,
    pub capacity_bytes: usize,
    pub used_bytes: usize,
    /// Buffered entries, keyed by the serialized index key.
    pub entries: Vec<(Vec<u8>, WriteBufferEntry)>,
}

/// What a buffered write does when it is eventually flushed into the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteBufferEntry {
    Put(Vec<u8>),
    Delete,
}

/// Default per-branch write buffer size (~1MB).
pub const WRITE_BUFFER_BYTES: usize = 1 << 20;

impl WriteBuffer {
    pub fn new(branch: BranchId) -> Self {
        WriteBuffer {
            branch,
            capacity_bytes: WRITE_BUFFER_BYTES,
            used_bytes: 0,
            entries: Vec::new(),
        }
    }

    pub fn is_full(&self) -> bool {
        self.used_bytes >= self.capacity_bytes
    }

    /// Probe before descending. `None` means "not buffered, go to the tree" — it does **not**
    /// mean the key is absent.
    pub fn probe(&self, key: &[u8]) -> Option<&WriteBufferEntry> {
        self.entries
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .ok()
            .map(|i| &self.entries[i].1)
    }

    pub fn put(&mut self, key: Vec<u8>, entry: WriteBufferEntry) {
        let added = key.len()
            + match &entry {
                WriteBufferEntry::Put(v) => v.len(),
                WriteBufferEntry::Delete => 0,
            };
        match self.entries.binary_search_by(|(k, _)| k.as_slice().cmp(&key)) {
            Ok(i) => {
                let old = &self.entries[i];
                let removed = old.0.len()
                    + match &old.1 {
                        WriteBufferEntry::Put(v) => v.len(),
                        WriteBufferEntry::Delete => 0,
                    };
                self.used_bytes = self.used_bytes + added - removed;
                self.entries[i].1 = entry;
            }
            Err(i) => {
                self.used_bytes += added;
                self.entries.insert(i, (key, entry));
            }
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.used_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_buffer_probe_distinguishes_delete_from_absent() {
        let mut wb = WriteBuffer::new(BranchId::new(1, 0));
        wb.put(b"a".to_vec(), WriteBufferEntry::Put(b"1".to_vec()));
        wb.put(b"b".to_vec(), WriteBufferEntry::Delete);
        assert_eq!(wb.probe(b"a"), Some(&WriteBufferEntry::Put(b"1".to_vec())));
        assert_eq!(wb.probe(b"b"), Some(&WriteBufferEntry::Delete));
        assert_eq!(wb.probe(b"c"), None);
    }

    #[test]
    fn overwriting_a_key_does_not_double_count_bytes() {
        let mut wb = WriteBuffer::new(BranchId::new(1, 0));
        wb.put(b"k".to_vec(), WriteBufferEntry::Put(vec![0u8; 10]));
        let after_first = wb.used_bytes;
        wb.put(b"k".to_vec(), WriteBufferEntry::Put(vec![0u8; 10]));
        assert_eq!(wb.used_bytes, after_first);
        assert_eq!(wb.entries.len(), 1);
    }
}
