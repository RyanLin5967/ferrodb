//! The B+tree, and its latch protocol.
//!
//! # D23: this tree used to have no concurrency control at all
//!
//! `insert`, `delete` and `search` all take `&self`, the root sits in an `AtomicU32`, and the type
//! is `Sync` — so two threads could always call `insert` on one tree, and nothing stopped them.
//! Measured before this change (`bench/d23_btree_concurrency_before.txt`,
//! `tests/integration_btree_concurrency.rs`), on one shared tree with 1000 pre-loaded keys:
//!
//! ```text
//!   threads=1   0/3 rounds dirty                                    <- control
//!   threads=8   3/3 dirty   424-472 of 1200 inserts LOST
//!   threads=16  3/3 dirty   857-979 of 2400 inserts LOST
//!   threads=32  3/3 dirty  1200-1346 of 4800 inserts LOST, 17-25 reads of a
//!                          pre-existing key returned "not found"
//! ```
//!
//! Three distinct defects, all of them the same missing latch:
//!
//! 1. **Lost insert.** `insert` took the leaf's frame lock, deserialized, *dropped the lock*,
//!    mutated the in-memory copy, then re-took the lock to write it back. Two threads inserting
//!    different keys into one leaf both started from the same bytes and the second write-back
//!    erased the first. `insert_into_parent` had the identical read-drop-modify-retake shape.
//! 2. **Phantom miss.** `find_leaf` released each node before following its child pointer, so a
//!    reader could be handed a child id, have that child split away underneath it, and descend
//!    into a subtree that no longer held the key — reporting "not found" for a key that was never
//!    removed.
//! 3. **Broken leaf chain.** A split wrote the leaf (publishing `next -> new page`) before writing
//!    the new page itself, so a scanner could follow `next` into an unwritten page; and concurrent
//!    splits raced on the sibling `prev`/`next` fixups. A full range scan returned 1649-3287
//!    entries where 2200-5800 existed.
//!
//! # The protocol
//!
//! **Latch crabbing (Bayer & Schkolnick 1977)**, in its optimistic-descent-with-restart form. Not
//! B-link (Lehman & Yao 1981): a B-link tree needs a right-link and a high key in *every* node,
//! including internal ones, and `BPlusTreeInternalPage` has neither. Adding them is an on-disk
//! format change — new serialization, a migration for every existing database, and matching work
//! in `wal::recovery`. Crabbing is a pure locking discipline and costs no format change, which is
//! why it is the right answer for this tree rather than merely the first one.
//!
//! Latches are `src/storage/page_latch.rs`, deliberately NOT the buffer pool's frame `RwLock`s:
//! `fetch_page` holds `arc_cache` across frame locks, so holding a frame lock across a fetch
//! inverts the order and deadlocks. Page latches sit above the pool.
//!
//! - **Readers** (`search`, `range_scan`, `leftmost_leaf`) crab down: latch the child, *then*
//!   release the parent. At most two latches are held at once.
//! - **Writers take the fast path first.** Read-crab to the leaf, keep the **parent's read
//!   latch**, and write-latch the leaf. Holding the parent in read mode is what makes this safe:
//!   a split has to write-latch the parent, so the leaf cannot be split underneath. If the insert
//!   fits, one leaf is written and no ancestor is touched.
//! - **A writer that would split restarts pessimistically**, having written nothing, and takes
//!   write latches on the whole root-to-leaf path before doing anything. No safe-node size
//!   heuristic is used anywhere: guessing whether a node has room for a separator key it has not
//!   seen yet would be a guard with a silent failure mode, and a restart is exact.
//! - **The root is re-checked after latching.** A root split replaces `root_page_id` and leaves
//!   the old root holding only half its keys, so a descent that latched the old root before the
//!   split must start again. Every descent loop re-reads `root_page_id` under its first latch.
//!
//! Acquisition order is **down** the tree and **rightward** along the leaf chain, never upward or
//! leftward, so the wait-for graph is ordered by depth and then by key and cannot cycle.
//!
//! # What this does NOT make safe, stated rather than implied
//!
//! Two `BPlusTreeManager` values opened on the same root each keep their own `root_page_id`, so a
//! root split performed through one is invisible to the other until the catalog is re-read.
//! `planner::plan::open_table` builds a fresh manager per statement, and today the pgwire server
//! serialises whole statements behind `ServerContext::catalog()`, so no two live managers on one
//! tree overlap. Page latches live on the shared `BufferPoolManager` so they would still exclude
//! correctly, but the stale `root_page_id` is a separate defect and this change does not fix it.

use std::{marker::PhantomData, sync::{Arc, atomic::AtomicU32}};

use crate::{buffer::buffer_pool::BufferPoolManager, error::FerroError, storage::{index_page::{BPlusTreeInternalPage, BPlusTreeLeafPage, BTreeSerialize}, range_scan::RangeScanner}};
use crate::storage::index_page::BPlusTreePage;
use crate::storage::disk_manager::PAGE_SIZE;
use crate::storage::page_latch::{PageLatches, PageReadGuard, PageWriteGuard};
use std::sync::atomic::Ordering;
use std::ops::Bound;

pub struct BPlusTreeManager<K, V> {
    pub root_page_id: AtomicU32,
    pub buffer_pool: Arc<BufferPoolManager>,
    pub marker: PhantomData<(K, V)>
}

/// The latches a writer holds while it modifies one leaf on the fast path.
///
/// Both fields exist only to be dropped at the right moment: `parent` is what stops the leaf being
/// split while `leaf` is what stops it being written. Named rather than `_`-prefixed tuple members
/// because the *parent* one is the non-obvious half of the protocol.
struct LeafWriteLatch<'a> {
    #[allow(dead_code)]
    parent: Option<PageReadGuard<'a>>,
    #[allow(dead_code)]
    leaf: PageWriteGuard<'a>,
}

impl<K: Ord + Clone + BTreeSerialize,V: Clone + BTreeSerialize + Ord> BPlusTreeManager<K,V> {

    pub fn new(root_page_id: AtomicU32, buffer_pool: Arc<BufferPoolManager>) -> Self{
        BPlusTreeManager {root_page_id, buffer_pool, marker: PhantomData}
    }

    // allocates empty root leaf
    pub fn create(buffer_pool: Arc<BufferPoolManager>) -> Result<Self, FerroError> {
        let root_page_id = buffer_pool.new_page()?;
        let root_node = BPlusTreeLeafPage::<K, V>::new(root_page_id);
        let tree = Self {root_page_id: AtomicU32::new(root_page_id), buffer_pool, marker: PhantomData};
        let guard = tree.latches().write(root_page_id);
        tree.write_page(root_page_id, root_node.serialize()?)?;
        drop(guard);
        Ok(tree)
    }

    pub fn open(root_page_id: u32, buffer_pool: Arc<BufferPoolManager>) -> Self{
        Self { root_page_id: AtomicU32::new(root_page_id), buffer_pool, marker: PhantomData }
    }

    // ---------------------------------------------------------------------------------------
    // PUBLIC OPERATIONS
    // ---------------------------------------------------------------------------------------

    /// Point lookup. Read-crabs to the leaf and copies it out under the leaf's read latch.
    pub fn search(&self, key: &K) -> Result<Option<V>, FerroError> {
        let (_page_id, leaf) = self.read_leaf_for(key)?;
        match leaf.get(key) {
            Ok(Some(v)) => Ok(Some(v.clone())),
            Ok(None) => Ok(None),
            Err(_) => Err(FerroError::KeyNotFound)
        }
    }

    /// Remove one entry.
    ///
    /// Takes the same latches as a non-splitting insert, and for the same reason: a removal never
    /// splits, and `handle_underflow` is unreachable (see its doc comment), so no ancestor is ever
    /// touched and the parent's read latch plus the leaf's write latch is the whole requirement.
    pub fn delete(&self, key: &K) -> Result<(), FerroError> {
        let (page_id, _latch) = self.latch_leaf_for_write(key)?;
        let mut leaf = self.read_leaf_raw(page_id)?;
        leaf.remove_entry(key)?;
        self.write_page(page_id, leaf.serialize()?)
    }

    /// Insert one entry.
    ///
    /// Fast path first: if the leaf has room, only that leaf is latched for writing and only that
    /// leaf is written. If it would split, nothing has been written yet and the whole insert is
    /// retried with write latches on the entire root-to-leaf path.
    pub fn insert(&self, key: K, value: V) -> Result<(), FerroError> {
        if self.try_insert_without_split(&key, &value)? {
            return Ok(());
        }
        self.insert_splitting(key, value)
    }

    // uses sibling pointers to traverse leaves and does range scan from start -> end (inclusive)
    // should later return an iterator to load results lazily cuz memory could overflow
    //
    // Each leaf is copied out under its own read latch (here and in `RangeScanner::load_leaf`), so
    // no page is ever read while a writer is mid-update. The scan does NOT hold a latch between
    // `next()` calls: it is not a repeatable read and never claimed to be, but it cannot observe a
    // half-written page or follow `next` into a page that has not been written yet.
    pub fn range_scan(&self, lower: Bound<K>, upper: Bound<K>) -> Result<RangeScanner<K, V>, FerroError> {
        let leaf = match &lower {
            Bound::Included(k) | Bound::Excluded(k) => self.read_leaf_for(k)?.1,
            Bound::Unbounded => self.leftmost_leaf()?,
        };

        let idx = match &lower {
            Bound::Included(k) => leaf.binary_search(k), //first >= k
            Bound::Excluded(k) => leaf.upper_bound(k), //first > k
            Bound::Unbounded => 0
        };
        Ok(RangeScanner {buffer_pool: self.buffer_pool.clone(), leaf: Some(leaf), idx, upper})
    }

    // ---------------------------------------------------------------------------------------
    // LATCHING
    // ---------------------------------------------------------------------------------------

    fn latches(&self) -> &PageLatches {
        &self.buffer_pool.page_latches
    }

    /// Read-crab from the root to the leaf that would hold `key`, and copy that leaf out under its
    /// read latch.
    ///
    /// The crabbing step is `latch(child)` **before** `drop(parent)`. That is what makes the
    /// answer sound: a split of the child has to write-latch the parent, so while this thread
    /// holds the parent in read mode the child cannot be split away from under the pointer it
    /// just read.
    fn read_leaf_for(&self, key: &K) -> Result<(u32, BPlusTreeLeafPage<K, V>), FerroError> {
        loop {
            let root = self.root_page_id.load(Ordering::Acquire);
            let mut guard = self.latches().read(root);
            // A root split leaves the old root holding half its keys. Re-read under the latch.
            if self.root_page_id.load(Ordering::Acquire) != root {
                drop(guard);
                continue;
            }
            let mut curr = root;
            loop {
                match self.read_node_raw(curr)? {
                    BPlusTreePage::Leaf(leaf) => return Ok((curr, leaf)),
                    BPlusTreePage::Internal(n) => {
                        let child = n.find_child(key);
                        let child_guard = self.latches().read(child);
                        drop(guard);
                        guard = child_guard;
                        curr = child;
                    }
                }
            }
        }
    }

    /// Read-crab to the leaf that would hold `key`, then take that leaf's **write** latch while
    /// still holding its **parent's read** latch.
    ///
    /// The parent latch is the load-bearing half. Between releasing the leaf's read latch and
    /// taking its write latch, another writer may insert into the leaf — that is fine, it will
    /// finish and the contents are re-read afterwards. What must not happen is the leaf being
    /// *split*, because then `key` may no longer belong in it; a split write-latches the parent,
    /// which this thread holds in read mode.
    ///
    /// When the leaf IS the root there is no parent, and the only thing that can reshape it is a
    /// root split — which changes `root_page_id`, so that is re-checked instead.
    fn latch_leaf_for_write(&self, key: &K) -> Result<(u32, LeafWriteLatch<'_>), FerroError> {
        loop {
            let root = self.root_page_id.load(Ordering::Acquire);
            let mut parent: Option<PageReadGuard<'_>> = None;
            let mut guard = Some(self.latches().read(root));
            if self.root_page_id.load(Ordering::Acquire) != root {
                continue;
            }
            let mut curr = root;
            let leaf_id = loop {
                match self.read_node_raw(curr)? {
                    BPlusTreePage::Leaf(_) => break curr,
                    BPlusTreePage::Internal(n) => {
                        let child = n.find_child(key);
                        let child_guard = self.latches().read(child);
                        // `curr` becomes the parent of `child`; assigning here drops the
                        // grandparent's latch, which is the crabbing release.
                        parent = guard.take();
                        guard = Some(child_guard);
                        curr = child;
                    }
                }
            };
            // Release the leaf's READ latch, keep the parent's, and take the leaf's WRITE latch.
            drop(guard);
            let leaf = self.latches().write(leaf_id);
            if parent.is_none() && self.root_page_id.load(Ordering::Acquire) != leaf_id {
                // The root was a leaf and has since split. Nothing written; start again.
                continue;
            }
            return Ok((leaf_id, LeafWriteLatch { parent, leaf }));
        }
    }

    /// The fast path. Returns `Ok(true)` if the entry was inserted, `Ok(false)` if it would have
    /// split the leaf — in which case **nothing has been written** and the caller must retry
    /// pessimistically.
    fn try_insert_without_split(&self, key: &K, value: &V) -> Result<bool, FerroError> {
        let (page_id, _latch) = self.latch_leaf_for_write(key)?;
        let mut leaf = self.read_leaf_raw(page_id)?;
        leaf.insert_entry(key.clone(), value.clone());
        if leaf.is_full() {
            return Ok(false);
        }
        self.write_page(page_id, leaf.serialize()?)?;
        Ok(true)
    }

    /// The slow path: write latches on the whole root-to-leaf path, held for the duration.
    ///
    /// No early release. The textbook optimisation releases an ancestor as soon as a descendant is
    /// "safe", but the safe test for an internal node is whether it has room for a separator key
    /// that comes from a child it has not read yet, so the size of that key is unknown at the time
    /// the decision is made. Estimating it would be a guard that silently stops excluding when the
    /// estimate is low. Splits are rare — one per leaf-full of inserts — and the common case never
    /// reaches this function at all.
    fn insert_splitting(&self, key: K, value: V) -> Result<(), FerroError> {
        loop {
            let root = self.root_page_id.load(Ordering::Acquire);
            let mut guards: Vec<PageWriteGuard<'_>> = vec![self.latches().write(root)];
            if self.root_page_id.load(Ordering::Acquire) != root {
                continue; // `guards` is dropped here
            }
            let mut stack: Vec<u32> = Vec::new();
            let mut curr = root;
            let leaf_id = loop {
                match self.read_node_raw(curr)? {
                    BPlusTreePage::Leaf(_) => break curr,
                    BPlusTreePage::Internal(n) => {
                        stack.push(curr);
                        let child = n.find_child(&key);
                        guards.push(self.latches().write(child));
                        curr = child;
                    }
                }
            };
            let result = self.insert_into_latched_leaf(leaf_id, &mut stack, key, value);
            drop(guards);
            return result;
        }
    }

    /// Insert into a leaf whose whole root-to-leaf path this thread holds write latches on.
    ///
    /// Split ordering matters and is not the order the unlatched version used: the **new** leaf is
    /// written before the old one, because writing the old leaf is what publishes
    /// `next -> new_page_id`, and a concurrent scanner following that pointer must not land on a
    /// page that has not been written yet.
    fn insert_into_latched_leaf(&self, leaf_id: u32, stack: &mut Vec<u32>, key: K, value: V) -> Result<(), FerroError> {
        let mut leaf = self.read_leaf_raw(leaf_id)?;
        leaf.insert_entry(key, value);
        if !leaf.is_full() {
            return self.write_page(leaf_id, leaf.serialize()?);
        }

        let new_page_id = self.buffer_pool.new_page()?;
        // A freshly allocated page is reachable by nobody, so this latch is uncontended by
        // construction and adds no edge to the wait-for graph. It is taken anyway so that the
        // moment the page becomes reachable — the `write_page(leaf_id, ..)` below — a scanner
        // arriving at it waits rather than reading a page mid-write.
        let new_guard = self.latches().write(new_page_id);
        let (split_key, new_leaf) = leaf.split(new_page_id);

        if let Some(old_next_id) = new_leaf.next {
            // Rightward along the leaf chain, which is the permitted direction.
            let sibling = self.latches().write(old_next_id);
            let mut next_leaf = self.read_leaf_raw(old_next_id)?;
            next_leaf.prev = Some(new_page_id);
            self.write_page(old_next_id, next_leaf.serialize()?)?;
            drop(sibling);
        }

        self.write_page(new_page_id, new_leaf.serialize()?)?;
        self.write_page(leaf_id, leaf.serialize()?)?;
        drop(new_guard);

        self.insert_into_parent(stack, leaf_id, split_key, new_page_id)
    }

    // ---------------------------------------------------------------------------------------
    // RAW PAGE ACCESS — every one of these requires the caller to hold the page's latch
    // ---------------------------------------------------------------------------------------

    /// Read a page. **The caller must hold a read or write latch on `page_id`.**
    fn read_node_raw(&self, page_id: u32) -> Result<BPlusTreePage<K, V>, FerroError> {
        let frame_i = self.buffer_pool.fetch_page(page_id)?;
        let node = {
            let frame = self.buffer_pool.frames[frame_i].read().unwrap();
            BPlusTreePage::<K, V>::deserialize(frame.data)
        };
        self.buffer_pool.unpin_page(page_id, false);
        node
    }

    /// Read a page that must be a leaf. **The caller must hold a latch on `page_id`.**
    fn read_leaf_raw(&self, page_id: u32) -> Result<BPlusTreeLeafPage<K, V>, FerroError> {
        match self.read_node_raw(page_id)? {
            BPlusTreePage::Leaf(leaf) => Ok(leaf),
            BPlusTreePage::Internal(_) => Err(FerroError::Io(format!(
                "page {page_id} was reached as a leaf but holds an internal node"
            ))),
        }
    }

    /// Overwrite a page and mark it dirty. **The caller must hold the WRITE latch on `page_id`.**
    fn write_page(&self, page_id: u32, data: [u8; PAGE_SIZE]) -> Result<(), FerroError> {
        let frame_i = self.buffer_pool.fetch_page(page_id)?;
        {
            let mut frame = self.buffer_pool.frames[frame_i].write().unwrap();
            frame.data = data;
        }
        self.buffer_pool.unpin_page(page_id, true);
        Ok(())
    }

    // ---------------------------------------------------------------------------------------
    // HELPERS
    // ---------------------------------------------------------------------------------------

    // fetch page, deserialize to BPlusTreePage, unpin. Takes the page's read latch itself, so it
    // is for callers that hold none — not for use inside a descent, which must crab.
    pub fn read_node(&self, page_id: u32) -> Result<BPlusTreePage<K, V>, FerroError> {
        let guard = self.latches().read(page_id);
        let node = self.read_node_raw(page_id);
        drop(guard);
        node
    }

    /// Insert `mid_key`/`right_id` into the parent, splitting upward as needed.
    ///
    /// **The caller must hold write latches on every page id in `stack`** — `insert_splitting` is
    /// the only caller and takes them on the way down. This function acquires none of its own for
    /// existing pages, which is what stops it from trying to latch upward.
    fn insert_into_parent(&self, stack: &mut Vec<u32>, left_id: u32, mid_key: K, right_id: u32) -> Result<(), FerroError> {
        // root was split, so need to allocate new root, make an internal node with one key (mid_key)
        // and two children (left, right id); update root_page_id, tree height grew
        if stack.is_empty() {
            let new_page_id = self.buffer_pool.new_page()?;
            let mut new_root = BPlusTreeInternalPage::<K>::new(new_page_id);
            new_root.key_arr.push(mid_key);
            new_root.child_ptrs.push(left_id);
            new_root.child_ptrs.push(right_id);
            new_root.num_keys = 1;

            let new_guard = self.latches().write(new_page_id);
            self.write_page(new_page_id, new_root.serialize()?)?;
            drop(new_guard);
            // Release, paired with the Acquire load in every descent: the new root's bytes are in
            // the pool before any thread can learn the id that points at them.
            self.root_page_id.store(new_page_id, Ordering::Release);
            return Ok(())
            // later store the page id in catalog
        }
        // else, have to pop parent id from stack, read it, insert_key_child at right position, if
        // parent not full, done, if it's full, have to do internal split (where middle key moves
        // up), write back both, recurse with parent's middle key
        let parent_id = stack.pop().expect("non-empty");
        let mut parent_node = match self.read_node_raw(parent_id)? {
            BPlusTreePage::Internal(n) => n,
            BPlusTreePage::Leaf(_) => return Err(FerroError::Io(format!(
                "page {parent_id} was reached as an internal node but holds a leaf"
            ))),
        };

        // INSERT FIRST, THEN SPLIT - the same order `insert` uses for leaves, and a correctness
        // fix rather than a tidy-up.
        //
        // This used to ask `is_full()` BEFORE inserting. That check reads
        // `INTERNAL_HEADER_SIZE + keys + child_ptrs >= PAGE_SIZE`, i.e. "are the CURRENT contents
        // already at capacity", while its own comment says "does adding one more entry exceed
        // capacity?". So a node sitting at 4090 bytes reported not-full, took one more key plus a
        // 4-byte child pointer, and `serialize` wrote past the end of the page:
        //   `range end index 4100 out of range for slice of length 4096`.
        // Reachable by any tree deep enough to fill an internal node, which is why small fixtures
        // never saw it; it was found by driving 8000 branches through the branch catalog.
        //
        // Inserting first is safe because the node is an in-memory struct of `Vec`s at this point.
        // Only `serialize` is bounded by the page, and nothing is serialized until after the split.
        let index = parent_node.key_arr.binary_search(&mid_key).unwrap_or_else(|i| i);
        parent_node.insert_key_child(index, mid_key, right_id);

        if parent_node.is_full() {
            let new_parent_id = self.buffer_pool.new_page()?;
            let (up_key, new_parent) = parent_node.split(new_parent_id);
            // New page first, for the same reason as the leaf split: the parent's write is what
            // makes `new_parent_id` reachable.
            let new_guard = self.latches().write(new_parent_id);
            self.write_page(new_parent_id, new_parent.serialize()?)?;
            self.write_page(parent_id, parent_node.serialize()?)?;
            drop(new_guard);
            self.insert_into_parent(stack, parent_id, up_key, new_parent_id)?;
        } else {
            self.write_page(parent_id, parent_node.serialize()?)?;
        }
        Ok(())
    }

    pub fn free_subtree(&self, page_id: u32) -> Result<(), FerroError> {
        if let BPlusTreePage::Internal(internal) = self.read_node(page_id)? {
            for child in &internal.child_ptrs {
                self.free_subtree(*child)?;
            }
        }
        self.buffer_pool.free_page(page_id)
    }

    pub fn free_all(&self) -> Result<(), FerroError> {
        self.free_subtree(self.root_page_id.load(Ordering::Acquire))
    }

    pub fn read_leaf(&self, page_id: u32) -> Result<BPlusTreeLeafPage<K, V>, FerroError> {
        match self.read_node(page_id)? {
            BPlusTreePage::Leaf(leaf) => Ok(leaf),
            _ => Err(FerroError::Io(String::from("expected leaf"))),
        }
    }

    /// The leftmost leaf, reached by read-crabbing down the leftmost path.
    pub fn leftmost_leaf(&self) -> Result<BPlusTreeLeafPage<K, V>, FerroError> {
        loop {
            let root = self.root_page_id.load(Ordering::Acquire);
            let mut guard = self.latches().read(root);
            if self.root_page_id.load(Ordering::Acquire) != root {
                drop(guard);
                continue;
            }
            let mut curr = root;
            loop {
                match self.read_node_raw(curr)? {
                    BPlusTreePage::Leaf(leaf) => return Ok(leaf),
                    BPlusTreePage::Internal(internal) => {
                        let child = internal.child_ptrs[0];
                        let child_guard = self.latches().read(child);
                        drop(guard);
                        guard = child_guard;
                        curr = child;
                    }
                }
            }
        }
    }

    pub fn free_tree(&self) -> Result<(), FerroError> {
        self.free_from(self.root_page_id.load(Ordering::SeqCst))
    }

    fn free_from(&self, page_id: u32) -> Result<(), FerroError> {
        if let BPlusTreePage::Internal(node) = self.read_node(page_id)? {
            for child in &node.child_ptrs {
                self.free_from(*child)?;
            }
        }
        self.buffer_pool.free_page(page_id)
    }

    /// Rebalance an underfull node — **unimplemented, and E70 measured why that is currently safe.**
    ///
    /// The intent was: borrow from a sibling, else merge with one and drop the separator from the
    /// parent, recursing up; and if the root is internal and falls to a single child, make that child
    /// the new root. None of it is written.
    ///
    /// **Nothing can reach this, because no code path net-removes a key from a tree.** Measured
    /// 2026-08-17 (`tests/integration_index_debt.rs`):
    ///
    /// - `execution::delete` holds a `primary_index` field and never calls `delete` on it. A SQL
    ///   `DELETE` stamps `end_ts` on the version in place and leaves the entry, because a reader whose
    ///   snapshot predates the delete still has to find the row through the index.
    /// - `execution::insert` removes an entry only to put the same key straight back — that is what
    ///   E63's key reuse is.
    /// - `execution::update` removes and re-adds the same key when a row's `RecordId` moves.
    ///
    /// So leaf occupancy never dips: 350 deletes over a 400-key index left the entry count, the leaf
    /// count and the minimum keys-per-leaf all unchanged. `no_workload_drives_a_leaf_underfull` pins
    /// that, and it is the test that fails if this ever becomes reachable.
    ///
    /// **An error rather than `todo!()`.** This function returns `Result` so a caller can handle
    /// failure; `todo!()` aborts the process instead, and it was reachable-in-principle from `delete`,
    /// which is on a live path. Same argument as E62's seven binder panics. Implementing a real
    /// rebalance was considered and rejected for now: code that cannot execute would read as a working
    /// feature and be trusted as one, which is worse than an honest refusal. What the missing rebalance
    /// actually costs is not underfull nodes but dead entries nothing reclaims — 8x read amplification
    /// on a full index scan at 50 live rows in 400 entries, scaling with the dead-to-live ratio.
    pub fn handle_underflow(&self, _path: &mut Vec<u32>, node_id: u32) -> Result<(), FerroError> {
        Err(FerroError::Io(format!(
            "B+tree node {node_id} is underfull and rebalancing is not implemented. This should be \
             unreachable: no write path removes a key without re-inserting it, so occupancy never \
             drops (see tests/integration_index_debt.rs). If you are reading this, that invariant \
             broke - a new caller removes entries outright - and borrow-or-merge now has to be \
             written."
        )))
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use std::fs::OpenOptions;
    use crate::{catalog::column::Value, storage::{disk_manager::DiskManager, heap_file_manager::RecordId}};
    
    fn setup() -> (BPlusTreeManager::<Value, Value>, tempfile::TempDir){
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_index.db");
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(&path).unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        (BPlusTreeManager::<Value, Value>::create(bp.clone()).unwrap(), dir)
    }

    fn setup_leaf() -> (BPlusTreeManager<Value, RecordId>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_leaf.db");
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(&path).unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        (BPlusTreeManager::<Value, RecordId>::create(bp.clone()).unwrap(), dir)
    }

    #[test]
    fn test_create_init_empty_leaf() {
        let (tree, _dir) = setup();
        let root_id = tree.root_page_id.load(Ordering::Relaxed);
        let root_node = tree.read_node(root_id).unwrap();
        match root_node {
            BPlusTreePage::Leaf(leaf) => {
                assert_eq!(leaf.page_id, root_id);
                assert_eq!(leaf.num_keys, 0);
                assert_eq!(leaf.key_arr.len(), 0);
                assert_eq!(leaf.vals.len(), 0);
            }
            BPlusTreePage::Internal(_) => unreachable!()
        }
    }

    #[test]
    fn test_search_empty_tree_returns_none() {
        let (tree, _dir)= setup();
        let search_key = Value::Integer(67);
        let result = tree.search(&search_key).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_search_finds_key() {
        let (tree, _dir) = setup();
        let root_id = tree.root_page_id.load(Ordering::Relaxed);

        let frame_i = tree.buffer_pool.fetch_page(root_id).unwrap();
        let mut frame = tree.buffer_pool.frames[frame_i].write().unwrap();
        let mut leaf = BPlusTreeLeafPage::<Value, Value>::deserialize(frame.data).unwrap();
        let key = Value::Integer(69);
        let val = Value::Integer(6767);
        leaf.insert_entry(key.clone(), val.clone());
        leaf.insert_entry(Value::Integer(67), Value::Integer(6969));

        frame.data = leaf.serialize().unwrap();
        drop(frame);
        tree.buffer_pool.unpin_page(root_id, true);
        let result = tree.search(&key).unwrap();
        let bad_result = tree.search(&Value::Integer(76)).unwrap();
        assert_eq!(result, Some(val));
        assert_eq!(bad_result, None);
    }

    #[test]
    fn test_simple_insert() {
        let (tree, _dir) = setup();
        let result = tree.insert(Value::Integer(67), Value::Integer(69));
        assert!(result.is_ok());
    }

    #[test]
    fn test_internal_node_split_a_lot() {
        let (tree, _dir) = setup();
        for i in 0..400 {
            let result = tree.insert(Value::Integer(i), Value::Integer(i));
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_many_level_insert_and_search() {
        let (tree, _dir) = setup();
        for i in 0..2000 {
            let _ = tree.insert(Value::Integer(i), Value::Integer(i *2)).unwrap_or_else(|e| panic!("insert {} failed: {:?}", i, e));
        }
        for i in 0..2000 {
            let res = tree.search(&Value::Integer(i)).unwrap();
            assert_eq!(res, Some(Value::Integer(i*2)));
        }
    }

    #[test]
    fn test_many_level_reverse() {
        let (tree, _dir) = setup();
        for i in (0..2000).rev() {
            tree.insert(Value::Integer(i), Value::Integer(i)).unwrap();
        }
        for i in 0..2000 {
            assert_eq!(tree.search(&Value::Integer(i)).unwrap(), Some(Value::Integer(i)));
        }
    }

    #[test]
    fn test_delete_basic() {
        let (tree, _dir) = setup();
        let (leaf, _dir) = setup_leaf();
        tree.insert(Value::Integer(67),Value::Integer(6)).unwrap();
        tree.insert(Value::Integer(20), Value::Integer(7)).unwrap();

        leaf.insert(Value::Integer(67),RecordId::new(6, 1)).unwrap();
        leaf.insert(Value::Integer(20), RecordId::new(7, 2)).unwrap();
        tree.delete(&Value::Integer(67)).unwrap();
        leaf.delete(&Value::Integer(67)).unwrap();

        assert!(tree.search(&Value::Integer(67)).unwrap().is_none());
        assert!(leaf.search(&Value::Integer(67)).unwrap().is_none());
        assert_eq!(tree.search(&Value::Integer(20)).unwrap(), Some(Value::Integer(7)));
        assert_eq!(leaf.search(&Value::Integer(20)).unwrap(), Some(RecordId::new(7, 2)));
    }

    #[test]
    fn test_delete_not_real_key() {
        let (tree, _dir) = setup();
        assert!(tree.delete(&Value::Integer(934857)).is_err());
    }

    #[test]
    fn test_range_scan_basic() {
        let (tree, _dir) = setup();

        tree.insert(Value::Integer(50), Value::Integer(5)).unwrap();
        tree.insert(Value::Integer(10), Value::Integer(1)).unwrap();
        tree.insert(Value::Integer(30), Value::Integer(3)).unwrap();
        tree.insert(Value::Integer(20), Value::Integer(2)).unwrap();
        tree.insert(Value::Integer(40), Value::Integer(4)).unwrap();

        let result: Vec<(Value, Value)> = tree
            .range_scan(Bound::Included(Value::Integer(20)), Bound::Included(Value::Integer(40)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (Value::Integer(20), Value::Integer(2)));
        assert_eq!(result[1], (Value::Integer(30), Value::Integer(3)));
        assert_eq!(result[2], (Value::Integer(40), Value::Integer(4)));
    }

    #[test]
    fn test_range_scan_many_pages() {
        let (tree, _dir) = setup();
        for i in 0..1000 {
            tree.insert(Value::Integer(i), Value::Integer(i * 10)).unwrap();
        }

        let result: Vec<(Value, Value)> = tree
            .range_scan(Bound::Included(Value::Integer(450)), Bound::Included(Value::Integer(550)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(result.len(), 101);
        for (i, (k, v)) in result.iter().enumerate() {
            let expected_val = 450 + i as i32;
            assert_eq!(*k, Value::Integer(expected_val));
            assert_eq!(*v, Value::Integer(expected_val * 10));
        }
    }

    /// **Regression: an internal node must split before it overflows its page.**
    ///
    /// `insert_into_parent` used to ask `is_full()` BEFORE inserting, but that check reads
    /// "are the current contents already at capacity" while its own comment says "does adding one
    /// more entry exceed capacity?". A node sitting just under 4096 bytes reported not-full, took
    /// one more key plus a 4-byte child pointer, and `serialize` panicked with
    /// `range end index 4100 out of range for slice of length 4096`.
    ///
    /// It needs a tree deep enough that an internal node fills, which is why every existing fixture
    /// missed it: they use small `Value` keys and a few hundred rows. This uses wide byte keys so
    /// the internal level fills in thousands of inserts rather than millions, and it asserts every
    /// key is still readable afterwards - a split that loses a subtree is silent otherwise.
    #[test]
    fn internal_nodes_split_before_overflowing_the_page() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deep.db");
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true).open(&path).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let tree = BPlusTreeManager::<Vec<u8>, Vec<u8>>::create(bp).unwrap();

        // 60-byte keys: ~64 bytes on the page each, so an internal node reaches 4 KB in ~64 keys
        // and the tree needs three levels within a few thousand entries.
        let key = |i: u32| {
            let mut k = i.to_be_bytes().to_vec();
            k.resize(60, 0xAB);
            k
        };
        const N: u32 = 6000;
        for i in 0..N {
            tree.insert(key(i), vec![i as u8; 8]).expect("insert must not overflow a page");
        }
        for i in 0..N {
            assert_eq!(
                tree.search(&key(i)).expect("search"),
                Some(vec![i as u8; 8]),
                "key {i} was lost - a split dropped a subtree"
            );
        }
        // And the whole set must still come back in order from a scan.
        let all: Vec<Vec<u8>> = tree
            .range_scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
            .expect("scan")
            .map(|e| e.expect("entry").0)
            .collect();
        assert_eq!(all.len(), N as usize, "scan lost entries the point lookups could still find");
        let mut sorted = all.clone();
        sorted.sort();
        assert_eq!(all, sorted, "scan came back out of order");
    }
}
