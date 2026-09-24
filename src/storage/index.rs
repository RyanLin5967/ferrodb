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
//! leftward, so the wait-for graph is ordered by depth and then by key and cannot cycle. **One
//! exception, D233:** unlinking an emptied leaf write-latches its LEFT neighbour, then its RIGHT one
//! while still holding the left, to splice the chain, all while holding the root-to-leaf path.
//! `remove_and_unlink` argues why that cannot cycle, and states the premises the argument needs:
//! no holder of a leaf latch waits on another page latch while holding it (it may wait inside the
//! buffer pool, which takes none); every other writer that could first needs the root latch this
//! thread holds, which requires one shared root cell; and the chain names no page twice.
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

/// Whether a leaf write adds an entry or replaces the one already there.
///
/// Private on purpose: it selects between [`BPlusTreeManager::insert`] and
/// [`BPlusTreeManager::upsert`], which are the two public operations, and a third caller choosing
/// the mode for itself would be a third replace protocol to keep correct.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LeafWrite {
    /// Add the entry. An existing entry for the same key is **left in place**, which is how
    /// `insert` has always behaved.
    Insert,
    /// Remove any existing entry for the key and add the new one, in one in-memory leaf image and
    /// therefore in one page write.
    Replace,
}

pub struct BPlusTreeManager<K, V> {
    /// The tree's current root page, **shared** with every other handle on the same tree.
    ///
    /// # Why this is an `Arc` and not a plain `AtomicU32`
    ///
    /// `read_leaf_for` opens every descent with a root-split retry — load the root, latch it,
    /// re-load and restart if it moved. That guard is correct and it was **DORMANT**: `open()`
    /// wrapped the catalog's recorded root in a *fresh private* atomic, so two statements over one
    /// table held two independent root pointers and the retry compared a private value against
    /// itself. It could never fire. The global catalog mutex was the only thing actually providing
    /// the safety, which meant removing that mutex for concurrency would have ACTIVATED a latent
    /// defect rather than merely exposing a stale value. See `SCALE-DESIGN` D53.
    ///
    /// Sharing the cell — rather than sharing the whole manager — is what lets every existing
    /// call site keep building a cheap per-statement handle: `.load()` and `.store()` deref
    /// through the `Arc` unchanged.
    pub root_page_id: Arc<AtomicU32>,
    pub buffer_pool: Arc<BufferPoolManager>,
    pub marker: PhantomData<(K, V)>
}

impl<K: Ord + Clone + BTreeSerialize,V: Clone + BTreeSerialize + Ord> BPlusTreeManager<K,V> {

    pub fn new(root_page_id: AtomicU32, buffer_pool: Arc<BufferPoolManager>) -> Self{
        BPlusTreeManager {root_page_id: Arc::new(root_page_id), buffer_pool, marker: PhantomData}
    }

    /// Open a handle that SHARES `root` with every other handle built from the same cell.
    ///
    /// This is the constructor a statement should use, so a root split performed by one statement
    /// is seen by another's descent and the retry in `read_leaf_for` can fire. `open` below keeps
    /// the private-cell behaviour for callers that genuinely own the tree alone — recovery, and
    /// one-shot page-freeing.
    pub fn open_shared(root: Arc<AtomicU32>, buffer_pool: Arc<BufferPoolManager>) -> Self {
        Self { root_page_id: root, buffer_pool, marker: PhantomData }
    }

    /// The shared root cell, for registering this tree so later handles can share it.
    pub fn root_cell(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.root_page_id)
    }

    // allocates empty root leaf
    pub fn create(buffer_pool: Arc<BufferPoolManager>) -> Result<Self, FerroError> {
        let root_page_id = buffer_pool.new_page()?;
        let root_node = BPlusTreeLeafPage::<K, V>::new(root_page_id);
        let tree = Self {root_page_id: Arc::new(AtomicU32::new(root_page_id)), buffer_pool, marker: PhantomData};
        let guard = tree.latches().write(root_page_id);
        tree.write_page(root_page_id, root_node.serialize()?)?;
        drop(guard);
        Ok(tree)
    }

    /// Open a handle with a **private** root cell.
    ///
    /// ⚠ Two handles opened this way over one tree do NOT see each other's root splits, and the
    /// retry in `read_leaf_for` cannot fire between them. That is correct only for a caller that
    /// owns the tree alone for the duration — recovery, and one-shot page-freeing. Everything on
    /// a statement path wants [`open_shared`].
    pub fn open(root_page_id: u32, buffer_pool: Arc<BufferPoolManager>) -> Self{
        Self { root_page_id: Arc::new(AtomicU32::new(root_page_id)), buffer_pool, marker: PhantomData }
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

    /// Remove one entry. **An entry whose removal empties a leaf takes the leaf out of the tree**
    /// (free-at-empty, D233).
    ///
    /// The common case takes the same latches as a non-splitting insert: the parent's read latch
    /// until the leaf's write latch is in hand, then the leaf alone. A removal that would leave a
    /// leaf empty, when that leaf has a neighbour (so it is not the root), writes nothing and
    /// restarts on [`Self::delete_unlinking`], exactly as a splitting insert restarts on
    /// [`Self::write_splitting`].
    ///
    /// # Why an emptied leaf leaves the tree (Johnson & Shasha's free-at-empty)
    ///
    /// Before D233 an emptied leaf stayed in the chain for ever. A key space that drains from one
    /// end — the branch catalog's DEADLINE span, whose deadlines only move forward — therefore
    /// piled empty leaves exactly where every lease pass starts, and each pass walked all of
    /// them: about one leaf per 81 reaps, plus ~1,000 page reads past the 64-hop bound of
    /// [`Self::read_leaf_for`] (`frontier/deadline_leaf_adversary.md` §0). Johnson & Shasha
    /// ("B-trees with inserts and deletes: why free-at-empty is better than merge-at-half")
    /// remove a node only when it is empty and never rebalance at half; a queue-shaped key
    /// space drains every leaf completely, so free-at-empty reclaims every one of them.
    ///
    /// The unlinked page is **not freed** ([`Self::delete_unlinking`] says why, and what that costs
    /// in a key range that refills). It stays allocated, as a drained leaf did before D233; what
    /// changes is that no descent and no chain reaches it.
    pub fn delete(&self, key: &K) -> Result<(), FerroError> {
        {
            let (page_id, _latch) = self.latch_leaf_for_write(key)?;
            let mut leaf = self.read_leaf_raw(page_id)?;
            leaf.remove_entry(key)?;
            if !Self::would_unlink(&leaf) {
                return self.write_page(page_id, leaf.serialize()?);
            }
            // Nothing written, and the leaf's latch drops here: the removal is redone under the
            // whole path's write latches, which is where an unlink can be done.
        }
        self.delete_unlinking(key)
    }

    /// Whether a leaf image, after a removal, is one [`Self::delete`] takes out of the tree: empty,
    /// and with a neighbour on the leaf chain. A leaf with no neighbour is the only leaf, which is
    /// the root, and a tree always keeps one leaf.
    fn would_unlink(leaf: &BPlusTreeLeafPage<K, V>) -> bool {
        leaf.key_arr.is_empty() && (leaf.prev.is_some() || leaf.next.is_some())
    }

    /// Insert one entry.
    ///
    /// ⚠ **This does NOT replace.** Writing a key that is already present leaves TWO entries for
    /// it and `search` returns whichever the binary search lands on. Callers rewriting a key want
    /// [`Self::upsert`].
    ///
    /// Fast path first: if the leaf has room, only that leaf is latched for writing and only that
    /// leaf is written. If it would split, nothing has been written yet and the whole insert is
    /// retried with write latches on the entire root-to-leaf path.
    pub fn insert(&self, key: K, value: V) -> Result<(), FerroError> {
        if self.try_write_without_split(&key, &value, LeafWrite::Insert)? {
            return Ok(());
        }
        self.write_splitting(key, value, LeafWrite::Insert)
    }

    /// Replace `key`'s value, inserting it if absent — **without a window in which the key is
    /// absent to a concurrent reader.** SCALE-DESIGN D126.
    ///
    /// # What this replaces, and why an atomic form is needed
    ///
    /// Every caller that rewrites a key used to open-code `delete` then `insert`, because this
    /// type had no replace primitive. `delete` drops the leaf's write latch when it returns and
    /// `insert` re-acquires it, so **between the two the key does not exist** — and every reader
    /// on the point-lookup path ([`Self::search`] via `read_leaf_for`) takes no latch at all, so
    /// it sees that gap. `TableBranchCatalog::upsert` routed the branch RECORD key through that
    /// shape, which made an ordinary `set_root` or `renew_lease` transiently un-read a live
    /// branch's record; `descend_optimistic`'s own comment records the same defect reached from
    /// `execution::insert`'s key reuse. Neither bites while a global per-statement mutex excludes
    /// reader from writer, and both bite the moment that mutex is removed, which is the whole
    /// point of removing it.
    ///
    /// # Why this is atomic
    ///
    /// The removal and the addition are applied to the **same in-memory leaf image**, so the one
    /// `write_page` that publishes the new value is the same `write_page` that drops the old one.
    /// A page write is atomic to every reader: the latched path holds the page latch, and the
    /// lock-free path reads the frame's seqlock shadow and retries on a version change
    /// (`BufferPoolManager::read_frame_optimistic`).
    ///
    /// The split case is atomic for the same reason plus the existing B-link ordering. The leaf is
    /// mutated and split entirely in memory; the new right sibling is written first and the halved
    /// leaf — which is what publishes `next` — second. So a reader is always looking at either the
    /// whole pre-write leaf (key present, old value) or a published pair in which the key lives in
    /// exactly one half, and `descend_optimistic`'s right walk reaches the half it moved to.
    ///
    /// **It does not make a multi-key update atomic.** One key, one page write. A caller rewriting
    /// several keys still needs its own exclusion across them — `TableBranchCatalog::logical` is
    /// that, and stays.
    pub fn upsert(&self, key: K, value: V) -> Result<(), FerroError> {
        if self.try_write_without_split(&key, &value, LeafWrite::Replace)? {
            return Ok(());
        }
        self.write_splitting(key, value, LeafWrite::Replace)
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
    /// The point-read descent, **taking nothing shared** — D58.
    ///
    /// The latched crabbing below (`read_leaf_for_latched`) is correct and was the wall: the
    /// latch table is one mutex for every page, and even a per-page latch writes the root's word
    /// once per read, which D51 measured as the ×0.12 class at 16 threads
    /// (`bench/d51_sharedword_probe.txt`; `bench/d58_profile_16T_read_window.sample.txt`). This
    /// path reads each page as a torn-free snapshot through the frame's seqlock shadow
    /// (`BufferPoolManager::read_page_optimistic`) — loads only — and repairs a stale descent the
    /// way Lehman & Yao's B-link tree does: a split writes the new right sibling first, then the
    /// halved leaf with its `next` set, then the parent, so a reader that descended through a
    /// parent from before the split lands on a leaf to the LEFT of the key's true leaf, and walks
    /// `next` while the leaf's largest key is below the key. Internal nodes have no `next`, but a
    /// stale internal node sends the reader down its LAST child, whose subtree tops out below the
    /// key, and the leaf chain is global across the level — so the leaf walk repairs a stale
    /// descent at any height. Nothing here validates the parent, and nothing needs to.
    ///
    /// Both loops are bounded; past the bound, or on any page that is not resident under its
    /// hint, the latched path answers. It is always correct, only slow.
    ///
    /// What makes a snapshot safe to descend: **nothing frees a tree page** except `free_all`,
    /// under the exclusive catalog lock with readers drained, so a page this reader copied cannot
    /// have been reused as something else mid-descent. That is the load-bearing half. This used
    /// to say "leaves never go underfull", which was never what made it safe and stopped being
    /// true at D233: `delete` now takes an emptied leaf out of the tree, and it does so WITHOUT
    /// freeing the page (`delete_unlinking`), precisely so that this sentence keeps holding. A
    /// reader that lands on an unlinked leaf finds it empty and walks its intact `next`.
    fn read_leaf_for(&self, key: &K) -> Result<(u32, BPlusTreeLeafPage<K, V>), FerroError> {
        const RESTARTS: usize = 16;
        const RIGHT_WALK: usize = 64;
        'restart: for _ in 0..RESTARTS {
            let root = self.root_page_id.load(Ordering::Acquire);
            let Some(page) = self.buffer_pool.read_page_optimistic(root) else { continue 'restart };
            // A root split moved the root cell; the page read was the OLD root, which is now an
            // ordinary child — descending it would still be repaired by the leaf walk, but it is
            // cheaper to start again from the new root than to walk half the level.
            if self.root_page_id.load(Ordering::Acquire) != root {
                continue 'restart;
            }
            let node = match BPlusTreePage::<K, V>::deserialize(page.data) {
                Ok(n) if Self::header_page_id(&n) == root => n,
                _ => continue 'restart,
            };
            match self.descend_optimistic(node, root, key, RIGHT_WALK)? {
                Some(found) => return Ok(found),
                None => continue 'restart,
            }
        }
        self.read_leaf_for_latched(key)
    }

    /// The body of [`Self::read_leaf_for`] from an already-copied starting page: descend by
    /// snapshots, then walk right at the leaf. `Ok(None)` means "restart" — a page was not
    /// resident under its hint, a snapshot tore, or the walk exceeded `right_walk` hops.
    ///
    /// Public and hidden because it is a TEST SEAM: `tests/d58_latch_free_descent.rs` hands it a
    /// root snapshot taken BEFORE a split and asserts the moved key is still found — the
    /// interleaving the B-link walk exists for, which cannot be forced through `read_leaf_for`
    /// without pausing a writer mid-split. Not part of the API; nothing else may call it.

    /// The page id a snapshot's own header claims. **A second, independent identity check.**
///
    /// The shadow's `label` says which page the frame's bytes are; this says which page the BYTES
    /// say they are. They are written by different code at different times, so requiring both to
    /// equal the page we asked for turns any future label lie — the kind a fresh-context review found
    /// in D58's first version, where a relabelled frame published the outgoing page's bytes — from a
    /// silently wrong answer into a restart. Costs one `u32` read of a buffer already in hand.
    fn header_page_id(page: &BPlusTreePage<K, V>) -> u32 {
        match page {
            BPlusTreePage::Internal(n) => n.page_id,
            BPlusTreePage::Leaf(l) => l.page_id,
        }
    }

    #[doc(hidden)]
    pub fn descend_optimistic(
        &self,
        mut node: BPlusTreePage<K, V>,
        mut curr: u32,
        key: &K,
        right_walk: usize,
    ) -> Result<Option<(u32, BPlusTreeLeafPage<K, V>)>, FerroError> {
        // Bounded like the walk, and for the same reason: a snapshot is a copy of bytes that may
        // be mid-rewrite, so an internal node can point at a page that points back at it. A
        // latched descent cannot loop (it holds the pages it passed); this one can, and "restart,
        // then fall back to the latched path" is the honest answer. The bound is far above any
        // real height — a 4 KB page holds >100 keys, so 32 levels is >10^64 rows.
        const MAX_DEPTH: usize = 32;
        for _ in 0..MAX_DEPTH {
            match node {
                BPlusTreePage::Leaf(mut leaf) => {
                    // B-link repair: the key may have moved right in a split this descent did
                    // not see. Walk while the leaf cannot contain the key — it tops out below it,
                    // **or it is empty.**
                    //
                    // The empty case is not hypothetical and `is_some_and` got it wrong: it is
                    // false for an empty leaf, so the walk stopped there and returned it, and the
                    // key read as absent. Before D233, `BPlusTreeManager::delete` removed an entry
                    // and wrote the page back with no rebalance (`handle_underflow` is an honest
                    // refusal), and `execution::insert`'s key reuse did exactly `delete(key)` then
                    // `insert(key, rid)`, so a leaf holding one key was empty between those two
                    // writes, and a concurrent optimistic reader walking past it saw the gap.
                    // Found by a fresh-context review; `no_workload_drives_a_leaf_underfull` pins
                    // occupancy for the SQL paths and says nothing about this window.
                    //
                    // Still load-bearing after D233, for two reasons. `delete` publishes an emptied
                    // leaf before it unlinks it (`remove_and_unlink`, step 1), and a reader that
                    // descended through a parent from before the unlink can land on an unlinked
                    // leaf, which stays empty with its `next` intact.
                    let mut hops = 0;
                    while leaf.key_arr.last().is_none_or(|max| max < key) {
                        let Some(next) = leaf.next else { break };
                        hops += 1;
                        if hops > right_walk {
                            return Ok(None);
                        }
                        let Some(np) = self.buffer_pool.read_page_optimistic(next) else { return Ok(None) };
                        match BPlusTreePage::<K, V>::deserialize(np.data) {
                            Ok(BPlusTreePage::Leaf(l)) if l.page_id == next => {
                                curr = next;
                                leaf = l;
                            }
                            _ => return Ok(None),
                        }
                    }
                    return Ok(Some((curr, leaf)));
                }
                BPlusTreePage::Internal(n) => {
                    let child = n.find_child(key);
                    let Some(cp) = self.buffer_pool.read_page_optimistic(child) else { return Ok(None) };
                    node = match BPlusTreePage::<K, V>::deserialize(cp.data) {
                        Ok(n) if Self::header_page_id(&n) == child => n,
                        _ => return Ok(None),
                    };
                    curr = child;
                }
            }
        }
        Ok(None)
    }

    /// The latched descent: read-crabbing from the root. Correct under every interleaving, and
    /// the fallback for [`Self::read_leaf_for`] when a page is not resident or a snapshot keeps
    /// tearing. See the ordering discipline in `page_latch.rs`.
    fn read_leaf_for_latched(&self, key: &K) -> Result<(u32, BPlusTreeLeafPage<K, V>), FerroError> {
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
    ///
    /// Returns the leaf's write guard alone. The parent's is dropped before returning: it is
    /// needed only until the leaf latch is in hand, and holding it longer would block every
    /// splitter under that parent for the whole of this insert.
    fn latch_leaf_for_write(&self, key: &K) -> Result<(u32, PageWriteGuard<'_>), FerroError> {
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
            // Release the leaf's READ latch, KEEP the parent's, and take the leaf's WRITE latch.
            // The parent latch covers exactly this gap and not one instruction more: once the leaf
            // is write-latched, a splitter needs that same latch and is excluded by it, so the
            // parent is released immediately below rather than held for the whole modification.
            // Fire-checked, and it FIRES: bench/d23_fire_check.txt, BREAK B releases the parent
            // before latching the leaf and takes 16- and 32-thread arms to 3/3 and 3/3 dirty
            // rounds (lost_inserts 2-6, live_scan_disorder up to 169). Worth saying that this
            // only became demonstrable once the test grew a scanner that runs DURING the writes;
            // against the post-join scan it had, BREAK B passed and this comment was unearned.
            drop(guard);
            let leaf = self.latches().write(leaf_id);
            if parent.is_none() && self.root_page_id.load(Ordering::Acquire) != leaf_id {
                // The root was a leaf and has since split. Nothing written; start again.
                continue;
            }
            drop(parent);
            return Ok((leaf_id, leaf));
        }
    }

    /// Apply one key/value write to a leaf image **in memory**, with no page write of its own.
    ///
    /// This is the whole of the difference between `insert` and `upsert`, and it is deliberately
    /// one place: under [`LeafWrite::Replace`] the old entry is removed from the same image the
    /// new one is added to, so no caller can accidentally reintroduce a two-page-write replace.
    fn apply_to_leaf(leaf: &mut BPlusTreeLeafPage<K, V>, key: K, value: V, mode: LeafWrite) {
        if mode == LeafWrite::Replace {
            // Absent is the normal case for a first write, not an error. `remove_entry` returns
            // `KeyNotFound` for it and there is nothing else it can fail on.
            let _ = leaf.remove_entry(&key);
        }
        leaf.insert_entry(key, value);
    }

    /// The fast path. Returns `Ok(true)` if the entry was written, `Ok(false)` if it would have
    /// split the leaf — in which case **nothing has been written** and the caller must retry
    /// pessimistically.
    ///
    /// Under [`LeafWrite::Replace`] a same-size rewrite cannot reach the `Ok(false)` arm: no leaf
    /// is ever left persisted in a full state (a split leaves both halves under the threshold), so
    /// removing an entry and adding one of the same length lands on the size the leaf already had.
    /// That is what keeps `set_root` and `renew_lease`, which rewrite a fixed-width core record,
    /// on the single-latch path.
    fn try_write_without_split(&self, key: &K, value: &V, mode: LeafWrite) -> Result<bool, FerroError> {
        let (page_id, _latch) = self.latch_leaf_for_write(key)?;
        let mut leaf = self.read_leaf_raw(page_id)?;
        Self::apply_to_leaf(&mut leaf, key.clone(), value.clone(), mode);
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
    fn write_splitting(&self, key: K, value: V, mode: LeafWrite) -> Result<(), FerroError> {
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
            let result = self.write_into_latched_leaf(leaf_id, &mut stack, key, value, mode);
            drop(guards);
            return result;
        }
    }

    /// Write into a leaf whose whole root-to-leaf path this thread holds write latches on.
    ///
    /// Split ordering matters and is not the order the unlatched version used: the **new** leaf is
    /// written before the old one, because writing the old leaf is what publishes
    /// `next -> new_page_id`, and a concurrent scanner following that pointer must not land on a
    /// page that has not been written yet.
    ///
    /// That same ordering is what makes [`LeafWrite::Replace`] atomic here: the removal happens in
    /// the in-memory image before the split, so the key is in `leaf` until `write_page(leaf_id)`
    /// publishes both halves' contents, and in exactly one half afterwards. Nothing in between
    /// removes it from a page a reader can reach.
    fn write_into_latched_leaf(&self, leaf_id: u32, stack: &mut Vec<u32>, key: K, value: V, mode: LeafWrite) -> Result<(), FerroError> {
        let mut leaf = self.read_leaf_raw(leaf_id)?;
        Self::apply_to_leaf(&mut leaf, key, value, mode);
        if !leaf.is_full() {
            return self.write_page(leaf_id, leaf.serialize()?);
        }
        // ⛔ A full leaf holding ONE entry cannot be split into two that fit: the entry is itself
        // larger than a page. `split` takes `mid = len / 2`, which is 0 here, so `split_off(0)`
        // would move everything to the new page, leave THIS leaf empty, and then panic inside
        // `serialize` — `range end index 4143 out of range for slice of length 4096` on the test
        // below, from a `copy_from_slice` in `index_page.rs`, which names neither the key nor the
        // cause. Refuse by name instead, before anything is written.
        //
        // **D126 is why this is here.** `Insert` could only reach the case on an EMPTY leaf, where
        // it has always panicked. `Replace` reaches it on a populated one: a replacement value can
        // grow a one-entry leaf past the page without ever making it two entries, which is a shape
        // no `insert` can produce. The guard is written for both, and
        // `an_entry_larger_than_a_page_is_refused_rather_than_split_into_an_empty_leaf` fires it
        // from both directions and checks that a value that DOES fit is still accepted.
        if leaf.key_arr.len() < 2 {
            return Err(FerroError::Io(format!(
                "a single entry does not fit in a {PAGE_SIZE}-byte leaf page (page {leaf_id}); \
                 splitting cannot help, and doing it anyway would leave an empty leaf"
            )));
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

    /// The slow path of [`Self::delete`]: write latches on the whole root-to-leaf path, then the
    /// removal and, if it empties a linked leaf, the unlink. The same latch shape as
    /// [`Self::write_splitting`], and the same reason: the parent is about to change.
    ///
    /// # Nothing is freed, on purpose
    ///
    /// The latch-free descent is sound only because no tree page is freed while it runs
    /// ([`Self::read_leaf_for`]): a reader holding a stale page id could otherwise read a recycled
    /// page whose own header still names that id. So the unlinked leaf keeps its bytes, including
    /// `next`. A reader that descended through a parent from before the unlink lands on it, finds
    /// it empty, and walks right, which is the walk the empty case already took. A reader that
    /// descends afterwards never reaches it.
    ///
    /// **Each unlink leaks the leaf page, plus each internal page the cascade empties.** Returning
    /// them waits on D229's reclamation mechanism (a durable pending-free list, or a reachability
    /// sweep at open when no reader exists), which this change does not pre-empt.
    ///
    /// **What that costs depends on whether the drained key range refills** (D233 review F4):
    /// - A range that never refills, such as the catalog's DEADLINE span (deadlines only grow),
    ///   uses no more disk than before D233, where the same drained pages stayed allocated in the
    ///   chain.
    /// - A range that refills does use more. The catalog's FREE_ID span (fork pops the highest free
    ///   id: the key stores `u64::MAX - id`) and the STATE span of recycled ids are like this.
    ///   Before D233 a drained leaf there took the refill in place. Now the drained leaf is leaked and the refill splits a neighbour into a new page,
    ///   so such a span grows from O(max live) pages to O(splits), without bound, until D229 frees
    ///   unlinked pages.
    fn delete_unlinking(&self, key: &K) -> Result<(), FerroError> {
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
                        let child = n.find_child(key);
                        guards.push(self.latches().write(child));
                        curr = child;
                    }
                }
            };
            let result = self.remove_and_unlink(leaf_id, &stack, key);
            drop(guards);
            return result;
        }
    }

    /// Remove `key` from a leaf whose whole root-to-leaf path this thread holds write latches on,
    /// and take the leaf out of the tree if that empties it.
    ///
    /// # Order of the writes, each one a state a latch-free reader may see
    ///
    /// 1. The emptied leaf, `next` intact: the state every reader already copes with (it walks
    ///    past an empty leaf).
    /// 2. The chain splice, `prev.next = next` then `next.prev = prev`: a scan from the left now
    ///    skips the leaf, and a reader that still lands on it walks its intact `next`.
    /// 3. The parent drops its pointer and one separator ([`Self::unlink_from_parent`]): from here
    ///    no descent reaches the leaf.
    ///
    /// # Everything is read and checked before anything is written
    ///
    /// An unlink touches up to four pages, and a refusal after the first write would leave a leaf
    /// that is out of the chain but still routed (D233 review F3). So, under latches this thread
    /// already holds or takes here, every page is read and every link is checked first: `prev.next`
    /// and `next.prev` must name this leaf, and [`Self::unlink_from_parent`] must find where the
    /// cascade stops. Every new image is serialised too. Only then are the pages written, in the
    /// order above. What is left to fail after the first write is the pool's own I/O.
    ///
    /// # The one LEFTWARD latch in this file, and why it cannot deadlock
    ///
    /// The splice write-latches `prev`, which is leftward along the chain, against this file's
    /// acquisition order (down, then rightward). It then takes `next` while still holding `prev`,
    /// and holds both until the splice is written. It is safe because of who can hold a leaf latch
    /// while this thread waits for it:
    ///
    /// - a reader (`load_leaf`, or a latched descent's last step) copies the leaf and releases it,
    ///   and waits on no page latch while holding it;
    /// - a fast-path writer (`latch_leaf_for_write`) holds the leaf write latch and waits on no page
    ///   latch while holding it: it either writes and releases, or releases and restarts;
    /// - a pessimistic writer (a split, or another unlink) would need the ROOT's write latch first,
    ///   and this thread holds it for the whole operation, so no such writer is running.
    ///
    /// "Waits on no page latch" is the whole claim: a holder may wait inside the buffer pool (its
    /// `write_page` goes through `fetch_page`), and the pool never takes a page latch
    /// (`page_latch.rs`, enforced by `enter_pool`). So every holder of `prev`'s or `next`'s latch
    /// releases it without waiting on anything this thread holds, and the wait cannot close a
    /// cycle. The branch catalog also serialises every writer under `TableBranchCatalog::logical`,
    /// but this argument does not rely on that.
    ///
    /// **Its premises, stated** (review 2 Q2, G6, G7):
    /// - **Every writer on this tree shares one root cell** (`open_shared` / the catalog's one
    ///   manager). Two handles with private root cells (`open`) break "a pessimistic writer needs
    ///   the root latch this thread holds": a splitter descending from a stale root can hold `prev`
    ///   and wait on this leaf as its `old_next`, while this thread holds the leaf and waits on
    ///   `prev`. That is a cycle. It is unreachable today, because `delete`'s only caller is the
    ///   branch catalog, which owns one manager; see the module's "What this does NOT make safe".
    /// - The pool never takes a page latch (enforced in debug builds only, `page_latch.rs`).
    /// - The chain is consistent: `prev != next`, and neither is the leaf. That one is checked
    ///   above, before any latch, because latches are not re-entrant.
    fn remove_and_unlink(&self, leaf_id: u32, stack: &[u32], key: &K) -> Result<(), FerroError> {
        let mut leaf = self.read_leaf_raw(leaf_id)?;
        leaf.remove_entry(key)?;
        // Decided again under the path's write latches: a writer may have refilled the leaf between
        // the fast path letting go of it and this thread taking the path.
        let unlink = Self::would_unlink(&leaf);
        if !unlink {
            return self.write_page(leaf_id, leaf.serialize()?);
        }
        // Review 2 G6: page latches are not re-entrant, and this thread already holds the leaf's and
        // will hold `prev`'s while it takes `next`'s. A chain that names one page twice would make it
        // wait on itself, holding the root, which stalls the whole tree. Refused before either latch.
        if (leaf.prev.is_some() && leaf.prev == leaf.next) || leaf.prev == Some(leaf_id) || leaf.next == Some(leaf_id) {
            return Err(FerroError::Io(format!(
                "page {leaf_id}'s neighbours are prev {:?} and next {:?}: a chain that names one page \
                 twice is inconsistent, so the unlink is refused and nothing is written",
                leaf.prev, leaf.next
            )));
        }
        // `prev`/`next` are stable here: they change only in a split or an unlink, and both hold
        // the root's write latch, which this thread holds. Their KEYS may still change under a
        // fast-path writer until their latches are taken, which is why each image is read under its
        // latch and the latch is kept until that image is written.
        let _left_latch = leaf.prev.map(|id| self.latches().write(id));
        let left = match leaf.prev {
            Some(prev_id) => {
                let mut left = self.read_leaf_raw(prev_id)?;
                if left.next != Some(leaf_id) {
                    return Err(FerroError::Io(format!(
                        "page {leaf_id}'s prev is page {prev_id}, whose next is {:?}; the leaf chain is \
                         inconsistent, so the unlink is refused and nothing is written",
                        left.next
                    )));
                }
                left.next = leaf.next;
                Some((prev_id, left.serialize()?))
            }
            None => None,
        };
        let _right_latch = leaf.next.map(|id| self.latches().write(id));
        let right = match leaf.next {
            Some(next_id) => {
                let mut right = self.read_leaf_raw(next_id)?;
                if right.prev != Some(leaf_id) {
                    return Err(FerroError::Io(format!(
                        "page {leaf_id}'s next is page {next_id}, whose prev is {:?}; the leaf chain is \
                         inconsistent, so the unlink is refused and nothing is written",
                        right.prev
                    )));
                }
                right.prev = leaf.prev;
                Some((next_id, right.serialize()?))
            }
            None => None,
        };
        let (parent_id, parent) = self.unlink_from_parent(stack, leaf_id)?;
        let parent = parent.serialize()?;
        let leaf = leaf.serialize()?;

        // The writes, in the order a latch-free reader may see them (above).
        self.write_page(leaf_id, leaf)?;
        if let Some((prev_id, image)) = left {
            self.write_page(prev_id, image)?;
        }
        if let Some((next_id, image)) = right {
            self.write_page(next_id, image)?;
        }
        self.write_page(parent_id, parent)
    }

    /// Plan dropping `doomed` from its parent, cascading while a parent would be left with no
    /// child. **Writes nothing**: returns the page where the cascade stops and that page's new
    /// image, which [`Self::remove_and_unlink`] writes only once every check has passed.
    /// **The caller must hold write latches on every page in `stack`**, which is the path from the
    /// root down to `doomed`'s parent, so the image is still current when it is written.
    ///
    /// The separator rule is `cow::btree::unlink_up`'s (D104), the in-repo precedent: removing a
    /// child that is not the leftmost removes the separator to its LEFT, so its key range joins its
    /// left sibling's; removing the leftmost removes the separator to its RIGHT, so the next child
    /// takes the range and becomes the leftmost. Either way the parent's own range is unchanged, so
    /// nothing above it moves. A parent whose ONLY child goes is itself empty: it is left as it is,
    /// unreachable and not freed, and removed from its own parent in turn.
    ///
    /// The cascade cannot empty the root. [`Self::remove_and_unlink`] unlinks only a leaf with a
    /// neighbour, and the lowest common ancestor of the leaf and that neighbour has at least two
    /// children, one of which survives. Reaching the root with nothing left is therefore refused
    /// as a broken invariant rather than turned into an empty tree. The root is also never
    /// collapsed onto a single child; like `unlink_up`, this keeps a level a shrunken tree no
    /// longer needs.
    fn unlink_from_parent(&self, stack: &[u32], doomed: u32) -> Result<(u32, BPlusTreeInternalPage<K>), FerroError> {
        let mut doomed = doomed;
        let mut above = stack.iter().rev();
        loop {
            let Some(&parent_id) = above.next() else {
                return Err(FerroError::Io(format!(
                    "unlinking page {doomed} would leave the B+tree with no leaf; only a leaf with a \
                     neighbour is unlinked, so the tree's links are inconsistent"
                )));
            };
            let mut parent = match self.read_node_raw(parent_id)? {
                BPlusTreePage::Internal(n) => n,
                BPlusTreePage::Leaf(_) => return Err(FerroError::Io(format!(
                    "page {parent_id} was reached as an internal node but holds a leaf"
                ))),
            };
            let Some(slot) = parent.child_ptrs.iter().position(|&c| c == doomed) else {
                return Err(FerroError::Io(format!(
                    "page {parent_id} is on the path to page {doomed} but does not point at it"
                )));
            };
            if parent.child_ptrs.len() == 1 {
                doomed = parent_id;
                continue;
            }
            if slot == 0 {
                parent.key_arr.remove(0);
            } else {
                parent.key_arr.remove(slot - 1);
            }
            parent.child_ptrs.remove(slot);
            parent.num_keys = parent.key_arr.len() as u16;
            return Ok((parent_id, parent));
        }
    }

    // ---------------------------------------------------------------------------------------
    // RAW PAGE ACCESS — every one of these requires the caller to hold the page's latch
    // ---------------------------------------------------------------------------------------

    /// Read a page. **The caller must hold a read or write latch on `page_id`.**
    fn read_node_raw(&self, page_id: u32) -> Result<BPlusTreePage<K, V>, FerroError> {
        let frame_i = self.buffer_pool.fetch_page(page_id)?;
        let node = {
            // `frame_read`, not `frames[..].read()`: this runs while a page latch is held, and
            // the tracked accessor is what makes an inverted order fail a test instead of hanging.
            let frame = self.buffer_pool.frame_read(frame_i);
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
            // Tracked accessor - see `read_node_raw`.
            let mut frame = self.buffer_pool.frame_write(frame_i);
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
    /// ⛔ **CORRECTED by D233. The sentence below used to read "no code path net-removes a key
    /// from a tree", and at `9aa6968` that was false.** The branch catalog's tree net-removes keys:
    /// every reap drops a DEADLINE key, every detach a CHILD key, every recycled fork a FREE_ID
    /// key, and every state change moves a STATE key (`TableBranchCatalog::write_record`,
    /// `table_catalog.rs`). E70 measured the SQL paths only, and the comment generalised it to
    /// every tree. The consequence was a chain of empty leaves at the head of the DEADLINE span
    /// that every lease pass walked (`frontier/deadline_leaf_adversary.md`).
    ///
    /// **What is true, and why this is still not called:** `delete` now frees at empty (Johnson &
    /// Shasha): an emptied leaf leaves the tree, and nothing rebalances at half. That is the whole
    /// answer for a tree that drains, and merge-at-half is not needed to bound a walk. For the SQL
    /// trees nothing changes, because **no SQL path calls `delete` at all**: key reuse is `upsert`
    /// (D126: `execution::insert`, `execution::update`, `catalog::alter`), and a SQL `DELETE` stamps
    /// `end_ts` and keeps the entry. So no SQL leaf ever empties, `delete` never unlinks one, and
    /// `no_workload_drives_a_leaf_underfull` still holds and still pins it.
    ///
    /// **What was measured for the SQL paths**, 2026-08-17 (`tests/integration_index_debt.rs`):
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
            "B+tree node {node_id} is underfull and merge-at-half is not implemented. Nothing calls \
             this: an EMPTY leaf is taken out of the tree by delete (free-at-empty, D233), and an \
             underfull one is left as it is by design. If you are reading this, a new caller wants \
             rebalancing at half, which has to be written first."
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
        let mut frame = tree.buffer_pool.frame_write(frame_i);
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

    /// **An entry larger than a page must be REFUSED, not split into an empty leaf.**
    ///
    /// `BPlusTreeLeafPage::split` takes `mid = len / 2`. On a leaf holding ONE entry that is
    /// `mid = 0`, so `split_off(0)` moves everything to the new page and leaves the old leaf
    /// EMPTY — and `serialize` on the new page then indexes past `PAGE_SIZE` and panics inside
    /// `copy_from_slice`. Measured before the guard, on this exact test: it panicked at
    /// `src/storage/index_page.rs:117` with
    /// `range end index 4143 out of range for slice of length 4096`
    /// (27 header + 12 key + 4104 value).
    ///
    /// **D126 is why this is written down now.** `insert` could only reach the case by inserting
    /// an oversized entry into an EMPTY leaf; `upsert` reaches it on a POPULATED one, because a
    /// replacement value can grow a one-entry leaf past the page without ever making it two
    /// entries. Both arms are asserted, because the guard covers both and a guard that only ever
    /// fires on the new path would say nothing about the old one.
    ///
    /// The refusal happens before anything is written, so the pre-existing value must survive —
    /// asserted, because "refuses" and "refuses without corrupting" are different claims.
    #[test]
    fn an_entry_larger_than_a_page_is_refused_rather_than_split_into_an_empty_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversize.db");
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(&path).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let tree = BPlusTreeManager::<Vec<u8>, Vec<u8>>::create(bp).unwrap();

        // A `Vec<u8>` entry costs 4 length bytes plus its payload, and a leaf header is 27, so one
        // entry overflows a 4096-byte page at a payload of ~4053. 4100 is comfortably past it.
        let huge = vec![0x5Au8; 4100];
        let k = b"oversize".to_vec();
        tree.insert(k.clone(), vec![1u8; 8]).expect("the small entry must fit");

        // ARM 1 — UPSERT, the case D126 introduced: one entry already there, grown past the page.
        let e = tree.upsert(k.clone(), huge.clone()).expect_err("an oversized upsert must refuse");
        assert!(
            e.to_string().contains("does not fit"),
            "refused, but not by the guard that names the reason: {e}"
        );
        assert_eq!(
            tree.search(&k).expect("search"),
            Some(vec![1u8; 8]),
            "the refusal wrote something; the previous value must survive untouched"
        );

        // ARM 2 — INSERT into an empty leaf, the case that always existed and panicked.
        let dir2 = tempfile::tempdir().unwrap();
        let path2 = dir2.path().join("oversize2.db");
        let file2 = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(&path2).unwrap();
        let bp2 = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file2).unwrap())));
        let empty = BPlusTreeManager::<Vec<u8>, Vec<u8>>::create(bp2).unwrap();
        let e2 = empty.insert(k.clone(), huge).expect_err("an oversized insert must refuse");
        assert!(
            e2.to_string().contains("does not fit"),
            "refused, but not by the guard that names the reason: {e2}"
        );
        assert_eq!(empty.search(&k).expect("search"), None, "nothing must have been written");

        // NEGATIVE CONTROL. The guard must not refuse an entry that DOES fit, including one big
        // enough to force a genuine split of a leaf that holds more than one entry.
        let big_but_ok = vec![0x11u8; 2000];
        tree.upsert(k.clone(), big_but_ok.clone()).expect("a 2000-byte value fits");
        assert_eq!(tree.search(&k).expect("search"), Some(big_but_ok));
        for i in 0u32..40 {
            tree.insert(i.to_be_bytes().to_vec(), vec![0x22u8; 300])
                .expect("ordinary inserts must still split normally");
        }
        for i in 0u32..40 {
            assert_eq!(
                tree.search(&i.to_be_bytes().to_vec()).expect("search"),
                Some(vec![0x22u8; 300]),
                "key {i} lost after the splits the control forced"
            );
        }
    }

    // ---- D233: free-at-empty ----------------------------------------------------------------

    /// A 1,000-byte key: a leaf holds four entries and an internal page four children, so a few
    /// dozen keys build a tree deep enough for an unlink to cascade.
    fn wide(i: i32) -> Value {
        Value::Varchar(format!("{i:06}{}", "x".repeat(994)))
    }

    /// Every leaf reachable by descending from the root, left to right.
    fn leaves_by_descent(tree: &BPlusTreeManager<Value, Value>) -> Vec<u32> {
        fn walk(tree: &BPlusTreeManager<Value, Value>, page: u32, out: &mut Vec<u32>) {
            match tree.read_node(page).unwrap() {
                BPlusTreePage::Leaf(_) => out.push(page),
                BPlusTreePage::Internal(n) => {
                    for c in &n.child_ptrs {
                        walk(tree, *c, out);
                    }
                }
            }
        }
        let mut out = Vec::new();
        walk(tree, tree.root_page_id.load(Ordering::Acquire), &mut out);
        out
    }

    /// The leaf chain from the leftmost leaf through `next`.
    fn leaves_by_chain(tree: &BPlusTreeManager<Value, Value>) -> Vec<BPlusTreeLeafPage<Value, Value>> {
        let mut out = vec![tree.leftmost_leaf().unwrap()];
        while let Some(n) = out.last().unwrap().next {
            out.push(tree.read_leaf(n).unwrap());
        }
        out
    }

    fn height(tree: &BPlusTreeManager<Value, Value>) -> usize {
        let mut h = 1;
        let mut page = tree.root_page_id.load(Ordering::Acquire);
        while let BPlusTreePage::Internal(n) = tree.read_node(page).unwrap() {
            h += 1;
            page = n.child_ptrs[0];
        }
        h
    }

    /// D233: after `delete` empties leaves, the parents and the leaf chain must agree on which
    /// leaves are in the tree, no emptied leaf may stay in the chain, `prev` must mirror `next`,
    /// every surviving key must be found, and a key put back into an emptied range must be found
    /// by `search` AND by a full scan.
    ///
    /// Two removals are forced on a tree of height at least 3: keys 0..20, which empty a whole
    /// left subtree (the cascade, and the leftmost-child rule at every level it climbs), and keys
    /// 35..42, which empty at least one leaf in the middle (a slotted child). This is the test
    /// that sees an unlink that splices the chain but leaves the parent pointing at the leaf. The
    /// catalog's lease-pass test cannot see that, because its descent never lands there. Here a
    /// re-inserted key would land in a leaf no scan reaches.
    #[test]
    fn free_at_empty_keeps_the_chain_and_the_parents_in_agreement() {
        let (tree, _dir) = setup();
        const N: i32 = 60;
        for i in 0..N {
            tree.insert(wide(i), Value::Integer(i)).unwrap();
        }
        assert!(height(&tree) >= 3, "premise: height {} is too shallow to cascade", height(&tree));
        let before = leaves_by_descent(&tree);

        let gone: Vec<i32> = (0..20).chain(35..42).collect();
        for &i in &gone {
            tree.delete(&wide(i)).unwrap();
        }

        let descent = leaves_by_descent(&tree);
        let chain = leaves_by_chain(&tree);
        let chain_ids: Vec<u32> = chain.iter().map(|l| l.page_id).collect();
        assert!(descent.len() < before.len(), "premise: no leaf was taken out of the tree");
        assert_eq!(descent, chain_ids, "the parents and the leaf chain disagree about the leaves");
        assert!(chain.iter().all(|l| !l.key_arr.is_empty()), "an emptied leaf is still in the chain");
        assert_eq!(chain[0].prev, None, "the first leaf still has a left neighbour");
        for w in chain.windows(2) {
            assert_eq!(w[1].prev, Some(w[0].page_id), "prev does not mirror next");
        }

        for i in 0..N {
            let found = tree.search(&wide(i)).unwrap().is_some();
            assert_eq!(found, !gone.contains(&i), "key {i}: found = {found}");
        }

        for i in [3, 38] {
            tree.insert(wide(i), Value::Integer(i)).unwrap();
        }
        let scanned: Vec<Value> = tree
            .range_scan(Bound::Unbounded, Bound::Unbounded)
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        let mut sorted = scanned.clone();
        sorted.sort();
        assert_eq!(scanned, sorted, "the chain is out of order");
        assert_eq!(scanned.len(), (N as usize) - gone.len() + 2, "a full scan lost or duplicated a key");
        for i in [3, 38] {
            assert!(tree.search(&wide(i)).unwrap().is_some(), "re-inserted key {i} not found by search");
            assert_eq!(
                scanned.iter().filter(|k| **k == wide(i)).count(),
                1,
                "re-inserted key {i} not seen exactly once by a full scan"
            );
        }
    }

    // ---- D233 review: F1, F2, F3 and the two survivors it named ------------------------------

    /// A key that sorts after `wide(i)` and before `wide(i + 1)`, and is neither.
    fn between(i: i32) -> Value {
        Value::Varchar(format!("{i:06}{}y", "x".repeat(993)))
    }

    /// The raw bytes of a page as the pool holds it.
    fn page_bytes(tree: &BPlusTreeManager<Value, Value>, page: u32) -> [u8; PAGE_SIZE] {
        let frame_i = tree.buffer_pool.fetch_page(page).unwrap();
        let data = tree.buffer_pool.frames[frame_i].read().unwrap().data;
        tree.buffer_pool.unpin_page(page, false);
        data
    }

    /// The internal page that points at `child`, found by descent from the root.
    fn parent_of(tree: &BPlusTreeManager<Value, Value>, child: u32) -> u32 {
        fn walk(tree: &BPlusTreeManager<Value, Value>, page: u32, child: u32) -> Option<u32> {
            match tree.read_node(page).unwrap() {
                BPlusTreePage::Leaf(_) => None,
                BPlusTreePage::Internal(n) if n.child_ptrs.contains(&child) => Some(page),
                BPlusTreePage::Internal(n) => n.child_ptrs.iter().find_map(|&c| walk(tree, c, child)),
            }
        }
        walk(tree, tree.root_page_id.load(Ordering::Acquire), child).expect("the child is reachable by descent")
    }

    /// Rewrite one leaf in place, under its write latch: how these tests plant a broken link.
    fn rewrite_leaf(
        tree: &BPlusTreeManager<Value, Value>,
        page: u32,
        edit: impl FnOnce(&mut BPlusTreeLeafPage<Value, Value>),
    ) {
        let _latch = tree.latches().write(page);
        let mut leaf = tree.read_leaf_raw(page).unwrap();
        edit(&mut leaf);
        tree.write_page(page, leaf.serialize().unwrap()).unwrap();
    }

    /// Pages the allocator holds for this test's file: the set bits of its first bitmap page.
    fn allocated_pages(tree: &BPlusTreeManager<Value, Value>) -> usize {
        let bitmap = tree.buffer_pool.disk_manager.read(0).unwrap();
        bitmap[4..].iter().map(|b| b.count_ones() as usize).sum()
    }

    /// Every page reachable by descent from the root, internal pages included.
    fn reachable_pages(tree: &BPlusTreeManager<Value, Value>) -> usize {
        fn walk(tree: &BPlusTreeManager<Value, Value>, page: u32) -> usize {
            match tree.read_node(page).unwrap() {
                BPlusTreePage::Leaf(_) => 1,
                BPlusTreePage::Internal(n) => 1 + n.child_ptrs.iter().map(|&c| walk(tree, c)).sum::<usize>(),
            }
        }
        walk(tree, tree.root_page_id.load(Ordering::Acquire))
    }

    /// Every key still in the tree, by a full scan of the leaf chain.
    fn scan_all(tree: &BPlusTreeManager<Value, Value>) -> Vec<Value> {
        tree.range_scan(Bound::Unbounded, Bound::Unbounded).unwrap().map(|r| r.unwrap().0).collect()
    }

    /// **F1, the killer.** `remove_and_unlink` decides emptiness AGAIN under the path's write
    /// latches, because a writer may refill the leaf between the fast path letting go of it and the
    /// unlinker taking the path. That re-check is the only thing between a refill and a lost write.
    ///
    /// The post-refill state, without the race: the pessimistic path is called directly for a key
    /// whose leaf still holds other keys. Nothing may be unlinked, and every other key must stay
    /// reachable by descent and by chain. Mutant `let unlink = true;` unlinks the leaf with its keys
    /// in it; this test is what sees that. It passes at `0eda6ca`: it guards a line that is right.
    #[test]
    fn the_unlink_path_keeps_a_leaf_a_writer_refilled() {
        let (tree, _dir) = setup();
        const N: i32 = 16;
        for i in 0..N {
            tree.insert(wide(i), Value::Integer(i)).unwrap();
        }
        let chain = leaves_by_chain(&tree);
        assert!(chain.len() >= 3, "premise: {} leaves, need a middle one", chain.len());
        let mid = &chain[1];
        assert!(mid.key_arr.len() >= 2, "premise: the middle leaf holds {} keys, need two", mid.key_arr.len());
        let (mid_id, victim) = (mid.page_id, mid.key_arr[0].clone());

        tree.delete_unlinking(&victim).unwrap();

        let descent = leaves_by_descent(&tree);
        let chain_ids: Vec<u32> = leaves_by_chain(&tree).iter().map(|l| l.page_id).collect();
        assert!(descent.contains(&mid_id), "a leaf still holding keys was taken out of the parents");
        assert_eq!(descent, chain_ids, "the parents and the leaf chain disagree about the leaves");
        let scanned = scan_all(&tree);
        for i in 0..N {
            let k = wide(i);
            let want = k != victim;
            assert_eq!(tree.search(&k).unwrap().is_some(), want, "key {i} by descent");
            assert_eq!(scanned.contains(&k), want, "key {i} by chain");
        }
    }

    /// **F1, the concurrent arm. Not a killer**: it hits the refill window only when the scheduler
    /// puts it there. Four writers own interleaved keys, so every toggling leaf is shared by all
    /// four; each writer inserts and deletes its keys in rounds, so leaves empty (and unlink) and
    /// refill while other writers are mid-operation. Keys in every third block of four are never
    /// touched, and two optimistic readers assert on every pass that each of those is found: a key
    /// present for a reader's whole operation must never be missed. After the join every key is
    /// checked by descent and by chain, and the parents must agree with the chain.
    ///
    /// **Its premises (review 2 G5), so that it cannot pass without having raced anything:** the
    /// writers start only after each reader has finished one pass, so both readers are running
    /// while the writers work; each reader finishes at least one pass; and pages allocated minus
    /// pages reachable by descent must GROW over the run. Nothing frees a tree page, so that growth
    /// is exactly the pages the run unlinked.
    #[test]
    fn unlinks_racing_refills_and_readers_lose_no_key() {
        const KEYS: i32 = 240;
        const WRITERS: i32 = 4;
        const ROUNDS: i32 = 6;
        let stable = |i: i32| (i / 4) % 3 == 0;
        let (tree, _dir) = setup();
        for i in (0..KEYS).filter(|&i| stable(i)) {
            tree.insert(wide(i), Value::Integer(i)).unwrap();
        }
        let done = std::sync::atomic::AtomicBool::new(false);
        let first_passes = std::sync::atomic::AtomicUsize::new(0);
        let passes = [std::sync::atomic::AtomicUsize::new(0), std::sync::atomic::AtomicUsize::new(0)];
        let unreachable_before = allocated_pages(&tree) - reachable_pages(&tree);

        std::thread::scope(|s| {
            let writers: Vec<_> = (0..WRITERS)
                .map(|w| {
                    let (tree, first_passes) = (&tree, &first_passes);
                    s.spawn(move || {
                        while first_passes.load(Ordering::Acquire) < 2 {
                            std::thread::yield_now();
                        }
                        let mine: Vec<i32> = (0..KEYS).filter(|&i| !stable(i) && i % WRITERS == w).collect();
                        for r in 0..ROUNDS {
                            for &i in &mine {
                                if r % 2 == 0 {
                                    tree.insert(wide(i), Value::Integer(i)).unwrap();
                                } else {
                                    tree.delete(&wide(i)).unwrap();
                                }
                            }
                        }
                        // ROUNDS is even, so the last round deleted everything; put the even keys back.
                        for &i in mine.iter().filter(|&&i| i % 2 == 0) {
                            tree.insert(wide(i), Value::Integer(i)).unwrap();
                        }
                    })
                })
                .collect();
            for r in 0..2 {
                let (tree, done, first_passes, passes) = (&tree, &done, &first_passes, &passes);
                s.spawn(move || loop {
                    for i in 0..KEYS {
                        let got = tree.search(&wide(i)).unwrap();
                        if stable(i) {
                            assert_eq!(got, Some(Value::Integer(i)), "untouched key {i} missed by a concurrent reader");
                        } else if let Some(v) = got {
                            assert_eq!(v, Value::Integer(i), "key {i} read the wrong value");
                        }
                    }
                    if passes[r].fetch_add(1, Ordering::AcqRel) == 0 {
                        first_passes.fetch_add(1, Ordering::AcqRel);
                    }
                    if done.load(Ordering::Acquire) {
                        break;
                    }
                });
            }
            for w in writers {
                w.join().unwrap();
            }
            done.store(true, Ordering::Release);
        });

        for (r, n) in passes.iter().enumerate() {
            assert!(n.load(Ordering::Acquire) >= 1, "premise: reader {r} finished no pass");
        }
        let unreachable_after = allocated_pages(&tree) - reachable_pages(&tree);
        assert!(
            unreachable_after > unreachable_before,
            "premise: no page was unlinked ({unreachable_before} unreachable pages before, {unreachable_after} \
             after), so the run raced nothing"
        );

        let expected: Vec<i32> = (0..KEYS).filter(|&i| stable(i) || i % 2 == 0).collect();
        for i in 0..KEYS {
            assert_eq!(tree.search(&wide(i)).unwrap().is_some(), expected.contains(&i), "key {i} by descent after the join");
        }
        let want: Vec<Value> = expected.iter().map(|&i| wide(i)).collect();
        assert_eq!(scan_all(&tree), want, "a full scan after the join disagrees with the expected keys");
        let chain_ids: Vec<u32> = leaves_by_chain(&tree).iter().map(|l| l.page_id).collect();
        assert_eq!(leaves_by_descent(&tree), chain_ids, "the parents and the leaf chain disagree after the join");
    }

    /// **F2.** `d58_latch_free_descent::the_right_walk_crosses_an_empty_leaf` no longer crosses one:
    /// since D233 the emptied leaf is spliced out of the chain before its walk starts. That test is
    /// Ryan's to judge (⚖11) and is not edited. This is its replacement for the walk.
    ///
    /// A reader that read the root BEFORE an unlink descends through it onto the unlinked leaf,
    /// which is empty with its `next` intact. The unlinked leaf is the root's leftmost child, so its
    /// key range joins its right neighbour, and a key inserted into that range afterwards lives
    /// there. The reader must walk right off the empty leaf and find it. Mutant `is_some_and` (the
    /// original D58 bug) stops the walk on the empty leaf, and this test sees it.
    #[test]
    fn a_reader_from_before_an_unlink_walks_off_the_unlinked_leaf() {
        let (tree, _dir) = setup();
        for i in 0..8 {
            tree.insert(wide(i), Value::Integer(i)).unwrap();
        }
        assert_eq!(height(&tree), 2, "premise: the root's children must be leaves");
        let root = tree.root_page_id.load(Ordering::Acquire);
        let (doomed, right) = match tree.read_node(root).unwrap() {
            BPlusTreePage::Internal(n) => {
                assert!(n.child_ptrs.len() >= 3, "premise: the root has {} children, need 3", n.child_ptrs.len());
                (n.child_ptrs[0], n.child_ptrs[1])
            }
            BPlusTreePage::Leaf(_) => unreachable!("height 2"),
        };
        let gone = tree.read_leaf(doomed).unwrap().key_arr;
        let right_keys = tree.read_leaf(right).unwrap().key_arr;
        assert_eq!(gone.first(), Some(&wide(0)), "premise: the leftmost leaf holds the smallest key");
        let refill = between(0);

        // Snapshots of the root taken before the unlink, one per descent (a page image is consumed).
        let snap_refill = tree.read_node(root).unwrap();
        match &snap_refill {
            BPlusTreePage::Internal(n) => assert_eq!(
                n.find_child(&refill),
                doomed,
                "premise: the pre-unlink root routes the refill key to the leaf being unlinked"
            ),
            BPlusTreePage::Leaf(_) => unreachable!("height 2"),
        }
        let snaps_gone: Vec<_> = gone.iter().map(|_| tree.read_node(root).unwrap()).collect();
        let snaps_right: Vec<_> = right_keys.iter().map(|_| tree.read_node(root).unwrap()).collect();

        for k in &gone {
            tree.delete(k).unwrap();
        }
        assert!(!leaves_by_descent(&tree).contains(&doomed), "premise: the emptied leaf was not unlinked");
        let frozen = tree.read_leaf(doomed).unwrap();
        assert!(frozen.key_arr.is_empty(), "premise: the unlinked leaf is not empty");
        assert_eq!(frozen.next, Some(right), "premise: the unlinked leaf's next is not its old neighbour");
        tree.insert(refill.clone(), Value::Integer(-1)).unwrap();
        assert!(
            tree.read_leaf(right).unwrap().key_arr.contains(&refill),
            "premise: the refill key did not land in the leaf that absorbed the unlinked range"
        );

        let (page, leaf) = tree
            .descend_optimistic(snap_refill, root, &refill, 64)
            .unwrap()
            .expect("premise: the optimistic descent gave up (a page not resident, or torn)");
        assert!(
            leaf.key_arr.contains(&refill),
            "a reader holding the pre-unlink root stopped on page {page} (the unlinked leaf is {doomed}) \
             and missed a key that lives in {right}"
        );
        for (k, snap) in gone.iter().zip(snaps_gone) {
            let (_, leaf) = tree.descend_optimistic(snap, root, k, 64).unwrap().expect("premise: descent gave up");
            assert!(!leaf.key_arr.contains(k), "a deleted key was found through the pre-unlink root");
        }
        for (k, snap) in right_keys.iter().zip(snaps_right) {
            let (_, leaf) = tree.descend_optimistic(snap, root, k, 64).unwrap().expect("premise: descent gave up");
            assert!(leaf.key_arr.contains(k), "a key to the right of the unlinked leaf was missed");
        }
    }

    /// **The neighbour clause of `would_unlink`.** A leaf with no neighbour is the only leaf, the
    /// root, and deleting its last key must leave an empty, usable tree. Without the clause the
    /// delete takes the unlink path, finds no parent, and refuses. Nothing else empties a root leaf
    /// through `delete`, so without this test that mutant survives.
    #[test]
    fn deleting_the_last_key_of_the_only_leaf_keeps_a_usable_tree() {
        let (tree, _dir) = setup();
        tree.insert(Value::Integer(1), Value::Integer(10)).unwrap();
        tree.delete(&Value::Integer(1)).unwrap();
        assert_eq!(tree.search(&Value::Integer(1)).unwrap(), None);
        assert_eq!(leaves_by_descent(&tree), vec![tree.root_page_id.load(Ordering::Acquire)]);
        tree.insert(Value::Integer(2), Value::Integer(20)).unwrap();
        assert_eq!(tree.search(&Value::Integer(2)).unwrap(), Some(Value::Integer(20)));
    }

    /// A middle leaf emptied down to one key, with one of its neighbour links broken as `broken`
    /// says, then its last key deleted. The delete must be refused, and the leaf, its neighbours and
    /// its parent must be byte-identical to before it. Shared by the (a) and (b) tests below.
    fn a_broken_neighbour_link_refuses_the_unlink(broken: &str) {
        let (tree, _dir) = setup();
        for i in 0..16 {
            tree.insert(wide(i), Value::Integer(i)).unwrap();
        }
        let chain = leaves_by_chain(&tree);
        assert!(chain.len() >= 3, "premise: {} leaves, need a middle one", chain.len());
        let (prev, mid, next) = (chain[0].page_id, chain[1].page_id, chain[2].page_id);
        let keys = chain[1].key_arr.clone();
        for k in &keys[..keys.len() - 1] {
            tree.delete(k).unwrap();
        }
        match broken {
            "prev.next" => rewrite_leaf(&tree, prev, |l| l.next = Some(next)),
            _ => rewrite_leaf(&tree, next, |l| l.prev = Some(prev)),
        }
        let parent = parent_of(&tree, mid);
        let pages = [mid, prev, next, parent];
        let before: Vec<_> = pages.iter().map(|&p| page_bytes(&tree, p)).collect();

        let last = keys.last().unwrap();
        assert!(tree.delete(last).is_err(), "{broken} broken: the unlink was not refused");
        for (&p, b) in pages.iter().zip(&before) {
            assert!(page_bytes(&tree, p) == *b, "{broken} broken: the refused unlink rewrote page {p}");
        }
    }

    /// **F3 (a), red first against `0eda6ca`** (review 2 G4 split the one F3 test into three, so a
    /// red run shows each case). The left neighbour's `next` skips the leaf. `0eda6ca` checked no
    /// link: it spliced over the broken one and returned `Ok`.
    #[test]
    fn a_refused_unlink_with_a_broken_prev_link_writes_nothing() {
        a_broken_neighbour_link_refuses_the_unlink("prev.next");
    }

    /// **F3 (b), red first against `0eda6ca`.** The right neighbour's `prev` skips the leaf.
    #[test]
    fn a_refused_unlink_with_a_broken_next_link_writes_nothing() {
        a_broken_neighbour_link_refuses_the_unlink("next.prev");
    }

    /// **F3 (c), red first against `0eda6ca`: the cascade would empty the root.** Two leaves. The
    /// right one is unlinked, which leaves the root one child. Then a stale `next` is planted on the
    /// left one and it is emptied. Its neighbour still points back at it, so only the parents show
    /// the break, and only past the root. `0eda6ca` wrote the leaf and the splice, THEN planned the
    /// parents and refused. This is the one case that sees that order (mutant V6).
    #[test]
    fn a_refused_cascade_that_would_empty_the_root_writes_nothing() {
        let (tree, _dir) = setup();
        let mut i = 0;
        while height(&tree) < 2 {
            tree.insert(wide(i), Value::Integer(i)).unwrap();
            i += 1;
        }
        let root = tree.root_page_id.load(Ordering::Acquire);
        let (l1, l2) = match tree.read_node(root).unwrap() {
            BPlusTreePage::Internal(n) => {
                assert_eq!(n.child_ptrs.len(), 2, "premise: the first root split makes two leaves");
                (n.child_ptrs[0], n.child_ptrs[1])
            }
            BPlusTreePage::Leaf(_) => unreachable!("height 2"),
        };
        for k in tree.read_leaf(l2).unwrap().key_arr {
            tree.delete(&k).unwrap();
        }
        assert_eq!(leaves_by_descent(&tree), vec![l1], "premise: the right leaf was unlinked");
        assert_eq!(tree.read_leaf(l2).unwrap().prev, Some(l1), "premise: the unlinked leaf still points back");
        rewrite_leaf(&tree, l1, |l| l.next = Some(l2));
        let keys = tree.read_leaf(l1).unwrap().key_arr;
        for k in &keys[..keys.len() - 1] {
            tree.delete(k).unwrap();
        }
        let pages = [l1, l2, root];
        let before: Vec<_> = pages.iter().map(|&p| page_bytes(&tree, p)).collect();
        assert!(tree.delete(keys.last().unwrap()).is_err(), "an unlink that empties the root was not refused");
        for (&p, b) in pages.iter().zip(&before) {
            assert!(page_bytes(&tree, p) == *b, "the refused cascade rewrote page {p}");
        }
    }

    /// **Review 2 G6, red first against `f03e25d`.** A chain whose `prev` and `next` name the same
    /// page must be refused, not waited on. Page latches are not re-entrant, and `f03e25d` held
    /// `prev`'s write latch while it took `next`'s, so when the two are one page the unlinker waited
    /// on its own latch for ever, holding the root and stalling the whole tree.
    ///
    /// The broken leaf's `next` is set to its `prev`, which does point back at it, so the prev check
    /// passes and only the latch order can go wrong. The delete runs on its own thread; a delete
    /// that has not returned after 10 s is taken to be waiting on itself. On a red run that thread
    /// stays blocked on a latch of this test's own tree.
    #[test]
    fn a_chain_whose_prev_is_its_next_is_refused_not_waited_on() {
        let (tree, _dir) = setup();
        for i in 0..16 {
            tree.insert(wide(i), Value::Integer(i)).unwrap();
        }
        let chain = leaves_by_chain(&tree);
        assert!(chain.len() >= 3, "premise: {} leaves, need a middle one", chain.len());
        let (prev, mid) = (chain[0].page_id, chain[1].page_id);
        assert_eq!(chain[0].next, Some(mid), "premise: the left neighbour points at the leaf");
        let keys = chain[1].key_arr.clone();
        for k in &keys[..keys.len() - 1] {
            tree.delete(k).unwrap();
        }
        rewrite_leaf(&tree, mid, |l| l.next = Some(prev));
        let last = keys.last().unwrap().clone();

        let tree = Arc::new(tree);
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = Arc::clone(&tree);
        std::thread::spawn(move || {
            let _ = tx.send(worker.delete(&last).is_err());
        });
        match rx.recv_timeout(std::time::Duration::from_secs(10)) {
            Ok(refused) => assert!(refused, "an unlink whose prev is its next was not refused"),
            Err(_) => panic!("the unlink of page {mid} has not returned after 10 s: it is waiting on its own latch"),
        }
    }
}
