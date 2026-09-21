//! The copy-on-write B+tree.
//!
//! Design authority: DESIGN.md section 1.
//!
//! An update copies the path leaf -> root into the writing branch's **own** arenas and hands the
//! caller a new root page id to publish with `BranchCatalog::set_root`. Until that root pointer
//! moves, none of the shadowed pages are reachable by anybody — that is the whole commit protocol
//! (exit criterion 2), and it is why there is no undo log here.
//!
//! # The read path takes no branch
//!
//! [`CowTree::get`], [`CowTree::range_scan`] and [`CowTree::walk_pages`] take a **root page id and
//! nothing else**. They are structurally incapable of asking a parent branch anything, which is
//! the spec requirement: `child.root == parent.root` at fork, so ordinary descent already reaches
//! parent data. BranchBench (arXiv:2604.17180) measured the "not found here, ask my parent" overlay
//! pattern at up to 4000x read degradation, so this is enforced by the signature rather than by a
//! comment.
//!
//! # Copy-up, and when it stops
//!
//! The upward walk stops as soon as a level's page id did not change and no split needs
//! propagating: if a node was already private to the writer it is mutated in place, its parent's
//! pointer is still correct, and there is nothing to copy above it. A hot branch therefore
//! shadows a page once and then writes it directly, instead of re-shadowing the root on every key.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use crate::branch::types::{BranchId, Epoch, PageId};
use crate::cow::chunker;
use crate::cow::node::{self, Node, NodeMut};
use crate::cow::page_header::{stamp_checksum, PageHeader, PageType};
use crate::cow::{PageHandle, PageStore, WriteBuffer, WriteBufferEntry};
use crate::error::FerroError;
use crate::storage::disk_manager::PAGE_SIZE;

/// Descent guard. A well-formed tree is far shallower than this; exceeding it means a cycle, and
/// looping forever inside a page store is worse than failing.
const MAX_DESCENT: usize = 64;

/// One key that differs between two roots: `(key, before, after)`.
///
/// `before == None` means the key was inserted; `after == None` means it was deleted.
pub type Delta = (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>);

/// What changed between two roots, and how much of the tree had to be read to find out.
#[derive(Debug)]
pub struct TreeDiff {
    pub deltas: Vec<Delta>,
    /// Pages whose **entries were decoded**. Subtrees the two roots share are skipped whole,
    /// by page identity, so this stays proportional to what changed rather than to the tree.
    pub pages_examined: usize,
}


/// Separator keys promoted from a split, each with the page it separates off, in key order.
///
/// A vector rather than an `Option` because a content-defined split is k-way: re-chunking a leaf
/// can cut it at a content boundary *and* again at the byte cap, so more than one separator can
/// travel up from a single insert. Empty means nothing split.
type Split = Vec<(Vec<u8>, PageId)>;

/// The internal nodes walked on the way to a leaf, each with the child slot taken. `None` in the
/// slot means the leftmost child, which has no slot of its own.
type DescentPath = Vec<(PageId, Option<usize>)>;

/// The smallest key strictly greater than `key`: `key` with a zero byte appended.
///
/// # Why a leaf split does not promote the right piece's first key
///
/// The textbook separator is the first key of the right child, and with a content-defined
/// partition that is the wrong boundary. A chunk ends **at** its boundary entry, so the next
/// chunk owns every key after it — including the keys between the boundary and whatever the
/// right piece happens to hold right now. Promoting the right piece's first key gives that gap
/// to the *left* leaf, and a key arriving in it re-descends into a leaf that is already
/// terminated by a boundary, which splits off another one-entry leaf, which moves the gap, which
/// does it again. Measured before this line existed: 43 leaves from a shuffled build against 23
/// from the ascending one, strung with runs of `1, 1, 1, 1`.
///
/// Appending a zero byte names the key-space boundary itself rather than a row that sits near
/// it: every `k > key` satisfies `k >= successor_of(key)`, and `key` itself does not, so the cut
/// in the parent falls exactly where the chunker put it.
fn successor_of(key: &[u8]) -> Vec<u8> {
    let mut s = Vec::with_capacity(key.len() + 1);
    s.extend_from_slice(key);
    s.push(0);
    s
}

pub struct CowTree {
    store: Arc<dyn PageStore>,
}

impl CowTree {
    pub fn new(store: Arc<dyn PageStore>) -> Self {
        CowTree { store }
    }

    pub fn store(&self) -> &Arc<dyn PageStore> {
        &self.store
    }

    /// Create an empty tree owned by `branch`, returning its root page id.
    pub fn create(&self, branch: BranchId, epoch: Epoch) -> Result<PageId, FerroError> {
        let arena = self.store.arena_for(branch)?;
        let id = self.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch)?;
        let h = self.store.read_page(id)?;
        let mut f = h.write();
        NodeMut::new(&mut f.data).init();
        stamp_checksum(&mut f.data);
        Ok(id)
    }

    // ---- read path (no branch parameter, by design) ------------------------------------------

    /// Point lookup. Ordinary descent from `root`; never consults any other branch.
    pub fn get(&self, root: PageId, key: &[u8]) -> Result<Option<Vec<u8>>, FerroError> {
        let mut pid = root;
        for _ in 0..MAX_DESCENT {
            let h = self.store.read_page(pid)?;
            let f = h.read();
            let ty = PageHeader::read_from(&f.data)?.page_type;
            let n = Node::new(&f.data);
            match ty {
                PageType::BTreeLeaf => {
                    return Ok(match n.search(key)? {
                        Ok(i) => Some(n.value(i)?.to_vec()),
                        Err(_) => None,
                    })
                }
                PageType::BTreeInternal => {
                    pid = n.child_slot_for(key)?.1;
                }
                other => {
                    return Err(FerroError::Cow(format!(
                        "page {} is a {:?}, not a btree node",
                        pid, other
                    )))
                }
            }
        }
        Err(FerroError::Cow("btree descent exceeded the depth guard".into()))
    }

    /// Ordered scan over `[lo, hi)`. `None` bounds are unbounded.
    ///
    /// Returns a **cursor**, not a materialised `Vec`. See [`ScanCursor`] for why the peak memory
    /// of a scan is the property this signature exists to fix, and why the cursor carries a
    /// descent stack instead of following leaf sibling pointers.
    ///
    /// Nothing is read until the first `next()`, so the `Result` is about the arguments, not the
    /// tree; it is kept so a future seek-on-open cannot become a silent panic.
    pub fn range_scan(
        &self,
        root: PageId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
    ) -> Result<ScanCursor, FerroError> {
        Ok(ScanCursor::new(Arc::clone(&self.store), root, lo, hi))
    }

    /// Every page reachable from `root`, root first. This is the honest way to answer "how many
    /// pages does this branch's tree occupy", which exit criterion 1 is stated in terms of.
    pub fn walk_pages(&self, root: PageId) -> Result<Vec<PageId>, FerroError> {
        let mut out = Vec::new();
        let mut stack = vec![(root, 0usize)];
        while let Some((pid, depth)) = stack.pop() {
            if depth > MAX_DESCENT {
                return Err(FerroError::Cow("btree walk exceeded the depth guard".into()));
            }
            out.push(pid);
            let h = self.store.read_page(pid)?;
            let f = h.read();
            let ty = PageHeader::read_from(&f.data)?.page_type;
            if ty == PageType::BTreeInternal {
                for c in Node::new(&f.data).all_children()? {
                    stack.push((c, depth + 1));
                }
            }
        }
        Ok(out)
    }

    // ---- diff --------------------------------------------------------------------------------

    /// Structurally diff two roots of the same tree.
    ///
    /// This is the operation shadow paging exists to make cheap, and it is sound here **because of
    /// what DESIGN.md rules out**: there is no content addressing and there are no refcounts, so a
    /// subtree that did not change is not merely equal to its old self, it *is* the same page id.
    /// Equal page id therefore implies equal contents, and the whole subtree can be skipped
    /// without reading it. Under content addressing this shortcut would need a hash comparison;
    /// under refcounting the page would have been rewritten to bump a count and the identity would
    /// be lost.
    ///
    /// **Cost, stated precisely rather than rounded to "O(changed)":** page *identity* traversal
    /// is proportional to the tree, because the set of pages each side reaches has to be known
    /// before either can be pruned against the other. Entry *decoding* — deserialising cells, the
    /// expensive half — is proportional to what actually changed. `pages_examined` reports the
    /// second number so the claim can be checked rather than believed.
    pub fn diff(&self, base_root: PageId, head_root: PageId) -> Result<TreeDiff, FerroError> {
        // Same root is the common case for an agent that read but never wrote, and it is the
        // cleanest statement of the invariant: identical pointer, identical tree, nothing read.
        if base_root == head_root {
            return Ok(TreeDiff { deltas: Vec::new(), pages_examined: 0 });
        }

        let base_pages: HashSet<PageId> = self.walk_pages(base_root)?.into_iter().collect();
        let head_pages: HashSet<PageId> = self.walk_pages(head_root)?.into_iter().collect();

        let mut examined = 0usize;
        let mut before = BTreeMap::new();
        self.collect_unshared(base_root, &head_pages, 0, &mut before, &mut examined)?;
        let mut after = BTreeMap::new();
        self.collect_unshared(head_root, &base_pages, 0, &mut after, &mut examined)?;

        // A key living in a shared leaf is absent from both maps and correctly reports no change.
        // A key in a leaf that WAS copied appears in both maps; if its value is untouched the two
        // sides are equal and it is filtered here, which is what keeps an unchanged neighbour of
        // an edited row out of the changeset.
        let keys: BTreeSet<&Vec<u8>> = before.keys().chain(after.keys()).collect();
        let mut deltas = Vec::new();
        for k in keys {
            let b = before.get(k);
            let a = after.get(k);
            if b != a {
                deltas.push((k.clone(), b.cloned(), a.cloned()));
            }
        }
        Ok(TreeDiff { deltas, pages_examined: examined })
    }

    /// Gather leaf entries from every subtree of `pid` that `other` does not also contain.
    fn collect_unshared(
        &self,
        pid: PageId,
        other: &HashSet<PageId>,
        depth: usize,
        out: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        examined: &mut usize,
    ) -> Result<(), FerroError> {
        if depth > MAX_DESCENT {
            return Err(FerroError::Cow("btree diff exceeded the depth guard".into()));
        }
        // The pruning step. Shared page id => shared subtree => nothing in it changed.
        if other.contains(&pid) {
            return Ok(());
        }
        // Read the node, take what is needed, then drop the handle before recursing so a deep
        // tree does not pin one frame per level.
        let children = {
            let h = self.store.read_page(pid)?;
            let f = h.read();
            let ty = PageHeader::read_from(&f.data)?.page_type;
            let n = Node::new(&f.data);
            *examined += 1;
            if ty == PageType::BTreeLeaf {
                for (k, v) in n.leaf_entries()? {
                    out.insert(k, v);
                }
                Vec::new()
            } else {
                n.all_children()?
            }
        };
        for c in children {
            self.collect_unshared(c, other, depth + 1, out, examined)?;
        }
        Ok(())
    }

    // ---- write path --------------------------------------------------------------------------

    /// Insert or overwrite `key`. Returns the branch's **new root page id**, which the caller must
    /// publish; the tree is not visible to anyone until that pointer moves.
    pub fn insert(
        &self,
        root: PageId,
        branch: BranchId,
        epoch: Epoch,
        key: &[u8],
        value: &[u8],
    ) -> Result<PageId, FerroError> {
        if node::leaf_entry_bytes(key, value) > node::MAX_ENTRY_BYTES {
            return Err(FerroError::Cow(format!(
                "entry of {} bytes exceeds the {}-byte limit for a 4KB page",
                node::leaf_entry_bytes(key, value),
                node::MAX_ENTRY_BYTES
            )));
        }
        let (path, leaf_id) = self.descend(root, key)?;
        let cp = self.store.cow_page(leaf_id, branch, epoch)?;
        let new_leaf = cp.page_id;
        let split = self.leaf_put(&cp.handle, branch, epoch, key, value)?;
        drop(cp);
        self.relink_up(root, path, leaf_id, new_leaf, split, branch, epoch)
    }

    /// Remove `key` if present. Returns the new root page id (unchanged when the key was absent —
    /// a delete that hits nothing must not shadow anything).
    pub fn delete(
        &self,
        root: PageId,
        branch: BranchId,
        epoch: Epoch,
        key: &[u8],
    ) -> Result<PageId, FerroError> {
        let (path, leaf_id) = self.descend(root, key)?;
        {
            let h = self.store.read_page(leaf_id)?;
            let f = h.read();
            if Node::new(&f.data).search(key)?.is_err() {
                return Ok(root);
            }
        }
        let cp = self.store.cow_page(leaf_id, branch, epoch)?;
        let new_leaf = cp.page_id;
        {
            let mut f = cp.handle.write();
            let mut n = NodeMut::new(&mut f.data);
            let found = n.view().search(key)?;
            if let Ok(i) = found {
                n.remove_at(i)?;
            }
            stamp_checksum(&mut f.data);
        }
        drop(cp);
        self.relink_up(root, path, leaf_id, new_leaf, Vec::new(), branch, epoch)
    }

    /// Apply a whole [`WriteBuffer`] and return the new root.
    ///
    /// A branch that dies before this is called has allocated **zero pages** — the common case for
    /// an abandoned agent task, and why reaping one is nearly free.
    pub fn flush_write_buffer(
        &self,
        root: PageId,
        branch: BranchId,
        epoch: Epoch,
        buffer: &mut WriteBuffer,
    ) -> Result<PageId, FerroError> {
        let mut current = root;
        for (key, entry) in buffer.entries.iter() {
            current = match entry {
                WriteBufferEntry::Put(v) => self.insert(current, branch, epoch, key, v)?,
                WriteBufferEntry::Delete => self.delete(current, branch, epoch, key)?,
            };
        }
        buffer.clear();
        Ok(current)
    }

    /// Probe the write buffer before descending. `None` from the buffer means "not buffered", not
    /// "absent", so the tree is still consulted; a buffered `Delete` shadows the tree.
    pub fn get_buffered(
        &self,
        root: PageId,
        buffer: &WriteBuffer,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, FerroError> {
        match buffer.probe(key) {
            Some(WriteBufferEntry::Put(v)) => Ok(Some(v.clone())),
            Some(WriteBufferEntry::Delete) => Ok(None),
            None => self.get(root, key),
        }
    }

    // ---- internals ---------------------------------------------------------------------------

    /// Walk to the leaf that owns `key`, recording each internal node and the child slot taken.
    /// `None` in the slot means the leftmost child, which has no slot of its own.
    fn descend(
        &self,
        root: PageId,
        key: &[u8],
    ) -> Result<(DescentPath, PageId), FerroError> {
        let mut path = Vec::new();
        let mut pid = root;
        for _ in 0..MAX_DESCENT {
            let next = {
                let h = self.store.read_page(pid)?;
                let f = h.read();
                let ty = PageHeader::read_from(&f.data)?.page_type;
                match ty {
                    PageType::BTreeLeaf => return Ok((path, pid)),
                    PageType::BTreeInternal => Node::new(&f.data).child_slot_for(key)?,
                    other => {
                        return Err(FerroError::Cow(format!(
                            "page {} is a {:?}, not a btree node",
                            pid, other
                        )))
                    }
                }
            };
            path.push((pid, next.0));
            pid = next.1;
        }
        Err(FerroError::Cow("btree descent exceeded the depth guard".into()))
    }

    /// Write one entry into an already-shadowed leaf, re-chunking it if the content says so.
    ///
    /// # Why a leaf can split when it is nowhere near full
    ///
    /// A B+tree splits on overflow, which makes every leaf boundary a fact about *when* the page
    /// filled. This one splits where [`chunker::is_boundary`] says a chunk ends, which makes the
    /// boundary a fact about the bytes — the structural invariance a prolly tree has and a
    /// textbook B+tree does not (see `cow::chunker` for what that property buys here). Overflow
    /// is still handled, but as the escape hatch that keeps a page from bursting rather than as
    /// the thing that decides where leaves end.
    ///
    /// # Why an insert can never need more than a local re-chunk
    ///
    /// The boundary predicate reads one entry and nothing else, so inserting a key cannot move or
    /// erase any other entry's boundary — it can only add its own. The leaf therefore holds at
    /// most one interior boundary after the insert (the new entry's), and re-chunking the leaf's
    /// own entries is enough; there is no cascade into the neighbour this tree has no pointer to.
    ///
    /// **The exception, stated rather than hidden:** replacing an existing key's value *can*
    /// erase a boundary, and so can `delete`. That leaves a leaf whose last entry is no longer a
    /// boundary, which is a partition the chunker would not have chosen — repairing it means
    /// merging with the right neighbour, which needs the sibling access this layout deliberately
    /// does not have. Fresh inserts, the path that builds a tree, are exact.
    fn leaf_put(
        &self,
        handle: &PageHandle,
        branch: BranchId,
        epoch: Epoch,
        key: &[u8],
        value: &[u8],
    ) -> Result<Split, FerroError> {
        let entries = {
            let mut f = handle.write();
            let mut n = NodeMut::new(&mut f.data);
            let cell = node::leaf_cell(key, value);
            let found = n.view().search(key)?;
            let at = match found {
                Ok(i) => i,
                Err(i) => i,
            };
            let fits = match found {
                Ok(i) => n.replace_cell_at(i, &cell)?,
                Err(i) => n.insert_cell_at(i, &cell)?,
            };
            if fits {
                // Fast path: it went in without putting a chunk boundary in the leaf's interior,
                // so the partition is unchanged and nothing above needs to know.
                //
                // Exactly one entry can change status, and which one depends on where this landed.
                // An interior insert can only have brought its own boundary. An **append** brings
                // none of its own — it is the entry it displaced from the end that has just become
                // interior, and missing that is how an ascending build silently swallows every
                // boundary it appends.
                let interior_boundary = {
                    let v = n.view();
                    let last = v.count() - 1;
                    if at == last {
                        v.count() >= 2
                            && chunker::is_boundary(v.key(last - 1)?, v.value(last - 1)?)
                    } else {
                        chunker::is_boundary(key, value)
                    }
                };
                if !interior_boundary {
                    stamp_checksum(&mut f.data);
                    return Ok(Vec::new());
                }
                n.view().leaf_entries()?
            } else {
                let mut entries = n.view().leaf_entries()?;
                match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                    Ok(i) => entries[i].1 = value.to_vec(),
                    Err(i) => entries.insert(i, (key.to_vec(), value.to_vec())),
                }
                entries
            }
        };

        let sizes: Vec<usize> =
            entries.iter().map(|(k, v)| node::leaf_entry_bytes(k, v)).collect();
        let hashes: Vec<u32> = entries.iter().map(|(k, v)| chunker::cell_hash(k, v)).collect();
        let cuts = chunker::leaf_cuts(&sizes, &hashes, node::NODE_CAPACITY);
        if cuts.is_empty() {
            // The content says this is one chunk. It reached here only because the entry did not
            // fit the page as laid out, so rewriting the leaf compacts it.
            let mut f = handle.write();
            NodeMut::new(&mut f.data).fill_leaf(&entries)?;
            stamp_checksum(&mut f.data);
            return Ok(Vec::new());
        }

        // Piece 0 stays in the page the caller already shadowed; the rest are novel pages whose
        // separators travel up. Allocate before overwriting, so a failure to allocate leaves the
        // leaf exactly as it was rather than truncated to its first piece.
        let mut promoted: Split = Vec::with_capacity(cuts.len());
        for (j, &start) in cuts.iter().enumerate() {
            let end = cuts.get(j + 1).copied().unwrap_or(entries.len());
            // `alloc_for`, not a single `arena_for` hoisted out of the loop: a k-way split can
            // ask for several pages, and an `ArenaId` captured once refuses the moment its
            // extent fills rather than rolling over. See `PageStore::alloc_for`.
            let id = self.store.alloc_for(branch, PageType::BTreeLeaf, epoch)?;
            let rh = self.store.read_page(id)?;
            let mut f = rh.write();
            NodeMut::new(&mut f.data).fill_leaf(&entries[start..end])?;
            stamp_checksum(&mut f.data);
            promoted.push((successor_of(&entries[start - 1].0), id));
        }
        {
            let mut f = handle.write();
            NodeMut::new(&mut f.data).fill_leaf(&entries[..cuts[0]])?;
            stamp_checksum(&mut f.data);
        }
        Ok(promoted)
    }

    /// Point an already-shadowed internal node at its new child, and absorb the separators
    /// promoted from below. Returns separators of its own if it had to split in turn.
    ///
    /// **This level is still byte-balanced, deliberately.** An internal cell carries a `PageId`,
    /// and page ids are handed out in allocation order, so hashing an internal cell would make
    /// the boundary depend on exactly the history the leaf level has just been freed from.
    /// Content-defining this level needs child references that do not name an allocation, which
    /// is a separate change; what it inherits today is an invariant leaf partition underneath it.
    ///
    /// A content-defined leaf split promotes as many separators as it cut, not one, so both the
    /// absorb and the split here are k-way.
    fn internal_relink(
        &self,
        handle: &PageHandle,
        slot: Option<usize>,
        child: PageId,
        promoted: Split,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<Split, FerroError> {
        let at = match slot {
            None => 0,
            Some(i) => i + 1,
        };
        // Checked before anything is mutated: bailing out after the child pointer has moved but
        // before the checksum is restamped would leave a page that fails verification.
        if promoted
            .iter()
            .any(|(sep, _)| node::internal_entry_bytes(sep) > node::MAX_ENTRY_BYTES)
        {
            return Err(FerroError::Cow("separator key is too large for a 4KB page".into()));
        }
        let (leftmost, entries) = {
            let mut f = handle.write();
            let mut n = NodeMut::new(&mut f.data);
            match slot {
                None => n.set_leftmost(child),
                Some(i) => n.set_child(i, child)?,
            }
            if promoted.is_empty() {
                stamp_checksum(&mut f.data);
                return Ok(Vec::new());
            }
            // The separators belong at consecutive slots from `at`. Place as many as the page
            // takes; `insert_cell_at` compacts before it refuses, so the first refusal means the
            // node is genuinely full and the rest have to go through the split path.
            let mut placed = 0usize;
            for (j, (sep, right)) in promoted.iter().enumerate() {
                if !n.insert_cell_at(at + j, &node::internal_cell(sep, *right))? {
                    break;
                }
                placed = j + 1;
            }
            if placed == promoted.len() {
                stamp_checksum(&mut f.data);
                return Ok(Vec::new());
            }
            let mut entries = n.view().internal_entries()?;
            for (j, p) in promoted.iter().enumerate().skip(placed) {
                entries.insert(at + j, p.clone());
            }
            (n.leftmost(), entries)
        };

        let sizes: Vec<usize> =
            entries.iter().map(|(k, _)| node::internal_entry_bytes(k)).collect();
        let mut cuts = Vec::new();
        chunker::balanced_cuts(&sizes, node::NODE_CAPACITY, 0, &mut cuts);
        if cuts.is_empty() {
            // Everything fits once the cell heap is rewritten without its garbage.
            let mut f = handle.write();
            NodeMut::new(&mut f.data).fill_internal(leftmost, &entries)?;
            stamp_checksum(&mut f.data);
            return Ok(Vec::new());
        }

        // Each cut promotes the entry it lands on: its key becomes the parent's separator and its
        // child becomes the next node's leftmost, so that entry belongs to neither side.
        let mut out: Split = Vec::with_capacity(cuts.len());
        for (j, &s) in cuts.iter().enumerate() {
            let end = cuts.get(j + 1).copied().unwrap_or(entries.len());
            let (middle_key, right_leftmost) = entries[s].clone();
            let id = self.store.alloc_for(branch, PageType::BTreeInternal, epoch)?;
            let rh = self.store.read_page(id)?;
            let mut f = rh.write();
            NodeMut::new(&mut f.data).fill_internal(right_leftmost, &entries[s + 1..end])?;
            stamp_checksum(&mut f.data);
            out.push((middle_key, id));
        }
        {
            let mut f = handle.write();
            NodeMut::new(&mut f.data).fill_internal(leftmost, &entries[..cuts[0]])?;
            stamp_checksum(&mut f.data);
        }
        Ok(out)
    }

    /// Copy the path back up to the root, stopping the moment nothing above needs to change.
    #[allow(clippy::too_many_arguments)]
    fn relink_up(
        &self,
        root: PageId,
        path: DescentPath,
        mut child_old: PageId,
        mut child_new: PageId,
        mut promoted: Split,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        for (parent_id, slot) in path.into_iter().rev() {
            if child_new == child_old && promoted.is_empty() {
                // the node was private and was mutated in place: its parent already points at it
                return Ok(root);
            }
            let cp = self.store.cow_page(parent_id, branch, epoch)?;
            let new_parent = cp.page_id;
            let taken = std::mem::take(&mut promoted);
            promoted = self.internal_relink(&cp.handle, slot, child_new, taken, branch, epoch)?;
            drop(cp);
            child_old = parent_id;
            child_new = new_parent;
        }

        if !promoted.is_empty() {
            let arena = self.store.arena_for(branch)?;
            let new_root = self.store.alloc_in_arena(arena, PageType::BTreeInternal, epoch)?;
            let h = self.store.read_page(new_root)?;
            let mut f = h.write();
            NodeMut::new(&mut f.data).fill_internal(child_new, &promoted)?;
            stamp_checksum(&mut f.data);
            return Ok(new_root);
        }
        if child_new == child_old {
            return Ok(root);
        }
        Ok(child_new)
    }
}

// ---- the scan cursor -----------------------------------------------------------------------

/// One level of a cursor's root-to-leaf path: the children of that node which survived range
/// pruning, and which of them the cursor visits next.
struct Level {
    children: Vec<PageId>,
    next: usize,
}

/// A lazy, ordered cursor over `[lo, hi)`. One leaf page at a time; peak memory is a property of
/// the tree's *depth*, not of the rows it returns.
///
/// # Why this is a cursor and not a `Vec`
///
/// `range_scan` used to materialise every matching entry before returning, and
/// `PagedRows::scan_table` then built a second `Vec` of decoded rows from it. At 10^6 branches
/// with per-branch state, a scan whose ceiling the consumer cannot set is a hard memory wall.
///
/// Peak **live heap**, measured with a tracking global allocator — instrument
/// `examples/cow_scan_memory.rs`, raw output `bench/s23_cow_scan_memory_{before,after}.txt`:
///
/// ```text
///                       10^6 rows, before  ->  after
///   full scan, streamed        84.36 MiB   ->   5,640 B
///   scan_table, streamed      154.81 MiB   ->   5,640 B
///   first ten rows             84.24 MiB   ->   6,008 B
///   collect the whole table    84.36 MiB   ->   84.24 MiB   (unchanged, and must be)
/// ```
///
/// The first-ten-rows row is the one that states the defect: it was indistinguishable from the
/// full scan, because the size of the answer was decided before the caller ever saw a row. The
/// last row is the control — asking for the table in RAM still costs the table in RAM, so the
/// change moved the decision to the caller rather than hiding a cost somewhere else.
///
/// The streamed figures are flat from 10^4 to 10^6 rows (5,024 → 5,540 → 5,640 B): one 4 KiB
/// leaf buffer, the descent stack, and the entry being yielded. The first scan after a build
/// reads higher (160,120 B at 10^6) and that is the buffer pool's ARC bookkeeping settling, not
/// the cursor — the identical drain repeated immediately costs 5,640 B, which is why the
/// measurement runs it twice rather than asserting which cost is which.
///
/// # Why a descent stack rather than leaf sibling pointers
///
/// The textbook streaming cursor saves a leaf position and follows a `next_leaf` link. That is
/// ruled out here by the page layout, not by taste: with no sibling links, shadowing one leaf
/// copies one page; with them, the left neighbour's link must be rewritten too, and a one-key
/// update cascades into a copy of the whole leaf level. `cow::node` rejects them in its module
/// doc for exactly this reason, and LMDB — the design this store follows — iterates with a
/// cursor stack for the same reason. So the sibling link is replaced by an explicit stack of the
/// path from the root, which is the recursion `scan_into` used to run on the call stack, reified
/// so it can be suspended between `next()` calls.
///
/// Two other shapes were considered and lost:
///
/// - **Internal iteration** (`for_each(|k, v| ...)`) is also O(page) and is a smaller change, but
///   it inverts control: the consumer cannot stop early without threading a control-flow enum
///   through the callback, cannot interleave two scans (a merge join across branches), and does
///   not fit the pull-based `Executor::next()` shape the rest of the engine already uses.
///   "Bounded by the consumer's appetite" requires the consumer to hold the pull.
/// - **Chunked resume** (`range_scan_from(lo, limit)` returning a `Vec` and a resume key) bounds
///   memory too, but re-descends from the root for every chunk and makes each caller responsible
///   for writing the resume loop correctly. That is the right shape for a network boundary, not
///   for an in-process iterator.
///
/// # What it does *not* change
///
/// The leaf is copied out under the same read guard the eager scan decoded it under, so a
/// concurrent in-place write to an already-private leaf can no more be observed half-applied
/// than it could before. Per-leaf atomicity is preserved exactly; nothing here makes the scan a
/// snapshot that it was not already.
pub struct ScanCursor {
    store: Arc<dyn PageStore>,
    lo: Option<Vec<u8>>,
    hi: Option<Vec<u8>>,
    /// The suspended descent, root level first.
    stack: Vec<Level>,
    /// A private copy of the leaf currently being read from. This is the whole memory budget:
    /// one page, allocated once per cursor and reused for every leaf.
    leaf: Box<[u8; PAGE_SIZE]>,
    slot: usize,
    count: usize,
    /// Fused. Set on exhaustion and on error, so a caller that keeps polling after a failure
    /// gets `None` rather than the same page fault forever.
    done: bool,
}

impl ScanCursor {
    fn new(store: Arc<dyn PageStore>, root: PageId, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Self {
        let leaf: Box<[u8; PAGE_SIZE]> = vec![0u8; PAGE_SIZE]
            .into_boxed_slice()
            .try_into()
            .expect("a PAGE_SIZE vector is a PAGE_SIZE array");
        ScanCursor {
            store,
            lo: lo.map(|b| b.to_vec()),
            hi: hi.map(|b| b.to_vec()),
            // Seeding the root as a one-child level means the descent loop has no special first
            // step, and the depth guard counts the same levels the recursion used to count.
            stack: vec![Level { children: vec![root], next: 0 }],
            leaf,
            slot: 0,
            count: 0,
            done: false,
        }
    }

    /// The entry at slot `i` of the current leaf, or `None` when it is at or past `hi`.
    ///
    /// Reaching `hi` ends the **whole** scan, not just this leaf: a B+tree is ordered across
    /// leaves as well as within one, so every later entry is greater still. The eager scan only
    /// `break`ed out of the leaf and left the rest to range pruning, which returned the same
    /// entries while visiting more pages.
    fn entry_at(&self, i: usize) -> Result<Option<(Vec<u8>, Vec<u8>)>, FerroError> {
        let n = Node::new(&self.leaf);
        let k = n.key(i)?;
        if self.hi.as_deref().is_some_and(|h| k >= h) {
            return Ok(None);
        }
        Ok(Some((k.to_vec(), n.value(i)?.to_vec())))
    }

    /// A child is worth visiting when its key range can overlap `[lo, hi)`.
    fn children_in_range(&self, n: &Node<'_>) -> Result<Vec<PageId>, FerroError> {
        let (lo, hi) = (self.lo.as_deref(), self.hi.as_deref());
        let mut keep = Vec::with_capacity(n.count() + 1);
        for (ci, child) in n.all_children()?.into_iter().enumerate() {
            // child ci covers keys >= key(ci-1) and < key(ci)
            let lower = if ci == 0 { None } else { Some(n.key(ci - 1)?) };
            let upper = if ci < n.count() { Some(n.key(ci)?) } else { None };
            if upper.zip(lo).is_some_and(|(u, l)| u <= l) {
                continue;
            }
            if lower.zip(hi).is_some_and(|(lw, h)| lw >= h) {
                continue;
            }
            keep.push(child);
        }
        Ok(keep)
    }

    /// Walk the stack to the next leaf that can hold a matching key. `false` means the scan is
    /// over; the cursor holds at most one page and one path while it runs.
    fn advance_leaf(&mut self) -> Result<bool, FerroError> {
        loop {
            // The node about to be visited sits at depth `stack.len() - 1`, so this is the same
            // bound the recursive `scan_into` applied to its `depth` parameter.
            if self.stack.len() > MAX_DESCENT + 1 {
                return Err(FerroError::Cow("btree scan exceeded the depth guard".into()));
            }
            let Some(top) = self.stack.last_mut() else {
                return Ok(false);
            };
            if top.next >= top.children.len() {
                self.stack.pop();
                continue;
            }
            let pid = top.children[top.next];
            top.next += 1;

            // ONE read guard for the whole page, as the eager scan had. The page type and the
            // bytes must be sampled together: a second `h.read()` would let an in-place write to
            // an already-private leaf land between the two, and the cursor would be built from a
            // page observed at two different moments.
            //
            // The `drop(h)` is the enforcement, and it is deliberate. A comment asking the next
            // editor not to re-read the handle is advice; dropping it means a second read does
            // not COMPILE. That property is not observable from a test here — it lives inside
            // the frame lock, not in anything `PageStore` exposes, and a concurrency test for it
            // would be a detector that cannot be forced to fire — so the structure has to carry
            // it. (`bench/s23_fire_check.txt`, mutant D.) Releasing the pin here rather than at
            // the end of the iteration also matches `storage::range_scan::RangeScanner`, which
            // copies its leaf out and unpins immediately for the same reason.
            let h = self.store.read_page(pid)?;
            let step = {
                let f = h.read();
                match PageHeader::read_from(&f.data)?.page_type {
                    PageType::BTreeLeaf => {
                        self.leaf.copy_from_slice(&f.data);
                        Step::Leaf
                    }
                    PageType::BTreeInternal => {
                        Step::Internal(self.children_in_range(&Node::new(&f.data))?)
                    }
                    other => Step::NotANode(other),
                }
            };
            drop(h);
            match step {
                Step::Leaf => {
                    let (count, slot) = {
                        let n = Node::new(&self.leaf);
                        let slot = match self.lo.as_deref() {
                            // Equivalent to skipping entries below `lo` one at a time, which is
                            // what the eager scan did, but in log(count) comparisons.
                            Some(lo) => match n.search(lo)? {
                                Ok(i) => i,
                                Err(i) => i,
                            },
                            None => 0,
                        };
                        (n.count(), slot)
                    };
                    self.count = count;
                    self.slot = slot;
                    return Ok(true);
                }
                Step::Internal(children) => self.stack.push(Level { children, next: 0 }),
                Step::NotANode(other) => {
                    return Err(FerroError::Cow(format!(
                        "page {} is a {:?}, not a btree node",
                        pid, other
                    )))
                }
            }
        }
    }
}

/// What one page of the descent turned out to be, decided under a single read guard so the page
/// type and the page contents cannot be sampled from two different moments.
enum Step {
    Leaf,
    Internal(Vec<PageId>),
    NotANode(PageType),
}

impl Iterator for ScanCursor {
    type Item = Result<(Vec<u8>, Vec<u8>), FerroError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if self.slot < self.count {
                let i = self.slot;
                self.slot += 1;
                return match self.entry_at(i) {
                    Ok(Some(kv)) => Some(Ok(kv)),
                    Ok(None) => {
                        self.done = true;
                        None
                    }
                    Err(e) => {
                        self.done = true;
                        Some(Err(e))
                    }
                };
            }
            match self.advance_leaf() {
                Ok(true) => {}
                Ok(false) => {
                    self.done = true;
                    return None;
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

#[cfg(test)]
mod diff_tests {
    use super::*;
    use crate::branch::arena::ArenaPageStore;
    use crate::branch::catalog::LogBranchCatalog;
    use crate::branch::types::LeaseDeadline;
    use crate::branch::BranchCatalog;
    use crate::buffer::buffer_pool::BufferPoolManager;
    use crate::storage::disk_manager::DiskManager;

    const ARENA_BASE: u32 = 1024;

    fn tree() -> (tempfile::TempDir, Arc<LogBranchCatalog>, CowTree) {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("diff.db"))
            .unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let pool = Arc::new(BufferPoolManager::new(dm));
        let catalog = Arc::new(LogBranchCatalog::in_memory(1));
        let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog) as Arc<dyn BranchCatalog>, ARENA_BASE).unwrap());
        let t = CowTree::new(store as Arc<dyn PageStore>);
        (dir, catalog, t)
    }

    fn k(n: u32) -> Vec<u8> {
        n.to_be_bytes().to_vec()
    }

    /// A child branch to write on.
    ///
    /// Edits have to happen on a *forked* branch, not on the branch that built the tree.
    /// `cow_page` mutates in place when the page is private to the writing arena, which is what
    /// keeps a hot branch from shadowing the same page on every write — so writing to trunk right
    /// after filling trunk rewrites the tree destructively and leaves one version, not two. The
    /// fork moves the privacy barrier and forces a real copy. This is also the only case that
    /// matters: a diff exists to compare an agent's branch against its fork point.
    fn child(cat: &LogBranchCatalog) -> BranchId {
        cat.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap().branch_id
    }

    /// Fill a root with `n` keys and return it.
    fn filled(t: &CowTree, cat: &LogBranchCatalog, n: u32) -> PageId {
        let e = cat.next_epoch();
        let mut root = t.create(BranchId::TRUNK, e).unwrap();
        for i in 0..n {
            root = t.insert(root, BranchId::TRUNK, e, &k(i), format!("v{i}").as_bytes()).unwrap();
        }
        root
    }

    #[test]
    fn identical_roots_diff_to_nothing_without_reading_a_page() {
        let (_d, cat, t) = tree();
        let root = filled(&t, &cat, 200);
        let d = t.diff(root, root).unwrap();
        assert!(d.deltas.is_empty());
        assert_eq!(d.pages_examined, 0, "an unchanged branch must cost nothing to diff");
    }

    #[test]
    fn an_insert_an_update_and_a_delete_each_report_both_sides() {
        let (_d, cat, t) = tree();
        let base = filled(&t, &cat, 50);
        let b = child(&cat);
        let e = cat.next_epoch();

        let head = t.insert(base, b, e, &k(999), b"new").unwrap();
        let d = t.diff(base, head).unwrap();
        assert_eq!(d.deltas, vec![(k(999), None, Some(b"new".to_vec()))], "insert");

        let head = t.insert(base, b, e, &k(7), b"changed").unwrap();
        let d = t.diff(base, head).unwrap();
        assert_eq!(
            d.deltas,
            vec![(k(7), Some(b"v7".to_vec()), Some(b"changed".to_vec()))],
            "update"
        );

        let head = t.delete(base, b, e, &k(7)).unwrap();
        let d = t.diff(base, head).unwrap();
        assert_eq!(d.deltas, vec![(k(7), Some(b"v7".to_vec()), None)], "delete");
    }

    /// The property that makes the diff usable as a changeset: editing one row must not report
    /// the rows that happened to share its leaf. Copy-on-write rewrites the whole leaf, so every
    /// neighbour appears on BOTH sides of the comparison and has to be filtered by value.
    #[test]
    fn unchanged_neighbours_of_an_edited_row_are_not_reported() {
        let (_d, cat, t) = tree();
        let base = filled(&t, &cat, 300);
        let b = child(&cat);
        let e = cat.next_epoch();
        let head = t.insert(base, b, e, &k(150), b"only-this-one").unwrap();

        let d = t.diff(base, head).unwrap();
        assert_eq!(
            d.deltas.len(),
            1,
            "one edit reported {} changes; a copied leaf's untouched neighbours leaked in",
            d.deltas.len()
        );
        assert_eq!(d.deltas[0].0, k(150));
    }

    /// The claim shadow paging is for, measured: one edit in a large tree must not decode the
    /// tree. Stated as a ratio rather than an absolute so it tracks the tree rather than a
    /// hand-tuned constant.
    ///
    /// 4000 keys, not 400: the vacuity guard below rejected 400, which is only a 4-page tree —
    /// "decoded less than a quarter of it" is not a claim worth making about 4 pages.
    #[test]
    fn one_edit_in_a_large_tree_decodes_only_a_fraction_of_it() {
        let (_d, cat, t) = tree();
        let base = filled(&t, &cat, 4000);
        let total = t.walk_pages(base).unwrap().len();
        assert!(total > 16, "tree is only {total} pages; the measurement would be vacuous");

        let b = child(&cat);
        let e = cat.next_epoch();
        let head = t.insert(base, b, e, &k(2000), b"edited").unwrap();

        let d = t.diff(base, head).unwrap();
        // Printed, not just asserted: the number is the point of the test.
        println!(
            "    diff: {} of {total} pages decoded for a 1-row change in a {}-key tree",
            d.pages_examined, 4000
        );
        assert_eq!(d.deltas.len(), 1);
        assert!(
            d.pages_examined * 4 < total,
            "decoded {} of {total} pages for a one-row change; the shared-subtree pruning is not \
             working",
            d.pages_examined
        );
        assert!(d.pages_examined > 0, "a real change must have decoded something");
    }

    /// Diffing is symmetric in structure but not in direction: reversing the roots must turn
    /// inserts into deletes, not report nothing.
    #[test]
    fn reversing_the_roots_reverses_each_delta() {
        let (_d, cat, t) = tree();
        let base = filled(&t, &cat, 40);
        let b = child(&cat);
        let e = cat.next_epoch();
        let head = t.insert(base, b, e, &k(500), b"x").unwrap();

        let fwd = t.diff(base, head).unwrap();
        let rev = t.diff(head, base).unwrap();
        assert_eq!(fwd.deltas, vec![(k(500), None, Some(b"x".to_vec()))]);
        assert_eq!(rev.deltas, vec![(k(500), Some(b"x".to_vec()), None)]);
    }

    #[test]
    fn many_scattered_edits_are_all_reported_exactly_once() {
        let (_d, cat, t) = tree();
        let base = filled(&t, &cat, 300);
        let b = child(&cat);
        let e = cat.next_epoch();
        let mut head = base;
        let edited: Vec<u32> = (0..300).step_by(37).collect();
        for i in &edited {
            head = t.insert(head, b, e, &k(*i), b"E").unwrap();
        }
        let d = t.diff(base, head).unwrap();
        let got: Vec<u32> = d
            .deltas
            .iter()
            .map(|(key, _, _)| u32::from_be_bytes(key[..4].try_into().unwrap()))
            .collect();
        assert_eq!(got, edited, "scattered edits were dropped, duplicated or reordered");
        assert!(d.deltas.iter().all(|(_, b, a)| b.is_some() && a.as_deref() == Some(b"E")));
    }
}
