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
//! parent data. BranchBench measured the "not found here, ask my parent" overlay pattern at up to
//! 5400x read degradation, so this is enforced by the signature rather than by a comment.
//!
//! # Copy-up, and when it stops
//!
//! The upward walk stops as soon as a level's page id did not change and no split needs
//! propagating: if a node was already private to the writer it is mutated in place, its parent's
//! pointer is still correct, and there is nothing to copy above it. A hot branch therefore
//! shadows a page once and then writes it directly, instead of re-shadowing the root on every key.

use std::sync::Arc;

use crate::branch::types::{BranchId, Epoch, PageId};
use crate::cow::node::{self, Node, NodeMut};
use crate::cow::page_header::{stamp_checksum, PageHeader, PageType};
use crate::cow::{PageHandle, PageStore, WriteBuffer, WriteBufferEntry};
use crate::error::FerroError;

/// Descent guard. A well-formed tree is far shallower than this; exceeding it means a cycle, and
/// looping forever inside a page store is worse than failing.
const MAX_DESCENT: usize = 64;

/// A separator key promoted from a split, together with the page it separates off.
type Split = Option<(Vec<u8>, PageId)>;

/// The internal nodes walked on the way to a leaf, each with the child slot taken. `None` in the
/// slot means the leftmost child, which has no slot of its own.
type DescentPath = Vec<(PageId, Option<usize>)>;

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
    /// Uses a descent stack rather than leaf sibling pointers: in a shadow-paging tree a
    /// `next_leaf` link would force shadowing the left neighbour of every leaf that is copied,
    /// cascading a one-key update into a whole-level copy.
    pub fn range_scan(
        &self,
        root: PageId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
    ) -> Result<node::LeafEntries, FerroError> {
        let mut out = Vec::new();
        self.scan_into(root, lo, hi, 0, &mut out)?;
        Ok(out)
    }

    fn scan_into(
        &self,
        pid: PageId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        depth: usize,
        out: &mut node::LeafEntries,
    ) -> Result<(), FerroError> {
        if depth > MAX_DESCENT {
            return Err(FerroError::Cow("btree scan exceeded the depth guard".into()));
        }
        let children: Vec<PageId> = {
            let h = self.store.read_page(pid)?;
            let f = h.read();
            let ty = PageHeader::read_from(&f.data)?.page_type;
            let n = Node::new(&f.data);
            match ty {
                PageType::BTreeLeaf => {
                    for i in 0..n.count() {
                        let k = n.key(i)?;
                        if lo.map(|b| k < b).unwrap_or(false) {
                            continue;
                        }
                        if hi.map(|b| k >= b).unwrap_or(false) {
                            break;
                        }
                        out.push((k.to_vec(), n.value(i)?.to_vec()));
                    }
                    return Ok(());
                }
                PageType::BTreeInternal => {
                    // A child is worth visiting when its key range can overlap [lo, hi).
                    let mut keep = Vec::with_capacity(n.count() + 1);
                    let all = n.all_children()?;
                    for (ci, child) in all.into_iter().enumerate() {
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
                    keep
                }
                other => {
                    return Err(FerroError::Cow(format!(
                        "page {} is a {:?}, not a btree node",
                        pid, other
                    )))
                }
            }
        };
        for c in children {
            self.scan_into(c, lo, hi, depth + 1, out)?;
        }
        Ok(())
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
        self.relink_up(root, path, leaf_id, new_leaf, None, branch, epoch)
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

    /// Write one entry into an already-shadowed leaf, splitting it if it will not fit.
    fn leaf_put(
        &self,
        handle: &PageHandle,
        branch: BranchId,
        epoch: Epoch,
        key: &[u8],
        value: &[u8],
    ) -> Result<Split, FerroError> {
        let mut entries = {
            let mut f = handle.write();
            let mut n = NodeMut::new(&mut f.data);
            let cell = node::leaf_cell(key, value);
            let found = n.view().search(key)?;
            let fits = match found {
                Ok(i) => n.replace_cell_at(i, &cell)?,
                Err(i) => n.insert_cell_at(i, &cell)?,
            };
            if fits {
                stamp_checksum(&mut f.data);
                return Ok(None);
            }
            let mut entries = n.view().leaf_entries()?;
            match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                Ok(i) => entries[i].1 = value.to_vec(),
                Err(i) => entries.insert(i, (key.to_vec(), value.to_vec())),
            }
            entries
        };

        let sizes: Vec<usize> =
            entries.iter().map(|(k, v)| node::leaf_entry_bytes(k, v)).collect();
        let s = node::split_point(&sizes);
        let separator = entries[s].0.clone();
        let right_entries: Vec<_> = entries.split_off(s);

        let arena = self.store.arena_for(branch)?;
        let right_id = self.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch)?;
        {
            let rh = self.store.read_page(right_id)?;
            let mut f = rh.write();
            NodeMut::new(&mut f.data).fill_leaf(&right_entries)?;
            stamp_checksum(&mut f.data);
        }
        {
            let mut f = handle.write();
            NodeMut::new(&mut f.data).fill_leaf(&entries)?;
            stamp_checksum(&mut f.data);
        }
        Ok(Some((separator, right_id)))
    }

    /// Point an already-shadowed internal node at its new child, and absorb a separator promoted
    /// from below. Returns a separator of its own if it had to split in turn.
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
            .as_ref()
            .is_some_and(|(sep, _)| node::internal_entry_bytes(sep) > node::MAX_ENTRY_BYTES)
        {
            return Err(FerroError::Cow("separator key is too large for a 4KB page".into()));
        }
        let (leftmost, mut entries) = {
            let mut f = handle.write();
            let mut n = NodeMut::new(&mut f.data);
            match slot {
                None => n.set_leftmost(child),
                Some(i) => n.set_child(i, child)?,
            }
            let (sep, right) = match promoted {
                None => {
                    stamp_checksum(&mut f.data);
                    return Ok(None);
                }
                Some(p) => p,
            };
            if n.insert_cell_at(at, &node::internal_cell(&sep, right))? {
                stamp_checksum(&mut f.data);
                return Ok(None);
            }
            let mut entries = n.view().internal_entries()?;
            entries.insert(at, (sep, right));
            (n.leftmost(), entries)
        };

        let sizes: Vec<usize> =
            entries.iter().map(|(k, _)| node::internal_entry_bytes(k)).collect();
        let s = node::split_point(&sizes);
        let right_side: Vec<_> = entries.split_off(s);
        let (middle_key, right_leftmost) = right_side[0].clone();
        let right_entries = &right_side[1..];

        let arena = self.store.arena_for(branch)?;
        let right_id = self.store.alloc_in_arena(arena, PageType::BTreeInternal, epoch)?;
        {
            let rh = self.store.read_page(right_id)?;
            let mut f = rh.write();
            NodeMut::new(&mut f.data).fill_internal(right_leftmost, right_entries)?;
            stamp_checksum(&mut f.data);
        }
        {
            let mut f = handle.write();
            NodeMut::new(&mut f.data).fill_internal(leftmost, &entries)?;
            stamp_checksum(&mut f.data);
        }
        Ok(Some((middle_key, right_id)))
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
            if child_new == child_old && promoted.is_none() {
                // the node was private and was mutated in place: its parent already points at it
                return Ok(root);
            }
            let cp = self.store.cow_page(parent_id, branch, epoch)?;
            let new_parent = cp.page_id;
            promoted =
                self.internal_relink(&cp.handle, slot, child_new, promoted.take(), branch, epoch)?;
            drop(cp);
            child_old = parent_id;
            child_new = new_parent;
        }

        if let Some((sep, right)) = promoted {
            let arena = self.store.arena_for(branch)?;
            let new_root = self.store.alloc_in_arena(arena, PageType::BTreeInternal, epoch)?;
            let h = self.store.read_page(new_root)?;
            let mut f = h.write();
            NodeMut::new(&mut f.data).fill_internal(child_new, &[(sep, right)])?;
            stamp_checksum(&mut f.data);
            return Ok(new_root);
        }
        if child_new == child_old {
            return Ok(root);
        }
        Ok(child_new)
    }
}
