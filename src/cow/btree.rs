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

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use crate::branch::types::{BranchId, Epoch, PageId};
use crate::cow::node::{self, Node, NodeMut};
use crate::cow::page_header::{stamp_checksum, PageHeader, PageType};
use crate::cow::{PageHandle, PageStore, WriteBuffer, WriteBufferEntry};
use crate::error::FerroError;

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

/// The CoW B+tree's page layout, described to the branch reaper.
///
/// `TwoTierReaper::collapse` materialises a branch's visible state by deep-copying every page
/// reachable from its root before re-parenting it to trunk. It cannot know this module's node
/// format, so it takes a [`crate::branch::PageLinks`] walker; without one it refuses to collapse
/// rather than re-parent a branch onto ancestor-owned pages that the interval rule would then be
/// free to reclaim. This is that walker.
///
/// Only `BTreeInternal` pages have children. A leaf returns an empty vector — this tree chains no
/// sibling pointers, so a leaf's `leftmost` field is unused and must not be reported as a link, or
/// collapse would follow a zero page id.
pub struct CowPageLinks;

impl crate::branch::PageLinks for CowPageLinks {
    fn child_pages(
        &self,
        page_type: PageType,
        page: &[u8; crate::storage::disk_manager::PAGE_SIZE],
    ) -> Result<Vec<PageId>, FerroError> {
        match page_type {
            // Propagate rather than `unwrap_or_default()`. `all_children` genuinely errors on a
            // well-formed slot whose cell body is not 4 bytes ("internal cell is not a child
            // pointer", "node cell overruns the page"). Reporting that as "no children" would let
            // collapse detach a subtree it never copied.
            PageType::BTreeInternal => Node::new(page).all_children(),
            _ => Ok(Vec::new()),
        }
    }

    fn rewrite_child(&self, page: &mut [u8; crate::storage::disk_manager::PAGE_SIZE], old: PageId, new: PageId) {
        let count = Node::new(page).count();
        let mut n = NodeMut::new(page);
        if n.leftmost() == old {
            n.set_leftmost(new);
        }
        for i in 0..count {
            // `child` errors on a leaf cell; a leaf never reaches here because `child_pages`
            // reported no links for it, so there is nothing to rewrite.
            if n.view().child(i).ok() == Some(old) {
                let _ = n.set_child(i, new);
            }
        }
    }
}

#[cfg(test)]
mod cow_page_links_tests {
    use super::*;
    use crate::branch::PageLinks;
    use crate::cow::node::PAYLOAD_LEN;
    use crate::cow::PAGE_HEADER_SIZE;
    use crate::storage::disk_manager::PAGE_SIZE;

    /// S5: an undecodable internal node must be REFUSED, never reported as childless.
    ///
    /// `child_pages` used `all_children().unwrap_or_default()`. A well-formed slot whose cell body
    /// is the wrong size, or whose offset+length runs past the payload, is a real error — and
    /// answering "no children" makes `TwoTierReaper::deep_copy` copy the internal node without its
    /// subtree. `collapse` then re-parents the branch and detaches it from its old parent, leaving
    /// it rooted on ancestor-owned pages the interval rule may reclaim. Silent corruption, from
    /// the exact walker whose job is to refuse it.
    #[test]
    fn an_undecodable_internal_node_is_refused_not_reported_as_childless() {
        let mut page = [0u8; PAGE_SIZE];
        let p = PAGE_HEADER_SIZE;
        // One slot...
        page[p..p + 4].copy_from_slice(&1u32.to_be_bytes());
        // ...whose cell starts near the end of the payload and runs off it.
        let slot = p + 12;
        page[slot..slot + 4].copy_from_slice(&((PAYLOAD_LEN - 2) as u32).to_be_bytes());
        page[slot + 4..slot + 8].copy_from_slice(&100u32.to_be_bytes());

        let got = CowPageLinks.child_pages(PageType::BTreeInternal, &page);
        assert!(
            got.is_err(),
            "a corrupt internal node reported {:?} children instead of refusing",
            got.map(|v| v.len())
        );
    }

    /// Control: the refusal above must come from the corruption, not from the walker rejecting
    /// everything. A leaf still reports no children, and does so without error.
    #[test]
    fn a_leaf_reports_no_children_without_erroring() {
        let page = [0u8; PAGE_SIZE];
        let got = CowPageLinks.child_pages(PageType::BTreeLeaf, &page);
        assert_eq!(got.expect("a leaf must not error"), Vec::<PageId>::new());
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
        let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog), ARENA_BASE).unwrap());
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
