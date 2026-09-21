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
///
/// **Two numbers, because one of them was reporting an O(N) operation as cheap.** `pages_examined`
/// is the decode half and it is O(delta); `pages_walked` is the identity half and it is O(N).
/// Reading only the first is what made this path look like `cow::diff`'s synchronised descent
/// while costing a full traversal of both trees — see [`CowTree::diff`].
#[derive(Debug)]
pub struct TreeDiff {
    pub deltas: Vec<Delta>,
    /// Pages whose **entries were decoded**. Subtrees the two roots share are skipped whole,
    /// by page identity, so this stays proportional to what changed rather than to the tree.
    pub pages_examined: usize,
    /// Pages whose **identity had to be known** before either side could be pruned against the
    /// other: `|walk_pages(base)| + |walk_pages(head)|`. Proportional to the TREE, not to the
    /// change, and it is paid before the first entry is decoded.
    ///
    /// This is the number that makes the cost of this path visible. `cow::diff::diff` does not
    /// pay it at all — it descends the two roots together and never enumerates either side — so a
    /// comparison between the two paths is only honest when it is stated in this field rather
    /// than in `pages_examined` alone.
    pub pages_walked: usize,
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
    /// expensive half — is proportional to what actually changed.
    ///
    /// ⚠ **BOTH halves are now reported, and the second one is why.** `pages_examined` alone is
    /// O(delta), so a reader who trusted it read this O(N) operation as cheap — including the
    /// production `DIFF` path, which used to call this and report a handful of decoded pages
    /// while the two `walk_pages` calls below had just enumerated a million. `pages_walked` is
    /// that enumeration, and it is the number to compare against
    /// [`crate::cow::diff::DiffReport::visited`].
    ///
    /// **The production `DIFF` path no longer calls this** — `AgentRuntime::page_changeset` uses
    /// `cow::diff::diff`, whose synchronised descent never enumerates either side. This is kept
    /// for the callers that want a `BTreeMap`-shaped answer and for the control arm of the
    /// measurement in `examples/d103_production_diff_curve.rs`.
    pub fn diff(&self, base_root: PageId, head_root: PageId) -> Result<TreeDiff, FerroError> {
        // Same root is the common case for an agent that read but never wrote, and it is the
        // cleanest statement of the invariant: identical pointer, identical tree, nothing read.
        if base_root == head_root {
            return Ok(TreeDiff { deltas: Vec::new(), pages_examined: 0, pages_walked: 0 });
        }

        let base_walk = self.walk_pages(base_root)?;
        let head_walk = self.walk_pages(head_root)?;
        // Counted from the WALKS, not from the sets: two pages shared between the roots collapse
        // into one set entry, and the cost this records is the pages that were read, not the
        // distinct pages that survived deduplication.
        let walked = base_walk.len() + head_walk.len();
        let base_pages: HashSet<PageId> = base_walk.into_iter().collect();
        let head_pages: HashSet<PageId> = head_walk.into_iter().collect();

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
        Ok(TreeDiff { deltas, pages_examined: examined, pages_walked: walked })
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
        // A content cut promotes `successor_of(key)`, a byte longer than the key, so a key can fit
        // a leaf and still be too large to *separate* one. Refuse it here, against the one place
        // the limit is stated.
        //
        // `internal_relink` checks the separator too, and that is **not** a second guard that
        // makes this one redundant — measured by deleting this block and running
        // `the_key_limit_holds_on_both_sides_and_refuses_before_spending_a_page`: the over-long
        // insert *succeeded*. Nothing downstream fires until the leaf holding that key happens to
        // split, which may be many inserts later and will then refuse whichever unrelated key
        // triggered it. This is the only refusal that is deterministic and attributable.
        if key.len() > node::MAX_KEY_BYTES {
            return Err(FerroError::Cow(format!(
                "key of {} bytes exceeds the {}-byte key limit: its separator would need {} of the \
                 {} bytes an internal entry may use",
                key.len(),
                node::MAX_KEY_BYTES,
                node::separator_entry_bytes(key),
                node::MAX_ENTRY_BYTES
            )));
        }
        let (path, leaf_id) = self.descend(root, key)?;
        let cp = self.store.cow_page(leaf_id, branch, epoch)?;
        let new_leaf = cp.page_id;
        let (split, unterminated) = self.leaf_put(&cp.handle, branch, epoch, key, value)?;
        drop(cp);
        let root = self.relink_up(root, path, leaf_id, new_leaf, split, branch, epoch)?;
        if unterminated {
            // An overwrite can erase the terminating boundary of the leaf it lands in, exactly as
            // a delete can. `key` is the last entry of that run, so it still descends to the leaf
            // that lost its terminator even when the write above split the run.
            return self.merge_right(root, key, branch, epoch);
        }
        Ok(root)
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
        let (emptied, unterminated, first_key) = {
            let mut f = cp.handle.write();
            let mut n = NodeMut::new(&mut f.data);
            let found = n.view().search(key)?;
            if let Ok(i) = found {
                n.remove_at(i)?;
            }
            let v = n.view();
            let count = v.count();
            let emptied = count == 0;
            // Did this delete take the entry that TERMINATED the leaf? `cow::chunker` ends a leaf
            // AT a boundary entry, so a leaf whose last entry is no longer one has lost its
            // terminator and the content now calls for it to be joined with what follows.
            let unterminated = !emptied
                && !chunker::is_boundary(v.key(count - 1)?, v.value(count - 1)?);
            let first_key = if emptied { Vec::new() } else { v.key(0)?.to_vec() };
            stamp_checksum(&mut f.data);
            (emptied, unterminated, first_key)
        };
        drop(cp);
        // A leaf whose last entry has just left has to leave the tree with it.
        //
        // Leaving it linked is not merely untidy: with content-defined boundaries
        // (`cow::chunker`) the leaf partition is supposed to be a function of the rows, and an
        // empty leaf is a leaf the rows do not justify. Measured before this fix, 500 rows
        // reached by deleting half of 1000 carried 20 empty leaves whose live entries matched an
        // insert-only tree byte for byte, so the two agreed on every row and disagreed on
        // `cow::cid::leaf_partition_cid` — a false difference, reported by the one comparison
        // that is supposed to be exact across lineages.
        //
        // This removes THAT difference. It does not make delete converge in general: a delete
        // that destroys an interior content boundary leaves two leaves where the content calls
        // for one, and no leaf is emptied in that case at all. See `unlink_up`'s header.
        //
        // The root is the exception: an empty root leaf IS the empty tree, and is what
        // [`CowTree::create`] hands out.
        if emptied && !path.is_empty() {
            return self.unlink_up(root, path, new_leaf, branch, epoch);
        }
        let root = self.relink_up(root, path, leaf_id, new_leaf, Vec::new(), branch, epoch)?;
        if unterminated {
            // The leaf lost its terminating boundary. Join it with what follows, or the partition
            // stops being a function of the rows: `leaf_put`'s re-chunk only ever splits further,
            // so nothing else in the tree can ever rejoin these two.
            return self.merge_right(root, &first_key, branch, epoch);
        }
        Ok(root)
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
    ) -> Result<(Split, bool), FerroError> {
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
                    // Nothing moved in the partition — but an OVERWRITE of the last entry can
                    // still have erased the leaf's terminator without creating an interior one,
                    // which is the same defect as the delete case and just as permanent.
                    let v = n.view();
                    let last = v.count() - 1;
                    let unterminated = !chunker::is_boundary(v.key(last)?, v.value(last)?);
                    stamp_checksum(&mut f.data);
                    return Ok((Vec::new(), unterminated));
                }
                // The entry is already in the page and the re-chunk below reopens it through a
                // fresh `write()`. Restamp before this guard drops: the page is pinned throughout
                // so nothing could flush it in between, but a page whose checksum disagrees with
                // its bytes should be unreachable by construction, not by luck.
                let entries = n.view().leaf_entries()?;
                stamp_checksum(&mut f.data);
                entries
            } else {
                let mut entries = n.view().leaf_entries()?;
                match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                    Ok(i) => entries[i].1 = value.to_vec(),
                    Err(i) => entries.insert(i, (key.to_vec(), value.to_vec())),
                }
                entries
            }
        };

        self.write_leaf_chunked(handle, entries, branch, epoch)
    }

    /// Lay `entries` into `handle`'s page, cutting them where the CONTENT says to, and return the
    /// separators of any pieces that did not fit in that page.
    ///
    /// Extracted from [`CowTree::leaf_put`] so the delete-side merge re-chunks through the same
    /// code the insert side does. Two implementations of "where do the leaves end" is the one way
    /// to lose the property `cow::chunker` exists to provide, and it would be lost silently —
    /// both sides would look right in isolation and disagree only on trees that had seen both.
    fn write_leaf_chunked(
        &self,
        handle: &PageHandle,
        entries: node::LeafEntries,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<(Split, bool), FerroError> {
        // Whether the RUN ends on a boundary, which is the same question however it is cut: the
        // last piece ends on the run's last entry whatever the cuts do in between.
        let unterminated = match entries.last() {
            Some((k, v)) => !chunker::is_boundary(k, v),
            None => false,
        };
        let sizes: Vec<usize> =
            entries.iter().map(|(k, v)| node::leaf_entry_bytes(k, v)).collect();
        let hashes: Vec<u32> = entries.iter().map(|(k, _)| chunker::key_hash(k)).collect();
        let cuts = chunker::leaf_cuts(&sizes, &hashes, node::NODE_CAPACITY);
        if cuts.is_empty() {
            // The content says this is one chunk. It reached here only because the entry did not
            // fit the page as laid out, so rewriting the leaf compacts it.
            let mut f = handle.write();
            NodeMut::new(&mut f.data).fill_leaf(&entries)?;
            stamp_checksum(&mut f.data);
            return Ok((Vec::new(), unterminated));
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
        Ok((promoted, unterminated))
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

    /// The leftmost leaf of the subtree at `pid`.
    fn leftmost_leaf(&self, mut pid: PageId) -> Result<PageId, FerroError> {
        for _ in 0..MAX_DESCENT {
            let h = self.store.read_page(pid)?;
            let f = h.read();
            if PageHeader::read_from(&f.data)?.page_type == PageType::BTreeLeaf {
                return Ok(pid);
            }
            let next = Node::new(&f.data).leftmost();
            drop(f);
            drop(h);
            pid = next;
        }
        Err(FerroError::Cow("btree leftmost walk exceeded the depth guard".into()))
    }

    /// The leaf immediately right of the one `path` descended to, or `None` if it is the last.
    ///
    /// **No sibling pointer, and that distinction is the whole reason the merge is possible.**
    /// `cow::node`'s header rejects a stored `next_leaf` link for a reason specific to shadow
    /// paging: shadowing leaf N+1 would force shadowing N to update its pointer, cascading
    /// leftward along the entire leaf level. Nothing here is stored and nothing points sideways —
    /// this climbs the descent path to the shallowest ancestor that still has a child further
    /// right and takes that subtree's leftmost leaf, exactly as `ScanCursor` already does for an
    /// ordered scan. Cost is the depth, paid only on the delete that erases a boundary.
    fn right_neighbour(&self, path: &DescentPath) -> Result<Option<PageId>, FerroError> {
        for (parent_id, slot) in path.iter().rev() {
            let kids = {
                let h = self.store.read_page(*parent_id)?;
                let f = h.read();
                Node::new(&f.data).all_children()?
            };
            // `all_children` is [leftmost, child(0), child(1), ...], so slot i sits at i + 1.
            let taken = match slot {
                None => 0,
                Some(i) => i + 1,
            };
            if taken + 1 < kids.len() {
                return Ok(Some(self.leftmost_leaf(kids[taken + 1])?));
            }
        }
        Ok(None)
    }

    /// Repair a leaf whose terminating content boundary a delete has just removed, by absorbing
    /// its right neighbour and re-cutting the pair.
    ///
    /// # Why this is needed at all
    ///
    /// `cow::chunker` makes a leaf end **at** a boundary entry, so the partition is a function of
    /// the rows. Deleting that entry leaves the leaf unterminated, and `leaf_put`'s re-chunk can
    /// only ever split a leaf further, never rejoin one — so without this the leaf and its
    /// neighbour stay split where the content calls for one leaf, permanently. Measured before
    /// this existed: deleting one interior boundary key from a 1000-row build left 28 + 9 rows
    /// against a clean build's single leaf of 37.
    ///
    /// # Why it terminates, which is NOT the argument `unlink_up` uses
    ///
    /// `unlink_up` terminates because removing a slotted child always leaves the leftmost behind.
    /// Moving entries between leaves breaks that property, so the argument is rebuilt here: the
    /// merged run ends on the right neighbour's LAST entry, and that entry is a boundary because
    /// the neighbour was a well-formed leaf. So one absorption always terminates the left leaf and
    /// there is no second round. The single exception is a neighbour that was itself the last leaf
    /// in the tree, and the last leaf is allowed to end anywhere — so that case is finished too.
    ///
    /// # Why it is three passes rather than one clever one
    ///
    /// The two leaves can sit under different parents (measured: 3 of 486 leaves at depth 2), so
    /// the relink is over two root-to-leaf paths that share only a prefix. Rewriting both in one
    /// upward walk means tracking two cursors that merge partway up — the intricate version, and
    /// the one most likely to be subtly wrong in a core B+tree. Instead each step re-descends and
    /// uses a fresh, valid path, and every step is machinery that already has tests:
    /// `unlink_up` removes the neighbour, `write_leaf_chunked` re-cuts the merged run through the
    /// same code the insert side uses, and `relink_up` absorbs whatever that promotes. Three
    /// descents at O(depth) each, on a delete that fires roughly once per leaf's worth of rows.
    fn merge_right(
        &self,
        root: PageId,
        key_in_left: &[u8],
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        // 1. Locate the unterminated leaf afresh and find what is to its right.
        let (path_l, _) = self.descend(root, key_in_left)?;
        let Some(r_id) = self.right_neighbour(&path_l)? else {
            // It is the last leaf, which may end anywhere.
            return Ok(root);
        };
        let r_entries = {
            let h = self.store.read_page(r_id)?;
            let f = h.read();
            Node::new(&f.data).leaf_entries()?
        };
        let Some((r_first, _)) = r_entries.first().cloned() else {
            // An empty neighbour: nothing to absorb, and `delete` no longer leaves one behind.
            return Ok(root);
        };

        // 2. Take the neighbour out of the tree. Shadow it first — freeing a page this branch
        //    does not own would corrupt the ancestor that still points at it, not this branch.
        let (path_r, r_live) = self.descend(root, &r_first)?;
        let cpr = self.store.cow_page(r_live, branch, epoch)?;
        let r_shadow = cpr.page_id;
        drop(cpr);
        let root = self.unlink_up(root, path_r, r_shadow, branch, epoch)?;

        // 3. Re-descend — step 2 shadowed ancestors the first path shared — and rewrite the left
        //    leaf with the merged run, re-cut by content.
        let (path_l, l_live) = self.descend(root, key_in_left)?;
        let cpl = self.store.cow_page(l_live, branch, epoch)?;
        let l_shadow = cpl.page_id;
        let mut merged = {
            let f = cpl.handle.read();
            Node::new(&f.data).leaf_entries()?
        };
        merged.extend(r_entries);
        // The flag can only still be set when the neighbour just absorbed was itself the tree's
        // LAST leaf, because every other leaf ends on a boundary — and the last leaf is allowed
        // to end anywhere. So there is no second round, which is the termination argument above
        // stated as code rather than prose.
        let (promoted, _now_the_last_leaf) =
            self.write_leaf_chunked(&cpl.handle, merged, branch, epoch)?;
        drop(cpl);
        self.relink_up(root, path_l, l_live, l_shadow, promoted, branch, epoch)
    }

    /// Drop `doomed` out of the tree, cascading while its removal leaves a parent with no children
    /// at all, then relink the rest of the path normally. Returns the new root.
    ///
    /// # When a parent can become childless, stated exactly
    ///
    /// An internal node holds a leftmost child plus `count` (separator, child) slots, so it has
    /// `count + 1` children and there is no encoding for "no leftmost". Removing a **slotted**
    /// child therefore always leaves the leftmost behind and the parent survives. The node becomes
    /// childless in exactly one case: the child removed was the **leftmost** and `count == 0`, so
    /// it was the only one. That single case is the whole cascade condition, and it is why this
    /// loop terminates in tree depth rather than needing a rebalance.
    ///
    /// Removing the leftmost when `count > 0` is the one fiddly step: the next child is promoted
    /// into the leftmost field and *its* separator goes with it, because a separator is the lower
    /// bound of the child to its right and the leftmost child has no lower bound.
    ///
    /// # Ordering
    ///
    /// Each page is freed **after** its parent has stopped pointing at it, never before.
    /// `free_page` can hand a page straight back to the free space map, so freeing first would
    /// leave a live parent pointing at a page an allocator may already have reissued.
    ///
    /// # What this does NOT do, and what that costs
    ///
    /// It does not merge siblings and does not collapse a root left with a single child, so a tree
    /// that has had a lot deleted from it can keep a level it no longer needs. Both are
    /// rebalancing and both need the sibling access this layout deliberately lacks (`cow::node`'s
    /// header).
    ///
    /// This closes the empty-leaf defect and nothing else; an erased content boundary is
    /// [`CowTree::merge_right`]'s job, and the two are genuinely different repairs. A delete that
    /// erases a terminator empties no leaf at all, so there is nothing here for it to unlink —
    /// measured identical either side of this function before `merge_right` existed.
    fn unlink_up(
        &self,
        root: PageId,
        mut path: DescentPath,
        mut doomed: PageId,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        // ⛔ FREE LAST. Every page this cascade retires is collected here and handed back only
        // once the new root is established.
        //
        // Freeing inside the loop broke this function's own documented ordering ("Each page is
        // freed AFTER its parent has stopped pointing at it, never before"), in TWO arms:
        //
        //  * the `childless` arm returns `true` WITHOUT touching the node, so the parent's
        //    `leftmost` still points at `doomed` when the free ran -- directly under a comment
        //    reading "Nothing points at it now.", which was false exactly there. When `cow_page`
        //    wrote in place because the branch already owned the parent, that parent is a LIVE
        //    page pointing at one the free space map may have already reissued.
        //  * the cascade-past-root arm freed and THEN called `create`, which is fallible
        //    (`arena_for` + `alloc_in_arena` + `read_page`). On an error there the caller still
        //    holds the old root, whose page is now free.
        //
        // In the happy path the dangling pointer is transient -- the next iteration frees the
        // parent too -- so this is an error-path defect, and `relink_up` never freed anything,
        // which means delete USED to be fail-safe and had stopped being so.
        let mut retired: Vec<PageId> = Vec::new();
        let new_root = loop {
            let Some((parent_id, slot)) = path.pop() else {
                // Cascaded past the root: every page is gone, so the tree is the empty tree.
                retired.push(doomed);
                break self.create(branch, epoch)?;
            };

            let cp = self.store.cow_page(parent_id, branch, epoch)?;
            let new_parent = cp.page_id;
            let childless = {
                let mut f = cp.handle.write();
                let mut n = NodeMut::new(&mut f.data);
                let childless = match slot {
                    Some(i) => {
                        // The cell at `i` carries both the separator and the child pointer, so one
                        // removal takes both. The leftmost is untouched, so a child remains.
                        n.remove_at(i)?;
                        false
                    }
                    None if n.count() == 0 => true,
                    None => {
                        let promoted = n.view().child(0)?;
                        n.set_leftmost(promoted);
                        n.remove_at(0)?;
                        false
                    }
                };
                stamp_checksum(&mut f.data);
                childless
            };
            drop(cp);

            // Retired, NOT yet freed -- see the note at the top of this function.
            retired.push(doomed);

            if childless {
                doomed = new_parent;
                continue;
            }
            // The parent survived but was copied, so its own parent still has to be repointed.
            // With an empty path this returns `new_parent`, which is then the new root.
            break self.relink_up(root, path, parent_id, new_parent, Vec::new(), branch, epoch)?;
        };

        // The new root is established, so nothing reachable points at any retired page and a
        // failure from here can only LEAK one, never strand a live pointer -- the safe direction.
        // Every page is attempted even if one fails, because stopping early would leak the rest
        // for no gain; the first error is still returned rather than swallowed.
        let mut first_err = None;
        for page in retired {
            if let Err(e) = self.store.free_page(page, epoch) {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(new_root),
        }
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
/// These figures were taken before leaf boundaries became content-defined (`cow::chunker`), which
/// spends fanout on structural invariance and so adds roughly a level to a 10^6-row tree. What the
/// table *claims* is unaffected — the streamed cost is a property of the tree's depth, not of the
/// rows returned — but the streamed constants belong to the old, shallower shape and a re-run
/// would read somewhat higher. `bench/s23_cow_scan_memory_{before,after}.txt` are the raw runs.
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

#[cfg(test)]
mod delete_unlink_tests {
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
            .open(dir.path().join("unlink.db"))
            .unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let pool = Arc::new(BufferPoolManager::new(dm));
        let catalog = Arc::new(LogBranchCatalog::in_memory(1));
        let store = Arc::new(
            ArenaPageStore::new(pool, Arc::clone(&catalog) as Arc<dyn BranchCatalog>, ARENA_BASE)
                .unwrap(),
        );
        let t = CowTree::new(store as Arc<dyn PageStore>);
        (dir, catalog, t)
    }

    fn k(n: u32) -> Vec<u8> {
        n.to_be_bytes().to_vec()
    }

    fn filled(t: &CowTree, cat: &LogBranchCatalog, n: u32) -> PageId {
        let e = cat.next_epoch();
        let mut root = t.create(BranchId::TRUNK, e).unwrap();
        for i in 0..n {
            root = t.insert(root, BranchId::TRUNK, e, &k(i), format!("v{i}").as_bytes()).unwrap();
        }
        root
    }

    /// Entry counts of every leaf reachable from `root`, in key order.
    fn leaf_sizes(t: &CowTree, root: PageId) -> Vec<usize> {
        fn go(t: &CowTree, pid: PageId, depth: usize, out: &mut Vec<usize>) {
            assert!(depth <= MAX_DESCENT, "cycle while walking leaves");
            let h = t.store.read_page(pid).unwrap();
            let f = h.read();
            let ty = PageHeader::read_from(&f.data).unwrap().page_type;
            let n = Node::new(&f.data);
            if ty == PageType::BTreeLeaf {
                out.push(n.count());
            } else {
                let kids = n.all_children().unwrap();
                drop(f);
                drop(h);
                for c in kids {
                    go(t, c, depth + 1, out);
                }
            }
        }
        let mut out = Vec::new();
        go(t, root, 0, &mut out);
        out
    }

    /// Assert how many leaves (excluding the last) fail to end on a content boundary.
    fn assert_unterminated_leaves(t: &CowTree, root: PageId, want: usize, ctx: &str) {
        let leaves = leaf_entries_of(t, root);
        let bad: Vec<usize> = leaves
            .iter()
            .enumerate()
            .take(leaves.len() - 1)
            .filter(|(_, l)| {
                let (k, v) = l.last().expect("no leaf may be empty");
                !chunker::is_boundary(k, v)
            })
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            bad.len(),
            want,
            "{ctx}: {} of {} leaves do not end on a content boundary (leaves {:?}) -- the \
             partition is no longer a function of the rows",
            bad.len(),
            leaves.len(),
            &bad[..bad.len().min(8)]
        );
    }

    /// Leaves of the subtree at `pid`, in key order.
    fn collect_leaves(t: &CowTree, pid: PageId, out: &mut Vec<node::LeafEntries>) {
        let h = t.store.read_page(pid).unwrap();
        let f = h.read();
        let ty = PageHeader::read_from(&f.data).unwrap().page_type;
        let n = Node::new(&f.data);
        if ty == PageType::BTreeLeaf {
            out.push(n.leaf_entries().unwrap());
        } else {
            let kids = n.all_children().unwrap();
            drop(f);
            drop(h);
            for c in kids {
                collect_leaves(t, c, out);
            }
        }
    }

    /// Every leaf's entries, in key order.
    fn leaf_entries_of(t: &CowTree, root: PageId) -> Vec<node::LeafEntries> {
        fn go(t: &CowTree, pid: PageId, out: &mut Vec<node::LeafEntries>) {
            let h = t.store.read_page(pid).unwrap();
            let f = h.read();
            let ty = PageHeader::read_from(&f.data).unwrap().page_type;
            let n = Node::new(&f.data);
            if ty == PageType::BTreeLeaf {
                out.push(n.leaf_entries().unwrap());
            } else {
                let kids = n.all_children().unwrap();
                drop(f);
                drop(h);
                for c in kids { go(t, c, out); }
            }
        }
        let mut out = Vec::new();
        go(t, root, &mut out);
        out
    }

    /// How `cow::chunker` says this content should be cut, independent of any tree. This is the
    /// authority both a built tree and a repaired one are supposed to obey.
    fn chunker_partition(rows: &[(Vec<u8>, Vec<u8>)]) -> Vec<usize> {
        let sizes: Vec<usize> =
            rows.iter().map(|(k, v)| node::leaf_entry_bytes(k, v)).collect();
        let hashes: Vec<u32> = rows.iter().map(|(k, _)| chunker::key_hash(k)).collect();
        let cuts = chunker::leaf_cuts(&sizes, &hashes, node::NODE_CAPACITY);
        let mut out = Vec::new();
        let mut prev = 0usize;
        for &c in &cuts {
            out.push(c - prev);
            prev = c;
        }
        out.push(rows.len() - prev);
        out
    }

    /// **The property the unlink exists for.** An emptied leaf must leave the tree rather than
    /// stay linked holding nothing.
    ///
    /// Non-vacuous by construction: the fixture is asserted to be multi-leaf first, because a
    /// single-leaf tree is the one case where an emptied leaf legitimately stays (it is the root).
    #[test]
    fn an_emptied_leaf_is_unlinked_rather_than_left_in_the_tree() {
        let (_d, cat, t) = tree();
        let mut root = filled(&t, &cat, 1000);
        let before = leaf_sizes(&t, root);
        assert!(before.len() > 4, "tree is {} leaves; the claim would be vacuous", before.len());
        assert!(before.iter().all(|n| *n > 0), "fixture already had an empty leaf");

        // Delete a contiguous run big enough to empty whole leaves several times over.
        let e = cat.next_epoch();
        for i in 0..600u32 {
            root = t.delete(root, BranchId::TRUNK, e, &k(i)).unwrap();
        }

        let after = leaf_sizes(&t, root);
        assert!(
            after.iter().all(|n| *n > 0),
            "{} of {} leaves are empty after deleting 600 keys: {:?}",
            after.iter().filter(|n| **n == 0).count(),
            after.len(),
            after
        );
        assert!(after.len() < before.len(), "no leaf was actually unlinked ({} -> {})", before.len(), after.len());
    }

    /// The anti-corruption guard, and the one that matters most: unlinking must not lose, reorder
    /// or resurrect a row. Deletes a scattered half so the removals hit leftmost children,
    /// slotted children and whole subtrees rather than one shape.
    #[test]
    fn every_surviving_key_is_still_readable_after_scattered_deletion() {
        let (_d, cat, t) = tree();
        let mut root = filled(&t, &cat, 1000);
        let e = cat.next_epoch();

        let doomed: Vec<u32> = (0..1000).filter(|i| i % 3 != 0).collect();
        for &i in &doomed {
            root = t.delete(root, BranchId::TRUNK, e, &k(i)).unwrap();
        }

        for i in 0..1000u32 {
            let got = t.get(root, &k(i)).unwrap();
            if i % 3 == 0 {
                assert_eq!(
                    got.as_deref(),
                    Some(format!("v{i}").as_bytes()),
                    "survivor {i} is gone or wrong after deleting around it"
                );
            } else {
                assert_eq!(got, None, "deleted key {i} is still readable");
            }
        }

        // And in order, through the cursor rather than by point lookup — a broken separator shows
        // up here and not above.
        let scanned: Vec<u32> = t
            .range_scan(root, None, None)
            .unwrap()
            .map(|r| u32::from_be_bytes(r.unwrap().0.try_into().unwrap()))
            .collect();
        assert_eq!(scanned, (0..1000).filter(|i| i % 3 == 0).collect::<Vec<u32>>());
    }

    /// Emptying the FIRST leaf takes the leftmost-child path, where the successor is promoted into
    /// the leftmost field and its separator goes with it. That branch has no other coverage.
    #[test]
    fn emptying_the_leftmost_leaf_promotes_its_successor() {
        let (_d, cat, t) = tree();
        let mut root = filled(&t, &cat, 1000);
        let first_leaf_size = leaf_sizes(&t, root)[0];
        let before = leaf_sizes(&t, root).len();
        assert!(before > 2, "need a multi-leaf tree");

        let e = cat.next_epoch();
        for i in 0..first_leaf_size as u32 {
            root = t.delete(root, BranchId::TRUNK, e, &k(i)).unwrap();
        }

        let after = leaf_sizes(&t, root);
        assert!(after.iter().all(|n| *n > 0), "leftmost unlink left an empty leaf: {after:?}");
        assert_eq!(after.len(), before - 1, "exactly the first leaf should have gone");
        // The smallest surviving key must now be the first one past the old leaf.
        let lowest = t.range_scan(root, None, None).unwrap().next().unwrap().unwrap().0;
        assert_eq!(lowest, k(first_leaf_size as u32), "the promoted leftmost is wrong");
    }

    /// Cascading all the way past the root: every page goes, and what is left is a usable empty
    /// tree rather than a dangling root.
    #[test]
    fn deleting_every_key_leaves_a_usable_empty_tree() {
        let (_d, cat, t) = tree();
        let mut root = filled(&t, &cat, 1000);
        let e = cat.next_epoch();
        for i in 0..1000u32 {
            root = t.delete(root, BranchId::TRUNK, e, &k(i)).unwrap();
        }

        assert_eq!(leaf_sizes(&t, root), vec![0], "an emptied tree must be exactly one empty leaf");
        assert_eq!(t.walk_pages(root).unwrap().len(), 1, "the emptied tree still holds interior pages");
        assert_eq!(t.get(root, &k(0)).unwrap(), None);
        assert_eq!(t.range_scan(root, None, None).unwrap().count(), 0);

        // And it still works: the empty tree is a real tree, not a tombstone.
        let e2 = cat.next_epoch();
        let root = t.insert(root, BranchId::TRUNK, e2, &k(42), b"back").unwrap();
        assert_eq!(t.get(root, &k(42)).unwrap().as_deref(), Some(&b"back"[..]));
    }

    /// Unlinking must hand the pages back, not merely stop pointing at them. Without the
    /// `free_page` calls this passes the partition tests and leaks every emptied leaf.
    #[test]
    fn unlinked_pages_are_freed_rather_than_leaked() {
        let (_d, cat, t) = tree();
        let mut root = filled(&t, &cat, 1000);
        let before = t.store.live_page_count().unwrap();
        assert!(before > 8, "only {before} live pages; the measurement would be vacuous");

        let e = cat.next_epoch();
        for i in 0..1000u32 {
            root = t.delete(root, BranchId::TRUNK, e, &k(i)).unwrap();
        }
        let after = t.store.live_page_count().unwrap();

        println!("    live pages: {before} -> {after} after deleting every key");
        assert!(
            after < before,
            "live pages did not drop ({before} -> {after}); unlinked pages are being leaked"
        );
    }

    /// **Deleting on a fork must not touch the parent's tree.** This is where an unlink could
    /// corrupt something silently and in the wrong branch.
    ///
    /// `free_page` can hand a page straight back to the free space map, so a child that freed a
    /// page its ancestor still points at would corrupt the *ancestor*. Every page `unlink_up`
    /// frees came out of `cow_page` and is therefore owned by the writing branch — but "therefore"
    /// is exactly the kind of word this test exists to replace with a run.
    #[test]
    fn deleting_on_a_fork_leaves_the_parents_tree_intact() {
        let (_d, cat, t) = tree();
        let parent_root = filled(&t, &cat, 1000);
        let parent_pages = t.walk_pages(parent_root).unwrap();
        let parent_sizes = leaf_sizes(&t, parent_root);
        assert!(parent_sizes.len() > 4, "need a multi-leaf parent for this to mean anything");

        let child =
            cat.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap().branch_id;
        let e = cat.next_epoch();
        let mut child_root = parent_root;
        for i in 0..600u32 {
            child_root = t.delete(child_root, child, e, &k(i)).unwrap();
        }

        // The child really did the work — otherwise the parent surviving proves nothing.
        assert_eq!(t.get(child_root, &k(0)).unwrap(), None, "the child did not delete anything");
        assert!(
            leaf_sizes(&t, child_root).iter().all(|n| *n > 0),
            "the child kept an empty leaf"
        );

        // And the parent is untouched, page for page and row for row.
        assert_eq!(
            t.walk_pages(parent_root).unwrap(),
            parent_pages,
            "the child's delete changed the PARENT's page set"
        );
        assert_eq!(
            leaf_sizes(&t, parent_root),
            parent_sizes,
            "the child's delete changed the PARENT's partition"
        );
        for i in 0..1000u32 {
            assert_eq!(
                t.get(parent_root, &k(i)).unwrap().as_deref(),
                Some(format!("v{i}").as_bytes()),
                "key {i} vanished from the PARENT after the child deleted it"
            );
        }
    }

    /// **The merge, held to the chunker's own invariant.** Delete the key that terminates a leaf
    /// and the leaf must be rejoined with its neighbour, so that every leaf but the last still
    /// ends ON a content boundary — which is exactly what
    /// `cow::tests_chunking::every_leaf_but_the_last_ends_on_a_content_boundary` demands of the
    /// insert path, and what makes the partition a function of the rows.
    ///
    /// **Not** equality with `chunker::leaf_cuts` over the whole row list, which is a different
    /// and wrong claim: `leaf_cuts` re-cuts an over-capacity piece at a finer target, so its
    /// answer depends on the window it is given and legitimately differs from a tree's wherever
    /// that recursion fires. Asserting it cost an hour before the distinction was clear.
    ///
    /// The victims are spread across the tree rather than taken in order. Deleting terminators
    /// consecutively merges the same region over and over — measured at 42, 93, 105, 122, 173 and
    /// then 256 entries — until the run no longer fits a page and the byte cap cuts it somewhere
    /// the content did not choose. That is the chunker's own documented exception
    /// (`chunker::CHUNK_SHIFT`), not a property of this repair, and
    /// `deleting_every_boundary_drives_chunks_over_a_page` pins it separately.
    #[test]
    fn deleting_a_terminating_boundary_key_rejoins_the_leaf_with_its_neighbour() {
        let (_d, cat, t) = tree();
        let mut root = filled(&t, &cat, 4000);
        assert_unterminated_leaves(&t, root, 0, "a freshly built tree");

        let e = cat.next_epoch();
        let mut repaired = 0usize;
        for step in 0..30usize {
            let leaves = leaf_entries_of(&t, root);
            if leaves.len() < 8 {
                break;
            }
            // Spread out, so each repair joins two leaves rather than compounding one run.
            let idx = (step * 7 + 1) % (leaves.len() - 1);
            let (vk, vv) = leaves[idx].last().unwrap().clone();
            assert!(chunker::is_boundary(&vk, &vv), "fixture: leaf {idx} must end on a boundary");

            let before = leaves.len();
            root = t.delete(root, BranchId::TRUNK, e, &vk).unwrap();
            let after = leaf_entries_of(&t, root).len();
            assert!(after < before, "deleting terminator {vk:?} did not rejoin anything");
            assert_unterminated_leaves(&t, root, 0, &format!("after deleting terminator {vk:?}"));
            repaired += 1;
        }
        assert!(repaired > 10, "only {repaired} repairs exercised");
        println!("    {repaired} terminators deleted; every leaf but the last still ends on a boundary");
    }

    /// **The limit of the repair, measured rather than left to be discovered.** Deleting a leaf's
    /// terminator joins it to its neighbour, and the join has only ONE boundary — the neighbour's
    /// last entry. So deleting terminators repeatedly in one region grows a single run without
    /// bound, and once it no longer fits a page `chunker::leaf_cuts` falls back to cutting it by
    /// SIZE. Those cuts are not content boundaries, so the partition stops being a function of
    /// the rows there.
    ///
    /// That is `chunker::CHUNK_SHIFT`'s documented exception reached deliberately rather than by
    /// an `e^-8` accident, and the repair cannot avoid it: the content genuinely has no boundary
    /// left to cut on. Pinned so the number is visible if the trade is ever retuned.
    #[test]
    fn deleting_every_boundary_drives_chunks_over_a_page() {
        let (_d, cat, t) = tree();
        let mut root = filled(&t, &cat, 4000);
        let e = cat.next_epoch();

        for _ in 0..40 {
            let leaves = leaf_entries_of(&t, root);
            if leaves.len() < 3 {
                break;
            }
            let (vk, _) = leaves[0].last().unwrap().clone();
            root = t.delete(root, BranchId::TRUNK, e, &vk).unwrap();
        }

        let leaves = leaf_entries_of(&t, root);
        let capped = leaves
            .iter()
            .enumerate()
            .take(leaves.len() - 1)
            .filter(|(_, l)| {
                let (k, v) = l.last().unwrap();
                !chunker::is_boundary(k, v)
            })
            .count();
        let widest = leaves.iter().map(|l| l.len()).max().unwrap_or(0);
        println!(
            "    after 40 terminator deletes in one region: {} leaves, widest {widest} rows, \
             {capped} cut by the byte cap rather than by content",
            leaves.len()
        );
        // Every leaf must still FIT, which is the guarantee that has no exception.
        for (i, l) in leaves.iter().enumerate() {
            let bytes: usize =
                l.iter().map(|(k, v)| node::leaf_entry_bytes(k, v)).sum();
            assert!(bytes <= node::NODE_CAPACITY, "leaf {i} holds {bytes} bytes, over capacity");
        }
        assert!(capped > 0, "the byte cap never fired; this no longer measures its own subject");
    }

    /// The same repair when the neighbour is **under a different parent**, which is the case the
    /// climb exists for and the one a same-parent-only merge would silently skip.
    #[test]
    fn a_boundary_delete_rejoins_across_a_parent_as_well() {
        let (_d, cat, t) = tree();
        let mut root = filled(&t, &cat, 12_000);
        let depth = t.descend(root, &k(0)).unwrap().0.len();
        assert!(depth >= 2, "tree is {depth} internal level(s); no cross-parent case exists");

        // Terminators of leaves that are the LAST child of their own parent: repairing those is
        // exactly what needs the climb.
        let mut victims = Vec::new();
        {
            let leaves = leaf_entries_of(&t, root);
            for (i, l) in leaves.iter().enumerate().take(leaves.len() - 1) {
                let (kk, vv) = l.last().unwrap();
                let (path, _) = t.descend(root, kk).unwrap();
                let (parent, slot) = *path.last().unwrap();
                let kids = {
                    let h = t.store.read_page(parent).unwrap();
                    let f = h.read();
                    Node::new(&f.data).all_children().unwrap()
                };
                let taken = match slot { None => 0, Some(j) => j + 1 };
                if taken + 1 >= kids.len() {
                    victims.push(kk.clone());
                }
                let _ = (i, vv);
            }
        }
        assert!(!victims.is_empty(), "no leaf is its parent's last child; the climb is untested");

        let e = cat.next_epoch();
        let mut survivors: Vec<(Vec<u8>, Vec<u8>)> =
            leaf_entries_of(&t, root).into_iter().flatten().collect();
        for victim in &victims {
            root = t.delete(root, BranchId::TRUNK, e, victim).unwrap();
            survivors.retain(|(kk, _)| kk != victim);
            let want = chunker_partition(&survivors);
            let got: Vec<usize> = leaf_entries_of(&t, root).iter().map(|l| l.len()).collect();
            assert_eq!(got, want, "cross-parent repair of {victim:?} did not match the chunker");
        }
        println!("    {} cross-parent terminators repaired at depth {depth}", victims.len());
    }

    /// An OVERWRITE can erase a terminator too, and `leaf_put` can only ever split a leaf
    /// further, never rejoin one — so without the same repair the two leaves stay split for good.
    ///
    /// The value has to get SHORTER, not longer, and that is not a detail: an entry is a boundary
    /// when its key hash falls in the lowest `size / target` of the hash space, so growing the
    /// value makes a boundary MORE likely and can never erase one. The first version of this test
    /// appended bytes trying to clear the flag, never cleared it once, and failed on its own
    /// vacuity guard — which is the guard doing its job.
    #[test]
    fn an_overwrite_that_erases_a_terminator_also_rejoins() {
        let (_d, cat, t) = tree();
        // Roomier values than `filled` uses, so emptying one is a big enough size change to move
        // an entry across the boundary threshold at all.
        let e0 = cat.next_epoch();
        let mut root = t.create(BranchId::TRUNK, e0).unwrap();
        for i in 0..4000u32 {
            root = t
                .insert(root, BranchId::TRUNK, e0, &k(i), format!("value{i:08}").as_bytes())
                .unwrap();
        }
        assert_unterminated_leaves(&t, root, 0, "a freshly built tree");

        let e = cat.next_epoch();
        let mut repaired = 0usize;
        for _ in 0..40usize {
            // A terminator that STOPS being one when its value is emptied. Searched for rather
            // than assumed: only entries whose hash sits in the band between the two sizes flip.
            let leaves = leaf_entries_of(&t, root);
            if leaves.len() < 8 {
                break;
            }
            let victim = leaves
                .iter()
                .take(leaves.len() - 1)
                .map(|l| l.last().unwrap().clone())
                .find(|(vk, vv)| {
                    chunker::is_boundary(vk, vv) && !chunker::is_boundary(vk, b"")
                });
            let Some((vk, _)) = victim else { break };

            let before = leaves.len();
            root = t.insert(root, BranchId::TRUNK, e, &vk, b"").unwrap();
            let after = leaf_entries_of(&t, root).len();
            assert!(
                after < before,
                "overwriting terminator {vk:?} with a shorter value did not rejoin anything \
                 ({before} -> {after} leaves)"
            );
            assert_unterminated_leaves(&t, root, 0, &format!("after overwriting {vk:?}"));
            repaired += 1;
        }
        assert!(
            repaired > 3,
            "only {repaired} overwrites erased a terminator; the fixture is not exercising this"
        );
        println!("    {repaired} terminator overwrites repaired");
    }

    /// The anti-corruption guard for the merge, as for the unlink: a repair must not lose,
    /// reorder or resurrect a row.
    #[test]
    fn no_row_is_lost_or_reordered_by_a_boundary_repair() {
        let (_d, cat, t) = tree();
        let mut root = filled(&t, &cat, 4000);
        let e = cat.next_epoch();

        let mut expect: Vec<u32> = (0..4000).collect();
        for step in 0..40u32 {
            let leaves = leaf_entries_of(&t, root);
            if leaves.len() < 3 { break; }
            let (vk, _) = leaves[(step as usize * 7) % (leaves.len() - 1)].last().unwrap().clone();
            let n = u32::from_be_bytes(vk.clone().try_into().unwrap());
            root = t.delete(root, BranchId::TRUNK, e, &vk).unwrap();
            expect.retain(|i| *i != n);

            let scanned: Vec<u32> = t
                .range_scan(root, None, None)
                .unwrap()
                .map(|r| u32::from_be_bytes(r.unwrap().0.try_into().unwrap()))
                .collect();
            assert_eq!(scanned, expect, "rows diverged after repair {step}");
        }
        for i in &expect {
            assert_eq!(
                t.get(root, &k(*i)).unwrap().as_deref(),
                Some(format!("v{i}").as_bytes()),
                "survivor {i} is gone or wrong"
            );
        }
    }

    /// **The cascade branch of [`CowTree::unlink_up`], which nothing else here reaches.**
    ///
    /// Every other test in this module uses a 1000-row fixture, and that is a TWO-level tree — one
    /// root over ~44 leaves. A leaf's parent is therefore the root, so emptying a subtree only ever
    /// exercises the cascaded-past-the-root tail. The branch that removes a **childless internal
    /// node from its own parent** — the case `unlink_up`'s longest doc paragraph exists to justify,
    /// and the one where getting it wrong leaves a live grandparent pointing at a freed page —
    /// never executed. Found by review, not by these tests.
    ///
    /// This forces it: build deep enough for a middle level, pick one mid-level internal node,
    /// delete every key beneath it, and require that the node leave the tree while everything
    /// else survives untouched.
    #[test]
    fn a_childless_internal_node_is_removed_from_its_parent() {
        let (_d, cat, t) = tree();
        let mut root = filled(&t, &cat, 12_000);

        // Non-vacuity, asserted rather than assumed: without a middle level there is no such node.
        let depth = t.descend(root, &k(0)).unwrap().0.len();
        assert!(depth >= 2, "tree is {depth} internal level(s); the cascade branch cannot exist");

        // A mid-level internal node, and every key beneath it.
        let mid = {
            let h = t.store.read_page(root).unwrap();
            let f = h.read();
            let kids = Node::new(&f.data).all_children().unwrap();
            assert!(kids.len() >= 2, "root has {} children; removing one would empty the tree", kids.len());
            kids[0]
        };
        let mut doomed_keys = Vec::new();
        {
            let mut leaves = Vec::new();
            collect_leaves(&t, mid, &mut leaves);
            assert!(leaves.len() > 1, "the chosen node has {} leaf; pick a real subtree", leaves.len());
            for l in leaves {
                for (kk, _) in l {
                    doomed_keys.push(kk);
                }
            }
        }
        let survivors_before: Vec<Vec<u8>> = leaf_entries_of(&t, root)
            .into_iter()
            .flatten()
            .map(|(kk, _)| kk)
            .filter(|kk| !doomed_keys.contains(kk))
            .collect();
        assert!(!survivors_before.is_empty(), "the subtree is the whole tree");

        let pages_before = t.store.live_page_count().unwrap();
        let e = cat.next_epoch();
        for kk in &doomed_keys {
            root = t.delete(root, BranchId::TRUNK, e, kk).unwrap();
        }

        // The internal node itself must be gone from the tree, not merely emptied.
        let reachable = t.walk_pages(root).unwrap();
        assert!(
            !reachable.contains(&mid),
            "the emptied internal node {mid} is still linked into the tree"
        );
        // Its pages went back, rather than being unlinked and leaked.
        let pages_after = t.store.live_page_count().unwrap();
        println!(
            "    removed a mid-level subtree of {} rows at depth {depth}: live pages {pages_before} -> {pages_after}",
            doomed_keys.len()
        );
        assert!(pages_after < pages_before, "live pages did not drop ({pages_before} -> {pages_after})");

        // And nothing else moved: every surviving row still readable, in order, none resurrected.
        for kk in &doomed_keys {
            assert_eq!(t.get(root, kk).unwrap(), None, "deleted key {kk:?} is still readable");
        }
        let scanned: Vec<Vec<u8>> = t
            .range_scan(root, None, None)
            .unwrap()
            .map(|r| r.unwrap().0)
            .collect();
        assert_eq!(scanned, survivors_before, "the surviving rows changed or reordered");
        assert_unterminated_leaves(&t, root, 0, "after removing a whole mid-level subtree");
    }

    /// A delete that hits nothing must still shadow nothing — the unlink path must not fire on a
    /// miss and must not be reached by one.
    #[test]
    fn deleting_an_absent_key_changes_nothing() {
        let (_d, cat, t) = tree();
        let root = filled(&t, &cat, 500);
        let pages = t.walk_pages(root).unwrap();
        let e = cat.next_epoch();
        let same = t.delete(root, BranchId::TRUNK, e, &k(9999)).unwrap();
        assert_eq!(same, root, "a miss shadowed the tree");
        assert_eq!(t.walk_pages(same).unwrap(), pages, "a miss changed the page set");
    }
}

#[cfg(test)]
mod right_walk_probe {
    //! DESIGN PROBE for the neighbour merge. Establishes one fact: the right-hand neighbour of a
    //! leaf is reachable from an ordinary descent path, with no sibling pointer anywhere.
    use super::*;
    use crate::branch::arena::ArenaPageStore;
    use crate::branch::catalog::LogBranchCatalog;
    use crate::branch::BranchCatalog;
    use crate::buffer::buffer_pool::BufferPoolManager;
    use crate::storage::disk_manager::DiskManager;

    fn tree() -> (tempfile::TempDir, Arc<LogBranchCatalog>, CowTree) {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(dir.path().join("rw.db")).unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let pool = Arc::new(BufferPoolManager::new(dm));
        let cat = Arc::new(LogBranchCatalog::in_memory(1));
        let store = Arc::new(
            ArenaPageStore::new(pool, Arc::clone(&cat) as Arc<dyn BranchCatalog>, 1024).unwrap(),
        );
        (dir, cat, CowTree::new(store as Arc<dyn PageStore>))
    }
    fn k(n: u32) -> Vec<u8> { n.to_be_bytes().to_vec() }

    impl CowTree {
        /// The leftmost leaf of the subtree at `pid`.
        fn leftmost_leaf_probe(&self, mut pid: PageId) -> Result<PageId, FerroError> {
            for _ in 0..MAX_DESCENT {
                let h = self.store.read_page(pid)?;
                let f = h.read();
                if PageHeader::read_from(&f.data)?.page_type == PageType::BTreeLeaf {
                    return Ok(pid);
                }
                let next = Node::new(&f.data).leftmost();
                drop(f); drop(h);
                pid = next;
            }
            Err(FerroError::Cow("leftmost walk exceeded the depth guard".into()))
        }

        /// The leaf immediately to the right of the one `path` ends at, or `None` if it is the
        /// last. **No sibling pointer**: it climbs the descent path to the shallowest ancestor
        /// that still has a child further right, then takes that subtree's leftmost leaf.
        fn right_neighbour_probe(&self, path: &DescentPath) -> Result<Option<PageId>, FerroError> {
            for (parent_id, slot) in path.iter().rev() {
                let kids = {
                    let h = self.store.read_page(*parent_id)?;
                    let f = h.read();
                    Node::new(&f.data).all_children()?
                };
                // `all_children` is [leftmost, child(0), child(1), ...], so slot i sits at i+1.
                let taken = match slot { None => 0, Some(i) => i + 1 };
                if taken + 1 < kids.len() {
                    return Ok(Some(self.leftmost_leaf_probe(kids[taken + 1])?));
                }
            }
            Ok(None)
        }
    }

    /// Exhaustive, not a sample: for EVERY leaf in the tree, the descent-path right-walk must
    /// name exactly the next leaf in key order, and `None` for the last one.
    #[test]
    fn every_leaf_can_reach_its_right_neighbour_without_a_sibling_pointer() {
        let (_d, cat, t) = tree();
        let e = cat.next_epoch();
        let mut root = t.create(BranchId::TRUNK, e).unwrap();
        // Deep enough that the right neighbour is NOT always a sibling under one parent. The
        // first version of this probe used 2000 rows, got a 2-level tree (90 leaves under a
        // single root), and so never exercised the climb at all -- it verified only the easy
        // case while reading as exhaustive. The depth is asserted below for that reason.
        for i in 0..12_000u32 {
            root = t.insert(root, BranchId::TRUNK, e, &k(i), format!("v{i}").as_bytes()).unwrap();
        }

        // Ground truth: the leaves in key order, by first key.
        let mut truth: Vec<(PageId, Vec<u8>)> = Vec::new();
        fn walk(t: &CowTree, pid: PageId, out: &mut Vec<(PageId, Vec<u8>)>) {
            let h = t.store.read_page(pid).unwrap();
            let f = h.read();
            let ty = PageHeader::read_from(&f.data).unwrap().page_type;
            let n = Node::new(&f.data);
            if ty == PageType::BTreeLeaf {
                out.push((pid, n.key(0).unwrap().to_vec()));
            } else {
                let kids = n.all_children().unwrap();
                drop(f); drop(h);
                for c in kids { walk(t, c, out); }
            }
        }
        walk(&t, root, &mut truth);
        assert!(truth.len() > 8, "only {} leaves; the probe would be vacuous", truth.len());
        // The whole point: at depth 1 every right neighbour shares a parent and the climb is
        // never taken. Refuse rather than report a pass that proves only the easy half.
        let depth = t.descend(root, &truth[0].1).unwrap().0.len();
        assert!(depth >= 2, "tree is {depth} internal level(s); the cross-parent climb is untested");
        // And at least one leaf must actually REQUIRE the climb, i.e. be the last child of its
        // own parent while still having a neighbour to its right.
        let mut climbed = 0usize;
        for (i, (_, first_key)) in truth.iter().enumerate().take(truth.len() - 1) {
            let (path, _) = t.descend(root, first_key).unwrap();
            let (parent, slot) = *path.last().unwrap();
            let kids = {
                let h = t.store.read_page(parent).unwrap();
                let f = h.read();
                Node::new(&f.data).all_children().unwrap()
            };
            let taken = match slot { None => 0, Some(j) => j + 1 };
            if taken + 1 >= kids.len() { climbed += 1; }
            let _ = i;
        }
        assert!(climbed > 0, "no leaf is the last child of its parent; the climb is still untested");

        for (i, (leaf, first_key)) in truth.iter().enumerate() {
            let (path, landed) = t.descend(root, first_key).unwrap();
            assert_eq!(landed, *leaf, "descent to leaf {i}'s own first key missed it");
            let got = t.right_neighbour_probe(&path).unwrap();
            let want = truth.get(i + 1).map(|(p, _)| *p);
            assert_eq!(got, want, "right neighbour of leaf {i} of {}", truth.len());
        }
        println!(
            "    right-walk verified for all {} leaves at depth {}; {} of them required the \
             cross-parent climb. No sibling pointer anywhere.",
            truth.len(),
            depth,
            climbed
        );
    }
}
