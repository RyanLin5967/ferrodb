//! Slotted node layout for the copy-on-write B+tree.
//!
//! Design authority: DESIGN.md section 1.
//!
//! Nodes live entirely inside a page's **payload**, i.e. `page[PAGE_HEADER_SIZE..]`. The
//! self-describing [`crate::cow::PageHeader`] owns the first 24 bytes and nothing here may touch
//! them: `birth_epoch` and `arena_id` are what the GC algebra reads, so a node encoding that
//! overwrote the header would silently break reclamation.
//!
//! Deliberately **no leaf sibling pointers**. In a shadow-paging tree a `next_leaf` link makes
//! shadowing one leaf require shadowing its left neighbour too, which cascades along the whole
//! leaf level and turns a single-key update into a full-level copy. LMDB does not have them
//! either; ordered iteration uses a descent stack instead (see [`crate::cow::btree::CowTree`]).
//!
//! Payload layout (all integers big-endian, matching the rest of ferrodb):
//!
//! ```text
//! 0..4     count           u32   number of slots
//! 4..8     heap_end        u32   offset (within payload) where cell data begins
//! 8..12    leftmost_child  u32   internal nodes only; 0 in a leaf
//! 12..     slot array            count * { cell_offset u32, cell_len u32 }
//! ...
//! heap_end..PAYLOAD_LEN    cell data, growing backwards
//! ```
//!
//! Cell encoding:
//! - leaf:     `key_len u32 | key | value`   (the value is the remainder of the cell)
//! - internal: `key_len u32 | key | child u32`

use std::cmp::Ordering;

use crate::branch::types::PageId;
use crate::cow::page_header::PAGE_HEADER_SIZE;
use crate::error::FerroError;
use crate::storage::disk_manager::PAGE_SIZE;

/// Bytes of a page available to a node.
pub const PAYLOAD_LEN: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

const OFF_COUNT: usize = 0;
const OFF_HEAP_END: usize = 4;
const OFF_LEFTMOST: usize = 8;
const SLOT_BASE: usize = 12;
const SLOT_SIZE: usize = 8;

/// Space a node can use for slots plus cells.
pub const NODE_CAPACITY: usize = PAYLOAD_LEN - SLOT_BASE;

/// Largest a single entry (slot + cell) may be. A quarter of the node keeps the split
/// arithmetic safe: an overflowing node holds at most `NODE_CAPACITY + MAX_ENTRY_BYTES` and each
/// half of a byte-balanced split is then at most `(NODE_CAPACITY + MAX_ENTRY_BYTES)/2 +
/// MAX_ENTRY_BYTES`, which still fits.
pub const MAX_ENTRY_BYTES: usize = NODE_CAPACITY / 4;

/// Key/value pairs held by a leaf.
pub type LeafEntries = Vec<(Vec<u8>, Vec<u8>)>;
/// Separator/child pairs held by an internal node.
pub type InternalEntries = Vec<(Vec<u8>, PageId)>;

/// Bytes an entry occupies: its slot plus its cell.
pub fn leaf_entry_bytes(key: &[u8], value: &[u8]) -> usize {
    SLOT_SIZE + 4 + key.len() + value.len()
}

/// Bytes a separator entry occupies inside an internal node.
pub fn internal_entry_bytes(key: &[u8]) -> usize {
    SLOT_SIZE + 4 + key.len() + 4
}

pub fn leaf_cell(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut c = Vec::with_capacity(4 + key.len() + value.len());
    c.extend_from_slice(&(key.len() as u32).to_be_bytes());
    c.extend_from_slice(key);
    c.extend_from_slice(value);
    c
}

pub fn internal_cell(key: &[u8], child: PageId) -> Vec<u8> {
    let mut c = Vec::with_capacity(8 + key.len());
    c.extend_from_slice(&(key.len() as u32).to_be_bytes());
    c.extend_from_slice(key);
    c.extend_from_slice(&child.to_be_bytes());
    c
}

fn cell_key(cell: &[u8]) -> Result<&[u8], FerroError> {
    if cell.len() < 4 {
        return Err(FerroError::Cow("truncated node cell".into()));
    }
    let klen = u32::from_be_bytes(cell[0..4].try_into().unwrap()) as usize;
    cell.get(4..4 + klen).ok_or_else(|| FerroError::Cow("node cell key overruns cell".into()))
}

fn cell_rest(cell: &[u8]) -> Result<&[u8], FerroError> {
    let klen = u32::from_be_bytes(
        cell.get(0..4).ok_or_else(|| FerroError::Cow("truncated node cell".into()))?
            .try_into()
            .unwrap(),
    ) as usize;
    cell.get(4 + klen..).ok_or_else(|| FerroError::Cow("node cell body overruns cell".into()))
}

/// Read-only view of a node.
pub struct Node<'a> {
    payload: &'a [u8],
}

impl<'a> Node<'a> {
    pub fn new(page: &'a [u8; PAGE_SIZE]) -> Self {
        Node { payload: &page[PAGE_HEADER_SIZE..] }
    }

    pub fn count(&self) -> usize {
        u32::from_be_bytes(self.payload[OFF_COUNT..OFF_COUNT + 4].try_into().unwrap()) as usize
    }

    fn heap_end(&self) -> usize {
        u32::from_be_bytes(self.payload[OFF_HEAP_END..OFF_HEAP_END + 4].try_into().unwrap()) as usize
    }

    pub fn leftmost(&self) -> PageId {
        u32::from_be_bytes(self.payload[OFF_LEFTMOST..OFF_LEFTMOST + 4].try_into().unwrap())
    }

    fn slot(&self, i: usize) -> Result<(usize, usize), FerroError> {
        if i >= self.count() {
            return Err(FerroError::Cow(format!("node slot {} out of range", i)));
        }
        let at = SLOT_BASE + i * SLOT_SIZE;
        let off = u32::from_be_bytes(self.payload[at..at + 4].try_into().unwrap()) as usize;
        let len = u32::from_be_bytes(self.payload[at + 4..at + 8].try_into().unwrap()) as usize;
        if off + len > PAYLOAD_LEN {
            return Err(FerroError::Cow("node cell overruns the page".into()));
        }
        Ok((off, len))
    }

    fn cell(&self, i: usize) -> Result<&'a [u8], FerroError> {
        let (off, len) = self.slot(i)?;
        Ok(&self.payload[off..off + len])
    }

    pub fn key(&self, i: usize) -> Result<&'a [u8], FerroError> {
        cell_key(self.cell(i)?)
    }

    /// Leaf value at slot `i`.
    pub fn value(&self, i: usize) -> Result<&'a [u8], FerroError> {
        cell_rest(self.cell(i)?)
    }

    /// Internal child pointer at slot `i`.
    pub fn child(&self, i: usize) -> Result<PageId, FerroError> {
        let rest = cell_rest(self.cell(i)?)?;
        if rest.len() != 4 {
            return Err(FerroError::Cow("internal cell is not a child pointer".into()));
        }
        Ok(u32::from_be_bytes(rest.try_into().unwrap()))
    }

    /// Binary search over the node's keys. `Ok(i)` is an exact hit, `Err(i)` the insertion point.
    pub fn search(&self, key: &[u8]) -> Result<Result<usize, usize>, FerroError> {
        let (mut lo, mut hi) = (0usize, self.count());
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.key(mid)?.cmp(key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Ok(Ok(mid)),
            }
        }
        Ok(Err(lo))
    }

    /// Which child of an internal node covers `key`. `None` means the leftmost child, which has
    /// no slot of its own.
    pub fn child_slot_for(&self, key: &[u8]) -> Result<(Option<usize>, PageId), FerroError> {
        match self.search(key)? {
            Ok(i) => Ok((Some(i), self.child(i)?)),
            Err(0) => Ok((None, self.leftmost())),
            Err(i) => Ok((Some(i - 1), self.child(i - 1)?)),
        }
    }

    pub fn leaf_entries(&self) -> Result<LeafEntries, FerroError> {
        let mut out = Vec::with_capacity(self.count());
        for i in 0..self.count() {
            out.push((self.key(i)?.to_vec(), self.value(i)?.to_vec()));
        }
        Ok(out)
    }

    pub fn internal_entries(&self) -> Result<InternalEntries, FerroError> {
        let mut out = Vec::with_capacity(self.count());
        for i in 0..self.count() {
            out.push((self.key(i)?.to_vec(), self.child(i)?));
        }
        Ok(out)
    }

    /// Every child pointer, leftmost first. Used by the reaper and by whole-subtree walks.
    pub fn all_children(&self) -> Result<Vec<PageId>, FerroError> {
        let mut out = Vec::with_capacity(self.count() + 1);
        out.push(self.leftmost());
        for i in 0..self.count() {
            out.push(self.child(i)?);
        }
        Ok(out)
    }

    pub fn used_bytes(&self) -> usize {
        self.count() * SLOT_SIZE + (PAYLOAD_LEN - self.heap_end())
    }
}

/// Mutable view of a node.
pub struct NodeMut<'a> {
    payload: &'a mut [u8],
}

impl<'a> NodeMut<'a> {
    pub fn new(page: &'a mut [u8; PAGE_SIZE]) -> Self {
        NodeMut { payload: &mut page[PAGE_HEADER_SIZE..] }
    }

    /// Read-only view of the same node, for the searches a mutation has to do first.
    pub fn view(&self) -> Node<'_> {
        Node { payload: self.payload }
    }

    pub fn count(&self) -> usize {
        self.view().count()
    }

    fn set_count(&mut self, n: usize) {
        self.payload[OFF_COUNT..OFF_COUNT + 4].copy_from_slice(&(n as u32).to_be_bytes());
    }

    fn heap_end(&self) -> usize {
        self.view().heap_end()
    }

    fn set_heap_end(&mut self, v: usize) {
        self.payload[OFF_HEAP_END..OFF_HEAP_END + 4].copy_from_slice(&(v as u32).to_be_bytes());
    }

    pub fn leftmost(&self) -> PageId {
        self.view().leftmost()
    }

    pub fn set_leftmost(&mut self, child: PageId) {
        self.payload[OFF_LEFTMOST..OFF_LEFTMOST + 4].copy_from_slice(&child.to_be_bytes());
    }

    /// Reset the node to empty. Leaves the page header untouched.
    pub fn init(&mut self) {
        for b in self.payload.iter_mut() {
            *b = 0;
        }
        self.set_heap_end(PAYLOAD_LEN);
    }

    fn write_slot(&mut self, i: usize, off: usize, len: usize) {
        let at = SLOT_BASE + i * SLOT_SIZE;
        self.payload[at..at + 4].copy_from_slice(&(off as u32).to_be_bytes());
        self.payload[at + 4..at + 8].copy_from_slice(&(len as u32).to_be_bytes());
    }

    /// Append `cell` to the cell heap, reserving room for `extra_slots` slots beyond the ones
    /// already in use.
    ///
    /// `extra_slots` is not cosmetic: compaction rewrites the cells that are already slotted and
    /// must reserve **zero** extra, while an insert needs one. Reserving one during compaction
    /// makes a node that is exactly full impossible to compact, which surfaces as a spurious
    /// overflow at the worst moment.
    fn write_cell(&mut self, cell: &[u8], extra_slots: usize) -> Option<usize> {
        let count = self.count();
        let heap_end = self.heap_end();
        let slots_end = SLOT_BASE + (count + extra_slots) * SLOT_SIZE;
        if cell.len() > heap_end || slots_end > heap_end - cell.len() {
            return None;
        }
        let off = heap_end - cell.len();
        self.payload[off..heap_end].copy_from_slice(cell);
        self.set_heap_end(off);
        Some(off)
    }

    /// Rebuild the cell area, dropping the garbage left behind by removals and overwrites.
    fn compact(&mut self) -> Result<(), FerroError> {
        let count = self.count();
        let mut cells = Vec::with_capacity(count);
        {
            let v = self.view();
            for i in 0..count {
                cells.push(v.cell(i)?.to_vec());
            }
        }
        self.set_heap_end(PAYLOAD_LEN);
        for (i, cell) in cells.iter().enumerate() {
            let off = self
                .write_cell(cell, 0)
                .ok_or_else(|| FerroError::Cow("node compaction overflowed".into()))?;
            self.write_slot(i, off, cell.len());
        }
        Ok(())
    }

    /// Insert `cell` at slot `i`. Returns `false` (leaving the node untouched) when the node is
    /// full even after compaction — the caller must split.
    pub fn insert_cell_at(&mut self, i: usize, cell: &[u8]) -> Result<bool, FerroError> {
        let count = self.count();
        if i > count {
            return Err(FerroError::Cow(format!("insert slot {} past end {}", i, count)));
        }
        let off = match self.write_cell(cell, 1) {
            Some(off) => off,
            None => {
                self.compact()?;
                match self.write_cell(cell, 1) {
                    Some(off) => off,
                    None => return Ok(false),
                }
            }
        };
        // shift the slots above `i` up by one
        let from = SLOT_BASE + i * SLOT_SIZE;
        let to = SLOT_BASE + count * SLOT_SIZE;
        self.payload.copy_within(from..to, from + SLOT_SIZE);
        self.write_slot(i, off, cell.len());
        self.set_count(count + 1);
        Ok(true)
    }

    /// Replace the cell at slot `i`. Returns `false` when it does not fit.
    pub fn replace_cell_at(&mut self, i: usize, cell: &[u8]) -> Result<bool, FerroError> {
        if i >= self.count() {
            return Err(FerroError::Cow(format!("replace slot {} out of range", i)));
        }
        let off = match self.write_cell(cell, 0) {
            Some(off) => off,
            None => {
                self.compact()?;
                match self.write_cell(cell, 0) {
                    Some(off) => off,
                    None => return Ok(false),
                }
            }
        };
        self.write_slot(i, off, cell.len());
        Ok(true)
    }

    pub fn remove_at(&mut self, i: usize) -> Result<(), FerroError> {
        let count = self.count();
        if i >= count {
            return Err(FerroError::Cow(format!("remove slot {} out of range", i)));
        }
        let from = SLOT_BASE + (i + 1) * SLOT_SIZE;
        let to = SLOT_BASE + count * SLOT_SIZE;
        self.payload.copy_within(from..to, from - SLOT_SIZE);
        self.set_count(count - 1);
        Ok(())
    }

    /// Overwrite the child pointer stored at slot `i`, keeping its key.
    ///
    /// Written **in place**, over the existing cell's trailing four bytes. Rewriting the cell
    /// instead would need fresh heap space, and relinking a child after a copy-on-write is exactly
    /// the moment a node is most likely to be full — a relink that could fail for lack of space
    /// would strand the new child with nothing pointing at it.
    pub fn set_child(&mut self, i: usize, child: PageId) -> Result<(), FerroError> {
        let (off, len) = self.view().slot(i)?;
        let rest_len = {
            let v = self.view();
            cell_rest(v.cell(i)?)?.len()
        };
        if rest_len != 4 {
            return Err(FerroError::Cow("slot does not hold a child pointer".into()));
        }
        let at = off + len - 4;
        self.payload[at..at + 4].copy_from_slice(&child.to_be_bytes());
        Ok(())
    }

    /// Fill an empty leaf with `entries` (which must be sorted and must fit).
    pub fn fill_leaf(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), FerroError> {
        self.init();
        for (i, (k, v)) in entries.iter().enumerate() {
            let cell = leaf_cell(k, v);
            let off = self
                .write_cell(&cell, 1)
                .ok_or_else(|| FerroError::Cow("leaf fill overflowed the page".into()))?;
            self.write_slot(i, off, cell.len());
            self.set_count(i + 1);
        }
        Ok(())
    }

    /// Fill an empty internal node with `leftmost` plus `entries`.
    pub fn fill_internal(
        &mut self,
        leftmost: PageId,
        entries: &[(Vec<u8>, PageId)],
    ) -> Result<(), FerroError> {
        self.init();
        self.set_leftmost(leftmost);
        for (i, (k, c)) in entries.iter().enumerate() {
            let cell = internal_cell(k, *c);
            let off = self
                .write_cell(&cell, 1)
                .ok_or_else(|| FerroError::Cow("internal fill overflowed the page".into()))?;
            self.write_slot(i, off, cell.len());
            self.set_count(i + 1);
        }
        Ok(())
    }
}

/// Split point for a byte-balanced split: the first index at which the accumulated size reaches
/// half the total, clamped so both halves are non-empty.
pub fn split_point(sizes: &[usize]) -> usize {
    let total: usize = sizes.iter().sum();
    let mut acc = 0usize;
    for (i, s) in sizes.iter().enumerate() {
        acc += s;
        if acc * 2 >= total && i + 1 < sizes.len() {
            return i + 1;
        }
    }
    sizes.len().saturating_sub(1).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blank() -> [u8; PAGE_SIZE] {
        let mut p = [0u8; PAGE_SIZE];
        NodeMut::new(&mut p).init();
        p
    }

    #[test]
    fn node_body_never_touches_the_page_header() {
        let mut p = [0u8; PAGE_SIZE];
        p[..PAGE_HEADER_SIZE].copy_from_slice(&[0xAB; PAGE_HEADER_SIZE]);
        let mut n = NodeMut::new(&mut p);
        n.init();
        n.insert_cell_at(0, &leaf_cell(b"k", b"v")).unwrap();
        assert_eq!(&p[..PAGE_HEADER_SIZE], &[0xAB; PAGE_HEADER_SIZE]);
    }

    #[test]
    fn leaf_insert_search_and_remove() {
        let mut p = blank();
        {
            let mut n = NodeMut::new(&mut p);
            for k in [b"c", b"a", b"e", b"b"] {
                let idx = match n.view().search(k).unwrap() {
                    Ok(i) => i,
                    Err(i) => i,
                };
                assert!(n.insert_cell_at(idx, &leaf_cell(k, k)).unwrap());
            }
        }
        {
        let n = Node::new(&p);
        assert_eq!(n.count(), 4);
        assert_eq!(n.key(0).unwrap(), b"a");
        assert_eq!(n.key(3).unwrap(), b"e");
        assert_eq!(n.search(b"b").unwrap(), Ok(1));
        assert_eq!(n.search(b"d").unwrap(), Err(3));
        }

        NodeMut::new(&mut p).remove_at(1).unwrap();
        let n = Node::new(&p);
        assert_eq!(n.count(), 3);
        assert_eq!(n.key(1).unwrap(), b"c");
    }

    #[test]
    fn repeated_overwrite_is_reclaimed_by_compaction() {
        let mut p = blank();
        {
            let mut n = NodeMut::new(&mut p);
            n.insert_cell_at(0, &leaf_cell(b"k", &[0u8; 900])).unwrap();
            // 900-byte value rewritten far more times than the page could hold without compaction
            for _ in 0..50 {
                assert!(n.replace_cell_at(0, &leaf_cell(b"k", &[1u8; 900])).unwrap());
            }
            assert_eq!(n.count(), 1);
        }
        assert_eq!(Node::new(&p).value(0).unwrap(), &[1u8; 900]);
    }

    #[test]
    fn a_full_node_reports_full_instead_of_corrupting_itself() {
        let mut p = blank();
        let mut inserted = 0;
        {
            let mut n = NodeMut::new(&mut p);
            loop {
                let key = format!("{:08}", inserted).into_bytes();
                let cell = leaf_cell(&key, &[7u8; 200]);
                let idx = match n.view().search(&key).unwrap() {
                    Ok(i) => i,
                    Err(i) => i,
                };
                if !n.insert_cell_at(idx, &cell).unwrap() {
                    break;
                }
                inserted += 1;
                assert!(inserted < 1000, "node never filled up");
            }
        }
        assert!(inserted > 0);
        let v = Node::new(&p);
        assert_eq!(v.count(), inserted);
        for i in 0..inserted {
            assert_eq!(v.value(i).unwrap(), &[7u8; 200]);
        }
    }

    #[test]
    fn internal_child_routing_uses_the_leftmost_slot_for_small_keys() {
        let mut p = blank();
        {
            let mut n = NodeMut::new(&mut p);
            n.fill_internal(10, &[(b"m".to_vec(), 20), (b"t".to_vec(), 30)]).unwrap();
        }
        let n = Node::new(&p);
        assert_eq!(n.child_slot_for(b"a").unwrap(), (None, 10));
        assert_eq!(n.child_slot_for(b"m").unwrap(), (Some(0), 20));
        assert_eq!(n.child_slot_for(b"p").unwrap(), (Some(0), 20));
        assert_eq!(n.child_slot_for(b"z").unwrap(), (Some(1), 30));
        assert_eq!(n.all_children().unwrap(), vec![10, 20, 30]);
    }

    #[test]
    fn split_point_keeps_both_halves_non_empty() {
        assert_eq!(split_point(&[10, 10]), 1);
        assert_eq!(split_point(&[1, 1, 1, 1]), 2);
        // one huge entry first: the split must still leave something on the right
        assert_eq!(split_point(&[100, 1, 1]), 1);
        // one huge entry last
        assert_eq!(split_point(&[1, 1, 100]), 2);
    }
}
