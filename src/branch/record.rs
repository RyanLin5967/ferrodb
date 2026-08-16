//! The durable branch metadata record, and the GC reclamation predicate that its shape exists
//! to answer.
//!
//! Design authority: DESIGN.md section 1.
//!
//! Fork = one durable `BranchRecord` + append `fork_epoch` to the parent's sorted
//! `live_children` array. No page is read, written, or refcounted, which is exit criterion 1.

use crate::branch::types::{
    ArenaId, BranchError, BranchId, BranchState, Epoch, LeaseDeadline, PageId, MAX_BRANCH_DEPTH,
};
use crate::wal::log::crc32;

/// Durable metadata for one branch. This record *is* the branch — there is nothing else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchRecord {
    /// Identity as minted, including the generation live at creation time.
    pub branch_id: BranchId,
    /// Current generation of this id slot. Equals `branch_id.generation` while the branch is
    /// `Live`; bumped when the branch is reaped so stale handles fail loudly.
    pub generation: u32,
    /// `None` only for the trunk.
    pub parent_id: Option<BranchId>,
    /// The epoch at which this branch forked from its parent. This is the single value the
    /// reclamation rule consults.
    pub fork_epoch: Epoch,
    /// The B+tree root. At fork this is byte-identical to the parent's root — that is the
    /// entire fork operation, and it is why the read path never walks the parent chain.
    pub root_page_id: PageId,
    /// Non-cooperative reaping deadline. Every branch has one; there is no exemption class.
    pub lease_deadline: LeaseDeadline,
    pub state: BranchState,
    /// Private extents this branch allocates novel pages from. Reaping a childless leaf frees
    /// these wholesale with no sharing analysis.
    pub arenas: Vec<ArenaId>,
    /// Fork epochs of this branch's **live** children, kept sorted ascending. The reclamation
    /// rule is a range-emptiness query over this array: O(log k).
    pub live_children: Vec<Epoch>,
    /// Ancestry depth; 0 for trunk. Collapse when this would exceed `MAX_BRANCH_DEPTH`.
    pub depth: u8,
}

impl BranchRecord {
    /// The trunk record. Never reaped, no parent, depth 0.
    pub fn trunk(root_page_id: PageId, lease_deadline: LeaseDeadline) -> Self {
        BranchRecord {
            branch_id: BranchId::TRUNK,
            generation: 0,
            parent_id: None,
            fork_epoch: Epoch::ZERO,
            root_page_id,
            lease_deadline,
            state: BranchState::Live,
            arenas: Vec::new(),
            live_children: Vec::new(),
            depth: 0,
        }
    }

    /// Build the child record for a fork. Does **not** mutate the parent — the caller must also
    /// call [`BranchRecord::add_live_child`] on the parent and durably record both.
    pub fn fork_child(
        parent: &BranchRecord,
        child_id: BranchId,
        fork_epoch: Epoch,
        lease_deadline: LeaseDeadline,
    ) -> Result<Self, BranchError> {
        if parent.state != BranchState::Live {
            return Err(BranchError::NotWritable(parent.branch_id));
        }
        let depth = parent.depth + 1;
        if depth > MAX_BRANCH_DEPTH {
            return Err(BranchError::DepthExceeded { branch: parent.branch_id, depth });
        }
        Ok(BranchRecord {
            branch_id: child_id,
            generation: child_id.generation,
            parent_id: Some(parent.branch_id),
            fork_epoch,
            // The whole fork: the child's root IS the parent's root.
            root_page_id: parent.root_page_id,
            lease_deadline,
            state: BranchState::Live,
            arenas: Vec::new(),
            live_children: Vec::new(),
            depth,
        })
    }

    /// Insert a child's fork epoch into the sorted live-children array.
    pub fn add_live_child(&mut self, fork_epoch: Epoch) {
        let at = self.live_children.partition_point(|e| *e < fork_epoch);
        self.live_children.insert(at, fork_epoch);
    }

    /// Remove one occurrence of a child's fork epoch (called when that child is reaped).
    /// Returns true if an entry was removed.
    pub fn remove_live_child(&mut self, fork_epoch: Epoch) -> bool {
        let at = self.live_children.partition_point(|e| *e < fork_epoch);
        if at < self.live_children.len() && self.live_children[at] == fork_epoch {
            self.live_children.remove(at);
            true
        } else {
            false
        }
    }

    /// A childless leaf takes the reaper's fast path: free its arenas wholesale, no sharing
    /// analysis at all. This is the overwhelming majority of abandoned agent branches.
    pub fn is_childless_leaf(&self) -> bool {
        self.live_children.is_empty()
    }

    /// **The reclamation rule.** Page `p` is reclaimable iff no live child of this branch has
    /// `fork_epoch` in `[birth, freed)`.
    ///
    /// Correctness: a child forked at epoch `e` sees pages live at `e`; `p` was live over
    /// `[birth, freed)`; so `p` is visible to that child iff `e` falls in that interval.
    pub fn page_reclaimable(&self, birth: Epoch, freed: Epoch) -> bool {
        reclaimable(&self.live_children, birth, freed)
    }

    /// Ordinary reads/writes reject anything not `Live`, and reject a stale generation outright.
    pub fn check_readable(&self, requested: BranchId) -> Result<(), BranchError> {
        if requested.generation != self.generation || self.state == BranchState::Reaped {
            return Err(BranchError::Reaped {
                requested,
                current_generation: self.generation,
            });
        }
        match self.state {
            BranchState::Live => Ok(()),
            BranchState::Reaping => Err(BranchError::Reaping(self.branch_id)),
            BranchState::Reaped => Err(BranchError::Reaped {
                requested,
                current_generation: self.generation,
            }),
        }
    }

    /// Mark reaped and bump the generation so the id slot can never be confused for the branch
    /// that used to live in it.
    pub fn mark_reaped(&mut self) {
        self.state = BranchState::Reaped;
        self.generation += 1;
        self.arenas.clear();
    }

    // ---- durable form ------------------------------------------------------------------
    //
    // |branch_id.id u64|branch_id.generation u32|generation u32|has_parent u8|
    // |parent.id u64|parent.generation u32|fork_epoch u64|root_page_id u32|
    // |lease_deadline u64|state u8|depth u8|arena_len u32|arenas..u32|
    // |child_len u32|children..u64|crc32 u32|
    // All integers big-endian, matching the rest of ferrodb's on-disk encodings.

    pub fn serialize(&self) -> Vec<u8> {
        let mut b: Vec<u8> = Vec::with_capacity(64 + self.arenas.len() * 4 + self.live_children.len() * 8);
        b.extend_from_slice(&self.branch_id.id.to_be_bytes());
        b.extend_from_slice(&self.branch_id.generation.to_be_bytes());
        b.extend_from_slice(&self.generation.to_be_bytes());
        match self.parent_id {
            Some(p) => {
                b.push(1);
                b.extend_from_slice(&p.id.to_be_bytes());
                b.extend_from_slice(&p.generation.to_be_bytes());
            }
            None => {
                b.push(0);
                b.extend_from_slice(&0u64.to_be_bytes());
                b.extend_from_slice(&0u32.to_be_bytes());
            }
        }
        b.extend_from_slice(&self.fork_epoch.0.to_be_bytes());
        b.extend_from_slice(&self.root_page_id.to_be_bytes());
        b.extend_from_slice(&self.lease_deadline.0.to_be_bytes());
        b.push(self.state.as_u8());
        b.push(self.depth);
        b.extend_from_slice(&(self.arenas.len() as u32).to_be_bytes());
        for a in &self.arenas {
            b.extend_from_slice(&a.0.to_be_bytes());
        }
        b.extend_from_slice(&(self.live_children.len() as u32).to_be_bytes());
        for c in &self.live_children {
            b.extend_from_slice(&c.0.to_be_bytes());
        }
        let crc = crc32(&b);
        b.extend_from_slice(&crc.to_be_bytes());
        b
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, BranchError> {
        let mut c = Cursor::new(bytes);
        let body_len = bytes
            .len()
            .checked_sub(4)
            .ok_or_else(|| BranchError::Corrupt("record shorter than its checksum".into()))?;
        let stored = u32::from_be_bytes(
            bytes[body_len..]
                .try_into()
                .map_err(|_| BranchError::Corrupt("truncated checksum".into()))?,
        );
        if crc32(&bytes[..body_len]) != stored {
            return Err(BranchError::Corrupt("branch record checksum mismatch".into()));
        }

        let id = c.u64()?;
        let gen_at_birth = c.u32()?;
        let generation = c.u32()?;
        let has_parent = c.u8()?;
        let p_id = c.u64()?;
        let p_gen = c.u32()?;
        let parent_id = if has_parent == 1 { Some(BranchId::new(p_id, p_gen)) } else { None };
        let fork_epoch = Epoch(c.u64()?);
        let root_page_id = c.u32()?;
        let lease_deadline = LeaseDeadline(c.u64()?);
        let state = BranchState::from_u8(c.u8()?)?;
        let depth = c.u8()?;
        let arena_len = c.u32()? as usize;
        let mut arenas = Vec::with_capacity(arena_len);
        for _ in 0..arena_len {
            arenas.push(ArenaId(c.u32()?));
        }
        let child_len = c.u32()? as usize;
        let mut live_children = Vec::with_capacity(child_len);
        for _ in 0..child_len {
            live_children.push(Epoch(c.u64()?));
        }
        Ok(BranchRecord {
            branch_id: BranchId::new(id, gen_at_birth),
            generation,
            parent_id,
            fork_epoch,
            root_page_id,
            lease_deadline,
            state,
            arenas,
            live_children,
            depth,
        })
    }
}

/// Free-function form of the reclamation rule, so the GC path can evaluate it without holding a
/// whole `BranchRecord`.
///
/// `live_children` must be sorted ascending. Returns true iff the half-open interval
/// `[birth, freed)` contains no live child fork epoch. An empty or inverted interval is vacuously
/// reclaimable.
pub fn reclaimable(live_children: &[Epoch], birth: Epoch, freed: Epoch) -> bool {
    if freed <= birth {
        return true;
    }
    let lo = live_children.partition_point(|e| *e < birth);
    // first index with e >= freed
    let hi = live_children.partition_point(|e| *e < freed);
    lo == hi
}

/// A page that has been logically freed but must wait for the reclamation rule to clear it.
/// Slow-path reaping (branch had live children) parks entries here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingFree {
    pub page_id: PageId,
    pub arena_id: ArenaId,
    pub birth_epoch: Epoch,
    pub free_epoch: Epoch,
    /// The branch whose `live_children` array decides this entry.
    pub owner: BranchId,
}

/// One contiguous private extent. Reaping a childless leaf frees these whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArenaExtent {
    pub arena_id: ArenaId,
    pub owner: BranchId,
    pub start_page: PageId,
    pub page_count: u32,
    /// Next unallocated page within the extent, as an offset from `start_page`.
    pub next_free: u32,
}

impl ArenaExtent {
    pub fn remaining(&self) -> u32 {
        self.page_count.saturating_sub(self.next_free)
    }

    pub fn contains(&self, page_id: PageId) -> bool {
        page_id >= self.start_page && page_id < self.start_page + self.page_count
    }
}

struct Cursor<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(b: &'a [u8]) -> Self {
        Cursor { b, at: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], BranchError> {
        if self.at + n > self.b.len() {
            return Err(BranchError::Corrupt(format!(
                "branch record truncated at byte {} (wanted {})",
                self.at, n
            )));
        }
        let s = &self.b[self.at..self.at + n];
        self.at += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, BranchError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, BranchError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, BranchError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_copies_the_root_pointer_and_nothing_else() {
        let parent = BranchRecord::trunk(42, LeaseDeadline(0));
        let child = BranchRecord::fork_child(
            &parent,
            BranchId::new(1, 0),
            Epoch(10),
            LeaseDeadline(9_999),
        )
        .unwrap();
        assert_eq!(child.root_page_id, parent.root_page_id);
        assert_eq!(child.parent_id, Some(BranchId::TRUNK));
        assert_eq!(child.depth, 1);
        assert!(child.arenas.is_empty());
    }

    #[test]
    fn depth_guard_fires_at_eight() {
        let mut r = BranchRecord::trunk(1, LeaseDeadline(0));
        r.depth = MAX_BRANCH_DEPTH;
        let err = BranchRecord::fork_child(&r, BranchId::new(2, 0), Epoch(1), LeaseDeadline(0));
        assert!(matches!(err, Err(BranchError::DepthExceeded { .. })));
    }

    #[test]
    fn live_children_stay_sorted() {
        let mut r = BranchRecord::trunk(1, LeaseDeadline(0));
        for e in [Epoch(50), Epoch(10), Epoch(30), Epoch(20)] {
            r.add_live_child(e);
        }
        assert_eq!(r.live_children, vec![Epoch(10), Epoch(20), Epoch(30), Epoch(50)]);
        assert!(r.remove_live_child(Epoch(30)));
        assert!(!r.remove_live_child(Epoch(30)));
        assert_eq!(r.live_children, vec![Epoch(10), Epoch(20), Epoch(50)]);
    }

    #[test]
    fn reclamation_rule_is_half_open_over_the_fork_epochs() {
        let children = vec![Epoch(10), Epoch(20), Epoch(30)];
        // no child forked inside [1, 5) -> reclaimable
        assert!(reclaimable(&children, Epoch(1), Epoch(5)));
        // child at 20 sits inside [15, 25) -> pinned
        assert!(!reclaimable(&children, Epoch(15), Epoch(25)));
        // birth exactly at a fork epoch: that child sees the page -> pinned
        assert!(!reclaimable(&children, Epoch(20), Epoch(21)));
        // free exactly at a fork epoch: page was already dead at that epoch -> reclaimable
        assert!(reclaimable(&children, Epoch(11), Epoch(20)));
        // no live children at all -> always reclaimable
        assert!(reclaimable(&[], Epoch(0), Epoch(u64::MAX)));
    }

    #[test]
    fn stale_generation_is_a_hard_error_not_stale_data() {
        let mut r = BranchRecord::trunk(1, LeaseDeadline(0));
        r.branch_id = BranchId::new(5, 0);
        r.generation = 0;
        assert!(r.check_readable(BranchId::new(5, 0)).is_ok());
        r.mark_reaped();
        let err = r.check_readable(BranchId::new(5, 0)).unwrap_err();
        assert!(matches!(err, BranchError::Reaped { current_generation: 1, .. }));
    }

    #[test]
    fn record_roundtrips_through_bytes() {
        let mut r = BranchRecord::trunk(77, LeaseDeadline(123_456));
        r.branch_id = BranchId::new(9, 3);
        r.generation = 3;
        r.parent_id = Some(BranchId::new(2, 1));
        r.fork_epoch = Epoch(4242);
        r.depth = 4;
        r.state = BranchState::Reaping;
        r.arenas = vec![ArenaId(1), ArenaId(9)];
        r.live_children = vec![Epoch(1), Epoch(2), Epoch(3)];
        let bytes = r.serialize();
        assert_eq!(BranchRecord::deserialize(&bytes).unwrap(), r);
    }

    #[test]
    fn corrupt_record_is_rejected_not_guessed() {
        let r = BranchRecord::trunk(77, LeaseDeadline(1));
        let mut bytes = r.serialize();
        bytes[0] ^= 0xff;
        assert!(matches!(BranchRecord::deserialize(&bytes), Err(BranchError::Corrupt(_))));
    }
}
