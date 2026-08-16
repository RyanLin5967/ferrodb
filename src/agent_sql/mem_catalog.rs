//! An in-memory [`BranchCatalog`] so the SQL surface has something to fork against.
//!
//! Design authority: DESIGN.md section 1.
//!
//! **Scope, stated plainly.** This is branch *metadata* only: one `BranchRecord` per branch, the
//! parent's sorted `live_children` array kept correct, generations bumped on reap. It holds no
//! pages and therefore proves nothing about exit criterion 1 (fork copies zero data pages) or
//! exit criterion 8 (page count returns to baseline) — those belong to the durable branch engine
//! and its `PageStore`. Everything here is behind the shared `BranchCatalog` trait precisely so
//! the durable implementation drops in without the SQL surface changing.
//!
//! What it *does* guarantee, and what the surface depends on:
//! - fork writes one record and appends exactly one epoch to the parent (never a page copy);
//! - a reaped id is a hard error at the next `get`, never stale data.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::branch::record::BranchRecord;
use crate::branch::types::{BranchError, BranchId, BranchState, Epoch, LeaseDeadline, PageId};
use crate::branch::BranchCatalog;
use crate::error::FerroError;
use crate::tel::frame::TxnFrame;
use crate::tel::EffectLog;

pub struct MemBranchCatalog {
    epoch: AtomicU64,
    next_id: AtomicU64,
    records: Mutex<BTreeMap<u64, BranchRecord>>,
}

impl Default for MemBranchCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl MemBranchCatalog {
    pub fn new() -> Self {
        let mut records = BTreeMap::new();
        // The trunk is never reaped and has no parent. Root page 0 is a placeholder: this
        // catalog stores no pages.
        records.insert(0u64, BranchRecord::trunk(0, LeaseDeadline(u64::MAX)));
        MemBranchCatalog {
            epoch: AtomicU64::new(1),
            next_id: AtomicU64::new(1),
            records: Mutex::new(records),
        }
    }

    fn lookup(
        records: &BTreeMap<u64, BranchRecord>,
        branch: BranchId,
    ) -> Result<BranchRecord, FerroError> {
        let rec = records
            .get(&branch.id)
            .ok_or(BranchError::NotFound(branch))?;
        rec.check_readable(branch)?;
        Ok(rec.clone())
    }

    /// Mark a branch reaped: bumps the generation so the old handle is a hard error, and removes
    /// its fork epoch from the parent's live-children array so the parent's pages become
    /// reclaimable again.
    pub fn reap(&self, branch: BranchId) -> Result<(), FerroError> {
        let mut records = self.records.lock().unwrap();
        let rec = Self::lookup(&records, branch)?;
        if let Some(parent) = rec.parent_id {
            if let Some(p) = records.get_mut(&parent.id) {
                p.remove_live_child(rec.fork_epoch);
            }
        }
        if let Some(r) = records.get_mut(&branch.id) {
            r.mark_reaped();
        }
        Ok(())
    }

    /// The record even if reaped — for reporting, never for reads.
    pub fn peek(&self, id: u64) -> Option<BranchRecord> {
        self.records.lock().unwrap().get(&id).cloned()
    }
}

impl BranchCatalog for MemBranchCatalog {
    fn next_epoch(&self) -> Epoch {
        Epoch(self.epoch.fetch_add(1, Ordering::SeqCst))
    }

    fn current_epoch(&self) -> Epoch {
        Epoch(self.epoch.load(Ordering::SeqCst))
    }

    fn fork(&self, parent: BranchId, lease: LeaseDeadline) -> Result<BranchRecord, FerroError> {
        let fork_epoch = self.next_epoch();
        let mut records = self.records.lock().unwrap();
        let parent_rec = Self::lookup(&records, parent)?;
        let child_id = BranchId::new(self.next_id.fetch_add(1, Ordering::SeqCst), 0);
        let child = BranchRecord::fork_child(&parent_rec, child_id, fork_epoch, lease)?;
        // Atomic with respect to the parent update: a child not listed in its parent is a GC
        // correctness hole, so both happen under the same lock.
        records
            .get_mut(&parent.id)
            .ok_or(BranchError::NotFound(parent))?
            .add_live_child(fork_epoch);
        records.insert(child_id.id, child.clone());
        Ok(child)
    }

    fn get(&self, branch: BranchId) -> Result<BranchRecord, FerroError> {
        let records = self.records.lock().unwrap();
        Self::lookup(&records, branch)
    }

    fn put(&self, record: &BranchRecord) -> Result<(), FerroError> {
        self.records
            .lock()
            .unwrap()
            .insert(record.branch_id.id, record.clone());
        Ok(())
    }

    fn set_root(&self, branch: BranchId, root: PageId) -> Result<(), FerroError> {
        let mut records = self.records.lock().unwrap();
        let mut rec = Self::lookup(&records, branch)?;
        rec.root_page_id = root;
        records.insert(branch.id, rec);
        Ok(())
    }

    fn live_branches(&self) -> Result<Vec<BranchRecord>, FerroError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.state == BranchState::Live)
            .cloned()
            .collect())
    }

    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError> {
        let mut records = self.records.lock().unwrap();
        let mut rec = Self::lookup(&records, branch)?;
        rec.lease_deadline = lease;
        records.insert(branch.id, rec);
        Ok(())
    }
}

/// An in-memory [`EffectLog`]: frames appended by the SQL layer, read back by merge.
///
/// Durability is the WAL's job and is not modelled here. What *is* modelled, because merge
/// depends on it, is that frames come back in sequence order per branch.
#[derive(Default)]
pub struct MemEffectLog {
    frames: Mutex<Vec<TxnFrame>>,
}

impl MemEffectLog {
    pub fn new() -> Self {
        MemEffectLog::default()
    }

    pub fn len(&self) -> usize {
        self.frames.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl EffectLog for MemEffectLog {
    fn append(&self, frame: &TxnFrame) -> Result<(), FerroError> {
        let mut frames = self.frames.lock().unwrap();
        // Replacing a frame with the same (branch, txn) is an update of the open frame, not a
        // second copy: Add is not idempotent and two copies would double-count.
        match frames
            .iter()
            .position(|f| f.branch == frame.branch && f.txn_id == frame.txn_id)
        {
            Some(i) => frames[i] = frame.clone(),
            None => frames.push(frame.clone()),
        }
        Ok(())
    }

    fn frames_for(&self, branch: BranchId, from_seq: u64) -> Result<Vec<TxnFrame>, FerroError> {
        let frames = self.frames.lock().unwrap();
        let mut out: Vec<TxnFrame> = frames
            .iter()
            .filter(|f| f.branch == branch && f.seq >= from_seq)
            .cloned()
            .collect();
        out.sort_by_key(|f| f.seq);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tel::ids::TxnId;

    #[test]
    fn a_reappended_frame_replaces_rather_than_duplicates() {
        let log = MemEffectLog::new();
        let branch = BranchId::new(3, 0);
        let mut f = TxnFrame::new(TxnId(1), branch, crate::branch::types::CommitHash::ZERO, 0, 1);
        log.append(&f).unwrap();
        f.seq = 0;
        log.append(&f).unwrap();
        assert_eq!(log.frames_for(branch, 0).unwrap().len(), 1);
    }

    #[test]
    fn fork_records_the_child_in_the_parent_and_shares_the_root() {
        let c = MemBranchCatalog::new();
        let child = c.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
        let trunk = c.get(BranchId::TRUNK).unwrap();
        assert_eq!(trunk.live_children, vec![child.fork_epoch]);
        assert_eq!(child.root_page_id, trunk.root_page_id);
        assert_eq!(child.depth, 1);
    }

    #[test]
    fn reading_a_reaped_branch_is_a_hard_error_not_stale_data() {
        let c = MemBranchCatalog::new();
        let child = c.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
        c.reap(child.branch_id).unwrap();
        let err = c.get(child.branch_id).unwrap_err();
        assert!(matches!(err, FerroError::Branch(_)), "got {:?}", err);
        // and the parent no longer pins pages on the reaped child's behalf
        assert!(c.get(BranchId::TRUNK).unwrap().live_children.is_empty());
    }
}
