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

    /// Charged under the one lock this catalog has, so it is atomic against every other mutation
    /// here — the same reason `fork` updates the parent and inserts the child under it.
    fn charge_row_writes(&self, branch: BranchId, n: u64) -> Result<(), FerroError> {
        let mut records = self.records.lock().unwrap();
        let mut rec = Self::lookup(&records, branch)?;
        match rec.envelope.as_mut() {
            Some(e) => e.charge(n)?,
            None => {
                return Err(FerroError::Constraint(format!(
                    "cannot charge {n} row-write(s) to {branch}: it has no capability envelope"
                )))
            }
        }
        records.insert(branch.id, rec);
        Ok(())
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

    fn expired_before(&self, now_millis: u64) -> Result<Vec<BranchRecord>, FerroError> {
        let mut out: Vec<BranchRecord> = self
            .records
            .lock()
            .unwrap()
            .values()
            .filter(|r| {
                r.state == BranchState::Live
                    && !r.branch_id.is_trunk()
                    && r.lease_deadline.is_expired_at(now_millis)
            })
            .cloned()
            .collect();
        out.sort_unstable_by_key(|r| r.branch_id.id);
        Ok(out)
    }

    fn in_state(&self, state: BranchState) -> Result<Vec<BranchRecord>, FerroError> {
        let mut out: Vec<BranchRecord> =
            self.records.lock().unwrap().values().filter(|r| r.state == state).cloned().collect();
        out.sort_unstable_by_key(|r| r.branch_id.id);
        Ok(out)
    }

    /// Ordered, like the durable catalog's, so that a caller cannot come to depend on hash order
    /// in tests and then meet a different order in production. The two implementations agreeing
    /// about order is the only reason a test against this one says anything about that one.
    fn scan(&self)
        -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        let mut out: Vec<BranchRecord> = self.records.lock().unwrap().values().cloned().collect();
        out.sort_unstable_by_key(|r| r.branch_id.id);
        Ok(Box::new(out.into_iter().map(Ok)))
    }

    fn max_live_child(&self, parent_id: u64) -> Result<Option<Epoch>, FerroError> {
        Ok(self.records.lock().unwrap().get(&parent_id).and_then(|r| r.live_children.last().copied()))
    }

    fn live_child_in_epoch_range(
        &self,
        parent_id: u64,
        lo: Epoch,
        hi: Epoch,
    ) -> Result<bool, FerroError> {
        let records = self.records.lock().unwrap();
        let Some(rec) = records.get(&parent_id) else { return Ok(false) };
        Ok(!crate::branch::record::reclaimable(&rec.live_children, lo, hi))
    }

    fn has_live_children(&self, parent_id: u64) -> Result<bool, FerroError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .get(&parent_id)
            .map(|r| !r.live_children.is_empty())
            .unwrap_or(false))
    }

    fn live_count(&self) -> usize {
        self.records.lock().unwrap().values().filter(|r| r.state == BranchState::Live).count()
    }

    fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> {
        self.records
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or_else(|| BranchError::NotFound(BranchId::new(id, 0)).into())
    }

    /// The in-memory catalog does not recycle ids, so this is a no-op rather than a lie: `fork`
    /// here always mints a fresh id, and a free list nothing reads would be dead state.
    fn release_id(&self, _id: u64) {}

    fn detach_child(&self, parent_id: u64, fork_epoch: Epoch) -> Result<bool, FerroError> {
        let mut records = self.records.lock().unwrap();
        let Some(prec) = records.get_mut(&parent_id) else { return Ok(false) };
        Ok(prec.remove_live_child(fork_epoch))
    }

    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError> {
        let mut records = self.records.lock().unwrap();
        let mut rec = Self::lookup(&records, branch)?;
        rec.lease_deadline = lease;
        records.insert(branch.id, rec);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
