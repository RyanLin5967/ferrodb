//! The durable branch catalog: an append-only record log with an in-memory index.
//!
//! Design authority: DESIGN.md section 1.
//!
//! **Fork is one durable record write plus one epoch appended to the parent's sorted
//! live-children array.** No data page is read, written, or refcounted — that is the O(1) claim
//! (exit criterion 1), and it is why this file only ever touches branch *metadata*.
//!
//! Durability is an append-only log of serialized [`BranchRecord`]s, last-write-wins per id on
//! replay. That shape was chosen because the alternative — updating a record in place — would
//! make the parent's `live_children` append and the child's creation two separately-failable
//! writes, and a child that exists but is not listed in its parent is a GC correctness hole.
//! Appending both records to one log and fsyncing once makes them atomic together.
//!
//! ## Id recycling and the generation counter
//!
//! A reaped id slot may be handed out again, but the slot's `generation` only ever increases, so
//! a stale handle presenting the old generation gets [`BranchError::Reaped`] rather than somebody
//! else's data. A slot is only eligible for recycling once its reaped record has an empty
//! `live_children` array — until then the record is still the authority that decides whether that
//! branch's parked pages may be released, so it must not be overwritten.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use crate::branch::record::BranchRecord;
use crate::branch::types::{BranchError, BranchId, BranchState, Epoch, LeaseDeadline, PageId};
use crate::branch::BranchCatalog;
use crate::error::FerroError;

/// The trunk's lease. Trunk is not an exemption *class* — the lease rule is applied to it exactly
/// like every other branch — it simply holds a lease that never expires, because reaping the
/// trunk would delete the database rather than reclaim an abandoned agent task.
pub const TRUNK_LEASE: LeaseDeadline = LeaseDeadline(u64::MAX);

struct CatalogState {
    /// Current record per id slot. A reaped slot keeps its record: its `live_children` array is
    /// still the authority for any page parked in the pending-free log under its name.
    records: HashMap<u64, BranchRecord>,
    /// Id slots whose reaped record no longer pins anything and may be handed out again.
    free_ids: Vec<u64>,
}

/// Append-only, crash-replayable branch catalog.
pub struct LogBranchCatalog {
    state: RwLock<CatalogState>,
    epoch: AtomicU64,
    next_id: AtomicU64,
    /// `None` for a purely in-memory catalog (tests, and the merge/gate layers' scratch state).
    sink: Option<Mutex<File>>,
}

impl LogBranchCatalog {
    /// A catalog with no durable backing. The branch records still behave identically; only
    /// crash recovery is absent.
    pub fn in_memory(trunk_root: PageId) -> Self {
        let mut records = HashMap::new();
        records.insert(0u64, BranchRecord::trunk(trunk_root, TRUNK_LEASE));
        LogBranchCatalog {
            state: RwLock::new(CatalogState { records, free_ids: Vec::new() }),
            epoch: AtomicU64::new(0),
            next_id: AtomicU64::new(1),
            sink: None,
        }
    }

    /// Open (creating if absent) a durable catalog at `path`, replaying whatever is already there.
    pub fn open(path: &Path, trunk_root: PageId) -> Result<Self, FerroError> {
        let existing = if path.exists() { Self::replay(path)? } else { Vec::new() };

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .map_err(|e| FerroError::Io(e.to_string()))?;

        let mut records: HashMap<u64, BranchRecord> = HashMap::new();
        records.insert(0u64, BranchRecord::trunk(trunk_root, TRUNK_LEASE));
        // Last write wins per id slot; the log is append-only and strictly ordered.
        for r in existing {
            records.insert(r.branch_id.id, r);
        }

        let mut max_id = 0u64;
        let mut max_epoch = 0u64;
        let mut free_ids = Vec::new();
        for (id, r) in records.iter() {
            max_id = max_id.max(*id);
            max_epoch = max_epoch.max(r.fork_epoch.0);
            if let Some(last) = r.live_children.last() {
                max_epoch = max_epoch.max(last.0);
            }
            if r.state == BranchState::Reaped && r.live_children.is_empty() && *id != 0 {
                free_ids.push(*id);
            }
        }

        Ok(LogBranchCatalog {
            state: RwLock::new(CatalogState { records, free_ids }),
            epoch: AtomicU64::new(max_epoch),
            next_id: AtomicU64::new(max_id + 1),
            sink: Some(Mutex::new(file)),
        })
    }

    fn replay(path: &Path) -> Result<Vec<BranchRecord>, FerroError> {
        let mut f = File::open(path).map_err(|e| FerroError::Io(e.to_string()))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).map_err(|e| FerroError::Io(e.to_string()))?;
        let mut out = Vec::new();
        let mut at = 0usize;
        while at + 4 <= buf.len() {
            let len = u32::from_be_bytes(buf[at..at + 4].try_into().unwrap()) as usize;
            at += 4;
            if at + len > buf.len() {
                // Torn tail from a crash mid-append. Everything before it is intact and
                // checksummed, so stop here rather than guessing at the fragment.
                break;
            }
            match BranchRecord::deserialize(&buf[at..at + len]) {
                Ok(r) => out.push(r),
                Err(e) => return Err(e.into()),
            }
            at += len;
        }
        Ok(out)
    }

    fn append(&self, records: &[&BranchRecord]) -> Result<(), FerroError> {
        let Some(sink) = &self.sink else { return Ok(()) };
        let mut f = sink.lock().unwrap();
        let mut out = Vec::new();
        for r in records {
            let bytes = r.serialize();
            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(&bytes);
        }
        f.write_all(&out).map_err(|e| FerroError::Io(e.to_string()))?;
        f.sync_data().map_err(|e| FerroError::Io(e.to_string()))?;
        Ok(())
    }

    /// Fetch a record **ignoring the generation guard**. Only the reaper may use this: it has to
    /// read the record of a branch it is in the middle of reaping, and it has to consult the
    /// `live_children` array of an already-reaped parent to decide whether that parent's parked
    /// pages are now releasable.
    pub fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> {
        let st = self.state.read().unwrap();
        st.records
            .get(&id)
            .cloned()
            .ok_or_else(|| BranchError::NotFound(BranchId::new(id, 0)).into())
    }

    /// Every record, live or reaped.
    pub fn all_records(&self) -> Vec<BranchRecord> {
        self.state.read().unwrap().records.values().cloned().collect()
    }

    /// Mark an id slot reusable. Refuses while the slot's record still lists live children,
    /// because that array is what decides the fate of pages parked under this branch's name.
    pub fn release_id(&self, id: u64) {
        if id == 0 {
            return;
        }
        let mut st = self.state.write().unwrap();
        let reusable = st
            .records
            .get(&id)
            .map(|r| r.state == BranchState::Reaped && r.live_children.is_empty())
            .unwrap_or(false);
        if reusable && !st.free_ids.contains(&id) {
            st.free_ids.push(id);
        }
    }

    /// Number of branches in state `Live`, trunk included.
    pub fn live_count(&self) -> usize {
        self.state
            .read()
            .unwrap()
            .records
            .values()
            .filter(|r| r.state == BranchState::Live)
            .count()
    }
}

impl BranchCatalog for LogBranchCatalog {
    fn next_epoch(&self) -> Epoch {
        Epoch(self.epoch.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn current_epoch(&self) -> Epoch {
        Epoch(self.epoch.load(Ordering::SeqCst))
    }

    fn fork(&self, parent: BranchId, lease: LeaseDeadline) -> Result<BranchRecord, FerroError> {
        let fork_epoch = self.next_epoch();
        let mut st = self.state.write().unwrap();

        let parent_rec = st
            .records
            .get(&parent.id)
            .cloned()
            .ok_or(BranchError::NotFound(parent))?;
        parent_rec.check_readable(parent)?;

        // Recycle a retired slot if one is free, otherwise mint a new one. Either way the
        // generation comes from the slot's history, never from zero.
        let (child_num, generation) = match st.free_ids.pop() {
            Some(id) => {
                let slot_gen = st.records.get(&id).map(|r| r.generation).unwrap_or(0);
                (id, slot_gen)
            }
            None => (self.next_id.fetch_add(1, Ordering::SeqCst), 0),
        };
        let child_id = BranchId::new(child_num, generation);

        let child = BranchRecord::fork_child(&parent_rec, child_id, fork_epoch, lease)?;

        let mut new_parent = parent_rec;
        new_parent.add_live_child(fork_epoch);

        // One durable write covering both halves. A child that exists but is not listed in its
        // parent is a GC correctness hole, so the two records share a single fsync.
        self.append(&[&child, &new_parent])?;

        st.records.insert(parent.id, new_parent);
        st.records.insert(child_num, child.clone());
        Ok(child)
    }

    fn get(&self, branch: BranchId) -> Result<BranchRecord, FerroError> {
        let st = self.state.read().unwrap();
        let rec = st.records.get(&branch.id).ok_or(BranchError::NotFound(branch))?;
        rec.check_readable(branch)?;
        Ok(rec.clone())
    }

    fn put(&self, record: &BranchRecord) -> Result<(), FerroError> {
        self.append(&[record])?;
        self.state.write().unwrap().records.insert(record.branch_id.id, record.clone());
        Ok(())
    }

    fn set_root(&self, branch: BranchId, root: PageId) -> Result<(), FerroError> {
        let mut rec = self.get(branch)?;
        rec.root_page_id = root;
        self.put(&rec)
    }

    fn live_branches(&self) -> Result<Vec<BranchRecord>, FerroError> {
        let st = self.state.read().unwrap();
        Ok(st.records.values().filter(|r| r.state == BranchState::Live).cloned().collect())
    }

    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError> {
        let mut rec = self.get(branch)?;
        rec.lease_deadline = lease;
        self.put(&rec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::types::MAX_BRANCH_DEPTH;

    fn cat() -> LogBranchCatalog {
        LogBranchCatalog::in_memory(1)
    }

    #[test]
    fn fork_records_the_child_in_the_parent_atomically() {
        let c = cat();
        let child = c.fork(BranchId::TRUNK, LeaseDeadline(5_000)).unwrap();
        let trunk = c.get(BranchId::TRUNK).unwrap();
        assert_eq!(trunk.live_children, vec![child.fork_epoch]);
        assert_eq!(child.root_page_id, trunk.root_page_id, "fork copies the root pointer only");
        assert!(child.arenas.is_empty(), "fork allocates no arena and therefore no page");
    }

    #[test]
    fn epochs_are_strictly_monotonic_across_forks() {
        let c = cat();
        let a = c.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let b = c.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        assert!(b.fork_epoch > a.fork_epoch);
        assert!(c.current_epoch() >= b.fork_epoch);
    }

    #[test]
    fn reading_a_reaped_branch_is_a_hard_error() {
        let c = cat();
        let child = c.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let mut rec = c.get(child.branch_id).unwrap();
        rec.mark_reaped();
        c.put(&rec).unwrap();
        let err = c.get(child.branch_id).unwrap_err();
        assert!(matches!(err, FerroError::Branch(_)), "got {:?}", err);
        assert!(err.to_string().contains("reaped"));
    }

    #[test]
    fn a_recycled_id_slot_never_answers_to_the_old_handle() {
        let c = cat();
        let old = c.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let mut rec = c.get(old.branch_id).unwrap();
        // detach from the parent so the slot becomes eligible for reuse
        let mut trunk = c.get(BranchId::TRUNK).unwrap();
        trunk.remove_live_child(rec.fork_epoch);
        c.put(&trunk).unwrap();
        rec.mark_reaped();
        c.put(&rec).unwrap();
        c.release_id(old.branch_id.id);

        let fresh = c.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        assert_eq!(fresh.branch_id.id, old.branch_id.id, "the id slot was reused");
        assert_ne!(fresh.branch_id.generation, old.branch_id.generation);
        assert!(c.get(fresh.branch_id).is_ok());
        assert!(c.get(old.branch_id).is_err(), "the stale handle must not reach the new branch");
    }

    #[test]
    fn a_slot_still_pinning_children_is_not_recycled() {
        let c = cat();
        let parent = c.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let _grandchild = c.fork(parent.branch_id, LeaseDeadline(1)).unwrap();
        let mut rec = c.get(parent.branch_id).unwrap();
        rec.mark_reaped();
        c.put(&rec).unwrap();
        c.release_id(parent.branch_id.id);
        let fresh = c.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        assert_ne!(
            fresh.branch_id.id, parent.branch_id.id,
            "a slot whose live_children array still decides parked pages must not be overwritten"
        );
    }

    #[test]
    fn depth_guard_refuses_the_ninth_fork() {
        let c = cat();
        let mut cur = BranchId::TRUNK;
        for _ in 0..MAX_BRANCH_DEPTH {
            cur = c.fork(cur, LeaseDeadline(1)).unwrap().branch_id;
        }
        assert_eq!(c.get(cur).unwrap().depth, MAX_BRANCH_DEPTH);
        let err = c.fork(cur, LeaseDeadline(1)).unwrap_err();
        assert!(err.to_string().contains("depth"), "got {}", err);
    }

    #[test]
    fn durable_catalog_survives_a_reopen() {
        let dir = std::env::temp_dir().join(format!("ferro-cat-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("branches.log");
        let _ = std::fs::remove_file(&path);

        let ids: Vec<BranchId>;
        {
            let c = LogBranchCatalog::open(&path, 7).unwrap();
            ids = (0..4)
                .map(|_| c.fork(BranchId::TRUNK, LeaseDeadline(1234)).unwrap().branch_id)
                .collect();
            c.set_root(ids[0], 99).unwrap();
        }
        let c2 = LogBranchCatalog::open(&path, 7).unwrap();
        assert_eq!(c2.live_count(), 5, "trunk plus four children");
        assert_eq!(c2.get(ids[0]).unwrap().root_page_id, 99);
        assert_eq!(c2.get(BranchId::TRUNK).unwrap().live_children.len(), 4);
        // a fresh fork after recovery must not collide with a recovered id
        let n = c2.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        assert!(!ids.contains(&n.branch_id));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn a_torn_tail_keeps_every_intact_record() {
        let dir = std::env::temp_dir().join(format!("ferro-cat-torn-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("torn.log");
        let _ = std::fs::remove_file(&path);
        {
            let c = LogBranchCatalog::open(&path, 3).unwrap();
            c.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        }
        // simulate a crash part-way through an append
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&999u32.to_be_bytes());
        bytes.extend_from_slice(&[0u8; 5]);
        std::fs::write(&path, &bytes).unwrap();

        let c2 = LogBranchCatalog::open(&path, 3).unwrap();
        assert_eq!(c2.live_count(), 2);
        std::fs::remove_file(&path).unwrap();
    }
}
