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

use crate::branch::record::{BranchRecord, CapabilityEnvelope};
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

        let (records, free_ids, max_id, max_epoch) = Self::index(existing, trunk_root);

        Ok(LogBranchCatalog {
            state: RwLock::new(CatalogState { records, free_ids }),
            epoch: AtomicU64::new(max_epoch),
            next_id: AtomicU64::new(max_id + 1),
            sink: Some(Mutex::new(file)),
        })
    }

    /// Re-read `path` and replace this catalog's whole index with what it holds.
    ///
    /// For F6: a snapshot install writes a *different* catalog's records over this node's file, and
    /// nothing else in this type can be told about it. [`LogBranchCatalog::put`] cannot be used for
    /// that — it appends one record, never removes one the sender no longer has, and does not
    /// advance `epoch`, `next_id` or `free_ids`, which are derived in [`LogBranchCatalog::open`]
    /// and nowhere else. A node re-seeded through `put` would keep branches the leader had reaped
    /// and go on minting ids that collide with the ones it was just given.
    ///
    /// Shares `replay` and the derivation with `open` rather than restating either: two places
    /// computing `free_ids` is two places for the recycling rule to drift.
    pub fn reload_from(&self, path: &Path, trunk_root: PageId) -> Result<(), FerroError> {
        let existing = if path.exists() { Self::replay(path)? } else { Vec::new() };
        let (records, free_ids, max_id, max_epoch) = Self::index(existing, trunk_root);

        // The sink is re-pointed at the file the records came from, so a later `put` appends to the
        // catalog this node now holds rather than to the one it was replaced from.
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .map_err(|e| FerroError::Io(e.to_string()))?;

        let mut st = self.state.write().unwrap();
        st.records = records;
        st.free_ids = free_ids;
        drop(st);
        self.epoch.store(max_epoch, Ordering::SeqCst);
        self.next_id.store(max_id + 1, Ordering::SeqCst);
        if let Some(sink) = &self.sink {
            *sink.lock().unwrap() = file;
        }
        Ok(())
    }

    /// The index a set of replayed records implies: the record map, the recyclable id slots, and
    /// the two counters derived from them. One copy, shared by `open` and `reload_from`.
    fn index(
        existing: Vec<BranchRecord>,
        trunk_root: PageId,
    ) -> (HashMap<u64, BranchRecord>, Vec<u64>, u64, u64) {
        let mut records: HashMap<u64, BranchRecord> = HashMap::new();
        records.insert(0u64, BranchRecord::trunk(trunk_root, TRUNK_LEASE));
        // Last write wins per id slot; the log is append-only and strictly ordered.
        for r in existing {
            records.insert(r.branch_id.id, r);
        }

        // **`live_children` is DERIVED here, not read from the parent's serialised record.**
        //
        // It used to be stored, which meant `fork` had to rewrite the whole parent record on every
        // fork so the new epoch reached disk — and `live_children` grows with the number of
        // children, so bytes-per-fork grew with the children the parent already had. Measured
        // before this change (`bench/durable_catalog_scaling.txt`): the catalog log QUADRUPLED on
        // every doubling of branch count — 1.07MB at 500 branches, 257MB at 8,000 — and reopen time
        // with it. That is O(N^2) in a structure whose whole purpose is to make forking cheap.
        //
        // The array was always redundant: a child record already carries `parent_id` and
        // `fork_epoch`, so the parent's live children are exactly the live records that name it.
        // Deriving costs one pass over records that were being read anyway.
        //
        // **It also closes a correctness hole rather than trading against one.** `fork` wrote child
        // and parent under a single fsync precisely because a child durable without its parent's
        // entry would be a page nobody knows to park. Derived, the child record *is* the parent's
        // entry — the two cannot disagree because there is only one of them.
        //
        // A reaped child is not live, and `detach_from_parent` runs BEFORE `mark_reaped`, so a
        // crash between the two leaves a parent whose stored array had already lost the epoch while
        // the child still reads Live. Derivation re-adds it, which PARKS the pages until the
        // resumed reap finishes — the safe direction. The unsafe direction, dropping a child that
        // is still live, cannot happen: a live child's own record says so.
        let mut derived: HashMap<u64, Vec<Epoch>> = HashMap::new();
        for r in records.values() {
            if r.state == BranchState::Reaped {
                continue;
            }
            if let Some(p) = r.parent_id {
                derived.entry(p.id).or_default().push(r.fork_epoch);
            }
        }
        for (id, mut kids) in derived {
            // Sorted ascending: `reclaimable` runs a range-emptiness query over this array and
            // `add_live_child` maintains the same order, so a derived array that arrived in hash
            // order would answer differently from a stored one.
            kids.sort_unstable();
            if let Some(rec) = records.get_mut(&id) {
                rec.live_children = kids;
            }
        }
        // A parent that now has no live children must end up with an EMPTY array, not the stale one
        // its last serialised copy held — `release_id` refuses to recycle a slot while the array is
        // non-empty, so a leftover entry would strand the id for ever.
        let has_kids: std::collections::HashSet<u64> = records
            .values()
            .filter(|r| r.state != BranchState::Reaped)
            .filter_map(|r| r.parent_id.map(|p| p.id))
            .collect();
        for (id, rec) in records.iter_mut() {
            if !has_kids.contains(id) {
                rec.live_children.clear();
            }
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
        // `records` is a `HashMap`, so the loop above collected these in hash order — and `fork`
        // **pops** this vector to choose which id a recycled slot hands out, which is then
        // serialised into a durable branch record. Two opens of one log gave the same fork two
        // different ids. Sorted, `pop` takes the highest reaped slot and does it reproducibly.
        free_ids.sort_unstable();
        (records, free_ids, max_id, max_epoch)
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
        // Inserted in order rather than pushed. `open` rebuilds this vector sorted and `fork`
        // pops it, so a push would make the id a fork recycles depend on whether a restart
        // intervened: reaping a chain deepest-first releases 2 then 1, and `pop` on the pushed
        // order hands out 1 while `pop` after a reopen of the very same log hands out 2. The binary
        // search subsumes the membership check it replaces — `Err(pos)` is exactly "not present,
        // and this is where it belongs".
        if reusable {
            if let Err(pos) = st.free_ids.binary_search(&id) {
                st.free_ids.insert(pos, id);
            }
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
        // **Only the child is appended.** The parent's `live_children` is derived at replay from
        // the children that name it (see `index`), so rewriting the parent here would write bytes
        // that replay ignores — and those bytes are what made the log O(N^2). The in-memory parent
        // is still updated below, because live readers use it without replaying.
        self.append(&[&child])?;

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

    /// Still a walk, and deliberately so. This is the *log* catalog: its records live in a
    /// `HashMap` with no index over deadlines, so O(N) is the best it can do and pretending
    /// otherwise would be a lie in a signature. D2 narrowed the interface first precisely so this
    /// body can later be replaced by a range descent without a single caller changing.
    ///
    /// Sorted because `reap_expired`'s own `sort_by_key` is stable, so whatever order arrives here
    /// survives as its tie-break and reaches every record and page that reap makes durable.
    fn expired_before(&self, now_millis: u64) -> Result<Vec<BranchRecord>, FerroError> {
        let st = self.state.read().unwrap();
        let mut out: Vec<BranchRecord> = st
            .records
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
        let st = self.state.read().unwrap();
        let mut out: Vec<BranchRecord> =
            st.records.values().filter(|r| r.state == state).cloned().collect();
        out.sort_unstable_by_key(|r| r.branch_id.id);
        Ok(out)
    }

    /// Ordered by branch id so that a sweep over every record is reproducible at all: each reap
    /// this feeds appends durable branch records and frees pages, and a `HashMap`'s order made
    /// that sequence depend on nothing a test or a replay can pin.
    ///
    /// Id order is **not** the order a reap wants, and `reaper::resume_interrupted_reaps` re-sorts
    /// deepest-first before acting — a parent resumed before its own child cannot release its id
    /// slot. Ordering here is what makes the accessor deterministic; the reap key belongs to the
    /// reaper, which states why at its sort.
    ///
    /// It buffers, which the trait's contract permits but does not want. The records are already
    /// all resident in the `HashMap` this reads, so buffering costs a second copy and no more —
    /// and removing the first copy is D1's job, not this method's.
    fn scan(&self)
        -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        let st = self.state.read().unwrap();
        let mut out: Vec<BranchRecord> = st.records.values().cloned().collect();
        out.sort_unstable_by_key(|r| r.branch_id.id);
        Ok(Box::new(out.into_iter().map(Ok)))
    }

    /// Answered from the in-memory `live_children` array this implementation already derives at
    /// replay. That array is what the table catalog replaces with a key span; the queries are
    /// declared on the trait so callers stop depending on the representation.
    fn max_live_child(&self, parent_id: u64) -> Result<Option<Epoch>, FerroError> {
        let st = self.state.read().unwrap();
        Ok(st.records.get(&parent_id).and_then(|r| r.live_children.last().copied()))
    }

    fn live_child_in_epoch_range(
        &self,
        parent_id: u64,
        lo: Epoch,
        hi: Epoch,
    ) -> Result<bool, FerroError> {
        let st = self.state.read().unwrap();
        let Some(rec) = st.records.get(&parent_id) else { return Ok(false) };
        // `!reclaimable` is exactly "a live child forked in [lo, hi)". Expressed through the same
        // function so the two can never drift apart.
        Ok(!crate::branch::record::reclaimable(&rec.live_children, lo, hi))
    }

    fn has_live_children(&self, parent_id: u64) -> Result<bool, FerroError> {
        let st = self.state.read().unwrap();
        Ok(st.records.get(&parent_id).map(|r| !r.live_children.is_empty()).unwrap_or(false))
    }

    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError> {
        let mut rec = self.get(branch)?;
        rec.lease_deadline = lease;
        self.put(&rec)
    }

    fn envelope_of(&self, branch: BranchId) -> Result<Option<CapabilityEnvelope>, FerroError> {
        let st = self.state.read().unwrap();
        let rec = st.records.get(&branch.id).ok_or(BranchError::NotFound(branch))?;
        // Same readability rule as `get`: a branch mid-reap or at a stale generation does not get
        // to answer questions about its authority either.
        rec.check_readable(branch)?;
        Ok(rec.envelope.clone())
    }

    /// Read, charge and durably record **under the write lock**, so nothing can interleave.
    ///
    /// The read-modify-write that this replaces held no lock across its two halves, so it wrote
    /// back a record snapshot that could be several mutations stale — losing a published root or a
    /// child's fork epoch, not just a row-write. Holding the write lock across the whole thing is
    /// the same discipline `fork` uses for the same reason.
    ///
    /// `append` under the write lock matches `fork`, which already takes the state lock and then
    /// the sink lock; the ordering is established, so this adds no new deadlock edge. It cannot
    /// call `self.put` — that takes the same lock, and it is not reentrant.
    fn charge_row_writes(&self, branch: BranchId, n: u64) -> Result<(), FerroError> {
        let mut st = self.state.write().unwrap();
        let rec = st.records.get(&branch.id).ok_or(BranchError::NotFound(branch))?;
        rec.check_readable(branch)?;
        let mut rec = rec.clone();
        match rec.envelope.as_mut() {
            Some(e) => e.charge(n)?,
            None => {
                return Err(FerroError::Constraint(format!(
                    "cannot charge {n} row-write(s) to {branch}: it has no capability envelope"
                )))
            }
        }
        self.append(&[&rec])?;
        st.records.insert(branch.id, rec);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::types::MAX_BRANCH_DEPTH;

    fn cat() -> LogBranchCatalog {
        LogBranchCatalog::in_memory(1)
    }

        /// **A parent whose stored array still lists a child that is Reaped comes back EMPTY.**
    ///
    /// The derivation only visits parents that HAVE live children, so a parent that has none is
    /// never written by that loop and would keep whatever its last serialised copy held. That is
    /// reachable: `detach_from_parent` runs before `mark_reaped`, so a crash between them — or a
    /// `put` that never landed — leaves exactly this shape on disk.
    ///
    /// It matters because `release_id` refuses to recycle a slot while `live_children` is
    /// non-empty. A stale entry for a child that no longer exists strands that id permanently, and
    /// `reclaimable` keeps parking pages against a fork epoch nothing can ever reach.
    ///
    /// Written after a mutant that deleted the `clear()` survived the test above.
    #[test]
    fn a_parent_listing_only_reaped_children_comes_back_with_an_empty_array() {
        let dir = std::env::temp_dir().join(format!("ferrodb-stale-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("branches.log");

        {
            let c = LogBranchCatalog::open(&path, 1).unwrap();
            let child = c.fork(BranchId::TRUNK, LeaseDeadline(5_000)).unwrap();
            // The parent's stored copy still lists the child — this is what a crash between
            // `detach_from_parent` and `mark_reaped` leaves behind.
            let mut trunk = c.get_raw(0).unwrap();
            assert_eq!(trunk.live_children, vec![child.fork_epoch]);
            c.put(&trunk).unwrap();
            let mut crec = c.get_raw(child.branch_id.id).unwrap();
            crec.mark_reaped();
            c.put(&crec).unwrap();
            trunk = c.get_raw(0).unwrap();
            assert_eq!(
                trunk.live_children,
                vec![child.fork_epoch],
                "precondition: the stored parent must still list the now-reaped child"
            );
        }

        let reopened = LogBranchCatalog::open(&path, 1).unwrap();
        assert!(
            reopened.get(BranchId::TRUNK).unwrap().live_children.is_empty(),
            "trunk came back listing a child that is Reaped; release_id will refuse that slot for \
             ever and reclaimable will park pages against an epoch nothing can reach"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **`live_children` survives a reopen even though `fork` never writes the parent.**
    ///
    /// S1: the array used to be stored inside the parent record, so every fork rewrote the whole
    /// parent and the log grew O(N^2) — 257MB at 8,000 branches, measured in
    /// `bench/durable_catalog_scaling.txt`. It is now derived at replay from the children that name
    /// the parent, and `fork` appends the child alone.
    ///
    /// This pins the derivation against a real file, because the in-memory path cannot fail the way
    /// replay can: the live catalog keeps the array `add_live_child` built, so a broken derivation
    /// is invisible until something reopens. Reaped children must be excluded, and a parent that
    /// loses its last live child must end up with an EMPTY array — `release_id` refuses to recycle
    /// a slot while the array is non-empty, so a stale entry strands the id for ever.
    #[test]
    fn live_children_is_derived_at_replay_and_excludes_reaped_children() {
        let dir = std::env::temp_dir().join(format!("ferrodb-derive-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("branches.log");

        let (kept, gone) = {
            let c = LogBranchCatalog::open(&path, 1).unwrap();
            // ENOUGH children that hash order is reliably not ascending. With two, a derived
            // array left in `HashMap` order passes by luck — measured: dropping the sort survived
            // this test until the count went up.
            let kids: Vec<_> = (0..24)
                .map(|_| c.fork(BranchId::TRUNK, LeaseDeadline(5_000)).unwrap())
                .collect();
            let (a, b, d) = (kids[0].clone(), kids[11].clone(), kids[23].clone());
            assert_eq!(
                c.get(BranchId::TRUNK).unwrap().live_children.len(),
                24,
                "in-memory array is built by add_live_child and is not what this test is about"
            );
            // Reap the middle one the way the reaper does: detach, then mark reaped.
            let mut brec = c.get_raw(b.branch_id.id).unwrap();
            let mut trunk = c.get_raw(0).unwrap();
            trunk.remove_live_child(brec.fork_epoch);
            c.put(&trunk).unwrap();
            brec.mark_reaped();
            c.put(&brec).unwrap();
            let mut expect: Vec<_> =
                kids.iter().filter(|k| k.fork_epoch != b.fork_epoch).map(|k| k.fork_epoch).collect();
            expect.sort_unstable();
            let _ = (&a, &d);
            (expect, b.fork_epoch)
        };

        let reopened = LogBranchCatalog::open(&path, 1).unwrap();
        let trunk = reopened.get(BranchId::TRUNK).unwrap();
        assert_eq!(
            trunk.live_children, kept,
            "after a reopen trunk should list exactly its LIVE children, ascending — a fork whose \
             parent was never rewritten still has to reach the array, and a reaped child must not"
        );
        assert!(
            !trunk.live_children.contains(&gone),
            "a reaped child is still listed as live, so pages it could see will never be freed"
        );
        // ASCENDING, asserted separately from the contents. `reclaimable` answers the liveness
        // question with `partition_point` over this array, so an array holding the right epochs in
        // the wrong order returns a wrong answer while every membership check still passes — which
        // is exactly how a mutant that deleted the sort survived the first version of this test.
        assert!(
            trunk.live_children.windows(2).all(|w| w[0] <= w[1]),
            "derived live_children is not ascending, so the range-emptiness query that decides \
             whether a page is reclaimable will answer on an unsorted array: {:?}",
            trunk.live_children
        );

        // A childless branch must come back with an EMPTY array, not the stale one its last
        // serialised copy held.
        let leaf = reopened
            .get_raw(reopened.get(BranchId::TRUNK).unwrap().live_children.len() as u64)
            .map(|r| r.live_children.clone())
            .unwrap_or_default();
        assert!(leaf.is_empty(), "a branch with no children came back with a non-empty array: {leaf:?}");

        let _ = std::fs::remove_dir_all(&dir);
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

    /// **Which id a fork recycles is a durable choice, and it was made by a `HashMap`.**
    ///
    /// B10's finding 4, the catalog half. `open` rebuilds `free_ids` by walking the record map, and
    /// `fork` pops that vector and serialises the id it got into an appended record. Replayed twice
    /// — two processes opening one log, or one process opening it after a crash — the same log
    /// handed the same fork different ids.
    ///
    /// Both copies below start from byte-identical logs, so any disagreement is iteration order.
    #[test]
    fn two_opens_of_one_log_recycle_branch_ids_in_the_same_order() {
        let dir = std::env::temp_dir().join(format!("ferro-cat-recycle-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let base = dir.join("base.log");
        let _ = std::fs::remove_file(&base);

        // Twelve children of trunk, every one of them reaped and childless: twelve reusable slots.
        {
            let c = LogBranchCatalog::open(&base, 1).unwrap();
            let kids: Vec<BranchId> = (0..12)
                .map(|_| c.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id)
                .collect();
            for k in kids {
                let mut rec = c.get_raw(k.id).unwrap();
                rec.mark_reaped();
                c.put(&rec).unwrap();
            }
        }

        let recycled_from = |name: &str| -> Vec<u64> {
            let copy = dir.join(name);
            std::fs::copy(&base, &copy).unwrap();
            let c = LogBranchCatalog::open(&copy, 1).unwrap();
            (0..6)
                .map(|_| c.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id.id)
                .collect()
        };
        let left = recycled_from("left.log");
        let right = recycled_from("right.log");

        assert_eq!(
            left, right,
            "one log replayed twice recycled two different id sequences, so which id a durable \
             record gets depends on a HashMap"
        );
        assert_eq!(
            left,
            vec![12, 11, 10, 9, 8, 7],
            "sorted free ids are popped highest-first; this is the sequence, not an arbitrary one"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **Which slot is next must not depend on whether a restart intervened.**
    ///
    /// `open` rebuilds `free_ids` sorted; `release_id` used to push. Reaping a chain deepest-first
    /// releases 2 then 1, so `pop` handed out **1** — while `pop` after reopening the very same log
    /// handed out **2**. The id a durable branch record receives is not allowed to turn on that.
    #[test]
    fn a_runtime_release_and_a_reload_agree_on_which_slot_is_next() {
        let dir = std::env::temp_dir().join(format!("ferro-cat-order-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let live_log = dir.join("chain.log");
        let reload_log = dir.join("chain-reload.log");
        for f in [&live_log, &reload_log] {
            let _ = std::fs::remove_file(f);
        }

        let from_running_catalog = {
            let c = LogBranchCatalog::open(&live_log, 1).unwrap();
            let a = c.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
            let b = c.fork(a.branch_id, LeaseDeadline(0)).unwrap();
            assert_eq!((a.branch_id.id, b.branch_id.id), (1, 2), "fixture: the ids moved");

            // Retire the chain deepest-first, the order `reap_expired` reaps in.
            let mut child = c.get_raw(b.branch_id.id).unwrap();
            child.mark_reaped();
            c.put(&child).unwrap();
            c.release_id(b.branch_id.id);

            let mut parent = c.get_raw(a.branch_id.id).unwrap();
            parent.remove_live_child(b.fork_epoch);
            parent.mark_reaped();
            c.put(&parent).unwrap();
            c.release_id(a.branch_id.id);

            // Copy the log *before* the fork below appends to it, so the reload sees exactly the
            // durable state this catalog is holding in memory right now.
            std::fs::copy(&live_log, &reload_log).unwrap();
            c.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id.id
        };

        let from_reloaded_log = {
            let c = LogBranchCatalog::open(&reload_log, 1).unwrap();
            c.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap().branch_id.id
        };

        assert_eq!(
            from_running_catalog, from_reloaded_log,
            "one durable log handed the same fork slot {from_running_catalog} while running and \
             slot {from_reloaded_log} after a restart"
        );
        assert_eq!(
            from_running_catalog, 2,
            "both paths must take the highest retired slot, not whichever was released last"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `resume_interrupted_reaps` walks `in_state` and reaps in the order it arrives, and every
    /// reap appends durable records and frees pages — so every accessor that can feed a sweep has
    /// to be ordered, not merely happen to be.
    #[test]
    fn every_record_sweep_comes_back_in_branch_id_order() {
        let c = cat();
        for _ in 0..16 {
            c.fork(BranchId::TRUNK, LeaseDeadline(0)).unwrap();
        }
        // One reaped and one quarantined slot, so the three accessors are three different lists
        // and a test that passed by returning the same list every time would fail here.
        let mut rec = c.get_raw(3).unwrap();
        rec.mark_reaped();
        c.put(&rec).unwrap();
        let mut held = c.get_raw(5).unwrap();
        held.state = BranchState::Quarantined;
        c.put(&held).unwrap();

        // 17 records: trunk plus 16 forks. Live is 15 of them (id 3 reaped, id 5 quarantined),
        // and `expired_before` is 14 of those — it excludes trunk as well, which is the whole
        // reason the exclusion lives inside the query rather than in each caller.
        let sweeps: Vec<(&str, usize, Vec<u64>)> = vec![
            ("scan", 17, c.scan().unwrap().map(|r| r.unwrap().branch_id.id).collect()),
            ("in_state(Live)", 15,
             c.in_state(BranchState::Live).unwrap().iter().map(|r| r.branch_id.id).collect()),
            ("in_state(Quarantined)", 1,
             c.in_state(BranchState::Quarantined).unwrap().iter().map(|r| r.branch_id.id).collect()),
            ("expired_before", 14,
             c.expired_before(u64::MAX).unwrap().iter().map(|r| r.branch_id.id).collect()),
        ];
        for (name, want, ids) in sweeps {
            assert_eq!(ids.len(), want, "fixture: {name} returned {ids:?}");
            let mut sorted = ids.clone();
            sorted.sort_unstable();
            assert_eq!(ids, sorted, "{name} came back in hash order");
        }
    }

    /// The three queries that replace `live_children` as an array. They are the reclamation rule
    /// and the privacy barrier, so they get asserted directly rather than only through the arena.
    #[test]
    fn the_live_child_queries_answer_the_rule_the_array_used_to() {
        let c = cat();
        // Children of trunk at successive fork epochs.
        let a = c.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let b = c.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        let d = c.fork(BranchId::TRUNK, LeaseDeadline(1)).unwrap();
        assert!(a.fork_epoch < b.fork_epoch && b.fork_epoch < d.fork_epoch, "fixture ordering");

        // max_live_child is the LATEST, which is what the privacy barrier depends on.
        assert_eq!(c.max_live_child(BranchId::TRUNK.id).unwrap(), Some(d.fork_epoch));
        assert_eq!(c.max_live_child(a.branch_id.id).unwrap(), None, "a leaf has no children");
        assert!(c.has_live_children(BranchId::TRUNK.id).unwrap());
        assert!(!c.has_live_children(a.branch_id.id).unwrap());

        // The reclamation rule, half-open: a page born at `lo` and freed at `hi` is pinned exactly
        // when some live child forked inside [lo, hi).
        let t = BranchId::TRUNK.id;
        assert!(
            c.live_child_in_epoch_range(t, a.fork_epoch, Epoch(a.fork_epoch.0 + 1)).unwrap(),
            "a child forked exactly at lo is INSIDE a half-open [lo, hi)"
        );
        assert!(
            !c.live_child_in_epoch_range(t, d.fork_epoch, d.fork_epoch).unwrap(),
            "an empty window pins nothing"
        );
        assert!(
            !c.live_child_in_epoch_range(t, Epoch(d.fork_epoch.0 + 1), Epoch(d.fork_epoch.0 + 99))
                .unwrap(),
            "a window entirely after every child pins nothing"
        );
        assert!(
            c.live_child_in_epoch_range(t, a.fork_epoch, Epoch(d.fork_epoch.0 + 1)).unwrap(),
            "a window spanning every child is pinned"
        );

        // A slot with no record answers false rather than erroring: nothing pins the page.
        assert!(!c.live_child_in_epoch_range(9_999, Epoch(0), Epoch(u64::MAX)).unwrap());
        assert_eq!(c.max_live_child(9_999).unwrap(), None);
        assert!(!c.has_live_children(9_999).unwrap());
    }

    /// The narrowed surface exists to make a selective question selective. These assert the
    /// *selection*, which is the part a re-implementation over a B+tree must preserve exactly.
    #[test]
    fn expired_before_excludes_trunk_the_unexpired_and_the_not_live() {
        let c = cat();
        // Trunk's own lease must be EXPIRED for this fixture to test anything. Left at
        // `TRUNK_LEASE` it is not expired at `now`, so the trunk assertion below passes whether
        // or not the exclusion exists — and a mutant that deleted the exclusion survived exactly
        // that way before this line was added. The exclusion is now the only thing keeping trunk
        // out of the result.
        let mut trunk = c.get(BranchId::TRUNK).unwrap();
        trunk.lease_deadline = LeaseDeadline(1);
        c.put(&trunk).unwrap();

        let early = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        let late = c.fork(BranchId::TRUNK, LeaseDeadline(9_000)).unwrap();
        let reaping = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        let mut r = c.get(reaping.branch_id).unwrap();
        r.state = BranchState::Reaping;
        c.put(&r).unwrap();

        let ids: Vec<u64> =
            c.expired_before(1_000).unwrap().iter().map(|r| r.branch_id.id).collect();
        assert_eq!(ids, vec![early.branch_id.id], "expected only the expired Live non-trunk branch");
        assert!(!ids.contains(&BranchId::TRUNK.id), "trunk must never be a reap candidate");
        assert!(!ids.contains(&late.branch_id.id), "an unexpired lease is not a candidate");
        assert!(!ids.contains(&reaping.branch_id.id), "a Reaping branch is not a fresh candidate");

        // Boundary: `is_expired_at` is inclusive, so a deadline exactly at `now` is expired.
        assert!(c.expired_before(100).unwrap().iter().any(|r| r.branch_id == early.branch_id));
        assert!(c.expired_before(99).unwrap().is_empty(), "a lease one ms out is not expired");
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
