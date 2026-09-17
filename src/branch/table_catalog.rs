//! The branch catalog stored the way every other database stores a catalog: **as a table.**
//!
//! `pg_class` is a heap table with B+tree indexes. SQLite's `sqlite_schema` is a table on page 1.
//! InnoDB moved `.frm` files into data-dictionary tables in 8.0 specifically to stop having a
//! second storage mechanism. This is that, over `BPlusTreeManager`, which already backs every
//! user table's primary index in this repo.
//!
//! **What it replaces and why.** `LogBranchCatalog` is an append-only file replayed at open into a
//! `HashMap<u64, BranchRecord>` of every record. Measured (`bench/o1_residency_marginal.txt`), that
//! map costs ~685 resident bytes per branch and rises, because a doubling table's freed
//! predecessors are retained by the allocator. Open is O(operations ever written) and the log never
//! compacts, so a branch created and reaped leaves records for ever. Here the records live in tree
//! pages under a bounded buffer pool, and open is one descent to read a header key.
//!
//! **One tree, not seven.** Records and every index over them share a single tree, separated by the
//! leading tag byte in [`crate::branch::tree_keys`]. One root page id to persist rather than seven,
//! and therefore one thing to keep crash-consistent instead of seven that can disagree.
//!
//! **Unbounded fields are key spans, not record fields.** A leaf page holds about 2 KB of entries
//! in total. `live_children` at 10⁶ branches is 8 MB, `arenas` reaches ~900 ids for a branch that
//! has written ~230 MB, and an envelope carries a table list. A record that can outgrow a page is
//! a wall with no error message, so all three live in their own spans.
//!
//! See `SCALE-DESIGN.md` D2b.

use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::branch::record::{BranchRecord, CapabilityEnvelope};
use crate::branch::tree_keys as keys;
use crate::branch::types::{
    ArenaId, BranchError, BranchId, BranchState, Epoch, LeaseDeadline, PageId,
};
use crate::branch::BranchCatalog;
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::error::FerroError;
use crate::storage::index::BPlusTreeManager;

/// Header payload: `next_id` then `epoch`, both big-endian.
const HEADER_BYTES: usize = 16;

pub struct TableBranchCatalog {
    tree: BPlusTreeManager<Vec<u8>, Vec<u8>>,
    /// Serialises **logical** operations, each of which touches several keys.
    ///
    /// The tree's own locking protects a single key. A fork writes a core record, a deadline key, a
    /// state key and a child key; without this, two concurrent forks can interleave and a reader
    /// can see a branch that exists but is not yet in its parent's live set — which is the GC
    /// correctness hole the log catalog closed by putting both halves in one append.
    logical: Mutex<()>,
    next_id: AtomicU64,
    epoch: AtomicU64,
}

impl TableBranchCatalog {
    /// Create an empty catalog containing only trunk, and return it with the tree's root page id.
    ///
    /// The root page id is the **one** value that cannot live in the tree, because reading the tree
    /// requires it. Everything else — `next_id`, the epoch counter — is a key, so `open` is one
    /// descent rather than a replay.
    pub fn create(pool: Arc<BufferPoolManager>, trunk_root: PageId) -> Result<Self, FerroError> {
        let tree = BPlusTreeManager::<Vec<u8>, Vec<u8>>::create(pool)?;
        let cat = TableBranchCatalog {
            tree,
            logical: Mutex::new(()),
            next_id: AtomicU64::new(1),
            epoch: AtomicU64::new(1),
        };
        let trunk = BranchRecord::trunk(trunk_root, crate::branch::TRUNK_LEASE);
        cat.tree.insert(keys::record(trunk.branch_id.id), trunk.serialize_core())?;
        cat.tree.insert(keys::state(trunk.state.as_u8(), trunk.branch_id.id), Vec::new())?;
        // Trunk is deliberately absent from the DEADLINE span: it is excluded from every reap
        // query, so an entry for it would sit at the head of the range for ever and every scan
        // would step over it.
        cat.write_header()?;
        Ok(cat)
    }

    /// Reopen an existing catalog from its tree root. One descent, no replay.
    pub fn open(pool: Arc<BufferPoolManager>, root_page_id: u32) -> Result<Self, FerroError> {
        let tree = BPlusTreeManager::<Vec<u8>, Vec<u8>>::open(root_page_id, pool);
        let cat = TableBranchCatalog {
            tree,
            logical: Mutex::new(()),
            next_id: AtomicU64::new(1),
            epoch: AtomicU64::new(1),
        };
        let bytes = cat.tree.search(&keys::header())?.ok_or_else(|| {
            FerroError::Branch("branch catalog header key is missing; the tree root is wrong \
                                or the catalog was never created".into())
        })?;
        if bytes.len() != HEADER_BYTES {
            return Err(BranchError::Corrupt(format!(
                "catalog header must be {HEADER_BYTES} bytes, got {}",
                bytes.len()
            ))
            .into());
        }
        cat.next_id.store(u64::from_be_bytes(bytes[0..8].try_into().unwrap()), Ordering::SeqCst);
        cat.epoch.store(u64::from_be_bytes(bytes[8..16].try_into().unwrap()), Ordering::SeqCst);
        Ok(cat)
    }

    /// The tree's current root page id. **The caller must persist this**: it moves whenever a root
    /// split happens, and it is the only value `open` cannot find for itself.
    pub fn root_page_id(&self) -> u32 {
        self.tree.root_page_id.load(Ordering::SeqCst)
    }

    fn write_header(&self) -> Result<(), FerroError> {
        let mut v = Vec::with_capacity(HEADER_BYTES);
        v.extend_from_slice(&self.next_id.load(Ordering::SeqCst).to_be_bytes());
        v.extend_from_slice(&self.epoch.load(Ordering::SeqCst).to_be_bytes());
        self.upsert(keys::header(), v)
    }

    /// Replace a key's value.
    ///
    /// **`BPlusTreeManager::insert` does not replace** — `insert_entry` always inserts, so writing
    /// an existing key a second time leaves TWO entries and `search` returns whichever the binary
    /// search lands on. For a catalog that would mean a branch with two records that disagree,
    /// chosen nondeterministically. Delete-then-insert is the only upsert the storage layer offers.
    fn upsert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), FerroError> {
        match self.tree.delete(&key) {
            Ok(()) => {}
            // Absent is the normal case for a first write, not an error.
            Err(FerroError::KeyNotFound) => {}
            Err(e) => return Err(e),
        }
        self.tree.insert(key, value)
    }

    fn remove_if_present(&self, key: &Vec<u8>) -> Result<bool, FerroError> {
        match self.tree.delete(key) {
            Ok(()) => Ok(true),
            Err(FerroError::KeyNotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// A record with its unbounded fields filled in from their spans.
    ///
    /// `arenas` is populated because `ArenaPageStore` still mutates it and calls `put`, and a
    /// record that came back empty would delete every arena on the next write. `live_children` is
    /// deliberately left EMPTY — it is unbounded, and every caller was moved onto the indexed
    /// queries first so that nothing reads it.
    fn hydrate(&self, mut rec: BranchRecord) -> Result<BranchRecord, FerroError> {
        let (lo, hi) = keys::arenas_of(rec.branch_id.id);
        for entry in self.tree.range_scan(Bound::Included(lo), Bound::Excluded(hi))? {
            let (k, _) = entry?;
            if k.len() == 13 {
                rec.arenas.push(ArenaId(u32::from_be_bytes(k[9..13].try_into().unwrap())));
            }
        }
        rec.envelope = self.envelope_bytes(rec.branch_id.id)?;
        Ok(rec)
    }

    fn envelope_bytes(&self, id: u64) -> Result<Option<CapabilityEnvelope>, FerroError> {
        match self.tree.search(&keys::envelope(id))? {
            Some(b) => Ok(Some(CapabilityEnvelope::deserialize(&b).map_err(FerroError::from)?)),
            None => Ok(None),
        }
    }

    fn core(&self, id: u64) -> Result<Option<BranchRecord>, FerroError> {
        match self.tree.search(&keys::record(id))? {
            Some(b) => Ok(Some(BranchRecord::deserialize_core(&b)?)),
            None => Ok(None),
        }
    }

    /// Write a record's core plus the index entries derived from it, removing the entries derived
    /// from `old` first. The CHILD span is **not** touched here: children are inserted by `fork`
    /// and removed by `detach_child`, because `put` receives records whose `live_children` is
    /// empty by construction and diffing against that would delete every child.
    fn write_record(
        &self,
        rec: &BranchRecord,
        old: Option<&BranchRecord>,
    ) -> Result<(), FerroError> {
        if let Some(prev) = old {
            self.remove_if_present(&keys::state(prev.state.as_u8(), prev.branch_id.id))?;
            if Self::in_deadline_index(prev) {
                self.remove_if_present(&keys::deadline(prev.lease_deadline.0, prev.branch_id.id))?;
            }
        }
        self.upsert(keys::record(rec.branch_id.id), rec.serialize_core())?;
        self.upsert(keys::state(rec.state.as_u8(), rec.branch_id.id), Vec::new())?;
        if Self::in_deadline_index(rec) {
            self.upsert(keys::deadline(rec.lease_deadline.0, rec.branch_id.id), Vec::new())?;
        }
        match &rec.envelope {
            Some(e) => self.upsert(keys::envelope(rec.branch_id.id), e.serialize())?,
            None => {
                self.remove_if_present(&keys::envelope(rec.branch_id.id))?;
            }
        }
        // Arenas: the record is the authority, so the span is made to match it.
        let (lo, hi) = keys::arenas_of(rec.branch_id.id);
        let existing: Vec<Vec<u8>> = self
            .tree
            .range_scan(Bound::Included(lo), Bound::Excluded(hi))?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        for k in existing {
            if k.len() == 13 {
                let a = ArenaId(u32::from_be_bytes(k[9..13].try_into().unwrap()));
                if !rec.arenas.contains(&a) {
                    self.remove_if_present(&k)?;
                }
            }
        }
        for a in &rec.arenas {
            self.upsert(keys::arena(rec.branch_id.id, a.0), Vec::new())?;
        }
        Ok(())
    }

    /// Only `Live`, non-trunk branches are in the deadline index.
    ///
    /// A quarantined branch keeps its record for as long as an operator wants to look at it and its
    /// lease expired long ago; left in the index it would sit at the head of the range for ever and
    /// every 30-second reap scan would step over it before reaching anything real. That is an
    /// unbounded walk reintroduced into the hot path through the back door.
    fn in_deadline_index(rec: &BranchRecord) -> bool {
        rec.state == BranchState::Live && !rec.branch_id.is_trunk()
    }

    /// Ids of every entry in a span whose key ends with an 8-byte branch id.
    fn ids_in_span(&self, lo: Vec<u8>, hi: Vec<u8>) -> Result<Vec<u64>, FerroError> {
        let mut out = Vec::new();
        for entry in self.tree.range_scan(Bound::Included(lo), Bound::Excluded(hi))? {
            let (k, _) = entry?;
            let n = k.len();
            if n >= 8 {
                out.push(u64::from_be_bytes(k[n - 8..].try_into().unwrap()));
            }
        }
        Ok(out)
    }

    /// Live branches, for parity with `LogBranchCatalog::live_count`.
    pub fn live_count(&self) -> Result<usize, FerroError> {
        let (lo, hi) = keys::whole_state(BranchState::Live.as_u8());
        Ok(self.ids_in_span(lo, hi)?.len())
    }
}

impl BranchCatalog for TableBranchCatalog {
    fn next_epoch(&self) -> Epoch {
        Epoch(self.epoch.fetch_add(1, Ordering::SeqCst))
    }

    fn current_epoch(&self) -> Epoch {
        Epoch(self.epoch.load(Ordering::SeqCst))
    }

    fn fork(&self, parent: BranchId, lease: LeaseDeadline) -> Result<BranchRecord, FerroError> {
        let fork_epoch = self.next_epoch();
        let _g = self.logical.lock().unwrap();

        let parent_rec = self.core(parent.id)?.ok_or(BranchError::NotFound(parent))?;
        parent_rec.check_readable(parent)?;

        // Recycle a retired slot if one is free, otherwise mint a new one. Either way the
        // generation comes from the slot's history, never from zero — a reused id whose generation
        // restarted would make a stale handle look current.
        let (lo, hi) = keys::whole_group(keys::tag::FREE_ID);
        let recycled = self
            .tree
            .range_scan(Bound::Included(lo), Bound::Excluded(hi))?
            .next()
            .transpose()?
            .and_then(|(k, _)| keys::free_id_from_key(&k));

        let (child_num, generation) = match recycled {
            Some(id) => {
                self.remove_if_present(&keys::free_id(id))?;
                let slot_gen = self.core(id)?.map(|r| r.generation).unwrap_or(0);
                (id, slot_gen)
            }
            None => (self.next_id.fetch_add(1, Ordering::SeqCst), 0),
        };
        let child_id = BranchId::new(child_num, generation);
        let child = BranchRecord::fork_child(&parent_rec, child_id, fork_epoch, lease)?;

        self.write_record(&child, None)?;
        // The child's entry in its parent's live set. A child that exists but is not listed in its
        // parent is a GC correctness hole, which is why both happen under one logical lock.
        self.tree.insert(keys::child(parent.id, fork_epoch.0), Vec::new())?;
        self.write_header()?;
        Ok(child)
    }

    fn get(&self, branch: BranchId) -> Result<BranchRecord, FerroError> {
        let rec = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        rec.check_readable(branch)?;
        self.hydrate(rec)
    }

    fn put(&self, record: &BranchRecord) -> Result<(), FerroError> {
        let _g = self.logical.lock().unwrap();
        let old = self.core(record.branch_id.id)?;
        self.write_record(record, old.as_ref())
    }

    fn set_root(&self, branch: BranchId, root: PageId) -> Result<(), FerroError> {
        let _g = self.logical.lock().unwrap();
        let mut rec = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        rec.check_readable(branch)?;
        let old = rec.clone();
        rec.root_page_id = root;
        self.write_record(&rec, Some(&old))
    }

    fn expired_before(&self, now_millis: u64) -> Result<Vec<BranchRecord>, FerroError> {
        let (lo, hi) = keys::expired_at_or_before(now_millis);
        let mut out = Vec::new();
        for id in self.ids_in_span(lo, hi)? {
            if let Some(rec) = self.core(id)? {
                // The index holds only Live non-trunk branches, so this re-check is belt and
                // braces against an index entry that outlived its record rather than a filter the
                // query depends on.
                if Self::in_deadline_index(&rec) && rec.lease_deadline.is_expired_at(now_millis) {
                    out.push(self.hydrate(rec)?);
                }
            }
        }
        out.sort_unstable_by_key(|r| r.branch_id.id);
        Ok(out)
    }

    fn in_state(&self, state: BranchState) -> Result<Vec<BranchRecord>, FerroError> {
        let (lo, hi) = keys::whole_state(state.as_u8());
        let mut out = Vec::new();
        for id in self.ids_in_span(lo, hi)? {
            if let Some(rec) = self.core(id)? {
                out.push(self.hydrate(rec)?);
            }
        }
        out.sort_unstable_by_key(|r| r.branch_id.id);
        Ok(out)
    }

    fn scan(&self)
        -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        let (lo, hi) = keys::whole_group(keys::tag::RECORD);
        let it = self.tree.range_scan(Bound::Included(lo), Bound::Excluded(hi))?;
        // Streaming: the scanner walks leaf by leaf, so a full system view or snapshot holds one
        // record at a time rather than a second copy of the catalog.
        Ok(Box::new(it.map(|e| {
            let (_, v) = e?;
            BranchRecord::deserialize_core(&v).map_err(FerroError::from)
        })))
    }

    fn max_live_child(&self, parent_id: u64) -> Result<Option<Epoch>, FerroError> {
        let (lo, hi) = keys::children_of(parent_id);
        // One descent: the CHILD key stores the complement of the fork epoch, so the newest child
        // is the FIRST key in the span. Ascending would put it last and reading it would mean
        // consuming every child trunk has.
        match self.tree.range_scan(Bound::Included(lo), Bound::Excluded(hi))?.next().transpose()? {
            Some((k, _)) => Ok(keys::child_epoch_from_key(&k).map(Epoch)),
            None => Ok(None),
        }
    }

    fn live_child_in_epoch_range(
        &self,
        parent_id: u64,
        lo: Epoch,
        hi: Epoch,
    ) -> Result<bool, FerroError> {
        let Some((klo, khi)) = keys::children_in_epoch_range(parent_id, lo.0, hi.0) else {
            // An empty window pins nothing.
            return Ok(false);
        };
        Ok(self
            .tree
            .range_scan(Bound::Included(klo), Bound::Included(khi))?
            .next()
            .transpose()?
            .is_some())
    }

    fn has_live_children(&self, parent_id: u64) -> Result<bool, FerroError> {
        let (lo, hi) = keys::children_of(parent_id);
        Ok(self
            .tree
            .range_scan(Bound::Included(lo), Bound::Excluded(hi))?
            .next()
            .transpose()?
            .is_some())
    }

    /// Counts the `Live` state span. Unlike the log catalog's, this is a scan of that span rather
    /// than of every record, so it is proportional to the answer.
    fn live_count(&self) -> usize {
        TableBranchCatalog::live_count(self).unwrap_or(0)
    }

    fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> {
        // No `check_readable`: that is the whole point. A branch mid-reap or already reaped still
        // owns the children that decide the fate of its pages.
        let rec = self.core(id)?.ok_or(BranchError::NotFound(BranchId::new(id, 0)))?;
        self.hydrate(rec)
    }

    fn release_id(&self, id: u64) {
        if id == 0 {
            return;
        }
        let _g = self.logical.lock().unwrap();
        // Refuse while the slot still has live children - that set decides the fate of pages
        // parked under this branch's name. Errors are swallowed to match the inherent method's
        // signature on the log catalog, which returns nothing: a failure here leaks an id slot,
        // which is recoverable, while propagating it would abort a reap midway, which is not.
        let reusable = match (self.core(id), self.has_live_children(id)) {
            (Ok(Some(rec)), Ok(false)) => rec.state == BranchState::Reaped,
            _ => false,
        };
        if reusable {
            let _ = self.upsert(keys::free_id(id), Vec::new());
        }
    }

    fn detach_child(&self, parent_id: u64, fork_epoch: Epoch) -> Result<bool, FerroError> {
        let _g = self.logical.lock().unwrap();
        self.remove_if_present(&keys::child(parent_id, fork_epoch.0))
    }

    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError> {
        let _g = self.logical.lock().unwrap();
        let mut rec = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        rec.check_readable(branch)?;
        let old = rec.clone();
        rec.lease_deadline = lease;
        self.write_record(&rec, Some(&old))
    }

    fn envelope_of(&self, branch: BranchId) -> Result<Option<CapabilityEnvelope>, FerroError> {
        // A point lookup on its own key, which is the reason the envelope is stored separately:
        // the write funnel asks this on every statement and must not pay for the rest of a record.
        let rec = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        rec.check_readable(branch)?;
        self.envelope_bytes(branch.id)
    }

    fn charge_row_writes(&self, branch: BranchId, n: u64) -> Result<(), FerroError> {
        // Under the logical lock so the read-modify-write cannot lose a concurrent charge, and so
        // it cannot discard a `set_root` or `renew_lease` that landed in the window: only the
        // envelope key is written back, never a whole stale record.
        let _g = self.logical.lock().unwrap();
        let rec = self.core(branch.id)?.ok_or(BranchError::NotFound(branch))?;
        rec.check_readable(branch)?;
        let mut env = self.envelope_bytes(branch.id)?.ok_or_else(|| {
            FerroError::Branch(format!(
                "{branch} has no capability envelope; refusing to charge {n} row-writes against a \
                 policy that no longer exists"
            ))
        })?;
        env.charge(n)?;
        self.upsert(keys::envelope(branch.id), env.serialize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk_manager::DiskManager;
    use std::fs::OpenOptions;

    fn cat(tag: &str) -> (TableBranchCatalog, std::path::PathBuf, Arc<BufferPoolManager>) {
        let path = std::env::temp_dir()
            .join(format!("ferro-tablecat-{}-{}.db", std::process::id(), tag));
        let _ = std::fs::remove_file(&path);
        let f = OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())));
        let c = TableBranchCatalog::create(Arc::clone(&pool), 1).unwrap();
        (c, path, pool)
    }

    #[test]
    fn a_fresh_catalog_holds_trunk_and_nothing_else() {
        let (c, p, _pool) = cat("fresh");
        let trunk = c.get(BranchId::TRUNK).unwrap();
        assert_eq!(trunk.branch_id, BranchId::TRUNK);
        assert_eq!(trunk.root_page_id, 1);
        assert_eq!(c.live_count().unwrap(), 1);
        let all: Vec<u64> =
            c.scan().unwrap().map(|r| r.unwrap().branch_id.id).collect();
        assert_eq!(all, vec![0], "scan must return exactly trunk");
        let _ = std::fs::remove_file(p);
    }

    /// **The duplicate-key hazard.** `BPlusTreeManager::insert` never replaces, so a second write
    /// to one key leaves TWO entries and `search` returns whichever the binary search lands on.
    /// For a catalog that is one branch with two disagreeing records, chosen nondeterministically.
    #[test]
    fn writing_a_record_twice_leaves_exactly_one_of_it() {
        let (c, p, _pool) = cat("dup");
        let child = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();

        let mut rec = c.get(child.branch_id).unwrap();
        rec.root_page_id = 42;
        c.put(&rec).unwrap();
        rec.root_page_id = 99;
        c.put(&rec).unwrap();

        let ids: Vec<u64> = c.scan().unwrap().map(|r| r.unwrap().branch_id.id).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(ids.len(), sorted.len(), "a branch appears twice in the record span: {ids:?}");
        assert_eq!(c.get(child.branch_id).unwrap().root_page_id, 99, "read the stale copy");
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn fork_records_the_child_in_its_parents_live_set() {
        let (c, p, _pool) = cat("fork");
        let a = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        let b = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        assert!(a.fork_epoch < b.fork_epoch, "fixture ordering");

        assert!(c.has_live_children(BranchId::TRUNK.id).unwrap());
        // The LATEST, which is what the privacy barrier depends on, and the reason the key stores
        // a complemented epoch: it is the first entry in the span, one descent away.
        assert_eq!(c.max_live_child(BranchId::TRUNK.id).unwrap(), Some(b.fork_epoch));
        assert_eq!(c.max_live_child(a.branch_id.id).unwrap(), None, "a leaf has no children");

        // The reclamation rule, half-open.
        let t = BranchId::TRUNK.id;
        assert!(c.live_child_in_epoch_range(t, a.fork_epoch, Epoch(a.fork_epoch.0 + 1)).unwrap());
        assert!(!c.live_child_in_epoch_range(t, a.fork_epoch, a.fork_epoch).unwrap(), "empty window");
        assert!(!c
            .live_child_in_epoch_range(t, Epoch(b.fork_epoch.0 + 1), Epoch(b.fork_epoch.0 + 9))
            .unwrap());

        // detach removes exactly one child and reports it.
        assert!(c.detach_child(t, a.fork_epoch).unwrap(), "detach reported nothing removed");
        assert!(!c.detach_child(t, a.fork_epoch).unwrap(), "detach removed the same child twice");
        assert_eq!(c.max_live_child(t).unwrap(), Some(b.fork_epoch));
        assert!(c.detach_child(t, b.fork_epoch).unwrap());
        assert!(!c.has_live_children(t).unwrap(), "trunk still reports children after both left");
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn expired_before_selects_only_expired_live_non_trunk_branches() {
        let (c, p, _pool) = cat("expired");
        let early = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        let late = c.fork(BranchId::TRUNK, LeaseDeadline(9_000)).unwrap();
        let held = c.fork(BranchId::TRUNK, LeaseDeadline(100)).unwrap();
        let mut h = c.get(held.branch_id).unwrap();
        h.state = BranchState::Quarantined;
        c.put(&h).unwrap();

        let ids: Vec<u64> =
            c.expired_before(1_000).unwrap().iter().map(|r| r.branch_id.id).collect();
        assert_eq!(ids, vec![early.branch_id.id], "expected only the expired Live non-trunk branch");
        assert!(!ids.contains(&late.branch_id.id), "an unexpired lease is not a candidate");
        assert!(!ids.contains(&held.branch_id.id), "a quarantined branch is not a candidate");
        assert!(!ids.contains(&BranchId::TRUNK.id), "trunk must never be a reap candidate");

        // A quarantined branch must have LEFT the deadline index, or it sits at the head of the
        // range for ever and every scan steps over it.
        assert!(c.in_state(BranchState::Quarantined).unwrap().len() == 1);
        assert!(
            c.expired_before(u64::MAX).unwrap().iter().all(|r| r.branch_id.id != held.branch_id.id),
            "the quarantined branch is still in the deadline index"
        );
        let _ = std::fs::remove_file(p);
    }

    /// Open must be a descent, not a replay: the header carries `next_id` and the epoch, so a
    /// reopened catalog cannot mint an id that collides with a live branch.
    #[test]
    fn reopening_recovers_the_counters_without_replaying_anything() {
        let (c, p, pool) = cat("reopen");
        for _ in 0..8 {
            c.fork(BranchId::TRUNK, LeaseDeadline(50)).unwrap();
        }
        let root = c.root_page_id();
        let next_id = c.next_id.load(Ordering::SeqCst);
        let epoch = c.current_epoch();
        drop(c);

        let re = TableBranchCatalog::open(pool, root).unwrap();
        assert_eq!(re.next_id.load(Ordering::SeqCst), next_id, "next_id did not survive");
        assert_eq!(re.current_epoch(), epoch, "epoch did not survive");
        assert_eq!(re.live_count().unwrap(), 9, "trunk plus eight children");

        // The id it mints next must not collide with one already handed out.
        let fresh = re.fork(BranchId::TRUNK, LeaseDeadline(50)).unwrap();
        assert_eq!(fresh.branch_id.id, next_id, "a reopened catalog reused a live id");
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn scan_returns_every_record_in_branch_id_order() {
        let (c, p, _pool) = cat("scan");
        for _ in 0..12 {
            c.fork(BranchId::TRUNK, LeaseDeadline(50)).unwrap();
        }
        let ids: Vec<u64> = c.scan().unwrap().map(|r| r.unwrap().branch_id.id).collect();
        assert_eq!(ids.len(), 13, "trunk plus twelve");
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(ids, sorted, "scan came back out of branch-id order");
        let _ = std::fs::remove_file(p);
    }
}
