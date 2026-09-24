//! A statement's provenance made durable by ONE sync, on every exit (D219).
//!
//! A `MERGE` writes provenance from three places: the ALTER rewrite re-stamps the rows it moves,
//! the executors stamp every version the publish loop writes, and `record_applied` records which
//! run published each row. Against the durable store every one of those calls used to sync, so a
//! merge of δ rows paid on the order of 2δ fsyncs while every other statement waited on the catalog
//! lock it holds.
//!
//! [`ProvenanceFlush`] is what the merge holds instead. The code that stamps through the trait —
//! the executors and the rewrite, neither of which knows it is inside a merge — is handed
//! [`ProvenanceFlush::stamper`], whose `stamp` is `stamp_pending`: applied to the index at once,
//! with every guard, and written later. The merge's first durable write after that (its row
//! authorship) carries every pending stamp ahead of its own records in one append and one sync, and
//! [`ProvenanceFlush::flush`] covers a merge with no row to attribute.
//!
//! **The guard exists for the exits nobody writes a flush on.** A merge that fails half way has
//! already applied its stamps to the index, exactly as it had already synced them before D219; the
//! guard's `Drop` writes them on the early return, so the index and the file still agree and no
//! record outlives the statement unwritten. This is `ForkDurability`'s shape
//! (`agent_sql/session.rs`): the durability debt is an object, so every `?` discharges it.
//!
//! # Crash semantics, before and after
//!
//! **Before:** the rewrite's and the publish loop's stamps each synced as they were written, which
//! is BEFORE the publish transaction's WAL commit (and, for the rewrite, before the heap flush that
//! makes the rewrite durable); row authorship synced after the commit.
//!
//! * A crash before the commit: recovery rolls the publish back, but the stamps already synced for
//!   its versions stay in the file, attributing `(page, slot)`s the rollback frees and a later
//!   write may reuse.
//! * A crash between the commit and the authorship sync: the rows and every physical stamp are
//!   durable; row authorship is a prefix of the merge's records.
//!
//! **After:** nothing reaches the file until the one append after the commit.
//!
//! * A crash before the commit: the file holds nothing from this merge, so the stale-stamp case is
//!   gone.
//! * A crash between the commit and the one sync: the rows are durable, and the file holds a
//!   PREFIX of `[rewrite stamps, publish stamps, row authorship]`, possibly empty, because the
//!   reader stops at the first frame whose length or CRC fails. So a committed version CAN now
//!   reopen physically unattributed, which it could not before, and a re-published row reopens
//!   under its previous author, as before. A row the rewrite moved is in the same case: its heap
//!   move is durable before its stamp is.
//! * Once the sync returns, everything is durable, and the MERGE returns only after it (the
//!   explicit flush on success, `Drop` on every other exit). The durability point did not move past
//!   the acknowledgement.
//!
//! Neither shape is atomic per merge. The window in which a committed merge's provenance can be
//! missing went from "until the last of δ syncs" to one sync; closing it entirely needs the
//! provenance carried by the WAL commit itself, which is a format decision and not this change.

use std::sync::Arc;

use crate::error::FerroError;
use crate::provenance::{ProvId, ProvenanceStore, RunEntity, SyncCounts};
use crate::storage::heap_file_manager::RecordId;

/// Holds one statement's pending provenance and makes it durable — by [`Self::flush`] on the
/// success path, and by `Drop` on every other.
pub struct ProvenanceFlush {
    store: Arc<dyn ProvenanceStore>,
    stamper: Arc<dyn ProvenanceStore>,
    flushed: bool,
}

impl ProvenanceFlush {
    pub fn new(store: Arc<dyn ProvenanceStore>) -> Self {
        let stamper: Arc<dyn ProvenanceStore> = Arc::new(Deferred(Arc::clone(&store)));
        ProvenanceFlush { store, stamper, flushed: false }
    }

    /// The store to hand to code that stamps versions through the trait. Its `stamp` is the
    /// underlying store's `stamp_pending`; every other call passes straight through.
    pub fn stamper(&self) -> &Arc<dyn ProvenanceStore> {
        &self.stamper
    }

    /// Make everything pending durable now, reporting a failure. Call it before acknowledging the
    /// statement; nothing pending is not a write, so it costs no sync when an earlier durable write
    /// already carried the stamps.
    pub fn flush(mut self) -> Result<(), FerroError> {
        self.flushed = true;
        self.store.flush()
    }
}

impl Drop for ProvenanceFlush {
    fn drop(&mut self) {
        if !self.flushed {
            // An early exit: the statement is returning an error or unwinding. Its stamps are in
            // the index already, so they are written now rather than left for a later statement.
            // The failure is not swallowed: the durable store poisons ITSELF when an append fails,
            // so every later write refuses — the same argument `AgentRuntime::forget_table` makes.
            let _ = self.store.flush();
        }
    }
}

/// A store whose `stamp` is `stamp_pending`. Everything else passes straight through, reads
/// included, so the index a rewrite reads its attribution from is the one it stamps into.
struct Deferred(Arc<dyn ProvenanceStore>);

impl ProvenanceStore for Deferred {
    fn intern(&self, run: &RunEntity) -> Result<ProvId, FerroError> {
        self.0.intern(run)
    }

    fn lookup(&self, id: ProvId) -> Result<RunEntity, FerroError> {
        self.0.lookup(id)
    }

    fn attribute(&self, rid: RecordId) -> Result<ProvId, FerroError> {
        self.0.attribute(rid)
    }

    fn stamp(&self, rid: RecordId, id: ProvId) -> Result<(), FerroError> {
        self.0.stamp_pending(rid, id)
    }

    fn stamp_pending(&self, rid: RecordId, id: ProvId) -> Result<(), FerroError> {
        self.0.stamp_pending(rid, id)
    }

    fn flush(&self) -> Result<(), FerroError> {
        self.0.flush()
    }

    fn page_dictionary_lens(&self) -> Result<Vec<(u32, usize)>, FerroError> {
        self.0.page_dictionary_lens()
    }

    fn stamp_row(&self, table: u32, row: u64, id: ProvId) -> Result<(), FerroError> {
        self.0.stamp_row(table, row, id)
    }

    fn stamp_rows(&self, rows: &[(u32, u64)], id: ProvId) -> Result<(), FerroError> {
        self.0.stamp_rows(rows, id)
    }

    fn row_author(&self, table: u32, row: u64) -> Result<ProvId, FerroError> {
        self.0.row_author(table, row)
    }

    fn attributed_rows(&self, table: u32) -> Result<Vec<(u64, ProvId)>, FerroError> {
        self.0.attributed_rows(table)
    }

    fn forget_table(&self, table: u32) -> Result<(), FerroError> {
        self.0.forget_table(table)
    }

    fn sync_counts(&self) -> SyncCounts {
        self.0.sync_counts()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::types::BranchId;
    use crate::provenance::sha256::prompt_digest;
    use crate::provenance::DurableProvenanceStore;

    fn run() -> RunEntity {
        RunEntity::new(
            ProvId::NONE,
            "restock",
            "run-1",
            "claude-opus",
            "2026-05",
            prompt_digest("restock"),
            1,
            BranchId::new(4, 0),
        )
    }

    fn rid(page: u32, slot: u16) -> RecordId {
        RecordId { page_id: page, slot_num: slot }
    }

    /// **D219: stamps through the stamper and the row authorship after them are ONE sync, and the
    /// file is byte-for-byte the file the same calls made one sync at a time.**
    ///
    /// The byte comparison is what the crash argument rests on: replay reads frames in file order,
    /// so the same frames in the same order reopen to the same answers, torn tail or not.
    #[test]
    fn stamps_through_the_stamper_ride_the_next_sync_and_write_the_same_file() {
        let dir = tempfile::tempdir().unwrap();
        let (eager, deferred) = (dir.path().join("eager.log"), dir.path().join("deferred.log"));
        {
            let s = DurableProvenanceStore::open(&eager).unwrap();
            let id = s.intern(&run()).unwrap();
            for slot in 0..3 {
                s.stamp(rid(1, slot), id).unwrap();
            }
            s.stamp_rows(&[(7, 1), (7, 2)], id).unwrap();
            assert_eq!(s.sync_counts().total(), 5, "the control is one sync per call, run included");
        }
        {
            let s: Arc<dyn ProvenanceStore> = Arc::new(DurableProvenanceStore::open(&deferred).unwrap());
            let id = s.intern(&run()).unwrap();
            let before = s.sync_counts();
            let guard = ProvenanceFlush::new(Arc::clone(&s));
            for slot in 0..3 {
                guard.stamper().stamp(rid(1, slot), id).unwrap();
            }
            assert_eq!(s.sync_counts(), before, "a pending stamp issued a sync");
            assert_eq!(s.attribute(rid(1, 2)).unwrap(), id, "a pending stamp is not in the index");
            s.stamp_rows(&[(7, 1), (7, 2)], id).unwrap();
            guard.flush().unwrap();
            assert_eq!(
                s.sync_counts().total() - before.total(),
                1,
                "three stamps and a batch of row authorship took more than one sync"
            );
        }
        assert_eq!(
            std::fs::read(&deferred).unwrap(),
            std::fs::read(&eager).unwrap(),
            "deferring the stamps wrote a different file"
        );
    }

    /// A statement with no row to attribute still makes its stamps durable, with one sync from the
    /// flush — and a pending stamp is NOT in the file until something writes it.
    #[test]
    fn the_flush_alone_makes_pending_stamps_durable_with_one_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let s: Arc<dyn ProvenanceStore> = Arc::new(DurableProvenanceStore::open(&path).unwrap());
        let id = s.intern(&run()).unwrap();
        let guard = ProvenanceFlush::new(Arc::clone(&s));
        guard.stamper().stamp(rid(3, 0), id).unwrap();
        guard.stamper().stamp(rid(3, 1), id).unwrap();

        let unwritten = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(unwritten.recovery().stamps, 0, "a pending stamp reached the file before a flush");
        drop(unwritten);

        let before = s.sync_counts();
        guard.flush().unwrap();
        assert_eq!(s.sync_counts().stamps - before.stamps, 1, "the flush did not issue exactly one sync");
        assert_eq!(s.sync_counts().total() - before.total(), 1);
        let reopened = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(reopened.recovery().stamps, 2);
        assert_eq!(reopened.attribute(rid(3, 1)).unwrap(), id);
    }

    /// **An early exit writes what the statement already stamped.** The guard dropped without a
    /// flush — a `?` or a panic between the stamps and the flush — still leaves the index and the
    /// file agreeing, which is what a stamp that synced on the spot used to guarantee.
    #[test]
    fn a_guard_dropped_without_a_flush_still_writes_its_stamps() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let s: Arc<dyn ProvenanceStore> = Arc::new(DurableProvenanceStore::open(&path).unwrap());
        let id = s.intern(&run()).unwrap();
        {
            let guard = ProvenanceFlush::new(Arc::clone(&s));
            guard.stamper().stamp(rid(5, 0), id).unwrap();
        }
        let reopened = DurableProvenanceStore::open(&path).unwrap();
        assert_eq!(reopened.attribute(rid(5, 0)).unwrap(), id, "the early exit lost a stamp");
    }

    /// The stamper refuses what `stamp` refuses, AT THE STAMP, and a refused stamp is never queued:
    /// a publish that hits a guard still fails at the write that caused it, before its commit.
    #[test]
    fn the_stamper_refuses_at_the_stamp_and_queues_nothing_it_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prov.log");
        let s: Arc<dyn ProvenanceStore> = Arc::new(DurableProvenanceStore::open(&path).unwrap());
        let guard = ProvenanceFlush::new(Arc::clone(&s));
        assert!(guard.stamper().stamp(rid(1, 0), ProvId(9)).is_err(), "an uninterned id was queued");
        assert!(guard.stamper().stamp(rid(1, 0), ProvId::NONE).is_err(), "NONE was queued");
        let before = s.sync_counts();
        guard.flush().unwrap();
        assert_eq!(s.sync_counts(), before, "a flush of refused stamps issued a sync");
        // Anti-vacuity: a legal stamp through the same stamper is queued and written.
        let id = s.intern(&run()).unwrap();
        let guard = ProvenanceFlush::new(Arc::clone(&s));
        guard.stamper().stamp(rid(1, 0), id).unwrap();
        guard.flush().unwrap();
        assert_eq!(DurableProvenanceStore::open(&path).unwrap().attribute(rid(1, 0)).unwrap(), id);
    }
}
