use std::sync::atomic::Ordering as AtomicOrdering;
use std::{ops::Bound, sync::Arc};

use crate::{catalog::{column::Value, schema::Schema}, error::FerroError, execution::{executor::Executor, index_scan::{INDEX_SCANS, INDEX_SCAN_ENTRIES}}, storage::{heap_file_manager::{HeapFileManager, RecordId}, index::BPlusTreeManager, range_scan::RangeScanner}, wal::{txn::ReadView, visibility::resolve_visibility}};

pub struct SecondaryIndexScan {
    pub heap: HeapFileManager,
    pub scanner: RangeScanner<(Value, Value), ()>,
    pub primary_index: BPlusTreeManager<Value, RecordId>,
    pub schema: Schema,
    /// **D179 — the lower bound the caller asked for, in the COLUMN's value space.**
    ///
    /// Not the scanner's start key, which lives in the tree's `(value, pk)` key space and is
    /// computed by `optimizer::secondary_scan_start`. The two differ for exactly one bound, and
    /// that difference is the whole of D179 — see [`SecondaryIndexScan::next`].
    pub sec_lower: Bound<Value>,
    pub sec_upper: Bound<Value>,
    pub view: Arc<ReadView>,
    pub tt_heap: HeapFileManager,
    pub col_index: usize,
    /// D181 — index entries pulled, flushed once in `Drop`. Not `pub`: see `IndexScan::examined`.
    examined: u64,
}

impl SecondaryIndexScan {
    /// The only way to build one, so `examined` cannot start at anything but zero.
    ///
    /// `sec_lower`/`sec_upper` are the bounds in the COLUMN's value space; `scanner` must already
    /// have been opened at `optimizer::secondary_scan_start(&sec_lower)`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        heap: HeapFileManager,
        scanner: RangeScanner<(Value, Value), ()>,
        primary_index: BPlusTreeManager<Value, RecordId>,
        schema: Schema,
        sec_lower: Bound<Value>,
        sec_upper: Bound<Value>,
        view: Arc<ReadView>,
        tt_heap: HeapFileManager,
        col_index: usize,
    ) -> Self {
        Self { heap, scanner, primary_index, schema, sec_lower, sec_upper, view, tt_heap, col_index, examined: 0 }
    }
}

impl Drop for SecondaryIndexScan {
    fn drop(&mut self) {
        // D179/D181 — the single flush. A secondary scan is an index scan and now says so; see
        // `index_scan::INDEX_SCANS`.
        INDEX_SCANS.fetch_add(1, AtomicOrdering::Relaxed);
        INDEX_SCAN_ENTRIES.fetch_add(self.examined, AtomicOrdering::Relaxed);
    }
}

impl Executor for SecondaryIndexScan {
    /// # D179 — why a strict lower bound is enforced HERE and not by the scanner's start key
    ///
    /// A secondary tree is keyed `(value, pk)`. `Bound::Included(v)` has an exact start key,
    /// `(v, Null)`: `Null` sorts below every pk (`Value`'s type ranks put it at 0, and column 0 is
    /// `NOT NULL`), so that is the first key whose value is `v`. A **strictly excluded** `v` has no
    /// such key. It would have to start just past the LAST key with value `v`, and there is no
    /// maximum pk to write down — the nearest expressible start key is `(v, Null)`, which is the
    /// first key `> v` was asked to exclude, not the last.
    ///
    /// So the start key cannot carry the exclusion and something after it must. That something is
    /// the skip below: open at `(v, Null)` anyway and DISCARD the leading run of entries whose
    /// value is still `v`. It is the exact mirror of the `sec_upper` check three lines above,
    /// which has always had to live here for the same reason — an upper bound in value space is not
    /// a key in `(value, pk)` space either.
    ///
    /// The run is bounded by the number of rows sharing `v`, and every entry in it is one this
    /// executor reads and drops. That is the cost of the mechanism and it is real: `> v` on a
    /// column where a million rows share `v` walks a million index entries before returning
    /// anything. It is still bounded by ONE value's duplicates where the alternative — what this
    /// engine did before D179 — was a sequential scan of the whole table.
    ///
    /// `continue`, never `return None`: the run is a prefix to skip, not the end of the scan.
    /// Returning would have made `WHERE sec > v` answer the empty set whenever any row had
    /// `sec == v`, which is the failure mode this comment exists to stop someone reintroducing.
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        loop {
            let (sec, pk) = match self.scanner.next()? {
                Ok(((sec, pk), ())) => (sec, pk),
                Err(e) => return Some(Err(e))
            };
            // D181 — counted where the tree yielded it, before any check can discard it.
            self.examined += 1;
            let past = match &self.sec_upper {
                Bound::Included(u) => &sec > u,
                Bound::Excluded(u) => &sec >= u,
                Bound::Unbounded => false
            };
            if past { return None }
            // D179 — the lower-bound mirror. `<=` rather than `==` because it costs nothing and
            // does not depend on the start key being exactly right: anything at or below a strictly
            // excluded lower bound is out of range however the scan came to be positioned there.
            let before = match &self.sec_lower {
                Bound::Excluded(l) => &sec <= l,
                Bound::Included(_) | Bound::Unbounded => false
            };
            if before { continue }
            let rid = match self.primary_index.search(&pk) {
                Ok(Some(r)) => r,
                Err(e) => return Some(Err(e)),
                Ok(None) => continue
            };
            let tuple = match self.heap.read(rid) {
                Ok(t) => t,
                Err(e) => return Some(Err(e))
            };
            let vt = match resolve_visibility(&self.view, &self.tt_heap, tuple) {
                Ok(Some(v)) => v,
                Ok(None) => continue,
                Err(e) => return Some(Err(e))
            };
            let vals = match vt.deserialize(&self.schema) {
                Ok(v) => v,
                Err(e) => return Some(Err(e))
            };
            if vals[self.col_index] != sec {
                continue;
            }
            return Some(Ok((rid, vals)))
        }
    }
}
