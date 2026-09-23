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
    /// **D187 — drop an entry whose indexed value is NULL, because this scan is bounded.**
    ///
    /// `insert.rs` posts `(vals[col].clone(), vals[0].clone())` unconditionally, so a NULL indexed
    /// value gets an index entry like any other, and `type_rank` sorts it below everything. The
    /// upper-bound test below is `&sec > u`, and `Null > Integer(5)` is **false**, so the entry is
    /// never "past" the bound and this scan yields it — a row SQL excludes, because `NULL < 5` is
    /// UNKNOWN.
    ///
    /// See [`crate::execution::index_scan::IndexScan::skip_nulls`] for why this is a flag and not
    /// an unconditional skip; the primary scan carries the identical field for the identical
    /// reason, and the rule is stated once, there.
    ///
    /// ⚠ The check sits BEFORE the bound arms, not inside one, and the reason is worth stating
    /// because it is not symmetry — it is the opposite.
    ///
    /// **The two arms depend on `type_rank` in OPPOSITE DIRECTIONS.** The upper arm is EXPOSED
    /// because `Null > u` is false, so a NULL is never "past" the bound. A lower bound is PROTECTED
    /// by the same fact wearing the other sign: the tree descent seeks the first key `>= lower` and
    /// NULL sorts beneath it, and D179's `sec_lower` arm reaches the same outcome because
    /// `Null <= l` is true. D179 chose that `<=` for an unrelated stated reason and it covers NULL
    /// by accident.
    ///
    /// ⇒ **So anyone reordering `type_rank` breaks exactly ONE of the two, and nothing says which.**
    /// A NULL that stopped sorting below every literal would leave the lower arm silently admitting
    /// NULLs while the upper arm kept working. Putting the test ahead of both arms is what makes
    /// that reordering safe: neither arm has to know about NULL, this one check does, and a third
    /// arm added later inherits it. (Verified against d179-sec-index by reading its arm, and
    /// confirmed independently by that lane.)
    pub skip_nulls: bool,
    /// D181 — index entries pulled, flushed once in `Drop`. Not `pub`: see `IndexScan::examined`.
    examined: u64,
}

impl SecondaryIndexScan {
    /// The only way to build one, so `examined` cannot start at anything but zero.
    ///
    /// `skip_nulls` is a PARAMETER and not a post-construction assignment on purpose: this
    /// doc says `new` is the only way to build one, and a field set afterwards would weaken
    /// exactly that invariant for the flag that decides whether NULL rows are yielded.
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
        skip_nulls: bool,
    ) -> Self {
        Self { heap, scanner, primary_index, schema, sec_lower, sec_upper, view, tt_heap, col_index, skip_nulls, examined: 0 }
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
            //
            // ⛔ THIS INCREMENT MUST STAY ABOVE THE NULL SKIP BELOW. NEVER MOVE IT UNDER.
            //
            // `examined` is an entries-PULLED count. Put it below the skip and it under-counts by
            // exactly the number of NULL entries dropped: the same work reported as a smaller
            // number — a fabricated improvement, in the direction the author is hoping for, with no
            // test failing. D181 and D187 both insert at this line and NEITHER IS WRONG ALONE; the
            // defect exists only in the resolution. Found by a trial merge before either landed.
            //
            // ✅ The check that catches a wrong resolution: D181's arms must still read 2 vs 800 at
            // n=800 and 2 vs 1600 at n=1600. An arm that IMPROVES here is the symptom, not the goal.
            self.examined += 1;
            // D187 — see `skip_nulls`. Ahead of the bound arms, and `continue` rather than
            // `return None`: NULL entries are a PREFIX of the tree, so stopping here would truncate
            // the scan before it reached a single real row.
            if self.skip_nulls && matches!(sec, Value::Null) {
                continue;
            }
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
