use std::{ops::Bound, sync::Arc};

use crate::{catalog::{column::Value, schema::Schema}, error::FerroError, execution::executor::Executor, storage::{heap_file_manager::{HeapFileManager, RecordId}, index::BPlusTreeManager, range_scan::RangeScanner}, wal::{txn::ReadView, visibility::resolve_visibility}};

pub struct SecondaryIndexScan {
    pub heap: HeapFileManager,
    pub scanner: RangeScanner<(Value, Value), ()>,
    pub primary_index: BPlusTreeManager<Value, RecordId>,
    pub schema: Schema,
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
}

impl Executor for SecondaryIndexScan {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        loop {
            let (sec, pk) = match self.scanner.next()? {
                Ok(((sec, pk), ())) => (sec, pk),
                Err(e) => return Some(Err(e))
            };
            // ⛔ ANY PER-ENTRY COUNTER MUST INCREMENT ABOVE THIS SKIP, NEVER BELOW IT.
            //
            // D181 adds `self.examined += 1` here, documented as "counted where the tree yielded
            // it, before any check can discard it" — an entries-PULLED count. Put it below this
            // skip and it silently under-counts by exactly the number of NULL entries dropped: the
            // same work reported as a smaller number, i.e. a fabricated improvement, in the
            // direction the author is hoping for, with no test failing. Found in a trial merge
            // against d179-sec-index, where both branches insert at this exact line and NEITHER is
            // wrong alone — the defect exists only in the resolution.
            //
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