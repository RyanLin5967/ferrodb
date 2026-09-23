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
    /// ⚠ The check sits BEFORE the bound arms, not inside one. The two directions of this
    /// asymmetry are not symmetric: the upper arm leaks NULLs (`Null > u` is false), while a lower
    /// bound excludes them for free — the tree descent seeks to the first key `>= lower` and NULL
    /// sorts beneath it, and D179's `sec_lower` arm reaches the same outcome because `Null <= l` is
    /// true. Putting the test ahead of both means neither arm has to know about NULL, and a third
    /// arm added later inherits it.
    pub skip_nulls: bool,
}

impl Executor for SecondaryIndexScan {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        loop {
            let (sec, pk) = match self.scanner.next()? {
                Ok(((sec, pk), ())) => (sec, pk),
                Err(e) => return Some(Err(e))
            };
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