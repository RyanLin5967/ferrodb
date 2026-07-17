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
}

impl Executor for SecondaryIndexScan {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        loop {
            let (sec, pk) = match self.scanner.next()? {
                Ok(((sec, pk), ())) => (sec, pk),
                Err(e) => return Some(Err(e))
            };
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