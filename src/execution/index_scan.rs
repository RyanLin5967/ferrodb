use std::sync::Arc;

use crate::wal::visibility::resolve_visibility;
use crate::{catalog::schema::Schema, error::FerroError, execution::executor::Executor, storage::range_scan::RangeScanner, wal::txn::ReadView};
use crate::storage::heap_file_manager::HeapFileManager;
use crate::catalog::column::Value;
use crate::storage::heap_file_manager::RecordId;

pub struct IndexScan {
    pub heap: HeapFileManager,
    pub scanner: RangeScanner<Value, RecordId>,
    pub schema: Schema,
    pub view: Arc<ReadView>,
    pub tt_heap: HeapFileManager,
}

impl Executor for IndexScan {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        loop {
            let (_key, rid) = match self.scanner.next()? {
                Ok((k ,v)) => (k,v),
                Err(e) => return Some(Err(e))
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
            return Some(vt.deserialize(&self.schema).map(|vals| (rid, vals)));
        }
    }
}