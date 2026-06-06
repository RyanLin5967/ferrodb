use crate::{catalog::schema::Schema, error::FerroError, execution::{executor::Executor}, storage::range_scan::RangeScanner};
use crate::storage::heap_file_manager::HeapFileManager;
use crate::catalog::column::Value;
use crate::storage::heap_file_manager::RecordId;

pub struct IndexScan {
    pub heap: HeapFileManager,
    pub scanner: RangeScanner<Value, RecordId>,
    pub schema: Schema
}

impl Executor for IndexScan {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        let (_key, rid) = match self.scanner.next()? {
            Ok((k ,v)) => (k,v),
            Err(e) => return Some(Err(e))
        };
        let tuple = match self.heap.read(rid) {
            Ok(t) => t,
            Err(e) => return Some(Err(e))
        };
        let vals = match tuple.deserialize(&self.schema) {
            Ok(v) => v,
            Err(e) => return Some(Err(e))
        };
        Some(Ok((rid, vals)))
    }
}