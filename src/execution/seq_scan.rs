use crate::{error::FerroError, execution::executor::Executor, storage::{heap_scanner::HeapScanner}};
use crate::storage::heap_file_manager::RecordId;
use crate::catalog::column::Value;
use crate::catalog::schema::Schema;
pub struct SeqScan {
    pub scanner: HeapScanner,
    pub schema: Schema,
}

impl Executor for SeqScan {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        let (rid, tuple) = match self.scanner.next()? {
            Ok((r, t)) => (r, t),
            Err(e) => return Some(Err(e))
        };
        let values = match tuple.deserialize(&self.schema) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        Some(Ok((rid, values)))
    }
}