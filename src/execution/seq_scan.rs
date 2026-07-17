use std::sync::Arc;

use crate::wal::txn::ReadView;
use crate::{error::FerroError, execution::executor::Executor, storage::heap_scanner::HeapScanner, wal::visibility::resolve_visibility};
use crate::storage::heap_file_manager::{HeapFileManager, RecordId};
use crate::catalog::column::Value;
use crate::catalog::schema::Schema;
pub struct SeqScan {
    pub scanner: HeapScanner,
    pub schema: Schema,
    pub view: Arc<ReadView>,
    pub tt_heap: HeapFileManager,
}

impl Executor for SeqScan {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        loop {
            let (rid, tuple) = match self.scanner.next()? {
                Ok((r, t)) => (r, t),
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