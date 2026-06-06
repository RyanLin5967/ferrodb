use crate::{error::FerroError, execution::executor::Executor, storage::{heap_scanner::HeapScanner, tuple::Tuple}};
use crate::storage::heap_file_manager::RecordId;
pub struct SeqScan {
    pub scanner: HeapScanner
}

impl Executor for SeqScan {
    fn next(&mut self) -> Option<Result<(RecordId, Tuple), FerroError>> {
        self.scanner.next()
    }
}