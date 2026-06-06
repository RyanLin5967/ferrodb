use crate::{error::FerroError, execution::executor::Executor, storage::heap_file_manager::HeapFileManager};
use crate::catalog::schema::Schema;
use crate::storage::index::BPlusTreeManager;
use crate::catalog::column::Value;
use crate::storage::heap_file_manager::RecordId;
use crate::execution::index_handle::IndexHandle;

pub struct Delete {
    pub child: Box<dyn Executor>,
    pub heap: HeapFileManager,
    pub schema: Schema,
    pub primary_index: BPlusTreeManager<Value, RecordId>,
    pub secondary_indexes: Vec<IndexHandle>,
}

impl Delete {
    pub fn execute(&mut self) -> Result<usize, FerroError> {
        let mut res = Vec::new();
        let mut count = 0;
        loop {
            let (rid, values) = match self.child.next() {
                Some(Ok((r, t))) => (r, t),
                Some(Err(e)) => return Err(e),
                None => break
            };
            res.push((rid, values));
        }
        for (rid, values) in res {
            self.heap.delete(rid)?;
            self.primary_index.delete(&values[0])?;
            for handle in &self.secondary_indexes {
                handle.tree.delete(&(values[handle.col_index].clone(), values[0].clone()))?;
            }
            count += 1;
        }
        
        Ok(count)
    }
}