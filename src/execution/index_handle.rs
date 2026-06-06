use crate::storage::index::BPlusTreeManager;
use crate::catalog::column::Value;
pub struct IndexHandle {
    pub col_index: usize,
    pub tree: BPlusTreeManager<(Value, Value), ()>,
}