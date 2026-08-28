use crate::storage::index::BPlusTreeManager;
use crate::catalog::column::Value;
pub struct IndexHandle {
    pub col_index: usize,
    pub tree: BPlusTreeManager<(Value, Value), ()>,
}

/// A full-text index open for writing — B8.
///
/// The tree is the *same type* as `IndexHandle`'s, because a posting list is a secondary index with
/// a token in the first component instead of the whole column value. This is a distinct struct all
/// the same: the two are maintained differently (one entry per value versus one per distinct token),
/// they live in different catalog lists, and their roots are written back by different functions. A
/// single `Vec<IndexHandle>` holding both would compile and would post whole column values as if
/// they were tokens.
pub struct FullTextHandle {
    pub col_index: usize,
    /// The column's name, so a split root can be written back to the right catalog record.
    pub column_name: String,
    pub tree: BPlusTreeManager<(Value, Value), ()>,
}
