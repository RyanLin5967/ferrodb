use crate::{catalog::column::Value};

pub struct ColumnStats {
    pub distinct: usize,
    pub nulls: usize,
    pub min: Option<Value>,
    pub max: Option<Value>,
}

pub struct TableStats {
    pub row_count: usize,
    pub columns: Vec<ColumnStats>,
}
