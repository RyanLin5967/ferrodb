use crate::error::FerroError;
use crate::execution::executor::evaluate;
use crate::storage::tuple::Tuple;
use crate::{parser::parser::Expr};
use crate::storage::heap_file_manager::HeapFileManager;
use crate::catalog::schema::Schema;
use crate::catalog::column::Value;
use crate::storage::index::BPlusTreeManager;
use crate::storage::heap_file_manager::RecordId;
use crate::execution::index_handle::IndexHandle;

pub struct Insert {
    pub values: Vec<Expr>,
    pub heap: HeapFileManager,
    pub schema: Schema,
    pub primary_index: BPlusTreeManager<Value, RecordId>,
    pub secondary_indexes: Vec<IndexHandle>,
}

impl Insert {
    pub fn execute(&mut self) -> Result<usize, FerroError>{
        let mut vals = Vec::with_capacity(self.values.len());
        for expr in &self.values {
            vals.push(evaluate(expr, &[], &self.schema)?);
        }
        if vals.len() != self.schema.columns.len() {
            return Err(FerroError::Contraint("value count != column count".into()))
        }
        for (i, col) in self.schema.columns.iter().enumerate() {
            if !col.nullable && matches!(vals[i], Value::Null) {
                return Err(FerroError::Contraint(format!("column {} can't be null", col.name)))
            }
        }
        if self.primary_index.search(&vals[0])?.is_some() {
            return Err(FerroError::Contraint("duplicate primary key".into()))
        }
        let tuple = Tuple::serialize(&vals, &self.schema)?;
        let rid = self.heap.insert(tuple)?;
        self.primary_index.insert(vals[0].clone(), rid)?;
        for sec_idx in &self.secondary_indexes {
            sec_idx.tree.insert((vals[sec_idx.col_index].clone(), vals[0].clone()), ())?;
        }
        Ok(1)
    }
}
