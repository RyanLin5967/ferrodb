use crate::error::FerroError;
use crate::execution::executor::evaluate;
use crate::storage::tuple::Tuple;
use crate::{catalog::schema::Schema, execution::executor::Executor, parser::parser::Expr, storage::heap_file_manager::HeapFileManager};
use crate::storage::index::BPlusTreeManager;
use crate::storage::heap_file_manager::RecordId;
use crate::catalog::column::Value;
use crate::execution::index_handle::IndexHandle;

pub struct Update {
    pub child: Box<dyn Executor>,
    pub schema: Schema,
    pub assignments: Vec<(usize, Expr)>, // col idx -> new value expr
    pub heap: HeapFileManager,
    pub primary_index: BPlusTreeManager<Value, RecordId>,
    pub secondary_indexes: Vec<IndexHandle>,
}

impl Update {
    pub fn execute(&mut self) -> Result<usize, FerroError>{
        if self.assignments.iter().any(|(col, _)| *col == 0) {
            return Err(FerroError::Parse("can't update primary key".into()));
        }
        let mut res = Vec::new();
        loop {
            let (rid, values) = match self.child.next() {
                Some(Ok((r, t))) => (r, t),
                Some(Err(e)) => return Err(e),
                None => break
            };
            res.push((rid, values));
        }
        let mut count = 0;
        for (rid, old_values) in res {
            let mut new_values = old_values.clone();
            for (col_idx, expr) in &self.assignments {
                new_values[*col_idx] = evaluate(expr, &old_values, &self.schema)?;
            }
            
            for (i, col) in self.schema.columns.iter().enumerate() {
                if !col.nullable && matches!(new_values[i], Value::Null) {
                    return Err(FerroError::Contraint(format!("column {} can't be null", col.name)))
                }
            }
            let pk = old_values[0].clone();
            let tuple = Tuple::serialize(&new_values, &self.schema)?;
            let new_rid = self.heap.update(rid, tuple)?;
            if new_rid != rid {
                self.primary_index.delete(&pk)?;
                self.primary_index.insert(pk.clone(), new_rid)?;
            }

            for handle in &self.secondary_indexes {
                let old_v = &old_values[handle.col_index];
                let new_v = &new_values[handle.col_index];
                if old_v != new_v {
                    handle.tree.delete(&(old_v.clone(), pk.clone()))?;
                    handle.tree.insert((new_v.clone(), pk.clone()), ())?;
                }
            }
            count += 1;
        }
        Ok(count)
    }
}