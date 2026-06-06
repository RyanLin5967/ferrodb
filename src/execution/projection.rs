use crate::catalog::schema::Schema;
use crate::execution::executor::{Executor, evaluate};
use crate::error::FerroError;
use crate::storage::heap_file_manager::RecordId;
use crate::catalog::column::Value;
use crate::parser::parser::Expr;

pub struct Projection {
    pub child: Box<dyn Executor>,
    pub columns: Vec<Expr>,
    pub schema: Schema
}

impl Executor for Projection {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        let (rid, values) = match self.child.next()? {
            Ok((r, v)) => (r, v),
            Err(e) => return Some(Err(e))
        };
        let mut eval_values = Vec::with_capacity(self.columns.len());
        for expr in &self.columns {
            eval_values.push( match evaluate(expr, &values, &self.schema) {
                Ok(v) => v,
                Err(e) => return Some(Err(e))
            })
        }
        Some(Ok((rid, eval_values)))
    }
}