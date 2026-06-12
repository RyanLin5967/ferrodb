use crate::binder::binder::BoundExpr;
use crate::execution::executor::{Executor, evaluate};
use crate::error::FerroError;
use crate::storage::heap_file_manager::RecordId;
use crate::catalog::column::Value;

pub struct Projection {
    pub child: Box<dyn Executor>,
    pub exprs: Vec<BoundExpr>,
}

impl Executor for Projection {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        let (rid, values) = match self.child.next()? {
            Ok((r, v)) => (r, v),
            Err(e) => return Some(Err(e))
        };
        let mut eval_values = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            eval_values.push( match evaluate(expr, &values) {
                Ok(v) => v,
                Err(e) => return Some(Err(e))
            })
        }
        Some(Ok((rid, eval_values)))
    }
}