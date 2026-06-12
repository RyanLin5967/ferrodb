use crate::binder::binder::BoundExpr;
use crate::{error::FerroError, execution::executor::Executor};
use crate::storage::heap_file_manager::RecordId;
use crate::execution::executor::evaluate;
use crate::catalog::column::Value;

pub struct Filter {
    pub child: Box<dyn Executor>,
    pub predicate: BoundExpr,
}

// operator that applies predicate. gets rows from child, emits ones where predicate is true
impl Executor for Filter {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        loop {
            let (rid, values) = match self.child.next()? {
                Ok((r, t)) => (r, t),
                Err(e) => return Some(Err(e))
            };
            match evaluate(&self.predicate, &values) {
                Ok(Value::Boolean(true)) => return Some(Ok((rid, values))),
                Ok(_) => continue,
                Err(e) => return Some(Err(e))
            }
        }
    }
}