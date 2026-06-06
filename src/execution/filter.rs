use crate::{catalog::schema::Schema, error::FerroError, execution::executor::Executor, parser::parser::Expr, storage::tuple::Tuple};
use crate::storage::heap_file_manager::RecordId;
use crate::execution::executor::evaluate;
use crate::catalog::column::Value;

pub struct Filter {
    pub child: Box<dyn Executor>,
    pub predicate: Expr,
    pub schema: Schema,
}

// operator that applies predicate. gets rows from child, emits ones where predicate is true
impl Executor for Filter {
    fn next(&mut self) -> Option<Result<(RecordId, Tuple), FerroError>> {
        loop {
            let (rid, tuple) = match self.child.next()? {
                Ok((r, t)) => (r, t),
                Err(e) => return Some(Err(e))
            };
            let values = match tuple.deserialize(&self.schema) {
                Ok(v) => v,
                Err(e) => return Some(Err(e))
            };
            match evaluate(&self.predicate, &values, &self.schema) {
                Ok(Value::Boolean(true)) => return Some(Ok((rid, tuple))),
                Ok(_) => continue,
                Err(e) => return Some(Err(e))
            }
        }
    }
}