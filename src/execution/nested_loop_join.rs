use crate::{binder::binder::BoundExpr, catalog::column::Value, error::FerroError, execution::executor::{Executor, evaluate}, parser::parser::JoinType, storage::heap_file_manager::RecordId};

pub struct NestedLoopJoin {
    pub left: Box<dyn Executor>,
    pub right: Box<dyn Executor>,
    pub on: BoundExpr,
    pub right_rows: Option<Vec<(RecordId, Vec<Value>)>>,
    pub join_type: JoinType,
    pub cur_left: Option<(RecordId, Vec<Value>)>,
    pub right_idx: usize,
    pub left_matched: bool, // for left/full 
    pub left_width: usize, // future null padding
    pub right_width: usize,
}

impl NestedLoopJoin {

    pub fn new(left: Box<dyn Executor>, right: Box<dyn Executor>, on: BoundExpr, join_type: JoinType, left_width: usize, right_width: usize) -> Self {
        Self { left, right, on, right_rows: None, join_type, cur_left: None, right_idx: 0, left_matched: false, left_width, right_width }
    }

    pub fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>>{
        if self.right_rows.is_none() {
            let mut right_rows: Vec<(RecordId, Vec<Value>)> = Vec::new();
            while let Some(r) = self.right.next() {
                match r {
                    Ok(row) => right_rows.push(row),
                    Err(e) => return Some(Err(e)),
                }
            }
        }
        loop {
            if self.cur_left.is_none() {
                match self.left.next() {
                    Some(Ok(row)) => {
                        self.cur_left = Some(row);
                        self.right_idx = 0;
                        self.left_matched = false;
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => return None
                }
            }
            let (left_rid, left_vals) = self.cur_left.clone().unwrap();
            let right_len = self.right_rows.as_ref().unwrap().len();
            while self.right_idx < right_len {
                let right_vals = self.right_rows.as_ref().unwrap()[self.right_idx].1.clone();
                self.right_idx += 1;

                let mut combined = left_vals.clone();
                combined.extend(right_vals);
                match evaluate(&self.on, &combined) {
                    Ok(Value::Boolean(true)) => {
                        self.left_matched = true;
                        return Some(Ok((left_rid, combined)));
                    }
                    Ok(_) => continue,
                    Err(e) => return Some(Err(e))
                }
            }
            
            self.cur_left = None;
            if matches!(self.join_type, JoinType::Left) && !self.left_matched {
                let mut combined = left_vals;
                combined.extend(std::iter::repeat(Value::Null).take(self.right_width));
                return Some(Ok((left_rid, combined)));
            }
        }
    }
}