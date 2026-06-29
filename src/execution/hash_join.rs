use std::{collections::HashMap, hash::{Hash, Hasher}, iter::repeat, mem::discriminant};

use crate::{binder::binder::BoundExpr, catalog::column::Value, error::FerroError, execution::executor::{Executor, evaluate}, parser::parser::JoinType, storage::heap_file_manager::RecordId};

pub struct HashKey(pub Vec<Value>);

impl PartialEq for HashKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len() && self.0.iter().zip(&other.0).all(|(a, b)| strict_eq(a, b))
    }
}
impl Eq for HashKey {}
impl Hash for HashKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for v in &self.0 {
            discriminant(v).hash(state);
            match v {
                Value::Integer(i) => i.hash(state),
                Value::Float(f) => f.to_bits().hash(state),
                Value::Boolean(b) => b.hash(state),
                Value::Varchar(s) => s.hash(state),
                Value::Null => {}
            }
        }
    }
}

fn strict_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x == y,
        (Value::Varchar(x), Value::Varchar(y)) => x == y,
        (Value::Boolean(x), Value::Boolean(y)) => x == y,
        (Value::Null, Value::Null) => true,
        _ => false
    }
}

fn make_key(vals: &[Value], cols: &[usize]) -> Option<HashKey> {
    let mut key: Vec<Value> = Vec::with_capacity(cols.len());
    for &c in cols {
        let v = vals.get(c)?;
        if matches!(v, Value::Null) {
            return None;
        }
        key.push(v.clone());
    }
    Some(HashKey(key))
}

pub struct HashJoin {
    pub left: Box<dyn Executor>,
    pub right: Box<dyn Executor>,
    pub on: BoundExpr,
    pub right_keys: Vec<usize>,
    pub left_keys: Vec<usize>,
    pub join_type: JoinType,
    pub right_width: usize,
    pub table: Option<HashMap<HashKey, Vec<(RecordId, Vec<Value>)>>>,
    pub cur_left: Option<(RecordId, Vec<Value>)>,
    pub cur_bucket: Vec<(RecordId, Vec<Value>)>,
    pub bucket_idx: usize,
    pub left_matched: bool,
}

impl HashJoin {
    pub fn new(left: Box<dyn Executor>, right: Box<dyn Executor>, on: BoundExpr, join_type: JoinType, left_keys: Vec<usize>, right_keys: Vec<usize>, right_width: usize) -> Self {
        HashJoin { left, right, on, right_keys, left_keys, join_type, right_width, table: None, cur_left: None, cur_bucket: Vec::new(), bucket_idx: 0, left_matched: false }
    }   
}

impl Executor for HashJoin {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>> {
        if self.table.is_none() {
            let mut table: HashMap<HashKey, Vec<(RecordId, Vec<Value>)>> = HashMap::new();
            while let Some(r) = self.right.next() {
                let row = match r {
                    Ok(r) => r,
                    Err(e) => return Some(Err(e))
                };
                if let Some(key) = make_key(&row.1, &self.right_keys) {
                    table.entry(key).or_default().push(row);
                }
            }
            self.table = Some(table);
        }
        
        loop {
            if self.cur_left.is_none() {
                match self.left.next() {
                    Some(Ok(row)) => {
                        self.cur_bucket = match make_key(&row.1, &self.left_keys) {
                            Some(k) => self.table.as_ref().unwrap().get(&k).cloned().unwrap_or_default(),
                            None => Vec::new()
                        };
                        self.cur_left = Some(row);
                        self.bucket_idx = 0;
                        self.left_matched = false;
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => return None
                }
            }
            let (left_rid, left_vals) = self.cur_left.clone().unwrap();
            while self.bucket_idx < self.cur_bucket.len() {
                let right_vals = self.cur_bucket[self.bucket_idx].1.clone();
                self.bucket_idx += 1;
                let mut combined = left_vals.clone();
                combined.extend(right_vals);
                match evaluate(&self.on, &combined) {
                    Ok(Value::Boolean(true)) => {
                        self.left_matched = true;
                        return Some(Ok((left_rid, combined)))
                    }
                    Ok(_) => continue,
                    Err(e) => return Some(Err(e))
                }
            }
            self.cur_left = None;
            if matches!(self.join_type, JoinType::Left) && !self.left_matched {
                let mut combined = left_vals;
                combined.extend(repeat(Value::Null).take(self.right_width));
                return Some(Ok((left_rid, combined)));
            }
        }
    }
}