use std::{collections::HashMap, hash::{Hash, Hasher}, iter::repeat, mem::discriminant};

use crate::{binder::binder::BoundExpr, catalog::column::Value, error::FerroError, execution::executor::{Executor, evaluate}, parser::parser::JoinType, storage::heap_file_manager::RecordId};

pub struct HashKey(pub Vec<Value>);

// A hash join's key equality has to be the SAME equality the `=` operator uses, or the answer to
// a query depends on which join algorithm the optimizer picked. This used to compare strictly by
// variant, which agreed with `=` only as long as `=` also refused to compare across Integer and
// Float. Once Value::cmp learned to compare those two numerically (R10), the two drifted apart:
// `ON i.k = f.k` over an INTEGER and a FLOAT column returned every matching row under a nested
// loop and ZERO rows under a hash join. So delegate to Value's own equality.
impl PartialEq for HashKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len() && self.0.iter().zip(&other.0).all(|(a, b)| a == b)
    }
}
impl Eq for HashKey {}

// Hash must follow Eq: equal keys hash equal, or they land in different buckets and never meet.
// Since Integer(5) and Float(5.0) are now equal, they cannot be separated by discriminant, and
// the two must hash through one canonical numeric form. Widening i32 -> f64 is lossless, and
// `to_bits` matches the `total_cmp` that Value::cmp uses: -0.0 and 0.0 stay distinct under both,
// and two NaNs agree under both.
impl Hash for HashKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        const NUMERIC: u8 = 0xF0;
        for v in &self.0 {
            match v {
                Value::Integer(i) => { NUMERIC.hash(state); (*i as f64).to_bits().hash(state) }
                Value::Float(f) => { NUMERIC.hash(state); f.to_bits().hash(state) }
                Value::Boolean(b) => { discriminant(v).hash(state); b.hash(state) }
                Value::Varchar(s) => { discriminant(v).hash(state); s.hash(state) }
                Value::Null => discriminant(v).hash(state),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::{binder::binder::Binder, buffer::buffer_pool::BufferPoolManager,
        catalog::catalog::Catalog, catalog::column::Value,
        execution::{executor::{run, evaluate}, session::Session},
        optimizer::{cost_model::{contains_hash_join, contains_nlj}, optimizer::{optimize, pushdown}},
        parser::{parser::Parser, scanner::Scanner},
        storage::disk_manager::DiskManager, wal::{log::WalManager, txn::TxnManager},
        binder::binder::BoundExpr, parser::scanner::TokenType,
        execution::executor::Outcome};
    use super::*;

    fn setup() -> (Catalog, Arc<BufferPoolManager>, Arc<TxnManager>, tempfile::TempDir) {
        let file = tempfile::tempfile().unwrap();
        let dir = tempdir().unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("wal.test")).unwrap());
        let txn = Arc::new(TxnManager::new(wal, bp.clone()));
        (catalog, bp, txn, dir)
    }

    fn exec(sql: &str, catalog: &mut Catalog, bp: Arc<BufferPoolManager>, txn: Arc<TxnManager>) -> Outcome {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut parser = Parser::new(tokens);
        let stmts = parser.parse();
        assert!(parser.errors.is_empty(), "parse errors: {:?}", parser.errors);
        let mut session = Session::new();
        let mut last = None;
        for stmt in stmts {
            last = Some(run(stmt, catalog, bp.clone(), txn.clone(), &mut session).unwrap());
        }
        last.unwrap()
    }

    // The equality the rest of the engine speaks: `compare` in executor.rs, which routes
    // through Value's PartialOrd. After the R10 fix this says Integer(5) == Float(5.0).
    fn engine_says_equal(a: Value, b: Value) -> bool {
        let e = BoundExpr::BinaryOp {
            left: Box::new(BoundExpr::Literal(a)),
            operator: TokenType::Equal,
            right: Box::new(BoundExpr::Literal(b)),
        };
        matches!(evaluate(&e, &[]).unwrap(), Value::Boolean(true))
    }

    // A hash join's key equality MUST agree with the `=` operator, or the same query returns
    // different rows depending on which join algorithm the optimizer happened to pick.
    #[test]
    fn hash_key_equality_agrees_with_the_equality_operator() {
        assert!(engine_says_equal(Value::Integer(5), Value::Float(5.0)),
            "precondition: the engine's = says 5 equals 5.0");
        assert!(
            HashKey(vec![Value::Integer(5)]) == HashKey(vec![Value::Float(5.0)]),
            "hash join disagrees with the = operator about 5 vs 5.0"
        );
    }

    // Eq without a matching Hash is a silently broken HashMap: equal keys that hash
    // differently land in different buckets and never meet.
    #[test]
    fn equal_hash_keys_hash_identically() {
        use std::collections::hash_map::DefaultHasher;
        let h = |k: &HashKey| { let mut s = DefaultHasher::new(); k.hash(&mut s); s.finish() };
        let int_key = HashKey(vec![Value::Integer(5)]);
        let float_key = HashKey(vec![Value::Float(5.0)]);
        assert!(int_key == float_key, "precondition: these keys are equal");
        assert_eq!(h(&int_key), h(&float_key), "equal keys must hash equal");
    }

    // End to end: an equi-join across an INTEGER and a FLOAT column, planned as a hash join.
    #[test]
    fn hash_join_matches_integer_key_against_float_key() {
        let (mut catalog, bp, txn, _dir) = setup();
        let mut sql = String::from("CREATE TABLE ints (k INTEGER NOT NULL, tag VARCHAR(8));");
        sql.push_str("CREATE TABLE floats (id INTEGER NOT NULL, k FLOAT, tag VARCHAR(8));");
        for i in 0..300 { sql.push_str(&format!("INSERT INTO ints VALUES ({}, 'i{}');", i, i)); }
        for i in 0..600 { sql.push_str(&format!("INSERT INTO floats VALUES ({}, {}.0, 'f{}');", i, i % 30, i)); }
        exec(&sql, &mut catalog, bp.clone(), txn.clone());
        catalog.analyze("ints").unwrap();
        catalog.analyze("floats").unwrap();

        let tokens = Scanner::new(
            "SELECT i.tag, f.tag FROM ints i JOIN floats f ON i.k = f.k;".chars().collect(),
            Vec::new()).scan_tokens().unwrap();
        let stmt = Parser::new(tokens).parse().remove(0);
        let logical = Binder::new(&catalog).bind(stmt).unwrap();
        let plan = optimize(pushdown(logical), &catalog).unwrap();
        assert!(contains_hash_join(&plan), "precondition: this query must plan as a hash join");

        let out = exec("SELECT i.tag, f.tag FROM ints i JOIN floats f ON i.k = f.k;",
            &mut catalog, bp.clone(), txn.clone());
        let n = match out { Outcome::Rows(rs) => rs.len(), _ => panic!("expected Outcome::Rows") };
        // keys 0..29 exist on both sides; floats has 20 rows per key, ints has 1.
        assert_eq!(n, 600, "hash join dropped rows the = operator says match");
    }

    // The invariant underneath the bug: an answer must not depend on the plan. The same query
    // over the same rows, once as a nested loop and once as a hash join, must agree.
    #[test]
    fn nested_loop_and_hash_join_agree_across_integer_and_float_keys() {
        let (mut catalog, bp, txn, _dir) = setup();
        let mut sql = String::from("CREATE TABLE ints (k INTEGER NOT NULL, tag VARCHAR(8));");
        sql.push_str("CREATE TABLE floats (id INTEGER NOT NULL, k FLOAT, tag VARCHAR(8));");
        for i in 0..300 { sql.push_str(&format!("INSERT INTO ints VALUES ({}, 'i{}');", i, i)); }
        for i in 0..600 { sql.push_str(&format!("INSERT INTO floats VALUES ({}, {}.0, 'f{}');", i, i % 30, i)); }
        exec(&sql, &mut catalog, bp.clone(), txn.clone());
        catalog.analyze("ints").unwrap();
        catalog.analyze("floats").unwrap();

        // `>= AND <=` is the same predicate as `=` for non-null numerics, but the optimizer only
        // recognises `=` as an equi-join, so this is how the two algorithms run the same question.
        let equi = "SELECT i.tag, f.tag FROM ints i JOIN floats f ON i.k = f.k;";
        let range = "SELECT i.tag, f.tag FROM ints i JOIN floats f ON i.k >= f.k AND i.k <= f.k;";

        let plan_of = |q: &str, catalog: &Catalog| {
            let tokens = Scanner::new(q.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let stmt = Parser::new(tokens).parse().remove(0);
            let logical = Binder::new(catalog).bind(stmt).unwrap();
            optimize(pushdown(logical), catalog).unwrap()
        };
        assert!(contains_hash_join(&plan_of(equi, &catalog)), "precondition: `=` should plan as a hash join");
        assert!(contains_nlj(&plan_of(range, &catalog)), "precondition: `>= AND <=` should stay a nested loop");

        let count = |q: &str, catalog: &mut Catalog| {
            match exec(q, catalog, bp.clone(), txn.clone()) {
                Outcome::Rows(rs) => rs.len(), _ => panic!("expected Outcome::Rows")
            }
        };
        let hash_rows = count(equi, &mut catalog);
        let nlj_rows = count(range, &mut catalog);
        assert_eq!(hash_rows, nlj_rows, "the same question returned different rows under two plans");
        assert_eq!(hash_rows, 600);
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