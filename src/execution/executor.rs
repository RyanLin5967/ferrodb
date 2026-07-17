use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::binder::binder::BoundExpr;
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::Value;
use crate::catalog::schema::Schema;
use crate::execution::index_handle::IndexHandle;
use crate::execution::session::Session;
use crate::parser::parser::{Stmt};
use crate::planner::plan::{Plan, explain, plan};
use crate::storage::index::BPlusTreeManager;
use crate::wal::txn::{ReadView, TxnManager};
use crate::{error::FerroError};
use crate::storage::heap_file_manager::RecordId;
use crate::parser::scanner::TokenType;

pub trait Executor {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>>;
}

pub trait Modify {
    fn execute(&mut self, catalog: &mut Catalog) -> Result<usize, FerroError>;
}

pub enum Outcome {
    Rows(Vec<Vec<Value>>),
    Affected(usize),
    Explain(String),
    Ok,
}

pub fn run(stmt: Stmt, catalog: &mut Catalog, bp: Arc<BufferPoolManager>, txn: Arc<TxnManager>, session: &mut Session) -> Result<Outcome, FerroError> {
    match stmt {
        Stmt::Begin => {
            if session.current.is_some() {
                return Err(FerroError::Txn("txn already started".into()))
            }
            session.current = Some(txn.begin()?);
            Ok(Outcome::Ok)
        }
        Stmt::Commit => match session.current.take() {
            Some(id) => {
                txn.commit(id)?;
                Ok(Outcome::Ok)
            }
            None => Err(FerroError::Txn("not in active txn".into()))
        }
        Stmt::Rollback => match session.current.take() {
            Some(id) => {
                txn.abort(id)?;
                Ok(Outcome::Ok)
            }
            None => Err(FerroError::Txn("not in active txn".into()))
        }
        Stmt::CreateIndex { table, column_name , ..} => {
            if session.current.is_some() {
                return Err(FerroError::Txn("DDL not allowed in txn".into()))
            }
            catalog.create_index(&table, &column_name)?;
            txn.checkpoint()?;
            return Ok(Outcome::Ok)
        }
        Stmt::CreateTable { table, columns } => {
            if session.current.is_some() {
                return Err(FerroError::Txn("DDL not allowed in txn".into()))
            }
            catalog.create_table(table, Schema{columns})?;
            txn.checkpoint()?;
            return Ok(Outcome::Ok)
        }
        Stmt::Analyze { table } => {
            catalog.analyze(&table)?;
            return Ok(Outcome::Ok)
        }
        Stmt::Explain(s) => {
            let text = explain(*s, catalog)?;
            return Ok(Outcome::Explain(text));
        }
        dml => {
            if matches!(dml, Stmt::Select { .. }) {
                let view = Arc::new(match session.current {
                    Some(txn_id) => ReadView { snapshot: txn.snapshot_of(txn_id)?, txn_id},
                    None => ReadView { snapshot: txn.read_snapshot(), txn_id: 0 }
                });
                match plan(dml, catalog, bp.clone(), None, view)? {
                    Plan::Read(mut root) => {
                        let mut res = Vec::new();
                        loop {
                            let (_, values) = match root.next() {
                                Some(Ok((r, v))) => (r, v),
                                Some(Err(e)) => return Err(e),
                                None =>{break;}
                            };
                            res.push(values);
                        }
                        return Ok(Outcome::Rows(res))
                    }
                    Plan::Write(_) => unreachable!()
                }
            } else {
                let (txn_id, implicit) = match session.current {
                    Some(id) => (id, false),
                    None => (txn.begin()?, true)
                };
                let view = match txn.snapshot_of(txn_id) {
                    Ok(snapshot) => Arc::new(ReadView { snapshot, txn_id }),
                    Err(e) => {
                        txn.abort(txn_id)?;
                        session.current = None;
                        return Err(e)
                    }
                };
                let planned = match plan(dml, catalog, bp.clone(), Some((txn.clone(), txn_id)), view) {
                    Ok(p) => p,
                    Err(e) => {
                        txn.abort(txn_id)?;
                        session.current = None;
                        return Err(e);
                    }
                };
                match planned {
                    Plan::Write(mut op) => match op.execute(catalog) {
                        Ok(count) => {
                            if implicit { txn.commit(txn_id)? };
                            return Ok(Outcome::Affected(count))
                        }
                        Err(e) => {
                            txn.abort(txn_id)?;
                            session.current = None;
                            Err(e)
                        }
                    },
                    Plan::Read(_) => unreachable!()
                }
            }
        }
    }
}

pub fn sync_roots(table: &str, schema: &Schema, primary: &BPlusTreeManager<Value, RecordId>, secondaries: &[IndexHandle], catalog: &mut Catalog) -> Result<(), FerroError> {
    let cur_primary = primary.root_page_id.load(Ordering::Relaxed);
    let stored_primary = catalog.get_table(table).ok_or(FerroError::KeyNotFound)?.primary_index_root;
    if cur_primary != stored_primary {
        catalog.update_primary_root(table, cur_primary)?;
    }
    for handle in secondaries {
        let cur = handle.tree.root_page_id.load(Ordering::Relaxed);
        let col_name = schema.columns[handle.col_index].name.clone();
        let stored = catalog.get_table(table).and_then(|e| e.indexes.iter().find(|i| i.column_name == col_name).map(|i| i.root_page_id));
        if stored != Some(cur) {
            catalog.update_index_root(table, &col_name, cur)?;
        }
    }
    Ok(())
}
pub fn evaluate(expr: &BoundExpr, row: &[Value]) -> Result<Value, FerroError> {
    return match expr {
        BoundExpr::Literal(v) => Ok(v.clone()),
        BoundExpr::BinaryOp { left, operator, right } => {
            let l = evaluate(left, row)?;
            let r = evaluate(right ,row)?;

            match operator {
                TokenType::Plus | TokenType::Minus | TokenType::Star | TokenType::Slash => arithmetic(&l, &r, operator),
                TokenType::Equal | TokenType::BangEqual | TokenType::Less | TokenType::LessEqual
                | TokenType::Greater | TokenType::GreaterEqual => compare(&l, &r, operator),
                TokenType::And | TokenType::Or => logical(&l, &r, operator),
                _ => Err(FerroError::Parse("invalid binary op".into()))
            }
        }
        BoundExpr::UnaryOp { operator, right } => {
            let v = evaluate(right, row)?;
            match operator {
                TokenType::Minus => match v {
                    Value::Integer(i) => Ok(Value::Integer(-i)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(FerroError::Parse("unary minus non numeric".into()))
                },
                TokenType::Not => match v {
                    Value::Boolean(b) => Ok(Value::Boolean(!b)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(FerroError::Parse("not on non boolean".into()))
                },
                _ => Err(FerroError::Parse("invalid unary op".into()))
            }
        }
        BoundExpr::Column(idx) => row.get(*idx).cloned().ok_or_else(|| FerroError::Parse(format!("row missing column at {}", idx)))
    }
}

fn arithmetic(l: &Value, r: &Value, op: &TokenType) -> Result<Value, FerroError> {
    match (l, r) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Integer(a), Value::Integer(b)) => {
            let res = match op {
                TokenType::Plus => a + b,
                TokenType::Minus => a - b,
                TokenType::Star => a * b,
                TokenType::Slash => {
                    if *b == 0 {return Err(FerroError::Parse("div by 0".into()))}
                    a/b
                }
                _ => return Err(FerroError::Parse("invalid arithmetic op".into()))
            };
            Ok(Value::Integer(res))
        }
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(float_arith(*a, *b, *op)?)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(float_arith(*a as f64, *b, *op)?)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(float_arith(*a, *b as f64, *op)?)),
        _ => Err(FerroError::Parse("can't add non numbers".into()))
    }
}

fn float_arith(a: f64, b: f64, op: TokenType) -> Result<f64, FerroError> {
    Ok(match op {
        TokenType::Plus => a + b,
        TokenType::Minus => a-b,
        TokenType::Star => a * b,
        TokenType::Slash => {
            if b == 0.0 {return Err(FerroError::Parse("div by 0".into()));}
            a/b
        }
        _ => return Err(FerroError::Parse("invalid arithmetic op".into()))
    })
}

fn compare(l: &Value, r: &Value, op: &TokenType) -> Result<Value, FerroError> {
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        return Ok(Value::Null)
    }
    let res = match op {
        TokenType::Equal => l== r,
        TokenType::BangEqual => l != r,
        TokenType::Less => l < r,
        TokenType::LessEqual => l <= r,
        TokenType::Greater => l > r,
        TokenType::GreaterEqual => l >= r,
        _ => return Err(FerroError::Parse("invalid comparison op".into()))
    };
    Ok(Value::Boolean(res))
}

fn logical(l: &Value, r: &Value, op: &TokenType) -> Result<Value, FerroError> {
    let lb = as_bool_opt(l)?;
    let rb = as_bool_opt(r)?;
    let res = match op {
        TokenType::And => match (lb, rb) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None
        }
        TokenType::Or => match (lb, rb) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None
        }
        _ => return Err(FerroError::Parse("invalid logical op".into()))
    };
    Ok(res.map_or(Value::Null, Value::Boolean))
}

fn as_bool_opt(v: &Value) -> Result<Option<bool>, FerroError> {
    match v {
        Value::Boolean(b) => Ok(Some(*b)),
        Value::Null => Ok(None),
        _ => Err(FerroError::Parse("expected bool".into()))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::ops::Bound;
    use super::*;
    use crate::parser::scanner::Scanner;
    use crate::parser::parser::Parser;
    use crate::storage::disk_manager::DiskManager;
use crate::storage::heap_file_manager::HeapFileManager;
use crate::wal::log::WalManager;
    use tempfile::tempdir;

    fn parse_one(sql: &str) -> Result<Stmt, FerroError> {
        let chars: Vec<char> = sql.chars().collect();
        let tokens = Scanner::new(chars, Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(parser.errors.remove(0))
        }
        stmts.into_iter().next().ok_or(FerroError::SqlParseError("no statement found".into()))
    }

    fn exec(sql: &str, catalog: &mut Catalog, bp: Arc<BufferPoolManager>, txn: Arc<TxnManager>) -> Result<Outcome, FerroError> {
        let mut session = Session::new();
        run(parse_one(sql)?, catalog, bp, txn, &mut session)
    }

    fn setup() -> (Catalog, Arc<BufferPoolManager>, tempfile::TempDir, Arc<TxnManager>) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("exec.db");
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("test.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal, bp.clone()));
        (catalog, bp, dir, txn)
    } 

    fn seed() -> (Catalog, Arc<BufferPoolManager>, tempfile::TempDir, Arc<TxnManager>) {
        let (mut c, bp, dir, txn) = setup();
        exec("CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(50));", &mut c, bp.clone(), txn.clone()).unwrap();
        for s in [
            "INSERT INTO users VALUES (1, 'alice');",
            "INSERT INTO users VALUES (2, 'bob');",
            "INSERT INTO users VALUES (3, 'carol');",
        ] {
            exec(s, &mut c, bp.clone(), txn.clone()).unwrap();
        }
        (c, bp, dir, txn)
    }

    fn seed_join() -> (Catalog, Arc<BufferPoolManager>, tempfile::TempDir, Arc<TxnManager>) {
        let (mut c, bp, dir, txn) = setup();
        exec("CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(50));", &mut c, bp.clone(), txn.clone()).unwrap();
        exec("CREATE TABLE posts (id INTEGER NOT NULL, user_id INTEGER, title VARCHAR(50));", &mut c, bp.clone(), txn.clone()).unwrap();
        for s in [
            "INSERT INTO users VALUES (1, 'alice');",
            "INSERT INTO users VALUES (2, 'bob');",
            "INSERT INTO users VALUES (3, 'carol');",
            "INSERT INTO posts VALUES (10, 1, 'hi');",
            "INSERT INTO posts VALUES (11, 1, 'yo');",
            "INSERT INTO posts VALUES (12, 2, 'sup');",
            "INSERT INTO posts VALUES (13, 99, 'orphan');",
        ] {
            exec(s, &mut c, bp.clone(), txn.clone()).unwrap();
        }
        exec("ANALYZE users;", &mut c, bp.clone(), txn.clone()).unwrap();
        exec("ANALYZE posts;", &mut c, bp.clone(), txn.clone()).unwrap();
        (c, bp, dir, txn)
    }

    fn name_title(rs: &[Vec<Value>]) -> Vec<(String, Option<String>)> {
        let mut v: Vec<(String, Option<String>)> = rs.iter().map(|r| {
            let name = match &r[0] { Value::Varchar(s) => s.clone(), _ => panic!() };
            let title = match &r[1] {
                Value::Varchar(s) => Some(s.clone()),
                Value::Null => None,
                _ => panic!()
            };
            (name, title)
        }).collect();
        v.sort();
        v
    }

    fn two_names(rs: &[Vec<Value>]) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = rs.iter().map(|r| {
            let a = match &r[0] {Value::Varchar(s) => s.clone(), _ => panic!()};
            let b = match &r[1] {Value::Varchar(s) => s.clone(), _ => panic!()};
            (a, b)
        }).collect();
        v.sort();
        v
    }

    fn rows(out: Outcome) -> Vec<Vec<Value>> {
        match out {
            Outcome::Rows(r) => r,
            _ => panic!("expected rows")
        }
    }

    fn affected(out: Outcome) -> usize {
        match out {
            Outcome::Affected(a) => a,
            _ => panic!("expected affected")
        }
    }

    fn sorted_ids(rs: &[Vec<Value>]) -> Vec<i32> {
        let mut v: Vec<i32> = rs.iter().map(|r| match &r[0] {Value::Integer(i) => *i, _ => panic!()}).collect();
        v.sort();
        v
    }

    #[test]
    fn test_analyze_basic() {
        let (mut c, _bp, _d, _txn) = seed();
        c.analyze("users").unwrap();
        let stats = c.stats.get("users").unwrap();
        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.columns[0].distinct, 3);
        assert_eq!(stats.columns[0].nulls, 0);
        assert_eq!(stats.columns[0].min, Some(Value::Integer(1)));
        assert_eq!(stats.columns[0].max, Some(Value::Integer(3)));

        assert_eq!(stats.columns[1].distinct, 3);
        assert_eq!(stats.columns[1].nulls, 0);
        assert_eq!(stats.columns[1].min, Some(Value::Varchar("alice".into())));
        assert_eq!(stats.columns[1].max, Some(Value::Varchar("carol".into())));
    }

    #[test]
    fn test_analyze_nulls_duplicates() {
        let (mut c, bp, _d, txn) = setup();
        exec("CREATE TABLE t (id INTEGER NOT NULL, val INTEGER);", &mut c, bp.clone(), txn.clone()).unwrap();
        exec("INSERT INTO t VALUES (1, 10);", &mut c, bp.clone(), txn.clone()).unwrap();
        exec("INSERT INTO t VALUES (2, 10);", &mut c, bp.clone(), txn.clone()).unwrap();
        exec("INSERT INTO t VALUES (3, NULL);", &mut c, bp.clone(), txn).unwrap();
        c.analyze("t").unwrap();
        let stats = c.stats.get("t").unwrap();

        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.columns[1].distinct, 1);
        assert_eq!(stats.columns[1].nulls, 1);
        assert_eq!(stats.columns[1].min, Some(Value::Integer(10)));
        assert_eq!(stats.columns[1].max, Some(Value::Integer(10)));
    }

    #[test]
    fn test_analyze_empty_table() {
        let (mut c, bp, _d, txn) = setup();
        exec("CREATE TABLE a (id INTEGER NOT NULL);", &mut c, bp.clone(), txn.clone()).unwrap();
        c.analyze("a").unwrap();
        let stats = c.stats.get("a").unwrap();

        assert_eq!(stats.row_count, 0);
        assert_eq!(stats.columns[0].distinct, 0);
        assert_eq!(stats.columns[0].nulls, 0);
        assert_eq!(stats.columns[0].min, None);
        assert_eq!(stats.columns[0].max, None);
    }

    #[test]
    fn test_analyze_unknown_table_error() {
        let (mut c, _bp, _d, _txn) = setup();
        assert!(c.analyze("idk").is_err());
    }
    #[test]
    fn test_inner_join() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(name_title(&r), vec![("alice".into(), Some("hi".into())), ("alice".into(), Some("yo".into())), ("bob".into(), Some("sup".into()))]);
    }

    #[test]
    fn test_inner_join_with_keyword() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.user_id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn test_join_select_star() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT * FROM users u INNER JOIN posts p ON u.id = p.user_id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r.len(), 3);
        assert!(r.iter().all(|row| row.len() == 5));
        assert!(r.iter().any(|row| row == &vec![
            Value::Integer(1), Value::Varchar("alice".into()),
            Value::Integer(10), Value::Integer(1), Value::Varchar("hi".into())
        ]));
    }

    #[test]
    fn test_join_with_where() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.id = 1;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(name_title(&r), vec![("alice".into(), Some("hi".into())), ("alice".into(), Some("yo".into()))]);
    }

    #[test]
    fn test_join_no_match() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.id;", &mut c, bp.clone(), txn).unwrap());
        assert!(r.is_empty());
    }

    #[test]
    fn test_left_join() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u LEFT JOIN posts p ON u.id = p.user_id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(name_title(&r), vec![("alice".into(), Some("hi".into())), ("alice".into(), Some("yo".into())), ("bob".into(), Some("sup".into())), ("carol".into(), None)]);
    }
    
    #[test]
    fn test_left_outer_keyword() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u LEFT OUTER JOIN posts p ON u.id = p.user_id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r.len(), 4);
    }

    #[test]
    fn test_left_no_match() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT u.name, p.title FROM users u LEFT JOIN posts p ON u.id = p.id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(name_title(&r), vec![("alice".into(), None), ("bob".into(), None), ("carol".into(), None)]);
    }

    #[test]
    fn test_self_join() {
        let (mut c, bp, _d, txn) = seed_join();
        let r = rows(exec("SELECT a.name, b.name FROM users a JOIN users b ON a.id = b.id;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(two_names(&r), vec![("alice".into(), "alice".into()), ("bob".into(), "bob".into()), ("carol".into(), "carol".into())]);
    }

    #[test]
    fn test_unsupported_join_type_error() {
        let (mut c, bp, _d, txn) = seed_join();
        assert!(exec("SELECT u.name, p.title FROM users u RIGHT JOIN posts p ON u.id = p.user_id;", &mut c, bp.clone(), txn).is_err());
    }

    #[test]
    fn test_select_all() {
        let (mut c, bp, _d, txn) = seed();
        let r = rows(exec("SELECT * FROM users;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r.len(), 3);
        assert_eq!(sorted_ids(&r), vec![1,2,3]);
    }

    #[test]
    fn test_filter() {
        let (mut c, bp, _d, txn) = seed();
        let r = rows(exec("SELECT * FROM users WHERE id = 2;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0][0], Value::Integer(2));
        assert_eq!(r[0][1], Value::Varchar("bob".into()));
    }

    #[test]
    fn test_comparison_filter() {
        let (mut c, bp, _d, txn) = seed();
        let r = rows(exec("SELECT * FROM users WHERE id > 1;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(sorted_ids(&r), vec![2, 3]);
    }

    #[test]
    fn test_projection(){ 
        let (mut c, bp, _d, txn) = seed();
        let r = rows(exec("SELECT name FROM users WHERE id = 1;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].len(), 1);
        assert_eq!(r[0][0], Value::Varchar("alice".into()));
    }

    #[test]
    fn test_update_then_select() {
        let (mut c, bp, _d, txn) = seed();
        assert_eq!(affected(exec("UPDATE users SET name = 'ALICE' WHERE id = 1;", &mut c, bp.clone(), txn.clone()).unwrap()), 1);
        let r = rows(exec("SELECT name FROM users WHERE id = 1;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(r[0][0], Value::Varchar("ALICE".into()));
    }

    #[test]
    fn test_delete_then_select() {
        let (mut c, bp, _d, txn) = seed();
        assert_eq!(affected(exec("DELETE FROM users WHERE id = 2;", &mut c, bp.clone(), txn.clone()).unwrap()), 1);
        let r = rows(exec("SELECT * FROM users;", &mut c, bp.clone(), txn).unwrap());
        assert_eq!(sorted_ids(&r), vec![1,3]);
    }

    #[test]
    fn test_duplicate_primary_key_errors() {
        let (mut c, bp, _d, txn) = seed();
        assert!(exec("INSERT INTO users VALUES (1, 'dup');", &mut c, bp.clone(), txn).is_err());
    }

    #[test]
    fn not_null_violation_errors() {
        let (mut c, bp, _d, txn) = seed();
        assert!(exec("INSERT INTO users VALUES (NULL, 'x');", &mut c, bp.clone(), txn).is_err())
    }
 
    #[test]
    fn root_split_persists_across_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("reopen.db");
        let n = 1000;

        {
            let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
            let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
            let mut c = Catalog::create(bp.clone()).unwrap();
            let wal = Arc::new(WalManager::new(dir.path().join("reopen.wal")).unwrap());
            let txn = Arc::new(TxnManager::new(wal, bp.clone()));
            exec("CREATE TABLE nums (id INTEGER NOT NULL);", &mut c, bp.clone(), txn.clone()).unwrap();
            for i in 0..n {
                exec(&format!("INSERT INTO nums VALUES ({});", i), &mut c, bp.clone(), txn.clone()).unwrap();
            }
            bp.flush_all().unwrap();
        }

        {
            let file = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
            let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
            let c = Catalog::open(bp.clone(), 1).unwrap();
            let entry = c.get_table("nums").unwrap();
            let tree = BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp.clone());
            let all = tree.range_scan(Bound::Unbounded, Bound::Unbounded)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(all.len(), n as usize);
        }
    }

    #[test]
    fn test_arithmetic() {
        let int4 = Value::Integer(4);
        let int2 = Value::Integer(2);
        let float2 = Value::Float(2.0);
        let null = Value::Null;

        assert_eq!(arithmetic(&int4, &int2, &TokenType::Plus).unwrap(), Value::Integer(6));
        assert_eq!(arithmetic(&int4, &int2, &TokenType::Minus).unwrap(), Value::Integer(2));
        assert_eq!(arithmetic(&int4, &int2, &TokenType::Star).unwrap(), Value::Integer(8));
        assert_eq!(arithmetic(&int4, &int2, &TokenType::Slash).unwrap(), Value::Integer(2));    
        assert_eq!(arithmetic(&int4, &float2, &TokenType::Plus).unwrap(), Value::Float(6.0));
        assert_eq!(arithmetic(&float2, &int4, &TokenType::Star).unwrap(), Value::Float(8.0));
        assert_eq!(arithmetic(&int4, &null, &TokenType::Plus).unwrap(), Value::Null);
        assert!(arithmetic(&int4, &Value::Integer(0), &TokenType::Slash).is_err());
    }

    #[test]
    fn test_comparison() {
        let int5 = Value::Integer(5);
        let int10 = Value::Integer(10);

        assert_eq!(compare(&int5, &int10, &TokenType::Less).unwrap(), Value::Boolean(true));
        assert_eq!(compare(&int5, &int5, &TokenType::Equal).unwrap(), Value::Boolean(true));
        assert_eq!(compare(&int5, &int10, &TokenType::BangEqual).unwrap(), Value::Boolean(true));
        assert_eq!(compare(&int5, &Value::Null, &TokenType::Greater).unwrap(), Value::Null);
    }

    #[test]
    fn test_logical() {
        let t = Value::Boolean(true);
        let f = Value::Boolean(false);
        let n = Value::Null;

        assert_eq!(logical(&t, &t, &TokenType::And).unwrap(), Value::Boolean(true));
        assert_eq!(logical(&t, &f, &TokenType::And).unwrap(), Value::Boolean(false));
        assert_eq!(logical(&t, &n, &TokenType::And).unwrap(), Value::Null);
        assert_eq!(logical(&f, &n, &TokenType::And).unwrap(), Value::Boolean(false));
        assert_eq!(logical(&t, &f, &TokenType::Or).unwrap(), Value::Boolean(true));
        assert_eq!(logical(&f, &f, &TokenType::Or).unwrap(), Value::Boolean(false));
        assert_eq!(logical(&t, &n, &TokenType::Or).unwrap(), Value::Boolean(true));
        assert_eq!(logical(&f, &n, &TokenType::Or).unwrap(), Value::Null);
    }

    #[test]
    fn test_unary() {
        let e_minus = BoundExpr::UnaryOp {
            operator: TokenType::Minus,
            right: Box::new(BoundExpr::Literal (Value::Integer(5)))
        };
        assert_eq!(evaluate(&e_minus, &[]).unwrap(), Value::Integer(-5));
        let e_not = BoundExpr::UnaryOp {
            operator: TokenType::Not,
            right: Box::new(BoundExpr::Literal(Value::Boolean(true)))
        };
        assert_eq!(evaluate(&e_not, &[]).unwrap(), Value::Boolean(false));
    }

    #[test]
    fn test_block_commits_atomically() {
        let (mut catalog, bp, _dir, txn) = setup();
        let mut s = Session::new();
        let exec = |sql: &str, catalog: &mut Catalog, s: &mut Session| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let stmts = p.parse();
            assert!(p.errors.is_empty());
            run(stmts.into_iter().next().unwrap(), catalog, bp.clone(), txn.clone(), s)
        };
        exec("CREATE TABLE t (id INTEGER NOT NULL);", &mut catalog, &mut s).unwrap();
        exec("BEGIN;", &mut catalog, &mut s).unwrap();
        exec("INSERT INTO t VALUES (1);", &mut catalog, &mut s).unwrap();
        exec("INSERT INTO t VALUES (2);", &mut catalog, &mut s).unwrap();
        exec("COMMIT;", &mut catalog, &mut s).unwrap();
        match exec("SELECT id FROM t;", &mut catalog, &mut s).unwrap() {
            Outcome::Rows(r) => assert_eq!(r.len(), 2),
            _ => panic!()
        }
    }

    #[test]
    fn test_block_rollback_discards_everything() {
        let (mut catalog, bp, _dir, txn) = setup();
        let mut s = Session::new();
        let exec = |sql: &str, catalog: &mut Catalog, s: &mut Session| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let stmts = p.parse();
            assert!(p.errors.is_empty());
            run(stmts.into_iter().next().unwrap(), catalog, bp.clone(), txn.clone(), s)
        };
        exec("CREATE TABLE t (id INTEGER NOT NULL);", &mut catalog, &mut s).unwrap();
        exec("BEGIN;", &mut catalog, &mut s).unwrap();
        exec("INSERT INTO t VALUES (1);", &mut catalog, &mut s).unwrap();
        exec("ROLLBACK;", &mut catalog, &mut s).unwrap();
        match exec("SELECT id FROM t;", &mut catalog, &mut s).unwrap() {
            Outcome::Rows(r) => assert!(r.is_empty()),
            _ => panic!()
        }
        assert!(matches!(exec("COMMIT;", &mut catalog, &mut s), Err(FerroError::Txn(_))));
    }

    #[test]
    fn test_error_aborts_everything() {
        let (mut catalog, bp, _dir, txn) = setup();
        let mut s = Session::new();
        let exec = |sql: &str, catalog: &mut Catalog, s: &mut Session| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let stmts = p.parse();
            assert!(p.errors.is_empty());
            run(stmts.into_iter().next().unwrap(), catalog, bp.clone(), txn.clone(), s)
        };
        exec("CREATE TABLE t (id INTEGER NOT NULL);", &mut catalog, &mut s).unwrap();
        exec("BEGIN;", &mut catalog, &mut s).unwrap();
        exec("INSERT INTO t VALUES (1);", &mut catalog, &mut s).unwrap();
        assert!(exec("INSERT INTO idk VALUES (1);", &mut catalog,  &mut s).is_err());
        match exec("SELECT id FROM t;", &mut catalog, &mut s).unwrap() {
            Outcome::Rows(r) => assert!(r.is_empty()),
            _ => panic!()
        }
        assert!(matches!(exec("COMMIT;", &mut catalog, &mut s), Err(FerroError::Txn(_))));
    }

    #[test]
    fn test_rejects_ddl_inside_block() {
        let (mut catalog, bp, _dir, txn) = setup();
        let mut s = Session::new();
        let exec = |sql: &str, catalog: &mut Catalog, s: &mut Session| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let stmts = p.parse();
            assert!(p.errors.is_empty());
            run(stmts.into_iter().next().unwrap(), catalog, bp.clone(), txn.clone(), s)
        };
        assert!(matches!(exec("COMMIT;", &mut catalog, &mut s), Err(FerroError::Txn(_))));
        assert!(matches!(exec("ROLLBACK;", &mut catalog, &mut s), Err(FerroError::Txn(_))));
        exec("BEGIN;", &mut catalog, &mut s).unwrap();
        assert!(matches!(exec("BEGIN;", &mut catalog, &mut s), Err(FerroError::Txn(_))));
        assert!(matches!(exec("CREATE TABLE idk (id INTEGER NOT NULL);", &mut catalog, &mut s), Err(FerroError::Txn(_))));
        exec("ROLLBACK;", &mut catalog, &mut s).unwrap();
    }

    #[test]
    fn test_insert_stamps_begin_ts() {
        let (mut catalog, bp, _dir, txn) = setup();
        let mut s = Session::new();
        let exec = |sql: &str, catalog: &mut Catalog, s: &mut Session| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let mut p = Parser::new(tokens);
            let stmts = p.parse();
            assert!(p.errors.is_empty());
            run(stmts.into_iter().next().unwrap(), catalog, bp.clone(), txn.clone(), s)
        };
        exec("CREATE TABLE t (id INTEGER NOT NULL, str VARCHAR(10));", &mut catalog, &mut s).unwrap();
        let expected = txn.next_txn_id.load(Ordering::SeqCst);
        exec("INSERT INTO t VALUES (1, 'a');", &mut catalog, &mut s).unwrap();
        let entry = catalog.get_table("t").unwrap();
        let heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
        let (_, tuple) = heap.scan().next().unwrap().unwrap();
        let h = tuple.version_header().unwrap();
        assert_eq!(h.begin_ts, expected);
        assert_eq!(h.end_ts, 0);
        assert_eq!(h.prev(), None);
    }
}