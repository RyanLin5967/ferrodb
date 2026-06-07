use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::Value;
use crate::catalog::schema::Schema;
use crate::execution::index_handle::IndexHandle;
use crate::parser::parser::{Expr, Stmt};
use crate::planner::plan::Outcome;
use crate::storage::index::BPlusTreeManager;
use crate::{error::FerroError};
use crate::storage::heap_file_manager::RecordId;
use crate::parser::scanner::TokenType;

pub trait Executor {
    fn next(&mut self) -> Option<Result<(RecordId, Vec<Value>), FerroError>>;
}

pub trait Modify {
    fn execute(&mut self, catalog: &mut Catalog) -> Result<usize, FerroError>;
}

pub fn run(stmt: Stmt, catalog: &mut Catalog, bp: Arc<BufferPoolManager>) -> Result<Outcome, FerroError> {

    todo!()
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
pub fn evaluate(expr: &Expr, row: &[Value], schema: &Schema) -> Result<Value, FerroError> {
    return match expr {
        Expr::Literal { value_type, value } => match value_type {
            TokenType::TypeBoolean => match value.to_lowercase().as_str(){
                "true" => Ok(Value::Boolean(true)),
                "false" => Ok(Value::Boolean(false)),
                _ => Err(FerroError::Parse(format!("bad bool literal: {}", value)))
            }
            TokenType::TypeFloat => Ok(Value::Float(value.parse::<f64>().map_err(|e| FerroError::Parse(format!("float parse fail: {}", e)))?)),
            TokenType::TypeInt => Ok(Value::Integer(value.parse::<i32>().map_err(|e|FerroError::Parse(format!("integer parse fail: {}", e)))?)),
            TokenType::TypeNull => Ok(Value::Null),
            TokenType::TypeVarchar => Ok(Value::Varchar(value.clone())),
            _ => return Err(FerroError::Parse("invalid value type".into()))
        }
        Expr::BinaryOp { left, operator, right } => {
            let l = evaluate(left, row, schema)?;
            let r = evaluate(right ,row, schema)?;

            match operator {
                TokenType::Plus | TokenType::Minus | TokenType::Star | TokenType::Slash => arithmetic(&l, &r, operator),
                TokenType::Equal | TokenType::BangEqual | TokenType::Less | TokenType::LessEqual
                | TokenType::Greater | TokenType::GreaterEqual => compare(&l, &r, operator),
                TokenType::And | TokenType::Or => logical(&l, &r, operator),
                _ => Err(FerroError::Parse("invalid binary op".into()))
            }
        }
        Expr::UnaryOp { operator, right } => {
            let v = evaluate(right, row, schema)?;
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
        Expr::ColumnRef(s) => {
            let idx = schema.columns.iter().position(|c|c.name == *s).ok_or(FerroError::Parse(format!("unknonwn column: {}", s)))?;
            row.get(idx).cloned().ok_or(FerroError::Parse(format!("row missing column: {}", s)))
        }
        Expr::Grouping(e) => evaluate(e, row, schema)
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
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::column::DataType;

    fn mock_schema() -> Schema {
        Schema{ columns: vec![
            Column::new("id".into(), DataType::Integer, false),
            Column::new("name".into(), DataType::Varchar(255), false),
            Column::new("is_active".into(), DataType::Boolean, false),
        ]}
    }

    fn mock_row() -> Vec<Value>{
        vec![
            Value::Integer(1),
            Value::Varchar("Alice".into()),
            Value::Boolean(true),
        ]
    }

    #[test]
    fn test_literal() {
        let schema = mock_schema();
        let row = mock_row();

        let cases = vec![
            (TokenType::TypeInt, "42", Value::Integer(42)),
            (TokenType::TypeFloat, "3.14", Value::Float(3.14)),
            (TokenType::TypeBoolean, "true", Value::Boolean(true)),
            (TokenType::TypeBoolean, "FALSE", Value::Boolean(false)),
            (TokenType::TypeNull, "null", Value::Null),
            (TokenType::TypeVarchar, "hello", Value::Varchar("hello".into())),
        ];

        for (t_type, val_str, expected) in cases {
            let expr = Expr::Literal { value_type: t_type, value: val_str.into() };
            assert_eq!(evaluate(&expr, &row, &schema).unwrap(), expected);
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
        let schema = mock_schema();
        let row = mock_row();

        let e_minus = Expr::UnaryOp {
            operator: TokenType::Minus,
            right: Box::new(Expr::Literal { value_type: TokenType::TypeInt, value: "5".into() })
        };
        assert_eq!(evaluate(&e_minus, &row, &schema).unwrap(), Value::Integer(-5));
        let e_not = Expr::UnaryOp {
            operator: TokenType::Not,
            right: Box::new(Expr::Literal { value_type: TokenType::TypeBoolean, value: "true".into() })
        };
        assert_eq!(evaluate(&e_not, &row, &schema).unwrap(), Value::Boolean(false));
    }
}