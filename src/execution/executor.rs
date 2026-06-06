use crate::catalog::column::Value;
use crate::catalog::schema::Schema;
use crate::parser::parser::Expr;
use crate::{error::FerroError, storage::tuple::Tuple};
use crate::storage::heap_file_manager::RecordId;
use crate::parser::scanner::TokenType;

pub trait Executor {
    fn next(&mut self) -> Option<Result<(RecordId, Tuple), FerroError>>;
}

fn evaluate(expr: &Expr, row: &[Value], schema: &Schema) -> Result<Value, FerroError> {
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
