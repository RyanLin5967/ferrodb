use crate::{binder::binder::BoundExpr, catalog::{column::{DataType, Value}, schema::Schema, stats::{ColumnStats, TableStats}}, parser::scanner::TokenType, storage::disk_manager::PAGE_SIZE};

// magic constants
pub const DEFAULT_SEQ_PAGE_COST: f64 = 1.0;
pub const DEFAULT_RANDOM_PAGE_COST: f64 = 4.0;
pub const DEFAULT_CPU_TUPLE_COST: f64 = 0.01;
pub const DEFAULT_CPU_INDEX_TUPLE_COST: f64 = 0.005;
pub const DEFAULT_CPU_OPERATOR_COST: f64 = 0.0025;
pub const DEFAULT_PARALLEL_TUPLE_COST: f64 = 0.1;
pub const DEFAULT_PARALLEL_SETUP_COST: f64 = 1000.0;
pub const DEFAULT_SELECTIVITY: f64 = 1.0/3.0;
pub const DEFAULT_RANGE_SELECTIVITY: f64 = 0.25;

// Column(i) op Literal(v):
// = -> 1/V(i)
// != -> 1 - 1/V(i)
// < -> (c - max(A))/(max(A) - min(A)) (same for <=)
// > -> (max(A) - c)/(max(A) - min(A)) (same for >=)
// AND -> s(left) * s(right)
// OR -> s(left) + s(right) - s(left) * s(right)
// NOT -> 1 - s(child)
// Col = Col -> 1/max(V(a), V(b))
// else -> default constant
pub fn selectivity(predicate: &BoundExpr, stats: &TableStats) -> f64 {
    match predicate {
        BoundExpr::BinaryOp { left, operator: TokenType::And, right } => selectivity(left, stats) * selectivity(right, stats),
        BoundExpr::BinaryOp { left, operator: TokenType::Or, right } => {
            let sl = selectivity(left, stats);
            let sr = selectivity(right, stats);
            sl + sr - sl*sr
        }
        BoundExpr::UnaryOp { operator: TokenType::Not, right } => 1.0 - selectivity(right, stats),
        BoundExpr::BinaryOp { left, operator, right } => {
            let (col, op, val) = match (&**left, &**right) {
                (BoundExpr::Column(c), BoundExpr::Literal(v)) => (*c, *operator, v),
                (BoundExpr::Literal(v), BoundExpr::Column(c)) => (*c, flip(*operator), v),
                (BoundExpr::Column(a), BoundExpr::Column(b)) if *operator == TokenType::Equal => {
                    let va = stats.columns[*a].distinct.max(1);
                    let vb = stats.columns[*b].distinct.max(1);
                    return 1.0/va.max(vb) as f64;
                }
                _ => return DEFAULT_SELECTIVITY,
            };
            let cs = &stats.columns[col];
            let v = cs.distinct.max(1) as f64;
            match op {
                TokenType::Equal => 1.0/v,
                TokenType::BangEqual => 1.0 - 1.0/v,
                TokenType::Less | TokenType::LessEqual => range_selectivity(cs, val, true),
                TokenType::Greater | TokenType::GreaterEqual => range_selectivity(cs, val, false),
                _ => DEFAULT_SELECTIVITY
            }
        }
        BoundExpr::Literal(Value::Boolean(b)) => if *b {1.0} else {0.0},
        _ => DEFAULT_SELECTIVITY
    }
}

fn range_selectivity(cs: &ColumnStats, val: &Value, less_than: bool) -> f64 {
    let (Some(min), Some(max), Some(v)) = (cs.min.as_ref().and_then(get_val), cs.max.as_ref().and_then(get_val), get_val(val)) else {
        return DEFAULT_RANGE_SELECTIVITY;
    };
    if max <= min { return DEFAULT_RANGE_SELECTIVITY;}
    let frac = if less_than { (v - min)/(max-min) } else { (max - v)/(max - min) };
    frac.clamp(0.0, 1.0)
}

fn flip(op: TokenType) -> TokenType {
    match op {
        TokenType::Less => TokenType::Greater,
        TokenType::Greater => TokenType::Less,
        TokenType::GreaterEqual => TokenType::LessEqual,
        TokenType::LessEqual => TokenType::GreaterEqual,
        other => other
    }
}

fn get_val(v: &Value) -> Option<f64> {
    match v {
        Value::Integer(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        _ => None
    }
}

fn row_width(schema: &Schema) -> usize {
    schema.columns.iter().map(|c| match c.data_type {
        DataType::Boolean => 1,
        DataType::Float => 8,
        DataType::Integer => 4,
        DataType::Varchar(n) => n as usize,
    }).sum()
}

fn num_pages(row_count: usize, schema: &Schema) -> usize {
    let per_row = (PAGE_SIZE/row_width(schema).max(1)).max(1);
    row_count.div_ceil(per_row)
}