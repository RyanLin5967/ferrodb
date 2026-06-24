use std::{ops::Bound};

use crate::{binder::binder::BoundExpr, catalog::{catalog::Catalog, column::{DataType, Value}, schema::Schema, stats::{ColumnStats}}, parser::{parser::JoinType, scanner::TokenType}, planner::physical_plan::PhysicalPlan, storage::disk_manager::PAGE_SIZE};

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
pub const DEFAULT_TABLE_ROWS: usize = 1000;
pub const DEFAULT_DISTINCT: usize = 100;

pub struct RelStats {
    pub rows: f64,
    pub columns: Vec<ColumnStats>,
}
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
pub fn selectivity(predicate: &BoundExpr, cols: &[ColumnStats]) -> f64 {
    match predicate {
        BoundExpr::BinaryOp { left, operator: TokenType::And, right } => selectivity(left, cols) * selectivity(right, cols),
        BoundExpr::BinaryOp { left, operator: TokenType::Or, right } => {
            let sl = selectivity(left, cols);
            let sr = selectivity(right, cols);
            sl + sr - sl*sr
        }
        BoundExpr::UnaryOp { operator: TokenType::Not, right } => 1.0 - selectivity(right, cols),
        BoundExpr::BinaryOp { left, operator, right } => {
            let (col, op, val) = match (&**left, &**right) {
                (BoundExpr::Column(c), BoundExpr::Literal(v)) => (*c, *operator, v),
                (BoundExpr::Literal(v), BoundExpr::Column(c)) => (*c, flip(*operator), v),
                (BoundExpr::Column(a), BoundExpr::Column(b)) if *operator == TokenType::Equal => {
                    let va = cols[*a].distinct.max(1);
                    let vb = cols[*b].distinct.max(1);
                    return 1.0/va.max(vb) as f64;
                }
                _ => return DEFAULT_SELECTIVITY,
            };
            let cs = &cols[col];
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

pub fn cardinality(plan: &PhysicalPlan, catalog: &Catalog) -> RelStats {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let mut in_stats = cardinality(input, catalog);
            let sel = selectivity(predicate, &in_stats.columns);
            let rows = (in_stats.rows * sel).max(1.0);
            apply_equalities(&mut in_stats.columns, predicate);
            cap_distinct(&mut in_stats.columns, rows);
            RelStats { rows, columns: in_stats.columns }
        }
        PhysicalPlan::IndexScan { table, column, lower, upper } => {
            let mut base = base_stats(table, catalog);
            let sel = base.columns.get(*column).map(|cs| bound_selectivity(cs, lower, upper)).unwrap_or(DEFAULT_SELECTIVITY);
            let rows = (base.rows *sel).max(1.0);
            if let (Bound::Included(lo), Bound::Included(hi)) = (lower, upper) {
                if lo == hi{
                    if let Some(c) = base.columns.get_mut(*column) {
                        c.distinct = 1;
                        c.min = Some(lo.clone());
                        c.max = Some(hi.clone());
                    }
                }
            }
            cap_distinct(&mut base.columns, rows);
            RelStats {rows, columns: base.columns}
        }
        PhysicalPlan::NestedLoopJoin { left, right, on, join_type, .. } => {
            let l = cardinality(left, catalog);
            let r = cardinality(right, catalog);
            let mut pairs: Vec<(usize, usize)> = Vec::new();
            equi_pairs(on, &mut pairs);
            
            let l_width = l.columns.len();
            let mut divisor = 1.0;
            let mut keys: Vec<(usize, usize)> = Vec::new();
            for (a, b) in &pairs {
                let (li, ri) = if *a < l_width && *b >= l_width{
                    (*a, *b - l_width)
                } else if *b < l_width && *a >= l_width {
                    (*b, *a - l_width)
                } else {
                    continue;
                };
                let vl = l.columns.get(li).map(|cs| cs.distinct.max(1)).unwrap_or(1);
                let vr = l.columns.get(ri).map(|cs| cs.distinct.max(1)).unwrap_or(1);
                divisor *= vl.max(vr) as f64;
                keys.push((vl, vr));
            }
            let mut rows = (l.rows * r.rows/divisor).max(1.0);
            if matches!(join_type, JoinType::Left) {
                rows = rows.max(l.rows);
            }
            let mut columns = l.columns.clone();
            columns.extend(r.columns.iter().cloned());
            for (li, ri) in keys {
                let v = l.columns[li].distinct.min(r.columns[ri].distinct).max(1);
                columns[li].distinct = v;
                columns[l_width + ri].distinct = v;
            }
            cap_distinct(&mut columns, rows);
            RelStats { rows, columns }
        },
        PhysicalPlan::Projection { input, exprs } => {
            let in_stats = cardinality(input, catalog);
            let columns: Vec<ColumnStats> = exprs.iter().map(|e| match e {
                BoundExpr::Column(i) => in_stats.columns.get(*i).cloned().unwrap_or(ColumnStats { distinct: 1, nulls: 0, min: None, max: None }),
                _ => ColumnStats { distinct: in_stats.rows.ceil().max(1.0) as usize, nulls: 0, min: None, max: None }
            }).collect();
            RelStats { rows: in_stats.rows, columns}
        },
        PhysicalPlan::SeqScan { table } => base_stats(table, catalog)
        
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

fn bound_selectivity(cs: &ColumnStats, lower: &Bound<Value>, upper: &Bound<Value>) -> f64 {
    if let (Bound::Included(lo), Bound::Included(hi)) = (lower, upper) {
        if lo == hi {
            return 1.0/cs.distinct.max(1) as f64;
        }
    }
    let (Some(min), Some(max)) = (cs.min.as_ref().and_then(get_val), cs.max.as_ref().and_then(get_val)) else {return DEFAULT_RANGE_SELECTIVITY;};
    if max <= min {return DEFAULT_RANGE_SELECTIVITY;}
    let lo = match lower {
        Bound::Included(v) | Bound::Excluded(v) => get_val(v).unwrap_or(min),
        Bound::Unbounded => min
    };
    let hi = match upper {
        Bound::Included(v) | Bound::Excluded(v) => get_val(v).unwrap_or(max),
        Bound::Unbounded => max
    };
    ((hi.min(max) - lo.max(min))/(max-min)).clamp(0.0, 1.0)
}

fn equi_pairs(on: &BoundExpr, out: &mut Vec<(usize, usize)>) {
    match on {
        BoundExpr::BinaryOp { left, operator: TokenType::And, right } => {
            equi_pairs(left, out);
            equi_pairs(right, out);
        }
        BoundExpr::BinaryOp { left, operator: TokenType::Equal, right } => {
            match (&**left, &**right) {
                (BoundExpr::Column(a), BoundExpr::Column(b)) => out.push((*a, *b)),
                _ => {}
            }
        }
        _ => {}
    }
}

fn apply_equalities(cols: &mut [ColumnStats], expr: &BoundExpr) {
    match expr {
        BoundExpr::BinaryOp { left, operator: TokenType::And, right } => {
            apply_equalities(cols, left);
            apply_equalities(cols, right);
        }
        BoundExpr::BinaryOp { left, operator: TokenType::Equal, right } => {
            let pair = match (&**left, &**right) {
                (BoundExpr::Column(i), BoundExpr::Literal(v)) => Some((*i, v)),
                (BoundExpr::Literal(v), BoundExpr::Column(i)) => Some((*i, v)),
                _ => None
            };
            if let Some((i, v)) = pair {
                if let Some(c) = cols.get_mut(i) {
                    c.distinct = 1;
                    c.max = Some(v.clone());
                    c.min = Some(v.clone());
                }
            }
        }
        _ => {}
    }
}

fn base_stats(table: &String, catalog: &Catalog) -> RelStats {
    match catalog.table_stats(table) {
        Some(s) => RelStats { rows: s.row_count as f64, columns: s.columns.clone() },
        None => {
            let n = catalog.get_table(table).map(|e| e.schema.columns.len()).unwrap_or(0);
            RelStats {rows: DEFAULT_TABLE_ROWS as f64, columns: vec![ColumnStats {distinct: DEFAULT_DISTINCT, nulls: 0, min: None, max: None}; n]}
        }
    }
}

fn cap_distinct(cols: &mut [ColumnStats], rows: f64) {
    let cap = rows.ceil().max(1.0) as usize;
    for c in cols.iter_mut() {
        c.distinct = c.distinct.min(cap).max(1);
    }
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