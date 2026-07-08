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
pub const DEFAULT_INDEX_FANOUT: f64 = 100.0;
pub const DEFAULT_ENTRIES_PER_LEAF: f64 = 100.0;

pub struct RelStats {
    pub rows: f64,
    pub columns: Vec<ColumnStats>,
}

pub struct Costed {
    pub stats: RelStats,
    pub cost: f64,
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

// cardinality + cost in one pass
pub fn cost(plan: &PhysicalPlan, catalog: &Catalog) -> Costed {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let mut child = cost(input, catalog);
            let sel = selectivity(predicate, &child.stats.columns);
            let rows = (child.stats.rows * sel).max(1.0);
            apply_equalities(&mut child.stats.columns, predicate);
            cap_distinct(&mut child.stats.columns, rows);
            let cost = child.cost + child.stats.rows * DEFAULT_CPU_TUPLE_COST;
            Costed {stats: RelStats { rows, columns: child.stats.columns }, cost}
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
            let height = tree_height(base.rows);
            let leaf_pages = (rows/DEFAULT_ENTRIES_PER_LEAF).ceil().max(1.0);
            let mut cost = height * DEFAULT_RANDOM_PAGE_COST + leaf_pages * DEFAULT_SEQ_PAGE_COST + rows * DEFAULT_RANDOM_PAGE_COST;
            if *column != 0 {
                cost += rows * DEFAULT_RANDOM_PAGE_COST;
            }
            Costed {stats: RelStats {rows, columns: base.columns}, cost}
        }
        PhysicalPlan::NestedLoopJoin { left, right, on, join_type, .. } => {
            let l = cost(left, catalog);
            let r = cost(right, catalog);
            let stats = join_cardinality(&l.stats, &r.stats, on, join_type);
            let cost = l.cost + r.cost + l.stats.rows * r.stats.rows * DEFAULT_CPU_TUPLE_COST;
            Costed {stats, cost}
        },
        PhysicalPlan::Projection { input, exprs } => {
            let child = cost(input, catalog);
            let columns: Vec<ColumnStats> = exprs.iter().map(|e| match e {
                BoundExpr::Column(i) => child.stats.columns.get(*i).cloned().unwrap_or(ColumnStats { distinct: 1, nulls: 0, min: None, max: None }),
                _ => ColumnStats { distinct: child.stats.rows.ceil().max(1.0) as usize, nulls: 0, min: None, max: None }
            }).collect();
            let cost = child.cost + child.stats.rows * DEFAULT_CPU_TUPLE_COST;
            Costed { stats: RelStats { rows: child.stats.rows, columns }, cost}
        },
        PhysicalPlan::SeqScan { table } => {
            let stats = base_stats(table, catalog);
            let cost = table_pages(table, catalog)*DEFAULT_SEQ_PAGE_COST + stats.rows * DEFAULT_CPU_TUPLE_COST;
            Costed { stats, cost }
        },
        PhysicalPlan::HashJoin { left, right, on, join_type, .. } => {
            let l = cost(left, catalog);
            let r = cost(right, catalog);
            let stats = join_cardinality(&l.stats, &r.stats, on, join_type);
            let cost = l.cost + r.cost + (r.stats.rows + l.stats.rows + stats.rows) * DEFAULT_CPU_TUPLE_COST;
            Costed { stats, cost }
        }
    }
}

// helpers

pub fn join_cardinality(l: &RelStats, r: &RelStats, on: &BoundExpr, join_type: &JoinType) -> RelStats {
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
        let vr = r.columns.get(ri).map(|cs| cs.distinct.max(1)).unwrap_or(1);
        divisor *= vl.max(vr) as f64;
        keys.push((li, ri));
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
    RelStats { rows, columns}
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

pub fn equi_pairs(on: &BoundExpr, out: &mut Vec<(usize, usize)>) {
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

fn table_pages(table: &str, catalog: &Catalog) -> f64 {
    match catalog.get_table(table) {
        Some(e) => {
            let row_count = catalog.table_stats(table).map(|s| s.row_count).unwrap_or(DEFAULT_TABLE_ROWS);
            num_pages(row_count, &e.schema) as f64
        }
        None => (DEFAULT_TABLE_ROWS/50).max(1) as f64
    }
}

fn tree_height(rows: f64) -> f64 {
    if rows <= 1.0 {
        return 1.0;
    }
    (rows.ln()/DEFAULT_INDEX_FANOUT.ln()).ceil().max(1.0)
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

pub fn contains_hash_join(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::HashJoin { .. } => true,
        PhysicalPlan::NestedLoopJoin { left, right, .. } => contains_hash_join(left) | contains_hash_join(right),
        PhysicalPlan::Projection { input, .. } | PhysicalPlan::Filter { input, .. } => contains_hash_join(input),
        _ => false
    }
}

pub fn contains_nlj(plan: &PhysicalPlan) -> bool {
    match plan {
        PhysicalPlan::NestedLoopJoin { .. } => true,
        PhysicalPlan::HashJoin { left, right, .. } => contains_nlj(left) | contains_nlj(right),
        PhysicalPlan::Projection { input, .. } | PhysicalPlan::Filter { input, .. } => contains_nlj(input),
        _ => false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::tempdir;

use crate::{binder::binder::Binder, buffer::buffer_pool::BufferPoolManager, execution::executor::run, optimizer::optimizer::{optimize, pushdown}, parser::{parser::Parser, scanner::Scanner}, storage::disk_manager::DiskManager, wal::{log::WalManager, txn::TxnManager}};
    use super::*;

    fn setup() -> (Catalog, Arc<BufferPoolManager>, Arc<TxnManager>) {
        let file = tempfile::tempfile().unwrap();
        let dir = tempdir().unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("wal.test")).unwrap());
        let txn = Arc::new(TxnManager::new(wal, bp.clone()));
        (catalog, bp, txn)
    }

    fn exec(sql: &str, catalog: &mut Catalog, bp: Arc<BufferPoolManager>, txn: Arc<TxnManager>) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut parser = Parser::new(tokens);
        let stmts = parser.parse();
        assert!(parser.errors.is_empty(), "parse errors: {:?}", parser.errors);
        for stmt in stmts {
            run(stmt, catalog, bp.clone(), txn.clone()).unwrap();
        }
    }

    #[test]
    fn test_cost_index_vs_seq() {
        let (mut catalog, bp, txn) = setup();
        let mut sql = String::from("CREATE TABLE t (id INTEGER NOT NULL);");
        for i in 0..2000 {
            sql.push_str(&format!("INSERT INTO t VALUES ({});", i));
        }
        exec(&sql, &mut catalog, bp.clone(), txn.clone());
        exec("ANALYZE t;", &mut catalog, bp, txn);
        catalog.analyze("t").unwrap();
        let seq = cost(&PhysicalPlan::SeqScan { table: "t".into() }, &catalog);
        let point = cost(&PhysicalPlan::IndexScan { table: "t".into(), column: 0, lower: Bound::Included(Value::Integer(2)), upper: Bound::Included(Value::Integer(2)) }, &catalog);
        let big = cost(&PhysicalPlan::IndexScan { table: "t".into(), column: 0, lower: Bound::Included(Value::Integer(67)), upper: Bound::Unbounded }, &catalog);
        assert_eq!(point.stats.rows, 1.0);
        assert!(point.cost < seq.cost);
        assert!(big.cost > seq.cost);
    }

    #[test]
    fn test_cost_secondary_over_primary() {
        let (mut catalog, bp, txn) = setup();
        exec("CREATE TABLE t (a INTEGER NOT NULL, b INTEGER);", &mut catalog, bp.clone(), txn);
        let primary = cost(&PhysicalPlan::IndexScan { table: "t".into(), column: 0, lower: Bound::Included(Value::Integer(5)), upper: Bound::Included(Value::Integer(5)) }, &catalog);
        let secondary = cost(&PhysicalPlan::IndexScan { table: "t".into(), column: 1, lower: Bound::Included(Value::Integer(5)), upper: Bound::Included(Value::Integer(5)) }, &catalog);
        assert_eq!(primary.stats.rows, secondary.stats.rows);
        assert!(secondary.cost > primary.cost);
    }

    #[test]
    fn test_cardinality() {
        let (mut catalog, bp, txn) = setup();
        let mut sql = String::from("CREATE TABLE users (id INTEGER NOT NULL, age INTEGER);");
        for i in 0..50 {
            sql.push_str(&format!("INSERT INTO users VALUES ({}, {});", i, i%10));
        }
        sql.push_str("CREATE TABLE posts (id INTEGER NOT NULL, user_id INTEGER);");
        for i in 0..150 {
            sql.push_str(&format!("INSERT INTO posts VALUES ({}, {});", i, i%50));
        }
        exec(&sql, &mut catalog, bp.clone(), txn.clone());
        exec("ANALYZE users;", &mut catalog, bp.clone(), txn.clone());
        exec("ANALYZE posts;", &mut catalog, bp, txn);
        // sel = 1/10 -> 5 rows
        let filtered = cost(&PhysicalPlan::Filter { 
            input: Box::new(PhysicalPlan::SeqScan { table: "users".into() }), 
            predicate: BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(1)), operator: TokenType::Equal, right: Box::new(BoundExpr::Literal(Value::Integer(0))) }
        }, &catalog);
        assert_eq!(filtered.stats.columns[1].distinct, 1);
        assert!((filtered.stats.rows - 5.0).abs() < 0.0001, "{}", filtered.stats.rows);
        assert!((filtered.stats.columns[0].distinct as f64) <= filtered.stats.rows.ceil());

        let joined = cost(&PhysicalPlan::NestedLoopJoin { 
            left: Box::new(PhysicalPlan::SeqScan { table: "users".into() }), 
            right: Box::new(PhysicalPlan::SeqScan { table: "posts".into() }), 
            on: BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(0)), operator: TokenType::Equal, right: Box::new(BoundExpr::Column(3)) }, 
            join_type: JoinType::Inner, 
            right_width: 2 
        }, &catalog);

        assert!((joined.stats.rows - 150.0).abs() < 1.0, "{}", joined.stats.rows);
    }   

    #[test]
    fn test_fallback() {
        let varchar_col = ColumnStats { distinct: 20, nulls: 0, min: Some(Value::Varchar("a".into())), max: Some(Value::Varchar("z".into()))};
        let sel = bound_selectivity(&varchar_col, &Bound::Included(Value::Varchar("n".into())), &Bound::Unbounded);
        assert_eq!(sel, DEFAULT_RANGE_SELECTIVITY);

        let no_bounds = ColumnStats { distinct: 100, nulls: 0, min: None, max: None};
        let sel = bound_selectivity(&no_bounds, &Bound::Included(Value::Integer(5)), &Bound::Unbounded);
        assert_eq!(sel, DEFAULT_RANGE_SELECTIVITY);

        let eq = bound_selectivity(&no_bounds, &Bound::Included(Value::Integer(1)), &Bound::Included(Value::Integer(1)));
        assert!((eq - 1.0/100.0).abs() < 1e-12);
    }

    #[test]
    fn explain_index_flip_at_volume() {
        let (mut catalog, bp, txn) = setup();
        let mut sql = String::from("CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(50));");
        for i in 0..2000 {
            sql.push_str(&format!("INSERT INTO users VALUES ({}, 'user{}');", i, i));
        }
        exec(&sql, &mut catalog, bp.clone(), txn);
        catalog.analyze("users").unwrap();

        let optimize_sql = |sql: &str, catalog: &Catalog| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let stmt = Parser::new(tokens).parse().remove(0);
            let logical = Binder::new(catalog).bind(stmt).unwrap();
            optimize(pushdown(logical), catalog).unwrap()
        };

        let point = optimize_sql("SELECT name FROM users WHERE id = 5;", &catalog);
        assert!(matches!(point, PhysicalPlan::Projection { input, .. }
            if matches!(*input, PhysicalPlan::IndexScan { .. })));

        let broad = optimize_sql("SELECT name FROM users WHERE id > 0;", &catalog);
        assert!(matches!(broad, PhysicalPlan::Projection { input, .. }
            if matches!(*input, PhysicalPlan::Filter { .. })));
    }

    #[test]
    fn test_join_picks_hash() {
        let (mut catalog, bp, txn) = setup();
        let mut sql = String::from("CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(255));");
        sql.push_str("CREATE TABLE posts (id INTEGER NOT NULL, user_id INTEGER, title VARCHAR(255));");
        for i in 0..300 {
            sql.push_str(&format!("INSERT INTO users VALUES ({}, 'u{}');", i, i));
        }
        for i in 0..600 {
            sql.push_str(&format!("INSERT INTO posts VALUES ({}, {}, 't{}');", i, i%30, i));
        }
        exec(&sql, &mut catalog, bp.clone(), txn);
        catalog.analyze("users").unwrap();
        catalog.analyze("posts").unwrap();

        let optimize_sql = |sql: &str, catalog: &Catalog| {
            let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
            let stmt = Parser::new(tokens).parse().remove(0);
            let logical = Binder::new(catalog).bind(stmt).unwrap();
            optimize(pushdown(logical), catalog).unwrap()
        };
        let equi = optimize_sql("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id;", &catalog);
        assert!(contains_hash_join(&equi));

        let non_equi = optimize_sql("SELECT u.name, p.title FROM users u JOIN posts p ON u.id > p.user_id;", &catalog);
        assert!(contains_nlj(&non_equi));
    }
}