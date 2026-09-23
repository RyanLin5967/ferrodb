use std::{collections::HashSet, ops::Bound, sync::Arc};

use crate::{binder::binder::BoundExpr, buffer::buffer_pool::BufferPoolManager, catalog::{catalog::Catalog, catalog_page::TableEntry, column::Value}, error::FerroError, execution::{executor::Executor, filter::Filter, hash_join::HashJoin, index_scan::IndexScan, nested_loop_join::NestedLoopJoin, projection::Projection, sec_index_scan::SecondaryIndexScan, seq_scan::SeqScan}, optimizer::{cost_model::{DEFAULT_CPU_TUPLE_COST, cost, equi_pairs, join_cardinality}, search_algorithm::reorder_inner_joins}, parser::{parser::JoinType, scanner::TokenType}, planner::{logical_plan::LogicalPlan, physical_plan::PhysicalPlan, plan::predicate_to_bounds}, storage::{heap_file_manager::{HeapFileManager, RecordId}, index::BPlusTreeManager}, wal::txn::ReadView};

pub fn optimize(lp: LogicalPlan, catalog: &Catalog) -> Result<PhysicalPlan, FerroError> {
    match lp {
        LogicalPlan::Filter { input, predicate } => {
            if let LogicalPlan::Scan { table, .. } = input.as_ref() {
                if let Some(physical) = build_index_scan(table, &predicate, catalog) {
                    return Ok(physical)
                }
            }
            Ok(PhysicalPlan::Filter { input: Box::new(optimize(*input, catalog)?), predicate })
        }
        LogicalPlan::Join { left, right, join_type, on } => match join_type {
            JoinType::Left => {
                let right_width = right.output_schema().len();
                let left_width = left.output_schema().len();
                let pl = optimize(*left, catalog)?;
                let pr = optimize(*right, catalog)?;
                Ok(build_join(pl, pr, on, join_type, left_width, right_width, catalog))
            }
            JoinType::Inner => reorder_inner_joins(LogicalPlan::Join { left, right, join_type, on }, catalog),
            _ => Err(FerroError::Bind("right/full not implemented".into()))
        }
        LogicalPlan::Projection { input, exprs, .. } => {
            Ok(PhysicalPlan::Projection { input: Box::new(optimize(*input, catalog)?), exprs })
        }
        LogicalPlan::Scan { table, .. } => {
            Ok(PhysicalPlan::SeqScan { table })
        }
    }
}

// physical -> executors
pub fn lower(plan: PhysicalPlan, catalog: &Catalog, bp: Arc<BufferPoolManager>, view: Arc<ReadView>) -> Result<Box<dyn Executor>, FerroError> {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let child = lower(*input, catalog, bp, view)?;
            Ok(Box::new(Filter{child, predicate}))
        }
        PhysicalPlan::SeqScan { table } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Bind(format!("unknown table: {}", table)))?;
            let heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
            let tt_heap = HeapFileManager::open(entry.time_travel_root, bp);
            Ok(Box::new(SeqScan::new(heap.scan(), entry.schema.clone(), view, tt_heap)))
        }
        PhysicalPlan::Projection { input, exprs, .. } => {
            let child = lower(*input, catalog, bp, view)?;
            Ok(Box::new(Projection {child, exprs}))
        }
        PhysicalPlan::NestedLoopJoin { left, right, on, join_type, right_width } => {
            let left_exec = lower(*left, catalog, bp.clone(), view.clone())?;
            let right_exec = lower(*right, catalog, bp, view)?;
            Ok(Box::new(NestedLoopJoin::new(left_exec, right_exec, on, join_type, right_width)))
        }
        PhysicalPlan::IndexScan { table, column, lower, upper } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Bind(format!("unknown table: {}", table)))?;
            let schema = entry.schema.clone();
            let heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
            let tt_heap = HeapFileManager::open(entry.time_travel_root, bp.clone());
            if column == 0 {
                // SHARED root cell (D53). The point-lookup path every indexed read takes was
                // missed when D53 wired plan::open_table: a private cell here means an index scan
                // could descend from a root a concurrent split had already moved.
                let tree = match catalog.root_cell(&table, None) {
                    Some(cell) => BPlusTreeManager::<Value, RecordId>::open_shared(cell, bp),
                    None => BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp),
                };
                let scanner = tree.range_scan(lower, upper)?;
                return Ok(Box::new(IndexScan{heap, scanner, schema, tt_heap, view}))
            } 
            let col_name = schema.columns.get(column).ok_or(FerroError::Bind("unknown column".into()))?.name.clone();
            let sec_root = entry.indexes.iter().find(|i| i.column_name == col_name).ok_or(FerroError::Bind("no index found".into()))?.root_page_id;
            let sec_tree = match catalog.root_cell(&table, Some(&col_name)) {
                Some(cell) => BPlusTreeManager::<(Value, Value), ()>::open_shared(cell, bp.clone()),
                None => BPlusTreeManager::<(Value, Value), ()>::open(sec_root, bp.clone()),
            };
            let primary_index = match catalog.root_cell(&table, None) {
                Some(cell) => BPlusTreeManager::<Value, RecordId>::open_shared(cell, bp.clone()),
                None => BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp.clone()),
            };

            let scan_lower = secondary_scan_lower(&lower)
                .ok_or_else(|| FerroError::Bind("lower bound sec index isn't supported".into()))?;
            let scanner = sec_tree.range_scan(scan_lower, Bound::Unbounded)?;
            Ok(Box::new(SecondaryIndexScan {heap, scanner, primary_index, schema, sec_upper: upper, tt_heap, view, col_index: column}))
        }
        PhysicalPlan::HashJoin { left, right, on, join_type, left_keys, right_keys, right_width } => {
            let left_exec = lower(*left, catalog, bp.clone(), view.clone())?;
            let right_exec = lower(*right, catalog, bp, view)?;
            Ok(Box::new(HashJoin::new(left_exec, right_exec, on, join_type, left_keys, right_keys, right_width)))
        }
    }
}

pub fn build_join(left: PhysicalPlan, right: PhysicalPlan, on: BoundExpr, join_type: JoinType, left_width: usize, right_width: usize, catalog: &Catalog) -> PhysicalPlan{
    let mut pairs = Vec::new();
    equi_pairs(&on, &mut pairs);
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    for (a, b) in &pairs {
        if *a < left_width && *b >= left_width {
            left_keys.push(*a);
            right_keys.push(*b - left_width);
        } else if *b < left_width && *a >= left_width {
            left_keys.push(*b);
            right_keys.push(*a - left_width);
        }
    }

    let lc = cost(&left, catalog);
    let rc = cost(&right, catalog);
    let jstats = join_cardinality(&lc.stats, &rc.stats, &on, &join_type);
    let nlj_marginal = lc.stats.rows * rc.stats.rows * DEFAULT_CPU_TUPLE_COST;
    let hash_marginal = (lc.stats.rows + rc.stats.rows + jstats.rows) * DEFAULT_CPU_TUPLE_COST;
    if !left_keys.is_empty() && hash_marginal < nlj_marginal {
        PhysicalPlan::HashJoin { left: Box::new(left), right: Box::new(right), on, join_type, left_keys, right_keys, right_width }
    } else {
        PhysicalPlan::NestedLoopJoin { left: Box::new(left), right: Box::new(right), on, join_type, right_width }
    }
}

/// The secondary tree's lower bound for a `(value, pk)` key — or `None` when this bound **cannot
/// be expressed at all** against that key shape.
///
/// A secondary index is keyed on `(value, pk)`, so `Bound::Included(v)` becomes `(v, Value::Null)`:
/// `Null` sorts below every pk, so that is exactly the first key whose value is `v`. A *strictly
/// excluded* `v` has no such spelling — it would have to start just past the LAST key with value
/// `v`, and there is no maximum pk to write down. Hence `None` rather than an approximation: the
/// nearest expressible bound is `Included`, which would silently return the rows `> v` was asked to
/// exclude.
///
/// # D178 — this is the SINGLE authority, and that is the point
///
/// This predicate had one consumer: `lower`, which refused and returned an error. `build_index_scan`
/// did not consult it, so the optimizer could **choose a plan that could not then be built**, and on
/// the SELECT path that is a live, reachable defect rather than a latent one. Measured at `fe40276`
/// + D176's counters, in `bench/d178_run1_BEFORE_RAW.txt`, on a 1,000-row table with an index on
/// `v` and `ANALYZE` run:
///
/// ```text
///   SELECT id FROM h WHERE v > 9980 ;  ERROR: binding error: lower bound sec index isn't supported
///   SELECT id FROM h WHERE v > 9900 ;  OK, 9 rows
/// ```
///
/// Same table, same index, same predicate shape — the only difference is that the higher cutoff
/// estimates few enough rows for the index side to win the cost comparison, at which point the
/// engine refuses a query it answers correctly one row-estimate lower. A user sees a statement
/// start failing because their data grew.
///
/// So `build_index_scan` now asks this question BEFORE proposing an `IndexScan`, through
/// [`index_scan_lowerable`], and falls through to the sequential scan it was already costing
/// against. Both callers read this one function; neither carries its own copy of the rule, so they
/// cannot drift apart. `lower`'s refusal STAYS — a hand-built `PhysicalPlan` can still name an
/// unbuildable scan, and `plan::tests::test_index_scan_secondary_rejects_strict_lower` pins that.
///
/// ⚠ What this does NOT do is make `secondary > v` use the index. That needs a real fix in
/// `SecondaryIndexScan` — start at `Included((v, Null))` and skip the `sec == v` run, the mirror of
/// the `sec_upper` check it already carries — and that is a separate row, not this one. Until then
/// the honest outcome is a correct sequential scan instead of an error.
fn secondary_scan_lower(lower: &Bound<Value>) -> Option<Bound<(Value, Value)>> {
    match lower {
        Bound::Included(v) => Some(Bound::Included((v.clone(), Value::Null))),
        Bound::Unbounded => Some(Bound::Unbounded),
        Bound::Excluded(_) => None,
    }
}

/// Can [`lower`] actually build an `IndexScan` on `column` with this lower bound?
///
/// Column 0 is the primary tree, keyed on the value alone, so every bound is expressible there.
/// Everything else is a `(value, pk)` secondary tree and defers to [`secondary_scan_lower`].
fn index_scan_lowerable(column: usize, lower: &Bound<Value>) -> bool {
    column == 0 || secondary_scan_lower(lower).is_some()
}

fn build_index_scan(table: &str, predicate: &BoundExpr, catalog: &Catalog) -> Option<PhysicalPlan> {
    let mut conjuncts = Vec::new();
    let entry = catalog.get_table(table)?;
    split_and(predicate.clone(), &mut conjuncts);
    // D178 — an index on the column is necessary but NOT sufficient: the bound has to be one
    // `lower` can express against that tree's key. A conjunct that fails this is passed over and
    // the next indexed one is considered, so `sec > v AND id = 7` still reaches the primary index
    // instead of falling all the way back to a sequential scan.
    let chosen = conjuncts.iter().position(|c| {
        predicate_to_bounds(c)
            .is_some_and(|(col, lo, _)| has_index(entry, col) && index_scan_lowerable(col, &lo))
    })?;
    let index_conjunct = conjuncts.remove(chosen);
    let (column, lower, upper) = predicate_to_bounds(&index_conjunct)?;
    let scan = PhysicalPlan::IndexScan { table: table.into(), column, lower, upper };
    let candidate = if conjuncts.is_empty() {
        scan
    } else {
        PhysicalPlan::Filter { input: Box::new(scan), predicate: combine_and(conjuncts) }
    };

    let seq = PhysicalPlan::Filter { input: Box::new(PhysicalPlan::SeqScan { table: table.into() }), predicate: predicate.clone() };
    if cost(&candidate, catalog).cost < cost(&seq, catalog).cost {
        return Some(candidate)
    }
    Some(seq)
}

fn has_index(entry: &TableEntry, col: usize) -> bool {
    col == 0 || entry.schema.columns.get(col).is_some_and(|c| entry.indexes.iter().any(|i| i.column_name == c.name))
}

pub fn split_and(expr: BoundExpr, output: &mut Vec<BoundExpr>) {
    match expr {
        BoundExpr::BinaryOp { left, operator:TokenType::And, right } => {
            split_and(*left, output);
            split_and(*right, output);
        }
        other => output.push(other),
    }
}

pub fn combine_and(mut conjuncts: Vec<BoundExpr>) -> BoundExpr {
    let mut combined = conjuncts.remove(0);
    for conjunct in conjuncts {
        combined = BoundExpr::BinaryOp { left: Box::new(combined), operator: TokenType::And, right: Box::new(conjunct)
        }
    }
    combined
}

pub fn collect_columns(expr: &BoundExpr, output: &mut HashSet<usize>) {
    match expr {
        BoundExpr::BinaryOp { left, right, .. } => {
            collect_columns(left, output);
            collect_columns(right, output);
        }
        BoundExpr::UnaryOp { right, .. } => collect_columns(right, output),
        
        BoundExpr::Column(i) => {output.insert(*i);}
        BoundExpr::Literal(_) => {}
    }
}

pub fn remap(expr: BoundExpr, offset: usize) -> BoundExpr {
    match expr {
        BoundExpr::BinaryOp { left, operator, right } => { BoundExpr::BinaryOp { left: Box::new(remap(*left, offset)), operator, right: Box::new(remap(*right, offset))} }
        BoundExpr::UnaryOp { operator, right } => { BoundExpr::UnaryOp { operator, right: Box::new(remap(*right, offset)) } }
        BoundExpr::Literal(v) => BoundExpr::Literal(v),
        BoundExpr::Column(i) => BoundExpr::Column(i-offset)
    }
}

pub fn wrap_filter(plan: LogicalPlan, conjuncts: Vec<BoundExpr>) -> LogicalPlan {
    if conjuncts.is_empty() {
        plan
    } else {
        LogicalPlan::Filter { input: Box::new(plan), predicate: combine_and(conjuncts) }
    }
}

pub fn push(plan: LogicalPlan, carried: Vec<BoundExpr>) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let mut c = carried;
            split_and(predicate, &mut c);
            push(*input, c)
        }
        LogicalPlan::Join { left, right, join_type, on } => {
            let left_width = left.output_schema().len();
            let (mut go_left, mut go_right, mut stay) = (Vec::new(), Vec::new(), Vec::new());
            for expr in carried {
                let mut cols = HashSet::new();
                collect_columns(&expr, &mut cols);
                if cols.is_empty() {
                    stay.push(expr);
                } else if cols.iter().all(|&c| c < left_width) {
                    go_left.push(expr);
                } else if cols.iter().all(|&c| c >= left_width) {
                    go_right.push(remap(expr, left_width));
                } else {
                    stay.push(expr);
                }
            }

            let joined = LogicalPlan::Join { left: Box::new(push(*left, go_left)), right: Box::new(push(*right, go_right)), join_type, on };
            wrap_filter(joined, stay)
        }
        LogicalPlan::Projection { input, exprs, output } => {
            let inner = push(*input, Vec::new());
            let proj = LogicalPlan::Projection { input: Box::new(inner), exprs, output };
            wrap_filter(proj, carried)
        }
        LogicalPlan::Scan { .. } => wrap_filter(plan, carried)
    }
}

pub fn pushdown(plan: LogicalPlan) -> LogicalPlan {
    push(plan, Vec::new())
}

pub fn explain_plan(plan: &PhysicalPlan, catalog: &Catalog) -> String {
    let mut out = String::new();
    format_node(plan, catalog, 0, &mut out);
    out
}    

fn format_node(plan: &PhysicalPlan, catalog: &Catalog, indent: usize, out: &mut String) {
    let pad = "  ".repeat(indent);
    let text = match plan {
        PhysicalPlan::Filter { predicate, .. } => format!("Filter ({})", format_expr(predicate)),
        PhysicalPlan::IndexScan { table, column, lower, upper } => format!("Index scan on {} (col {}, {})", table, column, format_bounds(lower,upper)),
        PhysicalPlan::NestedLoopJoin {  on, join_type, .. } => format!("Nested loop join {:?} (on {})", join_type, format_expr(on)),
        PhysicalPlan::Projection { exprs, .. } => format!("Projection [{}]", exprs.iter().map(|e| format_expr(e)).collect::<Vec<_>>().join(", ")),
        PhysicalPlan::SeqScan { table } => format!("Sequential scan on {}", table),
        PhysicalPlan::HashJoin { join_type, on, .. } => format!("Hash join {:?} (on {})", join_type, format_expr(on)),
    };
    let costed = cost(plan, catalog);
    out.push_str(&format!("{}{} (rows={:.0} cost={:.2})\n", pad, text, costed.stats.rows, costed.cost));
    match plan {
        PhysicalPlan::Filter { input, .. } => format_node(input, catalog, indent + 1, out),
        PhysicalPlan::IndexScan { .. } => {}
        PhysicalPlan::NestedLoopJoin { left, right, .. }  | PhysicalPlan::HashJoin { left, right,.. } => {
            format_node(left, catalog, indent + 1, out);
            format_node(right, catalog, indent + 1, out);
        }
        PhysicalPlan::Projection { input, .. } => format_node(input, catalog, indent + 1, out),
        PhysicalPlan::SeqScan { .. } => {},
    }
}

fn format_expr(e: &BoundExpr) -> String {
    match e {
        BoundExpr::BinaryOp { left, operator, right } => {
            format!("{} {} {}", format_expr(left), op_symbol(*operator), format_expr(right))
        }
        BoundExpr::Column(i) => format!("#{}", i),
        BoundExpr::UnaryOp { operator, right } => {
            format!("{} {}", op_symbol(*operator), format_expr(right))
        }
        BoundExpr::Literal(v) => format_value(v)
    }
}

fn op_symbol(op: TokenType) -> &'static str {
    match op {
        TokenType::Equal => "=",
        TokenType::BangEqual => "!=",
        TokenType::Less => "<",
        TokenType::LessEqual => "<=",
        TokenType::Greater => ">",
        TokenType::GreaterEqual => ">=",
        TokenType::And => "AND",
        TokenType::Or => "OR",
        TokenType::Not => "NOT",
        _ => "?"
    }
}

fn format_value(v: &Value) -> String {
    match v {
        Value::Boolean(b) => b.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::Timestamp(ms) => ms.to_string(),
        Value::Null => "NULL".into(),
        Value::Varchar(s) => format!("'{}'", s)
    }
}

fn format_bounds(lower: &Bound<Value>, upper: &Bound<Value>) -> String {
    if let (Bound::Included(l), Bound::Excluded(h)) = (lower, upper) {
        if l == h { return format!("= {}", format_value(l));}
    }
    let l = match lower {
        Bound::Excluded(v) => format!("({}", format_value(v)),
        Bound::Included(v) => format!("[{}", format_value(v)),
        Bound::Unbounded => "(-inf".into()
    };
    let u = match upper {
        Bound::Excluded(v) => format!("{})", format_value(v)),
        Bound::Included(v) => format!("{}]", format_value(v)),
        Bound::Unbounded => "inf)".into()
    };
    format!("{}, {}", l, u)
}

#[cfg(test)]
mod tests {
    use crate::{binder::binder::BoundColumn, catalog::column::DataType};
    use super::*;

    #[test]
    fn test_split_and_roundtrip() {
        let expr = BoundExpr::BinaryOp { left: Box::new(
            BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(1)), operator: TokenType::And, right: Box::new(BoundExpr::Column(3)) }
        ), operator: TokenType::And, right: Box::new(BoundExpr::Column(2)) };
        let mut output: Vec<BoundExpr> = Vec::new();
        split_and(expr, &mut output);
        assert_eq!(output, vec![BoundExpr::Column(1), BoundExpr::Column(3), BoundExpr::Column(2) ]);
    }

    #[test]
    fn test_combine_column() {
        let conjuncts = vec![BoundExpr::Column(1), BoundExpr::Column(3), BoundExpr::Column(2)];
        let expr = BoundExpr::BinaryOp { left: Box::new(
            BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(1)), operator: TokenType::And, right: Box::new(BoundExpr::Column(3)) }
        ), operator: TokenType::And, right: Box::new(BoundExpr::Column(2)) };
        let combined = combine_and(conjuncts);
        assert_eq!(combined, expr);
    }

    #[test]
    fn test_collect_columns() {
        let mut output = HashSet::new();
        let expr = BoundExpr::BinaryOp { 
            left: Box::new(BoundExpr::UnaryOp { 
                operator: TokenType::Not, 
                right: Box::new(BoundExpr::Literal(Value::Varchar("idk".into()))) 
            }), 
            operator: TokenType::Or, 
            right: Box::new(BoundExpr::BinaryOp { 
                left: Box::new(BoundExpr::Column(1)), 
                operator: TokenType::And, 
                right: Box::new(BoundExpr::Column(3)), 
            }), 
        };
        collect_columns(&expr, &mut output);
        assert_eq!(output, HashSet::from([1, 3]))
    }

    #[test]
    fn test_remap() {
        let expr = BoundExpr::UnaryOp { operator: TokenType::Not, right: Box::new(BoundExpr::Column(3)) };
        assert_eq!(remap(expr, 2), BoundExpr::UnaryOp { operator: TokenType::Not, right: Box::new(BoundExpr::Column(1)) });
        let nested = BoundExpr::BinaryOp { 
            left: Box::new(BoundExpr::UnaryOp { operator: TokenType::Not, right: Box::new(BoundExpr::Column(5))}), 
            operator: TokenType::Not, 
            right: Box::new(BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(4)), operator: TokenType::And, right: Box::new(BoundExpr::Column(3)) })
        };
        assert_eq!(remap(nested, 2), BoundExpr::BinaryOp { 
            left: Box::new(BoundExpr::UnaryOp { operator: TokenType::Not, right: Box::new(BoundExpr::Column(3))}), 
            operator: TokenType::Not, 
            right: Box::new(BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(2)), operator: TokenType::And, right: Box::new(BoundExpr::Column(1)) })
        })
    }

    #[test]
    fn test_wrap_filter_empty_conjuncts() {
        let plan = LogicalPlan::Projection { input: 
            Box::new(LogicalPlan::Scan { table: "users".into(), alias: Some("u".into()), output: vec![]}), 
            exprs: vec![BoundExpr::Column(1)], 
            output: vec![BoundColumn {qualifier: "p".into(), name: "p".into(), data_type: DataType::Integer, nullable: true}]
        };
        let res = wrap_filter(plan.clone(), vec![]);
        assert_eq!(res, plan);
    }

    #[test]
    fn test_wrap_filter_non_empty_conjuncts() {
        let plan = LogicalPlan::Projection { input: 
            Box::new(LogicalPlan::Scan { table: "users".into(), alias: Some("u".into()), output: vec![]}), 
            exprs: vec![BoundExpr::Column(1)], 
            output: vec![BoundColumn {qualifier: "p".into(), name: "p".into(), data_type: DataType::Integer, nullable: true}]
        };
        let res = wrap_filter(plan.clone(), vec![BoundExpr::Column(1), BoundExpr::Column(2)]);
        assert_eq!(res, LogicalPlan::Filter { input: Box::new(plan), predicate: BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(1)), operator: TokenType::And, right: Box::new(BoundExpr::Column(2)) } })
    }

    #[test]
    fn test_push_split_remap() {
        let c = BoundColumn { qualifier: "t".into(), name: "x".into(), data_type: DataType::Integer, nullable: true};
        let plan = LogicalPlan::Filter { 
            input: Box::new(LogicalPlan::Join { 
                left: Box::new(LogicalPlan::Scan { table: "users".into(), alias: None, output: vec![c.clone(), c.clone()]}),  
                right: Box::new(LogicalPlan::Scan { table: "posts".into(), alias: None, output: vec![c.clone(), c.clone(), c.clone()] }), 
                join_type: JoinType::Inner, 
                on: BoundExpr::Literal(Value::Boolean(true))
            }), 
            predicate: BoundExpr::BinaryOp { 
                left: Box::new(BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(0)), operator: TokenType::Equal, right: Box::new(BoundExpr::Literal(Value::Integer(5))) }), 
                operator: TokenType::And, 
                right: Box::new(BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(3)), operator: TokenType::Equal, right: Box::new(BoundExpr::Literal(Value::Integer(7))) })
            }
        };
        match pushdown(plan) {
            LogicalPlan::Join { left, right, .. } => match (*left, *right) {
                (LogicalPlan::Filter { input: li, predicate: lp }, LogicalPlan::Filter { input: ri, predicate: rp }) => {
                    assert_eq!(lp, BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(0)), operator: TokenType::Equal, right: Box::new(BoundExpr::Literal(Value::Integer(5))) });
                    assert!(matches!(*li, LogicalPlan::Scan { table, .. } if table == "users"));
                    assert_eq!(rp, BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(1)), operator: TokenType::Equal, right: Box::new(BoundExpr::Literal(Value::Integer(7))) });
                    assert!(matches!(*ri, LogicalPlan::Scan { table, ..} if table == "posts"));
                }   
                _ => panic!()
            }
            _ => panic!()
        }
    }

    #[test]
    fn test_push_spanning_predicate_stays() {
        let c = BoundColumn { qualifier: "t".into(), name: "x".into(), data_type: DataType::Integer, nullable: true};
        let spanning = BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(0)), operator: TokenType::Greater, right: Box::new(BoundExpr::Column(3)) };
        let plan = LogicalPlan::Join { 
            left: Box::new(LogicalPlan::Scan { table: "users".into(), alias: None, output: vec![c.clone(), c.clone()] }), 
            right: Box::new(LogicalPlan::Scan { table: "posts".into(), alias: None, output: vec![c.clone(), c.clone(), c.clone()] }), 
            join_type: JoinType::Inner, 
            on: BoundExpr::Literal(Value::Boolean(true)) 
        };
        match push(plan, vec![spanning.clone()]) {
            LogicalPlan::Filter { input, predicate } => {
                assert_eq!(predicate, spanning);
                assert!(matches!(*input, LogicalPlan::Join{..}));
            }
            _ => panic!()
        }
    }
}