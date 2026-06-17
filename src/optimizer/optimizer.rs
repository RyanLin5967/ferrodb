use std::{collections::HashSet, ops::Bound, sync::Arc};

use crate::{binder::binder::BoundExpr, buffer::buffer_pool::BufferPoolManager, catalog::{catalog::Catalog, column::Value}, error::FerroError, execution::{executor::Executor, filter::Filter, index_scan::IndexScan, nested_loop_join::NestedLoopJoin, projection::Projection, sec_index_scan::SecondaryIndexScan, seq_scan::SeqScan}, parser::{parser::JoinType, scanner::TokenType}, planner::{logical_plan::LogicalPlan, physical_plan::PhysicalPlan}, storage::{heap_file_manager::{HeapFileManager, RecordId}, index::BPlusTreeManager}};

// 1:1 for now
pub fn optimize(lp: LogicalPlan, catalog: &Catalog) -> Result<PhysicalPlan, FerroError> {
    match lp {
        LogicalPlan::Filter { input, predicate } => Ok(PhysicalPlan::Filter { input: Box::new(optimize(*input, catalog)?), predicate }),
        LogicalPlan::Join { left, right, join_type, on } => match join_type {
            JoinType::Inner | JoinType::Left => {
                let right_width = right.output_schema().len();
                Ok(PhysicalPlan::NestedLoopJoin { left: Box::new(optimize(*left, catalog)?), right: Box::new(optimize(*right, catalog)?), on, join_type, right_width })
            }
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
pub fn lower(plan: PhysicalPlan, catalog: &Catalog, bp: Arc<BufferPoolManager>) -> Result<Box<dyn Executor>, FerroError> {
    match plan {
        PhysicalPlan::Filter { input, predicate } => {
            let child = lower(*input, catalog, bp)?;
            Ok(Box::new(Filter{child, predicate}))
        }
        PhysicalPlan::SeqScan { table } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Bind(format!("unknown table: {}", table)))?;
            let heap = HeapFileManager::open(entry.first_directory_page_id, bp);
            Ok(Box::new(SeqScan { scanner: heap.scan(), schema: entry.schema.clone()}))
        }
        PhysicalPlan::Projection { input, exprs, .. } => {
            let child = lower(*input, catalog, bp)?;
            Ok(Box::new(Projection {child, exprs}))
        }
        PhysicalPlan::NestedLoopJoin { left, right, on, join_type, right_width } => {
            let left_exec = lower(*left, catalog, bp.clone())?;
            let right_exec = lower(*right, catalog, bp)?;
            Ok(Box::new(NestedLoopJoin::new(left_exec, right_exec, on, join_type, right_width)))
        }
        PhysicalPlan::IndexScan { table, column, lower, upper } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Bind(format!("unknown table: {}", table)))?;
            let schema = entry.schema.clone();
            let heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
            if column == 0 {
                let tree = BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp);
                let scanner = tree.range_scan(lower, upper)?;
                return Ok(Box::new(IndexScan{heap, scanner, schema}))
            } 
            let col_name = schema.columns.get(column).ok_or(FerroError::Bind("unknown column".into()))?.name.clone();
            let sec_root = entry.indexes.iter().find(|i| i.column_name == col_name).ok_or(FerroError::Bind("no index found".into()))?.root_page_id;
            let sec_tree = BPlusTreeManager::<(Value, Value), ()>::open(sec_root, bp.clone());
            let primary_index = BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp);

            let scan_lower = match lower {
                Bound::Excluded(_) => return Err(FerroError::Bind("lower bound sec index isn't supported".into())),
                Bound::Included(v) => Bound::Included((v, Value::Null)),
                Bound::Unbounded => Bound::Unbounded
            };
            let scanner = sec_tree.range_scan(scan_lower, Bound::Unbounded)?;
            Ok(Box::new(SecondaryIndexScan {heap, scanner, primary_index, schema, sec_upper: upper}))
        }
    }
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