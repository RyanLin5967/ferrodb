use std::{ops::Bound, sync::Arc};

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

pub fn predicate_to_bounds(pred: &BoundExpr) -> Option<(usize, Bound<Value>, Bound<Value>)> {
    let BoundExpr::BinaryOp { left, operator, right } = pred else { return None; };
    let (col, op, val) = match (&**left, &**right) {
        (BoundExpr::Column(c), BoundExpr::Literal(v)) => (*c, *operator, v.clone()),
        (BoundExpr::Literal(v), BoundExpr::Column(c)) => (*c, flip(*operator)?, v.clone()),
        _ => return None
    };
    let (lower, upper) = match op {
        TokenType::Equal => (Bound::Included(val.clone()), Bound::Included(val)),
        TokenType::Less => (Bound::Unbounded, Bound::Excluded(val)),
        TokenType::LessEqual => (Bound::Unbounded, Bound::Included(val)),
        TokenType::Greater => (Bound::Excluded(val), Bound::Unbounded),
        TokenType::GreaterEqual => (Bound::Included(val), Bound::Unbounded),
        _ => return None
    };
    Some((col, lower, upper))
}

fn flip(op: TokenType) -> Option<TokenType> {
    match op {
        TokenType::Less => Some(TokenType::Greater),
        TokenType::LessEqual => Some(TokenType::GreaterEqual),
        TokenType::Greater => Some(TokenType::Less),
        TokenType::GreaterEqual => Some(TokenType::LessEqual),
        TokenType::Equal => Some(TokenType::Equal),
        _ => return None
    }
}