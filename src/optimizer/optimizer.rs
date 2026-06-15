use std::sync::Arc;

use crate::{buffer::buffer_pool::BufferPoolManager, catalog::catalog::Catalog, error::FerroError, execution::{executor::Executor, filter::Filter, nested_loop_join::NestedLoopJoin, projection::Projection, seq_scan::SeqScan}, parser::parser::JoinType, planner::{logical_plan::LogicalPlan, physical_plan::PhysicalPlan}, storage::heap_file_manager::HeapFileManager};

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
    }
}