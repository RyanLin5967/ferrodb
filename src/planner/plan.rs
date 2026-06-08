use crate::{buffer::buffer_pool::BufferPoolManager, catalog::{catalog::Catalog, catalog_page::TableEntry, column::Value}, error::FerroError, execution::{delete::Delete, executor::Executor, filter::Filter, index_handle::IndexHandle, insert::Insert, projection::Projection, seq_scan::SeqScan, update::Update}, parser::parser::{Expr, Stmt}, storage::{heap_file_manager::{HeapFileManager, RecordId}, index::BPlusTreeManager}};
use std::sync::Arc;
use crate::execution::executor::Modify;

pub enum Plan {
    Read(Box<dyn Executor>),
    Write(Box<dyn Modify>),
}

// for dml
pub fn plan(stmt: Stmt, catalog: &Catalog, bp: Arc<BufferPoolManager>) -> Result<Plan, FerroError> {
    
    match stmt {
        // for now always use seq scan
        Stmt::Select { table, columns, where_clause } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Parse("table not found".into()))?;
            let scan = build_scan(entry, where_clause, bp)?;
            let is_star = columns.len() == 1 && matches!(&columns[0], Expr::ColumnRef(name) if name == "*");
            let node: Box<dyn Executor> = if is_star {
                scan
            } else {
                Box::new(Projection { child: scan, columns, schema: entry.schema.clone()})
            };
            return Ok(Plan::Read(node))
        }
        Stmt::Delete { table, where_clause } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Parse("table not found".into()))?;
            let (heap, tree, handles) = open_table(entry, bp.clone())?;
            let scan = build_scan(entry, where_clause, bp)?;
            let delete = Delete {table, child: scan, heap, schema: entry.schema.clone(), primary_index: tree, secondary_indexes: handles};
            return Ok(Plan::Write(Box::new(delete)))
        }
        Stmt::Insert { table, values } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Parse("table not found".into()))?;
            let (heap, tree, handles) = open_table(entry, bp)?;
            let insert = Insert {table, values, heap, schema: entry.schema.clone(), primary_index: tree, secondary_indexes: handles};
            return Ok(Plan::Write(Box::new(insert)))
        }
        Stmt::Update { table, assignments, where_clause } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Parse("table not found".into()))?;
            let (heap, tree, handles) = open_table(entry, bp.clone())?;
            let child = build_scan(entry, where_clause, bp)?;
            let mut resolved = Vec::with_capacity(assignments.len());
            for (name, expr) in assignments {
                let idx = entry.schema.columns.iter().position(|c| c.name == name).ok_or(FerroError::Parse(format!("unkonwn column: {}", name)))?;
                resolved.push((idx, expr));
            }
            let update = Update {table, child, schema: entry.schema.clone(), assignments: resolved, heap, primary_index: tree, secondary_indexes: handles};
            return Ok(Plan::Write(Box::new(update)))
        }
        _ => return Err(FerroError::OnlyDML)
    }
}   

// opens heapfilemanager twice (could cause errors)
fn open_table(entry: &TableEntry, bp: Arc<BufferPoolManager>) -> Result<(HeapFileManager, BPlusTreeManager<Value, RecordId>, Vec<IndexHandle>), FerroError> {
    let heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
    let tree = BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp.clone());
    let mut handles = Vec::with_capacity(entry.indexes.len());
    for info in &entry.indexes {
        let col_index = entry.schema.columns.iter().position(|c| c.name == info.column_name).ok_or(FerroError::KeyNotFound)?;
        let tree = BPlusTreeManager::<(Value, Value), ()>::open(info.root_page_id, bp.clone());
        handles.push(IndexHandle{col_index, tree})
    }
    Ok((heap, tree, handles))
}

fn build_scan(entry: &TableEntry, where_clause: Option<Expr>, bp: Arc<BufferPoolManager>) -> Result<Box<dyn Executor>, FerroError> {
    let heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
    let scanner = heap.scan();
    let mut node: Box<dyn Executor> = Box::new(SeqScan {
        scanner, schema: entry.schema.clone(),
    });
    if let Some(pred) = where_clause {
        node = Box::new(Filter { child: node, predicate: pred, schema: entry.schema.clone()})
    }
    Ok(node)
}