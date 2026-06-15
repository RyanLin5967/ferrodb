use crate::{binder::binder::{Binder, BoundExpr, Scope}, buffer::buffer_pool::BufferPoolManager, catalog::{catalog::Catalog, catalog_page::TableEntry, column::Value}, error::FerroError, execution::{delete::Delete, executor::Executor, filter::Filter, index_handle::IndexHandle, insert::Insert, seq_scan::SeqScan, update::Update}, optimizer::optimizer::{lower, optimize}, parser::parser::Stmt, storage::{heap_file_manager::{HeapFileManager, RecordId}, index::BPlusTreeManager}};
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
        Stmt::Select { .. } => { // JOIN
            let logical = Binder::new(catalog).bind(stmt)?;
            let physical = optimize(logical, catalog)?;
            Ok(Plan::Read(lower(physical, catalog, bp)?))
        }
        Stmt::Delete { table, where_clause } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Parse("table not found".into()))?;
            let (heap, tree, handles) = open_table(entry, bp.clone())?;
            let bound_where = match where_clause {
                Some(w) => {
                    let binder = Binder::new(catalog);
                    let scope = single_table_scope(catalog, &table)?;
                    Some(binder.bind_expr(w, &scope)?)
                }
                None => None
            };
            let scan = build_scan(entry, bound_where, bp)?;
            let delete = Delete {table, child: scan, heap, schema: entry.schema.clone(), primary_index: tree, secondary_indexes: handles};
            return Ok(Plan::Write(Box::new(delete)))
        }
        Stmt::Insert { table, values } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Parse("table not found".into()))?;
            let (heap, tree, handles) = open_table(entry, bp)?;
            let binder = Binder::new(catalog);
            let empty = Scope::new();
            let mut bound_vals = Vec::with_capacity(values.len());
            for v in values {
                bound_vals.push(binder.bind_expr(v, &empty)?);
            }
            let insert = Insert {table, values: bound_vals, heap, schema: entry.schema.clone(), primary_index: tree, secondary_indexes: handles};
            return Ok(Plan::Write(Box::new(insert)))
        }
        Stmt::Update { table, assignments, where_clause } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Parse("table not found".into()))?;
            let (heap, tree, handles) = open_table(entry, bp.clone())?;
            let binder = Binder::new(catalog);
            let scope = single_table_scope(catalog, &table)?;
            let mut resolved = Vec::with_capacity(assignments.len());
            for (name, expr) in assignments {
                let idx = entry.schema.columns.iter().position(|c| c.name == name).ok_or(FerroError::Parse(format!("unknown column: {}", name)))?;
                resolved.push((idx, binder.bind_expr(expr, &scope)?));
            }
            let bound_where = match where_clause {
                Some(w) => Some(binder.bind_expr(w, &scope)?),
                None => None
            };
            let child = build_scan(entry, bound_where, bp)?;
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

fn build_scan(entry: &TableEntry, predicate: Option<BoundExpr>, bp: Arc<BufferPoolManager>) -> Result<Box<dyn Executor>, FerroError> {
    let heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
    let scanner = heap.scan();
    let mut node: Box<dyn Executor> = Box::new(SeqScan {
        scanner, schema: entry.schema.clone(),
    });
    if let Some(pred) = predicate {
        node = Box::new(Filter { child: node, predicate: pred})
    }
    Ok(node)
}

fn single_table_scope(catalog: &Catalog, table: &str) -> Result<Scope, FerroError> {
    let entry = catalog.get_table(table).ok_or(FerroError::Parse(format!("unknown table: {}", table)))?;
    let mut scope = Scope::new();
    scope.add_table(table, &entry.schema)?;
    Ok(scope)
}