use crate::{binder::binder::{Binder, BoundExpr, Scope}, buffer::buffer_pool::BufferPoolManager, catalog::{catalog::Catalog, catalog_page::TableEntry, column::Value}, error::FerroError, execution::{delete::Delete, executor::Executor, filter::Filter, index_handle::IndexHandle, insert::Insert, seq_scan::SeqScan, update::Update}, optimizer::optimizer::{explain_plan, lower, optimize, pushdown}, parser::{parser::Stmt, scanner::TokenType}, storage::{heap_file_manager::{HeapFileManager, RecordId}, index::BPlusTreeManager}, wal::txn::{ReadView, TxnManager}};
use std::{ops::Bound, sync::Arc};
use crate::execution::executor::Modify;

pub enum Plan {
    Read(Box<dyn Executor>),
    Write(Box<dyn Modify>),
}

// for dml
pub fn plan(stmt: Stmt, catalog: &Catalog, bp: Arc<BufferPoolManager>, txn_ctx: Option<(Arc<TxnManager>, u64)>, view: Arc<ReadView>) -> Result<Plan, FerroError> {
    
    match stmt {
        Stmt::Select { .. } => {
            let logical = Binder::new(catalog).bind(stmt)?;
            let logical = pushdown(logical);
            let physical = optimize(logical, catalog)?;
            Ok(Plan::Read(lower(physical, catalog, bp, view)?))
        }
        Stmt::Delete { table, where_clause } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Parse("table not found".into()))?;
            let (txn, txn_id) = txn_ctx.ok_or(FerroError::Wal("no transaction for delete".into()))?;
            let (heap, tree, handles) = open_table(entry, bp.clone(), txn, txn_id)?;
            let bound_where = match where_clause {
                Some(w) => {
                    let binder = Binder::new(catalog);
                    let scope = single_table_scope(catalog, &table)?;
                    Some(binder.bind_expr(w, &scope)?)
                }
                None => None
            };
            let scan = build_scan(entry, bound_where, bp, view)?;
            let delete = Delete {table, child: scan, heap, schema: entry.schema.clone(), primary_index: tree, secondary_indexes: handles};
            return Ok(Plan::Write(Box::new(delete)))
        }
        Stmt::Insert { table, values } => {
            let entry = catalog.get_table(&table).ok_or(FerroError::Parse("table not found".into()))?;
            let (txn, txn_id) = txn_ctx.ok_or(FerroError::Wal("no transaction for delete".into()))?;
            let (heap, tree, handles) = open_table(entry, bp, txn, txn_id)?;
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
            let (txn, txn_id) = txn_ctx.ok_or(FerroError::Wal("no transaction for delete".into()))?;
            let (heap, tree, handles) = open_table(entry, bp.clone(), txn, txn_id)?;
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
            let child = build_scan(entry, bound_where, bp, view)?;
            let update = Update {table, child, schema: entry.schema.clone(), assignments: resolved, heap, primary_index: tree, secondary_indexes: handles};
            return Ok(Plan::Write(Box::new(update)))
        }
        _ => return Err(FerroError::OnlyDML)
    }
}   

pub fn explain(stmt: Stmt, catalog: &Catalog) -> Result<String, FerroError> {
    if !matches!(stmt, Stmt::Select { .. }) {
        return Err(FerroError::Bind("EXPLAIN only suports SELECT".into()));
    }
    let logical = Binder::new(catalog).bind(stmt)?;
    let logical = pushdown(logical);
    let physical = optimize(logical, catalog)?;
    Ok(explain_plan(&physical, catalog))
}

// opens heapfilemanager twice (could cause errors)
fn open_table(entry: &TableEntry, bp: Arc<BufferPoolManager>, txn: Arc<TxnManager>, txn_id: u64) -> Result<(HeapFileManager, BPlusTreeManager<Value, RecordId>, Vec<IndexHandle>), FerroError> {
    let mut heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
    heap.set_transaction(txn, txn_id);
    let tree = BPlusTreeManager::<Value, RecordId>::open(entry.primary_index_root, bp.clone());
    let mut handles = Vec::with_capacity(entry.indexes.len());
    for info in &entry.indexes {
        let col_index = entry.schema.columns.iter().position(|c| c.name == info.column_name).ok_or(FerroError::KeyNotFound)?;
        let tree = BPlusTreeManager::<(Value, Value), ()>::open(info.root_page_id, bp.clone());
        handles.push(IndexHandle{col_index, tree})
    }
    Ok((heap, tree, handles))
}

fn build_scan(entry: &TableEntry, predicate: Option<BoundExpr>, bp: Arc<BufferPoolManager>, view: Arc<ReadView>) -> Result<Box<dyn Executor>, FerroError> {
    let heap = HeapFileManager::open(entry.first_directory_page_id, bp.clone());
    let tt_heap = HeapFileManager::open(entry.time_travel_root, bp.clone());
    let scanner = heap.scan();
    let mut node: Box<dyn Executor> = Box::new(SeqScan {
        scanner, schema: entry.schema.clone(), tt_heap, view
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

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, fs::OpenOptions};

use tempfile::tempdir;

use crate::{execution::{executor::run, session::Session}, parser::{parser::Parser, scanner::{Scanner, TokenType}}, planner::physical_plan::PhysicalPlan, storage::disk_manager::DiskManager, wal::{log::WalManager, txn::Snapshot}};
    use super::*;

    fn run_sql(sql: &str, catalog: &mut Catalog, bp: Arc<BufferPoolManager>, txn: Arc<TxnManager>) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let stmts = p.parse();
        let mut session = Session::new();
        for stmt in stmts {
            run(stmt, catalog, bp.clone(), txn.clone(), &mut session).unwrap();
        }
    }

    fn setup() -> (Catalog, Arc<BufferPoolManager>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("plan.db");
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let mut catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("test.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal, bp.clone()));
        run_sql("CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(50));", &mut catalog, bp.clone(), txn.clone());
        for s in [
            "INSERT INTO users VALUES (1, 'a');",
            "INSERT INTO users VALUES (2, 'b');",
            "INSERT INTO users VALUES (3, 'c');",
            "INSERT INTO users VALUES (4, 'd');",
            "INSERT INTO users VALUES (5, 'e');",
        ] {
            run_sql(s, &mut catalog, bp.clone(), txn.clone());
        }
        run_sql("CREATE INDEX idk ON users (name);", &mut catalog, bp.clone(), txn.clone());
        (catalog, bp, dir)
    }

    fn col_op_lit(c: usize, op: TokenType, v: Value) -> BoundExpr {
        BoundExpr::BinaryOp{ left:Box::new(BoundExpr::Column(c)), operator: op, right: Box::new(BoundExpr::Literal(v))}
    }

    fn drain(mut exec: Box<dyn Executor>) -> Vec<Vec<Value>> {
        let mut output = Vec::new();
        while let Some(r) = exec.next() {
            output.push(r.unwrap().1);
        }
        output
    }

    #[test]
    fn test_bounds_basic() {
        assert_eq!(predicate_to_bounds(&col_op_lit(0, TokenType::Equal, Value::Integer(5))), Some((0, Bound::Included(Value::Integer(5)), Bound::Included(Value::Integer(5)))));
        assert_eq!(predicate_to_bounds(&col_op_lit(2, TokenType::Less, Value::Integer(10))), Some((2, Bound::Unbounded, Bound::Excluded(Value::Integer(10)))));
        assert_eq!(predicate_to_bounds(&col_op_lit(1, TokenType::GreaterEqual, Value::Integer(3))), Some((1, Bound::Included(Value::Integer(3)), Bound::Unbounded)));
    }

    #[test]
    fn test_bounds_flipped() {
        let pred = BoundExpr::BinaryOp { left: Box::new(BoundExpr::Literal(Value::Integer(5))), operator: TokenType::Less, right: Box::new(BoundExpr::Column(0)) };
        assert_eq!(predicate_to_bounds(&pred), Some((0, Bound::Excluded(Value::Integer(5)), Bound::Unbounded)));
    }

    #[test]
    fn test_bounds_invalid() {
        assert_eq!(predicate_to_bounds(&col_op_lit(0, TokenType::BangEqual, Value::Integer(0))), None);
        let pred = BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(1)), operator: TokenType::Less, right: Box::new(BoundExpr::Column(0)) };
        assert_eq!(predicate_to_bounds(&pred), None);
        assert_eq!(predicate_to_bounds(&BoundExpr::Literal(Value::Integer(1))), None);
    }

    #[test]
    fn test_index_scan_point() {
        let (c, bp, _d) = setup();
        let plan= PhysicalPlan::IndexScan { table: "users".into(), column: 0, lower: Bound::Included(Value::Integer(2)), upper: Bound::Included(Value::Integer(2)) };
        let rows = drain(lower(plan, &c, bp, Arc::new(ReadView { snapshot: Snapshot {high_water: 0, active: HashSet::new()}, txn_id: 0 })).unwrap());
        assert_eq!(rows, vec![vec![Value::Integer(2), Value::Varchar("b".into())]]);
    }

    #[test]
    fn test_index_scan_range() {
        let (c, bp, _d) = setup();
        let plan = PhysicalPlan::IndexScan { table: "users".into(), column: 0, lower: Bound::Included(Value::Integer(2)), upper: Bound::Included(Value::Integer(4)) };
        let rows = drain(lower(plan, &c, bp, Arc::new(ReadView { snapshot: Snapshot {high_water: 0, active: HashSet::new()}, txn_id: 0 })).unwrap());
        assert_eq!(rows, vec![
            vec![Value::Integer(2), Value::Varchar("b".into())], 
            vec![Value::Integer(3), Value::Varchar("c".into())], 
            vec![Value::Integer(4), Value::Varchar("d".into())]]);
    }

    #[test]
    fn test_index_scan_unbounded() {
        let (c, bp, _d) = setup();
        let plan = PhysicalPlan::IndexScan { table: "users".into(), column: 0, lower: Bound::Included(Value::Integer(3)), upper: Bound::Unbounded };
        let rows = drain(lower(plan, &c, bp, Arc::new(ReadView { snapshot: Snapshot {high_water: 0, active: HashSet::new()}, txn_id: 0 })).unwrap());
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::Integer(3));
    }

    #[test]
    fn test_index_scan_secondary_equality() {
        let (c, bp, _d) = setup();
        let plan = PhysicalPlan::IndexScan { table: "users".into(), column: 1, lower: Bound::Included(Value::Varchar("c".into())), upper: Bound::Included(Value::Varchar("c".into())) };
        let rows = drain(lower(plan, &c, bp, Arc::new(ReadView { snapshot: Snapshot {high_water: 0, active: HashSet::new()}, txn_id: 0 })).unwrap());
        assert_eq!(rows, vec![vec![Value::Integer(3), Value::Varchar("c".into())]])
    }

    #[test]
    fn test_index_scan_secondary_rejects_strict_lower() {
        let (c, bp, _d) = setup();
        let plan = PhysicalPlan::IndexScan { table: "users".into(), column: 1, lower: Bound::Excluded(Value::Varchar("b".into())), upper: Bound::Unbounded };
        assert!(lower(plan, &c, bp, Arc::new(ReadView { snapshot: Snapshot {high_water: 0, active: HashSet::new()}, txn_id: 0 })).is_err()); // todo: composite bound handling 
    }
}