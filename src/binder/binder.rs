use crate::{catalog::{catalog::Catalog, column::{DataType, Value}, schema::Schema}, error::FerroError, parser::{parser::{Expr, JoinClause, JoinType, Stmt, TableRef}, scanner::TokenType}};

#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpr {
    BinaryOp {
        left: Box<BoundExpr>,
        operator: TokenType,
        right: Box<BoundExpr>
    },
    UnaryOp {
        operator: TokenType,
        right: Box<BoundExpr>
    },
    Literal(Value),
    Column(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundColumn {
    pub qualifier: String, 
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

pub struct Scope {
    pub columns: Vec<BoundColumn>, // index = offset in combined row
}

impl Scope {
    pub fn new() -> Self{
        Self {columns: Vec::new()}
    }

    // adds table's cols using column's qualifier
    pub fn add_table(&mut self, qualifier: &str, schema: &Schema) -> Result<(), FerroError> {
        let cols = &schema.columns;
        if self.columns.iter().any(|c| c.qualifier == qualifier) {
            return Err(FerroError::Bind(format!("duplicate table/alias: {}", qualifier)))
        }
        for i in 0..cols.len() {
            self.columns.push(BoundColumn{qualifier: qualifier.into(), name: cols[i].name.clone(), data_type: cols[i].data_type.clone(), nullable: cols[i].nullable});
        }
        Ok(())
    }

    // qualified (Some) or bare (None) column -> index, checks for unknown table/column
    pub fn resolve(&self, table: Option<&str>, column: &str) -> Result<usize, FerroError>{
        if table.is_some() {
            if let Some(idx) = self.columns.iter().position(|c| c.qualifier == table.expect("") && c.name == column) {
                Ok(idx)
            } else {
                return Err(FerroError::Bind("unknown column".into()))
            }
        } else {
            let mut found = None;
            for (i, col) in self.columns.iter().enumerate() {
                if col.name == column {
                    if found.is_some() {
                        return Err(FerroError::Bind("ambiguous column".into()))
                    } 
                    found = Some(i)
                }
            }
            found.ok_or(FerroError::Bind("not found".into()))
        }
    }

    // '*' (None) -> all indices, 'q.*' (Some) -> that table's indicies
    pub fn expand_star(&self, qualifier: Option<&str>) -> Result<Vec<usize>, FerroError>{
        match qualifier {
            Some(q) => {
                let mut indexes = Vec::new();
                for (i, col) in self.columns.iter().enumerate() {
                    if col.qualifier == q {
                        indexes.push(i);
                    }
                }
                if indexes.is_empty() {
                    return Err(FerroError::Bind(format!("unknown table/alias: {}", q)));
                }
                Ok(indexes)
            }
            None => {
                return Ok((0..self.columns.len()).collect())
            }
        }
    }
}

pub enum LogicalPlan {
    Scan {
        table: String,
        alias: Option<String>,
        output: Vec<BoundColumn>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        on: BoundExpr,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: BoundExpr,
    },
    Projection {
        input: Box<LogicalPlan>,
        exprs: Vec<BoundExpr>,
        output: Vec<BoundColumn>,
    }
}

impl LogicalPlan {
    // combine columns 
    pub fn output_schema(&self) -> Vec<BoundColumn> {
        match self {
            LogicalPlan::Filter { input, .. } => {
                input.output_schema()
            }
            LogicalPlan::Join { left, right, .. } => {
                let mut cols = left.output_schema();
                cols.extend(right.output_schema());
                cols
            }
            LogicalPlan::Projection {  output, .. } => {
                output.clone()
            }  
            LogicalPlan::Scan { output, .. } => {
                output.clone()
            }
        }
    }
}
pub struct Binder<'a> {
    catalog: &'a Catalog,
}

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a Catalog) -> Self{
        Self { catalog }
    }

    // parsed statement -> bound logical plan
    pub fn bind(&self, stmt: Stmt) -> Result<LogicalPlan, FerroError> {
        match stmt {
            Stmt::Select { from, columns, where_clause, joins } => {
                self.bind_select(from, joins, columns, where_clause)
            }
            Stmt::Insert { .. } => {
                todo!()
            }
            Stmt::Delete { .. } => {
                todo!() 
            }
            Stmt::Update { .. } => {
                todo!()
            }
            Stmt::CreateIndex { .. } => {
                todo!()
            }
            Stmt::CreateTable {.. } => {
                todo!()
            }
            Stmt::Join {..} => todo!()
        }
    }

    // SELECT: build scope from FROM/JOIN -> filter -> projection
    pub fn bind_select(&self, from: TableRef, joins: Vec<JoinClause>, columns: Vec<Expr>, where_clause: Option<Expr>) -> Result<LogicalPlan, FerroError> {
        let mut scope = Scope::new();
        let mut node = self.bind_from(from, joins, &mut scope)?;
        if let Some(pred) = where_clause {
            let predicate = self.bind_expr(pred, &scope)?;
            node = LogicalPlan::Filter { input: Box::new(node), predicate };
        }

        let (exprs, output) = self.bind_projection(columns, &scope)?;
        node = LogicalPlan::Projection { input: Box::new(node), exprs, output };
        Ok(node)
    }

    // FROM + JOINs -> left-deep Scan/Join tree: fills scope left to right
    pub fn bind_from(&self, from: TableRef, joins: Vec<JoinClause>, scope: &mut Scope) -> Result<LogicalPlan, FerroError> {
        let mut node = self.bind_scan(&from, scope)?;
        for join in joins {
            let right = self.bind_scan(&join.table, scope)?;
            let on = self.bind_expr(join.on, scope)?;
            node = LogicalPlan::Join { left: Box::new(node), right: Box::new(right), join_type: join.join_type, on }
        }
        Ok(node)
    }

    // one table -> Scan node: adds its columns to scope
    pub fn bind_scan(&self, table: &TableRef, scope: &mut Scope) -> Result<LogicalPlan, FerroError> {
        let table_entry = match self.catalog.get_table(&table.name) {
            Some(t) => t,
            None => return Err(FerroError::Bind("unknown table: {}".into())),
        };
        let qualifier = table.alias.clone().unwrap_or_else(|| table.name.clone());
        scope.add_table(&qualifier, &table_entry.schema)?;
        let output = table_entry.schema.columns.iter().map(|c| BoundColumn {
            qualifier: qualifier.clone(),
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            nullable: c.nullable,
        }).collect();

        Ok(LogicalPlan::Scan {table: table.name.clone(), alias: table.alias.clone(), output})
    }

    // resolved parsed expr against scope
    pub fn bind_expr(&self, expr: Expr, scope: &Scope) -> Result<BoundExpr, FerroError> {
        match expr {
            Expr::BinaryOp { left, operator, right } => {
                let l = self.bind_expr(*left, scope)?;
                let r = self.bind_expr(*right, scope)?;
                return Ok(BoundExpr::BinaryOp { left: Box::new(l), operator, right: Box::new(r) })
            }
            Expr::UnaryOp { operator, right } => {
                let r = self.bind_expr(*right, scope)?;
                return Ok(BoundExpr::UnaryOp { operator, right: Box::new(r) })
            }
            Expr::Grouping(inner) => {
                return Ok(self.bind_expr(*inner, scope))?
            }
            Expr::Literal { value_type, value } => {
                return self.bind_literal(value_type, value);
            }
            Expr::ColumnRef { table, column  } => {
                let idx = scope.resolve(table.as_deref(), &column)?;
                return Ok(BoundExpr::Column(idx));
            }
        }
    }

    // projection list -> (bound exprs, bound column)
    pub fn bind_projection(&self, columns: Vec<Expr>, scope: &Scope) -> Result<(Vec<BoundExpr>, Vec<BoundColumn>), FerroError> {
        let mut exprs: Vec<BoundExpr> = Vec::new();
        let mut output: Vec<BoundColumn> = Vec::new();

        for col in columns {
            match col {
                Expr::ColumnRef { table, column } if column == "*" => {
                    for i in scope.expand_star(table.as_deref())? {
                        exprs.push(BoundExpr::Column(i));
                        output.push(scope.columns[i].clone());
                    }
                }
                Expr::ColumnRef { table, column } => {
                    let i = scope.resolve(table.as_deref(), &column)?;
                    exprs.push(BoundExpr::Column(i));
                    output.push(scope.columns[i].clone());
                }
                other => {
                    let bound = self.bind_expr(other, scope)?;
                    exprs.push(bound);
                    // placeholder
                    output.push(BoundColumn{qualifier: String::new(), name: "?column?".into(), data_type: DataType::Integer, nullable: true});
                }
            }
        }
        Ok((exprs, output))
    }

    // parse literal into value
    pub fn bind_literal(&self, value_type: TokenType, value: String) -> Result<BoundExpr, FerroError> {
        let v = match value_type {
            TokenType::Number => {
                if value.contains('.') {
                    Value::Float(value.parse::<f64>().map_err(|e| FerroError::Bind(format!("invalid float: {}, {}", value, e)))?)
                } else {
                    Value::Integer(value.parse::<i32>().map_err(|e| FerroError::Bind(format!("invalid int: {}, {}", value, e)))?)
                }
            }
            TokenType::String => Value::Varchar(value),
            TokenType::True => Value::Boolean(true),
            TokenType::False => Value::Boolean(false),
            TokenType::Null => Value::Null,
            _ => return Err(FerroError::Bind(format!("invalid literal: {}", value)))
        };
        Ok(BoundExpr::Literal(v))
    }
}

#[cfg(test)]
mod tests {

    use crate::catalog::column::Column;
    use crate::parser::parser::Parser;
    use crate::parser::scanner::Scanner;
    use crate::{buffer::buffer_pool::BufferPoolManager, storage::disk_manager::DiskManager};
    use super::*;
    use std::fs::OpenOptions;
    use std::sync::Arc;

    fn setup() -> (Catalog, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("binder.db");
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(&path).unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let mut c = Catalog::create(bp).unwrap();
        c.create_table("users".into(), Schema::new(vec![col("id", DataType::Integer, false), col("name", DataType::Varchar(255), true)])).unwrap();
        c.create_table("posts".into(), Schema::new(vec![col("id", DataType::Integer, false), col("user_id", DataType::Integer, false), col("title", DataType::Varchar(255), true)])).unwrap();
        (c, dir)
    }

    fn col(name: &str, data_type: DataType, nullable: bool) -> Column {
        Column { name: name.to_string(), data_type, nullable }
    }

    fn scope() -> Scope {
        let mut s = Scope::new();
        s.add_table("u", &Schema::new(vec![col("id", DataType::Integer, false), col("name", DataType::Varchar(255), true)])).unwrap();
        s.add_table("p", &Schema::new(vec![col("id", DataType::Integer, false), col("user_id", DataType::Integer, false)])).unwrap();
        s
    }

    fn parse_one(sql: &str) -> Stmt {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        assert_eq!(stmts.len(), 1);
        stmts.remove(0)
    }

    #[test]
    fn test_literal() {
        let (catalog, _dir) = setup();
        let binder = Binder::new(&catalog);
        assert_eq!(binder.bind_literal(TokenType::String, "users".into()).unwrap(), BoundExpr::Literal(Value::Varchar("users".to_string())));
        assert_eq!(binder.bind_literal(TokenType::Number, "1".into()).unwrap(), BoundExpr::Literal(Value::Integer(1)));
        assert_eq!(binder.bind_literal(TokenType::True, "true".into()).unwrap(), BoundExpr::Literal(Value::Boolean(true)));
        assert_eq!(binder.bind_literal(TokenType::False, "false".into()).unwrap(), BoundExpr::Literal(Value::Boolean(false)));
        assert_eq!(binder.bind_literal(TokenType::Number, "1.1".into()).unwrap(), BoundExpr::Literal(Value::Float(1.1)));
        assert_eq!(binder.bind_literal(TokenType::Null, "null".into()).unwrap(), BoundExpr::Literal(Value::Null));
    }

    #[test]
    fn test_resolve_qualified_unique() {
        let s = scope();
        assert_eq!(s.resolve(Some("u"), "id").unwrap(), 0);
        assert_eq!(s.resolve(Some("u"), "name").unwrap(), 1);
        assert_eq!(s.resolve(Some("p"), "user_id").unwrap(), 3);
        assert_eq!(s.resolve(None, "name").unwrap(), 1);
        assert_eq!(s.resolve(None, "user_id").unwrap(), 3);
    }

    #[test]
    fn test_resolve_ambiguous() {
        let s = scope();
        assert!(matches!(s.resolve(None, "id"), Err(FerroError::Bind(_))));
    }

    #[test]
    fn test_resolve_unknown_table() {
        let s = scope();
        assert!(matches!(s.resolve(None, "idk"), Err(FerroError::Bind(_))));
    }

    #[test]
    fn test_resolve_unknown_column() {
        let s = scope();
        assert!(matches!(s.resolve(Some("u"), "idk"), Err(FerroError::Bind(_))));
        assert!(matches!(s.resolve(None, "idk"), Err(FerroError::Bind(_))));
    }

    #[test]
    fn test_duplicate_qualifier() {
        let mut s = scope();
        let dup = s.add_table("u", &Schema::new(vec![col("x", DataType::Integer, false)]));
        assert!(matches!(dup, Err(FerroError::Bind(_))));
    }

    #[test]
    fn test_expand_star() {
        let s = scope();
        assert_eq!(s.expand_star(None).unwrap(), vec![0,1,2,3]);
        assert_eq!(s.expand_star(Some("u")).unwrap(), vec![0, 1]);
        assert_eq!(s.expand_star(Some("p")).unwrap(), vec![2,3]);
        assert!(matches!(s.expand_star(Some("idk")), Err(FerroError::Bind(_))));
    }

    #[test]
    fn test_simple_select() {
        let (catalog, _dir) = setup();
        let plan = Binder::new(&catalog).bind(parse_one("SELECT * FROM users;")).unwrap();
        match plan {
            LogicalPlan::Projection { exprs, output, .. } => {
                assert_eq!(exprs, vec![BoundExpr::Column(0), BoundExpr::Column(1)]);
                assert_eq!(output.len(), 2);
                assert_eq!(output[0].name, "id");
                assert_eq!(output[1].name, "name");
            }
            _ => panic!("expected projection")
        }
    }

    #[test]
    fn test_select_star() {
        let (catalog, _dir) = setup();
        let plan = Binder::new(&catalog).bind(parse_one("SELECT name FROM users WHERE id > 5;")).unwrap();
        match plan {
            LogicalPlan::Projection { input, .. } => match *input{
                LogicalPlan::Filter { input, predicate} => {
                    assert_eq!(predicate, BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(0)), operator: TokenType::Greater, right: Box::new(BoundExpr::Literal(Value::Integer(5))) });
                    assert!(matches!(*input, LogicalPlan::Scan { .. }))
                }
                _ => panic!("expected filter"),
            }
            _ => panic!("expected projection")
        }
    }

    #[test]
    fn test_join() {
        let (catalog, _dir) = setup();
        let plan = Binder::new(&catalog).bind(parse_one("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id;")).unwrap();
        match plan {
            LogicalPlan::Projection { input, exprs, .. } => {
                assert_eq!(exprs, vec![BoundExpr::Column(1), BoundExpr::Column(4)]);
                match *input {
                    LogicalPlan::Join { left, right, join_type, on } => {
                        assert!(matches!(join_type, JoinType::Inner));
                        assert_eq!(on, BoundExpr::BinaryOp { left: Box::new(BoundExpr::Column(0)), operator: TokenType::Equal, right: Box::new(BoundExpr::Column(3)) });
                        assert!(matches!(*left, LogicalPlan::Scan { .. }));
                        assert!(matches!(*right, LogicalPlan::Scan { .. }));
                    }
                    _ => panic!("expected join")
                }
            }
            _ => panic!("expected projection")
        }
    }

    #[test]
    fn test_self_join() {
        let (catalog, _dir) = setup();
        let plan = Binder::new(&catalog).bind(parse_one("SELECT e.name, m.name FROM users e JOIN users m ON e.id = m.id;")).unwrap();
        match plan {
            LogicalPlan::Projection { exprs, .. } => {
                assert_eq!(exprs, vec![BoundExpr::Column(1), BoundExpr::Column(3)]);
            }
            _ => panic!("expected projection")
        }
    }

    #[test]
    fn test_ambiguous_column() {
        let (catalog, _dir) = setup();
        assert!(matches!(Binder::new(&catalog).bind(parse_one("SELECT id FROM users u JOIN posts p ON u.id = p.user_id;")), Err(FerroError::Bind(_))));
    }

    #[test]
    fn test_unkown_table() {
        let (catalog, _dir) = setup();
        assert!(matches!(Binder::new(&catalog).bind(parse_one("SELECT name FROM idk;")), Err(FerroError::Bind(_))));
    }

    #[test]
    fn test_duplicate_qualifier_bind() {
        let (catalog, _dir) = setup();
        assert!(matches!(Binder::new(&catalog).bind(parse_one("SELECT name FROM users JOIN users ON users.id = users.id;")), Err(FerroError::Bind(_))));
    }
}