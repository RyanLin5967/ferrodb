use crate::{catalog::{catalog::Catalog, column::{DataType, Value}, schema::Schema}, error::FerroError, parser::{parser::{Expr, JoinClause, JoinType, Stmt, TableRef}, scanner::TokenType}};

#[derive(Debug, Clone)]

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

#[derive(Debug, Clone)]
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
            TokenType::False => Value::Boolean(true),
            TokenType::Null => Value::Null,
            _ => return Err(FerroError::Bind(format!("invalid literal: {}", value)))
        };
        Ok(BoundExpr::Literal(v))
    }
}