use crate::{agent_sql::runtime::BranchResolver, branch::types::BranchId, catalog::{catalog::Catalog, column::{DataType, Value}, schema::Schema}, error::FerroError, parser::{parser::{BranchRef, Expr, JoinClause, Stmt, TableRef}, scanner::TokenType}, planner::logical_plan::LogicalPlan, provenance::revert::RevertMode};

/// An agent-session statement with every name resolved.
///
/// Design authority: DESIGN.md section 5. Branch names and merge ids are resolved here so the
/// runtime receives identities, never strings to look up.
#[derive(Debug, Clone)]
pub enum BoundAgentStmt {
    BeginAgentSession {
        agent_id: String,
        run_id: Option<String>,
        /// The branch to fork from: trunk, or the current session's branch for a nested task.
        parent: BranchId,
    },
    Diff {
        branch: BranchId,
    },
    Merge {
        branch: BranchId,
    },
    Abandon {
        branch: BranchId,
    },
    RevertMerge {
        merge_id: String,
        mode: RevertMode,
    },
    /// `SELECT ... AS OF BRANCH b` — read another branch's *uncommitted* state.
    SelectAsOf {
        branch: BranchId,
        stmt: Stmt,
    },
}

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
            Stmt::CreateTable { .. } => {
                todo!()
            }
            Stmt::Analyze { .. } => { unreachable!() }
            Stmt::Join { .. } => todo!(),
            Stmt::Explain { .. } | Stmt::Begin | Stmt::Commit | Stmt::Rollback => unreachable!(),
            // Agent statements do not describe a relational plan; they are bound by
            // `bind_agent`, which resolves branch names and merge ids instead of columns.
            Stmt::BeginAgentSession { .. }
            | Stmt::Diff { .. }
            | Stmt::Merge { .. }
            | Stmt::Abandon { .. }
            | Stmt::RevertMerge { .. } => Err(FerroError::Bind(
                "agent-session statements are bound by Binder::bind_agent, not into a plan".into(),
            )),
        }
    }

    /// Bind an agent-session statement.
    ///
    /// Design authority: DESIGN.md section 5. Name resolution happens here and nowhere else: the
    /// parser never touches the branch catalog, and the runtime never parses. `current` is the
    /// branch of the session issuing the statement, which is what `DIFF` / `MERGE` / `ABANDON`
    /// mean when no branch is named.
    pub fn bind_agent(
        &self,
        stmt: &Stmt,
        resolver: &dyn BranchResolver,
        current: Option<BranchId>,
    ) -> Result<BoundAgentStmt, FerroError> {
        let target = |branch: &Option<BranchRef>| -> Result<BranchId, FerroError> {
            match branch {
                Some(b) => resolver.resolve_branch(&b.name),
                None => current.ok_or_else(|| {
                    FerroError::Bind(
                        "no agent session in this connection; name a branch (e.g. BRANCH b_1)".into(),
                    )
                }),
            }
        };
        match stmt {
            Stmt::BeginAgentSession { agent, run } => {
                if agent.trim().is_empty() {
                    return Err(FerroError::Bind("agent id must not be empty".into()));
                }
                if let Some(r) = run {
                    if r.trim().is_empty() {
                        return Err(FerroError::Bind("run id must not be empty".into()));
                    }
                }
                Ok(BoundAgentStmt::BeginAgentSession {
                    agent_id: agent.clone(),
                    run_id: run.clone(),
                    // Forking from the session's own branch nests the task; from trunk otherwise.
                    parent: current.unwrap_or(BranchId::TRUNK),
                })
            }
            Stmt::Diff { branch } => Ok(BoundAgentStmt::Diff { branch: target(branch)? }),
            Stmt::Merge { branch } => Ok(BoundAgentStmt::Merge { branch: target(branch)? }),
            Stmt::Abandon { branch } => Ok(BoundAgentStmt::Abandon { branch: target(branch)? }),
            Stmt::RevertMerge { merge_id, cascade } => {
                if merge_id.trim().is_empty() {
                    return Err(FerroError::Bind("merge id must not be empty".into()));
                }
                Ok(BoundAgentStmt::RevertMerge {
                    merge_id: merge_id.clone(),
                    // Halt is the default deliberately: cascading a revert through an agent's
                    // downstream work is not recoverable by that agent.
                    mode: if *cascade { RevertMode::Cascade } else { RevertMode::Halt },
                })
            }
            Stmt::Select { from, columns, where_clause, joins } => {
                let branch_ref = from.as_of.as_ref().ok_or_else(|| {
                    FerroError::Bind("SELECT without AS OF BRANCH is not an agent statement".into())
                })?;
                let branch = resolver.resolve_branch(&branch_ref.name)?;
                if !joins.is_empty() {
                    return Err(FerroError::Bind(
                        "AS OF BRANCH does not support joins yet".into(),
                    ));
                }
                // Bind it now so an unknown column fails at bind time, not mid-scan.
                let mut plain = from.clone();
                plain.as_of = None;
                self.bind_select(plain, Vec::new(), columns.clone(), where_clause.clone())?;
                Ok(BoundAgentStmt::SelectAsOf { branch, stmt: stmt.clone() })
            }
            other => Err(FerroError::Bind(format!(
                "not an agent-session statement: {:?}",
                other
            ))),
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
                return self.bind_expr(*inner, scope);
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
        Ok(BoundExpr::Literal(Binder::literal_value(value_type, value)?))
    }

    /// The literal's value, with no expression wrapper.
    ///
    /// Guard capture needs the bare `Value`: a `GuardExpr` has to survive into a merge on a
    /// different branch, so it cannot carry a `BoundExpr::Column` offset into one plan's row.
    pub fn literal_value(value_type: TokenType, value: String) -> Result<Value, FerroError> {
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
        Ok(v)
    }
}

#[cfg(test)]
mod tests {

    use crate::catalog::column::Column;
    use crate::parser::parser::{JoinType, Parser};
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

    // ---- agent-session statements ------------------------------------------------------------

    struct StubResolver;

    impl BranchResolver for StubResolver {
        fn resolve_branch(&self, name: &str) -> Result<BranchId, FerroError> {
            match name {
                "b_1" => Ok(BranchId::new(1, 0)),
                "b_2" => Ok(BranchId::new(2, 0)),
                other => Err(FerroError::Branch(format!("unknown branch: {}", other))),
            }
        }
    }

    fn bind_agent(sql: &str, current: Option<BranchId>) -> Result<BoundAgentStmt, FerroError> {
        let (catalog, _dir) = setup();
        Binder::new(&catalog).bind_agent(&parse_one(sql), &StubResolver, current)
    }

    #[test]
    fn test_bind_begin_agent_session() {
        match bind_agent("BEGIN AGENT SESSION AS 'pricing-agent' RUN 'r_8fk2';", None).unwrap() {
            BoundAgentStmt::BeginAgentSession { agent_id, run_id, parent } => {
                assert_eq!(agent_id, "pricing-agent");
                assert_eq!(run_id.as_deref(), Some("r_8fk2"));
                assert_eq!(parent, BranchId::TRUNK);
            }
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
        // inside a session, a new task forks from that session's branch
        let nested = bind_agent("BEGIN AGENT SESSION AS 'a';", Some(BranchId::new(1, 0))).unwrap();
        match nested {
            BoundAgentStmt::BeginAgentSession { parent, .. } => {
                assert_eq!(parent, BranchId::new(1, 0))
            }
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
    }

    #[test]
    fn test_bind_diff_merge_abandon_default_to_the_session_branch() {
        let current = Some(BranchId::new(2, 0));
        assert!(matches!(
            bind_agent("DIFF;", current).unwrap(),
            BoundAgentStmt::Diff { branch } if branch == BranchId::new(2, 0)
        ));
        assert!(matches!(
            bind_agent("MERGE;", current).unwrap(),
            BoundAgentStmt::Merge { branch } if branch == BranchId::new(2, 0)
        ));
        assert!(matches!(
            bind_agent("ABANDON;", current).unwrap(),
            BoundAgentStmt::Abandon { branch } if branch == BranchId::new(2, 0)
        ));
        // an explicitly named branch wins over the session's own
        assert!(matches!(
            bind_agent("DIFF BRANCH b_1;", current).unwrap(),
            BoundAgentStmt::Diff { branch } if branch == BranchId::new(1, 0)
        ));
    }

    #[test]
    fn test_bind_without_a_session_or_branch_is_an_error() {
        for sql in ["DIFF;", "MERGE;", "ABANDON;"] {
            let err = bind_agent(sql, None).unwrap_err();
            assert!(err.to_string().contains("no agent session"), "{} gave {}", sql, err);
        }
        assert!(bind_agent("DIFF BRANCH b_9;", None).is_err());
    }

    #[test]
    fn test_bind_select_as_of_resolves_the_branch_and_the_columns() {
        match bind_agent("SELECT name FROM users AS OF BRANCH b_1;", None).unwrap() {
            BoundAgentStmt::SelectAsOf { branch, .. } => assert_eq!(branch, BranchId::new(1, 0)),
            other => panic!("expected SelectAsOf, got {:?}", other),
        }
        // an unknown column fails at bind time, not mid-scan
        assert!(bind_agent("SELECT nope FROM users AS OF BRANCH b_1;", None).is_err());
        // an unknown branch is an error, never an empty result
        assert!(bind_agent("SELECT name FROM users AS OF BRANCH b_9;", None).is_err());
        // a SELECT without AS OF is not an agent statement
        assert!(bind_agent("SELECT name FROM users;", None).is_err());
    }

    #[test]
    fn test_bind_revert_merge_defaults_to_halt() {
        match bind_agent("REVERT MERGE m_44;", None).unwrap() {
            BoundAgentStmt::RevertMerge { merge_id, mode } => {
                assert_eq!(merge_id, "m_44");
                assert_eq!(mode, RevertMode::Halt);
            }
            other => panic!("expected RevertMerge, got {:?}", other),
        }
        match bind_agent("REVERT MERGE m_44 CASCADE;", None).unwrap() {
            BoundAgentStmt::RevertMerge { mode, .. } => assert_eq!(mode, RevertMode::Cascade),
            other => panic!("expected RevertMerge, got {:?}", other),
        }
    }

    #[test]
    fn test_bind_rejects_an_agent_statement_as_a_plan() {
        let (catalog, _dir) = setup();
        let err = Binder::new(&catalog).bind(parse_one("DIFF;")).unwrap_err();
        assert!(matches!(err, FerroError::Bind(_)));
    }

    #[test]
    fn test_duplicate_qualifier_bind() {
        let (catalog, _dir) = setup();
        assert!(matches!(Binder::new(&catalog).bind(parse_one("SELECT name FROM users JOIN users ON users.id = users.id;")), Err(FerroError::Bind(_))));
    }
}