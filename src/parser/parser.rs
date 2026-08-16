use crate::{catalog::column::{Column, DataType}, error::FerroError, parser::{scanner::{Token, TokenType::{self}}}};

pub struct Parser {
    pub tokens: Vec<Token>,
    pub current: usize,
    pub errors: Vec<FerroError>
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
    /// `AS OF BRANCH b_123` — read this table as the named branch sees it, including that
    /// branch's *uncommitted* state (DESIGN.md exit criterion 3). `None` is the ordinary read of
    /// the session's own branch.
    pub as_of: Option<BranchRef>,
}

impl TableRef {
    pub fn plain(name: String, alias: Option<String>) -> Self {
        TableRef { name, alias, as_of: None }
    }
}

/// How a branch was named in SQL. Resolution to a `BranchId` happens in the binder, not here —
/// the parser never touches the branch catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchRef {
    pub name: String,
}

impl BranchRef {
    pub fn new(name: impl Into<String>) -> Self {
        BranchRef { name: name.into() }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType { 
    Inner, 
    Left, 
    Right, 
    Full
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableRef,
    pub on: Expr
}

#[derive(Debug, Clone)]
pub enum Expr {
    BinaryOp {
        left: Box<Expr>,
        operator: TokenType,
        right: Box<Expr>
    },
    UnaryOp {
        operator: TokenType,
        right: Box<Expr>
    },
    Literal{
        value_type: TokenType,
        value: String,
    },
    ColumnRef{
        table: Option<String>,
        column: String,
    },
    // for parentheses overriding precedence
    Grouping(Box<Expr>),
}

impl Expr {
    /// Render back to SQL text.
    ///
    /// This exists for one reason: a captured `Guard` must hand the agent **the violated
    /// predicate itself** (DESIGN.md exit criterion 7), and the agent wrote it in SQL. Rendering
    /// the parsed expression keeps that text alive without the executor having to carry raw
    /// source offsets around.
    pub fn to_sql(&self) -> String {
        match self {
            Expr::BinaryOp { left, operator, right } => {
                format!("{} {} {}", left.to_sql(), token_sql(*operator), right.to_sql())
            }
            Expr::UnaryOp { operator, right } => {
                format!("{} {}", token_sql(*operator), right.to_sql())
            }
            Expr::Literal { value_type, value } => match value_type {
                TokenType::String => format!("'{}'", value),
                _ => value.clone(),
            },
            Expr::ColumnRef { table, column } => match table {
                Some(t) => format!("{}.{}", t, column),
                None => column.clone(),
            },
            Expr::Grouping(inner) => format!("({})", inner.to_sql()),
        }
    }
}

fn token_sql(t: TokenType) -> &'static str {
    match t {
        TokenType::Plus => "+",
        TokenType::Minus => "-",
        TokenType::Star => "*",
        TokenType::Slash => "/",
        TokenType::Equal => "=",
        TokenType::BangEqual => "<>",
        TokenType::Less => "<",
        TokenType::LessEqual => "<=",
        TokenType::Greater => ">",
        TokenType::GreaterEqual => ">=",
        TokenType::And => "AND",
        TokenType::Or => "OR",
        TokenType::Not => "NOT",
        TokenType::Bang => "NOT",
        _ => "?",
    }
}

#[derive(Debug, Clone)]
pub enum Stmt {
    Select {
        from: TableRef,
        columns: Vec<Expr>,
        where_clause: Option<Expr>,
        joins: Vec<JoinClause>
    },
    Insert {
        table: String,
        values: Vec<Expr>
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>
    },
    Delete {
        table: String,
        where_clause: Option<Expr>
    },
    CreateTable {
        table: String,
        columns: Vec<Column>,
    },
    CreateIndex {
        index_name: String,
        table: String,
        column_name: String
    },
    Join {
        table: String,
        on: Expr,
    },
    Analyze {
        table: String,
    },
    Explain(Box<Stmt>),
    Begin, Commit, Rollback,

    // ---- agent-isolation surface (DESIGN.md section 5) --------------------------------------
    /// `BEGIN AGENT SESSION AS 'pricing-agent' RUN 'r_8fk2';`
    ///
    /// Forks a branch for one agent task. The fork copies zero data pages (exit criterion 1);
    /// the agent identity and run id are what provenance interns (exit criterion 9).
    BeginAgentSession {
        agent: String,
        run: Option<String>,
    },
    /// `DIFF;` — the structured changeset this session's branch would merge (exit criterion 4).
    Diff {
        branch: Option<BranchRef>,
    },
    /// `MERGE;` — three-way merge of this session's branch into its parent, reporting
    /// Clean / Commuting / Conflict / ResolvedWithLoss (exit criterion 5).
    Merge {
        branch: Option<BranchRef>,
    },
    /// `ABANDON;` — drop this session's branch. Abandoning is also what happens with *no* client
    /// cooperation at all when the lease expires (exit criterion 8); this is the polite form.
    Abandon {
        branch: Option<BranchRef>,
    },
    /// `REVERT MERGE m_44 CASCADE;` — causal rollback over retained read-sets. Without `CASCADE`
    /// the revert halts and reports the dependency tree (exit criterion 10).
    RevertMerge {
        merge_id: String,
        cascade: bool,
    },
}

// OR -> AND -> NOT -> equality/comparison -> term -> factor -> unary -> primary
impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self{
        Self {tokens, current: 0, errors: Vec::new()}
    }

    pub fn parse(&mut self) -> Vec<Stmt> {
        let mut statements = Vec::new();

        while !self.is_at_end() {
            match self.parse_statement() {
                Ok(stmt) => statements.push(stmt),
                Err(err) => {
                    self.errors.push(err);
                    self.synchronize();
                }
            }
        }
        statements
    }
    pub fn parse_statement(&mut self) -> Result<Stmt, FerroError>{
        if self.match_token(&[TokenType::Select]) {
            return self.parse_select()
        } else if self.match_token(&[TokenType::Insert]) {
            return self.parse_insert()
        } else if self.match_token(&[TokenType::Update]) {
            return self.parse_update()
        } else if self.match_token(&[TokenType::Delete]) { 
            return self.parse_delete()
        } else if self.match_token(&[TokenType::Analyze]) {
            return self.parse_analyze()
        } else if self.match_token(&[TokenType::Explain]){
            return self.parse_explain()
        } else if self.match_token(&[TokenType::Begin]) {
            if self.match_token(&[TokenType::Agent]) {
                return self.parse_begin_agent_session()
            }
            return self.parse_txn_stmt(Stmt::Begin)
        } else if self.match_token(&[TokenType::Diff]) {
            let branch = self.parse_optional_branch_arg()?;
            self.consume(TokenType::Semicolon, "expected ;")?;
            return Ok(Stmt::Diff { branch })
        } else if self.match_token(&[TokenType::Merge]) {
            let branch = self.parse_optional_branch_arg()?;
            self.consume(TokenType::Semicolon, "expected ;")?;
            return Ok(Stmt::Merge { branch })
        } else if self.match_token(&[TokenType::Abandon]) {
            let branch = self.parse_optional_branch_arg()?;
            self.consume(TokenType::Semicolon, "expected ;")?;
            return Ok(Stmt::Abandon { branch })
        } else if self.match_token(&[TokenType::Revert]) {
            return self.parse_revert()
        } else if self.match_token(&[TokenType::Commit]) {
            return self.parse_txn_stmt(Stmt::Commit)
        } else if self.match_token(&[TokenType::Rollback]) {
            return self.parse_txn_stmt(Stmt::Rollback)
        } else if self.match_token(&[TokenType::Create]){
            if self.match_token(&[TokenType::Index]) {
                return self.parse_create_index()
            } else if self.match_token(&[TokenType::Table]) {
                return self.parse_create_table()
            } else {
                return Err(Parser::error(self.peek(), "expected TABLE or INDEX after CREATE".into()));
            }
        } else {
            return Err(Parser::error(self.peek(), "expected a statement".into()));
        }
    }

    // SELECT vals FROM table1 AS t INNER JOIN table2 AS s ON t.x = s.y WHERE expr
    pub fn parse_select(&mut self) -> Result<Stmt, FerroError>{
        let mut columns = Vec::new();
        loop {
            if self.match_token(&[TokenType::Star]) {
                columns.push(Expr::ColumnRef { table: None, column: "*".to_string() });
            }else {
                columns.push(self.expression()?);
            }
            if !self.match_token(&[TokenType::Comma]) {break;}
        }
        self.consume(TokenType::From, "expected FROM")?;
        let main_table = self.parse_table_ref()?;
        let mut joins = Vec::new();
        loop {
            let mut join_type = None;
            if self.check(TokenType::Identifier) {
                let lexeme_upper = self.peek().lexeme.to_uppercase();
                match lexeme_upper.as_str() {
                    "INNER" => {
                        self.advance();
                        join_type = Some(JoinType::Inner);
                    }
                    "LEFT" => {
                        self.advance();
                        self.match_token(&[TokenType::Outer]);
                        join_type = Some(JoinType::Left);
                    }
                    "RIGHT" => {
                        self.advance();
                        self.match_token(&[TokenType::Outer]);
                        join_type = Some(JoinType::Right);
                    }
                    "FULL" => {
                        self.advance();
                        self.match_token(&[TokenType::Outer]);
                        join_type = Some(JoinType::Full);
                    }
                    _ => {}
                }
            }

            let has_join = self.match_token(&[TokenType::Join]);
            if join_type.is_some() && !has_join {
                return Err(Parser::error(self.peek(), "expected join".to_string()))
            }
            if join_type.is_none() && !has_join { break; }
            let actual_join_type = join_type.unwrap_or(JoinType::Inner); // default to inner
            let join_table = self.parse_table_ref()?;
            self.consume(TokenType::On, "expected on".into())?;
            let on = self.expression()?;

            joins.push(JoinClause {join_type: actual_join_type, table: join_table, on});
        }

        let where_clause = if self.match_token(&[TokenType::Where]) {
            Some(self.expression()?)
        } else {
            None
        };
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::Select { from: main_table, columns, where_clause, joins})
    }

    // INSERT INTO table VALUES vals
    pub fn parse_insert(&mut self) -> Result<Stmt, FerroError>{
        if !self.match_token(&[TokenType::Into]) {
            return Err(Parser::error(self.peek(), "expected INTO".into()));
        } 
        let table = self.consume(TokenType::Identifier, "expected table name")?.lexeme;
        if !self.match_token(&[TokenType::Values]) {
            return Err(Parser::error(self.peek(), "expected VALUES".into()));
        }
        self.consume(TokenType::LeftParen, "expected (")?;
        let mut values = Vec::new();
        loop {
            values.push(self.expression()?);
            if !self.match_token(&[TokenType::Comma]) {break;}
        }
        self.consume(TokenType::RightParen, "expected )")?;
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::Insert { table, values })
    }

    // UPDATE table SET col = val WHERE expr (optional)
    pub fn parse_update(&mut self) -> Result<Stmt, FerroError>{
        let table = self.consume(TokenType::Identifier, "expected table name")?.lexeme;
        if !self.match_token(&[TokenType::Set]) {
            return Err(Parser::error(self.peek(), "expected SET".into()));
        }
        let mut assignments: Vec<(String, Expr)> = Vec::new();

        loop {
            let column_name = self.consume(TokenType::Identifier, "expected column name")?.lexeme;
            self.consume(TokenType::Equal, "expected =")?;
            let value = self.expression()?;
            assignments.push((column_name, value));
            if !self.match_token(&[TokenType::Comma]) {break;}
        }

        let where_clause = if self.match_token(&[TokenType::Where]) {
            Some(self.expression()?)
        } else {
            None
        };
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok (Stmt::Update { table, assignments, where_clause })
    }

    // DELETE FROM table WHERE expr
    pub fn parse_delete(&mut self) -> Result<Stmt, FerroError>{
        if !self.match_token(&[TokenType::From]) {
            return Err(Parser::error(self.peek(), "expected FROM".into()));
        }
        let table = self.consume(TokenType::Identifier, "expected table name")?.lexeme;
        let where_clause = if self.match_token(&[TokenType::Where]) {
            Some(self.expression()?)
        } else {
            None
        };

        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::Delete { table, where_clause })
    }

    // CREATE TABLE name (col, datatype, (null/not null)...)
    pub fn parse_create_table(&mut self) -> Result<Stmt, FerroError> {
        let table = self.consume(TokenType::Identifier, "expected table name")?.lexeme;
        let mut columns = Vec::new();
        self.consume(TokenType::LeftParen, "expected (")?;
        loop {
            let name = self.consume(TokenType::Identifier, "expected col name")?.lexeme;
            let data_type = if self.match_token(&[TokenType::TypeInt]) {
                DataType::Integer
            } else if self.match_token(&[TokenType::TypeBoolean]) {
                DataType::Boolean
            } else if self.match_token(&[TokenType::TypeFloat]) {
                DataType::Float
            } else if self.match_token(&[TokenType::TypeVarchar]) {
                self.consume(TokenType::LeftParen, "expected ( after VARCHAR")?;
                let size_token = self.consume(TokenType::Number, "expected size")?;
                let size: u16 = size_token.lexeme.parse().map_err(|_| Parser::error(size_token.clone(), "invalid size".into()))?;
                self.consume(TokenType::RightParen, "expected )")?;
                DataType::Varchar(size)
            }else {
                return Err(Parser::error(self.peek(), "expected data type".into()));
            };

            let mut nullable = true;
            if self.match_token(&[TokenType::Not]) {
                if self.match_token(&[TokenType::Null]) {
                    nullable = false;
                } else {
                    return Err(Parser::error(self.peek(), "unexpected NOT".into()));
                }
            } else if self.match_token(&[TokenType::Null]) {
                nullable = true;
            }
            columns.push(Column { name, data_type, nullable });

            if !self.match_token(&[TokenType::Comma]) {
                break;
            }
        }
        self.consume(TokenType::RightParen, "expected )")?;
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::CreateTable { table, columns })
    }

    // CREATE INDEX index_name ON table (col)
    pub fn parse_create_index(&mut self) -> Result<Stmt, FerroError> {
        let index_name = self.consume(TokenType::Identifier, "expected index name")?.lexeme;
        if !self.match_token(&[TokenType::On]) {
            return Err(Parser::error(self.peek(), "expected ON".into()));
        }

        let table = self.consume(TokenType::Identifier, "expected table name")?.lexeme;
        self.consume(TokenType::LeftParen, "expected (")?;
        let column_name = self.consume(TokenType::Identifier, "expected column name")?.lexeme;
        if self.check(TokenType::Comma) {
            return Err(Parser::error(self.peek(), "composite indexes not supported yet".into()));
        }
        self.consume(TokenType::RightParen, "expected )")?;
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::CreateIndex { index_name, table, column_name })
    }

    pub fn parse_analyze(&mut self) -> Result<Stmt, FerroError> {
        let name = self.consume(TokenType::Identifier, "expected table name")?.lexeme;
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::Analyze { table: name })
    }

    pub fn parse_explain(&mut self) -> Result<Stmt, FerroError> {
        let right = self.parse_statement()?;
        Ok(Stmt::Explain(Box::new(right)))
    }

    pub fn parse_txn_stmt(&mut self, stmt: Stmt) -> Result<Stmt, FerroError> {
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(stmt)
    }

    // BEGIN AGENT SESSION AS 'agent-id' [RUN 'run-id']
    pub fn parse_begin_agent_session(&mut self) -> Result<Stmt, FerroError> {
        self.consume(TokenType::Session, "expected SESSION after BEGIN AGENT")?;
        self.consume(TokenType::As, "expected AS after BEGIN AGENT SESSION")?;
        let agent = self.consume(TokenType::String, "expected a quoted agent id")?.lexeme;
        let run = if self.match_token(&[TokenType::Run]) {
            Some(self.consume(TokenType::String, "expected a quoted run id after RUN")?.lexeme)
        } else {
            None
        };
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::BeginAgentSession { agent, run })
    }

    // REVERT MERGE m_44 [CASCADE]
    pub fn parse_revert(&mut self) -> Result<Stmt, FerroError> {
        self.consume(TokenType::Merge, "expected MERGE after REVERT")?;
        let merge_id = self.consume_name("expected a merge id")?;
        let cascade = self.match_token(&[TokenType::Cascade]);
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::RevertMerge { merge_id, cascade })
    }

    // optional `BRANCH b_1` argument on DIFF / MERGE / ABANDON; absent means "this session's branch"
    pub fn parse_optional_branch_arg(&mut self) -> Result<Option<BranchRef>, FerroError> {
        if self.match_token(&[TokenType::Branch]) {
            return Ok(Some(BranchRef::new(self.consume_name("expected a branch name")?)));
        }
        Ok(None)
    }

    /// An unquoted name: an identifier, or a bare number so `BRANCH 3` and `MERGE 44` scan.
    pub fn consume_name(&mut self, message: &str) -> Result<String, FerroError> {
        if self.check(TokenType::Identifier) || self.check(TokenType::Number) {
            return Ok(self.advance().lexeme);
        }
        Err(Parser::error(self.peek(), message.to_string()))
    }

    pub fn parse_table_ref(&mut self) -> Result<TableRef, FerroError> {
        let name = self.consume(TokenType::Identifier, "expected table name")?.lexeme;
        let mut alias = None;

        // `AS OF` is a time/branch qualifier, not an alias: do not let `AS` eat the `OF`.
        let as_alias = self.check(TokenType::As) && !self.check_next(TokenType::Of);
        if as_alias {
            self.advance();
            alias = Some(self.consume(TokenType::Identifier, "expected alias")?.lexeme);
        } else if self.check(TokenType::Identifier) {
            let lexeme_upper = self.peek().lexeme.to_uppercase();
            if !["JOIN", "WHERE", "ON", "INNER", "LEFT", "RIGHT", "FULL", "OUTER"].contains(&lexeme_upper.as_str()) {
                alias = Some(self.advance().lexeme);
            }
        }

        let as_of = if self.check(TokenType::As) && self.check_next(TokenType::Of) {
            self.advance();
            self.advance();
            self.consume(TokenType::Branch, "expected BRANCH after AS OF")?;
            Some(BranchRef::new(self.consume_name("expected a branch name")?))
        } else {
            None
        };
        Ok(TableRef{name, alias, as_of})
    }

    pub fn match_token(&mut self, types: &[TokenType]) -> bool{
        for token_type in types {
            if self.check(*token_type) {
                self.advance();
                return true;
            }
        }
        false
    }

    pub fn check(&self, token_type: TokenType) -> bool {
        if self.is_at_end() {
            return false;
        }
        return self.peek().token_type == token_type;
    }

    /// One token of lookahead past `peek`. Needed to tell `AS alias` from `AS OF BRANCH b`.
    pub fn check_next(&self, token_type: TokenType) -> bool {
        match self.tokens.get(self.current + 1) {
            Some(t) => t.token_type == token_type,
            None => false,
        }
    }

    pub fn advance(&mut self) -> Token{
        if !self.is_at_end() {
            self.current += 1;
        }
        self.previous()
    }

    pub fn is_at_end(&self) -> bool {
        return self.peek().token_type == TokenType::Eof || self.current >= self.tokens.len()
    }

    pub fn peek(&self) -> Token{
        self.tokens.get(self.current).cloned().unwrap_or_else(|| Token::new(TokenType::Eof, "".to_string(), 0))
    }

    pub fn previous(&self) -> Token{
        self.tokens[self.current - 1].clone()
    }

    pub fn consume(&mut self, token_type: TokenType, message: &str) -> Result<Token, FerroError>{
        if self.check(token_type){
            return Ok(self.advance());
        }
        Err(Parser::error(self.peek(), message.to_string()))
    }

    pub fn expression(&mut self ) -> Result<Expr, FerroError>{
        return self.or();
    }

    pub fn or(&mut self) -> Result<Expr, FerroError>{
        let mut expr = self.and()?;
        while self.match_token(&[TokenType::Or]) {
            let operator = self.previous().token_type;
            let right = self.and()?;
            expr = Expr::BinaryOp { left: Box::new(expr), operator, right: Box::new(right) };
        }
        Ok(expr)
    }

    pub fn and(&mut self) -> Result<Expr, FerroError>{
        let mut expr = self.not()?;
        while self.match_token(&[TokenType::And]) {
            let operator = self.previous().token_type;
            let right = self.not()?;
            expr = Expr::BinaryOp { left: Box::new(expr), operator, right: Box::new(right) };
        }
        Ok(expr)
    }

    pub fn not(&mut self) -> Result<Expr, FerroError>{
        if self.match_token(&[TokenType::Not]) {
            let operator = self.previous().token_type;
            let right = self.not()?;
            return Ok(Expr::UnaryOp { operator, right: Box::new(right) });
        }
        self.equality()
    }

    pub fn equality(&mut self) -> Result<Expr, FerroError>{
        let mut expr = self.comparison()?;
        while self.match_token(&[TokenType::BangEqual, TokenType::Equal]){
            let operator = self.previous().token_type;
            let right = self.comparison()?;
            expr = Expr::BinaryOp { left: Box::new(expr), operator, right: Box::new(right) };
        }
        Ok(expr)
    }

    pub fn comparison(&mut self) -> Result<Expr, FerroError>{
        let mut expr = self.term()?;
        while self.match_token(&[TokenType::Greater, TokenType::GreaterEqual, TokenType::Less, TokenType::LessEqual]) {
            let operator = self.previous().token_type;
            let right = self.term()?;
            expr = Expr::BinaryOp { left: Box::new(expr), operator, right: Box::new(right) };
        }
        Ok(expr)
    }
    
    pub fn term(&mut self) -> Result<Expr, FerroError>{
        let mut expr = self.factor()?;
        while self.match_token(&[TokenType::Minus, TokenType::Plus]) {
            let operator = self.previous().token_type;
            let right = self.factor()?;
            expr = Expr::BinaryOp { left: Box::new(expr), operator, right: Box::new(right) };
        }
        Ok(expr)
    }

    pub fn factor(&mut self) -> Result<Expr, FerroError>{
        let mut expr = self.unary()?;
        while self.match_token(&[TokenType::Slash, TokenType::Star]) {
            let operator = self.previous().token_type;
            let right = self.unary()?;
            expr = Expr::BinaryOp { left: Box::new(expr), operator, right: Box::new(right) };
        }
        Ok(expr)
    }

    pub fn unary(&mut self) -> Result<Expr, FerroError>{
        if self.match_token(&[TokenType::Bang, TokenType::Minus]) {
            let operator = self.previous().token_type;
            let right = self.unary()?;
            return  Ok(Expr::UnaryOp { operator, right: Box::new(right) });
        }
        self.primary()

    }

    pub fn primary(&mut self) -> Result<Expr, FerroError>{
        if self.match_token(&[TokenType::False]) {return Ok(Expr::Literal { value_type: TokenType::False, value: String::from("false") })}
        if self.match_token(&[TokenType::True]) {return Ok(Expr::Literal { value_type: TokenType::True, value: String::from("true") })}
        if self.match_token(&[TokenType::Null]) {return Ok(Expr::Literal { value_type: TokenType::Null, value: String::from("null") })}
        if self.match_token(&[TokenType::Number, TokenType::String]) {
            let prev = self.previous();
            return Ok(Expr::Literal { value_type: prev.token_type, value: prev.lexeme })
        }

        if self.match_token(&[TokenType::Identifier]) {
            let first_part = self.previous().lexeme;
            if self.match_token(&[TokenType::Dot]) {

                if self.match_token(&[TokenType::Star]) {
                    return Ok(Expr::ColumnRef { table: Some(first_part), column: "*".into() })
                }
                let second_part = self.consume(TokenType::Identifier, "expected column name after '.'")?.lexeme;
                return Ok(Expr::ColumnRef { table: Some(first_part), column: second_part });
            }
            return Ok(Expr::ColumnRef { table: None, column: first_part });
        }

        if self.match_token(&[TokenType::LeftParen]) {
            let expr = self.expression()?;
            self.consume(TokenType::RightParen, "expected right parentheses")?;
            return Ok(Expr::Grouping(Box::new(expr)))
        }
        Err(Parser::error(self.peek(), "unsupported token".to_string()))
    }

    pub fn error(token: Token, message: String) -> FerroError{
        if token.token_type == TokenType::Eof {
            return FerroError::SqlParseError(format!("{} at end {}", token.line, message));
        } else {
            return FerroError::SqlParseError(format!("{} at ' {} ' {}", token.line, token.lexeme, message));
        }
    }

    pub fn synchronize(&mut self) {
        self.advance();

        while !self.is_at_end() {
            if self.previous().token_type == TokenType::Semicolon {
                return;
            }
            match self.peek().token_type {
                TokenType::Select | TokenType::Insert | TokenType::Update | TokenType::Delete | TokenType::Create=> return,
                _ => {}
            }
            self.advance();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::parser::scanner::Scanner;

    fn t(token_type: TokenType, lexeme: &str) -> Token {
        Token::new(token_type, lexeme.to_string(), 1)
    }

    fn parse_sql(sql: &str) -> Result<Vec<Stmt>, String> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new())
            .scan_tokens()
            .map_err(|e| e.to_string())?;
        let mut p = Parser::new(tokens);
        let stmts = p.parse();
        if !p.errors.is_empty() {
            return Err(p.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "));
        }
        Ok(stmts)
    }

    fn one(sql: &str) -> Stmt {
        let mut s = parse_sql(sql).expect("should parse");
        assert_eq!(s.len(), 1, "expected exactly one statement from {}", sql);
        s.remove(0)
    }

    #[test]
    fn test_parse_begin_agent_session() {
        match one("BEGIN AGENT SESSION AS 'pricing-agent' RUN 'r_8fk2';") {
            Stmt::BeginAgentSession { agent, run } => {
                assert_eq!(agent, "pricing-agent");
                assert_eq!(run.as_deref(), Some("r_8fk2"));
            }
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
        // RUN is optional
        match one("BEGIN AGENT SESSION AS 'pricing-agent';") {
            Stmt::BeginAgentSession { agent, run } => {
                assert_eq!(agent, "pricing-agent");
                assert!(run.is_none());
            }
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
        // plain BEGIN still means a transaction
        assert!(matches!(one("BEGIN;"), Stmt::Begin));
    }

    #[test]
    fn test_begin_agent_session_requires_a_quoted_agent_id() {
        assert!(parse_sql("BEGIN AGENT SESSION AS pricing;").is_err());
        assert!(parse_sql("BEGIN AGENT AS 'a';").is_err());
    }

    #[test]
    fn test_parse_diff_merge_abandon() {
        assert!(matches!(one("DIFF;"), Stmt::Diff { branch: None }));
        assert!(matches!(one("MERGE;"), Stmt::Merge { branch: None }));
        assert!(matches!(one("ABANDON;"), Stmt::Abandon { branch: None }));
        match one("DIFF BRANCH b_7;") {
            Stmt::Diff { branch: Some(b) } => assert_eq!(b.name, "b_7"),
            other => panic!("expected Diff, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_select_as_of_branch() {
        match one("SELECT * FROM inventory AS OF BRANCH b_123;") {
            Stmt::Select { from, .. } => {
                assert_eq!(from.name, "inventory");
                assert_eq!(from.alias, None);
                assert_eq!(from.as_of, Some(BranchRef::new("b_123")));
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn test_as_of_coexists_with_alias_and_where() {
        match one("SELECT i.qty FROM inventory i AS OF BRANCH b_2 WHERE i.qty > 0;") {
            Stmt::Select { from, where_clause, .. } => {
                assert_eq!(from.alias, Some("i".to_string()));
                assert_eq!(from.as_of, Some(BranchRef::new("b_2")));
                assert!(where_clause.is_some());
            }
            other => panic!("expected Select, got {:?}", other),
        }
        // `AS alias` still works and is not confused with `AS OF`
        match one("SELECT * FROM inventory AS i;") {
            Stmt::Select { from, .. } => {
                assert_eq!(from.alias, Some("i".to_string()));
                assert!(from.as_of.is_none());
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    #[test]
    fn test_as_of_without_branch_keyword_is_an_error() {
        assert!(parse_sql("SELECT * FROM t AS OF b_1;").is_err());
        assert!(parse_sql("SELECT * FROM t AS OF BRANCH;").is_err());
    }

    #[test]
    fn test_parse_revert_merge() {
        match one("REVERT MERGE m_44 CASCADE;") {
            Stmt::RevertMerge { merge_id, cascade } => {
                assert_eq!(merge_id, "m_44");
                assert!(cascade);
            }
            other => panic!("expected RevertMerge, got {:?}", other),
        }
        // no CASCADE means halt-and-report, which is the deliberate default
        match one("REVERT MERGE m_44;") {
            Stmt::RevertMerge { merge_id, cascade } => {
                assert_eq!(merge_id, "m_44");
                assert!(!cascade);
            }
            other => panic!("expected RevertMerge, got {:?}", other),
        }
        assert!(parse_sql("REVERT m_44;").is_err());
    }

    #[test]
    fn test_expr_renders_back_to_sql_for_guard_capture() {
        match one("SELECT * FROM t WHERE qty >= 5 AND name = 'a';") {
            Stmt::Select { where_clause: Some(w), .. } => {
                assert_eq!(w.to_sql(), "qty >= 5 AND name = 'a'");
            }
            other => panic!("expected Select with WHERE, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_select_str() {
        let tokens = vec![
            t(TokenType::Select, "SELECT"),
            t(TokenType::Identifier, "u"),
            t(TokenType::Dot, "."),
            t(TokenType::Star, "*"),
            t(TokenType::Comma, ","),
            t(TokenType::Identifier, "p"),
            t(TokenType::Dot, "."),
            t(TokenType::Identifier, "id"),
            t(TokenType::From, "FROM"),
            t(TokenType::Identifier, "users"),
            t(TokenType::Identifier, "u"),
            t(TokenType::Semicolon, ";"),
            t(TokenType::Eof, ""),
        ];

        let mut parser = Parser::new(tokens);
        let stmts = parser.parse();
        assert!(parser.errors.is_empty());
        assert_eq!(stmts.len(), 1);

        match &stmts[0] {
            Stmt::Select {from, columns, where_clause, ..} => {
                assert_eq!(from.name, "users");
                assert!(where_clause.is_none());
                assert_eq!(from.alias, Some("u".to_string()));
                assert_eq!(columns.len(), 2);

                match &columns[0] {
                    Expr::ColumnRef { table, column } => {
                        assert_eq!(table, &Some("u".to_string()));
                        assert_eq!(column, "*");
                    }
                    _ => panic!("bruh")
                }

                match &columns[1] {
                    Expr::ColumnRef { table, column } => {
                        assert_eq!(table, &Some("p".to_string()));
                        assert_eq!(column, "id");
                    }
                    _ => panic!("bruh")
                }
            }
            _ => panic!("bruh")
        }
    }

    #[test]
    fn test_parse_insert() {
        let tokens = vec![
            t(TokenType::Insert, "INSERT"),
            t(TokenType::Into, "INTO"),
            t(TokenType::Identifier, "users"),
            t(TokenType::Values, "VALUES"), 
            t(TokenType::LeftParen, "("),
            t(TokenType::Number, "67"),
            t(TokenType::Comma, ","),
            t(TokenType::String, "\"idk\""),
            t(TokenType::RightParen, ")"),
            t(TokenType::Semicolon, ";"),
            t(TokenType::Eof, "")
        ];

        let mut parser = Parser::new(tokens);
        let stmts = parser.parse();

        assert!(parser.errors.is_empty());
        match &stmts[0] {
            Stmt::Insert { table, values } => {
                assert_eq!(table, "users");
                assert_eq!(values.len(), 2);
                assert!(matches!(values[0], Expr::Literal { value_type: TokenType::Number, ref value } if value == "67"));
                assert!(matches!(values[1], Expr::Literal { value_type: TokenType::String, ref value } if value == "\"idk\""))
            }
            _ => panic!("bruh")
        }
    }

    #[test]
    fn test_update_with_where() {
        let tokens = vec![
            t(TokenType::Update, "UPDATE"),
            t(TokenType::Identifier, "users"),
            t(TokenType::Set, "SET"),
            t(TokenType::Identifier, "age"), 
            t(TokenType::Equal, "="),
            t(TokenType::Number, "67"),
            t(TokenType::Where, "WHERE"),
            t(TokenType::Identifier, "id"),
            t(TokenType::Equal, "="),
            t(TokenType::Number, "1"),
            t(TokenType::Semicolon, ";"),
            t(TokenType::Eof, ""),
        ];
        let mut parser = Parser::new(tokens);
        let stmts = parser.parse();
        assert!(parser.errors.is_empty());

        match &stmts[0] {
            Stmt::Update { table, assignments, where_clause } => {
                assert_eq!(table, "users");
                assert_eq!(assignments.len(),1);
                assert_eq!(assignments[0].0, "age");
                assert!(matches!(assignments[0].1, Expr::Literal { value_type: TokenType::Number, ref value } if value == "67"));
                assert!(matches!(where_clause.as_ref().unwrap(), Expr::BinaryOp { operator: TokenType::Equal, .. }))
            }
            _ => panic!("bruh")
        }
    }

    #[test]
    fn test_parse_create_table() {
        let tokens = vec![
            t(TokenType::Create, "CREATE"),
            t(TokenType::Table, "TABLE"),
            t(TokenType::Identifier, "users"),
            t(TokenType::LeftParen, "("),
            t(TokenType::Identifier, "id"),
            t(TokenType::TypeInt, "INTEGER"),
            t(TokenType::Comma, ","),
            t(TokenType::Identifier, "username"),
            t(TokenType::TypeVarchar, "VARCHAR"),
            t(TokenType::LeftParen, "("),
            t(TokenType::Number, "2"),
            t(TokenType::RightParen, ")"),
            t(TokenType::Not, "NOT"),
            t(TokenType::Null, "NULL"),
            t(TokenType::RightParen, ")"),
            t(TokenType::Semicolon, ";"),
            t(TokenType::Eof, ""),
        ];

        let mut parser = Parser::new(tokens);
        let stmts = parser.parse();

        assert!(parser.errors.is_empty());

        match &stmts[0] {
            Stmt::CreateTable { table, columns } => {
                assert_eq!(table, "users");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
                assert!(matches!(columns[0].data_type, DataType::Integer));
                assert!(columns[0].nullable);
                assert_eq!(columns[1].name, "username");
                assert!(matches!(columns[1].data_type, DataType::Varchar(2)));
                assert!(!columns[1].nullable);
            }
            _ => panic!("bruh")
        }
    }

    #[test]
    fn test_panic_mode_synchronization() {
        let tokens = vec![
            //invalid
            t(TokenType::Select, "SELECT"),
            t(TokenType::Star, "*"),
            t(TokenType::Identifier, "users"),
            t(TokenType::Semicolon, ";"),

            // valid
            t(TokenType::Delete, "DELETE"), 
            t(TokenType::From, "FROM"),
            t(TokenType::Identifier, "users"),
            t(TokenType::Semicolon, ";"),
            t(TokenType::Eof, ""),
        ];

        let mut parser = Parser::new(tokens);
        let stmts = parser.parse();

        assert_eq!(parser.errors.len(), 1);
        assert_eq!(stmts.len(), 1);

        match &stmts[0] {
            Stmt::Delete { table, .. } => {
                assert_eq!(table, "users");
            }
            _ => panic!("bruh")
        }
    }

    #[test]
    fn test_select_with_join() {
        // SELECT u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.user_id;
        let tokens = vec![
            t(TokenType::Select, "SELECT"),
            t(TokenType::Identifier, "u"),
            t(TokenType::Dot, "."),
            t(TokenType::Identifier, "name"),
            t(TokenType::Comma, ","),
            t(TokenType::Identifier, "p"),
            t(TokenType::Dot, "."),
            t(TokenType::Identifier, "title"),
            t(TokenType::From, "FROM"),
            t(TokenType::Identifier, "users"),
            t(TokenType::Identifier, "u"),
            t(TokenType::Identifier, "INNER"),
            t(TokenType::Join, "JOIN"),
            t(TokenType::Identifier, "posts"),
            t(TokenType::Identifier, "p"),
            t(TokenType::On, "ON"),
            t(TokenType::Identifier, "u"),
            t(TokenType::Dot, "."),
            t(TokenType::Identifier, "id"),
            t(TokenType::Equal, "="),
            t(TokenType::Identifier, "p"),
            t(TokenType::Dot, "."),
            t(TokenType::Identifier, "user_id"),
            t(TokenType::Semicolon, ";"),
            t(TokenType::Eof, ""),
        ];

        let mut parser = Parser::new(tokens);
        let stmts = parser.parse();
        assert!(parser.errors.is_empty());
        assert_eq!(stmts.len(), 1);

        match &stmts[0] {
            Stmt::Select { from, columns: _, where_clause: _, joins } => {
                assert_eq!(from.name, "users");
                assert_eq!(from.alias, Some("u".to_string()));

                assert_eq!(joins.len(), 1);
                assert!(matches!(&joins[0].join_type, JoinType::Inner));
                assert_eq!(&joins[0].table.name, "posts");
                assert_eq!(joins[0].table.alias, Some("p".to_string()));

                match &joins[0].on {
                    Expr::BinaryOp { left, operator, right } => {
                        assert_eq!(*operator, TokenType::Equal);
                        if let Expr::ColumnRef { table, column } = &**left {
                            assert_eq!(table, &Some("u".to_string()));
                            assert_eq!(column, "id");
                        } else {
                            panic!("bruh");
                        }
                        if let Expr::ColumnRef { table, column } = &**right {
                            assert_eq!(table, &Some("p".to_string()));
                            assert_eq!(column, "user_id");
                        } else {
                            panic!("bruh");
                        }
                    }
                    _ => panic!("bruh")
                }
            }
            _ => panic!("bruh")
        }
    }
}