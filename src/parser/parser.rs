use crate::{catalog::column::{Column, DataType}, error::FerroError, parser::scanner::{Scanner, Token, TokenType::{self}}};

pub struct Parser {
    pub tokens: Vec<Token>,
    pub current: usize,
    pub errors: Vec<FerroError>
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
    ColumnRef(String),
    // for parentheses overriding precedence
    Grouping(Box<Expr>),
}

#[derive(Debug, Clone)]
pub enum Stmt {
    Select {
        table: String,
        columns: Vec<Expr>,
        where_clause: Option<Expr>
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
    }
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

    // SELECT vals FROM table WHERE expr (optional)
    pub fn parse_select(&mut self) -> Result<Stmt, FerroError>{
        let mut columns = Vec::new();
        if self.match_token(&[TokenType::Star]) {
            columns.push(Expr::ColumnRef("*".to_string()));
        }else {
            loop {
                columns.push(self.expression()?);
                if !self.match_token(&[TokenType::Comma]) {break;}
            }
        }
        self.consume(TokenType::From, "expected FROM")?;
        let table = self.consume(TokenType::Identifier, "expected table name")?.lexeme;
        let where_clause = if self.match_token(&[TokenType::Where]) {
            Some(self.expression()?)
        } else {
            None
        };
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::Select { table, columns, where_clause})
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
            return Ok(Expr::ColumnRef(self.previous().lexeme));
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