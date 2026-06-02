use crate::{catalog::column::Column, error::FerroError, parser::scanner::{Scanner, Token, TokenType::{self}}};

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
        columns: Vec<Expr>,
        table: String,
        where_clause: Option<Expr>
    },
    Insert {
        table: String,
        values: String
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
        name: String,
        columns: Vec<Column>,
    },
    CreateIndex {
        index_name: String,
        table_name: String,
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
        todo!()
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