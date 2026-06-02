use crate::{catalog::column::Column, parser::scanner::{Scanner, Token, TokenType::{self}}};

pub struct Parser {
    tokens: Vec<Token>,
    current: usize,
}

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

pub struct AST {
    
}
// OR -> AND -> NOT -> equality/comparison -> term -> factor -> unary -> primary
impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self{
        Self {tokens, current: 0}
    }

    pub fn parse_statement(&mut self) {
        todo!()
    }
    pub fn match_token(&mut self, types: Vec<TokenType>) -> bool{
        for token_type in types {
            if self.check(token_type) {
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
        return self.peek().token_type == TokenType::Eof
    }

    pub fn peek(&self) -> Token{
        self.tokens[self.current].clone()
    }

    pub fn previous(&self) -> Token{
        self.tokens[self.current - 1].clone()
    }

    pub fn consume(&mut self, token_type: TokenType) -> Token{
        if self.check(token_type){
            return self.advance();
        }
        panic!("todo error handling")
    }

    pub fn expression(&mut self ) -> Expr{
        return self.or();
    }

    pub fn or(&mut self) -> Expr{
        let mut expr = self.and();
        while self.match_token(vec![TokenType::Or]) {
            let operator = self.previous().token_type;
            let right = self.and();
            expr = Expr::BinaryOp { left: Box::new(expr), operator, right: Box::new(right) };
        }
        expr
    }

    pub fn and(&mut self) -> Expr{
        let mut expr = self.not();
        while self.match_token(vec![TokenType::And]) {
            let operator = self.previous().token_type;
            let right = self.not();
            expr = Expr::BinaryOp { left: Box::new(expr), operator, right: Box::new(right) };
        }
        expr
    }

    pub fn not(&mut self) -> Expr{
        if self.match_token(vec![TokenType::Not]) {
            let operator = self.previous().token_type;
            let right = self.not();
            return Expr::UnaryOp { operator, right: Box::new(right) };
        }
        self.equality()
    }
    
    pub fn equality(&mut self) -> Expr{
        let mut expr = self.comparison();
        while self.match_token(vec![TokenType::BangEqual, TokenType::Equal]){
            let operator = self.previous().token_type;
            let right = self.comparison();
            expr = Expr::BinaryOp { left: Box::new(expr), operator, right: Box::new(right) };
        }
        expr
    }

    pub fn comparison(&mut self) -> Expr{
        let mut expr = self.term();
        while self.match_token(vec![TokenType::Greater, TokenType::GreaterEqual, TokenType::Less, TokenType::LessEqual]) {
            let operator = self.previous().token_type;
            let right = self.term();
            expr = Expr::BinaryOp { left: Box::new(expr), operator, right: Box::new(right) };
        }
        expr
    }
    
    pub fn term(&mut self) -> Expr{
        let mut expr = self.factor();
        while self.match_token(vec![TokenType::Minus, TokenType::Plus]) {
            let operator = self.previous().token_type;
            let right = self.factor();
            expr = Expr::BinaryOp { left: Box::new(expr), operator, right: Box::new(right) };
        }
        expr
    }

    pub fn factor(&mut self) -> Expr{
        let mut expr = self.unary();
        while self.match_token(vec![TokenType::Slash, TokenType::Star]) {
            let operator = self.previous().token_type;
            let right = self.unary();
            expr = Expr::BinaryOp { left: Box::new(expr), operator, right: Box::new(right) };
        }
        expr

    }

    pub fn unary(&mut self) -> Expr{
        if self.match_token(vec![TokenType::Bang, TokenType::Minus]) {
            let operator = self.previous().token_type;
            let right = self.unary();
            return  Expr::UnaryOp { operator, right: Box::new(right) };
        }
        self.primary()

    }

    pub fn primary(&mut self) -> Expr{
        if self.match_token(vec![TokenType::False]) {return Expr::Literal { value_type: TokenType::False, value: String::from("false") }}
        if self.match_token(vec![TokenType::True]) {return Expr::Literal { value_type: TokenType::True, value: String::from("true") }}
        if self.match_token(vec![TokenType::Null]) {return Expr::Literal { value_type: TokenType::Null, value: String::from("null") }}
        if self.match_token(vec![TokenType::Number, TokenType::String]) {
            let prev = self.previous();
            return Expr::Literal { value_type: prev.token_type, value: prev.lexeme }
        }

        if self.match_token(vec![TokenType::Identifier]) {
            return Expr::ColumnRef(self.previous().lexeme);
        }

        if self.match_token(vec![TokenType::LeftParen]) {
            let expr = self.expression();
            self.consume(TokenType::RightParen);
            return Expr::Grouping(Box::new(expr))
        }
        panic!("todo handle error")
    }

}