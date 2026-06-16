use crate::error::FerroError;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TokenType {
    LeftParen, RightParen, Comma, Dot, Minus, Plus, Semicolon, Slash, Star, 

    Bang, BangEqual, Equal, Greater, GreaterEqual, Less, LessEqual,

    Identifier, String, Number, 

    And, Not, False, Null, Or, True, 
    Create, Table, Insert, Into, Values, Select, From, Where, Update, Set, Delete, Index, On, As, Join, Outer, Analyze,

    TypeInt, TypeVarchar, TypeFloat, TypeBoolean, TypeNull,
    Eof,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Token {
    pub token_type: TokenType,
    pub lexeme: String,
    pub line: usize,
}

pub struct Scanner {
    pub source: Vec<char>,
    pub tokens: Vec<Token>,
    pub start: usize,
    pub current: usize,
    pub line: usize,
    pub errors: Vec<FerroError>
}

impl Token {
    pub fn new(token_type: TokenType, lexeme: String, line: usize) -> Self {
        return Token { token_type, lexeme, line }
    }
}

impl Scanner {
    pub fn new(source: Vec<char>, tokens: Vec<Token>) -> Self{
        Self {source, tokens, start: 0, current: 0, line: 1, errors: Vec::new()}
    }

    pub fn scan_tokens(mut self) -> Result<Vec<Token>, FerroError>{
        while !self.is_at_end() {
            self.start = self.current;
            self.scan_token();
        }
        if self.errors.is_empty() {
            self.tokens.push(Token::new(TokenType::Eof, "".to_string(), self.line));
            return Ok(self.tokens)
        }
        return Err(FerroError::SqlParseError(self.errors.iter().map(|e| e.to_string()).collect::<Vec<String>>().join("\n")))
    }
    pub fn scan_token(&mut self){
        
        let c = self.advance();
        match c {
            '(' => self.add_token(TokenType::LeftParen),
            ')' => self.add_token(TokenType::RightParen),
            ',' => self.add_token(TokenType::Comma),
            '.' => self.add_token(TokenType::Dot),
            '-' => {
                if self.match_char('-') {
                    while self.peek() != '\n' && !self.is_at_end() {
                        self.advance();
                    }
                }else {
                    self.add_token(TokenType::Minus)
                }
                
            },
            '/' => self.add_token(TokenType::Slash),
            '+' => self.add_token(TokenType::Plus),
            ';' => self.add_token(TokenType::Semicolon),
            '*' => self.add_token(TokenType::Star),
            '!' => {
                let matches = self.match_char('=');
                self.add_token(if matches {TokenType::BangEqual} else {TokenType::Bang})
            }
            '=' => self.add_token(TokenType::Equal),
            '<' => {
                if self.match_char('=') {
                    self.add_token(TokenType::LessEqual)
                }else if self.match_char('>'){
                    self.add_token(TokenType::BangEqual)
                } else {
                    self.add_token(TokenType::Less)
                }
            }
            '>' => {
                let matches = self.match_char('=');
                self.add_token(if matches {TokenType::GreaterEqual} else {TokenType::Greater})
            }
            '\n' => self.line += 1,
            ' ' | '\r' | '\t' => {},
            '\'' => self.string(),
            _ => {
                if c.is_ascii_digit() {
                    self.number();
                } else if c.is_ascii_alphabetic() || c == '_' {
                    self.identifier();
                } else {
                    self.errors.push(Scanner::error(self.line, format!("unexpected character: {}", c)))
                }
            }
        }
    }

    pub fn is_at_end(&self) -> bool {
        return self.current >= self.source.len();
    }

    pub fn advance(&mut self) -> char{
        self.current += 1;
        return self.source[self.current-1];
    }

    pub fn add_token(&mut self, token_type: TokenType) {
        let text: String = self.source[self.start..self.current].iter().collect();
        self.tokens.push(Token::new(token_type, text, self.line));
    }

    pub fn match_char(&mut self, expected: char) -> bool {
        if self.is_at_end() || self.source[self.current] != expected {
            return false;
        }
        self.current += 1;
        true
    }

    pub fn string(&mut self) {
        while self.peek() != '\'' && !self.is_at_end() {
            if self.peek() == '\n' {
                self.line += 1;
            }
            self.advance();
        }
        if self.is_at_end() {
            self.errors.push(Scanner::error(self.line, "no closing quote".to_string()));
        }
        self.advance();

        let value:String = self.source[self.start + 1..self.current-1].iter().collect();
        self.tokens.push(Token::new(TokenType::String, value, self.line));
    }

    pub fn number(&mut self) {
        while !self.is_at_end() && self.source[self.current].is_ascii_digit() {
            self.advance();
        }

        if self.peek() == '.' && !self.is_at_end() {
            if self.current + 1 < self.source.len() && self.source[self.current + 1].is_ascii_digit() {
                self.advance();

                while !self.is_at_end() && self.source[self.current].is_ascii_digit() {
                    self.advance();
                }
            }
        }
        let value: String = self.source[self.start..self.current].iter().collect();
        self.tokens.push(Token::new(TokenType::Number, value, self.line));
    }

    pub fn identifier(&mut self) {
        while !self.is_at_end() && (self.source[self.current].is_ascii_alphanumeric() || self.source[self.current] == '_') {
            self.advance();
        }
        let text: String = self.source[self.start..self.current].iter().collect();
        let token_type = match text.to_uppercase().as_str() {
            "SELECT" => TokenType::Select,
            "FROM" => TokenType::From,
            "WHERE" => TokenType::Where,
            "INSERT" => TokenType::Insert,
            "INTO" => TokenType::Into,
            "VALUES" => TokenType::Values,
            "CREATE" => TokenType::Create,
            "TABLE" => TokenType::Table,
            "AND" => TokenType::And,
            "OR" => TokenType::Or,
            "UPDATE" => TokenType::Update,
            "SET" => TokenType::Set,
            "DELETE" => TokenType::Delete,
            "INDEX" => TokenType::Index,
            "ON" => TokenType::On,
            "NOT" => TokenType::Not,
            "TRUE" => TokenType::True,
            "FALSE" => TokenType::False,
            "INTEGER" => TokenType::TypeInt,
            "FLOAT" => TokenType::TypeFloat,
            "BOOLEAN" => TokenType::TypeBoolean,
            "VARCHAR" => TokenType::TypeVarchar,
            "NULL" => TokenType::Null,
            "AS" => TokenType::As,
            "JOIN" => TokenType::Join,
            "OUTER" => TokenType::Outer,
            "ANALYZE" => TokenType::Analyze,
            _ => TokenType::Identifier
        };
        self.add_token(token_type);
    }
    pub fn peek(&self) -> char{
        if self.is_at_end() {return '\0'}
        return self.source[self.current];
    }

    pub fn error(line: usize, message: String) -> FerroError{
        FerroError::SqlParseError(format!("Line {}: {}", line, message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(input : &str) -> Vec<TokenType> {
        let scanner = Scanner::new(input.chars().collect(), Vec::new());
        scanner.scan_tokens().unwrap().iter().map(|t| t.token_type).collect()
    }

    #[test]
    fn test_select_statement() {
        use TokenType::*;
        let toks = scan("SELECT * FROM users WHERE age > 18;");
        assert_eq!(toks, vec![
            Select, Star, From, Identifier, Where, Identifier, Greater, Number, Semicolon, Eof
        ]);
    }

    #[test]
    fn test_whitespace_ignored() {
        use TokenType::*;
        let toks = scan("SELECT\t*\n  FROM   t ;");
        assert_eq!(toks, vec![Select, Star, From, Identifier, Semicolon, Eof]);
    }

    #[test]
    fn test_all_keywords() {
        use TokenType::*;
        let toks = scan("CREATE TABLE INSERT INTO VALUES UPDATE SET DELETE INDEX ON AND OR NOT NULL TRUE FALSE");
        assert_eq!(toks, vec![
            Create, Table, Insert, Into, Values, Update, Set, Delete, Index, On,
            And, Or, Not, Null, True, False, Eof
        ]);
    }

    #[test]
    fn test_identifier_vs_keyword() {
        use TokenType::*;
        let toks = scan("selected user_name _temp col1");
        assert_eq!(toks, vec![Identifier, Identifier, Identifier, Identifier, Eof]);
    }

    #[test]
    fn test_operators() {
        use TokenType::*;
        let toks = scan("= != <> < <= > >= + - * /");
        assert_eq!(toks, vec![
            Equal, BangEqual, BangEqual, Less, LessEqual, Greater, GreaterEqual,
            Plus, Minus, Star, Slash, Eof
        ]);
    }
}