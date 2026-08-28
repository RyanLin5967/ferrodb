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

/// What one `ALTER TABLE` does to one column.
///
/// **Column-level, and end-anchored.** There is no `DROP COLUMN` and no positional `ADD ... AFTER`,
/// because a column ordinal is the identity of a column everywhere below the parser: tuple bytes
/// are laid out positionally, `tel::ColId` is documented as "a column, by ordinal within its
/// table's schema", and every captured `Op`, `Guard` and merge-policy key holds one. Removing a
/// column, or inserting one anywhere but the end, silently re-points all of them at a different
/// column — and a merge would still report `Clean`. Appending cannot.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterAction {
    /// `ADD COLUMN c <type> [NULL]`. Appended at the end of the table.
    AddColumn(Column),
    /// `RENAME COLUMN a TO b`. Names live only in the catalog, so no row is touched.
    RenameColumn { from: String, to: String },
    /// `ALTER COLUMN c TYPE <type>`. Only conversions the catalog can prove total are accepted;
    /// see `catalog::alter`.
    RetypeColumn { column: String, to: DataType },
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
    /// `DROP TABLE t` — E69.
    ///
    /// The WAL has carried a `DdlOp::DropTable` record type, the decoder has turned it into a
    /// `DROP_TABLE` event, and the Go sink has had a branch for it since E15 — but nothing could ever
    /// write one, because `DdlOp::CreateTable` was the only op any code path logged. The README says
    /// "`CREATE_TABLE` and `DROP_TABLE` are" carried; half of that was untrue.
    DropTable {
        table: String,
    },
    /// `ALTER TABLE t ADD COLUMN c INTEGER;` and friends — B11.
    ///
    /// One statement, one column-level action. `SchemaChange` had exactly two variants before
    /// this, both whole-table, so the most common real-world CDC break — a column added, renamed
    /// or retyped mid-stream — had no representation anywhere in the system.
    AlterTable {
        table: String,
        action: AlterAction,
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
    /// `CREATE FULLTEXT INDEX ix ON docs (body);` — B8.
    ///
    /// Same shape as `CreateIndex`, including that `index_name` is parsed and then dropped: the
    /// catalog's identity for an index is `(table, column)`, and inventing a second identity for
    /// full-text indexes alone would be a surface this engine cannot honour anywhere else.
    CreateFullTextIndex {
        index_name: String,
        table: String,
        column_name: String
    },
    /// `SEARCH docs (body) FOR 'wireless charger' TOP 5;` — B8, ranked retrieval.
    ///
    /// **Why this is a statement and not a `WHERE` predicate.** Retrieval is not a filter: it
    /// returns the *best* rows, so it needs a ranking and a bound, and this SQL surface has neither
    /// `ORDER BY` nor `LIMIT` to supply them. Expressing it as a predicate would also mean either a
    /// new field on `Stmt::Select` or a new `BoundExpr` variant, and both are destructured
    /// field-by-field inside `src/agent_sql/` and `src/tel/` — a retrieval feature is not a reason
    /// to reach into the merge engine.
    ///
    /// `TOP` is optional; when it is absent the operator uses its own default bound
    /// (`execution::fulltext_search::DEFAULT_TOP_K`), which is the honest reading of "there is no
    /// LIMIT here": a bound still exists, so it is named rather than implied.
    Search {
        table: String,
        column_name: String,
        query: String,
        top_k: Option<usize>,
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
    /// `BEGIN AGENT SESSION AS 'pricing-agent' RUN 'r_8fk2' MODEL 'claude-opus-5/2026-05'`
    /// `  PROMPT 'top up everything below reorder';`
    ///
    /// Forks a branch for one agent task. The fork copies zero data pages (exit criterion 1);
    /// the agent identity, run id, model and prompt are what provenance interns (exit criterion 9).
    ///
    /// `MODEL` is optional because a non-agent client has no model to declare, but criterion 9
    /// names the model explicitly, so leaving it out is recorded as the literal string
    /// `unspecified` rather than silently attributed to anything.
    BeginAgentSession {
        agent: String,
        run: Option<String>,
        /// `name/version`; the version half is optional.
        model: Option<String>,
        /// The prompt behind the run, as typed. **Held as text only as far as the runtime**, which
        /// interns `prompt_digest(prompt)` into `RunEntity::prompt_hash` and drops the text: the
        /// digest is the whole point of the field, so that a prompt containing customer data does
        /// not become a durable copy of it.
        ///
        /// `None` is the clause omitted and stays the all-zero hash, which is a *different* value
        /// from `prompt_digest("")` — "no prompt was declared" and "the prompt was empty" are
        /// different facts and the column must be able to tell them apart.
        prompt: Option<String>,
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
    /// ```text
    /// SIMULATE AS 'pricing-agent' RUN 'r_9'
    ///   CANDIDATE 'cut-5'  ( UPDATE inventory SET qty = qty - 5 WHERE id = 1; )
    ///   CANDIDATE 'cut-8'  ( UPDATE inventory SET qty = qty - 8 WHERE id = 1; )
    ///   ASSERT ON inventory (qty >= 0)
    ///   ADMIT ALL;
    /// ```
    ///
    /// Fork one branch per candidate off the current base, run each candidate on its own branch,
    /// score every one against the declared assertions through the gate a production `MERGE`
    /// uses, and admit the winners. The losers are left for the lease reaper.
    Simulate {
        agent: String,
        run: Option<String>,
        /// `name/version`; the version half is optional, as on `BEGIN AGENT SESSION`.
        model: Option<String>,
        /// `(name, body)` in declaration order. A body may be empty — "change nothing" is a
        /// legitimate candidate to compare the others against.
        candidates: Vec<(String, Vec<Stmt>)>,
        /// `(table, predicate)`. The predicate is checked against every row of that table as the
        /// merge would leave it.
        assertions: Vec<(String, Expr)>,
        admit: AdmitSpec,
    },
}

/// How many winners `SIMULATE` may admit. There is no default: how much of a simulation's output
/// to publish is the caller's decision, and guessing it wrong publishes work nobody asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitSpec {
    /// Every candidate still admissible when its turn comes.
    All,
    /// At most `n`. `ADMIT 0` scores everything and publishes nothing.
    AtMost(usize),
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
        } else if self.match_token(&[TokenType::Simulate]) {
            return self.parse_simulate()
        } else if self.match_token(&[TokenType::Commit]) {
            return self.parse_txn_stmt(Stmt::Commit)
        } else if self.match_token(&[TokenType::Rollback]) {
            return self.parse_txn_stmt(Stmt::Rollback)
        } else if self.match_token(&[TokenType::Drop]) {
            self.consume(TokenType::Table, "expected TABLE after DROP")?;
            let table = self.consume(TokenType::Identifier, "expected table name")?.lexeme;
            self.consume(TokenType::Semicolon, "expected ;")?;
            return Ok(Stmt::DropTable { table })
        } else if self.match_token(&[TokenType::Search]) {
            return self.parse_search()
        } else if self.match_token(&[TokenType::Alter]) {
            return self.parse_alter_table()
        } else if self.match_token(&[TokenType::Create]){
            if self.match_token(&[TokenType::Index]) {
                return self.parse_create_index()
            } else if self.match_token(&[TokenType::Fulltext]) {
                self.consume(TokenType::Index, "expected INDEX after FULLTEXT")?;
                return self.parse_create_fulltext_index()
            } else if self.match_token(&[TokenType::Table]) {
                return self.parse_create_table()
            } else {
                return Err(Parser::error(self.peek(), "expected TABLE, INDEX or FULLTEXT INDEX after CREATE".into()));
            }
        } else {
            if let Some(e) = self.unsupported_here() {
                return Err(e);
            }
            return Err(Parser::error(self.peek(), "expected a statement".into()));
        }
    }

    // SELECT vals FROM table1 AS t INNER JOIN table2 AS s ON t.x = s.y WHERE expr
    /// **E67 — a feature this parser does not have must not read as a typo.**
    ///
    /// Measured 2026-08-17, through the shipped binary:
    ///
    /// ```text
    /// SELECT COUNT(*) FROM t;              -> 1 at ' ( ' expected FROM
    /// SELECT * FROM t ORDER BY v;          -> 1 at ' BY ' expected ;
    /// SELECT * FROM t LIMIT 1;             -> 1 at ' 1 ' expected ;
    /// DROP TABLE t;                        -> 1 at ' DROP ' expected a statement
    /// ```
    ///
    /// Every one of those is valid SQL that this database does not implement, and every message says
    /// the reader mistyped something. "expected ;" after `ORDER` is the worst of them: it points at
    /// `BY`, which is not the problem, and invites the reader to hunt for a punctuation error in a
    /// statement whose punctuation is correct.
    ///
    /// The keywords are matched by uppercased lexeme rather than by token type because none of them is
    /// a token - the scanner has no `ORDER`, `LIMIT` or `DROP` - and that is the same idiom
    /// `parse_select` already uses for `INNER`/`LEFT`/`RIGHT`/`FULL` just above.
    ///
    /// This is an ALLOWLIST of things known to be missing, so it can only ever improve a message; a
    /// word not on it falls through to the ordinary syntax error, which is the right answer for an
    /// actual typo. Add to it when a feature is asked for and refused.
    fn unsupported_here(&self) -> Option<FerroError> {
        let tok = self.peek();
        let what = Self::unsupported_keyword(&tok.lexeme)?;
        Some(FerroError::SqlParseError(format!(
            "{} at ' {} ': {what} is not supported by this database",
            tok.line, tok.lexeme
        )))
    }

    /// SQL this parser does not implement, keyed by the word that starts it.
    ///
    /// **One list, two consumers, and that is the point.** `parse_table_ref` had its own denylist of
    /// words that must not be taken as a table alias - `JOIN`, `WHERE`, `ON`, the join words - and
    /// `ORDER`/`LIMIT`/`GROUP` were not on it. So `SELECT * FROM t ORDER BY v` parsed as "table `t`,
    /// aliased `ORDER`" and then complained about `BY`: the reason the error pointed at a word that
    /// was not the problem, and the reason a refusal added at the end of `parse_select` never fired,
    /// because the keyword had already been eaten three functions earlier.
    ///
    /// A word added here now fixes the alias guard and the message together. Keeping them separate is
    /// what produced a denylist missing three entries in the first place.
    fn unsupported_keyword(lexeme: &str) -> Option<&'static str> {
        Some(match lexeme.to_uppercase().as_str() {
            "ORDER" => "ORDER BY",
            "GROUP" => "GROUP BY",
            "HAVING" => "HAVING",
            "LIMIT" => "LIMIT",
            "OFFSET" => "OFFSET",
            "UNION" => "UNION",
            "DISTINCT" => "DISTINCT",
            "TRUNCATE" => "TRUNCATE",
            _ => return None,
        })
    }

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
        // `SELECT COUNT(id) FROM t` stops with the expression parsed as a bare column reference and
        // the `(` still unread, so the failure surfaced as "expected FROM" - a message about the wrong
        // token entirely. There are no functions or aggregates in this SQL surface; say that.
        if self.check(TokenType::LeftParen) {
            return Err(FerroError::SqlParseError(format!(
                "{} at ' {} ': function calls and aggregates such as COUNT, SUM and AVG are not \
                 supported by this database",
                self.peek().line,
                self.peek().lexeme
            )));
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
        if !self.check(TokenType::Semicolon) {
            if let Some(e) = self.unsupported_here() {
                return Err(e);
            }
        }
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
            let data_type = self.parse_data_type()?;
            let nullable = self.parse_nullability()?;
            columns.push(Column { name, data_type, nullable });

            if !self.match_token(&[TokenType::Comma]) {
                break;
            }
        }
        self.consume(TokenType::RightParen, "expected )")?;
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::CreateTable { table, columns })
    }

    /// One column type, spelled exactly as `CREATE TABLE` spells it.
    ///
    /// Lifted out of `parse_create_table` so `ALTER TABLE` cannot drift from it. Two parsers for
    /// one type vocabulary is how a database ends up accepting `BIGINT` in one statement and not
    /// the other, and the wire contract in `replication::logical::sql_type_of` assumes there is
    /// exactly one.
    pub fn parse_data_type(&mut self) -> Result<DataType, FerroError> {
        if self.match_token(&[TokenType::TypeInt]) {
            Ok(DataType::Integer)
        } else if self.match_token(&[TokenType::TypeBoolean]) {
            Ok(DataType::Boolean)
        } else if self.match_token(&[TokenType::TypeFloat]) {
            Ok(DataType::Float)
        } else if self.match_token(&[TokenType::TypeBigInt]) {
            Ok(DataType::BigInt)
        } else if self.match_token(&[TokenType::TypeDecimal]) {
            // No `DECIMAL(p,s)`. This engine stores the digits the writer supplied, so there
            // is nothing for a declared precision to do except introduce a rounding rule —
            // which is the loss the type exists to prevent. See `Value::Decimal`.
            Ok(DataType::Decimal)
        } else if self.match_token(&[TokenType::TypeTimestamp]) {
            Ok(DataType::Timestamp)
        } else if self.match_token(&[TokenType::TypeVarchar]) {
            self.consume(TokenType::LeftParen, "expected ( after VARCHAR")?;
            let size_token = self.consume(TokenType::Number, "expected size")?;
            let size: u16 = size_token
                .lexeme
                .parse()
                .map_err(|_| Parser::error(size_token.clone(), "invalid size".into()))?;
            self.consume(TokenType::RightParen, "expected )")?;
            Ok(DataType::Varchar(size))
        } else {
            Err(Parser::error(self.peek(), "expected data type".into()))
        }
    }

    /// The optional `NULL` / `NOT NULL` suffix. Absent means nullable, as `CREATE TABLE` has
    /// always read it.
    pub fn parse_nullability(&mut self) -> Result<bool, FerroError> {
        if self.match_token(&[TokenType::Not]) {
            if self.match_token(&[TokenType::Null]) {
                return Ok(false);
            }
            return Err(Parser::error(self.peek(), "unexpected NOT".into()));
        }
        self.match_token(&[TokenType::Null]);
        Ok(true)
    }

    /// A bare word matched by *lexeme*, case-insensitively, without reserving it.
    ///
    /// `ADD`, `COLUMN`, `RENAME`, `TO` and `TYPE` go through here rather than becoming token
    /// types. Reserving them would make `CREATE TABLE t (type INTEGER, ...)` stop parsing, and a
    /// column called `type` or `to` is an ordinary thing for someone to have. This is the idiom
    /// `parse_select` already uses for `INNER`/`LEFT`/`RIGHT`/`FULL`.
    fn match_word(&mut self, word: &str) -> bool {
        if self.is_at_end() {
            return false;
        }
        if self.peek().lexeme.to_uppercase() == word {
            self.advance();
            return true;
        }
        false
    }

    fn consume_word(&mut self, word: &str, message: &str) -> Result<(), FerroError> {
        if self.match_word(word) {
            return Ok(());
        }
        Err(Parser::error(self.peek(), message.to_string()))
    }

    /// `ALTER TABLE t ...` — B11.
    ///
    /// ```text
    /// ALTER TABLE t ADD COLUMN c INTEGER;
    /// ALTER TABLE t RENAME COLUMN old TO new;
    /// ALTER TABLE t ALTER COLUMN c TYPE BIGINT;
    /// ```
    ///
    /// `COLUMN` is required in all three rather than optional in some, because one rule produces
    /// one error message. The two shapes this deliberately refuses — `DROP COLUMN` and any
    /// positional `ADD ... AFTER`/`FIRST` — are refused *by name*, with the reason, rather than
    /// falling through to "expected ADD, RENAME or ALTER": a reader who typed valid SQL that this
    /// database will not do should be told which of those two it is (E67).
    pub fn parse_alter_table(&mut self) -> Result<Stmt, FerroError> {
        self.consume(TokenType::Table, "expected TABLE after ALTER")?;
        let table = self.consume(TokenType::Identifier, "expected table name")?.lexeme;

        // `DROP COLUMN` is real SQL and is refused on a data-model rule, not a syntax one.
        if self.check(TokenType::Drop) {
            return Err(FerroError::SqlParseError(format!(
                "{} at \' {} \': ALTER TABLE DROP COLUMN is not supported by this database. A \
                 column\'s ordinal is its identity here — tuple bytes are positional and every \
                 recorded effect, guard and merge policy holds an ordinal — so removing one \
                 silently re-points all of them at a different column. Columns can be added at the \
                 end, renamed, and retyped.",
                self.peek().line,
                self.peek().lexeme
            )));
        }

        if self.match_word("ADD") {
            self.consume_word("COLUMN", "expected COLUMN after ADD")?;
            let name = self.consume(TokenType::Identifier, "expected column name")?.lexeme;
            let data_type = self.parse_data_type()?;
            let nullable = self.parse_nullability()?;
            // Positional placement, refused by name for the same reason as DROP COLUMN.
            if self.match_word("AFTER") || self.match_word("FIRST") || self.match_word("BEFORE") {
                return Err(FerroError::SqlParseError(format!(
                    "{}: ALTER TABLE ADD COLUMN places the column at the END of the table and \
                     takes no position. A column\'s ordinal is its identity here, so inserting one \
                     mid-table re-points every recorded effect, guard and merge policy after it.",
                    self.previous().line
                )));
            }
            self.consume(TokenType::Semicolon, "expected ;")?;
            return Ok(Stmt::AlterTable {
                table,
                action: AlterAction::AddColumn(Column { name, data_type, nullable }),
            });
        }

        if self.match_word("RENAME") {
            self.consume_word("COLUMN", "expected COLUMN after RENAME")?;
            let from = self.consume(TokenType::Identifier, "expected column name")?.lexeme;
            self.consume_word("TO", "expected TO after the column name")?;
            let to = self.consume(TokenType::Identifier, "expected the new column name")?.lexeme;
            self.consume(TokenType::Semicolon, "expected ;")?;
            return Ok(Stmt::AlterTable { table, action: AlterAction::RenameColumn { from, to } });
        }

        if self.match_token(&[TokenType::Alter]) {
            self.consume_word("COLUMN", "expected COLUMN after ALTER")?;
            let column = self.consume(TokenType::Identifier, "expected column name")?.lexeme;
            self.consume_word("TYPE", "expected TYPE after the column name")?;
            let to = self.parse_data_type()?;
            self.consume(TokenType::Semicolon, "expected ;")?;
            return Ok(Stmt::AlterTable { table, action: AlterAction::RetypeColumn { column, to } });
        }

        Err(Parser::error(
            self.peek(),
            "expected ADD COLUMN, RENAME COLUMN or ALTER COLUMN after the table name".to_string(),
        ))
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

    // CREATE FULLTEXT INDEX index_name ON table (col)   — B8. `CREATE` and `FULLTEXT INDEX` are
    // already consumed. Deliberately the same token-for-token shape as `parse_create_index` above,
    // so the only thing a reader has to learn is the one extra word.
    pub fn parse_create_fulltext_index(&mut self) -> Result<Stmt, FerroError> {
        let index_name = self.consume(TokenType::Identifier, "expected index name")?.lexeme;
        if !self.match_token(&[TokenType::On]) {
            return Err(Parser::error(self.peek(), "expected ON".into()));
        }

        let table = self.consume(TokenType::Identifier, "expected table name")?.lexeme;
        self.consume(TokenType::LeftParen, "expected (")?;
        let column_name = self.consume(TokenType::Identifier, "expected column name")?.lexeme;
        if self.check(TokenType::Comma) {
            return Err(Parser::error(self.peek(), "a full-text index covers one column; multi-column full-text indexes are not supported".into()));
        }
        self.consume(TokenType::RightParen, "expected )")?;
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::CreateFullTextIndex { index_name, table, column_name })
    }

    // SEARCH table (col) FOR 'query text' [TOP k]   — B8. `SEARCH` is already consumed.
    pub fn parse_search(&mut self) -> Result<Stmt, FerroError> {
        let table = self.consume(TokenType::Identifier, "expected table name")?.lexeme;
        self.consume(TokenType::LeftParen, "expected ( and the name of a full-text indexed column")?;
        let column_name = self.consume(TokenType::Identifier, "expected column name")?.lexeme;
        if self.check(TokenType::Comma) {
            return Err(Parser::error(self.peek(), "SEARCH reads one full-text indexed column".into()));
        }
        self.consume(TokenType::RightParen, "expected )")?;
        if !self.match_token(&[TokenType::For]) {
            return Err(Parser::error(self.peek(), "expected FOR followed by the quoted search text".into()));
        }
        let query = self.consume(TokenType::String, "expected the search text in single quotes")?.lexeme;

        let mut top_k = None;
        if self.match_token(&[TokenType::Top]) {
            let k_token = self.consume(TokenType::Number, "expected a row count after TOP")?;
            let k: usize = k_token.lexeme.parse().map_err(|_| {
                Parser::error(k_token.clone(), format!("TOP takes a whole number of rows, not '{}'", k_token.lexeme))
            })?;
            // Refused here rather than returning nothing: `TOP 0` reads like a query and answers
            // like an empty table, which is the one answer a caller cannot tell from "no matches".
            if k == 0 {
                return Err(Parser::error(k_token, "TOP must be at least 1; a bound of 0 rows would return nothing whatever the data says".into()));
            }
            top_k = Some(k);
        }
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::Search { table, column_name, query, top_k })
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

    // BEGIN AGENT SESSION AS 'agent-id' [RUN 'run-id'] [MODEL 'name/version'] [PROMPT 'text']
    //
    // The clauses keep a fixed order, the same one `SIMULATE` already uses for its `RUN` / `MODEL`
    // prefix. Accepting them in any order here would have made two statements that read alike
    // disagree about what is legal, which is a worse thing to explain than an ordering; what the
    // fixed order costs in diagnostics is paid back by `end_of_agent_session_clauses`, which names
    // the misplaced or repeated word instead of reporting `expected ;` at it.
    pub fn parse_begin_agent_session(&mut self) -> Result<Stmt, FerroError> {
        self.consume(TokenType::Session, "expected SESSION after BEGIN AGENT")?;
        self.consume(TokenType::As, "expected AS after BEGIN AGENT SESSION")?;
        let agent = self.consume(TokenType::String, "expected a quoted agent id")?.lexeme;
        let run = if self.match_token(&[TokenType::Run]) {
            Some(self.consume(TokenType::String, "expected a quoted run id after RUN")?.lexeme)
        } else {
            None
        };
        let model = if self.match_token(&[TokenType::Model]) {
            Some(self.consume(TokenType::String, "expected a quoted model after MODEL")?.lexeme)
        } else {
            None
        };
        let prompt = if self.match_prompt() {
            // Neither trimmed nor refused when empty. The digest IS the run's identity, so two
            // prompts differing only in whitespace are two prompts and normalising them here would
            // silently merge two actors; and `PROMPT ''` must hash the empty string, because the
            // one value it has to stay distinguishable from is the all-zero hash of no clause.
            Some(
                self.consume(TokenType::String, "expected the prompt in single quotes after PROMPT")?
                    .lexeme,
            )
        } else {
            None
        };
        self.end_of_agent_session_clauses()?;
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::BeginAgentSession { agent, run, model, prompt })
    }

    /// `PROMPT` in clause position, matched by **lexeme rather than reserved as a keyword**.
    ///
    /// This is the idiom `ADMIT ALL` already uses and for the same stated reason: `prompt` is a
    /// plausible column name — in a database whose whole subject is agent runs it is close to
    /// inevitable — and reserving a word costs every user of it forever. The scanner is therefore
    /// deliberately unchanged by this clause, and `scanner::tests::prompt_is_not_a_reserved_word`
    /// pins that so a later change cannot reserve it by accident.
    fn peek_prompt(&self) -> bool {
        self.check(TokenType::Identifier) && self.peek().lexeme.eq_ignore_ascii_case("prompt")
    }

    fn match_prompt(&mut self) -> bool {
        if self.peek_prompt() {
            self.advance();
            return true;
        }
        false
    }

    /// Refuse a repeated or out-of-order clause **by name**.
    ///
    /// Without this the fixed clause order reports `expected ;` at the offending word, which tells
    /// a caller that something is wrong and nothing about what. It names the word instead — and
    /// names only the word: `Parser::error` echoes the offending token's lexeme, so the check has
    /// to fire on `PROMPT`, never on the string after it, or a syntax error would quote prompt text
    /// back into a message the whole feature exists to keep it out of.
    fn end_of_agent_session_clauses(&mut self) -> Result<(), FerroError> {
        let word = if self.check(TokenType::Run) {
            "RUN"
        } else if self.check(TokenType::Model) {
            "MODEL"
        } else if self.peek_prompt() {
            "PROMPT"
        } else {
            return Ok(());
        };
        Err(Parser::error(
            self.peek(),
            format!(
                "{word} is repeated or out of order; the clauses are BEGIN AGENT SESSION \
                 AS 'agent' [RUN 'run'] [MODEL 'name/version'] [PROMPT 'text']"
            ),
        ))
    }

    // REVERT MERGE m_44 [CASCADE]
    pub fn parse_revert(&mut self) -> Result<Stmt, FerroError> {
        self.consume(TokenType::Merge, "expected MERGE after REVERT")?;
        let merge_id = self.consume_name("expected a merge id")?;
        let cascade = self.match_token(&[TokenType::Cascade]);
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::RevertMerge { merge_id, cascade })
    }

    // SIMULATE AS 'agent' [RUN 'r'] [MODEL 'm/v']
    //   CANDIDATE 'name' ( <stmt>; ... )   (one or more)
    //   ASSERT ON <table> ( <predicate> )  (one or more)
    //   ADMIT (ALL | <n>)
    //
    // The candidate bodies are ordinary statements, parsed by `parse_statement` rather than by a
    // second grammar: a candidate is a program this database can already run, and giving it its
    // own dialect is how the two drift apart.
    pub fn parse_simulate(&mut self) -> Result<Stmt, FerroError> {
        self.consume(TokenType::As, "expected AS after SIMULATE")?;
        let agent = self.consume(TokenType::String, "expected a quoted agent id")?.lexeme;
        let run = if self.match_token(&[TokenType::Run]) {
            Some(self.consume(TokenType::String, "expected a quoted run id after RUN")?.lexeme)
        } else {
            None
        };
        let model = if self.match_token(&[TokenType::Model]) {
            Some(self.consume(TokenType::String, "expected a quoted model after MODEL")?.lexeme)
        } else {
            None
        };

        let mut candidates: Vec<(String, Vec<Stmt>)> = Vec::new();
        while self.match_token(&[TokenType::Candidate]) {
            let name = self
                .consume(TokenType::String, "expected a quoted candidate name after CANDIDATE")?
                .lexeme;
            self.consume(TokenType::LeftParen, "expected ( to open the candidate body")?;
            let mut body: Vec<Stmt> = Vec::new();
            while !self.check(TokenType::RightParen) {
                if self.is_at_end() {
                    return Err(Parser::error(
                        self.peek(),
                        format!("unterminated body for candidate '{}': expected )", name),
                    ));
                }
                body.push(self.parse_statement()?);
            }
            self.consume(TokenType::RightParen, "expected ) to close the candidate body")?;
            candidates.push((name, body));
        }
        if candidates.is_empty() {
            return Err(Parser::error(
                self.peek(),
                "SIMULATE needs at least one CANDIDATE 'name' ( ... )".into(),
            ));
        }

        let mut assertions: Vec<(String, Expr)> = Vec::new();
        while self.match_token(&[TokenType::Assert]) {
            self.consume(TokenType::On, "expected ON after ASSERT")?;
            let table =
                self.consume(TokenType::Identifier, "expected a table name after ASSERT ON")?.lexeme;
            self.consume(TokenType::LeftParen, "expected ( around the asserted predicate")?;
            let predicate = self.expression()?;
            self.consume(TokenType::RightParen, "expected ) after the asserted predicate")?;
            assertions.push((table, predicate));
        }
        if assertions.is_empty() {
            // Refused in the grammar as well as in the runtime, because the message can be better
            // here: a simulation with nothing declared scores every candidate perfectly against no
            // evidence, and that reads as a result.
            return Err(Parser::error(
                self.peek(),
                "SIMULATE needs at least one ASSERT ON <table> ( <predicate> ): with nothing \
                 declared every candidate scores perfectly against no evidence"
                    .into(),
            ));
        }

        self.consume(
            TokenType::Admit,
            "expected ADMIT ALL or ADMIT <n> after the candidates and assertions",
        )?;
        let admit = if self.check(TokenType::Number) {
            let tok = self.advance();
            let n: usize = tok.lexeme.parse().map_err(|_| {
                Parser::error(tok.clone(), format!("ADMIT needs a whole number, got '{}'", tok.lexeme))
            })?;
            AdmitSpec::AtMost(n)
        } else if self.check(TokenType::Identifier) && self.peek().lexeme.eq_ignore_ascii_case("all")
        {
            self.advance();
            AdmitSpec::All
        } else {
            return Err(Parser::error(self.peek(), "expected ALL or a number after ADMIT".into()));
        };
        self.consume(TokenType::Semicolon, "expected ;")?;
        Ok(Stmt::Simulate { agent, run, model, candidates, assertions, admit })
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
            // Clause keywords, plus everything `unsupported_keyword` knows about. Without the second
            // half, `FROM t ORDER BY v` aliased the table `ORDER` and reported a problem with `BY`.
            let is_clause_word =
                ["JOIN", "WHERE", "ON", "INNER", "LEFT", "RIGHT", "FULL", "OUTER"]
                    .contains(&lexeme_upper.as_str());
            if !is_clause_word && Self::unsupported_keyword(&lexeme_upper).is_none() {
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

    /// The whole statement, parsed once, because every clause of it is load-bearing: the
    /// candidates are programs, the assertions are what they are scored against, and ADMIT is how
    /// much of the result gets published.
    #[test]
    fn test_parse_simulate() {
        let sql = "SIMULATE AS 'pricing-agent' RUN 'r_9' MODEL 'claude-opus-5/2026-05' \
                   CANDIDATE 'cut-5' ( UPDATE inventory SET qty = qty - 5 WHERE id = 1; ) \
                   CANDIDATE 'cut-8' ( UPDATE inventory SET qty = qty - 8 WHERE id = 1; \
                                       INSERT INTO audit VALUES (1, 'cut-8'); ) \
                   ASSERT ON inventory (qty >= 0) \
                   ASSERT ON inventory (qty <= 100) \
                   ADMIT ALL;";
        match one(sql) {
            Stmt::Simulate { agent, run, model, candidates, assertions, admit } => {
                assert_eq!(agent, "pricing-agent");
                assert_eq!(run.as_deref(), Some("r_9"));
                assert_eq!(model.as_deref(), Some("claude-opus-5/2026-05"));
                assert_eq!(candidates.len(), 2);
                assert_eq!(candidates[0].0, "cut-5");
                assert_eq!(candidates[0].1.len(), 1);
                assert_eq!(candidates[1].0, "cut-8");
                assert_eq!(candidates[1].1.len(), 2, "a candidate body is a program, not one statement");
                assert!(matches!(candidates[1].1[1], Stmt::Insert { .. }));
                assert_eq!(assertions.len(), 2);
                assert_eq!(assertions[0].0, "inventory");
                assert_eq!(assertions[0].1.to_sql(), "qty >= 0");
                assert_eq!(admit, AdmitSpec::All);
            }
            other => panic!("expected Simulate, got {:?}", other),
        }
    }

    #[test]
    fn simulate_admits_all_or_a_count() {
        let head = "SIMULATE AS 'a' CANDIDATE 'c' ( DELETE FROM t WHERE id = 1; ) ASSERT ON t (qty >= 0) ";
        for (clause, expect) in [
            ("ADMIT 2;", AdmitSpec::AtMost(2)),
            // A dry run: score everything, publish nothing. Explicit rather than a mode flag.
            ("ADMIT 0;", AdmitSpec::AtMost(0)),
            ("ADMIT all;", AdmitSpec::All),
        ] {
            match one(&format!("{head}{clause}")) {
                Stmt::Simulate { admit, .. } => assert_eq!(admit, expect, "for {clause}"),
                other => panic!("expected Simulate, got {:?}", other),
            }
        }
        let err = parse_sql(&format!("{head}ADMIT sometimes;")).unwrap_err();
        assert!(err.contains("expected ALL or a number"), "got {err}");
        let err = parse_sql(&format!("{head}ADMIT;")).unwrap_err();
        assert!(err.contains("expected ALL or a number"), "got {err}");
    }

    /// **The reason `ALL` is not a reserved word.** Reserving one costs everybody who used it as a
    /// column name, forever, and `ADMIT` is the only position where `ALL` can appear. If someone
    /// later adds it to the scanner's keyword table this test fails and says why.
    #[test]
    fn all_is_still_an_ordinary_identifier_everywhere_else() {
        match one("SELECT all FROM t;") {
            Stmt::Select { columns, .. } => assert_eq!(columns.len(), 1),
            other => panic!("a column named `all` stopped parsing: {:?}", other),
        }
        assert!(parse_sql("CREATE TABLE all (id INTEGER NOT NULL);").is_ok());
    }

    #[test]
    fn simulate_refuses_a_run_with_nothing_to_compare_or_nothing_to_check() {
        // No candidates: nothing to compare.
        let err = parse_sql("SIMULATE AS 'a' ASSERT ON t (qty >= 0) ADMIT ALL;").unwrap_err();
        assert!(err.contains("at least one CANDIDATE"), "got {err}");
        // No assertions: every candidate would score perfectly against no evidence.
        let err =
            parse_sql("SIMULATE AS 'a' CANDIDATE 'c' ( DELETE FROM t WHERE id = 1; ) ADMIT ALL;")
                .unwrap_err();
        assert!(err.contains("at least one ASSERT"), "got {err}");
    }

    /// A body that runs off the end of the token stream is reported against the CANDIDATE.
    ///
    /// Two failures, deliberately kept apart, because the first version of this test conflated
    /// them and could not fail. Asserting only `err.contains("expected")` was worthless: delete
    /// the `is_at_end` guard and `parse_statement` reports "expected a statement" at EOF, which
    /// contains "expected", so the test stayed green with the guard it names in its own
    /// doc-comment gone. Each case now asserts the message only its own path produces.
    #[test]
    fn an_unterminated_candidate_body_names_the_candidate_it_belongs_to() {
        // Runs to EOF inside the body: only the `is_at_end` guard can produce this message.
        let err = parse_sql("SIMULATE AS 'a' CANDIDATE 'c' ( DELETE FROM t WHERE id = 1;")
            .unwrap_err();
        assert!(err.contains("unterminated body for candidate 'c'"), "got {err}");

        // A body containing something that is not a statement stops at that token instead, and
        // says so — the `)` was never reached but the stream did not end either.
        let err = parse_sql(
            "SIMULATE AS 'a' CANDIDATE 'c' ( DELETE FROM t WHERE id = 1; ASSERT ON t (qty >= 0) ADMIT ALL;",
        )
        .unwrap_err();
        assert!(err.contains("expected a statement"), "got {err}");
    }

    #[test]
    fn test_parse_begin_agent_session() {
        match one("BEGIN AGENT SESSION AS 'pricing-agent' RUN 'r_8fk2';") {
            Stmt::BeginAgentSession { agent, run, model, prompt } => {
                assert_eq!(agent, "pricing-agent");
                assert_eq!(run.as_deref(), Some("r_8fk2"));
                assert!(model.is_none());
                assert!(prompt.is_none(), "no PROMPT clause must parse as no prompt");
            }
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
        // RUN is optional
        match one("BEGIN AGENT SESSION AS 'pricing-agent';") {
            Stmt::BeginAgentSession { agent, run, model, prompt } => {
                assert_eq!(agent, "pricing-agent");
                assert!(run.is_none());
                assert!(model.is_none());
                assert!(prompt.is_none());
            }
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
        // MODEL is optional too, and follows RUN
        match one("BEGIN AGENT SESSION AS 'a' RUN 'r' MODEL 'claude-opus-5/2026-05';") {
            Stmt::BeginAgentSession { model, .. } => {
                assert_eq!(model.as_deref(), Some("claude-opus-5/2026-05"));
            }
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
        // and MODEL without RUN
        match one("BEGIN AGENT SESSION AS 'a' MODEL 'gpt-9';") {
            Stmt::BeginAgentSession { run, model, .. } => {
                assert!(run.is_none());
                assert_eq!(model.as_deref(), Some("gpt-9"));
            }
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
        // plain BEGIN still means a transaction
        assert!(matches!(one("BEGIN;"), Stmt::Begin));
    }

    /// **E79b rule: the PROMPT clause is optional, and omitting it is not the empty prompt.**
    ///
    /// The parser can only carry the distinction, not enforce it — `None` versus `Some("")` here is
    /// what `AgentRuntime::begin_session_as` turns into `[0u8; 32]` versus `prompt_digest("")`. If
    /// this collapsed the two (defaulting to `Some(String::new())`, say) the runtime could not tell
    /// "no prompt was declared" from "the prompt was empty" however carefully it hashed.
    #[test]
    fn a_prompt_clause_is_optional_and_absent_is_not_empty() {
        match one("BEGIN AGENT SESSION AS 'a' PROMPT 'restock everything below reorder';") {
            Stmt::BeginAgentSession { agent, run, model, prompt } => {
                assert_eq!(agent, "a");
                assert!(run.is_none());
                assert!(model.is_none());
                assert_eq!(prompt.as_deref(), Some("restock everything below reorder"));
            }
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
        // Absent.
        match one("BEGIN AGENT SESSION AS 'a';") {
            Stmt::BeginAgentSession { prompt, .. } => assert_eq!(prompt, None),
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
        // Present and empty. Not the same value as absent, and not refused: an empty prompt is a
        // prompt, and it is the one input that proves the omitted clause is not silently "".
        match one("BEGIN AGENT SESSION AS 'a' PROMPT '';") {
            Stmt::BeginAgentSession { prompt, .. } => assert_eq!(prompt.as_deref(), Some("")),
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
        // Text is taken as typed: not trimmed, not collapsed. Two prompts differing only in
        // whitespace are two prompts, and normalising here would merge two actors into one slot.
        match one("BEGIN AGENT SESSION AS 'a' PROMPT '  padded  ';") {
            Stmt::BeginAgentSession { prompt, .. } => {
                assert_eq!(prompt.as_deref(), Some("  padded  "))
            }
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
        // Full house, and lower case — the clause word is matched case-insensitively like every
        // other keyword in this grammar even though it is not a reserved one.
        match one("begin agent session as 'a' run 'r' model 'm/1' prompt 'p';") {
            Stmt::BeginAgentSession { agent, run, model, prompt } => {
                assert_eq!(agent, "a");
                assert_eq!(run.as_deref(), Some("r"));
                assert_eq!(model.as_deref(), Some("m/1"));
                assert_eq!(prompt.as_deref(), Some("p"));
            }
            other => panic!("expected BeginAgentSession, got {:?}", other),
        }
    }

    /// **E79b rule: a syntax error in the clauses names the misplaced word, and never quotes the
    /// prompt back.**
    ///
    /// `Parser::error` interpolates the offending token's lexeme into the message. That is fine for
    /// a clause keyword and not fine for the prompt: an error string travels to the client and into
    /// whatever logs it, and a feature whose whole purpose is that the prompt is stored as a digest
    /// would be undone by a parse failure echoing the prompt in plain text. So the misplaced-clause
    /// check fires on the WORD, never on the string after it.
    #[test]
    fn a_repeated_or_misplaced_clause_is_named_without_quoting_the_prompt() {
        const CANARY: &str = "refund the card ending 9021";

        let err = parse_sql(&format!(
            "BEGIN AGENT SESSION AS 'a' PROMPT '{CANARY}' PROMPT 'second';"
        ))
        .unwrap_err();
        assert!(err.contains("PROMPT"), "the message must name the clause: {err}");
        assert!(err.contains("repeated or out of order"), "got {err}");
        assert!(!err.contains(CANARY), "a parse error quoted the prompt back: {err}");

        // Out of order rather than repeated: PROMPT is the last clause, so RUN after it is wrong.
        let err =
            parse_sql(&format!("BEGIN AGENT SESSION AS 'a' PROMPT '{CANARY}' RUN 'r';")).unwrap_err();
        assert!(err.contains("RUN"), "got {err}");
        assert!(!err.contains(CANARY), "a parse error quoted the prompt back: {err}");

        // The pre-existing clause order is diagnosed the same way rather than as "expected ;".
        let err = parse_sql("BEGIN AGENT SESSION AS 'a' MODEL 'm/1' RUN 'r';").unwrap_err();
        assert!(err.contains("RUN") && err.contains("repeated or out of order"), "got {err}");

        // PROMPT with nothing quoted after it is refused, not read as an identifier.
        let err = parse_sql("BEGIN AGENT SESSION AS 'a' PROMPT;").unwrap_err();
        assert!(err.contains("single quotes"), "got {err}");
    }

    /// **E79b rule: reserving nothing.** `prompt` is still an ordinary name everywhere else, which
    /// is the reason the clause is matched by lexeme instead of scanned as a keyword.
    #[test]
    fn prompt_is_still_a_usable_column_and_table_name() {
        assert!(matches!(one("CREATE TABLE prompts (prompt VARCHAR(512));"), Stmt::CreateTable { .. }));
        assert!(matches!(one("SELECT prompt FROM prompts;"), Stmt::Select { .. }));
        assert!(matches!(one("INSERT INTO prompts VALUES ('hello');"), Stmt::Insert { .. }));
        assert!(matches!(one("UPDATE prompts SET prompt = 'x' WHERE prompt = 'y';"), Stmt::Update { .. }));
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

    // ---- B11: column-level DDL ---------------------------------------------------------------

    /// **Breaking shape: `ALTER TABLE ... ADD COLUMN`, which no input could produce before.**
    ///
    /// Without the fix `ALTER` is an ordinary identifier that `unsupported_keyword` refuses in
    /// statement position, so this text produces
    /// `1 at ' ALTER ': ALTER is not supported by this database` and no `Stmt` at all.
    #[test]
    fn alter_table_add_column_parses() {
        let stmts = parse_sql("ALTER TABLE inventory ADD COLUMN note VARCHAR(20);").unwrap();
        match &stmts[0] {
            Stmt::AlterTable { table, action: AlterAction::AddColumn(col) } => {
                assert_eq!(table, "inventory");
                assert_eq!(col.name, "note");
                assert_eq!(col.data_type, DataType::Varchar(20));
                assert!(col.nullable, "a column added with no NULL/NOT NULL suffix is nullable");
            }
            other => panic!("not an ADD COLUMN: {other:?}"),
        }
    }

    /// Breaking shape: `NOT NULL` on the added column. The suffix is parsed by the same helper
    /// `CREATE TABLE` uses, so it must survive the lift-out.
    #[test]
    fn alter_table_add_column_carries_not_null() {
        let stmts = parse_sql("ALTER TABLE t ADD COLUMN c INTEGER NOT NULL;").unwrap();
        match &stmts[0] {
            Stmt::AlterTable { action: AlterAction::AddColumn(col), .. } => {
                assert!(!col.nullable)
            }
            other => panic!("not an ADD COLUMN: {other:?}"),
        }
    }

    /// Breaking shape: `RENAME COLUMN a TO b`, where `TO` is not a token type.
    #[test]
    fn alter_table_rename_column_parses() {
        let stmts = parse_sql("ALTER TABLE inventory RENAME COLUMN qty TO quantity;").unwrap();
        match &stmts[0] {
            Stmt::AlterTable { table, action: AlterAction::RenameColumn { from, to } } => {
                assert_eq!(table, "inventory");
                assert_eq!(from, "qty");
                assert_eq!(to, "quantity");
            }
            other => panic!("not a RENAME COLUMN: {other:?}"),
        }
    }

    /// Breaking shape: `ALTER COLUMN c TYPE t`, whose second `ALTER` is the same token that
    /// started the statement.
    #[test]
    fn alter_table_retype_column_parses() {
        let stmts = parse_sql("ALTER TABLE inventory ALTER COLUMN qty TYPE BIGINT;").unwrap();
        match &stmts[0] {
            Stmt::AlterTable { table, action: AlterAction::RetypeColumn { column, to } } => {
                assert_eq!(table, "inventory");
                assert_eq!(column, "qty");
                assert_eq!(*to, DataType::BigInt);
            }
            other => panic!("not a retype: {other:?}"),
        }
    }

    /// **Anti-vacuity for reserving `ALTER`.** The words the grammar needs — `ADD`, `COLUMN`,
    /// `RENAME`, `TO`, `TYPE` — are matched by lexeme precisely so they stay usable as ordinary
    /// names. Breaking shape: a table whose columns are called `type`, `to` and `add`. Reserve
    /// any of them as a token type and this stops parsing.
    #[test]
    fn the_alter_grammar_words_are_still_usable_as_column_names() {
        let stmts =
            parse_sql("CREATE TABLE t (id INTEGER, type VARCHAR(4), to INTEGER, add INTEGER, column INTEGER, rename INTEGER);")
                .unwrap();
        match &stmts[0] {
            Stmt::CreateTable { columns, .. } => {
                let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
                assert_eq!(names, vec!["id", "type", "to", "add", "column", "rename"]);
            }
            other => panic!("not a CREATE TABLE: {other:?}"),
        }
        // And they still read as names on the way back out.
        parse_sql("SELECT type FROM t WHERE to = 1;").unwrap();
    }

    /// `DROP COLUMN` is valid SQL this database refuses on a data-model rule. Breaking shape:
    /// the refusal falling through to a syntax error that names none of the reason (E67's
    /// complaint about `DROP TABLE` before it was implemented).
    #[test]
    fn drop_column_is_refused_by_name_with_the_reason() {
        let err = parse_sql("ALTER TABLE t DROP COLUMN c;").unwrap_err();
        assert!(err.contains("DROP COLUMN"), "the refusal does not name the feature: {err}");
        assert!(err.contains("ordinal"), "the refusal does not give the reason: {err}");
    }

    /// Same for positional placement: `ADD COLUMN c INTEGER AFTER b` is real SQL elsewhere.
    #[test]
    fn positional_add_column_is_refused_by_name() {
        let err = parse_sql("ALTER TABLE t ADD COLUMN c INTEGER AFTER b;").unwrap_err();
        assert!(err.contains("END of the table"), "no reason given: {err}");
    }

    /// Anti-vacuity for the refusals above: the three supported forms are not refused, and a
    /// genuine typo still reads as a typo rather than as one of the two named refusals.
    #[test]
    fn a_mistyped_alter_still_says_what_was_expected() {
        let err = parse_sql("ALTER TABLE t MODIFY COLUMN c INTEGER;").unwrap_err();
        assert!(
            err.contains("expected ADD COLUMN, RENAME COLUMN or ALTER COLUMN"),
            "unhelpful message: {err}"
        );
    }
}
