//! The extended query protocol: `Parse` / `Bind` / `Describe` / `Execute` / `Close` / `Sync`.
//!
//! ## What the extended protocol is for, and why a driver insists on it
//!
//! The simple query protocol sends a string and gets rows back. It has no parameters, so every
//! value has to be spliced into the SQL text by the client, and no way to ask what a statement
//! will return before running it. asyncpg — like every driver that offers `execute(sql, *args)` —
//! uses the extended protocol for everything, so a server with only `Q` cannot run a single one of
//! its queries.
//!
//! The sequence a driver actually sends, confirmed against asyncpg's own `coreproto.pyx`:
//!
//! ```text
//! prepare:  Parse(name, sql) Describe('S', name) Flush
//!           -> ParseComplete, ParameterDescription, RowDescription|NoData     [no ReadyForQuery]
//! run:      Bind(portal, name, args) Execute(portal, limit) Sync
//!           -> BindComplete, DataRow*, CommandComplete|PortalSuspended, ReadyForQuery
//! ```
//!
//! Two properties of that sequence are load-bearing and easy to get wrong:
//!
//! 1. **`Flush` is not `Sync`.** A `ReadyForQuery` sent in answer to `Flush` leaves asyncpg in a
//!    state where it is not expecting one, and the connection hangs. Only `Sync` ends a sequence.
//! 2. **An error skips the rest of the sequence.** The three messages arrive in one packet, so
//!    when `Bind` fails the `Execute` behind it has already been received. Running it anyway is
//!    how one bad statement produces two errors and one desynchronised connection. Everything
//!    after a failure is discarded until `Sync` — [`Connection::failed`].

use std::collections::HashMap;
use std::sync::Arc;

use crate::binder::binder::{Binder, BoundColumn};
use crate::catalog::catalog::Catalog;
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::execution::executor::{run, Outcome, try_run_read};
use crate::execution::session::Session;
use crate::parser::parser::{Parser, Stmt};
use crate::parser::scanner::{Scanner, TokenType};
use crate::pgwire::message::{Body, Field, Message, TxnStatus};
use crate::pgwire::params;
use crate::pgwire::session::{ProbeValue, SessionCommand, SessionParams};
use crate::pgwire::types::{self, oid, oid_of_type, ParamLiteral};
use crate::pgwire::{sqlstate_of, ServerContext};

/// Everything that belongs to one connection rather than to the server.
pub struct Connection {
    pub session: Session,
    pub session_params: SessionParams,
    /// Prepared statements by name. The unnamed statement `""` lives here too and is simply
    /// replaced on every `Parse`, which is what the protocol says it does.
    statements: HashMap<String, Arc<Statement>>,
    portals: HashMap<String, Portal>,
    /// Set by any failure inside an extended-protocol sequence, cleared by `Sync`. See the module
    /// note: without it, the `Execute` that was pipelined behind a failed `Bind` runs anyway.
    failed: bool,
    /// This connection's cached catalog snapshot, `(epoch, snapshot)`.
    ///
    /// Per-connection rather than shared on purpose: a shared cache would need a lock or a
    /// refcount bump on every statement, and both were measured as the wall this exists to remove
    /// (`bench/d51_sharedword_probe.txt`: RwLock::read x0.121, Arc::clone x0.137, relaxed load
    /// x7.823 over 16 threads).
    catalog_cache: Option<(u64, Arc<Catalog>)>,
    /// This connection's busy slot, written only by this connection. Registered with the
    /// `ServerContext` once, at connection start.
    read_slot: Arc<std::sync::atomic::AtomicBool>,
}

impl Connection {
    /// The busy slot, for `ServerContext::register_reader`. Cloning the `Arc` is what lets the
    /// registry notice, by refcount, that this connection has gone away.
    pub fn read_slot(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.read_slot)
    }

    pub fn new(session: Session) -> Self {
        Connection {
            session,
            session_params: SessionParams::new(),
            statements: HashMap::new(),
            portals: HashMap::new(),
            failed: false,
            catalog_cache: None,
            read_slot: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// `Sync`, or a simple query, ends the error state and starts a new sequence.
    pub fn resync(&mut self) {
        self.failed = false;
        // An unnamed portal does not survive a synchronisation point; a named one does, and is
        // the client's to close.
        self.portals.remove("");
    }

    pub fn fail_until_sync(&mut self) {
        self.failed = true;
    }

    pub fn txn_status(&self) -> TxnStatus {
        // Reported from the ordinary transaction only. An *agent session* is not a transaction —
        // it is a branch that outlives statements and is ended by MERGE or ABANDON, not by
        // COMMIT — so reporting `T` for one would invite a driver to roll it back.
        if self.session.current.is_some() {
            TxnStatus::InTransaction
        } else {
            TxnStatus::Idle
        }
    }
}

/// What one dispatched frontend message produced.
pub struct Output {
    pub messages: Vec<Message>,
    /// Whether the bytes must reach the client now. True for `Sync`, for `Flush`, and for any
    /// error — an error the client never sees is a client that waits forever.
    pub flush: bool,
}

impl Output {
    fn none() -> Self {
        Output { messages: Vec::new(), flush: false }
    }
    fn one(m: Message) -> Self {
        Output { messages: vec![m], flush: false }
    }
    fn err(code: &'static str, message: impl Into<String>) -> Self {
        Output { messages: vec![Message::error(code, message)], flush: true }
    }
}

/// A prepared statement: parsed once, executed many times with different parameters.
pub struct Statement {
    pub sql: String,
    pub kind: Kind,
    /// One OID per `$n`, in order — the promise made in `ParameterDescription`.
    pub param_oids: Vec<i32>,
    /// The columns this statement will return, or `None` for a statement that returns none.
    /// Computed at `Parse` from the catalog, because `Describe` has to answer before any row
    /// exists.
    pub fields: Option<Vec<Field>>,
}

pub enum Kind {
    /// An empty query string. Distinct from a statement returning nothing: the protocol has a
    /// dedicated `EmptyQueryResponse` for it, and clients count on it.
    Empty,
    Sql { stmt: Stmt, verb: &'static str },
    Session(SessionCommand),
}

/// One execution's results, before they are turned into wire messages.
pub struct RunResult {
    pub rows: Vec<Vec<Value>>,
    /// The `CommandComplete` tag, or `None` for an empty query.
    pub tag: Option<String>,
}

struct Portal {
    stmt: Arc<Statement>,
    params: Vec<ParamLiteral>,
    /// The formats the client asked for, one per column, already resolved from the shorthand
    /// forms `Bind` allows.
    fields: Option<Vec<Field>>,
    /// Rows produced by the first `Execute`, not yet sent. A portal is executed **once**, however
    /// many `Execute` messages it takes to drain it — re-running it on the second would repeat
    /// the statement's side effects and skip the rows already sent.
    pending: Option<std::collections::VecDeque<Vec<Value>>>,
    tag: Option<String>,
    done: bool,
}

impl Statement {
    /// Parse one statement for the extended protocol.
    ///
    /// `conn` is here for one reason: [`Connection::catalog_cache`]. Parsing needs the schema —
    /// parameter types and the result description — and reads it from this connection's cached
    /// SNAPSHOT rather than from the exclusive catalog. See [`Statement::parse_one`].
    pub fn parse(
        sql: &str,
        declared: &[i32],
        conn: &mut Connection,
        ctx: &ServerContext,
    ) -> Result<Statement, (&'static str, String)> {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Ok(Statement {
                sql: sql.to_string(),
                kind: Kind::Empty,
                param_oids: Vec::new(),
                fields: None,
            });
        }
        let parts = split_statements(trimmed);
        if parts.len() > 1 {
            // Postgres refuses this too, and for the same reason: one prepared statement has one
            // parameter list and one result description, and two statements have two of each.
            return Err((
                "42601",
                "cannot insert multiple commands into a prepared statement".into(),
            ));
        }
        // Only `catalog_cache` is borrowed, so nothing else on the connection is held across the
        // parse — the same disjoint-fields idiom `execute` uses below.
        let Connection { catalog_cache, .. } = &mut *conn;
        Self::parse_one(
            parts.first().map(|s| s.as_str()).unwrap_or(trimmed),
            declared,
            catalog_cache,
            ctx,
        )
    }

    /// `catalog_cache` is this connection's snapshot slot, threaded in rather than reached through
    /// `&mut Connection`, because that is all of the connection parsing touches.
    fn parse_one(
        sql: &str,
        declared: &[i32],
        catalog_cache: &mut Option<(u64, Arc<Catalog>)>,
        ctx: &ServerContext,
    ) -> Result<Statement, (&'static str, String)> {
        // `begin transaction` / `start transaction` / `end` / `abort` are the same three statements
        // this engine's parser knows as `BEGIN` / `COMMIT` / `ROLLBACK`, under the names drivers
        // actually use. Normalised here rather than in the parser, because which spellings a *wire
        // client* sends is a property of the protocol surface and not of the SQL dialect.
        let sql = match crate::pgwire::session::normalise_txn_control(sql) {
            Some(Ok(canonical)) => canonical,
            Some(Err(e)) => return Err(e),
            None => sql,
        };
        if let Some(cmd) = crate::pgwire::session::parse(sql) {
            let cmd = cmd?;
            if sql.contains('$') {
                return Err((
                    "0A000",
                    "parameters are not supported in SET, SHOW, RESET or a server probe".into(),
                ));
            }
            let (fields, _) = crate::pgwire::session::describe(&cmd);
            return Ok(Statement {
                sql: sql.to_string(),
                kind: Kind::Session(cmd),
                param_oids: Vec::new(),
                fields,
            });
        }

        let (rewritten, nparams) = params::rewrite(sql).map_err(|e| ("42P02", e))?;
        // The ferrodb parser requires a terminating semicolon, and it is missing twice over: a
        // driver never sends one, and `split_statements` above consumes the one a hand-written
        // client did send. So it is supplied here, for every statement rather than only for the
        // driver's. Removing this line fails **every** statement on the server with `expected ;`,
        // which is what a fire-check of it showed: all five wire tests died, including the ones
        // whose SQL ends in a semicolon.
        let with_semi = if rewritten.trim_end().ends_with(';') {
            rewritten
        } else {
            format!("{};", rewritten.trim_end())
        };
        // A `pg_catalog` query fails in the *scanner*, on the dot in `pg_catalog.pg_type`, so the
        // explanation has to be attached here as well as at the binder below. Without it a driver
        // author reads "expected ;" and goes looking for a syntax error in a query that is
        // perfectly good SQL and simply has nowhere to run.
        let hint = crate::pgwire::session::pg_catalog_hint(sql).unwrap_or("");
        let tokens = Scanner::new(with_semi.chars().collect(), Vec::new())
            .scan_tokens()
            .map_err(|e| ("42601", format!("{}{hint}", strip_sentinels(&e.to_string()))))?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err((
                "42601",
                format!(
                    "{}{hint}",
                    strip_sentinels(
                        &parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
                    )
                ),
            ));
        }
        if stmts.is_empty() {
            return Ok(Statement {
                sql: sql.to_string(),
                kind: Kind::Empty,
                param_oids: Vec::new(),
                fields: None,
            });
        }
        if stmts.len() > 1 {
            return Err((
                "42601",
                "cannot insert multiple commands into a prepared statement".into(),
            ));
        }
        let stmt = stmts.remove(0);
        let verb = command_tag_verb(&stmt);
        // **D151 — the shared snapshot, not the exclusive catalog.** Both halves of the answer —
        // what the parameters are and what the columns are — are SCHEMA, and the schema is exactly
        // what `Catalog::epoch` tracks, so a snapshot that is current for the epoch is current for
        // this. `read_catalog` takes the exclusive lock only on first use and after a schema
        // change; `ctx.catalog()` took it for EVERY `Kind::Sql`, including the ones `try_run_read`
        // then ran on the shared path — so a pure reader such as `SELECT c FROM t` announced a
        // writer and drained every other connection's in-flight read, at PARSE time, before it ran
        // on the shared path and took nothing. ⚠ Not `SELECT 1`, which the enclosing `if let` above
        // has already answered as a liveness probe: `bench/d151_e6_before.txt`'s A0 control counts
        // ZERO for it, before this change as much as after. Measured at 1,604 of 6,369
        // announcements (~25%) in the deciding arm — and this is NOT a W4 rescue: it moves a system
        // already pinned against its ceiling at high fork rates, and is worth several times as much
        // one decade of fork rate away.
        let catalog = ctx.read_catalog(catalog_cache);
        let param_oids = params::infer_types(&stmt, nparams, declared, catalog);
        let fields =
            describe_stmt(&stmt, catalog).map_err(|e| (sqlstate_of(&e), format!("{e}{hint}")))?;
        Ok(Statement { sql: sql.to_string(), kind: Kind::Sql { stmt, verb }, param_oids, fields })
    }

    /// Parse a simple-query string, which may carry several statements.
    pub fn parse_batch(
        sql: &str,
        conn: &mut Connection,
        ctx: &ServerContext,
    ) -> Result<Vec<Statement>, FerroError> {
        let mut out = Vec::new();
        // Borrowed once for the whole batch: the cache is refreshed at most once per epoch, so a
        // ten-statement simple query pays the same as a one-statement one.
        let Connection { catalog_cache, .. } = &mut *conn;
        for part in split_statements(sql) {
            match Statement::parse_one(&part, &[], catalog_cache, ctx) {
                Ok(s) => out.push(s),
                // A simple query has no `ParameterDescription` to carry a SQLSTATE, so the code is
                // folded back into the error the caller reports.
                Err((_code, msg)) => return Err(FerroError::SqlParseError(msg)),
            }
        }
        Ok(out)
    }

    pub fn describe_rows(&self) -> Option<Vec<Field>> {
        self.fields.clone()
    }

    /// Run the statement with these parameters.
    pub fn execute(
        &self,
        conn: &mut Connection,
        ctx: &ServerContext,
        args: &[ParamLiteral],
    ) -> Result<RunResult, FerroError> {
        match &self.kind {
            Kind::Empty => Ok(RunResult { rows: Vec::new(), tag: None }),
            Kind::Session(cmd) => run_session(cmd, conn),
            Kind::Sql { stmt, verb } => {
                let stmt = params::substitute(stmt, args).map_err(FerroError::Bind)?;

                // **The shared read path, tried first.** A statement that only reads runs against
                // a per-connection catalog SNAPSHOT and takes no process-wide lock at all, so N
                // readers do not serialise. `try_run_read` answers `None` for anything needing the
                // catalog exclusively, and there is no separate "is this a read?" predicate that
                // could disagree with it. Fields are destructured because `session` and
                // `catalog_cache` are borrowed at once and they are disjoint.
                let outcome = {
                    let Connection { session, catalog_cache, read_slot, .. } = &mut *conn;
                    // Refresh the snapshot FIRST. This is the only place the read path can take
                    // the catalog mutex, and it must not happen while the slot is busy: a writer
                    // draining would wait for this slot while this reader waited for the mutex.
                    let shared = ctx.read_catalog(catalog_cache);
                    // The pass lives only for the attempt. If `try_run_read` answers `None` the
                    // pass is already dropped by the time the exclusive path runs, which is what
                    // keeps that same deadlock out of the write path too.
                    let attempted = {
                        match ctx.begin_read(read_slot) {
                            Some(_pass) => {
                                try_run_read(&stmt, shared, ctx.bp.clone(), ctx.txn.clone(), session)
                            }
                            // A writer is announced. Standing down is correct, not a failure:
                            // the exclusive path below blocks on the mutex and is what every
                            // statement did before this line existed.
                            None => None,
                        }
                    };
                    match attempted {
                        Some(read) => read?,
                        None => {
                            // **The catalog lock, held for exactly one statement.** See
                            // `pgwire::serve` for why this is the outermost lock and why holding
                            // it for longer would rebuild the sequential server this replaced.
                            let mut catalog = ctx.catalog();
                            let o =
                                run(stmt, &mut catalog, ctx.bp.clone(), ctx.txn.clone(), session)?;
                            drop(catalog);
                            o
                        }
                    }
                };
                Ok(match outcome {
                    Outcome::Rows(rows) => {
                        let n = rows.len();
                        RunResult { rows, tag: Some(format!("SELECT {n}")) }
                    }
                    // B9's variant, carrying its own column names and declared types. E78a takes
                    // only the rows, so the merged tree compiles and behaves like `Rows`; making
                    // the wire actually USE `t.columns` — field names and OIDs through B12's
                    // `Field`/`encode_row`, in both the simple and extended paths — is E78b, and is
                    // a rewrite rather than a merge because B9 never faced B12's constraint that
                    // fields are computed at PARSE time by `describe_stmt`.
                    Outcome::Table(t) => {
                        let n = t.rows.len();
                        RunResult { rows: t.rows, tag: Some(format!("SELECT {n}")) }
                    }
                    Outcome::Affected(k) => {
                        RunResult { rows: Vec::new(), tag: Some(format!("{verb} {k}")) }
                    }
                    Outcome::Explain(text) => {
                        let rows: Vec<Vec<Value>> = text
                            .lines()
                            .map(|l| vec![Value::Varchar(l.to_string())])
                            .collect();
                        let n = rows.len();
                        RunResult { rows, tag: Some(format!("SELECT {n}")) }
                    }
                    // Typed columns, from the same `to_rows` whose shape `describe_stmt`
                    // announced. This was `format!("{a:?}")` — a Debug literal in a single text
                    // column, carrying a comment defending it as deliberate. It was deliberate, and
                    // it was the only option while the field list had to be known before the
                    // statement ran; it is not any more.
                    Outcome::Agent(a) => {
                        let t = a.to_rows();
                        let n = t.rows.len();
                        RunResult { rows: t.rows, tag: Some(format!("SELECT {n}")) }
                    },
                    Outcome::Ok => RunResult { rows: Vec::new(), tag: Some(verb.to_string()) },
                })
            }
        }
    }
}

/// Session commands are the one statement kind that changes the *connection* rather than the data,
/// so they run here rather than in the executor.
fn run_session(cmd: &SessionCommand, conn: &mut Connection) -> Result<RunResult, FerroError> {
    let (_, tag) = crate::pgwire::session::describe(cmd);
    let mut rows = Vec::new();
    match cmd {
        SessionCommand::Set { name, value } => {
            conn.session_params
                .set(name, value)
                .map_err(|(_, msg)| FerroError::Bind(msg))?;
        }
        SessionCommand::Reset { name } => match name {
            Some(n) => conn.session_params.reset(n).map_err(|(_, msg)| FerroError::Bind(msg))?,
            None => conn.session_params.reset_all(),
        },
        SessionCommand::Show { name } => {
            let value = conn.session_params.get(name).ok_or_else(|| {
                FerroError::Bind(format!("unrecognized configuration parameter \"{name}\""))
            })?;
            rows.push(vec![Value::Varchar(value.to_string())]);
        }
        SessionCommand::ShowAll => {
            for (name, value) in conn.session_params.all() {
                rows.push(vec![
                    Value::Varchar(name),
                    Value::Varchar(value),
                    // Postgres puts help text here. Inventing sentences that describe *Postgres*
                    // behaviour for a server that does not have it would be worse than empty.
                    Value::Varchar(String::new()),
                ]);
            }
        }
        SessionCommand::CloseAll => conn.portals.clear(),
        SessionCommand::Deallocate { name } => match name {
            Some(n) => {
                conn.statements.remove(n);
            }
            None => conn.statements.clear(),
        },
        SessionCommand::DiscardAll => {
            conn.portals.clear();
            conn.statements.clear();
            conn.session_params.reset_all();
        }
        // Validated at parse time and then nothing to do: `check_txn_modifiers` only accepts
        // options this engine already satisfies.
        SessionCommand::Unlisten | SessionCommand::SetTransaction => {}
        SessionCommand::Probe { value, .. } => {
            let text = match value {
                ProbeValue::Literal(s) => s.clone(),
                ProbeValue::Void => String::new(),
                ProbeValue::Setting(name) => conn
                    .session_params
                    .get(name)
                    .ok_or_else(|| {
                        FerroError::Bind(format!(
                            "unrecognized configuration parameter \"{name}\""
                        ))
                    })?
                    .to_string(),
            };
            rows.push(vec![Value::Varchar(text)]);
        }
    }
    Ok(RunResult { rows, tag: Some(tag) })
}

/// Turn one row into a `DataRow`, under the description already sent for it.
pub fn encode_row(row: &[Value], fields: &[Field]) -> Result<Message, FerroError> {
    if row.len() != fields.len() {
        // Reached only if `describe_stmt` and the executor disagree about a statement's shape.
        // That is a bug in this server, and it must surface here rather than as a client-side
        // "the number of columns in the result row is different from what was described".
        return Err(FerroError::Bind(format!(
            "this server described {} column(s) and produced a row of {}; the description and the \
             executor disagree about this statement",
            fields.len(),
            row.len()
        )));
    }
    let mut cols = Vec::with_capacity(row.len());
    for (v, f) in row.iter().zip(fields) {
        cols.push(
            types::encode_value(v, f.type_oid, f.format)
                .map_err(|e| FerroError::Bind(format!("column `{}`: {e}", f.name)))?,
        );
    }
    Ok(Message::DataRow(cols))
}

/// The columns a statement will return, worked out before it runs.
///
/// This reuses the binder rather than reimplementing projection: `bind_projection` is the same
/// call the executor makes, so `SELECT *` expands to the same columns in the same order, and a
/// name that does not resolve fails here — at `Parse`, which is where a driver expects to be told.
fn describe_stmt(stmt: &Stmt, catalog: &Catalog) -> Result<Option<Vec<Field>>, FerroError> {
    match stmt {
        Stmt::Select { from, columns, joins, .. } => {
            // B9's system views, which are NOT in `Catalog::tables` by design — they are
            // materialised from the agent layer on every read. `scope_for_table` below looks only
            // there, so without this a `SELECT * FROM ferro_branches` was rejected at PARSE time as
            // an unknown table and never reached the executor, where `system_views::intercept` was
            // waiting for it as the very first check. The views worked from the CLI and were
            // invisible over the wire.
            //
            // `describe_select` binds the same scope and the same projection `run_select` does, so
            // what this announces and what the executor sends cannot drift.
            if let Some(view) = crate::catalog::system_views::view_for(catalog, &from.name) {
                let out = crate::catalog::system_views::describe_select(view, stmt, catalog)?;
                return Ok(Some(fields_of(out)));
            }
            if joins.is_empty() {
                // Single table, which covers the plain read and the `AS OF BRANCH` read: the
                // agent runtime builds exactly this scope for its own projection.
                let scope = params::scope_for_table(catalog, &from.name, from.alias.as_deref())?;
                let (_, out) = Binder::new(catalog).bind_projection(columns.clone(), &scope)?;
                Ok(Some(fields_of(out)))
            } else {
                let plan = Binder::new(catalog).bind(stmt.clone())?;
                Ok(Some(fields_of(plan.output_schema())))
            }
        }
        Stmt::Explain(_) => Ok(Some(vec![Field::text("QUERY PLAN", oid::TEXT)])),
        // The agent statements. This announced ONE `text` column called `agent`, and the executor
        // duly sent one column holding `format!("{a:?}")` — a Rust Debug literal on the wire, which
        // a client can only re-parse by hand. B9 built `to_rows` to fix exactly that and could not
        // reach here, because B12 computes fields at PARSE time and B9's shape looked unknowable
        // until the statement ran.
        //
        // It is knowable: a statement kind determines its `AgentOutput` variant, and a variant's
        // columns depend only on the variant. `columns_for_stmt` reads the SAME per-variant lists
        // `to_rows` builds from, so what is announced here and what the executor sends cannot drift
        // — which this server checks, refusing a described/produced contradiction outright.
        Stmt::BeginAgentSession { .. }
        | Stmt::Diff { .. }
        | Stmt::Merge { .. }
        | Stmt::Abandon { .. }
        | Stmt::RevertMerge { .. } => Ok(crate::agent_sql::dispatch::columns_for_stmt(stmt)
            .map(fields_of)),
        _ => Ok(None),
    }
}

fn fields_of(cols: Vec<BoundColumn>) -> Vec<Field> {
    cols.into_iter()
        .map(|c| {
            // `bind_projection` marks a computed column — anything that is not a plain column
            // reference — with an empty qualifier and the name `?column?`, and fills its type in
            // with a placeholder. That placeholder is not an inference, so announcing it would be
            // announcing a type nobody worked out. `text` is announced instead, and every value
            // has a text form, so the promise is one this server can always keep.
            let type_oid = if c.qualifier.is_empty() && c.name == "?column?" {
                oid::TEXT
            } else {
                oid_of_type(&c.data_type)
            };
            Field::text(c.name, type_oid)
        })
        .collect()
}

/// The word a client sees in `CommandComplete`. Clients key off these, so they are the protocol's
/// spelling rather than this codebase's.
fn command_tag_verb(stmt: &Stmt) -> &'static str {
    match stmt {
        Stmt::Select { .. } => "SELECT",
        Stmt::Insert { .. } => "INSERT 0",
        Stmt::Update { .. } => "UPDATE",
        Stmt::Delete { .. } => "DELETE",
        Stmt::CreateTable { .. } => "CREATE TABLE",
        Stmt::DropTable { .. } => "DROP TABLE",
        Stmt::CreateIndex { .. } => "CREATE INDEX",
        Stmt::Begin => "BEGIN",
        Stmt::Commit => "COMMIT",
        Stmt::Rollback => "ROLLBACK",
        Stmt::Analyze { .. } => "ANALYZE",
        _ => "OK",
    }
}

/// A parse error mentioning a sentinel is about a parameter; say `$1` rather than leaking the
/// rewriting this server does internally.
fn strip_sentinels(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    let mut rest = msg;
    while let Some(i) = rest.find("__ferro_param_") {
        out.push_str(&rest[..i]);
        let after = &rest[i + "__ferro_param_".len()..];
        let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        out.push('$');
        out.push_str(&digits);
        rest = &after[digits.len()..];
        rest = rest.strip_prefix("__").unwrap_or(rest);
    }
    out.push_str(rest);
    out
}

/// Split a query string on top-level semicolons, keeping quoted text whole.
///
/// The ferrodb parser handles several statements itself, but a simple query may mix ordinary SQL
/// with the session commands this module owns — asyncpg's connection pool releases a connection by
/// sending four of them in one string — and those never reach that parser.
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_string = false;
    let mut in_ident = false;
    for c in sql.chars() {
        match c {
            '\'' if !in_ident => {
                in_string = !in_string;
                cur.push(c);
            }
            '"' if !in_string => {
                in_ident = !in_ident;
                cur.push(c);
            }
            ';' if !in_string && !in_ident => {
                if !cur.trim().is_empty() {
                    parts.push(cur.trim().to_string());
                }
                cur.clear();
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        parts.push(cur.trim().to_string());
    }
    parts
}

// ---- the message loop ------------------------------------------------------------------------

/// Handle one extended-protocol frontend message.
pub fn dispatch(tag: u8, body: &[u8], conn: &mut Connection, ctx: &ServerContext) -> Output {
    // `Sync` is the synchronisation point and is processed even in the failed state — that is the
    // entire point of it.
    if tag == b'S' {
        conn.resync();
        return Output { messages: vec![Message::ReadyForQuery(conn.txn_status())], flush: true };
    }
    if tag == b'H' {
        return Output { messages: Vec::new(), flush: true };
    }
    if conn.failed {
        // Discarded, deliberately and silently: these messages were already in flight when the
        // failure happened, and answering them would produce a second error for a statement the
        // client has already been told about.
        return Output::none();
    }
    let out = match tag {
        b'P' => parse_message(body, conn, ctx),
        b'B' => bind_message(body, conn),
        b'D' => describe_message(body, conn),
        b'E' => execute_message(body, conn, ctx),
        b'C' => close_message(body, conn),
        other => Output::err("08P01", format!("unexpected message type '{}'", other as char)),
    };
    if out.messages.iter().any(|m| matches!(m, Message::ErrorResponse { .. })) {
        conn.fail_until_sync();
    }
    out
}

fn parse_message(body: &[u8], conn: &mut Connection, ctx: &ServerContext) -> Output {
    let mut b = Body::new(body);
    let (name, sql, declared) = match (|| -> Result<(String, String, Vec<i32>), String> {
        let name = b.cstr()?;
        let sql = b.cstr()?;
        let n = b.i16()?;
        if n < 0 {
            return Err(format!("Parse declares {n} parameter types"));
        }
        let mut declared = Vec::with_capacity(n as usize);
        for _ in 0..n {
            declared.push(b.i32()?);
        }
        Ok((name, sql, declared))
    })() {
        Ok(v) => v,
        Err(e) => return Output::err("08P01", format!("malformed Parse message: {e}")),
    };

    // Postgres refuses to redefine a *named* statement that is still open; the unnamed one is
    // replaced on every Parse, which is what makes it the anonymous statement.
    if !name.is_empty() && conn.statements.contains_key(&name) {
        return Output::err(
            "42P05",
            format!("prepared statement \"{name}\" already exists"),
        );
    }
    match Statement::parse(&sql, &declared, conn, ctx) {
        Ok(stmt) => {
            conn.statements.insert(name, Arc::new(stmt));
            Output::one(Message::ParseComplete)
        }
        Err((code, msg)) => Output::err(code, msg),
    }
}

fn bind_message(body: &[u8], conn: &mut Connection) -> Output {
    let mut b = Body::new(body);
    let portal_name = match b.cstr() {
        Ok(v) => v,
        Err(e) => return Output::err("08P01", format!("malformed Bind message: {e}")),
    };
    let stmt_name = match b.cstr() {
        Ok(v) => v,
        Err(e) => return Output::err("08P01", format!("malformed Bind message: {e}")),
    };
    let Some(stmt) = conn.statements.get(&stmt_name).cloned() else {
        return Output::err(
            "26000",
            format!("prepared statement \"{stmt_name}\" does not exist"),
        );
    };

    let parsed = (|| -> Result<(Vec<i16>, Vec<Option<Vec<u8>>>, Vec<i16>), String> {
        let nfmt = b.i16()?;
        let mut formats = Vec::new();
        for _ in 0..nfmt.max(0) {
            formats.push(b.i16()?);
        }
        let nparams = b.i16()?;
        let mut raw: Vec<Option<Vec<u8>>> = Vec::new();
        for _ in 0..nparams.max(0) {
            let len = b.i32()?;
            if len == -1 {
                raw.push(None);
            } else if len < 0 {
                return Err(format!("parameter length {len} is negative and is not the NULL marker"));
            } else {
                raw.push(Some(b.bytes(len as usize)?.to_vec()));
            }
        }
        let nres = b.i16()?;
        let mut result_formats = Vec::new();
        for _ in 0..nres.max(0) {
            result_formats.push(b.i16()?);
        }
        Ok((formats, raw, result_formats))
    })();
    let (formats, raw, result_formats) = match parsed {
        Ok(v) => v,
        Err(e) => return Output::err("08P01", format!("malformed Bind message: {e}")),
    };

    if raw.len() != stmt.param_oids.len() {
        return Output::err(
            "08P01",
            format!(
                "bind message supplies {} parameters, but prepared statement \"{stmt_name}\" \
                 requires {}",
                raw.len(),
                stmt.param_oids.len()
            ),
        );
    }

    let mut args = Vec::with_capacity(raw.len());
    for (i, value) in raw.iter().enumerate() {
        // The format code list has three legal shapes: none (all text), one (that format for
        // every parameter), or one per parameter. Any other length is a client bug and is a far
        // better error than decoding text as binary.
        let format = match formats.len() {
            0 => 0,
            1 => formats[0],
            n if n == raw.len() => formats[i],
            n => {
                return Output::err(
                    "08P01",
                    format!("bind message has {n} parameter format codes for {} parameters", raw.len()),
                )
            }
        };
        match value {
            None => args.push(ParamLiteral { token: TokenType::Null, text: "NULL".into() }),
            Some(bytes) => match types::decode_param(stmt.param_oids[i], format, bytes) {
                Ok(p) => args.push(p),
                Err(e) => {
                    return Output::err("22P02", format!("parameter ${}: {e}", i + 1));
                }
            },
        }
    }

    let fields = match apply_result_formats(stmt.fields.clone(), &result_formats) {
        Ok(f) => f,
        Err(e) => return Output::err("08P01", e),
    };

    conn.portals.insert(
        portal_name,
        Portal { stmt, params: args, fields, pending: None, tag: None, done: false },
    );
    Output::one(Message::BindComplete)
}

/// Resolve the `Bind` result-format shorthand into one format per column.
fn apply_result_formats(
    fields: Option<Vec<Field>>,
    formats: &[i16],
) -> Result<Option<Vec<Field>>, String> {
    let Some(mut fields) = fields else {
        return Ok(None);
    };
    for (i, f) in fields.iter_mut().enumerate() {
        f.format = match formats.len() {
            0 => 0,
            1 => formats[0],
            n if n == i.max(0) + 1 || n > i => formats[i],
            n => {
                return Err(format!(
                    "bind message has {n} result format codes for {} columns",
                    i + 1
                ))
            }
        };
        if f.format != 0 && f.format != 1 {
            return Err(format!("unknown result format code {} for column `{}`", f.format, f.name));
        }
    }
    if formats.len() > 1 && formats.len() != fields.len() {
        return Err(format!(
            "bind message has {} result format codes for {} columns",
            formats.len(),
            fields.len()
        ));
    }
    Ok(Some(fields))
}

fn describe_message(body: &[u8], conn: &mut Connection) -> Output {
    let mut b = Body::new(body);
    let what = match b.u8() {
        Ok(v) => v,
        Err(e) => return Output::err("08P01", format!("malformed Describe message: {e}")),
    };
    let name = match b.cstr() {
        Ok(v) => v,
        Err(e) => return Output::err("08P01", format!("malformed Describe message: {e}")),
    };
    match what {
        b'S' => {
            let Some(stmt) = conn.statements.get(&name) else {
                return Output::err("26000", format!("prepared statement \"{name}\" does not exist"));
            };
            let mut messages = vec![Message::ParameterDescription(stmt.param_oids.clone())];
            messages.push(match &stmt.fields {
                // Format codes are zero here because no portal exists yet to have chosen any.
                Some(fields) => Message::RowDescription(fields.clone()),
                None => Message::NoData,
            });
            Output { messages, flush: false }
        }
        b'P' => {
            let Some(portal) = conn.portals.get(&name) else {
                return Output::err("34000", format!("portal \"{name}\" does not exist"));
            };
            Output::one(match &portal.fields {
                Some(fields) => Message::RowDescription(fields.clone()),
                None => Message::NoData,
            })
        }
        other => Output::err(
            "08P01",
            format!("Describe must name a statement ('S') or a portal ('P'), not '{}'", other as char),
        ),
    }
}

fn execute_message(body: &[u8], conn: &mut Connection, ctx: &ServerContext) -> Output {
    let mut b = Body::new(body);
    let (name, max_rows) = match (|| -> Result<(String, i32), String> {
        let name = b.cstr()?;
        let max = b.i32()?;
        Ok((name, max))
    })() {
        Ok(v) => v,
        Err(e) => return Output::err("08P01", format!("malformed Execute message: {e}")),
    };
    if max_rows < 0 {
        return Output::err("08P01", format!("Execute row limit {max_rows} is negative"));
    }
    let Some(portal) = conn.portals.get(&name) else {
        return Output::err("34000", format!("portal \"{name}\" does not exist"));
    };
    if portal.done {
        // A drained portal is not an error to execute again in Postgres; it simply produces
        // nothing more. Saying `SELECT 0` would claim a fresh execution that did not happen.
        return Output::one(Message::CommandComplete(
            portal.tag.clone().unwrap_or_else(|| "SELECT 0".into()),
        ));
    }
    let stmt = Arc::clone(&portal.stmt);
    let args = portal.params.clone();
    let fields = portal.fields.clone();
    let already_run = portal.pending.is_some();

    if !already_run {
        let result = match stmt.execute(conn, ctx, &args) {
            Ok(r) => r,
            Err(e) => {
                // The portal is spent either way; leaving it executable would let a retry run the
                // statement a second time.
                if let Some(p) = conn.portals.get_mut(&name) {
                    p.done = true;
                }
                return Output::err(sqlstate_of(&e), e.to_string());
            }
        };
        if result.rows.is_empty() && result.tag.is_none() {
            if let Some(p) = conn.portals.get_mut(&name) {
                p.done = true;
            }
            return Output::one(Message::EmptyQueryResponse);
        }
        if fields.is_none() && !result.rows.is_empty() {
            if let Some(p) = conn.portals.get_mut(&name) {
                p.done = true;
            }
            return Output::err(
                "XX000",
                "this server described the statement as returning no rows and then produced some",
            );
        }
        let Some(p) = conn.portals.get_mut(&name) else {
            return Output::err("34000", format!("portal \"{name}\" vanished while it ran"));
        };
        p.pending = Some(result.rows.into());
        p.tag = result.tag;
    }

    let Some(p) = conn.portals.get_mut(&name) else {
        return Output::err("34000", format!("portal \"{name}\" does not exist"));
    };
    let limit = if max_rows == 0 { usize::MAX } else { max_rows as usize };
    let mut messages = Vec::new();
    let pending = p.pending.as_mut().expect("set immediately above");
    let empty_fields: Vec<Field> = Vec::new();
    let fields_ref = fields.as_ref().unwrap_or(&empty_fields);
    let mut sent = 0usize;
    while sent < limit {
        let Some(row) = pending.pop_front() else { break };
        match encode_row(&row, fields_ref) {
            Ok(m) => messages.push(m),
            Err(e) => {
                p.done = true;
                return Output { messages: vec![Message::error(sqlstate_of(&e), e.to_string())], flush: true };
            }
        }
        sent += 1;
    }
    if pending.is_empty() {
        p.done = true;
        messages.push(Message::CommandComplete(
            p.tag.clone().unwrap_or_else(|| "SELECT 0".into()),
        ));
    } else {
        // The client asked for a window of rows and there are more. It resumes with another
        // Execute on this same portal; the statement is not run again.
        messages.push(Message::PortalSuspended);
    }
    Output { messages, flush: false }
}

fn close_message(body: &[u8], conn: &mut Connection) -> Output {
    let mut b = Body::new(body);
    let what = match b.u8() {
        Ok(v) => v,
        Err(e) => return Output::err("08P01", format!("malformed Close message: {e}")),
    };
    let name = match b.cstr() {
        Ok(v) => v,
        Err(e) => return Output::err("08P01", format!("malformed Close message: {e}")),
    };
    match what {
        // Closing something that is not there is not an error in Postgres, and a driver relies on
        // that: it closes statements it is no longer sure the server still has.
        b'S' => {
            conn.statements.remove(&name);
            Output::one(Message::CloseComplete)
        }
        b'P' => {
            conn.portals.remove(&name);
            Output::one(Message::CloseComplete)
        }
        other => Output::err(
            "08P01",
            format!("Close must name a statement ('S') or a portal ('P'), not '{}'", other as char),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_split_on_top_level_semicolons_only() {
        assert_eq!(split_statements("SELECT 1; SELECT 2;"), vec!["SELECT 1", "SELECT 2"]);
        assert_eq!(
            split_statements("INSERT INTO t VALUES ('a;b');"),
            vec!["INSERT INTO t VALUES ('a;b')"],
            "a semicolon inside a string is data, not a statement boundary"
        );
        assert_eq!(split_statements(";;;").len(), 0);
        // The shape asyncpg's pool sends on every connection release.
        assert_eq!(
            split_statements("SELECT pg_advisory_unlock_all();\nCLOSE ALL;\nUNLISTEN *;\nRESET ALL;")
                .len(),
            4
        );
    }

    #[test]
    fn a_parse_error_names_the_parameter_and_not_the_rewriting() {
        assert_eq!(strip_sentinels("expected ; at __ferro_param_2__ here"), "expected ; at $2 here");
        assert_eq!(strip_sentinels("nothing to strip"), "nothing to strip");
    }

    #[test]
    fn result_format_shorthands_all_resolve_to_one_format_per_column() {
        let fields = Some(vec![Field::text("a", oid::INT4), Field::text("b", oid::TEXT)]);
        // No codes at all: everything text.
        let out = apply_result_formats(fields.clone(), &[]).unwrap().unwrap();
        assert_eq!(out.iter().map(|f| f.format).collect::<Vec<_>>(), vec![0, 0]);
        // One code: it applies to every column. This is the shape asyncpg sends — it writes the
        // single int32 0x00010001, which is "one format code, binary".
        let out = apply_result_formats(fields.clone(), &[1]).unwrap().unwrap();
        assert_eq!(out.iter().map(|f| f.format).collect::<Vec<_>>(), vec![1, 1]);
        // One per column.
        let out = apply_result_formats(fields.clone(), &[0, 1]).unwrap().unwrap();
        assert_eq!(out.iter().map(|f| f.format).collect::<Vec<_>>(), vec![0, 1]);
        // Anything else is a client bug and must not be guessed at.
        assert!(apply_result_formats(fields.clone(), &[0, 1, 0]).is_err());
        assert!(apply_result_formats(fields, &[2]).is_err());
    }
}
