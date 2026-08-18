//! A subset of the PostgreSQL v3 frontend/backend protocol, over `std::net`.
//!
//! Enough of it that an ordinary Postgres client can connect, run `SELECT` and `INSERT`, and
//! disconnect: startup (including the SSL probe every client sends first), the simple query
//! protocol, error responses, and termination. Extended query — `Parse`/`Bind`/`Execute` — is not
//! implemented, and the server says so with a real `ErrorResponse` rather than hanging or
//! returning something a client would misread as success.
//!
//! No dependencies. Framing is four-byte big-endian lengths and one-byte message tags, which is
//! all the protocol actually is; the rest is knowing which messages must appear in which order.
//!
//! ## The one asymmetry worth knowing
//!
//! Every message carries `type_byte + i32 length`, and the length **includes its own four bytes
//! but excludes the type byte**. The startup packet is the exception: it has no type byte at all,
//! because at that point the server does not yet know what protocol it is speaking. Getting this
//! off by one is the classic way a hand-written implementation appears to work and then desyncs
//! a few messages in, so [`Message::encode`] is the only place that computes it.

use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::catalog::system_views::NamedRows;
use crate::catalog::column::DataType;
use crate::execution::executor::{run, Outcome};
use crate::planner::logical_plan::is_computed_column;
use crate::execution::session::Session;
use crate::parser::parser::Parser;
use crate::parser::scanner::Scanner;
use crate::wal::txn::TxnManager;

/// Postgres type OIDs for the value kinds this database has.
mod oid {
    pub const BOOL: i32 = 16;
    pub const INT8: i32 = 20;
    pub const INT4: i32 = 23;
    pub const TEXT: i32 = 25;
    pub const FLOAT8: i32 = 701;
    pub const NUMERIC: i32 = 1700;
}

/// `80877103` — the magic version a client sends to ask for TLS before anything else.
const SSL_REQUEST_CODE: i32 = 80_877_103;
/// `196608` — protocol 3.0, the version every modern client speaks.
const PROTOCOL_V3: i32 = 196_608;

/// A backend message, ready to be framed.
enum Message {
    AuthenticationOk,
    ParameterStatus(&'static str, String),
    BackendKeyData { pid: i32, key: i32 },
    ReadyForQuery,
    RowDescription(Vec<(String, i32)>),
    DataRow(Vec<Option<String>>),
    CommandComplete(String),
    EmptyQueryResponse,
    ErrorResponse { severity: &'static str, code: &'static str, message: String },
}

impl Message {
    fn tag(&self) -> u8 {
        match self {
            Message::AuthenticationOk => b'R',
            Message::ParameterStatus(..) => b'S',
            Message::BackendKeyData { .. } => b'K',
            Message::ReadyForQuery => b'Z',
            Message::RowDescription(_) => b'T',
            Message::DataRow(_) => b'D',
            Message::CommandComplete(_) => b'C',
            Message::EmptyQueryResponse => b'I',
            Message::ErrorResponse { .. } => b'E',
        }
    }

    fn body(&self) -> Vec<u8> {
        let mut b = Vec::new();
        match self {
            Message::AuthenticationOk => b.extend_from_slice(&0i32.to_be_bytes()),
            Message::ParameterStatus(k, v) => {
                push_cstr(&mut b, k);
                push_cstr(&mut b, v);
            }
            Message::BackendKeyData { pid, key } => {
                b.extend_from_slice(&pid.to_be_bytes());
                b.extend_from_slice(&key.to_be_bytes());
            }
            // 'I' = idle, not in a transaction. This server runs each simple query on its own.
            Message::ReadyForQuery => b.push(b'I'),
            Message::RowDescription(fields) => {
                b.extend_from_slice(&(fields.len() as i16).to_be_bytes());
                for (name, type_oid) in fields {
                    push_cstr(&mut b, name);
                    b.extend_from_slice(&0i32.to_be_bytes()); // table oid: unknown
                    b.extend_from_slice(&0i16.to_be_bytes()); // column attr: unknown
                    b.extend_from_slice(&type_oid.to_be_bytes());
                    b.extend_from_slice(&(-1i16).to_be_bytes()); // type size: variable
                    b.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier: none
                    b.extend_from_slice(&0i16.to_be_bytes()); // format: text
                }
            }
            Message::DataRow(cols) => {
                b.extend_from_slice(&(cols.len() as i16).to_be_bytes());
                for c in cols {
                    match c {
                        // -1 length is SQL NULL, and is distinct from a zero-length string.
                        None => b.extend_from_slice(&(-1i32).to_be_bytes()),
                        Some(s) => {
                            b.extend_from_slice(&(s.len() as i32).to_be_bytes());
                            b.extend_from_slice(s.as_bytes());
                        }
                    }
                }
            }
            Message::CommandComplete(tag) => push_cstr(&mut b, tag),
            Message::EmptyQueryResponse => {}
            Message::ErrorResponse { severity, code, message } => {
                b.push(b'S');
                push_cstr(&mut b, severity);
                b.push(b'C');
                push_cstr(&mut b, code);
                b.push(b'M');
                push_cstr(&mut b, message);
                b.push(0); // terminator for the field list
            }
        }
        b
    }

    /// Frame the message. The length covers itself and the body, but **not** the type byte.
    fn encode(&self) -> Vec<u8> {
        let body = self.body();
        let mut out = Vec::with_capacity(body.len() + 5);
        out.push(self.tag());
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(&body);
        out
    }
}

fn push_cstr(b: &mut Vec<u8>, s: &str) {
    b.extend_from_slice(s.as_bytes());
    b.push(0);
}

fn read_exact(r: &mut impl Read, n: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_i32(r: &mut impl Read) -> std::io::Result<i32> {
    let b = read_exact(r, 4)?;
    Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

fn oid_of(v: &Value) -> i32 {
    match v {
        Value::Integer(_) => oid::INT4,
        Value::Float(_) => oid::FLOAT8,
        Value::Boolean(_) => oid::BOOL,
        Value::BigInt(_) => oid::INT8,
        // `numeric`'s text format is exactly the digit string this type already holds, so a
        // conforming client reads it back without loss.
        Value::Decimal(_) => oid::NUMERIC,
        // Deliberately NOT `timestamp` (1114). This engine stores epoch milliseconds and has no
        // calendar formatter, so it would send `1700000000000` where a conforming client expects
        // `2023-11-14 22:13:20`. Announcing `int8` and sending an integer is true; announcing
        // `timestamp` and sending an integer is a parse error at every real driver.
        Value::Timestamp(_) => oid::INT8,
        Value::Varchar(_) | Value::Null => oid::TEXT,
    }
}

/// The OID for a column's **declared** type, as opposed to [`oid_of`]'s answer for one observed
/// value.
///
/// The two exist together rather than one calling the other because they answer different questions.
/// `oid_of` looks at a value and can only be asked about a column that has one; this looks at a
/// schema and answers for an empty result too, which is the entire reason an empty system view is
/// distinguishable from a broken one — the field list is still fully typed with no rows to infer
/// from.
///
/// `Timestamp` maps to `int8` here for the same reason it does in `oid_of`, and the mapping is
/// duplicated rather than shared: they are two tables over two different domains that happen to
/// agree, and collapsing them would make the next divergence silent.
fn oid_of_type(t: &DataType) -> i32 {
    match t {
        DataType::Integer => oid::INT4,
        DataType::Float => oid::FLOAT8,
        DataType::Boolean => oid::BOOL,
        DataType::BigInt => oid::INT8,
        DataType::Decimal => oid::NUMERIC,
        // Epoch milliseconds with no calendar formatter, so `int8` is the true answer. See `oid_of`.
        DataType::Timestamp => oid::INT8,
        DataType::Varchar(_) => oid::TEXT,
    }
}

/// The `RowDescription` fields for a result that knows its own schema.
///
/// A declared type is used as declared, with one exception that is a correctness fix rather than a
/// nicety: `LogicalPlan`'s output schema carries a **placeholder** type for a projected expression
/// that is not a bare column reference (`SELECT qty * 2` — see `logical_plan::is_computed_column`),
/// and that placeholder is a hardcoded `Integer`. Announcing `int4` and then sending `3.5` is a parse
/// error at every conforming driver, so for those columns the OID comes from the value the query
/// actually produced, and falls back to `text` when there are no rows to look at. `text` is the one
/// type every client will accept the bytes of.
fn fields_of(rows: &NamedRows) -> Vec<(String, i32)> {
    rows.columns
        .iter()
        .enumerate()
        .map(|(at, col)| {
            let oid = if is_computed_column(col) {
                rows.rows
                    .iter()
                    .find_map(|r| match r.get(at) {
                        Some(Value::Null) | None => None,
                        Some(v) => Some(oid_of(v)),
                    })
                    .unwrap_or(oid::TEXT)
            } else {
                oid_of_type(&col.data_type)
            };
            (col.name.clone(), oid)
        })
        .collect()
}

/// Emit a schema-carrying result: `RowDescription`, one `DataRow` per row, then `CommandComplete`.
///
/// **Refuses a row whose width disagrees with the announced field list.** `system_views::run_select`
/// already refuses that on its own path; `AgentOutput::to_rows` had no equivalent, and this is the
/// one place both converge, so the guard belongs here rather than duplicated in each producer.
///
/// The reason it is a refusal and not a debug assertion: `DataRow` carries its own field count, so a
/// row narrower than the `RowDescription` is a **well-formed protocol message**. A client reads it
/// as valid data with every value after the missing one under the wrong column name, and nothing —
/// not the client, not the wire, not a test that only looks at values — can tell. An error the caller
/// sees beats data the caller trusts.
fn typed_result(t: &NamedRows, out: &mut Vec<Message>) -> Result<(), FerroError> {
    let fields = fields_of(t);
    for (i, r) in t.rows.iter().enumerate() {
        if r.len() != fields.len() {
            return Err(FerroError::Internal(format!(
                "row {i} has {} values against {} announced columns ({}); sending it would put every \
                 value after the {}th under the wrong column name",
                r.len(),
                fields.len(),
                fields.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>().join(", "),
                r.len()
            )));
        }
    }
    out.push(Message::RowDescription(fields));
    let count = t.rows.len();
    for r in &t.rows {
        out.push(Message::DataRow(r.iter().map(render).collect()));
    }
    out.push(Message::CommandComplete(format!("SELECT {count}")));
    Ok(())
}

/// Text-format rendering. `None` is SQL NULL, which the protocol encodes as length -1 rather than
/// as the string "NULL" — a client cannot tell those apart otherwise.
fn render(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Integer(i) => Some(i.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Boolean(b) => Some(if *b { "t".into() } else { "f".into() }),
        Value::BigInt(i) => Some(i.to_string()),
        Value::Decimal(d) => Some(d.clone()),
        Value::Timestamp(ms) => Some(ms.to_string()),
        Value::Varchar(s) => Some(s.clone()),
    }
}

/// Everything one connection needs to answer queries.
pub struct ServerContext {
    pub catalog: Catalog,
    pub bp: Arc<BufferPoolManager>,
    pub txn: Arc<TxnManager>,
    /// **Shared by every connection, deliberately.** A runtime per connection would give each
    /// client its own branch namespace: two clients could not see each other's branches, `AS OF
    /// BRANCH` across connections would resolve nothing, and a merge would land somewhere the other
    /// client never sees. Branches are a property of the database, not of the socket.
    ///
    /// Until this field existed the server built `Session::new()` — `storage: None` — so an agent
    /// session over the wire staged its writes in a `BTreeMap` while the copy-on-write engine sat
    /// beside it, which is the same gap E31 closed for the CLI and E35 for the demo.
    pub runtime: Arc<crate::agent_sql::runtime::AgentRuntime>,
}

/// Serve connections until `listener` stops yielding them.
///
/// Single-threaded and sequential on purpose: this exists to show the protocol is real, and a
/// thread pool would add a concurrency story this row is not making a claim about.
pub fn serve(listener: TcpListener, ctx: &mut ServerContext) -> std::io::Result<()> {
    for stream in listener.incoming() {
        let stream = stream?;
        if let Err(e) = handle(stream, ctx) {
            eprintln!("pgwire: connection ended: {e}");
        }
    }
    Ok(())
}

/// Handle one connection start to finish.
pub fn handle(mut stream: TcpStream, ctx: &mut ServerContext) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);

    // --- startup -----------------------------------------------------------------------------
    // Clients open with an SSL probe. Refusing it with a bare 'N' is a legal answer and the
    // client then re-sends a real startup packet, so the loop runs at most twice.
    let mut params_seen = false;
    for _ in 0..2 {
        let len = read_i32(&mut reader)?;
        if len < 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("startup packet claims {len} bytes, which cannot hold its own header"),
            ));
        }
        let body = read_exact(&mut reader, (len - 4) as usize)?;
        let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        if code == SSL_REQUEST_CODE {
            stream.write_all(b"N")?;
            stream.flush()?;
            continue;
        }
        if code != PROTOCOL_V3 {
            let msg = Message::ErrorResponse {
                severity: "FATAL",
                code: "0A000",
                message: format!(
                    "unsupported protocol version {}.{}; this server speaks 3.0 only",
                    code >> 16,
                    code & 0xffff
                ),
            };
            stream.write_all(&msg.encode())?;
            stream.flush()?;
            return Ok(());
        }
        params_seen = true;
        break;
    }
    if !params_seen {
        return Ok(());
    }

    // No authentication: this is a local demonstration server, and pretending otherwise by
    // sending AuthenticationCleartextPassword and then accepting anything would be worse.
    for m in [
        Message::AuthenticationOk,
        Message::ParameterStatus("server_version", "3.0 (ferrodb)".into()),
        Message::ParameterStatus("client_encoding", "UTF8".into()),
        Message::ParameterStatus("DateStyle", "ISO, MDY".into()),
        Message::BackendKeyData { pid: std::process::id() as i32, key: 0 },
        Message::ReadyForQuery,
    ] {
        stream.write_all(&m.encode())?;
    }
    stream.flush()?;

    // --- message loop ------------------------------------------------------------------------
    let mut session = Session::with_runtime(Arc::clone(&ctx.runtime));
    loop {
        let mut tag = [0u8; 1];
        if reader.read_exact(&mut tag).is_err() {
            return Ok(()); // client vanished; nothing to say to a closed socket
        }
        let len = read_i32(&mut reader)?;
        let body = read_exact(&mut reader, (len as usize).saturating_sub(4))?;

        match tag[0] {
            b'Q' => {
                let sql = String::from_utf8_lossy(&body)
                    .trim_end_matches('\0')
                    .trim()
                    .to_string();
                for m in answer(&sql, &mut session, ctx) {
                    stream.write_all(&m.encode())?;
                }
                stream.write_all(&Message::ReadyForQuery.encode())?;
                stream.flush()?;
            }
            b'X' => return Ok(()),
            other => {
                // Extended query and everything else. Saying so beats silence: a client that gets
                // no reply hangs, and one that gets a bare ReadyForQuery concludes it succeeded.
                let m = Message::ErrorResponse {
                    severity: "ERROR",
                    code: "0A000",
                    message: format!(
                        "message type '{}' is not implemented; this server supports the simple \
                         query protocol only",
                        other as char
                    ),
                };
                stream.write_all(&m.encode())?;
                stream.write_all(&Message::ReadyForQuery.encode())?;
                stream.flush()?;
            }
        }
    }
}

/// Run one SQL string and turn the result into backend messages.
fn answer(sql: &str, session: &mut Session, ctx: &mut ServerContext) -> Vec<Message> {
    if sql.is_empty() {
        return vec![Message::EmptyQueryResponse];
    }
    match execute(sql, session, ctx) {
        Ok(msgs) => msgs,
        Err(e) => vec![Message::ErrorResponse {
            severity: "ERROR",
            code: "42000",
            message: e.to_string(),
        }],
    }
}

fn execute(
    sql: &str,
    session: &mut Session,
    ctx: &mut ServerContext,
) -> Result<Vec<Message>, FerroError> {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    if !parser.errors.is_empty() {
        return Err(FerroError::SqlParseError(
            parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
        ));
    }
    if stmts.is_empty() {
        return Ok(vec![Message::EmptyQueryResponse]);
    }

    let mut out = Vec::new();
    let n = stmts.len();
    for (i, stmt) in stmts.drain(..).enumerate() {
        let verb = command_tag_verb(&stmt);
        let outcome = run(stmt, &mut ctx.catalog, ctx.bp.clone(), ctx.txn.clone(), session)?;
        match outcome {
            Outcome::Rows(rows) => {
                // Column names are not carried through the executor, so they are positional here.
                // Naming them `column1..N` is honest; inventing plausible names would not be.
                let width = rows.first().map(|r| r.len()).unwrap_or(0);
                let fields: Vec<(String, i32)> = (0..width)
                    .map(|c| {
                        let oid = rows
                            .iter()
                            .find_map(|r| match r.get(c) {
                                Some(Value::Null) | None => None,
                                Some(v) => Some(oid_of(v)),
                            })
                            .unwrap_or(oid::TEXT);
                        (format!("column{}", c + 1), oid)
                    })
                    .collect();
                out.push(Message::RowDescription(fields));
                let count = rows.len();
                for r in rows {
                    out.push(Message::DataRow(r.iter().map(render).collect()));
                }
                out.push(Message::CommandComplete(format!("SELECT {count}")));
            }
            Outcome::Affected(k) => {
                out.push(Message::CommandComplete(format!("{verb} {k}")));
            }
            Outcome::Explain(text) => {
                out.push(Message::RowDescription(vec![("QUERY PLAN".into(), oid::TEXT)]));
                for line in text.lines() {
                    out.push(Message::DataRow(vec![Some(line.to_string())]));
                }
                out.push(Message::CommandComplete(format!("SELECT {}", text.lines().count())));
            }
            // A system view, or anything else that carries its own schema: real column names, and
            // an OID per column even when there are no rows.
            Outcome::Table(t) => typed_result(&t, &mut out)?,
            // **This used to be one `text` column holding `format!("{a:?}")`.**
            //
            // A client could read it and could do nothing with it: the `Debug` rendering of a
            // `MergeReport` is a Rust literal, so a driver got one opaque string where the design
            // promises structured data an agent can act on (DESIGN.md exit criteria 4 and 5 —
            // "structured throughout ... never rendered text"). `AgentOutput::to_rows` is the typed
            // form beside `Display`; `Display` stays because the CLI wants a sentence and a wire
            // client wants columns.
            Outcome::Agent(a) => typed_result(&a.to_rows(), &mut out)?,
            Outcome::Ok => out.push(Message::CommandComplete(verb.to_string())),
        }
        let _ = (i, n);
    }
    Ok(out)
}

/// The word a client sees in `CommandComplete`. Clients key off these, so they are the protocol's
/// spelling rather than this codebase's.
fn command_tag_verb(stmt: &crate::parser::parser::Stmt) -> &'static str {
    use crate::parser::parser::Stmt;
    match stmt {
        Stmt::Select { .. } => "SELECT",
        Stmt::Insert { .. } => "INSERT 0",
        Stmt::Update { .. } => "UPDATE",
        Stmt::Delete { .. } => "DELETE",
        Stmt::CreateTable { .. } => "CREATE TABLE",
        _ => "OK",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_length_covers_itself_and_the_body_but_not_the_tag() {
        // The off-by-one that makes a hand-written implementation desync a few messages in.
        let m = Message::CommandComplete("SELECT 1".into());
        let bytes = m.encode();
        assert_eq!(bytes[0], b'C');
        let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        assert_eq!(len as usize, bytes.len() - 1, "length must exclude the type byte");
        assert_eq!(len as usize, m.body().len() + 4, "length must include its own 4 bytes");
    }

    #[test]
    fn a_null_column_is_length_minus_one_not_the_text_null() {
        let bytes = Message::DataRow(vec![None, Some("hi".into())]).encode();
        // tag(1) + len(4) + ncols(2) then the first column's length.
        let first = i32::from_be_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);
        assert_eq!(first, -1, "SQL NULL must be -1, or a client cannot distinguish it from a string");
    }

    #[test]
    fn an_empty_string_is_length_zero_and_not_null() {
        let bytes = Message::DataRow(vec![Some(String::new())]).encode();
        let first = i32::from_be_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);
        assert_eq!(first, 0, "an empty string must be length 0, distinct from NULL's -1");
    }

    #[test]
    fn error_response_fields_are_terminated_and_typed() {
        let bytes = Message::ErrorResponse {
            severity: "ERROR",
            code: "42000",
            message: "boom".into(),
        }
        .encode();
        assert_eq!(bytes[0], b'E');
        assert_eq!(*bytes.last().unwrap(), 0, "the field list needs its terminating zero byte");
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("ERROR") && text.contains("42000") && text.contains("boom"));
    }

    #[test]
    fn row_description_advertises_one_field_per_column() {
        let bytes = Message::RowDescription(vec![
            ("a".into(), oid::INT4),
            ("b".into(), oid::TEXT),
        ])
        .encode();
        let n = i16::from_be_bytes([bytes[5], bytes[6]]);
        assert_eq!(n, 2);
    }

    fn cols(names: &[(&str, DataType)]) -> Vec<crate::binder::binder::BoundColumn> {
        names
            .iter()
            .map(|(n, t)| crate::binder::binder::BoundColumn {
                qualifier: "v".into(),
                name: (*n).into(),
                data_type: t.clone(),
                nullable: false,
            })
            .collect()
    }

    /// **A row narrower than the announced field list is refused, not sent.**
    ///
    /// The breaking shape is a producer whose column list and rows are built in two places and drift —
    /// which is every `AgentOutput::to_rows` arm and every system view. This cannot be caught
    /// downstream: `DataRow` carries its own field count, so a short row is a **well-formed protocol
    /// message**. A client reads it as valid data with every value after the gap sitting under the
    /// wrong column name, and neither the wire, nor the client, nor a test that only compares values
    /// can tell. So the refusal has to happen here, at the one point both producers converge.
    #[test]
    fn a_row_narrower_than_its_row_description_is_refused_rather_than_sent() {
        let columns = cols(&[("a", DataType::Integer), ("b", DataType::Varchar(8))]);

        // Anti-vacuity first: the well-formed case must go out, and go out complete.
        let good = NamedRows::new(
            columns.clone(),
            vec![vec![Value::Integer(1), Value::Varchar("x".into())]],
        );
        let mut out = Vec::new();
        typed_result(&good, &mut out).expect("a well-formed result must be sent");
        assert!(matches!(out[0], Message::RowDescription(ref f) if f.len() == 2), "{:?}", out.len());
        assert!(matches!(out[1], Message::DataRow(ref r) if r.len() == 2));
        assert!(matches!(out[2], Message::CommandComplete(ref t) if t == "SELECT 1"));

        // One value short: refused, and nothing is emitted for it.
        let ragged = NamedRows::new(columns.clone(), vec![vec![Value::Integer(1)]]);
        let mut out = Vec::new();
        let err = typed_result(&ragged, &mut out).expect_err("a ragged row must be refused");
        assert!(out.is_empty(), "a refused result still emitted {} messages", out.len());
        let msg = err.to_string();
        assert!(msg.contains("1 values against 2"), "the refusal does not say what mismatched: {msg}");
        assert!(msg.contains("wrong column name"), "the refusal does not say why it matters: {msg}");

        // Wider than the schema is equally wrong, and in the other direction: the extra value would
        // be dropped by a client that trusts the field count.
        let wide = NamedRows::new(
            columns,
            vec![vec![Value::Integer(1), Value::Varchar("x".into()), Value::Integer(9)]],
        );
        let mut out = Vec::new();
        assert!(typed_result(&wide, &mut out).is_err(), "an over-wide row was sent");

        // And a result with columns but no rows is fine — that is an empty view, not a broken one.
        let empty = NamedRows::new(cols(&[("only", DataType::BigInt)]), Vec::new());
        let mut out = Vec::new();
        typed_result(&empty, &mut out).expect("an empty result is not an error");
        assert!(matches!(out[0], Message::RowDescription(ref f) if f.len() == 1));
        assert!(matches!(out[1], Message::CommandComplete(ref t) if t == "SELECT 0"));
    }

    #[test]
    fn values_map_to_the_postgres_type_oids_a_client_expects() {
        assert_eq!(oid_of(&Value::Integer(1)), 23);
        assert_eq!(oid_of(&Value::Float(1.0)), 701);
        assert_eq!(oid_of(&Value::Boolean(true)), 16);
        assert_eq!(oid_of(&Value::Varchar("x".into())), 25);
    }

    #[test]
    fn booleans_render_as_t_and_f_the_way_postgres_text_format_does() {
        assert_eq!(render(&Value::Boolean(true)).as_deref(), Some("t"));
        assert_eq!(render(&Value::Boolean(false)).as_deref(), Some("f"));
        assert_eq!(render(&Value::Null), None);
    }
}
