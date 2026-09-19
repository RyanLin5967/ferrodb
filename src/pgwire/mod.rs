//! A subset of the PostgreSQL v3 frontend/backend protocol, over `std::net`.
//!
//! Enough of it that an ordinary Postgres client — a real driver, not only a client written
//! against this server — can connect, prepare and run parameterised statements, and disconnect:
//! startup (including the SSL probe every client sends first), the simple query protocol, the
//! extended query protocol, session parameters, error responses, and termination.
//!
//! No dependencies. Framing is four-byte big-endian lengths and one-byte message tags, which is
//! all the protocol actually is; the rest is knowing which messages must appear in which order.
//! [`message`] owns the framing, [`types`] owns the two value encodings, [`params`] owns
//! parameters, [`extended`] owns the Parse/Bind/Describe/Execute state machine and [`session`]
//! owns `SET`/`SHOW` and the handful of probes a driver makes on its own behalf.
//!
//! ## Concurrency
//!
//! Each connection gets a thread, and the catalog sits behind a mutex inside [`ServerContext`].
//! See [`serve`] for the lock ordering, which is the part that has to be got right.

pub mod extended;
pub mod message;
pub mod params;
pub mod session;
pub mod types;

use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::error::FerroError;
use crate::execution::session::Session;
use crate::wal::txn::TxnManager;

use self::extended::{Connection, Statement};
use self::message::{read_exact, read_i32, Message, TxnStatus};

/// `80877103` — the magic version a client sends to ask for TLS before anything else.
const SSL_REQUEST_CODE: i32 = 80_877_103;
/// `80877102` — a cancel request, which arrives on its own connection and carries no startup.
const CANCEL_REQUEST_CODE: i32 = 80_877_102;
/// `196608` — protocol 3.0, the version every modern client speaks.
const PROTOCOL_V3: i32 = 196_608;

/// What this server reports as `server_version`.
///
/// **It is a compatibility claim, and clients act on it.** asyncpg turns this string into a
/// version tuple and switches features on from it: at 11 or above it starts issuing `SET jit`
/// probes, at 12 it starts using `COPY ... WHERE`. This server has neither, so claiming a modern
/// major version would invite a client to use a surface that is not here. 9.6 is the oldest
/// release every current driver still supports, which makes it the honest floor: everything a
/// client will attempt against 9.6 is inside what is implemented.
pub const SERVER_VERSION: &str = "9.6.0 (ferrodb 0.1.0)";

/// Everything one connection needs to answer queries.
///
/// Shared by every connection through an `Arc`, so it holds no per-connection state — that lives
/// in [`Connection`].
pub struct ServerContext {
    /// **Behind a mutex, because connections are concurrent.**
    ///
    /// `executor::run` takes `&mut Catalog`, and a `MutexGuard` derefs to exactly that, so the
    /// executor's signature is unchanged by this. See [`serve`] for the lock ordering rule.
    pub catalog: Mutex<Catalog>,
    /// A lock-free mirror of `catalog.epoch()`, for readers.
    ///
    /// **One relaxed-order load is the entire per-statement cost of the shared read path.** That
    /// is not a guess: `bench/d51_sharedword_probe.txt` measured a shared relaxed load scaling to
    /// x7.823 over 16 threads while `RwLock::read` reached x0.121 and `Arc::clone` x0.137 — 490x
    /// apart in absolute terms. It is also why this is an epoch mirror and not an `RwLock<Catalog>`:
    /// D49 measured turso collapsing to x0.308 on exactly that shape, in a real engine.
    epoch: std::sync::atomic::AtomicU64,
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

impl ServerContext {
    pub fn new(
        catalog: Catalog,
        bp: Arc<BufferPoolManager>,
        txn: Arc<TxnManager>,
        runtime: Arc<crate::agent_sql::runtime::AgentRuntime>,
    ) -> Self {
        let e = catalog.epoch();
        ServerContext {
            catalog: Mutex::new(catalog),
            epoch: std::sync::atomic::AtomicU64::new(e),
            bp,
            txn,
            runtime,
        }
    }

    /// The catalog, for the duration of one statement.
    ///
    /// Poisoning is not propagated as a panic: a connection thread that panicked while holding
    /// this lock would otherwise take every *other* connection down with it on their next
    /// statement. The data behind the lock is the on-disk catalog, which is reloadable, so the
    /// surviving connections are better served by continuing.
    pub fn catalog(&self) -> CatalogGuard<'_> {
        let inner = match self.catalog.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        CatalogGuard { inner, epoch: &self.epoch }
    }

    /// The catalog for a READ statement, as a per-connection cached snapshot.
    ///
    /// The common path is **one relaxed load and a comparison** — no lock, and no `Arc::clone`,
    /// which is why this returns a borrow out of the caller's cache rather than an `Arc`: cloning
    /// an `Arc` per statement is an atomic RMW on one refcount, measured at x0.137 over 16 threads
    /// (`bench/d51_sharedword_probe.txt`), which would have reintroduced the wall one level down.
    ///
    /// A snapshot may be stale about a tree's recorded `primary_index_root` and that is safe:
    /// since D53 the root cell is SHARED, so `plan::open_table` descends from the live cell. What
    /// a stale snapshot would get wrong is the SCHEMA, and the epoch moves on exactly that.
    pub fn read_catalog<'c>(&self, cache: &'c mut Option<(u64, Arc<Catalog>)>) -> &'c Catalog {
        let now = self.epoch.load(std::sync::atomic::Ordering::Acquire);
        let stale = match cache.as_ref() {
            Some((cached, _)) => *cached != now,
            None => true,
        };
        if stale {
            // Taking the exclusive lock here is correct and rare: only on first use and after a
            // schema change. It is the one place the read path can block, and it cannot livelock
            // because `now` is re-read on the next statement, not spun on.
            let snapshot = Arc::new(self.catalog().clone());
            *cache = Some((now, snapshot));
        }
        &cache.as_ref().expect("just populated").1
    }
}

/// An exclusive catalog borrow that republishes the schema epoch when it is released.
///
/// Correct by construction rather than by discipline: every exclusive acquisition goes through
/// `ServerContext::catalog()`, so there is no path that can mutate the catalog and forget to tell
/// the readers. A caller that took the lock and changed nothing simply stores the same number.
pub struct CatalogGuard<'a> {
    inner: std::sync::MutexGuard<'a, Catalog>,
    epoch: &'a std::sync::atomic::AtomicU64,
}

impl std::ops::Deref for CatalogGuard<'_> {
    type Target = Catalog;
    fn deref(&self) -> &Catalog {
        &self.inner
    }
}

impl std::ops::DerefMut for CatalogGuard<'_> {
    fn deref_mut(&mut self) -> &mut Catalog {
        &mut self.inner
    }
}

impl Drop for CatalogGuard<'_> {
    fn drop(&mut self) {
        // Release, paired with the Acquire load in `read_catalog`: a reader that observes the new
        // epoch must also observe everything this statement wrote to the catalog.
        self.epoch.store(self.inner.epoch(), std::sync::atomic::Ordering::Release);
    }
}

/// Serve connections until `listener` stops yielding them, one thread per connection.
///
/// ## Lock ordering
///
/// One lock is added here — the catalog mutex — and it is taken **outermost**, for the duration of
/// a single statement, and released before the next message is read. Nothing beneath it ever takes
/// it back: `executor::run`, `AgentRuntime` and the branch catalog all receive `&mut Catalog` and
/// have no way to reach this mutex. So the order this file introduces is
///
/// > catalog mutex → (runtime state lock ⇄ branch catalog lock, in whatever order they already use)
///
/// which leaves the existing pair exactly as `src/agent_sql/runtime.rs` documents it and adds no
/// third order. Holding it across a whole *connection* rather than a statement would be simpler
/// and would also be indistinguishable from the old sequential server: the second client would sit
/// idle until the first disconnected.
pub fn serve(listener: TcpListener, ctx: Arc<ServerContext>) -> std::io::Result<()> {
    for stream in listener.incoming() {
        let stream = stream?;
        let ctx = Arc::clone(&ctx);
        // Detached on purpose: a connection's lifetime is the client's business, and joining here
        // would rebuild the sequential server this replaced.
        std::thread::spawn(move || {
            if let Err(e) = handle(stream, &ctx) {
                eprintln!("pgwire: connection ended: {e}");
            }
        });
    }
    Ok(())
}

/// Handle one connection start to finish.
pub fn handle(mut stream: TcpStream, ctx: &Arc<ServerContext>) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);

    // --- startup -----------------------------------------------------------------------------
    // Clients open with an SSL probe. Refusing it with a bare 'N' is a legal answer and the
    // client then re-sends a real startup packet, so the loop runs at most twice.
    let mut params_seen = false;
    let mut startup_params: Vec<(String, String)> = Vec::new();
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
        if code == CANCEL_REQUEST_CODE {
            // A cancel request opens a second connection, says which backend to interrupt, and
            // expects the server to close without answering. There is nothing to cancel here —
            // statements are not interruptible — so closing quietly is the whole correct
            // behaviour, and it is what libpq expects to see.
            return Ok(());
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
        // The rest of the startup packet is NUL-terminated key/value pairs, ending with an empty
        // key. A client sets parameters here instead of with `SET` — asyncpg sends
        // `client_encoding` this way on every connection — so ignoring them would make `SHOW`
        // answer with a default the client never chose.
        startup_params = decode_startup_params(&body[4..]);
        params_seen = true;
        break;
    }
    if !params_seen {
        return Ok(());
    }

    // No authentication: this is a local demonstration server, and pretending otherwise by
    // sending AuthenticationCleartextPassword and then accepting anything would be worse.
    let mut conn = Connection::new(Session::with_runtime(Arc::clone(&ctx.runtime)));
    for (k, v) in startup_params {
        conn.session_params.apply_startup(&k, &v);
    }
    stream.write_all(&Message::AuthenticationOk.encode())?;
    for (k, v) in conn.session_params.startup_status() {
        stream.write_all(&Message::ParameterStatus(k, v).encode())?;
    }
    for m in [
        Message::BackendKeyData { pid: std::process::id() as i32, key: 0 },
        Message::ReadyForQuery(TxnStatus::Idle),
    ] {
        stream.write_all(&m.encode())?;
    }
    stream.flush()?;

    // --- message loop ------------------------------------------------------------------------
    // Extended-protocol replies accumulate here until a `Sync`, a `Flush` or an error asks for
    // them; see the `b'P' | ...` arm below.
    let mut pending: Vec<u8> = Vec::new();
    loop {
        let mut tag = [0u8; 1];
        if reader.read_exact(&mut tag).is_err() {
            return Ok(()); // client vanished; nothing to say to a closed socket
        }
        let len = read_i32(&mut reader)?;
        if len < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("message '{}' claims {len} bytes, which cannot hold its own length", tag[0] as char),
            ));
        }
        let body = read_exact(&mut reader, (len - 4) as usize)?;

        // A message that is not part of an extended sequence ends one: whatever is still buffered
        // has to reach the client before this message's own reply does, or the two arrive in the
        // wrong order.
        if !matches!(tag[0], b'P' | b'B' | b'D' | b'E' | b'C' | b'H' | b'S') && !pending.is_empty() {
            stream.write_all(&pending)?;
            stream.flush()?;
            pending.clear();
        }

        match tag[0] {
            b'Q' => {
                let sql = String::from_utf8_lossy(&body)
                    .trim_end_matches('\0')
                    .trim()
                    .to_string();
                // A simple query ends the extended-protocol error state too: `Q` is its own
                // synchronisation point, exactly like `Sync`.
                conn.resync();
                for m in simple_query(&sql, &mut conn, ctx) {
                    stream.write_all(&m.encode())?;
                }
                stream.write_all(&Message::ReadyForQuery(conn.txn_status()).encode())?;
                stream.flush()?;
            }
            b'X' => return Ok(()),
            // Everything below is the extended query protocol. It differs from `Q` in one way that
            // shapes the whole loop: replies are *not* self-delimiting. `Parse`/`Bind`/`Describe`
            // produce no `ReadyForQuery`; only `Sync` does. So a client can pipeline the lot in one
            // packet, and the server must not answer until it has read the whole sequence.
            b'P' | b'B' | b'D' | b'E' | b'C' | b'H' | b'S' => {
                let out = extended::dispatch(tag[0], &body, &mut conn, ctx);
                for m in out.messages {
                    pending.extend_from_slice(&m.encode());
                }
                // Held back until the client asks for it, exactly as Postgres does: a driver
                // pipelines Parse/Bind/Execute/Sync in one packet and wants one answer back, and
                // a write per message turns one round trip into five. The size bound is what
                // stops a large result set from being buffered in full.
                if out.flush || pending.len() >= 32 * 1024 {
                    stream.write_all(&pending)?;
                    stream.flush()?;
                    pending.clear();
                }
            }
            b'd' | b'c' | b'f' => {
                // COPY data from a client that thinks a COPY is in progress. There is no COPY
                // support here, so the only useful answer is to say so; silently dropping the
                // bytes would leave the client waiting forever for a CopyDone acknowledgement.
                conn.fail_until_sync();
                stream.write_all(
                    &Message::error(
                        "0A000",
                        "COPY is not implemented by this server; use INSERT",
                    )
                    .encode(),
                )?;
                stream.flush()?;
            }
            other => {
                // An unknown tag means this server and the client disagree about the protocol.
                // Saying so beats silence: a client that gets no reply hangs, and one that gets a
                // bare ReadyForQuery concludes it succeeded.
                conn.fail_until_sync();
                let m = Message::error(
                    "08P01",
                    format!("unknown frontend message type '{}'", other as char),
                );
                stream.write_all(&m.encode())?;
                stream.flush()?;
            }
        }
    }
}

/// The key/value pairs a startup packet carries after its protocol version.
///
/// A malformed tail is dropped rather than refused: these are advisory settings, and a client that
/// gets its own startup packet wrong has bigger problems than `application_name`.
fn decode_startup_params(mut rest: &[u8]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    loop {
        let Some(k_end) = rest.iter().position(|&b| b == 0) else { return out };
        let key = String::from_utf8_lossy(&rest[..k_end]).to_string();
        rest = &rest[k_end + 1..];
        if key.is_empty() {
            return out;
        }
        let Some(v_end) = rest.iter().position(|&b| b == 0) else { return out };
        let value = String::from_utf8_lossy(&rest[..v_end]).to_string();
        rest = &rest[v_end + 1..];
        out.push((key, value));
    }
}

/// Run one simple-query string and turn the result into backend messages.
///
/// A simple query may carry several statements separated by semicolons, and every one of them
/// reports its own `CommandComplete`.
fn simple_query(sql: &str, conn: &mut Connection, ctx: &ServerContext) -> Vec<Message> {
    if sql.is_empty() {
        return vec![Message::EmptyQueryResponse];
    }
    match run_simple(sql, conn, ctx) {
        Ok(msgs) => msgs,
        Err(e) => vec![Message::error(sqlstate_of(&e), e.to_string())],
    }
}

fn run_simple(
    sql: &str,
    conn: &mut Connection,
    ctx: &ServerContext,
) -> Result<Vec<Message>, FerroError> {
    let stmts = Statement::parse_batch(sql, conn, ctx)?;
    if stmts.is_empty() {
        return Ok(vec![Message::EmptyQueryResponse]);
    }
    let mut out = Vec::new();
    for stmt in stmts {
        // Text format throughout: the simple query protocol has no way to ask for anything else.
        let fields = stmt.describe_rows();
        let mut result = stmt.execute(conn, ctx, &[])?;
        if let Some(mut fields) = fields {
            // **A computed column's type comes from the value, and only the simple protocol may do
            // this.** `bind_projection` types `SELECT branch_id = 9999` with the binder's `Integer`
            // placeholder while the value is a BOOLEAN, so `fields_of` refuses to announce the
            // placeholder and says `text` instead — a promise it can always keep, because every
            // value has a text form.
            //
            // Here it can do better. The simple query protocol has no `Describe`, so this
            // `RowDescription` is built AFTER the statement ran and a real value is in hand. The
            // extended path cannot: it must answer `Describe` before any row exists, so `text`
            // remains the honest answer there and this deliberately does not touch it.
            //
            // Only the placeholder is upgraded. A column whose declared type the binder actually
            // worked out keeps it, so this cannot silently re-type a real column from one row.
            if let Some(first) = result.rows.first() {
                for (i, f) in fields.iter_mut().enumerate() {
                    if f.name == "?column?" && f.type_oid == types::oid::TEXT {
                        if let Some(v) = first.get(i) {
                            f.type_oid = types::oid_of(v);
                        }
                    }
                }
            }
            out.push(Message::RowDescription(fields.clone()));
            for row in result.rows.drain(..) {
                out.push(extended::encode_row(&row, &fields)?);
            }
        }
        out.push(match result.tag {
            Some(tag) => Message::CommandComplete(tag),
            None => Message::EmptyQueryResponse,
        });
    }
    Ok(out)
}

/// The SQLSTATE this server reports for an engine error.
///
/// Coarse on purpose, and stated rather than defaulted: a client branches on these codes, and the
/// one distinction that changes client behaviour is "your statement was wrong" (class 42, which a
/// driver reports to its caller) against "the server could not do it" (class 58/XX, which a
/// connection pool may treat as a reason to discard the connection).
pub fn sqlstate_of(e: &FerroError) -> &'static str {
    match e {
        FerroError::SqlParseError(_) => "42601", // syntax_error
        FerroError::Bind(_) => "42P01",          // undefined_table / undefined_column
        FerroError::Constraint(_) => "23000",    // integrity_constraint_violation
        FerroError::Txn(_) => "25000",           // invalid_transaction_state
        FerroError::Branch(_) => "42000",        // this server's own branch statements
        _ => "XX000",                            // internal_error
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reported_server_version_parses_as_a_version_and_stays_below_the_jit_era() {
        // asyncpg reads this with a regex that takes the first number it finds, and switches on
        // `>= (11, 0)`. If this string ever starts with a bigger number, drivers will begin
        // issuing statements this server does not implement.
        let first: u32 = SERVER_VERSION
            .split(|c: char| !c.is_ascii_digit())
            .find(|s| !s.is_empty())
            .and_then(|s| s.parse().ok())
            .expect("server_version must begin with a number a driver can parse");
        assert!(first < 11, "claiming {first} turns on client features this server lacks");
        assert!(first >= 9, "below 9 a modern driver may refuse to connect at all");
    }
}
