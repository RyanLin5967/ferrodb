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
    /// Set while an EXCLUSIVE catalog borrow is outstanding.
    ///
    /// Half of a two-flag handshake with the per-connection busy slots below. `DROP TABLE` frees
    /// heap, time-travel, primary and secondary pages **immediately**
    /// (`Catalog::drop_table`), and once a read stopped taking the outermost lock there was
    /// nothing left to stop it descending those pages as they were freed. The epoch cannot fix
    /// that: it is bumped *after* the pages are gone, and a statement already in flight never
    /// re-reads it.
    writer_active: std::sync::atomic::AtomicBool,
    /// One busy slot per live connection, each written only by its owner.
    ///
    /// Per-connection rather than a shared reader count on purpose: a count is one word every
    /// reader RMWs, which is exactly the wall this whole line removed — measured at x0.137 for
    /// `Arc::clone` and x0.121 for `RwLock::read` against a relaxed load's x7.823
    /// (`bench/d51_sharedword_probe.txt`). This is the same shape as D44's per-thread touch
    /// shards, which this project already shipped.
    readers: Mutex<Vec<Arc<std::sync::atomic::AtomicBool>>>,
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
        // D101 — building a `ServerContext` is this process stating which engine its statements
        // run on. From here an agent statement on any OTHER runtime is refused; see
        // `agent_sql::designated` for the defect that motivates it and for the blind spots.
        crate::agent_sql::designated::designate(&runtime);
        ServerContext {
            catalog: Mutex::new(catalog),
            epoch: std::sync::atomic::AtomicU64::new(e),
            writer_active: std::sync::atomic::AtomicBool::new(false),
            readers: Mutex::new(Vec::new()),
            bp,
            txn,
            runtime,
        }
    }

    /// A `Session` bound to **this** context's runtime.
    ///
    /// The one-call correct way to make a session beside a `ServerContext`, and the reason to
    /// reach for it is that `Session::new()` beside a `ServerContext` is a bug: it builds a
    /// private `AgentRuntime::new()` with `storage: None` and runs every agent statement against
    /// that instead of the engine this context owns. `agent_sql::designated` refuses such a
    /// statement; this exists so the refusal has an obvious answer rather than only a diagnosis.
    pub fn session(&self) -> crate::execution::session::Session {
        crate::execution::session::Session::with_runtime(Arc::clone(&self.runtime))
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
        // EVERY exclusive borrow drains the readers, not only the ones that free pages.
        // Deciding here which statements will free pages would be a second predicate that can
        // drift from what the executor actually does — the same trap `try_run_read` avoids by
        // returning `Option` instead of exposing `is_read()`. Draining unconditionally is
        // strictly stronger and costs nothing when no read is in flight, which is the common
        // case: the scan is N atomic loads over live connections.
        self.drain_readers();
        CatalogGuard { inner, epoch: &self.epoch, writer_active: &self.writer_active }
    }

    /// Register a connection's busy slot. Called once per connection, never on a statement.
    pub fn register_reader(&self, slot: Arc<std::sync::atomic::AtomicBool>) {
        let mut v = match self.readers.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        // Self-cleaning: a slot only the registry still holds belongs to a connection that has
        // gone away, so the list cannot grow without bound across a server's life.
        v.retain(|s| Arc::strong_count(s) > 1);
        v.push(slot);
    }

    /// Announce a writer and wait until no reader is inside a shared-path statement.
    fn drain_readers(&self) {
        use std::sync::atomic::Ordering::SeqCst;
        self.writer_active.store(true, SeqCst);
        // Copy the list and release the registry lock BEFORE spinning, so a connection opening
        // right now is not blocked behind a drain it has nothing to do with.
        let slots: Vec<Arc<std::sync::atomic::AtomicBool>> = match self.readers.lock() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        };
        for s in slots {
            while s.load(SeqCst) {
                std::hint::spin_loop();
            }
        }
    }

    /// Enter a shared-path read, or `None` if a writer is announced.
    ///
    /// **`SeqCst` on both sides is what makes this sound**, and it is the whole argument: with a
    /// single total order over the store here and the load in `drain_readers`, either the writer
    /// observes this slot busy and waits, or this reader observes `writer_active` and stands
    /// down. Both can happen; neither can be missed. Relaxed or Acquire/Release would permit the
    /// store-buffering case where each misses the other, which is precisely a reader descending
    /// pages a writer is freeing.
    ///
    /// Standing down is not a failure: the caller falls back to the exclusive path, which blocks
    /// on the mutex and is correct.
    ///
    /// ⛔ **This used to end "It happens only while DDL is actually in flight." That is FALSE, and
    /// it is false by construction rather than by degree.** The announcer set is exhaustively
    /// enumerable and none of it is DDL-gated: `ServerContext::drain_readers` is the only writer
    /// of `writer_active`, [`ServerContext::catalog`] is its only caller, and `catalog()` has
    /// exactly four callers in `src/` — `branch::lease_thread`'s `RuntimeLock` impl,
    /// `pgwire::extended::Statement::parse_one`, the exclusive execution path, and
    /// [`ServerContext::read_catalog`]'s snapshot refresh. **`parse_one` takes it unconditionally
    /// for every `Kind::Sql`**, so a plain `SELECT 1` on the simple query protocol announces a
    /// writer and drains every reader at parse time.
    ///
    /// ⚠ **And do NOT replace that sentence with "the lease thread announces every
    /// `DEFAULT_SCAN_MILLIS`", which is the correction a previous pass tried and which is also
    /// false.** `scan_once` computes its candidates OUTSIDE the lock and takes it once per
    /// *reaped branch* (`REAP_CHUNK`), so a scan that finds nothing expired takes it **zero**
    /// times — `lease_thread`'s own module doc says exactly that. Measured, the lease thread
    /// announced zero times in every arm ever run, because a branch expires
    /// `DEFAULT_LEASE_MILLIS` (15 min) after fork and no arm runs that long. Its real axis is
    /// **expired branches per tick**, and it is still unmeasured.
    ///
    /// ⇒ **What governs the stand-down rate is the offered EXCLUSIVE rate, and it is measured:**
    /// on the extended query protocol — what real drivers use — it runs from **1.07% at one fork
    /// per 200 statements to 95% at one per one**. It is also a FIXED POINT, because a stood-down
    /// read falls back and therefore announces, so the rate and the announcer set drive each
    /// other. **Quote a rate only with the fork rate it was taken at.**
    ///
    /// ⚠ **PROVENANCE, stated precisely because an earlier version of this comment got it wrong.**
    /// The evidence is `bench/w4_check3_rederived.txt`, which is **NOT ON `main`** — it lives on
    /// branch `w4-check3-rederive` at `cef94dc` and has not landed. This comment previously cited
    /// it as a bare path, so a reader on `main` would `ls bench/` and find nothing, unable to tell
    /// whether it was deleted or never produced. **A citation must name a tree it can be found
    /// in.** ⚠ That artifact also cites `extended.rs:290`/`:387`; on `main` those sites are
    /// `:283`/`:357` — the branch carries measurement instrumentation, so **line numbers are not
    /// comparable across the two trees** and only the symbols are.
    pub fn begin_read<'a>(
        &self,
        slot: &'a std::sync::atomic::AtomicBool,
    ) -> Option<ReadPass<'a>> {
        use std::sync::atomic::Ordering::SeqCst;
        slot.store(true, SeqCst);
        if self.writer_active.load(SeqCst) {
            slot.store(false, SeqCst);
            return None;
        }
        Some(ReadPass { slot })
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
    writer_active: &'a std::sync::atomic::AtomicBool,
}

/// Proof that a shared-path read is in flight. Clears its slot on drop, including on a panic or
/// an early `?`, which is why it is a guard and not a pair of calls.
pub struct ReadPass<'a> {
    slot: &'a std::sync::atomic::AtomicBool,
}

impl Drop for ReadPass<'_> {
    fn drop(&mut self) {
        self.slot.store(false, std::sync::atomic::Ordering::SeqCst);
    }
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
        // Released after the epoch, so a reader that stands down and retries sees the new one.
        self.writer_active.store(false, std::sync::atomic::Ordering::SeqCst);
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
    // Registered once, here, rather than on each statement: a per-statement registration would be
    // a write to shared state on the read path, which is the wall this whole line removed.
    ctx.register_reader(conn.read_slot());
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
