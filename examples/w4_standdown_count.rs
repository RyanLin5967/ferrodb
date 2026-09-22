//! W4 check 3 — **does standing down stay rare?** A COUNT, not a timing.
//!
//! # The question
//!
//! `ServerContext::begin_read` takes a per-connection slot and then checks `writer_active`; if a
//! writer is announced it clears the slot and answers `None`, and the caller falls back to the
//! exclusive catalog mutex. Its doc claims that *"happens only while DDL is actually in flight"*.
//!
//! W4 proposes moving DML onto that shared path. **If the claim is wrong — if MERGE, forks or the
//! reaper announce often enough that an arriving statement stands down routinely — W4 buys
//! nothing**, because a stood-down statement takes the exclusive mutex anyway. That bound holds
//! under every design option on the table, including making the root cell durable: option (c)
//! removes the tail write and dissolves the deadlock in check 4, but a shared-path DML still calls
//! `begin_read` and still stands down whatever the tail does.
//!
//! # ⛔ Why this is not a timing experiment
//!
//! Other lanes share this box, with a 46x quiet-vs-loaded spread measured here. A duration
//! measured under that is an upper bound and nothing better. **The discriminating quantity is a
//! ratio of two integers** — `begin_read` either returned `Some(ReadPass)` or it did not — and an
//! integer is load-immune. Nothing in this harness is timed, and nothing in it needs a quiet box.
//!
//! # ⚠ THE LAYER, stated because ignoring it has invalidated three results in this project
//!
//! Every number here is measured **through pgwire**: a real `TcpListener`, the shipped
//! `pgwire::handle` per connection, the real startup handshake, and real frontend messages on a
//! loopback socket. Nothing drives `AgentRuntime`, `Session` or `ServerContext` directly. The one
//! deviation from a production deployment is that the client threads live in the same process as
//! the server threads, which is what lets the counters be read without an IPC channel; it changes
//! scheduling, not which functions run or what they count.
//!
//! The accept loop is this file's own rather than `pgwire::serve`, for one reason: `serve` never
//! returns, and an arm must be able to stop accepting before the next arm resets the counters.
//! Per connection it calls the same `pgwire::handle` that `serve` calls, with nothing between.
//!
//! # PRE-REGISTERED OUTCOMES — written before the first run
//!
//! * **A** — the stand-down fraction is near zero in workloads 1-3. The source's claim holds and
//!   W4's payoff survives check 3.
//! * **B** — it is material in workload 2 (agent-realistic) or 3 (merge-heavy). **Check 3 FAILS
//!   and W4's win evaporates.** This is the more valuable result and is to be reported plainly.
//! * **C** — the counts cannot discriminate; for example the reaper's cadence dominates and
//!   cannot be separated from client-caused announcements. That is a fact about the INSTRUMENT and
//!   must be labelled as one, together with what would settle it.
//!
//! "Material" is not left to taste: the threshold is stated in the artifact as **1%** of attempts,
//! chosen because W4's own model is that the gain scales with the fraction of statements that keep
//! the shared path. A 1-in-100 fallback leaves 99% of the win; a 1-in-5 does not.
//!
//! ⚠ **The deciding arm is `agent` — concurrent forks and reads, no DDL** — because that is the
//! shape BranchBench names and the shape this project claims to serve.
//!
//! # ⚠ What a DML-bearing arm does and does not transfer
//!
//! Today a DML takes the exclusive path, so it announces a writer for its whole statement; under
//! W4 it would not. So an arm containing DML has a LARGER announcer set here than the same arm
//! would have under W4, and its fraction is a **today** number that overstates W4's. An arm whose
//! only announcers are forks, MERGE, DDL and the reaper has the same announcer set in both worlds
//! and transfers directly. `agent_dml` is included precisely so the size of that gap is visible
//! rather than argued about; `agent`, `merge` and `ddl` are the transferable arms.
//!
//! # Arms
//!
//! | arm | clients | announcers |
//! |---|---|---|
//! | `readonly` | all readers | parse, and the first-statement snapshot refresh |
//! | `agent` ⭐ | readers + forkers (`BEGIN AGENT SESSION` / `ABANDON`) | the above + the fork statement |
//! | `agent_dml` | readers + forkers + DML on main | the above + DML (a *today* number) |
//! | `merge` | readers + fork/update/MERGE | the above + MERGE |
//! | `ddl` | readers + forkers + CREATE/DROP TABLE | the above + DDL — the upper bound |
//!
//! Each arm runs under both wire protocols, because they differ in one way that turns out to
//! matter: `Statement::parse_one` takes the EXCLUSIVE catalog, so the simple query protocol
//! announces a writer on every statement while the extended protocol announces once per prepared
//! statement. A real driver (asyncpg, pg8000) caches prepared statements and is the `extended`
//! row; `psql` and anything issuing ad-hoc SQL is the `simple` row.
//!
//! # Refusals
//!
//! Nothing here reports a calm number it did not earn. The run exits non-zero on: an
//! uninstrumented build (`counts()` answering `None` — which is why it is an `Option` and not a
//! defaulted zero), zero shared-path attempts, any SQL error from any client, and an arm whose
//! defining statement — the fork, the merge, the DDL — executed zero times.
//!
//! Usage:
//! ```text
//! cargo run --release --features w4-standdown-count --example w4_standdown_count
//! W4_CLIENTS=8 W4_ROUNDS=300 W4_LEASE_MS=30000 W4_ARMS=agent,merge ... (all arms by default)
//! ```

use std::collections::HashSet;
use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::lease_thread::{LeaseThread, RuntimeLock};
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::{BranchCatalog, Reaper, TableBranchCatalog};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::pgwire::standdown::{self, Counts};
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::db_lock::DbLock;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

// ---------------------------------------------------------------------------------------------
// A minimal frontend. Not a driver: enough of the v3 protocol to issue statements and to notice
// an error, which is all a counting harness needs. The server is the shipped one.
// ---------------------------------------------------------------------------------------------

const PROTOCOL_V3: i32 = 196608;

struct Client {
    w: TcpStream,
    r: BufReader<TcpStream>,
    /// Prepared statement names already sent as `Parse` on THIS connection, so the extended arm
    /// re-binds rather than re-parsing — which is what a driver that caches does, and the whole
    /// reason the two protocol rows differ.
    prepared: HashSet<String>,
}

impl Client {
    fn connect(addr: SocketAddr) -> std::io::Result<Client> {
        let w = TcpStream::connect(addr)?;
        w.set_nodelay(true)?;
        let r = BufReader::new(w.try_clone()?);
        let mut c = Client { w, r, prepared: HashSet::new() };
        c.startup()?;
        Ok(c)
    }

    fn startup(&mut self) -> std::io::Result<()> {
        let mut body = Vec::new();
        body.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
        for (k, v) in [("user", "w4"), ("database", "w4")] {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut pkt = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        pkt.extend_from_slice(&body);
        self.w.write_all(&pkt)?;
        self.w.flush()?;
        self.drain_to_ready().map(|_| ())
    }

    fn msg(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    /// Simple query protocol: one `Query` message, which re-parses every time.
    fn simple(&mut self, sql: &str) -> std::io::Result<Vec<String>> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        self.w.write_all(&Client::msg(b'Q', &body))?;
        self.w.flush()?;
        self.drain_to_ready()
    }

    /// Extended protocol: `Parse` once per statement NAME, then `Bind`/`Execute`/`Sync`.
    fn extended(&mut self, name: &str, sql: &str) -> std::io::Result<Vec<String>> {
        let mut out = Vec::new();
        if !self.prepared.contains(name) {
            let mut body = name.as_bytes().to_vec();
            body.push(0);
            body.extend_from_slice(sql.as_bytes());
            body.push(0);
            body.extend_from_slice(&0i16.to_be_bytes()); // no parameter type hints
            out.extend_from_slice(&Client::msg(b'P', &body));
            self.prepared.insert(name.to_string());
        }
        let mut bind = vec![0u8]; // unnamed portal
        bind.extend_from_slice(name.as_bytes());
        bind.push(0);
        for n in [0i16, 0, 0] {
            // no format codes, no parameters, no result format codes
            bind.extend_from_slice(&n.to_be_bytes());
        }
        out.extend_from_slice(&Client::msg(b'B', &bind));
        let mut exec = vec![0u8]; // unnamed portal
        exec.extend_from_slice(&0i32.to_be_bytes()); // unlimited rows
        out.extend_from_slice(&Client::msg(b'E', &exec));
        out.extend_from_slice(&Client::msg(b'S', &[]));
        self.w.write_all(&out)?;
        self.w.flush()?;
        self.drain_to_ready()
    }

    /// Read backend messages until `ReadyForQuery`, returning every error text seen.
    fn drain_to_ready(&mut self) -> std::io::Result<Vec<String>> {
        let mut errors = Vec::new();
        loop {
            let mut tag = [0u8; 1];
            self.r.read_exact(&mut tag)?;
            let mut len = [0u8; 4];
            self.r.read_exact(&mut len)?;
            let n = i32::from_be_bytes(len);
            if n < 4 {
                return Err(std::io::Error::other(format!("backend sent length {n}")));
            }
            let mut body = vec![0u8; (n - 4) as usize];
            self.r.read_exact(&mut body)?;
            match tag[0] {
                b'E' => errors.push(error_text(&body)),
                b'Z' => return Ok(errors),
                _ => {}
            }
        }
    }

    /// Terminate, then read to EOF. The read is the point: it returns only once the server's
    /// `handle` has returned and dropped the socket, so a joined client thread is proof the
    /// connection thread is gone and cannot announce a writer into the next arm's counters. A
    /// sleep would prove nothing.
    fn terminate(mut self) -> std::io::Result<()> {
        self.w.write_all(&Client::msg(b'X', &[]))?;
        self.w.flush()?;
        let mut sink = Vec::new();
        self.r.read_to_end(&mut sink)?;
        Ok(())
    }
}

/// The `M` field of an `ErrorResponse`, which is the human-readable message.
fn error_text(body: &[u8]) -> String {
    let mut rest = body;
    let mut out = String::new();
    while let Some(&field) = rest.first() {
        if field == 0 {
            break;
        }
        rest = &rest[1..];
        let end = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
        let value = String::from_utf8_lossy(&rest[..end]).to_string();
        if field == b'M' {
            out = value;
        }
        rest = &rest[(end + 1).min(rest.len())..];
    }
    if out.is_empty() { format!("unparseable ErrorResponse: {body:?}") } else { out }
}

// ---------------------------------------------------------------------------------------------
// The engine, built exactly as `examples/pgserver.rs` builds it: a real arena page store, a
// storage-backed `AgentRuntime` with a reaper, and the lease thread that scans under the same
// catalog mutex a statement takes. A stub runtime here would answer a different question.
// ---------------------------------------------------------------------------------------------

struct Engine {
    ctx: Arc<ServerContext>,
    lease: Option<LeaseThread>,
    store: Arc<ArenaPageStore>,
    arena_path: String,
    _lock: DbLock,
}

fn build_engine(db: &str, lease_ms: u64) -> Engine {
    let lock = DbLock::acquire(Path::new(db)).expect("db lock");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(db)
        .expect("open db");
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(format!("{db}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let catalog = Catalog::create(bp.clone()).unwrap();

    let arena_path = format!("{db}.arena");
    let branches =
        Arc::new(TableBranchCatalog::default_for_database(db, 1).expect("branch catalog"));
    let base = bp.disk_manager.high_water().expect("high water") + 32_736;
    let store: Arc<ArenaPageStore> = Arc::new(
        ArenaPageStore::new(bp.clone(), branches.clone() as Arc<dyn BranchCatalog>, base)
            .expect("arena"),
    );
    store.checkpoint_to(std::path::PathBuf::from(&arena_path));

    let reaper = Arc::new(TwoTierReaper::new(
        branches.clone() as Arc<dyn BranchCatalog>,
        store.clone(),
    ));
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches.clone() as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            store.clone() as Arc<dyn PageStore>,
        )
        .expect("storage-backed runtime")
        .with_reaper(reaper.clone() as Arc<dyn Reaper>),
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp, txn, runtime.clone()));
    let lease = LeaseThread::start(
        reaper,
        runtime,
        ctx.clone() as Arc<dyn RuntimeLock>,
        std::time::Duration::from_millis(lease_ms),
    )
    .expect("lease thread");
    Engine { ctx, lease: Some(lease), store, arena_path, _lock: lock }
}

impl Engine {
    fn shutdown(mut self) {
        if let Some(l) = self.lease.take() {
            l.stop();
        }
        self.store.checkpoint(Path::new(&self.arena_path)).expect("checkpoint");
    }
}

// ---------------------------------------------------------------------------------------------
// Arms
// ---------------------------------------------------------------------------------------------

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum Role {
    Reader,
    Forker,
    ForkMerge,
    Dml,
    Ddl,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum Proto {
    /// One `Query` message per statement. `parse_one` runs every time, so the client takes the
    /// EXCLUSIVE catalog itself immediately before its own `begin_read`.
    Simple,
    /// `Parse` once per statement text, then `Bind`/`Execute`/`Sync` -- what asyncpg and pg8000
    /// do. A repeated read never touches the exclusive catalog at all.
    Extended,
    /// Extended framing with a FRESH statement name every time, so `Parse` runs on every
    /// statement. **This row exists to discriminate a mechanism, not to model a driver**: it is
    /// `extended` in every respect except that the client re-acquires the exclusive catalog just
    /// before its own `begin_read`, exactly as `simple` does. If `simple`'s lower stand-down rate
    /// comes from that self-release and not from something else in the simple-query code path,
    /// this row must track `simple`; if it tracks `extended`, that explanation is wrong.
    ExtendedReparse,
}

impl Proto {
    fn label(self) -> &'static str {
        match self {
            Proto::Simple => "simple",
            Proto::Extended => "extended",
            Proto::ExtendedReparse => "ext_reparse",
        }
    }
}

const PROTOS: [Proto; 3] = [Proto::Simple, Proto::Extended, Proto::ExtendedReparse];

/// How an arm splits `clients` threads between roles. The reader half is what a stand-down
/// actually costs under W4; the rest is the announcer population.
fn roles(arm: &str, clients: usize) -> Vec<Role> {
    // `W4_ANNOUNCERS` overrides the split so the stand-down fraction can be read as a CURVE over
    // the announcer mix rather than as one ratio at one arbitrary mix. One number cannot tell a
    // property of the design from a property of the mix that produced it.
    if let Some(n) = std::env::var("W4_ANNOUNCERS").ok().and_then(|v| v.parse::<usize>().ok()) {
        let n = n.min(clients);
        let announcer = match arm {
            "agent" | "ddl" | "agent_dml" => Role::Forker,
            "merge" => Role::ForkMerge,
            _ => Role::Reader,
        };
        let mut v = vec![Role::Reader; clients - n];
        v.extend(std::iter::repeat_n(announcer, n));
        return v;
    }
    let half = (clients / 2).max(1);
    match arm {
        "readonly" => vec![Role::Reader; clients],
        "agent" => {
            let mut v = vec![Role::Reader; half];
            v.extend(std::iter::repeat_n(Role::Forker, clients - half));
            v
        }
        "agent_dml" => {
            let mut v = vec![Role::Reader; half];
            let rest = clients - half;
            v.extend(std::iter::repeat_n(Role::Forker, rest / 2));
            v.extend(std::iter::repeat_n(Role::Dml, rest - rest / 2));
            v
        }
        "merge" => {
            let mut v = vec![Role::Reader; half];
            v.extend(std::iter::repeat_n(Role::ForkMerge, clients - half));
            v
        }
        "ddl" => {
            let mut v = vec![Role::Reader; half];
            let rest = clients - half;
            v.extend(std::iter::repeat_n(Role::Forker, rest.saturating_sub(1)));
            v.push(Role::Ddl);
            v
        }
        other => panic!("unknown arm {other:?}"),
    }
}

/// How often a `Forker` actually forks, in rounds. 1 means every round.
fn fork_every() -> usize {
    std::env::var("W4_FORK_EVERY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(1)
}

/// The statements one round of a role issues, as `(prepared-name, sql)`.
///
/// The name is what the extended arm keys its `Parse` cache on, so a role whose SQL is constant
/// parses once per connection and a role whose SQL carries a fresh identifier parses every round.
/// That is the real driver behaviour, not a simplification: a fork statement naming a new run is a
/// new query text and no cache can reuse it.
fn round(role: Role, w: usize, i: usize) -> Vec<(String, String)> {
    match role {
        Role::Reader => {
            vec![("read".into(), format!("SELECT id, v FROM t WHERE id = {};", i % 64))]
        }
        // `W4_FORK_EVERY=n` makes a forker fork once every n rounds and read in between, which is
        // the OFFERED-LOAD axis: it varies the fraction of all statements that are exclusive
        // without introducing a sleep. A sleep would make the answer depend on wall-clock
        // latencies and therefore on what else is running on this box; a statement-count ratio
        // does not. (The mapping from a statement-count ratio to a time duty cycle goes through
        // the fork/read latency ratio, which is itself roughly load-invariant because both slow
        // together — stated as the assumption it is.)
        Role::Forker => {
            let every = fork_every();
            if every > 1 && i % every != 0 {
                return vec![(
                    "read".into(),
                    format!("SELECT id, v FROM t WHERE id = {};", i % 64),
                )];
            }
            vec![
                (
                    format!("fork{w}_{i}"),
                    format!("BEGIN AGENT SESSION AS 'a{w}' RUN 'r{w}_{i}';"),
                ),
                ("abandon".into(), "ABANDON;".into()),
            ]
        }
        Role::ForkMerge => vec![
            (
                format!("fork{w}_{i}"),
                format!("BEGIN AGENT SESSION AS 'm{w}' RUN 'r{w}_{i}';"),
            ),
            ("upd".into(), format!("UPDATE t SET v = v + 1 WHERE id = {w};")),
            ("merge".into(), "MERGE;".into()),
        ],
        Role::Dml => {
            vec![("dml".into(), format!("UPDATE t SET v = v + 1 WHERE id = {};", 100 + w))]
        }
        Role::Ddl => vec![
            (
                format!("mk{w}_{i}"),
                format!("CREATE TABLE d{w}_{i} (id INTEGER NOT NULL);"),
            ),
            (format!("rm{w}_{i}"), format!("DROP TABLE d{w}_{i};")),
        ],
    }
}

/// The statement whose absence would make an arm a different experiment. An arm that ran zero of
/// these measured the calmer workload, not the one it names, so the run refuses.
fn defining_role(arm: &str) -> Option<Role> {
    match arm {
        "readonly" => None,
        "agent" | "agent_dml" => Some(Role::Forker),
        "merge" => Some(Role::ForkMerge),
        "ddl" => Some(Role::Ddl),
        _ => None,
    }
}

struct ArmResult {
    arm: String,
    proto: Proto,
    counts: Counts,
    statements: u64,
    defining_statements: u64,
    composition: String,
}

fn run_arm(arm: &str, proto: Proto, clients: usize, rounds: usize, lease_ms: u64, dir: &Path)
    -> ArmResult
{
    let roles = roles(arm, clients);
    let db = dir.join(format!("{arm}_{}.db", proto.label()));
    let engine = build_engine(db.to_str().unwrap(), lease_ms);

    // The shipped connection handler behind this file's own accept loop; see the module doc.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    let serving = Arc::new(AtomicBool::new(true));
    let accept_ctx = Arc::clone(&engine.ctx);
    let accept_flag = Arc::clone(&serving);
    let accept = std::thread::spawn(move || {
        for stream in listener.incoming() {
            if !accept_flag.load(Ordering::SeqCst) {
                return;
            }
            let Ok(stream) = stream else { return };
            let ctx = Arc::clone(&accept_ctx);
            std::thread::spawn(move || {
                let _ = ferrodb::pgwire::handle(stream, &ctx);
            });
        }
    });

    // ---- setup, over the wire like everything else -------------------------------------------
    let mut setup = Client::connect(addr).expect("setup connection");
    for sql in [
        "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);".to_string(),
        (0..64)
            .map(|i| format!("INSERT INTO t VALUES ({i}, {i});"))
            .collect::<Vec<_>>()
            .join(" "),
        (0..clients)
            .map(|w| format!("INSERT INTO t VALUES ({}, 0);", 100 + w))
            .collect::<Vec<_>>()
            .join(" "),
    ] {
        let errs = setup.simple(&sql).expect("setup io");
        assert!(errs.is_empty(), "setup failed on `{sql}`: {errs:?}");
    }
    setup.terminate().expect("setup terminate");

    // ---- the measured run --------------------------------------------------------------------
    let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let statements = Arc::new(AtomicU64::new(0));
    let defining = Arc::new(AtomicU64::new(0));
    let want = defining_role(arm);
    let defining_prefix: &'static str = match want {
        Some(Role::Forker) | Some(Role::ForkMerge) => "BEGIN AGENT SESSION",
        Some(Role::Ddl) => "CREATE TABLE",
        Some(Role::Dml) => "UPDATE",
        Some(Role::Reader) | None => "",
    };
    // Twice: once after every client has warmed up (so first-statement snapshot refreshes are not
    // the measurement), and once to release them all into the measured loop together.
    let gate = Arc::new(Barrier::new(clients + 1));

    let mut threads = Vec::new();
    for (w, role) in roles.iter().copied().enumerate() {
        let (errors, statements, defining, gate) = (
            Arc::clone(&errors),
            Arc::clone(&statements),
            Arc::clone(&defining),
            Arc::clone(&gate),
        );
        threads.push(std::thread::spawn(move || {
            let mut c = match Client::connect(addr) {
                Ok(c) => c,
                Err(e) => {
                    errors.lock().unwrap().push(format!("client {w} connect: {e}"));
                    gate.wait();
                    gate.wait();
                    return;
                }
            };
            let mut seq = 0usize;
            let mut issue = |c: &mut Client, name: &str, sql: &str| match proto {
                Proto::Simple => c.simple(sql),
                Proto::Extended => c.extended(name, sql),
                Proto::ExtendedReparse => {
                    seq += 1;
                    c.extended(&format!("{name}__{seq}"), sql)
                }
            };

            // Warm-up: one read, which forces this connection's catalog snapshot to exist and,
            // under `extended`, gets the reader's own statement prepared.
            if let Err(e) = issue(&mut c, "read", "SELECT id, v FROM t WHERE id = 1;") {
                errors.lock().unwrap().push(format!("client {w} warmup: {e}"));
            }
            gate.wait(); // everyone warm; the main thread resets the counters here
            gate.wait(); // go

            for i in 0..rounds {
                for (name, sql) in round(role, w, i) {
                    match issue(&mut c, &name, &sql) {
                        Ok(errs) => {
                            statements.fetch_add(1, Ordering::Relaxed);
                            // Only a statement that really is the arm's defining statement
                            // counts: with `W4_FORK_EVERY` a Forker round may issue a read, and
                            // counting that would let the zero-forks refusal pass on an arm that
                            // never forked.
                            if Some(role) == want && sql.starts_with(defining_prefix) {
                                defining.fetch_add(1, Ordering::Relaxed);
                            }
                            if !errs.is_empty() {
                                errors
                                    .lock()
                                    .unwrap()
                                    .push(format!("client {w} `{sql}`: {errs:?}"));
                            }
                        }
                        Err(e) => {
                            errors.lock().unwrap().push(format!("client {w} io on `{sql}`: {e}"));
                            return;
                        }
                    }
                }
            }
            if let Err(e) = c.terminate() {
                errors.lock().unwrap().push(format!("client {w} terminate: {e}"));
            }
        }));
    }

    gate.wait();
    standdown::reset();
    gate.wait();
    for t in threads {
        t.join().expect("a client thread panicked");
    }

    let counts = standdown::counts().unwrap_or_else(|| {
        eprintln!(
            "REFUSED: pgwire::standdown::counts() answered None. This build does not carry the \
             counters -- rebuild with `--features w4-standdown-count`. Reporting a zero here \
             would say 'never stood down' about a build that was never measuring."
        );
        std::process::exit(2);
    });

    // Stop accepting, then read the counters. Every client thread is joined and every client
    // terminated, so no connection thread survives to move a counter after this point.
    serving.store(false, Ordering::SeqCst);
    let _ = TcpStream::connect(addr); // unblock the accept
    let _ = accept.join();
    engine.shutdown();

    let errs = errors.lock().unwrap();
    if !errs.is_empty() {
        eprintln!("REFUSED: arm {arm}/{} reported {} SQL or IO errors, so the statement mix is \
                   not the one this arm names. First few: {:?}",
                  proto.label(), errs.len(), &errs[..errs.len().min(5)]);
        std::process::exit(4);
    }

    let mut composition = std::collections::BTreeMap::new();
    for r in &roles {
        *composition.entry(format!("{r:?}")).or_insert(0usize) += 1;
    }
    let composition = composition
        .iter()
        .map(|(k, v)| format!("{v}x{k}"))
        .collect::<Vec<_>>()
        .join("+");

    ArmResult {
        arm: arm.to_string(),
        proto,
        counts,
        statements: statements.load(Ordering::Relaxed),
        defining_statements: defining.load(Ordering::Relaxed),
        composition,
    }
}

/// Rounds for one arm.
///
/// `merge` gets its own, smaller budget and the reason is an ENGINE LIMIT, not a taste: a page's
/// provenance dictionary holds 255 entries, and every MERGE that lands on the same page consumes
/// one. At 400 rounds x 4 merging clients the arm ran 1600 merges into one page and the engine
/// correctly refused with "page provenance dictionary full", which cascaded into "an agent
/// session is already open on this connection" on every following round. That was a real refusal
/// of a workload the engine cannot serve, not a harness bug, and the honest fix is to run the arm
/// inside the limit rather than to widen it. `W4_MERGE_ROUNDS` overrides.
fn rounds_for(arm: &str, rounds: usize) -> usize {
    if arm == "merge" {
        env_usize("W4_MERGE_ROUNDS", 40).min(rounds)
    } else {
        rounds
    }
}

/// A fraction for machine-readable output: `n/a` rather than `0` when nothing was attempted.
fn opt(f: Option<f64>) -> String {
    match f {
        Some(v) => format!("{v:.6}"),
        None => "n/a".to_string(),
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn main() {
    // Refuse before a single statement runs, rather than after a whole run has produced zeros.
    if standdown::counts().is_none() {
        eprintln!(
            "REFUSED: this build is not instrumented. Run with \
             `--features w4-standdown-count`. `counts()` answering None is a DIFFERENT FACT from \
             an observed zero, and this harness will not collapse them."
        );
        std::process::exit(2);
    }

    let clients = env_usize("W4_CLIENTS", 8);
    let rounds = env_usize("W4_ROUNDS", 300);
    let lease_ms = env_usize("W4_LEASE_MS", 30_000) as u64;
    let arms: Vec<String> = std::env::var("W4_ARMS")
        .unwrap_or_else(|_| "readonly,agent,agent_dml,merge,ddl".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let dir = tempfile::tempdir().expect("tempdir");

    println!("# W4 check 3 -- stand-downs at ServerContext::begin_read, as a fraction of attempts");
    println!("# layer: through pgwire over a loopback TCP socket; shipped `pgwire::handle` per");
    println!("#        connection; nothing drives AgentRuntime or Session directly.");
    println!(
        "# clients={clients} rounds={rounds} lease_scan_ms={lease_ms} fork_every={} announcers={}",
        fork_every(),
        std::env::var("W4_ANNOUNCERS").unwrap_or_else(|_| "default".into())
    );
    println!("# a COUNT, not a timing: no duration is measured and none is reported.");
    println!();

    let mut results = Vec::new();
    for arm in &arms {
        for proto in PROTOS {
            let r = run_arm(arm, proto, clients, rounds_for(arm, rounds), lease_ms, dir.path());

            if r.counts.attempts() == 0 {
                eprintln!(
                    "REFUSED: arm {arm}/{} made ZERO shared-path attempts. A run that collected \
                     nothing has not passed.",
                    proto.label()
                );
                std::process::exit(3);
            }
            if let Some(want) = defining_role(arm) {
                if r.defining_statements == 0 {
                    eprintln!(
                        "REFUSED: arm {arm}/{} ran zero {want:?} statements, so it measured a \
                         calmer workload than the one it names.",
                        proto.label()
                    );
                    std::process::exit(5);
                }
            }
            // Emitted NOW, not only in the summary table below. The first full run of this
            // harness refused in the fourth arm and took nine completed arms down with it,
            // because every number was printed at the end. A partial result is worth keeping.
            let c = r.counts;
            println!(
                "ROW arm={} proto={} sh_att={} sh_stood={} sh_frac={} dml_att={} dml_frac={} \
                 exc_att={} exc_frac={} all_frac={} ann_parse={} ann_refr={} ann_exec={} \
                 ann_lease={} ann_unattr={} mismatch={} gap={} stmts={} defining={}",
                r.arm,
                r.proto.label(),
                c.attempts_shared_today(),
                c.stood_down_shared_today,
                opt(c.stand_down_fraction_shared_today()),
                c.attempts_dml(),
                opt(c.stand_down_fraction_dml()),
                c.attempts_exclusive(),
                opt(c.stand_down_fraction_exclusive()),
                opt(c.stand_down_fraction()),
                c.announce_parse,
                c.announce_read_refresh,
                c.announce_exec_exclusive,
                c.announce_lease_scan,
                c.announce_unattributed(),
                c.classify_mismatch,
                c.class_coverage_gap(),
                r.statements,
                r.defining_statements,
            );
            use std::io::Write as _;
            let _ = std::io::stdout().flush();
            results.push(r);
        }
    }

    // ⭐ THE HEADLINE TABLE. `shared_today` is the number check 3 turns on: statements the shared
    // path already serves, which is the position a DML would occupy under W4. The `all` column is
    // kept beside it only so the difference between the two is visible — it is NOT the answer,
    // because it counts fork and MERGE statements that are exclusive under W4 as well.
    let pct = |f: Option<f64>| match f {
        Some(v) => format!("{:.2}%", v * 100.0),
        // Not "0.00%": a class with no attempts measured nothing about that class.
        None => "  n/a".to_string(),
    };
    println!("## HEADLINE -- stand-down fraction by what the statement would be under W4");
    println!(
        "{:<10} {:<9} | {:>9} {:>9} {:>11} | {:>7} {:>9} | {:>7} {:>9} | {:>9}",
        "arm", "proto",
        "sh_att", "sh_stood", "SHARED_TODAY",
        "dml_att", "dml_stood",
        "exc_att", "exc_stood",
        "all_stood"
    );
    for r in &results {
        let c = r.counts;
        println!(
            "{:<10} {:<9} | {:>9} {:>9} {:>11} | {:>7} {:>9} | {:>7} {:>9} | {:>9}",
            r.arm,
            r.proto.label(),
            c.attempts_shared_today(),
            c.stood_down_shared_today,
            pct(c.stand_down_fraction_shared_today()),
            c.attempts_dml(),
            pct(c.stand_down_fraction_dml()),
            c.attempts_exclusive(),
            pct(c.stand_down_fraction_exclusive()),
            pct(c.stand_down_fraction()),
        );
    }

    println!();
    println!("## ANNOUNCERS -- which exclusive acquirer made a writer visible");
    println!(
        "{:<10} {:<9} {:>9} {:>9} {:>9} {:>10}  {:>9} {:>8} {:>8} {:>8} {:>7}",
        "arm", "proto", "attempts", "admitted", "stood_dn", "stand_down",
        "ann_parse", "ann_refr", "ann_exec", "ann_leas", "ann_unk"
    );
    for r in &results {
        let c = r.counts;
        let f = c.stand_down_fraction().expect("attempts > 0, checked above");
        println!(
            "{:<10} {:<9} {:>9} {:>9} {:>9} {:>9.2}%  {:>9} {:>8} {:>8} {:>8} {:>7}",
            r.arm,
            r.proto.label(),
            c.attempts(),
            c.admitted,
            c.stood_down,
            f * 100.0,
            c.announce_parse,
            c.announce_read_refresh,
            c.announce_exec_exclusive,
            c.announce_lease_scan,
            c.announce_unattributed(),
        );
    }

    println!();
    println!("# composition and statement counts");
    for r in &results {
        println!(
            "#   {:<10} {:<9} clients={:<28} statements={} defining={}",
            r.arm,
            r.proto.label(),
            r.composition,
            r.statements,
            r.defining_statements
        );
    }

    // The classifier is a SECOND predicate beside `try_run_read`, which the codebase deliberately
    // avoids having. It is allowed here only because it decides nothing and is checked: a
    // disagreement with the real dispatch means the split is mislabelled, and a mislabelled split
    // is worse than no split, so the run refuses rather than printing it.
    let mismatch: u64 = results.iter().map(|r| r.counts.classify_mismatch).sum();
    if mismatch > 0 {
        eprintln!(
            "REFUSED: `standdown::classify` disagreed with `try_run_read` on {mismatch} admitted \
             statements. The class split above is mislabelled and must not be read."
        );
        std::process::exit(6);
    }
    let gap: i128 = results.iter().map(|r| r.counts.class_coverage_gap()).sum();
    if gap != 0 {
        eprintln!(
            "REFUSED: the class counters cover {gap} fewer attempts than `begin_read` recorded, \
             so something called begin_read outside the pgwire statement path and the split does \
             not describe the whole population."
        );
        std::process::exit(7);
    }

    let unattributed: u64 = results.iter().map(|r| r.counts.announce_unattributed()).sum();
    if unattributed > 0 {
        println!();
        println!(
            "# ⚠ {unattributed} exclusive acquisitions were not attributed to a known site. The \
             stand-down fraction is measured at `begin_read` and does not depend on the \
             breakdown, but the per-site columns are INCOMPLETE and must be labelled so."
        );
    }
}
