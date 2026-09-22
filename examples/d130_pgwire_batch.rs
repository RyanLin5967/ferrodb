//! D130 — is the group-commit BATCH set by the THREAD COUNT? The pgwire layer.
//!
//! ```text
//! cargo run --release --example d130_pgwire_batch -- [forks_per_thread] [T,T,T]
//! ```
//!
//! # The question, and why it needs its own harness
//!
//! `src/branch/group_commit.rs` is single-stage leader/follower commit-when-ready: the leader reads
//! `let target = st.requested` at the instant it starts its fsync, so a batch is exactly whatever
//! had taken a ticket when the leader looked. D130 pre-registers a sweep over the thread count `T`
//! at a FIXED rung and reads `f/sync ÷ T`:
//!
//!   1. `f/sync ÷ T` ≈ 0.5, FLAT in T   ⇒ a wake-up race; the batch is pinned at half the threads.
//!   2. `f/sync ÷ T` FALLS as T rises   ⇒ the batch is bounded by something that is NOT the thread
//!                                        count — arrival, or a serial section that admits only
//!                                        `D/S` forks per fsync.
//!   3. `f/sync ÷ T` ≈ 1.0 at some T    ⇒ the batch already captures everyone.
//!
//! `examples/fork_concurrency.rs` answers that at the DIRECT layer (`TableBranchCatalog::fork`,
//! holding `logical` and nothing else). This file answers it at the layer production runs, because
//! Amendment 1 to the ceiling pre-registration binds every number to its layer and E.6 measured
//! that these are **not the same experiment**: through pgwire the locks held are
//! `ServerContext::catalog()` → `AgentRuntime`'s `Mutex<State>` → `TableBranchCatalog::logical`,
//! outermost first, and the fork statement is not admitted to `try_run_read`'s shared path.
//!
//! ⚠ **BAND, 2026-09-22 (D159). The paragraph above describes the tree it was written against and
//! two of its clauses have since stopped being true. It is banded rather than repaired, so a reader
//! of the banked `bench/d130_batch_vs_threads.txt` can still see which tree that run measured.**
//!
//! * **The nesting is wrong even for the tree it was written against.** `AgentRuntime`'s
//!   `Mutex<State>` is never held while `TableBranchCatalog::logical` is taken — `begin_session_as`
//!   forks *first* (`runtime.rs:1475-1477`) and locks `state` *after* (`:1512`), so they are
//!   siblings under the catalog mutex, not nested. `runtime.rs:1095-1100` states the runtime's one
//!   lock order and it is the opposite of the chain above: the catalog lock *"must not be reached
//!   while `state` is held"*.
//! * **The fsync is no longer under `ServerContext::catalog()`.** D159 split the fork into stage +
//!   durable (`BranchCatalog::fork_staged` / `await_fork_durable`): pgwire drops the catalog guard
//!   and only then awaits the sync, so forkers can meet inside `CommitGroup::wait_durable`. The
//!   fork statement is still **not** admitted to `try_run_read`'s shared path — that clause stands —
//!   and everything except the sync still runs under the catalog guard, deliberately.
//! * ⚠ **What this does NOT license.** D159 amendment 4 predicts `f/sync` stays near 1.00 on this
//!   harness anyway, because this harness gives every fork a **unique run id** (`r<thread>_<iter>`,
//!   see `run_ids` below) and so pays `prov_store.intern`'s own fsync per fork, under `state`,
//!   inside the catalog guard. **That is a second serialiser this change does not touch.** A flat
//!   arm P here is therefore the predicted reading, not evidence that the split did nothing — the
//!   split's own evidence is `tests/d159_fork_sync_is_deferred.rs`, which is deterministic.
//!
//! # The instrument
//!
//! `TableBranchCatalog::syncs_issued()` is a counter on the server's own catalog, read from inside
//! this process before and after the client phase. `f/sync` is therefore a ratio of two counters
//! taken by one instrument over one run, which is what makes it usable on a loaded box. **No
//! timing is reported and none should be added**: the ladder's own header already labels
//! statements/sec an upper bound, each fork here is a TCP round trip, and a throughput number from
//! this harness would be a fact about the socket.
//!
//! # Two arms, because the connection shape is a confound
//!
//! * **P — conn per fork.** One TCP connection, one `BEGIN AGENT SESSION`, disconnect. This is the
//!   clean ratio: the only agent statement on the wire is the fork, so `syncs` cannot contain work
//!   that is not a fork. Its cost is that a thread spends time connecting rather than queueing, so
//!   fewer threads are simultaneously inside `wait_durable` than `T` suggests. **That is a reason
//!   this arm could report a small batch for an instrument reason, which is why arm S exists.**
//! * **S — persistent conn.** One connection per thread, `BEGIN AGENT SESSION` then `ABANDON`,
//!   repeatedly. A thread returns to the fork immediately, so the arrival rate is not capped by
//!   connection setup. Its cost is the mirror image: `ABANDON` retires a branch through the same
//!   catalog, so this arm's `syncs` includes syncs that are **not** forks and its `f/sync` is a
//!   LOWER bound on the fork batch. The two arms bracket the confound; neither alone does.
//!
//! No `LeaseThread`. In production the lease scan takes the same outermost mutex and would issue
//! catalog work of its own on a timer, landing syncs in a ratio that is supposed to be
//! attributable to client statements. Leaving it out can only make the batch look LARGER, which is
//! the direction that would falsify outcome 2 — the safe direction for the reading being tested.
//!
//! # What it refuses to report
//!
//! - A cell where the server named a different number of distinct branches than the clients asked
//!   for. The loop counter is this harness's bookkeeping; the branch names are the server's.
//! - A cell where the sync counter did not move. A counter wired to nothing prints a clean-looking
//!   ratio, and `f/sync` with a zero denominator is not a small batch, it is no measurement.
//! - Any SQL error from any statement. A refused `BEGIN AGENT SESSION` is a fork that did not
//!   happen, and counting the arm around it reports a mediated path as an unmediated one.
//!
//! Every refusal exits non-zero, so a run that could not see its subject cannot be read as a run
//! that saw nothing wrong.

use std::collections::BTreeSet;
use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper, TableBranchCatalog};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::pgwire::{serve, ServerContext};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;
const PROTOCOL_V3: i32 = 196_608;

// ---------------------------------------------------------------------------------------------
// a minimal protocol-v3 frontend
// ---------------------------------------------------------------------------------------------
//
// Hand-rolled because this repo has zero runtime dependencies and adding a driver to answer a
// counting question would be the larger change. It is a real client on a real socket: startup
// packet, simple `Query`, replies read to `ReadyForQuery`. Nothing here reaches into the server's
// internals, which is the entire point — a harness that drove `AgentRuntime` directly would be
// measuring the DIRECT layer again under a pgwire label, which is the failure E.6 exists to name.

struct Client {
    w: TcpStream,
    r: BufReader<TcpStream>,
}

/// What one round trip returned: every text field of every `DataRow`, and the first error.
struct Reply {
    texts: Vec<String>,
    error: Option<String>,
}

impl Client {
    fn connect(addr: std::net::SocketAddr) -> std::io::Result<Client> {
        let w = TcpStream::connect(addr)?;
        w.set_nodelay(true)?;
        let r = BufReader::new(w.try_clone()?);
        let mut c = Client { w, r };
        let mut body = Vec::new();
        body.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
        for (k, v) in [("user", "d130"), ("database", "d130")] {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        c.w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
        c.w.write_all(&body)?;
        c.w.flush()?;
        let hello = c.read_to_ready()?;
        if let Some(e) = hello.error {
            return Err(std::io::Error::other(format!("startup refused: {e}")));
        }
        Ok(c)
    }

    fn query(&mut self, sql: &str) -> std::io::Result<Reply> {
        let mut body = Vec::with_capacity(sql.len() + 1);
        body.extend_from_slice(sql.as_bytes());
        body.push(0);
        self.w.write_all(b"Q")?;
        self.w.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
        self.w.write_all(&body)?;
        self.w.flush()?;
        self.read_to_ready()
    }

    /// `Terminate`. Sent explicitly so the server's connection thread returns before the counter is
    /// read, rather than being reaped by a dropped socket at an unknown moment.
    fn terminate(&mut self) -> std::io::Result<()> {
        self.w.write_all(b"X")?;
        self.w.write_all(&4i32.to_be_bytes())?;
        self.w.flush()
    }

    fn read_to_ready(&mut self) -> std::io::Result<Reply> {
        let mut out = Reply { texts: Vec::new(), error: None };
        loop {
            let mut tag = [0u8; 1];
            self.r.read_exact(&mut tag)?;
            let mut lenb = [0u8; 4];
            self.r.read_exact(&mut lenb)?;
            let len = i32::from_be_bytes(lenb);
            if len < 4 {
                return Err(std::io::Error::other(format!("backend message length {len}")));
            }
            let mut body = vec![0u8; (len - 4) as usize];
            self.r.read_exact(&mut body)?;
            match tag[0] {
                b'Z' => return Ok(out),
                b'E' => {
                    if out.error.is_none() {
                        out.error = Some(decode_error(&body));
                    }
                }
                b'D' => decode_row(&body, &mut out.texts),
                _ => {}
            }
        }
    }
}

/// `ErrorResponse` is (byte code, C string)* terminated by a zero byte; `M` is the message.
fn decode_error(body: &[u8]) -> String {
    let mut i = 0usize;
    let mut msg = String::new();
    while i < body.len() && body[i] != 0 {
        let code = body[i];
        i += 1;
        let start = i;
        while i < body.len() && body[i] != 0 {
            i += 1;
        }
        let text = String::from_utf8_lossy(&body[start..i]).to_string();
        if code == b'M' {
            msg = text;
        }
        i += 1;
    }
    msg
}

/// `DataRow` is a field count then (i32 length, bytes) per field; `-1` is NULL.
fn decode_row(body: &[u8], out: &mut Vec<String>) {
    if body.len() < 2 {
        return;
    }
    let fields = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut i = 2usize;
    for _ in 0..fields {
        if i + 4 > body.len() {
            return;
        }
        let n = i32::from_be_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
        i += 4;
        if n < 0 {
            out.push(String::new());
            continue;
        }
        let n = n as usize;
        if i + n > body.len() {
            return;
        }
        out.push(String::from_utf8_lossy(&body[i..i + n]).to_string());
        i += n;
    }
}

/// The branch identities a reply announced, as the SERVER named them.
///
/// Half of the independent witness that a fork happened. Matching `b_<digits>` over every text
/// field rather than indexing one column keeps it from silently reading the wrong field if
/// `session_started_columns()` is ever reordered — a wrong column returns no matches, which fails
/// loudly, instead of returning a plausible string.
///
/// ⛔ **A BRANCH NAME IS NOT UNIQUE ACROSS A RUN THAT ABANDONS**, which is why this is only half.
/// `TableBranchCatalog::fork` recycles a retired slot when one is free (`table_catalog.rs`, "Recycle
/// a retired slot if one is free, otherwise mint a new one"); the identity that survives is the
/// (id, generation) pair, and `b_<id>` drops the generation. Counting DISTINCT names as the fork
/// total therefore under-counts arm S by the recycling factor — it reported 2 for 8 forks at T=2,
/// which is what the proof pass refused on. The run id below is the half that stays unique.
fn branch_names(reply: &Reply) -> Vec<String> {
    reply
        .texts
        .iter()
        .filter(|s| {
            s.strip_prefix("b_").is_some_and(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))
        })
        .cloned()
        .collect()
}

/// The run ids a reply echoed back, as the SERVER recorded them.
///
/// Every `BEGIN AGENT SESSION` in this harness carries a run id unique across the whole cell
/// (`r<thread>_<iteration>`), and a `SessionStarted` row exists only for a statement that
/// succeeded. So the count of DISTINCT run ids in the replies is a server-side statement that this
/// many distinct fork statements ran — and unlike the branch name it cannot be deflated by slot
/// recycling, because the catalog never invents a run id.
fn run_ids(reply: &Reply) -> Vec<String> {
    reply
        .texts
        .iter()
        .filter(|s| {
            s.strip_prefix('r').and_then(|t| t.split_once('_')).is_some_and(|(a, b)| {
                !a.is_empty()
                    && !b.is_empty()
                    && a.bytes().all(|c| c.is_ascii_digit())
                    && b.bytes().all(|c| c.is_ascii_digit())
            })
        })
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------------------------
// the server-side rig
// ---------------------------------------------------------------------------------------------

struct Rig {
    branches: Arc<TableBranchCatalog>,
    addr: std::net::SocketAddr,
    _dir: PathBuf,
}

/// Build one pgwire server, the shape `examples/pgserver.rs` ships, and start serving.
///
/// The `Arc<TableBranchCatalog>` is kept because `syncs_issued()` is an inherent method on the
/// concrete catalog and not on the `BranchCatalog` trait the runtime holds — the server has no SQL
/// surface that exposes the counter, so the only way to read it is to be the process that built it.
fn rig(root: &Path, tag: &str) -> Rig {
    let dir = root.join(format!("d130-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("rig dir");
    let db = dir.join("ferro.db").to_string_lossy().into_owned();

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&db)
        .expect("open db");
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));
    let wal = Arc::new(WalManager::new(format!("{db}.wal").into()).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let catalog = Catalog::create(bp.clone()).unwrap();

    let branches: Arc<TableBranchCatalog> =
        Arc::new(TableBranchCatalog::default_for_database(&db, FIRST_CATALOG_PAGE_ID).unwrap());
    let base = bp.disk_manager.high_water().expect("high water") + 32_736;
    let store: Arc<ArenaPageStore> = Arc::new(
        ArenaPageStore::new(bp.clone(), branches.clone() as Arc<dyn BranchCatalog>, base)
            .expect("arena"),
    );
    // The reaper is part of the shipped shape (`pgserver.rs`): without it every `ABANDON` marks a
    // branch `Reaped` and leaks its pages. Arm S is the one that abandons, and its ratio is already
    // declared a lower bound for exactly this reason.
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
        .with_reaper(reaper as Arc<dyn Reaper>),
    );

    let ctx = Arc::new(ServerContext::new(catalog, bp, txn, runtime));
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    // Detached: the cell ends when its clients have been joined, and a server parked in
    // `incoming()` with nobody connected issues no syncs.
    std::thread::spawn(move || {
        let _ = serve(listener, ctx);
    });
    Rig { branches, addr, _dir: dir }
}

// ---------------------------------------------------------------------------------------------
// arms
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    /// One connection per fork; a single `BEGIN AGENT SESSION` on each, then `Terminate`.
    PerFork,
    /// One connection per thread; `BEGIN AGENT SESSION` then `ABANDON`, repeatedly.
    Persistent,
}

impl Arm {
    fn tag(self) -> &'static str {
        match self {
            Arm::PerFork => "P",
            Arm::Persistent => "S",
        }
    }
    fn label(self) -> &'static str {
        match self {
            Arm::PerFork => "P  conn per fork, BEGIN only",
            Arm::Persistent => "S  persistent conn, BEGIN+ABANDON",
        }
    }
}

/// ⭐ THE POSITIVE CONTROL. `T` threads calling `fork` on the rig's OWN catalog, no socket.
///
/// **Without this the pgwire result is an unfalsified negative.** Arms P and S both report a batch
/// of 1.00 and 0.20, flat in `T`; read alone, that is indistinguishable from a harness that could
/// never have reported anything else — a counter read at the wrong moment, a thread pool that
/// serialises the clients, a rig whose catalog is not the one the server uses. This arm reads the
/// SAME `syncs_issued()` on the SAME `Arc<TableBranchCatalog>` the running server was built from,
/// in the same process, and differs only in that it does not go through pgwire. If it shows a
/// batch while P and S do not, the instrument can see batching and the wire layer has none.
///
/// A pgwire server IS listening throughout. It contributes nothing because nobody connects to it,
/// which is a fact this arm demonstrates rather than assumes.
fn run_direct_control(t: usize, f: usize, root: &Path) -> Cell {
    let rig = rig(root, &format!("D-t{t}"));
    let before = rig.branches.syncs_issued();

    let lease = LeaseDeadline(u64::MAX);
    let mut ids: BTreeSet<u64> = BTreeSet::new();
    std::thread::scope(|s| {
        let mut hs = Vec::with_capacity(t);
        for _ in 0..t {
            let cat = Arc::clone(&rig.branches);
            hs.push(s.spawn(move || {
                let mut mine = Vec::with_capacity(f);
                for _ in 0..f {
                    mine.push(cat.fork(BranchId::TRUNK, lease).expect("fork").branch_id.id);
                }
                mine
            }));
        }
        for h in hs {
            ids.extend(h.join().expect("control thread"));
        }
    });

    let after = rig.branches.syncs_issued();
    // Nothing is abandoned here either, so no slot is retired and every fork must mint a fresh id.
    if ids.len() != t * f {
        eprintln!(
            "d130: REFUSING. D  direct control at T={t}: asked for {} forks, the catalog minted {} \
             distinct branch ids.",
            t * f,
            ids.len()
        );
        std::process::exit(1);
    }
    if after <= before {
        eprintln!(
            "d130: REFUSING. D  direct control at T={t}: the catalog issued 0 fsyncs across {} \
             forks.",
            t * f
        );
        std::process::exit(1);
    }
    Cell { forks: ids.len(), syncs: after - before }
}

struct Cell {
    forks: usize,
    syncs: u64,
}

fn refuse_on_error(r: &Reply, what: &str) {
    if let Some(e) = &r.error {
        eprintln!(
            "d130: REFUSING. `{what}` was rejected by the server: {e}\n     A refused statement is \
             work that did not happen; counting the arm around it would report a batch this run \
             never formed."
        );
        std::process::exit(1);
    }
}

/// Run one cell: `t` client threads, `f` forks each, on a freshly built server.
fn run_cell(arm: Arm, t: usize, f: usize, root: &Path) -> Cell {
    let rig = rig(root, &format!("{}-t{t}", arm.tag()));

    // Snapshot AFTER the rig is built. `Catalog::create`, the arena and the runtime's construction
    // are fixture cost; charging their syncs to the clients would inflate `f/sync`'s denominator
    // and push the answer toward outcome 2 for a reason that has nothing to do with batching.
    let before = rig.branches.syncs_issued();

    let mut reported: BTreeSet<String> = BTreeSet::new();
    let mut runs: BTreeSet<String> = BTreeSet::new();
    let mut named = 0usize;
    let mut asked = 0usize;
    let addr = rig.addr;
    std::thread::scope(|s| {
        let mut hs = Vec::with_capacity(t);
        for th in 0..t {
            hs.push(s.spawn(move || {
                let mut names: Vec<String> = Vec::new();
                let mut mine_runs: Vec<String> = Vec::new();
                let mut named = 0usize;
                let mut asked = 0usize;
                let mut persistent = match arm {
                    Arm::Persistent => Some(Client::connect(addr).expect("connect")),
                    Arm::PerFork => None,
                };
                for i in 0..f {
                    let sql = format!("BEGIN AGENT SESSION AS 'a{th}' RUN 'r{th}_{i}';");
                    match arm {
                        Arm::PerFork => {
                            let mut c = Client::connect(addr).expect("connect");
                            let r = c.query(&sql).expect("BEGIN AGENT SESSION round trip");
                            refuse_on_error(&r, "BEGIN AGENT SESSION");
                            let b = branch_names(&r);
                            named += b.len();
                            names.extend(b);
                            mine_runs.extend(run_ids(&r));
                            asked += 1;
                            let _ = c.terminate();
                        }
                        Arm::Persistent => {
                            let c = persistent.as_mut().expect("persistent client");
                            let r = c.query(&sql).expect("BEGIN AGENT SESSION round trip");
                            refuse_on_error(&r, "BEGIN AGENT SESSION");
                            let b = branch_names(&r);
                            named += b.len();
                            names.extend(b);
                            mine_runs.extend(run_ids(&r));
                            asked += 1;
                            let r = c.query("ABANDON;").expect("ABANDON round trip");
                            refuse_on_error(&r, "ABANDON");
                        }
                    }
                }
                if let Some(mut c) = persistent {
                    let _ = c.terminate();
                }
                (names, mine_runs, named, asked)
            }));
        }
        for h in hs {
            let (n, r, nm, a) = h.join().expect("client thread");
            reported.extend(n);
            runs.extend(r);
            named += nm;
            asked += a;
        }
    });

    let after = rig.branches.syncs_issued();

    // THE WITNESS. `asked` is this harness's own loop counter and is not evidence of anything;
    // these three are the server's.
    //
    // (a) every reply carried exactly one branch name, so every statement minted a branch;
    // (b) the distinct run ids equal the statements issued, so no statement was silently skipped
    //     or double-counted — and a run id, unlike a branch name, cannot be deflated by slot
    //     recycling;
    // (c) for arm P only, the distinct branch names also equal the forks. Nothing is abandoned in
    //     that arm, so no slot is ever retired and none can be recycled; the stronger check is
    //     therefore valid there and is kept. It is NOT valid for arm S, and was the proof pass's
    //     refusal.
    if named != asked {
        eprintln!(
            "d130: REFUSING. {} at T={t}: {asked} statements returned {named} branch names. A \
             `SessionStarted` row without a branch is not a fork.",
            arm.label()
        );
        std::process::exit(1);
    }
    if runs.len() != asked {
        eprintln!(
            "d130: REFUSING. {} at T={t}: asked for {asked} forks but the server echoed {} distinct \
             run ids. The fork total cannot be taken from this harness's own counter when the two \
             witnesses disagree.",
            arm.label(),
            runs.len()
        );
        std::process::exit(1);
    }
    if arm == Arm::PerFork && reported.len() != asked {
        eprintln!(
            "d130: REFUSING. {} at T={t}: {asked} forks but only {} distinct branch names. Nothing \
             is abandoned in this arm, so no retired slot exists to recycle and every fork must \
             have minted a fresh id.",
            arm.label(),
            reported.len()
        );
        std::process::exit(1);
    }
    if after <= before {
        eprintln!(
            "d130: REFUSING. {} at T={t}: the catalog issued {} fsyncs across {asked} forks. A \
             denominator of zero is not a large batch, it is an instrument that did not move.",
            arm.label(),
            after - before
        );
        std::process::exit(1);
    }

    // `runs.len()`, not `reported.len()`: the run ids are the witness that survives slot recycling,
    // and in arm S the distinct branch names are FEWER than the forks by construction.
    Cell { forks: runs.len(), syncs: after - before }
}

// ---------------------------------------------------------------------------------------------
// forcing the instrument to fire, and to stay silent
// ---------------------------------------------------------------------------------------------

/// The counter must move when a fork happens and must NOT move when nothing does.
///
/// Without the first half the whole table is unfalsifiable: a counter wired to nothing prints the
/// same clean ratio a real one would. Without the second half a counter that ticks on something
/// else — a background flush, a timer — would inflate every denominator and manufacture outcome 2.
fn self_check(root: &Path) {
    let rig = rig(root, "selfcheck");

    let quiet_a = rig.branches.syncs_issued();
    let quiet_b = rig.branches.syncs_issued();
    if quiet_b != quiet_a {
        eprintln!(
            "d130: REFUSING. the sync counter moved by {} with no work between two reads. \
             Something other than a commit is ticking it, so every denominator below is inflated.",
            quiet_b - quiet_a
        );
        std::process::exit(1);
    }

    let before = rig.branches.syncs_issued();
    rig.branches
        .fork(BranchId::TRUNK, LeaseDeadline(u64::MAX))
        .expect("self-check fork");
    let after = rig.branches.syncs_issued();
    if after != before + 1 {
        eprintln!(
            "d130: REFUSING. one serial fork moved the sync counter by {}, not 1. The instrument \
             does not count what it claims to count, so no cell below means anything.",
            after - before
        );
        std::process::exit(1);
    }
    println!(
        "instrument self-check: two idle reads moved the counter by 0, and one serial fork moved \
         it by exactly 1. OK"
    );
}

// ---------------------------------------------------------------------------------------------

fn main() {
    let f: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(40);
    let threads: Vec<usize> = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "1,2,4,8,16,32,64,128".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .filter(|&t: &usize| t > 0)
        .collect();
    if threads.is_empty() || f == 0 {
        eprintln!("d130: REFUSING. an empty thread list or zero forks per thread collects nothing.");
        std::process::exit(1);
    }

    let root = std::env::temp_dir().join(format!("ferrodb-d130-{}", std::process::id()));
    std::fs::create_dir_all(&root).expect("root");

    println!("D130 — IS THE GROUP-COMMIT BATCH SET BY THE THREAD COUNT? MODE=pgwire.");
    println!();
    println!("⚠ LAYER: the shipped pgwire server over a real TCP socket, one client connection per");
    println!("  thread or per fork, forks issued as `BEGIN AGENT SESSION`. Locks held, outermost");
    println!("  first: `ServerContext::catalog()` -> `AgentRuntime` state -> `TableBranchCatalog`'s");
    println!("  `logical`. E.6 measured that this is NOT the same experiment as MODE=direct, which");
    println!("  is `examples/fork_concurrency.rs`. Amendment 1 binds each number to its layer.");
    println!();
    println!("  f/sync   = forks the server named, divided by fsyncs the catalog issued. A ratio of");
    println!("             two counters taken by one instrument over one run, so it is load-immune.");
    println!("  f/sync÷T = the pre-registered discriminator. ≈0.5 flat ⇒ outcome 1 (wake-up race);");
    println!("             falling in T ⇒ outcome 2 (the batch is not bounded by the threads);");
    println!("             ≈1.0 ⇒ outcome 3 (the batch already captures everyone).");
    println!();
    println!("⛔ NO THROUGHPUT IS REPORTED. Each fork is a TCP round trip and the box is shared;");
    println!("   a rate from this harness would be a fact about the socket. The counts are the");
    println!("   evidence.");
    println!();
    println!("# start: {}  load: {}", stamp(), loadavg());
    println!();

    self_check(&root);
    println!();

    println!("  arm                                threads    forks    syncs    f/sync   f/sync÷T");
    let row = |label: &str, t: usize, c: &Cell| {
        let per = c.forks as f64 / c.syncs as f64;
        println!(
            "  {:32}  {:5}   {:6}   {:6}   {:7.2}   {:8.4}",
            label, t, c.forks, c.syncs, per, per / t as f64
        );
    };
    for arm in [Arm::PerFork, Arm::Persistent] {
        for &t in &threads {
            row(arm.label(), t, &run_cell(arm, t, f, &root));
        }
        println!();
    }
    for &t in &threads {
        row("D  POSITIVE CONTROL, no socket", t, &run_direct_control(t, f, &root));
    }
    println!();
    println!("⭐ ARM D IS THE FALSIFIER FOR ARMS P AND S. Same counter, same Arc<TableBranchCatalog>,");
    println!("   same process, a pgwire server listening throughout — it simply does not go through");
    println!("   the socket. If D shows a batch and P/S do not, the instrument can see batching and");
    println!("   the wire layer has none. If D is ALSO flat at 1.00, then nothing above is a fact");
    println!("   about pgwire and every cell in this run is a fact about this harness.");

    println!("# end: {}  load: {}", stamp(), loadavg());
    let _ = std::fs::remove_dir_all(&root);
}

fn stamp() -> String {
    std::process::Command::new("date")
        .arg("+%Y-%m-%dT%H:%M:%SZ")
        .env("TZ", "UTC")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

/// The box is shared. `f/sync` is load-immune by construction, but a reader deserves to see what
/// the machine was doing rather than take that on trust.
fn loadavg() -> String {
    std::process::Command::new("uptime")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.split("load averages:").nth(1).map(|t| t.trim().to_string()))
        .unwrap_or_else(|| "unknown".into())
}
