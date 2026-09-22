//! E.6 — is every production fork mediated by the outer statement mutex? A COUNT, not a timing.
//!
//! ```text
//! cargo run --release --features lock_census --example e6_outer_lock_count -- [F] [T,T,T,T]
//! ```
//!
//! # WHICH LAYER EACH ARM DRIVES, AND WHICH LOCKS THAT LAYER HOLDS IN PRODUCTION
//!
//! This section is not decoration. E.6 exists because `examples/fork_concurrency.rs` — the
//! harness behind the 21x/1.9x fork-concurrency ceiling — states its N and its object and never
//! states its LAYER, and the layer is the whole disagreement. Every arm below therefore names
//! both, and a reader who disbelieves the headline can check the claim against the body.
//!
//! | arm | layer driven | locks that layer holds IN PRODUCTION |
//! |-----|--------------|--------------------------------------|
//! | A1  | the shipped pgwire server, over a real TCP socket, one client connection per thread, forks issued as SQL | `ServerContext::catalog()` — the outermost per-statement mutex (`src/pgwire/mod.rs`, `serve`'s lock-ordering note: *"taken outermost, for the duration of a single statement … Nothing beneath it ever takes it back"*) → `AgentRuntime`'s `Mutex<State>` → `TableBranchCatalog`'s `logical` |
//! | A2  | the same server; one connection per fork, a single `BEGIN AGENT SESSION` on each | identical to A1 |
//! | A0  | the same server, same client shape as A2, `SELECT 1;` in place of the fork — **the control** | identical to A1; the statement is a read, so it may take the shared read path instead |
//! | B   | `TableBranchCatalog` directly, as `examples/fork_concurrency.rs` does at `:26`, `:32`, `:101` | `logical` ONLY. No `ServerContext`, no statement mutex, no connection. **No shipped front-end reaches `fork` this way**; the configuration that does is an embedded `AgentRuntime` caller. |
//!
//! Arms A1/A2/A0 build the branch catalog with `TableBranchCatalog::default_for_database`, the
//! shape `examples/pgserver.rs` ships; arm B uses `open_sidecar`, the shape `fork_concurrency.rs`
//! uses today. Both are the same type and the same `fork`. The difference is stated rather than
//! smoothed over, because it is not the difference the result turns on: arm B cannot reach the
//! counted lock under either constructor.
//!
//! # THE INSTRUMENT
//!
//! A process-global `AtomicU64` incremented inside `ServerContext::catalog()` itself — so the
//! census is complete by construction rather than by having found every call site — read as a
//! DELTA across each arm. Compiled in only under `--features lock_census`; an uninstrumented run
//! reads `None` and this harness exits non-zero rather than printing a zero it cannot justify.
//!
//! **Counts, not durations.** An integer is load-immune: this does not need a quiet box and does
//! not need the fleet's measure-lock. No number in this harness's output is a time, and that is
//! deliberate — a millisecond here would be a claim this experiment has not earned.
//!
//! # FORCING THE DETECTOR TO FIRE
//!
//! A zero from arm B is worthless on its own: a counter that is never incremented reads exactly
//! like a lock that is never taken. Three things make the zero mean something.
//!
//! 1. `self_check` asserts that one explicit `ctx.catalog()` moves the counter by exactly 1.
//! 2. Arms A and B run in the SAME PROCESS against the SAME static. A's non-zero is the proof
//!    that the instrument was live at the moment B read zero.
//! 3. B's arm runs with a pgwire server built and listening. It reads zero because nothing
//!    connects to it, not because there is nothing to count.
//!
//! # WHAT IT REFUSES
//!
//! - An uninstrumented build (`catalog_acquisitions() == None`): exit 1. A run that collected
//!   nothing has not passed.
//! - Any client-side SQL error: exit 1. A refused `BEGIN AGENT SESSION` is a fork that did not
//!   happen, and averaging it in would report a mediated path as an unmediated one.
//! - A disagreement between the loop counter and the branch identities the SERVER reported:
//!   exit 1. The fork total must not come from this harness's own bookkeeping alone.

use std::collections::BTreeSet;
use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, TableBranchCatalog};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::pgwire::{catalog_acquisitions, serve, ServerContext};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;
const PROTOCOL_V3: i32 = 196_608;

// ---------------------------------------------------------------------------------------------
// the instrument
// ---------------------------------------------------------------------------------------------

/// Exclusive acquisitions so far, or refuse: `None` means this binary was built without
/// `--features lock_census` and has measured nothing at all.
fn acquisitions() -> u64 {
    match catalog_acquisitions() {
        Some(n) => n,
        None => {
            eprintln!(
                "e6: REFUSING. `pgwire::catalog_acquisitions()` is None, so this binary was built \
                 without `--features lock_census` and the census is not compiled in. That is not \
                 zero acquisitions; it is no measurement. Re-run with:\n  \
                 cargo run --release --features lock_census --example e6_outer_lock_count"
            );
            std::process::exit(1);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// a minimal protocol-v3 frontend
// ---------------------------------------------------------------------------------------------
//
// Hand-rolled because this repo has zero runtime dependencies and adding a driver to answer a
// counting question would be the larger change. It is a real client on a real socket: startup
// packet, simple `Query`, replies read to `ReadyForQuery`. Nothing here reaches into the server's
// internals, which is the entire point — arm A must drive the shipped path or it is not arm A.

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
        for (k, v) in [("user", "e6"), ("database", "e6")] {
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

    /// `Terminate`. Sent explicitly so the server's connection thread returns before this arm's
    /// counter is read, rather than being reaped by a dropped socket at an unknown moment.
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
        let val = String::from_utf8_lossy(&body[start..i]).to_string();
        i += 1;
        if code == b'M' {
            msg = val;
        }
    }
    if msg.is_empty() { "<error with no message field>".into() } else { msg }
}

/// `DataRow`: i16 field count, then per field an i32 length (-1 = NULL) and that many bytes.
fn decode_row(body: &[u8], out: &mut Vec<String>) {
    if body.len() < 2 {
        return;
    }
    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut i = 2usize;
    for _ in 0..count {
        if i + 4 > body.len() {
            return;
        }
        let n = i32::from_be_bytes([body[i], body[i + 1], body[i + 2], body[i + 3]]);
        i += 4;
        if n < 0 {
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
/// This is the independent witness that a fork happened. The loop counter in each arm says how
/// many forks were *asked for*; this says how many distinct branches the server *reported*, and
/// the two are checked against each other before anything is printed. Matching `b_<digits>` over
/// every text field rather than indexing one column keeps it from silently reading the wrong
/// field if `session_started_columns()` is ever reordered — a wrong column would return no
/// matches, which fails loudly, instead of returning a plausible string.
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

// ---------------------------------------------------------------------------------------------
// the server-side rig (arms A0/A1/A2)
// ---------------------------------------------------------------------------------------------

struct Rig {
    ctx: Arc<ServerContext>,
    addr: std::net::SocketAddr,
    _dir: PathBuf,
}

/// Build one pgwire server, the shape `examples/pgserver.rs` ships, and start serving.
///
/// **No `LeaseThread`.** In production the lease scan takes this same outermost mutex once per
/// interval, and every acquisition it made would land in a census that is supposed to be
/// attributable to client statements. Leaving it out makes the count a LOWER bound on what
/// production takes, which is the safe direction for the claim being tested.
fn rig(root: &Path, tag: &str) -> Rig {
    let dir = root.join(format!("e6-{tag}-{}", std::process::id()));
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
    let store: Arc<ArenaPageStore> =
        Arc::new(ArenaPageStore::new(bp.clone(), branches.clone() as Arc<dyn BranchCatalog>, base).unwrap());
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches.clone() as Arc<dyn BranchCatalog>,
            Arc::new(MemEffectLog::new()),
            store.clone() as Arc<dyn PageStore>,
        )
        .expect("storage-backed runtime"),
    );

    let ctx = Arc::new(ServerContext::new(catalog, bp, txn, runtime));
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let serving = Arc::clone(&ctx);
    // Detached: the arm ends when its clients have been joined, and a server parked in
    // `incoming()` with nobody connected cannot touch the counted lock.
    std::thread::spawn(move || {
        let _ = serve(listener, serving);
    });
    Rig { ctx, addr, _dir: dir }
}

// ---------------------------------------------------------------------------------------------
// arms
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum ArmA {
    /// One connection per thread; `BEGIN AGENT SESSION` then `ABANDON` per fork.
    Persistent,
    /// One connection per fork; a single `BEGIN AGENT SESSION` on each.
    PerFork,
    /// The control: identical to `PerFork` but `SELECT 1;` in place of the fork.
    Control,
}

impl ArmA {
    fn label(self) -> &'static str {
        match self {
            ArmA::Persistent => "A1 pgwire, persistent conn",
            ArmA::PerFork => "A2 pgwire, conn per fork",
            ArmA::Control => "A0 CONTROL, SELECT 1",
        }
    }
}

struct Cell {
    forks: usize,
    acquisitions: u64,
}

/// Run one arm-A cell: `t` client threads, `f` statements each, on a freshly built server.
fn run_arm_a(arm: ArmA, t: usize, f: usize, root: &Path) -> Cell {
    let rig = rig(root, &format!("{}-t{t}", match arm {
        ArmA::Persistent => "a1",
        ArmA::PerFork => "a2",
        ArmA::Control => "a0",
    }));

    // Snapshot AFTER the rig is built: `Catalog::create` and the runtime's construction are
    // fixture cost, not statement cost, and charging them to the clients would inflate the
    // headline in the direction the headline argues for.
    let before = acquisitions();

    let mut reported: BTreeSet<String> = BTreeSet::new();
    let mut asked = 0usize;
    let addr = rig.addr;
    std::thread::scope(|s| {
        let mut hs = Vec::with_capacity(t);
        for th in 0..t {
            hs.push(s.spawn(move || {
                let mut names: Vec<String> = Vec::new();
                let mut asked = 0usize;
                let mut client = match arm {
                    ArmA::Persistent => Some(Client::connect(addr).expect("connect")),
                    _ => None,
                };
                for i in 0..f {
                    match arm {
                        ArmA::Persistent => {
                            let c = client.as_mut().expect("persistent client");
                            let r = c
                                .query(&format!(
                                    "BEGIN AGENT SESSION AS 'a{th}' RUN 'r{th}_{i}';"
                                ))
                                .expect("BEGIN AGENT SESSION round trip");
                            refuse_on_error(&r, "BEGIN AGENT SESSION");
                            names.extend(branch_names(&r));
                            asked += 1;
                            let r = c.query("ABANDON;").expect("ABANDON round trip");
                            refuse_on_error(&r, "ABANDON");
                        }
                        ArmA::PerFork => {
                            let mut c = Client::connect(addr).expect("connect");
                            let r = c
                                .query(&format!(
                                    "BEGIN AGENT SESSION AS 'a{th}' RUN 'r{th}_{i}';"
                                ))
                                .expect("BEGIN AGENT SESSION round trip");
                            refuse_on_error(&r, "BEGIN AGENT SESSION");
                            names.extend(branch_names(&r));
                            asked += 1;
                            let _ = c.terminate();
                        }
                        ArmA::Control => {
                            let mut c = Client::connect(addr).expect("connect");
                            let r = c.query("SELECT 1;").expect("SELECT round trip");
                            refuse_on_error(&r, "SELECT 1");
                            let _ = c.terminate();
                        }
                    }
                }
                if let Some(mut c) = client {
                    let _ = c.terminate();
                }
                (names, asked)
            }));
        }
        for h in hs {
            let (names, a) = h.join().expect("client thread");
            reported.extend(names);
            asked += a;
        }
    });

    let after = acquisitions();

    // The independent witness. `asked` is this harness's bookkeeping; `reported` is what the
    // SERVER named. A fork total taken from the loop alone would be a number this file computed
    // about itself.
    if arm != ArmA::Control && reported.len() != asked {
        eprintln!(
            "e6: REFUSING. {} at T={t}: asked for {asked} forks but the server named {} distinct \
             branches. The fork total cannot be taken from this harness's own counter when the \
             two witnesses disagree.",
            arm.label(),
            reported.len()
        );
        std::process::exit(1);
    }

    Cell { forks: if arm == ArmA::Control { 0 } else { reported.len() }, acquisitions: after - before }
}

fn refuse_on_error(r: &Reply, what: &str) {
    if let Some(e) = &r.error {
        eprintln!(
            "e6: REFUSING. `{what}` was rejected by the server: {e}\n     A refused statement is \
             a fork that did not happen; counting the arm around it would report a mediated path \
             as an unmediated one."
        );
        std::process::exit(1);
    }
}

/// ARM B — `TableBranchCatalog::fork` directly, exactly as `examples/fork_concurrency.rs` does.
///
/// A pgwire server IS listening while this runs (built by the caller). It reads zero because
/// nothing connects to it, not because the counter is dead — and the A arms in this same process
/// against this same static are the proof of that.
fn run_arm_b(t: usize, f: usize, root: &Path) -> Cell {
    let dir = root.join(format!("e6-b-t{t}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("arm B dir");
    let path = dir.join("branches.branchcat");
    let _ = std::fs::remove_file(&path);
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&path, 1).expect("open catalog"));
    let lease = LeaseDeadline(u64::MAX);

    let before = acquisitions();
    let mut ids: BTreeSet<u64> = BTreeSet::new();
    std::thread::scope(|s| {
        let mut hs = Vec::with_capacity(t);
        for _ in 0..t {
            let cat = Arc::clone(&cat);
            hs.push(s.spawn(move || {
                let mut mine = Vec::with_capacity(f);
                for _ in 0..f {
                    let rec = cat.fork(BranchId::TRUNK, lease).expect("fork");
                    mine.push(rec.branch_id.id as u64);
                }
                mine
            }));
        }
        for h in hs {
            ids.extend(h.join().expect("arm B thread"));
        }
    });
    let after = acquisitions();

    if ids.len() != t * f {
        eprintln!(
            "e6: REFUSING. arm B at T={t}: asked for {} forks, the catalog minted {} distinct \
             branch ids.",
            t * f,
            ids.len()
        );
        std::process::exit(1);
    }
    Cell { forks: ids.len(), acquisitions: after - before }
}

// ---------------------------------------------------------------------------------------------
// forcing the detector to fire
// ---------------------------------------------------------------------------------------------

/// One explicit acquisition must move the counter by exactly one.
///
/// Without this the whole result is unfalsifiable: a counter wired to nothing prints the same
/// zero for arm B that a genuinely unmediated path would.
fn self_check(root: &Path) {
    let rig = rig(root, "selfcheck");
    let before = acquisitions();
    drop(rig.ctx.catalog());
    let after = acquisitions();
    if after != before + 1 {
        eprintln!(
            "e6: REFUSING. one explicit `ctx.catalog()` moved the census by {}, not 1. The \
             instrument does not count what it claims to count, so no arm below means anything.",
            after - before
        );
        std::process::exit(1);
    }
    println!("instrument self-check: one explicit ctx.catalog() moved the census by exactly 1. OK");
}

// ---------------------------------------------------------------------------------------------

fn main() {
    let f: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(8);
    let threads: Vec<usize> = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "1,4,16,64".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .filter(|&t: &usize| t > 0)
        .collect();
    if threads.is_empty() {
        eprintln!("e6: REFUSING. no thread counts to run.");
        std::process::exit(1);
    }

    let root = std::env::temp_dir().join(format!("ferrodb-e6-{}", std::process::id()));
    std::fs::create_dir_all(&root).expect("root");

    println!("E.6 -- acquisitions of ServerContext::catalog() during a concurrent-fork workload,");
    println!("driven THROUGH PGWIRE against the same workload driven DIRECTLY.");
    println!("Recorded {}. Counts only; this file quotes no durations, and an integer count is", stamp());
    println!("load-immune, so this run did not need a quiet box or the fleet measure-lock.");
    println!();
    println!("LAYERS AND THEIR PRODUCTION LOCKS");
    println!("  A1/A2/A0  the shipped pgwire server over a real TCP socket, forks issued as SQL.");
    println!("            Production locks: ServerContext::catalog() OUTERMOST, one per statement,");
    println!("            then AgentRuntime's Mutex<State>, then TableBranchCatalog's `logical`.");
    println!("  B         TableBranchCatalog directly, as examples/fork_concurrency.rs does");
    println!("            (:26, :32, :101). Production locks at this layer: `logical` ONLY --");
    println!("            no statement mutex, because no shipped front-end reaches fork this way.");
    println!();
    println!("forks/thread F = {f}. Total forks = F x T, so the totals RISE with T and the");
    println!("acquisition column can be read against the statement rate directly.");
    println!();

    self_check(&root);
    println!();

    println!("  arm                          T   forks   acquisitions   forks/acquisition");
    let mut a1: Vec<(usize, Cell)> = Vec::new();
    let mut a2: Vec<(usize, Cell)> = Vec::new();
    let mut a0: Vec<(usize, Cell)> = Vec::new();
    let mut b: Vec<(usize, Cell)> = Vec::new();

    for &t in &threads {
        let c = run_arm_a(ArmA::Persistent, t, f, &root);
        row(ArmA::Persistent.label(), t, &c);
        a1.push((t, c));
    }
    for &t in &threads {
        let c = run_arm_a(ArmA::PerFork, t, f, &root);
        row(ArmA::PerFork.label(), t, &c);
        a2.push((t, c));
    }
    for &t in &threads {
        let c = run_arm_a(ArmA::Control, t, f, &root);
        // The control issues no forks; its ratio column is meaningless and is printed as `--`
        // rather than as a zero that reads like a measurement.
        println!(
            "  {:26} {:3}   {:5}   {:12}   {:>17}",
            ArmA::Control.label(),
            t,
            "--",
            c.acquisitions,
            "--"
        );
        a0.push((t, c));
    }
    for &t in &threads {
        let c = run_arm_b(t, f, &root);
        row("B  direct, no ServerContext", t, &c);
        b.push((t, c));
    }

    println!();
    println!("READING IT");
    println!("  A0 is the per-connection/per-statement FLOOR: acquisitions a client pays for");
    println!("  connecting and running a trivial statement, with no fork in it at all. A2 minus");
    println!("  A0 at the same T is what the fork statement itself costs in acquisitions.");
    for (&t, ((_, c2), (_, c0))) in threads.iter().zip(a2.iter().zip(a0.iter())) {
        println!(
            "    T={t:3}: A2 {} - A0 {} = {} acquisitions attributable to the fork statement, \
             over {} forks",
            c2.acquisitions,
            c0.acquisitions,
            c2.acquisitions as i64 - c0.acquisitions as i64,
            c2.forks
        );
    }
    println!();
    let b_total: u64 = b.iter().map(|(_, c)| c.acquisitions).sum();
    let a_total: u64 = a1.iter().chain(a2.iter()).map(|(_, c)| c.acquisitions).sum();
    println!("  arm B took {b_total} acquisitions in total across every T, while arms A1+A2 took");
    println!("  {a_total} in the SAME PROCESS against the SAME counter. That pairing is what makes");
    println!("  B's zero a measurement rather than a silent instrument.");

    let _ = std::fs::remove_dir_all(&root);
}

fn row(label: &str, t: usize, c: &Cell) {
    let ratio = if c.acquisitions == 0 {
        "n/a (no acquisitions)".to_string()
    } else {
        format!("{:.3}", c.forks as f64 / c.acquisitions as f64)
    };
    println!("  {:26} {:3}   {:5}   {:12}   {:>17}", label, t, c.forks, c.acquisitions, ratio);
}

fn stamp() -> String {
    std::process::Command::new("date")
        .arg("+%Y-%m-%dT%H:%M:%S%z")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}
