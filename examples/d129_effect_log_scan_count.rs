//! D129 — how many frames the effect log's linear searches walk, per `append`, bucketed HIT/MISS.
//!
//! `cargo run --release --example d129_effect_log_scan_count`
//!
//! # What this is and is not
//!
//! ⛔ **It is a COUNT, deliberately, and it opens with no timing.** `SCALE-DESIGN.md` D129 says so
//! in its own words, and the reason is on the record: this box is shared and three results on this
//! project have died of a duration measured beside other work. A scan length is a property of the
//! data structure and the workload; it does not move when somebody else's `cargo` starts.
//!
//! ⛔ **Through pgwire, over a real TCP socket.** `serve` is spawned on this process's own
//! listener and every statement below arrives as a `Query` message on a socket, so the statement
//! takes the locks, the session plumbing and the designated runtime that production takes. E.6
//! established that a harness driving `AgentRuntime` directly is not that layer, and D101
//! established that `Session::new()` beside a `ServerContext` silently runs on a private stub.
//! Neither is reachable from here: this harness holds no `Session` at all — the server builds them
//! from `ctx.session()` on the connection thread.
//!
//! The client is written to the v3 protocol in this file rather than reusing `pgwire::message`'s
//! encoder. `tests/pg/pg_client.py` makes the independence argument for the protocol's own sake;
//! here it is simply the shortest way to get a socket that the server treats as a client, and the
//! claim being measured is about what happens **below** the wire, not about the wire.
//!
//! # The three sites, and why the bucketing is the instrument
//!
//! `MemEffectLog::frames` is a `Vec<TxnFrame>` keyed by `(branch, txn_id)` and searched
//! front-to-back in three places (`scan_count` in `src/tel/log.rs` names them). The searches
//! short-circuit, so a **hit** costs `index + 1` and a **miss** costs the whole Vec. A mean over
//! both hides which one the workload has, so they are counted apart.
//!
//! ⚠ And the short-circuit's value depends on *where the key sits*. `append` pushes a new frame to
//! the **back**, and a scan starts at the **front** — so the frame an agent re-appends is the one
//! furthest from the start. Whether that makes a hit as expensive as a miss is a fact about this
//! workload, and it is one of the pre-registered outcomes rather than an assumption.

use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::{BranchCatalog, TableBranchCatalog};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::pgwire::{serve, ServerContext};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::log::scan_count::{self, SiteCount, Snapshot};
use ferrodb::tel::{EffectLog, MemEffectLog};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Rows in the base table. NOT an axis — small and fixed. The axis is the session count.
const ROWS: i64 = 200;
/// Writes inside one agent session on the main axis. **Three, not one**: the first append of a
/// session is the miss and the rest are hits, so a session of one write would populate only one
/// bucket and the comparison the row turns on could not be made.
const WRITES_PER_SESSION: usize = 3;

fn env_usize(k: &str, d: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

// =================================================================================================
// A PostgreSQL v3 client, simple query protocol only
// =================================================================================================

struct Wire {
    w: TcpStream,
    r: BufReader<TcpStream>,
}

const SSL_REQUEST: i32 = 80877103;
const PROTOCOL_V3: i32 = 196608;

impl Wire {
    fn open(addr: SocketAddr) -> Result<Wire, String> {
        let s = TcpStream::connect(addr).map_err(|e| format!("connect: {e}"))?;
        s.set_nodelay(true).ok();
        let r = BufReader::new(s.try_clone().map_err(|e| e.to_string())?);
        let mut c = Wire { w: s, r };
        c.startup()?;
        Ok(c)
    }

    /// One backend message: a 1-byte tag, then a length that includes itself but not the tag.
    fn msg(&mut self) -> Result<(u8, Vec<u8>), String> {
        let mut tag = [0u8; 1];
        self.r.read_exact(&mut tag).map_err(|e| format!("read tag: {e}"))?;
        let mut len = [0u8; 4];
        self.r.read_exact(&mut len).map_err(|e| format!("read len: {e}"))?;
        let n = i32::from_be_bytes(len);
        if n < 4 {
            return Err(format!("message claims {n} bytes, which cannot hold its own header"));
        }
        let mut body = vec![0u8; (n - 4) as usize];
        self.r.read_exact(&mut body).map_err(|e| format!("read body: {e}"))?;
        Ok((tag[0], body))
    }

    fn startup(&mut self) -> Result<(), String> {
        // Every real client probes for TLS first and the server answers with a bare byte, outside
        // the tagged framing. Skipping it would desynchronise the stream, not merely skip a step.
        let mut probe = Vec::new();
        probe.extend_from_slice(&8i32.to_be_bytes());
        probe.extend_from_slice(&SSL_REQUEST.to_be_bytes());
        self.w.write_all(&probe).map_err(|e| e.to_string())?;
        self.w.flush().map_err(|e| e.to_string())?;
        let mut b = [0u8; 1];
        self.r.read_exact(&mut b).map_err(|e| format!("ssl reply: {e}"))?;
        if b[0] != b'N' {
            return Err(format!("expected a TLS refusal, got {:?}", b[0] as char));
        }

        let mut params = Vec::new();
        for kv in ["user", "ferro", "database", "ferro", "application_name", "d129"] {
            params.extend_from_slice(kv.as_bytes());
            params.push(0);
        }
        params.push(0);
        let mut pkt = Vec::new();
        pkt.extend_from_slice(&((params.len() + 8) as i32).to_be_bytes());
        pkt.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
        pkt.extend_from_slice(&params);
        self.w.write_all(&pkt).map_err(|e| e.to_string())?;
        self.w.flush().map_err(|e| e.to_string())?;

        loop {
            let (tag, body) = self.msg()?;
            match tag {
                b'Z' => return Ok(()),
                b'E' => return Err(format!("error during startup: {}", field(&body, b'M'))),
                _ => {}
            }
        }
    }

    /// One simple query, run to `ReadyForQuery`. `Err` carries the server's own message, never one
    /// invented here — a refusal this harness paraphrased would be a refusal it could mis-read.
    fn q(&mut self, sql: &str) -> Result<(), String> {
        let mut pkt = vec![b'Q'];
        pkt.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
        pkt.extend_from_slice(sql.as_bytes());
        pkt.push(0);
        self.w.write_all(&pkt).map_err(|e| e.to_string())?;
        self.w.flush().map_err(|e| e.to_string())?;
        let mut err: Option<String> = None;
        loop {
            let (tag, body) = self.msg()?;
            match tag {
                b'Z' => break,
                b'E' => err = Some(field(&body, b'M')),
                _ => {}
            }
        }
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl Drop for Wire {
    fn drop(&mut self) {
        let _ = self.w.write_all(&[b'X', 0, 0, 0, 4]);
        let _ = self.w.flush();
    }
}

/// One field out of an `ErrorResponse` body, which is NUL-separated `<code><text>` chunks.
fn field(body: &[u8], want: u8) -> String {
    for chunk in body.split(|b| *b == 0) {
        if chunk.first() == Some(&want) {
            return String::from_utf8_lossy(&chunk[1..]).into_owned();
        }
    }
    String::from_utf8_lossy(body).into_owned()
}

// =================================================================================================
// The server under test
// =================================================================================================

struct Server {
    addr: SocketAddr,
    log: Arc<MemEffectLog>,
    /// Kept alive for the life of the arm. Dropping it would let `designated` retire the runtime
    /// the serving threads are still using.
    _ctx: Arc<ServerContext>,
}

/// Wired exactly as `examples/pgserver.rs` wires the server: an arena-backed `AgentRuntime` behind
/// a `ServerContext`, with `MemEffectLog` — which is the effect log the pgwire server actually
/// constructs. (`src/cli/cli.rs` uses `DurableEffectLog`; that store's `append` calls
/// `classify_append` **and then** `MemEffectLog::append`, so it pays both of the sites counted
/// here. The `SITE_CLASSIFY` column below is what says so rather than an assertion in prose.)
fn build(dir: &std::path::Path, tag: &str) -> Server {
    let d = dir.join(tag);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(d.join("main.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(d.join("main.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&d.join("b.branchcat"), 1).unwrap());
    let branches: Arc<dyn BranchCatalog> = cat.clone();
    // Same fixed floor and the same reason as D68/D114: taking `high_water()` here would put the
    // arena at page 2 and leave the ordinary table nowhere to grow.
    const ARENA_BASE: u32 = 1024;
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), ARENA_BASE).unwrap());
    // ⚠ `with_storage`, NOT `with_catalog`: `with_catalog` leaves `storage: None`, so the branch
    // engine is absent and agent writes never reach arena pages. D67 withdrew numbers over that.
    let log = Arc::new(MemEffectLog::new());
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches,
            Arc::clone(&log) as Arc<dyn EffectLog>,
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    let serve_ctx = Arc::clone(&ctx);
    std::thread::spawn(move || {
        let _ = serve(listener, serve_ctx);
    });

    let s = Server { addr, log, _ctx: ctx };

    // Base table, over the wire like everything else.
    let mut c = Wire::open(s.addr).expect("first connection");
    c.q("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);").expect("create");
    for i in 1..=ROWS {
        c.q(&format!("INSERT INTO t VALUES ({i}, {});", i * 7)).expect("insert");
    }
    s
}

/// One agent's whole life: a fresh socket, a fresh branch, `writes` writes, and — unless `merge` —
/// no ending at all, which is the state an abandoned agent leaves behind.
///
/// A distinct agent name per session on purpose: this is the fork-heavy shape, so every session is
/// a different branch. (d114 reuses ONE name because it merges repeatedly into one lineage and the
/// provenance slot refuses redeclaration; d75 parks thousands of branches under distinct names,
/// which is the case here.)
fn one_session(s: &Server, k: usize, writes: usize, merge: bool) -> Result<(), String> {
    let mut c = Wire::open(s.addr)?;
    c.q(&format!("BEGIN AGENT SESSION AS 'a{k}';"))?;
    for w in 0..writes {
        let id = 1 + ((k * 97 + w) as i64) % ROWS;
        c.q(&format!("UPDATE t SET v = {} WHERE id = {id};", k as i64))?;
    }
    if merge {
        c.q("MERGE;")?;
    }
    Ok(())
}

// =================================================================================================
// Reporting
// =================================================================================================

fn f(v: Option<f64>) -> String {
    match v {
        Some(x) => format!("{x:>10.1}"),
        // ⛔ Never "0.0". A bucket with no calls in it is not a bucket that was cheap.
        None => "         -".to_string(),
    }
}

fn site_row(name: &str, c: &SiteCount) {
    println!(
        "    {name:<26} calls {:>7}  ({:>7} hit / {:>7} miss)   scanned/hit {}  scanned/miss {}",
        c.calls(),
        c.hit_calls,
        c.miss_calls,
        f(c.per_hit()),
        f(c.per_miss()),
    );
}

fn report(label: &str, d: &Snapshot) {
    println!("  {label}");
    site_row("MemEffectLog::append", &d.append());
    site_row("classify_append", &d.classify());
    site_row("frame()", &d.frame());
}

// =================================================================================================
// Fire-checks — run BEFORE the axis, and the axis is not printed if they do not pass
// =================================================================================================

/// Did the counter count, and do the two buckets separate?
///
/// ⛔ A counter reading 0 is indistinguishable from a path that never ran, so the axis below is
/// worthless until this has shown the counter moving on a workload built to move it — and moving
/// in the *right bucket*, which a single total could not show.
fn fire_checks(dir: &std::path::Path) -> bool {
    let n = env_usize("D129_FIRECHECK_N", 200);
    let mut ok = true;
    println!("=== FIRE-CHECK — force the counter to fire, and separate the buckets ===");
    println!();

    // (1) MISS-ONLY. Each session writes exactly once, so every append is the first append under a
    // key that is not present. Nothing here can produce a hit on `append`.
    {
        let s = build(dir, "fc_miss");
        let before = scan_count::snapshot();
        for k in 0..n {
            one_session(&s, k, 1, false).unwrap_or_else(|e| panic!("miss-only session {k}: {e}"));
        }
        let d = scan_count::snapshot().since(&before);
        report(&format!("(1) MISS-ONLY  — {n} sessions x 1 write"), &d);
        let a = d.append();
        let pass = a.miss_calls >= n as u64 && a.hit_calls * 4 < a.miss_calls;
        println!(
            "      => {} : miss_calls {} vs hit_calls {} (want misses >= {n} and dominant)",
            if pass { "PASS" } else { "FAIL" },
            a.miss_calls,
            a.hit_calls
        );
        ok &= pass;
        println!("      log length after: {}", s.log.len());
    }
    println!();

    // (2) HIT-HEAVY. ONE session, many writes. `stage_all` re-appends the open frame once per
    // statement, so after the first append every one of these finds its key.
    {
        let s = build(dir, "fc_hit");
        let before = scan_count::snapshot();
        one_session(&s, 0, n, false).unwrap_or_else(|e| panic!("hit-heavy session: {e}"));
        let d = scan_count::snapshot().since(&before);
        report(&format!("(2) HIT-HEAVY  — 1 session x {n} writes"), &d);
        let a = d.append();
        let pass = a.hit_calls >= (n as u64) / 2 && a.hit_calls > a.miss_calls;
        println!(
            "      => {} : hit_calls {} vs miss_calls {} (want hits dominant)",
            if pass { "PASS" } else { "FAIL" },
            a.hit_calls,
            a.miss_calls
        );
        ok &= pass;
    }
    println!();

    // (3) ANTI-VACUITY — the counter must NOT fire on everything. Plain SQL on trunk, no agent
    // session anywhere. Without this, a counter wired to some unrelated hot path would pass (1)
    // and (2) on volume alone and the axis would be measuring the wrong thing.
    {
        let s = build(dir, "fc_none");
        let before = scan_count::snapshot();
        let mut c = Wire::open(s.addr).expect("connect");
        for i in 1..=(n as i64) {
            let id = 1 + (i * 13) % ROWS;
            c.q(&format!("UPDATE t SET v = {i} WHERE id = {id};")).expect("plain update");
        }
        drop(c);
        let d = scan_count::snapshot().since(&before);
        report(&format!("(3) NO AGENT SESSION — {n} plain UPDATEs on trunk"), &d);
        let pass = d.append().calls() == 0;
        println!(
            "      => {} : append calls {} (want 0 — a non-agent write appends no frame)",
            if pass { "PASS" } else { "FAIL" },
            d.append().calls()
        );
        ok &= pass;
    }
    println!();
    ok
}

// =================================================================================================
// The axis
// =================================================================================================

/// `blocks` blocks of `block` sessions each, snapshotting between them.
///
/// The counters are cumulative since process start, so each row is a *delta* over its own block —
/// a running total would rise whatever the per-append cost did, which is the mistake this shape
/// exists to avoid. What a row reports is therefore the scan length **paid by the appends in that
/// block**, against the number of sessions that ran before it.
fn axis(dir: &std::path::Path, tag: &str, what: &str, merge: bool, blocks: usize, block: usize) -> bool {
    let s = build(dir, tag);
    println!("=== AXIS: {what} ===");
    println!(
        "    {:>9} {:>9} {:>8} {:>8} {:>12} {:>12} {:>12}",
        "sessions", "log_len", "hits", "misses", "scan/hit", "scan/miss", "scanned"
    );
    let mut total_appends = 0u64;
    let mut rows: Vec<(usize, f64, f64)> = Vec::new();
    for b in 0..blocks {
        let before = scan_count::snapshot();
        let done_before = b * block;
        for k in 0..block {
            let id = done_before + k;
            if let Err(e) = one_session(&s, id, WRITES_PER_SESSION, merge) {
                println!("⛔ session {id} failed: {e}");
                return false;
            }
        }
        let d = scan_count::snapshot().since(&before);
        let a = d.append();
        total_appends += a.calls();
        println!(
            "    {:>9} {:>9} {:>8} {:>8} {} {} {:>12}",
            done_before,
            s.log.len(),
            a.hit_calls,
            a.miss_calls,
            f(a.per_hit()),
            f(a.per_miss()),
            a.scanned(),
        );
        if let (Some(h), Some(m)) = (a.per_hit(), a.per_miss()) {
            rows.push((done_before, h, m));
        }
    }
    if total_appends == 0 {
        println!("⛔ ZERO appends across the whole axis. Not a result.");
        return false;
    }
    println!("    final log length: {} frames, never pruned", s.log.len());
    if let (Some(first), Some(last)) = (rows.first(), rows.last()) {
        println!(
            "    growth over the axis: scan/hit {:.1} -> {:.1} ({:.1}x),  scan/miss {:.1} -> {:.1} ({:.1}x)",
            first.1,
            last.1,
            last.1 / first.1.max(1e-9),
            first.2,
            last.2,
            last.2 / first.2.max(1e-9),
        );
    }
    println!();
    true
}

fn main() {
    println!("D129 — scanned elements per effect-log append, bucketed HIT / MISS.");
    println!("{}", ferrodb::build_provenance());
    println!("Driven over a real TCP socket against `pgwire::serve` in this process.");
    println!("⛔ COUNTS ONLY. No timing is reported and none was taken — SCALE-DESIGN.md D129.");
    println!();

    let dir = std::env::temp_dir().join(format!("d129_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    if !fire_checks(&dir) {
        println!("⛔ A FIRE-CHECK FAILED. The counter has not been shown to measure what it claims,");
        println!("   so no axis is reported. This is not a zero result; it is no result.");
        let _ = std::fs::remove_dir_all(&dir);
        std::process::exit(1);
    }

    let blocks = env_usize("D129_BLOCKS", 8);
    let block = env_usize("D129_BLOCK", 250);
    println!("Axis: {blocks} blocks of {block} sessions, {WRITES_PER_SESSION} writes per session.");
    println!();

    // PARK — branches opened and never ended. The frames are for LIVE work.
    let a = axis(&dir, "park", "PARK — sessions begin, write, and never end (live branches)", false, blocks, block);
    // CHURN — every session MERGEs. Its branch is finished, its transaction committed. Nothing
    // about those frames is still needed, and this arm exists to show whether they are still
    // walked — which is the whole case for option (2) PRUNE over option (1) INDEX.
    let b = axis(&dir, "churn", "CHURN — every session MERGEs, so every branch is FINISHED", true, blocks, block);

    let _ = std::fs::remove_dir_all(&dir);
    if !(a && b) {
        std::process::exit(1);
    }
}
