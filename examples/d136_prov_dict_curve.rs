//! D136 — the per-page provenance dictionary's occupancy as a CURVE, and its distribution.
//!
//! `cargo run --release --example d136_prov_dict_curve`
//!
//! # What this measures, and why a curve rather than a breaking point
//!
//! `src/provenance/store.rs:34` caps a page's provenance dictionary at
//! `MAX_PAGE_DICT_ENTRIES = 255` and **refuses** a 256th run rather than widening the per-version
//! slot past a byte. D129's all-merge arm hit that refusal at session 495 and died there. "It
//! broke at 495" is a single point, and a single point cannot distinguish the two explanations
//! that matter:
//!
//! - **one hot page** — some page the merge path keeps coming back to, which would make the cap a
//!   property of the merge path and reachable at any table size; or
//! - **a saturating population** — every page filling together because the fixture's 200 rows
//!   occupy a handful of pages, which would make 495 an artifact of THIS table and push the cap
//!   out in proportion to the table.
//!
//! Those are opposite findings and the breaking session is the same number under both. So this
//! harness samples **the whole distribution** across the axis: the max, the page id holding it,
//! how many pages sit above each threshold, and the tail.
//!
//! ⛔ **COUNTS ONLY. No timing is reported and none is taken.** A dictionary occupancy is a
//! property of the workload and the page layout; it does not move when somebody else's `cargo`
//! starts, which is the whole reason this is the measurement being run on a shared box.
//!
//! ⛔ **Through pgwire, over a real TCP socket** — `serve` on this process's own listener, every
//! statement arriving as a `Query` message. Inherited from D129 for the reason D129 states: E.6
//! established that driving `AgentRuntime` directly is not the layer production runs, and D101
//! that a `Session::new()` beside a `ServerContext` silently runs on a private stub. This harness
//! holds no `Session` at all.
//!
//! # The instrument
//!
//! `ProvenanceStore::page_dictionary_lens()` — added for this run, a **required** trait method
//! with no default, because a default returning an empty `Vec` would report "nothing near the
//! cap" for a store that had merely never been asked. That is the one wrong answer that reads
//! exactly like a clean one.
//!
//! It is fire-checked three ways before any axis runs: against a hand-built store whose expected
//! values are written here and not read back from the subject; against the cap itself, forced to
//! refuse; and against the wire, where a zero max after real agent writes fails the run rather
//! than printing a calm table.

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
use ferrodb::provenance::{
    MemProvenanceStore, ProvId, ProvenanceStore, RunEntity, MAX_PAGE_DICT_ENTRIES,
};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::tel::{EffectLog, MemEffectLog};
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Writes inside one agent session. Three, as D129, so the fixture is the one whose breaking
/// point is being turned into a curve rather than a differently-shaped workload.
const WRITES_PER_SESSION: usize = 3;

/// D129's table size. Kept as the default so the main axis is comparable with the prior run; the
/// LOCALITY arm is the one that varies it, because varying it is the question.
const ROWS_DEFAULT: i64 = 200;

fn env_usize(k: &str, d: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

// =================================================================================================
// A PostgreSQL v3 client, simple query protocol only. Verbatim from D129.
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
        for kv in ["user", "ferro", "database", "ferro", "application_name", "d136"] {
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
// The server under test. D129's `build`, with the row count made a parameter and the runtime kept
// so the provenance store can be read — that is the only change, and it is the instrument.
// =================================================================================================

struct Server {
    addr: SocketAddr,
    rows: i64,
    runtime: Arc<AgentRuntime>,
    /// Kept alive for the life of the arm. Dropping it would let `designated` retire the runtime
    /// the serving threads are still using.
    _ctx: Arc<ServerContext>,
}

impl Server {
    /// The whole page population, ordered by page id. Read through the `dyn ProvenanceStore` the
    /// runtime actually holds — not a store this harness built beside it, which is the D67/D101
    /// failure this harness is one layer of plumbing away from.
    fn dict_lens(&self) -> Vec<(u32, usize)> {
        self.runtime
            .provenance()
            .page_dictionary_lens()
            .expect("page_dictionary_lens: the store refused to enumerate its own pages")
    }
}

fn build(dir: &std::path::Path, tag: &str, rows: i64) -> Server {
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
    // Same fixed floor and the same reason as D68/D114/D129: taking `high_water()` here would put
    // the arena at page 2 and leave the ordinary table nowhere to grow. The LOCALITY arm's largest
    // table is checked against this floor by `pages_below_arena` below rather than assumed.
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
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), Arc::clone(&runtime)));

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    let serve_ctx = Arc::clone(&ctx);
    std::thread::spawn(move || {
        let _ = serve(listener, serve_ctx);
    });

    let s = Server { addr, rows, runtime, _ctx: ctx };

    // Base table, over the wire like everything else. These INSERTs run outside any agent session,
    // so `self.author` is `None` in `execution/insert.rs` and they stamp nothing — which the
    // NO-AGENT control below asserts rather than assumes.
    let mut c = Wire::open(s.addr).expect("first connection");
    c.q("CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);").expect("create");
    for i in 1..=rows {
        c.q(&format!("INSERT INTO t VALUES ({i}, {});", i * 7)).expect("insert");
    }
    s
}

/// One agent's whole life: a fresh socket, a fresh branch, `WRITES_PER_SESSION` writes, and —
/// unless `merge` — no ending at all, which is the state an abandoned agent leaves behind.
///
/// A distinct agent name per session on purpose, as D129. `intern` keys on `(agent_id, run_id)`
/// (`src/agent_sql/runtime.rs:1522`), so a distinct name is a distinct `ProvId` and a dictionary
/// occupancy of N means **N distinct sessions wrote versions onto that page**.
fn one_session(s: &Server, k: usize, merge: bool) -> Result<(), String> {
    let mut c = Wire::open(s.addr)?;
    c.q(&format!("BEGIN AGENT SESSION AS 'a{k}';"))?;
    for w in 0..WRITES_PER_SESSION {
        let id = 1 + ((k * 97 + w) as i64) % s.rows;
        c.q(&format!("UPDATE t SET v = {} WHERE id = {id};", k as i64))?;
    }
    if merge {
        c.q("MERGE;")?;
    }
    Ok(())
}

// =================================================================================================
// The distribution
// =================================================================================================

/// One sample of the whole page population. Every field is an integer count.
struct Dist {
    pages: usize,
    max: usize,
    argmax: u32,
    /// Occupancy of the second-fullest page. `max` alone cannot tell one hot page from a
    /// population climbing together; `max` beside `second` and `ge_128` can.
    second: usize,
    ge_255: usize,
    ge_128: usize,
    ge_64: usize,
    ge_16: usize,
    median: usize,
    sum: usize,
    /// Sorted descending, for the tail print at the end of an arm.
    tail: Vec<(u32, usize)>,
}

impl Dist {
    fn of(lens: &[(u32, usize)]) -> Dist {
        let mut by_len: Vec<(u32, usize)> = lens.to_vec();
        by_len.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let occ: Vec<usize> = by_len.iter().map(|(_, n)| *n).collect();
        let n = occ.len();
        Dist {
            pages: n,
            max: occ.first().copied().unwrap_or(0),
            argmax: by_len.first().map(|(p, _)| *p).unwrap_or(u32::MAX),
            second: occ.get(1).copied().unwrap_or(0),
            ge_255: occ.iter().filter(|c| **c >= MAX_PAGE_DICT_ENTRIES).count(),
            ge_128: occ.iter().filter(|c| **c >= 128).count(),
            ge_64: occ.iter().filter(|c| **c >= 64).count(),
            ge_16: occ.iter().filter(|c| **c >= 16).count(),
            median: if n == 0 { 0 } else { occ[n / 2] },
            sum: occ.iter().sum(),
            tail: by_len,
        }
    }
}

fn header() {
    println!(
        "    {:>9} {:>7} {:>7} {:>9} {:>7} {:>7} {:>7} {:>7} {:>7} {:>8}",
        "sessions", "pages", "max", "argmax_pg", "2nd", ">=255", ">=128", ">=64", "median", "sum"
    );
}

fn row(sessions: usize, d: &Dist) {
    println!(
        "    {:>9} {:>7} {:>7} {:>9} {:>7} {:>7} {:>7} {:>7} {:>7} {:>8}",
        sessions,
        d.pages,
        d.max,
        if d.argmax == u32::MAX { "-".to_string() } else { d.argmax.to_string() },
        d.second,
        d.ge_255,
        d.ge_128,
        d.ge_64,
        d.median,
        d.sum
    );
}

fn tail_print(d: &Dist, n: usize) {
    let shown: Vec<String> =
        d.tail.iter().take(n).map(|(p, c)| format!("pg{p}:{c}")).collect();
    println!(
        "      fullest {} pages: {}{}",
        shown.len().min(n),
        shown.join("  "),
        if d.tail.len() > n { format!("  ... (+{} more pages)", d.tail.len() - n) } else { String::new() }
    );
    println!("      pages above 16: {} of {}", d.ge_16, d.pages);
}

// =================================================================================================
// FIRE-CHECKS — force the instrument to fire before any axis is believed
// =================================================================================================

fn run_n(n: usize) -> RunEntity {
    RunEntity::new(
        ProvId::NONE,
        &format!("agent-{n}"),
        &format!("run-{n}"),
        "m",
        "v",
        [0u8; 32],
        1_700_000_000_000,
        ferrodb::branch::BranchId::new(1, 0),
    )
}

/// (F1) The enumeration against a store built by hand. The expected values are written here —
/// two entries on page 7, one on page 9 — and are **not** read back out of the subject.
fn fire_enumeration() -> bool {
    let s = MemProvenanceStore::new();
    let a = s.intern(&run_n(1)).unwrap();
    let b = s.intern(&run_n(2)).unwrap();
    let c = s.intern(&run_n(3)).unwrap();
    // Page 7 gets runs a and b across three slots; run `a` twice, so a repeat must NOT count twice.
    s.stamp(RecordId { page_id: 7, slot_num: 0 }, a).unwrap();
    s.stamp(RecordId { page_id: 7, slot_num: 1 }, b).unwrap();
    s.stamp(RecordId { page_id: 7, slot_num: 2 }, a).unwrap();
    // Page 9 gets one run.
    s.stamp(RecordId { page_id: 9, slot_num: 0 }, c).unwrap();
    let got = s.page_dictionary_lens().unwrap();
    let want = vec![(7u32, 2usize), (9u32, 1usize)];
    let ok = got == want;
    println!("  (F1) ENUMERATION  got {got:?}  want {want:?}  => {}", if ok { "PASS" } else { "FAIL" });
    if !ok {
        println!("      the instrument does not report the population it was handed; no axis below means anything");
    }
    ok
}

/// (F2) The cap itself, forced. A binary existing is not a binary having the feature: this build
/// must refuse the 256th distinct run on one page, and must report exactly 255 when it does.
fn fire_cap() -> bool {
    let s = MemProvenanceStore::new();
    let mut last_ok = 0usize;
    let mut refusal: Option<String> = None;
    for i in 0..(MAX_PAGE_DICT_ENTRIES + 1) {
        let id = s.intern(&run_n(1000 + i)).unwrap();
        match s.stamp(RecordId { page_id: 3, slot_num: i as u16 }, id) {
            Ok(()) => last_ok = i + 1,
            Err(e) => {
                refusal = Some(e.to_string());
                break;
            }
        }
    }
    let lens = s.page_dictionary_lens().unwrap();
    let reported = lens.iter().find(|(p, _)| *p == 3).map(|(_, n)| *n).unwrap_or(0);
    let ok = last_ok == MAX_PAGE_DICT_ENTRIES
        && reported == MAX_PAGE_DICT_ENTRIES
        && refusal.is_some();
    println!(
        "  (F2) CAP FIRES    accepted {last_ok} (want {MAX_PAGE_DICT_ENTRIES}), enumeration reports {reported}, refused: {}",
        refusal.as_deref().unwrap_or("<NOTHING — the 256th was ACCEPTED>")
    );
    println!("      => {}", if ok { "PASS" } else { "FAIL" });
    ok
}

/// (F3) Anti-vacuity through the wire, and (F4) the no-agent control in the same server.
///
/// (F3) after real agent sessions the enumeration must be non-empty with a non-zero max — a zero
/// here would mean this harness is reading a store the server never writes, which is exactly the
/// defect D67 withdrew numbers over, and it must fail the run rather than print a calm table.
///
/// (F4) plain `UPDATE`s outside any agent session must add **nothing**. Without it, a counter
/// wired to any write at all would pass (F3).
fn fire_wire(dir: &std::path::Path) -> bool {
    let n = 12usize;
    let s = build(dir, "fire", ROWS_DEFAULT);

    let empty = Dist::of(&s.dict_lens());
    let clean_start = empty.pages == 0 && empty.sum == 0;
    println!(
        "  (F3a) BEFORE ANY SESSION  pages {} sum {}  => {}",
        empty.pages,
        empty.sum,
        if clean_start { "PASS" } else { "FAIL (the base-table INSERTs stamped something)" }
    );

    for k in 0..n {
        one_session(&s, k, true).unwrap_or_else(|e| panic!("fire-check session {k}: {e}"));
    }
    let after = Dist::of(&s.dict_lens());
    let fired = after.pages > 0 && after.max > 0;
    println!(
        "  (F3b) AFTER {n} MERGED SESSIONS  pages {} max {} (pg{}) sum {}  => {}",
        after.pages,
        after.max,
        after.argmax,
        after.sum,
        if fired { "PASS" } else { "FAIL — ZERO. The store being read is not the store being written." }
    );

    // (F4) NO AGENT SESSION. Plain UPDATEs on trunk, same table, same server.
    let before_plain = Dist::of(&s.dict_lens());
    {
        let mut c = Wire::open(s.addr).expect("connect");
        for i in 1..=60i64 {
            c.q(&format!("UPDATE t SET v = 999 WHERE id = {i};")).expect("plain update");
        }
    }
    let after_plain = Dist::of(&s.dict_lens());
    let quiet = after_plain.sum == before_plain.sum;
    println!(
        "  (F4) NO-AGENT CONTROL  60 plain UPDATEs: sum {} -> {}  => {}",
        before_plain.sum,
        after_plain.sum,
        if quiet { "PASS (an unattributed write stamps nothing)" } else { "FAIL" }
    );

    clean_start && fired && quiet
}

// =================================================================================================
// The axes
// =================================================================================================

struct AxisResult {
    ok: bool,
    /// The session index whose statement was refused, if the cap was reached.
    refused_at: Option<usize>,
    last: Dist,
    sessions_run: usize,
}

/// What an arm is pre-registered to collect.
///
/// ⛔ Run 1 of this harness applied a single "an arm that collected nothing has not passed" rule
/// to every arm, and exited 1 because the PARK **control** collected zero — which is the result
/// the control exists to produce. The rule is right and the scope was wrong: anti-vacuity is a
/// property of the *instrument*, not of every arm.
///
/// Stating the direction per arm is strictly STRONGER than the rule it replaces. `Zero` does not
/// merely tolerate a zero; it **fails on a non-zero**, so a change that made a parked, unmerged
/// session stamp a page dictionary would now break this arm — and that is precisely the
/// regression the control is here to catch. And a `Zero` arm is not believed on the strength of
/// its own silence: `axis` forces it to fire on its own server before accepting the zero.
#[derive(Clone, Copy, PartialEq)]
enum Expect {
    NonZero,
    Zero,
}

/// Run one arm, sampling the whole page population every `block` sessions, until `max_sessions`
/// or until the store refuses.
fn axis(
    dir: &std::path::Path,
    tag: &str,
    what: &str,
    rows: i64,
    merge: bool,
    block: usize,
    max_sessions: usize,
    expect: Expect,
) -> AxisResult {
    let s = build(dir, tag, rows);
    println!("=== AXIS: {what} ===");
    println!("    table rows: {rows}, writes/session: {WRITES_PER_SESSION}, sample every {block} sessions");
    header();

    let mut refused_at = None;
    let mut done = 0usize;
    let mut last = Dist::of(&s.dict_lens());
    row(0, &last);

    'outer: while done < max_sessions {
        for k in done..(done + block).min(max_sessions) {
            if let Err(e) = one_session(&s, k, merge) {
                last = Dist::of(&s.dict_lens());
                row(k, &last);
                println!("    ⛔ session {k} refused: {e}");
                refused_at = Some(k);
                done = k;
                break 'outer;
            }
        }
        done = (done + block).min(max_sessions);
        last = Dist::of(&s.dict_lens());
        row(done, &last);
    }

    tail_print(&last, 10);

    let ok = match expect {
        // A run that collected nothing has not passed.
        Expect::NonZero => {
            let ok = last.sum > 0 && last.pages > 0;
            if !ok {
                println!("    ⛔ ZERO dictionary entries in an arm registered NON-ZERO. Not a result.");
            }
            ok
        }
        // A control's zero is only a result once the instrument has been forced to fire ON THIS
        // SERVER. The fire-checks ran against a different server; a store this one never wrote to
        // would produce exactly the zero above and look like a clean control.
        Expect::Zero => {
            let stayed = last.sum == 0 && last.pages == 0;
            if !stayed {
                println!(
                    "    ⛔ an arm registered ZERO stamped {} entries across {} pages. \
                     A parked, unmerged session is reaching the page dictionaries.",
                    last.sum, last.pages
                );
            }
            // The coda: one merged session on this very server must move it off zero.
            // A session id distinct from every one this arm used, and small enough that `k * 97`
            // in `one_session` cannot wrap: a wrapped id goes negative through `as i64`, selects
            // no row, stamps nothing, and would fail this coda for a reason that is not the store.
            let coda = one_session(&s, done + 1_000_000, true)
                .map(|()| Dist::of(&s.dict_lens()))
                .map(|d| d.sum > 0)
                .unwrap_or(false);
            println!(
                "      LIVENESS CODA on this server: one MERGED session after the arm moves sum \
                 {} -> {}  => {}",
                last.sum,
                Dist::of(&s.dict_lens()).sum,
                if coda { "PASS (the zero above was measured by a live instrument)" } else { "FAIL (the zero is vacuous)" }
            );
            stayed && coda
        }
    };
    println!();
    AxisResult { ok, refused_at, last, sessions_run: done }
}

/// The locality model, stated as a PREDICTION and only then tested.
///
/// The locality table measures a fill rate of roughly `1/pages` dictionary entries per session.
/// If that is the mechanism rather than a coincidence of two small tables, a workload spreading
/// over `pages` pages must refuse at about `MAX_PAGE_DICT_ENTRIES / rate` sessions. This takes
/// the rate at a checkpoint far below the cap, **prints the prediction before the refusal is
/// observed**, and only then runs on.
///
/// ⚠ What this is and is not: an extrapolation of the subject's own early behaviour across a ~5x
/// range. It tests whether the linear model reaches the cap. It is NOT an independent oracle for
/// the cap's value, and a refutation here is a finding, not a broken run — so it does not gate
/// the exit code, and the output says which it was either way.
fn prediction_arm(dir: &std::path::Path, rows: i64, checkpoint: usize, ceiling: usize) -> bool {
    println!("=== PREDICTION — the locality model run out to the cap ===");
    let s = build(dir, "predict", rows);
    for k in 0..checkpoint {
        if one_session(&s, k, true).is_err() {
            println!("    ⛔ refused before the checkpoint at {checkpoint}; no prediction possible.");
            return false;
        }
    }
    let at_checkpoint = Dist::of(&s.dict_lens());
    let rate = at_checkpoint.max as f64 / checkpoint as f64;
    let predicted = (MAX_PAGE_DICT_ENTRIES as f64 / rate).round() as usize;
    println!("    rows {rows}, checkpoint {checkpoint} sessions: {} pages, max {}, 2nd {}.",
        at_checkpoint.pages, at_checkpoint.max, at_checkpoint.second);
    println!("    fill rate {rate:.4} entries/session on the fullest page (1/pages would be {:.4}).",
        1.0 / at_checkpoint.pages.max(1) as f64);
    println!("    ⇒ PRE-REGISTERED PREDICTION, written before the refusal is observed:");
    println!("      the store must refuse at about session {predicted} (ceiling {ceiling}).");

    let mut refused = None;
    for k in checkpoint..ceiling {
        if one_session(&s, k, true).is_err() {
            refused = Some(k);
            break;
        }
    }
    let end = Dist::of(&s.dict_lens());
    match refused {
        Some(k) => {
            let err = (k as f64 - predicted as f64).abs() / predicted as f64;
            let held = err <= 0.15;
            println!("    OBSERVED: refused at session {k}. predicted {predicted}. error {:.1}%.", err * 100.0);
            println!("    => PREDICTION {}", if held { "HELD" } else { "REFUTED — the model does not reach the cap linearly" });
            tail_print(&end, 8);
            held
        }
        None => {
            println!("    OBSERVED: no refusal within the {ceiling}-session ceiling (max reached {}).", end.max);
            println!("    => PREDICTION NOT TESTED — the ceiling was too low to reach {predicted}.");
            tail_print(&end, 8);
            false
        }
    }
}

// =================================================================================================

fn main() {
    let block = env_usize("D136_BLOCK", 25);
    let max_sessions = env_usize("D136_MAX_SESSIONS", 700);
    let loc_sessions = env_usize("D136_LOC_SESSIONS", 400);

    println!("D136 — per-page provenance dictionary occupancy as a function of session count.");
    println!("at {}", option_env!("D136_HEAD").unwrap_or("<head stamped by the runner, not here>"));
    println!("Driven over a real TCP socket against `pgwire::serve` in this process.");
    println!("⛔ COUNTS ONLY. No timing is reported and none was taken — SCALE-DESIGN.md D136.");
    println!("cap under test: MAX_PAGE_DICT_ENTRIES = {MAX_PAGE_DICT_ENTRIES} (src/provenance/store.rs:34)");
    println!();

    let dir = std::env::temp_dir().join(format!("d136-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    println!("=== FIRE-CHECK — force the instrument to fire, and force the cap to refuse ===");
    let f1 = fire_enumeration();
    let f2 = fire_cap();
    let f3 = fire_wire(&dir);
    println!();
    if !(f1 && f2 && f3) {
        println!("⛔ FIRE-CHECKS FAILED. No axis below is reported.");
        let _ = std::fs::remove_dir_all(&dir);
        std::process::exit(1);
    }

    // PARK — the control. Sessions begin, write, and never end. D129 ran 1750 of these and never
    // reached the cap; if this arm MOVES like the merge arm, the prior finding was never about
    // merging at all.
    let park = axis(
        &dir,
        "park",
        "PARK — sessions begin, write, and never end (live branches)  [CONTROL]",
        ROWS_DEFAULT,
        false,
        block,
        max_sessions,
        Expect::Zero,
    );

    // CHURN — every session MERGEs. This is the arm that died at 495.
    let churn = axis(
        &dir,
        "churn",
        "CHURN — every session MERGEs, so every branch is FINISHED",
        ROWS_DEFAULT,
        true,
        block,
        max_sessions,
        Expect::NonZero,
    );

    // LOCALITY — the arm that separates the two explanations. Same merge workload, same session
    // count, table size varied. If `max` is flat across the row, the cap is a property of the
    // merge path and one hot page carries it. If `max` falls as the table grows, 495 was a
    // property of a 200-row fixture.
    println!("=== LOCALITY — CHURN at {loc_sessions} sessions, table size varied ===");
    println!("    the discriminating arm: is `max` flat in table size (one hot page), or does it");
    println!("    fall as pages multiply (a 200-row fixture saturating its whole population)?");
    println!(
        "    {:>7} {:>9} {:>7} {:>7} {:>9} {:>7} {:>7} {:>8} {:>10}",
        "rows", "sessions", "pages", "max", "argmax_pg", "2nd", ">=128", "sum", "max/sess"
    );
    let mut loc_rows: Vec<(i64, Dist, usize, Option<usize>)> = Vec::new();
    for rows in [50i64, 200, 800, 3200] {
        let s = build(&dir, &format!("loc{rows}"), rows);
        let mut refused = None;
        let mut ran = 0usize;
        for k in 0..loc_sessions {
            if let Err(_e) = one_session(&s, k, true) {
                refused = Some(k);
                break;
            }
            ran = k + 1;
        }
        let d = Dist::of(&s.dict_lens());
        println!(
            "    {:>7} {:>9} {:>7} {:>7} {:>9} {:>7} {:>7} {:>8} {:>10.3}",
            rows,
            match refused {
                Some(k) => format!("{ran} (REFUSED at {k})"),
                None => format!("{ran}"),
            },
            d.pages,
            d.max,
            d.argmax,
            d.second,
            d.ge_128,
            d.sum,
            d.max as f64 / ran.max(1) as f64
        );
        tail_print(&d, 6);
        loc_rows.push((rows, d, ran, refused));
    }
    println!();

    // The locality model, run out to the cap on a table 4x the one it was fitted on.
    let predicted_ok = prediction_arm(
        &dir,
        env_usize("D136_PRED_ROWS", 800) as i64,
        env_usize("D136_PRED_CHECKPOINT", 400),
        env_usize("D136_PRED_CEILING", 4000),
    );
    println!();

    // =============================================================================================
    // What the numbers say, stated only as far as they reach.
    // =============================================================================================
    println!("=== READING ===");
    println!(
        "  CONTROL (PARK): {} sessions, max occupancy {}, {} pages{}. {}",
        park.sessions_run,
        park.last.max,
        park.last.pages,
        if park.last.argmax == u32::MAX {
            " (no page carries any attribution at all)".to_string()
        } else {
            format!(", fullest page {}", park.last.argmax)
        },
        match park.refused_at {
            Some(k) => format!("REFUSED at session {k}."),
            None => "never refused.".to_string(),
        }
    );
    println!(
        "  CHURN:          {} sessions, max occupancy {} on page {}, {} pages. {}",
        churn.sessions_run,
        churn.last.max,
        churn.last.argmax,
        churn.last.pages,
        match churn.refused_at {
            Some(k) => format!("REFUSED at session {k}."),
            None => format!("never refused within {} sessions.", churn.sessions_run),
        }
    );
    // One hot page vs a saturating population, decided by the shape at the end of CHURN.
    let c = &churn.last;
    println!(
        "  SHAPE at the end of CHURN: max {} / 2nd {} / pages>=128 {} of {} pages.",
        c.max, c.second, c.ge_128, c.pages
    );
    if c.pages > 1 && c.second * 4 < c.max {
        println!("  ⇒ ONE HOT PAGE: the fullest page carries >4x the second's occupancy.");
    } else if c.pages > 1 && c.ge_128 * 2 >= c.pages {
        println!("  ⇒ GENERAL SATURATION: at least half the page population is above 128.");
    } else {
        println!("  ⇒ NEITHER extreme: a small band of pages fills together. See the tail above.");
    }
    // The locality verdict: compare max at equal session count across table sizes.
    if let (Some(first), Some(last)) = (loc_rows.first(), loc_rows.last()) {
        println!(
            "  LOCALITY: rows {} -> {} ({}x table), max occupancy {} -> {} at the same workload.",
            first.0,
            last.0,
            last.0 / first.0,
            first.1.max,
            last.1.max
        );
        println!(
            "            pages carrying attribution: {} -> {}.",
            first.1.pages, last.1.pages
        );
    }
    println!(
        "  PREDICTION: the locality model {} when run out to the cap on a larger table.",
        if predicted_ok { "HELD" } else { "did NOT hold (see the arm above)" }
    );
    println!(
        "  ⚠ Every number above is a COUNT. No wall-clock figure was taken anywhere in this run."
    );
    println!(
        "  ⚠ The exit code gates COLLECTION INTEGRITY only — fire-checks, and each arm matching \
         its pre-registered direction. A refuted prediction is a finding and does NOT set it."
    );

    let ok = park.ok && churn.ok && loc_rows.iter().all(|(_, d, _, _)| d.sum > 0);
    let _ = std::fs::remove_dir_all(&dir);
    if !ok {
        println!("⛔ an arm collected nothing. Not a result.");
        std::process::exit(1);
    }
    println!("OK");
}
