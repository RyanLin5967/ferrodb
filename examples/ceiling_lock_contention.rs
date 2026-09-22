//! ⛔⛔ **CEILING MEASUREMENT SCAFFOLD. MUST NEVER MERGE.** See `src/branch/lockcount.rs`.
//!
//! # Is the 1.9× ceiling the structure, or the mutex? Asked as a COUNT.
//!
//! Pre-registered in `frontier/INVENTION-TRIGGER.md`, section *"IS THE 1.9× CEILING THE STRUCTURE,
//! OR THE MUTEX?"*, with three outcomes and Amendments 1–2. **This harness may not add a fourth.**
//! It may report that the counts cannot discriminate, which is a result about the instrument.
//!
//! The recorded pair this attacks (`bench/d123_serial_attribution.txt` §3b, warm=0, T=64):
//!
//! ```text
//!   stub  forks/sec     S_eff  HOLD_TOT       gap  U(hold)
//!      0     4788.6   0.20883   0.20079   0.00804    96.1%
//!      3     9019.1   0.11088   0.00941   0.10147     8.5%
//! ```
//!
//! HOLD 21× down, throughput 1.9× up, GAP 12.6× up, utilisation 96.1% → 8.5%. The project read
//! *"the handoff is the floor"* off that and retired the section-shortening family. **Only the
//! first half was measured.** Amendment 2's discriminator:
//!
//! * gap is **park/unpark latency** ⇒ threads are still piling onto the lock, so contended
//!   acquisitions per operation stay HIGH even as utilisation collapses.
//! * contention **tracks the collapsing utilisation** instead ⇒ the remaining time is not threads
//!   blocking on each other, and **outcome 1 is dead with no duration measured at all.**
//!
//! # ⚠ EVERY NUMBER CARRIES ITS LAYER (Amendment 1's fourth condition)
//!
//! | mode | layer | locks that layer holds |
//! |------|-------|------------------------|
//! | `direct` | `TableBranchCatalog` driven directly, as `examples/fork_concurrency.rs` does — **the layer the recorded 21×/1.9× pair was measured at** | `logical` only |
//! | `pgwire` | the shipped pgwire server over a real TCP socket, forks issued as SQL — **what production does** | `ServerContext::catalog()` outermost → `AgentRuntime` state → `logical` |
//!
//! E.6 established those are not the same experiment. A number here without its layer named is
//! not citable, and the printer refuses to emit one.
//!
//! # `model` — forcing the detector to fire, in both directions
//!
//! A count that comes back low is worthless until the counter has been made to come back high.
//! `model` runs the SAME wrapper over a bare `Mutex<()>` with no fsync, no tree and no second
//! lock: T threads, each looping *hold H spins inside, then W spins outside*. Sweeping W/H walks
//! the same hold-shrinks/outside-grows transition the stub ladder walks, in a system where the
//! answer is known by construction. It gives:
//!
//! * a **must-fire** cell (H large, W=0 ⇒ contention per op must approach 1.0),
//! * a **must-not-fire** cell (T=1 ⇒ contention per op must be exactly 0.000),
//! * and the **shape a pure mutex produces** when the section shortens, which is what tells a
//!   reading on the real ladder from an instrument artifact.
//!
//! ```text
//! cargo run --release --features lock_census --example ceiling_lock_contention -- <mode> [args]
//!   model                                  the fire-check and the pure-mutex reference shape
//!   direct  [N] [T,T,T] [warm]             layer A: the L0→L3 ladder on TableBranchCatalog
//!   pgwire  [F] [T,T,T]                    layer B: the L0→L3 ladder through the shipped server
//! ```

use std::collections::BTreeSet;
use std::io::{BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::d123_probe as probe;
use ferrodb::branch::lockcount as lk;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::pgwire::{catalog_acquisitions, catalog_contended, serve, ServerContext};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const FIRST_CATALOG_PAGE_ID: u32 = 1;
const PROTOCOL_V3: i32 = 196_608;

/// Exit non-zero rather than print a number the run did not earn.
fn refuse(why: &str) -> ! {
    eprintln!("ceiling: REFUSING. {why}");
    std::process::exit(1);
}

fn stamp(label: &str) {
    let load = std::process::Command::new("uptime")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.split("age").last().unwrap_or("").trim().to_string())
        .unwrap_or_else(|| "unknown".into());
    let when = std::process::Command::new("date")
        .arg("-u")
        .arg("+%Y-%m-%dT%H:%M:%SZ")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into());
    println!("# {label}: {when}  load_at_acquire={load}");
}

// =================================================================================================
// model — the fire-check, and the reference shape a PURE mutex produces
// =================================================================================================

#[inline(never)]
fn spin(n: u64) {
    let mut acc = 0u64;
    for i in 0..n {
        acc = std::hint::black_box(acc.wrapping_add(i ^ 0x9e37_79b9_7f4a_7c15));
    }
    std::hint::black_box(acc);
}

struct ModelCell {
    contended_per_op: f64,
    acq_per_op: f64,
    ops: u64,
    secs: f64,
}

/// T threads × `iters` iterations: hold the mutex for `hold` spins, then `outside` spins free.
fn model_cell(t: usize, iters: u64, hold: u64, outside: u64) -> ModelCell {
    let m: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    lk::set_global_mode(false);
    lk::reset();
    lk::set_enabled(true);
    let t0 = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..t {
            let m = Arc::clone(&m);
            s.spawn(move || {
                for _ in 0..iters {
                    {
                        let mut g = lk::acquire_unwrap(&m, lk::LK_LOGICAL);
                        *g += 1;
                        spin(hold);
                    }
                    lk::bump_op();
                    spin(outside);
                }
                lk::flush_thread();
            });
        }
    });
    let secs = t0.elapsed().as_secs_f64();
    lk::set_enabled(false);
    let c = lk::snapshot();
    if c.threads as usize != t {
        refuse(&format!("model lost a thread's accumulators: {} of {t} flushed", c.threads));
    }
    if c.ops != iters * t as u64 {
        refuse(&format!("model counted {} ops, ran {}", c.ops, iters * t as u64));
    }
    ModelCell {
        contended_per_op: c.contended_per_op(lk::LK_LOGICAL),
        acq_per_op: c.acq_per_op(lk::LK_LOGICAL),
        ops: c.ops,
        secs,
    }
}

fn mode_model() {
    println!("CEILING — MODE=model. THE FIRE-CHECK, and the shape a PURE mutex produces.");
    println!("LAYER: none. A bare `Mutex<()>`: no fsync, no tree, no second lock. This cell exists");
    println!("to make the counter fire on purpose and to show what it reads when it should read 0.");
    stamp("model_start");
    println!();
    println!("  {:>16} {:>4} {:>9} {:>9} {:>8} {:>14} {:>10}", "cell", "T", "hold", "outside", "ops", "contended/op", "acq/op");

    // ── must NOT fire ────────────────────────────────────────────────────────────────────────
    let c = model_cell(1, 20_000, 2_000, 0);
    println!("  {:>16} {:>4} {:>9} {:>9} {:>8} {:>14.5} {:>10.4}", "NEG single-thread", 1, 2000, 0, c.ops, c.contended_per_op, c.acq_per_op);
    if c.contended_per_op != 0.0 {
        refuse(&format!(
            "NEGATIVE CONTROL FAILED: one thread on a private mutex reported {} contended \
             acquisitions per op. The counter fires spuriously and NOTHING else in this run is \
             interpretable.",
            c.contended_per_op
        ));
    }

    // ── must fire ────────────────────────────────────────────────────────────────────────────
    let c = model_cell(64, 400, 20_000, 0);
    println!("  {:>16} {:>4} {:>9} {:>9} {:>8} {:>14.5} {:>10.4}", "POS 64t all-lock", 64, 20000, 0, c.ops, c.contended_per_op, c.acq_per_op);
    if c.contended_per_op < 0.5 {
        refuse(&format!(
            "POSITIVE CONTROL FAILED: 64 threads doing nothing but hold one mutex reported only \
             {:.4} contended acquisitions per op. The counter does not fire when contention is \
             certain, so a low reading anywhere else in this run means nothing.",
            c.contended_per_op
        ));
    }

    // ── the reference shape: shrink the section, grow the outside work ───────────────────────
    println!();
    println!("  REFERENCE SHAPE — 64 threads, total work per iteration held CONSTANT at 20,000");
    println!("  spins, moved out of the section a step at a time. This is the stub ladder's own");
    println!("  transition (hold shrinks, outside grows) in a system with NO other term.");
    println!();
    println!("  {:>16} {:>4} {:>9} {:>9} {:>8} {:>14} {:>10}", "cell", "T", "hold", "outside", "ops", "contended/op", "acq/op");
    for (hold, outside) in [(20_000u64, 0u64), (10_000, 10_000), (2_000, 18_000), (500, 19_500), (100, 19_900)] {
        let c = model_cell(64, 400, hold, outside);
        println!(
            "  {:>16} {:>4} {:>9} {:>9} {:>8} {:>14.5} {:>10.4}",
            format!("U~{:.0}%", 100.0 * hold as f64 / 20_000.0),
            64, hold, outside, c.ops, c.contended_per_op, c.acq_per_op
        );
    }
    println!();
    println!("⭐ READ IT THIS WAY. In a PURE mutex the contended count is what a queue looks like:");
    println!("   it stays pinned near 1.0 while threads are actually blocking on each other, and");
    println!("   it falls only when they stop colliding. Whatever this column does here is the");
    println!("   reference the real ladder's column is compared against.");
}

// =================================================================================================
// direct — LAYER A: TableBranchCatalog, the layer the recorded 21x/1.9x pair was measured at
// =================================================================================================

fn open_catalog(dir: &Path, tag: &str) -> Arc<TableBranchCatalog> {
    let main_path = dir.join(format!("main-{tag}.db"));
    let mf = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&main_path)
        .unwrap();
    let _main_pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(mf).unwrap())));
    let path = dir.join(format!("branches-{tag}.branchcat"));
    let _ = std::fs::remove_file(&path);
    Arc::new(TableBranchCatalog::open_sidecar(&path, 1).expect("open catalog"))
}

struct DirectArm {
    forks: usize,
    secs: f64,
    syncs: u64,
    counts: lk::Counts,
}

/// One rung. Mirrors `d123_serial_attribution::run_arm` — same constructor, same warm-up shape,
/// same `n / t` fork-bounded loop — so the ladder is the recorded one and not a lookalike.
fn run_direct(dir: &Path, tag: &str, n: usize, t: usize, warm: usize, stub: u8, global: bool) -> DirectArm {
    let cat = open_catalog(dir, tag);
    let lease = LeaseDeadline(u64::MAX);

    // Warm with the stub OFF and the census OFF, so every rung starts from the same tree shape
    // whatever the timed loop is about to skip, and no warm fork lands in the counts.
    const WARM_THREADS: usize = 32;
    lk::set_enabled(false);
    probe::configure(false, 0, 0);
    let per_warm = warm / WARM_THREADS;
    if per_warm > 0 {
        std::thread::scope(|s| {
            for _ in 0..WARM_THREADS {
                let cat = Arc::clone(&cat);
                s.spawn(move || {
                    for _ in 0..per_warm {
                        cat.fork(BranchId::TRUNK, lease).expect("warm fork");
                    }
                });
            }
        });
    }

    // Probe OFF: this run's evidence is integers, so the clock is never read and the timing
    // instrument's own perturbation is not in the way. `stub` is the only thing that varies.
    probe::configure(false, stub, 0);
    lk::set_global_mode(global);
    lk::reset();
    lk::set_enabled(true);

    let syncs_before = cat.syncs_issued();
    let per = n / t.max(1);
    let total = per * t;

    let t0 = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..t {
            let cat = Arc::clone(&cat);
            s.spawn(move || {
                for _ in 0..per {
                    cat.fork(BranchId::TRUNK, lease).expect("fork");
                }
                lk::flush_thread();
            });
        }
    });
    let secs = t0.elapsed().as_secs_f64();
    lk::set_enabled(false);
    let counts = lk::snapshot();

    if counts.ops as usize != total {
        refuse(&format!(
            "{tag}: census counted {} forks, the harness ran {total}. A per-operation ratio \
             assembled from two disagreeing bookkeepings is not a measurement.",
            counts.ops
        ));
    }
    if !global && counts.threads as usize != t {
        refuse(&format!(
            "{tag}: {} of {t} threads flushed their accumulators. Loss shrinks the contended \
             count in the direction that looks like LESS contention, which is the direction that \
             would falsely kill outcome 1.",
            counts.threads
        ));
    }
    DirectArm { forks: total, secs, syncs: cat.syncs_issued() - syncs_before, counts }
}

fn mode_direct(dir: &Path, n: usize, threads: &[usize], warm: usize) {
    println!("CEILING — MODE=direct. THE L0→L3 KEY-COUNT LADDER, AS A CONTENTION COUNT.");
    println!();
    println!("⚠ LAYER: `TableBranchCatalog` driven directly (open_sidecar + cat.fork), holding");
    println!("  `logical` and NOTHING ELSE. This is the layer `examples/fork_concurrency.rs` drives");
    println!("  and the layer the recorded 21x/1.9x/12.6x pair was measured at. It is NOT what any");
    println!("  shipped front-end does — see MODE=pgwire. (E.6; INVENTION-TRIGGER Amendment 1.)");
    println!();
    println!("⛔ Stubbed levels are NOT a database. L1 writes no child record; L2 leaks retired");
    println!("   slots; L3 also drops the envelope (capability escape) and the parent's live-child");
    println!("   entry (GC hole). Every level KEEPS write_header + stage so >=1 page is dirty and");
    println!("   the fsync stays real — `syncs` is the check that this held.");
    println!();
    println!("   contended/op  = acquisitions of `logical` whose try_lock() failed, per fork.");
    println!("                   UPPER bound on blocking (holder may release in the window), so a");
    println!("                   LOW value is the STRONG direction. See src/branch/lockcount.rs.");
    println!("   acq/op        = acquisitions of `logical` per fork. Free, and it bounds the answer.");
    println!("   forks/sec     = UPPER BOUND ONLY. Box not quiet; stamped below. Counts are the evidence.");
    stamp("direct_start");
    println!();
    println!(
        "  {:>7} {:>5} {:>9} {:>9} {:>14} {:>10} {:>10} {:>8} {:>8}",
        "threads", "stub", "forks", "ops", "contended/op", "acq/op", "cont.frac", "syncs", "f/sync"
    );
    for &t in threads {
        let mut base_ct = f64::NAN;
        let mut base_tp = f64::NAN;
        let mut rows: Vec<(u8, f64, f64, f64)> = Vec::new();
        for stub in 0u8..=3 {
            let a = run_direct(dir, &format!("d{t}_{stub}"), n, t, warm, stub, false);
            let c = &a.counts;
            let ct = c.contended_per_op(lk::LK_LOGICAL);
            let tp = a.forks as f64 / a.secs;
            if stub == 0 {
                base_ct = ct;
                base_tp = tp;
            }
            if a.syncs == 0 {
                refuse(&format!(
                    "T={t} L{stub}: syncs collapsed to 0. The rung lost DURABILITY, not serial \
                     work, and D123's own validity gate voids it."
                ));
            }
            println!(
                "  {:>7} {:>5} {:>9} {:>9} {:>14.5} {:>10.4} {:>9.1}% {:>8} {:>8.1}",
                t,
                stub,
                a.forks,
                c.ops,
                ct,
                c.acq_per_op(lk::LK_LOGICAL),
                100.0 * c.contended_frac(lk::LK_LOGICAL),
                a.syncs,
                a.forks as f64 / a.syncs.max(1) as f64
            );
            rows.push((stub, ct, tp, a.secs));
        }
        println!("    L0→L3 at T={t}:  contended/op {:.5} → {:.5}  ({:.2}x)   [forks/sec {:.0} → {:.0}, {:.2}x, UPPER BOUND]",
            base_ct, rows[3].1,
            if base_ct > 0.0 { rows[3].1 / base_ct } else { f64::NAN },
            base_tp, rows[3].2, rows[3].2 / base_tp);
        println!();
    }

    // The cross-check that licenses the pgwire arm's global-atomic mode.
    println!("  CROSS-CHECK — the same rung counted BOTH ways. Thread-local accumulation is used");
    println!("  above; the pgwire arm must use process-wide atomics because the library owns its");
    println!("  connection threads. If the two modes disagree, one of them is losing counts.");
    let t = *threads.last().unwrap_or(&64);
    let tl = run_direct(dir, "xc_tl", n, t, warm, 0, false);
    let gl = run_direct(dir, "xc_gl", n, t, warm, 0, true);
    let a = tl.counts.contended_per_op(lk::LK_LOGICAL);
    let b = gl.counts.contended_per_op(lk::LK_LOGICAL);
    println!("    T={t} L0  thread-local {a:.5}   global-atomic {b:.5}   ratio {:.4}", b / a.max(f64::MIN_POSITIVE));
    println!("    (acq/op {:.4} vs {:.4}; ops {} vs {})",
        tl.counts.acq_per_op(lk::LK_LOGICAL), gl.counts.acq_per_op(lk::LK_LOGICAL), tl.counts.ops, gl.counts.ops);
    println!();
    println!("⭐ THE DISCRIMINATOR (INVENTION-TRIGGER Amendment 2, pre-registered):");
    println!("   contention per op RISES as the section shortens  ⇒ the gap is park/unpark handoff.");
    println!("   it does NOT rise                                 ⇒ the remaining time is not threads");
    println!("                                                      blocking on each other, and");
    println!("                                                      pre-registered OUTCOME 1 IS DEAD.");
}

// =================================================================================================
// pgwire — LAYER B: what production actually does
// =================================================================================================

struct Client {
    w: TcpStream,
    r: BufReader<TcpStream>,
}

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
        for (k, v) in [("user", "ceiling"), ("database", "ceiling")] {
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

struct Rig {
    addr: std::net::SocketAddr,
    _dir: PathBuf,
}

/// The shape `examples/pgserver.rs` ships, lifted from `examples/e6_outer_lock_count.rs::rig` so
/// the two harnesses drive the same server and their counts are comparable.
///
/// **No `LeaseThread`**, for E.6's reason: its periodic scan takes the same outermost mutex, and
/// those acquisitions are not attributable to client statements. Leaving it out makes every outer
/// count a LOWER bound on production, which is the safe direction here.
fn rig(root: &Path, tag: &str) -> Rig {
    let dir = root.join(format!("ceiling-{tag}-{}", std::process::id()));
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
        ArenaPageStore::new(bp.clone(), branches.clone() as Arc<dyn BranchCatalog>, base).unwrap(),
    );
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
    std::thread::spawn(move || {
        let _ = serve(listener, ctx);
    });
    Rig { addr, _dir: dir }
}

fn outer_acq() -> u64 {
    catalog_acquisitions().unwrap_or_else(|| {
        refuse(
            "`pgwire::catalog_acquisitions()` is None: built without `--features lock_census`. \
             That is not zero acquisitions, it is no measurement.",
        )
    })
}

fn outer_cont() -> u64 {
    catalog_contended().unwrap_or_else(|| {
        refuse("`pgwire::catalog_contended()` is None: built without `--features lock_census`.")
    })
}

struct PgArm {
    forks: usize,
    secs: f64,
    outer_acq: u64,
    outer_cont: u64,
    inner: lk::Counts,
}

fn run_pgwire(root: &Path, tag: &str, t: usize, f: usize, stub: u8) -> PgArm {
    let rig = rig(root, tag);
    let addr = rig.addr;

    probe::configure(false, stub, 0);
    // GLOBAL mode: `serve()` owns the connection threads, so no thread-local can be flushed
    // before the counters are read. See `lockcount::GLOBAL_MODE`.
    lk::set_global_mode(true);
    lk::reset();
    lk::set_enabled(true);
    // Snapshot AFTER the rig is built: `Catalog::create` and the runtime's construction are
    // fixture cost, not statement cost.
    let oa0 = outer_acq();
    let oc0 = outer_cont();

    let mut reported: BTreeSet<String> = BTreeSet::new();
    let mut asked = 0usize;
    let t0 = Instant::now();
    std::thread::scope(|s| {
        let mut hs = Vec::with_capacity(t);
        for th in 0..t {
            hs.push(s.spawn(move || {
                let mut names: Vec<String> = Vec::new();
                let mut asked = 0usize;
                let mut c = Client::connect(addr).expect("connect");
                for i in 0..f {
                    let r = c
                        .query(&format!("BEGIN AGENT SESSION AS 'a{th}' RUN 'r{th}_{i}';"))
                        .expect("BEGIN AGENT SESSION round trip");
                    if let Some(e) = &r.error {
                        refuse(&format!(
                            "BEGIN AGENT SESSION returned an error: {e}. A refused fork is a fork \
                             that did not happen; averaging it in reports a mediated path as an \
                             unmediated one."
                        ));
                    }
                    names.extend(branch_names(&r));
                    asked += 1;
                    let r = c.query("ABANDON;").expect("ABANDON round trip");
                    if let Some(e) = &r.error {
                        refuse(&format!("ABANDON returned an error: {e}"));
                    }
                }
                let _ = c.terminate();
                (names, asked)
            }));
        }
        for h in hs {
            let (names, a) = h.join().expect("client thread");
            reported.extend(names);
            asked += a;
        }
    });
    let secs = t0.elapsed().as_secs_f64();
    lk::set_enabled(false);

    if reported.len() != asked {
        refuse(&format!(
            "{tag}: asked for {asked} forks but the server named {} distinct branches. The fork \
             total may not come from this harness's own counter when the two witnesses disagree.",
            reported.len()
        ));
    }
    let inner = lk::snapshot();
    PgArm {
        forks: asked,
        secs,
        outer_acq: outer_acq() - oa0,
        outer_cont: outer_cont() - oc0,
        inner,
    }
}

fn mode_pgwire(root: &Path, f: usize, threads: &[usize]) {
    println!("CEILING — MODE=pgwire. THE SAME L0→L3 LADDER, AT THE LAYER PRODUCTION RUNS.");
    println!();
    println!("⚠ LAYER: the shipped pgwire server over a real TCP socket, one client connection per");
    println!("  thread, forks issued as `BEGIN AGENT SESSION`. Locks held, outermost first:");
    println!("  `ServerContext::catalog()` -> `AgentRuntime` state -> `TableBranchCatalog::logical`.");
    println!("  E.6 measured that this is NOT the same experiment as MODE=direct.");
    println!();
    println!("  outer = ServerContext::catalog(), the per-statement mutex every connection waits on.");
    println!("  inner = TableBranchCatalog::logical, the mutex the 21x/1.9x pair is about.");
    println!("  Counted with process-wide atomics (the library owns these threads) — the `direct`");
    println!("  mode's cross-check is what licenses that.");
    stamp("pgwire_start");
    println!();
    println!(
        "  {:>7} {:>5} {:>7} {:>10} {:>12} {:>10} {:>12} {:>9}",
        "threads", "stub", "forks", "outer/op", "outerCont/op", "inner/op", "innerCont/op", "stmts/sec"
    );
    for &t in threads {
        for stub in 0u8..=3 {
            let a = run_pgwire(root, &format!("p{t}_{stub}"), t, f, stub);
            let ops = a.forks.max(1) as f64;
            if a.inner.ops as usize != a.forks {
                refuse(&format!(
                    "T={t} L{stub}: the inner census counted {} forks against {} the server named. \
                     A per-operation ratio from two disagreeing witnesses is not a measurement.",
                    a.inner.ops, a.forks
                ));
            }
            println!(
                "  {:>7} {:>5} {:>7} {:>10.4} {:>12.5} {:>10.4} {:>12.5} {:>9.1}",
                t,
                stub,
                a.forks,
                a.outer_acq as f64 / ops,
                a.outer_cont as f64 / ops,
                a.inner.acq_per_op(lk::LK_LOGICAL),
                a.inner.contended_per_op(lk::LK_LOGICAL),
                a.forks as f64 / a.secs
            );
        }
        println!();
    }
    println!("⚠ stmts/sec is an UPPER BOUND on a loaded box and is NOT evidence here. Each fork is");
    println!("  two TCP round trips, so this arm's throughput is bounded by the socket, not the lock.");
}

// =================================================================================================

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "model".into());
    let n: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(8000);
    let threads: Vec<usize> = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "1,8,64".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let warm: usize = std::env::args().nth(4).and_then(|s| s.parse().ok()).unwrap_or(0);

    let dir = std::env::temp_dir().join(format!("ferrodb-ceiling-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    println!("⛔ CEILING MEASUREMENT SCAFFOLD — branch CEILING-park-or-structure, MUST NEVER MERGE.");
    println!();
    match mode.as_str() {
        "model" => mode_model(),
        "direct" => mode_direct(&dir, n, &threads, warm),
        "pgwire" => mode_pgwire(&dir, n, &threads),
        other => refuse(&format!("unknown mode `{other}` (model | direct | pgwire)")),
    }
    stamp("end");
    let _ = std::fs::remove_dir_all(&dir);
}
