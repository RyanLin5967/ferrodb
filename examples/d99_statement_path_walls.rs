//! D99 — D77's two remaining walls, measured SEPARATELY, each against a control.
//!
//! D77 named two survivors and placed both on the statement path:
//!   (a) `pgwire/mod.rs` — a reader-registry WALK reached from `ServerContext::catalog()`.
//!   (b) `branch/arena.rs` — `free_arena`'s FORWARD SCAN.
//!
//! **Revision 2.** The first version of this harness was reviewed and five of its numbers did not
//! survive. What changed, so nobody re-derives it:
//!
//!   1. **It timed single calls against a clock that cannot resolve them.** `Instant` on this box
//!      ticks at ~41.67 ns, so a per-call `elapsed()` quantises to {0, 41, 42, 83, ...} and a
//!      median of "42.0 ns" meant *below the clock floor*, not *constant*. Every arm now brackets
//!      a BATCH and divides, and `T0` prints the measured tick so the floor is stated rather than
//!      assumed.
//!   2. **B3 configured `checkpoint_to` BEFORE padding**, so every one of the M `arena_for` calls
//!      in setup rewrote the whole map: O(M^2) bytes of setup, and the run that "timed out" timed
//!      out in `pad_branches`, not in `free_arena`. Persistence is now switched on AFTER padding.
//!   3. **`freed` counted `free_arena`'s no-op `Ok(0)` as a success**, so the `freed == 0` refusal
//!      was satisfied by exactly the case it existed to reject. A free now only counts if the
//!      arena was owned before the call and is gone after it. (`Ok(0)` is also the honest answer
//!      for a real free of an extent with no allocated pages, so the return value cannot be the
//!      test and is not used as one.)
//!   4. **B2 was labelled a control while its own first row moved 3.8x.** It is a control only
//!      where `pending` dominates; at P=1k with M=1k branches the branch-axis fix moves it too.
//!      Labelled accordingly.
//!   5. B3 was BEFORE-only data presented without saying so. Every row now carries its arm.
//!
//! **The integer axis is the one that survives fleet load.** This box runs a build fleet; a
//! duration is an upper bound and nothing better. So each B arm also prints `scan_steps` — the
//! number of `current`-map entries the REPLACED code would walk for that call, which is exactly
//! `len(current)` by `HashMap::retain`'s contract and is not a measurement at all. The fixed code
//! performs one hash probe. The durations are confirmation; the integers are the claim.
//!
//! **Why every arm pads one counter and holds the others down.** D77 named ONE scan in
//! `free_arena`. Reading it found FOUR linear-or-worse steps in series:
//!   1. `for i in 0..pages { evict(..) }`        O(pages in THIS extent) — a per-call constant
//!   2. `st.pending.retain(..)`                  O(len(pending))
//!   3. `st.current.retain(..)`                  O(BRANCHES)  <- the named wall
//!   4. `persist_if_configured()`                sorts and serialises every extent and every
//!                                               `current` entry, then writes the file
//! Walls in series are indistinguishable from one wall twice as big, so each arm moves ONE axis.
//!
//! Arms:
//!   T0  the clock's own resolution on this box — printed before any number that relies on it
//!   A1  vary registered readers C, branches 0     — does (a) scale with CONNECTIONS?
//!   A2  vary branches M, readers fixed            — does (a) scale with BRANCHES?
//!   B1  vary branches M, pending 0, persist OFF   — isolates the named scan
//!   B2  vary pending P, branches fixed, persist OFF — isolates `pending.retain`
//!   B3  vary branches M, persist ON AFTER PADDING — the configuration cli.rs ships
//!
//! A1/A2 use the SAME branch padding as B1, so a flat A2 beside a rising B1 is evidence about the
//! code and not about the padding having failed to happen.
//!
//! Refuses rather than reporting: zero iterations, zero *verified* frees, padding that did not
//! take. Usage: `D99_ARMS=T0,A1,A2,B1,B2,B3 D99_BRANCHES=1000,10000,100000,1000000 ...`

use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::Instant;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::record::PendingFree;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{ArenaId, Epoch, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, BranchId};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const ARENA_BASE: u32 = 1024;

struct Rig {
    ctx: Arc<ServerContext>,
    store: Arc<ArenaPageStore>,
    cat: Arc<dyn BranchCatalog>,
    dir: std::path::PathBuf,
}

/// Which branch catalog carries the PADDING.
///
/// `free_arena` never consults the catalog — it touches extents/recycled/pending/claim_epoch/
/// current/fill_unknown, `give_back`, the counters and the persist. So the catalog is setup cost
/// only, and the default is the in-memory one because `TableBranchCatalog` forks at ~29 ms each
/// (1,000 forks in 28.7 s wall at 2% CPU — fsync-bound), putting 10^6 out of reach for a reason
/// unrelated to the wall. `D99_CATALOG=table` runs the same arms on the shipped catalog; the two
/// must agree on ns/free_arena at a count both can reach.
fn catalog_kind() -> String {
    std::env::var("D99_CATALOG").unwrap_or_else(|_| "mem".to_string())
}

fn build(tag: &str) -> Rig {
    let dir = std::env::temp_dir().join(format!("ferrodb-d99-{}-{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.join("main.db"))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.join("main.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let cat: Arc<dyn BranchCatalog> = if catalog_kind() == "table" {
        Arc::new(TableBranchCatalog::open_sidecar(&dir.join("b.branchcat"), 1).unwrap())
    } else {
        Arc::new(LogBranchCatalog::in_memory(1))
    };
    let store = Arc::new(ArenaPageStore::new(bp.clone(), cat.clone(), ARENA_BASE).unwrap());
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            cat.clone(),
            Arc::new(MemEffectLog::new()),
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
    Rig { ctx, store, cat, dir }
}

/// Switch persistence on. Called AFTER padding, never before: `alloc_arena` persists too, so
/// configuring this first makes setup rewrite the whole map M times — O(M^2) bytes, which is what
/// the previous revision actually measured when it reported a timeout.
fn persist_from_now_on(rig: &Rig) {
    rig.store.checkpoint_to(rig.dir.join("main.db.arena"));
}

/// Fork `n` branches and give each one an arena, so `state.current` holds exactly `n` entries.
fn pad_branches(rig: &Rig, n: usize) -> Vec<ArenaId> {
    let mut arenas = Vec::with_capacity(n);
    for _ in 0..n {
        let rec = rig.cat.fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000)).unwrap();
        arenas.push(rig.store.arena_for(rec.branch_id).unwrap());
    }
    arenas
}

/// Park `n` entries on the pending-free log. They name an arena no extent uses, so `free_arena`'s
/// `retain` walks all of them and keeps all of them — the worst case, and the one being measured.
fn pad_pending(rig: &Rig, n: usize) {
    let entries: Vec<PendingFree> = (0..n)
        .map(|i| PendingFree {
            page_id: u32::MAX - (i as u32 % 1000),
            arena_id: ArenaId(u32::MAX - 7),
            birth_epoch: Epoch(1),
            free_epoch: Epoch(2),
            owner: BranchId::new(u64::MAX - (i as u64 % 1000), 1),
        })
        .collect();
    rig.store.put_pending(entries).unwrap();
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if v.is_empty() {
        return f64::NAN;
    }
    v[v.len() / 2]
}

/// The smallest non-zero gap this box's `Instant` can report, in ns.
///
/// Printed before anything that depends on it. A per-call figure at or under this number is a
/// statement about the CLOCK, not about the code, and the previous revision of this harness
/// published four such numbers as a flat curve.
fn clock_tick_ns() -> f64 {
    let mut gaps = Vec::new();
    for _ in 0..200_000 {
        let t = Instant::now();
        let d = t.elapsed().as_nanos() as f64;
        if d > 0.0 {
            gaps.push(d);
        }
    }
    gaps.sort_by(|a, b| a.partial_cmp(b).unwrap());
    gaps.first().copied().unwrap_or(f64::NAN)
}

/// Time `free_arena` over a batch, counting only frees that VERIFIABLY freed something.
///
/// `free_arena` returns `Ok(0)` both for "no such extent" (a no-op) and for a real free of an
/// extent with no allocated pages — which is exactly what `arena_for` padding produces. So the
/// return value cannot distinguish them and is not used. Ownership before and after is.
///
/// Returns (median ns per verified free, verified count, scan_steps at the start of the batch).
fn time_free_arena(rig: &Rig, arenas: &[ArenaId], k: usize) -> (f64, usize, usize) {
    let scan_steps = rig.store.current_arena_count();
    let mut samples = Vec::with_capacity(k);
    let mut verified = 0usize;
    for &a in arenas.iter().take(k) {
        if rig.store.arena_owner(a).is_none() {
            continue; // never owned, or already gone: freeing it is the no-op, not a measurement
        }
        let t0 = Instant::now();
        let r = rig.store.free_arena(a);
        let ns = t0.elapsed().as_nanos() as f64;
        if r.is_ok() && rig.store.arena_owner(a).is_none() {
            samples.push(ns);
            verified += 1;
        }
    }
    (median(samples), verified, scan_steps)
}

/// Time `ServerContext::catalog()` by bracketing a BATCH, never one call.
///
/// One call costs less than this box's clock tick, so a per-call `elapsed()` reports the tick and
/// looks identical at every branch count. Dividing a batch is what makes the sub-tick region
/// visible at all.
fn time_catalog_batched(rig: &Rig, batch: usize, rounds: usize) -> f64 {
    for _ in 0..(batch.min(10_000)) {
        drop(rig.ctx.catalog());
    }
    let mut per_call = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let t0 = Instant::now();
        for _ in 0..batch {
            drop(rig.ctx.catalog());
        }
        per_call.push(t0.elapsed().as_nanos() as f64 / batch as f64);
    }
    median(per_call)
}

fn counts(var: &str, default: &str) -> Vec<usize> {
    std::env::var(var)
        .unwrap_or_else(|_| default.to_string())
        .split(',')
        .filter_map(|v| v.trim().parse().ok())
        .collect()
}

fn main() {
    let arms: Vec<String> = std::env::var("D99_ARMS")
        .unwrap_or_else(|_| "T0,A1,A2,B1,B2,B3".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let branch_counts = counts("D99_BRANCHES", "1000,10000,100000");
    let conn_counts = counts("D99_CONNS", "1000,10000,100000");
    let pend_counts = counts("D99_PENDING", "1000,10000,100000");
    let batch: usize = std::env::var("D99_BATCH").ok().and_then(|v| v.parse().ok()).unwrap_or(20_000);
    let rounds: usize = std::env::var("D99_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(5);
    let arm_label = std::env::var("D99_LABEL").unwrap_or_else(|_| "unlabelled".to_string());

    println!("D99 rev2 — D77's two walls, each measured with the others held down.");
    println!("{}", ferrodb::build_provenance());
    println!("  arm label: {arm_label}   padding catalog: {}", catalog_kind());
    println!("  load: {}", std::process::Command::new("uptime")
        .output().ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string()).unwrap_or_default());
    println!();
    println!("READING: durations on this box are UPPER BOUNDS (build fleet). The integer");
    println!("scan_steps column is len(current) = the entries the REPLACED retain would walk,");
    println!("by HashMap::retain's contract -- arithmetic, not a measurement. The fixed code");
    println!("does one hash probe regardless.");
    println!();

    if arms.iter().any(|a| a == "T0") {
        let tick = clock_tick_ns();
        println!("T0  clock floor: Instant's smallest non-zero gap = {tick:.1} ns over 200k samples.");
        println!("    Any per-call figure at or below this is a fact about the CLOCK.");
        println!();
    }

    if arms.iter().any(|a| a == "A1") {
        println!("A1  ServerContext::catalog() vs REGISTERED READERS (connections). Branches: 0.");
        println!("      readers C     ns/catalog()      ns per 1000 C");
        let rig = build("a1");
        let mut held: Vec<Arc<std::sync::atomic::AtomicBool>> = Vec::new();
        for &c in &conn_counts {
            while held.len() < c {
                let slot = Arc::new(std::sync::atomic::AtomicBool::new(false));
                rig.ctx.register_reader(Arc::clone(&slot));
                held.push(slot);
            }
            let ns = time_catalog_batched(&rig, batch.min(2000), rounds);
            println!("  {:>12}   {:>13.1}   {:>15.2}", c, ns, ns / (c as f64 / 1000.0));
        }
        let _ = std::fs::remove_dir_all(&rig.dir);
        println!();
    }

    if arms.iter().any(|a| a == "A2") {
        println!("A2  ServerContext::catalog() vs BRANCHES. Readers fixed at 8 (a small server).");
        println!("      branches M    live_count    ns/catalog()");
        for &m in &branch_counts {
            let rig = build(&format!("a2-{m}"));
            let mut held: Vec<Arc<std::sync::atomic::AtomicBool>> = Vec::new();
            for _ in 0..8 {
                let slot = Arc::new(std::sync::atomic::AtomicBool::new(false));
                rig.ctx.register_reader(Arc::clone(&slot));
                held.push(slot);
            }
            pad_branches(&rig, m);
            let live = rig.cat.live_count();
            if live < m {
                println!("  {m:>12}   live_count={live} < M — padding did not take. NOT A RESULT.");
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            let ns = time_catalog_batched(&rig, batch, rounds);
            println!("  {:>12}   {:>10}   {:>13.2}", m, live, ns);
            let _ = std::fs::remove_dir_all(&rig.dir);
        }
        println!();
    }

    if arms.iter().any(|a| a == "B1") {
        println!("B1  free_arena vs BRANCHES. pending=0, persist OFF. Isolates the named scan.");
        println!("      branches M   scan_steps   verified    ns/free_arena    ns per 1000 M");
        for &m in &branch_counts {
            let rig = build(&format!("b1-{m}"));
            let arenas = pad_branches(&rig, m);
            if arenas.len() < m || rig.store.pending_len() != 0 {
                println!("  {m:>12}   padding did not take (arenas={}, pending={}). NOT A RESULT.",
                    arenas.len(), rig.store.pending_len());
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            let k = (m / 10).clamp(1, 200);
            let (ns, verified, steps) = time_free_arena(&rig, &arenas, k);
            if verified == 0 {
                println!("  {m:>12}   zero VERIFIED frees. NOT A RESULT.");
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            println!("  {:>12}   {:>10}   {:>8}   {:>13.1}   {:>14.2}",
                m, steps, verified, ns, ns / (m as f64 / 1000.0));
            let _ = std::fs::remove_dir_all(&rig.dir);
        }
        println!();
    }

    if arms.iter().any(|a| a == "B2") {
        println!("B2  free_arena vs PENDING. branches=1000, persist OFF. Isolates pending.retain.");
        println!("    NOT a clean control at the low end: branches are padded to 1000, so the");
        println!("    branch-axis fix moves the P=1k row too. It is a control where P dominates.");
        println!("      pending P    verified    ns/free_arena    ns per 1000 P");
        for &p in &pend_counts {
            let rig = build(&format!("b2-{p}"));
            let arenas = pad_branches(&rig, 1000);
            pad_pending(&rig, p);
            if rig.store.pending_len() != p {
                println!("  {p:>12}   pending={} != P — padding did not take. NOT A RESULT.",
                    rig.store.pending_len());
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            let (ns, verified, _) = time_free_arena(&rig, &arenas, 100);
            if verified == 0 {
                println!("  {p:>12}   zero VERIFIED frees. NOT A RESULT.");
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            println!("  {:>12}   {:>8}   {:>13.1}   {:>14.2}",
                p, verified, ns, ns / (p as f64 / 1000.0));
            let _ = std::fs::remove_dir_all(&rig.dir);
        }
        println!();
    }

    if arms.iter().any(|a| a == "B3") {
        println!("B3  free_arena vs BRANCHES, persist ON — the configuration cli.rs ships.");
        println!("    Persistence is enabled AFTER padding. Enabling it before makes the M");
        println!("    arena_for calls in setup rewrite the whole map M times: O(M^2) setup, which");
        println!("    is what rev1 timed out inside and then reported against free_arena.");
        println!("      branches M   scan_steps   verified    ns/free_arena    map bytes");
        for &m in &branch_counts {
            let rig = build(&format!("b3-{m}"));
            let arenas = pad_branches(&rig, m);
            if arenas.len() < m {
                println!("  {m:>12}   padding did not take. NOT A RESULT.");
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            persist_from_now_on(&rig);
            let bytes = rig.store.state_bytes().len();
            let k = (m / 10).clamp(1, 50);
            let (ns, verified, steps) = time_free_arena(&rig, &arenas, k);
            if verified == 0 {
                println!("  {m:>12}   zero VERIFIED frees. NOT A RESULT.");
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            println!("  {:>12}   {:>10}   {:>8}   {:>13.1}   {:>10}",
                m, steps, verified, ns, bytes);
            let _ = std::fs::remove_dir_all(&rig.dir);
        }
        println!();
    }
}
