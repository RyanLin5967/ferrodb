//! D99 — the two branch-count walls D77 left open, measured SEPARATELY, each against a control.
//!
//! D77's structural enumeration named two survivors and placed both on the statement path:
//!
//!   (a) `pgwire/mod.rs` — a reader-registry WALK reached from `ServerContext::catalog()`.
//!   (b) `branch/arena.rs` — `free_arena`'s FORWARD SCAN.
//!
//! **Neither label survives being read, and the point of this harness is to settle which parts of
//! them survive being MEASURED.** Two facts are visible in the source before any timing:
//!
//! * `register_reader` is called once per TCP CONNECTION (`pgwire/mod.rs`, in the startup path),
//!   and the registry self-cleans by refcount. Its N is live connections. The branch count does
//!   not enter it anywhere. So (a) may be a wall, but if it is, it is not a BRANCH-count wall —
//!   and this project's objective is 10^6 branches, not 10^6 connections.
//! * `free_arena` contains **four** linear-or-worse steps in series, not one:
//!     1. `for i in 0..pages { evict(..) }`      — O(pages in THIS extent), a per-call constant
//!     2. `st.pending.retain(..)`                — O(len(pending))
//!     3. `st.current.retain(|_, a| *a != arena)` — O(BRANCHES)   <- the named wall
//!     4. `persist_if_configured()` -> `state_bytes()` — sorts AND serialises every extent and
//!        every `current` entry, then writes the whole file: O((A + M) log(A + M)) + an IO.
//!
//!   Step 4 is not hypothetical in production: `cli.rs` and `examples/pgserver.rs` both call
//!   `checkpoint_to`. D79 already found that every 10^6 result this project published had it OFF.
//!
//! **Walls in series are indistinguishable from one wall twice as big**, which this project has
//! already been caught by. So every arm below pads ONE counter and holds the others down, and the
//! checkpoint is measured both OFF and ON rather than assumed either way.
//!
//! Arms:
//!   A1  vary registered readers C, branch padding 0        — does (a) scale with CONNECTIONS?
//!   A2  vary branches M, readers fixed                     — does (a) scale with BRANCHES?
//!   B1  vary branches M, pending 0, checkpoint OFF         — isolates `current.retain`
//!   B2  vary pending P, branches fixed, checkpoint OFF     — isolates `pending.retain`
//!   B3  vary branches M, pending 0, checkpoint ON          — the configuration production runs
//!
//! A1 and A2 use the SAME branch padding as B1, so a flat A2 beside a rising B1 is evidence about
//! the code and not about the padding having failed to happen.
//!
//! Reports ns per operation. A slope across three decades separates a constant from a complexity
//! class; a single before/after ratio cannot. Refuses rather than printing a number it cannot
//! attribute: zero iterations, or padding that did not take.
//!
//! Usage: `D99_ARMS=A1,A2,B1,B2,B3 D99_BRANCHES=1000,10000,100000 d99_statement_path_walls`

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

/// Everything the two walls need, wired the way `cli.rs` wires it.
struct Rig {
    ctx: Arc<ServerContext>,
    store: Arc<ArenaPageStore>,
    cat: Arc<dyn BranchCatalog>,
    dir: std::path::PathBuf,
}

/// Which branch catalog carries the PADDING.
///
/// `free_arena` never consults the catalog — it reads `extents`, `recycled`, `pending`,
/// `claim_epoch` and `current`, calls `give_back`, and persists. So the catalog is setup cost
/// only, and the default is the in-memory one because `TableBranchCatalog` forks at ~29ms each
/// (measured: 1000 forks in 28.7s wall at 2% CPU — fsync-bound), which puts 10^6 branches out of
/// reach for a reason that has nothing to do with the wall under test.
///
/// `D99_CATALOG=table` runs the same arms on the shipped catalog. The two must agree on
/// ns/free_arena at a count both can reach, and B1x below is that cross-check: if they disagree,
/// the instrument changed the answer and no result here is admissible.
fn catalog_kind() -> String {
    std::env::var("D99_CATALOG").unwrap_or_else(|_| "mem".to_string())
}

fn build(tag: &str, persist: bool) -> Rig {
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
    let branches: Arc<dyn BranchCatalog> = cat.clone();
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), ARENA_BASE).unwrap());
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    // The shipped binary calls this (`cli.rs`). Measuring only with it OFF is how a wall that
    // production pays every time stays invisible — D79's finding, applied here as an arm.
    if persist {
        store.checkpoint_to(dir.join("main.db.arena"));
    }
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
    Rig { ctx, store, cat, dir }
}

/// Fork `n` branches and give each one an arena, so `state.current` holds exactly `n` entries.
///
/// Direct to the store rather than through `BEGIN AGENT SESSION`: the question is what the arena
/// map costs, and routing it through SQL would pay for the planner and the executor on an axis
/// that has nothing to do with either.
fn pad_branches(rig: &Rig, n: usize) -> Vec<ArenaId> {
    let mut arenas = Vec::with_capacity(n);
    for _ in 0..n {
        let rec = rig.cat.fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000)).unwrap();
        // `arena_for` -> `alloc_arena` inserts into `current`, which is the counter under test.
        arenas.push(rig.store.arena_for(rec.branch_id).unwrap());
    }
    arenas
}

/// Park `n` entries on the pending-free log without freeing anything real.
///
/// The entries name an arena id that no extent uses, so `free_arena`'s `retain` walks all of them
/// and keeps all of them — which is the worst case and the one the scan is being measured for.
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

/// Time `free_arena` on `k` of the padded arenas, one call at a time.
///
/// Returns the median nanoseconds for a single call. Freeing shrinks `current` by one each time,
/// so `k` is kept an order of magnitude below the padding: the axis must not move while it is
/// being read.
fn time_free_arena(rig: &Rig, arenas: &[ArenaId], k: usize) -> (f64, usize) {
    let mut samples = Vec::with_capacity(k);
    let mut freed = 0usize;
    for &a in arenas.iter().take(k) {
        let t0 = Instant::now();
        let r = rig.store.free_arena(a);
        let ns = t0.elapsed().as_nanos() as f64;
        if r.is_ok() {
            samples.push(ns);
            freed += 1;
        }
    }
    (median(samples), freed)
}

/// Time `ServerContext::catalog()`, which is what calls `drain_readers`.
fn time_catalog(rig: &Rig, iters: usize) -> f64 {
    // Warm the lock and the page cache so the first acquisition is not the measurement.
    for _ in 0..1000 {
        drop(rig.ctx.catalog());
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let g = rig.ctx.catalog();
        let ns = t0.elapsed().as_nanos() as f64;
        drop(g);
        samples.push(ns);
    }
    median(samples)
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
        .unwrap_or_else(|_| "A1,A2,B1,B2,B3".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let branch_counts = counts("D99_BRANCHES", "1000,10000,100000");
    let conn_counts = counts("D99_CONNS", "1000,10000,100000");
    let pend_counts = counts("D99_PENDING", "1000,10000,100000");
    let iters: usize = std::env::var("D99_ITERS").ok().and_then(|v| v.parse().ok()).unwrap_or(2000);

    println!("D99 — the two walls D77 left open, each measured with the others held down.");
    println!("{}", ferrodb::build_provenance());
    println!("  padding catalog: {} (free_arena never reads the catalog; setup cost only)", catalog_kind());
    println!();
    println!("PRE-REGISTERED READING:");
    println!("  flat across the axis  -> the axis is not this code's N. Wall refuted ON THAT AXIS.");
    println!("  ns proportional to N  -> O(N). Linear, whatever the constant.");
    println!("  ns ~ log N            -> cleared the bar.");
    println!();

    // ---------------- A1: does drain_readers scale with CONNECTIONS? ----------------
    if arms.iter().any(|a| a == "A1") {
        println!("A1  ServerContext::catalog() vs REGISTERED READERS (connections). Branches: 0.");
        println!("      readers C     ns/catalog()      ns per 1000 C");
        let rig = build("a1", false);
        // Held for the whole arm: `register_reader` retains only slots whose Arc someone else
        // still holds, so padding that is dropped immediately would silently not happen.
        let mut held: Vec<Arc<std::sync::atomic::AtomicBool>> = Vec::new();
        let mut prev = 0usize;
        for &c in &conn_counts {
            while held.len() < c {
                let slot = Arc::new(std::sync::atomic::AtomicBool::new(false));
                rig.ctx.register_reader(Arc::clone(&slot));
                held.push(slot);
            }
            let ns = time_catalog(&rig, iters);
            println!("  {:>12}   {:>13.1}   {:>15.2}", c, ns, ns / (c as f64 / 1000.0));
            prev = c;
        }
        let _ = prev;
        let _ = std::fs::remove_dir_all(&rig.dir);
        println!();
    }

    // ---------------- A2: does drain_readers scale with BRANCHES? ----------------
    if arms.iter().any(|a| a == "A2") {
        println!("A2  ServerContext::catalog() vs BRANCHES. Readers fixed at 8 (a small server).");
        println!("      branches M    live_count    ns/catalog()");
        for &m in &branch_counts {
            let rig = build(&format!("a2-{m}"), false);
            let mut held: Vec<Arc<std::sync::atomic::AtomicBool>> = Vec::new();
            for _ in 0..8 {
                let slot = Arc::new(std::sync::atomic::AtomicBool::new(false));
                rig.ctx.register_reader(Arc::clone(&slot));
                held.push(slot);
            }
            pad_branches(&rig, m);
            // Refuse rather than report: if the padding did not take, the axis is fiction.
            let live = rig.cat.live_count();
            if live < m {
                println!("  {m:>12}   live_count={live} < M — padding did not take. NOT A RESULT.");
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            let ns = time_catalog(&rig, iters);
            println!("  {:>12}   {:>10}   {:>13.1}", m, live, ns);
            let _ = std::fs::remove_dir_all(&rig.dir);
        }
        println!();
    }

    // ---------------- B1: free_arena vs branches, checkpoint OFF ----------------
    if arms.iter().any(|a| a == "B1") {
        println!("B1  free_arena vs BRANCHES. pending=0, checkpoint OFF. Isolates current.retain.");
        println!("      branches M    freed    ns/free_arena    ns per 1000 M");
        for &m in &branch_counts {
            let rig = build(&format!("b1-{m}"), false);
            let arenas = pad_branches(&rig, m);
            if arenas.len() < m || rig.store.pending_len() != 0 {
                println!("  {m:>12}   padding did not take (arenas={}, pending={}). NOT A RESULT.",
                    arenas.len(), rig.store.pending_len());
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            let k = (m / 10).clamp(1, 200);
            let (ns, freed) = time_free_arena(&rig, &arenas, k);
            if freed == 0 {
                println!("  {m:>12}   zero frees succeeded. NOT A RESULT.");
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            println!("  {:>12}   {:>6}   {:>13.1}   {:>14.2}",
                m, freed, ns, ns / (m as f64 / 1000.0));
            let _ = std::fs::remove_dir_all(&rig.dir);
        }
        println!();
    }

    // ---------------- B2: free_arena vs pending, checkpoint OFF ----------------
    if arms.iter().any(|a| a == "B2") {
        println!("B2  free_arena vs PENDING. branches=1000, checkpoint OFF. Isolates pending.retain.");
        println!("      pending P     freed    ns/free_arena    ns per 1000 P");
        for &p in &pend_counts {
            let rig = build(&format!("b2-{p}"), false);
            let arenas = pad_branches(&rig, 1000);
            pad_pending(&rig, p);
            if rig.store.pending_len() != p {
                println!("  {p:>12}   pending={} != P — padding did not take. NOT A RESULT.",
                    rig.store.pending_len());
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            let (ns, freed) = time_free_arena(&rig, &arenas, 100);
            if freed == 0 {
                println!("  {p:>12}   zero frees succeeded. NOT A RESULT.");
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            println!("  {:>12}   {:>6}   {:>13.1}   {:>14.2}",
                p, freed, ns, ns / (p as f64 / 1000.0));
            let _ = std::fs::remove_dir_all(&rig.dir);
        }
        println!();
    }

    // ---------------- B3: free_arena vs branches, checkpoint ON (production) ----------------
    if arms.iter().any(|a| a == "B3") {
        println!("B3  free_arena vs BRANCHES, checkpoint ON — the configuration cli.rs ships.");
        println!("      branches M    freed    ns/free_arena    ns per 1000 M");
        for &m in &branch_counts {
            let rig = build(&format!("b3-{m}"), true);
            let arenas = pad_branches(&rig, m);
            if arenas.len() < m {
                println!("  {m:>12}   padding did not take. NOT A RESULT.");
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            let k = (m / 10).clamp(1, 200);
            let (ns, freed) = time_free_arena(&rig, &arenas, k);
            if freed == 0 {
                println!("  {m:>12}   zero frees succeeded. NOT A RESULT.");
                let _ = std::fs::remove_dir_all(&rig.dir);
                continue;
            }
            println!("  {:>12}   {:>6}   {:>13.1}   {:>14.2}",
                m, freed, ns, ns / (m as f64 / 1000.0));
            let _ = std::fs::remove_dir_all(&rig.dir);
        }
        println!();
    }
}
