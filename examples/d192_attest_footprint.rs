//! D192: how many bytes `AgentRuntime::attested` (`AttestedHistory`) keeps resident per branch,
//! measured as a SLOPE over a branch-lifecycle curve.
//!
//!   d192_attest_footprint <merge|abandon|fork> [checkpoints,comma,separated]
//!
//! Arms, one fresh runtime each, driven through the production lifecycle (SQL
//! `BEGIN AGENT SESSION`, then `AgentRuntime::merge` / `AgentRuntime::abandon`):
//!
//! * `merge`   — fork, write one row, merge. The merge publishes and reaps the worker, so each branch
//!   leaves `[Fork, Reap]` on itself and `[Merge]` on trunk: 3 entries.
//! * `abandon` — fork, abandon: `[Fork, Reap]`, 2 entries.
//! * `fork`    — fork only, branch left live: `[Fork]`, 1 entry.
//!
//! `abandon` against `fork` is the arm that discriminates "a reap frees the branch's history" from
//! "a reap APPENDS to it": a reap that freed anything would put abandon BELOW fork.
//!
//! # Three instruments, and what each can and cannot see
//!
//! 1. **LIVE model** — `AgentRuntime::attested_footprint()` lengths × element sizes (+1 control
//!    byte per hash-table item). Phase-free, so it is the class figure; it cannot see capacity slack.
//! 2. **ALLOC model** — the same accessor's CAPACITIES × element sizes, plus a stated hash-table
//!    layout model (buckets derived from `HashMap::capacity()`, `round_up(buckets*size_of(T), 8) +
//!    buckets + GROUP`). That layout is a model of std's hashbrown, not a fact, which is why:
//! 3. **ALLOCATOR** — a tracking `#[global_allocator]` (the `examples/cow_scan_memory.rs`
//!    instrument) brackets `AttestedHistory::load_untrusted(rt.attested_log())`: a replay through the
//!    same single `push` path, whose every length and capacity is ASSERTED equal to the runtime's
//!    before its bytes are believed. Its bytes are what the allocator actually handed out, so the
//!    residual `allocator − alloc_model` checks instrument 2 rather than repeating it. The replay is
//!    then dropped and the bytes it RETURNS must equal the bytes it took; a mismatch means another
//!    thread allocated inside the window and that point is contaminated.
//!
//! Not measured here: allocator rounding and per-block metadata beyond `Layout::size()` (the
//! tracking allocator counts requested bytes), and RSS. Resident cost lies between LIVE and ALLOC:
//! untouched `Vec` capacity is address space, not resident pages.
//!
//! The runtime is built the way `src/cli/cli.rs::run_cli` builds it — durable table catalog, arena
//! page store, durable effect log, durable provenance, two-tier reaper — so the whole-process heap
//! column is this engine's configuration and not a test double's. The lease thread is NOT started:
//! a background thread allocating inside the replay bracket would contaminate instrument 3.
use std::alloc::{GlobalAlloc, Layout, System};
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use ferrodb::agent_sql::runtime::{AgentRuntime, ExecCtx};
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::attest::{AttestFootprint, Attestation, AttestedHistory, HistoryEntry};
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cow::PageStore;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::DurableEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

// ---- the instrument (examples/cow_scan_memory.rs, live counter only) ------------------------

static LIVE: AtomicUsize = AtomicUsize::new(0);

struct Tracking;

unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            LIVE.fetch_add(l.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, new) };
        if !q.is_null() {
            LIVE.fetch_sub(l.size(), Ordering::Relaxed);
            LIVE.fetch_add(new, Ordering::Relaxed);
        }
        q
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        if !p.is_null() {
            LIVE.fetch_add(l.size(), Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static A: Tracking = Tracking;

fn live() -> usize {
    LIVE.load(Ordering::SeqCst)
}

// ---- the byte models ------------------------------------------------------------------------

/// hashbrown's control-group width. 8 on aarch64 (NEON) and the generic fallback, 16 with SSE2.
/// A MODEL CONSTANT — instrument 3's residual is what says whether it is right.
const GROUP: usize = if cfg!(all(target_arch = "x86_64", target_feature = "sse2")) { 16 } else { 8 };

const E: usize = std::mem::size_of::<HistoryEntry>();
const IDX: usize = std::mem::size_of::<usize>();
const NODE: usize = std::mem::size_of::<[u8; 32]>();
const LEVEL_VEC: usize = std::mem::size_of::<Vec<[u8; 32]>>();
const BB: usize = std::mem::size_of::<(BranchId, Vec<usize>)>();
const HD: usize = std::mem::size_of::<(BranchId, Attestation)>();

/// Bytes of one hash-table allocation, from `HashMap::capacity()`. Zero capacity is the
/// unallocated empty singleton.
fn table_bytes(cap_items: usize, t: usize) -> usize {
    if cap_items == 0 {
        return 0;
    }
    let buckets = if cap_items < 8 { cap_items + 1 } else { cap_items / 7 * 8 };
    let ctrl_align = GROUP.max(8);
    (buckets * t).div_ceil(ctrl_align) * ctrl_align + buckets + GROUP
}

fn live_model(f: &AttestFootprint) -> usize {
    f.entries_len * E
        + f.level_nodes_len * NODE
        + f.by_branch_idx_len * IDX
        + f.by_branch_keys * (BB + 1)
        + f.heads_keys * (HD + 1)
}

fn alloc_model(f: &AttestFootprint) -> usize {
    f.entries_cap * E
        + f.level_nodes_cap * NODE
        + f.levels_outer_cap * LEVEL_VEC
        + f.by_branch_idx_cap * IDX
        + table_bytes(f.by_branch_table_cap, BB)
        + table_bytes(f.heads_table_cap, HD)
}

/// Instrument 3: bytes the allocator hands a replay of the runtime's log, and the bytes it gives
/// back on drop. Returns (built, returned, replay footprint).
fn replay(rt: &AgentRuntime) -> (usize, usize, AttestFootprint) {
    let b0 = live();
    let h = AttestedHistory::load_untrusted(rt.attested_log());
    let b1 = live();
    let fp = h.footprint();
    let d0 = live();
    drop(h);
    let d1 = live();
    (b1.wrapping_sub(b0), d0.wrapping_sub(d1), fp)
}

// ---- the runtime, built as run_cli builds it ------------------------------------------------

/// Distinct agent identities cycled through. Below `MAX_PAGE_DICT_ENTRIES` (255) with room to spare.
const IDENTITY_POOL: usize = 64;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Result<Self, FerroError> {
        let dir = tempfile::tempdir().map_err(|e| FerroError::Io(e.to_string()))?;
        let db_path = dir.path().join("d192.db").to_string_lossy().into_owned();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&db_path)
            .map_err(|e| FerroError::Io(e.to_string()))?;
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file)?)));
        let wal = Arc::new(WalManager::new(format!("{db_path}.wal").into())?);
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        let catalog = Catalog::create(bp.clone())?;

        // run_cli's TRUNK_ROOT_PLACEHOLDER and DEFAULT_ARENA_HEADROOM (both private there).
        let branches = Arc::new(TableBranchCatalog::default_for_database(&db_path, 1)?);
        let base = bp.disk_manager.high_water()?.saturating_add(32_736);
        let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), base)?);
        store.checkpoint_to(format!("{db_path}.arena").into());
        let reaper = Arc::new(TwoTierReaper::new(branches.clone(), store.clone()));
        let effects = DurableEffectLog::default_for_database(&db_path)?;
        let runtime = Arc::new(
            AgentRuntime::with_storage(
                branches as Arc<dyn BranchCatalog>,
                effects,
                store as Arc<dyn PageStore>,
            )?
            .with_durable_provenance(format!("{db_path}.provenance"))?
            .with_reaper(reaper as Arc<dyn Reaper>),
        );
        Ok(Db { catalog, bp, txn, runtime, _dir: dir })
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() || stmts.len() != 1 {
            return Err(FerroError::SqlParseError(format!("{sql}: {:?}", parser.errors)));
        }
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) {
        if let Err(e) = self.exec(sql, s) {
            panic!("{sql} failed: {e}");
        }
    }

    /// One branch lifecycle for `arm`. Returns the number of attested entries it must add.
    fn lifecycle(&mut self, arm: &str, i: usize) -> usize {
        let mut s = Session::with_runtime(self.runtime.clone());
        // Identities are POOLED. With a distinct (agent, run) per lifecycle, every merge stamps a
        // new run into the one page holding row 1, and that page's provenance dictionary refuses
        // its 256th run (`MAX_PAGE_DICT_ENTRIES`, src/provenance/store.rs) — the first run of this
        // harness died there at ~255 merges. Interning a known run is a lookup, and nothing in
        // `AttestedHistory` is sized by the identity: every entry is fixed-width and every fork
        // still mints its own `BranchId`.
        let who = i % IDENTITY_POOL;
        self.ok(&format!("BEGIN AGENT SESSION AS 'agent-{who}' RUN 'r_{who}';"), &mut s);
        let branch = s.agent.as_ref().expect("BEGIN AGENT SESSION opened no branch").branch;
        match arm {
            "merge" => {
                self.ok(&format!("UPDATE inventory SET qty = {i} WHERE id = 1;"), &mut s);
                let rt = self.runtime.clone();
                let mut ctx =
                    ExecCtx { catalog: &mut self.catalog, bp: self.bp.clone(), txn: self.txn.clone() };
                let report = rt.merge(&mut ctx, branch).expect("merge failed");
                assert!(!report.outcome.is_conflict(), "lifecycle {i} conflicted");
                assert!(report.applied_to_target, "lifecycle {i} did not publish");
                3
            }
            "abandon" => {
                self.runtime.abandon(branch).expect("abandon failed");
                2
            }
            "fork" => 1,
            other => panic!("unknown arm {other:?}; expected merge|abandon|fork"),
        }
    }
}

fn lsq(xs: &[f64], ys: &[f64]) -> f64 {
    let n = xs.len() as f64;
    let (mx, my) = (xs.iter().sum::<f64>() / n, ys.iter().sum::<f64>() / n);
    let num: f64 = xs.iter().zip(ys).map(|(x, y)| (x - mx) * (y - my)).sum();
    let den: f64 = xs.iter().map(|x| (x - mx) * (x - mx)).sum();
    num / den
}

fn main() {
    let mut args = std::env::args().skip(1);
    let arm = args.next().unwrap_or_else(|| {
        eprintln!("usage: d192_attest_footprint <merge|abandon|fork> [checkpoints]");
        std::process::exit(2)
    });
    if !matches!(arm.as_str(), "merge" | "abandon" | "fork") {
        eprintln!("unknown arm {arm:?}; expected merge|abandon|fork");
        std::process::exit(2);
    }
    let checkpoints: Vec<usize> = args
        .next()
        .unwrap_or_else(|| "1000,1400,2000,2800,4000,5600,8000,11200,16000".into())
        .split(',')
        .map(|s| s.trim().parse().expect("checkpoint must be an integer"))
        .collect();
    if checkpoints.len() < 2 || checkpoints.windows(2).any(|w| w[0] >= w[1]) {
        eprintln!("need >= 2 strictly increasing checkpoints; got {checkpoints:?}");
        std::process::exit(2);
    }

    println!("D192 attest footprint — arm={arm} checkpoints={checkpoints:?}");
    println!(
        "size_of: HistoryEntry={E} usize={IDX} node={NODE} Vec<[u8;32]>={LEVEL_VEC} \
         (BranchId,Vec<usize>)={BB} (BranchId,Attestation)={HD} GROUP(model)={GROUP}"
    );

    let mut db = Db::new().expect("runtime construction");
    {
        let mut s = Session::with_runtime(db.runtime.clone());
        db.ok("CREATE TABLE inventory (id INTEGER NOT NULL, qty INTEGER);", &mut s);
        db.ok("INSERT INTO inventory VALUES (1, 100);", &mut s);
    }

    // Negative control: an empty log replays to zero bytes, and the counter is quiet.
    let len0 = db.runtime.attested_len();
    let (built0, returned0, fp0) = replay(&db.runtime);
    println!(
        "control: attested_len before any fork = {len0}; empty-log replay built={built0} \
         returned={returned0} footprint={fp0:?}"
    );
    assert_eq!(len0, 0, "the seed attested something; the per-branch arithmetic would be offset");
    assert_eq!((built0, returned0), (0, 0), "empty replay allocated: the bracket is not clean");

    println!();
    println!(
        "{:>7} {:>8} {:>7} {:>12} {:>12} {:>12} {:>9} {:>12} {:>13} {:>8} {:>8}",
        "N", "att_len", "keys", "live_model", "alloc_model", "allocator", "resid", "returned",
        "proc_heap", "alloc/N", "secs"
    );
    let t0 = Instant::now();
    let heap_start = live();
    let mut done = 0usize;
    let mut expect_len = 0usize;
    let (mut xs, mut ys_live, mut ys_model, mut ys_alloc, mut ys_heap) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut all_ok = true;
    for &n in &checkpoints {
        while done < n {
            expect_len += db.lifecycle(&arm, done);
            done += 1;
        }
        let fp = db.runtime.attested_footprint();
        let len = db.runtime.attested_len();
        let (built, returned, rfp) = replay(&db.runtime);
        let heap = live();
        let (lm, am) = (live_model(&fp), alloc_model(&fp));
        let resid = built as i64 - am as i64;
        println!(
            "{:>7} {:>8} {:>7} {:>12} {:>12} {:>12} {:>9} {:>12} {:>13} {:>8.1} {:>8.1}",
            n,
            len,
            fp.by_branch_keys,
            lm,
            am,
            built,
            resid,
            returned,
            heap - heap_start.min(heap),
            built as f64 / n as f64,
            t0.elapsed().as_secs_f64()
        );
        // P2, P6, P9: refuse rather than print a slope over a point these fail at.
        let mut why = Vec::new();
        if len != expect_len {
            why.push(format!("P2 attested_len {len} != expected {expect_len}"));
        }
        if rfp != fp {
            why.push(format!("P6 replay footprint differs: runtime {fp:?} replay {rfp:?}"));
        }
        if built != returned {
            why.push(format!("P9 built {built} != returned {returned}"));
        }
        if !why.is_empty() {
            all_ok = false;
            println!("        ⛔ N={n}: {}", why.join("; "));
        }
        println!("        footprint {fp:?}");
        xs.push(n as f64);
        ys_live.push(lm as f64);
        ys_model.push(am as f64);
        ys_alloc.push(built as f64);
        ys_heap.push((heap - heap_start.min(heap)) as f64);
    }

    println!();
    println!("SEGMENT SLOPES (B/branch between consecutive checkpoints):");
    for i in 1..xs.len() {
        let dx = xs[i] - xs[i - 1];
        println!(
            "  {:>6}->{:<6} live {:>8.1}  alloc_model {:>8.1}  allocator {:>8.1}  proc_heap {:>9.1}",
            xs[i - 1],
            xs[i],
            (ys_live[i] - ys_live[i - 1]) / dx,
            (ys_model[i] - ys_model[i - 1]) / dx,
            (ys_alloc[i] - ys_alloc[i - 1]) / dx,
            (ys_heap[i] - ys_heap[i - 1]) / dx
        );
    }
    println!(
        "LSQ SLOPE over {} checkpoints (B/branch): live {:.1}  alloc_model {:.1}  allocator {:.1}  \
         proc_heap {:.1}",
        xs.len(),
        lsq(&xs, &ys_live),
        lsq(&xs, &ys_model),
        lsq(&xs, &ys_alloc),
        lsq(&xs, &ys_heap)
    );
    println!("elapsed {:.1}s", t0.elapsed().as_secs_f64());
    if all_ok {
        println!("rc=0 arm={arm} (P2, P6, P9 held at every checkpoint)");
    } else {
        println!("rc=1 arm={arm} (a checkpoint failed P2/P6/P9 — see the ⛔ lines; slopes above are NOT usable)");
        std::process::exit(1);
    }
}
