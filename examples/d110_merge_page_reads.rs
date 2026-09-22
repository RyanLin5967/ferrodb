//! **D110 — does a MERGE's page-read count grow with the table? The falsifier for wiring merge3.**
//!
//! # The question this exists to settle
//!
//! `src/cow/merge3.rs` is a structural three-way merge that skips a subtree the three roots agree
//! on, so its cost is O(delta x log N) rather than O(N). It has **no production caller**. The
//! proposal is to give it one on the production merge path (`agent_sql::runtime::evaluate_merge`,
//! reached by `MERGE;`).
//!
//! That is only worth doing if the production merge is **not already** proportional to the delta.
//! Skipping identical subtrees is an advantage over a merge that would otherwise walk the tree; it
//! is no advantage at all over a merge that never walks the tree in the first place.
//!
//! # Why page reads and not a wall clock
//!
//! This box runs a build fleet and a wall clock here measures the fleet. Worse, D68/D69 spent three
//! rows misreading exactly this curve: a timer wrapped `BEGIN AGENT SESSION` + N `UPDATE`s +
//! `MERGE` and the total was reported as "merge latency" (`bench/d69_fsync_counted.txt`). The
//! linear term was in the UPDATEs.
//!
//! So: an integer counter, incremented inside `PageStore::read_page`, read immediately before and
//! immediately after the `MERGE;` statement and **nothing else**. Load-independent, and it cannot
//! absorb a neighbouring phase the way a wall clock did.
//!
//! ⚠ **What this counter can and cannot see — stated here rather than discovered later.**
//!
//! It counts reads of *branch-engine* pages: the `ArenaPageStore` that `PagedRows` puts agent rows
//! on, which is the store `cow::merge3` would descend. That is the right scope for this question,
//! because merge3 can only ever skip pages in that store.
//!
//! It is **blind to the ordinary-table side**. `evaluate_merge`'s per-row point lookup goes through
//! `scan_table_where` -> the planner -> the buffer pool, and `BufferPoolManager` exposes no public
//! fetch counter in this build, so those reads are not in this number. A conditional column was
//! drafted here and removed rather than shipped returning a constant zero — a counter that cannot
//! fire is worse than no counter, because its zero reads as evidence.
//!
//! So this instrument can prove the branch-engine half of the merge is flat in table size. It
//! cannot, alone, prove the SQL half is. The `writes reads` column is the partial control: it goes
//! through the same planner and the same buffer pool, so if the planner were seq-scanning on an
//! equality over the primary key, that column is where it would show. The existing WAL-byte
//! instrument (`bench/d69_fsync_counted.txt`) is blind to reads altogether, which is the gap this
//! file narrows: a full table scan reads everything and writes nothing.
//!
//! # PRE-REGISTERED, written before the first run
//!
//! Fixed delta of 4 rows; table size varies over 5 sizes, 16x end to end.
//!
//! * If `page reads per MERGE` is **FLAT** in table size, the production merge is already
//!   O(delta): it enumerates the delta from the workspace map and does a point lookup per touched
//!   row. Wiring merge3 then buys **no complexity change**, and the honest answer is to say so and
//!   not wire it.
//! * If it **RISES** with table size, there is a shape to fix and merge3 is the mechanism.
//! * A rise in the BUILD/UPDATE phase is **not** a result for this row — that is the separate wall
//!   D69 already named (`UPDATE ... WHERE id = ?` on the primary key scanning rather than
//!   descending). It is reported in its own column so it cannot be mistaken for the merge's cost.
//!
//! ⚠ A zero is not a pass. If the merge does not apply, or the counter never moves, the run
//! refuses rather than reporting a flat line — a merge that did not happen reads no pages, and
//! that is indistinguishable from a perfectly O(1) merge by this instrument alone.
//!
//! Run: `cargo run --release --example d110_merge_page_reads`
//! Env: `D110_SIZES=1000,2000,4000,8000,16000`  `D110_MERGES=25`  `D110_DELTA=4`

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{ArenaId, BranchId, Epoch, PageId};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::Value;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{CowPage, PageHandle, PageStore};
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// A `PageStore` that counts `read_page` and delegates everything else.
///
/// Every other method forwards unchanged, so the store under measurement behaves exactly as the
/// production one does. Only `read_page` is observed, and it is observed by incrementing a
/// `u64` — no allocation, no lock, nothing that could itself scale with the table.
struct CountingStore {
    inner: Arc<dyn PageStore>,
    reads: Arc<AtomicU64>,
}

impl CountingStore {
    fn new(inner: Arc<dyn PageStore>) -> (Arc<Self>, Arc<AtomicU64>) {
        let reads = Arc::new(AtomicU64::new(0));
        (Arc::new(CountingStore { inner, reads: Arc::clone(&reads) }), reads)
    }
}

impl PageStore for CountingStore {
    fn read_page(&self, page_id: PageId) -> Result<PageHandle, FerroError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_page(page_id)
    }

    fn alloc_in_arena(
        &self,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<PageId, FerroError> {
        self.inner.alloc_in_arena(arena, page_type, birth_epoch)
    }

    fn cow_page(
        &self,
        page_id: PageId,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<CowPage, FerroError> {
        self.inner.cow_page(page_id, branch, epoch)
    }

    fn free_page(&self, page_id: PageId, free_epoch: Epoch) -> Result<(), FerroError> {
        self.inner.free_page(page_id, free_epoch)
    }

    fn alloc_arena(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        self.inner.alloc_arena(branch)
    }

    fn arena_for(&self, branch: BranchId) -> Result<ArenaId, FerroError> {
        self.inner.arena_for(branch)
    }

    fn free_arena(&self, arena: ArenaId) -> Result<u32, FerroError> {
        self.inner.free_arena(arena)
    }

    fn live_page_count(&self) -> Result<u32, FerroError> {
        self.inner.live_page_count()
    }

    fn flush(&self) -> Result<(), FerroError> {
        self.inner.flush()
    }
}

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    reads: Arc<AtomicU64>,
    runtime: Arc<AgentRuntime>,
}

/// Run one statement the way the server does. Copied from `examples/d68_merge_is_o_table.rs` so
/// the two harnesses drive the engine identically.
fn exec(s: &Server, sql: &str, sess: &mut Session) -> Result<Outcome, String> {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new())
        .scan_tokens()
        .map_err(|e| e.to_string())?;
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    if !parser.errors.is_empty() {
        return Err(format!("parse failed for {sql}: {:?}", parser.errors));
    }
    let stmt = stmts.remove(0);
    let mut cat = s.ctx.catalog();
    let out = run(stmt, &mut cat, s.bp.clone(), s.txn.clone(), sess);
    drop(cat);
    out.map_err(|e| e.to_string())
}

fn build_sized(dir: &std::path::Path, tag: &str, nrows: i64) -> Server {
    let d = dir.join(tag);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let file = OpenOptions::new()
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
    // Same fixed arena floor as d68: taking `high_water()` here leaves the ordinary table nowhere
    // to grow.
    const ARENA_BASE: u32 = 1024;
    let raw = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), ARENA_BASE).unwrap());
    let (store, reads) = CountingStore::new(Arc::clone(&raw) as Arc<dyn PageStore>);
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches,
            Arc::new(MemEffectLog::new()),
            store as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), Arc::clone(&runtime)));
    let s = Server { ctx, bp, txn, reads, runtime };

    // ⛔ D101 — `s.ctx.session()`, NEVER `Session::new()`. `Session::new` builds its OWN
    // `AgentRuntime::new()` (`storage: None`, private in-memory branch catalog, private effect
    // log), so every agent statement below would run on a STUB and the arena-backed runtime
    // this harness constructs would be built and never touched. `agent_sql::designated` now
    // refuses such a statement rather than measuring it.
    let mut sess = s.ctx.session();
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    for i in 1..=nrows {
        exec(&s, &format!("INSERT INTO t VALUES ({i}, {});", i * 7), &mut sess).unwrap();
    }
    s
}

/// What one merge cycle cost, split by phase so the merge's number cannot absorb the writes'.
struct Cycle {
    writes_reads: u64,
    merge_reads: u64,
    applied: bool,
}

fn one_cycle(s: &Server, tid: usize, seq: u64, delta: usize, nrows: i64) -> Cycle {
    // ⛔ D101 — `s.ctx.session()`, NEVER `Session::new()`. `Session::new` builds its OWN
    // `AgentRuntime::new()` (`storage: None`, private in-memory branch catalog, private effect
    // log), so every agent statement below would run on a STUB and the arena-backed runtime
    // this harness constructs would be built and never touched. `agent_sql::designated` now
    // refuses such a statement rather than measuring it.
    let mut sess = s.ctx.session();
    if exec(s, &format!("BEGIN AGENT SESSION AS 'a{tid}';"), &mut sess).is_err() {
        return Cycle { writes_reads: 0, merge_reads: 0, applied: false };
    }

    // ---- the WRITE phase, counted separately -------------------------------------------------
    //
    // Spread the touched rows across the key space so the delta cannot get lucky with locality,
    // and keep the COUNT fixed at `delta` however big the table is. That is what makes the merge
    // column a statement about table size rather than about how much was written.
    let before_writes = s.reads.load(Ordering::Relaxed);
    for w in 0..delta {
        let id = 1 + ((seq as i64 * 7919 + w as i64 * (nrows / delta.max(1) as i64).max(1))
            % nrows.max(1));
        if exec(s, &format!("UPDATE t SET v = v + 1 WHERE id = {id};"), &mut sess).is_err() {
            return Cycle { writes_reads: 0, merge_reads: 0, applied: false };
        }
    }
    let writes_reads = s.reads.load(Ordering::Relaxed) - before_writes;

    // ---- the MERGE phase. The counter brackets this statement and nothing else. ---------------
    let before_merge = s.reads.load(Ordering::Relaxed);
    let applied = matches!(exec(s, "MERGE;", &mut sess), Ok(_));
    let merge_reads = s.reads.load(Ordering::Relaxed) - before_merge;

    Cycle { writes_reads, merge_reads, applied }
}

/// ⚠ **THE FIRE-CHECK. Without this the zero below is worthless.**
///
/// If `MERGE reads` comes out 0, there are two possibilities and they look identical: the merge
/// genuinely reads no branch-engine pages, or this counter is dead and every number it ever
/// produced was noise. A detector that has never been seen firing is not a clean result.
///
/// So: drive a path that MUST read COW pages, through the SAME `CountingStore`, in the SAME
/// process. `AgentRuntime::get_row` goes `PagedRows::get` -> `CowTree` descent -> `read_page`, and
/// `put_row` builds the tree it descends. If the counter moves here and not across `MERGE;`, the
/// merge's zero is a measurement about the merge and not about the instrument.
fn counter_fires(s: &Server) -> (u64, u64) {
    let before = s.reads.load(Ordering::Relaxed);
    // Put a handful of rows on trunk's COW tree, then read them back.
    for i in 0..64u64 {
        s.runtime
            .put_row(BranchId::TRUNK, "t", i, &[Value::Integer(i as i32), Value::Integer(7)])
            .expect("put_row onto the branch-engine tree");
    }
    let after_put = s.reads.load(Ordering::Relaxed);
    for i in 0..64u64 {
        let _ = s.runtime.get_row(BranchId::TRUNK, "t", i).expect("get_row");
    }
    let after_get = s.reads.load(Ordering::Relaxed);
    (after_put - before, after_get - after_put)
}

fn median(mut v: Vec<u64>) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_unstable();
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2] as f64
    } else {
        (v[n / 2 - 1] + v[n / 2]) as f64 / 2.0
    }
}

fn main() {
    println!("ferrodb D110 — page reads per MERGE against TABLE SIZE, at fixed delta");
    println!("build provenance: {}", ferrodb::build_provenance());
    println!(
        "instrument: PageStore::read_page call count, bracketing the MERGE statement alone. \
         No wall clock."
    );
    println!(
        "PRE-REGISTERED: if `merge reads` is FLAT in table size, the production merge is already \
         O(delta)\n  and wiring cow::merge3 buys NO complexity change. If it RISES, merge3 has a \
         shape to offer."
    );
    println!();

    let sizes: Vec<i64> = std::env::var("D110_SIZES")
        .ok()
        .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![1_000, 2_000, 4_000, 8_000, 16_000]);
    let merges: usize = std::env::var("D110_MERGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(25);
    let delta: usize = std::env::var("D110_DELTA")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);

    println!("workload: {merges} merge cycles per size, {delta} rows updated per branch.");
    println!();
    println!("  table rows   merges   writes reads   MERGE reads   reads per 1000 rows");

    let dir = std::env::temp_dir().join(format!("d110_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let mut rows_out: Vec<(i64, f64, f64)> = Vec::new();
    let mut any_applied = false;
    let mut fire: Option<(u64, u64)> = None;

    for &n in &sizes {
        let s = build_sized(&dir, &format!("n{n}"), n);
        if fire.is_none() {
            fire = Some(counter_fires(&s));
        }
        let mut w = Vec::new();
        let mut m = Vec::new();
        let mut applied_count = 0usize;
        for i in 0..merges {
            let c = one_cycle(&s, 0, i as u64, delta, n);
            if !c.applied {
                continue;
            }
            applied_count += 1;
            any_applied = true;
            w.push(c.writes_reads);
            m.push(c.merge_reads);
        }
        if applied_count == 0 {
            println!("  {n:>10}   NO MERGE APPLIED — not a result, and not a zero");
            continue;
        }
        let wm = median(w);
        let mm = median(m);
        println!(
            "  {n:>10}   {applied_count:>6}   {wm:>12.1}   {mm:>11.1}   {:>19.4}",
            mm / (n as f64 / 1000.0)
        );
        rows_out.push((n, wm, mm));
    }

    let _ = std::fs::remove_dir_all(&dir);

    println!();
    // ---- refusals, so a run that measured nothing cannot read as a flat line ------------------
    if !any_applied {
        eprintln!(
            "⛔ NO MERGE APPLIED AT ANY SIZE. A merge that did not happen reads no pages, which is \
             indistinguishable from an O(1) merge by this instrument. This is not a result."
        );
        std::process::exit(2);
    }
    if rows_out.len() < 2 {
        eprintln!("⛔ fewer than two sizes reached; a single point cannot show a slope.");
        std::process::exit(2);
    }

    // ---- the fire-check decides what a zero MEANS -------------------------------------------
    let (put_reads, get_reads) = fire.expect("the fire-check runs at the first size");
    println!("counter fire-check — a path that MUST read branch-engine COW pages:");
    println!("  64 x AgentRuntime::put_row  -> {put_reads} read_page calls");
    println!("  64 x AgentRuntime::get_row  -> {get_reads} read_page calls");
    if put_reads == 0 && get_reads == 0 {
        eprintln!(
            "⛔ THE COUNTER NEVER FIRED. `put_row` and `get_row` descend the CowTree and must read \
             pages; this counter saw none, so it is dead or wrapped around the wrong store. Every \
             zero in the table above is therefore meaningless. This is not a result."
        );
        std::process::exit(2);
    }
    println!("  -> the counter COUNTS. A zero in the MERGE column is a fact about the merge.");
    println!();

    let merge_all_zero = rows_out.iter().all(|(_, _, m)| *m == 0.0);
    if merge_all_zero {
        println!("=== READING ===");
        println!("  **THE PRODUCTION MERGE READS ZERO BRANCH-ENGINE PAGES, AT EVERY TABLE SIZE.**");
        println!("  Not flat — ABSENT. `MERGE;` never descends the CowTree that `cow::merge3` merges.");
        println!();
        println!("  The SQL surface keeps base tables in ordinary heap/index pages. `PagedRows` is a");
        println!("  parallel store, reached only by put_row / get_row / scan_rows / page_changeset,");
        println!("  none of which the MERGE path calls — `self.rows()` has exactly five call sites in");
        println!("  runtime.rs and `evaluate_merge` is not one of them.");
        println!();
        println!("  ⇒ Wiring cow::merge3 into MERGE is NOT a performance change. There is no tree on");
        println!("    that path whose subtrees it could skip. It is a STORAGE MIGRATION — moving base");
        println!("    tables into the CoW tree — which runtime.rs's own doc names as future work:");
        println!("    \"when base tables live in the tree as well, the fork root will hold real rows\".");
        println!();
        println!("  ⚠ Blind spot, restated at the point of the claim: this counter sees the branch");
        println!("    engine only. It proves the CoW tree is untouched by a merge. It does NOT");
        println!("    measure the heap side, which has no public fetch counter in this build.");
        return;
    }

    println!("=== READING ===");
    let (n0, _, m0) = rows_out[0];
    let (n1, w1, m1) = *rows_out.last().unwrap();
    let size_factor = n1 as f64 / n0 as f64;
    let merge_factor = if m0 > 0.0 { m1 / m0 } else { f64::NAN };
    println!(
        "  table grew {size_factor:.1}x ({n0} -> {n1}); MERGE page reads went {m0:.1} -> {m1:.1} \
         = {merge_factor:.2}x"
    );
    println!(
        "  the WRITES phase at the largest size read {w1:.1} pages — reported so the merge column \
         cannot be confused with it"
    );
    if merge_factor < 1.5 {
        println!(
            "  ⇒ FLAT. The production merge does NOT read more pages on a bigger table. It is \
             already\n    proportional to the delta, and cow::merge3's subtree skipping has \
             nothing to skip."
        );
    } else {
        println!(
            "  ⇒ RISES. There is a table-size term in the merge's own page reads; merge3 is the \
             mechanism for it."
        );
    }
}
