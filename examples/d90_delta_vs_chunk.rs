//! D90 — **the wall content-defined chunking does not climb: a tens-of-bytes change costs a page.**
//!
//! ---
//! ⛔⛔ **BANDED 2026-09-22 — THE ARENA SECTION'S INTERPRETATION IS CONTAMINATED BY D81. THE RAW
//! NUMBERS AND THE CHUNK-VS-DELTA RESULT ARE NOT.**
//!
//! This banner covers ONE section of this harness — "THE ARENA FREE-SPACE MAP", near the end. That
//! section models `arena B` as `replaces x image` and SELF-CHECKS that every flat-region cycle
//! performs exactly one `replace_atomically`. **D81 made `<db>.arena` append-only**: a claim
//! appends 45 bytes and a free 25, and the whole image is rewritten only when the tail outgrows
//! its share. So a cycle now performs **ZERO** replaces and N appends, the `arena_replaces != 1`
//! check falls to its **"NOT confirmed"** branch **BY DESIGN**, and that print is the correct
//! answer rather than a broken harness.
//!
//! ⇒ **AND THE SECTION'S STATED CONCLUSION IS NOW A DESCRIPTION OF A FIXED PROBLEM.** It reads:
//! *"a database that has done N merges rewrites O(N) bytes on every subsequent merge... That is a
//! real scaling problem and it belongs to whoever owns the free-space map."* **That is the exact
//! wall D81 removed** — measured at 530 MB -> 1.19 MB (446x) over four phases, with fsyncs per
//! claim going 2.000 -> 1.00. Read it as the statement of a problem that has SINCE BEEN SOLVED,
//! not as a finding about the current tree.
//!
//! ⚠ **NOT REPAIRED, DELIBERATELY, AND THE REST OF THIS FILE IS UNAFFECTED.** Re-modelling
//! `arena B` for an append-only map is this lane's own measurement decision, not a side effect of
//! landing D81; and making the section's premise true again would be repairing the fixture to fit
//! the assertion. Nothing outside that one section makes any claim about the arena, so **the
//! amplification sweep, the order control and the chunk-vs-delta finding stand as measured.**
//! Whoever next runs d90 should re-cut the arena model — and delete this banner then, not before.
//! ---
//!
//! # The claim being turned into a number
//!
//! ForkBase's paper concedes in its footnote 2 that content-defined dedup LOSES to delta encoding
//! when the delta is much smaller than the chunk. That concession is an argument. This is the
//! measurement, in the exact shape the objective cares about: an agent forks a branch, changes a
//! handful of rows, merges. The change is tens of bytes. The storage unit is a 4096-byte page.
//!
//! The question is NOT "is a merge expensive" — D68/D69/D71/D86 already answered that in
//! milliseconds. It is **how many bytes the system stores in exchange for how many bytes actually
//! changed**, and whether that ratio depends on how small the change was.
//!
//! # AMPLIFICATION = stored / real, and why the sweep is the result
//!
//! A single before/after ratio cannot separate a constant from a complexity class — one number is
//! consistent with "storage costs a page" and with "storage costs what you changed plus a fixed
//! overhead". So this sweeps `r` = rows changed over twelve doublings at a FIXED table, and the
//! SHAPE of the amplification column is the finding. Two shapes, two different worlds:
//!
//! * **FLAT high amplification across the small-`r` region** — storage cost is independent of how
//!   small the change was. That is the wall: a 45-byte edit and a 45-byte edit that happens to be
//!   alone on its page both cost 4096 bytes, and no amount of better chunking changes it, because
//!   the chunk IS the unit of storage.
//! * **Amplification falling as `r` falls** — the system is already paying in proportion to the
//!   change, the premise is wrong, and this line of work is dead.
//!
//! # ⚠ PRE-REGISTERED FALSIFIERS — recorded here BEFORE the first number exists
//!
//! * **(a) If amplification FALLS as `r` FALLS, the wall does not exist and the premise is wrong.**
//!   A small change that costs proportionally less is exactly what a delta-encoded store does. If
//!   the left end of the sweep is the CHEAP end per byte of real change, report the premise as
//!   falsified and stop arguing for it.
//! * **(b) If bytes-stored is already PROPORTIONAL to `r` — stored/r roughly constant all the way
//!   down to r = 1 — then ferrodb already delta-encodes and this whole line of work is dead.**
//!   Say so plainly rather than reporting the absolute bytes as if they were a problem.
//! * **(c) If amplification RISES as `r` rises**, the instrument is measuring something other than
//!   the change — per-merge bookkeeping that grows with the branch, most likely — and the run is a
//!   statement about that bookkeeping, not about chunk-vs-delta. Report it as such; it is not a
//!   result about the wall.
//!
//! The expected shape, stated so it can be wrong: amplification ~ PAGE_SIZE/ROW_SIZE and FLAT at
//! small `r`, decaying towards 1 only once `r` is large enough that several changed rows land on
//! one page. The DECAY is not the wall; the FLAT REGION is.
//!
//! # What is measured, and with which instrument
//!
//! Three storage sinks, counted separately, because they are three different mechanisms and a
//! single aggregate would let one hide inside another:
//!
//! | sink | instrument | what it is |
//! |---|---|---|
//! | data pages | a counting [`Storage`] under `DiskManager` — this file, not a new src counter | the 4096-byte units the change actually landed in |
//! | WAL | `ferrodb::wal::log::fsync_counters()` | durable log bytes, already counted by the engine |
//! | arena state | `ferrodb::storage::atomic_file::atomic_replace_counters()` | the free-space map's rewrites |
//!
//! The page sink is counted as **DISTINCT page ids**, not as write calls: a page written twice in
//! one merge is one 4096-byte unit of storage, and counting the calls would inflate the very number
//! the argument turns on.
//!
//! **The buffer pool is why `flush_all` brackets every cycle.** Dirty frames live in memory until
//! eviction or a checkpoint, so counting page writes without forcing them would measure the
//! eviction policy rather than the merge. `flush_all()` runs BEFORE the cycle (so the window starts
//! clean) and AFTER it (so the cycle's own dirty pages land inside the window). Every page id in
//! the window was therefore dirtied by that cycle and by nothing else.
//!
//! **ROW_SIZE is read out of the system, not assumed.** `Tuple::serialize` is the engine's own row
//! encoder; the example calls it on a row of this table's shape and uses what it returns. The
//! headline amplification divides by the FULL NEW ROW image, which is the *generous* denominator —
//! a cell-level delta would store less — so the reported amplification is a LOWER BOUND.
//!
//! # The order control, and why it is not optional
//!
//! Every cycle in the sweep runs against the same server, and D86 established that `State::applied`
//! grows without pruning. So a column that rises with `r` could be rising with *cycle number*
//! instead. The sweep therefore runs TWICE — ascending, then descending — and both passes are
//! printed. A shape that survives reversal is a fact about `r`; a shape that flips is a fact about
//! how many merges have already happened, and falsifier (c) owns it.
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::catalog::column::{Column, DataType, Value};
use ferrodb::catalog::schema::Schema;
use ferrodb::cow::PageStore;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::pgwire::ServerContext;
use ferrodb::storage::atomic_file::atomic_replace_counters;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::storage::Storage;
use ferrodb::storage::tuple::Tuple;
use ferrodb::tel::MemEffectLog;
use ferrodb::wal::log::{fsync_counters, WalManager};
use ferrodb::wal::txn::TxnManager;

/// A [`Storage`] that records **which 4096-byte units** a write landed in.
///
/// Deliberately a wrapper in this example rather than a counter added to `DiskManager`: the number
/// this run turns on is a property of the storage UNIT, and injecting the instrument at the same
/// seam `SimStorage` uses keeps the engine's production path exactly as it ships.
///
/// `pages` is a SET. `DiskManager::write` loops until a whole page is written and a merge rewrites
/// the same page repeatedly; counting calls would report storage the system never allocated.
struct CountingStorage {
    inner: File,
    pages: Mutex<HashSet<u64>>,
    /// Every byte handed to `pwrite` in the window, including re-writes of a page already counted.
    /// Reported next to the distinct count so the gap between them is visible rather than assumed.
    raw_bytes: AtomicU64,
    writes: AtomicU64,
}

impl CountingStorage {
    fn new(inner: File) -> Self {
        CountingStorage {
            inner,
            pages: Mutex::new(HashSet::new()),
            raw_bytes: AtomicU64::new(0),
            writes: AtomicU64::new(0),
        }
    }

    /// Start a fresh measurement window. Called with the buffer pool already flushed, so nothing
    /// dirtied before this point can land inside the window.
    fn reset(&self) {
        self.pages.lock().unwrap().clear();
        self.raw_bytes.store(0, Ordering::Relaxed);
        self.writes.store(0, Ordering::Relaxed);
    }

    /// `(distinct pages, raw bytes handed to pwrite, pwrite calls)` since the last [`Self::reset`].
    fn window(&self) -> (u64, u64, u64) {
        (
            self.pages.lock().unwrap().len() as u64,
            self.raw_bytes.load(Ordering::Relaxed),
            self.writes.load(Ordering::Relaxed),
        )
    }
}

impl Storage for CountingStorage {
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        let wrote = self.inner.pwrite(buf, offset)?;
        // Attribute by the page the write STARTED in. `DiskManager::write` never straddles a page
        // boundary — it writes exactly one page per call site — so this is the page id, not an
        // approximation of it.
        self.pages.lock().unwrap().insert(offset / PAGE_SIZE as u64);
        self.raw_bytes.fetch_add(wrote as u64, Ordering::Relaxed);
        self.writes.fetch_add(1, Ordering::Relaxed);
        Ok(wrote)
    }
    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.pread(buf, offset)
    }
    fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
}

struct Server {
    ctx: Arc<ServerContext>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    store: Arc<ArenaPageStore>,
    counter: Arc<CountingStorage>,
}

/// Run one statement the way the server does: take the catalog, run, drop.
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

/// The same configuration D68 was corrected into: `with_storage`, so the ARENA and CoW pages are
/// really in the loop. `with_catalog` sets `storage: None` and sends agent writes to an in-memory
/// effect log, which would make every byte on this page a fiction.
fn build(dir: &Path, nrows: i64) -> Server {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.join("main.db"))
        .unwrap();
    let counter = Arc::new(CountingStorage::new(file));
    let disk = DiskManager::with_storage(counter.clone() as Arc<dyn Storage>).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(disk)));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.join("main.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    let cat = Arc::new(TableBranchCatalog::open_sidecar(&dir.join("b.branchcat"), 1).unwrap());
    let branches: Arc<dyn BranchCatalog> = cat.clone();
    // The arena floor must sit ABOVE where the ordinary table will grow to, or the table runs out
    // of pages below the reserved region. A 100k-row table is ~1100 pages; 1024 is not enough, and
    // the failure mode is a mid-run "no free page below the reserved arena region" rather than a
    // wrong number, so it is sized off the row count instead of copied from D68's 1024.
    let arena_base: u32 = std::env::var("D90_ARENA_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| ((nrows / 40) as u32 + 4096).next_power_of_two());
    let store = Arc::new(ArenaPageStore::new(bp.clone(), branches.clone(), arena_base).unwrap());
    // **Without this the arena column is structurally zero, not measured.** `ArenaPageStore`
    // persists its free-space map through `persist_if_configured`, which is a no-op until a
    // checkpoint path is set — so a harness that never calls `checkpoint_to` reports
    // `atomic_replace_counters()` deltas of 0 for every r and looks like it measured a sink that
    // was never wired. The first full run of this sweep did exactly that.
    //
    // Configuring it can only ADD bytes to the cost of a small change, never remove them, so it
    // cannot be a thumb on the scale for the wall this run is testing for.
    store.checkpoint_to(dir.join("main.arena"));
    let runtime = Arc::new(
        AgentRuntime::with_storage(
            branches,
            Arc::new(MemEffectLog::new()),
            Arc::clone(&store) as Arc<dyn PageStore>,
        )
        .expect("attach arena storage"),
    );
    let ctx = Arc::new(ServerContext::new(catalog, bp.clone(), txn.clone(), runtime));
    let s = Server { ctx, bp, txn, store, counter };

    // `with_runtime`, for the same reason every measured cycle uses it — see `one_cycle`.
    let mut sess = Session::with_runtime(Arc::clone(&s.ctx.runtime));
    exec(&s, "CREATE TABLE t (id INTEGER NOT NULL, v INTEGER);", &mut sess).unwrap();
    let t0 = std::time::Instant::now();
    // **Batched into transactions, and ONLY the build is.** One INSERT per transaction costs one
    // fsync per row — measured at ~22 ms/row on this machine, which puts a 100k-row build past
    // half an hour and makes the table size the thing that limits the sweep. Batching changes the
    // number of COMMITS during construction and nothing about where the rows land, so the heap the
    // sweep then measures is identical. Every MEASURED cycle below is left exactly as the engine
    // ships: one statement, one transaction, one fsync.
    const BATCH: i64 = 1000;
    let mut i = 1;
    while i <= nrows {
        let hi = (i + BATCH - 1).min(nrows);
        exec(&s, "BEGIN;", &mut sess).unwrap();
        for k in i..=hi {
            exec(&s, &format!("INSERT INTO t VALUES ({k}, {});", k * 7), &mut sess).unwrap();
        }
        exec(&s, "COMMIT;", &mut sess).unwrap();
        if hi % 20_000 == 0 || hi == nrows {
            println!("  ... built {hi}/{nrows} rows ({:.1}s)", t0.elapsed().as_secs_f64());
        }
        i = hi + 1;
    }
    s
}

/// One measured branch: fork, change `r` scattered rows, merge, and count the bytes stored.
struct Cycle {
    r: i64,
    pages: u64,
    raw_bytes: u64,
    writes: u64,
    wal_bytes: u64,
    fsyncs: u64,
    arena_replaces: u64,
    arena_bytes: u64,
    /// `live_page_count()` delta — the arena's OWN count of pages it holds, an instrument that
    /// shares no code with the counting `Storage`. Two instruments that disagree in SHAPE mean one
    /// of them is wrong, and a single one could never say which.
    live_delta: i64,
    merge_ms: f64,
    total_ms: f64,
    /// The branch this cycle merged FROM, read out of the merge report.
    ///
    /// Every cycle runs under ONE agent id, because the provenance store interns on
    /// `(agent_id, run_id)` and `bind_run` refuses to redeclare a slot for a different actor — so a
    /// per-cycle agent name makes every merge after the first fail. That makes "each cycle forks a
    /// FRESH branch" an assumption rather than a fact, and the whole sweep is worthless if the
    /// cycles are accumulating into one branch instead. So it is read back and checked.
    from: ferrodb::branch::types::BranchId,
    /// The branch and provenance slot the SESSION reported at `BEGIN`, before any merge ran.
    branch: ferrodb::branch::types::BranchId,
    prov: ferrodb::provenance::ProvId,
    /// Which merge this was, counting from the start of the process. The arena's free-space map is
    /// a function of this and not of `r`, so the two have to be separable.
    seq: usize,
}

/// The bytes whose size is a question about `r`: the data pages the change landed in, plus the WAL.
///
/// **The arena's free-space map is deliberately NOT in here**, and leaving it in was wrong. Its
/// image is rewritten WHOLE on every merge and grows by a fixed increment per branch that has ever
/// existed, so it is a cost of the MERGE COUNT, not of how many rows changed. Folding it into the
/// headline made a one-row change look like 164x in one pass and 234x in the other — the same
/// measurement, differing only in how many merges preceded it. It is reported in its own column and
/// analysed separately below.
fn stored_r(c: &Cycle) -> u64 {
    c.pages * PAGE_SIZE as u64 + c.wal_bytes
}

/// `seq` becomes the RUN id, and that is load-bearing rather than cosmetic.
///
/// A per-cycle AGENT id is refused: the provenance store interns on `(agent_id, run_id)` and
/// `bind_run` will not redeclare a slot for a different actor, so the second merge fails outright.
/// A per-cycle agent id AND a fixed run id therefore cannot work — but holding the agent fixed and
/// varying the RUN does, because it is still a distinct key. **The first attempt held BOTH fixed,
/// copying D68, and every cycle then re-entered branch 1@g0**: the sweep would have reported the
/// accumulated cost of all previous cycles as if each were an independent fork. `assert_fresh_branches`
/// is what caught it, which is why the branch id is read back out of the merge report at all.
fn one_cycle(s: &Server, r: i64, nrows: i64, seq: usize) -> Option<Cycle> {
    // Settle everything dirtied so far, so the window below contains only this cycle's pages.
    s.bp.flush_all().unwrap();
    s.counter.reset();
    let (f0, b0) = fsync_counters();
    let (a0, ab0) = atomic_replace_counters();
    let live0 = s.store.live_page_count().unwrap_or(0) as i64;

    // ⚠ `with_runtime`, NEVER `Session::new()`. **This is the defect that made the first three
    // attempts at this run meaningless, and it is invisible from the outside.** `Session::new()`
    // builds its OWN `AgentRuntime::new()` — `storage: None`, an in-memory effect log, its own
    // branch state and its own provenance store. Every cycle then reported `branch=1@g0 prov=prov1`
    // from a runtime created milliseconds earlier: no arena, no CoW pages, and a MERGE that
    // "applied" against nothing. `src/pgwire/mod.rs:358` is what the real server does —
    // `Session::with_runtime(Arc::clone(&ctx.runtime))` — and its own comment records that the
    // server had this exact bug until the field existed.
    //
    // The symptom was NOT a wrong number, it was a refusal: the second cycle collided on provenance
    // slot prov1 because its runtime was brand new. Had the slot not been checked, this would have
    // produced a complete, plausible sweep measuring an in-memory stub.
    let mut sess = Session::with_runtime(Arc::clone(&s.ctx.runtime));
    let t_all = std::time::Instant::now();
    // The session's OWN branch and provenance slot, read out of `SessionStarted` rather than
    // inferred from the merge report afterwards. This is the authority for "did this cycle get a
    // fresh fork": the merge report's `from` is a second-hand account of the same fact.
    let (branch, prov) = match exec(s, &format!("BEGIN AGENT SESSION AS 'd90' RUN 'r{seq}';"), &mut sess) {
        Ok(Outcome::Agent(AgentOutput::SessionStarted(a))) => (a.branch, a.prov),
        Ok(_) => {
            eprintln!("  r={r}: BEGIN did not return a SessionStarted — the instrument is wrong");
            return None;
        }
        Err(e) => {
            eprintln!("  r={r}: BEGIN AGENT SESSION failed: {e}");
            return None;
        }
    };
    // **FIRE CHECK for the anti-vacuity guard below.** `D90_FIRECHECK=1` stages NOTHING while
    // still forking and merging, so the branch publishes an empty changeset and every merge still
    // reports `applied_to_target`. The read-back must then refuse every single cycle and the run
    // must exit non-zero. If it instead prints a sweep, the guard is decorative and every number
    // it protects is unprotected — which is the only way to know the guard is not decorative.
    let fire_check = std::env::var("D90_FIRECHECK").is_ok();
    for i in 0..r {
        if fire_check {
            break;
        }
        // 7919 is prime and does not divide `nrows`, so `i -> i * 7919 mod nrows` is injective for
        // every `r` this sweep reaches: exactly `r` DISTINCT rows, spread across the whole key
        // space. Clustering them would let one page absorb many changes and would report the
        // best case as if it were the typical one.
        let id = 1 + (i * 7919) % nrows;
        exec(s, &format!("UPDATE t SET v = {} WHERE id = {id};", i + 1), &mut sess).ok()?;
    }
    if std::env::var("D90_TRACE").is_ok() {
        eprintln!(
            "    [trace] r={r} seq={seq} BEGIN gave branch={}@g{} prov={prov}",
            branch.id, branch.generation
        );
    }
    let t_merge = std::time::Instant::now();
    // ⚠ `applied_to_target`, not `is_ok()`. A quarantined merge returns Ok and did not do the work
    // being counted; counting its bytes would report a cheap merge that never happened.
    let (applied, from) = match exec(s, "MERGE;", &mut sess) {
        Ok(Outcome::Agent(AgentOutput::Merge(report))) => (report.applied_to_target, report.from),
        Ok(_) => {
            // D68 lost a whole run to this: a `MERGE` returns `Outcome::Agent`, and a match that
            // expects anything else falls through to a default and counts zero FOREVER, in every
            // arm, while looking like a working instrument. Refuse instead of returning a zero.
            eprintln!("  r={r}: MERGE returned an outcome that is not a merge report — the");
            eprintln!("         instrument is wrong, not the system. Refusing to report a number.");
            return None;
        }
        Err(e) => {
            eprintln!("  r={r}: MERGE failed: {e}");
            return None;
        }
    };
    let merge_ms = t_merge.elapsed().as_secs_f64() * 1000.0;
    if std::env::var("D90_TRACE").is_ok() {
        eprintln!(
            "    [trace] r={r} seq={seq} branch={}@g{} prov={prov} merged_from={}@g{} applied={applied}",
            branch.id, branch.generation, from.id, from.generation
        );
    }
    if !applied {
        eprintln!("  r={r}: merge did NOT apply to target — not a result, and not a zero");
        return None;
    }
    // The cycle's own dirty pages have to reach the device to be countable.
    s.bp.flush_all().unwrap();
    let total_ms = t_all.elapsed().as_secs_f64() * 1000.0;

    let (pages, raw_bytes, writes) = s.counter.window();
    let (f1, b1) = fsync_counters();
    let (a1, ab1) = atomic_replace_counters();
    let live1 = s.store.live_page_count().unwrap_or(0) as i64;

    // ---- ANTI-VACUITY: the merge must have actually MOVED THE ROWS ------------------------
    //
    // `applied_to_target` says the merge published a changeset. It does NOT say the changeset
    // contained anything. A merge that published nothing would report `applied = true`, cost the
    // one page every merge costs, and this sweep would then report that page as the price of
    // changing one row — a flat line manufactured out of an empty operation, which is exactly the
    // shape the run is looking for and therefore the one it must not be able to fake.
    //
    // So the LAST row the branch wrote is read back through an ordinary `SELECT` against the
    // trunk, outside any agent session, and must carry the value the branch put there. Read AFTER
    // the counters so the verification cannot land inside the measurement window.
    let last_i = r - 1;
    let check_id = 1 + (last_i * 7919) % nrows;
    let expect = Value::Integer((last_i + 1) as i32);
    let mut verify = Session::with_runtime(Arc::clone(&s.ctx.runtime));
    match exec(s, &format!("SELECT v FROM t WHERE id = {check_id};"), &mut verify) {
        Ok(Outcome::Rows(rows)) => {
            let got = rows.first().and_then(|row| row.first()).cloned();
            if got.as_ref() != Some(&expect) {
                eprintln!(
                    "  r={r}: MERGE reported applied, but trunk row id={check_id} reads {got:?}, \
                     not {expect:?}. The merge published nothing this sweep can price. \
                     Refusing to report its bytes."
                );
                return None;
            }
        }
        Ok(_) => {
            eprintln!("  r={r}: the read-back SELECT did not return Rows — the check is broken, refusing.");
            return None;
        }
        Err(e) => {
            eprintln!("  r={r}: the read-back SELECT failed: {e}. Refusing to report unverified bytes.");
            return None;
        }
    }
    Some(Cycle {
        r,
        pages,
        raw_bytes,
        writes,
        wal_bytes: b1 - b0,
        fsyncs: f1 - f0,
        arena_replaces: a1 - a0,
        arena_bytes: ab1 - ab0,
        live_delta: live1 - live0,
        merge_ms,
        total_ms,
        from,
        branch,
        prov,
        seq,
    })
}

/// The engine's own row encoder, on a row of this table's shape. Not a constant typed in by hand.
fn row_bytes() -> usize {
    let schema = Schema::new(vec![
        Column { name: "id".into(), data_type: DataType::Integer, nullable: false },
        Column { name: "v".into(), data_type: DataType::Integer, nullable: true },
    ]);
    Tuple::serialize(&[Value::Integer(12345), Value::Integer(678)], &schema, 1)
        .expect("a two-integer row must serialize")
        .data
        .len()
}

/// Every cycle must have merged from a DIFFERENT branch, or the sweep is not a sweep over forks.
///
/// This is the premise the whole run rests on and it is not observable from the timings: cycles
/// that silently reused one branch would produce a perfectly plausible table in which each row is
/// the accumulated cost of everything before it. Read out of the system (`MergeReport::from`)
/// rather than assumed, and a repeat is a refusal, not a warning.
fn assert_fresh_branches(cycles: &[Cycle], label: &str) {
    // A run that collected nothing has not passed. Every assertion below lives inside the loop,
    // so on an empty slice this whole guard is vacuous: it returns cleanly, `print_pass` writes
    // its control header over an empty table, and the harness exits 0 having measured nothing.
    // That is indistinguishable in the output from a pass, which is the direction that gets
    // quoted. Refuse instead, and name which pass came back empty.
    assert!(
        !cycles.is_empty(),
        "{label}: zero cycles were collected, so every freshness check below is vacuous. An \
         empty pass is not a passing pass — it is a pass that did not run. Refusing to print a \
         control header over an empty table."
    );
    let mut seen: HashSet<(u64, u32)> = HashSet::new();
    let mut seen_prov: HashSet<ferrodb::provenance::ProvId> = HashSet::new();
    for c in cycles {
        let key = (c.branch.id, c.branch.generation);
        assert!(
            seen.insert(key),
            "{label}: r={} ran on branch {}@g{}, which an earlier cycle already used. The cycles \
             are NOT independent forks and every number in this run is the accumulated cost of \
             its predecessors. Refusing to report it.",
            c.r, c.branch.id, c.branch.generation
        );
        assert_eq!(
            (c.from.id, c.from.generation),
            key,
            "{label}: r={} began on branch {}@g{} but the merge reports it came from {}@g{}. The \
             statement that was measured is not the statement that was merged.",
            c.r, c.branch.id, c.branch.generation, c.from.id, c.from.generation
        );
        // The provenance slot is the OTHER half of the same freshness question, and it is the half
        // that actually surfaced the stub-runtime bug: a `Session::new()` builds a brand-new
        // `AgentRuntime`, so every cycle reported `prov1` and the SECOND merge died on a
        // provenance-slot collision. A shared runtime advances prov1, prov2, prov3... So a repeated
        // slot means the runtime was rebuilt underneath the sweep, which is exactly the
        // configuration this harness must never measure.
        assert!(
            seen_prov.insert(c.prov),
            "{label}: r={} reported provenance slot {:?}, which an earlier cycle already used. The \
             runtime was rebuilt between cycles, so these statements did not run against the arena \
             engine this harness configured. Refusing to report it.",
            c.r, c.prov
        );
    }
}

fn print_pass(label: &str, rows: &[Cycle], row_sz: usize) {
    println!();
    println!("  --- {label} ---");
    println!(
        "  {:>6} | {:>7} {:>12} | {:>10} {:>7} | {:>12} {:>10} | {:>8} || {:>6} {:>10} | {:>9}",
        "r", "pages", "page bytes", "WAL bytes", "fsyncs", "STORED(r)", "real B", "AMPLIF",
        "arepl", "arena B", "merge ms*"
    );
    println!(
        "         the columns LEFT of || answer \"what does changing r rows cost\". The two RIGHT of\n         \
         it do not: arena B is the free-space map, rewritten whole per merge and sized by the\n         \
         MERGE COUNT, and merge ms* is wall-clock taken on a loaded box (see the header)."
    );
    for c in rows {
        let page_bytes = c.pages * PAGE_SIZE as u64;
        let stored = stored_r(c);
        let real = c.r as u64 * row_sz as u64;
        println!(
            "  {:>6} | {:>7} {:>12} | {:>10} {:>7} | {:>12} {:>10} | {:>8.1} || {:>6} {:>10} | {:>9.1}",
            c.r, c.pages, page_bytes, c.wal_bytes, c.fsyncs,
            stored, real, stored as f64 / real as f64,
            c.arena_replaces, c.arena_bytes, c.merge_ms
        );
    }
    println!(
        "  (raw pwrite bytes / calls and the arena's own live-page delta, as a cross-check on the\n   \
         distinct-page count — the two instruments share no code — plus the branch each cycle forked:)"
    );
    for c in rows {
        println!(
            "    r={:<6} distinct pages {:>6}   raw pwrite bytes {:>12} in {:>7} calls   live_page_count delta {:>+7}   total ms {:>9.1}   merged from branch {}@g{}",
            c.r, c.pages, c.raw_bytes, c.writes, c.live_delta, c.total_ms,
            c.branch.id, c.branch.generation
        );
    }
}

fn main() {
    println!("D90 — DELTA vs CHUNK: what does a tens-of-bytes change cost in bytes stored?");
    println!("{}", ferrodb::build_provenance());
    println!();
    println!("WHAT IS AND IS NOT A MEASUREMENT IN THIS FILE");
    println!("  Every byte and page below is a COUNTER read out of the engine — pages from a counting");
    println!("  Storage under DiskManager, WAL bytes from fsync_counters(), arena bytes from");
    println!("  atomic_replace_counters(). Counters do not move when the machine is busy, so this run");
    println!("  is deliberately taken WITHOUT the fleet measure lock and its numbers are unaffected by");
    println!("  whatever else was compiling. Both passes reproducing the same page counts is the check.");
    println!("  The `merge ms*` column is the exception: it is wall-clock, it was taken on a loaded");
    println!("  box, and it is NOT a measurement. It is there to show the sweep did work, not how");
    println!("  fast. Do not quote it. For merge latency see D68/D71, taken under the lock.");
    println!();
    println!("PRE-REGISTERED FALSIFIERS (recorded before the first number exists):");
    println!("  (a) If AMPLIFICATION FALLS as r FALLS, the wall does not exist and the premise is");
    println!("      WRONG — a small change already costs proportionally less.");
    println!("  (b) If STORED bytes is already PROPORTIONAL to r (stored/r roughly constant down to");
    println!("      r = 1), ferrodb ALREADY DELTA-ENCODES and this whole line of work is DEAD.");
    println!("  (c) If AMPLIFICATION RISES with r, the instrument is measuring per-merge bookkeeping");
    println!("      that grows with the branch, NOT chunk-vs-delta. Not a result about the wall.");
    println!("  The wall, if it exists, is a FLAT high-amplification region at small r: storage cost");
    println!("  INDEPENDENT of how small the change was. The decay at large r is not the wall.");
    println!();

    let nrows: i64 = std::env::var("D90_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(100_000);
    let rs: Vec<i64> = std::env::var("D90_R")
        .unwrap_or_else(|_| "1,2,4,8,16,32,64,128,256,512,1024,2048,4096".to_string())
        .split(',')
        .filter_map(|v| v.parse().ok())
        .collect();
    let reverse_pass = std::env::var("D90_REVERSE").map(|v| v != "0").unwrap_or(true);
    assert!(!rs.is_empty(), "an empty sweep is not a measurement");
    assert!(
        *rs.iter().max().unwrap() < nrows,
        "r must stay below the row count or the scattered ids stop being distinct"
    );

    let run_start = (fsync_counters(), atomic_replace_counters());
    let row_sz = row_bytes();
    println!("PAGE_SIZE = {PAGE_SIZE}, ROW_SIZE = {row_sz} bytes (from Tuple::serialize, not assumed)");
    println!("  -> a change confined to ONE row costs at most PAGE_SIZE/ROW_SIZE = {:.1}x if the page",
             PAGE_SIZE as f64 / row_sz as f64);
    println!("     is the unit of storage. That ratio is the prediction the FLAT region must match.");
    println!("  NOTE: the denominator is the FULL NEW ROW image. A cell-level delta would store");
    println!("  less, so every amplification below is a LOWER BOUND on the true one.");
    println!();
    println!("table = {nrows} rows, fixed. sweep r = {rs:?}");
    println!("building the table (one INSERT per row, as the engine ships) ...");

    let dir = std::env::temp_dir().join(format!("ferrodb-d90-{}", std::process::id()));
    let t0 = std::time::Instant::now();
    let s = build(&dir, nrows);
    s.bp.flush_all().unwrap();
    let (base_pages, _, _) = {
        s.counter.reset();
        s.bp.flush_all().unwrap();
        s.counter.window()
    };
    println!(
        "built in {:.1}s; table occupies ~{} pages by the file, {} live arena pages",
        t0.elapsed().as_secs_f64(),
        s.bp.disk_manager.high_water().map(|h| h.to_string()).unwrap_or_else(|_| "?".into()),
        s.store.live_page_count().unwrap_or(0)
    );
    let _ = base_pages;

    // One agent, a fresh RUN per cycle — see `one_cycle`. `seq` never resets across the two passes,
    // so PASS 2 cannot re-enter a PASS 1 branch.
    let mut seq = 0usize;

    // ---- WARM-UP, and why discarding these cycles is not massaging the result ----------------
    //
    // The first run of this sweep had PASS 1 and PASS 2 disagreeing by up to 20x, with PASS 1
    // always the expensive one — the signature of FIRST TOUCH, not of r. A merge that has never
    // run before allocates the arena's first extent, faults in catalog pages and grows the branch
    // sidecar, and all of that is charged to whichever r happens to be measured first.
    //
    // The alternative to warming up is not "a cleaner measurement", it is attributing one-time
    // construction to the smallest r in the sweep — which is exactly the direction that would
    // manufacture the wall this run is trying to falsify. The warm-up is therefore run at the
    // LARGEST r, so it cannot leave behind a state that flatters small changes.
    //
    // These cycles are discarded, and the ORDER CONTROL is what says whether enough of them ran:
    // if the two passes still disagree, the warm-up was too short and the run says so.
    let warm: usize = std::env::var("D90_WARMUP").ok().and_then(|v| v.parse().ok()).unwrap_or(6);
    let warm_r = *rs.iter().max().unwrap();
    println!("warming up: {warm} discarded cycles at r={warm_r} (first-touch allocation is not a");
    println!("cost of changing r rows; the order control below says whether this was enough)");
    for _ in 0..warm {
        seq += 1;
        if one_cycle(&s, warm_r, nrows, seq).is_none() {
            eprintln!("REFUSED: a warm-up cycle did not complete; the harness cannot reach a steady state.");
            let _ = std::fs::remove_dir_all(&dir);
            std::process::exit(1);
        }
    }

    let mut up: Vec<Cycle> = Vec::new();
    for &r in rs.iter() {
        seq += 1;
        eprintln!("  [pass 1] r={r} ...");
        match one_cycle(&s, r, nrows, seq) {
            Some(c) => up.push(c),
            None => println!("  r={r}: NO CYCLE — refusing to report a zero as a measurement"),
        }
    }
    if up.is_empty() {
        eprintln!("\nREFUSED: not one cycle completed. A run that collected nothing has not passed.");
        let _ = std::fs::remove_dir_all(&dir);
        std::process::exit(1);
    }
    assert_fresh_branches(&up, "PASS 1");
    print_pass("PASS 1: r ASCENDING", &up, row_sz);

    let mut down: Vec<Cycle> = Vec::new();
    if reverse_pass {
        let mut rev = rs.clone();
        rev.reverse();
        for &r in rev.iter() {
            seq += 1;
            eprintln!("  [pass 2] r={r} ...");
            match one_cycle(&s, r, nrows, seq) {
                Some(c) => down.push(c),
                None => println!("  r={r}: NO CYCLE (reverse pass)"),
            }
        }
        assert_fresh_branches(&down, "PASS 2");
        let mut both: Vec<&Cycle> = up.iter().chain(down.iter()).collect();
        both.sort_by_key(|c| (c.from.id, c.from.generation));
        for w in both.windows(2) {
            assert!(
                (w[0].from.id, w[0].from.generation) != (w[1].from.id, w[1].from.generation),
                "a PASS 2 cycle reused a PASS 1 branch ({:?}) — the passes are not independent",
                w[0].from
            );
        }
        down.sort_by_key(|c| c.r);
        print_pass("PASS 2: r DESCENDING (the order control — shape must survive reversal)", &down, row_sz);
    }

    // ---- verdict, computed from the rows rather than read off them by eye ----
    println!();
    println!("=== WHICH FALSIFIER FIRED ===");
    let amp = |c: &Cycle| stored_r(c) as f64 / (c.r as u64 * row_sz as u64) as f64;
    let stored_per_r = |c: &Cycle| stored_r(c) as f64 / c.r as f64;
    let a_small = amp(&up[0]);
    let a_large = amp(up.last().unwrap());
    let predicted = PAGE_SIZE as f64 / row_sz as f64;

    // ⚠ **THE FLAT THING IS STORED BYTES, NOT AMPLIFICATION, AND THE TWO CANNOT BOTH BE FLAT.**
    //
    // The brief for this run asked for "the region where amplification is ~PAGE_SIZE/ROW_SIZE and
    // FLAT — i.e. storage cost is independent of how small the change was". Those two halves are
    // not the same statement and cannot both hold: amplification is stored/real, and real = r *
    // ROW_SIZE by construction, so a storage cost that is INDEPENDENT of r makes amplification fall
    // as 1/r. Flat amplification would instead mean stored bytes PROPORTIONAL to r, which is
    // falsifier (b) — delta encoding, the opposite of a wall.
    //
    // The parenthetical is the real criterion, so that is what is tested: the leading run of r
    // values over which STORED BYTES stays within 1.5x of its r=1 value. Amplification is still
    // reported, and its value AT r=1 is the one the PAGE_SIZE/ROW_SIZE prediction speaks to.
    let stored = |c: &Cycle| stored_r(c) as f64;
    let s_small = stored(&up[0]);
    let flat_len = up
        .iter()
        .take_while(|c| stored(c) / s_small < 1.5 && s_small / stored(c) < 1.5)
        .count();
    let flat_hi = up[flat_len.saturating_sub(1).min(up.len() - 1)].r;
    println!(
        "  amplification at r={}: {a_small:.1}x   at r={}: {a_large:.1}x   predicted PAGE/ROW at r=1: {predicted:.1}x",
        up[0].r, up.last().unwrap().r
    );
    println!(
        "  FLAT REGION (STORED BYTES within 1.5x of its r={} value, = {:.0} B): r = {} .. {}  ({} of {} points)",
        up[0].r, s_small, up[0].r, flat_hi, flat_len, up.len()
    );
    println!(
        "  stored bytes per changed row: r={} -> {:.0} B/row   r={} -> {:.0} B/row",
        up[0].r, stored_per_r(&up[0]), up.last().unwrap().r, stored_per_r(up.last().unwrap())
    );
    println!(
        "  NOTE: amplification FALLS as r RISES here, which is the arithmetic signature of a flat");
    println!(
        "  storage cost, not evidence against one. Falsifier (a) is about the opposite direction.");

    if a_small < a_large / 2.0 {
        println!();
        println!("  ⇒ FALSIFIER (a) FIRED: amplification is LOWER at small r than at large r.");
        println!("    A small change already costs proportionally less. The wall does not exist in");
        println!("    this system and the delta-vs-chunk premise is WRONG as applied to ferrodb.");
    } else if stored_per_r(&up[0]) < 4.0 * row_sz as f64 {
        println!();
        println!("  ⇒ FALSIFIER (b) FIRED: a single changed row costs {:.0} B, within 4x of the",
                 stored_per_r(&up[0]));
        println!("    {row_sz}-byte row itself. ferrodb ALREADY stores in proportion to the change.");
        println!("    This line of work is DEAD — say so plainly rather than reporting absolutes.");
    } else if flat_len < 2 {
        println!();
        println!("  ⇒ NEITHER (a) NOR (b): there is no flat region at all, so the sweep does not");
        println!("    show a storage cost independent of r. Read the shape before claiming a wall.");
    } else {
        println!();
        println!("  ⇒ NEITHER FALSIFIER FIRED. THE WALL IS PRESENT AND MEASURED.");
        println!("    Across r = {} .. {} — a {}-fold range of how much actually changed — the total",
                 up[0].r, flat_hi, flat_hi / up[0].r.max(1));
        println!("    bytes stored is FLAT at ~{s_small:.0} B. Changing {} row costs the same as changing",
                 up[0].r);
        println!("    {flat_hi}. Storage cost is INDEPENDENT of how small the change was.");
        println!("    At r={}, that is {a_small:.1}x amplification against a {row_sz}-byte row, versus the",
                 up[0].r);
        println!("    PAGE_SIZE/ROW_SIZE prediction of {predicted:.1}x — the page IS the unit of storage.");
        println!("    This is precisely the case ForkBase's footnote 2 concedes: no content-defined");
        println!("    chunking helps, because the cost is the chunk, not the chunking.");
    }

    if reverse_pass && !down.is_empty() {
        println!();
        println!("  ORDER CONTROL: ascending vs descending amplification, per r");
        let mut worst = 0.0f64;
        for c in &up {
            if let Some(d) = down.iter().find(|d| d.r == c.r) {
                let ratio = amp(d) / amp(c);
                worst = worst.max(if ratio > 1.0 { ratio } else { 1.0 / ratio });
                println!("    r={:<6} up {:>8.1}x   down {:>8.1}x   ratio {:.2}", c.r, amp(c), amp(d), ratio);
            }
        }
        println!(
            "    worst up/down disagreement: {worst:.2}x  -> {}",
            if worst < 1.5 {
                "the shape SURVIVES reversal; it is a fact about r, not about cycle number"
            } else {
                "the shape does NOT survive reversal — falsifier (c): this is per-merge bookkeeping growing with cycle count, not the wall"
            }
        );
    }

    if a_large > a_small * 1.5 {
        println!();
        println!("  ⚠ AMPLIFICATION RISES with r — falsifier (c) is in play. Read the order control");
        println!("    above before reading this run as a statement about chunk-vs-delta.");
    }

    // ---- the arena's free-space map: a SECOND cost, on a different axis --------------------
    //
    // Split out because it is not an answer to this run's question and folding it in corrupted the
    // one that is. `replace_atomically` rewrites the map WHOLE, and its image carries a record per
    // arena that has ever been claimed — so its size tracks the number of merges, not `r`. The two
    // passes make that visible: at r=1 the ascending pass paid one figure and the descending pass
    // another, differing only in how many merges had already run.
    //
    // Reported as a per-merge growth rate, which is the shape that matters: if it is a constant
    // number of bytes per merge, then a database that has done N merges rewrites O(N) bytes on
    // every subsequent merge, whatever that merge changed. That is a real scaling problem and it
    // belongs to whoever owns the free-space map, not to chunk-vs-delta.
    //
    // ⛔⛔ **EVERYTHING IN THE TWO PARAGRAPHS ABOVE DESCRIBES THE PRE-D81 ARENA, AND D81 HAS
    // LANDED.** Read them as the statement of a problem that has since been FIXED, not as a
    // finding about the current tree. `<db>.arena` is now `[image][tail record]*`: a claim appends
    // 45 bytes and a free 25, and `replace_atomically` fires only when the tail outgrows its share
    // of the image. The O(N)-bytes-per-merge scaling problem this section identified — and handed
    // to "whoever owns the free-space map" — is the wall D81 removed, measured at 530 MB -> 1.19 MB
    // (446x) over four phases with fsyncs per claim going 2.000 -> 1.00.
    //
    // ⚠ **SO THIS SECTION'S SELF-CHECK IS EXPECTED TO REPORT "NOT confirmed" NOW, AND THAT IS THE
    // CORRECT ANSWER RATHER THAN A BROKEN HARNESS.** It asserts every flat-region cycle performs
    // EXACTLY ONE `replace_atomically`; post-D81 a cycle performs ZERO replaces and N appends until
    // compaction, so `arena_replaces` is 0 and the `odd` branch fires. The counter is still real
    // and still measures what it says — what changed is the system under it.
    //
    // ⇒ **NOT REPAIRED HERE, DELIBERATELY.** Making this section's premise true again would be
    // editing the fixture to fit the assertion, and re-modelling `arena B` for an append-only map
    // is a measurement decision belonging to d90's own lane, not a side effect of landing D81.
    // The honest state is: the numbers below are still measured, and the MODEL around them is
    // stale. Whoever next runs d90 should re-cut the model, not delete this note.
    {
        println!();
        println!("  THE ARENA FREE-SPACE MAP — a SECOND flat cost, and a different axis:");
        println!("    `arena B` is bytes WRITTEN during the cycle, not the image's size. PRE-D81:");
        println!("    one `replace_atomically` rewrote the whole map, so bytes ~ replaces x image.");
        println!("    POST-D81 the map is append-only and a cycle may perform ZERO replaces, so");
        println!("    that model no longer holds and (1) below is EXPECTED to read NOT confirmed.");
        println!();
        // Checked against the rows, not asserted in prose: the claim is that one whole rewrite
        // serves every r in the flat region, and a single cycle with two replaces would break it.
        //
        // ⛔ **D81: THIS CHECK NOW REPORTS "NOT confirmed", AND THAT IS THE CORRECT ANSWER.** See
        // the banner at the head of this file. `<db>.arena` is append-only as of D81, so a cycle
        // performs ZERO `replace_atomically` calls and N appends until compaction — `arena_replaces`
        // is 0, `!= 1` is true for every cycle, and the `odd` branch below fires for all of them.
        // ⚠ **The counter is NOT broken and the branch is NOT a failure**: `arena_replaces` still
        // counts exactly what it says, and what moved is the system underneath it. The premise
        // being tested — "one whole rewrite serves every r in the flat region" — is what stopped
        // being true, because whole rewrites stopped being how the map is maintained.
        // ⇒ Left to fire rather than rewritten: a check quietly adjusted to keep printing
        // CONFIRMED would be repairing the fixture to fit the assertion, which is the one edit
        // this repo refuses. Re-cut the model, then delete this note and the banner together.
        let flat_cycles: Vec<&Cycle> =
            up.iter().chain(down.iter()).filter(|c| c.r <= flat_hi).collect();
        let odd: Vec<&&Cycle> = flat_cycles.iter().filter(|c| c.arena_replaces != 1).collect();
        if odd.is_empty() {
            println!("    (1) CONFIRMED over all {} cycles with r = {} .. {}: every cycle performs EXACTLY",
                     flat_cycles.len(), up[0].r, flat_hi);
            println!("        one replace. One whole free-map rewrite buys a 1-row change and a");
            println!("        {flat_hi}-row change alike — a SECOND cost independent of how small the change was.");
        } else {
            println!("    (1) NOT confirmed: {} of {} cycles in r = {} .. {} did not perform exactly one",
                     odd.len(), flat_cycles.len(), up[0].r, flat_hi);
            println!("        replace (e.g. r={} performed {}). The one-rewrite-per-merge reading is wrong.",
                     odd[0].r, odd[0].arena_replaces);
        }
        let mut same: Vec<(i64, u64, u64, usize, usize)> = Vec::new();
        for c in &up {
            if let Some(d) = down.iter().find(|d| d.r == c.r) {
                same.push((c.r, c.arena_bytes, d.arena_bytes, c.seq, d.seq));
            }
        }
        let all_grew = same.iter().all(|(_, ub, db, _, _)| db > ub);
        println!();
        println!("    (2) {} though r is identical — so the size tracks merge",
                 if all_grew { "At EVERY r the later pass wrote MORE," }
                 else { "The later pass did NOT write more at every r," });
        println!("        history, not r. That is why it is excluded from STORED(r):");
        println!("             r    up B (merge #)    down B (merge #)");
        for (r, ub, db, us, ds) in &same {
            println!("        {r:>6}   {ub:>7} (#{us:<3})     {db:>7} (#{ds:<3}){}",
                     if db > ub { "" } else { "   <- NOT larger; the claim above does not hold here" });
        }
        println!();
        println!("    A per-merge growth constant is NOT reported: the gap between the two passes at");
        println!("    a given r spans merges of OTHER r values that claim different numbers of");
        println!("    extents, so no controlled estimate of it exists in this run. Sizing that cost");
        println!("    needs its own sweep over merge count at fixed r.");
    }

    // ---- whole-run totals, so an UNWIRED counter cannot pass as a measured zero -------------
    //
    // A per-window delta of 0 reads identically whether the sink was quiet or the instrument was
    // never connected. The first full run of this sweep reported `arena B = 0` for all thirteen r
    // values because `persist_if_configured` is a no-op until a checkpoint path is set — a column
    // of zeros that looked like a finding and was a wiring bug. These totals are the check: a sink
    // that is zero for the WHOLE run, build included, is not being measured.
    let ((f_end, b_end), (a_end, ab_end)) = (fsync_counters(), atomic_replace_counters());
    let ((f_beg, b_beg), (a_beg, ab_beg)) = run_start;
    println!();
    println!("  WHOLE-RUN TOTALS (build + warm-up + both passes), to prove each sink is wired:");
    println!("    WAL:   {} fsyncs, {} bytes", f_end - f_beg, b_end - b_beg);
    println!("    arena: {} atomic replaces, {} bytes", a_end - a_beg, ab_end - ab_beg);
    for (name, n) in [("WAL fsyncs", f_end - f_beg), ("arena replaces", a_end - a_beg)] {
        if n == 0 {
            println!("    ⚠ {name} is ZERO for the entire run: that sink is NOT WIRED, and every");
            println!("      per-r zero in its column above is an artefact, not a measurement.");
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(test)]
mod fire_check {
    //! The empty-pass refusal in `assert_fresh_branches` is a detector, and a detector that has
    //! never been forced to fire is not a clean result. These two prove both directions: it
    //! panics on the empty slice it exists for, and it does NOT panic on a legitimate one-cycle
    //! pass (a guard that refuses everything is the same defect wearing the other sign).
    use super::*;

    fn a_cycle(r: i64, branch_id: u64, generation: u32, prov: u32) -> Cycle {
        let b = ferrodb::branch::types::BranchId { id: branch_id, generation };
        Cycle {
            r,
            pages: 0,
            raw_bytes: 0,
            writes: 0,
            wal_bytes: 0,
            fsyncs: 0,
            arena_replaces: 0,
            arena_bytes: 0,
            live_delta: 0,
            merge_ms: 0.0,
            total_ms: 0.0,
            from: b,
            branch: b,
            prov: ferrodb::provenance::ProvId(prov),
            seq: 0,
        }
    }

    #[test]
    #[should_panic(expected = "zero cycles were collected")]
    fn an_empty_pass_is_refused_rather_than_printed_as_a_control() {
        assert_fresh_branches(&[], "FIRE CHECK");
    }

    #[test]
    fn a_pass_that_actually_collected_something_still_passes() {
        assert_fresh_branches(&[a_cycle(1, 7, 0, 1)], "FIRE CHECK");
    }
}
