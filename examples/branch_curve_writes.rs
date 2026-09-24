//! D32: the same curve as `branch_curve`, but **every branch writes one page**.
//!
//! `branch_curve` reached 10⁶ branches and is honest about what it measured — the CATALOG. Its hot
//! loop calls exactly one operation, `cat.fork(..)`, and it builds a `BufferPoolManager` it binds to
//! `_main_pool` and never uses. **No branch in that run ever wrote a page**, so its `cat MB` column
//! is catalog bytes and the data file stays empty.
//!
//! That matters because of D31: `arena_for` gives each writing branch its own extent, and
//! `alloc_arena` reserves `ARENA_EXTENT_PAGES` (256) of them at once. So a branch's FIRST 4 KB page
//! costs 1 MiB. Fork-only workloads never pay it; every real agent workload does.
//!
//! This harness adds the three calls `branch_curve` leaves out — `arena_for`, `next_epoch`,
//! `alloc_in_arena` — and reports **DATA FILE bytes**, because reporting the catalog column here
//! would reproduce exactly the blindness the repo already paid for: `branch_scaling_bench.rs` used
//! an in-memory catalog and therefore could not see an O(N²) durable cost at all. The instrument has
//! to exercise the path being claimed about.
//!
//!   branch_curve_writes [checkpoints,comma,separated] [threads] [byte_budget_gb]
//!
//! **READ-VS-N** (`bench/read_vs_n/PREREG.md`): `CURVE_ARMS=read,merge,restart` (any subset)
//! switches to the PRODUCTION layout (the database is opened by the shipped binary's own
//! `cli::open_database`, and each branch writes one row through `AgentRuntime::put_row`) and adds,
//! at every checkpoint, arm 1 (point-read cost against live N, with per-read census counters),
//! arm 2 axis (i) (K SQL `MERGE`s right after an open, so M is fixed while N grows) and arm 3 (the
//! full production open, timed in a fresh child process: this binary re-run as
//! `--open-only <db>`), then arm 2 axis (ii) after the last checkpoint (N fixed, M grows).
//! `CURVE_READ_K` sets reads per arm per thread count (default 16,384); `CURVE_MERGE_K` merges per
//! checkpoint (64); `CURVE_MERGE_M` axis (ii)'s M targets (256,1024,4096,16384);
//! `CURVE_FIRECHECK=<mode>` forces one guard to fire. Without `CURVE_ARMS` every line below runs
//! the historical D61/D65 path unchanged.
//!
//! **The byte budget is a refusal, not a tuning knob.** At 1 MiB/branch the 10⁶ point would need
//! ~1 TiB, which no machine here has, and a benchmark that fills the disk takes the machine down
//! with it. It stops at the budget and SAYS SO, naming the N it reached. "Stopped early on space" is
//! the result, not a failure of the run — and if it does NOT stop early, that kills D31, which is
//! the outcome D31's own falsifier asks for.
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use ferrodb::agent_sql::dispatch::AgentOutput;
use ferrodb::agent_sql::runtime::{merge_log_counters, table_id, AgentRuntime, StateSizes};
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::lease_thread::{scan_interval_from_env, CatalogLock};
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline, PageId, ARENA_EXTENT_PAGES};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::buffer::read_census::{this_thread, ReadCensus};
use ferrodb::catalog::column::Value;
use ferrodb::cli::cli::{open_database, OpenDatabase};
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::index_scan::index_scan_counters;
use ferrodb::execution::seq_scan::seq_scan_counters;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::wal::log::FSYNC_CALLS;
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::index_page::BPlusTreePage;

/// Say which of the two worlds this number came from, and NAME THE HYPOTHESIS.
///
/// **This function exists because the line it replaces read backwards after the fix landed.**
/// The old text was *"If that is far below 1048576 B, the first-page amplification is NOT binding
/// here and D31 should be CLOSED rather than built"*. That reading holds only while geometric
/// extent growth is ABSENT: then a run that completes the budget really does falsify D31. Once the
/// growth rule is BUILT, the same branch prints for the opposite reason — the run completes
/// *because the fix worked* — so an unconditioned verdict tells its reader to close the row at the
/// exact moment the row succeeded, and a reader with no context believes the line rather than
/// inferring the inversion.
///
/// A conditional verdict has to name the hypothesis it is conditioned on. The two reference values
/// below are what distinguishes the cases, so both are printed in every outcome, including the one
/// where neither fits.
fn verdict(bytes_per_branch: f64) {
    /// One page: what a branch's first page costs when extents grow geometrically from one.
    const GROWN: f64 = PAGE_SIZE as f64;
    /// One whole extent: what it cost when every extent was `ARENA_EXTENT_PAGES` long.
    const FLAT: f64 = (ARENA_EXTENT_PAGES as usize * PAGE_SIZE) as f64;
    /// "Within a small factor of". Generous on purpose — the catalog, the trunk's own pages and
    /// the partially filled last extent all land in the gap, and the two references are 256x
    /// apart, so nothing can be near both.
    const NEAR: f64 = 8.0;

    println!();
    println!("  reference: one page = {GROWN:.0} B  ·  one full extent = {FLAT:.0} B  ({}x apart)",
             ARENA_EXTENT_PAGES);
    if bytes_per_branch <= GROWN * NEAR {
        println!("VERDICT — AMPLIFICATION GONE. {bytes_per_branch:.0} B/branch is within {NEAR:.0}x of a");
        println!("single page, so a branch's first page costs about a page. This is the tree WITH");
        println!("geometric extent growth (D31) built; without it this number is ~{FLAT:.0}.");
    } else if bytes_per_branch >= FLAT / NEAR {
        println!("VERDICT — AMPLIFICATION PRESENT. {bytes_per_branch:.0} B/branch is within {NEAR:.0}x of a");
        println!("whole extent, so every branch pays {ARENA_EXTENT_PAGES} pages for its first one. This is the");
        println!("tree WITHOUT geometric extent growth, and it is what D31 exists to remove.");
    } else {
        println!("VERDICT — AMBIGUOUS. {bytes_per_branch:.0} B/branch sits between the two references above,");
        println!("near neither. Do NOT read this as either result: say which tree it was taken on,");
        println!("and look at `pages live` and the allocated-blocks column before concluding.");
    }
}


/// Bytes actually ALLOCATED to a file, as opposed to its addressed length -- `None` where the
/// platform cannot answer.
///
/// This is the whole instrument of D31: the gap between `len()` and allocation is the 256x space
/// amplification, so a fabricated number here would fabricate the finding. `std` exposes
/// `MetadataExt::blocks()` on unix only and has no Windows equivalent, so Windows gets `None` and
/// the caller prints `NaN` rather than a zero that reads as "no amplification".
///
/// Gated the way `storage::disk_manager::pwrite` already gates its platform split.
fn allocated_bytes(path: &std::path::Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(path).ok().map(|m| m.blocks() * 512)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}


/// Free bytes on the filesystem holding `path`, via `df -k`.
///
/// **A shared-machine guard, not a tuning knob — D61.** The byte budget below refuses to let this
/// run's own database grow past a size; it says nothing about what else is on the disk. Another
/// session on this machine tripped a disk monitor twice on 2026-09-19 while this repo held three
/// worktree targets, and an ENOSPC in someone else's lane reads exactly like a real test failure.
/// So the run also stops when the DISK is low, whatever its own database weighs.
fn free_bytes(path: &std::path::Path) -> Option<u64> {
    let out = std::process::Command::new("df").arg("-k").arg(path).output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text.lines().nth(1)?;
    let avail_kb: u64 = line.split_whitespace().nth(3)?.parse().ok()?;
    Some(avail_kb * 1024)
}

/// Stop if the filesystem drops below this, whatever this run's own budget says. See
/// [`free_bytes`]. 20 GiB leaves a working margin for every other lane on a shared machine.
const FREE_FLOOR: u64 = 20 * (1u64 << 30);

/// Removes the run directory when dropped — including during a panic's unwind.
struct RemoveOnDrop(std::path::PathBuf);
impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn be_u32(b: &[u8], at: usize) -> Option<usize> {
    b.get(at..at.checked_add(4)?).map(|s| u32::from_be_bytes(s.try_into().unwrap()) as usize)
}

/// Byte offset of the `current` section in a v3 arena image, walking the layout
/// `ArenaPageStore::image_len` walks: 21-byte header, counted free list of 8-byte entries, counted
/// extents of 28 bytes plus a counted recycled list of 4-byte ids. `None` for anything else.
///
/// A mis-walk cannot produce a false pass: [`reload_control`] requires the count found at this
/// offset to equal the run's branch count, which a wrong offset does not reproduce.
fn current_section_offset(img: &[u8]) -> Option<usize> {
    if img.first() != Some(&3) {
        return None;
    }
    let mut at = 21usize;
    let n_free = be_u32(img, at)?;
    at = at.checked_add(n_free.checked_mul(8)?.checked_add(4)?)?;
    let n_ext = be_u32(img, at)?;
    at = at.checked_add(4)?;
    for _ in 0..n_ext {
        at = at.checked_add(28)?;
        let rec = be_u32(img, at)?;
        at = at.checked_add(rec.checked_mul(4)?.checked_add(4)?)?;
    }
    Some(at)
}

/// D65 run 2's POSITIVE CONTROL on the arena column: did the timed reopen really load the map?
///
/// ⛔ **Run 2 first shipped this as `reopened.state_bytes() == img`, which can NEVER hold for
/// N > 0**: `ArenaPageStore::load_state` deserialises `current` and then deliberately clears it
/// ("Never resume filling a restored extent"), so the re-serialised state is `16 * N` bytes
/// shorter than the image. Found by review before the first run; the column would have read "NO"
/// at every checkpoint and voided itself under its own pre-registration.
///
/// The control now states the round trip `load_state` actually promises. Relative to `img`, the
/// re-serialisation `reload` must have: every byte before `current` (header, free list, EVERY
/// extent with its owner, start, length, fill mark and recycled list) identical; `current` present
/// in `img` with exactly `n` entries and EMPTY in `reload`; the `pending` section identical; and a
/// length exactly `16 * n` shorter. The CRC is not compared — `state_bytes` recomputes it.
///
/// What it proves: the extents section — the O(extents) part this column exists to time — was
/// deserialised in full, because a store that skipped it re-serialises an empty extent list.
/// What it does not prove: that the timer contained only that work.
fn reload_control(img: &[u8], reload: &[u8], n: usize) -> Result<(), &'static str> {
    let cur = current_section_offset(img).ok_or("walk")?;
    let n_cur = be_u32(img, cur).ok_or("walk")?;
    if n_cur != n {
        return Err("walk");
    }
    let removed = 16 * n_cur;
    if img.len() < cur + 4 + removed + 4 || reload.len() + removed != img.len() {
        return Err("len");
    }
    if reload[..cur] != img[..cur] {
        return Err("ext");
    }
    if reload[cur..cur + 4] != [0u8; 4] {
        return Err("cur");
    }
    if reload[cur + 4..reload.len() - 4] != img[cur + 4 + removed..img.len() - 4] {
        return Err("pend");
    }
    Ok(())
}

// =================================================================================================
// READ-VS-N — `bench/read_vs_n/PREREG.md`. Everything below this line and above `main` exists for
// `CURVE_ARMS`, and none of it runs without it.
// =================================================================================================

/// The table and row every branch writes, and trunk's value for it.
const TABLE: &str = "t";
const ROW: u64 = 1;
const TRUNK_VALUE: i64 = -1;
/// Reads per timed block. Arms alternate block by block, so both see the same box.
const BLOCK: usize = 64;
/// FIXED thread counts for the read arm, per the brief: one, then eight.
const READ_THREADS: [usize; 2] = [1, 8];
/// The parent's lease thread runs its first pass and then sleeps for this long. The leases in this
/// workload never expire, so its only other work would be the 60 s orphan sweep — a full catalog
/// walk dropped into a timed window at a random moment. The CHILD, which is the restart being
/// measured, uses the production interval.
const PARENT_SCAN_INTERVAL: Duration = Duration::from_secs(86_400);

/// Which PREREG arms this invocation runs. `None` from [`Arms::from_env`] means the historical
/// D61/D65 path, unchanged.
#[derive(Clone, Copy, Debug)]
struct Arms {
    read: bool,
    merge: bool,
    restart: bool,
}

impl Arms {
    /// `CURVE_ARMS=read,merge,restart`, any subset. Refuses a name it does not know rather than
    /// running less than was asked for.
    fn from_env() -> Option<Arms> {
        let raw = std::env::var("CURVE_ARMS").ok()?;
        let mut arms = Arms { read: false, merge: false, restart: false };
        for name in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            match name {
                "read" => arms.read = true,
                "merge" => arms.merge = true,
                "restart" => arms.restart = true,
                other => panic!("CURVE_ARMS: unknown arm {other:?}; the arms are read, merge, restart"),
            }
        }
        assert!(arms.read || arms.merge || arms.restart, "CURVE_ARMS={raw:?} names no arm");
        Some(arms)
    }
}

/// `CURVE_FIRECHECK=<mode>`: break exactly one guard's premise on purpose, so the guard is seen to
/// fire (PREREG section 4). Each mode is an injection at the harness call site, which is where each
/// of these guards lives — it tests the guard, not the engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fire {
    None,
    /// G2: the branch arm expects its NEIGHBOUR's value.
    WrongPage,
    /// G3: the branch arm's census deltas are discarded.
    CensusOff,
    /// G4: the control also asks the catalog.
    ControlCatalog,
    /// G5: the control reads a random branch's page by id, which misses at large N.
    ControlCold,
    /// G6: the identity is checked against height + 1.
    WrongHeight,
    /// G7: the control spins 10·2^checkpoint µs per read, so it grows with N.
    ControlDrift,
    /// H2: the parent expects one arena more than it counted.
    WrongArenas,
    /// H3: the parent expects one live branch more than it counted.
    WrongLive,
    /// M1: every merge's branch is quarantined first, so it returns `Ok` having published nothing.
    MergeQuarantined,
    /// M2: axis (i) expects its batch to start at one applied entry, not zero.
    WrongStart,
    /// M3: the identity expects one visit more than the log held.
    WrongVisits,
    /// M4: the constant-delta check expects one op more than the first merge appended.
    WrongDelta,
    /// M5: the parent expects one live branch more after a merge batch than before it.
    WrongLiveMerge,
}

impl Fire {
    fn from_env() -> Fire {
        match std::env::var("CURVE_FIRECHECK").as_deref() {
            Err(_) | Ok("") => Fire::None,
            Ok("wrong-page") => Fire::WrongPage,
            Ok("census-off") => Fire::CensusOff,
            Ok("control-catalog") => Fire::ControlCatalog,
            Ok("control-cold") => Fire::ControlCold,
            Ok("wrong-height") => Fire::WrongHeight,
            Ok("control-drift") => Fire::ControlDrift,
            Ok("wrong-arenas") => Fire::WrongArenas,
            Ok("wrong-live") => Fire::WrongLive,
            Ok("merge-quarantined") => Fire::MergeQuarantined,
            Ok("wrong-start") => Fire::WrongStart,
            Ok("wrong-visits") => Fire::WrongVisits,
            Ok("wrong-delta") => Fire::WrongDelta,
            Ok("wrong-live-merge") => Fire::WrongLiveMerge,
            Ok(other) => panic!(
                "CURVE_FIRECHECK: unknown mode {other:?}; the modes are wrong-page, census-off, \
                 control-catalog, control-cold, wrong-height, control-drift, wrong-arenas, \
                 wrong-live, merge-quarantined, wrong-start, wrong-visits, wrong-delta, \
                 wrong-live-merge"
            ),
        }
    }
}

/// The three read arms. `TrunkId` is the control: it cannot depend on N.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Arm {
    Branch,
    TrunkId,
    TrunkCat,
}
const ARMS: [Arm; 3] = [Arm::Branch, Arm::TrunkId, Arm::TrunkCat];

impl Arm {
    fn name(self) -> &'static str {
        match self {
            Arm::Branch => "branch",
            Arm::TrunkId => "trunk-id",
            Arm::TrunkCat => "trunk-cat",
        }
    }
}

/// One arm's totals at one (N, threads): the catalog phase (`cat`) and the data phase (`data`) of
/// every read, counted separately.
#[derive(Clone, Copy, Debug, Default)]
struct ArmTotals {
    reads: u64,
    nanos: u128,
    cat: ReadCensus,
    data: ReadCensus,
    /// Reads that did not return what that branch wrote (G2).
    wrong: u64,
}

impl ArmTotals {
    fn add(&mut self, o: &ArmTotals) {
        self.reads += o.reads;
        self.nanos += o.nanos;
        self.cat.add(&o.cat);
        self.data.add(&o.data);
        self.wrong += o.wrong;
    }
    fn ns_per_read(&self) -> f64 {
        self.nanos as f64 / self.reads.max(1) as f64
    }
    fn per(&self, count: u64) -> f64 {
        count as f64 / self.reads.max(1) as f64
    }
}

/// One printed READ block: an (N, threads) point, all three arms.
struct ReadRow {
    n: usize,
    /// Height of the branch catalog's tree, walked from its root — independent of the census.
    h: u64,
    threads: usize,
    arms: [ArmTotals; 3],
}

/// splitmix64: a named, seeded generator, so a run's branch choices are reproducible from the seed
/// rule printed in its header.
fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn read_seed(n: usize, threads: usize, t: usize) -> u64 {
    0xD197_0000_0000_0000 ^ ((n as u64) << 16) ^ ((threads as u64) << 8) ^ t as u64
}

/// Levels from the branch catalog's root to a leaf, walked through `read_node`. This is the
/// independent instrument PREREG P2/G6 checks the census against, so it must not come from the
/// census. It fetches pages and moves this thread's counts: call it outside every bracket.
fn catalog_height(cat: &TableBranchCatalog) -> u64 {
    let tree = BPlusTreeManager::<Vec<u8>, Vec<u8>>::open(
        cat.root_page_id(),
        Arc::clone(cat.pool_handle()),
    );
    let mut page = cat.root_page_id();
    let mut h = 1;
    loop {
        match tree.read_node(page).expect("walk the branch catalog") {
            BPlusTreePage::Leaf(_) => return h,
            BPlusTreePage::Internal(n) => {
                page = n.child_ptrs[0];
                h += 1;
            }
        }
    }
}

/// Everything the loop in `main` reads or writes through. In the production layout these are
/// clones out of an [`OpenDatabase`] and are REPLACED after every restart.
struct Handles {
    cat_concrete: Arc<TableBranchCatalog>,
    cat: Arc<dyn BranchCatalog>,
    store: Arc<ArenaPageStore>,
    /// `Some` only in the production layout.
    runtime: Option<Arc<AgentRuntime>>,
    statement_lock: Option<Arc<CatalogLock>>,
}

impl Handles {
    fn of(db: &OpenDatabase) -> Handles {
        Handles {
            cat_concrete: Arc::clone(&db.branches),
            cat: db.branches.clone() as Arc<dyn BranchCatalog>,
            store: Arc::clone(&db.store),
            runtime: Some(Arc::clone(&db.runtime)),
            statement_lock: Some(Arc::clone(&db.catalog)),
        }
    }
}

/// Wait for the lease thread's first pass to reach its end — the second full sweep, PREREG R3.
/// `None` if it did not within `bound`.
fn wait_first_pass(db: &OpenDatabase, bound: Duration) -> Option<Duration> {
    let t = Instant::now();
    while db.lease.stats().finished == 0 {
        if t.elapsed() > bound {
            return None;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Some(t.elapsed())
}

/// Open the production database, wait out its first lease pass, and hand it back. Every open the
/// PARENT does goes through here, so no timed window can overlap a sweep it started.
fn parent_open(db_path: &str, failures: &mut Vec<String>) -> OpenDatabase {
    let db = open_database(db_path, PARENT_SCAN_INTERVAL).expect("open the production database");
    let bound = Duration::from_secs(60) + db.timings.lease_start * 10;
    if wait_first_pass(&db, bound).is_none() {
        failures.push(format!(
            "the parent's first lease pass did not finish within {bound:?}; a sweep may overlap a \
             timed window"
        ));
    }
    db
}

/// Untimed: every branch once in order (up to 65,536), then 16,384 random reads. For N below the
/// pools this makes everything resident; above them it leaves the pools in the random steady state
/// the timed reads then sample.
fn warm_up(runtime: &AgentRuntime, branches: &[BranchId]) {
    let rows = runtime.storage().expect("the production runtime is page-backed");
    let tid = table_id(TABLE).0;
    let read = |b: BranchId| {
        let root = runtime.root_of(b).expect("root_of");
        let _ = rows.get(root, tid, ROW).expect("get");
    };
    for &b in branches.iter().take(65_536) {
        read(b);
    }
    let mut rng = read_seed(branches.len(), 0, 0xFF);
    for _ in 0..16_384 {
        read(branches[(splitmix(&mut rng) % branches.len() as u64) as usize]);
    }
}

/// PREREG arm 1 at one (N, threads): K reads per arm, in blocks of [`BLOCK`], the arm order rotated
/// per block and every thread on the same arm at once.
#[allow(clippy::too_many_arguments)]
fn read_phase(
    runtime: &AgentRuntime,
    store: &ArenaPageStore,
    branches: &[BranchId],
    trunk_root: PageId,
    cold_pages: &[PageId],
    threads: usize,
    k: usize,
    checkpoint_index: usize,
    fire: Fire,
) -> [ArmTotals; 3] {
    let blocks = (k / threads / BLOCK).max(1);
    let barrier = Barrier::new(threads);
    let rows = runtime.storage().expect("the production runtime is page-backed");
    let tid = table_id(TABLE).0;
    let n = branches.len() as u64;
    // 10 µs doubling per checkpoint: already 2x between the first two, far past G7's 1.5 band even
    // if the control read itself costs as much as the spin.
    let drift = Duration::from_micros(10u64 << checkpoint_index.min(16));
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let barrier = &barrier;
                s.spawn(move || {
                    let mut rng = read_seed(branches.len(), threads, t);
                    let mut tot = [ArmTotals::default(); 3];
                    for block in 0..blocks {
                        for j in 0..ARMS.len() {
                            let a = (block + j) % ARMS.len();
                            let arm = ARMS[a];
                            barrier.wait();
                            let t0 = Instant::now();
                            for _ in 0..BLOCK {
                                // Drawn on every arm, so every arm does the same arithmetic.
                                let pick = (splitmix(&mut rng) % n) as usize;
                                let c0 = this_thread();
                                let (ok, c1) = match arm {
                                    Arm::Branch | Arm::TrunkCat => {
                                        let (b, want) = match arm {
                                            Arm::Branch => {
                                                let writer = if fire == Fire::WrongPage {
                                                    branches[(pick + 1) % branches.len()]
                                                } else {
                                                    branches[pick]
                                                };
                                                (branches[pick], writer.id as i64)
                                            }
                                            _ => (BranchId::TRUNK, TRUNK_VALUE),
                                        };
                                        let root = runtime.root_of(b).expect("root_of");
                                        let c1 = this_thread();
                                        let got = rows.get(root, tid, ROW).expect("get");
                                        (got == Some(vec![Value::BigInt(want)]), c1)
                                    }
                                    Arm::TrunkId => {
                                        if fire == Fire::ControlCatalog {
                                            let _ = runtime.root_of(BranchId::TRUNK);
                                        }
                                        let c1 = this_thread();
                                        let page = if fire == Fire::ControlCold {
                                            cold_pages[pick]
                                        } else {
                                            trunk_root
                                        };
                                        if fire == Fire::ControlDrift {
                                            let until = Instant::now() + drift;
                                            while Instant::now() < until {}
                                        }
                                        (store.read_page(page).is_ok(), c1)
                                    }
                                };
                                let c2 = this_thread();
                                let r = &mut tot[a];
                                r.reads += 1;
                                if !ok {
                                    r.wrong += 1;
                                }
                                if !(fire == Fire::CensusOff && arm == Arm::Branch) {
                                    r.cat.add(&c1.since(&c0));
                                    r.data.add(&c2.since(&c1));
                                }
                            }
                            tot[a].nanos += t0.elapsed().as_nanos();
                        }
                    }
                    tot
                })
            })
            .collect();
        let mut sum = [ArmTotals::default(); 3];
        for h in handles {
            let tot = h.join().expect("reader thread");
            for (s, t) in sum.iter_mut().zip(tot.iter()) {
                s.add(t);
            }
        }
        sum
    })
}

fn print_read_header() {
    println!(
        "  READ          N   h   T  arm          reads     ns/read   c.desc   c.att    c.opt  c.omiss   \
         c.walk    c.lat  c.fetch  c.fault    c.hop  d.fetch  d.fault  wrong"
    );
}

fn print_read_row(row: &ReadRow) {
    for (i, arm) in ARMS.iter().enumerate() {
        let r = &row.arms[i];
        println!(
            "  READ   {:>8} {:>3} {:>3}  {:<10} {:>7} {:>11.1} {:>8.3} {:>7.3} {:>8.3} {:>8.3} {:>8.3} \
             {:>8.3} {:>8.3} {:>8.3} {:>8.3} {:>8.3} {:>8.3} {:>6}",
            row.n,
            row.h,
            row.threads,
            arm.name(),
            r.reads,
            r.ns_per_read(),
            r.per(r.cat.descents),
            r.per(r.cat.attempts),
            r.per(r.cat.optimistic),
            r.per(r.cat.optimistic_misses),
            r.per(r.cat.right_walks),
            r.per(r.cat.latched),
            r.per(r.cat.fetches),
            r.per(r.cat.faults),
            r.per(r.cat.scan_leaves),
            r.per(r.data.fetches),
            r.per(r.data.faults),
            r.wrong,
        );
    }
}

/// The integer guards on one READ block (PREREG G2, G3, G4, G6). G5 and G7 are about the ns
/// columns and are judged in the summary.
fn read_guards(row: &ReadRow, fire: Fire, failures: &mut Vec<String>) {
    let at = format!("N={} T={}", row.n, row.threads);
    let [branch, control, trunk_cat] = &row.arms;
    for (i, arm) in ARMS.iter().enumerate() {
        if row.arms[i].wrong > 0 {
            failures.push(format!(
                "G2 {at} {}: {} of {} reads did not return what was written",
                arm.name(),
                row.arms[i].wrong,
                row.arms[i].reads
            ));
        }
    }
    if branch.cat.descents == 0 || branch.data.fetches == 0 {
        failures.push(format!(
            "G3 {at}: the census did not fire on the branch arm (c.desc {}, d.fetch {})",
            branch.cat.descents, branch.data.fetches
        ));
    }
    if control.cat != ReadCensus::default() || control.data.fetches != control.reads {
        failures.push(format!(
            "G4 {at}: the control touched the catalog or did not fetch exactly once per read \
             (c = {:?}, d.fetch {} over {} reads)",
            control.cat, control.data.fetches, control.reads
        ));
    }
    let h = if fire == Fire::WrongHeight { row.h + 1 } else { row.h };
    for (arm, r) in [(Arm::Branch, branch), (Arm::TrunkCat, trunk_cat)] {
        if r.cat.attempts == r.cat.descents
            && r.cat.optimistic != r.cat.descents * h + r.cat.right_walks
        {
            failures.push(format!(
                "G6 {at} {}: no restarts, yet c.opt {} != c.desc {} x h {} + c.walk {}",
                arm.name(),
                r.cat.optimistic,
                r.cat.descents,
                h,
                r.cat.right_walks
            ));
        }
    }
}

/// What one restart measured. Microseconds, as the child printed them.
#[derive(Default)]
struct RestartRow {
    n: usize,
    expect_arenas: u64,
    expect_live: u64,
    child: std::collections::HashMap<String, u64>,
    parent_total_us: u64,
    parent_lease_us: u64,
}

impl RestartRow {
    fn get(&self, key: &str) -> u64 {
        self.child.get(key).copied().unwrap_or(u64::MAX)
    }
}

/// The CHILD: a fresh process that opens the database exactly as the shipped binary does, times
/// it, waits for the lease thread's first pass, closes cleanly, and prints ONE tagged line.
///
/// A fresh process because a real restart is one (D65 adversary, finding e): the parent has a
/// large live heap and warm allocator. The OS page cache is warm either way — purging it needs
/// root — so this is a WARM-CACHE restart, and PREREG says so.
fn open_only_child(db_path: &str) -> ! {
    let interval = scan_interval_from_env().expect("lease scan interval");
    let c0 = this_thread();
    let db = open_database(db_path, interval).expect("open the database");
    let c = this_thread().since(&c0);
    let t = db.timings;
    let open_visits = db.reaper.open_sweep_visits();
    let freed = db.reaper.open_sweep_freed();
    let bound = Duration::from_secs(60) + t.lease_start * 10;
    let first = wait_first_pass(&db, bound);
    let visits_total = db.reaper.sweep_visits();
    let descents_total = db.reaper.sweep_descents();
    // Untimed positive control (PREREG H3): the database that opened is the populated one.
    let live = db.branches.live_count().map(|n| n as u64).unwrap_or(u64::MAX);
    let (_, closed) = db.close();
    closed.expect("close the database cleanly");
    let us = |d: Duration| d.as_micros() as u64;
    println!(
        "RESTART_RESULT total_us={} lock_us={} files_us={} recover_us={} sql_catalog_us={} \
         rebuild_us={} branch_catalog_us={} arena_us={} effect_log_us={} runtime_us={} \
         provenance_us={} lease_start_us={} \
         open_visits={} freed={} first_pass_done={} first_pass_us={} visits_total={} \
         descents_total={} live={} c_desc={} c_att={} c_opt={} c_omiss={} c_lat={} c_fetch={} \
         c_fault={} c_hop={}",
        us(t.total),
        us(t.lock),
        us(t.boot.files),
        us(t.boot.recover),
        us(t.boot.catalog),
        us(t.boot.rebuild),
        us(t.branch_catalog),
        us(t.arena),
        us(t.effect_log),
        us(t.runtime),
        us(t.provenance),
        us(t.lease_start),
        open_visits,
        freed,
        first.is_some() as u64,
        first.map(us).unwrap_or(0),
        visits_total,
        descents_total,
        live,
        c.descents,
        c.attempts,
        c.optimistic,
        c.optimistic_misses,
        c.latched,
        c.fetches,
        c.faults,
        c.scan_leaves,
    );
    std::process::exit(0);
}

/// Run the child on `db_path` and parse its line. `Err` names what went wrong (PREREG H1).
fn run_child(db_path: &str) -> Result<std::collections::HashMap<String, u64>, String> {
    let exe = std::env::current_exe().map_err(|e| format!("own path: {e}"))?;
    let out = std::process::Command::new(exe)
        .arg("--open-only")
        .arg(db_path)
        .stderr(std::process::Stdio::inherit())
        .output()
        .map_err(|e| format!("spawn: {e}"))?;
    if !out.status.success() {
        return Err(format!("child exited {:?}", out.status.code()));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = text.lines().filter(|l| l.starts_with("RESTART_RESULT ")).collect();
    if lines.len() != 1 {
        return Err(format!("child printed {} RESTART_RESULT lines, not 1", lines.len()));
    }
    let mut map = std::collections::HashMap::new();
    for kv in lines[0].split_whitespace().skip(1) {
        let (k, v) = kv.split_once('=').ok_or_else(|| format!("unparseable field {kv:?}"))?;
        let v: u64 = v.parse().map_err(|e| format!("field {k}={v:?}: {e}"))?;
        map.insert(k.to_string(), v);
    }
    Ok(map)
}

fn print_restart_header() {
    println!(
        "  RESTART       N   arenas   total ms  lease_st ms   arena ms  br_cat ms  recover ms  \
         tel ms  prov ms  open visits  1st-pass visits  1st-pass ms  c.desc/v  c.att/v  c.fault/v  \
         freed  live  parent ms"
    );
}

fn print_restart_row(r: &RestartRow) {
    let ms = |k: &str| r.get(k) as f64 / 1000.0;
    let visits = r.get("open_visits");
    let per_visit = |k: &str| r.get(k) as f64 / visits.max(1) as f64;
    let first_pass_visits = r.get("visits_total").saturating_sub(visits);
    println!(
        "  RESTART {:>8} {:>8} {:>10.3} {:>12.3} {:>10.3} {:>10.3} {:>11.3} {:>7.3} {:>8.3} {:>12} \
         {:>16} {:>12.3} {:>9.3} {:>8.3} {:>10.4} {:>6} {:>5} {:>10.3}",
        r.n,
        r.expect_arenas,
        ms("total_us"),
        ms("lease_start_us"),
        ms("arena_us"),
        ms("branch_catalog_us"),
        ms("recover_us"),
        ms("effect_log_us"),
        ms("provenance_us"),
        visits,
        first_pass_visits,
        ms("first_pass_us"),
        per_visit("c_desc"),
        per_visit("c_att"),
        per_visit("c_fault"),
        r.get("freed"),
        r.get("live"),
        r.parent_total_us as f64 / 1000.0,
    );
}

/// PREREG H2-H5 on one restart.
fn restart_guards(r: &RestartRow, fire: Fire, failures: &mut Vec<String>) {
    let at = format!("N={}", r.n);
    let want_arenas = r.expect_arenas + (fire == Fire::WrongArenas) as u64;
    let want_live = r.expect_live + (fire == Fire::WrongLive) as u64;
    if r.get("open_visits") != want_arenas {
        failures.push(format!(
            "H2 {at}: the open sweep visited {} arenas; the parent counted {want_arenas} live",
            r.get("open_visits")
        ));
    }
    if r.get("live") != want_live {
        failures.push(format!(
            "H3 {at}: the reopened catalog has {} live branches; the parent counted {want_live}",
            r.get("live")
        ));
    }
    if r.get("freed") != 0 {
        failures.push(format!(
            "H4 {at}: the open sweep freed {} extents from a cleanly closed database",
            r.get("freed")
        ));
    }
    if r.get("first_pass_done") != 1 {
        failures.push(format!("H5 {at}: the lease thread's first pass did not finish in its bound"));
    }
}

// ---- ARM 2: MERGE against N and against M (PREREG amendment A3) --------------------------------

/// The SQL table every merge publishes one NEW row into, created once on trunk.
const MERGE_TABLE: &str = "m";
/// Merges each axis-(ii) report averages: the last this many before its M target.
const MERGE_BLOCK: usize = 64;

/// One SQL statement, the way `run_cli`'s `execute_sql` runs it: parsed, then run under the
/// statement lock with the database's own pool and transaction manager.
fn exec_sql(db: &OpenDatabase, sess: &mut Session, sql: &str) -> Result<Outcome, String> {
    let tokens = Scanner::new(sql.chars().collect(), Vec::new())
        .scan_tokens()
        .map_err(|e| format!("{sql}: {e:?}"))?;
    let mut parser = Parser::new(tokens);
    let mut stmts = parser.parse();
    if !parser.errors.is_empty() || stmts.is_empty() {
        return Err(format!("{sql}: {:?}", parser.errors));
    }
    let mut cat = db.catalog.lock();
    run(stmts.remove(0), &mut cat, db.bp.clone(), db.txn.clone(), sess).map_err(|e| format!("{sql}: {e}"))
}

/// What one `MERGE;` cost and did. Everything is bracketed around that statement ALONE: the
/// session's `BEGIN` and `INSERT` are outside, and so are the `state_sizes` reads, which take the
/// lock every statement takes.
#[derive(Clone, Copy, Debug, Default)]
struct MergeOne {
    nanos: u128,
    applied_to_target: bool,
    before: StateSizes,
    after: StateSizes,
    attested_after: u64,
    /// `highest_applied_seq`'s entries (Q1).
    v_hi: u64,
    /// `concurrent_op`'s reads through D86's index (Q3).
    v_cell: u64,
    seq_tuples: u64,
    index_scans: u64,
    fsyncs: u64,
    census: ReadCensus,
}

/// One agent task: `BEGIN AGENT SESSION`, one INSERT of a row nobody has written, `MERGE;`.
fn merge_cycle(db: &OpenDatabase, id: i64, fire: Fire) -> MergeOne {
    let mut sess = Session::with_runtime(Arc::clone(&db.runtime));
    exec_sql(db, &mut sess, &format!("BEGIN AGENT SESSION AS 'mc{id}';")).expect("begin");
    exec_sql(db, &mut sess, &format!("INSERT INTO {MERGE_TABLE} VALUES ({id}, {id});"))
        .expect("insert");
    if fire == Fire::MergeQuarantined {
        let b = sess.agent.as_ref().expect("an open agent session").branch;
        db.runtime.quarantine(b, "READ-VS-N merge-quarantined fire check").expect("quarantine");
    }
    let before = db.runtime.state_sizes();
    let (v0, c0) = merge_log_counters();
    let seq0 = seq_scan_counters().1;
    let ix0 = index_scan_counters().0;
    let f0 = FSYNC_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let census0 = this_thread();
    let t = Instant::now();
    let out = exec_sql(db, &mut sess, "MERGE;");
    let nanos = t.elapsed().as_nanos();
    let census = this_thread().since(&census0);
    let fsyncs = FSYNC_CALLS.load(std::sync::atomic::Ordering::Relaxed) - f0;
    let index_scans = index_scan_counters().0 - ix0;
    let seq_tuples = seq_scan_counters().1 - seq0;
    let (v1, c1) = merge_log_counters();
    let after = db.runtime.state_sizes();
    let applied_to_target =
        matches!(&out, Ok(Outcome::Agent(AgentOutput::Merge(r))) if r.applied_to_target);
    MergeOne {
        nanos,
        applied_to_target,
        before,
        after,
        attested_after: db.runtime.attested_len() as u64,
        v_hi: v1 - v0,
        v_cell: c1 - c0,
        seq_tuples,
        index_scans,
        fsyncs,
        census,
    }
}

/// The per-merge guards (PREREG A3: M1, M3, M4). `delta` holds the first merge's append count for
/// the whole run, so M4 compares every merge against the same number.
fn merge_guards(at: &str, one: &MergeOne, fire: Fire, delta: &mut Option<u64>, failures: &mut Vec<String>) {
    if !one.applied_to_target {
        failures.push(format!("M1 {at}: a MERGE did not reach the target, so it measured nothing"));
        return;
    }
    let want_visits = one.before.applied as u64 + (fire == Fire::WrongVisits) as u64;
    if one.v_hi != want_visits {
        failures.push(format!(
            "M3 {at}: highest_applied_seq visited {} entries of a {}-entry log",
            one.v_hi, want_visits
        ));
    }
    let appended = (one.after.applied - one.before.applied) as u64;
    let first = *delta.get_or_insert(appended);
    let want = first + (fire == Fire::WrongDelta) as u64;
    if appended != want {
        failures.push(format!("M4 {at}: this merge appended {appended} ops; the run's first appended {want}"));
    }
}

/// One printed MERGE row: the per-merge MEANS of a batch (axis i) or of a block (axis ii).
struct MergeRow {
    axis: &'static str,
    n: usize,
    /// Merges published since the last open, at the end of the block.
    m: usize,
    ones: Vec<MergeOne>,
}

impl MergeRow {
    fn mean(&self, f: impl Fn(&MergeOne) -> u64) -> f64 {
        self.ones.iter().map(|o| f(o) as f64).sum::<f64>() / self.ones.len().max(1) as f64
    }
    fn ns_median(&self) -> f64 {
        let mut v: Vec<u128> = self.ones.iter().map(|o| o.nanos).collect();
        v.sort_unstable();
        v.get(v.len() / 2).copied().unwrap_or(0) as f64
    }
}

fn print_merge_header() {
    println!(
        "  MERGE axis        N        M  merges   ns/merge  ns median     V_hi   V_cell  d.applied  \
         applied  captures   merges  versions  wkspaces  attested  seq tup  ix scans   fsyncs   \
         c.desc  c.fault    c.att"
    );
}

fn print_merge_row(r: &MergeRow) {
    let last = r.ones.last().copied().unwrap_or_default();
    println!(
        "  MERGE {:>4} {:>8} {:>8} {:>7} {:>10.0} {:>10.0} {:>8.1} {:>8.2} {:>10.2} {:>8} {:>9} \
         {:>8} {:>9} {:>9} {:>9} {:>8.2} {:>9.2} {:>8.2} {:>8.2} {:>8.3} {:>8.2}",
        r.axis,
        r.n,
        r.m,
        r.ones.len(),
        r.mean(|o| o.nanos as u64),
        r.ns_median(),
        r.mean(|o| o.v_hi),
        r.mean(|o| o.v_cell),
        r.mean(|o| (o.after.applied - o.before.applied) as u64),
        last.after.applied,
        last.after.captures,
        last.after.merges,
        last.after.versions,
        last.after.workspaces,
        last.attested_after,
        r.mean(|o| o.seq_tuples),
        r.mean(|o| o.index_scans),
        r.mean(|o| o.fsyncs),
        r.mean(|o| o.census.descents),
        r.mean(|o| o.census.faults),
        r.mean(|o| o.census.attempts),
    );
}

/// Close cleanly and reopen, so `State` — which lives in memory (`reopen_with_storage` builds
/// `State::default()`) — starts empty: M = 0. Returns the new handles; the caller swaps them in.
fn reopen_for_merges(db: OpenDatabase, db_path: &str, failures: &mut Vec<String>) -> OpenDatabase {
    let (_, closed) = db.close();
    closed.expect("close cleanly before a merge batch");
    parent_open(db_path, failures)
}

/// `ln(b/a) / ln(nb/na)`: the local log-log slope between two checkpoints.
fn slope(a: f64, b: f64, na: usize, nb: usize) -> f64 {
    (b / a).ln() / (nb as f64 / na as f64).ln()
}

fn main() {
    // READ-VS-N arm 3: this binary re-executes itself as the restart being measured. Decided
    // before anything else is parsed, so a child cannot mistake its database path for checkpoints.
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("--open-only") {
        open_only_child(argv.get(2).expect("usage: --open-only <database path>"));
    }
    let arms = Arms::from_env();
    let fire = Fire::from_env();
    let read_k: usize = std::env::var("CURVE_READ_K").ok().and_then(|v| v.parse().ok()).unwrap_or(16_384);
    // Arm 2 (PREREG A3): K merges per checkpoint on axis (i), and the M targets of axis (ii).
    let merge_k: usize = std::env::var("CURVE_MERGE_K").ok().and_then(|v| v.parse().ok()).unwrap_or(64);
    let merge_targets: Vec<usize> = std::env::var("CURVE_MERGE_M")
        .unwrap_or_else(|_| "256,1024,4096,16384".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let checkpoints: Vec<usize> = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "1000,2000,4000,8000".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let threads: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(8);
    let budget_gb: f64 = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(8.0);
    let budget = (budget_gb * 1e9) as u64;

    let dir = std::env::temp_dir().join(format!("ferrodb-wcurve-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    // Declared first so it drops LAST, after every handle below. A panic anywhere in this run
    // (every `expect` here is one) used to skip the old trailing `remove_dir_all` and leave a
    // multi-GB database in the temp dir of a disk this harness is guarding with FREE_FLOOR.
    let _cleanup = RemoveOnDrop(dir.clone());
    // READ-VS-N: guard failures, and failures that void only the ns columns. Printed at the end,
    // and either one makes the run exit 2.
    let mut failures: Vec<String> = Vec::new();
    let mut ns_void: Vec<String> = Vec::new();
    // READ-VS-N's PRODUCTION LAYOUT (`CURVE_ARMS` set): the database is `curve.db`, opened by the
    // shipped binary's own `open_database`. Otherwise the historical D61/D65 files, built by hand
    // exactly as before.
    let db_path_str = dir.join("curve.db").to_str().expect("a UTF-8 temp path").to_string();
    let mut db: Option<OpenDatabase> = None;
    let (main_path, cat_path) = if arms.is_some() {
        (dir.join("curve.db"), dir.join("curve.db.branchcat"))
    } else {
        (dir.join("main.db"), dir.join("branches.branchcat"))
    };
    let mut hd = if arms.is_some() {
        let opened = parent_open(&db_path_str, &mut failures);
        let hd = Handles::of(&opened);
        db = Some(opened);
        hd
    } else {
        let mf = std::fs::OpenOptions::new()
            .create(true).read(true).write(true).open(&main_path).unwrap();
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(mf).unwrap())));
        let _ = std::fs::remove_file(&cat_path);
        // Two handles to ONE catalog, deliberately: `root_page_id` is on the concrete type and not on
        // the `BranchCatalog` trait, and D65's reopen needs the CURRENT root rather than the 1 this
        // was opened with — reopening at a stale root would time the wrong thing.
        // ⛔ CORRECTED 2026-09-24 (D65 run 4 adversary, `artie-research frontier/d65_run4_adversary.md` §3; lead-verified in
        // `TableBranchCatalog::open_sidecar` at 9aa6968): `trunk_root` is used ONLY when the file is fresh (`create_with_header`).
        // A populated sidecar reopens from its header page, so no reopen can time a stale root. The concern above cannot arise,
        // and the error, if any, was in the measurement's favour. Comment only; the timed code is unchanged.
        let cat_concrete = Arc::new(TableBranchCatalog::open_sidecar(&cat_path, 1).expect("open catalog"));
        let cat: Arc<dyn BranchCatalog> = cat_concrete.clone();
        let base = pool.disk_manager.high_water().unwrap();
        let store = Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&cat), base).unwrap());
        Handles { cat_concrete, cat, store, runtime: None, statement_lock: None }
    };
    // The arena floor: fixed at creation and carried in the checkpoint, so it survives a restart.
    let base = hd.store.base_page();
    // **D79: this harness did NOT persist the free-space map, and the shipped binary does.**
    //
    // `ArenaPageStore` writes the map only if `checkpoint_to` has been called (see
    // `ArenaPageStore::checkpoint_to`; cited as `arena.rs:914` when D79 was written, and the line
    // has since moved — grep the name, not the number). `cli.rs` calls it; this file never did —
    // so D61's published 10^6 curve, and every other 10^6 result in this repo, measured a
    // configuration production does not run. THAT HALF STILL STANDS.
    //
    // ⛔ **THE NEXT SENTENCE OF D79 IS SUPERSEDED — BANDED, NOT DELETED, SO THE REVERSAL IS
    // VISIBLE WHERE THE CLAIM WAS MADE.** D79 said the map "is re-serialised and re-fsynced IN
    // FULL on every new branch's first page write, which is `sum(48i) = 24N^2` bytes over a run:
    // ~24 TB at 10^6." **That was true when written and is FALSE AT HEAD.** D81 (`53a6b66`) made
    // a claim append a 45-byte tail record instead, so the write volume is O(N); the three
    // remaining full-rewrite sites fire once per REAP, not once per branch created.
    //
    // D168 re-derived the consequence in THIS harness: the OFF/ON penalty is **flat in N**
    // (1.36x -> 1.36x across N=500..4000) where D80 measured it GROWING 1.88x -> 3.29x before
    // D81 landed. See `bench/d168_persistence_penalty_at_head.txt`. A flat ~1.4x remains and is
    // the per-claim fsync, which D81 never claimed to remove.
    //
    // `CURVE_PERSIST=1` turns it on so the two curves can be compared. It is OFF by default so
    // that re-running this file reproduces the historical numbers rather than silently replacing
    // them with different ones under the same name.
    let persist = std::env::var("CURVE_PERSIST").map(|v| v == "1").unwrap_or(false);
    if persist && arms.is_none() {
        hd.store.checkpoint_to(dir.join("main.db.arena"));
    }
    println!(
        "free-space map persistence: {}",
        if arms.is_some() {
            "ON (the production open arms it; CURVE_PERSIST is not consulted)"
        } else if persist {
            "ON (as cli.rs:120 ships)"
        } else {
            "OFF (historical default)"
        }
    );
    let lease = LeaseDeadline(u64::MAX);

    // READ-VS-N: every branch this run created, in creation order, and trunk's root page.
    let mut branches: Vec<BranchId> = Vec::new();
    let mut trunk_root: PageId = 0;
    let mut read_rows: Vec<ReadRow> = Vec::new();
    let mut restart_rows: Vec<RestartRow> = Vec::new();
    let mut checkpoint_index = 0usize;
    // Arm 2: every printed MERGE row, the next never-used row id, and the first merge's append
    // count, which every later merge is held to (M4).
    let mut merge_rows: Vec<MergeRow> = Vec::new();
    let mut next_merge_row: i64 = 1;
    let mut merge_delta: Option<u64> = None;
    if let (Some(arms), Some(rt), Some(statement_lock)) =
        (arms, hd.runtime.clone(), hd.statement_lock.clone())
    {
        // Trunk's row, written BEFORE any fork so every branch inherits the page holding it.
        {
            let _statement = statement_lock.lock();
            rt.put_row(BranchId::TRUNK, TABLE, ROW, &[Value::BigInt(TRUNK_VALUE)])
                .expect("seed trunk's row");
        }
        trunk_root = rt.root_of(BranchId::TRUNK).expect("trunk's root");
        if arms.merge {
            // Arm 2's target table, on trunk, through an ordinary session. Every merge publishes
            // one new row into it (PREREG A3: an INSERT, not an UPDATE, so D178's scan is not on
            // the path).
            let mut plain = Session::with_runtime(Arc::clone(&rt));
            exec_sql(
                db.as_ref().expect("the production database is open"),
                &mut plain,
                &format!("CREATE TABLE {MERGE_TABLE} (id INTEGER NOT NULL, v INTEGER);"),
            )
            .expect("create the merge table");
        }
        println!("READ-VS-N (bench/read_vs_n/PREREG.md). Built {}", ferrodb::build_provenance());
        println!(
            "  arms: read={} merge={} restart={}   firecheck: {fire:?}   K = {read_k} reads per arm \
             per thread count   read threads {READ_THREADS:?}   block {BLOCK}",
            arms.read, arms.merge, arms.restart
        );
        println!(
            "  merge arm: axis (i) {merge_k} merges per checkpoint, each after an open (M starts at 0); \
             axis (ii) after the last checkpoint, M targets {merge_targets:?}, each reporting the last \
             {MERGE_BLOCK} merges before it. One new row of table {MERGE_TABLE} per merge."
        );
        println!(
            "  layout: PRODUCTION, via ferrodb::cli::cli::open_database at {db_path_str}. Parent lease \
             interval {PARENT_SCAN_INTERVAL:?}, first pass awaited before anything is timed; the \
             restart child is a fresh process running the same open at the production interval."
        );
        println!(
            "  workload: trunk row {ROW} = BigInt({TRUNK_VALUE}); each branch forks from trunk and \
             writes row {ROW} = BigInt(its id) with put_row, both under the statement lock."
        );
        println!("  seed rule: splitmix64 from 0xD197<<48 ^ N<<16 ^ threads<<8 ^ thread");
        print_read_header();
        print_restart_header();
        print_merge_header();
    }

    println!("D32: the curve to 10^6 WITH ONE PAGE WRITTEN PER BRANCH. {threads} threads.");
    println!("ARENA_EXTENT_PAGES = {ARENA_EXTENT_PAGES}, so D31 predicts ~{} KiB of DATA file per branch.",
             ARENA_EXTENT_PAGES * 4);
    println!("Budget: {budget_gb} GB. The run REFUSES to exceed it rather than fill the disk.");
    println!();
    // Two space columns on purpose. `data MB` is FILE LENGTH; `alloc MB` is blocks*512, what the
    // filesystem actually gave out. A reservation scheme can inflate length far past allocation, and
    // quoting only length would overstate the wall. Both are reported so neither can be cherry-picked.
    // The first nine columns are the historical ones, byte-for-byte in format, so every earlier
    // artifact of this harness still lines up. The five after `reopen ms` are D65 run 2's; see the
    // block that computes them for what each one proves.
    println!("         N   forks/sec   data MB   alloc MB   len B/branch   alloc B/branch   cat B/branch   pages live   reopen ms   cat live@re   img B/branch   arena re ms   arena re pg    reload ctl");

    let mut done = 0usize;
    let mut stopped_early: Option<(usize, u64)> = None;
    let mut negatives_checked = false;

    for &target in &checkpoints {
        if target <= done || stopped_early.is_some() {
            continue;
        }
        let seg = target - done;
        let per = seg / threads.max(1);
        let actually = per * threads;
        if actually == 0 {
            continue;
        }

        let t0 = Instant::now();
        let mut created: Vec<BranchId> = Vec::new();
        std::thread::scope(|s| {
            let writers: Vec<_> = (0..threads).map(|_| {
                let cat = Arc::clone(&hd.cat);
                let store = Arc::clone(&hd.store);
                let prod = hd.runtime.clone().zip(hd.statement_lock.clone());
                s.spawn(move || {
                    let mut mine: Vec<BranchId> = Vec::new();
                    for _ in 0..per {
                        if let Some((rt, statement_lock)) = &prod {
                            // READ-VS-N (PREREG section 1): the fork and the row write under ONE
                            // hold of the statement lock, as the CLI and pgwire run statements.
                            // `put_row` copies trunk's leaf into this branch's arena and publishes
                            // it with `set_root`, so the branch owns exactly one page.
                            let _statement = statement_lock.lock();
                            let rec = cat.fork(BranchId::TRUNK, lease).expect("fork");
                            rt.put_row(rec.branch_id, TABLE, ROW, &[Value::BigInt(rec.branch_id.id as i64)])
                                .expect("put_row");
                            mine.push(rec.branch_id);
                            continue;
                        }
                        let rec = cat.fork(BranchId::TRUNK, lease).expect("fork");
                        // THE THREE CALLS `branch_curve` LEAVES OUT. This is the whole difference.
                        let arena = store.arena_for(rec.branch_id).expect("arena");
                        let ep = cat.next_epoch();
                        let p = store
                            .alloc_in_arena(arena, PageType::BTreeLeaf, ep)
                            .expect("alloc");
                        let h = store.read_page(p).expect("read");
                        let mut f = h.write();
                        f.data[PAGE_HEADER_SIZE] = 0xD3;
                        stamp_checksum(&mut f.data);
                    }
                    mine
                })
            }).collect();
            for w in writers {
                created.extend(w.join().expect("writer thread"));
            }
        });
        let secs = t0.elapsed().as_secs_f64();
        branches.extend(created);
        done += actually;

        let md = std::fs::metadata(&main_path).ok();
        let data = md.as_ref().map(|m| m.len()).unwrap_or(0);
        let alloc = allocated_bytes(&main_path).unwrap_or(0);
        let cbytes = std::fs::metadata(&cat_path).map(|m| m.len()).unwrap_or(0);

        // D65 — reopen the catalog from disk, WITH DATA PRESENT.
        //
        // S4's O(1)-reopen claim has only ever been checked by `examples/branch_curve.rs`, which is
        // FORK-ONLY: no branch in it calls `arena_for` or `alloc_in_arena`, so it reopens a catalog
        // whose branches own no pages. This harness is the one that writes, and it did not measure
        // reopen at all — and it deletes its database at the end, so D61 could not answer this
        // after the fact. The timing block is `branch_curve.rs`'s own, reused rather than rewritten.
        //
        // Reported PER CHECKPOINT on purpose: one reopen number at 10^6 cannot separate O(1) from
        // O(log N) from a small O(N). The column across the decade is the measurement; a single
        // cell is an anecdote.
        let root = hd.cat_concrete.root_page_id();
        let t_reopen = Instant::now();
        let re = TableBranchCatalog::open_sidecar(&cat_path, root).expect("reopen");
        let reopen_ms = t_reopen.elapsed().as_secs_f64() * 1000.0;

        // D65 run 2 — POSITIVE CONTROL for the column above. A flat `reopen ms` is only evidence if
        // the catalog it opened is the populated one: an open that found an empty or stale tree
        // would be flat for the wrong reason. So ask the REOPENED handle how many branches are
        // live. Expected N + 1 (the trunk is Live too). Untimed — this is O(N) by construction
        // (`TableBranchCatalog::live_count` walks the Live span) and is not part of an open.
        let re_live = re.live_count().map(|n| n.to_string()).unwrap_or_else(|e| format!("ERR:{e:?}"));
        let re: Arc<dyn BranchCatalog> = Arc::new(re);

        // D65 run 2 — THE ARENA'S reopen, which the catalog column above does NOT measure.
        //
        // SCALE-DESIGN D189's addendum routed this question to D65: `<db>.arena`'s image holds every
        // extent, `current` entry and `pending` entry, and `reopen_from_checkpoint` deserialises
        // all of it in counted loops, so by source the arena's open is O(extents), once, at open.
        // D189 says in so many words not to quote the catalog's O(1) reopen for the arena. The
        // block above times `TableBranchCatalog::open_sidecar` only — a different file and a
        // different structure — so without this block a flat catalog column would be read as
        // closing a question it never asked.
        //
        // This is the production restart's CODE PATH: `cli.rs` writes `store.checkpoint` at clean
        // exit and opens with `ArenaPageStore::reopen_from_checkpoint`. ⚠ It is timed WARM-CACHE:
        // the probe was written and fsynced moments before and is read once below, so the OS page
        // cache holds it. A cold restart (after a reboot) additionally pays reading the image off
        // the SSD; that arm is not measured here — purging the cache needs root. The catalog
        // column above is warm for the same reason. What IS inside the timer, by source:
        // `base_page_in_state` (a structural walk + CRC32 via `image_len`), `reopen` (bitmap scan
        // of `main.db`, small), `load_file` (`image_len` AGAIN: a second walk + CRC32), and
        // `load_state` (a third CRC32 plus the deserialisation: per extent ~5 hash inserts —
        // `extents`, `recycled`, `current`, `claim_epoch`, `fill_unknown`). The checkpoint goes
        // to a SEPARATE probe path, so the live run's file (if `CURVE_PERSIST=1` armed one) is
        // never touched — `checkpoint` aimed anywhere but the armed path leaves its accounting
        // alone, by its own doc comment. The fresh pool over `main.db` is built OUTSIDE the timer:
        // it is the same constant the catalog column already pays, and the question is the
        // arena's deserialisation, not a second copy of that constant.
        //
        // Nothing is written through the reopened store: `reopen_from_checkpoint` arms the probe
        // path but writes only on a later claim, and the store is dropped without one.
        let probe = dir.join("probe.arena");
        hd.store.checkpoint(&probe).expect("probe checkpoint");
        // How many `current` entries the image must carry. Historically one per branch, so `done`.
        // In the production layout trunk writes too, and every restart clears `current`
        // ("Never resume filling a restored extent", `load_state`), so the count is the store's own.
        let n_current = if arms.is_some() { hd.store.current_arena_count() } else { done };
        let img = std::fs::read(&probe).expect("read probe image");
        let pool2 = {
            let f = std::fs::OpenOptions::new().read(true).write(true).open(&main_path).unwrap();
            Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())))
        };
        let t_arena = Instant::now();
        let reopened = ArenaPageStore::reopen_from_checkpoint(pool2, Arc::clone(&re), &probe)
            .expect("arena reopen");
        let arena_ms = t_arena.elapsed().as_secs_f64() * 1000.0;
        // POSITIVE CONTROL for the arena column — see `reload_control` for what it proves, and for
        // why its first version (plain byte equality) could never pass.
        let reload = reopened.state_bytes();
        let reload_ctl = match reload_control(&img, &reload, n_current) {
            Ok(()) => "ok".to_string(),
            Err(why) => format!("NO:{why}"),
        };
        // ...and the control must be able to FIRE. Once, at the first checkpoint, feed it three
        // reloads that are wrong in the three ways that matter; each must be refused, or the
        // column above means nothing and the run stops here rather than print it.
        if !negatives_checked {
            negatives_checked = true;
            // (1) A load that skipped the map entirely: a store assembled at the same base with
            //     NO image loaded, which is exactly what `reopen` without `load_file` produces.
            let pool3 = {
                let f = std::fs::OpenOptions::new().read(true).write(true).open(&main_path).unwrap();
                Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())))
            };
            let unloaded = ArenaPageStore::reopen(pool3, Arc::clone(&re), base)
                .expect("assemble unloaded store")
                .state_bytes();
            // (2) A load that kept `current` — the premise of the control's first version.
            // (3) A load that got one extent byte wrong: the last byte before `current`.
            let cur = current_section_offset(&img).expect("walk the probe image");
            let mut flipped = reload.clone();
            flipped[cur - 1] ^= 1;
            let neg = [
                ("unloaded", reload_control(&img, &unloaded, n_current)),
                ("kept-current", reload_control(&img, &img, n_current)),
                ("flipped-extent", reload_control(&img, &flipped, n_current)),
            ];
            for (name, r) in &neg {
                assert!(r.is_err(), "reload control PASSED the {name} negative: it cannot fire");
            }
            println!(
                "  # reload-control negatives at N={done}: {}",
                neg.iter().map(|(n, r)| format!("{n}={r:?}")).collect::<Vec<_>>().join("  ")
            );
        }
        // NOT a load control (review finding 3): `load_state` stores `live_pages` straight from
        // the image HEADER, so this reads N whether or not a single extent was deserialised. It
        // proves the header round-trips, nothing more.
        let arena_pages = reopened.live_page_count().unwrap_or(u32::MAX);
        drop(reopened);
        drop(re);
        let _ = std::fs::remove_file(&probe);

        println!(
            "  {:>8}   {:>9.1}   {:>7.1}   {:>8.1}   {:>12.0}   {:>14.0}   {:>12.0}   {:>10}   {:>9.3}   {:>11}   {:>12.1}   {:>11.3}   {:>11}   {:>11}",
            done,
            actually as f64 / secs,
            data as f64 / 1e6,
            alloc as f64 / 1e6,
            data as f64 / done as f64,
            alloc as f64 / done as f64,
            cbytes as f64 / done as f64,
            hd.store.live_page_count().unwrap_or(0),
            reopen_ms,
            re_live,
            img.len() as f64 / done as f64,
            arena_ms,
            arena_pages,
            reload_ctl,
        );

        // ---- READ-VS-N, at this checkpoint (bench/read_vs_n/PREREG.md) -------------------------
        if let Some(arms) = arms {
            // G1: the branches read below are exactly the ones this run created, all live.
            let live = hd.cat_concrete.live_count().map(|n| n as u64).unwrap_or(u64::MAX);
            if branches.len() != done || live != done as u64 + 1 {
                failures.push(format!(
                    "G1 N={done}: {} branches recorded, {live} live in the catalog (want {done} and {})",
                    branches.len(),
                    done + 1
                ));
            }
            if arms.read {
                let rt = hd.runtime.clone().expect("the production runtime");
                // Flush the way production does before a timed read, so no window pays a dirty
                // write-back: WAL, then the whole pool, then fsync (`TxnManager::checkpoint`).
                db.as_ref()
                    .expect("the production database is open")
                    .txn
                    .checkpoint()
                    .expect("checkpoint before the read arm");
                let h = catalog_height(&hd.cat_concrete);
                // Only `control-cold` needs these, and collecting them is O(N) catalog reads.
                let cold_pages: Vec<PageId> = if fire == Fire::ControlCold {
                    branches.iter().map(|&b| rt.root_of(b).expect("root_of")).collect()
                } else {
                    Vec::new()
                };
                warm_up(&rt, &branches);
                for &threads in &READ_THREADS {
                    let arms_tot = read_phase(
                        &rt,
                        &hd.store,
                        &branches,
                        trunk_root,
                        &cold_pages,
                        threads,
                        read_k,
                        checkpoint_index,
                        fire,
                    );
                    let row = ReadRow { n: done, h, threads, arms: arms_tot };
                    print_read_row(&row);
                    read_guards(&row, fire, &mut failures);
                    read_rows.push(row);
                }
            }
            if arms.restart {
                // Counted BEFORE the close, independently of anything the child reports (H2, H3).
                let expect_arenas = hd.store.live_arenas().len() as u64;
                let (_, closed) = db.take().expect("the production database is open").close();
                closed.expect("close cleanly before the restart");
                // Every handle into the files goes before the child opens them.
                drop(hd);
                let mut row = RestartRow { n: done, expect_arenas, expect_live: live, ..Default::default() };
                match run_child(&db_path_str) {
                    Ok(child) => row.child = child,
                    Err(why) => failures.push(format!("H1 N={done}: {why}")),
                }
                // The parent's own reopen: the same open, in a warm process. A comparison only.
                let reopened = parent_open(&db_path_str, &mut failures);
                row.parent_total_us = reopened.timings.total.as_micros() as u64;
                row.parent_lease_us = reopened.timings.lease_start.as_micros() as u64;
                hd = Handles::of(&reopened);
                db = Some(reopened);
                if !row.child.is_empty() {
                    restart_guards(&row, fire, &mut failures);
                }
                print_restart_row(&row);
                restart_rows.push(row);
            }
            if arms.merge {
                // Axis (i): N grows, M fixed. The batch must start on an EMPTY `State`, which
                // lives in memory, so it runs right after an open: the restart arm's, or its own.
                if !arms.restart {
                    let open = db.take().expect("the production database is open");
                    drop(hd);
                    let reopened = reopen_for_merges(open, &db_path_str, &mut failures);
                    hd = Handles::of(&reopened);
                    db = Some(reopened);
                }
                let open = db.as_ref().expect("the production database is open");
                let live_before = hd.cat_concrete.live_count().map(|n| n as u64).unwrap_or(u64::MAX);
                let start = open.runtime.state_sizes().applied;
                if start != (fire == Fire::WrongStart) as usize {
                    failures.push(format!(
                        "M2 N={done}: the axis-(i) batch began with {start} applied entries, so M \
                         was not held fixed"
                    ));
                }
                let mut ones = Vec::with_capacity(merge_k);
                for _ in 0..merge_k {
                    let one = merge_cycle(open, next_merge_row, fire);
                    next_merge_row += 1;
                    merge_guards(&format!("axis i N={done}"), &one, fire, &mut merge_delta, &mut failures);
                    ones.push(one);
                }
                let live_after = hd.cat_concrete.live_count().map(|n| n as u64).unwrap_or(u64::MAX);
                let want = live_before + (fire == Fire::WrongLiveMerge) as u64;
                if live_after != want {
                    failures.push(format!(
                        "M5 N={done}: {live_after} live branches after the merge batch, {want} expected"
                    ));
                }
                let row = MergeRow { axis: "i", n: done, m: ones.len(), ones };
                print_merge_row(&row);
                merge_rows.push(row);
            }
            checkpoint_index += 1;
        }

        if data >= budget {
            stopped_early = Some((done, data));
            break;
        }
        // The disk, not just this run's share of it. See `free_bytes`.
        if let Some(free) = free_bytes(&main_path) {
            if free < FREE_FLOOR {
                println!(
                    "  STOPPING: {:.1} GiB free, floor is {:.0} GiB. Not this run's budget -- the \
                     DISK. Reported as a stop, not a result.",
                    free as f64 / (1u64 << 30) as f64,
                    FREE_FLOOR as f64 / (1u64 << 30) as f64,
                );
                stopped_early = Some((done, data));
                break;
            }
        }
    }

    println!();
    match stopped_early {
        Some((n, bytes)) => {
            let alloc_b = allocated_bytes(&main_path).unwrap_or(0);
            let alloc_per = alloc_b as f64 / n as f64;
            let per_branch = bytes as f64 / n as f64;
            println!("Allocated (blocks*512) rather than merely addressed: {:.0} B/branch, {:.2} GB total.",
                     alloc_per, alloc_b as f64 / 1e9);
            println!("  -> 10^6 branches x {:.0} allocated B = {:.2} TB actually on disk.",
                     alloc_per, alloc_per * 1e6 / 1e12);
            println!("STOPPED EARLY ON SPACE at N = {n}, data file {:.2} GB ({:.0} bytes/branch).",
                     bytes as f64 / 1e9, per_branch);
            println!("Extrapolated (ARITHMETIC, NOT MEASURED) to the objective:");
            println!("  10^6 branches x {:.0} B = {:.2} TB of data file.", per_branch,
                     per_branch * 1e6 / 1e12);
            println!("bench/curve_to_1e6.txt was reached fork-only, on the one workload that does not");
            println!("pay this. Fix is geometric extent growth (SCALE-DESIGN D31 option 1), NOT");
            println!("lowering ARENA_EXTENT_PAGES -- reclamation is per-extent on purpose.");
            if arms.is_none() {
                verdict(per_branch);
            }
        }
        None => {
            let data = std::fs::metadata(&main_path).map(|m| m.len()).unwrap_or(0);
            println!("DID NOT stop early within the budget: N = {done}, {:.0} bytes/branch.",
                     data as f64 / done.max(1) as f64);
            if arms.is_none() {
                verdict(data as f64 / done.max(1) as f64);
            }
        }
    }
    if arms.is_some_and(|a| a.merge) {
        // ---- ARM 2, axis (ii): N fixed at the last checkpoint, M grows (PREREG A3) -------------
        // One open, then merges up to each target. Nothing reopens in between, so `applied`,
        // `captures` and `merges` only grow; each target reports the last MERGE_BLOCK merges.
        let open = db.take().expect("the production database is open");
        drop(hd);
        let reopened = reopen_for_merges(open, &db_path_str, &mut failures);
        hd = Handles::of(&reopened);
        db = Some(reopened);
        let open = db.as_ref().expect("the production database is open");
        let live_before = hd.cat_concrete.live_count().map(|n| n as u64).unwrap_or(u64::MAX);
        let mut m_done = 0usize;
        for &target in &merge_targets {
            let mut block = Vec::new();
            while m_done < target {
                let one = merge_cycle(open, next_merge_row, fire);
                next_merge_row += 1;
                m_done += 1;
                merge_guards(&format!("axis ii M={m_done}"), &one, fire, &mut merge_delta, &mut failures);
                if m_done + MERGE_BLOCK > target {
                    block.push(one);
                }
            }
            let row = MergeRow { axis: "ii", n: done, m: m_done, ones: block };
            print_merge_row(&row);
            merge_rows.push(row);
        }
        let live_after = hd.cat_concrete.live_count().map(|n| n as u64).unwrap_or(u64::MAX);
        let want = live_before + (fire == Fire::WrongLiveMerge) as u64;
        if live_after != want {
            failures.push(format!(
                "M5 axis ii: {live_after} live branches after {m_done} merges, {want} expected"
            ));
        }
    }
    if let Some(arms) = arms {
        // D61's space verdict is about ITS layout. The production file also holds the SQL catalog
        // and a 32,736-page hole below the arena floor, so bytes/branch here is not its question.
        println!("(D61's space verdict is not printed: the production layout puts the arena above a");
        println!(" headroom region, so file bytes/branch is not the quantity that verdict judges.)");
        read_vs_n_summary(arms, &read_rows, &restart_rows, &merge_rows, fire, &mut failures, &mut ns_void);
    }
    let code = if failures.is_empty() && ns_void.is_empty() { 0 } else { 2 };
    if let Some(open) = db.take() {
        let (_, closed) = open.close();
        if let Err(e) = closed {
            println!("NOT A RESULT: the production database did not close cleanly at the end: {e}");
        }
    }
    drop(hd);
    // The run directory is removed by `_cleanup` — here, before a non-zero exit skips destructors,
    // and on the way out otherwise, panic or not.
    drop(_cleanup);
    if code != 0 {
        std::process::exit(code);
    }
}

/// The READ-VS-N summary: the per-N curves the pre-registration is judged against, the two guards
/// that can only be judged across N (G5, G7), and the verdict lines. Evaluating the predictions
/// themselves is the reader's job, against `bench/read_vs_n/PREREG.md` — the harness prints facts
/// and guard results, and does not grade its own run.
fn read_vs_n_summary(
    arms: Arms,
    read_rows: &[ReadRow],
    restart_rows: &[RestartRow],
    merge_rows: &[MergeRow],
    fire: Fire,
    failures: &mut Vec<String>,
    ns_void: &mut Vec<String>,
) {
    println!();
    println!("=== READ-VS-N SUMMARY ===================================================================");
    if arms.merge {
        // Arm 2. Axis (i) is read against N, axis (ii) against M; the slope columns are local
        // log-log slopes from the previous row of the same axis.
        for (axis, against) in [("i", "N"), ("ii", "M")] {
            let rows: Vec<&MergeRow> = merge_rows.iter().filter(|r| r.axis == axis).collect();
            println!("arm 2, axis ({axis}) — per-merge means, against {against}:");
            println!("         N        M   ns/merge   slope     V_hi   slope(V_hi)   V_cell  captures  attested  seq tup  c.fault");
            let mut prev: Option<(usize, f64, f64)> = None;
            for r in &rows {
                let x = if axis == "i" { r.n } else { r.m };
                let (ns, vhi) = (r.mean(|o| o.nanos as u64), r.mean(|o| o.v_hi));
                let (s_ns, s_v) = match prev {
                    // A zero V_hi (the first rows of a batch) has no logarithm; say so, not NaN.
                    Some((px, pns, pv)) if pv > 0.0 && vhi > 0.0 => (
                        format!("{:>7.3}", slope(pns, ns, px, x)),
                        format!("{:>13.3}", slope(pv, vhi, px, x)),
                    ),
                    Some((px, pns, _)) => (format!("{:>7.3}", slope(pns, ns, px, x)), format!("{:>13}", "-")),
                    None => (format!("{:>7}", "-"), format!("{:>13}", "-")),
                };
                let last = r.ones.last().copied().unwrap_or_default();
                println!(
                    "  {:>8} {:>8} {:>10.0} {s_ns} {:>8.1} {s_v} {:>8.2} {:>9} {:>9} {:>8.2} {:>8.3}",
                    r.n,
                    r.m,
                    ns,
                    vhi,
                    r.mean(|o| o.v_cell),
                    last.after.captures,
                    last.attested_after,
                    r.mean(|o| o.seq_tuples),
                    r.mean(|o| o.census.faults),
                );
                prev = Some((x, ns, vhi));
            }
            if rows.len() < 2 {
                failures.push(format!("arm 2 axis ({axis}): {} row(s); one point is not a curve", rows.len()));
            }
        }
    }
    if arms.read {
        for &t in &READ_THREADS {
            let rows: Vec<&ReadRow> = read_rows.iter().filter(|r| r.threads == t).collect();
            println!("arm 1, T={t}:");
            println!("         N   h   branch ns   control ns    ratio   slope(branch)   slope(ratio)   c.att/rd  c.fault/rd  d.fault/rd");
            let mut prev: Option<(usize, f64, f64)> = None;
            for r in &rows {
                let [branch, control, _] = &r.arms;
                let (b, c) = (branch.ns_per_read(), control.ns_per_read());
                let (sb, sr) = match prev {
                    Some((pn, pb, pr)) => (
                        format!("{:>15.3}", slope(pb, b, pn, r.n)),
                        format!("{:>14.3}", slope(pr, b / c, pn, r.n)),
                    ),
                    None => (format!("{:>15}", "-"), format!("{:>14}", "-")),
                };
                println!(
                    "  {:>8} {:>3} {:>11.1} {:>12.1} {:>8.3} {sb} {sr} {:>10.3} {:>11.3} {:>11.3}",
                    r.n,
                    r.h,
                    b,
                    c,
                    b / c,
                    branch.per(branch.cat.attempts),
                    branch.per(branch.cat.faults),
                    branch.per(branch.data.faults),
                );
                prev = Some((r.n, b, b / c));
                // G5: a control that missed the pool at this N is not a control at this N.
                if control.data.faults > 0 {
                    ns_void.push(format!(
                        "G5 N={} T={t}: the control faulted {} times; its page was evicted, so it \
                         depends on N here",
                        r.n, control.data.faults
                    ));
                }
            }
            // G7: the control's ns across N. It cannot depend on N, so if it moved, the box did.
            let cs: Vec<f64> = rows.iter().map(|r| r.arms[1].ns_per_read()).collect();
            if let (Some(lo), Some(hi)) = (
                cs.iter().cloned().reduce(f64::min),
                cs.iter().cloned().reduce(f64::max),
            ) {
                let moved = hi / lo;
                println!("  control ns max/min across N at T={t}: {moved:.3} (band 1.5)");
                if moved > 1.5 {
                    ns_void.push(format!(
                        "G7 T={t}: the control moved {moved:.2}x across N (band 1.5x); the box moved, \
                         so no ns column at T={t} can be read as a function of N"
                    ));
                }
            }
            if rows.len() < 2 {
                failures.push(format!("arm 1 T={t}: {} checkpoint(s) read; one point is not a curve", rows.len()));
            }
        }
    }
    if arms.restart {
        println!("arm 3 (fresh-process open; microsecond timers in the child, printed here in ms):");
        println!("         N   total ms   slope   lease_start ms   slope   arena ms   us/visit   1st-pass ms   parent total ms   parent lease ms");
        let mut prev: Option<(usize, f64, f64)> = None;
        for r in restart_rows.iter().filter(|r| !r.child.is_empty()) {
            let total = r.get("total_us") as f64 / 1000.0;
            let lease = r.get("lease_start_us") as f64 / 1000.0;
            let (st, sl) = match prev {
                Some((pn, pt, pl)) => (
                    format!("{:>7.3}", slope(pt, total, pn, r.n)),
                    format!("{:>7.3}", slope(pl, lease, pn, r.n)),
                ),
                None => (format!("{:>7}", "-"), format!("{:>7}", "-")),
            };
            println!(
                "  {:>8} {:>10.3} {st} {:>16.3} {sl} {:>10.3} {:>10.3} {:>13.3} {:>17.3} {:>17.3}",
                r.n,
                total,
                lease,
                r.get("arena_us") as f64 / 1000.0,
                r.get("lease_start_us") as f64 / r.get("open_visits").max(1) as f64,
                r.get("first_pass_us") as f64 / 1000.0,
                r.parent_total_us as f64 / 1000.0,
                r.parent_lease_us as f64 / 1000.0,
            );
            prev = Some((r.n, total, lease));
        }
        let measured = restart_rows.iter().filter(|r| !r.child.is_empty()).count();
        if measured < 2 {
            failures.push(format!("arm 3: {measured} restart(s) measured; one point is not a curve"));
        }
    }
    println!();
    if fire != Fire::None {
        println!("FIRECHECK {fire:?}: exactly the guard this mode breaks must appear below, and no other.");
    }
    if failures.is_empty() && ns_void.is_empty() {
        println!("GUARDS: every guard held. The counters and the ns columns stand as printed.");
    }
    for f in failures.iter() {
        println!("NOT A RESULT: {f}");
    }
    for v in ns_void.iter() {
        println!("NOT A RESULT (the ns columns only; the integer counters stand): {v}");
    }
}
