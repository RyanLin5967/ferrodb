//! D7 — how branch-create and read latency behave as branches accumulate.
//!
//! This is the claim the whole design rests on. DESIGN.md cites BranchBench (arXiv 2604.17180)
//! measuring the *overlay* pattern at up to **5400x read degradation** as branches accumulate, and
//! the substrate here is chosen specifically to avoid that: shadow paging with no content
//! addressing, no refcounts, and ancestry held only in branch metadata, so a fork touches no data
//! page and a read is an ordinary descent rather than a walk up a chain of parents.
//!
//! # What this measures, and what it does not
//!
//! **Measures:** ferrodb, on this machine, right now. `std::time::Instant`, wall clock, one
//! process, warm cache after a warmup pass.
//!
//! **Does NOT measure:** Dolt, or BranchBench, or anything else. The 5400x figure above is quoted
//! from that paper about other systems' overlay pattern; nothing here reproduces it, and no number
//! this program prints is a comparison against it. Saying "we are 5400x better" on the strength of
//! this file would be inventing a measurement that was never taken. The honest claim available
//! from this program is only about how ferrodb scales against *itself* as branch count rises.
//!
//! Run it with `cargo run --release --example branch_scaling_bench`. Debug numbers are meaningless
//! and the program says so rather than printing them as if they counted.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::Value;
use ferrodb::cow::PageStore;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;

const ARENA_BASE: u32 = 1024;
/// Rows on trunk. Enough to make the tree multi-level, so a read is a real descent.
const TRUNK_ROWS: u64 = 2_000;
/// Read samples per configuration.
const READ_SAMPLES: usize = 2_000;

struct Stats {
    mean: Duration,
    p50: Duration,
    p99: Duration,
    max: Duration,
}

fn stats(mut v: Vec<Duration>) -> Stats {
    v.sort_unstable();
    let n = v.len();
    let sum: Duration = v.iter().sum();
    Stats {
        mean: sum / n as u32,
        p50: v[n / 2],
        p99: v[(n * 99) / 100],
        max: v[n - 1],
    }
}

fn us(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

fn build(tag: &str) -> (tempfile::TempDir, Arc<ArenaPageStore>, AgentRuntime) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(Arc::clone(&dm)));
    let branches = Arc::new(LogBranchCatalog::in_memory(1));
    let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&branches), ARENA_BASE).unwrap());
    let rt = AgentRuntime::with_storage(
        branches,
        Arc::new(MemEffectLog::new()),
        Arc::clone(&store) as Arc<dyn PageStore>,
    )
    .unwrap();
    (dir, store, rt)
}

struct Row {
    branches: usize,
    diverged: bool,
    fork: Stats,
    /// Through `AgentRuntime::get_row`: catalog lookup for the branch's root, then descent.
    read: Stats,
    /// The descent alone, from a root resolved once.
    ///
    /// Split out because the first number came back as *exactly* the same p50 in every
    /// configuration, which is not what a real tree descent looks like as data changes underneath
    /// it. That is the signature of a fixed cost dominating the thing being measured — here the
    /// per-call catalog lookup — and a constant that swamps the signal would hide degradation
    /// just as effectively as there being none.
    descent: Stats,
    pages: u32,
}

/// `diverged`: whether each branch writes before the reads are timed.
///
/// This distinction is the whole measurement. With idle branches every fork still points at
/// trunk's root, so a read descends one shared tree and "flat" is close to tautological. The case
/// an overlay design actually degrades on is branches that have *diverged* — each with its own
/// pages — because answering then means deciding what this branch sees. Reporting only the idle
/// number would be measuring the easy half and quoting it as the claim.
fn measure(n: usize, diverged: bool) -> Row {
    let (_dir, store, rt) = build(&format!("scale{n}"));

    for r in 0..TRUNK_ROWS {
        rt.put_row(
            BranchId::TRUNK,
            "inventory",
            r,
            &[Value::Integer(r as i32), Value::Varchar(format!("widget-{r}"))],
        )
        .unwrap();
    }
    // Warm the cache so the first configuration is not penalised for being first.
    for r in 0..200 {
        rt.get_row(BranchId::TRUNK, "inventory", r).unwrap();
    }

    let mut fork_times = Vec::with_capacity(n);
    let mut last = BranchId::TRUNK;
    for i in 0..n {
        let t = Instant::now();
        let s = rt
            .begin_session("bench-agent", Some(&format!("r_{i}")), BranchId::TRUNK)
            .unwrap();
        fork_times.push(t.elapsed());
        last = s.branch;
        if diverged {
            // Each branch gets its own page, so the trees genuinely differ. Timed outside the
            // fork measurement: this is the cost of writing, not of forking.
            rt.put_row(
                last,
                "inventory",
                (i as u64) % TRUNK_ROWS,
                &[Value::Integer(-(i as i32)), Value::Varchar("mine".into())],
            )
            .unwrap();
        }
    }

    // Read from the newest branch, with every other branch alive. This is the number that
    // degrades in an overlay design: the branch has to answer without consulting its ancestors.
    let mut read_times = Vec::with_capacity(READ_SAMPLES);
    for i in 0..READ_SAMPLES {
        let key = (i as u64 * 7919) % TRUNK_ROWS; // spread across the tree
        let t = Instant::now();
        let got = rt.get_row(last, "inventory", key).unwrap();
        read_times.push(t.elapsed());
        // Never let the read be optimised away, and check it actually found something: a
        // benchmark that measures a miss is measuring nothing.
        assert!(got.is_some(), "read at {n} branches missed row {key}");
    }

    // Same reads, with the branch's root resolved once instead of per call.
    let root = rt.root_of(last).unwrap();
    let rows_store = rt.storage().expect("built with storage");
    let mut descent_times = Vec::with_capacity(READ_SAMPLES);
    for i in 0..READ_SAMPLES {
        let key = (i as u64 * 7919) % TRUNK_ROWS;
        let t = Instant::now();
        let got = rows_store.get(root, ferrodb::agent_sql::runtime::table_id("inventory").0, key).unwrap();
        descent_times.push(t.elapsed());
        assert!(got.is_some(), "descent at {n} branches missed row {key}");
    }

    let pages = store.live_page_count().unwrap();
    Row {
        branches: n,
        diverged,
        fork: stats(fork_times),
        read: stats(read_times),
        descent: stats(descent_times),
        pages,
    }
}

/// Prove the read instrument RESPONDS to the thing it is supposed to be sensitive to.
///
/// Every configuration below reported a p50 of exactly the same microsecond figure, for both the
/// full `get_row` path and the bare descent. That is not what a tree descent looks like when the
/// data underneath it changes, and it is the signature of a fixed per-call cost swamping the
/// signal. If the number cannot move at all, then "flat across branch counts" is unfalsifiable by
/// this instrument and proves nothing — so make it move on purpose, by growing the tree, before
/// reading anything into its flatness.
fn calibrate() -> (f64, f64) {
    let small = descent_p50_for(2_000);
    let large = descent_p50_for(40_000);
    (small, large)
}

fn descent_p50_for(rows: u64) -> f64 {
    let (_dir, _store, rt) = build(&format!("cal{rows}"));
    for r in 0..rows {
        rt.put_row(
            BranchId::TRUNK,
            "inventory",
            r,
            &[Value::Integer(r as i32), Value::Varchar(format!("widget-{r}"))],
        )
        .unwrap();
    }
    let root = rt.root_of(BranchId::TRUNK).unwrap();
    let store = rt.storage().unwrap();
    let tbl = ferrodb::agent_sql::runtime::table_id("inventory").0;
    for r in 0..200 {
        store.get(root, tbl, r).unwrap();
    }
    let mut v = Vec::with_capacity(READ_SAMPLES);
    for i in 0..READ_SAMPLES {
        let key = (i as u64 * 7919) % rows;
        let t = Instant::now();
        assert!(store.get(root, tbl, key).unwrap().is_some());
        v.push(t.elapsed());
    }
    us(stats(v).p50)
}

fn main() {
    if cfg!(debug_assertions) {
        eprintln!(
            "REFUSING to print numbers from a debug build - they would be meaningless and would \
             be quoted anyway.\nRun: cargo run --release --example branch_scaling_bench"
        );
        std::process::exit(2);
    }

    println!("ferrodb branch-scaling benchmark");
    println!("instrument: std::time::Instant, wall clock, single process, warm cache");
    println!("trunk: {TRUNK_ROWS} rows; reads: {READ_SAMPLES} samples per configuration\n");

    let (small, large) = calibrate();
    println!(
        "calibration: descent p50 over 2,000 rows = {small:.2}us, over 40,000 rows = {large:.2}us \
         (x{:.2})",
        large / small
    );
    if large <= small * 1.15 {
        eprintln!(
            "\nthe read instrument barely moved when the tree grew 20x ({small:.2} -> {large:.2}us).\n\
             It is dominated by a fixed per-call cost, so it cannot detect degradation and the \
             flat numbers below would mean nothing. REFUSING to report them as evidence."
        );
        std::process::exit(3);
    }
    println!("the instrument responds to tree size, so flatness below is a real result.\n");

    let mut rows: Vec<Row> = Vec::new();
    for diverged in [false, true] {
        for n in [10usize, 100, 1000] {
            rows.push(measure(n, diverged));
        }
    }

    println!(
        "{:>9} | {:>9} | {:>7} | {:>17} | {:>17} | {:>17}",
        "branches", "state", "pages", "fork (us)", "get_row (us)", "descent (us)"
    );
    println!(
        "{:>9} | {:>9} | {:>7} | {:>8} {:>8} | {:>8} {:>8} | {:>8} {:>8}",
        "", "", "", "p50", "p99", "p50", "p99", "p50", "p99"
    );
    println!("{}", "-".repeat(96));
    for r in &rows {
        println!(
            "{:>9} | {:>9} | {:>7} | {:>8.2} {:>8.2} | {:>8.2} {:>8.2} | {:>8.2} {:>8.2}",
            r.branches,
            if r.diverged { "diverged" } else { "idle" },
            r.pages,
            us(r.fork.p50),
            us(r.fork.p99),
            us(r.read.p50),
            us(r.read.p99),
            us(r.descent.p50),
            us(r.descent.p99),
        );
    }

    // The diverged series is the one that carries the claim.
    let base = rows.iter().find(|r| r.diverged && r.branches == 10).unwrap();
    let top = rows.iter().find(|r| r.diverged && r.branches == 1000).unwrap();
    let read_ratio = us(top.read.p50) / us(base.read.p50);
    let fork_ratio = us(top.fork.p50) / us(base.fork.p50);

    let descent_ratio = us(top.descent.p50) / us(base.descent.p50);
    println!("\ndiverged branches, {} -> {}:", base.branches, top.branches);
    println!("  get_row p50 x{read_ratio:.2}  (includes a per-call catalog lookup)");
    println!("  descent p50 x{descent_ratio:.2}  (tree only, root resolved once)");
    println!(
        "  fork  p50 x{fork_ratio:.2}  (below 1.0 is warm-up, not forks getting faster with \
         branch count - do not read it as a speedup)"
    );
    println!("  pages {} -> {}", base.pages, top.pages);

    println!(
        "\nThese are ferrodb's numbers against ITSELF at three branch counts, on this machine.\n\
         BranchBench's 5400x figure is that paper's measurement of other systems' overlay pattern;\n\
         nothing here reproduces it and none of the above is a comparison against it."
    );

    // The load-bearing claim, asserted rather than left for the reader to eyeball. Generous
    // bound: this is a wall-clock measurement on a shared machine, so it is a check against
    // *degradation*, not a performance target.
    let idle_top = rows.iter().find(|r| !r.diverged && r.branches == 1000).unwrap();
    if idle_top.pages != rows.iter().find(|r| !r.diverged && r.branches == 10).unwrap().pages {
        eprintln!("\nidle forks allocated pages; a fork is supposed to copy nothing");
        std::process::exit(1);
    }

    // Gate on the DESCENT, not on get_row: a fixed per-call cost would keep the get_row ratio
    // near 1.0 even if the tree itself were degrading badly, so gating on it would be a guard
    // that cannot fire.
    if descent_ratio > 3.0 {
        eprintln!(
            "\nDESCENT LATENCY DEGRADED x{descent_ratio:.2} from {} to {} diverged branches. The \
             design's whole premise is that it does not.",
            base.branches, top.branches
        );
        std::process::exit(1);
    }
    println!("\nread latency did not degrade materially with branch count.");
}
