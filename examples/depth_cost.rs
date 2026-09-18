//! D13b: **what actually degrades as branch depth rises?**
//!
//! `MAX_BRANCH_DEPTH = 8` is a hard cap with no stated derivation. This measures the three
//! candidate costs at a range of depths so the number can be defended or changed on evidence:
//!
//! * **(a) fork latency** — the cost of the fork that lands AT depth d.
//! * **(b) read latency** — a point read on the branch at depth d.
//! * **(c) pinned pages** — pages that cannot be reclaimed while a leaf at depth d is live. The
//!   reclamation rule protects any page a live child can still read, so a live leaf holds its
//!   ancestors' pages open. This is the hypothesis the cap is a memory bound.
//! * **(d) chain-reap latency** — reaping the whole chain, which is where the cascade in
//!   `detach_from_parent` walks up the parent chain once per level, calling `has_live_children`
//!   at each step.
//!
//! Depths above `MAX_BRANCH_DEPTH` cannot be built through `fork`, which refuses them. Rather
//! than route around the guard, this binary **skips** any depth above the compiled-in cap and
//! says so: to measure 16/32/64 you raise the constant and rebuild, and the overlapping depths
//! then cross-check that raising it changed nothing about the shallow numbers.
//!
//! Run: `cargo run --release --example depth_cost`

use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline, MAX_BRANCH_DEPTH};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::{CowPageLinks, CowTree, PageStore};
use ferrodb::storage::disk_manager::DiskManager;

/// Keys in the trunk tree every branch inherits. Big enough to be a multi-level tree, so a point
/// read is a real descent rather than one leaf.
const TRUNK_KEYS: u32 = 4_000;
/// Keys each branch in the chain writes of its own, so every level owns real pages to pin.
///
/// Overridable, because it is the CONTROL on the read-latency column. Each level's own keys land
/// in the same tree, so a deep chain holds more keys than a shallow one and a slower read at
/// depth 64 could be the taller B+tree rather than the ancestry. Re-running with a small value
/// holds total data nearly constant across depths and separates the two.
fn own_keys() -> u32 {
    std::env::var("OWN_KEYS").ok().and_then(|v| v.parse().ok()).unwrap_or(200)
}
/// Point reads per depth, for the read-latency median.
const READ_SAMPLES: usize = 2_000;
/// Independent repeats of the whole chain build, for the fork-latency median.
const FORK_REPEATS: usize = 9;

fn key(i: u32) -> Vec<u8> {
    format!("k{:07}", i).into_bytes()
}
fn val(i: u32) -> Vec<u8> {
    format!("v{:07}", i).into_bytes()
}

struct Env {
    catalog: Arc<LogBranchCatalog>,
    store: Arc<ArenaPageStore>,
    tree: CowTree,
    reaper: TwoTierReaper,
    path: std::path::PathBuf,
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn env(tag: &str) -> Env {
    let path = std::env::temp_dir().join(format!("ferro-depth-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(pool, Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base).unwrap(),
    );
    let tree = CowTree::new(Arc::clone(&store) as Arc<dyn PageStore>);
    let reaper =
        TwoTierReaper::new(Arc::clone(&catalog) as Arc<dyn BranchCatalog>, Arc::clone(&store))
            .with_links(Arc::new(CowPageLinks));
    Env { catalog, store, tree, reaper, path }
}

/// Seed trunk with a real multi-level tree that every branch below inherits for free.
fn seed_trunk(e: &Env) {
    let ep = e.catalog.next_epoch();
    let mut root = e.tree.create(BranchId::TRUNK, ep).unwrap();
    for i in 0..TRUNK_KEYS {
        root = e.tree.insert(root, BranchId::TRUNK, ep, &key(i), &val(i)).unwrap();
    }
    e.catalog.set_root(BranchId::TRUNK, root).unwrap();
}

/// Build a chain of `d` branches, each writing `OWN_KEYS` of its own. Returns the chain and the
/// latency of the LAST fork — the one that lands at depth d.
fn build_chain(e: &Env, d: u8) -> (Vec<BranchId>, f64) {
    let mut chain = Vec::new();
    let mut parent = BranchId::TRUNK;
    let mut last_fork_us = 0.0;
    for level in 0..d {
        let t = Instant::now();
        let rec = e.catalog.fork(parent, LeaseDeadline(u64::MAX)).unwrap();
        last_fork_us = t.elapsed().as_secs_f64() * 1e6;
        let b = rec.branch_id;
        // Its own writes, in its own key range so levels do not overwrite each other.
        let ep = e.catalog.next_epoch();
        let mut root = rec.root_page_id;
        for i in 0..own_keys() {
            let k = TRUNK_KEYS + level as u32 * own_keys() + i;
            root = e.tree.insert(root, b, ep, &key(k), &val(k)).unwrap();
        }
        e.catalog.set_root(b, root).unwrap();
        chain.push(b);
        parent = b;
    }
    (chain, last_fork_us)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

struct Row {
    depth: u8,
    fork_us: f64,
    read_us: f64,
    live_after_build: u32,
    pinned_by_live_leaf: u32,
    reserved_pinned: u32,
    chain_reap_us: f64,
}

fn measure(d: u8) -> Row {
    // ---- (a) fork latency: median over independent chain builds --------------------------------
    let mut forks = Vec::with_capacity(FORK_REPEATS);
    for r in 0..FORK_REPEATS {
        let e = env(&format!("fork{d}-{r}"));
        seed_trunk(&e);
        let (_, us) = build_chain(&e, d);
        forks.push(us);
    }

    // ---- (b),(c),(d) on one chain --------------------------------------------------------------
    let e = env(&format!("main{d}"));
    seed_trunk(&e);
    let (chain, _) = build_chain(&e, d);
    let leaf = *chain.last().unwrap();
    let leaf_root = e.catalog.get(leaf).unwrap().root_page_id;

    // (b) point reads on the leaf. Keys are drawn from the trunk range, so the read descends the
    // inherited tree — the case DESIGN.md's invariant 2 is about.
    let mut reads = Vec::with_capacity(READ_SAMPLES);
    let mut seed = 0x2545F4914F6CDD1Du64;
    for _ in 0..READ_SAMPLES {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let k = key((seed % TRUNK_KEYS as u64) as u32);
        let t = Instant::now();
        let got = e.tree.get(leaf_root, &k).unwrap();
        reads.push(t.elapsed().as_secs_f64() * 1e6);
        assert!(got.is_some(), "seeded key missing at depth {d}");
    }

    let live_after_build = e.store.live_page_count().unwrap();
    let _reserved_after_build = e.store.reserved_page_count();

    // (c) Reap every ANCESTOR while the leaf stays live. Whatever does not come back is held open
    // by the leaf: the interval rule protects any page a live child can still read.
    for b in chain.iter().rev().skip(1) {
        e.reaper.reap(*b).unwrap();
    }
    e.reaper.drain_pending().unwrap();
    let live_with_leaf = e.store.live_page_count().unwrap();
    let reserved_with_leaf = e.store.reserved_page_count();

    // (d) Now the leaf goes too. The difference is what the live leaf was pinning, and the time
    // is the cascade: `detach_from_parent` walks up the parent chain, `has_live_children` per step.
    let t = Instant::now();
    e.reaper.reap(leaf).unwrap();
    e.reaper.drain_pending().unwrap();
    let chain_reap_us = t.elapsed().as_secs_f64() * 1e6;
    let live_all_gone = e.store.live_page_count().unwrap();
    let reserved_all_gone = e.store.reserved_page_count();

    Row {
        depth: d,
        fork_us: median(forks),
        read_us: median(reads),
        live_after_build,
        pinned_by_live_leaf: live_with_leaf.saturating_sub(live_all_gone),
        reserved_pinned: reserved_with_leaf.saturating_sub(reserved_all_gone),
        chain_reap_us,
    }
}

fn main() {
    let depths: Vec<u8> = std::env::args()
        .skip(1)
        .map(|a| a.parse().expect("depth must be a small integer"))
        .collect();
    let depths = if depths.is_empty() { vec![2, 4, 8, 16, 32, 64] } else { depths };

    println!("MAX_BRANCH_DEPTH = {MAX_BRANCH_DEPTH}  (depths above it are skipped, not forced)");
    println!(
        "trunk_keys={} own_keys_per_level={} read_samples={} fork_repeats={}",
        TRUNK_KEYS, own_keys(), READ_SAMPLES, FORK_REPEATS
    );
    println!();
    println!(
        "{:>5}  {:>10}  {:>10}  {:>12}  {:>12}  {:>12}  {:>12}",
        "depth", "fork us", "read us", "live pages", "pinned pgs", "pinned rsvd", "reap us"
    );

    for d in depths {
        if d > MAX_BRANCH_DEPTH {
            println!("{:>5}  skipped: above the compiled MAX_BRANCH_DEPTH of {}", d, MAX_BRANCH_DEPTH);
            continue;
        }
        let r = measure(d);
        println!(
            "{:>5}  {:>10.2}  {:>10.3}  {:>12}  {:>12}  {:>12}  {:>12.1}",
            r.depth,
            r.fork_us,
            r.read_us,
            r.live_after_build,
            r.pinned_by_live_leaf,
            r.reserved_pinned,
            r.chain_reap_us
        );
    }
}
