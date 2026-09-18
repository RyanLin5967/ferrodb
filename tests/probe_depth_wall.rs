//! PROBE: force the depth-escape detector to fire.
//!
//! `MAX_BRANCH_DEPTH = 8` is the only cap on ancestry; `collapse` is the only way past it.
//! `collapse` allocates ONE arena (reaper.rs:352) and threads that single ArenaId through every
//! `deep_copy` recursion (reaper.rs:188,192). `ArenaPageStore::alloc_in_arena` hard-errors when
//! the extent is exhausted (arena.rs:1014) and never rolls over. `ARENA_EXTENT_PAGES = 256`.
//!
//! Prediction under test: a branch whose reachable tree exceeds 256 pages cannot be collapsed,
//! and is therefore permanently pinned at depth <= 8.

use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::{CowPageLinks, CowTree, PageStore};
use ferrodb::storage::disk_manager::DiskManager;

struct Env {
    catalog: Arc<LogBranchCatalog>,
    store: Arc<ArenaPageStore>,
    path: std::path::PathBuf,
}
impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn env(tag: &str) -> Env {
    let path = std::env::temp_dir().join(format!("ferro-probe-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new()
        .create(true).read(true).write(true).open(&path).unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(
            Arc::clone(&pool),
            Arc::clone(&catalog) as Arc<dyn BranchCatalog>,
            base,
        ).unwrap(),
    );
    Env { catalog, store, path }
}

fn key(i: u32) -> Vec<u8> { format!("k{:08}", i).into_bytes() }
fn val(i: u32) -> Vec<u8> { vec![(i & 0xff) as u8; 200] }

/// Build a tree with `rows` keys and report how many pages it reaches, then try to collapse a
/// depth-limited branch that owns it.
fn collapse_at_size(tag: &str, rows: u32) -> (usize, Result<u8, String>) {
    let e = env(tag);
    let tree = CowTree::new(Arc::clone(&e.store) as Arc<dyn PageStore>);
    let reaper = TwoTierReaper::new(
        Arc::clone(&e.catalog) as Arc<dyn BranchCatalog>,
        Arc::clone(&e.store),
    ).with_links(Arc::new(CowPageLinks));

    let ep = e.catalog.next_epoch();
    let mut root = tree.create(BranchId::TRUNK, ep).unwrap();
    for i in 0..rows {
        root = tree.insert(root, BranchId::TRUNK, ep, &key(i), &val(i)).unwrap();
    }
    e.catalog.set_root(BranchId::TRUNK, root).unwrap();

    // Fork to the depth cap, exactly as an agent chain would.
    let mut cur = BranchId::TRUNK;
    for _ in 0..4 {
        cur = e.catalog.fork(cur, LeaseDeadline(u64::MAX)).unwrap().branch_id;
    }
    let pages = tree.walk_pages(e.catalog.get(cur).unwrap().root_page_id).unwrap().len();

    match reaper.collapse(cur) {
        Ok(rec) => (pages, Ok(rec.depth)),
        Err(err) => (pages, Err(err.to_string())),
    }
}

#[test]
fn collapse_survives_a_tree_that_fits_in_one_extent() {
    // NEGATIVE CONTROL. Without this, a failure below could be anything.
    let (pages, r) = collapse_at_size("small", 200);
    eprintln!("SMALL: reachable pages = {pages}, collapse = {r:?}");
    assert!(pages < 256, "control must fit one extent; got {pages} pages");
    assert_eq!(r.unwrap(), 1, "collapse must succeed below the extent size");
}

#[test]
fn collapse_fails_once_the_tree_outgrows_one_arena_extent() {
    let (pages, r) = collapse_at_size("big", 6000);
    eprintln!("BIG: reachable pages = {pages}, collapse = {r:?}");
    assert!(pages > 256, "probe must exceed one extent; got {pages} pages");
    let err = r.expect_err("PREDICTION: collapse cannot allocate past one 256-page extent");
    assert!(
        err.contains("exhausted"),
        "expected an arena-exhaustion refusal, got: {err}"
    );
}

#[test]
fn the_ninth_fork_is_refused_so_collapse_is_the_only_way_past_depth_eight() {
    let e = env("depth");
    let mut cur = BranchId::TRUNK;
    for _ in 0..8 {
        cur = e.catalog.fork(cur, LeaseDeadline(u64::MAX)).unwrap().branch_id;
    }
    assert_eq!(e.catalog.get(cur).unwrap().depth, 8);
    let err = e.catalog.fork(cur, LeaseDeadline(u64::MAX)).unwrap_err().to_string();
    eprintln!("NINTH FORK: {err}");
    assert!(err.contains("depth"), "got {err}");
}
