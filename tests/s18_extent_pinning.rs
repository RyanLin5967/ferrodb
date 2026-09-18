//! S18: what an MCTS tree pins that a flat fanout does not.
//!
//! An arena extent comes back to the free-space map exactly one way: `free_arena`, called from
//! the reaper's FAST path for a childless leaf (`src/branch/reaper.rs:233-238`). An MCTS interior
//! node is not a childless leaf for as long as any descendant is live.

use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline, ARENA_EXTENT_PAGES};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

struct Env { catalog: Arc<LogBranchCatalog>, store: Arc<ArenaPageStore>, path: std::path::PathBuf }
impl Drop for Env { fn drop(&mut self) { let _ = std::fs::remove_file(&self.path); } }

fn env(tag: &str) -> Env {
    let path = std::env::temp_dir().join(format!("s18-pin-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(pool, Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base).unwrap());
    Env { catalog, store, path }
}

/// Write ONE page — the smallest thing a branch can do that makes it a writer.
fn write_one(e: &Env, b: BranchId) {
    let arena = e.store.arena_for(b).unwrap();
    let ep = e.catalog.next_epoch();
    let p = e.store.alloc_in_arena(arena, PageType::BTreeLeaf, ep).unwrap();
    let h = e.store.read_page(p).unwrap();
    let mut f = h.write();
    f.data[PAGE_HEADER_SIZE] = 1;
    stamp_checksum(&mut f.data);
}

#[test]
fn one_written_page_reserves_a_whole_extent_and_a_chain_never_gives_it_back() {
    let e = env("chain");
    let reaper = TwoTierReaper::new(
        Arc::clone(&e.catalog) as Arc<dyn BranchCatalog>, Arc::clone(&e.store));

    let base = e.store.reserved_page_count();
    println!("ARENA_EXTENT_PAGES={ARENA_EXTENT_PAGES}  PAGE_SIZE={PAGE_SIZE}  \
              extent = {} KiB", ARENA_EXTENT_PAGES as usize * PAGE_SIZE / 1024);

    // ---- THE MCTS SHAPE: a selection path. Each node writes one page. ----
    let mut chain = Vec::new();
    let mut cur = BranchId::TRUNK;
    for _ in 0..7 {
        let rec = e.catalog.fork(cur, LeaseDeadline::from_now(60_000)).unwrap();
        cur = rec.branch_id;
        write_one(&e, cur);
        chain.push(cur);
    }
    let reserved_chain = e.store.reserved_page_count() - base;
    println!("7-node selection path, 1 page written each: reserved {reserved_chain} pages \
              = {} KiB for 7 pages of actual data",
             reserved_chain as usize * PAGE_SIZE / 1024);
    assert_eq!(reserved_chain, 7 * ARENA_EXTENT_PAGES,
               "one whole extent per writing branch, regardless of how little it wrote");

    // Prune the INTERIOR nodes — exactly what MCTS does to a losing subtree's spine while the
    // deepest node is still being expanded. Deepest-last, so every reap sees a live child.
    for b in chain.iter().take(6).copied() {
        let _ = reaper.reap(b);
    }
    let reserved_after_prune = e.store.reserved_page_count() - base;
    println!("after reaping the 6 interior nodes: reserved {reserved_after_prune} pages \
              ({} extents still held)", reserved_after_prune / ARENA_EXTENT_PAGES);

    // ---- THE FLAT CONTROL: 7 childless leaves off trunk, same writes, same reaps. ----
    let e2 = env("flat");
    let reaper2 = TwoTierReaper::new(
        Arc::clone(&e2.catalog) as Arc<dyn BranchCatalog>, Arc::clone(&e2.store));
    let base2 = e2.store.reserved_page_count();
    let mut flat = Vec::new();
    for _ in 0..7 {
        let rec = e2.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
        write_one(&e2, rec.branch_id);
        flat.push(rec.branch_id);
    }
    assert_eq!(e2.store.reserved_page_count() - base2, 7 * ARENA_EXTENT_PAGES);
    for b in flat { reaper2.reap(b).unwrap(); }
    let reserved_flat = e2.store.reserved_page_count() - base2;
    println!("flat fanout, same 7 branches reaped:        reserved {reserved_flat} pages \
              ({} extents still held)", reserved_flat / ARENA_EXTENT_PAGES);

    assert_eq!(reserved_flat, 0, "a flat fanout returns every extent wholesale");
    assert!(reserved_after_prune > 0,
            "the chain must still hold extents the flat fanout gave back");
    println!("\nSAME writes, SAME number of reaps, SAME data: chain holds {} extents, \
              flat holds {}", reserved_after_prune / ARENA_EXTENT_PAGES,
             reserved_flat / ARENA_EXTENT_PAGES);
}
