//! PROBE: what does `collapse` -- the only way past MAX_BRANCH_DEPTH -- cost, and where does it
//! stop working?

use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline, ARENA_EXTENT_PAGES};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::{CowPageLinks, CowTree, PageStore};
use ferrodb::storage::disk_manager::DiskManager;

fn key(i: u32) -> Vec<u8> {
    format!("k{:08}", i).into_bytes()
}
fn val(i: u32) -> Vec<u8> {
    format!("v{:08}", i).into_bytes()
}

struct Outcome {
    data_pages: u32,
    fork_ms: f64,
    collapse_ms: f64,
    err: Option<String>,
    reserved_after: u32,
}

fn one(n: u32, chain: usize) -> Outcome {
    let path =
        std::env::temp_dir().join(format!("ferro-dc-{}-{}-{}.db", std::process::id(), n, chain));
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base)
            .unwrap(),
    );
    let tree = CowTree::new(Arc::clone(&store) as Arc<dyn PageStore>);
    let reaper =
        TwoTierReaper::new(Arc::clone(&catalog) as Arc<dyn BranchCatalog>, Arc::clone(&store))
            .with_links(Arc::new(CowPageLinks));

    let ep = catalog.next_epoch();
    let mut root = tree.create(BranchId::TRUNK, ep).unwrap();
    for i in 0..n {
        root = tree.insert(root, BranchId::TRUNK, ep, &key(i), &val(i)).unwrap();
    }
    catalog.set_root(BranchId::TRUNK, root).unwrap();
    let data_pages = store.live_page_count().unwrap();

    let mut cur = BranchId::TRUNK;
    for _ in 0..chain {
        cur = catalog.fork(cur, LeaseDeadline(u64::MAX)).unwrap().branch_id;
    }
    catalog.set_root(cur, root).unwrap();

    let t = Instant::now();
    let _f = catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
    let fork_ms = t.elapsed().as_secs_f64() * 1e3;

    let t = Instant::now();
    let res = reaper.collapse(cur);
    let collapse_ms = t.elapsed().as_secs_f64() * 1e3;
    let err = res.err().map(|e| e.to_string());
    let reserved_after = store.reserved_page_count();
    let _ = std::fs::remove_file(&path);
    Outcome { data_pages, fork_ms, collapse_ms, err, reserved_after }
}

#[test]
fn collapse_cost_and_ceiling() {
    println!("ARENA_EXTENT_PAGES = {}", ARENA_EXTENT_PAGES);
    println!("{:>8} {:>11} {:>9} {:>12} {:>10}  {}", "keys", "data_pages", "fork_ms", "collapse_ms", "ms/page", "result");
    for n in [500u32, 2_000, 8_000, 12_000, 16_000, 20_000, 24_000] {
        let o = one(n, 7);
        println!(
            "{:>8} {:>11} {:>9.4} {:>12.3} {:>10.3}  {}",
            n,
            o.data_pages,
            o.fork_ms,
            o.collapse_ms,
            o.collapse_ms / o.data_pages as f64,
            match &o.err {
                None => "ok".to_string(),
                Some(e) => format!("REFUSED: {e}  (reserved pages left behind: {})", o.reserved_after),
            }
        );
    }
    println!("--- same data (8000 keys), different chain length ---");
    for chain in [1usize, 4, 7] {
        let o = one(8_000, chain);
        println!("chain_len={} data_pages={} collapse_ms={:.3}", chain, o.data_pages, o.collapse_ms);
    }
}
