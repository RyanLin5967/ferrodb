//! S18: what does the ONLY escape from MAX_BRANCH_DEPTH cost?
//!
//! `TwoTierReaper::collapse` deep-copies every page reachable from the branch's root
//! (`src/branch/reaper.rs:155-203`) with a hard budget of `MAX_COLLAPSE_PAGES = 1 << 16`
//! (`src/branch/reaper.rs:63`). This measures the copy against dataset size and forces the cap.

use std::sync::Arc;
use std::time::Instant;

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
    fn drop(&mut self) { let _ = std::fs::remove_file(&self.path); }
}

fn env(tag: &str) -> Env {
    let path = std::env::temp_dir().join(format!("s18-collapse-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(pool, Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base).unwrap(),
    );
    Env { catalog, store, path }
}

fn k(i: u32) -> Vec<u8> { format!("k{:08}", i).into_bytes() }
fn v(i: u32) -> Vec<u8> { format!("v{:08}", i).into_bytes() }

/// Build `rows` rows on trunk, fork a chain to depth 7, collapse it, report pages and time.
fn one(rows: u32, tag: &str) -> (usize, f64) {
    let e = env(tag);
    let tree = CowTree::new(Arc::clone(&e.store) as Arc<dyn PageStore>);
    let reaper = TwoTierReaper::new(
        Arc::clone(&e.catalog) as Arc<dyn BranchCatalog>,
        Arc::clone(&e.store),
    ).with_links(Arc::new(CowPageLinks));

    let ep = e.catalog.next_epoch();
    let mut root = tree.create(BranchId::TRUNK, ep).unwrap();
    for i in 0..rows {
        root = tree.insert(root, BranchId::TRUNK, ep, &k(i), &v(i)).unwrap();
    }
    e.catalog.set_root(BranchId::TRUNK, root).unwrap();

    // The MCTS shape: a selection path seven deep. The eighth fork is the one that refuses.
    let mut cur = BranchId::TRUNK;
    for _ in 0..7 {
        cur = e.catalog.fork(cur, LeaseDeadline(u64::MAX)).unwrap().branch_id;
    }
    let deep = e.catalog.get(cur).unwrap();
    assert_eq!(deep.depth, 7);
    // The eighth fork is refused: this is the wall collapse exists to get past.
    assert!(e.catalog.fork(cur, LeaseDeadline(u64::MAX)).is_ok(), "depth 8 is still allowed");

    let pages_before = tree.walk_pages(deep.root_page_id).unwrap().len();
    let live_before = e.store.live_page_count().unwrap();

    let t = Instant::now();
    let outcome = reaper.collapse(cur);
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    let collapsed = match outcome {
        Ok(c) => c,
        Err(e) => {
            println!(
                "rows={rows:>7}  reachable_pages={pages_before:>6}  collapse REFUSED after \
                 {ms:.3} ms: {e}"
            );
            return (pages_before, -1.0);
        }
    };

    let live_after = e.store.live_page_count().unwrap();
    let copied = tree.walk_pages(collapsed.root_page_id).unwrap().len();
    assert_eq!(copied, pages_before, "collapse copied the whole reachable graph");
    assert_eq!(
        live_after - live_before, pages_before as u32,
        "collapse ADDS a full copy; it frees nothing"
    );
    println!(
        "rows={rows:>7}  reachable_pages={pages_before:>6}  collapse={ms:>9.3} ms  \
         live_pages {live_before} -> {live_after} (+{})",
        live_after - live_before
    );
    (pages_before, ms)
}

#[test]
fn collapse_cost_is_the_whole_dataset_and_it_adds_a_second_copy() {
    println!("MAX_COLLAPSE_PAGES = {} pages = {} MiB at 4 KiB",
             1usize << 16, (1usize << 16) * 4096 / (1024 * 1024));
    let mut rows = Vec::new();
    for n in [1_000u32, 4_000, 8_000, 12_000, 16_000, 17_000, 18_000, 20_000, 24_000, 64_000] {
        rows.push((n, one(n, &format!("n{n}"))));
    }
    println!("\nrows      pages   ms         result");
    for (n, (p, ms)) in &rows {
        println!("{n:<9} {p:<7} {:<10} {}",
                 if *ms < 0.0 { "-".to_string() } else { format!("{ms:.3}") },
                 if *ms < 0.0 { "REFUSED" } else { "ok" });
    }
    let ok_max = rows.iter().filter(|(_, (_, ms))| *ms >= 0.0).map(|(_, (p, _))| *p).max().unwrap();
    let fail_min = rows.iter().filter(|(_, (_, ms))| *ms < 0.0).map(|(_, (p, _))| *p).min().unwrap();
    println!("\nLARGEST reachable page graph collapse SURVIVED: {ok_max} pages ({} KiB)",
             ok_max * 4);
    println!("SMALLEST that it REFUSED:                      {fail_min} pages ({} KiB)",
             fail_min * 4);
    println!("ARENA_EXTENT_PAGES = {}", ferrodb::branch::types::ARENA_EXTENT_PAGES);
    assert!(ok_max <= 256, "collapse survived past one extent ({ok_max} pages)");
    assert!(fail_min > 256, "collapse refused inside one extent ({fail_min} pages)");
}
