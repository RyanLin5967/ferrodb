//! S18 probe 4: what does exceeding MAX_BRANCH_DEPTH cost, and where does the escape refuse?
//!
//! `fork` refuses past MAX_BRANCH_DEPTH (= 8). The only escape is `Reaper::collapse`, which
//! `deep_copy`s the branch's ENTIRE reachable page graph into a fresh arena, with a budget of
//! MAX_COLLAPSE_PAGES (= 1<<16 = 65536 pages = 256 MiB).
//!
//! Arm 1 — build a chain to depth 8, show the 9th fork refuses, collapse, and measure what the
//!         collapse copied and how long it took.
//! Arm 2 — pass a trunk bigger than the collapse budget and show the refusal is real, not inferred.
//!
//! Usage: cargo run --release --example storage_collapse_cost -- <trunk_rows> <value_bytes>

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, PageLinks, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::{CowPageLinks, CowTree, PageStore};
use ferrodb::storage::disk_manager::DiskManager;

fn key(i: u32) -> Vec<u8> {
    format!("k{:08}", i).into_bytes()
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let trunk_rows: u32 = a.get(1).map(|s| s.parse().unwrap()).unwrap_or(200_000);
    let value_bytes: usize = a.get(2).map(|s| s.parse().unwrap()).unwrap_or(8);

    let path = std::env::temp_dir().join(format!("s18-col-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let file = OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base)
            .unwrap(),
    );
    let tree = CowTree::new(store.clone() as Arc<dyn PageStore>);
    let reaper = TwoTierReaper::new(
        Arc::clone(&catalog) as Arc<dyn BranchCatalog>,
        Arc::clone(&store),
    )
    .with_links(Arc::new(CowPageLinks) as Arc<dyn PageLinks>);

    let v = vec![b'x'; value_bytes];
    let e0 = catalog.next_epoch();
    let mut root = tree.create(BranchId::TRUNK, e0).unwrap();
    let tbuild = std::time::Instant::now();
    for i in 0..trunk_rows {
        let e = catalog.next_epoch();
        root = tree.insert(root, BranchId::TRUNK, e, &key(i), &v).unwrap();
    }
    catalog.set_root(BranchId::TRUNK, root).unwrap();
    let trunk_pages = tree.walk_pages(root).unwrap().len();
    println!(
        "trunk: rows={} value_bytes={} pages={} ({:.1} MB) built in {:.1?}",
        trunk_rows,
        value_bytes,
        trunk_pages,
        trunk_pages as f64 * 4096.0 / 1e6,
        tbuild.elapsed()
    );
    println!("MAX_BRANCH_DEPTH=8  MAX_COLLAPSE_PAGES=65536 (256.0 MB)\n");

    // ---- chain to depth 8 ------------------------------------------------------------------
    let mut cur = BranchId::TRUNK;
    let mut depth_reached = 0u8;
    for d in 1..=9u8 {
        match catalog.fork(cur, LeaseDeadline::from_now(3_600_000)) {
            Ok(rec) => {
                cur = rec.branch_id;
                depth_reached = rec.depth;
                // one real write, so the branch is not vacuous
                let e = catalog.next_epoch();
                let r = tree.insert(rec.root_page_id, rec.branch_id, e, &key(d as u32), &v).unwrap();
                catalog.set_root(rec.branch_id, r).unwrap();
            }
            Err(e) => {
                println!("fork #{d} REFUSED at depth {depth_reached}: {e}");
                break;
            }
        }
    }
    println!("deepest branch: {cur} at depth {depth_reached}");

    // ---- collapse it -----------------------------------------------------------------------
    let live_before = store.live_page_count().unwrap();
    let reserved_before = store.reserved_page_count();
    let t0 = std::time::Instant::now();
    let res = reaper.collapse(cur);
    let el = t0.elapsed();
    match res {
        Ok(rec) => {
            let live_after = store.live_page_count().unwrap();
            let reserved_after = store.reserved_page_count();
            println!(
                "\nCOLLAPSE ok in {:.3?}: pages_copied={} reserved_delta={} new_depth={} \
                 new_root={}",
                el,
                live_after - live_before,
                reserved_after - reserved_before,
                rec.depth,
                rec.root_page_id
            );
            println!(
                "  the branch's OWN novel pages before collapse were ~1-3; it copied {} \
                 ({:.1} MB), i.e. the whole reachable dataset",
                live_after - live_before,
                (live_after - live_before) as f64 * 4096.0 / 1e6
            );
            println!(
                "  per-collapse cost = {:.3} ms; a chain of depth D needs floor(D/8) collapses, \
                 so depth {} costs {:.1} s of copying",
                el.as_secs_f64() * 1e3,
                1000,
                el.as_secs_f64() * (1000.0 / 8.0)
            );
            // and fork now works again from the collapsed branch
            match catalog.fork(rec.branch_id, LeaseDeadline::from_now(3_600_000)) {
                Ok(c) => println!("  fork after collapse: ok, child depth {}", c.depth),
                Err(e) => println!("  fork after collapse: REFUSED {e}"),
            }
        }
        Err(e) => {
            println!("\nCOLLAPSE REFUSED after {:.3?}: {e}", el);
            println!(
                "  the branch is now permanently stuck at depth {depth_reached}: fork refuses \
                 above MAX_BRANCH_DEPTH and collapse is the only escape"
            );
        }
    }

    let _ = std::fs::remove_file(&path);
}
