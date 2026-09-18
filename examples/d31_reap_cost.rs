//! D31: what geometric extent growth costs the REAPER.
//!
//! The fix for D31 trades one big extent per branch for several small ones, and the reaper's fast
//! path frees exactly `record.arenas` — one `free_arena` per extent. So the question the change
//! has to answer is whether it moved cost from space to time, which is the objection that rules
//! out lowering `ARENA_EXTENT_PAGES` and would rule this out too if it were true.
//!
//! For each per-branch page count it reports extents per branch, reserved pages per branch, and
//! the wall time of a `reap_expired` sweep that frees the lot.
//!
//! **Written to compile against the tree BEFORE the change as well as after**, so the two columns
//! come from one instrument: it uses `arena_for` + `alloc_in_arena` per page rather than
//! `PageStore::alloc_for`, which only exists after. Run it on both and diff.
//!
//!   d31_reap_cost [branches] [pages,per,branch,list]
use std::sync::Arc;
use std::time::Instant;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::storage::disk_manager::DiskManager;

fn main() {
    let branches: usize =
        std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(50);
    let sizes: Vec<u32> = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "1,7,64,500,2000".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    println!("D31 reap cost. {branches} branches per row, TableBranchCatalog on a real file.");
    println!();
    println!("  pages/branch   extents/branch   reserved pages/branch   reaped   reap ms");

    for &per_branch in &sizes {
        let dir = std::env::temp_dir()
            .join(format!("ferrodb-d31reap-{}-{}", std::process::id(), per_branch));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let f = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(dir.join("main.db"))
            .unwrap();
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(f).unwrap())));
        let catalog: Arc<dyn BranchCatalog> = Arc::new(
            TableBranchCatalog::open_sidecar(&dir.join("branches.branchcat"), 1).unwrap(),
        );
        let base = pool.disk_manager.high_water().unwrap();
        let store =
            Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), base).unwrap());
        let reaper = TwoTierReaper::new(Arc::clone(&catalog), Arc::clone(&store));

        let mut ids = Vec::with_capacity(branches);
        for _ in 0..branches {
            let rec = catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(1)).unwrap();
            let epoch = catalog.next_epoch();
            for i in 0..per_branch {
                // Per page, deliberately: this is what every allocating path in the engine does,
                // and it is the spelling that exists in both trees.
                let arena = store.arena_for(rec.branch_id).unwrap();
                let p = store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap();
                let h = store.read_page(p).unwrap();
                let mut frame = h.write();
                frame.data[PAGE_HEADER_SIZE] = (i & 0xff) as u8;
                stamp_checksum(&mut frame.data);
            }
            ids.push(rec.branch_id);
        }

        let extents: usize =
            ids.iter().map(|b| catalog.get(*b).map(|r| r.arenas.len()).unwrap_or(0)).sum();
        let reserved = store.reserved_page_count();

        let t0 = Instant::now();
        let reaped = reaper.reap_expired(u64::MAX).unwrap();
        let ms = t0.elapsed().as_secs_f64() * 1000.0;

        println!(
            "  {:>12}   {:>14.2}   {:>21.1}   {:>6}   {:>7.1}",
            per_branch,
            extents as f64 / branches as f64,
            reserved as f64 / branches as f64,
            reaped.len(),
            ms
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
