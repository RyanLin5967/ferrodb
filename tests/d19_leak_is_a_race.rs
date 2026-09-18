//! Is the 19-page leak on TableBranchCatalog a LOGIC bug or a CONCURRENCY race?
//!
//! Same workload, same catalog, only the thread count changes. A logic bug leaks at 1 thread too;
//! a race does not. This is the control that decides which, and it is the cheapest one available.
use std::sync::Arc;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::storage::disk_manager::DiskManager;

fn leak_for(threads: usize, per_thread: usize, table: bool, tag: &str) -> u32 {
    let path = std::env::temp_dir().join(format!("d19-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog: Arc<dyn BranchCatalog> = if table {
        let cp = std::env::temp_dir().join(format!("d19-{}-{}.cat", std::process::id(), tag));
        let _ = std::fs::remove_file(&cp);
        Arc::new(TableBranchCatalog::open_sidecar(&cp, 1).unwrap())
    } else {
        Arc::new(LogBranchCatalog::in_memory(1))
    };
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog), base).unwrap());
    let reaper = TwoTierReaper::new(Arc::clone(&catalog), Arc::clone(&store));
    let baseline = store.live_page_count().unwrap();

    std::thread::scope(|scope| {
        for _ in 0..threads {
            let catalog = Arc::clone(&catalog);
            let store = Arc::clone(&store);
            scope.spawn(move || {
                for _ in 0..per_thread {
                    let rec = catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(50)).unwrap();
                    let arena = store.arena_for(rec.branch_id).unwrap();
                    let ep = catalog.next_epoch();
                    let p = store.alloc_in_arena(arena, PageType::BTreeLeaf, ep).unwrap();
                    let h = store.read_page(p).unwrap();
                    let mut f = h.write();
                    f.data[PAGE_HEADER_SIZE] = 0xD1;
                    stamp_checksum(&mut f.data);
                }
            });
        }
    });
    let reaped = reaper.reap_expired(u64::MAX).unwrap();
    reaper.drain_pending().ok();
    let after = store.live_page_count().unwrap();
    let _ = std::fs::remove_file(&path);
    println!("[{tag}] threads={threads} total={} reaped={} baseline={baseline} after={after} LEAK={}",
             threads*per_thread, reaped.len(), after.saturating_sub(baseline));
    after.saturating_sub(baseline)
}

#[test]
fn the_control_that_says_race_or_logic() {
    let t1  = leak_for(1, 1000, true,  "table-1t");
    let t8  = leak_for(8,  125, true,  "table-8t");
    let l8  = leak_for(8,  125, false, "log-8t");
    println!("TABLE 1-thread leak={t1}  TABLE 8-thread leak={t8}  LOG 8-thread leak={l8}");
    assert_eq!(t1, 0, "single-threaded leak on the table catalog would mean a LOGIC bug, not a race");
}
