//! **D15's own stated next step, finally written.**
//!
//! D15 recorded that the reclamation reader takes no lock, and said: *"the next step is a test
//! that FAILS, not a patch: drive forks and reclamation concurrently and assert no page visible to
//! a live child is freed."* Every existing concurrency test reaps only AFTER every forking thread
//! has joined, so the reaper never runs against a moving catalog -- which is precisely the window
//! D15 is about.
//!
//! Shape: SURVIVOR branches take a long lease and write a known payload; VICTIM branches take an
//! already-expired lease and exist only to give the reaper constant work. A reaper thread sweeps
//! throughout. At the end every survivor must still read its own payload back. A freed page is
//! recycled, so a survivor whose page was reclaimed underneath it reads back something else --
//! that is the corruption this asserts against, not a leak.
use std::sync::atomic::{AtomicBool, Ordering};
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

fn storm(table: bool, tag: &str) {
    let path = std::env::temp_dir().join(format!("d15-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog: Arc<dyn BranchCatalog> = if table {
        let cp = std::env::temp_dir().join(format!("d15-{}-{}.cat", std::process::id(), tag));
        let _ = std::fs::remove_file(&cp);
        Arc::new(TableBranchCatalog::open_sidecar(&cp, 1).unwrap())
    } else {
        Arc::new(LogBranchCatalog::in_memory(1))
    };
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog), base).unwrap());
    let reaper = TwoTierReaper::new(Arc::clone(&catalog), Arc::clone(&store));

    const WRITERS: usize = 6;
    const PER: usize = 60;
    let stop = Arc::new(AtomicBool::new(false));
    let mut survivors: Vec<(BranchId, u32, u8)> = Vec::new();

    std::thread::scope(|s| {
        // The reaper runs THROUGHOUT, against a catalog that is being mutated under it.
        let rstop = Arc::clone(&stop);
        let r = &reaper;
        s.spawn(move || {
            while !rstop.load(Ordering::Relaxed) {
                let _ = r.reap_expired(u64::MAX / 2);
                let _ = r.drain_pending();
            }
        });

        let mut hs = Vec::new();
        for t in 0..WRITERS {
            let catalog = Arc::clone(&catalog);
            let store = Arc::clone(&store);
            hs.push(s.spawn(move || {
                let mut mine = Vec::new();
                for i in 0..PER {
                    // A victim: already expired, so the concurrent reaper has real work to do.
                    if let Ok(v) = catalog.fork(BranchId::TRUNK, LeaseDeadline(0)) {
                        if let Ok(a) = store.arena_for(v.branch_id) {
                            let ep = catalog.next_epoch();
                            let _ = store.alloc_in_arena(a, PageType::BTreeLeaf, ep);
                        }
                    }
                    // A survivor: long lease, known payload, must still be readable at the end.
                    let b = catalog
                        .fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000))
                        .expect("survivor fork");
                    let arena = store.arena_for(b.branch_id).expect("arena");
                    let ep = catalog.next_epoch();
                    let p = store.alloc_in_arena(arena, PageType::BTreeLeaf, ep).expect("alloc");
                    let payload = (t * 31 + i) as u8 | 0x80;
                    {
                        let h = store.read_page(p).expect("read");
                        let mut f = h.write();
                        f.data[PAGE_HEADER_SIZE] = payload;
                        stamp_checksum(&mut f.data);
                    }
                    mine.push((b.branch_id, p, payload));
                }
                mine
            }));
        }
        for h in hs {
            survivors.extend(h.join().expect("no writer panicked"));
        }
        stop.store(true, Ordering::Relaxed);
    });

    let mut lost = Vec::new();
    for (b, p, payload) in &survivors {
        // Only branches the catalog still calls Live are entitled to their pages.
        let Ok(rec) = catalog.get(*b) else { continue };
        if rec.state != ferrodb::branch::types::BranchState::Live {
            continue;
        }
        match store.read_page(*p) {
            Ok(h) => {
                let got = h.read().data[PAGE_HEADER_SIZE];
                if got != *payload {
                    lost.push(format!("b{} page {p}: payload {:#x} -> {:#x}", b.id, payload, got));
                }
            }
            Err(e) => lost.push(format!("b{} page {p}: unreadable ({e})", b.id)),
        }
    }
    let _ = std::fs::remove_file(&path);
    println!("[{tag}] survivors={} corrupted={}", survivors.len(), lost.len());
    assert!(
        lost.is_empty(),
        "A LIVE BRANCH LOST DATA WHILE THE REAPER RAN CONCURRENTLY -- {} of {} survivors. \
         This is D15: the reclamation reader takes no lock, so a sweep landing inside a fork's \
         window can answer 'no live child' and free a page that branch can still read.\n{}",
        lost.len(), survivors.len(), lost.join("\n")
    );
}

#[test]
fn log_catalog_survives_a_concurrent_reaper() { storm(false, "log"); }

#[test]
fn table_catalog_survives_a_concurrent_reaper() { storm(true, "table"); }
