//! **D15's own stated next step, finally written.**
//!
//! D15 recorded that the reclamation reader takes no lock, and said: *"the next step is a test
//! that FAILS, not a patch: drive forks and reclamation concurrently and assert no page visible to
//! a live child is freed."* Every existing concurrency test reaps only AFTER every forking thread
//! has joined, so the reaper never runs against a moving catalog -- which is precisely the window
//! D15 is about.
//!
//! Shape: a VICTIM takes an already-expired lease and writes a known payload; a SURVIVOR then
//! forks FROM THAT VICTIM with a long lease, so the payload page is inside the survivor's
//! visibility window. A reaper thread sweeps throughout, and the victim is always reapable. At the
//! end every survivor must still read its parent's payload back. A freed page is recycled, so a
//! survivor whose page was reclaimed underneath it reads back something else -- that is the
//! corruption this asserts against, not a leak.
//!
//! The parent/child topology is load-bearing and was got WRONG first: with victim and survivor as
//! SIBLINGS off trunk, breaking the reclamation guard on purpose still left the test green at
//! 360/360, because arenas are per branch and a sibling's reap can never free this branch's pages.
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
                // The CLUSTER'S clock, not u64::MAX/2. That sentinel expires EVERY lease,
                // including the survivors' 600 s ones -- so every survivor was reaped and then
                // skipped by the `state != Live` guard below, and the assertion ran on an
                // almost-empty set while reporting "corrupted=0". Victims take LeaseDeadline(0)
                // and are expired against any real clock; survivors are not.
                let _ = r.reap_expired(LeaseDeadline::now_millis());
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
                    // VICTIM -> SURVIVOR, a PARENT and its CHILD. The first version of this test
                    // forked both from TRUNK as siblings, and a sibling's reap can never free a
                    // page this branch reads -- arenas are per branch. Fire-checked: breaking the
                    // reclamation guard on purpose left that version GREEN at 360/360, which is
                    // the definition of a detector that cannot fire.
                    //
                    // The page that matters is written by the VICTIM, BEFORE the survivor forks,
                    // so the survivor inherits it and the reclamation rule is the only thing
                    // standing between the reaper and live data.
                    // The victim starts with a LONG lease. Forking it as already-expired
                    // raced the sweeper and lost: the reaper took it before the survivor could
                    // fork off it, and every writer died on
                    // `branch bN@g0 has been reaped (id slot is now at generation 1)`.
                    // The branch must exist long enough to become a PARENT; only then is it
                    // expired, which is the state this test is about.
                    let victim = catalog
                        .fork(BranchId::TRUNK, LeaseDeadline::from_now(600_000))
                        .expect("victim fork");
                    let arena = store.arena_for(victim.branch_id).expect("arena");
                    let ep = catalog.next_epoch();
                    let p = store.alloc_in_arena(arena, PageType::BTreeLeaf, ep).expect("alloc");
                    let payload = (t * 31 + i) as u8 | 0x80;
                    {
                        let h = store.read_page(p).expect("read");
                        let mut f = h.write();
                        f.data[PAGE_HEADER_SIZE] = payload;
                        stamp_checksum(&mut f.data);
                    }
                    // The survivor forks AFTER the page exists, so the page is inside its
                    // visibility window and the rule must pin it for as long as this child lives.
                    let b = catalog
                        .fork(victim.branch_id, LeaseDeadline::from_now(600_000))
                        .expect("survivor fork");
                    // NOW expire the parent. From this instant the sweeper may reap it at any
                    // point, including mid-fork of the NEXT iteration -- which is the concurrent
                    // window D15 is about. The reclamation rule is the only thing that keeps
                    // page `p` alive, and this child is the live child it must see.
                    catalog
                        .renew_lease(victim.branch_id, LeaseDeadline(0))
                        .expect("expire the victim");
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
    let mut checked = 0usize;
    for (b, p, payload) in &survivors {
        // Only branches the catalog still calls Live are entitled to their pages.
        let Ok(rec) = catalog.get(*b) else { continue };
        if rec.state != ferrodb::branch::types::BranchState::Live {
            continue;
        }
        checked += 1;
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
    println!("[{tag}] survivors={} checked={checked} corrupted={}", survivors.len(), lost.len());
    // A run that collected nothing has not passed. Without this the test reports "corrupted=0"
    // when the reaper has removed every branch it was supposed to protect.
    assert!(
        checked >= survivors.len() * 9 / 10,
        "only {checked} of {} survivors were still Live to be checked -- the reaper is eating the \
         branches this test exists to protect, so `corrupted=0` says nothing",
        survivors.len()
    );
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
