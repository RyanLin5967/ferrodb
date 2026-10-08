//! PROBE (not a deliverable): does reclamation stay correct when an ancestor is reaped while a
//! transitive descendant still reads its pages?
//!
//! The reclamation rule consults ONE level of ancestry (the owner's own live-children epochs).
//! Visibility is transitive: C forked from B forked from A reads A's pages through A's root.
//! Reaping B removes B's epoch from A's live set, which makes A a "childless leaf" and sends
//! A's own reap down the FAST PATH -- free_arena wholesale, no sharing analysis at all.

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
    let path = std::env::temp_dir().join(format!("ferro-depthprobe-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(dm));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(
            Arc::clone(&pool),
            Arc::clone(&catalog) as std::sync::Arc<dyn ferrodb::branch::BranchCatalog>,
            base,
        )
        .unwrap(),
    );
    Env { catalog, store, path }
}

fn key(i: u32) -> Vec<u8> {
    format!("k{:06}", i).into_bytes()
}
fn val(i: u32) -> Vec<u8> {
    format!("v{:06}", i).into_bytes()
}

#[test]
fn reaping_a_middle_branch_orphans_its_descendant_onto_reclaimable_ancestor_pages() {
    let e = env("orphan");
    let tree = CowTree::new(Arc::clone(&e.store) as Arc<dyn PageStore>);
    let reaper = TwoTierReaper::new(
        Arc::clone(&e.catalog) as Arc<dyn BranchCatalog>,
        Arc::clone(&e.store),
    )
    .with_links(Arc::new(CowPageLinks));

    // trunk holds a small tree.
    let ep = e.catalog.next_epoch();
    let mut root = tree.create(BranchId::TRUNK, ep).unwrap();
    for i in 0..50 {
        root = tree.insert(root, BranchId::TRUNK, ep, &key(i), &val(i)).unwrap();
    }
    e.catalog.set_root(BranchId::TRUNK, root).unwrap();

    // A forks off trunk and writes 400 keys of its OWN, into A's OWN arena.
    let a = e.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
    let ep_a = e.catalog.next_epoch();
    let mut a_root = a.root_page_id;
    for i in 100..500 {
        a_root = tree.insert(a_root, a.branch_id, ep_a, &key(i), &val(i)).unwrap();
    }
    e.catalog.set_root(a.branch_id, a_root).unwrap();
    let after_a = e.store.live_page_count().unwrap();

    // B forks off A; C forks off B. Neither writes: both roots ARE A's root.
    let b = e.catalog.fork(a.branch_id, LeaseDeadline(u64::MAX)).unwrap();
    let c = e.catalog.fork(b.branch_id, LeaseDeadline(u64::MAX)).unwrap();
    assert_eq!(c.root_page_id, a_root, "fixture: C must be rooted on A's pages");

    // C can read A's data right now.
    for i in (100..500).step_by(37) {
        assert_eq!(tree.get(c.root_page_id, &key(i)).unwrap(), Some(val(i)), "fixture read");
    }

    // The middle agent finishes: MERGE/ABANDON -> seal() -> reaper.reap(branch), with no check
    // for live children. C is still live and still rooted on A's pages.
    reaper.reap(b.branch_id).unwrap();

    let a_after_b = e.catalog.get_raw(a.branch_id.id).unwrap();
    println!(
        "PROBE after reap(B): A.live_children = {:?} (empty => A is a 'childless leaf')",
        a_after_b.live_children
    );

    // Now the top agent finishes too.
    let freed = reaper.reap(a.branch_id).unwrap();
    let after_reaps = e.store.live_page_count().unwrap();
    println!(
        "PROBE after reap(A): freed={} live_pages {} -> {} (pending={})",
        freed,
        after_a,
        after_reaps,
        e.store.pending_len()
    );

    // Force the freed space to be handed out again, exactly as a new agent would.
    let d = e.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
    let ep_d = e.catalog.next_epoch();
    let mut d_root = d.root_page_id;
    for i in 5000..5400 {
        d_root = tree.insert(d_root, d.branch_id, ep_d, &key(i), &val(i)).unwrap();
    }
    e.catalog.set_root(d.branch_id, d_root).unwrap();

    // Can C still read the data it was reading a moment ago?
    let mut wrong = 0usize;
    let mut errs = 0usize;
    for i in 100..500 {
        match tree.get(c.root_page_id, &key(i)) {
            Ok(Some(v)) if v == val(i) => {}
            Ok(_) => wrong += 1,
            Err(_) => errs += 1,
        }
    }
    println!("PROBE C reads after the chain was reaped: wrong={} errs={} of 400", wrong, errs);
    assert_eq!(
        wrong + errs,
        0,
        "the orphaned descendant lost {} reads ({} wrong answers, {} hard errors) after its \
         ancestors were reaped -- the one-level reclamation rule freed pages it can still see",
        wrong + errs,
        wrong,
        errs
    );
}
