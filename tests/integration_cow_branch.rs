//! Cross-module integration: the CoW B+tree (cow module) running on the arena-backed,
//! catalog-integrated page store (branch module), and `collapse` materialising a real tree
//! through the real page-layout walker.
//!
//! Each module's own tests exercise its substrate against a stand-in for the other side —
//! `CowTree` over `CowStore`, `TwoTierReaper` over a toy `PageLinks`. Nothing proved the two
//! agents' pieces compose, which is the only thing that matters after a merge.

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
    let path =
        std::env::temp_dir().join(format!("ferro-integ-{}-{}.db", std::process::id(), tag));
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
    let base = pool.disk_manager.next_page_id.load(std::sync::atomic::Ordering::SeqCst);
    let store =
        Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), base).unwrap());
    Env { catalog, store, path }
}

/// Enough keys to force at least one internal level, so `collapse` has real child links to
/// rewrite rather than a single leaf.
const N: u32 = 400;

fn key(i: u32) -> Vec<u8> {
    format!("k{:06}", i).into_bytes()
}
fn val(i: u32) -> Vec<u8> {
    format!("v{:06}", i).into_bytes()
}

#[test]
fn the_cow_btree_runs_on_the_arena_store_and_a_child_sees_the_parents_data_without_a_copy() {
    let e = env("compose");
    let tree = CowTree::new(Arc::clone(&e.store) as Arc<dyn PageStore>);

    // Trunk writes a real tree.
    let ep = e.catalog.next_epoch();
    let mut root = tree.create(BranchId::TRUNK, ep).unwrap();
    for i in 0..N {
        root = tree.insert(root, BranchId::TRUNK, ep, &key(i), &val(i)).unwrap();
    }
    e.catalog.set_root(BranchId::TRUNK, root).unwrap();
    let pages_before = e.store.live_page_count().unwrap();
    assert!(pages_before > 1, "expected a multi-page tree, got {}", pages_before);

    // Fork copies zero data pages, and the child reads the parent's data by ordinary descent.
    let child = e.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
    assert_eq!(
        e.store.live_page_count().unwrap(),
        pages_before,
        "fork allocated pages"
    );
    assert_eq!(child.root_page_id, root, "child root is not the parent root");
    for i in 0..N {
        assert_eq!(tree.get(child.root_page_id, &key(i)).unwrap(), Some(val(i)));
    }
}

#[test]
fn collapse_materialises_a_real_cow_btree_through_the_real_page_walker() {
    let e = env("collapse");
    let tree = CowTree::new(Arc::clone(&e.store) as Arc<dyn PageStore>);
    let reaper = TwoTierReaper::new(Arc::clone(&e.catalog), Arc::clone(&e.store))
        .with_links(Arc::new(CowPageLinks));

    let ep = e.catalog.next_epoch();
    let mut root = tree.create(BranchId::TRUNK, ep).unwrap();
    for i in 0..N {
        root = tree.insert(root, BranchId::TRUNK, ep, &key(i), &val(i)).unwrap();
    }
    e.catalog.set_root(BranchId::TRUNK, root).unwrap();

    // A chain deep enough that the collapsed branch is genuinely re-parented.
    let mut cur = BranchId::TRUNK;
    for _ in 0..4 {
        cur = e.catalog.fork(cur, LeaseDeadline(u64::MAX)).unwrap().branch_id;
    }
    let deep = e.catalog.get(cur).unwrap();
    assert!(deep.depth > 1, "expected a deep branch, got depth {}", deep.depth);

    // The branch writes one of its own keys, so its visible state is not merely the trunk's.
    let ep2 = e.catalog.next_epoch();
    let new_root = tree.insert(deep.root_page_id, cur, ep2, &key(7), b"mine").unwrap();
    e.catalog.set_root(cur, new_root).unwrap();

    // The tree must genuinely have internal nodes, or `rewrite_child` is never called and this
    // test would pass without exercising the walker at all.
    let levels = tree.walk_pages(new_root).unwrap();
    assert!(levels.len() > 1, "tree is a single page; the walker would never be exercised");
    let internal = levels
        .iter()
        .filter(|p| {
            e.store.read_page(**p).unwrap().header().unwrap().page_type
                == ferrodb::cow::PageType::BTreeInternal
        })
        .count();
    assert!(internal > 0, "tree has no internal node; rewrite_child would never fire");

    let collapsed = reaper.collapse(cur).unwrap();

    assert_eq!(collapsed.depth, 1, "collapse did not reset depth");
    assert_eq!(collapsed.parent_id, Some(BranchId::TRUNK), "not re-parented to trunk");
    assert_ne!(collapsed.root_page_id, new_root, "collapse did not materialise a new root");

    // Every key survives the deep copy, including the branch's own write.
    for i in 0..N {
        let want = if i == 7 { b"mine".to_vec() } else { val(i) };
        assert_eq!(
            tree.get(collapsed.root_page_id, &key(i)).unwrap(),
            Some(want),
            "key {} lost or corrupted by collapse",
            i
        );
    }

    // The materialised tree is disjoint from the pages it was copied from: that is the whole
    // point of collapsing before re-parenting.
    let old: std::collections::HashSet<_> =
        tree.walk_pages(new_root).unwrap().into_iter().collect();
    let new: std::collections::HashSet<_> =
        tree.walk_pages(collapsed.root_page_id).unwrap().into_iter().collect();
    assert!(new.is_disjoint(&old), "collapsed tree still shares pages with its source");
    assert_eq!(new.len(), old.len(), "collapsed tree has a different shape");
}

/// Negative control for the test above: without the walker, collapse must refuse rather than
/// re-parent onto ancestor-owned pages. Proves the assertion above is testing the walker and not
/// passing for some unrelated reason.
#[test]
fn collapse_still_refuses_when_no_walker_is_supplied() {
    let e = env("norefuse");
    let reaper = TwoTierReaper::new(Arc::clone(&e.catalog), Arc::clone(&e.store));
    let b = e.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX)).unwrap();
    let err = reaper.collapse(b.branch_id).unwrap_err();
    assert!(err.to_string().contains("refusing to re-parent"), "got {}", err);
}
