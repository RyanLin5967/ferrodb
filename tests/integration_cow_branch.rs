//! Cross-module integration: the CoW B+tree (cow module) running on the arena-backed,
//! catalog-integrated page store (branch module).
//!
//! Each module's own tests exercise its substrate against a stand-in for the other side —
//! `CowTree` over `CowStore`. Nothing proved the two agents' pieces compose, which is the only
//! thing that matters after a merge.
//!
//! **D63 removed the other half of this file.** Two cases here drove `collapse` through
//! `CowPageLinks`, the real page-layout walker; both went with the feature.

use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::{CowTree, PageStore};
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
    let base = pool.disk_manager.high_water().unwrap();
    let store =
        Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog) as std::sync::Arc<dyn ferrodb::branch::BranchCatalog>, base).unwrap());
    Env { catalog, store, path }
}

/// Enough keys to force at least one internal level, so the fork below shares a real multi-level
/// tree rather than a single leaf.
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

