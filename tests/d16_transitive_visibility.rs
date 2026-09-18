//! S18: the reclamation rule is an ancestry-interval query over **direct children only**
//! (`src/branch/record.rs:276-283`, `src/branch/mod.rs:123-134`). MCTS prunes INTERIOR nodes,
//! and reaping an interior node severs the one link the rule consults.
//!
//! Run against BOTH catalogs. `LogBranchCatalog` is the important one: it keeps the live set
//! inside the record, so the shipped table catalog's empty `live_children` cannot be blamed for
//! the failure -- this is the DESIGN, not an implementation slip. `TableBranchCatalog` is the one
//! that actually ships, and it fails through a DIFFERENT line (`live_child_at` resolving a reaped
//! interior node to "not live"), so one test over one catalog would have left half the fix
//! unverified.

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

struct Env {
    catalog: Arc<dyn BranchCatalog>,
    store: Arc<ArenaPageStore>,
    path: std::path::PathBuf,
    cat_path: Option<std::path::PathBuf>,
}
impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        if let Some(c) = &self.cat_path {
            let _ = std::fs::remove_file(c);
        }
    }
}

fn env_with(tag: &str, table: bool) -> Env {
    let path = std::env::temp_dir().join(format!("s18-trans-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let (catalog, cat_path): (Arc<dyn BranchCatalog>, Option<std::path::PathBuf>) = if table {
        let cp = std::env::temp_dir()
            .join(format!("s18-trans-{}-{}.cat", std::process::id(), tag));
        let _ = std::fs::remove_file(&cp);
        (Arc::new(TableBranchCatalog::open_sidecar(&cp, 1).unwrap()), Some(cp))
    } else {
        (Arc::new(LogBranchCatalog::in_memory(1)), None)
    };
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(pool, Arc::clone(&catalog), base).unwrap());
    Env { catalog, store, path, cat_path }
}

fn write_one(e: &Env, b: BranchId) -> u32 {
    let arena = e.store.arena_for(b).unwrap();
    let ep = e.catalog.next_epoch();
    let p = e.store.alloc_in_arena(arena, PageType::BTreeLeaf, ep).unwrap();
    let h = e.store.read_page(p).unwrap();
    let mut f = h.write();
    f.data[PAGE_HEADER_SIZE] = 0xAB;
    stamp_checksum(&mut f.data);
    p
}

/// GRANDPARENT writes P. PARENT forks (sees P). CHILD forks off PARENT (sees P).
/// MCTS prunes PARENT — an interior node — while CHILD is still being expanded.
/// After that reap, does anything still record that P is visible to a live branch?
#[test]
fn log_catalog_keeps_the_grandchild_when_an_interior_node_is_pruned() {
    interior_prune_must_not_lose_the_grandchild("log", false);
}

/// The catalog that actually ships. It fails through a different line than the log catalog
/// (`table_catalog.rs::live_child_at`), so it needs its own arm or half the fix is unverified.
#[test]
fn table_catalog_keeps_the_grandchild_when_an_interior_node_is_pruned() {
    interior_prune_must_not_lose_the_grandchild("table", true);
}

fn interior_prune_must_not_lose_the_grandchild(tag: &str, table: bool) {
    let e = env_with(tag, table);
    let reaper = TwoTierReaper::new(Arc::clone(&e.catalog), Arc::clone(&e.store));

    let gp = e.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
    let p = write_one(&e, gp.branch_id);
    let birth = e.store.read_page(p).unwrap().header().unwrap().birth_epoch;

    let parent = e.catalog.fork(gp.branch_id, LeaseDeadline::from_now(60_000)).unwrap();
    let child = e.catalog.fork(parent.branch_id, LeaseDeadline::from_now(60_000)).unwrap();
    assert_eq!(child.parent_id, Some(parent.branch_id));

    // CHILD can read P: P was born before PARENT forked, and CHILD's root IS PARENT's root,
    // which IS GP's root.
    assert!(birth < parent.fork_epoch, "P predates PARENT's fork");
    assert_eq!(child.root_page_id, gp.root_page_id, "CHILD's root is GP's root");

    // BEFORE the prune: the rule protects P, because PARENT forked inside the window.
    let now = e.catalog.next_epoch();
    assert!(
        e.catalog.live_child_in_epoch_range(gp.branch_id.id, birth, now).unwrap(),
        "before the prune, GP has a live child forked after P was born"
    );

    // MCTS prunes the interior node.
    reaper.reap(parent.branch_id).unwrap();

    // CHILD is still Live and still holds a root that reaches P.
    let child_now = e.catalog.get(child.branch_id).unwrap();
    assert_eq!(child_now.state, ferrodb::branch::types::BranchState::Live,
               "the pruned node's CHILD is still live");
    assert_eq!(child_now.root_page_id, gp.root_page_id, "and still points at GP's root");

    // AFTER the prune: ask the rule the same question.
    let after = e.catalog.next_epoch();
    let still_protected =
        e.catalog.live_child_in_epoch_range(gp.branch_id.id, birth, after).unwrap();
    println!("CHILD b{} is Live and reads page {p}; GP b{} live_child_in_epoch_range({birth:?}..{after:?}) = {still_protected}",
             child.branch_id.id, gp.branch_id.id);
    println!("GP's live children after the prune: max_live_child = {:?}, has_live_children = {}",
             e.catalog.max_live_child(gp.branch_id.id).unwrap(),
             e.catalog.has_live_children(gp.branch_id.id).unwrap());
    assert!(
        still_protected,
        "THE RULE LOST THE GRANDCHILD. Reaping the interior PARENT removed the only CHILD-index \
         entry under GP, so GP's page {p} now reads as reclaimable while CHILD b{} is Live and \
         its root still reaches it. The rule is stated over DIRECT children; MCTS prunes interior \
         nodes, which is exactly the operation that breaks the chain.",
        child.branch_id.id
    );
}
