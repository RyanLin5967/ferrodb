//! S18 fire-check. **TWO OF THESE TESTS FAIL ON PURPOSE.** They are reproducers for structural
//! limits found while mapping BranchBench's MCTS topology onto this design, not a gate:
//!
//!   * `interior_node_reaped_while_a_child_is_live_must_not_free_the_childs_pages` — FAILS.
//!     Against the shipped `TableBranchCatalog` the reaper takes the FAST path for an interior
//!     node and frees pages a live child can still read.
//!   * `depth_cap_stops_the_chain_at_eight` — FAILS at depth 9, which is the documented cap
//!     (`MAX_BRANCH_DEPTH = 8`) observed against the shipped catalog rather than the test one.
//!
//! The other three pass and are the controls that make those two mean something.
//!
//! Does the reaper's fast/slow path decision survive the SHIPPED catalog?
//!
//! Every reaper test in the repo runs against `LogBranchCatalog` (see
//! `src/branch/arena.rs:1282`, `harness::Harness::new`). The shipped binary uses
//! `TableBranchCatalog` (`src/cli/cli.rs:91`). The two disagree about `BranchRecord::live_children`.

use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline, PageId};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

fn tmp(name: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("s18-{}-{}.db", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

struct Rig {
    catalog: Arc<TableBranchCatalog>,
    store: Arc<ArenaPageStore>,
    reaper: TwoTierReaper,
}

fn rig(name: &str) -> Rig {
    let dbp = tmp(name);
    let catp = tmp(&format!("{name}-cat"));
    let file = std::fs::OpenOptions::new()
        .create(true).read(true).write(true).open(&dbp).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Arc::new(TableBranchCatalog::open_sidecar(&catp, 1).unwrap());
    let base = bp.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(bp, Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base).unwrap(),
    );
    let reaper = TwoTierReaper::new(
        Arc::clone(&catalog) as Arc<dyn BranchCatalog>,
        Arc::clone(&store),
    );
    Rig { catalog, store, reaper }
}

fn write_pages(r: &Rig, b: BranchId, n: u32) -> Vec<PageId> {
    let arena = r.store.arena_for(b).unwrap();
    let epoch = r.catalog.next_epoch();
    (0..n)
        .map(|i| {
            let p = r.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap();
            let h = r.store.read_page(p).unwrap();
            let mut f = h.write();
            f.data[PAGE_HEADER_SIZE] = (i & 0xff) as u8;
            stamp_checksum(&mut f.data);
            p
        })
        .collect()
}

/// THE MCTS SHAPE. Reaping an interior node whose child is still live.
///
/// A writes page P. C forks off A AFTER P was born, so C can read P. A is abandoned and reaped
/// while C is still live. The reclamation rule says P is NOT reclaimable: C forked in
/// [birth(P), free(P)).
#[test]
fn interior_node_reaped_while_a_child_is_live_must_not_free_the_childs_pages() {
    let r = rig("interior");
    let a = r.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
    let pages = write_pages(&r, a.branch_id, 5);
    let live_after_write = r.store.live_page_count().unwrap();

    // C forks off A. C's root IS A's root, so C can read every page A has written.
    let c = r.catalog.fork(a.branch_id, LeaseDeadline::from_now(60_000)).unwrap();
    assert!(
        r.catalog.has_live_children(a.branch_id.id).unwrap(),
        "fixture: A must have a live child in the CHILD index"
    );
    assert_eq!(c.parent_id, Some(a.branch_id));

    // Every page A wrote is visible to C.
    for p in &pages {
        let h = r.store.read_page(*p).unwrap();
        assert!(h.header().unwrap().birth_epoch < c.fork_epoch, "page born before C forked");
    }

    // A is abandoned. The interior node is pruned; its child survives.
    let freed = r.reaper.reap(a.branch_id).unwrap();

    let live_after_reap = r.store.live_page_count().unwrap();
    assert_eq!(
        live_after_reap, live_after_write,
        "THE SLOW PATH DID NOT RUN. reap(A) freed {freed} pages that C can still read \
         (live {live_after_write} -> {live_after_reap}); A had a live child, so every one of \
         those pages is pinned by the interval rule."
    );
}

/// The flat-fanout control: the same reap, with NO live child. Here the fast path is correct.
#[test]
fn flat_fanout_leaf_reap_is_correct() {
    let r = rig("flat");
    let base = r.store.live_page_count().unwrap();
    let a = r.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
    write_pages(&r, a.branch_id, 5);
    assert_eq!(r.store.live_page_count().unwrap(), base + 5);
    r.reaper.reap(a.branch_id).unwrap();
    assert_eq!(r.store.live_page_count().unwrap(), base, "a childless leaf returns to baseline");
}

/// The depth cap, against the shipped catalog.
#[test]
fn depth_cap_stops_the_chain_at_eight() {
    let r = rig("depth");
    let mut cur = BranchId::TRUNK;
    let mut reached = 0u8;
    loop {
        match r.catalog.fork(cur, LeaseDeadline::from_now(60_000)) {
            Ok(rec) => {
                cur = rec.branch_id;
                reached = rec.depth;
            }
            Err(e) => {
                panic!("fork refused at depth {reached}: {e}");
            }
        }
        if reached >= 32 {
            break;
        }
    }
}

const _: () = assert!(PAGE_SIZE == 4096);

// ---------------------------------------------------------------------------------------
// THE CONTROL. Identical shape, `LogBranchCatalog` instead — the catalog every reaper test
// in the repo uses (`src/branch/arena.rs:1282`). If this passes while the table-catalog
// version fails, the variable is the catalog, not the fixture.
// ---------------------------------------------------------------------------------------

use ferrodb::branch::catalog::LogBranchCatalog;

struct LogRig {
    catalog: Arc<LogBranchCatalog>,
    store: Arc<ArenaPageStore>,
    reaper: TwoTierReaper,
}

fn log_rig(name: &str) -> LogRig {
    let dbp = tmp(name);
    let file = std::fs::OpenOptions::new()
        .create(true).read(true).write(true).open(&dbp).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let base = bp.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(bp, Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base).unwrap(),
    );
    let reaper = TwoTierReaper::new(
        Arc::clone(&catalog) as Arc<dyn BranchCatalog>,
        Arc::clone(&store),
    );
    LogRig { catalog, store, reaper }
}

#[test]
fn control_the_log_catalog_takes_the_slow_path_on_the_same_shape() {
    let r = log_rig("control");
    let a = r.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();

    let arena = r.store.arena_for(a.branch_id).unwrap();
    let epoch = r.catalog.next_epoch();
    for i in 0..5u32 {
        let p = r.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap();
        let h = r.store.read_page(p).unwrap();
        let mut f = h.write();
        f.data[PAGE_HEADER_SIZE] = (i & 0xff) as u8;
        stamp_checksum(&mut f.data);
    }
    let live_after_write = r.store.live_page_count().unwrap();

    let c = r.catalog.fork(a.branch_id, LeaseDeadline::from_now(60_000)).unwrap();
    assert_eq!(c.parent_id, Some(a.branch_id));
    assert!(r.catalog.has_live_children(a.branch_id.id).unwrap());
    // THE DIFFERENCE, stated directly: the log catalog's record carries the live set.
    assert!(
        !r.catalog.get_raw(a.branch_id.id).unwrap().live_children.is_empty(),
        "log catalog: live_children is inside the record"
    );

    let freed = r.reaper.reap(a.branch_id).unwrap();
    assert_eq!(
        r.store.live_page_count().unwrap(), live_after_write,
        "the log catalog must park all 5 pages, not free them (freed {freed})"
    );
}

/// And the table catalog's record, on the same shape, comes back with an EMPTY live set —
/// which is what `is_childless_leaf()` reads.
#[test]
fn table_catalog_hands_the_reaper_an_empty_live_set() {
    let r = rig("emptyset");
    let a = r.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
    let _c = r.catalog.fork(a.branch_id, LeaseDeadline::from_now(60_000)).unwrap();
    assert!(r.catalog.has_live_children(a.branch_id.id).unwrap(), "the INDEX knows");
    let rec = r.catalog.get_raw(a.branch_id.id).unwrap();
    assert!(rec.live_children.is_empty(), "the RECORD does not");
    assert!(
        rec.is_childless_leaf(),
        "so is_childless_leaf() — the reaper's fast/slow switch — says LEAF for an interior node"
    );
}
