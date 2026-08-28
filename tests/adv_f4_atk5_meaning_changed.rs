//! ATTACK 5 — single-node behaviour that changed meaning, and coverage the existing suite does not
//! actually provide.
//!
//! `tests/integration_cluster_grants.rs` states the case for single-node safety as: *"Single-node
//! ferrodb therefore drives the whole grant machinery on every allocation, which is what makes the
//! 1349 existing tests evidence that the machinery works rather than evidence that it is
//! bypassed."* This file measures the two halves of that: which allocations actually reach a
//! `GrantedCounter`, and one API whose semantics changed silently.
//!
//! Nothing here is about a cluster. Every test runs standalone.

use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, ARENA_EXTENT_PAGES};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cluster::ClusterScope;
use ferrodb::cow::{PageStore, PageType};
use ferrodb::storage::disk_manager::DiskManager;

const ARENA_BASE: u32 = 1024;

struct Store {
    store: Arc<ArenaPageStore>,
    catalog: Arc<LogBranchCatalog>,
    _dir: tempfile::TempDir,
}

fn store(tag: &str) -> Store {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(Arc::clone(&dm)));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let store =
        Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), ARENA_BASE).unwrap());
    Store { store, catalog, _dir: dir }
}

// =================================================================================================
// A5.1 — how much of the allocation path the existing suite actually exercises.
// =================================================================================================

/// The extent-start watermark is the only observable proof that a `GrantedCounter::take` happened.
/// This measures how many page allocations move it: one per **extent**, not one per page. So the
/// existing suite's coverage of the grant machinery is coverage of extent *claims*, and the
/// per-page path (`alloc_in_arena`) reaches no counter at all — which is why the cluster guard is
/// absent there (see ATTACK 1).
#[test]
fn a5_1_measure_which_allocations_reach_a_granted_counter() {
    let _scope = ClusterScope::standalone();
    let s = store("a5_1");
    let epoch = s.catalog.next_epoch();

    let arena = s.store.arena_for(BranchId::TRUNK).unwrap();
    let after_claim = s.store.extent_watermark();

    let mut moves = 0;
    let mut prev = after_claim;
    for _ in 0..(ARENA_EXTENT_PAGES - 1) {
        s.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap();
        let now = s.store.extent_watermark();
        if now != prev {
            moves += 1;
            prev = now;
        }
    }

    println!(
        "a5_1: {} page allocations inside a claimed extent moved the extent-start watermark {} \
         time(s). Watermark after the claim: {after_claim}; after {} allocations: {prev}.",
        ARENA_EXTENT_PAGES - 1,
        moves,
        ARENA_EXTENT_PAGES - 1
    );
    assert_eq!(
        moves, 0,
        "the per-page path does reach the counter after all, which would contradict ATTACK 1"
    );
}

// =================================================================================================
// A5.2 — `load_state` silently stopped being able to lower a watermark.
// =================================================================================================

/// Before 4168c16, `load_state` did:
///
/// ```text
/// self.space.next_extent_start.store(next_start, Ordering::SeqCst);
/// self.space.next_arena_id.store(next_arena, Ordering::SeqCst);
/// ```
///
/// an unconditional **set**. It now calls `GrantedCounter::raise_issued_through`, which is
/// documented as "Monotone, and it never lowers." That is a safer rule, and it is a different rule:
/// loading an older image into a store that has advanced past it no longer rewinds the counter,
/// while the `extents` map, `current`, `reserved_pages` and `live_pages` around it ARE replaced
/// wholesale.
///
/// This pins the new behaviour and the resulting split between the two. No existing test covers the
/// rewind direction — every one of them loads into a freshly constructed store, where max and set
/// agree.
#[test]
fn a5_2_load_state_no_longer_rewinds_the_watermark_but_does_rewind_the_map() {
    let _scope = ClusterScope::standalone();
    let s = store("a5_2");
    let epoch = s.catalog.next_epoch();

    // Image taken at one extent.
    let a1 = s.store.arena_for(BranchId::TRUNK).unwrap();
    s.store.alloc_in_arena(a1, PageType::BTreeLeaf, epoch).unwrap();
    let old_image = s.store.state_bytes();
    let w1 = s.store.extent_watermark();
    let reserved1 = s.store.reserved_page_count();

    // Advance well past it: three more extents on three more branches.
    for _ in 0..3 {
        let b = s
            .catalog
            .fork(BranchId::TRUNK, ferrodb::branch::types::LeaseDeadline(u64::MAX / 2))
            .unwrap()
            .branch_id;
        let a = s.store.arena_for(b).unwrap();
        s.store.alloc_in_arena(a, PageType::BTreeLeaf, epoch).unwrap();
    }
    let w2 = s.store.extent_watermark();
    let reserved2 = s.store.reserved_page_count();
    assert!(w2 > w1, "fixture: the store did not advance ({w1} -> {w2})");

    // Load the OLD image back in.
    s.store.load_state(&old_image).unwrap();
    let w3 = s.store.extent_watermark();
    let reserved3 = s.store.reserved_page_count();

    println!(
        "a5_2: watermark {w1} -> {w2} -> {w3} after loading the {w1}-era image; \
         reserved_pages {reserved1} -> {reserved2} -> {reserved3}"
    );

    // The new rule.
    assert_eq!(
        w3, w2,
        "raise_issued_through is documented as monotone, so loading an older image must keep the \
         higher watermark"
    );
    // And the half that is NOT monotone: the map went back.
    assert_eq!(
        reserved3, reserved1,
        "reserved_pages is replaced wholesale by the image, unlike the watermark"
    );
    assert!(
        reserved3 < reserved2,
        "MEANING CHANGED: load_state keeps the advanced watermark ({w3}) while resetting \
         reserved_pages to the image's value ({reserved3}, down from {reserved2}). Pages \
         [{reserved3}..{reserved2}) worth of extents are still counted as issued by the counter \
         but are no longer in the extents map, so they can never be reclaimed. Pre-4168c16 the \
         `store` call rewound both together. Exit criterion 8 is stated in reserved_pages, and no \
         existing test loads an image into an advanced store."
    );
}
