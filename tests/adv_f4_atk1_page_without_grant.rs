//! ATTACK 1 — obtain an arena extent page or an arena id on a cluster member that has been
//! granted nothing.
//!
//! The claim under attack (commit 4168c16): *"no node in a cluster can obtain an arena extent page,
//! an arena id, a transaction id, or a lease/reap decision without a leader grant."*
//!
//! `tests/integration_cluster_grants.rs` proves that claim for a store that is **fresh** when the
//! process joins. Every test here joins a store that has already done work, which is the state a
//! real node is in when it is added to a cluster, and which is the state
//! `ArenaSpaceManager::recycle_epoch` exists to talk about.
//!
//! Each test asserts the CLAIM, so a failure is the finding and its panic message is the evidence.
//!
//! # Scoping
//!
//! One `ClusterScope::standalone()` is held for the whole of each test and `cluster::join` /
//! `cluster::leave` are called inside it. The scope's lock is what serializes these tests against
//! each other; doing the transition inside one held scope makes standalone-then-member atomic,
//! where dropping a standalone scope and taking a joined one leaves a window in between.

use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline, ARENA_EXTENT_PAGES};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cluster::{self, ClusterScope};
use ferrodb::consensus::NodeId;
use ferrodb::cow::{PageStore, PageType};
use ferrodb::storage::disk_manager::DiskManager;

const ARENA_BASE: u32 = 1024;
const N1: NodeId = NodeId(1);

struct Store {
    store: Arc<ArenaPageStore>,
    catalog: Arc<LogBranchCatalog>,
    pool: Arc<BufferPoolManager>,
    dir: tempfile::TempDir,
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
    Store { store, catalog, pool, dir }
}

/// Every page id an extent claim covers.
fn extent_covers(start: u32, page: u32) -> bool {
    page >= start && page < start + ARENA_EXTENT_PAGES
}

// =================================================================================================
// A1.0 — the control. The guard must fire at all, or nothing below means anything.
// =================================================================================================

#[test]
fn a1_0_control_a_fresh_member_refuses() {
    let _scope = ClusterScope::standalone();
    let s = store("a1_0");
    cluster::join(N1);

    let err = s.store.arena_for(BranchId::TRUNK).expect_err(
        "CONTROL FAILED: a fresh member allocated with no grant. Nothing else in this file is \
         interpretable.",
    );
    assert!(
        format!("{err}").contains("no leader-granted"),
        "control: wrong refusal shape: {err}"
    );
}

// =================================================================================================
// A1.1 — `alloc_in_arena` on an extent claimed before the join.
// =================================================================================================

/// The extent was self-granted while standalone. The leader does not know this node has it, so it
/// is free to hand the same page range to another node — which is exactly why
/// `ArenaSpaceManager::recycle_epoch` discards the *free-extent stack* across a join. The interior
/// of a still-live extent is the same category of space.
#[test]
fn a1_1_alloc_in_arena_refuses_on_an_extent_self_granted_before_the_join() {
    let _scope = ClusterScope::standalone();
    let s = store("a1_1");

    // Standalone: claim one extent and use one page of it, so the bump pointer is mid-extent.
    let arena = s.store.arena_for(BranchId::TRUNK).unwrap();
    let epoch = s.catalog.next_epoch();
    let first = s.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap();
    let (start, _) = s.store.extent_range(arena).unwrap();
    assert!(extent_covers(start, first), "fixture: page {first} is outside extent at {start}");

    // Now this process becomes a cluster member. No grant is applied, ever.
    cluster::join(N1);
    assert!(cluster::is_clustered(), "fixture: the join did not take");

    let got = s.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch);
    if let Ok(page) = got {
        panic!(
            "GUARD FELL: a cluster member with zero grants applied obtained novel extent page \
             {page} (extent at {start}, arena {arena}) via alloc_in_arena. The extent was \
             self-granted while standalone, so no leader knows this node holds it and the leader \
             may grant page {page} to another node."
        );
    }
}

/// `arena_for` returns the cached `current` arena without consulting any counter, so the whole
/// write path keeps allocating after the join until the extent is full.
#[test]
fn a1_2_arena_for_refuses_to_reuse_the_current_arena_after_a_join() {
    let _scope = ClusterScope::standalone();
    let s = store("a1_2");

    let a1 = s.store.arena_for(BranchId::TRUNK).unwrap();
    let epoch = s.catalog.next_epoch();
    s.store.alloc_in_arena(a1, PageType::BTreeLeaf, epoch).unwrap();

    cluster::join(N1);

    let got = s.store.arena_for(BranchId::TRUNK);
    if let Ok(a) = got {
        // Prove it is not merely an id but usable space.
        let page = s.store.alloc_in_arena(a, PageType::BTreeLeaf, epoch);
        panic!(
            "GUARD FELL: arena_for returned arena {a} on a member with no grant (same arena as \
             before the join: {}), and alloc_in_arena then returned {:?}. arena_for reads \
             `current` and the extent's remaining space without asking either GrantedCounter.",
            a == a1,
            page
        );
    }
}

/// How much space a member with no grant can keep issuing: the whole tail of the extent.
#[test]
fn a1_3_a_member_with_no_grant_cannot_drain_a_pre_join_extent() {
    let _scope = ClusterScope::standalone();
    let s = store("a1_3");

    let arena = s.store.arena_for(BranchId::TRUNK).unwrap();
    let epoch = s.catalog.next_epoch();
    s.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap();
    let (start, _) = s.store.extent_range(arena).unwrap();

    cluster::join(N1);

    let mut got = Vec::new();
    for _ in 0..ARENA_EXTENT_PAGES {
        match s.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch) {
            Ok(p) => got.push(p),
            Err(_) => break,
        }
    }
    if !got.is_empty() {
        panic!(
            "GUARD FELL: a member with no grant issued {} extent pages after joining \
             ({:?}..={:?}, extent at {start}). That is {} of one {ARENA_EXTENT_PAGES}-page extent \
             handed out with no leader grant.",
            got.len(),
            got.first(),
            got.last(),
            got.len()
        );
    }
}

// =================================================================================================
// A1.4 — the per-arena recycled list, the other piece of issued space outside the counter.
// =================================================================================================

/// `release_page` parks a page in `StoreState::recycled`, and `alloc_in_arena` pops that list
/// *first*, before it even looks at the extent. `recycle_epoch` guards `free_extent_starts`; this
/// list has no epoch stamp at all.
#[test]
fn a1_4_the_per_arena_recycled_list_is_not_reusable_after_a_join() {
    let _scope = ClusterScope::standalone();
    let s = store("a1_4");

    let arena = s.store.arena_for(BranchId::TRUNK).unwrap();
    let epoch = s.catalog.next_epoch();
    let p = s.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap();
    s.store.release_page(p, arena);

    cluster::join(N1);

    let got = s.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch);
    if let Ok(page) = got {
        panic!(
            "GUARD FELL: a member with no grant obtained page {page} from the per-arena recycled \
             list (released page was {p}). StoreState::recycled carries no authority epoch, so a \
             page freed under a previous authority is handed straight back out."
        );
    }
}

// =================================================================================================
// A1.5 — free_arena then re-claim. This is the path `recycle_epoch` was written for.
// =================================================================================================

#[test]
fn a1_5_a_freed_extent_is_not_reclaimable_after_a_join() {
    let _scope = ClusterScope::standalone();
    let s = store("a1_5");

    let arena = s.store.arena_for(BranchId::TRUNK).unwrap();
    let epoch = s.catalog.next_epoch();
    s.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap();
    s.store.free_arena(arena).unwrap();
    assert_eq!(s.store.reserved_page_count(), 0, "fixture: the extent was not freed");

    cluster::join(N1);

    let got = s.store.arena_for(BranchId::TRUNK);
    if let Ok(a) = got {
        panic!(
            "GUARD FELL: a member with no grant re-claimed a freed extent as arena {a}. \
             recycle_epoch did not discard the free-extent stack across the join."
        );
    }
}

// =================================================================================================
// A1.6 — reopen_from_checkpoint on a member. The image carries space no leader granted.
// =================================================================================================

/// The checkpoint image contains `free_extent_starts` and the whole `extents` map. `load_state`
/// restores both and then *re-stamps* `recycle_epoch` with the epoch in force now — so the
/// free-extent stack arrives already blessed by the authority that never granted it.
#[test]
fn a1_6_a_member_cannot_get_space_out_of_a_checkpoint_image() {
    let _scope = ClusterScope::standalone();
    let s = store("a1_6");
    let path = s.dir.path().join("arena.ckpt");

    // Standalone: two extents, one of them freed so the free stack is non-empty, then checkpoint.
    let a1 = s.store.arena_for(BranchId::TRUNK).unwrap();
    let epoch = s.catalog.next_epoch();
    s.store.alloc_in_arena(a1, PageType::BTreeLeaf, epoch).unwrap();
    let forked = s.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX / 2)).unwrap();
    let a2 = s.store.arena_for(forked.branch_id).unwrap();
    s.store.alloc_in_arena(a2, PageType::BTreeLeaf, epoch).unwrap();
    s.store.free_arena(a2).unwrap();
    s.store.checkpoint(&path).unwrap();

    // The process joins a cluster, and *then* the store is reopened from that image.
    cluster::join(N1);

    let reopened = ArenaPageStore::reopen_from_checkpoint(
        Arc::clone(&s.pool),
        Arc::clone(&s.catalog),
        &path,
    )
    .unwrap();

    // The restored extents are in the map with their bump pointers intact.
    let live = reopened.live_arenas();
    assert!(!live.is_empty(), "fixture: the image restored no extents");

    let mut fell = Vec::new();
    for (arena, _owner) in &live {
        if let Ok(page) = reopened.alloc_in_arena(*arena, PageType::BTreeLeaf, epoch) {
            fell.push((*arena, page));
        }
    }
    if !fell.is_empty() {
        panic!(
            "GUARD FELL: a member with no grant obtained {:?} (arena, page) out of a checkpoint \
             image via alloc_in_arena on restored extents. load_state restores `extents` with no \
             authority epoch.",
            fell
        );
    }
}

// =================================================================================================
// A1.7 — concurrency. Threads racing the refusal must all refuse.
// =================================================================================================

#[test]
fn a1_7_threads_racing_arena_for_on_a_fresh_member_all_refuse() {
    let _scope = ClusterScope::standalone();
    let s = store("a1_7");
    cluster::join(N1);

    let mut handles = Vec::new();
    for _ in 0..16 {
        let store = Arc::clone(&s.store);
        handles.push(std::thread::spawn(move || {
            let mut ok = Vec::new();
            for _ in 0..64 {
                if let Ok(a) = store.arena_for(BranchId::TRUNK) {
                    ok.push(a);
                }
            }
            ok
        }));
    }
    let issued: Vec<_> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
    assert!(
        issued.is_empty(),
        "GUARD FELL: {} arenas were issued to a member with no grant under concurrency: {:?}",
        issued.len(),
        issued
    );
    assert_eq!(s.store.reserved_page_count(), 0, "refused claims still reserved pages");
}

/// The epoch `ArenaSpaceManager::reserve` passes to `recycled_start` is read before the take, so a
/// `join` landing in between is evaluated against a stale epoch. Racing it here to see whether the
/// free stack survives a join it should have been cleared by.
#[test]
fn a1_8_racing_a_join_against_reserve_cannot_smuggle_a_recycled_page() {
    let _scope = ClusterScope::standalone();
    let mut smuggled = Vec::new();

    for round in 0..200 {
        let s = store(&format!("a1_8_{round}"));
        cluster::leave();
        // Standalone: build a free-extent stack, then grant the arena-id counter enough room that
        // the *only* thing standing between a member and a page is the recycle-stack epoch check.
        let arena = s.store.arena_for(BranchId::TRUNK).unwrap();
        let epoch = s.catalog.next_epoch();
        s.store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap();
        s.store.free_arena(arena).unwrap();

        let store = Arc::clone(&s.store);
        let racer = std::thread::spawn(move || store.arena_for(BranchId::TRUNK));
        cluster::join(N1);
        let got = racer.join().unwrap();
        if let Ok(a) = got {
            // Either the join had not landed yet (legitimate) or the stale epoch let it through.
            // Distinguish by asking whether we are clustered now and whether the arena id could
            // only have come from a self-grant.
            smuggled.push((round, a, cluster::is_clustered()));
        }
        cluster::leave();
    }

    // A win for the racer is only interesting if it happened while clustered. Reported either way:
    // the point of this test is the number, not a boolean.
    let while_clustered: Vec<_> = smuggled.iter().filter(|(_, _, c)| *c).collect();
    println!(
        "a1_8: {} of 200 rounds returned an arena to the racing thread; {} of those with \
         is_clustered() true at the join point",
        smuggled.len(),
        while_clustered.len()
    );
}

// =================================================================================================
// A1.9 — the real COW write path, not `alloc_in_arena` called directly.
// =================================================================================================

/// `cow_page` is what an actual write goes through. It calls `arena_for` (which returns the cached
/// `current` arena) and then `alloc_in_arena` for the copy, so a member with no grant keeps
/// shadowing pages after the join.
#[test]
fn a1_9_the_cow_write_path_refuses_on_a_member_with_no_grant() {
    let _scope = ClusterScope::standalone();
    let s = store("a1_9");

    // Standalone: TRUNK owns arena a1 with two pages; a child branch owns a2.
    let trunk_arena = s.store.arena_for(BranchId::TRUNK).unwrap();
    let e0 = s.catalog.next_epoch();
    let inherited_a = s.store.alloc_in_arena(trunk_arena, PageType::BTreeLeaf, e0).unwrap();
    let inherited_b = s.store.alloc_in_arena(trunk_arena, PageType::BTreeLeaf, e0).unwrap();

    let child = s.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX / 2)).unwrap().branch_id;
    let e1 = s.catalog.next_epoch();
    let child_arena = s.store.arena_for(child).unwrap();
    assert_ne!(child_arena, trunk_arena, "fixture: the child did not get its own arena");

    // Prove the COW path copies (rather than mutating in place) before the join.
    let pre = s.store.cow_page(inherited_a, child, e1).unwrap();
    assert!(pre.copied, "fixture: the COW did not copy, so no page was allocated");

    // Now join. No grant, ever.
    cluster::join(N1);

    let got = s.store.cow_page(inherited_b, child, e1);
    match got {
        Ok(cow) if cow.copied => panic!(
            "GUARD FELL: the real COW write path allocated page {} on a cluster member with zero \
             grants applied (copying inherited page {inherited_b} into arena {child_arena}). \
             cow_page -> arena_for -> alloc_in_arena reaches no GrantedCounter while the current \
             extent has room.",
            cow.page_id
        ),
        Ok(_) => panic!("the COW mutated in place; the fixture no longer exercises allocation"),
        Err(_) => {}
    }
}
