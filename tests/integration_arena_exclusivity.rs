//! The arena region belongs to the arena store alone, and that is enforced rather than documented.
//!
//! `ArenaPageStore` claims `[base_page, infinity)` exclusively and checked only that `base_page`
//! sits at or above the disk manager's high-water mark. That check is not sufficient: the legacy
//! bitmap allocator's bits are zero from page 0, so it treats the whole arena region as free and
//! hands out pages that are already inside an extent. On a fresh database the very first
//! `allocate()` collided with the very first arena page -- both writers get the same page and each
//! silently overwrites the other.
//!
//! `ArenaPageStore::new` now registers the region with `DiskManager::reserve_from`, and the
//! bitmap allocator refuses to cross the floor instead of aliasing across it.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::BranchId;
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::{PageStore, PageType};
use ferrodb::storage::disk_manager::DiskManager;

/// The arena region has to start above whatever the ordinary heap and index pages will ever need;
/// it is a deliberate partition, not "wherever the file happens to end right now".
const ARENA_BASE: u32 = 1024;

fn parts(tag: &str) -> (tempfile::TempDir, Arc<DiskManager>, Arc<BufferPoolManager>, Arc<LogBranchCatalog>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{}.db", tag)))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(Arc::clone(&dm)));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    (dir, dm, pool, catalog)
}

#[test]
fn the_legacy_allocator_never_hands_out_a_page_inside_an_arena_extent() {
    let (_dir, dm, pool, catalog) = parts("exclusive");
    let store =
        Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), ARENA_BASE).unwrap());

    // A branch takes an extent and writes pages into it.
    let epoch = catalog.next_epoch();
    let arena = store.arena_for(BranchId::TRUNK).unwrap();
    let arena_pages: Vec<u32> = (0..64)
        .map(|_| store.alloc_in_arena(arena, PageType::BTreeLeaf, epoch).unwrap())
        .collect();

    // The ordinary heap/index path allocates, as any non-agent statement would.
    let legacy_pages: Vec<u32> = (0..64).map(|_| dm.allocate().unwrap()).collect();

    let clash: Vec<_> = legacy_pages.iter().filter(|p| arena_pages.contains(p)).collect();
    assert!(
        clash.is_empty(),
        "the bitmap allocator handed out {:?}, already inside an arena extent",
        clash
    );
    assert!(
        legacy_pages.iter().all(|p| *p < ARENA_BASE),
        "a legacy page landed at or above the arena floor: {:?}",
        legacy_pages
    );
    assert!(
        arena_pages.iter().all(|p| *p >= ARENA_BASE),
        "an arena page landed below its own base: {:?}",
        arena_pages
    );
}

#[test]
fn the_bitmap_allocator_refuses_rather_than_crossing_the_floor() {
    // Forcing the guard to fire: an arena pinned at the high-water mark of a fresh file leaves
    // the bitmap allocator no room at all. It must say so, not quietly alias into the region.
    let (_dir, dm, pool, catalog) = parts("starved");
    let base = dm.next_page_id.load(Ordering::SeqCst);
    let _store =
        ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), base).unwrap();

    let err = dm.allocate().unwrap_err();
    assert!(
        err.to_string().contains("reserved arena region"),
        "expected a refusal naming the reserved region, got {}",
        err
    );
}

#[test]
fn an_unreserved_database_allocates_exactly_as_before() {
    // The negative control. Every pre-existing database registers no arena, and for those the
    // floor is absent and the allocator's behaviour must be untouched.
    let (_dir, dm, _pool, _catalog) = parts("unreserved");
    assert_eq!(dm.arena_floor(), u32::MAX, "an unreserved database has no floor");

    let pages: Vec<u32> = (0..64).map(|_| dm.allocate().unwrap()).collect();
    let mut sorted = pages.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), pages.len(), "the allocator handed out a page twice");

    // and a freed page comes back
    dm.deallocate(pages[10]).unwrap();
    assert_eq!(dm.allocate().unwrap(), pages[10]);
}

#[test]
fn a_second_lower_arena_region_is_refused() {
    // Lowering the floor would put pages that are already inside the first store's extents back
    // into circulation.
    let (_dir, dm, pool, catalog) = parts("twofloors");
    let _a = ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), ARENA_BASE).unwrap();
    let err = dm.reserve_from(ARENA_BASE - 100).unwrap_err();
    assert!(err.to_string().contains("already reserved"), "got {}", err);
    assert_eq!(dm.arena_floor(), ARENA_BASE);
}

#[test]
fn an_arena_page_cannot_be_freed_through_the_bitmap() {
    // An arena page has no bit in the bitmap; clearing the bit at that index would free an
    // unrelated page belonging to the legacy allocator.
    let (_dir, dm, pool, catalog) = parts("crossfree");
    let _store =
        ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), ARENA_BASE).unwrap();
    let err = dm.deallocate(ARENA_BASE + 5).unwrap_err();
    assert!(err.to_string().contains("not this allocator's to free"), "got {}", err);
}
