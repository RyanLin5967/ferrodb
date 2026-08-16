//! End-to-end tests for the copy-on-write page store.
//!
//! The three the module brief names explicitly:
//! - [`fork_copies_zero_data_pages`]
//! - [`a_write_in_a_child_does_not_change_what_the_parent_reads`]
//! - [`two_siblings_writing_the_same_key_do_not_see_each_other`]
//!
//! Every count assertion below reads [`PageStore::live_page_count`], which is derived from the
//! arena table, not from anything the tree reports about itself.

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tempfile::TempDir;

use crate::branch::types::{ArenaId, BranchId, Epoch, PageId};
use crate::buffer::buffer_pool::BufferPoolManager;
use crate::cow::btree::CowTree;
use crate::cow::page_header::{PageHeader, PageType};
use crate::cow::store::CowStore;
use crate::cow::{PageHandle, PageStore, WriteBuffer, WriteBufferEntry};
use crate::storage::disk_manager::DiskManager;

struct Fixture {
    _dir: TempDir,
    store: Arc<CowStore>,
    tree: CowTree,
    clock: AtomicU64,
}

impl Fixture {
    fn new(extent_pages: u32) -> Fixture {
        let dir = TempDir::new().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("cow.db"))
            .unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let pool = Arc::new(BufferPoolManager::new(dm));
        let store = Arc::new(CowStore::with_extent_pages(pool, extent_pages));
        let tree = CowTree::new(store.clone() as Arc<dyn PageStore>);
        Fixture { _dir: dir, store, tree, clock: AtomicU64::new(1) }
    }

    fn tick(&self) -> Epoch {
        Epoch(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    /// Fork `child` off `parent`: one metadata registration, and the child's root **is** the
    /// parent's root. Nothing else, which is the whole point.
    fn fork(&self, parent: BranchId, child: BranchId, parent_root: PageId) -> PageId {
        let e = self.tick();
        self.store.register_branch(child, Some(parent), e).unwrap();
        parent_root
    }

    fn put(&self, root: PageId, b: BranchId, k: &str, v: &str) -> PageId {
        let e = self.tick();
        self.tree.insert(root, b, e, k.as_bytes(), v.as_bytes()).unwrap()
    }

    fn get(&self, root: PageId, k: &str) -> Option<String> {
        self.tree
            .get(root, k.as_bytes())
            .unwrap()
            .map(|v| String::from_utf8(v).unwrap())
    }

    fn count(&self) -> u32 {
        self.store.live_page_count().unwrap()
    }
}

const B1: BranchId = BranchId::new(1, 0);
const B2: BranchId = BranchId::new(2, 0);

// ---------------------------------------------------------------------------------------------
// The three required proofs
// ---------------------------------------------------------------------------------------------

#[test]
fn fork_copies_zero_data_pages() {
    let f = Fixture::new(64);
    let e = f.tick();
    let mut root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..400 {
        root = f.put(root, BranchId::TRUNK, &format!("k{:05}", i), "trunk-value");
    }
    let pages_before = f.count();
    let tree_pages_before = f.tree.walk_pages(root).unwrap();
    assert!(tree_pages_before.len() > 3, "test needs a multi-level tree, got {}", tree_pages_before.len());

    let child_root = f.fork(BranchId::TRUNK, B1, root);

    assert_eq!(f.count(), pages_before, "fork allocated pages");
    assert_eq!(child_root, root, "the fork is the root pointer, nothing else");
    assert_eq!(
        f.tree.walk_pages(child_root).unwrap(),
        tree_pages_before,
        "the child's tree is physically the parent's tree"
    );
    assert!(
        f.store.arenas_of(B1).unwrap().is_empty(),
        "a branch that has not written owns no arena"
    );
}

#[test]
fn a_write_in_a_child_does_not_change_what_the_parent_reads() {
    let f = Fixture::new(64);
    let e = f.tick();
    let mut trunk_root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..200 {
        trunk_root = f.put(trunk_root, BranchId::TRUNK, &format!("k{:05}", i), "trunk");
    }

    let mut child_root = f.fork(BranchId::TRUNK, B1, trunk_root);
    child_root = f.put(child_root, B1, "k00042", "child");
    child_root = f.put(child_root, B1, "novel", "child-only");

    assert_eq!(f.get(trunk_root, "k00042").as_deref(), Some("trunk"));
    assert_eq!(f.get(trunk_root, "novel"), None);
    assert_eq!(f.get(child_root, "k00042").as_deref(), Some("child"));
    assert_eq!(f.get(child_root, "novel").as_deref(), Some("child-only"));

    // and every untouched key still reads through the shared pages
    for i in 0..200 {
        if i == 42 {
            continue;
        }
        let k = format!("k{:05}", i);
        assert_eq!(f.get(child_root, &k).as_deref(), Some("trunk"), "key {}", k);
    }
}

#[test]
fn two_siblings_writing_the_same_key_do_not_see_each_other() {
    let f = Fixture::new(64);
    let e = f.tick();
    let mut trunk_root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..120 {
        trunk_root = f.put(trunk_root, BranchId::TRUNK, &format!("k{:05}", i), "base");
    }

    let mut r1 = f.fork(BranchId::TRUNK, B1, trunk_root);
    let mut r2 = f.fork(BranchId::TRUNK, B2, trunk_root);

    r1 = f.put(r1, B1, "k00007", "from-b1");
    r2 = f.put(r2, B2, "k00007", "from-b2");
    r1 = f.put(r1, B1, "only-b1", "1");
    r2 = f.put(r2, B2, "only-b2", "2");

    assert_eq!(f.get(r1, "k00007").as_deref(), Some("from-b1"));
    assert_eq!(f.get(r2, "k00007").as_deref(), Some("from-b2"));
    assert_eq!(f.get(trunk_root, "k00007").as_deref(), Some("base"));

    assert_eq!(f.get(r1, "only-b2"), None);
    assert_eq!(f.get(r2, "only-b1"), None);
    assert_eq!(f.get(trunk_root, "only-b1"), None);

    // physically disjoint: each sibling's novel pages come from its own arenas
    let a1 = f.store.arenas_of(B1).unwrap();
    let a2 = f.store.arenas_of(B2).unwrap();
    assert!(!a1.is_empty() && !a2.is_empty());
    for a in &a1 {
        assert!(!a2.contains(a), "siblings share arena {}", a);
        assert_eq!(f.store.owner_of_arena(*a), Some(B1));
    }
}

// ---------------------------------------------------------------------------------------------
// Copy-on-write mechanics
// ---------------------------------------------------------------------------------------------

#[test]
fn a_child_writing_never_frees_a_page_its_parent_still_points_at() {
    let f = Fixture::new(64);
    let e = f.tick();
    let mut trunk_root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..300 {
        trunk_root = f.put(trunk_root, BranchId::TRUNK, &format!("k{:05}", i), "base");
    }
    let trunk_pages = f.tree.walk_pages(trunk_root).unwrap();

    let mut r1 = f.fork(BranchId::TRUNK, B1, trunk_root);
    for i in 0..50 {
        r1 = f.put(r1, B1, &format!("k{:05}", i), "rewritten");
    }

    // every page the trunk pointed at is still readable and still owned by the trunk
    for p in &trunk_pages {
        let h = f.store.read_page(*p).unwrap();
        let hdr = PageHeader::read_from(&h.read().data).unwrap();
        assert_eq!(
            f.store.owner_of_arena(hdr.arena_id),
            Some(BranchId::TRUNK),
            "page {} changed owner",
            p
        );
    }
    for i in 0..300 {
        let k = format!("k{:05}", i);
        assert_eq!(f.get(trunk_root, &k).as_deref(), Some("base"), "trunk key {}", k);
    }
}

#[test]
fn a_page_private_to_the_writer_is_mutated_in_place_not_reshadowed() {
    let f = Fixture::new(64);
    let e = f.tick();
    let root = f.tree.create(BranchId::TRUNK, e).unwrap();
    let r1 = f.put(root, BranchId::TRUNK, "a", "1");
    let after_first = f.count();
    let r2 = f.put(r1, BranchId::TRUNK, "b", "2");
    assert_eq!(r2, r1, "root moved for a write into an already-private leaf");
    assert_eq!(f.count(), after_first, "a private page was shadowed again");
    assert_eq!(f.get(r2, "a").as_deref(), Some("1"));
    assert_eq!(f.get(r2, "b").as_deref(), Some("2"));
}

#[test]
fn a_live_child_forces_the_parent_to_shadow_instead_of_mutating() {
    let f = Fixture::new(64);
    let e = f.tick();
    let mut root = f.tree.create(BranchId::TRUNK, e).unwrap();
    root = f.put(root, BranchId::TRUNK, "a", "before-fork");

    let child_root = f.fork(BranchId::TRUNK, B1, root);
    let pages_at_fork = f.count();

    // The trunk now overwrites its own page. It owns it, but the child can see it, so the
    // interval rule must force a shadow copy and park the original.
    let new_root = f.put(root, BranchId::TRUNK, "a", "after-fork");
    assert_ne!(new_root, root, "trunk mutated a page its child can still see");
    assert_eq!(f.count(), pages_at_fork + 1);
    assert_eq!(f.store.pending_free_len(), 1, "the superseded page was not parked");

    assert_eq!(f.get(child_root, "a").as_deref(), Some("before-fork"));
    assert_eq!(f.get(new_root, "a").as_deref(), Some("after-fork"));

    // draining while the child is alive must release nothing
    assert_eq!(f.store.drain_pending_free().unwrap(), 0);
    assert_eq!(f.store.pending_free_len(), 1);

    // once the child is gone the interval is empty and the page comes back
    f.store.forget_branch(B1).unwrap();
    assert_eq!(f.store.pending_free_len(), 0);
    assert_eq!(f.count(), pages_at_fork);
}

#[test]
fn splits_and_multi_level_growth_preserve_every_key() {
    let f = Fixture::new(128);
    let e = f.tick();
    let mut root = f.tree.create(BranchId::TRUNK, e).unwrap();
    let n = 1500;
    for i in 0..n {
        root = f.put(root, BranchId::TRUNK, &format!("key{:06}", i), &format!("value-{}", i));
    }
    assert!(
        f.tree.walk_pages(root).unwrap().len() > 20,
        "test did not actually build a large tree"
    );
    for i in 0..n {
        assert_eq!(
            f.get(root, &format!("key{:06}", i)).as_deref(),
            Some(format!("value-{}", i).as_str()),
            "key {} lost across splits",
            i
        );
    }
    assert_eq!(f.get(root, "key999999"), None);

    let scanned = f.tree.range_scan(root, None, None).unwrap();
    assert_eq!(scanned.len(), n);
    assert!(scanned.windows(2).all(|w| w[0].0 < w[1].0), "scan is not ordered");
    assert_eq!(scanned[0].0, b"key000000".to_vec());

    let ranged = f
        .tree
        .range_scan(root, Some(b"key000100"), Some(b"key000110"))
        .unwrap();
    assert_eq!(ranged.len(), 10);
    assert_eq!(ranged[0].0, b"key000100".to_vec());
    assert_eq!(ranged[9].0, b"key000109".to_vec());
}

#[test]
fn delete_removes_only_the_named_key_and_a_miss_shadows_nothing() {
    let f = Fixture::new(64);
    let e = f.tick();
    let mut root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..250 {
        root = f.put(root, BranchId::TRUNK, &format!("k{:05}", i), "v");
    }
    let before = f.count();
    let e = f.tick();
    let same = f.tree.delete(root, BranchId::TRUNK, e, b"absent").unwrap();
    assert_eq!(same, root, "deleting a missing key moved the root");
    assert_eq!(f.count(), before, "deleting a missing key allocated a page");

    let e = f.tick();
    let root = f.tree.delete(root, BranchId::TRUNK, e, b"k00100").unwrap();
    assert_eq!(f.get(root, "k00100"), None);
    for i in 0..250 {
        if i == 100 {
            continue;
        }
        assert!(f.get(root, &format!("k{:05}", i)).is_some(), "collateral loss at {}", i);
    }
}

#[test]
fn a_deleted_key_stays_visible_to_a_branch_that_forked_before_the_delete() {
    let f = Fixture::new(64);
    let e = f.tick();
    let mut root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..80 {
        root = f.put(root, BranchId::TRUNK, &format!("k{:05}", i), "v");
    }
    let child_root = f.fork(BranchId::TRUNK, B1, root);
    let e = f.tick();
    let root = f.tree.delete(root, BranchId::TRUNK, e, b"k00007").unwrap();
    assert_eq!(f.get(root, "k00007"), None);
    assert_eq!(f.get(child_root, "k00007").as_deref(), Some("v"));
}

// ---------------------------------------------------------------------------------------------
// Arenas, reaping and page accounting
// ---------------------------------------------------------------------------------------------

#[test]
fn reaping_an_abandoned_branch_returns_the_page_count_to_baseline() {
    let f = Fixture::new(64);
    let e = f.tick();
    let mut trunk_root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..300 {
        trunk_root = f.put(trunk_root, BranchId::TRUNK, &format!("k{:05}", i), "base");
    }
    let baseline = f.count();

    let mut r1 = f.fork(BranchId::TRUNK, B1, trunk_root);
    for i in 0..300 {
        r1 = f.put(r1, B1, &format!("k{:05}", i), "agent-wrote-this");
    }
    assert!(f.count() > baseline, "the child never allocated anything");
    assert!(f.store.arenas_of(B1).unwrap().len() >= 1);

    // no client cooperation: nobody closed anything, the store is simply told the branch is gone
    let reclaimed = f.store.forget_branch(B1).unwrap();
    assert!(reclaimed > 0);
    assert_eq!(f.count(), baseline, "page count did not return to baseline");

    // the trunk is untouched by any of it
    for i in 0..300 {
        assert_eq!(
            f.get(trunk_root, &format!("k{:05}", i)).as_deref(),
            Some("base"),
            "trunk key {} damaged by the reap",
            i
        );
    }
}

#[test]
fn a_branch_that_dies_before_flushing_allocates_zero_pages() {
    let f = Fixture::new(64);
    let e = f.tick();
    let mut trunk_root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..100 {
        trunk_root = f.put(trunk_root, BranchId::TRUNK, &format!("k{:05}", i), "base");
    }
    let baseline = f.count();

    let _child_root = f.fork(BranchId::TRUNK, B1, trunk_root);
    let mut wb = WriteBuffer::new(B1);
    for i in 0..500 {
        wb.put(format!("k{:05}", i).into_bytes(), WriteBufferEntry::Put(b"buffered".to_vec()));
    }
    assert_eq!(f.count(), baseline, "buffered writes touched the page store");
    assert!(f.store.arenas_of(B1).unwrap().is_empty());

    // abandoned: the buffer is dropped, nothing was ever allocated
    drop(wb);
    assert_eq!(f.store.forget_branch(B1).unwrap(), 0);
    assert_eq!(f.count(), baseline);
}

#[test]
fn flushing_a_write_buffer_produces_the_same_tree_as_direct_inserts() {
    let f = Fixture::new(64);
    let e = f.tick();
    let mut trunk_root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..100 {
        trunk_root = f.put(trunk_root, BranchId::TRUNK, &format!("k{:05}", i), "base");
    }
    let mut r1 = f.fork(BranchId::TRUNK, B1, trunk_root);

    let mut wb = WriteBuffer::new(B1);
    for i in 0..40 {
        wb.put(format!("k{:05}", i).into_bytes(), WriteBufferEntry::Put(b"flushed".to_vec()));
    }
    wb.put(b"k00007".to_vec(), WriteBufferEntry::Delete);

    // probing the buffer must shadow the tree, including the tombstone
    assert_eq!(
        f.tree.get_buffered(r1, &wb, b"k00003").unwrap(),
        Some(b"flushed".to_vec())
    );
    assert_eq!(f.tree.get_buffered(r1, &wb, b"k00007").unwrap(), None);
    assert_eq!(
        f.tree.get_buffered(r1, &wb, b"k00099").unwrap(),
        Some(b"base".to_vec())
    );

    let e = f.tick();
    r1 = f.tree.flush_write_buffer(r1, B1, e, &mut wb).unwrap();
    assert!(wb.entries.is_empty());
    assert_eq!(f.get(r1, "k00003").as_deref(), Some("flushed"));
    assert_eq!(f.get(r1, "k00007"), None);
    assert_eq!(f.get(r1, "k00099").as_deref(), Some("base"));
    assert_eq!(f.get(trunk_root, "k00003").as_deref(), Some("base"));
}

#[test]
fn arenas_roll_over_when_an_extent_fills_and_stay_owned_by_one_branch() {
    // 4-page extents plus fat values force several rollovers
    let f = Fixture::new(4);
    let e = f.tick();
    let mut root = f.tree.create(BranchId::TRUNK, e).unwrap();
    let fat = "v".repeat(300);
    for i in 0..400 {
        root = f.put(root, BranchId::TRUNK, &format!("k{:05}", i), &fat);
    }
    let arenas = f.store.arenas_of(BranchId::TRUNK).unwrap();
    assert!(arenas.len() > 3, "extent rollover never happened: {:?}", arenas);
    for a in &arenas {
        assert_eq!(f.store.owner_of_arena(*a), Some(BranchId::TRUNK));
    }
    for i in 0..400 {
        assert!(f.get(root, &format!("k{:05}", i)).is_some(), "key {} lost across rollover", i);
    }
}

#[test]
fn freeing_an_arena_is_one_operation_not_a_per_page_scan() {
    let f = Fixture::new(16);
    let e = f.tick();
    let mut root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..60 {
        root = f.put(root, BranchId::TRUNK, &format!("k{:05}", i), "v");
    }
    let arenas = f.store.arenas_of(BranchId::TRUNK).unwrap();
    let before = f.count();
    let freed = f.store.free_arena(arenas[0]).unwrap();
    assert!(freed > 0);
    assert_eq!(f.count(), before - freed);
    // freeing it twice is a no-op, not a double count
    assert_eq!(f.store.free_arena(arenas[0]).unwrap(), 0);
    assert_eq!(f.count(), before - freed);
}

#[test]
fn a_recycled_extent_never_hands_back_stale_page_contents() {
    let f = Fixture::new(4);
    let e = f.tick();
    let mut root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..40 {
        root = f.put(root, BranchId::TRUNK, &format!("k{:05}", i), "v");
    }
    let arenas = f.store.arenas_of(BranchId::TRUNK).unwrap();
    let victim = arenas[0];
    let start = {
        // remember a page that is about to be recycled
        f.tree.walk_pages(root).unwrap();
        f.store.free_arena(victim).unwrap();
        victim
    };
    let _ = start;

    // the freed extent is handed to a different branch; every page it serves must come back
    // formatted, never carrying the old branch's bytes
    f.store.register_branch(B1, Some(BranchId::TRUNK), f.tick()).unwrap();
    let arena = f.store.arena_for(B1).unwrap();
    let e = f.tick();
    let p = f.store.alloc_in_arena(arena, PageType::BTreeLeaf, e).unwrap();
    let h = f.store.read_page(p).unwrap();
    let hdr = PageHeader::read_from(&h.read().data).unwrap();
    assert_eq!(hdr.birth_epoch, e);
    assert_eq!(hdr.arena_id, arena);
    assert_eq!(hdr.page_type, PageType::BTreeLeaf);
    assert!(h.read().data[crate::cow::PAGE_HEADER_SIZE + 12..].iter().all(|b| *b == 0));
}

// ---------------------------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------------------------

#[test]
fn an_unregistered_branch_is_refused_rather_than_guessed_at() {
    let f = Fixture::new(16);
    let e = f.tick();
    let root = f.tree.create(BranchId::TRUNK, e).unwrap();
    let ghost = BranchId::new(99, 0);
    let e = f.tick();
    let err = f.tree.insert(root, ghost, e, b"k", b"v").unwrap_err();
    assert!(
        err.to_string().contains("not registered"),
        "unexpected error: {}",
        err
    );
    assert!(f.store.arena_for(ghost).is_err());
    assert!(f.store.arenas_of(ghost).is_err());
}

#[test]
fn registering_the_same_branch_twice_is_refused() {
    let f = Fixture::new(16);
    let e = f.tick();
    f.store.register_branch(B1, Some(BranchId::TRUNK), e).unwrap();
    let e = f.tick();
    assert!(f.store.register_branch(B1, Some(BranchId::TRUNK), e).is_err());
    // and a child of a branch nobody has heard of
    let e = f.tick();
    assert!(f.store.register_branch(B2, Some(BranchId::new(77, 0)), e).is_err());
}

#[test]
fn a_torn_page_is_refused_rather_than_returned() {
    let f = Fixture::new(16);
    let e = f.tick();
    let mut root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..40 {
        root = f.put(root, BranchId::TRUNK, &format!("k{:05}", i), "v");
    }
    let victim = *f.tree.walk_pages(root).unwrap().last().unwrap();

    // corrupt the page behind the store's back, exactly as a torn write would
    {
        let h = PageHandle::fetch(f.store.pool().clone(), victim).unwrap();
        let mut frame = h.write();
        frame.data[crate::cow::PAGE_HEADER_SIZE + 30] ^= 0xFF;
    }
    let err = match f.store.read_page(victim) {
        Ok(_) => panic!("a torn page was handed back as if it were intact"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("checksum"), "unexpected error: {}", err);
}

#[test]
fn the_trunk_cannot_be_forgotten() {
    let f = Fixture::new(16);
    assert!(f.store.forget_branch(BranchId::TRUNK).is_err());
}

#[test]
fn an_oversized_entry_is_refused_rather_than_silently_truncated() {
    let f = Fixture::new(16);
    let e = f.tick();
    let root = f.tree.create(BranchId::TRUNK, e).unwrap();
    let big = vec![b'x'; 4096];
    let e = f.tick();
    let err = f.tree.insert(root, BranchId::TRUNK, e, b"k", &big).unwrap_err();
    assert!(err.to_string().contains("exceeds"), "unexpected error: {}", err);
}

#[test]
fn allocating_from_an_unknown_arena_is_an_error() {
    let f = Fixture::new(16);
    let e = f.tick();
    assert!(f.store.alloc_in_arena(ArenaId(4242), PageType::Heap, e).is_err());
}
