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

/// The ownership half of the privacy rule.
///
/// Through [`CowTree`] alone this is belt-and-braces — a sibling's root never points at another
/// sibling's pages, so the birth-epoch test already refuses. But [`PageStore::cow_page`] is a
/// public entry point that the branch engine and the reaper call directly with a page id and a
/// branch, and there the two halves come apart: a page born *after* a branch forked, in someone
/// else's arena, passes the epoch test and fails only on ownership. Without this test the
/// ownership check is unfalsifiable, and an unfalsifiable guard is not a verified one.
#[test]
fn a_branch_can_never_mutate_another_branchs_page_in_place() {
    let f = Fixture::new(16);
    f.store.register_branch(B1, Some(BranchId::TRUNK), Epoch(2)).unwrap();
    f.store.register_branch(B2, Some(BranchId::TRUNK), Epoch(3)).unwrap();

    let a1 = f.store.arena_for(B1).unwrap();
    let p = f.store.alloc_in_arena(a1, PageType::BTreeLeaf, Epoch(10)).unwrap();

    // B2 forked at epoch 3, the page was born at epoch 10: the birth-epoch test alone would call
    // this private. Only ownership says otherwise.
    assert!(Epoch(10) >= Epoch(3), "the test setup must defeat the epoch test, not rely on it");
    assert!(!f.store.is_private(p, B2, Epoch(11)).unwrap());
    assert!(f.store.is_private(p, B1, Epoch(11)).unwrap(), "B1 owns it and must write in place");

    let cp = f.store.cow_page(p, B2, Epoch(11)).unwrap();
    assert!(cp.copied, "B2 was handed another branch's page to mutate");
    assert_ne!(cp.page_id, p);
    drop(cp);

    // B1's page is still B1's, and B2 did not free it
    let hdr = PageHeader::read_from(&f.store.read_page(p).unwrap().read().data).unwrap();
    assert_eq!(f.store.owner_of_arena(hdr.arena_id), Some(B1));
    assert_eq!(hdr.birth_epoch, Epoch(10));
    assert_eq!(f.store.pending_free_len(), 0);
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

    // The scan streams now, so materialising it is the test's own choice; the assertions below
    // are the same ones, on the same `Vec`.
    let scanned: Vec<_> =
        f.tree.range_scan(root, None, None).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(scanned.len(), n);
    assert!(scanned.windows(2).all(|w| w[0].0 < w[1].0), "scan is not ordered");
    assert_eq!(scanned[0].0, b"key000000".to_vec());

    let ranged: Vec<_> = f
        .tree
        .range_scan(root, Some(b"key000100"), Some(b"key000110"))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(ranged.len(), 10);
    assert_eq!(ranged[0].0, b"key000100".to_vec());
    assert_eq!(ranged[9].0, b"key000109".to_vec());
}

// ---- the scan cursor ------------------------------------------------------------------------

/// A `PageStore` that counts the pages read through it. Everything else is delegation.
///
/// The count is the only instrument that can tell a lazy scan from an eager one from the
/// outside: both return the same rows, and only one of them reads the whole tree to hand over
/// the first.
struct CountingStore {
    inner: Arc<dyn PageStore>,
    reads: AtomicU64,
}

impl CountingStore {
    fn new(inner: Arc<dyn PageStore>) -> Arc<CountingStore> {
        Arc::new(CountingStore { inner, reads: AtomicU64::new(0) })
    }
    fn take(&self) -> u64 {
        self.reads.swap(0, Ordering::SeqCst)
    }
}

impl PageStore for CountingStore {
    fn alloc_in_arena(
        &self,
        arena: ArenaId,
        page_type: PageType,
        birth_epoch: Epoch,
    ) -> Result<PageId, crate::error::FerroError> {
        self.inner.alloc_in_arena(arena, page_type, birth_epoch)
    }
    fn read_page(&self, page_id: PageId) -> Result<PageHandle, crate::error::FerroError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_page(page_id)
    }
    fn cow_page(
        &self,
        page_id: PageId,
        branch: BranchId,
        epoch: Epoch,
    ) -> Result<crate::cow::CowPage, crate::error::FerroError> {
        self.inner.cow_page(page_id, branch, epoch)
    }
    fn free_page(&self, page_id: PageId, free_epoch: Epoch) -> Result<(), crate::error::FerroError> {
        self.inner.free_page(page_id, free_epoch)
    }
    fn alloc_arena(&self, branch: BranchId) -> Result<ArenaId, crate::error::FerroError> {
        self.inner.alloc_arena(branch)
    }
    fn arena_for(&self, branch: BranchId) -> Result<ArenaId, crate::error::FerroError> {
        self.inner.arena_for(branch)
    }
    fn free_arena(&self, arena: ArenaId) -> Result<u32, crate::error::FerroError> {
        self.inner.free_arena(arena)
    }
    fn live_page_count(&self) -> Result<u32, crate::error::FerroError> {
        self.inner.live_page_count()
    }
    fn flush(&self) -> Result<(), crate::error::FerroError> {
        self.inner.flush()
    }
}

/// Build a multi-level tree and return it wrapped in a read counter.
fn counted_tree(n: u32) -> (Fixture, Arc<CountingStore>, CowTree, PageId) {
    let f = Fixture::new(128);
    let e = f.tick();
    let mut root = f.tree.create(BranchId::TRUNK, e).unwrap();
    for i in 0..n {
        root = f.put(root, BranchId::TRUNK, &format!("key{i:06}"), &format!("value-{i}"));
    }
    let counting = CountingStore::new(Arc::clone(&f.store) as Arc<dyn PageStore>);
    let tree = CowTree::new(Arc::clone(&counting) as Arc<dyn PageStore>);
    (f, counting, tree, root)
}

/// The shape claim, made falsifiable: reading the first row must not read the tree.
///
/// This is the test that stops the `Vec` coming back. Every other scan assertion in this file
/// passes just as happily against an eager implementation — same rows, same order — so without a
/// page count there is nothing in the suite that a reverted `range_scan` would break. The two
/// halves matter equally: `all` proves the tree really is large enough for the question to mean
/// something, and `one` proves the cursor did not visit it.
#[test]
fn reading_the_first_row_of_a_scan_does_not_read_the_whole_tree() {
    let (_f, counting, tree, root) = counted_tree(1500);

    counting.take();
    let first = tree.range_scan(root, None, None).unwrap().next().unwrap().unwrap();
    let one = counting.take();
    assert_eq!(first.0, b"key000000".to_vec(), "the cursor did not start at the first key");

    let drained: Vec<_> =
        tree.range_scan(root, None, None).unwrap().collect::<Result<_, _>>().unwrap();
    let all = counting.take();
    assert_eq!(drained.len(), 1500);

    assert!(
        all >= 15,
        "the fixture tree is only {all} pages, so 'the scan did not read the tree' is vacuous"
    );
    assert!(
        one <= 4,
        "the first row cost {one} page reads against a {all}-page tree. A root-to-leaf descent is \
         3 or 4; anything near {all} means the scan materialised the tree before yielding, which \
         is the Theta(table) memory ceiling this cursor exists to remove."
    );
}

/// A cursor that skips or repeats a row is worse than the `Vec` it replaced, so the bounds are
/// checked against an independent `BTreeMap` rather than against the tree's own opinion.
///
/// The interesting cases are the ones the rewrite touched: the start slot is now found by binary
/// search instead of by skipping entries one at a time, and `hi` now ends the whole scan instead
/// of just the current leaf. Both are off-by-one territory, and both are exercised at keys that
/// exist, keys that do not, and keys outside the tree entirely.
#[test]
fn every_bound_yields_exactly_the_keys_a_reference_map_holds() {
    use std::collections::BTreeMap;

    let n = 1500u32;
    let (_f, _counting, tree, root) = counted_tree(n);
    let reference: BTreeMap<Vec<u8>, Vec<u8>> = (0..n)
        .map(|i| (format!("key{i:06}").into_bytes(), format!("value-{i}").into_bytes()))
        .collect();

    let k = |i: u32| format!("key{i:06}").into_bytes();
    let cases: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> = vec![
        (None, None),
        (Some(k(0)), None),
        (None, Some(k(n))),
        (Some(k(0)), Some(k(n))),
        // Inside one leaf, and spanning many.
        (Some(k(100)), Some(k(110))),
        (Some(k(7)), Some(k(1400))),
        // Bounds that are not keys: before everything, after everything, between two keys.
        (Some(b"aaa".to_vec()), Some(b"zzz".to_vec())),
        (Some(b"key000100x".to_vec()), Some(b"key000200x".to_vec())),
        (Some(b"zzz".to_vec()), None),
        (None, Some(b"aaa".to_vec())),
        // Degenerate: empty by construction, and one key wide.
        (Some(k(500)), Some(k(500))),
        (Some(k(900)), Some(k(100))),
        (Some(k(42)), Some(k(43))),
        // Exactly the last key, which is where an exclusive upper bound goes wrong.
        (Some(k(n - 1)), None),
        (Some(k(n - 1)), Some(k(n))),
        (None, Some(k(n - 1))),
    ];

    for (lo, hi) in cases {
        let got: Vec<(Vec<u8>, Vec<u8>)> = tree
            .range_scan(root, lo.as_deref(), hi.as_deref())
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let want: Vec<(Vec<u8>, Vec<u8>)> = reference
            .iter()
            .filter(|(key, _)| {
                lo.as_deref().is_none_or(|l| key.as_slice() >= l)
                    && hi.as_deref().is_none_or(|h| key.as_slice() < h)
            })
            .map(|(a, b)| (a.clone(), b.clone()))
            .collect();
        let show = |b: &Option<Vec<u8>>| match b {
            Some(v) => String::from_utf8_lossy(v).into_owned(),
            None => "unbounded".into(),
        };
        assert_eq!(
            got,
            want,
            "scan of [{}, {}) does not match the reference map",
            show(&lo),
            show(&hi)
        );
    }
}

/// A cursor that hit an error must stay dead. Polling a fused iterator after a failure has to
/// return `None`, not re-raise the same page fault forever — a caller looping on `while let
/// Some(x)` would otherwise spin on a corrupt page rather than surface it once.
#[test]
fn a_cursor_that_failed_does_not_yield_again() {
    let (_f, _counting, tree, root) = counted_tree(1500);
    // A root that is not a btree node at all: the store's own page 0 header is not stamped as
    // one, so the first `next()` must fail and every later one must be `None`.
    let mut cursor = tree.range_scan(root, None, None).unwrap();
    assert!(cursor.next().is_some(), "fixture: the tree is empty");

    // Now the failing case, on a page that exists but is not a node.
    let bad = tree.store().alloc_in_arena(
        tree.store().arena_for(BranchId::TRUNK).unwrap(),
        PageType::Heap,
        Epoch(1),
    ).unwrap();
    let mut cursor = tree.range_scan(bad, None, None).unwrap();
    assert!(
        matches!(cursor.next(), Some(Err(_))),
        "a non-node root must be reported, not silently scanned as empty"
    );
    assert!(cursor.next().is_none(), "the cursor re-raised after an error instead of fusing");
    assert!(cursor.next().is_none());
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
    assert!(!f.store.arenas_of(B1).unwrap().is_empty());

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

/// Every other test shrinks the extent to keep the fixtures small, which means the default
/// constructor — the one the branch engine will actually call — would otherwise be untested.
#[test]
fn the_default_one_megabyte_extent_works_end_to_end() {
    use crate::branch::types::ARENA_EXTENT_PAGES;
    use crate::storage::disk_manager::PAGE_SIZE;
    assert_eq!(ARENA_EXTENT_PAGES as usize * PAGE_SIZE, 1 << 20, "an extent is not 1MB");

    let dir = TempDir::new().unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("cow.db"))
        .unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let store = Arc::new(CowStore::new(pool));
    let tree = CowTree::new(store.clone() as Arc<dyn PageStore>);

    let mut root = tree.create(BranchId::TRUNK, Epoch(1)).unwrap();
    for i in 0..600u32 {
        root = tree
            .insert(root, BranchId::TRUNK, Epoch(2 + i as u64), format!("k{:05}", i).as_bytes(), b"v")
            .unwrap();
    }
    let baseline = store.live_page_count().unwrap();
    assert!(baseline > 1);

    store.register_branch(B1, Some(BranchId::TRUNK), Epoch(5000)).unwrap();
    let mut r1 = root;
    for i in 0..600u32 {
        r1 = tree
            .insert(r1, B1, Epoch(6000 + i as u64), format!("k{:05}", i).as_bytes(), b"child")
            .unwrap();
    }
    assert_eq!(tree.get(root, b"k00300").unwrap(), Some(b"v".to_vec()));
    assert_eq!(tree.get(r1, b"k00300").unwrap(), Some(b"child".to_vec()));

    store.flush().unwrap();
    store.forget_branch(B1).unwrap();
    assert_eq!(store.live_page_count().unwrap(), baseline);
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
    let recycled_pages = f.tree.walk_pages(root).unwrap();
    assert!(!recycled_pages.is_empty());
    f.store.free_arena(victim).unwrap();

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
