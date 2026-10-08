//! READ-VS-N — the read census's own fire-checks.
//!
//! `bench/read_vs_n/PREREG.md` reads its primary evidence off `ferrodb::buffer::read_census`, and a
//! counter that was never forced to move is not evidence: a mis-wired one reads zero, and zero is
//! exactly what a resident, well-behaved read would also print. So every counter the harness quotes
//! is made to move here by an event caused on purpose, and made to stay still by the case that must
//! not count it. Expected values come from the event (one fetch is one fetch), never from calling
//! the code under test to find out what it does.

use std::fs::OpenOptions;
use std::ops::Bound;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::buffer::read_census::this_thread;
use ferrodb::catalog::column::Value;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::index::BPlusTreeManager;
use ferrodb::storage::index_page::BPlusTreePage;

type Tree = BPlusTreeManager<Value, Value>;

fn pool(dir: &tempfile::TempDir, name: &str) -> Arc<BufferPoolManager> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(name))
        .unwrap();
    Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())))
}

/// Is `page` in any frame? Asked of the frames themselves, not of the page table's mirror, whose
/// "absent" can also mean a slot collision.
fn resident(bp: &BufferPoolManager, page: u32) -> bool {
    (0..bp.frames.len()).any(|i| bp.frame_read(i).page_id == Some(page))
}

/// Allocate and dirty fresh pages until `gone()` holds. Refuses if it never does: a test named for
/// a miss that never produced one proves nothing (`tests/d58_latch_free_descent.rs`, test 3).
fn churn_until(bp: &BufferPoolManager, what: &str, gone: impl Fn() -> bool) {
    for _ in 0..8 {
        for _ in 0..1500 {
            let p = bp.new_page().unwrap();
            let fi = bp.fetch_page(p).unwrap();
            bp.frame_write(fi).data = [0x55; PAGE_SIZE];
            bp.unpin_page(p, true);
        }
        if gone() {
            return;
        }
    }
    panic!("the fixture never evicted {what}: the pool did not churn");
}

/// Levels from the root to a leaf, walked through `read_node`. Call it OUTSIDE any bracket — it
/// fetches pages, and those fetches would land in the counts under test.
fn height(t: &Tree) -> u64 {
    let mut page = t.root_page_id.load(Ordering::SeqCst);
    let mut h = 1;
    loop {
        match t.read_node(page).unwrap() {
            BPlusTreePage::Leaf(_) => return h,
            BPlusTreePage::Internal(n) => {
                page = n.child_ptrs[0];
                h += 1;
            }
        }
    }
}

fn tree_of(bp: &Arc<BufferPoolManager>, keys: impl Iterator<Item = i32>) -> Tree {
    let t = Tree::create(bp.clone()).unwrap();
    for k in keys {
        t.insert(Value::Integer(k), Value::Integer(k.wrapping_mul(10))).unwrap();
    }
    t
}

#[test]
fn a_miss_is_one_fault_and_a_hit_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let bp = pool(&dir, "fault.db");
    let x = bp.new_page().unwrap();
    churn_until(&bp, "page x", || !resident(&bp, x));

    let c0 = this_thread();
    bp.fetch_page(x).unwrap();
    bp.unpin_page(x, false);
    let miss = this_thread().since(&c0);
    assert_eq!((miss.fetches, miss.faults), (1, 1), "a fetch of an evicted page: {miss:?}");

    let c1 = this_thread();
    bp.fetch_page(x).unwrap();
    bp.unpin_page(x, false);
    let hit = this_thread().since(&c1);
    assert_eq!((hit.fetches, hit.faults), (1, 0), "a fetch of a resident page: {hit:?}");

    let c2 = this_thread();
    assert!(bp.read_page_optimistic(x).is_some(), "x was fetched a moment ago");
    let warm = this_thread().since(&c2);
    assert_eq!(
        (warm.optimistic, warm.optimistic_misses, warm.fetches, warm.faults),
        (1, 0, 0, 0),
        "an optimistic copy of a resident page: {warm:?}"
    );

    churn_until(&bp, "page x a second time", || !resident(&bp, x));
    let c3 = this_thread();
    assert!(bp.read_page_optimistic(x).is_none(), "x is in no frame");
    let cold = this_thread().since(&c3);
    assert_eq!(
        (cold.optimistic, cold.optimistic_misses, cold.fetches, cold.faults),
        (1, 1, 0, 0),
        "an optimistic read of an evicted page misses and NEVER faults it in: {cold:?}"
    );
}

/// PREREG P2 and P7, at the layer they are stated in.
#[test]
fn a_warm_descent_copies_one_page_per_level_and_a_cold_one_restarts_sixteen_times_then_latches() {
    let dir = tempfile::tempdir().unwrap();
    let bp = pool(&dir, "descent.db");
    let t = tree_of(&bp, 1..=5_000);
    let h = height(&t);
    assert!(h >= 2, "5,000 keys built a tree of height {h}; the descent never leaves the root");
    let key = Value::Integer(2_500);
    let want = Some(Value::Integer(25_000));
    assert_eq!(t.search(&key).unwrap(), want);

    let c0 = this_thread();
    assert_eq!(t.search(&key).unwrap(), want);
    let warm = this_thread().since(&c0);
    assert_eq!((warm.descents, warm.attempts, warm.latched), (1, 1, 0), "{warm:?}");
    assert_eq!(warm.right_walks, 0, "a present key's leaf holds it: {warm:?}");
    assert_eq!(warm.optimistic, h, "one copy per level of a height-{h} tree: {warm:?}");
    assert_eq!(
        (warm.optimistic_misses, warm.fetches, warm.faults),
        (0, 0, 0),
        "a resident descent takes nothing from the pool but copies: {warm:?}"
    );

    let root = t.root_page_id.load(Ordering::SeqCst);
    churn_until(&bp, "the root", || !resident(&bp, root));
    let c1 = this_thread();
    assert_eq!(t.search(&key).unwrap(), want);
    let cold = this_thread().since(&c1);
    assert_eq!((cold.descents, cold.latched), (1, 1), "{cold:?}");
    // `RESTARTS` in `read_leaf_for` is 16, and every attempt stops at the non-resident root. If
    // the constant changes, PREREG P7's "+15 per latched descent" changes with it — this is where
    // that dependency is written down.
    assert_eq!(cold.attempts, 16, "attempts before the latched fallback: {cold:?}");
    assert_eq!(
        (cold.optimistic, cold.optimistic_misses),
        (16, 16),
        "each attempt made one copy, of the root, and it missed: {cold:?}"
    );
    assert_eq!(cold.fetches, h, "the latched path fetches one page per level: {cold:?}");
    assert!((1..=h).contains(&cold.faults), "the root at least was read from the file: {cold:?}");
}

/// PREREG P3: an absent key that sorts between two leaves lands on the left one and walks right.
#[test]
fn an_absent_key_just_below_a_separator_walks_right_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let bp = pool(&dir, "walk.db");
    // Even keys only, so `separator - 1` is absent by construction.
    let t = tree_of(&bp, (1..=5_000).map(|k| 2 * k));
    let root = t.root_page_id.load(Ordering::SeqCst);
    let s = match t.read_node(root).unwrap() {
        BPlusTreePage::Internal(n) => n.key_arr[0].clone(),
        BPlusTreePage::Leaf(_) => panic!("5,000 keys fit in one leaf: the fixture has no separator"),
    };
    let s = match s {
        Value::Integer(v) => v,
        other => panic!("separator {other:?} is not an integer key"),
    };
    let h = height(&t);
    let probe = Value::Integer(s - 1);
    assert_eq!(t.search(&probe).unwrap(), None, "odd keys were never inserted");

    let c0 = this_thread();
    assert_eq!(t.search(&probe).unwrap(), None);
    let d = this_thread().since(&c0);
    assert_eq!((d.descents, d.attempts, d.latched), (1, 1, 0), "{d:?}");
    assert_eq!(d.right_walks, 1, "one hop from the left leaf to the separator's: {d:?}");
    assert_eq!(d.optimistic, h + 1, "one copy per level plus one per hop: {d:?}");
}

#[test]
fn a_range_scan_counts_every_leaf_after_its_first() {
    let dir = tempfile::tempdir().unwrap();
    let bp = pool(&dir, "scan.db");
    let t = tree_of(&bp, 1..=5_000);

    // Leaves counted independently of the scanner: down the leftmost path, then along the chain.
    let mut page = t.root_page_id.load(Ordering::SeqCst);
    while let BPlusTreePage::Internal(n) = t.read_node(page).unwrap() {
        page = n.child_ptrs[0];
    }
    let mut leaves = 1u64;
    while let BPlusTreePage::Leaf(l) = t.read_node(page).unwrap() {
        match l.next {
            Some(next) => {
                leaves += 1;
                page = next;
            }
            None => break,
        }
    }
    assert!(leaves >= 2, "the fixture has one leaf; a scan of it loads nothing after its first");

    let c0 = this_thread();
    let seen = t
        .range_scan(Bound::Included(Value::Integer(1)), Bound::Included(Value::Integer(5_000)))
        .unwrap()
        .count();
    let d = this_thread().since(&c0);
    assert_eq!(seen, 5_000);
    assert_eq!(d.descents, 1, "a bounded scan starts with one descent: {d:?}");
    assert_eq!(d.scan_leaves, leaves - 1, "{leaves} leaves, the first found by the descent: {d:?}");
}
