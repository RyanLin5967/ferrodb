//! D53 — the root-split retry in `read_leaf_for` was DORMANT, and this is what makes it fire.
//!
//! `BPlusTreeManager::read_leaf_for` opens every descent with a textbook root-split retry:
//!
//! ```ignore
//! let root = self.root_page_id.load(Ordering::Acquire);
//! let mut guard = self.latches().read(root);
//! // A root split leaves the old root holding half its keys. Re-read under the latch.
//! if self.root_page_id.load(Ordering::Acquire) != root { drop(guard); continue; }
//! ```
//!
//! It could never fire. `open()` wrapped the catalog's recorded root in a **private** `AtomicU32`,
//! so two handles on one tree held two independent root pointers: a split through one advanced
//! only *its* cell, and the other's retry compared a private value against itself. The global
//! `Mutex<Catalog>` in `pgwire` was the only thing actually providing the safety — which means
//! removing it for concurrency would have ACTIVATED a latent defect rather than merely exposing a
//! stale catalog value. See `SCALE-DESIGN` D53.
//!
//! These tests are deterministic on purpose. A threaded race would reproduce intermittently and
//! prove less: the defect is not a timing window, it is **which cell each handle reads**, so it
//! can be shown with no concurrency at all.

use std::fs::OpenOptions;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::Value;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::index::BPlusTreeManager;

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

/// Insert ascending keys through `t` until its root page id changes, and return the keys written.
///
/// Returning the count rather than assuming a fanout is the point: a hard-coded "insert 500" would
/// silently stop forcing a split the day a page grows, and the test would pass while testing
/// nothing.
fn split_the_root(t: &Tree, max: i32) -> (u32, u32, i32) {
    let before = t.root_page_id.load(Ordering::Acquire);
    for k in 1..=max {
        t.insert(Value::Integer(k), Value::Integer(k * 10)).unwrap();
        let now = t.root_page_id.load(Ordering::Acquire);
        if now != before {
            return (before, now, k);
        }
    }
    panic!("no root split after {max} inserts; this test can no longer force the condition it exists to test");
}

#[test]
fn a_private_root_cell_makes_the_split_retry_dormant() {
    let dir = tempfile::tempdir().unwrap();
    let bp = pool(&dir, "private.db");
    let writer = Tree::create(bp.clone()).unwrap();
    let root0 = writer.root_page_id.load(Ordering::Acquire);

    // A second handle opened the way every statement used to open one: a PRIVATE cell seeded from
    // the root as it stands now.
    let reader = Tree::open(root0, bp.clone());

    let (before, after, n) = split_the_root(&writer, 5000);
    assert_ne!(before, after, "the fixture did not actually split the root");

    // The writer's cell moved. The reader's did not, and cannot: they are different cells.
    assert_eq!(
        reader.root_page_id.load(Ordering::Acquire),
        root0,
        "a private cell must not have observed the split -- if it did, `open` is already sharing \
         and this test is measuring something else"
    );
    assert_ne!(
        writer.root_page_id.load(Ordering::Acquire),
        reader.root_page_id.load(Ordering::Acquire),
        "the two handles must disagree about the root; that disagreement IS the defect"
    );

    // And the consequence: descending from a stale root cannot see every key. We assert the
    // OUTCOME, not just the pointer, because a pointer difference that changed no answer would
    // not be worth a fix.
    let mut missed = 0;
    for k in 1..=n {
        if reader.search(&Value::Integer(k)).unwrap().is_none() {
            missed += 1;
        }
    }
    assert!(
        missed > 0,
        "a handle on a stale private root found all {n} keys, so the stale pointer changed no \
         answer -- D53's premise would be wrong and the design entry must be corrected"
    );
}

#[test]
fn a_shared_root_cell_lets_the_split_retry_fire() {
    let dir = tempfile::tempdir().unwrap();
    let bp = pool(&dir, "shared.db");
    let writer = Tree::create(bp.clone()).unwrap();

    // The fix: the second handle shares the writer's cell, which is what `Catalog::root_cell`
    // hands every statement.
    let reader = Tree::open_shared(writer.root_cell(), bp.clone());

    let (before, after, n) = split_the_root(&writer, 5000);
    assert_ne!(before, after, "the fixture did not actually split the root");

    assert_eq!(
        reader.root_page_id.load(Ordering::Acquire),
        writer.root_page_id.load(Ordering::Acquire),
        "a shared cell must show the split to both handles"
    );

    for k in 1..=n {
        assert_eq!(
            reader.search(&Value::Integer(k)).unwrap(),
            Some(Value::Integer(k * 10)),
            "key {k} was written before the split and must still be found after it"
        );
    }
}
