//! D58 — the point-read descent takes nothing shared, and must still be right under every
//! interleaving the latches used to exclude.
//!
//! Three things can now go wrong that could not before, and each has a test that FORCES it:
//!
//! 1. **A torn page.** An optimistic reader copies a frame's shadow while a writer refreshes it.
//!    The seqlock must make the reader retry, never hand it half-old, half-new bytes. Forced by a
//!    writer that fills a page with one repeated byte value, changing the value every write, while
//!    readers copy and assert every byte of every snapshot is the same value. Killed by removing
//!    the second version load (then torn pages appear within milliseconds).
//! 2. **A stale descent.** A reader's snapshot of an internal node predates a split; the key it
//!    wants moved to a leaf the snapshot does not point at. The B-link walk must find it. Forced
//!    through the `descend_optimistic` seam with a root snapshot taken BEFORE splits, which is
//!    the interleaving `read_leaf_for` cannot be paused into. Killed by removing the walk.
//! 3. **Eviction under a reader.** The pool evicts and reuses the frame a reader is looking at.
//!    The reader must notice (version/label) and fall back, never read another page's bytes as
//!    this one's. Forced by a tree far larger than the pool with readers and inserters racing,
//!    every optimistic answer checked against the value the key was inserted with.
//!
//! And the thing that must NOT change: every existing point read still returns what the latched
//! path returns, which the whole suite (`d53`, `d56`, `d57`, the B+tree concurrency tests) pins.

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::buffer::buffer_pool::BufferPoolManager;
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

// ---------------------------------------------------------------------------------------------
// 1. A torn page is never returned
// ---------------------------------------------------------------------------------------------

#[test]
fn an_optimistic_read_never_returns_a_torn_page() {
    let dir = tempfile::tempdir().unwrap();
    let bp = pool(&dir, "torn.db");
    let page = bp.new_page().unwrap();
    bp.unpin_page(page, false);

    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let writer = {
        let (bp, stop, writes) = (bp.clone(), stop.clone(), writes.clone());
        std::thread::spawn(move || {
            let frame_i = bp.fetch_page(page).unwrap();
            let mut fill: u8 = 0;
            while !stop.load(Ordering::Relaxed) {
                fill = fill.wrapping_add(1);
                bp.frame_write(frame_i).data = [fill; PAGE_SIZE];
                writes.fetch_add(1, Ordering::Relaxed);
                // A refresh back-to-back with the next leaves the version odd almost always, and
                // eight readers pulling the same 64 cache lines the writer is storing to make a
                // copy slower than the write period, so readers would only ever retry (measured:
                // 747 ok against 97M retries with a 3 us gap). A gap on the order of a real page
                // write's spacing gives readers windows, and a refresh still lands INSIDE many
                // copies, which is where a torn copy would come from.
                let t = Instant::now();
                while t.elapsed() < Duration::from_micros(40) {
                    std::hint::spin_loop();
                }
            }
            bp.unpin_page(page, true);
        })
    };

    let readers: Vec<_> = (0..8)
        .map(|_| {
            let (bp, stop) = (bp.clone(), stop.clone());
            std::thread::spawn(move || {
                let (mut ok, mut retried) = (0u64, 0u64);
                while !stop.load(Ordering::Relaxed) {
                    match bp.read_page_optimistic(page) {
                        Some(p) => {
                            let first = p.data[0];
                            assert!(
                                p.data.iter().all(|b| *b == first),
                                "TORN PAGE returned by an optimistic read: byte 0 is {first} but another byte differs"
                            );
                            ok += 1;
                        }
                        None => retried += 1,
                    }
                }
                (ok, retried)
            })
        })
        .collect();

    std::thread::sleep(Duration::from_millis(2000));
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    let (mut ok, mut retried) = (0, 0);
    for r in readers {
        let (o, r) = r.join().unwrap();
        ok += o;
        retried += r;
    }
    let w = writes.load(Ordering::Relaxed);
    // The detector must have been ABLE to fire: writes and reads both happened, and some reads
    // overlapped a refresh (retried > 0). A run with no overlap proves nothing.
    assert!(w > 1000, "the writer made only {w} writes; the race was never exercised");
    eprintln!("torn-page probe: writes={w} ok={ok} retried={retried}");
    assert!(ok > 1000, "readers completed only {ok} snapshots ({retried} retried, {w} writes)");
    assert!(retried > 0, "no read ever overlapped a refresh ({ok} ok, {w} writes): the race was not exercised");
}

// ---------------------------------------------------------------------------------------------
// 2. A stale descent is repaired by the walk
// ---------------------------------------------------------------------------------------------

#[test]
fn a_root_snapshot_from_before_the_splits_still_finds_every_key() {
    let dir = tempfile::tempdir().unwrap();
    let bp = pool(&dir, "stale.db");
    let t = Tree::create(bp.clone()).unwrap();

    // Grow the tree until the root is internal and has split at least once more, so a stale
    // snapshot of it points at a strict subset of the leaves.
    let mut n = 0;
    let root0 = t.root_page_id.load(Ordering::Acquire);
    while t.root_page_id.load(Ordering::Acquire) == root0 {
        n += 1;
        t.insert(Value::Integer(n), Value::Integer(n * 10)).unwrap();
    }
    let root1 = t.root_page_id.load(Ordering::Acquire);
    // Take the stale snapshot NOW: the root as it stands after the first root split.
    let stale = bp.read_page_optimistic(root1).expect("root resident");
    let stale_node = BPlusTreePage::<Value, Value>::deserialize(stale.data).unwrap();
    let n_at_snapshot = n;
    // Keep growing: every leaf to the right of what the snapshot knows is new, and the root
    // itself changes again when the internal level splits.
    while n < n_at_snapshot * 8 {
        n += 1;
        t.insert(Value::Integer(n), Value::Integer(n * 10)).unwrap();
    }
    // The snapshot must actually be stale: the live root was rewritten with separators the
    // snapshot does not have (the fixture is wrong otherwise, and the test would prove nothing).
    assert!(
        !bp.shadow_still(stale.frame_i, stale.version),
        "the root page did not change after the snapshot; the fixture forced no staleness"
    );
    assert!(n > n_at_snapshot * 4);

    // Every key, including the ones inserted after the snapshot, must be reachable from it.
    let mut missed = Vec::new();
    for k in 1..=n {
        let node = BPlusTreePage::<Value, Value>::deserialize(stale.data).unwrap();
        let _ = &stale_node;
        let key = Value::Integer(k);
        match t.descend_optimistic(node, root1, &key, 100_000).unwrap() {
            Some((_, leaf)) => {
                if leaf.get(&key).ok().flatten() != Some(&Value::Integer(k * 10)) {
                    missed.push(k);
                }
            }
            None => missed.push(k),
        }
    }
    assert!(
        missed.is_empty(),
        "{} of {n} keys were not found from a stale root snapshot (first: {:?}); the B-link walk is not repairing a stale descent",
        missed.len(),
        missed.first()
    );
    // And through the public path, which restarts from the live root: the same answer.
    for k in [1, n_at_snapshot, n_at_snapshot + 1, n] {
        assert_eq!(t.search(&Value::Integer(k)).unwrap(), Some(Value::Integer(k * 10)));
    }
    assert_eq!(t.search(&Value::Integer(n + 1)).unwrap(), None);
}

// ---------------------------------------------------------------------------------------------
// 3. Eviction under a reader
// ---------------------------------------------------------------------------------------------

#[test]
fn readers_racing_inserters_and_eviction_never_see_a_wrong_value() {
    let dir = tempfile::tempdir().unwrap();
    let bp = pool(&dir, "evict.db");
    let t = Arc::new(Tree::create(bp.clone()).unwrap());

    // Far more pages than the pool holds, so frames are evicted and reused throughout.
    let seed: i32 = 60_000;
    for k in 1..=seed {
        t.insert(Value::Integer(k), Value::Integer(k * 10)).unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let inserted = Arc::new(AtomicU64::new(seed as u64));

    let inserter = {
        let (t, stop, inserted) = (t.clone(), stop.clone(), inserted.clone());
        std::thread::spawn(move || {
            let mut k: i32 = seed;
            while !stop.load(Ordering::Relaxed) {
                k += 1;
                t.insert(Value::Integer(k), Value::Integer(k.wrapping_mul(10))).unwrap();
                inserted.store(k as u64, Ordering::Release);
            }
        })
    };
    let readers: Vec<_> = (0..8)
        .map(|r| {
            let (t, stop, inserted) = (t.clone(), stop.clone(), inserted.clone());
            std::thread::spawn(move || {
                let mut x: u64 = 0x9E37_79B9_7F4A_7C15 ^ (r as u64 + 1);
                let mut reads = 0u64;
                let t0 = Instant::now();
                while !stop.load(Ordering::Relaxed) && t0.elapsed() < Duration::from_secs(4) {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let hi = inserted.load(Ordering::Acquire);
                    let k = (x % hi) as i32 + 1;
                    let got = t.search(&Value::Integer(k)).unwrap();
                    assert_eq!(
                        got,
                        Some(Value::Integer(k.wrapping_mul(10))),
                        "reader {r}: key {k} (inserted, <= {hi}) came back as {got:?}"
                    );
                    reads += 1;
                }
                reads
            })
        })
        .collect();
    let reads: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
    stop.store(true, Ordering::Relaxed);
    inserter.join().unwrap();
    let final_n = inserted.load(Ordering::Acquire);
    assert!(reads > 10_000, "only {reads} reads: the race was not exercised");
    assert!(final_n > seed as u64 + 100, "the inserter made only {} inserts", final_n - seed as u64);
}

// ---------------------------------------------------------------------------------------------
// 3b. A stale page-table hint, forced
// ---------------------------------------------------------------------------------------------

/// The window in test 3 is nanoseconds wide and the race test above could not hit it in
/// seconds: the reader takes the frame hint for page X, then X is evicted and the frame reloaded
/// with page Y before the reader looks at the shadow. Without the label check the reader would
/// return Y's bytes as X's, with a stable even version, and nothing downstream could tell. Forced
/// here through the seam: take the hint, churn the pool until that frame holds another page,
/// then read the frame AS X.
#[test]
fn a_stale_frame_hint_is_refused_by_the_label() {
    let dir = tempfile::tempdir().unwrap();
    let bp = pool(&dir, "hint.db");
    let x = bp.new_page().unwrap();
    {
        let fi = bp.fetch_page(x).unwrap();
        bp.frame_write(fi).data = [0xAA; PAGE_SIZE];
        bp.unpin_page(x, true);
    }
    let hint = bp.fetch_page(x).unwrap();
    bp.unpin_page(x, false);
    assert!(bp.read_frame_optimistic(hint, x).is_some(), "the fixture's hint is not even valid before the churn");

    // Churn: allocate and touch far more pages than the pool holds until frame `hint` is reused.
    let mut reused = false;
    for _ in 0..8 {
        for _ in 0..1500 {
            let p = bp.new_page().unwrap();
            let fi = bp.fetch_page(p).unwrap();
            bp.frame_write(fi).data = [0x55; PAGE_SIZE];
            bp.unpin_page(p, true);
        }
        let now_holds = bp.frame_read(hint).page_id;
        if now_holds != Some(x) {
            reused = true;
            break;
        }
    }
    assert!(reused, "the fixture never evicted page {x} from frame {hint}; the pool did not churn");

    // The stale hint, used AS X: must be refused, never answered with another page's bytes.
    match bp.read_frame_optimistic(hint, x) {
        None => {}
        Some(p) => panic!(
            "a stale hint for page {x} returned frame {hint}'s bytes (first byte {:#x}) as that page — the label guard is gone",
            p.data[0]
        ),
    }
    // And the honest path still finds X, now in some other frame, with its own bytes.
    let p = bp.read_page_optimistic(x).or_else(|| {
        let fi = bp.fetch_page(x).unwrap();
        bp.unpin_page(x, false);
        bp.read_frame_optimistic(fi, x)
    });
    assert_eq!(p.map(|p| p.data[0]), Some(0xAA));
}
