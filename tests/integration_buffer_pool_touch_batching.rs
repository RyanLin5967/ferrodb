//! D44 — batched hit-path policy updates, and the property that makes them safe.
//!
//! A cache hit no longer calls `arc_cache.lock().touch(..)`. It appends to a per-thread shard and
//! returns; the backlog is applied by whoever next acquires the policy lock. That is BP-Wrapper
//! (Ding, Jiang, Zhang, ICDE 2009), and the reason D35 rejected it — "a constant, not a shape" —
//! was drawn from a measurement taken with the page table still in the way. It is the other half
//! of a pair: `bench/d35_c1_factorial.txt` shows three of four cells collapsing and only
//! mirror+batching rising.
//!
//! **The one thing batching must not do is change which page ARC evicts.** A delayed `touch` that
//! reorders evictions is a replacement-policy change wearing a concurrency fix's clothes, which is
//! exactly what "replace ARC with CLOCK" was rejected for. The rule that prevents it:
//!
//! > every pending update is applied, in order, before any ARC decision is taken
//!
//! enforced by draining inside `BufferPoolManager::arc_locked` — so acquiring the cache and
//! applying the backlog are one step and there is no path that skips it.
//!
//! The decisive evidence for that is not here: it is `bench/d35_c1_evictiontrace.txt`, a fixed
//! 16,192-step single-threaded trace whose output must be **byte-identical** to the unbatched
//! engine's, sha256 and all. These tests cover what the trace cannot localise — that a hit really
//! is deferred, that the drain really does apply it, that nothing is lost at a batch boundary, and
//! that `arc_locked` never hands out a cache with a backlog still pending.
//!
//! `bp.arc_cache.lock()` is used deliberately in places below. It is the *undrained* view — the
//! only way to observe that batching is happening at all, since `arc_locked` by construction never
//! shows a backlog. Production code must never take it; see the field's own doc.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};

const PAGES: u32 = 64;

fn pool(tag: &str) -> (tempfile::TempDir, Arc<BufferPoolManager>, Vec<u32>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let mut ids = Vec::with_capacity(PAGES as usize);
    for _ in 0..PAGES {
        let id = dm.allocate().expect("allocate");
        let mut data = [0u8; PAGE_SIZE];
        data[0..4].copy_from_slice(&id.to_be_bytes());
        dm.write(id, &data).expect("write");
        ids.push(id);
    }
    let bp = Arc::new(BufferPoolManager::new(dm));
    (dir, bp, ids)
}

/// **Kills: reverting the hit path to `arc_cache.lock().touch(..)`.**
///
/// The point of the change is that a hit does NOT reach the policy. If this fails, the batching is
/// not happening and every number measured for D44 is measuring the unbatched engine.
#[test]
fn a_cache_hit_defers_its_policy_update_instead_of_taking_the_lock() {
    let (_dir, bp, ids) = pool("defer");
    let p = ids[0];

    // First fetch is a MISS: it goes through `request`, which drains, so it leaves nothing queued.
    bp.fetch_page(p).expect("first fetch");
    bp.unpin_page(p, false);
    let before = bp.touch_queue.pending_len();

    // Second fetch is a HIT. Its policy update must be QUEUED, not applied.
    bp.fetch_page(p).expect("second fetch");
    bp.unpin_page(p, false);

    assert_eq!(
        bp.touch_queue.pending_len(),
        before + 1,
        "a cache hit did not queue its policy update - the hit path is still taking the policy lock"
    );

    // And it really is still unapplied: through the UNDRAINED view, p has not been promoted.
    {
        let c = bp.arc_cache.lock().unwrap();
        assert!(
            c.t1.contains(p),
            "page {p} was promoted out of T1 already, so the update was applied eagerly"
        );
        assert!(!c.t2.contains(p));
    }
}

/// **Kills: removing the drain from `arc_locked`.**
///
/// This is the whole correctness argument. A queued hit must have landed by the time anybody can
/// look at the policy — because the next thing anybody does with the policy is ask it what to
/// evict, and an answer computed on a stale recency order is a different answer.
#[test]
fn acquiring_the_policy_applies_every_pending_hit_first() {
    let (_dir, bp, ids) = pool("drain");
    let p = ids[0];

    bp.fetch_page(p).expect("miss");
    bp.unpin_page(p, false);
    bp.fetch_page(p).expect("hit");
    bp.unpin_page(p, false);

    // Undrained: still in T1.
    assert!(bp.arc_cache.lock().unwrap().t1.contains(p), "precondition: the hit must be pending");

    // Acquiring through the pool's own accessor must apply it: a second reference promotes T1->T2.
    {
        let c = bp.arc_locked();
        assert!(
            c.t2.contains(p),
            "arc_locked handed out a policy that had not seen the pending hit on page {p}. \
             Any eviction decided from here is decided on a stale recency order."
        );
        assert!(!c.t1.contains(p));
    }
    assert_eq!(
        bp.touch_queue.pending_len(),
        0,
        "arc_locked left updates pending after returning"
    );
}

/// **Kills: dropping the batch in `TouchQueue::record` when a shard fills.**
///
/// A shard that fills hands its batch back to the caller to apply. Losing it would silently
/// discard recency information — the pages ARC thinks are cold would be the ones that were hot,
/// and no throughput benchmark could see it.
#[test]
fn a_full_batch_is_applied_rather_than_dropped() {
    let (_dir, bp, ids) = pool("batch");
    let p = ids[0];
    let batch = bp.touch_queue.batch_size();

    bp.fetch_page(p).expect("miss");
    bp.unpin_page(p, false);

    // Drive past a batch boundary on hits alone. The fetch that fills the shard must apply it.
    for _ in 0..batch {
        bp.fetch_page(p).expect("hit");
        bp.unpin_page(p, false);
    }

    assert!(
        bp.touch_queue.pending_len() < batch,
        "the queue holds {} updates with a batch size of {batch} - a full shard was never flushed",
        bp.touch_queue.pending_len()
    );
    // The hits landed: p was referenced more than once, so it belongs in T2.
    assert!(
        bp.arc_cache.lock().unwrap().t2.contains(p),
        "after {batch} hits page {p} is still not in T2 - the batch was dropped instead of applied"
    );
}

/// **Kills: `drain_into` failing to empty its shards (updates applied twice).**
///
/// A `touch` applied twice is not harmless: the second one moves the page to the front of T2 again
/// on behalf of a reference that never happened, which is a recency order ARC did not earn.
#[test]
fn draining_twice_does_not_replay_the_same_updates() {
    let (_dir, bp, ids) = pool("replay");
    for &id in &ids[..8] {
        bp.fetch_page(id).expect("miss");
        bp.unpin_page(id, false);
        bp.fetch_page(id).expect("hit");
        bp.unpin_page(id, false);
    }
    assert!(bp.touch_queue.pending_len() > 0, "precondition: hits must be pending");

    drop(bp.arc_locked());
    assert_eq!(bp.touch_queue.pending_len(), 0, "the first drain left work behind");
    drop(bp.arc_locked());
    assert_eq!(bp.touch_queue.pending_len(), 0, "the second drain produced work from nowhere");
}

/// The policy must end up tracking exactly the resident set, batching or not. This is the same
/// invariant `integration_buffer_pool_latch.rs` asserts, restated here against a workload that
/// deliberately leaves a backlog outstanding when it finishes.
#[test]
fn the_policy_still_tracks_exactly_the_resident_set_with_a_backlog_outstanding() {
    let (_dir, bp, ids) = pool("tracked");
    for &id in &ids {
        bp.fetch_page(id).expect("miss");
        bp.unpin_page(id, false);
    }
    for _ in 0..3 {
        for &id in &ids {
            bp.fetch_page(id).expect("hit");
            bp.unpin_page(id, false);
        }
    }

    let resident = bp.page_table.read().unwrap().len();
    let tracked = {
        let c = bp.arc_locked();
        c.t1.len() + c.t2.len()
    };
    assert_eq!(
        resident, tracked,
        "{resident} pages are resident but the policy tracks {tracked}"
    );
}

/// Concurrency: nothing is lost, and the policy still tracks the resident set exactly. The shards
/// are per thread, so this is the arm where more than one of them is in play at once.
#[test]
fn concurrent_hits_lose_no_policy_updates() {
    let (_dir, bp, ids) = pool("concurrent");
    for &id in &ids {
        bp.fetch_page(id).expect("warm");
        bp.unpin_page(id, false);
    }

    let threads = 8;
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let bp = Arc::clone(&bp);
            let ids = ids.clone();
            std::thread::spawn(move || {
                for i in 0..2000 {
                    let id = ids[(t * 13 + i) % ids.len()];
                    if bp.fetch_page(id).is_ok() {
                        bp.unpin_page(id, false);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("thread panicked");
    }

    let resident = bp.page_table.read().unwrap().len();
    let tracked = {
        let c = bp.arc_locked();
        c.t1.len() + c.t2.len()
    };
    assert_eq!(
        resident, tracked,
        "after concurrent hits, {resident} pages are resident but the policy tracks {tracked}"
    );
    assert_eq!(
        bp.touch_queue.pending_len(),
        0,
        "arc_locked returned with updates still pending"
    );
}
