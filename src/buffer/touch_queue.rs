//! Batched hit-path policy updates, so a cache HIT does not touch a process-wide lock.
//!
//! # What this is, and the one thing it must not do
//!
//! BP-Wrapper (Ding, Jiang, Zhang, ICDE 2009): a cache hit does not update the replacement policy
//! directly. It appends the page id to a per-thread FIFO, and whichever thread next holds the
//! policy lock drains every FIFO and applies the updates. The policy lock stops being a per-hit
//! cost and becomes a per-batch one.
//!
//! **It must not change which page ARC evicts.** That is the whole reason this is a batching
//! scheme and not a policy change: D35 already rejected "replace ARC with CLOCK" as a policy
//! decision in a concurrency fix's clothes, and a delayed `touch` that reorders evictions would be
//! the same mistake by a quieter route. The rule that buys it:
//!
//! > **Every pending update is applied, in order, before any ARC decision is taken.**
//!
//! `BufferPoolManager::arc_locked` is where that is enforced — it drains into the cache as part of
//! *acquiring* it, so there is no way to reach [`crate::buffer::arc::ArcCache`] and ask it
//! anything without the backlog having landed first. Single-threaded, the sequence of operations
//! ARC sees is therefore **identical** to the unbatched code, which is what makes the eviction
//! trace byte-identical rather than merely close. Under concurrency the interleaving between
//! threads differs — it already did, because threads race for the policy lock.
//!
//! # Why sharded mutexes and not a lock-free ring
//!
//! The cost being removed is not "a lock" in the abstract. It is **one cache line that every core
//! atomically RMWs** — the same finding as the page table's `RwLock` reader count. An *uncontended*
//! mutex is a single CAS on a line that stays in the owning core's L1; it is the sharing that
//! costs, not the locking. So each thread gets its own shard and the mutex is uncontended in the
//! common case, which buys the same thing a lock-free ring would at a fraction of the complexity
//! and with none of its memory-ordering surface.
//!
//! Shards are padded to 128 bytes. Without that, four `Mutex<Vec<u32>>` share a cache line and the
//! sharding achieves nothing — the threads would contend on the line instead of the lock, which is
//! the bug this whole lane is about, reintroduced one level down.
//!
//! # The lock-order rule, which is structural rather than documented
//!
//! The order is `arc_cache -> touch shard`: a drainer holds the policy lock and takes shard locks
//! under it. The hit path takes a shard lock **alone**.
//!
//! A hit that fills its shard has to apply the batch, which needs the policy lock — and taking it
//! while still holding the shard lock would invert the order and deadlock against a drainer.
//! [`TouchQueue::record`] therefore **removes the batch from the shard and releases the shard lock
//! before returning it**. The caller is handed a plain `Vec`, so it cannot be holding the shard
//! lock when it goes for the policy lock. The inversion is not expressible rather than forbidden.

use std::cell::Cell;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

// Per-thread slot index, assigned on first use and never reused.
//
// A plain `Cell` read on the fast path -- one thread-local load, not an atomic RMW. The counter
// below is only touched once per thread ever.
thread_local! {
    static SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
}

static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

#[inline]
fn my_slot() -> usize {
    SLOT.with(|s| {
        let v = s.get();
        if v != usize::MAX {
            return v;
        }
        let assigned = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
        s.set(assigned);
        assigned
    })
}

/// One shard, padded so two of them never share a cache line. See the module doc.
#[repr(align(128))]
struct Shard {
    pending: Mutex<Vec<u32>>,
}

/// Pending hit-path policy updates, sharded per thread.
pub struct TouchQueue {
    shards: Box<[Shard]>,
    /// How many updates a shard accumulates before its owner must apply them.
    batch: usize,
}

impl TouchQueue {
    /// A queue with at least `min_shards` shards (rounded up to a power of two, because the index
    /// is a mask) and a batch size of `batch`.
    ///
    /// **`batch` is the knob that trades lock traffic against staleness.** It is the factor by
    /// which policy-lock acquisitions on the hit path are reduced — one per `batch` hits instead
    /// of one per hit — and it is also how far ARC's recency order can lag reality under
    /// concurrency, bounded by `batch * threads` updates. It cannot affect single-threaded
    /// behaviour at all, because the drain-before-decide rule makes the applied sequence identical
    /// whatever the batch size is.
    pub fn new(min_shards: usize, batch: usize) -> Self {
        assert!(batch > 0, "a batch size of zero would never flush");
        let n = min_shards.max(1).next_power_of_two();
        TouchQueue {
            shards: (0..n)
                .map(|_| Shard { pending: Mutex::new(Vec::with_capacity(batch)) })
                .collect(),
            batch,
        }
    }

    /// Record a hit on `page_id`.
    ///
    /// Returns `Some(batch)` when this shard has just filled, and the caller **must** apply it to
    /// the cache — dropping it would lose recency information and degrade ARC, which is exactly
    /// what this type exists not to do. The shard lock is already released when the batch comes
    /// back, so the caller may take the policy lock without inverting the order.
    #[must_use = "a returned batch must be applied to the ArcCache, or ARC loses these hits"]
    pub fn record(&self, page_id: u32) -> Option<Vec<u32>> {
        let shard = &self.shards[my_slot() & (self.shards.len() - 1)];
        let mut pending = shard.pending.lock().unwrap();
        pending.push(page_id);
        if pending.len() >= self.batch {
            // Swap in a fresh buffer rather than draining into the caller's, so the shard lock is
            // held for a pointer swap and not for a copy.
            return Some(std::mem::replace(&mut *pending, Vec::with_capacity(self.batch)));
        }
        None
    }

    /// Append every pending update to `out`, oldest first within each shard, and empty the shards.
    ///
    /// Order **between** shards is the shard order and not a global arrival order. That is
    /// deliberate and it is the only reordering this type introduces: two threads' hits were
    /// already racing for the policy lock, so no global order existed to preserve. Within one
    /// thread's shard the order is exactly FIFO, which is what makes the single-threaded case
    /// identical to the unbatched code.
    pub fn drain_into(&self, out: &mut Vec<u32>) {
        for shard in self.shards.iter() {
            let mut pending = shard.pending.lock().unwrap();
            out.append(&mut pending);
        }
    }

    /// How many shards there are. For tests.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// How many updates a shard accumulates before its owner must apply them. For tests, which
    /// need to be able to drive a batch boundary without hard-coding the constant.
    pub fn batch_size(&self) -> usize {
        self.batch
    }

    /// How many updates are pending across every shard. For tests and assertions; racy under
    /// concurrency by construction.
    pub fn pending_len(&self) -> usize {
        self.shards.iter().map(|s| s.pending.lock().unwrap().len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_is_returned_until_the_batch_is_full() {
        let q = TouchQueue::new(4, 3);
        assert!(q.record(1).is_none());
        assert!(q.record(2).is_none());
        let batch = q.record(3).expect("the third record must fill a batch of 3");
        assert_eq!(batch, vec![1, 2, 3], "a batch must come back in FIFO order");
        assert_eq!(q.pending_len(), 0, "the shard was not emptied when it flushed");
    }

    #[test]
    fn a_drain_takes_everything_and_empties_the_queue() {
        let q = TouchQueue::new(4, 100);
        for id in 1..=5u32 {
            assert!(q.record(id).is_none());
        }
        let mut out = Vec::new();
        q.drain_into(&mut out);
        assert_eq!(out, vec![1, 2, 3, 4, 5]);
        assert_eq!(q.pending_len(), 0);
        // A second drain must be empty rather than repeating the batch: applying a `touch` twice
        // would promote a page to T2 that was only referenced once.
        let mut again = Vec::new();
        q.drain_into(&mut again);
        assert!(again.is_empty(), "drain returned the same updates twice");
    }

    #[test]
    fn shard_count_is_rounded_up_to_a_power_of_two() {
        // The index is `slot & (len - 1)`, which is only a valid index for a power-of-two length.
        assert_eq!(TouchQueue::new(5, 8).shard_count(), 8);
        assert_eq!(TouchQueue::new(8, 8).shard_count(), 8);
        assert_eq!(TouchQueue::new(0, 8).shard_count(), 1);
    }

    /// Nothing may be dropped: every id recorded must come back exactly once, through the two
    /// exits combined. A lost update is a hit ARC never hears about.
    #[test]
    fn every_recorded_update_comes_back_exactly_once() {
        let q = TouchQueue::new(1, 7);
        let mut seen: Vec<u32> = Vec::new();
        for id in 0..100u32 {
            if let Some(batch) = q.record(id) {
                seen.extend(batch);
            }
        }
        q.drain_into(&mut seen);
        assert_eq!(seen, (0..100).collect::<Vec<u32>>(), "updates were lost or reordered");
    }

    #[test]
    fn concurrent_recorders_lose_nothing() {
        use std::sync::Arc;
        let q = Arc::new(TouchQueue::new(8, 16));
        let threads = 8;
        let per_thread = 1000u32;
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let q = Arc::clone(&q);
                std::thread::spawn(move || {
                    let mut mine = Vec::new();
                    for i in 0..per_thread {
                        if let Some(batch) = q.record(t as u32 * per_thread + i) {
                            mine.extend(batch);
                        }
                    }
                    mine
                })
            })
            .collect();
        let mut seen: Vec<u32> = Vec::new();
        for h in handles {
            seen.extend(h.join().expect("thread panicked"));
        }
        q.drain_into(&mut seen);
        seen.sort_unstable();
        assert_eq!(
            seen,
            (0..threads as u32 * per_thread).collect::<Vec<u32>>(),
            "concurrent recording lost or duplicated updates"
        );
    }
}
