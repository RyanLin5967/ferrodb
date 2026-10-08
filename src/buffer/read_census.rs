//! **Per-thread counts of what a read touched — READ-VS-N.** Observing only.
//!
//! `bench/read_vs_n/PREREG.md` asks what a point read on one branch costs as the number of live,
//! written branches grows, and answers primarily in integers, because this machine is never quiet:
//! a wall clock moves with the fleet, while the number of pages a read copied, faulted in or
//! restarted over does not. Until this module the pool exposed no fetch counter at all —
//! `examples/d110_merge_page_reads.rs` wrapped `PageStore` for exactly that reason, and could not
//! see the branch catalog's pool, which is a different `BufferPoolManager`.
//!
//! # Why per-thread, and not an `AtomicU64` on the pool
//!
//! A shared counter bumped on the hit path is the wall this module's neighbours spent D44, D51 and
//! D58 removing: one word written by every reader is the ×0.12 class at 16 threads
//! (`bench/d51_sharedword_probe.txt`), and `examples/bufpool_fault_concurrency.rs` once measured
//! its own `AtomicUsize` instead of the pool (see the module doc of `buffer_pool.rs`). A `Cell` in
//! a thread-local is written by one core only. What it costs is one thread-local access per event,
//! the same on every arm of a comparison and independent of how many pages or branches exist — so
//! it can offset a curve and cannot give one a slope.
//!
//! # What it counts, and what it is blind to
//!
//! Per THREAD, process-wide: not per pool and not per tree. A caller that wants the catalog's share
//! apart from the data pool's snapshots [`this_thread`] around each call itself, which is what the
//! READ-VS-N harness does. Blind to: `leftmost_leaf` (an unbounded scan starts there, not at
//! `read_leaf_for`), the write-path descents (`latch_leaf_for_write`, `write_splitting`), and any
//! page read through `DiskManager` without the pool. Work done on another thread — the lease
//! thread's sweep, a group-commit flush — lands in THAT thread's counts, never the caller's.
//!
//! Nothing may read these to decide anything. A counter that steers is no longer an observer, and
//! the harness's guards assume the counts are a side effect only.

use std::cell::Cell;

/// One thread's totals since it started. Subtract an earlier snapshot with [`ReadCensus::since`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadCensus {
    /// `BufferPoolManager::fetch_page` calls: the pinned path, hit or miss.
    pub fetches: u64,
    /// Pages read from the file by the pool's fault path — a buffer-pool MISS that reached
    /// `DiskManager::read`. Counted on the attempt, so a read that then failed is still here.
    pub faults: u64,
    /// `BufferPoolManager::read_page_optimistic` calls: one D58 snapshot copy each.
    pub optimistic: u64,
    /// ...of which returned `None`: no page-table hint (not resident, or a mirror collision took
    /// the slot), or the frame was relabelled or torn during the copy.
    pub optimistic_misses: u64,
    /// `BPlusTreeManager::read_leaf_for` calls: one per point `search` and one per `range_scan` that
    /// has a lower bound.
    pub descents: u64,
    /// Optimistic attempts inside those descents. Equal to `descents` when nothing restarted.
    pub attempts: u64,
    /// Descents the optimistic path gave up on and the latched path answered.
    pub latched: u64,
    /// B-link right-walk hops at the leaf level of an optimistic descent.
    pub right_walks: u64,
    /// Leaves a `RangeScanner` loaded after its first.
    pub scan_leaves: u64,
}

impl ReadCensus {
    /// What happened between `earlier` and `self`. Both must be snapshots of the SAME thread,
    /// `earlier` taken first — the counts only grow, so anything else is a caller's mistake.
    pub fn since(&self, earlier: &ReadCensus) -> ReadCensus {
        ReadCensus {
            fetches: self.fetches - earlier.fetches,
            faults: self.faults - earlier.faults,
            optimistic: self.optimistic - earlier.optimistic,
            optimistic_misses: self.optimistic_misses - earlier.optimistic_misses,
            descents: self.descents - earlier.descents,
            attempts: self.attempts - earlier.attempts,
            latched: self.latched - earlier.latched,
            right_walks: self.right_walks - earlier.right_walks,
            scan_leaves: self.scan_leaves - earlier.scan_leaves,
        }
    }

    /// Accumulate `other` into `self`, for totals across reads or across threads.
    pub fn add(&mut self, other: &ReadCensus) {
        self.fetches += other.fetches;
        self.faults += other.faults;
        self.optimistic += other.optimistic;
        self.optimistic_misses += other.optimistic_misses;
        self.descents += other.descents;
        self.attempts += other.attempts;
        self.latched += other.latched;
        self.right_walks += other.right_walks;
        self.scan_leaves += other.scan_leaves;
    }
}

/// The events, as indices into this thread's counters. Crate-private: only the pool and the tree
/// record, and nothing outside may forge a count.
#[derive(Clone, Copy)]
pub(crate) enum Event {
    Fetch,
    Fault,
    Optimistic,
    OptimisticMiss,
    Descent,
    Attempt,
    Latched,
    RightWalk,
    ScanLeaf,
}

const EVENTS: usize = 9;
// A variant added after `ScanLeaf` without raising `EVENTS` would index past the array at run
// time, on the hottest path in the engine. Refuse it at compile time instead.
const _: () = assert!(Event::ScanLeaf as usize + 1 == EVENTS);

thread_local! {
    static COUNTS: [Cell<u64>; EVENTS] = const { [const { Cell::new(0) }; EVENTS] };
}

/// Record one `event` on this thread.
#[inline]
pub(crate) fn bump(event: Event) {
    COUNTS.with(|c| {
        let n = &c[event as usize];
        n.set(n.get() + 1);
    });
}

/// This thread's totals so far.
pub fn this_thread() -> ReadCensus {
    COUNTS.with(|c| {
        let at = |e: Event| c[e as usize].get();
        ReadCensus {
            fetches: at(Event::Fetch),
            faults: at(Event::Fault),
            optimistic: at(Event::Optimistic),
            optimistic_misses: at(Event::OptimisticMiss),
            descents: at(Event::Descent),
            attempts: at(Event::Attempt),
            latched: at(Event::Latched),
            right_walks: at(Event::RightWalk),
            scan_leaves: at(Event::ScanLeaf),
        }
    })
}
