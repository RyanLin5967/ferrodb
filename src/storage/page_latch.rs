//! Per-page reader/writer latches, sitting **above** the buffer pool in the lock order.
//!
//! # Why this exists rather than reusing `buffer_pool.frames[i]`
//!
//! The obvious move for latch crabbing is to hold the frame's `RwLock` while descending. It
//! deadlocks, and the inversion is not subtle:
//!
//! - `BufferPoolManager::fetch_page` takes `arc_cache` **first** and holds it across the whole
//!   call — deliberately, see its comment — and takes frame locks underneath it (the pin bump on
//!   the hit path, the victim's lock on the evict path, and `frames[..].read()` inside the
//!   cache's `is_pinned` probe, which can land on *any* resident page).
//! - Crabbing needs to fetch the **child** while still holding the **parent**, which would take
//!   `frame(parent) → arc_cache`.
//!
//! So thread A holding `frame(P).write()` blocks on `arc_cache`, while thread B holding
//! `arc_cache` blocks on `frame(P).read()`. A latch layer above the pool removes the cycle: the
//! order is `page latch → arc_cache → page_table → frame`, and nothing under `arc_cache` ever
//! reaches for a page latch.
//!
//! # The ONE property this layer needs from the buffer pool
//!
//! **No `BufferPoolManager` operation may acquire a page latch, directly or transitively.**
//!
//! That is the whole contract, and it is worth stating separately because the paragraph above is
//! *not* it. "`fetch_page` holds `arc_cache` across frame locks" explains why the frame `RwLock`s
//! could not themselves be used as the crabbing latches — it is a fact about the pool's internals
//! that motivated building this layer, not a requirement on them. The pool is free to shard that
//! lock, drop it off the I/O path, or remove it entirely: crabbing keeps working, because crabbing
//! only ever calls *downward* (it holds a page latch and then calls `fetch_page`), and downward
//! calls cannot close a cycle unless the callee calls back up.
//!
//! This distinction is live: `S22-bufpool-latch` is taking the pool's global lock off the I/O path
//! while this is being written. That change is orthogonal to this layer by the contract above.
//!
//! # That contract is ENFORCED, not merely documented
//!
//! A documented invariant nothing checks is the exact shape that produced the three defects this
//! module exists to fix — `branch/group_commit.rs` said "BPlusTreeManager is not safe for
//! concurrent compound mutations" and enforced it by asking callers to remember a mutex. So the
//! contract is mechanical here, in two halves, both active under `debug_assertions`:
//!
//! 1. **A thread-local depth counter.** Every `BufferPoolManager` method that takes one of the
//!    pool's locks opens a *pool section* ([`enter_pool`]), and [`PageLatches::read`] and
//!    [`PageLatches::write`] refuse — loudly, with a panic naming the inversion — if a pool
//!    section is open on the calling thread. A future `fetch_page` that reaches up for a page
//!    latch therefore fails a test instead of deadlocking rarely in production.
//! 2. **An allowlist over who may latch at all** (`tests/lock_order_allowlist.rs`). Only
//!    `src/storage/index.rs` and `src/storage/range_scan.rs` may acquire a page latch. That is
//!    what makes half 1 complete rather than partial: the ~40 other `frames[i]` lock sites in the
//!    tree (`heap_file_manager`, `catalog`, `cow`, `wal`, `branch/arena`) are *untracked*, and
//!    that is sound only for as long as none of them takes a page latch. The allowlist test fails
//!    the moment one does.
//!
//! **Blind spots, stated here rather than discovered later.** (a) The counter is compiled out in
//! release; it is a test-time detector, not a runtime guard. (b) `src/branch/arena.rs` locks
//! `page_table` and `arc_cache` directly rather than through a `BufferPoolManager` method, so
//! those two acquisitions are untracked — harmless only because `arena.rs` is not on the
//! allowlist. (c) If `index.rs` or `range_scan.rs` ever reaches a frame lock through a *third*
//! module rather than through [`BufferPoolManager::frame_read`]/[`frame_write`] or a pool method,
//! that acquisition is invisible to half 1.
//!
//! [`frame_write`]: BufferPoolManager::frame_write
//!
//! # Why hand-rolled rather than `RwLock` per page
//!
//! A `HashMap<u32, RwLock<()>>` cannot hand out a guard that outlives the map lookup without
//! either a self-referential struct or `unsafe`, and this crate has no dependencies
//! (`Cargo.toml` lists none), so `parking_lot`'s owned `ArcRwLockReadGuard` is not available.
//! A counter under one `Mutex` + `Condvar` is a few lines, is obviously correct, and drops the
//! entry when the page goes idle so the table does not grow without bound.
//!
//! Striping a fixed array of `RwLock`s by `page_id % N` was considered and is **wrong here**:
//! crabbing latches a parent and then its child, and two pages that collide on a stripe would
//! make a thread block on a latch it already holds. Distinct pages must get distinct latches.
//!
//! # Fairness
//!
//! Writer-preferred: a reader waits while a writer is queued. Without it a steady stream of point
//! lookups starves the writer that is trying to split the leaf they are all reading.
//!
//! # The ordering discipline callers must keep
//!
//! Latches are acquired **down** the tree (parent before child) and **rightward** along the leaf
//! chain (a leaf before its `next`). Never upward, never leftward. The wait-for graph is then
//! ordered by depth and then by key, so it has no cycle. `src/storage/index.rs` is the only
//! caller and states where each latch is taken.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Condvar, Mutex};

#[cfg(doc)]
use crate::buffer::buffer_pool::BufferPoolManager;

// ------------------------------------------------------------------------------------------
// MECHANICAL LOCK-ORDER ENFORCEMENT — see "That contract is ENFORCED" in the module doc
// ------------------------------------------------------------------------------------------

/// Thread-local tracking of "am I inside the buffer pool right now".
///
/// A `u32` depth rather than a `bool` because pool methods nest: `free_page` calls
/// `delete_page`, and `flush_all` walks pages one at a time.
#[cfg(debug_assertions)]
mod order {
    use std::cell::Cell;

    thread_local! {
        static POOL_DEPTH: Cell<u32> = const { Cell::new(0) };
    }

    /// How many buffer-pool sections this thread currently has open. `0` means it holds no pool
    /// lock and may therefore acquire a page latch.
    pub fn pool_depth() -> u32 {
        POOL_DEPTH.with(|d| d.get())
    }

    /// Open a pool section: "this thread now holds, or is about to take, a buffer-pool lock."
    ///
    /// Call this at the top of any `BufferPoolManager` method that locks `arc_cache`,
    /// `page_table`, or a frame, and hold the returned guard for the whole method.
    #[must_use = "the pool section closes when this guard is dropped"]
    pub fn enter_pool() -> PoolSection {
        POOL_DEPTH.with(|d| d.set(d.get() + 1));
        PoolSection(())
    }

    pub struct PoolSection(pub(super) ());

    impl Drop for PoolSection {
        fn drop(&mut self) {
            POOL_DEPTH.with(|d| d.set(d.get() - 1));
        }
    }
}

/// Release build: every hook is a zero-sized no-op and `pool_depth()` const-folds to `0`, so the
/// assertion below compiles away entirely.
#[cfg(not(debug_assertions))]
mod order {
    pub struct PoolSection(pub(super) ());

    #[inline(always)]
    pub fn pool_depth() -> u32 {
        0
    }

    #[inline(always)]
    #[must_use]
    pub fn enter_pool() -> PoolSection {
        PoolSection(())
    }
}

pub use order::{PoolSection, enter_pool, pool_depth};

/// Panic if this thread is inside the buffer pool, because taking a page latch from there is the
/// `frame → arc_cache` inversion this layer exists to make impossible.
///
/// A panic and not a `Result`: the caller cannot do anything useful with "you have deadlocked the
/// storage engine", and the alternative to failing here is failing rarely, under load, as a hang
/// with no stack.
#[track_caller]
fn assert_page_latch_is_above_the_pool(mode: &str, page_id: u32) {
    let depth = pool_depth();
    assert!(
        depth == 0,
        "LOCK-ORDER INVERSION: a page latch ({mode} on page {page_id}) was requested while this \
         thread holds {depth} buffer-pool lock section(s). The order is \
         `page latch -> arc_cache -> page_table -> frame`, so a page latch may only be taken by a \
         thread holding NO pool lock. Taking one from underneath lets thread A hold frame(P) and \
         wait on this latch while thread B holds this latch and waits on frame(P). Fix the CALLER: \
         acquire the page latch first, then call into BufferPoolManager. See \
         src/storage/page_latch.rs."
    );
}

#[derive(Default)]
struct Latch {
    readers: u32,
    writer: bool,
    /// Writers blocked on this page. Readers defer to them; see "Fairness" above.
    writers_waiting: u32,
}

impl Latch {
    fn idle(&self) -> bool {
        self.readers == 0 && !self.writer && self.writers_waiting == 0
    }
}

/// One latch per page id, created on demand and dropped when the page goes idle.
///
/// **D58 — the table is STRIPED by page id.** It was one `Mutex<HashMap>` + one `Condvar` for
/// every page in the process: every latch acquire and every release took that one mutex, and
/// every release `notify_all`ed every waiter on every page. Profiled at 16 agent readers
/// (`bench/d58_profile_16T_read_window.sample.txt`): 47k of ~80k thread-samples were in
/// `PageLatches::read` / `PageReadGuard::drop` waiting on it. Striping the TABLE is not the
/// striping the module doc rejects below — that was striping the LATCHES, where two pages on one
/// stripe would share a latch and crabbing would self-block. Here each page keeps its own
/// `Latch` entry; only the map that holds it, and the condvar its waiters park on, are per-stripe,
/// and the stripe mutex is never held while a page latch is held, so a parent and a child on the
/// same stripe cannot deadlock. A waiter on stripe `s` may wake spuriously for another page on
/// `s`; it re-checks and sleeps again, exactly as before.
pub struct PageLatches {
    stripes: Vec<Stripe>,
}

#[derive(Default)]
struct Stripe {
    table: Mutex<HashMap<u32, Latch>>,
    wake: Condvar,
}

/// A power of two so the modulo is a mask; 64 stripes for a pool whose hot set at 16 readers is
/// a handful of pages spreads them with high probability, and a collision costs a spurious wake,
/// never correctness.
const STRIPES: usize = 64;

impl Default for PageLatches {
    fn default() -> Self {
        PageLatches { stripes: (0..STRIPES).map(|_| Stripe::default()).collect() }
    }
}

impl PageLatches {
    pub fn new() -> Self {
        PageLatches::default()
    }

    fn stripe(&self, page_id: u32) -> &Stripe {
        &self.stripes[(page_id as usize) & (STRIPES - 1)]
    }

    /// Shared access to `page_id`. Blocks while a writer holds or is waiting for it.
    ///
    /// Panics in a debug build if the calling thread is inside the buffer pool; see
    /// [`assert_page_latch_is_above_the_pool`].
    #[track_caller]
    pub fn read(&self, page_id: u32) -> PageReadGuard<'_> {
        assert_page_latch_is_above_the_pool("read", page_id);
        let st = self.stripe(page_id);
        let mut table = st.table.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            let l = table.entry(page_id).or_default();
            if !l.writer && l.writers_waiting == 0 {
                l.readers += 1;
                return PageReadGuard { latches: self, page_id };
            }
            table = st.wake.wait(table).unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Exclusive access to `page_id`. Blocks until no reader and no writer holds it.
    ///
    /// Panics in a debug build if the calling thread is inside the buffer pool; see
    /// [`assert_page_latch_is_above_the_pool`].
    #[track_caller]
    pub fn write(&self, page_id: u32) -> PageWriteGuard<'_> {
        assert_page_latch_is_above_the_pool("write", page_id);
        let st = self.stripe(page_id);
        let mut table = st.table.lock().unwrap_or_else(|p| p.into_inner());
        table.entry(page_id).or_default().writers_waiting += 1;
        loop {
            let l = table.entry(page_id).or_default();
            if !l.writer && l.readers == 0 {
                l.writers_waiting -= 1;
                l.writer = true;
                return PageWriteGuard { latches: self, page_id };
            }
            table = st.wake.wait(table).unwrap_or_else(|p| p.into_inner());
        }
    }

    fn release_read(&self, page_id: u32) {
        let st = self.stripe(page_id);
        let mut table = st.table.lock().unwrap_or_else(|p| p.into_inner());
        if let Entry::Occupied(mut e) = table.entry(page_id) {
            e.get_mut().readers -= 1;
            if e.get().idle() {
                e.remove();
            }
        }
        st.wake.notify_all();
    }

    fn release_write(&self, page_id: u32) {
        let st = self.stripe(page_id);
        let mut table = st.table.lock().unwrap_or_else(|p| p.into_inner());
        if let Entry::Occupied(mut e) = table.entry(page_id) {
            e.get_mut().writer = false;
            if e.get().idle() {
                e.remove();
            }
        }
        st.wake.notify_all();
    }

    /// Latches currently held or queued. Test-only introspection: a non-zero value after an
    /// operation returns means a guard leaked.
    pub fn outstanding(&self) -> usize {
        self.stripes.iter().map(|st| st.table.lock().unwrap_or_else(|p| p.into_inner()).len()).sum()
    }
}

#[must_use = "a page latch is released when its guard is dropped"]
pub struct PageReadGuard<'a> {
    latches: &'a PageLatches,
    page_id: u32,
}

impl Drop for PageReadGuard<'_> {
    fn drop(&mut self) {
        self.latches.release_read(self.page_id);
    }
}

#[must_use = "a page latch is released when its guard is dropped"]
pub struct PageWriteGuard<'a> {
    latches: &'a PageLatches,
    page_id: u32,
}

impl PageWriteGuard<'_> {
    pub fn page_id(&self) -> u32 {
        self.page_id
    }
}

impl Drop for PageWriteGuard<'_> {
    fn drop(&mut self) {
        self.latches.release_write(self.page_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn readers_share_and_a_writer_excludes() {
        let l = Arc::new(PageLatches::new());
        let a = l.read(7);
        let b = l.read(7); // two readers at once, or this would block forever
        drop(a);
            drop(b);

        let w = l.write(7);
        let blocked = Arc::new(AtomicUsize::new(0));
        let (l2, b2) = (Arc::clone(&l), Arc::clone(&blocked));
        let h = std::thread::spawn(move || {
            let _g = l2.read(7);
            b2.fetch_add(1, Ordering::SeqCst);
        });
        // The reader must not have got in while the write latch is held.
        std::thread::yield_now();
        assert_eq!(blocked.load(Ordering::SeqCst), 0, "a reader entered while a writer held the latch");
        drop(w);
        h.join().unwrap();
        assert_eq!(blocked.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn different_pages_do_not_block_each_other() {
        let l = PageLatches::new();
        let _a = l.write(1);
        let _b = l.write(2); // a stripe-by-hash design would deadlock here
        let _c = l.read(3);
    }

    #[test]
    fn the_table_empties_when_every_guard_is_dropped() {
        let l = PageLatches::new();
        {
            let _a = l.write(1);
            let _b = l.read(2);
            assert_eq!(l.outstanding(), 2);
        }
        assert_eq!(l.outstanding(), 0, "a latch entry outlived its guard");
    }

    /// Mutual exclusion is the whole point, so assert it rather than assuming it: a counter
    /// incremented non-atomically under the write latch must come out exact.
    #[test]
    fn the_write_latch_actually_serialises() {
        let l = Arc::new(PageLatches::new());
        let cell = Arc::new(Mutex::new(0u64)); // used only to hand a &mut across threads
        let mut hs = Vec::new();
        for _ in 0..8 {
            let l = Arc::clone(&l);
            let cell = Arc::clone(&cell);
            hs.push(std::thread::spawn(move || {
                for _ in 0..500 {
                    let _g = l.write(42);
                    // read-modify-write with a gap in the middle, exactly the shape the B+tree had
                    let v = *cell.lock().unwrap();
                    std::thread::yield_now();
                    *cell.lock().unwrap() = v + 1;
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(*cell.lock().unwrap(), 4000, "the write latch did not serialise");
    }

    // --------------------------------------------------------------------------------------
    // THE LOCK-ORDER DETECTOR — forced to fire, then shown not to fire spuriously
    // --------------------------------------------------------------------------------------
    //
    // A detector that has never fired is not a passing detector, so both directions are pinned
    // here. These use an IDLE page on purpose: without the assertion the inverted order below
    // would simply succeed (nothing holds the latch), so the test exercises the check itself
    // rather than hoping to catch a real deadlock.
    //
    // WHY THE NEXT FOUR ARE `#[cfg(debug_assertions)]`, and why that is a gating fix and not a
    // weakened test. The subject they exercise DOES NOT EXIST in release: `mod order` is
    // `#[cfg(not(debug_assertions))]` there, `pool_depth()` const-folds to 0, and the module doc
    // above says so in its own words -- "every hook is a zero-sized no-op ... so the assertion
    // below compiles away entirely". That is the design, documented before these tests were ever
    // observed to fail, and it is the right one twice over: the tracker costs a thread-local RMW
    // on every buffer-pool method, on a hit path D35 measured to be contention-bound by exactly
    // that class of per-call work; and a detector that `panic!`s in production would convert a
    // rare latent hang into a hard crash, which is a runtime-guard decision nobody has made.
    //
    // So these four assert the behaviour of a DEBUG-ONLY tool and are scoped to debug. The
    // release half is not left unasserted -- see `release_is_a_documented_no_op` at the end of
    // this module, which pins the other side. Change the `cfg` on `mod order` and that test
    // fails, which is the point: it forces the design decision to be made deliberately instead
    // of arriving as a side effect.

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "LOCK-ORDER INVERSION")]
    fn a_read_latch_taken_from_inside_the_pool_is_refused() {
        let l = PageLatches::new();
        let _inside = enter_pool(); // stands in for being part-way through `fetch_page`
        let _g = l.read(1); // must panic: `frame -> page latch` is the inverted order
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "LOCK-ORDER INVERSION")]
    fn a_write_latch_taken_from_inside_the_pool_is_refused() {
        let l = PageLatches::new();
        let _inside = enter_pool();
        let _g = l.write(1);
    }

    /// The other half: the CORRECT order must stay silent, or the detector is useless noise.
    #[cfg(debug_assertions)]
    #[test]
    fn latching_first_and_then_entering_the_pool_is_allowed() {
        let l = PageLatches::new();
        let _g = l.write(1); // page latch first ...
        let _inside = enter_pool(); // ... then down into the pool. This is the whole protocol.
        assert_eq!(pool_depth(), 1);
    }

    /// The depth is a counter, not a flag, because pool methods nest (`free_page` ->
    /// `delete_page`). An inner section closing must not re-open the door.
    #[cfg(debug_assertions)]
    #[test]
    fn nested_pool_sections_unwind_to_zero_and_not_below() {
        assert_eq!(pool_depth(), 0);
        {
            let _a = enter_pool();
            {
                let _b = enter_pool();
                assert_eq!(pool_depth(), 2);
            }
            assert_eq!(pool_depth(), 1, "an inner section closing must not clear the outer one");
        }
        assert_eq!(pool_depth(), 0);
        let l = PageLatches::new();
        let _g = l.read(1); // and now latching is permitted again
    }

    /// The counter is per-thread: one thread being inside the pool must not block another from
    /// latching, or the detector would fire on correct concurrent code.
    #[test]
    fn the_depth_is_per_thread() {
        let _inside = enter_pool();
        std::thread::spawn(|| {
            assert_eq!(pool_depth(), 0, "pool depth leaked across threads");
            let l = PageLatches::new();
            let _g = l.read(1);
        })
        .join()
        .unwrap();
    }

    /// The RELEASE half of the contract, so that neither profile carries an unasserted claim.
    ///
    /// The four `#[cfg(debug_assertions)]` tests above pin what the detector does when it exists.
    /// This pins what release is documented to do instead: the tracker is compiled out, so
    /// `pool_depth()` is 0 even inside a pool section, and the inverted order is NOT refused.
    ///
    /// This is deliberately not `#[should_panic]`-free by accident. If someone drops the `cfg` on
    /// `mod order` and makes the tracker real in release, this test FAILS -- which is correct.
    /// Turning a debug-time detector into a production guard that panics is a design decision
    /// about crash-versus-hang in a live database; it must be made on purpose, with the module
    /// doc updated in the same change, not arrive as a silent side effect of deleting an
    /// attribute.
    #[cfg(not(debug_assertions))]
    #[test]
    fn release_is_a_documented_no_op() {
        assert_eq!(pool_depth(), 0, "no pool section is open yet");
        let _inside = enter_pool();
        assert_eq!(
            pool_depth(),
            0,
            "release compiles the depth tracker out; if this is now nonzero the `cfg` on \
             `mod order` changed and the module doc must change with it"
        );
        // The inverted order. In debug this panics with LOCK-ORDER INVERSION; in release the
        // assertion is compiled away and the latch is simply taken. Reaching the next line at
        // all is the assertion.
        let l = PageLatches::new();
        let _g = l.read(1);
    }
}
