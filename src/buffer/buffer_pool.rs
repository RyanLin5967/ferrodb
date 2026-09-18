//! The buffer pool.
//!
//! # No pool-wide lock is held across IO, and that is the whole design
//!
//! D18/D19 closed a set of real lost-write and wrong-bytes races by making `fetch_page` hold ONE
//! lock — first the page table's write lock, then `arc_cache` — across the entire miss path. That
//! was correct and it was a wall: the lock was held across `DiskManager::read`, so every page miss
//! serialised every other thread in the process for the duration of a read syscall. Measured at 500
//! microseconds of modelled IO per read, aggregate fault throughput was 1548/s at one thread and
//! 1570/s at sixteen — **flat**, sixteen threads doing the work of one, which is the signature of a
//! lock held across the read syscall. After: 1557/s at one thread and 25248/s at sixteen, x16.22.
//! `bench/s22_bufpool_before_after.txt`, two interleaved repetitions that agree.
//!
//! An earlier draft of this paragraph said the before-curve was *negative* (1574/s falling to
//! 1409/s) and cited `bench/s22_bufpool_fault_before.txt`. That run was taken on a machine under
//! heavy and uncontrolled load from other work, and the decline did not reproduce when the two
//! arms were run interleaved. The honest before-curve is flat, not negative. Flat is already the
//! whole point, so nothing downstream of it changes — but a number that was quoted and then
//! failed to reproduce is corrected here rather than quietly dropped.
//!
//! The races those fixes closed are all of one shape: **a decision made under one lock, acted on
//! after it was released.** Holding the lock longer is one answer. The answer here is the standard
//! one, which is to keep the critical sections short and *verify the decision at the point of use*:
//!
//! * **The frame latch is the arbiter.** A page table lookup only ever produces a *candidate* frame.
//!   Every path re-checks `frame.page_id` under that frame's own `RwLock` before pinning it or
//!   reusing it. Two threads racing for one frame are serialised by that frame's lock and the loser
//!   sees a label it did not expect and retries. This is what replaces the pool-wide lock.
//! * **An in-transit marker, so faulters on the SAME page wait on THAT page.** [`BufferPoolManager::in_transit`]
//!   holds the pages with IO in flight. A thread that wants a page someone else is already reading
//!   waits on [`BufferPoolManager::transit_done`] instead of loading it a second time — which is the
//!   race that orphaned a frame and lost every write to it.
//! * **The replacement policy is consulted, not held.** `arc_cache` is taken for the verdict and
//!   dropped before any syscall. A verdict can therefore go stale, and every consumer of one treats
//!   it as a hint that must be re-verified at the frame latch.
//!
//! # Lock order
//!
//! `in_transit` -> `arc_cache` -> `page_table` -> `frame`. Every acquisition in this file respects
//! it, and so do `branch::arena::evict` and `wal::recovery`, which take the page table and then a
//! frame. Nothing acquires a pool-wide lock while holding a frame latch.
//!
//! # What IS still held across IO, on purpose
//!
//! A dirty victim's write-back runs while that victim's **frame latch** is held. That is the point
//! rather than a leftover: it blocks threads touching that one frame and nobody else. The
//! write-back also happens *before* the victim leaves the page table — see
//! [`BufferPoolManager::evict_into`] — because the other order lets a concurrent fetch of the victim
//! miss, read the stale copy from disk, and lose the dirty bytes still sitting in the frame.
//!
//! # What was considered, and why the losers lost
//!
//! Prior art first, because none of this is novel. The shape chosen here -- a per-frame latch, an
//! "IO in progress" marker that other faulters wait on, and a replacement policy that is consulted
//! and then released -- is the classic buffer manager of Gray and Reuter, and it is what
//! PostgreSQL's `bufmgr.c` does with its `BM_IO_IN_PROGRESS` flag, its per-buffer IO condition
//! variable, and its partitioned buffer mapping table. LeanStore and Umbra reach the same place by
//! a different route: one atomic state word per page (Unlocked / Shared / Exclusive / Marked /
//! Evicted) plus optimistic version validation, which is the same "verify the decision at the
//! point of use" rule expressed as a CAS rather than as a re-check under a latch.
//!
//! **CHOSEN: per-frame latch + `in_transit` marker + consult-don't-hold policy.** The critical
//! sections are all bounded by memory accesses rather than by syscalls. It keeps ARC exactly as it
//! is, so eviction QUALITY is untouched, and it is the option whose failure modes were already
//! understood here -- every race D18 and D19 fixed is of the form "decision made under one lock,
//! acted on after it was released", and the answer is to re-verify at the point of use rather than
//! to hold the lock longer.
//!
//! **REJECTED: shard `arc_cache` into N independent caches keyed by page id.** This is the first
//! thing anyone suggests and it is wrong for this structure. ARC's adaptivity is *global by
//! construction*: the `p` parameter and the b1/b2 ghost lists are how it decides whether the
//! workload is recency- or frequency-biased. Sharding produces N caches of capacity/N, each
//! adapting to a 1/N sample, and a hot shard cannot borrow a frame from a cold one. The cost is
//! paid in hit RATE, which does not appear in a throughput benchmark at all -- so this option
//! could have been shipped, measured as "faster", and been a regression. It also does not fix the
//! thing this change was for: a shard lock is still held across the read syscall.
//!
//! **REJECTED: replace ARC with CLOCK/second-chance.** This would make the hit path a single
//! atomic store of a reference bit and genuinely fix the arm that is still broken (below). It
//! loses because it is a replacement-policy decision wearing a concurrency fix's clothes. The
//! engine implements ARC deliberately, ghost lists and all; swapping it for CLOCK should be
//! decided on hit-rate evidence against real workloads, not smuggled in because CLOCK happens to
//! have a cheaper hit path.
//!
//! # What this did NOT fix, measured rather than assumed
//!
//! **The hit path still takes the process-wide `arc_cache` mutex on every hit**, in `fetch_page`,
//! for `touch`. Taking IO out from under that lock does nothing for a workload whose working set
//! fits in the pool, and `bench/s22_bufpool_before_after.txt` shows exactly that: with the working
//! set resident, throughput relative to one thread goes x1.00 / x0.74 / x0.43 / x0.31 / x0.31 at
//! 1/2/4/8/16 threads BEFORE and x1.00 / x0.51 / x0.51 / x0.27 / x0.26 AFTER. Both collapse.
//! Single-thread throughput improved about 30%; scaling did not improve at all.
//!
//! ⛔ **CORRECTED 2026-09-18: the paragraph that stood here named BP-Wrapper as "the next
//! structural change to this file". That has since been MEASURED and it is wrong** -- not the
//! citation, which is sound, but the diagnosis it rests on. Recorded here rather than deleted,
//! because the reasoning was reasonable and the next reader will otherwise re-derive it.
//!
//! BP-Wrapper (Ding, Jiang, Zhang, ICDE 2009) batches hit-path policy updates into per-thread
//! buffers and lets whichever thread next holds the policy lock drain them, so an arbitrary
//! replacement policy stops being a contention point. It is the correct standard answer **to the
//! question as posed**, and the question as posed was wrong: it assumed `arc_cache` is what holds
//! the slope.
//!
//! `bench/d35_gate_stubtouch.txt` (branch `D35-gate-stubtouch`, `ad46887`; 3 interleaved reps,
//! RESIDENT arm, parameters byte-identical to `bench/s22_bufpool_before_after.txt`) measures the
//! **upper bound** on every batching scheme by deleting `touch` from the hit path outright -- no
//! batcher can cost less than not doing the work at all. Medians, throughput at 16 threads
//! relative to 1: **BASE x0.142, `touch` deleted x0.129.** One-thread throughput rises 47% and
//! 16-thread rises 33%, and **the slope does not change sign; it gets marginally worse**, because
//! a constant-factor win lands harder at 1 thread than at 16. That is the signature of a curve
//! contention-bound by something else. **BP-Wrapper here is a constant, not a shape.**
//!
//! ⛔ **THE SENTENCE ABOVE IS RETRACTED — 2026-09-18, see the 2x2 below.** The measurement it
//! rests on is sound; the inference is not. That STUB arm deleted `touch` **with the page table
//! still on the hit path**, so it could not have shown a policy win even if one existed. It is
//! left standing rather than deleted because the next reader will otherwise re-derive it from the
//! same real numbers.
//!
//! What actually holds the slope was in this file the whole time. The resident hit loop is
//! `fetch_page` + `unpin_page`, and between them each iteration takes `page_table.read()` twice,
//! `frames[i].read()` twice, and two atomic RMWs on the pin counter -- **four RwLock read
//! acquisitions before `touch` is even reached.** A Rust `RwLock`'s reader count is one
//! process-wide cache line that every reader atomically RMWs; it contends exactly like a mutex,
//! it is simply not spelled `Mutex`. The `arc_cache` mutex was one synchronisation point out of
//! five, which is why removing it bought 33% and nothing else.
//!
//! # ⛔ TWO SERIALISED WALLS, AND EVERY EARLIER SINGLE-ARM READING OF THEM WAS WRONG
//!
//! A paragraph here once named **taking the page table off the hit path** as "the candidate that
//! measures as a shape change", on the strength of a gate arm reporting x0.936. **Both that claim
//! and the BP-Wrapper claim above are retracted, and they fall to the same confound.**
//! `bench/d35_c1_factorial.txt` (branch `ferrodb-D35-C1-pagetable`, `c0d07d2`) settles it by
//! running all four cells instead of arguing -- same harness, same parameters, interleaved,
//! 3 reps, per-rep slopes agreeing to within 0.01. Throughput at 16 threads relative to 1:
//!
//! | | `touch` KEPT | `touch` DELETED |
//! |---|---|---|
//! | **no mirror** | BASE x0.111 | STUB x0.080 |
//! | **mirror** | C1 x0.121 | **C1STUB x0.579**, rising monotonically 2T->16T |
//!
//! ⭐ **Those are the ROTATED numbers, and the rotation is load-bearing — WAITING FOR A QUIET
//! MACHINE WOULD NOT HAVE BEEN ENOUGH.** The first run of this factorial interleaved arms but kept
//! their ORDER fixed, and loadavg drifted monotonically upward across it, so `C1STUB` sat at the
//! highest load in every rep: a systematic POSITION bias, not noise. **Interleaving cancels only a
//! bias that is constant in TIME; on this machine drift is the normal case.** The re-run rotates
//! the four arms through a Latin square so each occupies each position exactly once, and the
//! summariser now prints 1T throughput BY POSITION as the check — 38.4M / 38.8M / 38.6M / 38.6M,
//! flat to within 1%. Load still rose 3.72 -> 13.14 DURING the re-run, with the suite lock free and
//! no `cargo` on the process table, which is the proof that quiet was never the fix.
//! Superseded raw data is kept, not deleted: `bench/d35_c1_factorial_SUPERSEDED_rising_load.txt`
//! (biased against C1STUB) and `bench/d35_c1_pagetable_SUPERSEDED_fixed_order.txt` (against C1).
//! Every cell moved by at most 0.015 and the ordering is identical — which is a RESULT of the
//! re-run, not a reason it could have been skipped.
//!
//! Three cells collapse; only the fourth rises. **`touch` and the page table are two serialising
//! points IN SERIES**, so removing either alone leaves the other binding. That is why STUB came
//! out marginally worse than BASE, and why the gate's C1 arm looked like a shape change: it was
//! built ON TOP OF its STUB arm, so it had BOTH removed and credited one cause with a two-cause
//! effect. The two agree where they should -- gate C1 at 16 threads 44.9M, this C1STUB 44.9M.
//!
//! ⭐ **The transferable form, which is worth more than the instance: an arm that removes one of
//! two SERIALISED walls measures the OTHER wall, not the one it removed. Two "no effect" results
//! in series are not evidence that neither is a wall.**
//!
//! **So neither half is a shape change alone.** A page-table mirror that RETAINS `touch` -- the
//! only version that can ship -- is x0.121 against a pre-registered x0.5 bar: a 1.2-1.5x constant,
//! the same order as the constant BP-Wrapper was rejected for being. It is **not** in this tree
//! for that reason; it is on `ferrodb-D35-C1-pagetable` at `c0d07d2`, certified green, waiting to
//! be built into the pair.
//!
//! **The candidate is the PAIR** -- the page table off the hit path **plus** a batched,
//! ARC-preserving `touch` -- and it is judged against a **x0.579 ceiling, not against zero**. A
//! scheme landing at x0.20 has recovered a sixth of the available headroom and is a MICRO wearing
//! a shape change's clothes. See `SCALE-DESIGN.md` D44 for the pre-registered falsifiers.
//!
//! ⛔ **`C1STUB` is a scaffold and can never ship.** Deleting `touch` degrades ARC's recency, and
//! that is measured, not assumed: `bench/d35_c1_evictiontrace.txt` shows the eviction sequence
//! changes outright, 7899 -> 7896 evictions in a different order. Reproduce it with
//! `examples/d35_eviction_trace.rs`, which is in this tree.
//!
//! ⚠ **And if the pair cannot clear x0.5, the diagnosis in this comment is wrong** -- the wall is
//! then the four `RwLock` read acquisitions plus two atomic RMWs per hit named above, and the
//! answer is a different data structure, not a lock fix.
//!
//! Quote the SLOPE, never a multiplier to more than two significant figures: medians are over
//! 3 reps with a BASE spread of 18.1M-23.6M at one thread under loadavg 16-33, and the two reps of
//! `s22_bufpool_before_after.txt` disagree 40% on multipliers under fleet load. What reproduces in
//! every rep is the sign of the slope.
//!
//! ⚠ The `buffer_pool.rs` edits on `D35-gate-stubtouch` are a MEASUREMENT SCAFFOLD and must never
//! be merged: they carry a `FERRO_D35_ARM` switch, a deleted `touch`, and a mirror sized for a
//! benchmark.
use std::sync::{Arc, OnceLock};
use std::sync::atomic::AtomicBool;
use std::sync::{Condvar, Mutex, atomic::AtomicU16, atomic::AtomicUsize};
use std::collections::{HashMap, HashSet};
use crate::error::FerroError;
use crate::storage::disk_manager::{DiskManager, PAGE_SIZE};
use crate::buffer::arc::ArcCache;
use crate::wal::log::WalManager;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;
use crate::buffer::arc::ArcResult;
use crate::storage::page_latch::{PageLatches, PoolSection, enter_pool};

pub struct Frame {
    pub data: [u8; PAGE_SIZE],
    pub page_id: Option<u32>,
    pub pin_counter: AtomicU16,
    pub dirty_flag: AtomicBool,
}

/// What happened when the pool tried to take a replacement victim's frame.
///
/// The distinction between the last two is load-bearing and was missing at first. The replacement
/// policy removes a victim from its resident lists at the moment it names it, so when the pool
/// cannot take that victim it has to say WHY: a victim that is still resident must be put back
/// (`Declined`), and a victim that has genuinely gone must not be (`Gone`) — reinstating that one
/// would leave the policy believing a page is resident when no frame holds it.
enum Evicted {
    /// The frame is taken, relabelled for the incoming page, and pinned once.
    Took(usize),
    /// The victim is still resident and still that page; it was pinned or re-dirtied in the
    /// write-back window. The policy must keep counting it.
    Declined,
    /// The victim is no longer resident under that id at all — deleted, or its frame already
    /// reused. The policy is right to have dropped it.
    Gone,
}

pub struct BufferPoolManager {
    pub frames: Vec<RwLock<Frame>>,
    pub page_table: RwLock<HashMap<u32, usize>>, // page_id -> frame index
    pub disk_manager: Arc<DiskManager>,
    pub arc_cache: Mutex<ArcCache>,
    pub wal: OnceLock<Arc<WalManager>>,

    /// Pages with IO in flight: being read in, or being written back out of a frame.
    ///
    /// **This is what stops two threads loading one page into two frames.** That race orphaned a
    /// frame — the page table resolved the page to the other one — and every write that landed in
    /// the orphan was silently lost. The old code prevented it by holding a pool-wide lock across
    /// the whole miss path; this prevents it by making the second thread wait on *this page*, which
    /// leaves every thread faulting a different page free to proceed.
    ///
    /// A page is in this set for the whole of its fault, INCLUDING the error paths — see
    /// `fetch_page`, where the removal is unconditional. A page left here by a failed load would
    /// hang every later fetch of it forever.
    in_transit: Mutex<HashSet<u32>>,
    /// Signalled whenever a page leaves [`BufferPoolManager::in_transit`].
    transit_done: Condvar,

    /// Where the next free-frame scan starts. A hint, never trusted: the scan re-checks every frame
    /// under its own write lock, so a stale hint costs a step and cannot hand out a taken frame.
    free_hint: AtomicUsize,

    /// **Page latches, and they are ABOVE this struct's own locks in the order.**
    ///
    /// They live here rather than on `BPlusTreeManager` because a manager is opened per statement
    /// — `planner::plan::open_table` builds a fresh one on every INSERT — so latch state held by a
    /// manager would be private to one statement and exclude nothing. The buffer pool is the only
    /// thing two concurrent users of the same tree share.
    ///
    /// **Nothing in this file may take one.** The order is
    /// `page_latch -> in_transit -> arc_cache -> page_table -> frame`, and taking a page latch from
    /// underneath any of those would invert it. See `src/storage/page_latch.rs`.
    ///
    /// D23 gave the reason as "`fetch_page` holds `arc_cache` across frame locks", and S22 made
    /// that false — the cache is now consulted and released, and nothing pool-wide is held across
    /// the read syscall. The ORDER survived the change; that particular justification for it did
    /// not, and `page_latch.rs` says so itself: the contract this layer needs is only that the pool
    /// never reaches UP for a page latch. Crabbing calls downward, and a downward call cannot close
    /// a cycle unless the callee calls back up.
    pub page_latches: PageLatches,
}

const MAX_BUFFER_POOL_PAGES: usize = 1024;

/// How many times `fetch_page` will re-verify before giving up.
///
/// Every retry in `fetch_page` follows a *verified* change of state — a frame relabelled under its
/// latch, a page that arrived while this thread was looking elsewhere, a victim that was pinned out
/// from under the policy — so each one follows real progress by some thread. The bound exists so
/// that a bug cannot express itself as a hang: a wedged buffer pool is harder to diagnose than one
/// that returns an error naming the invariant it could not satisfy.
const FETCH_ATTEMPTS: usize = 128;

/// A frame lock guard that also records, for its whole lifetime, that this thread is holding a
/// buffer-pool lock.
///
/// This exists so the lock-order assertion in `src/storage/page_latch.rs` can SEE a frame lock.
/// `self.frames[i].read()` is invisible to it; `self.frame_read(i)` is not. The two modules that
/// take page latches — `src/storage/index.rs` and `src/storage/range_scan.rs` — must go through
/// here, because they are the only ones that can produce the `frame -> page latch` inversion.
/// Everywhere else may keep indexing `frames` directly: those modules take no page latch, and
/// `tests/lock_order_allowlist.rs` fails if that ever stops being true.
///
/// Derefs to `Frame`, so it is a drop-in for the guard it wraps.
pub struct FrameGuard<G> {
    // Declared first so the frame lock is released BEFORE the pool section closes.
    guard: G,
    _pool: PoolSection,
}

impl<G: Deref<Target = Frame>> Deref for FrameGuard<G> {
    type Target = Frame;
    fn deref(&self) -> &Frame {
        &self.guard
    }
}

impl<G: DerefMut<Target = Frame>> DerefMut for FrameGuard<G> {
    fn deref_mut(&mut self) -> &mut Frame {
        &mut self.guard
    }
}

impl BufferPoolManager {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self{
        let frames: Vec<RwLock<Frame>> = (0..MAX_BUFFER_POOL_PAGES).map(|_| RwLock::new(Frame::new())).collect();
        BufferPoolManager {
            frames,
            page_table: RwLock::new(HashMap::new()),
            disk_manager,
            arc_cache: Mutex::new(ArcCache::new(MAX_BUFFER_POOL_PAGES)),
            wal: OnceLock::new(),
            in_transit: Mutex::new(HashSet::new()),
            transit_done: Condvar::new(),
            free_hint: AtomicUsize::new(0),
            page_latches: PageLatches::new(),
        }
    }

    /// Read-lock a frame, tracked for lock ordering. See [`FrameGuard`].
    pub fn frame_read(&self, frame_i: usize) -> FrameGuard<RwLockReadGuard<'_, Frame>> {
        let _pool = enter_pool();
        FrameGuard { guard: self.frames[frame_i].read().unwrap(), _pool }
    }

    /// Write-lock a frame, tracked for lock ordering. See [`FrameGuard`].
    pub fn frame_write(&self, frame_i: usize) -> FrameGuard<RwLockWriteGuard<'_, Frame>> {
        let _pool = enter_pool();
        FrameGuard { guard: self.frames[frame_i].write().unwrap(), _pool }
    }

    /// Return the frame holding `page_id`, faulting it in if it is not resident, and pin it.
    ///
    /// **No pool-wide lock is held across the disk read.** The module comment gives the reasoning;
    /// the shape is: try to pin what is already resident, and if that fails claim the exclusive
    /// right to fault this one page in, do the IO holding nothing, and publish. A thread whose page
    /// someone else is already reading waits on that page rather than on the pool.
    pub fn fetch_page(&self, page_id: u32) -> Result<usize, FerroError>{
        // Lock-order: this method takes the pool's locks, so page latches are forbidden from
        // here down. The thread-local depth counter is re-entrant, so the nested `enter_pool` in
        // `new_page -> fetch_page` is fine. See src/storage/page_latch.rs.
        let _pool = enter_pool();
        for _attempt in 0..FETCH_ATTEMPTS {
            // ---- 1. Already resident? Verified at the frame latch, no pool-wide lock held. ----
            if let Some(frame_i) = self.try_pin_resident(page_id) {
                // Policy only, and deliberately *after* the pin: the cache is a hint about what to
                // evict next, and holding it here is what used to serialise even pure cache hits.
                self.arc_cache.lock().unwrap().touch(page_id);
                return Ok(frame_i);

            }

            // ---- 2. Claim the right to fault it in, or wait for whoever already holds it. ----
            let mut transit = self.in_transit.lock().unwrap();
            if transit.contains(&page_id) {
                // Someone is reading exactly this page. Wait on THIS page; a thread faulting any
                // other page is not blocked by this and never touches this lock for long.
                // The guard comes back from `wait` and is dropped here, which is what releases the
                // transit lock before this thread loops round to re-read the page table.
                let _woken = self.transit_done.wait(transit).unwrap();
                continue;
            }
            // Re-check residency under the transit lock. Between step 1 and here a loader can have
            // published and left the set, and without this check this thread would load a page that
            // is already resident into a second frame — the orphaned-frame lost write.
            if self.page_table.read().unwrap().contains_key(&page_id) {
                drop(transit);
                continue;
            }
            transit.insert(page_id);
            drop(transit);

            // ---- 3. The fault itself. The only IO in the pool that is not under a frame latch. --
            let outcome = self.fault_in(page_id);

            // ---- 4. Leave the transit set and wake the waiters. UNCONDITIONAL. ----
            // A page left in the set by a failed load would hang every later fetch of it, which is
            // a worse failure than the one that put it there.
            {
                let mut transit = self.in_transit.lock().unwrap();
                transit.remove(&page_id);
            }
            self.transit_done.notify_all();

            match outcome {
                Ok(Some(frame_i)) => return Ok(frame_i),
                Ok(None) => continue, // a verified change of state; re-decide from the top
                Err(e) => return Err(e),
            }
        }
        Err(FerroError::Internal(format!(
            "buffer pool: {FETCH_ATTEMPTS} verified retries for page {page_id} without resolving \
             it. Each retry follows a real state change, so this means the pool is thrashing \
             rather than that it is wedged."
        )))
    }

    /// Pin `page_id` if it is resident **and the frame still holds it**.
    ///
    /// The page table lookup on its own is not enough, and this is the single most important line
    /// in the change that removed the pool-wide lock: the frame a lookup names can be handed to
    /// another page between the lookup and the pin. Re-checking `frame.page_id` under that frame's
    /// own lock makes the pin and the check one step against the evictor, which takes the same
    /// frame's write lock to relabel it. One of the two wins; the loser sees a label it did not
    /// expect and retries.
    fn try_pin_resident(&self, page_id: u32) -> Option<usize> {
        // Lock-order: takes a pool lock, so page latches are forbidden from here down. Reachable
        // only from `fetch_page`, which already opens a section -- the marker is here anyway
        // because the depth counter is re-entrant, so it costs nothing, and because a guard that
        // depends on the CALLER having opened it stops holding the moment someone adds a caller.
        // See src/storage/page_latch.rs.
        let _pool = enter_pool();
        let frame_i = self.page_table.read().unwrap().get(&page_id).copied()?;
        let frame = self.frames[frame_i].read().unwrap();
        if frame.page_id != Some(page_id) {
            return None;
        }
        frame.pin_counter.fetch_add(1, Ordering::Relaxed);
        Some(frame_i)
    }

    /// Is `page_id` pinned? The predicate the replacement policy uses to skip a victim.
    ///
    /// A page the table cannot resolve is one being faulted in right now, or one the cache and the
    /// table briefly disagree about. It has no frame to reclaim, so **"not evictable" is the only
    /// safe answer**. This used to index the table with `[]` and panic the process on exactly that
    /// case; under the old pool-wide lock it was unreachable, and without one it is ordinary.
    fn is_pinned(&self, page_id: u32) -> bool {
        // Lock-order: takes a pool lock, so page latches are forbidden from here down. Reachable
        // only from `fetch_page`, which already opens a section -- the marker is here anyway
        // because the depth counter is re-entrant, so it costs nothing, and because a guard that
        // depends on the CALLER having opened it stops holding the moment someone adds a caller.
        // See src/storage/page_latch.rs.
        let _pool = enter_pool();
        let Some(frame_i) = self.page_table.read().unwrap().get(&page_id).copied() else {
            return true;
        };
        self.frames[frame_i].read().unwrap().pin_counter.load(Ordering::Relaxed) > 0
    }

    /// Load `page_id` into a frame and publish it. The caller holds the transit claim for it.
    ///
    /// `Ok(Some(i))` published and pinned once. `Ok(None)` the world changed under a stale verdict
    /// and the caller should re-decide — never an error, because a stale verdict is the expected
    /// cost of not holding the cache lock across the IO.
    fn fault_in(&self, page_id: u32) -> Result<Option<usize>, FerroError> {
        // Lock-order: takes a pool lock, so page latches are forbidden from here down. Reachable
        // only from `fetch_page`, which already opens a section -- the marker is here anyway
        // because the depth counter is re-entrant, so it costs nothing, and because a guard that
        // depends on the CALLER having opened it stops holding the moment someone adds a caller.
        // See src/storage/page_latch.rs.
        let _pool = enter_pool();
        // The verdict. `arc_cache` is held for this and dropped before any syscall.
        let verdict = {
            let mut cache = self.arc_cache.lock().unwrap();
            cache.request(page_id, &|id| self.is_pinned(id))
        };

        let frame_i = match verdict {
            ArcResult::Hit => {
                // The cache says resident; step 1 and step 2 both just established it is not, and
                // step 2 held the transit lock while checking, so no loader can have published
                // since. The cache is carrying a false claim -- drop it and re-decide rather than
                // trusting either side.
                let _ = self.arc_cache.lock().unwrap().remove(page_id);
                return Ok(None);
            }
            ArcResult::PoolFull => return Err(FerroError::NotEnoughSpace),
            ArcResult::MissNoEvict => match self.claim_free_frame(page_id) {
                Some(i) => i,
                None => {
                    // Every frame filled between the verdict and the scan. Real under concurrency.
                    let _ = self.arc_cache.lock().unwrap().remove(page_id);
                    return Err(FerroError::NotEnoughSpace);
                }
            },
            ArcResult::MissEvict(victim) => {
                // Both the abandon paths and the error path must hand the victim back to the
                // policy, or it stops counting a page that is still in the pool and that page's
                // frame is never reclaimable again.
                let outcome = self.evict_into(victim, page_id);
                let give_back = |declined: bool| {
                    let mut cache = self.arc_cache.lock().unwrap();
                    if declined {
                        cache.reinstate(victim);
                    }
                    // The incoming page was moved into the resident lists by `request` and is not
                    // going to arrive, so that claim comes off too.
                    let _ = cache.remove(page_id);
                };
                match outcome {
                    Ok(Evicted::Took(i)) => i,
                    Ok(Evicted::Declined) => {
                        give_back(true);
                        return Ok(None);
                    }
                    Ok(Evicted::Gone) => {
                        give_back(false);
                        return Ok(None);
                    }
                    Err(e) => {
                        // The write-back failed, so the victim is untouched and still resident.
                        give_back(true);
                        return Err(e);
                    }
                }
            }
        };

        // The read. **Nothing pool-wide is held here, and neither is the frame latch**: the frame
        // is already labelled `page_id` and pinned, so no scan will claim it and no evictor will
        // take it. This is the syscall that used to serialise the whole process.
        let data = match self.disk_manager.read(page_id) {
            Ok(d) => d,
            Err(e) => {
                // The commonest way to get here is asking for a page past the end of the file,
                // which is exactly what probing whether a page exists does. Undo both halves of the
                // claim: the frame, and the cache's belief that this page is now resident. Leaving
                // the latter is what made a second probe of an absent page panic.
                self.release_frame(frame_i);
                let _ = self.arc_cache.lock().unwrap().remove(page_id);
                return Err(e);
            }
        };

        // Bytes first, mapping second. A reader that reached this frame through the page table must
        // never find the label already updated and the bytes not yet — that is serving another
        // page's contents, which is the one failure a storage engine cannot apologise for.
        self.frames[frame_i].write().unwrap().data = data;
        self.page_table.write().unwrap().insert(page_id, frame_i);
        Ok(Some(frame_i))
    }

    /// Take an unused frame and label it for `incoming`.
    ///
    /// **The claim is the write lock plus the label.** Checking `page_id.is_none()` and setting it
    /// happen under one acquisition of that frame's lock, so two threads scanning at once cannot
    /// both take it — the original bug here probed under a read lock and re-acquired a write lock,
    /// and both threads wrote a different page into the same frame.
    fn claim_free_frame(&self, incoming: u32) -> Option<usize> {
        // Lock-order: takes a pool lock, so page latches are forbidden from here down. Reachable
        // only from `fetch_page`, which already opens a section -- the marker is here anyway
        // because the depth counter is re-entrant, so it costs nothing, and because a guard that
        // depends on the CALLER having opened it stops holding the moment someone adds a caller.
        // See src/storage/page_latch.rs.
        let _pool = enter_pool();
        let n = self.frames.len();
        let start = self.free_hint.load(Ordering::Relaxed) % n;
        for step in 0..n {
            let i = (start + step) % n;
            let mut frame = self.frames[i].write().unwrap();
            if frame.page_id.is_none() {
                frame.page_id = Some(incoming);
                frame.pin_counter = AtomicU16::new(1);
                frame.dirty_flag = AtomicBool::new(false);
                self.free_hint.store((i + 1) % n, Ordering::Relaxed);
                return Some(i);
            }
        }
        None
    }

    /// Give up the frame claimed for a load that failed.
    ///
    /// `page_id = None` is what makes a frame free, and the data is zeroed so a free frame never
    /// holds a readable copy of a page nothing points at.
    fn release_frame(&self, frame_i: usize) {
        // Lock-order: takes a pool lock, so page latches are forbidden from here down. Reachable
        // only from `fetch_page`, which already opens a section -- the marker is here anyway
        // because the depth counter is re-entrant, so it costs nothing, and because a guard that
        // depends on the CALLER having opened it stops holding the moment someone adds a caller.
        // See src/storage/page_latch.rs.
        let _pool = enter_pool();
        let mut frame = self.frames[frame_i].write().unwrap();
        frame.page_id = None;
        frame.data = [0u8; PAGE_SIZE];
        frame.pin_counter = AtomicU16::new(0);
        frame.dirty_flag = AtomicBool::new(false);
    }

    /// Evict `victim` and hand its frame to `incoming`, labelled and pinned.
    ///
    /// All three outcomes are ordinary under concurrency and none of them means "force it":
    ///
    /// * [`Evicted::Took`] — the frame is taken, relabelled and pinned.
    /// * [`Evicted::Declined`] — the victim was pinned or re-dirtied in the write-back window. It
    ///   is **still resident**, so the caller must hand it back to the replacement policy.
    /// * [`Evicted::Gone`] — the victim is no longer resident under that id at all. The caller must
    ///   **not** hand it back.
    ///
    /// This used to be one `Ok(None)` covering all three refusals, and collapsing them leaked a
    /// frame on every declined eviction: the policy names a victim by removing it from its resident
    /// lists, so a victim it was never told to take back is one no future eviction can choose.
    ///
    /// # The ordering that matters
    ///
    /// The write-back happens **while the victim is still in the page table**. The other order is
    /// the tempting one — drop the mapping, then flush at leisure — and it silently loses writes: a
    /// concurrent `fetch_page(victim)` would miss, read the stale copy from disk, and the dirty
    /// bytes still sitting in this frame would go to disk afterwards or not at all.
    fn evict_into(&self, victim: u32, incoming: u32) -> Result<Evicted, FerroError> {
        // Lock-order: takes a pool lock, so page latches are forbidden from here down. Reachable
        // only from `fetch_page`, which already opens a section -- the marker is here anyway
        // because the depth counter is re-entrant, so it costs nothing, and because a guard that
        // depends on the CALLER having opened it stops holding the moment someone adds a caller.
        // See src/storage/page_latch.rs.
        let _pool = enter_pool();
        // A candidate, not an answer: the frame latch below decides whether it is still true.
        let Some(frame_i) = self.page_table.read().unwrap().get(&victim).copied() else {
            // `delete_page` or `free_page` removed the victim between the verdict and here.
            return Ok(Evicted::Gone);
        };

        // Write-back under the victim's OWN latch. A read lock is what keeps `data` stable -- a
        // writer needs the write lock -- and it is the same lock discipline `flush_page` uses. IO
        // under a per-frame latch is the design: it blocks threads touching this frame and nobody
        // else.
        {
            let frame = self.frames[frame_i].read().unwrap();
            if frame.page_id != Some(victim) {
                return Ok(Evicted::Gone);
            }
            if frame.pin_counter.load(Ordering::Relaxed) > 0 {
                return Ok(Evicted::Declined);
            }
            if frame.dirty_flag.load(Ordering::Relaxed) {
                self.wal_gate(&frame.data)?;
                self.disk_manager.write(victim, &frame.data)?;
                frame.dirty_flag.store(false, Ordering::Relaxed);
            }
        }

        // Re-verify and take it. The latch was released across the write-back, so the victim may
        // have been fetched, dirtied and unpinned again in between; `dirty` is checked as well as
        // the pin because that whole cycle can complete and leave the count back at zero.
        let mut pt = self.page_table.write().unwrap();
        let mut frame = self.frames[frame_i].write().unwrap();
        if frame.page_id != Some(victim) {
            return Ok(Evicted::Gone);
        }
        if frame.pin_counter.load(Ordering::Relaxed) > 0
            || frame.dirty_flag.load(Ordering::Relaxed)
        {
            return Ok(Evicted::Declined);
        }
        pt.remove(&victim);
        frame.page_id = Some(incoming);
        frame.pin_counter = AtomicU16::new(1);
        frame.dirty_flag = AtomicBool::new(false);
        Ok(Evicted::Took(frame_i))
    }

    // decrement pin count, if page was modified, add dirty flag
    pub fn unpin_page(&self, page_id: u32, is_dirty: bool) {
        // Lock-order: this method takes one of the pool's locks, so page latches are
        // forbidden from here down. See src/storage/page_latch.rs.
        let _pool = enter_pool();
        let pt = self.page_table.read().unwrap();
        let frame_i = pt[&page_id];
        drop(pt);

        let frame = self.frames[frame_i].read().unwrap();
        if is_dirty {
            frame.dirty_flag.store(true, Ordering::Relaxed);
        }
        let _ = frame.pin_counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
            if val > 0 {Some(val-1)} else {None}
        });
    }

    // allocate new page on disk using disk manager, load into a frame, return page id
    pub fn new_page(&self) -> Result<u32, FerroError>{
        // Lock-order: this method takes one of the pool's locks, so page latches are
        // forbidden from here down. See src/storage/page_latch.rs.
        let _pool = enter_pool();
        let page_id = self.disk_manager.allocate()?;
        self.disk_manager.write(page_id, &[0u8; PAGE_SIZE])?;
        self.fetch_page(page_id)?;
        self.unpin_page(page_id, false);
        Ok(page_id)
    }

    // writes a dirty page to disk
    pub fn flush_page(&self, page_id: u32) -> Result<(), FerroError>{
        // Lock-order: this method takes one of the pool's locks, so page latches are
        // forbidden from here down. See src/storage/page_latch.rs.
        let _pool = enter_pool();
        // The read lock is held across the frame access on purpose. Reassigning a frame requires
        // the WRITE lock, so holding this one makes "which frame holds this page" stable for the
        // duration of the flush. Dropping it first left a window in which the frame could be
        // handed to another page, and the bytes then written to disk under `page_id` were that
        // other page's — durable corruption, not a lost write.
        //
        // `wal_gate` and `DiskManager::write` take no page-table lock, so holding it here does
        // not invert the arc_cache -> page_table -> frame order used everywhere in this file.
        let pt = self.page_table.read().unwrap();

        // A page that is not resident has nothing buffered to flush. This used to index with
        // `pt[&page_id]`, which PANICS on a page another thread has already evicted — the same
        // `no entry found for key` crash D19 fixed in `fetch_page`.
        let Some(&frame_i) = pt.get(&page_id) else {
            return Ok(());
        };

        let frame = self.frames[frame_i].read().unwrap();
        if frame.dirty_flag.load(Ordering::Relaxed) {
            self.wal_gate(&frame.data)?;
            self.disk_manager.write(page_id, &frame.data)?;
            frame.dirty_flag.store(false, Ordering::Relaxed);
        }
        Ok(())
    }

    // write all dirty pages to disk
    pub fn flush_all(&self) -> Result<(), FerroError>{
        // Lock-order: this method takes one of the pool's locks, so page latches are
        // forbidden from here down. See src/storage/page_latch.rs.
        let _pool = enter_pool();
        let pt = self.page_table.read().unwrap();

        // **Ascending page id, not HashMap order, and the sort is load-bearing.**
        //
        // `page_table` is a `HashMap`, whose iteration order is seeded per instance, so this loop
        // used to write the same set of dirty pages in a *different order on every run of the same
        // program*. Nothing about a single run is wrong when it does that — the same bytes reach the
        // same offsets — but three things become impossible:
        //
        //   1. A crash cannot be reproduced. "The database broke when the 7th page write was torn"
        //      names a different page each run, so a failing crash point cannot be replayed, let
        //      alone bisected. That is the whole reason `storage::sim` exists, and it is dead weight
        //      without this line: the fault is aimed at an operation *index*.
        //   2. Two runs cannot be compared. The exit criterion "the same seed produces the same byte
        //      sequence" is unmeetable while the sequence is a function of a per-process hash seed.
        //   3. Any future ordering rule here has nothing to stand on. Write order is what decides
        //      which prefix of a flush survives a crash; a rule about it needs an order to exist.
        //
        // Ascending page id is the cheapest total order available, and it is also the friendliest to
        // a spinning disk. `wal_gate` may flush the WAL from inside this loop, so the order also
        // fixes when that happens.
        let mut pages: Vec<(u32, usize)> = pt.iter().map(|(&p, &f)| (p, f)).collect();
        pages.sort_unstable_by_key(|&(page_id, _)| page_id);

        for (page_id, frame_i) in pages {
            let frame = self.frames[frame_i].read().unwrap();
            if frame.dirty_flag.load(Ordering::Relaxed) {
                self.wal_gate(&frame.data)?;
                self.disk_manager.write(page_id, &frame.data)?;
                frame.dirty_flag.store(false,Ordering::Relaxed);
            }
        }
        Ok(())
    }

    // remove from buffer pool, deallocate on disk
    pub fn delete_page(&self, page_id: u32) -> Result<(), FerroError>{
        // Lock-order: this method takes one of the pool's locks, so page latches are
        // forbidden from here down. See src/storage/page_latch.rs.
        let _pool = enter_pool();
        let mut pt = self.page_table.write().unwrap();
        
        let frame_i = match pt.get(&page_id){
            Some(&i) => i,
            None => return Err(FerroError::KeyNotFound)
        };

        if self.frames[frame_i].read().unwrap().pin_counter.load(Ordering::Relaxed) > 0 {
            return Err(FerroError::PagePinned);
        }
        // Ask the disk manager BEFORE touching anything. `deallocate` can refuse — a page inside
        // a reserved arena region is not this allocator's to free — and evicting first would make
        // that refusal a lie: the caller gets an Err saying the free did not happen, while the
        // frame has already been zeroed and the page table entry dropped. For an unflushed dirty
        // page, memory is the only copy, so the "failed" delete is what destroys it.
        //
        // Still holding the page-table write lock, so no other thread can fault the page in
        // between the two steps. page_table -> bitmap_lock is the only order taken anywhere.
        self.disk_manager.deallocate(page_id)?;

        pt.remove(&page_id);
        drop(pt);

        let mut frame = self.frames[frame_i].write().unwrap();
        frame.page_id = None;
        frame.data = [0u8; PAGE_SIZE];
        frame.pin_counter = AtomicU16::new(0);
        frame.dirty_flag = AtomicBool::new(false);
        drop(frame);

        self.arc_cache.lock().unwrap().remove(page_id)?;
        Ok(())
    }

    pub fn free_page(&self, page_id: u32) -> Result<(), FerroError> {
        // Lock-order: this method takes one of the pool's locks, so page latches are
        // forbidden from here down. See src/storage/page_latch.rs.
        let _pool = enter_pool();
        let mut pt = self.page_table.write().unwrap();
        let resident = match pt.get(&page_id) {
            Some(&frame_i) => {
                if self.frames[frame_i].read().unwrap().pin_counter.load(Ordering::Relaxed) > 0 {
                    return Err(FerroError::PagePinned);
                }
                Some(frame_i)
            }
            None => None,
        };

        // Same ordering rule as `delete_page`: the refusable step goes first, so a refusal leaves
        // the pool exactly as it found it rather than reporting a failure it already half did.
        self.disk_manager.deallocate(page_id)?;

        if let Some(frame_i) = resident {
            pt.remove(&page_id);
            drop(pt);
            let mut frame = self.frames[frame_i].write().unwrap();
            frame.page_id = None;
            frame.data = [0u8; PAGE_SIZE];
            frame.pin_counter = AtomicU16::new(0);
            frame.dirty_flag = AtomicBool::new(false);
            drop(frame);
            self.arc_cache.lock().unwrap().remove(page_id)?;
        }
        Ok(())
    }

    /// Drop every cached page **without writing any of them back**.
    ///
    /// For F6: a snapshot install replaces the page file underneath this pool, so every frame it
    /// holds describes a database that no longer exists. Writing one back — which is what
    /// [`BufferPoolManager::flush_all`] would do, and what the eviction path does on its own —
    /// puts a page of the old database into the middle of the new one, and every such page still
    /// passes its checksum. So this is the one operation that must *not* flush.
    ///
    /// **Refuses whole rather than in part** if any frame is pinned. A pinned frame has a live
    /// reader holding an index into it; dropping the page underneath that reader would hand it
    /// another database's bytes with no error anywhere. The caller's answer to a refusal is to
    /// stop its readers, not to retry.
    pub fn invalidate_all(&self) -> Result<(), FerroError> {
        // Lock-order: this method takes the pool's locks, so page latches are forbidden from
        // here down. See src/storage/page_latch.rs.
        let _pool = enter_pool();
        // **`arc_cache` BEFORE `page_table`,** which is the module's order: `in_transit ->
        // arc_cache -> page_table -> frame`. Taking them the other way round here — which the
        // first version of this function did — is a lock inversion against the one path every read
        // in the database goes through, and the two deadlock: a thread holding the cache and
        // blocking on the table, this holding the table and blocking on the cache. `fault_in`
        // still takes the cache and then the table underneath it, inside `is_pinned`, so the
        // inversion is live and not historical.
        //
        // This paragraph used to say "`fetch_page` holds the cache across the whole of its work",
        // which was true of the pool this function was written against and stopped being true when
        // that lock came off the IO path. The ORDER survived the change; the reason given for it
        // had not, and a stale reason is worse than none because it reads as current.
        //
        // `delete_page` and `free_page` avoid the question by dropping the table lock before
        // touching the cache; this takes both in the module's order instead, because it has to
        // hold them across the whole sweep.
        // **`in_transit` first, and it is held for the whole sweep.** A page being faulted in owns
        // no frame yet between the policy's verdict and its claim, so the pin scan below cannot see
        // it — and that thread would publish a page of the OLD database into the table after this
        // function had reported everything dropped. Refusing while any fault is in flight is what
        // makes "refuses whole rather than in part" true against a concurrent fetch, and holding it
        // for the duration is what stops a new fault starting mid-sweep.
        let transit = self.in_transit.lock().unwrap();
        if !transit.is_empty() {
            return Err(FerroError::PagePinned);
        }

        let mut cache = self.arc_cache.lock().unwrap();
        let mut pt = self.page_table.write().unwrap();

        // Every frame is checked before any is touched: a partial invalidation leaves the pool
        // holding some pages of the old database and some of the new, which is worse than either.
        for frame in &self.frames {
            if frame.read().unwrap().pin_counter.load(Ordering::Relaxed) > 0 {
                return Err(FerroError::PagePinned);
            }
        }

        // **Collected, then cleared — never drained with a `?` inside the loop.** A `?` mid-drain
        // returns while `Drain`'s destructor goes on emptying the map, so the table would come back
        // empty while the frames it had named still carried `page_id = Some(..)`. Those frames are
        // then invisible to the free-frame scan in `fetch_page` and leak for the life of the
        // process, and the cache and the table disagree about every page they held. Nothing below
        // this line can fail, so "refuse whole rather than in part" is true of the whole function
        // and not only of the pinned case.
        let resident: Vec<(u32, usize)> = pt.iter().map(|(id, i)| (*id, *i)).collect();
        pt.clear();
        for (page_id, frame_i) in resident {
            let mut frame = self.frames[frame_i].write().unwrap();
            frame.page_id = None;
            frame.data = [0u8; PAGE_SIZE];
            frame.pin_counter = AtomicU16::new(0);
            frame.dirty_flag = AtomicBool::new(false);
            drop(frame);
            // The cache and the table are being emptied together, so a page the table no longer
            // names cannot be `remove`d "wrongly" — an error here would say the cache had already
            // forgotten it, which is the state being aimed at. Ignored rather than propagated, so
            // that no early return can leave the two half-cleared.
            let _ = cache.remove(page_id);
        }
        Ok(())
    }

    pub fn attach_wal(&self, wal: Arc<WalManager>) {
        let _ = self.wal.set(wal);
    }

    fn wal_gate(&self, data: &[u8; PAGE_SIZE]) -> Result<(), FerroError> {
        if let Some(wal) = self.wal.get() {
            let plsn = page_lsn_of(data);
            if plsn > 0 {
                wal.flush_up_to(plsn)?;
            }
        }
        Ok(())
    }
}

fn page_lsn_of(data: &[u8; PAGE_SIZE]) -> u64 {
    match data[0] {
        0 => u64::from_be_bytes(data[11..19].try_into().unwrap()),
        2 | 3 => u64::from_be_bytes(data[5..13].try_into().unwrap()),
        _ => 0,
    }
}

impl Frame {
    pub fn new() -> Self {
        Frame {data: [0u8; PAGE_SIZE], page_id: None, pin_counter: AtomicU16::new(0), dirty_flag: AtomicBool::new(false)}
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk_manager::DiskManager;
    use std::fs::OpenOptions;
    use std::sync::Arc;

    /// A pool over a real file, with an arena floor registered so `deallocate` will refuse.
    ///
    /// `tag` must be unique per test. These run as threads in one binary, so a filename keyed
    /// only on the pid is shared, and one test's `remove_file`-then-create races another's open.
    fn pool_with_arena_floor(tag: &str) -> (Arc<BufferPoolManager>, u32, std::path::PathBuf) {
        let path = std::env::temp_dir()
            .join(format!("ferro-bp-partialfree-{}-{}.db", std::process::id(), tag));
        let _ = std::fs::remove_file(&path);
        let file = OpenOptions::new().create(true).read(true).write(true)
            .open(&path).unwrap();
        let dm = Arc::new(DiskManager::new(file).unwrap());
        let bp = Arc::new(BufferPoolManager::new(dm));
        // Hand out a few pages, then declare everything from `floor` up to be arena-owned. The
        // pages already handed out above the floor are exactly the ones `deallocate` must refuse.
        let mut pages = Vec::new();
        for _ in 0..6 {
            pages.push(bp.new_page().unwrap());
        }
        let floor = pages[2];
        bp.disk_manager.reserve_from(floor).unwrap();
        (bp, pages[4], path) // pages[4] is above the floor -> deallocate refuses it
    }

    /// S6. `delete_page` evicted the frame before asking the disk manager, so a refusal came back
    /// as `Err` *after* the page had already been dropped from the pool — and an unflushed dirty
    /// page's contents went with it. The error says the free did not happen; the pool disagreed.
    #[test]
    fn a_refused_delete_leaves_the_page_intact_in_the_pool() {
        let (bp, page, path) = pool_with_arena_floor("delete");

        // Dirty the page and do NOT flush: memory is now the only copy of this byte.
        let frame_i = bp.fetch_page(page).unwrap();
        bp.frames[frame_i].write().unwrap().data[100] = 0xAB;
        bp.unpin_page(page, true);

        let err = bp.delete_page(page);
        assert!(err.is_err(), "precondition: the arena floor must make this deallocate refuse");

        // The delete was refused, so nothing about the page may have changed.
        let pt = bp.page_table.read().unwrap();
        let still_resident = pt.get(&page).copied();
        drop(pt);
        assert!(
            still_resident.is_some(),
            "delete_page reported failure but dropped page {} from the page table",
            page
        );
        let i = still_resident.unwrap();
        assert_eq!(
            bp.frames[i].read().unwrap().data[100], 0xAB,
            "delete_page reported failure but zeroed the frame, destroying the only copy"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Same contract for `free_page`, which has the same evict-then-ask ordering.
    #[test]
    fn a_refused_free_page_leaves_the_page_intact_in_the_pool() {
        let (bp, page, path) = pool_with_arena_floor("free");

        let frame_i = bp.fetch_page(page).unwrap();
        bp.frames[frame_i].write().unwrap().data[100] = 0xCD;
        bp.unpin_page(page, true);

        let err = bp.free_page(page);
        assert!(err.is_err(), "precondition: the arena floor must make this deallocate refuse");

        let pt = bp.page_table.read().unwrap();
        let still_resident = pt.get(&page).copied();
        drop(pt);
        assert!(
            still_resident.is_some(),
            "free_page reported failure but dropped page {} from the page table",
            page
        );
        assert_eq!(
            bp.frames[still_resident.unwrap()].read().unwrap().data[100], 0xCD,
            "free_page reported failure but zeroed the frame"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Control: when the disk manager does NOT refuse, the page really is evicted. Without this
    /// the two tests above would pass against a `delete_page` that simply never did anything.
    #[test]
    fn an_accepted_delete_still_evicts_the_page() {
        let (bp, _refused, path) = pool_with_arena_floor("accepted");
        let below = 1u32; // below the floor, so the deallocate is allowed
        bp.fetch_page(below).unwrap();
        bp.unpin_page(below, false);
        assert!(bp.page_table.read().unwrap().contains_key(&below));

        bp.delete_page(below).unwrap();
        assert!(
            !bp.page_table.read().unwrap().contains_key(&below),
            "an accepted delete must actually evict"
        );
        let _ = std::fs::remove_file(&path);
    }
}
