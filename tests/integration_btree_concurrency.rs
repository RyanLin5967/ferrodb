//! D23 — is `BPlusTreeManager` safe for concurrent use?
//!
//! `BPlusTreeManager` exposes `insert`, `delete` and `search` on `&self`, holds its root in an
//! `AtomicU32`, and is `Sync` (its fields are `Arc<BufferPoolManager>` + `AtomicU32`). So the type
//! system permits two threads to call `insert` on one tree. Nothing in `src/storage/index.rs`
//! takes a latch spanning a descent, a read-modify-write, or a split.
//!
//! This file asks that question with a workload rather than by reading the code. It is the same
//! shape as `integration_buffer_pool_concurrency.rs`: put N threads on one shared structure and
//! decide the verdict from the data, not from what the structure reports about itself.
//!
//! Two INDEPENDENT signals, because they indict different lines:
//!
//! 1. **Lost insert.** Every thread inserts its own disjoint key set. Afterwards, single-threaded,
//!    every key it inserted must be findable. Nothing in this test deletes, so a missing key is
//!    a write that was overwritten, not a write that was undone.
//! 2. **Phantom miss.** The tree is pre-populated single-threaded before any thread starts, and
//!    the readers only ever look for those pre-existing keys. Those keys are present at the start
//!    of the run and nothing removes them, so a reader that cannot find one has descended through
//!    a tree that was mid-change — it is a pure reader race, independent of whether any write was
//!    lost.
//!
//! Plus a structural check: a full range scan must return exactly the live key set, in order, with
//! no duplicates. A split that re-parents a subtree wrongly is silent to point lookups on the keys
//! that still resolve, and shows up here.
//!
//! The 1-thread arm is the CONTROL. It runs the identical code path with identical totals. If the
//! 1-thread arm passes and the 8/16/32 arms fail, concurrency is the variable.
//!
//! **Page budget is deliberate.** The whole workload fits well inside the buffer pool's 1024
//! frames, so no eviction happens and the pool's replacement path is not on trial here — D18/D19
//! already own that. Any failure this file reports is the tree's.

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::column::Value;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_file_manager::RecordId;
use ferrodb::storage::index::BPlusTreeManager;

/// Keys loaded single-threaded before any worker starts. Readers only ever ask for these.
const PREPOP: i32 = 1000;
/// Keys each worker inserts. Enough, across 8+ threads, to drive many leaf splits and some
/// internal splits while staying far inside 1024 frames.
const PER_THREAD: i32 = 150;
/// Spacing between consecutive pre-populated keys. Worker keys are placed in the gaps, so the two
/// sets INTERLEAVE across the whole key range instead of occupying disjoint tails of it.
///
/// The first cut of this file gave the workers keys above every pre-populated one. Readers then
/// only ever descended leaves no writer was touching, `phantom_misses` was 0, and that 0 was a
/// statement about the key layout rather than about reader safety. With the sets interleaved, a
/// reader probing a pre-existing key descends the same subtrees the writers are splitting.
const GAP: i32 = 64;

/// Pre-populated key `k` (`k` in `0..PREPOP`).
fn prepop_key(k: i32) -> i32 {
    k * GAP
}

/// Worker key for global insert index `g`. Lands strictly between two pre-populated keys, and is
/// unique across `g` as long as `PER_THREAD * threads / PREPOP < GAP - 1` — 4.8 at the widest arm.
fn worker_key(g: i32) -> i32 {
    (g % PREPOP) * GAP + 1 + (g / PREPOP)
}
/// Independent rounds per arm, each on a fresh tree. A race that fires in 1 round of 3 is still a
/// race; reporting the rate is what makes "it passed once" uninformative.
const ROUNDS: usize = 3;

type Tree = BPlusTreeManager<Value, RecordId>;

fn fresh_tree(tag: &str) -> (tempfile::TempDir, Arc<Tree>) {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let bp = Arc::new(BufferPoolManager::new(dm));
    let tree = Arc::new(Tree::create(bp).expect("create index"));
    (dir, tree)
}

fn rid(k: i32) -> RecordId {
    RecordId::new(k as u32 + 1, (k % 100) as u16)
}

/// What one round observed. Every field is a count of something that MUST be zero.
#[derive(Default, Debug)]
struct Round {
    /// Worker threads that panicked.
    panics: usize,
    /// `insert` calls that returned `Err`.
    insert_errors: usize,
    /// `search` calls for a PRE-EXISTING key that returned `Ok(None)` or `Err` while writers ran.
    phantom_misses: usize,
    /// Keys inserted by a worker that could not be found afterwards, single-threaded.
    lost_inserts: usize,
    /// Pre-populated keys that could not be found afterwards, single-threaded.
    lost_prepop: usize,
    /// Keys whose stored value is not the one that was written for them.
    wrong_values: usize,
    /// Entries a full range scan returned that are out of order or duplicated.
    scan_disorder: usize,
    /// Entries a full range scan returned, against the number of keys inserted.
    scan_len: usize,
    scan_expected: usize,
    /// Pre-populated keys a scan running CONCURRENTLY with the writers failed to return.
    ///
    /// Distinct from `scan_len`: that scan runs after every writer has joined, so it can only see
    /// damage that PERSISTED. This one is the live signal, and it is the only thing in this file
    /// that exercises `RangeScanner`'s page read latch at all.
    live_scan_misses: usize,
    /// Out-of-order, duplicate, or failed entries from a concurrent scan.
    live_scan_disorder: usize,
    /// Concurrent scans actually completed. A zero here means the signal above is vacuous.
    live_scans: usize,
}

impl Round {
    fn clean(&self) -> bool {
        self.panics == 0
            && self.insert_errors == 0
            && self.phantom_misses == 0
            && self.lost_inserts == 0
            && self.lost_prepop == 0
            && self.wrong_values == 0
            && self.scan_disorder == 0
            && self.scan_len == self.scan_expected
            && self.live_scan_misses == 0
            && self.live_scan_disorder == 0
    }
}

fn one_round(threads: usize, tag: &str) -> Round {
    let (_dir, tree) = fresh_tree(tag);
    let mut r = Round::default();

    // --- pre-populate, single-threaded. These keys exist before any worker starts. -------------
    for i in 0..PREPOP {
        let k = prepop_key(i);
        tree.insert(Value::Integer(k), rid(k)).expect("pre-populate insert");
    }
    for i in 0..PREPOP {
        let k = prepop_key(i);
        assert_eq!(
            tree.search(&Value::Integer(k)).expect("pre-populate search"),
            Some(rid(k)),
            "the fixture is wrong: key {k} is not readable before any thread started"
        );
    }

    // --- the concurrent phase -----------------------------------------------------------------
    // Worker keys are strided across threads (g = i*T + t) so threads land in the SAME leaves
    // rather than each owning its own tail of the key space — that is where a read-modify-write on
    // one page shows — and `worker_key` places them between pre-populated keys so readers probing
    // pre-existing keys descend subtrees the writers are actively splitting.
    let phantoms = Arc::new(AtomicUsize::new(0));
    let ins_err = Arc::new(AtomicUsize::new(0));
    // The scanner joins the barrier too, so it starts with the writers rather than after them.
    // The 1-thread arm runs WITHOUT it: that arm is the control and has to stay genuinely
    // single-threaded, or a failure there would no longer isolate concurrency as the variable.
    let scanning = threads > 1;
    let barrier = Arc::new(Barrier::new(threads + usize::from(scanning)));
    let writers_done = Arc::new(AtomicBool::new(false));
    let live_misses = Arc::new(AtomicUsize::new(0));
    let live_disorder = Arc::new(AtomicUsize::new(0));
    let live_scans = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let tree = Arc::clone(&tree);
            let phantoms = Arc::clone(&phantoms);
            let ins_err = Arc::clone(&ins_err);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..PER_THREAD {
                    let k = worker_key(i * threads as i32 + t as i32);
                    if tree.insert(Value::Integer(k), rid(k)).is_err() {
                        ins_err.fetch_add(1, Ordering::Relaxed);
                    }
                    // A key that existed before this thread started, and that nothing removes.
                    let probe = prepop_key((i * 7 + t as i32 * 13) % PREPOP);
                    match tree.search(&Value::Integer(probe)) {
                        Ok(Some(v)) if v == rid(probe) => {}
                        _ => {
                            phantoms.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    // --- a scan running WHILE the writers split leaves -----------------------------------------
    //
    // Why a pre-populated key is a sound invariant for a non-snapshot scan: every such key is in
    // the tree before the scan starts and nothing in this file removes one. A leaf-chain walk can
    // legitimately miss a key INSERTED after it passed that point, but it cannot legitimately miss
    // a key that was already there - the split protocol only ever moves keys RIGHTWARD into a new
    // leaf that the scan has not reached yet, and writes that leaf before publishing the `next`
    // pointer to it. A missing pre-populated key therefore means the walk left the chain.
    let scanner = scanning.then(|| {
        let tree = Arc::clone(&tree);
        let barrier = Arc::clone(&barrier);
        let writers_done = Arc::clone(&writers_done);
        let (misses, disorder, scans) =
            (Arc::clone(&live_misses), Arc::clone(&live_disorder), Arc::clone(&live_scans));
        std::thread::spawn(move || {
            barrier.wait();
            // At least one scan even if the writers finish first, so the counter is never vacuous.
            loop {
                let mut seen_prepop = 0usize;
                let mut last: Option<Value> = None;
                match tree.range_scan(Bound::Unbounded, Bound::Unbounded) {
                    Ok(scan) => {
                        for e in scan {
                            match e {
                                Ok((k, _)) => {
                                    if let Some(prev) = &last {
                                        if *prev >= k {
                                            disorder.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                    // Pre-populated keys are exactly the multiples of GAP;
                                    // `worker_key` adds 1..=5 so it never produces one.
                                    if let Value::Integer(i) = k {
                                        if i % GAP == 0 {
                                            seen_prepop += 1;
                                        }
                                    }
                                    last = Some(k);
                                }
                                Err(_) => {
                                    disorder.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    Err(_) => {
                        disorder.fetch_add(1, Ordering::Relaxed);
                    }
                }
                misses.fetch_add(PREPOP as usize - seen_prepop.min(PREPOP as usize), Ordering::Relaxed);
                scans.fetch_add(1, Ordering::Relaxed);
                if writers_done.load(Ordering::Acquire) {
                    break;
                }
            }
        })
    });

    for h in handles {
        if h.join().is_err() {
            r.panics += 1;
        }
    }
    writers_done.store(true, Ordering::Release);
    if let Some(s) = scanner {
        if s.join().is_err() {
            r.panics += 1;
        }
    }
    r.live_scan_misses = live_misses.load(Ordering::Relaxed);
    r.live_scan_disorder = live_disorder.load(Ordering::Relaxed);
    r.live_scans = live_scans.load(Ordering::Relaxed);
    assert!(
        !scanning || r.live_scans > 0,
        "no concurrent scan completed - live_scan_misses is measuring nothing"
    );
    r.phantom_misses = phantoms.load(Ordering::Relaxed);
    r.insert_errors = ins_err.load(Ordering::Relaxed);

    // --- verify, single-threaded --------------------------------------------------------------
    for i in 0..PREPOP {
        let k = prepop_key(i);
        match tree.search(&Value::Integer(k)) {
            Ok(Some(v)) if v == rid(k) => {}
            Ok(Some(_)) => r.wrong_values += 1,
            _ => r.lost_prepop += 1,
        }
    }
    for g in 0..(threads as i32 * PER_THREAD) {
        let k = worker_key(g);
        match tree.search(&Value::Integer(k)) {
            Ok(Some(v)) if v == rid(k) => {}
            Ok(Some(_)) => r.wrong_values += 1,
            _ => r.lost_inserts += 1,
        }
    }

    r.scan_expected = PREPOP as usize + threads * PER_THREAD as usize;
    match tree.range_scan(Bound::Unbounded, Bound::Unbounded) {
        Ok(scan) => {
            let mut last: Option<Value> = None;
            let mut n = 0usize;
            for e in scan {
                match e {
                    Ok((k, _)) => {
                        n += 1;
                        if let Some(prev) = &last {
                            if *prev >= k {
                                r.scan_disorder += 1;
                            }
                        }
                        last = Some(k);
                    }
                    Err(_) => r.scan_disorder += 1,
                }
            }
            r.scan_len = n;
        }
        Err(_) => r.scan_disorder += 1,
    }
    r
}

fn arm(threads: usize) {
    let mut dirty = 0usize;
    let mut reports = Vec::new();
    for round in 0..ROUNDS {
        let r = one_round(threads, &format!("d23_t{threads}_r{round}"));
        if !r.clean() {
            dirty += 1;
        }
        reports.push(format!("  round {round}: {r:?}"));
    }
    let body = reports.join("\n");
    println!("threads={threads} dirty_rounds={dirty}/{ROUNDS}\n{body}");
    assert_eq!(
        dirty, 0,
        "\nB+tree lost or corrupted state under {threads} concurrent threads \
         ({dirty} of {ROUNDS} rounds dirty).\nEvery counted field must be 0 and scan_len must \
         equal scan_expected.\n{body}\n"
    );
}

/// CONTROL. Same code path, same totals, one thread. Must pass.
#[test]
fn btree_concurrent_1_thread() {
    arm(1);
}

#[test]
fn btree_concurrent_8_threads() {
    arm(8);
}

#[test]
fn btree_concurrent_16_threads() {
    arm(16);
}

#[test]
fn btree_concurrent_32_threads() {
    arm(32);
}
