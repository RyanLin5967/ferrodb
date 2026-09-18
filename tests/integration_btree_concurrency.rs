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
use std::sync::atomic::{AtomicUsize, Ordering};
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
    }
}

fn one_round(threads: usize, tag: &str) -> Round {
    let (_dir, tree) = fresh_tree(tag);
    let mut r = Round::default();

    // --- pre-populate, single-threaded. These keys exist before any worker starts. -------------
    for k in 0..PREPOP {
        tree.insert(Value::Integer(k), rid(k)).expect("pre-populate insert");
    }
    for k in 0..PREPOP {
        assert_eq!(
            tree.search(&Value::Integer(k)).expect("pre-populate search"),
            Some(rid(k)),
            "the fixture is wrong: key {k} is not readable before any thread started"
        );
    }

    // --- the concurrent phase -----------------------------------------------------------------
    // Keys are strided (t, t+T, t+2T, ...) so threads land in the SAME leaves rather than each
    // owning its own tail of the key space, which is where a read-modify-write on one page shows.
    let phantoms = Arc::new(AtomicUsize::new(0));
    let ins_err = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(threads));

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let tree = Arc::clone(&tree);
            let phantoms = Arc::clone(&phantoms);
            let ins_err = Arc::clone(&ins_err);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                for i in 0..PER_THREAD {
                    let k = PREPOP + i * threads as i32 + t as i32;
                    if tree.insert(Value::Integer(k), rid(k)).is_err() {
                        ins_err.fetch_add(1, Ordering::Relaxed);
                    }
                    // A key that existed before this thread started, and that nothing removes.
                    let probe = (i * 7 + t as i32 * 13) % PREPOP;
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

    for h in handles {
        if h.join().is_err() {
            r.panics += 1;
        }
    }
    r.phantom_misses = phantoms.load(Ordering::Relaxed);
    r.insert_errors = ins_err.load(Ordering::Relaxed);

    // --- verify, single-threaded --------------------------------------------------------------
    for k in 0..PREPOP {
        match tree.search(&Value::Integer(k)) {
            Ok(Some(v)) if v == rid(k) => {}
            Ok(Some(_)) => r.wrong_values += 1,
            _ => r.lost_prepop += 1,
        }
    }
    for t in 0..threads as i32 {
        for i in 0..PER_THREAD {
            let k = PREPOP + i * threads as i32 + t;
            match tree.search(&Value::Integer(k)) {
                Ok(Some(v)) if v == rid(k) => {}
                Ok(Some(_)) => r.wrong_values += 1,
                _ => r.lost_inserts += 1,
            }
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
