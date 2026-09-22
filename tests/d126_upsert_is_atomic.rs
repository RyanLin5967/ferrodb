//! **D126: a key must never be transiently ABSENT while its value is being rewritten.**
//!
//! `TableBranchCatalog::upsert` was delete-then-insert, and `BPlusTreeManager` had no replace
//! primitive to call instead — that absence is the reason the catalog open-coded the pair.
//! `delete` drops the leaf write latch when it returns and `insert` re-acquires it, so between
//! the two calls the key **does not exist in the tree**. `write_record` routes the RECORD key
//! straight through `upsert`, and `set_root` / `renew_lease` / `set_state` / `reparent` /
//! `restrict_envelope` / `put` all reach it — two of those are hot-path writes. Every reader of
//! a record (`core`, `get_raw`, `has_live_children`, `max_live_child`,
//! `live_child_in_epoch_range`) takes **no lock at all**: `logical` is writers-only.
//!
//! ⇒ A reader could see "no record" for a perfectly healthy, live branch. D124's guards turn that
//! ambiguity into a refusal rather than a page free, which is the right direction and not the end
//! state. D126 removes the state itself.
//!
//! # What each test here is for
//!
//! 1. [`fire_check_delete_then_insert_is_observably_absent`] — the **instrument's fire check**.
//!    It open-codes exactly the old shape (`delete` then `insert`) and asserts the reader DOES
//!    see the key vanish. Without this, a zero from the tests below would be indistinguishable
//!    from a reader that cannot observe an absence at all.
//! 2. [`upsert_never_shows_an_absent_key`] — the same harness, same key, same thread counts,
//!    with `BPlusTreeManager::upsert`. Zero absences.
//! 3. [`upsert_never_shows_an_absent_key_while_splitting`] — the not-fits case. The value is
//!    grown until the leaf overflows, so the pessimistic split path runs; a counter proves pages
//!    were actually allocated, i.e. that splits really happened during the measured window.
//! 4. [`record_key_is_never_absent_during_set_root`] — the defect site itself, at the catalog
//!    level: a `set_root` loop against concurrent `get_raw`. This is the test that FAILED before
//!    the fix.
//!
//! Every counter here is an integer taken inside the loop, and every test asserts its own
//! positive control (reads happened, rounds happened) so that a harness which silently did
//! nothing fails instead of passing.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline, PageId};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::index::BPlusTreeManager;

type Tree = BPlusTreeManager<Vec<u8>, Vec<u8>>;

/// How many times the writer rewrites the key. Small on purpose: each rewrite opens exactly one
/// window, and a reader spinning beside it samples the key thousands of times per rewrite, so the
/// hit count is driven by reader throughput and not by this number.
const ROUNDS: u64 = 400;
const READERS: usize = 3;

/// The key under test, shaped like `tree_keys::record`: one tag byte then an 8-byte big-endian id.
fn target_key() -> Vec<u8> {
    let mut k = vec![0x01u8];
    k.extend_from_slice(&7u64.to_be_bytes());
    k
}

fn filler_key(i: u64) -> Vec<u8> {
    let mut k = vec![0x01u8];
    k.extend_from_slice(&(1000 + i).to_be_bytes());
    k
}

fn value(round: u64, len: usize) -> Vec<u8> {
    let mut v = round.to_be_bytes().to_vec();
    v.resize(len, 0xAB);
    v
}

fn tree() -> (Arc<BufferPoolManager>, Tree, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("d126.db");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let tree = Tree::create(Arc::clone(&pool)).unwrap();
    (pool, tree, dir)
}

/// What one race produced. `reads` is the positive control: a run that sampled nothing has not
/// proved anything, so every caller asserts on it.
struct Race {
    absent: u64,
    reads: u64,
}

/// Spin `READERS` threads on `tree.search(key)` while the calling thread runs `rewrite` `ROUNDS`
/// times, and count how often the key was not there.
///
/// The readers take no latch — `search` descends optimistically — which is exactly the position
/// the catalog's record readers are in.
fn race(tree: &Tree, key: &[u8], rounds: u64, rewrite: &(dyn Fn(u64) + Sync)) -> Race {
    let done = AtomicBool::new(false);
    let absent = AtomicU64::new(0);
    let reads = AtomicU64::new(0);
    let owned = key.to_vec();

    thread::scope(|s| {
        for _ in 0..READERS {
            let done = &done;
            let absent = &absent;
            let reads = &reads;
            let owned = &owned;
            s.spawn(move || {
                let mut local_reads = 0u64;
                let mut local_absent = 0u64;
                while !done.load(Ordering::Relaxed) {
                    local_reads += 1;
                    if tree.search(owned).unwrap().is_none() {
                        local_absent += 1;
                    }
                }
                reads.fetch_add(local_reads, Ordering::Relaxed);
                absent.fetch_add(local_absent, Ordering::Relaxed);
            });
        }
        for r in 0..rounds {
            rewrite(r);
        }
        done.store(true, Ordering::Relaxed);
    });

    Race { absent: absent.load(Ordering::SeqCst), reads: reads.load(Ordering::SeqCst) }
}

/// Seed enough filler keys that the tree has internal nodes, so the writer exercises the real
/// crabbing descent rather than the root-is-a-leaf special case.
fn seed(tree: &Tree, fillers: u64, filler_len: usize) {
    for i in 0..fillers {
        tree.insert(filler_key(i), value(i, filler_len)).unwrap();
    }
}

// ---------------------------------------------------------------------------------------------
// 1. THE FIRE CHECK. The instrument must be able to see an absence, or its zeros mean nothing.
// ---------------------------------------------------------------------------------------------

/// Open-codes the shape `TableBranchCatalog::upsert` used to have. This test asserts the DEFECT,
/// and it must keep passing for ever: it is what licenses reading a zero from the tests below.
#[test]
fn fire_check_delete_then_insert_is_observably_absent() {
    let (_pool, tree, _dir) = tree();
    seed(&tree, 300, 48);
    let key = target_key();
    tree.insert(key.clone(), value(0, 48)).unwrap();

    let r = race(&tree, &key, ROUNDS, &|round| {
        // Exactly the two calls the catalog made, with nothing held across them.
        tree.delete(&key).unwrap();
        tree.insert(key.clone(), value(round, 48)).unwrap();
    });

    assert!(r.reads > 0, "harness collected nothing: {} reads", r.reads);
    assert!(
        r.absent > 0,
        "the probe could not observe an absence even against delete-then-insert \
         ({} absent of {} reads) - so a zero from the upsert tests would prove nothing",
        r.absent,
        r.reads
    );
    eprintln!("fire check: delete+insert -> {} absent of {} reads", r.absent, r.reads);
}

// (tests 2 and 3 are added with the fix; `BPlusTreeManager::upsert` does not exist yet.)

// ---------------------------------------------------------------------------------------------
// 4. THE DEFECT SITE. This is the test that failed before the fix.
// ---------------------------------------------------------------------------------------------

/// `set_root` is an ordinary hot-path write. While one runs, `get_raw` — the reader the reaper
/// and the page store both use, and which takes no lock — must never report the branch missing.
#[test]
fn record_key_is_never_absent_during_set_root() {
    let dir = tempfile::tempdir().unwrap();
    let cat = TableBranchCatalog::open_sidecar(&dir.path().join("d126.cat"), 1).unwrap();
    let branch = cat.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
    let id = branch.branch_id.id;

    let done = AtomicBool::new(false);
    let reads = AtomicU64::new(0);
    let misses = AtomicU64::new(0);
    // Any error that is NOT "branch not found" is a different bug wearing this one's clothes, so
    // it is counted separately instead of being folded into the miss count.
    let other_errors = std::sync::Mutex::new(Vec::<String>::new());
    // Fewer rounds than the tree-level tests: every `set_root` ends in a real fsync.
    let rounds = 120u64;

    thread::scope(|s| {
        for _ in 0..READERS {
            let (done, reads, misses, other_errors, cat) =
                (&done, &reads, &misses, &other_errors, &cat);
            s.spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    reads.fetch_add(1, Ordering::Relaxed);
                    if let Err(e) = BranchCatalog::get_raw(cat, id) {
                        let msg = e.to_string();
                        if msg.contains("not found") {
                            misses.fetch_add(1, Ordering::Relaxed);
                        } else {
                            other_errors.lock().unwrap().push(msg);
                        }
                    }
                }
            });
        }
        for r in 0..rounds {
            cat.set_root(branch.branch_id, (r % 1000) as PageId + 1).unwrap();
        }
        done.store(true, Ordering::Relaxed);
    });

    let reads = reads.load(Ordering::SeqCst);
    let misses = misses.load(Ordering::SeqCst);
    let others = other_errors.into_inner().unwrap();

    assert!(reads > 0, "harness collected nothing: {reads} reads");
    assert!(others.is_empty(), "get_raw failed for an unexpected reason: {:?}", &others[..1]);
    assert_eq!(
        misses, 0,
        "the RECORD key of a live branch was missing {misses} times in {reads} lockless reads \
         taken during {rounds} set_root calls"
    );
}
