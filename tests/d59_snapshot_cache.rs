//! D59 — a cached snapshot must be indistinguishable from a freshly built one.
//!
//! `read_snapshot` took the active-transaction table's mutex and allocated a `HashSet` of every
//! open transaction, once per statement: 17.4k of ~80k thread-samples at 16 agent readers once
//! D58 had taken the page latch off the read path. `read_snapshot_cached` reuses the last
//! snapshot this thread built while the table's version has not moved — Postgres 14's
//! `xactCompletionCount`, and D54's catalog epoch in this repo.
//!
//! The whole correctness argument is "the version moves on every change, inside the critical
//! section that makes it". So these tests attack exactly that:
//!
//! 1. a transaction that BEGINS must appear in the next cached snapshot;
//! 2. a transaction that COMMITS must disappear from it, and become `includes`-visible;
//! 3. and under a writer churning begins and commits, a cached snapshot taken while the version
//!    did not move must equal the uncached one — which is what fails if the bump is published
//!    outside the lock, or if the snapshot is labelled with a version read before it.

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn manager(dir: &tempfile::TempDir, name: &str) -> Arc<TxnManager> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{name}.db")))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{name}.wal"))).unwrap());
    bp.attach_wal(wal.clone());
    Arc::new(TxnManager::new(wal, bp))
}

#[test]
fn a_transaction_that_begins_is_in_the_next_cached_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let t = manager(&dir, "begin");

    // Warm the cache, so the next call is the one that must miss.
    let before = t.read_snapshot_cached();
    assert!(before.active.is_empty(), "the fixture started with an open transaction");
    let warm = t.read_snapshot_cached();
    assert!(warm.active.is_empty());

    let id = t.begin().unwrap();
    let after = t.read_snapshot_cached();
    assert!(
        after.active.contains(&id),
        "a cached snapshot taken after BEGIN {id} does not show it active ({:?}): the version did \
         not move, so every reader on this thread is looking at a snapshot from before it",
        after.active
    );
    // And the uncached truth agrees, which is what "indistinguishable" means.
    assert_eq!(after.active, t.read_snapshot().active);
    assert!(!after.includes(id), "an active transaction must not be included by a snapshot");
}

#[test]
fn a_transaction_that_commits_leaves_the_next_cached_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let t = manager(&dir, "commit");

    let id = t.begin().unwrap();
    let during = t.read_snapshot_cached();
    assert!(during.active.contains(&id));
    let _warm = t.read_snapshot_cached();

    t.commit(id).unwrap();
    let after = t.read_snapshot_cached();
    assert!(
        !after.active.contains(&id),
        "a cached snapshot taken after COMMIT {id} still lists it active: a committed \
         transaction stays invisible for ever on this thread"
    );
    assert!(
        after.includes(id),
        "after COMMIT {id} the snapshot does not include it, so its rows are invisible \
         (high_water {}, active {:?})",
        after.high_water,
        after.active
    );
    assert_eq!(after.active, t.read_snapshot().active);
}

#[test]
fn an_abort_also_moves_the_version() {
    let dir = tempfile::tempdir().unwrap();
    let t = manager(&dir, "abort");
    let id = t.begin().unwrap();
    assert!(t.read_snapshot_cached().active.contains(&id));
    let _warm = t.read_snapshot_cached();
    t.abort(id).unwrap();
    let after = t.read_snapshot_cached();
    assert!(!after.active.contains(&id), "an aborted transaction is still listed active");
    assert_eq!(after.active, t.read_snapshot().active);
}

/// The ordering property, forced: a writer churns begins and commits while readers take cached
/// snapshots. **If the version did not move across a cached read, that snapshot must equal the
/// one the lock would have produced.** A bump published after the lock is released — or a
/// snapshot labelled with a version read before the table was — breaks exactly this.
#[test]
fn a_cached_snapshot_equals_the_locked_one_when_the_version_did_not_move() {
    let dir = tempfile::tempdir().unwrap();
    let t = manager(&dir, "race");
    let stop = Arc::new(AtomicBool::new(false));
    let churned = Arc::new(AtomicU64::new(0));

    let writer = {
        let (t, stop, churned) = (t.clone(), stop.clone(), churned.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let id = t.begin().unwrap();
                churned.fetch_add(1, Ordering::Relaxed);
                if id % 2 == 0 {
                    t.commit(id).unwrap();
                } else {
                    t.abort(id).unwrap();
                }
                churned.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (t, stop) = (t.clone(), stop.clone());
            std::thread::spawn(move || {
                let (mut checked, mut skipped) = (0u64, 0u64);
                let t0 = Instant::now();
                while !stop.load(Ordering::Relaxed) && t0.elapsed() < Duration::from_secs(4) {
                    let v1 = t.att_version();
                    let cached = t.read_snapshot_cached();
                    let locked = t.read_snapshot();
                    let v2 = t.att_version();
                    if v1 != v2 {
                        // The table changed under us; the two are allowed to differ.
                        skipped += 1;
                        continue;
                    }
                    assert_eq!(
                        cached.active, locked.active,
                        "the version did not move ({v1}) but the cached snapshot disagrees with \
                         the locked one: cached {:?}, locked {:?}",
                        cached.active, locked.active
                    );
                    checked += 1;
                }
                (checked, skipped)
            })
        })
        .collect();

    let (mut checked, mut skipped) = (0u64, 0u64);
    for r in readers {
        let (c, s) = r.join().unwrap();
        checked += c;
        skipped += s;
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    // The detector must have been able to fire: the writer churned, and readers both checked and
    // were forced to skip — i.e. their reads really did interleave with the writer's changes.
    let n = churned.load(Ordering::Relaxed);
    assert!(n > 1000, "the writer churned only {n} times; the race was not exercised");
    assert!(checked > 1000, "only {checked} comparisons were made");
    assert!(skipped > 0, "no read ever overlapped a change ({checked} checked): not a race");
}
