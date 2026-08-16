//! D23 — `TxnManager` under real threads.
//!
//! The half D22's title claimed and did not test. Reading it suggests it is sound: `begin()` holds
//! the ATT lock across both the id assignment and the insertion, and `read_snapshot()` loads
//! `next_txn_id` while holding that same lock — so there is no window where a transaction has an
//! id but is missing from the active set. Reading is how D18's cause was misattributed, though, so
//! it is settled here by running it.
//!
//! The invariant that matters is snapshot isolation itself: **a snapshot must never omit a
//! transaction that is still running.** A running transaction below `high_water` and absent from
//! `active` is treated as committed, and its uncommitted writes become visible. That is not a
//! performance bug or a crash — it is the isolation guarantee failing silently, which is the
//! hardest kind to notice in production.
//!
//! `abort()` also does `att.get_mut(&txn_id).unwrap()`, which panics on a transaction that is not
//! in the table — the same panic-on-missing-key shape as D19 and D21, so it is probed too.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

fn txn_manager(tag: &str) -> (tempfile::TempDir, Arc<TxnManager>) {
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
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{tag}.wal"))).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal);
    (dir, txn)
}

/// Two transactions must never share an id. Sharing one would merge two transactions' fates.
#[test]
fn concurrent_begins_get_distinct_transaction_ids() {
    let (_d, txn) = txn_manager("ids");
    const THREADS: usize = 8;
    const EACH: usize = 60;

    let ids: Vec<u64> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let txn = Arc::clone(&txn);
                s.spawn(move || {
                    (0..EACH)
                        .filter_map(|_| {
                            let id = txn.begin().ok()?;
                            txn.commit(id).ok()?;
                            Some(id)
                        })
                        .collect::<Vec<u64>>()
                })
            })
            .collect();
        handles.into_iter().flat_map(|h| h.join().expect("no panic")).collect()
    });

    assert!(ids.len() > THREADS * EACH / 2, "too few transactions ran to prove anything");
    let unique: std::collections::BTreeSet<u64> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len(), "two transactions shared an id");
}

/// **The isolation invariant.** While a transaction is running, every snapshot taken must list it
/// as active — otherwise a reader treats it as committed and sees writes that may yet be rolled
/// back.
#[test]
fn a_running_transaction_is_never_missing_from_a_snapshot() {
    let (_d, txn) = txn_manager("snap");
    const SAMPLES: u64 = 500;

    let holder_id = txn.begin().expect("begin the long-running transaction");

    let missed = Arc::new(AtomicU64::new(0));
    let samples = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    std::thread::scope(|s| {
        // Churn: other transactions starting and finishing around the held one.
        for _ in 0..4 {
            let txn = Arc::clone(&txn);
            let stop = Arc::clone(&stop);
            s.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(id) = txn.begin() {
                        let _ = txn.commit(id);
                    }
                }
            });
        }
        // The checker samples a fixed number of times — not on a timer, because a checker that
        // stops early reports a clean result while having observed nothing.
        {
            let txn = Arc::clone(&txn);
            let missed = Arc::clone(&missed);
            let samples = Arc::clone(&samples);
            s.spawn(move || {
                for _ in 0..SAMPLES {
                    let snap = txn.read_snapshot();
                    samples.fetch_add(1, Ordering::Relaxed);
                    if snap.high_water > holder_id && !snap.active.contains(&holder_id) {
                        missed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
        // Churners stop once the checker has finished its fixed run.
        while samples.load(Ordering::Relaxed) < SAMPLES {
            std::hint::spin_loop();
        }
        stop.store(true, Ordering::Relaxed);
    });

    assert_eq!(samples.load(Ordering::Relaxed), SAMPLES, "the checker did not complete its run");
    assert_eq!(
        missed.load(Ordering::Relaxed),
        0,
        "a running transaction was absent from {} of {SAMPLES} snapshots; a reader would treat its \
         uncommitted writes as committed",
        missed.load(Ordering::Relaxed)
    );

    txn.commit(holder_id).expect("commit the holder");
}

/// Committing and aborting concurrently must not panic. `abort()` unwraps its ATT lookup, which is
/// the same panic-on-missing-key shape as D19 and D21.
#[test]
fn concurrent_commits_and_aborts_do_not_panic() {
    let (_d, txn) = txn_manager("mixed");

    std::thread::scope(|s| {
        for t in 0..6 {
            let txn = Arc::clone(&txn);
            s.spawn(move || {
                for i in 0..40 {
                    let Ok(id) = txn.begin() else { continue };
                    if (t + i) % 2 == 0 {
                        let _ = txn.commit(id);
                    } else {
                        let _ = txn.abort(id);
                    }
                }
            });
        }
    });
}

/// A finished transaction must not linger in the active set, or readers keep treating long-gone
/// writes as in-flight and visibility never advances.
#[test]
fn committed_transactions_leave_the_active_set() {
    let (_d, txn) = txn_manager("drain");

    std::thread::scope(|s| {
        for _ in 0..6 {
            let txn = Arc::clone(&txn);
            s.spawn(move || {
                for _ in 0..50 {
                    if let Ok(id) = txn.begin() {
                        let _ = txn.commit(id);
                    }
                }
            });
        }
    });

    let snap = txn.read_snapshot();
    assert!(
        snap.active.is_empty(),
        "{} transaction(s) are still listed active after every one committed: {:?}",
        snap.active.len(),
        snap.active.iter().take(5).collect::<Vec<_>>()
    );
}
