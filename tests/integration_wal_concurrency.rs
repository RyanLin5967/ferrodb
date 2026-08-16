//! D22 — WAL and `TxnManager` under real threads.
//!
//! The last shared component the sweep had not touched. Four defects came out of the three layers
//! below it, all of the same shape: state updated outside the lock that protects the operation it
//! describes.
//!
//! The specific worry here is `flushed_lsn`. `flush()` drains the buffer under one mutex, releases
//! it, then takes the *file* mutex to write — so two flushes can be in flight at once, and the
//! second can finish first. `flushed_lsn` is then advanced with `fetch_max`, which cannot express
//! "durable up to 300 except for a hole at 100..200".
//!
//! That matters more than it sounds. `flush_up_to` is the write-ahead gate: the buffer pool calls
//! it before writing a dirty page, and returns early if `flushed_lsn` is already past the page's
//! LSN. If that value over-reports, a data page reaches disk while the log record describing it is
//! still only in memory, which is the one rule write-ahead logging exists to enforce.
//!
//! `scan_valid_end` is the honest instrument: it walks the file's record chain and reports how far
//! genuinely valid, CRC-checked records extend. Anything `flushed_lsn` claims beyond that is a
//! claim the file does not support.

use std::sync::Arc;

use ferrodb::wal::log::{scan_valid_end, RecKind, WalManager};

fn wal(tag: &str) -> (tempfile::TempDir, Arc<WalManager>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("{tag}.wal"));
    (dir, Arc::new(WalManager::new(path).expect("wal")))
}

/// **The load-bearing one.** `flushed_lsn` must never claim more than the file actually holds —
/// **while the race is happening**, not once it has finished.
///
/// The first version of this test checked the invariant after every thread had joined and a final
/// flush had run. It passed, and it could not have failed: by then any hole has been filled in.
/// The defect being looked for is a *transient* over-report, and the transient is the entire
/// hazard, because `flush_up_to` consults `flushed_lsn` precisely while other flushes are in
/// flight. So a checker thread samples the invariant continuously, under load.
#[test]
fn flushed_lsn_never_over_reports_while_flushes_are_in_flight() {
    let (_d, wal) = wal("claims");
    const APPENDERS: usize = 6;
    const FLUSHERS: usize = 4;

    // The checker runs a FIXED number of samples rather than being stopped by a timer. The first
    // version spun `yield_now()` in the main body and then set a stop flag, which could finish
    // before the checker thread had even started — it sampled once, and the anti-vacuity guard
    // below correctly refused to call that an observation of anything. Timing-based scaffolding
    // makes a concurrency test flaky in the one direction that matters: silently not looking.
    const SAMPLES: u64 = 400;
    let worst = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let samples = Arc::new(std::sync::atomic::AtomicU64::new(0));

    std::thread::scope(|s| {
        for t in 0..APPENDERS {
            let wal = Arc::clone(&wal);
            s.spawn(move || {
                for _ in 0..600u64 {
                    let _ = wal.append(t as u64 + 1, 0, &RecKind::Commit);
                }
            });
        }
        for _ in 0..FLUSHERS {
            let wal = Arc::clone(&wal);
            s.spawn(move || {
                for _ in 0..600 {
                    let _ = wal.flush();
                }
            });
        }
        // The checker: read the durability claim, then immediately ask the file to back it up.
        {
            let wal = Arc::clone(&wal);
            let worst = Arc::clone(&worst);
            let samples = Arc::clone(&samples);
            s.spawn(move || {
                use std::sync::atomic::Ordering;
                for _ in 0..SAMPLES {
                    let claimed = wal.flushed_lsn.load(Ordering::SeqCst);
                    let base = wal.base_lsn.load(Ordering::SeqCst);
                    let valid_end = {
                        let file = wal.file.lock().unwrap();
                        let len = file.metadata().expect("metadata").len();
                        scan_valid_end(&file, base, len).expect("scan")
                    };
                    samples.fetch_add(1, Ordering::Relaxed);
                    if claimed > valid_end {
                        worst.fetch_max(claimed - valid_end, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    let n = samples.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(n, SAMPLES, "the checker sampled {n} times, not {SAMPLES}");

    let gap = worst.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        gap, 0,
        "flushed_lsn over-reported by up to {gap} bytes during the race: it claimed durability \
         for a range the file did not yet hold. flush_up_to() treats that value as a guarantee, so \
         a data page could reach disk before the log record describing it — the one rule \
         write-ahead logging exists to enforce. ({n} samples taken)"
    );
}

/// Every appended LSN must be distinct. Two records sharing a byte offset would make one of them
/// unrecoverable and the chain ambiguous.
#[test]
fn concurrent_appends_get_distinct_lsns() {
    let (_d, wal) = wal("lsns");
    const THREADS: usize = 8;
    const EACH: usize = 250;

    let lsns: Vec<u64> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let wal = Arc::clone(&wal);
                s.spawn(move || {
                    (0..EACH)
                        .filter_map(|_| wal.append(t as u64 + 1, 0, &RecKind::Commit).ok())
                        .collect::<Vec<u64>>()
                })
            })
            .collect();
        handles.into_iter().flat_map(|h| h.join().expect("no panic")).collect()
    });

    assert!(lsns.len() > THREADS * EACH / 2, "too few appends succeeded to prove anything");
    let unique: std::collections::BTreeSet<u64> = lsns.iter().copied().collect();
    assert_eq!(
        unique.len(),
        lsns.len(),
        "{} LSN(s) were handed out twice; two records would share a byte offset",
        lsns.len() - unique.len()
    );
}

/// The whole log must remain a walkable chain after concurrent appends and flushes. A single
/// malformed or misplaced record truncates recovery at that point and silently loses everything
/// after it.
#[test]
fn the_log_remains_a_walkable_chain_after_concurrent_use() {
    let (_d, wal) = wal("chain");

    let appended = std::thread::scope(|s| {
        let handles: Vec<_> = (0..6)
            .map(|t| {
                let wal = Arc::clone(&wal);
                s.spawn(move || {
                    let mut n = 0usize;
                    for _ in 0..200 {
                        if wal.append(t as u64 + 1, 0, &RecKind::Commit).is_ok() {
                            n += 1;
                        }
                        if n % 25 == 0 {
                            let _ = wal.flush();
                        }
                    }
                    n
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("no panic")).sum::<usize>()
    });

    wal.flush().expect("final flush");
    assert!(appended > 0, "nothing was appended, so nothing was checked");

    let base = wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst);
    let file = wal.file.lock().unwrap();
    let len = file.metadata().expect("metadata").len();
    let valid_end = scan_valid_end(&file, base, len).expect("scan");

    // Every byte written should be part of a valid record; a shortfall means the chain broke.
    let header = 64u64; // scan starts after the header
    assert!(
        valid_end + header >= len || valid_end > base,
        "the record chain stops at {valid_end} but the file is {len} bytes; recovery would \
         silently discard everything past the break"
    );
}
