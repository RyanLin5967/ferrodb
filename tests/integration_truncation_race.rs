//! A primary must not ship a frame under an LSN that is not the frame's own.
//!
//! `ReplicationSource::read_from` samples the durable frontier once and then walks the log one
//! record at a time. That looks like a check-then-act spanning the whole walk — the shape behind
//! most defects in this repo — because `WalManager::truncate` moves `base_lsn` and empties the
//! file, and every byte offset in the WAL is computed from `base_lsn`.
//!
//! # This test does not demonstrate a bug. It demonstrates why there is not one.
//!
//! I wrote it expecting to catch a mis-labelled frame, then disabled the detector in `read_from`
//! and ran it three times: `truncations=6 batches=50 refusals=6`, zero violations, every time. The
//! hypothesis was wrong, and the reason is a property of `truncate` that is easy to miss — it sets
//! `base_lsn` to `next_lsn`, past *every* record in the log, never to an interior point. Any LSN a
//! walk is still holding is therefore below the new base, and `raw_frame` reaches its offset via
//! `lsn.checked_sub(base_lsn)`, which returns `None`. Stale offsets fail closed; they do not shift.
//!
//! So this is a **regression test for a property, not for a fix**. It matters because that property
//! is load-bearing and invisible: the day `truncate` learns to discard a prefix instead of the whole
//! log, the base lands mid-log, stale offsets stop failing closed, and this test starts failing.
//! That is exactly when someone needs to be told.
//!
//! The invariant asserted is the narrowest one that would catch it: **for every non-empty batch,
//! the first frame's embedded LSN equals the position the source says the batch starts at.**
//!
//! Frame layout, from `raw_frame` and the applier's own validation: `u32` length, then a `u64` LSN
//! at bytes 4..12.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use ferrodb::replication::ReplicationSource;
use ferrodb::wal::log::{RecKind, WalManager};

fn embedded_lsn(frame: &[u8]) -> u64 {
    u64::from_be_bytes(frame[4..12].try_into().unwrap())
}

#[test]
fn a_truncation_racing_a_read_never_yields_a_frame_at_the_wrong_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("race.wal")).unwrap());

    let stop = Arc::new(AtomicBool::new(false));
    let truncations = Arc::new(AtomicU64::new(0));
    let batches = Arc::new(AtomicU64::new(0));
    let refusals = Arc::new(AtomicU64::new(0));

    // A writer, so there is always something to ship.
    let writer = {
        let (wal, stop) = (Arc::clone(&wal), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut n = 0u8;
            while !stop.load(Ordering::Relaxed) {
                let _ = wal.append(
                    1,
                    0,
                    &RecKind::HeapInsert {
                        dir_root: 1,
                        page_id: 2,
                        slot: 0,
                        tuple: vec![n; 32],
                    },
                );
                let _ = wal.flush();
                n = n.wrapping_add(1);
                std::thread::yield_now();
            }
        })
    };

    // A checkpointer, truncating underneath the reader.
    let checkpointer = {
        let (wal, stop, truncations) = (Arc::clone(&wal), Arc::clone(&stop), Arc::clone(&truncations));
        std::thread::spawn(move || {
            let mut txn = 100u64;
            while !stop.load(Ordering::Relaxed) {
                // Only truncate once the log has actually grown, the way a real checkpoint does.
                // Truncating in a tight loop keeps base == frontier, so the reader never sees a
                // non-empty batch and the race is never run — which the anti-vacuity guard below
                // caught on the first attempt.
                let before = wal.base_lsn.load(Ordering::SeqCst);
                let end = wal.next_lsn.load(Ordering::SeqCst);
                if end - before > 20_000 {
                    if wal.truncate(txn).is_ok() && wal.base_lsn.load(Ordering::SeqCst) > before {
                        truncations.fetch_add(1, Ordering::Relaxed);
                    }
                    txn += 1;
                }
                std::thread::yield_now();
            }
        })
    };

    // The reader, checking every batch it is handed.
    //
    // Paced off what it is watching, not off a fixed count. A fixed 4000 iterations finished in
    // 0.02s — before the writer had produced enough log for a checkpoint to have anything to
    // discard — and the anti-vacuity guard below caught that too. It now runs until the race has
    // demonstrably happened enough times, with a deadline so a broken build cannot hang here.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut violations = Vec::new();
    while std::time::Instant::now() < deadline {
        if truncations.load(Ordering::Relaxed) >= 5
            && batches.load(Ordering::Relaxed) >= 50
            && refusals.load(Ordering::Relaxed) >= 1
        {
            break;
        }
        let src = ReplicationSource::new(&wal);
        let from = src.start_lsn();
        match src.read_from(from, 4096) {
            Ok((bytes, next)) if !bytes.is_empty() => {
                batches.fetch_add(1, Ordering::Relaxed);
                let start = next - bytes.len() as u64;
                if bytes.len() >= 12 {
                    let first = embedded_lsn(&bytes);
                    if first != start {
                        violations.push((first, start));
                    }
                }
            }
            // An empty batch, or a refusal because the range was truncated away, are both correct
            // outcomes. The bug being hunted is a batch that is returned and is WRONG.
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    msg.contains("truncated") || msg.contains("below the log's base"),
                    "read_from failed for a reason unrelated to truncation: {msg}"
                );
                refusals.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    checkpointer.join().unwrap();

    // Anti-vacuity, both directions. Without these, a run in which the checkpointer never fired or
    // the reader never got a batch would "pass" while testing nothing at all.
    let t = truncations.load(Ordering::Relaxed);
    let b = batches.load(Ordering::Relaxed);
    let r = refusals.load(Ordering::Relaxed);
    println!("truncations={t} batches={b} refusals={r}");

    assert!(t >= 5, "only {t} truncation(s) landed, so the race was barely run");
    assert!(b >= 50, "the reader received only {b} batch(es), so little was checked");

    // **The guard that makes this test mean something.** Truncations landing is not the same as
    // truncations landing INSIDE a read. `refusals` counts the times the detector actually fired,
    // so a run where the timing never lined up cannot masquerade as a run where the fix held.
    assert!(
        r >= 1,
        "the truncation detector never fired in {b} batches and {t} truncations, so this run never \
         put a truncation inside a read and proves nothing about the fix"
    );

    assert!(
        violations.is_empty(),
        "{} batch(es) were shipped under the wrong lsn out of {b} (truncations: {t}). \
         First few (embedded, claimed): {:?}",
        violations.len(),
        &violations[..violations.len().min(5)]
    );
}
