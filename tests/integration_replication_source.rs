//! E2 — a primary must never ship a record it has not durably written.
//!
//! This is the rule the whole scheme rests on. If the source streamed from the in-memory buffer,
//! a replica could hold records the primary loses on a crash. The replica is then **ahead of its
//! primary**, and that is not lag — it is divergence, with no later step able to reconcile it.
//! Lag is a normal state for a replica; being ahead is a corrupt one.
//!
//! The frontier used is `flushed_lsn`, and D22 is the reason it can be trusted at all: it used to
//! over-report by ~117,000 bytes under concurrent flushes, which would have made this guarantee
//! decorative.
//!
//! These tests start from `src.start_lsn()`, not from 0. The first version assumed the log begins
//! at the origin and every one of them failed with "lsn 0 is below the log's base" — a true answer
//! to the wrong question. That is why `start_lsn()` exists at all: a fresh replica has to be able
//! to ask where the log actually begins, because `truncate` moves it.

use std::sync::Arc;

use ferrodb::replication::ReplicationSource;
use ferrodb::wal::log::{RecKind, WalManager};

fn wal(tag: &str) -> (tempfile::TempDir, Arc<WalManager>) {
    let dir = tempfile::tempdir().unwrap();
    let w = WalManager::new(dir.path().join(format!("{tag}.wal"))).expect("wal");
    (dir, Arc::new(w))
}

/// **The load-bearing rule.** Unflushed records are invisible to a replica.
#[test]
fn nothing_is_shipped_before_it_is_durable() {
    let (_d, wal) = wal("durable");
    let src = ReplicationSource::new(&wal);

    for _ in 0..20 {
        wal.append(1, 0, &RecKind::Commit).expect("append");
    }

    // Appended but not flushed: the primary must offer nothing at all.
    let (bytes, next) = src.read_from(src.start_lsn(), 1 << 20).expect("read");
    assert!(
        bytes.is_empty(),
        "{} bytes were offered to a replica before the primary had written them; the replica \
         would be ahead of its primary after a crash",
        bytes.len()
    );
    assert_eq!(next, src.start_lsn(), "the frontier moved without anything being durable");

    // Now make them durable, and they become shippable.
    wal.flush().expect("flush");
    let (bytes, next) = src.read_from(src.start_lsn(), 1 << 20).expect("read");
    assert!(!bytes.is_empty(), "nothing was shipped even after a flush");
    assert_eq!(next, src.durable_lsn(), "the source stopped short of its own durable frontier");
}

/// Records appended after a flush must stay invisible until the next one — the frontier holds
/// while the log keeps growing behind it.
#[test]
fn the_frontier_holds_while_more_records_pile_up_behind_it() {
    let (_d, wal) = wal("frontier");
    let src = ReplicationSource::new(&wal);

    for _ in 0..10 {
        wal.append(1, 0, &RecKind::Commit).unwrap();
    }
    wal.flush().unwrap();
    let durable_then = src.durable_lsn();
    let (first_batch, after_first) = src.read_from(src.start_lsn(), 1 << 20).unwrap();
    assert_eq!(after_first, durable_then);

    // More records, deliberately not flushed.
    for _ in 0..10 {
        wal.append(2, 0, &RecKind::Commit).unwrap();
    }
    let (again, after_again) = src.read_from(after_first, 1 << 20).unwrap();
    assert!(
        again.is_empty(),
        "{} bytes past the durable frontier were offered",
        again.len()
    );
    assert_eq!(after_again, after_first, "the frontier advanced without a flush");

    // And after flushing, exactly the new ones appear.
    wal.flush().unwrap();
    let (second_batch, _) = src.read_from(after_first, 1 << 20).unwrap();
    assert!(!second_batch.is_empty(), "the second batch never became visible");
    assert_ne!(first_batch, second_batch, "the same bytes were shipped twice");
}

/// The shipped bytes must be the log's own bytes. Re-serialising a parsed record would mean the
/// replica validates a CRC this process computed rather than one that describes what crossed the
/// wire.
#[test]
fn shipped_bytes_are_the_logs_own_frames() {
    let (_d, wal) = wal("verbatim");
    let src = ReplicationSource::new(&wal);

    let lsn = wal.append(7, 0, &RecKind::Commit).unwrap();
    wal.flush().unwrap();

    let (bytes, _next) = src.read_from(lsn, 1 << 20).unwrap();
    assert!(bytes.len() >= 33, "frame is impossibly short: {}", bytes.len());

    // The frame's own length prefix must agree with what was shipped, and its embedded LSN must be
    // the one asked for — i.e. these are real frames lifted out of the log.
    let total = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    assert_eq!(total, bytes.len(), "frame length prefix disagrees with the shipped byte count");
    let embedded = u64::from_be_bytes(bytes[4..12].try_into().unwrap());
    assert_eq!(embedded, lsn, "the shipped frame is not the record that was asked for");
}

/// A replica already at the frontier gets an empty answer rather than an error or a re-send.
#[test]
fn a_caught_up_replica_is_told_nothing_rather_than_re_sent_everything() {
    let (_d, wal) = wal("caught");
    let src = ReplicationSource::new(&wal);
    wal.append(1, 0, &RecKind::Commit).unwrap();
    wal.flush().unwrap();

    let frontier = src.durable_lsn();
    let (bytes, next) = src.read_from(frontier, 1 << 20).unwrap();
    assert!(bytes.is_empty(), "a caught-up replica was sent {} bytes", bytes.len());
    assert_eq!(next, frontier);
}

/// The batch limit must be respected, and must still return a coherent frontier so the next
/// request continues exactly where this one stopped.
#[test]
fn a_byte_limit_splits_the_stream_without_losing_its_place() {
    let (_d, wal) = wal("limited");
    let src = ReplicationSource::new(&wal);
    for _ in 0..50 {
        wal.append(1, 0, &RecKind::Commit).unwrap();
    }
    wal.flush().unwrap();
    let frontier = src.durable_lsn();

    // Walk the whole log in small batches; the pieces must reassemble into the whole.
    let mut at = src.start_lsn();
    let mut total = Vec::new();
    let mut rounds = 0;
    while at < frontier {
        let (bytes, next) = src.read_from(at, 64).unwrap();
        assert!(!bytes.is_empty(), "a batch below the frontier returned nothing at lsn {at}");
        assert!(next > at, "the frontier did not advance from {at}");
        total.extend_from_slice(&bytes);
        at = next;
        rounds += 1;
        assert!(rounds < 1000, "batching did not terminate");
    }
    assert!(rounds > 1, "the limit never actually split the stream, so it was not tested");

    let (whole, _) = src.read_from(src.start_lsn(), 1 << 20).unwrap();
    assert_eq!(total, whole, "the batched stream does not reassemble into the whole log");
}
