//! E3 — a replica applying a primary's shipped WAL.
//!
//! The applier's job is to be *suspicious of the wire*. Every frame is CRC-checked against the
//! bytes that actually arrived and its embedded LSN is checked against where the walk places it,
//! because the two failures that matter here are silent: corrupted bytes applied as if valid, and
//! records applied out of order.
//!
//! It must also be **idempotent**. A reconnect asks from the replica's own LSN, and the primary
//! will happily re-send a record the replica already has. Redo goes through the same
//! `apply_redo` recovery uses, which skips any record at or below the page's own LSN — so an
//! overlap is absorbed rather than applied twice.
//!
//! A batch that fails validation is refused **whole**. Applying a prefix and then erroring would
//! leave the replica at an LSN it cannot justify, which is worse than not advancing at all.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::replication::{ReplicaApplier, ReplicationSource};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_page::Page;
use ferrodb::wal::log::{RecKind, WalManager};

/// A primary with a WAL, and a replica with its own pages.
struct Pair {
    _dir: tempfile::TempDir,
    primary_wal: Arc<WalManager>,
    replica_bp: Arc<BufferPoolManager>,
}

fn pair(tag: &str) -> Pair {
    let dir = tempfile::tempdir().unwrap();
    let primary_wal =
        Arc::new(WalManager::new(dir.path().join(format!("{tag}-primary.wal"))).unwrap());

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}-replica.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let replica_bp = Arc::new(BufferPoolManager::new(dm));
    Pair { _dir: dir, primary_wal, replica_bp }
}

/// A tuple big enough to be recognisable, tagged with `n`.
fn tuple(n: u8) -> Vec<u8> {
    let mut t = vec![0u8; 40];
    t[0] = n;
    t
}

/// Write an insert on the primary and make it durable.
fn primary_insert(wal: &WalManager, txn: u64, page_id: u32, slot: u16, n: u8) -> u64 {
    let lsn = wal
        .append(
            txn,
            0,
            &RecKind::HeapInsert { dir_root: 1, page_id, slot, tuple: tuple(n) },
        )
        .expect("append");
    wal.flush().expect("flush");
    lsn
}

fn ship_all(p: &Pair, applier: &ReplicaApplier) -> u64 {
    let src = ReplicationSource::new(&p.primary_wal);
    let (bytes, next) = src.read_from(applier.applied_lsn(), 1 << 20).expect("read");
    if bytes.is_empty() {
        return applier.applied_lsn();
    }
    applier.apply(next - bytes.len() as u64, &bytes).expect("apply")
}

#[test]
fn a_replica_applies_a_shipped_insert_and_advances() {
    let p = pair("basic");
    let src_start = ReplicationSource::new(&p.primary_wal).start_lsn();
    let applier = ReplicaApplier::new(Arc::clone(&p.replica_bp), src_start);

    primary_insert(&p.primary_wal, 1, 10, 0, 0xAB);
    let after = ship_all(&p, &applier);

    assert!(after > src_start, "the replica did not advance past the log's start");
    assert_eq!(applier.applied_lsn(), after);

    // The page must actually hold the row, judged from the page rather than from the applier.
    let idx = p.replica_bp.fetch_page(10).expect("fetch");
    let page = Page::deserialize(p.replica_bp.frames[idx].read().unwrap().data).expect("page");
    p.replica_bp.unpin_page(10, false);
    assert!(page.lsn > 0, "the replica's page carries no LSN, so nothing was applied");
}

/// **Idempotence.** Re-sending a batch the replica already has must change nothing.
#[test]
fn re_applying_the_same_batch_is_a_no_op() {
    let p = pair("idem");
    let src = ReplicationSource::new(&p.primary_wal);
    let applier = ReplicaApplier::new(Arc::clone(&p.replica_bp), src.start_lsn());

    primary_insert(&p.primary_wal, 1, 20, 0, 0x11);
    let (bytes, next) = src.read_from(src.start_lsn(), 1 << 20).unwrap();
    let start = next - bytes.len() as u64;

    let first = applier.apply(start, &bytes).expect("first apply");
    let page_lsn_after_first = {
        let idx = p.replica_bp.fetch_page(20).unwrap();
        let l = Page::deserialize(p.replica_bp.frames[idx].read().unwrap().data).unwrap().lsn;
        p.replica_bp.unpin_page(20, false);
        l
    };

    // Exactly what a reconnect does: the primary re-sends from a point the replica already passed.
    let second = applier.apply(start, &bytes).expect("re-apply must be accepted, not rejected");
    assert_eq!(second, first, "the frontier moved on a duplicate batch");

    let page_lsn_after_second = {
        let idx = p.replica_bp.fetch_page(20).unwrap();
        let l = Page::deserialize(p.replica_bp.frames[idx].read().unwrap().data).unwrap().lsn;
        p.replica_bp.unpin_page(20, false);
        l
    };
    assert_eq!(
        page_lsn_after_second, page_lsn_after_first,
        "re-applying the same records changed the page; redo is not idempotent"
    );
}

/// A corrupted byte on the wire must be refused, not applied.
#[test]
fn a_frame_whose_crc_fails_is_refused() {
    let p = pair("crc");
    let src = ReplicationSource::new(&p.primary_wal);
    let applier = ReplicaApplier::new(Arc::clone(&p.replica_bp), src.start_lsn());

    primary_insert(&p.primary_wal, 1, 30, 0, 0x22);
    let (mut bytes, next) = src.read_from(src.start_lsn(), 1 << 20).unwrap();
    let start = next - bytes.len() as u64;

    // Flip a byte in the record body, leaving the length prefix intact.
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;

    let err = applier.apply(start, &bytes).expect_err("a corrupted frame was accepted");
    assert!(format!("{err}").contains("CRC"), "refused for the wrong reason: {err}");
    assert_eq!(
        applier.applied_lsn(),
        src.start_lsn(),
        "the replica advanced despite refusing the batch"
    );
}

/// A frame claiming an LSN other than its position in the stream must be refused — that is the
/// signature of records arriving out of order, which would corrupt the replica silently.
#[test]
fn a_frame_at_the_wrong_lsn_is_refused() {
    let p = pair("order");
    let src = ReplicationSource::new(&p.primary_wal);
    let applier = ReplicaApplier::new(Arc::clone(&p.replica_bp), src.start_lsn());

    primary_insert(&p.primary_wal, 1, 40, 0, 0x33);
    let (bytes, next) = src.read_from(src.start_lsn(), 1 << 20).unwrap();
    let start = next - bytes.len() as u64;

    // Same bytes, told they begin somewhere else.
    let err = applier
        .apply(start + 8, &bytes)
        .expect_err("a frame was applied at an lsn it does not claim");
    assert!(format!("{err}").contains("out of order"), "refused for the wrong reason: {err}");
}

/// A batch is all-or-nothing: a bad frame at the end must prevent the good ones before it from
/// being applied, or the replica ends up at an LSN it cannot account for.
#[test]
fn a_batch_with_one_bad_frame_applies_none_of_it() {
    let p = pair("atomic");
    let src = ReplicationSource::new(&p.primary_wal);
    let applier = ReplicaApplier::new(Arc::clone(&p.replica_bp), src.start_lsn());

    primary_insert(&p.primary_wal, 1, 50, 0, 0x44);
    primary_insert(&p.primary_wal, 1, 51, 0, 0x55);
    let (mut bytes, next) = src.read_from(src.start_lsn(), 1 << 20).unwrap();
    let start = next - bytes.len() as u64;

    // Corrupt the LAST byte, which is inside the final frame's CRC.
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;

    assert!(applier.apply(start, &bytes).is_err(), "a batch with a bad frame was accepted");
    assert_eq!(applier.applied_lsn(), src.start_lsn(), "the replica advanced on a refused batch");

    // And the FIRST record, which was perfectly valid, must not have been applied either.
    //
    // The page is expected to be ABSENT, not merely empty: the batch was refused before anything
    // was materialised, so the replica's file never grew. That is stronger evidence than a zero
    // LSN would be. The first version of this assertion fetched the page and unwrapped, which
    // failed on the EOF that absence produces — the code was more correct than the test.
    match p.replica_bp.fetch_page(50) {
        Err(_) => { /* never materialised: nothing from the batch was applied */ }
        Ok(idx) => {
            let page = Page::deserialize(p.replica_bp.frames[idx].read().unwrap().data).unwrap();
            p.replica_bp.unpin_page(50, false);
            assert_eq!(
                page.lsn, 0,
                "a valid frame from a refused batch was applied; the batch is not all-or-nothing"
            );
        }
    }
}

/// Several batches in sequence must land the replica exactly where the primary's durable frontier
/// is — the convergence property the whole scheme exists for.
#[test]
fn streaming_in_batches_converges_on_the_primarys_frontier() {
    let p = pair("converge");
    let src = ReplicationSource::new(&p.primary_wal);
    let applier = ReplicaApplier::new(Arc::clone(&p.replica_bp), src.start_lsn());

    for i in 0..25u8 {
        primary_insert(&p.primary_wal, 1, 100 + i as u32, 0, i);
    }

    // Small batches, so the stream is split many times.
    let mut rounds = 0;
    while applier.applied_lsn() < src.durable_lsn() {
        let (bytes, next) = src.read_from(applier.applied_lsn(), 128).unwrap();
        assert!(!bytes.is_empty(), "a batch below the frontier returned nothing");
        applier.apply(next - bytes.len() as u64, &bytes).expect("apply");
        rounds += 1;
        assert!(rounds < 500, "streaming did not terminate");
    }

    assert!(rounds > 1, "the stream was never split, so batching was not exercised");
    assert_eq!(
        applier.applied_lsn(),
        src.durable_lsn(),
        "the replica did not converge on the primary's durable frontier"
    );
}
