//! D247 — writing back a dirty ARENA page must not flush the WAL.
//!
//! The CLI and pgserver build `ArenaPageStore` over the same WAL-attached pool as the tables.
//! `page_lsn_of` read every page whose first byte is 0 as a heap page. An arena page begins with its
//! birth epoch as a big-endian u64, so for any epoch below 2^56 byte 0 is 0, and bytes 11..19 (the
//! arena id's low byte, the checksum, the type and the flags) were taken for an LSN of about 2^24 to
//! 2^56. `wal_gate` then flushed and fsynced the log up to it. A commit always leaves its `TxnEnd`
//! in the buffer, so the first dirty arena write-back after any commit paid a WAL write and an
//! fsync.
//!
//! The arena page is changed AFTER a record is appended. A gate that flushed through a page's
//! last-change mark (`Frame::wal_mark`, D216) would flush for it too, so passing here also means
//! the page was recognised as an arena page, not merely that nothing was pending.
//!
//! One test in its own binary, so the process-wide `FSYNC_CALLS` has no other writer.
//!
//! Pre-registered from source, UNBUILT: FAILS at `00f4c39` at the `flushed_lsn` assertion.

use std::fs::OpenOptions;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::branch::types::{ArenaId, Epoch};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::{stamp_checksum, PageHeader, PageType};
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::heap_file_manager::HeapFileManager;
use ferrodb::storage::tuple::Tuple;
use ferrodb::wal::log::{fsync_counters, RecKind, WalManager};
use ferrodb::wal::txn::TxnManager;

#[test]
fn writing_back_a_dirty_arena_page_does_not_flush_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new().read(true).write(true).create(true).open(dir.path().join("d247.db")).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join("d247.wal")).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());

    let arena_page = bp.new_page().unwrap();
    assert!(arena_page > 0, "premise failed: page 0 would read as a heap page naming itself");
    // Something waiting in the log buffer, as a commit's TxnEnd always is.
    wal.append(999, 0, &RecKind::Begin).unwrap();
    let frame_i = bp.fetch_page(arena_page).unwrap();
    {
        let mut frame = bp.frame_write(frame_i);
        let mut data = [0u8; PAGE_SIZE];
        PageHeader::new(Epoch(1), ArenaId(1), PageType::BTreeLeaf).write_to(&mut data);
        data[24..32].copy_from_slice(b"payload!");
        stamp_checksum(&mut data);
        frame.data = data;
    }
    bp.unpin_page(arena_page, true);

    let flushed = wal.flushed_lsn.load(Ordering::SeqCst);
    assert!(flushed < wal.next_lsn.load(Ordering::SeqCst), "premise failed: nothing is waiting in the log buffer");
    let (fsyncs, _) = fsync_counters();
    bp.flush_all().unwrap();
    assert_eq!(
        wal.flushed_lsn.load(Ordering::SeqCst),
        flushed,
        "writing back an arena page flushed the WAL: its header was read as a heap page's LSN"
    );
    assert_eq!(fsync_counters().0, fsyncs, "writing back an arena page cost a WAL fsync");

    // The instrument can see a flush: a heap page written ahead of its buffered record does flush.
    let t = txn.begin().unwrap();
    let mut heap = HeapFileManager::new(bp.clone()).unwrap();
    heap.set_transaction(txn.clone(), t);
    heap.insert(Tuple::new(vec![1, 2, 3])).unwrap();
    let before = wal.flushed_lsn.load(Ordering::SeqCst);
    bp.flush_all().unwrap();
    assert!(
        wal.flushed_lsn.load(Ordering::SeqCst) > before,
        "premise failed: a heap page ahead of its record did not flush the log, so the assertion above could not see one either"
    );
    assert!(fsync_counters().0 > fsyncs, "premise failed: FSYNC_CALLS did not move for a real WAL flush");
    txn.abort(t).unwrap();
}
