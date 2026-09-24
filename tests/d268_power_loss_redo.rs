//! D267 + D268: redo after a POWER LOSS, in which every page-file write since the last checkpoint is
//! lost and only the log, which a COMMIT syncs, survives.
//!
//! A kill -9 cannot show either defect: the OS page cache survives it, so every write that returned
//! is kept. The lost-write double here is the one the crate already has: `storage::sim::SimFabric`
//! under `Durability::SyncOnly`, whose `restart()` keeps only synced bytes (Rule Zero; wired as
//! `tests/sim_durability.rs::open` wires it).
//!
//! - **D268:** a page freed and reused keeps its old owner's image on disk, with its own page id. When
//!   the reuse's writes are lost, redo parses that image and applies the new owner's first insert to
//!   it, and the old owner's rows become the new table's.
//! - **D267:** allocating a page sets its bitmap bit with an unsynced write. When that write is lost,
//!   recovery rebuilds the page from the log, and the bit stays clear, so the allocator hands the
//!   page out again.
//!
//! Lane report: artie-research `frontier/lane_d268_power_loss_redo.md` §2, tests 1-3. INFERRED from
//! source and never run.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::heap_file_manager::HeapFileManager;
use ferrodb::storage::sim::{Durability, SimFabric};
use ferrodb::storage::tuple::Tuple;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::recovery::recover;
use ferrodb::wal::txn::TxnManager;

const DB: &str = "d268.db";
const WAL: &str = "d268.wal";

struct Db {
    bp: Arc<BufferPoolManager>,
    wal: Arc<WalManager>,
    txn: Arc<TxnManager>,
}

fn open(fabric: &Arc<SimFabric>) -> Db {
    let dm = Arc::new(DiskManager::with_storage(fabric.open(DB)).expect("disk manager"));
    let bp = Arc::new(BufferPoolManager::new(dm));
    let wal = Arc::new(WalManager::with_storage(fabric.open(WAL), WAL.into()).expect("wal"));
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { bp, wal, txn }
}

/// Every row of the heap rooted at `dir`, in scan order (page, then slot).
fn rows(db: &Db, dir: u32) -> Vec<Vec<u8>> {
    HeapFileManager::open(dir, db.bp.clone())
        .scan()
        .collect::<Result<Vec<_>, _>>()
        .expect("scan")
        .into_iter()
        .map(|(_, t)| t.data)
        .collect()
}

/// Test 1. A reused page whose writes were all lost holds only its new owner's rows after redo.
#[test]
fn d268_a_reused_page_whose_writes_were_lost_holds_only_its_new_owners_rows() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let db = open(&fabric);
    let (a0, a1, b0) = (vec![0xA0u8; 40], vec![0xA1u8; 40], vec![0xB0u8; 40]);

    // Heap A: two rows on page P. Then a0 is deleted, and its commit releases the slot, so slot 0 is
    // free and slot 1 holds a1.
    let t = db.txn.begin().unwrap();
    let mut a = HeapFileManager::new(db.bp.clone()).unwrap();
    a.set_transaction(db.txn.clone(), t);
    let r0 = a.insert(Tuple::new(a0)).unwrap();
    let r1 = a.insert(Tuple::new(a1)).unwrap();
    db.txn.commit(t).unwrap();
    let p = r0.page_id;
    assert_eq!(r1.page_id, p, "premise: A's two rows are not on one page");
    let t = db.txn.begin().unwrap();
    a.set_transaction(db.txn.clone(), t);
    a.delete(r0).unwrap();
    db.txn.commit(t).unwrap();

    // Heap B's directory, then everything durable. P's image on disk is A's.
    let mut b = HeapFileManager::new(db.bp.clone()).unwrap();
    let b_dir = b.first_directory_page_id;
    db.txn.checkpoint().unwrap();
    // P freed, as a DROP frees it, and the free made durable. P's bytes on disk are still A's.
    db.bp.free_page(p).unwrap();
    db.txn.checkpoint().unwrap();

    // B's first row. The allocator hands out its lowest clear bit, which is P.
    let t = db.txn.begin().unwrap();
    b.set_transaction(db.txn.clone(), t);
    let rb = b.insert(Tuple::new(b0.clone())).unwrap();
    db.txn.commit(t).unwrap();
    assert_eq!(rb.page_id, p, "premise: B's first row did not reuse A's freed page {p}");

    // The power loss. Only synced bytes survive: P holds A's image again, and the log holds B's insert.
    let db = open(&fabric.restart());
    recover(&db.txn).expect("recover");
    assert_eq!(
        rows(&db, b_dir),
        vec![b0],
        "B holds a row it never wrote: redo applied B's first insert onto A's stale image of page {p}"
    );
}

/// Test 2. After a power loss, the allocator does not hand out a page the log rebuilt. Its first
/// half (the open succeeds and the row is there) is what the retracted D267 headline predicted would
/// fail: `recover`'s pre-loop extends the file first.
#[test]
fn d267_after_a_power_loss_the_allocator_does_not_hand_out_a_page_the_log_rebuilt() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let db = open(&fabric);
    let b0 = vec![0xB0u8; 40];
    let mut b = HeapFileManager::new(db.bp.clone()).unwrap();
    let b_dir = b.first_directory_page_id;
    db.txn.checkpoint().unwrap();
    let t = db.txn.begin().unwrap();
    b.set_transaction(db.txn.clone(), t);
    let p = b.insert(Tuple::new(b0.clone())).unwrap().page_id;
    db.txn.commit(t).unwrap();

    // The power loss cuts page P off the end of the file, and loses its bitmap bit.
    let rebooted = fabric.restart();
    let synced_len = rebooted.durable_image()[DB].len() as u64;
    assert!(
        synced_len <= p as u64 * PAGE_SIZE as u64,
        "premise: page {p} survived in a {synced_len}-byte file, so it was never past the end"
    );
    let db = open(&rebooted);
    recover(&db.txn).unwrap_or_else(|e| panic!("the open failed after a power loss cut page {p} off the file: {e}"));
    assert_eq!(rows(&db, b_dir), vec![b0], "redo did not rebuild B's row on page {p}");

    // As `open_recovered` does: a checkpoint makes recovery's work durable. Then the next allocation.
    db.txn.checkpoint().expect("checkpoint");
    let next = db.bp.disk_manager.allocate().expect("allocate");
    assert_ne!(
        next, p,
        "the allocator handed out page {p}, which the log rebuilt and B's directory lists: its bitmap \
         bit was lost with the power and nothing at open set it again"
    );
}

/// Test 3. An init insert does not reset a page that already holds its own image at or past the
/// record: a page can carry rows no record describes, written by `alter::rewrite_heap`'s unlogged
/// handle while a pin keeps the log. Passes before D268 too; it pins why D268's reset is gated.
#[test]
fn d268_an_init_insert_keeps_a_page_that_already_holds_its_own_later_image() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let db = open(&fabric);
    let (b0, u) = (vec![0xB0u8; 40], vec![0x0Du8; 40]);
    let mut b = HeapFileManager::new(db.bp.clone()).unwrap();
    let b_dir = b.first_directory_page_id;
    db.txn.checkpoint().unwrap();
    let t = db.txn.begin().unwrap();
    b.set_transaction(db.txn.clone(), t);
    let p = b.insert(Tuple::new(b0.clone())).unwrap().page_id;
    db.txn.commit(t).unwrap();

    // A pin at the log's base keeps every record through the next checkpoint.
    let base = db.wal.base_lsn.load(Ordering::SeqCst);
    let _pin = db.wal.pin(base).expect("pin");
    // An unlogged row onto P, written as `alter::rewrite_heap` writes (`txn: None`): no record, and
    // P's LSN stays at b0's record.
    let unlogged = HeapFileManager::open(b_dir, db.bp.clone());
    let ru = unlogged.insert(Tuple::new(u.clone())).unwrap();
    assert_eq!(ru.page_id, p, "premise: the unlogged row did not land on page {p}");
    db.txn.checkpoint().expect("checkpoint");
    assert_eq!(
        db.wal.base_lsn.load(Ordering::SeqCst),
        base,
        "premise: the checkpoint truncated the log despite the pin, so b0's record is not replayed"
    );

    let db = open(&fabric.restart());
    recover(&db.txn).expect("recover");
    assert_eq!(
        rows(&db, b_dir),
        vec![b0, u],
        "redo reset page {p}, which already held its own image at b0's record, and lost the unlogged row"
    );
}
