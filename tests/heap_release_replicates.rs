//! D213 on the redo path: a replica frees a committed delete's slot only at its `HeapRelease`, and
//! redo tells a CLR's delete (frees) from a forward one (retires).
//!
//! Redo is shared: a replica applies the primary's stream through `wal::recovery::apply_redo`, the
//! function recovery's redo uses, so these pin both. The stream is written by hand, so the page
//! arithmetic is exact and no executor is involved. INFERRED from source and never run.

use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::replication::{ReplicaApplier, ReplicationSource};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::storage::heap_page::{Page, RETIRED};
use ferrodb::wal::log::{RecKind, WalManager};

const PAGE: u32 = 10;

/// A primary's log, and a replica with its own pages. The shape of `integration_replica_applier`.
struct Pair {
    _dir: tempfile::TempDir,
    wal: Arc<WalManager>,
    replica_bp: Arc<BufferPoolManager>,
    applier: ReplicaApplier,
}

fn pair(tag: &str) -> Pair {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{tag}-primary.wal"))).unwrap());
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}-replica.db")))
        .unwrap();
    let replica_bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let applier = ReplicaApplier::new(Arc::clone(&replica_bp), ReplicationSource::new(&wal).start_lsn());
    Pair { _dir: dir, wal, replica_bp, applier }
}

impl Pair {
    fn log(&self, txn: u64, kind: RecKind) -> u64 {
        self.wal.append(txn, 0, &kind).expect("append")
    }

    fn insert(&self, txn: u64, slot: u16, len: usize, byte: u8) -> u64 {
        self.log(txn, RecKind::HeapInsert { dir_root: 1, page_id: PAGE, slot, tuple: vec![byte; len] })
    }

    /// Flush, ship everything past what the replica has applied, and apply it.
    fn ship(&self) -> Result<(), ferrodb::error::FerroError> {
        self.wal.flush().expect("flush");
        let (bytes, next) = ReplicationSource::new(&self.wal)
            .read_from(self.applier.applied_lsn(), 1 << 20)
            .expect("read");
        if bytes.is_empty() {
            return Ok(());
        }
        self.applier.apply(next - bytes.len() as u64, &bytes).map(|_| ())
    }

    fn page(&self) -> Page {
        let idx = self.replica_bp.fetch_page(PAGE).expect("fetch");
        let page = Page::deserialize(self.replica_bp.frames[idx].read().unwrap().data).expect("page");
        self.replica_bp.unpin_page(PAGE, false);
        page
    }
}

fn room(page: &Page) -> usize {
    page.get_free_space_end() as usize - page.get_free_space_start() as usize
}

/// A 3900 B tuple (slot 0, at the top) and a 100 B one (slot 1, the lowest) leave 65 B free. Txn 2
/// deletes slot 1 and commits; txn 3 then inserts 120 B, which needs 124 B with its slot and fits
/// only once slot 1's 100 B are free. On the primary they are free from txn 2's release on, so the
/// replica must free them at the same record, and not before.
#[test]
fn a_replica_frees_a_committed_delete_only_at_its_release() {
    let p = pair("release");
    p.insert(1, 0, 3900, 1);
    p.insert(1, 1, 100, 2);
    p.log(1, RecKind::Commit);
    p.log(2, RecKind::HeapDelete { dir_root: 1, page_id: PAGE, slot: 1, old: vec![2; 100] });
    p.log(2, RecKind::Commit);
    p.ship().expect("the replica refused the delete and its commit");

    let page = p.page();
    assert_eq!(page.slot_arr[1].length, 100 | RETIRED, "a forward delete did not RETIRE its slot on the replica");
    assert_eq!(room(&page), 65, "the replica freed a delete's bytes before its release");

    p.log(2, RecKind::HeapRelease { dir_root: 1, page_id: PAGE, slot: 1 });
    p.insert(3, 2, 120, 3);
    p.log(3, RecKind::Commit);
    p.ship().expect("the replica could not place an insert that fits only in the released bytes");

    let page = p.page();
    assert!(page.slot_arr[1].is_free(), "the release did not free the slot on the replica");
    assert_eq!(page.read(2).expect("slot 2").data, vec![3; 120], "the insert after the release is not on the replica");
    assert_eq!(page.read(0).expect("slot 0").data, vec![1; 3900], "the tuple above the released one changed");
}

/// Txn 2's insert is rolled back: its CLR carries a `HeapDelete`, which FREES the slot, because an
/// undone insert has no rollback of its own to wait for. Txn 3's forward delete, still open,
/// RETIRES its slot. One `redo_one` does both, told apart only by whether the record came in a CLR.
#[test]
fn a_clrs_delete_frees_its_slot_and_a_forward_one_retires_it() {
    let p = pair("clr");
    p.insert(1, 0, 50, 1);
    p.log(1, RecKind::Commit);
    let undone = p.insert(2, 1, 50, 2);
    p.log(2, RecKind::Abort);
    p.log(2, RecKind::Clr {
        undone_lsn: undone,
        undo_next: 0,
        redo: Box::new(RecKind::HeapDelete { dir_root: 1, page_id: PAGE, slot: 1, old: Vec::new() }),
    });
    p.log(2, RecKind::TxnEnd);
    p.log(3, RecKind::HeapDelete { dir_root: 1, page_id: PAGE, slot: 0, old: vec![1; 50] });
    p.ship().expect("the replica refused the stream");

    let page = p.page();
    assert!(page.slot_arr[1].is_free(), "a CLR's delete (an undone insert) retired its slot instead of freeing it");
    assert_eq!(page.slot_arr[0].length, 50 | RETIRED, "an open transaction's forward delete did not retire its slot");
}
