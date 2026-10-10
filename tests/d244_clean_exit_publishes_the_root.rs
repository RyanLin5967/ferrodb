//! D244 review F7 — **a clean exit publishes the branch catalog's root, so a publish that failed
//! and was never retried cannot leave a file the next open refuses.**
//!
//! Since D244 a publish that fails on a mutation's exit records nothing, and the NEXT mutation
//! retries it (`tests/d244_publish_root.rs`, A2). A process that makes no further mutation never
//! gets that retry. Meanwhile an eviction may write the split pages to disk. (A sync would not: since
//! D244 review 2, R2-2, `durable()` publishes an owed root before it flushes; `d244_publish_root.rs`,
//! A3.) Page 1 was never even dirty, so it keeps naming the old root. The next open searches for the
//! header key `[0x07]` under that root, which never holds it after a split, and refuses.
//!
//! `cli::exit_sequence` is the function `run_cli` calls once its REPL ends. It now publishes the
//! branch catalog's root and syncs it before the database and arena checkpoints.
//!
//! # Red evidence
//!
//! This file calls `exit_sequence` and `TableBranchCatalog::publish_root_durably`, and neither
//! exists at `9aa6968`, so it cannot compile there. It lives apart from `d244_publish_root.rs` so
//! that file's red tree still builds. Its red is shown by two mutants: MC removes the publish from
//! `exit_sequence`, and MD publishes without syncing. The lane report lists both.
//!
//! # The fault injector
//!
//! The same shape as `d244_publish_root.rs`'s, cut down to the one rule used here: fail the next
//! read of page 1, once. It is an in-memory file behind `DiskManager::with_storage`, and it can be
//! copied, so a trial open can read the file as it stands without touching it.

use std::fs::OpenOptions;
use std::io;
use std::sync::{Arc, Mutex};

use ferrodb::branch::table_catalog::SIDECAR_HEADER_PAGE;
use ferrodb::branch::{ArenaPageStore, BranchCatalog, BranchId, LeaseDeadline, TableBranchCatalog};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::cli::cli::exit_sequence;
use ferrodb::error::FerroError;
use ferrodb::storage::disk_manager::{DiskManager, PAGE_SIZE};
use ferrodb::storage::storage::Storage;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Where page 1 starts in the file.
const PAGE_ONE: u64 = SIDECAR_HEADER_PAGE as u64 * PAGE_SIZE as u64;
/// Every child's lease: one value, so deadline keys arrive in id order.
const LEASE: LeaseDeadline = LeaseDeadline(4_000_000_000_000);
/// Trunk's data root lives in another file; any page id will do.
const TRUNK_ROOT: u32 = 7;
/// The text the injected failure carries, so the test can tell it from any other error.
const INJECTED: &str = "d244 F7 injected read failure";
/// The arena's floor above the database's high-water mark. Any headroom will do: nothing here
/// allocates from the arena.
const ARENA_HEADROOM: u32 = 64;

/// An in-memory file that, once armed, fails the next read of page 1 and then carries on.
struct PageOneFault {
    image: Mutex<Vec<u8>>,
    armed: Mutex<bool>,
    fired: Mutex<u32>,
}

impl PageOneFault {
    fn new(image: Vec<u8>) -> Arc<Self> {
        Arc::new(PageOneFault { image: Mutex::new(image), armed: Mutex::new(false), fired: Mutex::new(0) })
    }

    fn arm(&self) {
        *self.armed.lock().unwrap() = true;
    }

    fn fired(&self) -> u32 {
        *self.fired.lock().unwrap()
    }

    /// A second file holding the same bytes, unarmed.
    fn copy(&self) -> Arc<Self> {
        PageOneFault::new(self.image.lock().unwrap().clone())
    }
}

impl Storage for PageOneFault {
    fn pwrite(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        let mut image = self.image.lock().unwrap();
        let end = offset as usize + buf.len();
        if image.len() < end {
            image.resize(end, 0);
        }
        image[offset as usize..end].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn pread(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        {
            let mut armed = self.armed.lock().unwrap();
            if *armed && offset == PAGE_ONE {
                *armed = false;
                *self.fired.lock().unwrap() += 1;
                return Err(io::Error::new(io::ErrorKind::Other, format!("{INJECTED}: the read of page 1")));
            }
        }
        let image = self.image.lock().unwrap();
        let start = (offset as usize).min(image.len());
        let n = buf.len().min(image.len() - start);
        buf[..n].copy_from_slice(&image[start..start + n]);
        Ok(n)
    }

    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.image.lock().unwrap().resize(len as usize, 0);
        Ok(())
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.image.lock().unwrap().len() as u64)
    }
}

/// Open what `storage` holds with a fresh pool, the way `open_sidecar` reopens a file.
fn open_branches(storage: &Arc<PageOneFault>) -> Result<TableBranchCatalog, FerroError> {
    let dm = DiskManager::with_storage(storage.clone() as Arc<dyn Storage>)?;
    let pool = Arc::new(BufferPoolManager::new(Arc::new(dm)));
    TableBranchCatalog::open_from_header(pool, SIDECAR_HEADER_PAGE)
}

/// Write every dirty page back, then drop every cached page, so the next access reads storage.
fn cold(cat: &TableBranchCatalog) {
    cat.pool_handle().flush_all().expect("flush the pool");
    cat.pool_handle().invalidate_all().expect("nothing is pinned between mutations");
}

#[test]
fn a_clean_exit_after_a_failed_publish_leaves_a_catalog_that_opens() {
    let dir = tempfile::tempdir().unwrap();

    // The database, its log and its catalog, in `run_cli`'s order: the arena is built after the
    // catalog, above the pages the catalog has allocated.
    let file = OpenOptions::new().read(true).write(true).create(true).open(dir.path().join("f7.db")).unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join("f7.wal")).unwrap());
    let txn = TxnManager::new(wal.clone(), bp.clone());
    bp.attach_wal(wal);
    let _catalog = Catalog::create(bp.clone()).expect("create the database catalog");

    // The branch catalog, in a file whose page-1 reads can be failed.
    let storage = PageOneFault::new(Vec::new());
    let dm = DiskManager::with_storage(storage.clone() as Arc<dyn Storage>).expect("disk manager");
    let (branches, header) = TableBranchCatalog::create_with_header(Arc::new(BufferPoolManager::new(Arc::new(dm))), TRUNK_ROOT)
        .expect("create the branch catalog");
    assert_eq!(header, SIDECAR_HEADER_PAGE, "premise failed: the header must be page 1, where the injector aims");
    let branches = Arc::new(branches);
    let base = bp.disk_manager.high_water().expect("high water") + ARENA_HEADROOM;
    let store = ArenaPageStore::new(bp.clone(), branches.clone() as Arc<dyn BranchCatalog>, base).expect("the arena");

    // 1. A publish that fails. Page 1 is read from storage only when a publish has a moved root to
    //    write, so the first fork whose publish reads it is the first fork that split the root.
    let root_before = branches.root_page_id();
    let mut ids = Vec::new();
    storage.arm();
    let refused = loop {
        assert!(ids.len() < 5_000, "premise failed: {} forks never moved the root", ids.len());
        cold(&branches);
        match branches.fork(BranchId::TRUNK, LEASE) {
            Ok(rec) => ids.push(rec.branch_id.id),
            Err(e) => break e.to_string(),
        }
    };
    assert_eq!(storage.fired(), 1, "premise failed: exactly one read of page 1 must have been failed");
    assert!(refused.contains(INJECTED), "premise failed: the fork failed for another reason: {refused}");
    assert_ne!(branches.root_page_id(), root_before, "premise failed: the root did not split before the failed publish");

    // 2. No further mutation. What an eviction would do: the split pages reach the file. Page 1
    //    does not, because the failed publish never wrote it. (A `durable()` here would publish
    //    first, which is A3's case, not this one.)
    branches.pool_handle().flush_all().expect("flush");
    match open_branches(&storage.copy()) {
        Ok(_) => panic!(
            "premise failed: the file opened BEFORE the exit, so there was no stale header for the exit to repair"
        ),
        Err(e) => assert!(
            e.to_string().contains("header key is missing"),
            "premise failed: the file refused before the exit, but for another reason: {e}"
        ),
    }

    // 3. The clean exit, through the function `run_cli` calls, then the arena and the branch
    //    catalog dropped. Nothing else holds the branch catalog's pool.
    // Since the merge with D230 the exit persists the database catalog before its checkpoint, so it
    // takes that catalog too: the one this fixture created in `run_cli`'s order.
    exit_sequence(&branches, &_catalog, &txn, &store, &dir.path().join("f7.arena")).expect("the clean exit");
    drop(store);
    drop(branches);

    let reopened = open_branches(&storage)
        .unwrap_or_else(|e| panic!("the branch catalog must open after a clean exit, and it refused: {e}"));
    for id in &ids {
        reopened
            .get_raw(*id)
            .unwrap_or_else(|e| panic!("branch {id} must still be in the reopened catalog: {e}"));
    }
}
