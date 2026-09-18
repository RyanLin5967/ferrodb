//! **D40's exit criterion, asked in the number the criterion is stated in.**
//!
//! `sweep_empty_extents`'s own doc said freeing empty extents "is what returns the *reserved*
//! page count to baseline rather than merely stopping its growth". D40 replaced that global scan
//! on the reap path with a sweep narrowed to the arenas the drain actually touched, and named the
//! falsifier: *"Reserved-page count is the ledger. If reserved pages do not return to the same
//! figure the global sweep reached, the narrowed sweep is missing a case."*
//!
//! So this runs `d19_leak_is_a_race`'s own workload — the one the quadratic was measured on — and
//! then runs the instrument the narrowed sweep replaced, on the same state, as a residue check.
//! A non-zero residue is a case the narrowed sweep missed.
//!
//! **It reaps branch by branch rather than through `reap_expired` on purpose.** `reap_expired`
//! ends in the crash-orphan collector, which is the global scan: letting it run first would mop
//! up exactly the residue this file exists to detect, and the test would pass while proving
//! nothing. `reap` is the narrowed path with nothing else behind it.
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::storage::disk_manager::DiskManager;

struct Fixture {
    catalog: Arc<dyn BranchCatalog>,
    store: Arc<ArenaPageStore>,
    reaper: TwoTierReaper,
    db: std::path::PathBuf,
    cat: std::path::PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.db);
        let _ = std::fs::remove_file(&self.cat);
    }
}

/// The shipped catalog, on real files. `TableBranchCatalog` is the one that ships; D19 records
/// what testing only `LogBranchCatalog` cost this project.
fn fixture(tag: &str) -> Fixture {
    let db = std::env::temp_dir().join(format!("d40-{}-{}.db", std::process::id(), tag));
    let cat = std::env::temp_dir().join(format!("d40-{}-{}.cat", std::process::id(), tag));
    let _ = std::fs::remove_file(&db);
    let _ = std::fs::remove_file(&cat);
    let file =
        std::fs::OpenOptions::new().create(true).read(true).write(true).open(&db).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog: Arc<dyn BranchCatalog> =
        Arc::new(TableBranchCatalog::open_sidecar(&cat, 1).unwrap());
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog), base).unwrap());
    let reaper = TwoTierReaper::new(Arc::clone(&catalog), Arc::clone(&store));
    Fixture { catalog, store, reaper, db, cat }
}

/// d19's inner loop: fork, claim an extent, write one novel page, abandon. Nobody calls close.
fn fork_and_write(f: &Fixture, parent: BranchId) -> BranchId {
    let rec = f.catalog.fork(parent, LeaseDeadline::from_now(50)).unwrap();
    let arena = f.store.arena_for(rec.branch_id).unwrap();
    let ep = f.catalog.next_epoch();
    let p = f.store.alloc_in_arena(arena, PageType::BTreeLeaf, ep).unwrap();
    let h = f.store.read_page(p).unwrap();
    let mut frame = h.write();
    frame.data[PAGE_HEADER_SIZE] = 0xD4;
    stamp_checksum(&mut frame.data);
    rec.branch_id
}

/// Assert the ledger came back, then run the global scan as a residue check.
fn assert_ledger_returned(f: &Fixture, live0: u32, reserved0: u32, what: &str) {
    assert_eq!(f.store.pending_len(), 0, "{what}: the pending-free log did not drain");
    assert_eq!(
        f.store.live_page_count().unwrap(),
        live0,
        "{what}: live pages did not return to baseline"
    );
    let narrowed = f.store.reserved_page_count();
    assert_eq!(
        narrowed, reserved0,
        "{what}: RESERVED pages did not return to baseline. This is the number exit criterion 8 \
         is stated in, and the narrowed sweep is what has to reach it now."
    );
    // The instrument the narrowed sweep replaced, run afterwards on the same state. Anything it
    // finds is an extent the narrowed sweep left behind — D40's named falsifier.
    assert_eq!(
        f.reaper.collect_orphaned_extents().unwrap(),
        0,
        "{what}: the global scan found extents the narrowed sweep missed"
    );
    assert_eq!(f.store.reserved_page_count(), narrowed, "{what}: the global scan moved the ledger");
}

#[test]
fn flat_fanout_returns_every_reserved_page_without_the_global_scan() {
    // d19's shape: a wide fan of childless leaves off trunk, every one abandoned. This is the
    // fast path, where `reap` frees each extent wholesale.
    let f = fixture("flat");
    let live0 = f.store.live_page_count().unwrap();
    let reserved0 = f.store.reserved_page_count();

    let branches: Vec<BranchId> = (0..1000).map(|_| fork_and_write(&f, BranchId::TRUNK)).collect();
    assert!(f.store.reserved_page_count() > reserved0, "fixture: nothing was reserved");
    assert_eq!(f.store.live_page_count().unwrap(), live0 + 1000, "fixture: the writes did not land");

    for b in branches {
        f.reaper.reap(b).unwrap();
    }
    f.reaper.drain_pending().unwrap();
    assert_ledger_returned(&f, live0, reserved0, "flat fanout");
}

#[test]
fn a_forked_chain_returns_every_reserved_page_without_the_global_scan() {
    // The slow path, which is the only one that empties an extent WITHOUT freeing it: pages are
    // parked against a live child and handed back by a later reap's drain. If the narrowed sweep
    // is missing a case, it is here — a global scan over every live arena could see those
    // extents and a sweep over "arenas this drain touched" has to be told about them.
    let f = fixture("chain");
    let live0 = f.store.live_page_count().unwrap();
    let reserved0 = f.store.reserved_page_count();

    let mut chains: Vec<Vec<BranchId>> = Vec::new();
    for _ in 0..40 {
        let mut chain = Vec::new();
        let mut cur = BranchId::TRUNK;
        for _ in 0..6 {
            cur = fork_and_write(&f, cur);
            chain.push(cur);
        }
        chains.push(chain);
    }
    assert_eq!(f.store.live_page_count().unwrap(), live0 + 240);

    // Shallowest first, so every reap but the last has a live child and takes the slow path.
    // Deepest-first (what `reap_expired` does) would make every one of these a fast-path free
    // and never park a page at all.
    for chain in &chains {
        for b in chain.iter().copied() {
            f.reaper.reap(b).unwrap();
        }
    }
    f.reaper.drain_pending().unwrap();
    assert_ledger_returned(&f, live0, reserved0, "forked chain");
}
