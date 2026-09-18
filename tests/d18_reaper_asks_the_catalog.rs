//! D18: the reaper asked the RECORD a question only the CATALOG can answer.
//! (`src/branch/record.rs:276-283`, `src/branch/mod.rs:123-134`). MCTS prunes INTERIOR nodes,
//! and reaping an interior node severs the one link the rule consults.
//!
//! Run against BOTH catalogs. `LogBranchCatalog` is the important one: it keeps the live set
//! inside the record, so the shipped table catalog's empty `live_children` cannot be blamed for
//! the failure -- this is the DESIGN, not an implementation slip. `TableBranchCatalog` is the one
//! that actually ships, and it fails through a DIFFERENT line (`live_child_at` resolving a reaped
//! interior node to "not live"), so one test over one catalog would have left half the fix
//! unverified.

use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::storage::disk_manager::DiskManager;

struct Env {
    catalog: Arc<dyn BranchCatalog>,
    store: Arc<ArenaPageStore>,
    path: std::path::PathBuf,
    cat_path: Option<std::path::PathBuf>,
}
impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        if let Some(c) = &self.cat_path {
            let _ = std::fs::remove_file(c);
        }
    }
}

fn env_with(tag: &str, table: bool) -> Env {
    let path = std::env::temp_dir().join(format!("s18-trans-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let (catalog, cat_path): (Arc<dyn BranchCatalog>, Option<std::path::PathBuf>) = if table {
        let cp = std::env::temp_dir()
            .join(format!("s18-trans-{}-{}.cat", std::process::id(), tag));
        let _ = std::fs::remove_file(&cp);
        (Arc::new(TableBranchCatalog::open_sidecar(&cp, 1).unwrap()), Some(cp))
    } else {
        (Arc::new(LogBranchCatalog::in_memory(1)), None)
    };
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(pool, Arc::clone(&catalog), base).unwrap());
    Env { catalog, store, path, cat_path }
}

fn write_one(e: &Env, b: BranchId) -> u32 {
    let arena = e.store.arena_for(b).unwrap();
    let ep = e.catalog.next_epoch();
    let p = e.store.alloc_in_arena(arena, PageType::BTreeLeaf, ep).unwrap();
    let h = e.store.read_page(p).unwrap();
    let mut f = h.write();
    f.data[PAGE_HEADER_SIZE] = 0xAB;
    stamp_checksum(&mut f.data);
    p
}


/// **D18.** PARENT writes page P into its own arena; CHILD forks off PARENT, so CHILD's root is
/// PARENT's root and CHILD can read P. Reaping PARENT must free NOTHING: P is visible to a live
/// child.
///
/// This catches BOTH sites of the same mistake, because one reap crosses both:
///   1. `reaper.rs` fast-path guard — read `rec.live_children.is_empty()`, which
///      `deserialize_core` leaves empty and `hydrate` never refills, so on the table catalog it
///      was ALWAYS true and the arena was freed wholesale with no sharing analysis.
///   2. `drain_pending` — read the same empty vec through `reclaimable(&[], ..)`, which is
///      vacuously true, so every page the slow path had just PARKED was released again.
///
/// The log catalog keeps `live_children` inside the record, so it passes either way. That is
/// precisely why this needs a table-catalog arm: the shipped catalog was the broken one.
fn reaping_a_parent_must_not_free_what_its_live_child_reads(tag: &str, table: bool) {
    let e = env_with(tag, table);
    let reaper = TwoTierReaper::new(Arc::clone(&e.catalog), Arc::clone(&e.store));

    let parent = e.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
    let p = write_one(&e, parent.branch_id);
    let child = e.catalog.fork(parent.branch_id, LeaseDeadline::from_now(60_000)).unwrap();
    assert_eq!(child.root_page_id, parent.root_page_id, "CHILD's root is PARENT's root");
    assert!(
        e.catalog.has_live_children(parent.branch_id.id).unwrap(),
        "precondition: the catalog knows PARENT has a live child"
    );

    let freed = reaper.reap(parent.branch_id).unwrap();

    assert_eq!(
        e.catalog.get(child.branch_id).unwrap().state,
        ferrodb::branch::types::BranchState::Live,
        "CHILD is still live after its parent was reaped"
    );
    assert_eq!(
        freed, 0,
        "THE REAPER FREED A PAGE ITS LIVE CHILD CAN STILL READ. Reaping PARENT freed {freed} \
         page(s) while CHILD b{} is Live and reads page {p} through PARENT's root. The reaper \
         asked the RECORD (`live_children`) instead of the CATALOG; the shipped table catalog \
         never populates that field, so the answer was always 'childless'.",
        child.branch_id.id
    );
}

#[test]
fn log_catalog_reap_must_not_free_what_a_live_child_reads() {
    reaping_a_parent_must_not_free_what_its_live_child_reads("d18-log", false);
}

/// The catalog that actually ships, and the one that was broken.
#[test]
fn table_catalog_reap_must_not_free_what_a_live_child_reads() {
    reaping_a_parent_must_not_free_what_its_live_child_reads("d18-table", true);
}
