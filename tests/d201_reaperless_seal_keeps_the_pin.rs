//! D201 — a runtime with a page store and NO reaper must not unpin a parent on behalf of a branch
//! whose children are still alive.
//!
//! `AgentRuntime::seal` has a reaper-less fallback. It detached the retiring branch from its parent
//! unconditionally and only then marked it `Reaped`. With live children below it that is D16's
//! interior-prune loss: the parent reads as childless at the branch's fork epoch, and with a page
//! store its pages at that epoch are free to be reclaimed while a grandchild still reads them
//! through the root it inherited. Detaching BEFORE the mark is also D3's unsafe crash window.
//!
//! Latent in production — the CLI chains `with_storage` and `with_reaper` — but
//! `with_storage` without `with_reaper` is representable, and most `with_storage` call sites in
//! this repo build exactly that.
//!
//! # PRE-REGISTERED, before the fix
//! At `d04aeeb` this fails at its FIRST assertion: trunk no longer holds a live child in the
//! parent's fork window after the parent is abandoned. After the fix it passes, and the two
//! controls (a childless branch IS detached; once the child goes too, the pin resolves away) pass
//! on both builds.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, Epoch};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::PageStore;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::tel::MemEffectLog;

#[test]
fn a_reaperless_abandon_keeps_pinning_the_parent_for_a_live_child() {
    let dir = tempfile::tempdir().unwrap();
    let file = OpenOptions::new().read(true).write(true).create(true).truncate(true)
        .open(dir.path().join("d201.db")).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog: Arc<dyn BranchCatalog> =
        Arc::new(TableBranchCatalog::open_sidecar(&dir.path().join("d201.branchcat"), 1).unwrap());
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), base).unwrap());

    // THE DANGEROUS COMBINATION: a page store, and no `with_reaper`.
    let rt = AgentRuntime::with_storage(
        Arc::clone(&catalog),
        Arc::new(MemEffectLog::new()),
        store as Arc<dyn PageStore>,
    )
    .unwrap();

    let t = BranchId::TRUNK.id;
    let parent = rt.begin_session("parent", None, BranchId::TRUNK).unwrap().branch;
    let child = rt.begin_session("child", None, parent).unwrap().branch;
    let lone = rt.begin_session("lone", None, BranchId::TRUNK).unwrap().branch;
    let pe = catalog.get(parent).unwrap().fork_epoch;
    let le = catalog.get(lone).unwrap().fork_epoch;
    let next = |e: Epoch| Epoch(e.0 + 1);

    rt.abandon(parent).unwrap();
    assert!(catalog.get(child).is_ok(), "fixture: the child must still be live");
    assert!(
        catalog.live_child_in_epoch_range(t, pe, next(pe)).unwrap(),
        "D201: the reaper-less abandon detached a branch whose child is live. Trunk no longer pins \
         its pages at the branch's fork epoch, and the child still reads them through its root"
    );

    // CONTROL: the fix must not pin everything. A childless branch is still detached.
    rt.abandon(lone).unwrap();
    assert!(
        !catalog.live_child_in_epoch_range(t, le, next(le)).unwrap(),
        "a childless branch kept its parent pinned after it was abandoned"
    );

    // And the pin is not permanent: once the child goes, nothing below the parent is alive, and
    // the parent's entry resolves to "not a pin" (D16's rule, read through `live_child_at`).
    rt.abandon(child).unwrap();
    assert!(
        !catalog.live_child_in_epoch_range(t, pe, next(pe)).unwrap(),
        "trunk is still pinned by a subtree in which nothing is alive"
    );
}
