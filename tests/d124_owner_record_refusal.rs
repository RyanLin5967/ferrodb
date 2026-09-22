//! **D124, the page path: a missing owner record must REFUSE, never release.**
//!
//! Two sites resolved a failed `get_raw` of a parked page's owner into "not pinned", and
//! "not pinned" means `release_page`:
//!
//! ```text
//! reaper::drain_pending_seeded   Err(_) => false      -> release_page
//! arena::free_page               get_raw(..).is_ok() && ..  -> release_page
//! ```
//!
//! Both stated the same false premise in their own comments — *"an owner with no record at all
//! pins nothing"*.
//!
//! An extent's owner IS published, by construction: `alloc_arena` is the only writer of the
//! extent map and it ends in `catalog.add_arena`, which every catalog refuses for a branch with
//! no record. And a record missing *right now* has not stopped existing — nothing deletes one,
//! retirement is a state flip to `Reaped`. Handing back a page the interval rule deliberately
//! parked for a live child, because the owner's record could not be read, is silent data loss.
//! Refusing is retryable; the entry stays in the pending log and the next drain succeeds.
//!
//! ⚠ **When this was written there was a second, ROUTINE way to reach that state, and D126 has
//! since removed it.** `TableBranchCatalog::upsert` was delete-then-insert with no latch held
//! across the two calls, and `write_record` routes the RECORD key through it, so an ordinary
//! `set_root` or `renew_lease` on the owner made `get_raw` miss for a moment on a perfectly
//! healthy branch — and that is the case these arms were freeing pages on. `BPlusTreeManager`
//! now replaces a value in place under one latch hold; `tests/d126_atomic_upsert.rs` and
//! `table_catalog::d126_record_key_probe` assert zero absences against a control that fires.
//!
//! **The arms under test here are unchanged and so is this test.** They catch an `Err`, and an
//! I/O error or a corrupt catalog still produces one — which is exactly what the decorator below
//! injects. What D126 changed is that a `Corrupt` from this path is now a real signal instead of
//! an expected artefact of a hot-path write, which is what makes D127 (the reaper swallowing it)
//! matter more rather than less.
//!
//! **W4.** `reap_expired` runs inside the per-statement lock that every `fork` also takes, so the
//! two are mutually excluded and the mid-rewrite path was never reachable in production. W4 exists
//! to remove that lock; D126 landed before it and removed the window rather than relying on the
//! exclusion, so W4 no longer activates it. `tests/d15_concurrent_fork_and_reap.rs` bypasses
//! `RuntimeLock` and is the harness where the D124 arms are reachable at all.
//!
//! **How the state is constructed.** Not by corrupting a catalog — that would prove nothing about
//! the arm, since the arm catches an `Err` and not an absence. A decorator hides ONE record from
//! `get_raw` and delegates everything else, which is precisely the input both arms swallowed.
//!
//! Every test here has a negative control in the same body: the same operation with nothing
//! hidden must behave exactly as it does today. A guard that refuses everything is not a guard.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::record::{BranchRecord, CapabilityEnvelope, CoreRecord};
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{ArenaId, BranchId, BranchState, Epoch, LeaseDeadline, PageId};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::error::FerroError;
use ferrodb::storage::disk_manager::DiskManager;

/// Hides exactly one id from `get_raw` and delegates everything else untouched.
///
/// `get_raw` is the only method overridden on purpose: the two sites under test read the record
/// ONLY to decide whether the owner exists, and hiding anything else would change which code path
/// runs rather than which answer it gets.
struct HidesOneRecord {
    inner: Arc<dyn BranchCatalog>,
    /// `u64::MAX` means nothing is hidden. No real id reaches it: ids are minted from 0 up.
    hidden: AtomicU64,
}

impl HidesOneRecord {
    fn new(inner: Arc<dyn BranchCatalog>) -> HidesOneRecord {
        HidesOneRecord { inner, hidden: AtomicU64::new(u64::MAX) }
    }
    fn hide(&self, id: u64) {
        self.hidden.store(id, Ordering::SeqCst);
    }
    fn show_everything(&self) {
        self.hidden.store(u64::MAX, Ordering::SeqCst);
    }
}

impl BranchCatalog for HidesOneRecord {
    fn get_raw(&self, id: u64) -> Result<BranchRecord, FerroError> {
        if id == self.hidden.load(Ordering::SeqCst) {
            return Err(ferrodb::branch::types::BranchError::NotFound(BranchId::new(id, 0)).into());
        }
        self.inner.get_raw(id)
    }

    fn get(&self, branch: BranchId) -> Result<BranchRecord, FerroError> {
        self.inner.get(branch)
    }
    fn next_epoch(&self) -> Epoch {
        self.inner.next_epoch()
    }
    fn current_epoch(&self) -> Epoch {
        self.inner.current_epoch()
    }
    fn fork(&self, parent: BranchId, lease: LeaseDeadline) -> Result<BranchRecord, FerroError> {
        self.inner.fork(parent, lease)
    }
    fn reparent(
        &self,
        branch: BranchId,
        parent: BranchId,
        fork_epoch: Epoch,
        root: PageId,
    ) -> Result<BranchRecord, FerroError> {
        self.inner.reparent(branch, parent, fork_epoch, root)
    }
    fn restrict_envelope(
        &self,
        branch: BranchId,
        envelope: CapabilityEnvelope,
    ) -> Result<(), FerroError> {
        self.inner.restrict_envelope(branch, envelope)
    }
    fn set_state(
        &self,
        branch: BranchId,
        expect: BranchState,
        to: BranchState,
    ) -> Result<(), FerroError> {
        self.inner.set_state(branch, expect, to)
    }
    fn set_root(&self, branch: BranchId, root: PageId) -> Result<(), FerroError> {
        self.inner.set_root(branch, root)
    }
    fn expired_before(&self, now_millis: u64) -> Result<Vec<CoreRecord>, FerroError> {
        self.inner.expired_before(now_millis)
    }
    fn in_state(&self, state: BranchState) -> Result<Vec<BranchRecord>, FerroError> {
        self.inner.in_state(state)
    }
    fn scan(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<BranchRecord, FerroError>> + '_>, FerroError> {
        self.inner.scan()
    }
    fn max_live_child(&self, parent_id: u64) -> Result<Option<Epoch>, FerroError> {
        self.inner.max_live_child(parent_id)
    }
    fn live_child_in_epoch_range(
        &self,
        parent_id: u64,
        lo: Epoch,
        hi: Epoch,
    ) -> Result<bool, FerroError> {
        self.inner.live_child_in_epoch_range(parent_id, lo, hi)
    }
    fn has_live_children(&self, parent_id: u64) -> Result<bool, FerroError> {
        self.inner.has_live_children(parent_id)
    }
    fn live_count(&self) -> usize {
        self.inner.live_count()
    }
    fn release_id(&self, id: u64) {
        self.inner.release_id(id)
    }
    fn attach_child(
        &self,
        parent_id: u64,
        fork_epoch: Epoch,
        child_id: u64,
    ) -> Result<(), FerroError> {
        self.inner.attach_child(parent_id, fork_epoch, child_id)
    }
    fn detach_child(&self, parent_id: u64, fork_epoch: Epoch) -> Result<bool, FerroError> {
        self.inner.detach_child(parent_id, fork_epoch)
    }
    fn renew_lease(&self, branch: BranchId, lease: LeaseDeadline) -> Result<(), FerroError> {
        self.inner.renew_lease(branch, lease)
    }
    fn charge_row_writes(&self, branch: BranchId, rows: u64) -> Result<(), FerroError> {
        self.inner.charge_row_writes(branch, rows)
    }
    fn add_arena(&self, branch: BranchId, arena: ArenaId) -> Result<(), FerroError> {
        self.inner.add_arena(branch, arena)
    }
}

struct Env {
    catalog: Arc<HidesOneRecord>,
    store: Arc<ArenaPageStore>,
    paths: Vec<std::path::PathBuf>,
}
impl Drop for Env {
    fn drop(&mut self) {
        for p in &self.paths {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Both catalogs, because the two arms under test are in the reaper and the store — code shared by
/// every catalog — and a one-catalog test would leave half the claim unmeasured.
fn env(tag: &str, table: bool) -> Env {
    let mut paths = Vec::new();
    let path = std::env::temp_dir().join(format!("d124-{}-{}.db", std::process::id(), tag));
    let _ = std::fs::remove_file(&path);
    paths.push(path.clone());
    let file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let inner: Arc<dyn BranchCatalog> = if table {
        let cp = std::env::temp_dir().join(format!("d124-{}-{}.cat", std::process::id(), tag));
        let _ = std::fs::remove_file(&cp);
        paths.push(cp.clone());
        Arc::new(TableBranchCatalog::open_sidecar(&cp, 1).unwrap())
    } else {
        Arc::new(LogBranchCatalog::in_memory(1))
    };
    let catalog = Arc::new(HidesOneRecord::new(inner));
    let base = pool.disk_manager.high_water().unwrap();
    let store = Arc::new(
        ArenaPageStore::new(pool, Arc::clone(&catalog) as Arc<dyn BranchCatalog>, base).unwrap(),
    );
    Env { catalog, store, paths }
}

/// Returns the page AND the extent it landed in. Extents grow geometrically from ONE page (D31),
/// so two pages of the same branch are routinely in different extents and a single captured
/// `ArenaId` would be asking about the wrong one.
fn write_one(e: &Env, b: BranchId) -> (PageId, ArenaId) {
    let arena = e.store.arena_for(b).unwrap();
    let ep = e.catalog.next_epoch();
    let p = e.store.alloc_in_arena(arena, PageType::BTreeLeaf, ep).unwrap();
    let h = e.store.read_page(p).unwrap();
    let mut f = h.write();
    f.data[PAGE_HEADER_SIZE] = 0xAB;
    stamp_checksum(&mut f.data);
    (p, arena)
}

/// Is the page still handed out, or did it go back to the free map? `release_page` is the only
/// thing that recycles one, so this is a direct read of "was it freed", not a proxy count.
fn still_allocated(e: &Env, page: PageId, arena: ArenaId) -> bool {
    e.store.allocated_pages(arena).contains(&page)
}

/// `ArenaPageStore::free_page`, the site that PRODUCES the parked entry.
///
/// Shape: PARENT owns an extent and writes a page into it; CHILD forks off PARENT after the page
/// was born, so the interval rule pins the page and `free_page` must park it. The negative control
/// runs first, on the same store and the same parent, so a difference between the two halves can
/// only be the hidden record.
fn free_page_refuses_when_the_owner_record_cannot_be_read(tag: &str, table: bool) {
    let e = env(tag, table);
    let parent = e.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
    // BOTH pages are born BEFORE the fork, so the interval rule pins both. A page born after
    // the fork is legitimately invisible to the child and `free_page` releases it -- correct, and
    // useless here: the arm under test is only reached for a page that would otherwise be PARKED.
    let (p1, a1) = write_one(&e, parent.branch_id);
    let (p2, a2) = write_one(&e, parent.branch_id);
    let child = e.catalog.fork(parent.branch_id, LeaseDeadline::from_now(60_000)).unwrap();
    assert_eq!(child.root_page_id, parent.root_page_id, "fixture: CHILD reads PARENT's pages");
    assert!(still_allocated(&e, p1, a1) && still_allocated(&e, p2, a2), "fixture: both allocated");

    // NEGATIVE CONTROL, first: nothing hidden, and the page is parked exactly as today.
    e.store.free_page(p1, e.catalog.next_epoch()).expect("free_page refused a readable owner");
    assert_eq!(e.store.pending_len(), 1, "control: a pinned page must be PARKED, not released");
    assert!(still_allocated(&e, p1, a1), "control: a pinned page must not be released");

    // ARMED. `free_page` used to answer `is_ok() && ..` = false here and call `release_page`.
    e.catalog.hide(parent.branch_id.id);
    let err = e.store.free_page(p2, e.catalog.next_epoch()).expect_err(
        "free_page RELEASED a page whose owner record it could not read. The owner is published \
         by construction (alloc_arena -> add_arena refuses a branch with no record), so a failed \
         read is a failed read -- and the page it just handed back is one the interval rule had \
         parked for a live child.",
    );
    assert!(err.to_string().contains("not found"), "refused with the wrong error: {err}");
    assert!(still_allocated(&e, p2, a2), "free_page refused AND released the page anyway");
    assert_eq!(e.store.pending_len(), 1, "the refusal parked a half-decided entry");

    // And the refusal is not permanent: with the record readable again the page parks as normal.
    e.catalog.show_everything();
    e.store.free_page(p2, e.catalog.next_epoch()).expect("free_page still refuses after unhiding");
    assert_eq!(e.store.pending_len(), 2, "the second page must now be parked too");
    assert!(still_allocated(&e, p2, a2), "a pinned page must be parked, not released");
}

#[test]
fn table_catalog_free_page_refuses_an_unreadable_owner() {
    free_page_refuses_when_the_owner_record_cannot_be_read("fp-table", true);
}

#[test]
fn log_catalog_free_page_refuses_an_unreadable_owner() {
    free_page_refuses_when_the_owner_record_cannot_be_read("fp-log", false);
}

/// `Reaper::drain_pending`, the site that CONSUMES the parked entry.
///
/// Three states in one body, because each is the control for the next:
///   1. parked and still pinned, nothing hidden -> the drain leaves it alone (today's behaviour);
///   2. parked, owner hidden -> the drain refuses, and the entry is still in the pending log;
///   3. parked, child reaped, nothing hidden -> the drain releases it (today's behaviour).
///
/// (3) is the assertion that makes (2) mean something: without it the drain could refuse or park
/// unconditionally and every assertion above would still hold.
fn drain_pending_refuses_when_the_owner_record_cannot_be_read(tag: &str, table: bool) {
    let e = env(tag, table);
    let reaper = TwoTierReaper::new(
        Arc::clone(&e.catalog) as Arc<dyn BranchCatalog>,
        Arc::clone(&e.store),
    );

    let parent = e.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(60_000)).unwrap();
    let (page, arena) = write_one(&e, parent.branch_id);
    let child = e.catalog.fork(parent.branch_id, LeaseDeadline::from_now(60_000)).unwrap();
    let free_epoch = e.catalog.next_epoch();
    e.store.free_page(page, free_epoch).expect("park the page");
    assert_eq!(e.store.pending_len(), 1, "fixture: the page must be PARKED for the drain to see");
    assert!(still_allocated(&e, page, arena), "fixture: parked is not released");

    // 1. CONTROL. A live child still pins it, so the drain must release nothing and not error.
    assert_eq!(reaper.drain_pending().expect("drain refused a readable owner"), 0);
    assert_eq!(e.store.pending_len(), 1, "control: a pinned entry must stay parked");
    assert!(still_allocated(&e, page, arena), "control: a pinned page must not be released");

    // 2. ARMED. This arm was `Err(_) => false`, i.e. release_page.
    e.catalog.hide(parent.branch_id.id);
    let err = reaper.drain_pending().expect_err(
        "drain_pending RELEASED a parked page because it could not read the owner's record. The \
         pending log exists precisely because a live child can still see that page.",
    );
    assert!(err.to_string().contains("not found"), "refused with the wrong error: {err}");
    assert!(still_allocated(&e, page, arena), "the refusal released the page anyway");
    assert_eq!(
        e.store.pending_len(),
        1,
        "the refusal DROPPED the entry: take_pending had already emptied the durable log, so the \
         page is now neither released nor ever revisited"
    );

    // 3. CONTROL, the case the old catch-all was lumped in with. Reap the child so nothing is
    //    below the owner any more; the entry becomes genuinely reclaimable and must be released.
    e.catalog.show_everything();
    e.catalog.set_state(child.branch_id, BranchState::Live, BranchState::Reaped).unwrap();
    e.catalog.detach_child(parent.branch_id.id, child.fork_epoch).unwrap();
    assert!(
        !e.catalog.has_live_children(parent.branch_id.id).unwrap(),
        "fixture: nothing is below the owner any more"
    );
    assert_eq!(
        reaper.drain_pending().expect("drain refused a legitimately reclaimable entry"),
        1,
        "the drain must still RELEASE a page nothing pins -- otherwise the refusal above is just \
         a drain that never releases anything"
    );
    assert_eq!(e.store.pending_len(), 0, "the released entry must leave the pending log");
    assert!(!still_allocated(&e, page, arena), "the page must go back to the free map");
}

#[test]
fn table_catalog_drain_pending_refuses_an_unreadable_owner() {
    drain_pending_refuses_when_the_owner_record_cannot_be_read("dp-table", true);
}

#[test]
fn log_catalog_drain_pending_refuses_an_unreadable_owner() {
    drain_pending_refuses_when_the_owner_record_cannot_be_read("dp-log", false);
}
