//! D235: a restart must not drop a D16 pin.
//!
//! D16 keeps a reaped INTERIOR branch attached to its parent while anything below it lives
//! (`reaper.rs::detach_from_parent`). That entry under the grandparent is the only thing saying the
//! grandparent's pages are still visible to the live grandchild. `LogBranchCatalog` holds the entry
//! in memory, and `tests/d16_transitive_visibility.rs` proves that much. But that test never
//! REOPENS a catalog. On reopen, `LogBranchCatalog::index` rebuilds every parent's live set from the
//! records, and at `9aa6968` it skipped every `Reaped` record. So the first restart after an interior
//! prune lost the pin, and with it the grandparent's pages. Adversary report:
//! `frontier/d16_restart_pin_adversary.md` @ artie-research `911aef8`.
//!
//! **Red before the fix, and it compiles against the base.** It names only API that existed at
//! `9aa6968`.
//!
//! **The shipped `TableBranchCatalog` is the CONTROL arm.** Its CHILD span is durable and never
//! rebuilt on open, so the same fixture must pass on it before and after the fix. That is what pins a
//! red log arm on the rebuild rather than on this fixture: a fixture that lost the pin on its own
//! would fail both arms.
//!
//! Both arms reap through `Reaper::reap_expired`, the production entry point, not a hand-rolled
//! state flip. Each checks its premises on both sides of the restart:
//! - before: the pin holds in memory;
//! - after: the pruned nodes still read `Reaped` and the leaves still read `Live`.
//! A restart that silently undid the reaps would otherwise pass for the wrong reason.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::table_catalog::TableBranchCatalog;
use ferrodb::branch::types::{BranchId, BranchState, Epoch, LeaseDeadline};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cow::page_header::PageType;
use ferrodb::cow::{stamp_checksum, PageStore, PAGE_HEADER_SIZE};
use ferrodb::storage::disk_manager::DiskManager;

/// Deadline of every branch the sweep prunes. Everything else never expires.
const PRUNE_AT: u64 = 1_000;
const NEVER: u64 = u64::MAX;

/// The files one arm uses, removed when the arm ends however it ends.
struct Files {
    db: PathBuf,
    cat: PathBuf,
}

impl Files {
    fn new(tag: &str) -> Files {
        let base = std::env::temp_dir().join(format!("d235-{}-{tag}", std::process::id()));
        let f = Files { db: base.with_extension("db"), cat: base.with_extension("cat") };
        let _ = std::fs::remove_file(&f.db);
        let _ = std::fs::remove_file(&f.cat);
        f
    }
}

impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.db);
        let _ = std::fs::remove_file(&self.cat);
    }
}

fn open_catalog(files: &Files, table: bool) -> Arc<dyn BranchCatalog> {
    if table {
        Arc::new(TableBranchCatalog::open_sidecar(&files.cat, 1).expect("open table catalog"))
    } else {
        Arc::new(LogBranchCatalog::open(&files.cat, 1).expect("open log catalog"))
    }
}

/// One fresh page in `b`'s arena. Returns its birth epoch, which is what the pin protects.
fn write_one(store: &ArenaPageStore, catalog: &dyn BranchCatalog, b: BranchId) -> Epoch {
    let arena = store.arena_for(b).unwrap();
    let ep = catalog.next_epoch();
    let p = store.alloc_in_arena(arena, PageType::BTreeLeaf, ep).unwrap();
    let h = store.read_page(p).unwrap();
    let mut f = h.write();
    f.data[PAGE_HEADER_SIZE] = 0xD2;
    stamp_checksum(&mut f.data);
    ep
}

/// What the fixture made, by id, so it can be checked against a catalog opened afterwards.
struct Shape {
    /// GP -> B (pruned) -> C (live): one reaped interior.
    gp: u64,
    b: u64,
    b_fork: Epoch,
    c: u64,
    gp_page_birth: Epoch,
    /// GP2 -> P1 (pruned) -> P2 (pruned) -> C2 (live): two reaped interiors in a row, so the rule
    /// has to hold TRANSITIVELY, and P1's id slot must not be freed.
    gp2: u64,
    p1: u64,
    p1_fork: Epoch,
    p2: u64,
    c2: u64,
    gp2_page_birth: Epoch,
}

/// The pin, asked every way the trait offers. `now` bounds the page's interval from above.
fn assert_pins(cat: &dyn BranchCatalog, s: &Shape, now: Epoch, when: &str) {
    for (parent, child_fork, birth, name) in [
        (s.gp, s.b_fork, s.gp_page_birth, "GP (pinned through the reaped interior B)"),
        (s.gp2, s.p1_fork, s.gp2_page_birth, "GP2 (pinned through reaped P1 and reaped P2)"),
    ] {
        assert!(
            cat.has_live_children(parent).unwrap(),
            "{when}: {name} reads as childless. The reaped interior below it still has a live \
             descendant reading this branch's pages, so its entry is a D16 pin and must survive."
        );
        assert_eq!(
            cat.max_live_child(parent).unwrap(),
            Some(child_fork),
            "{when}: {name}: the pin must report the pruned node's OWN fork epoch (D16 addendum)"
        );
        assert!(
            cat.live_child_in_epoch_range(parent, birth, now).unwrap(),
            "{when}: {name}: a page born at {birth:?} would be reclaimable, while a live \
             grandchild still reaches it through the root it inherited"
        );
    }
    // The middle of the two-interior chain is itself pinned by the live node below P2.
    assert!(
        cat.has_live_children(s.p1).unwrap(),
        "{when}: P1 (reaped) reads as childless while P2 (reaped) still has live C2 below it"
    );
}

fn prune_restart_and_check(tag: &str, table: bool) {
    let files = Files::new(tag);

    // ---- before the restart: the same history D16's reproducer builds, one level deeper -------
    let (shape, before_now) = {
        let catalog = open_catalog(&files, table);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&files.db)
            .unwrap();
        let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let base = pool.disk_manager.high_water().unwrap();
        let store = Arc::new(ArenaPageStore::new(pool, Arc::clone(&catalog), base).unwrap());
        let reaper = TwoTierReaper::new(Arc::clone(&catalog), Arc::clone(&store));

        let fork = |parent: BranchId, deadline: u64| {
            catalog.fork(parent, LeaseDeadline(deadline)).expect("fork").branch_id
        };
        let gp = fork(BranchId::TRUNK, NEVER);
        let gp_page_birth = write_one(&store, catalog.as_ref(), gp);
        let b = fork(gp, PRUNE_AT);
        let c = fork(b, NEVER);

        let gp2 = fork(BranchId::TRUNK, NEVER);
        let gp2_page_birth = write_one(&store, catalog.as_ref(), gp2);
        let p1 = fork(gp2, PRUNE_AT);
        let p2 = fork(p1, PRUNE_AT);
        let c2 = fork(p2, NEVER);

        let reaped: BTreeSet<u64> =
            reaper.reap_expired(PRUNE_AT).expect("reap").iter().map(|x| x.id).collect();
        assert_eq!(
            reaped,
            BTreeSet::from([b.id, p1.id, p2.id]),
            "fixture: the sweep must prune exactly the three interiors"
        );

        let shape = Shape {
            gp: gp.id,
            b: b.id,
            b_fork: catalog.get_raw(b.id).unwrap().fork_epoch,
            c: c.id,
            gp_page_birth,
            gp2: gp2.id,
            p1: p1.id,
            p1_fork: catalog.get_raw(p1.id).unwrap().fork_epoch,
            p2: p2.id,
            c2: c2.id,
            gp2_page_birth,
        };
        let now = catalog.next_epoch();
        // PREMISE: the pin holds in memory. Without this, a red below could be the fixture's fault.
        assert_pins(catalog.as_ref(), &shape, now, "before the restart");
        (shape, now)
        // `reaper`, `store` and `catalog` drop here: nothing survives into the reopen but the files.
    };

    // ---- the restart ---------------------------------------------------------------------------
    let catalog = open_catalog(&files, table);

    // PREMISES after the restart: the prunes persisted and the leaves did not.
    for (id, name) in [(shape.b, "B"), (shape.p1, "P1"), (shape.p2, "P2")] {
        assert_eq!(
            catalog.get_raw(id).unwrap().state,
            BranchState::Reaped,
            "premise: {name} must still read Reaped after the restart"
        );
    }
    for (id, name) in [(shape.gp, "GP"), (shape.c, "C"), (shape.gp2, "GP2"), (shape.c2, "C2")] {
        assert_eq!(
            catalog.get_raw(id).unwrap().state,
            BranchState::Live,
            "premise: {name} must still read Live after the restart"
        );
    }

    // THE CLAIM: the restart kept every pin.
    let now = Epoch(before_now.0.max(catalog.current_epoch().0) + 1);
    assert_pins(catalog.as_ref(), &shape, now, "after the restart");

    // And no pinned slot was handed out again. At `9aa6968` the log catalog's rebuild put P1 on its
    // free list, since P1's derived live set came back empty. A slot recycled while pages are still
    // parked under its id answers the pin question for them with the NEW branch's children.
    let minted = catalog.fork(BranchId::TRUNK, LeaseDeadline(NEVER)).expect("fork after restart");
    for (id, name) in [(shape.b, "B"), (shape.p1, "P1"), (shape.p2, "P2")] {
        assert_ne!(
            minted.branch_id.id, id,
            "the first fork after the restart recycled {name}'s slot, which still pins a live \
             descendant"
        );
    }
}

/// RED at `9aa6968`: `LogBranchCatalog::index` skips every `Reaped` record when it rebuilds the
/// live sets, so GP comes back childless and P1 comes back on the free list.
#[test]
fn a_log_catalog_reopen_keeps_the_pin_of_a_reaped_interior() {
    prune_restart_and_check("log", false);
}

/// CONTROL: the shipped catalog never rebuilds its CHILD span, so this passes before and after.
/// If it fails, the fixture or the reopen is wrong, and the log arm's result means nothing.
#[test]
fn a_table_catalog_reopen_keeps_the_pin_of_a_reaped_interior() {
    prune_restart_and_check("table", true);
}

/// **D235 review F3: a rebuild must not pin a recycled parent slot's new occupant.**
///
/// The running D16 reaper cannot build this shape: `release_id` refuses a slot with live children.
/// A PRE-D16 reaper could, because it detached unconditionally. So can the reaper-less `seal`
/// fallback in `runtime.rs` (SCALE-LEDGER D201), which detaches without asking about live children.
/// The shape: P -> X -> C, with X reaped and DETACHED from P although C lives. P, now childless, is
/// reaped and its slot released and recycled into N, a child of Q, and N is reaped too. X still
/// names P's OLD incarnation as its parent.
///
/// A rebuild that followed X's parent pointer by slot id alone would reach N, count it as holding
/// an entry, and file N's epoch under Q, so Q would read as having a live child it never had. The
/// rule compares the parent handle X recorded with the slot's current `branch_id` and stops.
///
/// Both rebuilders are asked: the log reopen (`index`) and a `migrate_from` of that reopened log.
/// It also asserts the pin the rule KEEPS on purpose: X's entry under P's old slot, because pages
/// old P parked are judged by the slot id. Without that assertion the test could not tell "the walk
/// stopped at the recycled slot" from "the walk never ran".
///
/// - **FAIL at `9aa6968`**, at that kept pin: the base rule derived no reaped record's entry.
/// - **FAIL at `6589552`**, at Q: the rule's first version had no incarnation check.
/// - **PASS at the tip.**
///
/// Only base API is named, so it compiles at all three. PREREG amendments 2–3.
#[test]
fn a_rebuild_does_not_pin_a_recycled_parent_slot_s_new_occupant() {
    let files = Files::new("recycled");
    let never = LeaseDeadline(NEVER);

    let (q, p_slot, x, x_fork, c, n) = {
        let cat = LogBranchCatalog::open(&files.cat, 1).expect("open log catalog");
        let q = cat.fork(BranchId::TRUNK, never).unwrap().branch_id;
        let p = cat.fork(BranchId::TRUNK, never).unwrap();
        let x = cat.fork(p.branch_id, never).unwrap();
        let c = cat.fork(x.branch_id, never).unwrap().branch_id;

        // The PRE-D16 reap of X: mark it, then detach it whatever still lives below it.
        cat.set_state(x.branch_id, BranchState::Live, BranchState::Reaping).unwrap();
        cat.set_state(x.branch_id, BranchState::Reaping, BranchState::Reaped).unwrap();
        cat.detach_child(p.branch_id.id, x.fork_epoch).unwrap();
        cat.release_id(x.branch_id.id); // refused: X still lists C
        // P is childless in memory now, so it is reaped and its slot released...
        cat.set_state(p.branch_id, BranchState::Live, BranchState::Reaping).unwrap();
        cat.set_state(p.branch_id, BranchState::Reaping, BranchState::Reaped).unwrap();
        cat.detach_child(BranchId::TRUNK.id, p.fork_epoch).unwrap();
        cat.release_id(p.branch_id.id);
        // ...and recycled into N, a child of Q, which is reaped in its turn.
        let n = cat.fork(q, never).unwrap().branch_id;
        assert_eq!(n.id, p.branch_id.id, "fixture: N must recycle P's slot");
        assert_ne!(n.generation, p.branch_id.generation, "fixture: N must be a new incarnation");
        cat.set_state(n, BranchState::Live, BranchState::Reaping).unwrap();
        cat.set_state(n, BranchState::Reaping, BranchState::Reaped).unwrap();
        cat.detach_child(q.id, cat.get_raw(n.id).unwrap().fork_epoch).unwrap();
        cat.release_id(n.id);

        // PREMISE, in memory: Q has no live child, and X still pins C.
        assert!(!cat.has_live_children(q.id).unwrap(), "premise: Q is childless before any rebuild");
        assert!(cat.has_live_children(x.branch_id.id).unwrap(), "premise: X still lists live C");
        (q, p.branch_id.id, x.branch_id, x.fork_epoch, c, n)
        // `cat` drops here: the reopen below reads only the file.
    };

    // Rebuild 1: reopen the log, i.e. `LogBranchCatalog::index`.
    let reopened = LogBranchCatalog::open(&files.cat, 1).expect("reopen log catalog");
    // Rebuild 2: migrate that log into the shipped catalog, i.e. `TableBranchCatalog::migrate_from`.
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&files.db)
        .unwrap();
    let pool = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let (migrated, _header) =
        TableBranchCatalog::migrate_from(pool, &reopened, 1).expect("migrate the reopened log");

    let rebuilt: [(&dyn BranchCatalog, &str); 2] = [
        (&reopened as &dyn BranchCatalog, "log reopen"),
        (&migrated as &dyn BranchCatalog, "migration"),
    ];
    for (cat, name) in rebuilt {
        // PREMISES after the rebuild: the slot holds N's incarnation, X is reaped, C lives, and C's
        // own entry under X survived (a live child's entry: true at every tree).
        assert_eq!(
            cat.get_raw(p_slot).unwrap().branch_id,
            n,
            "{name}: premise: P's old slot must hold N's incarnation"
        );
        assert_eq!(cat.get_raw(x.id).unwrap().state, BranchState::Reaped, "{name}: premise: X");
        assert_eq!(cat.get_raw(c.id).unwrap().state, BranchState::Live, "{name}: premise: C");
        assert!(
            cat.has_live_children(x.id).unwrap(),
            "{name}: premise: C's own entry under X must survive the rebuild"
        );
        // THE PIN THE RULE KEEPS ON PURPOSE: X is reaped with live C below it, so it holds an entry
        // under the slot it forked from, even though that slot now holds N. Pages old P parked are
        // judged by the slot id (`record::parent_entry_holders`' doc).
        assert!(
            cat.has_live_children(p_slot).unwrap(),
            "{name}: X's entry must stay under P's old slot (now N's). X is a D16 pin, since C lives \
             below it, and the base rule dropped every reaped record's entry"
        );
        assert_eq!(
            cat.max_live_child(p_slot).unwrap(),
            Some(x_fork),
            "{name}: the kept pin must report X's own fork epoch"
        );
        // THE CLAIM.
        assert!(
            !cat.has_live_children(q.id).unwrap(),
            "{name}: Q reads as having a live child. The rebuild followed X's parent pointer by \
             slot id into the recycled slot, counted its new occupant N as a pin, and filed N's \
             entry under Q"
        );
        assert_eq!(
            cat.max_live_child(q.id).unwrap(),
            None,
            "{name}: Q reports a live child's fork epoch it never had"
        );
    }
}
