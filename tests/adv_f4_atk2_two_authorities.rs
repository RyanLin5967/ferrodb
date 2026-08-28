//! ATTACK 2 — make two different authorities issue the same page id, arena id or txn id, and probe
//! the grant bookkeeping for orders it gets wrong.
//!
//! Two authorities exist in one process the moment `join` or `leave` is called: the node's own
//! standalone self-grant is one, the leader that grants it a range afterwards is the other. Ranges
//! are disjoint by construction, so the question this file asks is whether the *bookkeeping* that
//! makes them disjoint — `Grants::accepted_through`, `Grants::issued`, and the authority epoch —
//! survives the transitions.
//!
//! Tests named `*_holds` assert a property the guard is supposed to have and are expected to pass.
//! Tests that assert the claim and fail are the findings; the panic message is the evidence.
//!
//! `GrantedCounter` is exercised directly wherever the question is about the rule rather than the
//! wiring: it is `pub`, and driving it directly is the only way to reach an ordering that a leader
//! loop would have to be buggy to produce but that a re-delivered log suffix produces for free.

use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::types::{BranchId, ARENA_EXTENT_PAGES};
use ferrodb::branch::BranchCatalog;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cluster::{self, Applied, ClusterScope, GrantError, GrantedCounter};
use ferrodb::consensus::NodeId;
use ferrodb::cow::{PageStore, PageType};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

const ARENA_BASE: u32 = 1024;
const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);

struct Store {
    store: Arc<ArenaPageStore>,
    catalog: Arc<LogBranchCatalog>,
    _dir: tempfile::TempDir,
}

fn store(tag: &str) -> Store {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let dm = Arc::new(DiskManager::new(file).unwrap());
    let pool = Arc::new(BufferPoolManager::new(Arc::clone(&dm)));
    let catalog = Arc::new(LogBranchCatalog::in_memory(1));
    let store =
        Arc::new(ArenaPageStore::new(Arc::clone(&pool), Arc::clone(&catalog), ARENA_BASE).unwrap());
    Store { store, catalog, _dir: dir }
}

struct Engine {
    txn: Arc<TxnManager>,
    _dir: tempfile::TempDir,
}

fn engine(tag: &str) -> Engine {
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join(format!("{tag}.db")))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let wal = Arc::new(WalManager::new(dir.path().join(format!("{tag}.wal"))).unwrap());
    let txn = Arc::new(TxnManager::new(Arc::clone(&wal), Arc::clone(&bp)));
    bp.attach_wal(wal);
    Engine { txn, _dir: dir }
}

/// Drain a counter until it refuses, collecting every value it issued.
fn drain(c: &GrantedCounter, n: u64, cap: usize) -> Vec<u64> {
    let mut out = Vec::new();
    while out.len() < cap {
        match c.take(n) {
            Ok(v) => out.push(v),
            Err(_) => break,
        }
    }
    out
}

// =================================================================================================
// A2.1 — a grant applied before the join.
// =================================================================================================

#[test]
fn a2_1_a_grant_applied_before_the_join_is_refused_and_does_not_survive_it_holds() {
    let _scope = ClusterScope::standalone();
    let c = GrantedCounter::new("probe", 100, 8);

    // Standalone: the grant is refused outright.
    match c.apply_grant(N1, 1_000, 2_000) {
        Err(GrantError::NotClustered { .. }) => {}
        other => panic!("a standalone node accepted an outside range: {other:?}"),
    }

    // And it left nothing behind that the join could bless.
    cluster::join(N1);
    assert_eq!(c.remaining(), 0, "a refused pre-join grant left usable space behind");
    assert!(matches!(c.take(1), Err(GrantError::Exhausted { .. })), "member issued after a refusal");
}

// =================================================================================================
// A2.2 — join / leave / join. Does a granted range come back to life?
// =================================================================================================

#[test]
fn a2_2_a_range_does_not_survive_leave_then_rejoin_holds() {
    let _scope = ClusterScope::standalone();
    let c = GrantedCounter::new("probe", 0, 4);

    cluster::join(N1);
    assert!(matches!(c.apply_grant(N1, 100, 200), Ok(Applied::Accepted { usable: 100 })));
    assert_eq!(c.take(1).unwrap(), 100);

    cluster::leave();
    // Standalone now, so it self-grants from its own watermark. It must NOT issue from [100, 200)
    // as though the departed leader's grant were still its own... but it may legitimately
    // self-grant above `accepted_through`, which is 200.
    let solo = c.take(1).unwrap();
    assert!(solo >= 200, "a standalone node re-issued from a departed leader's range: {solo}");

    cluster::join(N1);
    assert_eq!(c.remaining(), 0, "a rejoin resurrected a range");
    assert!(matches!(c.take(1), Err(GrantError::Exhausted { .. })), "a rejoin resurrected a range");
}

// =================================================================================================
// A2.3 — no value is ever issued twice, across every transition order this file can build.
// =================================================================================================

#[test]
fn a2_3_no_value_is_issued_twice_across_a_transition_storm_holds() {
    let _scope = ClusterScope::standalone();
    let c = GrantedCounter::new("probe", 0, 7);
    let mut seen: Vec<u64> = Vec::new();

    for round in 0..40u64 {
        match round % 4 {
            0 => cluster::leave(),
            1 => cluster::join(N1),
            2 => {
                let _ = c.apply_grant(N1, round * 3, round * 3 + 50);
            }
            _ => cluster::join(N1),
        }
        // Grants that straddle, precede and follow the watermark, plus the same one twice.
        let _ = c.apply_grant(N1, 10, 60);
        let _ = c.apply_grant(N1, 10, 60);
        let _ = c.apply_grant(N1, 0, 5);
        seen.extend(drain(&c, 1, 12));
        let _ = c.apply_grant(N1, 30, 500);
        seen.extend(drain(&c, 3, 4));
    }

    let mut sorted = seen.clone();
    sorted.sort_unstable();
    let before = sorted.len();
    sorted.dedup();
    assert_eq!(
        before,
        sorted.len(),
        "GUARD FELL: a value was issued twice by one counter across authority transitions. \
         issued: {seen:?}"
    );
}

// =================================================================================================
// A2.4 — two grants delivered out of order. The lower range is swallowed.
// =================================================================================================

/// `accepted_through` is a single scalar and doubles as the duplicate-detector, so a grant whose
/// `hi` is at or below it is reported `Duplicate` regardless of whether its values were ever
/// granted to this node. Delivery order of two committed entries is not something a follower
/// controls.
#[test]
fn a2_4_a_lower_range_delivered_after_a_higher_one_is_not_lost() {
    let _scope = ClusterScope::standalone();
    let c = GrantedCounter::new("probe", 0, 4);
    cluster::join(N1);

    assert!(matches!(c.apply_grant(N1, 2_000, 3_000), Ok(Applied::Accepted { usable: 1_000 })));
    let second = c.apply_grant(N1, 1_000, 2_000);

    assert!(
        matches!(second, Ok(Applied::Accepted { .. })),
        "GUARD FELL: the leader granted [1000, 2000) to {N1} and the node answered {second:?}. \
         1000 values the leader believes this node owns are unusable for ever, and the leader was \
         told 'Duplicate', which reads as 'already have it'. remaining() = {} (only the higher \
         range).",
        c.remaining()
    );
}

// =================================================================================================
// A2.5 — the standalone-then-join deadlock. `accepted_through` outlives the epoch bump.
// =================================================================================================

/// `join` bumps the epoch and `evict_stale` drops the *held ranges*, but `accepted_through` and
/// `issued` are untouched. A node that self-granted while standalone therefore refuses every grant
/// the leader makes below that stale mark — as `Duplicate`, so the leader is told it already has it
/// and never grants again.
#[test]
fn a2_5_a_leader_can_still_supply_a_node_that_self_granted_while_standalone() {
    let _scope = ClusterScope::standalone();
    let c = GrantedCounter::new("extent-start", 1_024, ARENA_EXTENT_PAGES as u64 * 4);

    // Standalone: one extent's worth issued. The self-grant chunk covers four, so
    // accepted_through is now 1024 + 4*256 = 2048 while only 1024..1280 was ever issued.
    let first = c.take(ARENA_EXTENT_PAGES as u64).unwrap();
    assert_eq!(first, 1_024);
    assert_eq!(c.issued_through(), 1_280, "fixture: watermark is not where fetch_add left it");

    cluster::join(N1);

    // The leader knows nothing of the self-grant. It grants this node a fresh region that happens
    // to sit below the stale accepted_through — which, from the leader's side, is simply "the
    // region I have not handed out yet".
    let applied = c.apply_grant(N1, 1_280, 2_048);
    let after = c.take(ARENA_EXTENT_PAGES as u64);

    assert!(
        after.is_ok(),
        "GUARD FELL (liveness): apply_grant([1280, 2048)) returned {applied:?} and the node then \
         refused to allocate: {after:?}. accepted_through = 2048 from a standalone self-grant \
         survived the epoch bump, so every grant below it is swallowed as Duplicate and the node \
         refuses for ever while the leader believes it is supplied."
    );
}

// =================================================================================================
// A2.6 — the same grant twice with a take in between, and a straddling grant.
// =================================================================================================

#[test]
fn a2_6_redelivery_and_straddling_grants_never_re_offer_an_issued_value_holds() {
    let _scope = ClusterScope::standalone();
    let c = GrantedCounter::new("probe", 0, 4);
    cluster::join(N1);

    c.apply_grant(N1, 100, 200).unwrap();
    let a = c.take(1).unwrap();
    assert_eq!(a, 100);

    // Re-delivered in full: must be a no-op.
    assert_eq!(c.apply_grant(N1, 100, 200).unwrap(), Applied::Duplicate);
    // Straddling the watermark: only the suffix may be usable.
    c.apply_grant(N1, 150, 300).unwrap();

    let rest = drain(&c, 1, 1_000);
    let mut all = vec![a];
    all.extend(rest);
    let mut sorted = all.clone();
    sorted.sort_unstable();
    let n = sorted.len();
    sorted.dedup();
    assert_eq!(n, sorted.len(), "GUARD FELL: a value was issued twice: {all:?}");
    assert!(
        all.iter().all(|&v| (100..300).contains(&v)),
        "a value outside every granted range was issued: {all:?}"
    );
}

// =================================================================================================
// A2.7 — a grant addressed to another node.
// =================================================================================================

#[test]
fn a2_7_a_grant_for_another_node_is_refused_holds() {
    let _scope = ClusterScope::standalone();
    let c = GrantedCounter::new("probe", 0, 4);
    cluster::join(N1);

    match c.apply_grant(N2, 500, 600) {
        Err(GrantError::WrongNode { granted_to, self_id, .. }) => {
            assert_eq!((granted_to, self_id), (N2, N1));
        }
        other => panic!("GUARD FELL: {N1} accepted a range addressed to {N2}: {other:?}"),
    }
    assert_eq!(c.remaining(), 0, "a refused foreign grant left usable space");
    // And it did not poison the dedup mark either: the same range addressed correctly must work.
    assert!(matches!(c.apply_grant(N1, 500, 600), Ok(Applied::Accepted { usable: 100 })));
}

// =================================================================================================
// A2.8 — the one that matters: the SAME PAGE ID from two authorities, end to end.
// =================================================================================================

/// Node A self-grants page P while standalone and writes it. The process then joins, and a leader
/// with no knowledge of that self-grant hands the region containing P to a node — which issues P
/// again. Both stores use the same `ARENA_BASE`, i.e. the same physical page-id space, which is
/// what "two nodes on one region" means.
///
/// The `recycle_epoch` doc says this shape is the reason the epoch exists. It evicts the *counter's*
/// held ranges; it does not un-issue what standalone already wrote.
#[test]
fn a2_8_a_page_written_while_standalone_cannot_be_granted_to_a_member() {
    let _scope = ClusterScope::standalone();

    // ---- authority 1: this node, its own leader ----
    let a = store("a2_8_solo");
    let arena_a = a.store.arena_for(BranchId::TRUNK).unwrap();
    let epoch = a.catalog.next_epoch();
    let mut solo_pages = Vec::new();
    for _ in 0..4 {
        solo_pages.push(a.store.alloc_in_arena(arena_a, PageType::BTreeLeaf, epoch).unwrap());
    }
    let (solo_start, _) = a.store.extent_range(arena_a).unwrap();

    // ---- authority 2: a leader, which has never heard of the above ----
    cluster::join(N1);
    let b = store("a2_8_member");
    // The leader's view of free space: the region base onwards. Exactly what a leader that was
    // never told about a pre-join self-grant would propose.
    b.store.apply_arena_grant(N1, ARENA_BASE, 2 * ARENA_EXTENT_PAGES).unwrap();
    let arena_b = b.store.arena_for(BranchId::TRUNK).unwrap();
    let epoch_b = b.catalog.next_epoch();
    let mut member_pages = Vec::new();
    for _ in 0..4 {
        member_pages.push(b.store.alloc_in_arena(arena_b, PageType::BTreeLeaf, epoch_b).unwrap());
    }

    let collide: Vec<u32> =
        solo_pages.iter().copied().filter(|p| member_pages.contains(p)).collect();
    assert!(
        collide.is_empty(),
        "GUARD FELL: page(s) {collide:?} were issued by TWO authorities — self-granted and \
         written by a standalone node (extent at {solo_start}, arena {arena_a}), then granted to \
         {N1} by a leader and issued again (arena {arena_b}). Standalone pages: {solo_pages:?}; \
         granted pages: {member_pages:?}. Every one of those pages passes its own checksum on \
         both nodes."
    );
}

/// The same collision for arena ids, which is the one `BranchRecord::arenas` points at and the
/// reaper frees whole.
#[test]
fn a2_9_an_arena_id_used_while_standalone_cannot_be_granted_to_a_member() {
    let _scope = ClusterScope::standalone();

    let a = store("a2_9_solo");
    let mut solo = Vec::new();
    for _ in 0..3 {
        let arena = a.store.arena_for(BranchId::TRUNK).unwrap();
        solo.push(arena);
        let e = a.catalog.next_epoch();
        // Fill the extent so the next arena_for claims a fresh one.
        while a.store.alloc_in_arena(arena, PageType::BTreeLeaf, e).is_ok() {}
    }

    cluster::join(N1);
    let b = store("a2_9_member");
    b.store.apply_arena_grant(N1, ARENA_BASE, 2 * ARENA_EXTENT_PAGES).unwrap();
    let member = b.store.arena_for(BranchId::TRUNK).unwrap();

    assert!(
        !solo.contains(&member),
        "GUARD FELL: arena id {member} was issued both by a standalone self-grant ({solo:?}) and \
         by a leader grant to {N1}. BranchRecord::arenas then names one arena for two branches, \
         and the reaper frees exactly record.arenas."
    );
}

// =================================================================================================
// A2.10 — the same collision on txn ids, through the real TxnManager.
// =================================================================================================

#[test]
fn a2_10_a_txn_id_issued_while_standalone_cannot_be_granted_to_a_member() {
    let _scope = ClusterScope::standalone();

    let a = engine("a2_10_solo");
    let mut solo = Vec::new();
    for _ in 0..10 {
        solo.push(a.txn.begin().unwrap());
    }

    cluster::join(N1);
    let b = engine("a2_10_member");
    // A leader seeding a fresh cluster from the bottom of the id space.
    b.txn.apply_txn_id_grant(N1, 1, 1_000).unwrap();
    let mut member = Vec::new();
    for _ in 0..10 {
        member.push(b.txn.begin().unwrap());
    }

    let collide: Vec<u64> = solo.iter().copied().filter(|t| member.contains(t)).collect();
    assert!(
        collide.is_empty(),
        "GUARD FELL: txn id(s) {collide:?} were issued by two authorities. solo={solo:?} \
         member={member:?}. The TEL's stamp() leads with TxnId to order writes across branches, so \
         a duplicate corrupts merge ordering, which the merge engine cannot detect."
    );
}

/// The same `accepted_through` deadlock as A2.5, through `TxnManager`: a node that begins
/// transactions standalone and then joins refuses every grant below its stale mark.
#[test]
fn a2_11_a_leader_can_supply_txn_ids_to_a_node_that_ran_standalone_first() {
    let _scope = ClusterScope::standalone();
    let e = engine("a2_11");
    for _ in 0..10 {
        e.txn.begin().unwrap();
    }
    let watermark = e.txn.next_txn_id();
    assert_eq!(watermark, 11, "fixture: the watermark moved somewhere unexpected");

    cluster::join(N1);
    // The self-grant chunk is 256, so accepted_through is 257 while only 11 ids were issued. A
    // leader granting the next hundred ids after the watermark lands entirely below it.
    let applied = e.txn.apply_txn_id_grant(N1, watermark, watermark + 100);
    let began = e.txn.begin();
    assert!(
        began.is_ok(),
        "GUARD FELL (liveness): apply_txn_id_grant({N1}, {watermark}, {}) returned {applied:?} \
         and begin() then failed: {began:?}. The node will not begin a transaction and the leader \
         has been told the range was a duplicate.",
        watermark + 100
    );
}
