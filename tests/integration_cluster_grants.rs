//! F4 — the three node-local counters, wired through the real engine.
//!
//! `src/cluster/tests.rs` pins the *rules* against the bare grant book. This file pins the
//! *wiring*: that `ArenaPageStore`, `TxnManager` and `LeaseDeadline` actually go through it, that a
//! node with no grant refuses instead of allocating, and that single-node operation is untouched.
//!
//! # Why this is its own binary and why the tests serialize
//!
//! A process is a node, so the authority is process-scoped (see `src/cluster/mod.rs`), and
//! `cargo test` runs a binary's tests as threads of one process. Every test that arms a cluster
//! therefore holds a [`ClusterScope`], which serializes against every other scope and restores the
//! process to standalone on drop. Nothing else lives in this binary, so nothing else can be caught
//! by an armed clock.
//!
//! # What "two nodes" means here
//!
//! The nodes are simulated in sequence rather than in parallel, and that loses nothing: a grant is
//! a *disjoint range*, so whether two nodes consume theirs at the same instant or an hour apart,
//! the question is whether the ranges can ever overlap. Each test that needs two nodes builds two
//! independent stores under two scopes and compares what they issued.

use std::sync::Arc;

use ferrodb::branch::arena::ArenaPageStore;
use ferrodb::branch::catalog::LogBranchCatalog;
use ferrodb::branch::reaper::TwoTierReaper;
use ferrodb::branch::types::{BranchId, LeaseDeadline, ARENA_EXTENT_PAGES};
use ferrodb::branch::{BranchCatalog, Reaper};
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::cluster::{self, ClusterScope, GrantError};
use ferrodb::consensus::NodeId;
use ferrodb::cow::{PageStore, PageType};
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// Deliberately above anything the heap or index pages will need — a partition, not "wherever the
/// file happens to end". Matches `tests/integration_arena_exclusivity.rs`.
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

/// The pages an arena claim covers, as page ids.
fn extent_pages(start: u32) -> std::ops::Range<u32> {
    start..start + ARENA_EXTENT_PAGES
}

// =================================================================================================
// SINGLE NODE — the path 1349 tests depend on. If any of these break, everything else is moot.
// =================================================================================================

#[test]
fn a_node_with_no_cluster_configured_grants_itself_everything() {
    let _scope = ClusterScope::standalone();
    let s = store("solo_arena");

    // Exactly what the old `fetch_add` did: the first extent starts at the region base, and the
    // first arena id is 1 because ArenaId(0) is the shared/trunk sentinel.
    let a1 = s.store.arena_for(BranchId::TRUNK).unwrap();
    assert_eq!(a1.0, 1, "the first arena id changed, so every durable BranchRecord changed with it");
    assert_eq!(
        s.store.extent_range(a1).map(|r| r.0),
        Some(ARENA_BASE),
        "the first extent no longer starts at the region base"
    );
    assert_eq!(
        s.store.extent_watermark(),
        ARENA_BASE + ARENA_EXTENT_PAGES,
        "the watermark a checkpoint carries is not what fetch_add would have left"
    );

    // And it keeps working, extent after extent, with no grant anywhere in sight.
    let epoch = s.catalog.next_epoch();
    for _ in 0..(ARENA_EXTENT_PAGES + 8) {
        let a = s.store.arena_for(BranchId::TRUNK).unwrap();
        s.store.alloc_in_arena(a, PageType::BTreeLeaf, epoch).unwrap();
    }
}

#[test]
fn a_node_with_no_cluster_configured_still_begins_transactions() {
    let _scope = ClusterScope::standalone();
    let e = engine("solo_txn");

    // The WAL header seeds the counter at 1 on a fresh log, and `tests/sim_durability.rs`
    // hard-codes `first_txn_id() -> 1` into its durable-commit detector. A grant that changed this
    // would not fail that sweep, it would silently turn its detector off.
    assert_eq!(e.txn.begin().unwrap(), 1, "the first transaction id is no longer 1");
    for expect in 2..=300u64 {
        assert_eq!(e.txn.begin().unwrap(), expect);
    }
    assert_eq!(e.txn.next_txn_id(), 301, "the watermark the WAL header carries is wrong");
    assert_eq!(e.txn.read_snapshot().high_water, 301);
}

#[test]
fn a_node_with_no_cluster_configured_reads_its_own_wall_clock_for_leases() {
    let _scope = ClusterScope::standalone();
    let before = LeaseDeadline::now_millis();
    let d = LeaseDeadline::from_now(60_000);
    let after = LeaseDeadline::now_millis();
    assert!(after >= before, "the local clock went backwards");
    assert!(d.0 >= before + 60_000 && d.0 <= after + 60_000, "deadline {} is not now+60s", d.0);
    assert!(!d.is_expired_at(after), "a lease taken now is already expired");
    assert!(d.is_expired_at(d.0), "a lease is not expired at its own deadline");
}

#[test]
fn a_standalone_node_reaps_on_its_own_clock_exactly_as_before() {
    let _scope = ClusterScope::standalone();
    let s = store("solo_reap");
    let reaper = TwoTierReaper::new(Arc::clone(&s.catalog), Arc::clone(&s.store));

    let b = s.catalog.fork(BranchId::TRUNK, LeaseDeadline::from_now(10_000)).unwrap();
    assert!(
        reaper.reap_expired(LeaseDeadline::now_millis()).unwrap().is_empty(),
        "a live lease was reaped"
    );
    let reaped = reaper.reap_expired(LeaseDeadline::now_millis() + 60_000).unwrap();
    assert_eq!(reaped, vec![b.branch_id], "the expired branch was not reaped");
}

// =================================================================================================
// THE REFUSALS — a member that cannot prove it owns a value does not issue one.
// =================================================================================================

#[test]
fn a_member_with_no_extent_grant_refuses_to_allocate_an_arena() {
    let s = store("member_no_grant");
    let _scope = ClusterScope::joined(N1);

    // `examples/repl_primary.rs`: "every such page still passes its checksum, so refusing here is
    // the only detection point." This is that refusal.
    let err = s.store.arena_for(BranchId::TRUNK).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("no leader-granted"), "wrong refusal: {msg}");
    assert!(msg.contains("extent-start"), "the refusal does not name the counter: {msg}");

    // It stays refused. There is no second path that quietly succeeds.
    assert!(s.store.arena_for(BranchId::TRUNK).is_err());
    assert_eq!(s.store.reserved_page_count(), 0, "a refused claim still reserved pages");
}

#[test]
fn a_member_with_no_txn_grant_refuses_to_begin_a_transaction() {
    let e = engine("member_no_txn_grant");
    let _scope = ClusterScope::joined(N1);

    let err = e.txn.begin().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("no leader-granted"), "wrong refusal: {msg}");
    assert!(msg.contains("txn-id"), "the refusal does not name the counter: {msg}");

    // A refused begin leaves nothing half-open: the id is taken before anything is inserted into
    // the active-transaction table, so there is no entry to leak and no checkpoint to block.
    assert!(e.txn.begin_snapshot_read().is_err());
    assert_eq!(e.txn.next_txn_id(), 1, "a refused begin still moved the watermark");
}

#[test]
fn a_member_with_no_lease_tick_refuses_to_answer_what_time_it_is() {
    let _scope = ClusterScope::joined(N1);
    match LeaseDeadline::try_now_millis() {
        Err(GrantError::NoClusterTime { node }) => assert_eq!(node, N1),
        other => panic!("a node that has applied no LeaseTick answered the time: {other:?}"),
    }
    assert!(LeaseDeadline::try_from_now(60_000).is_err(), "a lease deadline was computed anyway");
}

#[test]
#[should_panic(expected = "does not know the cluster's time")]
fn the_infallible_lease_clock_aborts_rather_than_inventing_a_time() {
    // The infallible signature has no way to say "I do not know". Every value it could return is
    // worse: a local reading is the divergence being removed, a past one reaps live branches
    // unrecoverably, a future one silently stops reaping. So it aborts, loudly, naming the rule.
    let _scope = ClusterScope::joined(N1);
    let _ = LeaseDeadline::from_now(60_000);
}

// =================================================================================================
// THE GRANTS — what a member does once the leader has spoken.
// =================================================================================================

#[test]
fn a_member_allocates_from_its_grant_and_refuses_again_when_it_runs_out() {
    let s = store("member_granted");
    let _scope = ClusterScope::joined(N1);

    // Two extents' worth, and nothing more.
    s.store.apply_arena_grant(N1, 4096, 2 * ARENA_EXTENT_PAGES).unwrap();
    assert_eq!(s.store.grantable_extents(), 2);

    let a1 = s.store.arena_for(BranchId::TRUNK).unwrap();
    let b = s.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX / 2)).unwrap();
    let a2 = s.store.arena_for(b.branch_id).unwrap();

    let (s1, _) = s.store.extent_range(a1).unwrap();
    let (s2, _) = s.store.extent_range(a2).unwrap();
    assert_eq!(s1, 4096, "the first claim ignored the grant");
    assert_eq!(s2, 4096 + ARENA_EXTENT_PAGES, "the second claim ignored the grant");
    assert_ne!(a1, a2, "two live extents share one arena id");
    assert_eq!(s.store.grantable_extents(), 0);

    // A third branch has nothing left to claim, and is refused rather than served from a local
    // counter that would run straight into whatever the leader gave node 2.
    let c = s.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX / 2)).unwrap();
    assert!(s.store.arena_for(c.branch_id).is_err(), "an ungranted third extent was handed out");
}

#[test]
fn a_member_begins_transactions_from_its_granted_range_and_no_further() {
    let e = engine("member_txn_granted");
    let _scope = ClusterScope::joined(N1);

    e.txn.apply_txn_id_grant(N1, 5_000, 5_003).unwrap();
    assert_eq!(e.txn.begin().unwrap(), 5_000);
    assert_eq!(e.txn.begin().unwrap(), 5_001);
    assert_eq!(e.txn.begin().unwrap(), 5_002);
    assert!(e.txn.begin().is_err(), "a fourth id came out of a grant of three");
    assert_eq!(e.txn.next_txn_id(), 5_003);
}

#[test]
fn a_grant_addressed_to_another_node_is_refused_by_this_one() {
    let s = store("wrong_node_arena");
    let e = engine("wrong_node_txn");
    let _scope = ClusterScope::joined(N1);

    // Every node applies every committed entry, so node 1 sees node 2's grants. Taking one is
    // exactly the two-nodes-one-page failure this row exists to prevent.
    let err = s.store.apply_arena_grant(N2, 8192, ARENA_EXTENT_PAGES).unwrap_err();
    assert!(format!("{err}").contains("addressed to n2"), "wrong refusal: {err}");
    assert_eq!(s.store.grantable_extents(), 0, "a foreign grant was absorbed anyway");
    assert!(s.store.arena_for(BranchId::TRUNK).is_err());

    let err = e.txn.apply_txn_id_grant(N2, 900, 999).unwrap_err();
    assert!(format!("{err}").contains("addressed to n2"), "wrong refusal: {err}");
    assert!(e.txn.begin().is_err(), "a foreign txn grant was absorbed anyway");
}

#[test]
fn a_standalone_node_refuses_a_grant_it_was_never_supposed_to_get() {
    let s = store("standalone_grant");
    let _scope = ClusterScope::standalone();
    let err = s.store.apply_arena_grant(N1, 8192, ARENA_EXTENT_PAGES).unwrap_err();
    assert!(format!("{err}").contains("no cluster configured"), "wrong refusal: {err}");
}

#[test]
fn a_redelivered_grant_does_not_hand_out_the_same_extent_twice() {
    let s = store("redelivered");
    let _scope = ClusterScope::joined(N1);

    // A committed round may be delivered more than once — `WalBatch` is idempotent for exactly
    // this reason — so applying one twice must be a no-op, not a second range.
    s.store.apply_arena_grant(N1, 4096, ARENA_EXTENT_PAGES).unwrap();
    let a1 = s.store.arena_for(BranchId::TRUNK).unwrap();
    s.store.apply_arena_grant(N1, 4096, ARENA_EXTENT_PAGES).unwrap();

    let b = s.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX / 2)).unwrap();
    assert!(
        s.store.arena_for(b.branch_id).is_err(),
        "the re-delivered grant handed out a second extent over the first"
    );
    assert_eq!(s.store.extent_range(a1).map(|r| r.0), Some(4096));
}

// =================================================================================================
// TWO NODES — the failure the whole row is about.
// =================================================================================================

#[test]
fn two_nodes_with_disjoint_grants_never_hand_out_the_same_physical_page() {
    // The leader grants node 1 and node 2 adjacent, disjoint ranges. Each node claims everything
    // it may, and the two page sets must not intersect. Before this row both nodes started at
    // `base_page` and produced identical page ids — and "every such page still passes its
    // checksum", so nothing downstream would have noticed.
    let pages_of = |tag: &str, me: NodeId, first: u32, extents: u32| -> Vec<u32> {
        let s = store(tag);
        let _scope = ClusterScope::joined(me);
        s.store.apply_arena_grant(me, first, extents * ARENA_EXTENT_PAGES).unwrap();
        let mut out = Vec::new();
        let mut parent = BranchId::TRUNK;
        for _ in 0..extents {
            let a = s.store.arena_for(parent).unwrap();
            out.extend(extent_pages(s.store.extent_range(a).unwrap().0));
            parent = s.catalog.fork(parent, LeaseDeadline(u64::MAX / 2)).unwrap().branch_id;
        }
        // Both nodes also see the other's grant, and both must refuse it.
        let other = if me == N1 { N2 } else { N1 };
        assert!(s.store.apply_arena_grant(other, 99_000, ARENA_EXTENT_PAGES).is_err());
        out
    };

    let a = pages_of("two_node_a", N1, 4096, 3);
    let b = pages_of("two_node_b", N2, 4096 + 3 * ARENA_EXTENT_PAGES, 3);

    assert_eq!(a.len(), 3 * ARENA_EXTENT_PAGES as usize, "fixture: node 1 claimed nothing");
    assert_eq!(b.len(), 3 * ARENA_EXTENT_PAGES as usize, "fixture: node 2 claimed nothing");
    let set: std::collections::BTreeSet<u32> = a.iter().copied().collect();
    let clash: Vec<u32> = b.iter().copied().filter(|p| set.contains(p)).collect();
    assert!(clash.is_empty(), "two nodes were handed the same physical pages: {clash:?}");
}

#[test]
fn two_nodes_with_disjoint_grants_never_issue_the_same_transaction_id() {
    let ids_of = |tag: &str, me: NodeId, lo: u64, n: u64| -> Vec<u64> {
        let e = engine(tag);
        let _scope = ClusterScope::joined(me);
        e.txn.apply_txn_id_grant(me, lo, lo + n).unwrap();
        (0..n).map(|_| e.txn.begin().unwrap()).collect()
    };
    let a = ids_of("two_txn_a", N1, 1_000, 50);
    let b = ids_of("two_txn_b", N2, 1_050, 50);
    let set: std::collections::BTreeSet<u64> = a.iter().copied().collect();
    assert!(b.iter().all(|i| !set.contains(i)), "two nodes issued the same txn id");
    assert_eq!(set.len(), 50, "one node issued a duplicate to itself");
}

// =================================================================================================
// THE CLUSTER CLOCK — exit criterion 9.
// =================================================================================================

#[test]
fn two_nodes_at_the_same_tick_agree_about_whether_a_branch_is_live() {
    // The lease deadline is computed from the applied tick, not from a wall clock, so two nodes
    // applying the same `LeaseTick` compute the SAME deadline for the same `lease_millis` — which
    // is what lets the durable `lease_deadline` in a branch record agree without shipping it.
    let deadline_at = |me: NodeId, tick: u64| -> LeaseDeadline {
        let _scope = ClusterScope::joined(me);
        cluster::apply_lease_tick(tick).unwrap();
        LeaseDeadline::try_from_now(900_000).unwrap()
    };
    let tick = 1_700_000_000_000u64;
    let on_n1 = deadline_at(N1, tick);
    let on_n2 = deadline_at(N2, tick);
    assert_eq!(on_n1, on_n2, "two nodes computed different deadlines from one replicated tick");
    assert_eq!(on_n1.0, tick + 900_000);

    // And they agree about expiry, because expiry is a pure comparison against a replicated value.
    assert!(!on_n1.is_expired_at(tick));
    assert!(on_n2.is_expired_at(tick + 900_000));
}

#[test]
fn a_replicated_tick_and_not_the_wall_clock_decides_a_reap() {
    let s = store("cluster_reap");
    let reaper = TwoTierReaper::new(Arc::clone(&s.catalog), Arc::clone(&s.store));
    let _scope = ClusterScope::joined(N1);

    // A tick deliberately far from any real wall clock. If anything here read `SystemTime::now()`
    // the branch below would be reaped immediately, because its deadline is in 1970.
    let tick = 60_000u64;
    cluster::apply_lease_tick(tick).unwrap();
    let lease = LeaseDeadline::try_from_now(10_000).unwrap();
    assert_eq!(lease.0, 70_000);
    let b = s.catalog.fork(BranchId::TRUNK, lease).unwrap();

    assert!(
        reaper.reap_expired(LeaseDeadline::try_now_millis().unwrap()).unwrap().is_empty(),
        "a branch whose cluster lease has not expired was reaped"
    );

    // Time advances only when the cluster says so.
    cluster::apply_lease_tick(80_000).unwrap();
    assert_eq!(LeaseDeadline::try_now_millis().unwrap(), 80_000);
    assert_eq!(
        reaper.reap_expired(LeaseDeadline::try_now_millis().unwrap()).unwrap(),
        vec![b.branch_id],
        "the branch did not expire once the cluster's clock passed its deadline"
    );
}

#[test]
fn a_lease_tick_never_moves_the_clusters_clock_backwards() {
    let _scope = ClusterScope::joined(N1);
    // A re-delivered or reordered suffix of the log is ordinary. Expiry that could move backwards
    // would un-expire a branch a peer has already decided to reap — and a reap is unrecoverable,
    // so the two nodes could never be reconciled afterwards.
    assert_eq!(cluster::apply_lease_tick(5_000).unwrap(), 5_000);
    assert_eq!(cluster::apply_lease_tick(9_000).unwrap(), 9_000);
    assert_eq!(cluster::apply_lease_tick(1_000).unwrap(), 9_000, "the cluster clock rewound");
    assert_eq!(LeaseDeadline::try_now_millis().unwrap(), 9_000);
}

#[test]
fn a_standalone_node_refuses_a_lease_tick() {
    let _scope = ClusterScope::standalone();
    let err = cluster::apply_lease_tick(1_000).unwrap_err();
    assert_eq!(err, GrantError::NotClustered { counter: "lease-tick" });
}

// =================================================================================================
// AUTHORITY CHANGE — space granted under one authority is not this node's under another.
// =================================================================================================

#[test]
fn space_a_node_self_granted_while_standalone_is_revoked_when_it_joins() {
    let s = store("epoch_arena");
    {
        // Standalone: it grants itself, and holds more than it has issued.
        let _solo = ClusterScope::standalone();
        s.store.arena_for(BranchId::TRUNK).unwrap();
        assert!(s.store.grantable_extents() > 0, "fixture: nothing is held, so this proves nothing");
    }
    // Joining a cluster: whatever it was holding is space the leader does not know about and will
    // hand to somebody else.
    let _scope = ClusterScope::joined(N1);
    assert_eq!(s.store.grantable_extents(), 0, "self-granted space survived the join");
    let b = s.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX / 2)).unwrap();
    assert!(s.store.arena_for(b.branch_id).is_err(), "a joined node allocated from stale space");
}

#[test]
fn a_recycled_extent_start_from_a_previous_authority_is_not_handed_out_again() {
    // The recycle stack is the one piece of issued space that lives outside the grant book, so it
    // needs the same rule. A page freed while standalone is not this node's to reuse once a leader
    // owns the address space.
    let s = store("epoch_recycle");
    let freed_start;
    {
        let _solo = ClusterScope::standalone();
        let b = s.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX / 2)).unwrap();
        let a = s.store.arena_for(b.branch_id).unwrap();
        freed_start = s.store.extent_range(a).unwrap().0;
        s.store.free_arena(a).unwrap();
    }
    let _scope = ClusterScope::joined(N1);
    s.store.apply_arena_grant(N1, 60_000, ARENA_EXTENT_PAGES).unwrap();
    let c = s.catalog.fork(BranchId::TRUNK, LeaseDeadline(u64::MAX / 2)).unwrap();
    let a = s.store.arena_for(c.branch_id).unwrap();
    assert_eq!(
        s.store.extent_range(a).map(|r| r.0),
        Some(60_000),
        "the recycled start from the standalone era was handed out under the leader's authority"
    );
    assert_ne!(s.store.extent_range(a).map(|r| r.0), Some(freed_start));
}

// =================================================================================================
// CRASH AND RESTART — the durable watermark, and what replaying the log may re-offer.
// =================================================================================================

#[test]
fn a_restart_replays_its_grant_and_resumes_above_every_page_it_already_issued() {
    let s = store("restart");
    let path = s._dir.path().join("restart.arena");
    let _scope = ClusterScope::joined(N1);

    // A wide grant, partly consumed, checkpointed.
    s.store.apply_arena_grant(N1, 4096, 8 * ARENA_EXTENT_PAGES).unwrap();
    let mut parent = BranchId::TRUNK;
    for _ in 0..3 {
        s.store.arena_for(parent).unwrap();
        parent = s.catalog.fork(parent, LeaseDeadline(u64::MAX / 2)).unwrap().branch_id;
    }
    let watermark = s.store.extent_watermark();
    assert_eq!(watermark, 4096 + 3 * ARENA_EXTENT_PAGES);
    s.store.checkpoint(&path).unwrap();

    // Restart. The image restores the watermark; the grant itself comes back by replaying the log.
    let reopened = ArenaPageStore::reopen_from_checkpoint(
        Arc::new(BufferPoolManager::new(Arc::new(
            DiskManager::new(
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(s._dir.path().join("restart2.db"))
                    .unwrap(),
            )
            .unwrap(),
        ))),
        Arc::clone(&s.catalog),
        &path,
    )
    .unwrap();

    // Before the replay it holds nothing, because a checkpoint records what was ISSUED, never what
    // may be issued. A restarted member refuses until the log tells it what it owns.
    assert_eq!(reopened.grantable_extents(), 0);
    assert_eq!(reopened.extent_watermark(), watermark);
    assert!(reopened.arena_for(BranchId::TRUNK).is_err(), "a restart invented a grant");

    // Replay the same entry. Only its unconsumed suffix may be issued.
    reopened.apply_arena_grant(N1, 4096, 8 * ARENA_EXTENT_PAGES).unwrap();
    assert_eq!(reopened.grantable_extents(), 5, "the replay re-offered pages already handed out");
    let a = reopened.arena_for(BranchId::TRUNK).unwrap();
    assert_eq!(
        reopened.extent_range(a).map(|r| r.0),
        Some(watermark),
        "recovery resumed below a page the crashed session had already handed out"
    );
}

#[test]
fn recovery_raises_the_transaction_watermark_without_inventing_a_grant() {
    let e = engine("recover_txn");
    let _scope = ClusterScope::joined(N1);

    // What `wal::recovery::recover` does: it scans the retained records and reports one past the
    // highest id it saw. That is a statement about what was USED, and it must not become
    // permission to use more.
    e.txn.raise_next_txn_id(9_000);
    assert_eq!(e.txn.next_txn_id(), 9_000);
    assert!(e.txn.begin().is_err(), "a recovered watermark was mistaken for a grant");

    // Monotone: a second, lower report cannot lower it.
    e.txn.raise_next_txn_id(42);
    assert_eq!(e.txn.next_txn_id(), 9_000, "the watermark was lowered and will re-issue ids");

    // A grant that reaches back below the watermark yields only its suffix.
    e.txn.apply_txn_id_grant(N1, 8_990, 9_003).unwrap();
    assert_eq!(e.txn.begin().unwrap(), 9_000);
    assert_eq!(e.txn.begin().unwrap(), 9_001);
    assert_eq!(e.txn.begin().unwrap(), 9_002);
    assert!(e.txn.begin().is_err());
}

#[test]
fn the_checkpoint_image_is_byte_identical_to_what_a_node_local_counter_wrote() {
    // The arena image has no version tolerance (`load_state` refuses an unknown version and
    // rejects trailing bytes), `key_order_in_image` hard-codes a 21-byte header, and
    // `two_stores_in_the_same_state_checkpoint_byte_identical_images` demands byte equality. So
    // the grant book must be invisible to the image: only the issued watermark is written, into
    // exactly the slots the two `AtomicU32`s occupied.
    let _scope = ClusterScope::standalone();
    let s = store("image");
    s.store.arena_for(BranchId::TRUNK).unwrap();
    let bytes = s.store.state_bytes();

    let u32_at = |at: usize| u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap());
    assert_eq!(bytes[0], 2, "the state version changed; every <db>.arena on disk is now unreadable");
    assert_eq!(u32_at(1), ARENA_BASE, "base_page moved");
    assert_eq!(u32_at(5), ARENA_BASE + ARENA_EXTENT_PAGES, "next_extent_start slot is not the watermark");
    assert_eq!(u32_at(9), 2, "next_arena_id slot is not the watermark");

    // A store that has self-granted far past what it issued writes the same header as one that has
    // not, which is what keeps two nodes' images comparable.
    let t = store("image2");
    t.store.arena_for(BranchId::TRUNK).unwrap();
    assert_eq!(bytes, t.store.state_bytes(), "the grant book leaked into the durable image");
}

// =================================================================================================
// THE SEAM — the appliers are what a consensus Apply action calls.
// =================================================================================================

#[test]
fn the_three_appliers_take_exactly_the_frozen_commands_shape() {
    // Written as a compile-and-run check that the seam matches `consensus::Command`: if a variant
    // is reshaped, this stops building rather than silently diverging from the log format.
    use ferrodb::consensus::Command;
    let s = store("seam");
    let e = engine("seam");
    let _scope = ClusterScope::joined(N1);

    let cmds = vec![
        Command::ArenaGrant { node: N1, first_page: 20_000, page_count: ARENA_EXTENT_PAGES },
        Command::TxnIdRange { node: N1, lo: 700, hi: 800 },
        Command::LeaseTick { unix_millis: 1_700_000_000_000 },
    ];
    for c in cmds {
        match c {
            Command::ArenaGrant { node, first_page, page_count } => {
                s.store.apply_arena_grant(node, first_page, page_count).unwrap()
            }
            Command::TxnIdRange { node, lo, hi } => e.txn.apply_txn_id_grant(node, lo, hi).unwrap(),
            Command::LeaseTick { unix_millis } => {
                cluster::apply_lease_tick(unix_millis).unwrap();
            }
            _ => unreachable!("this test builds only the three F4 commands"),
        }
    }

    assert_eq!(s.store.extent_range(s.store.arena_for(BranchId::TRUNK).unwrap()).map(|r| r.0), Some(20_000));
    assert_eq!(e.txn.begin().unwrap(), 700);
    assert_eq!(LeaseDeadline::try_now_millis().unwrap(), 1_700_000_000_000);
}

/// Keeps the `Reaper` trait import honest — `reap_expired` is reached through it above.
#[test]
fn the_reaper_trait_is_the_path_under_test() {
    let _scope = ClusterScope::standalone();
    let s = store("trait_path");
    let reaper: Arc<dyn Reaper> = Arc::new(TwoTierReaper::new(Arc::clone(&s.catalog), Arc::clone(&s.store)));
    assert!(reaper.reap_expired(LeaseDeadline::now_millis()).unwrap().is_empty());
}
