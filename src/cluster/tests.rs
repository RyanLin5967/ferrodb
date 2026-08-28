//! Rule tests for the grant substrate.
//!
//! Every test here drives [`Grants`] and the pure clock helpers **directly**, passing the
//! authority and epoch as arguments rather than arming the process. That is deliberate and not
//! merely tidy: `cargo test` runs a binary's tests as threads of one process, the authority is
//! process-scoped, and a unit test that joined a cluster would make `branch/reaper.rs`'s and
//! `cow/btree.rs`'s lease-taking tests refuse in the same run. The process wiring is tested in
//! `tests/integration_cluster_grants.rs`, whose binary contains nothing else.
//!
//! Each test names the rule it pins. Every one of them has been run against a deliberately broken
//! copy of the rule and seen to fail — recorded in `scratchpad/F4-clusterstate.md`.

use super::*;

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);

/// A member holding nothing, at epoch 1.
fn member(counter: &'static str, start: u64) -> Grants {
    Grants::new(counter, start, 64)
}

// ---- the rule the row exists for ---------------------------------------------------------------

#[test]
fn a_member_with_no_grant_refuses_rather_than_allocating() {
    let mut g = member("extent-page", 256);
    let err = g.take(256, Authority::Member(N1), 1).unwrap_err();
    assert_eq!(err, GrantError::Exhausted { counter: "extent-page", node: N1, need: 256 });
    // And it stays refused: a second ask must not quietly succeed by some other path.
    assert!(g.take(1, Authority::Member(N1), 1).is_err());
    assert_eq!(g.issued, 256, "a refused take must not move the watermark");
}

#[test]
fn a_member_refuses_again_once_its_grant_is_used_up() {
    let mut g = member("txn-id", 1);
    g.apply_grant(N1, 10, 13, Authority::Member(N1), 1).unwrap();
    assert_eq!(g.take(1, Authority::Member(N1), 1).unwrap(), 10);
    assert_eq!(g.take(1, Authority::Member(N1), 1).unwrap(), 11);
    assert_eq!(g.take(1, Authority::Member(N1), 1).unwrap(), 12);
    // Exactly three values were granted and exactly three were issued.
    assert_eq!(
        g.take(1, Authority::Member(N1), 1).unwrap_err(),
        GrantError::Exhausted { counter: "txn-id", node: N1, need: 1 }
    );
}

// ---- single-node operation, which 1349 tests depend on -----------------------------------------

#[test]
fn a_standalone_node_issues_exactly_what_the_old_atomic_counter_did() {
    // The old code was `next_txn_id.fetch_add(1)` from `wal.header_txn_id`. The values must be
    // identical, or every test that names a txn id changes meaning.
    let mut g = Grants::new("txn-id", 1, 64);
    for expect in 1..=200u64 {
        assert_eq!(g.take(1, Authority::Standalone, 0).unwrap(), expect);
    }
    assert_eq!(g.issued, 201);
}

#[test]
fn a_standalone_extent_counter_walks_the_same_page_starts_as_fetch_add() {
    // The old code was `next_extent_start.fetch_add(extent_pages)` from `base_page`.
    let mut g = Grants::new("extent-page", 256, 256 * 8);
    for k in 0..40u64 {
        assert_eq!(g.take(256, Authority::Standalone, 0).unwrap(), 256 + k * 256);
    }
}

#[test]
fn a_standalone_node_self_grants_through_the_same_consume_path() {
    // There is one consume path, so single-node running is what exercises it. If `held` were
    // bypassed for standalone, this would read zero.
    let mut g = Grants::new("arena-id", 1, 4);
    assert_eq!(g.take(1, Authority::Standalone, 0).unwrap(), 1);
    assert_eq!(g.remaining_values(), 3, "a self-grant of 4 with one issued leaves three");
}

// ---- the guards ---------------------------------------------------------------------------------

#[test]
fn a_grant_addressed_to_another_node_is_refused() {
    // Every node applies every committed entry, so n2 sees n1's grant. Taking it is the
    // two-nodes-one-page failure.
    let mut g = member("extent-page", 256);
    let err = g.apply_grant(N1, 512, 768, Authority::Member(N2), 1).unwrap_err();
    assert_eq!(
        err,
        GrantError::WrongNode { counter: "extent-page", granted_to: N1, self_id: N2 }
    );
    assert!(g.take(1, Authority::Member(N2), 1).is_err(), "the refused grant left nothing behind");
}

#[test]
fn a_standalone_node_refuses_an_outside_grant() {
    let mut g = Grants::new("extent-page", 256, 256);
    let err = g.apply_grant(N1, 512, 768, Authority::Standalone, 0).unwrap_err();
    assert_eq!(err, GrantError::NotClustered { counter: "extent-page" });
}

#[test]
fn an_empty_grant_is_refused_rather_than_ignored() {
    let mut g = member("txn-id", 1);
    assert_eq!(
        g.apply_grant(N1, 40, 40, Authority::Member(N1), 1).unwrap_err(),
        GrantError::EmptyRange { counter: "txn-id", lo: 40, hi: 40 }
    );
    assert_eq!(
        g.apply_grant(N1, 40, 10, Authority::Member(N1), 1).unwrap_err(),
        GrantError::EmptyRange { counter: "txn-id", lo: 40, hi: 10 }
    );
}

#[test]
fn a_redelivered_grant_issues_nothing_twice() {
    // A committed round may be re-delivered — `WalBatch` is idempotent for exactly this reason.
    // Re-adding a range already issued from hands one page to two branches on ONE node.
    let mut g = member("extent-page", 256);
    assert_eq!(
        g.apply_grant(N1, 256, 512, Authority::Member(N1), 1).unwrap(),
        Applied::Accepted { usable: 256 }
    );
    assert_eq!(g.take(256, Authority::Member(N1), 1).unwrap(), 256);
    assert_eq!(
        g.apply_grant(N1, 256, 512, Authority::Member(N1), 1).unwrap(),
        Applied::Duplicate
    );
    assert_eq!(
        g.take(1, Authority::Member(N1), 1).unwrap_err(),
        GrantError::Exhausted { counter: "extent-page", node: N1, need: 1 },
        "the duplicate must have added nothing"
    );
}

#[test]
fn ranges_from_a_superseded_authority_are_not_issued_from() {
    // A store that self-granted while standalone, in a process that then joins a cluster, holds
    // space no leader knows about — and the leader will hand it to somebody else.
    // Chunk deliberately wider than the take, so a range really is left held under epoch 0 —
    // with chunk == take the self-grant is consumed exactly and there would be nothing stale to
    // test, and the assertion below would pass for the wrong reason.
    let mut g = Grants::new("extent-page", 256, 1024);
    assert_eq!(g.take(256, Authority::Standalone, 0).unwrap(), 256);
    assert_eq!(g.remaining_values(), 768, "fixture: nothing is held, so this proves nothing");

    // Epoch 1: joined a cluster. Whatever was held under epoch 0 is not ours.
    assert_eq!(
        g.take(1, Authority::Member(N1), 1).unwrap_err(),
        GrantError::Exhausted { counter: "extent-page", node: N1, need: 1 }
    );
    assert_eq!(g.remaining_values(), 0, "stale ranges are dropped, not merely skipped");
}

#[test]
fn a_grant_from_a_previous_epoch_is_not_issued_from_after_leaving() {
    let mut g = member("txn-id", 1);
    g.apply_grant(N1, 100, 200, Authority::Member(N1), 1).unwrap();
    assert_eq!(g.take(1, Authority::Member(N1), 1).unwrap(), 100);
    // Left the cluster: epoch 2, standalone. The leader's range is no longer ours to issue.
    let v = g.take(1, Authority::Standalone, 2).unwrap();
    assert!(v >= 200, "a standalone self-grant must start above everything already accepted, got {v}");
}

// ---- two nodes, which is the whole point --------------------------------------------------------

#[test]
fn two_members_with_disjoint_grants_never_issue_the_same_value() {
    let mut a = member("extent-page", 256);
    let mut b = member("extent-page", 256);
    a.apply_grant(N1, 256, 1024, Authority::Member(N1), 1).unwrap();
    b.apply_grant(N2, 1024, 2048, Authority::Member(N2), 1).unwrap();
    // Both nodes also see the *other's* grant, and both must refuse it.
    assert!(a.apply_grant(N2, 1024, 2048, Authority::Member(N1), 1).is_err());
    assert!(b.apply_grant(N1, 256, 1024, Authority::Member(N2), 1).is_err());

    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..3 {
        assert!(seen.insert(a.take(256, Authority::Member(N1), 1).unwrap()));
        assert!(seen.insert(b.take(256, Authority::Member(N2), 1).unwrap()));
    }
    assert_eq!(seen.len(), 6, "six extents were issued and every start page is distinct");
}

// ---- crash and restart --------------------------------------------------------------------------

#[test]
fn a_grant_partly_consumed_before_a_crash_resumes_at_the_watermark() {
    // The durable image carries the *issued* watermark; the grant itself comes back by replaying
    // the log. Re-applying it must yield only the unconsumed suffix.
    let mut before = member("extent-page", 256);
    before.apply_grant(N1, 256, 1280, Authority::Member(N1), 1).unwrap();
    assert_eq!(before.take(256, Authority::Member(N1), 1).unwrap(), 256);
    assert_eq!(before.take(256, Authority::Member(N1), 1).unwrap(), 512);
    let durable = before.issued;
    assert_eq!(durable, 768);

    // Restart: watermark restored, held ranges empty, then the log replays the same grant.
    let mut after = Grants::new("extent-page", durable, 64);
    assert_eq!(
        after.apply_grant(N1, 256, 1280, Authority::Member(N1), 1).unwrap(),
        Applied::Accepted { usable: 1280 - 768 }
    );
    assert_eq!(
        after.take(256, Authority::Member(N1), 1).unwrap(),
        768,
        "recovery must resume above every page the crashed session handed out"
    );
}

#[test]
fn a_grant_wholly_below_the_restored_watermark_is_a_duplicate_not_a_rewind() {
    let mut g = Grants::new("txn-id", 500, 64);
    assert_eq!(
        g.apply_grant(N1, 100, 200, Authority::Member(N1), 1).unwrap(),
        Applied::Duplicate
    );
    assert!(g.take(1, Authority::Member(N1), 1).is_err(), "nothing was added");
}

// ---- range bookkeeping --------------------------------------------------------------------------

#[test]
fn a_range_too_narrow_for_the_ask_is_kept_rather_than_split_or_dropped() {
    // `n` is 1 for ids and a whole extent for pages. A range that cannot answer an extent can
    // still answer an id, and dropping it would leak space no leader grants again.
    let mut g = member("extent-page", 0);
    g.apply_grant(N1, 10, 20, Authority::Member(N1), 1).unwrap(); // 10 wide
    g.apply_grant(N1, 100, 400, Authority::Member(N1), 1).unwrap(); // 300 wide
    assert_eq!(
        g.take(256, Authority::Member(N1), 1).unwrap(),
        100,
        "the ask must be answered from the range that fits, skipping the narrow one"
    );
    assert_eq!(g.take(10, Authority::Member(N1), 1).unwrap(), 10, "the narrow range survived");
}

#[test]
fn a_take_never_straddles_two_granted_ranges() {
    // Two adjacent-but-separate grants must not be fused into one allocation: the pages between
    // them belong to the same node here, but a grant is the unit of agreement and splicing two
    // would make an allocation that no single entry authorised.
    let mut g = member("extent-page", 0);
    g.apply_grant(N1, 0, 128, Authority::Member(N1), 1).unwrap();
    g.apply_grant(N1, 128, 256, Authority::Member(N1), 1).unwrap();
    assert_eq!(
        g.take(256, Authority::Member(N1), 1).unwrap_err(),
        GrantError::Exhausted { counter: "extent-page", node: N1, need: 256 }
    );
    assert_eq!(g.take(128, Authority::Member(N1), 1).unwrap(), 0);
    assert_eq!(g.take(128, Authority::Member(N1), 1).unwrap(), 128);
}

#[test]
fn the_issued_watermark_is_the_only_thing_a_checkpoint_has_to_carry() {
    let mut g = member("arena-id", 1);
    g.apply_grant(N1, 1, 1000, Authority::Member(N1), 1).unwrap();
    for _ in 0..7 {
        g.take(1, Authority::Member(N1), 1).unwrap();
    }
    assert_eq!(g.issued, 8, "seven ids issued from 1 leaves the watermark at 8");
}

// ---- the cluster clock ---------------------------------------------------------------------------

#[test]
fn a_member_with_no_lease_tick_refuses_to_decide_a_lease() {
    assert_eq!(
        lease_source(Authority::Member(N1), None).unwrap_err(),
        GrantError::NoClusterTime { node: N1 }
    );
}

#[test]
fn a_standalone_node_reads_its_own_wall_clock_because_it_is_the_cluster() {
    assert_eq!(lease_source(Authority::Standalone, None).unwrap(), LeaseSource::LocalWall);
    // Even if a stale cluster reading is lying around, standalone means the local clock.
    assert_eq!(lease_source(Authority::Standalone, Some(5)).unwrap(), LeaseSource::LocalWall);
}

#[test]
fn a_member_reads_the_replicated_tick_and_never_its_own_clock() {
    assert_eq!(
        lease_source(Authority::Member(N1), Some(1_700_000_000_000)).unwrap(),
        LeaseSource::Cluster(1_700_000_000_000)
    );
}

#[test]
fn two_members_at_the_same_tick_cannot_disagree_about_expiry() {
    // Exit criterion 9, at the level this module can answer it: expiry is a pure comparison
    // against a value both nodes were *told*, so there is nothing left to disagree about.
    let tick = 1_700_000_000_000u64;
    let a = lease_source(Authority::Member(N1), Some(tick)).unwrap();
    let b = lease_source(Authority::Member(N2), Some(tick)).unwrap();
    assert_eq!(a, b);
}

#[test]
fn a_lease_tick_never_moves_cluster_time_backwards() {
    // A re-delivered or reordered suffix of the log is normal. Expiry that could move backwards
    // would un-expire a branch a peer has already decided to reap.
    assert_eq!(fold_tick(None, 100), 100);
    assert_eq!(fold_tick(Some(100), 250), 250);
    assert_eq!(fold_tick(Some(250), 100), 250);
    assert_eq!(fold_tick(Some(250), 250), 250);
}

// ---- value-space exhaustion ----------------------------------------------------------------------

#[test]
fn a_standalone_node_at_the_end_of_the_value_space_refuses_distinguishably() {
    // Not reported as a missing grant: no leader can answer this one, and saying "ask the leader"
    // would send an operator hunting a problem that is not there.
    let mut g = Grants::new("txn-id", u64::MAX, 64);
    assert_eq!(
        g.take(1, Authority::Standalone, 0).unwrap_err(),
        GrantError::SpaceExhausted { counter: "txn-id", issued_through: u64::MAX }
    );
}
