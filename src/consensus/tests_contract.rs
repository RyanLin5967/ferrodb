//! Invariants of the contract itself — the ones that must hold before any rule is written, and that
//! every owner's file is built against.

use super::*;

#[test]
fn a_leaders_lease_expires_strictly_before_any_peer_can_win() {
    // The defect this pins: when the lease and the election timeout were the same number, a peer
    // drawing a short timeout campaigned while a leader on a long one still believed it held
    // office -- which is the two-leader overlap the lease exists to prevent. The drawn timeout
    // lies in [base, 2*base), so the lease must be strictly below `base` to be safe against the
    // shortest draw any peer can make.
    let c = Consensus::new(NodeId(1), config::Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 1), 7);
    assert!(
        c.lease < c.election_base,
        "a leader's lease ({}) is not strictly shorter than the shortest election timeout a peer \
         can draw ({}), so a peer can win while the old leader still believes it leads",
        c.lease, c.election_base
    );
}

#[test]
fn an_empty_configuration_never_has_a_quorum() {
    // `0 / 2 + 1 == 1`, so the arithmetic alone would let a joining node -- which holds an empty
    // configuration -- elect itself leader of a cluster that has not admitted it.
    let empty = config::Config::empty();
    assert!(!empty.has_quorum(1), "an empty configuration granted a quorum to a single vote");
    assert!(!empty.has_quorum(usize::MAX), "an empty configuration granted a quorum at all");
}

#[test]
fn a_repeated_member_cannot_inflate_the_quorum() {
    // One node counted twice is one vote counted twice.
    let c = config::Config::new([NodeId(1), NodeId(1), NodeId(2)], 1, 1);
    assert_eq!(c.len(), 2, "a duplicate member survived construction");
    assert_eq!(c.quorum(), 2);
}

#[test]
fn quorum_is_a_strict_majority_at_every_size() {
    for (n, want) in [(1usize, 1usize), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4), (7, 4)] {
        let cfg = config::Config::new((1..=n as u32).map(NodeId), 1, 1);
        assert_eq!(cfg.quorum(), want, "quorum of a {n}-node cluster");
    }
}

#[test]
fn a_joining_node_may_not_campaign_and_each_flag_is_cleared_by_its_own_evidence() {
    let c = Consensus::joining(NodeId(9), 3);
    assert!(!c.may_campaign(), "a node that knows neither the configuration nor the log stood for election");
    assert!(c.behind && c.unjoined, "joining must set both flags -- they are cleared by different evidence");
}

#[test]
fn a_node_outside_its_own_configuration_may_not_campaign() {
    // A removed node keeps running. It must not stand for election in a cluster it has left.
    let c = Consensus::new(NodeId(4), config::Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 1), 5);
    assert!(!c.may_campaign(), "a node absent from its own configuration stood for election");
}

#[test]
fn a_learner_is_sent_to_but_never_counted() {
    let cfg = config::Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 1)
        .with_learners([NodeId(4)]);
    assert!(cfg.is_known(NodeId(4)), "a learner must still be sent the log");
    assert!(!cfg.contains(NodeId(4)), "a learner was counted as a voter");
    assert_eq!(cfg.quorum(), 2, "a learner enlarged the quorum, reducing availability");
}

#[test]
fn a_voter_cannot_be_demoted_to_learner_by_a_careless_builder() {
    // Silently accepting this would shrink the voter set, and therefore the quorum, without any
    // membership change having been proposed.
    let cfg = config::Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 1)
        .with_learners([NodeId(2)]);
    assert!(cfg.contains(NodeId(2)), "a voter was demoted to learner by with_learners");
    assert_eq!(cfg.len(), 3);
}

#[test]
fn the_rng_is_reproducible_from_a_seed_and_a_zero_seed_is_not_a_fixed_point() {
    let mut a = Rng::new(42);
    let mut b = Rng::new(42);
    let xs: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
    let ys: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
    assert_eq!(xs, ys, "the same seed produced different sequences, so no failure is replayable");

    // Zero is a fixed point of xorshift: every node would draw the same timeout for ever, which is
    // a split vote that never resolves. Substituted at construction rather than asserted, because
    // 0 is the most likely seed a caller picks.
    let mut z = Rng::new(0);
    let first = z.next_u64();
    let second = z.next_u64();
    assert_ne!(first, 0, "a zero seed produced zero");
    assert_ne!(first, second, "a zero seed produced a constant sequence");
}

#[test]
fn timeouts_are_drawn_inside_the_documented_window() {
    // The lease invariant above is only sound if this window is what `new` actually draws from.
    for seed in 1..200u64 {
        let c = Consensus::new(NodeId(1), config::Config::new([NodeId(1), NodeId(2)], 1, 1), seed);
        assert!(
            c.election_timeout >= c.election_base && c.election_timeout < 2 * c.election_base,
            "seed {seed} drew {} outside [{}, {})",
            c.election_timeout, c.election_base, 2 * c.election_base
        );
    }
}
