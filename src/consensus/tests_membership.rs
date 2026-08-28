//! F5 — the membership rules, one named test per rule, each with a mutant recorded in
//! `scratchpad/F5-membership.md`.
//!
//! Three things about how these are written.
//!
//! **Nothing waits.** Time is `Event::Tick` and the network is a `Vec<Action>`, so an election
//! storm across a changing configuration is exact rather than probable, and every failure names a
//! seed.
//!
//! **Every step goes through [`Cluster::step`]**, which records every `RoleChanged` into a
//! per-term table and re-asserts the one property this row exists to protect on *every* action list
//! any test here ever produces: **no term ever has two leaders.** A rule checked in one test is a
//! rule checked on one path, and a membership change is precisely the thing that can produce a
//! second leader of one term without any node behaving incorrectly.
//!
//! **`replicate.rs` (F2) is `unimplemented!()` on this branch**, so `Event::Persisted`,
//! `Event::Propose` and any `Append`/`AppendResp` delivered through `step` panic. Two consequences,
//! both deliberate and both labelled at every site:
//!
//! * Appends are **dropped** by the router rather than delivered. That makes these tests strictly
//!   harsher, not weaker: no node ever hears a heartbeat, so every node believes there is no leader
//!   and campaigns, which is the worst case for the property above.
//! * Where a test needs the effect of an append — a peer's `matched` advancing, a leader's
//!   `commit` advancing, a follower accepting a heartbeat — it writes the field and says which F2
//!   handler it is standing in for. The membership rules themselves are always driven through
//!   `plan_change` / `begin_membership` / `note_config_ack` / `note_config_in_log` /
//!   `apply_committed_config`, never by reaching into `acked` or `cfg`.
//!
//! The election-driving helpers mirror `tests_election.rs`'s because that module's are private to
//! it; they are the only duplication here and they drive the real protocol rather than modelling it.

use super::Change;
use crate::consensus::config::{CfgAt, Config};
use crate::consensus::*;
use crate::error::FerroError;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

// ---------------------------------------------------------------- small helpers

fn n(id: u32) -> NodeId {
    NodeId(id)
}

fn cfg3() -> Config {
    Config::new([n(1), n(2), n(3)], 1, 0)
}

fn msg(from: u32, to: u32, term: Term, body: Body) -> Event {
    Event::Recv(Message { from: n(from), to: n(to), term, body })
}

fn sends(out: &[Action]) -> Vec<Message> {
    out.iter()
        .filter_map(|a| match a {
            Action::Send(m) => Some(m.clone()),
            _ => None,
        })
        .collect()
}

/// The reason inside a refusal, so a test can say *which* rule refused without matching on a whole
/// rendered message.
fn why(e: &FerroError) -> String {
    match e {
        FerroError::Constraint(s) => s.clone(),
        FerroError::Corruption(s) => s.clone(),
        other => format!("{other}"),
    }
}

fn assert_refused(e: &FerroError, naming: &str) {
    assert!(
        why(e).contains(naming),
        "refused for the wrong reason: wanted one naming {naming:?}, got {e}"
    );
}

/// Tick until this node stands for election, and no further.
fn precampaign(c: &mut Consensus) -> Vec<Action> {
    let budget = 4 * c.election_base + 4;
    let mut out = Vec::new();
    for _ in 0..budget {
        if c.role() != Role::Follower {
            break;
        }
        out.extend(c.step(Event::Tick));
    }
    assert_ne!(
        c.role(),
        Role::Follower,
        "node {} never left Follower in {budget} ticks",
        c.id()
    );
    out
}

/// Drive one node to leader of its own configuration through real vote traffic.
///
/// No field is reached into: it times out, collects a pre-vote quorum, then a real vote quorum,
/// exactly as `tests_election.rs` does.
fn win_election(c: &mut Consensus, granters: &[u32]) -> Vec<Action> {
    let me = c.id().0;
    let mut out = precampaign(c);
    // A single-voter configuration is its own majority: `start_precampaign` runs straight through
    // to leader and there is nobody to ask.
    if c.role() == Role::Leader {
        return out;
    }
    assert_eq!(
        c.role(),
        Role::PreCandidate,
        "node {me} never started a pre-campaign (term {})",
        c.term()
    );

    let asked = c.term() + 1;
    for g in granters {
        if c.role() != Role::PreCandidate {
            break;
        }
        out.extend(c.step(msg(*g, me, asked, Body::PreVoteResp { granted: true })));
    }
    assert_eq!(c.role(), Role::Candidate, "a pre-vote quorum did not raise node {me}'s term");

    let term = c.term();
    for g in granters {
        if c.role() != Role::Candidate {
            break;
        }
        out.extend(c.step(msg(*g, me, term, Body::RequestVoteResp { granted: true })));
    }
    assert_eq!(c.role(), Role::Leader, "a vote quorum did not elect node {me}");
    out
}

/// A leader of `cfg`, elected through the protocol, holding the acknowledgements a cluster that has
/// never changed its membership holds: **none**.
///
/// That is not a shortcut. A configuration handed to `Consensus::new` is the operator's assertion
/// about a cluster they are starting, not a change any leader made, so there is no `Membership`
/// entry for anybody to have acknowledged — which is exactly the state
/// `the_first_change_of_a_clusters_life_needs_no_acknowledgement` pins.
fn fresh_leader(id: u32, cfg: Config, seed: u64) -> Consensus {
    let peers: Vec<u32> = cfg.members().iter().map(|m| m.0).filter(|p| *p != id).collect();
    let mut c = Consensus::new(n(id), cfg, seed);
    win_election(&mut c, &peers);
    c
}

/// A leader whose current configuration arrived as a **committed change**, so the precondition has
/// something to be satisfied about.
///
/// Built by making the change through F5's own surface — plan, begin, the caller's report, the
/// acknowledgements, then the apply — rather than by writing `cfg` and `acked`, so that a test
/// resting on it is resting on the code under test.
fn leader_after_one_change(seed: u64) -> (Consensus, Config) {
    let mut l = fresh_leader(1, cfg3(), seed);
    let target = l.plan_change(Change::AddLearner(n(4))).expect("the first change of a cluster's life");
    l.begin_membership(&target).expect("the first change of a cluster's life");
    // The caller has fsynced the entry and reports what its log now holds.
    l.note_config_in_log(target.at());
    // A majority of the creating set fsyncs it too, which is what commits it.
    l.note_config_ack(n(2), target.at());
    l.apply_committed_config(target.clone()).expect("a committed configuration");
    (l, target)
}

// ---------------------------------------------------------------- the cluster harness

/// N `Consensus` instances, a message queue, and the record that makes the two-leader property
/// checkable on every step any test takes.
struct Cluster {
    nodes: Vec<Consensus>,
    /// Every node that has ever announced itself leader, by the term it announced it in.
    leaders: BTreeMap<Term, BTreeSet<NodeId>>,
    /// Appends the router dropped, because F2 cannot receive them yet. Counted rather than ignored:
    /// a router that silently dropped *everything* would make every property here vacuous, and the
    /// count is what tells a reader the vote traffic really flowed.
    dropped_appends: usize,
    delivered_votes: usize,
}

impl Cluster {
    /// `configs[i]` is what node `ids[i]` holds. Different configurations on different nodes is the
    /// point: the two-leader window a membership change opens exists precisely while some nodes
    /// have applied the change and others have not.
    fn new(ids: &[u32], configs: &[Config], seed: u64) -> Self {
        assert_eq!(ids.len(), configs.len());
        let nodes = ids
            .iter()
            .zip(configs)
            .map(|(id, cfg)| {
                // A distinct seed per node, or every node draws the same election timeout and the
                // storm is a lockstep split vote that measures nothing.
                Consensus::new(n(*id), cfg.clone(), seed.wrapping_mul(31).wrapping_add(*id as u64))
            })
            .collect();
        Cluster { nodes, leaders: BTreeMap::new(), dropped_appends: 0, delivered_votes: 0 }
    }

    fn at(&mut self, id: NodeId) -> &mut Consensus {
        self.nodes.iter_mut().find(|c| c.id() == id).expect("no such node")
    }

    fn get(&self, id: NodeId) -> &Consensus {
        self.nodes.iter().find(|c| c.id() == id).expect("no such node")
    }

    /// The only way this file steps a node in a cluster.
    ///
    /// **Records every leadership and re-asserts that no term has two.** That is the property a
    /// membership change threatens, so it is checked on every action list rather than in one test:
    /// two leaders of one term is not a state any single node can detect, and each of them behaves
    /// correctly given the configuration it believes in.
    fn step(&mut self, id: NodeId, ev: Event) -> Vec<Action> {
        let out = self.at(id).step(ev);
        for a in &out {
            if let Action::RoleChanged { role: Role::Leader, term, .. } = a {
                let holders = self.leaders.entry(*term).or_default();
                holders.insert(id);
                assert_eq!(
                    holders.len(),
                    1,
                    "term {term} elected {} leaders ({:?}) — a majority was counted against two \
                     different configurations, and nothing later in this protocol can notice",
                    holders.len(),
                    holders
                );
            }
        }
        out
    }

    /// Deliver every message the queue holds, and everything they produce, until it drains.
    ///
    /// `Append`, `AppendResp` and the snapshot bodies are dropped: `replicate.rs` is a stub and
    /// delivering one panics. Dropping them makes every test here harsher — no node ever hears a
    /// leader, so every node campaigns.
    fn deliver(&mut self, mut q: VecDeque<Message>) {
        let mut budget = 20_000;
        while let Some(m) = q.pop_front() {
            budget -= 1;
            assert!(budget > 0, "the router did not drain: a message loop");
            match &m.body {
                Body::Append { .. }
                | Body::AppendResp { .. }
                | Body::InstallSnapshot { .. }
                | Body::InstallSnapshotResp { .. } => {
                    self.dropped_appends += 1;
                    continue;
                }
                _ => {}
            }
            if !self.nodes.iter().any(|c| c.id() == m.to) {
                continue;
            }
            self.delivered_votes += 1;
            let to = m.to;
            let out = self.step(to, Event::Recv(m));
            q.extend(sends(&out));
        }
    }

    /// Tick every node once, then deliver everything that came back.
    fn tick_all(&mut self) {
        let ids: Vec<NodeId> = self.nodes.iter().map(|c| c.id()).collect();
        let mut q = VecDeque::new();
        for id in ids {
            let out = self.step(id, Event::Tick);
            q.extend(sends(&out));
        }
        self.deliver(q);
    }

    /// Let every node try to win, for as long as it takes several campaigns each.
    ///
    /// The assertion is inside [`Cluster::step`], so this is a driver and not a test: it exists to
    /// make the state machine produce as many campaigns as possible against configurations that
    /// disagree.
    fn election_storm(&mut self, rounds: u32) {
        for _ in 0..rounds {
            self.tick_all();
        }
    }

    /// Stand in for `replicate.rs`: the leader's heartbeat reached every peer and every peer
    /// answered.
    ///
    /// `Progress::silent` is zeroed (F2 does this on an `AppendResp`, and `mod.rs` documents the
    /// field as "ticks since this peer last answered, feeding the leader's own lease"), and each
    /// peer's `since_heard`/`leader` are set (F2 does this on an accepted `Append`). Without it a
    /// leader's lease dies within `lease` ticks and no test could hold a leader long enough to make
    /// two membership changes.
    fn heartbeat_round(&mut self, leader: u32) {
        let l = n(leader);
        let cfg = self.get(l).config().clone();
        let term = self.get(l).term();
        for id in cfg.members().iter().chain(cfg.learners()) {
            if *id == l {
                continue;
            }
            if let Some(p) = self.at(l).progress.get_mut(id) {
                p.silent = 0;
            }
            if self.nodes.iter().any(|c| c.id() == *id) {
                let f = self.at(*id);
                f.since_heard = 0;
                f.leader = Some(l);
                if f.term() < term {
                    // A heartbeat from a later term takes its receiver with it; `mod.rs` owns that
                    // rule and this stands in for the delivery that would have applied it.
                    let mut sink = Vec::new();
                    f.become_follower(term, Some(l), &mut sink);
                }
            }
        }
    }
}

// ================================================================ R1: only a leader

#[test]
fn only_a_leader_may_begin_a_membership_change() {
    // A follower cannot know what is in flight elsewhere, so its answer to "may this change begin"
    // would be a guess. The refusal is `NotLeader`, which is a redirect rather than a wait.
    let mut f = Consensus::new(n(2), cfg3(), 9);
    let err = f.plan_change(Change::AddLearner(n(4))).unwrap_err();
    assert_eq!(err, FerroError::NotLeader { leader: None }, "a follower planned a membership change");
    assert_eq!(
        f.may_change_membership().unwrap_err(),
        FerroError::NotLeader { leader: None }
    );
    let hand_built = f.config().adding_learner(n(4), 0);
    assert_eq!(
        f.begin_membership(&hand_built).unwrap_err(),
        FerroError::NotLeader { leader: None },
        "a follower began a membership change through the proposal gate"
    );

    // A candidate is not a leader either: its campaign may still lose, and a change begun in a
    // term nobody won is a change nobody can finish.
    let mut c = Consensus::new(n(1), cfg3(), 9);
    precampaign(&mut c);
    c.step(msg(2, 1, c.term() + 1, Body::PreVoteResp { granted: true }));
    assert_eq!(c.role(), Role::Candidate, "the harness did not produce a candidate");
    assert!(matches!(
        c.plan_change(Change::AddLearner(n(4))),
        Err(FerroError::NotLeader { .. })
    ));
    assert!(matches!(c.may_change_membership(), Err(FerroError::NotLeader { .. })));

    // The mirror, so a rule that simply refuses everything does not pass: a leader may.
    let l = fresh_leader(1, cfg3(), 9);
    l.plan_change(Change::AddLearner(n(4))).expect("a leader could not begin the first change");
    l.may_change_membership().expect("a leader was told it may not change membership");
}

// ================================================================ R2: nothing in flight

#[test]
fn a_change_is_refused_while_the_previous_one_is_still_in_this_nodes_log() {
    let mut l = fresh_leader(1, cfg3(), 21);
    assert!(!l.change_in_flight(), "a cluster that has changed nothing had a change in flight");

    let first = l.plan_change(Change::AddLearner(n(4))).unwrap();
    l.begin_membership(&first).expect("the first change");
    assert!(
        l.change_in_flight(),
        "a change was begun and the leader does not know it is in flight — the window between the \
         proposal and the caller's report after its fsync is exactly where a second change slips in"
    );

    // The second change is refused, and refused for the right reason.
    let err = l.plan_change(Change::Remove(n(3))).unwrap_err();
    assert_refused(&err, "still in this node's log unapplied");
    // And through the proposal gate too, which is the path a `Command` arriving over a transport
    // takes: a rule enforced only in the planner is a rule a retry walks around.
    let hand_built = Config::new([n(1), n(2)], first.version, l.term());
    assert!(l.begin_membership(&hand_built).is_err(), "an ungated proposal began a second change");

    // The caller's report after its fsync does not clear it — durability is not commitment.
    l.note_config_in_log(first.at());
    assert!(l.change_in_flight(), "an fsync was mistaken for a commit");
    assert!(l.plan_change(Change::Remove(n(3))).is_err());

    // Applying it does, and only then.
    l.note_config_ack(n(2), first.at());
    l.apply_committed_config(first.clone()).expect("a committed configuration");
    assert!(!l.change_in_flight(), "an applied change was still reported in flight");
    l.plan_change(Change::Promote(n(4))).expect("a change was refused after the previous one applied");
}

// ================================================================ R3: a majority of the creating set

#[test]
fn a_change_waits_for_the_previous_one_to_be_durable_on_a_majority_of_the_voters_now_in_force() {
    let (mut l, first) = leader_after_one_change(33);
    // `leader_after_one_change` acknowledged node 2, so the precondition holds here. Take it away
    // by making the next change and applying it with only this node's own acknowledgement, which
    // is what a leader holds the instant a change commits on a bare majority that does not include
    // enough of the *new* set.
    l.note_config_ack(n(3), first.at());
    let second = l.plan_change(Change::Promote(n(4))).expect("promotion of a caught-up learner");
    l.begin_membership(&second).unwrap();
    l.note_config_in_log(second.at());
    l.apply_committed_config(second.clone()).expect("a committed configuration");

    // Voters are now {1,2,3,4}: a quorum is 3 and only this node is known to hold version 3.
    assert_eq!(l.config().quorum(), 3);
    let err = l.plan_change(Change::AddLearner(n(5))).unwrap_err();
    assert_refused(&err, "durably by 1 of the 4 voters");

    // One more is still not a majority of four.
    l.note_config_ack(n(2), second.at());
    assert!(
        l.plan_change(Change::AddLearner(n(5))).is_err(),
        "two of four voters was counted as a majority"
    );

    // Three is.
    l.note_config_ack(n(3), second.at());
    l.plan_change(Change::AddLearner(n(5)))
        .expect("a change was refused although a majority holds the configuration that created it");
}

#[test]
fn an_in_flight_change_is_acknowledged_by_a_majority_of_the_set_that_created_it() {
    // The literal precondition of `DISTRIBUTED.md` §F5, counted in the one state where the creating
    // set exists to be counted: while the change is in flight, `cfg` still IS the set that created
    // it, because a configuration takes effect at commit. Once it applies, nothing retains the old
    // set — which is why `check_precondition` counts the set now in force instead, and why that has
    // to be the stronger of the two rather than an approximation of it.
    let mut l = fresh_leader(1, cfg3(), 37);
    assert_eq!(l.pending_change_is_acknowledged(), None, "a change was in flight before one was made");

    let target = l.plan_change(Change::AddLearner(n(4))).unwrap();
    l.begin_membership(&target).unwrap();
    assert_eq!(
        l.pending_change_is_acknowledged(),
        Some(false),
        "a change was acknowledged by a majority the instant it was begun, before any peer had it"
    );
    // `None` and `Some(false)` are different answers: "nothing to wait for" and "wait".
    assert!(l.change_in_flight());

    // One of three is not a majority. Two is.
    l.note_config_ack(n(2), target.at());
    assert_eq!(l.pending_change_is_acknowledged(), Some(true));

    // A learner cannot make up the number, and neither can a node outside the creating set.
    let mut m = fresh_leader(1, cfg3(), 38);
    let t2 = m.plan_change(Change::AddLearner(n(4))).unwrap();
    m.begin_membership(&t2).unwrap();
    m.note_config_ack(n(4), t2.at());
    assert_eq!(
        m.pending_change_is_acknowledged(),
        Some(false),
        "the node being admitted acknowledged its own admission into a majority of the set that \
         has not yet admitted it"
    );

    // And once it applies, the question is no longer about the creating set at all.
    m.note_config_ack(n(2), t2.at());
    m.apply_committed_config(t2).expect("a committed configuration");
    assert_eq!(m.pending_change_is_acknowledged(), None);
}

#[test]
fn the_first_change_of_a_clusters_life_needs_no_acknowledgement() {
    // A configuration handed to `Consensus::new` is the operator's assertion about a cluster they
    // are starting, not a change a leader made: there is no `Membership` entry for anybody to have
    // acknowledged. Requiring evidence here would refuse the first change for ever, and a cluster
    // that can never be grown is a worse failure than one that grows slowly.
    let l = fresh_leader(1, cfg3(), 41);
    assert!(l.acked.is_empty(), "a bootstrap configuration produced acknowledgements from nowhere");
    l.plan_change(Change::AddLearner(n(4)))
        .expect("the first change of a cluster's life was refused, so the cluster can never grow");

    // And the exemption is exactly once: after that change applies, the next one waits for a
    // majority, which is what stops the exemption from being a hole.
    let (l2, _) = leader_after_one_change(41);
    assert!(!l2.acked.is_empty());
}

// ================================================================ R4: the (version, term) pair

#[test]
fn an_acknowledgement_of_another_terms_configuration_of_the_same_version_does_not_count() {
    // Two *different* configurations can both be version 2: one created by a leader of term 3 that
    // died before committing it, one created by this leader in term 5. A stale acknowledgement of
    // the first counted toward the second is how the precondition silently stops holding — and then
    // two changes are in flight over an unacknowledged one, whose first and third sets are two
    // apart and whose majorities need not intersect.
    let mut l = Consensus::new(n(1), Config::new([n(1), n(2), n(3)], 2, 4), 55);
    l.hard.term = 4;
    win_election(&mut l, &[2, 3]);
    assert_eq!(l.term(), 5, "the campaign did not land in the term this test is about");

    // The caller reports that the newest configuration in this node's log is the one in force.
    let held = l.config().at();
    assert_eq!(held, CfgAt { version: 2, term: 4 });
    l.note_config_in_log(held);
    assert!(l.plan_change(Change::AddLearner(n(4))).is_err(), "one of three voters was a majority");

    // The stale pair: same version, an earlier term. It is a different configuration.
    l.note_config_ack(n(2), CfgAt { version: 2, term: 3 });
    let err = l.plan_change(Change::AddLearner(n(4))).unwrap_err();
    assert_refused(&err, "durably by 1 of the 3 voters");

    // The real one counts.
    l.note_config_ack(n(2), CfgAt { version: 2, term: 4 });
    l.plan_change(Change::AddLearner(n(4)))
        .expect("an acknowledgement of the configuration in force was not counted");
}

// ================================================================ R5/R8: who may be counted

#[test]
fn a_learners_acknowledgement_is_not_counted_toward_a_majority() {
    // A learner is replicated to and never counted. Counting one here would satisfy the
    // precondition on the word of a node that cannot vote, so the next change would begin against
    // a set that has not got the previous one.
    let mut l = Consensus::new(n(1), Config::new([n(1), n(2), n(3)], 2, 0).with_learners([n(4)]), 61);
    win_election(&mut l, &[2, 3]);
    let held = l.config().at();
    l.note_config_in_log(held);

    l.note_config_ack(n(4), held);
    assert!(
        l.config().is_known(n(4)) && !l.config().contains(n(4)),
        "the harness did not set up a learner"
    );
    let err = l.plan_change(Change::Remove(n(3))).unwrap_err();
    assert_refused(&err, "durably by 1 of the 3 voters");

    // A voter's acknowledgement does count, so this is not a rule that refuses everything.
    l.note_config_ack(n(2), held);
    l.plan_change(Change::Remove(n(3))).expect("a voter's acknowledgement was not counted");
}

#[test]
fn a_removed_members_acknowledgement_is_dropped_when_the_change_that_removed_it_applies() {
    // Hygiene rather than safety — the count filters to the current voters, so a stale entry could
    // not be counted anyway. It is here because `acked` is read on every change and a map that
    // accumulates every node a long-lived cluster has ever held is unbounded growth on a hot path.
    let mut l = Consensus::new(n(1), Config::new([n(1), n(2), n(3), n(4), n(5)], 2, 0), 71);
    win_election(&mut l, &[2, 3, 4, 5]);
    let held = l.config().at();
    l.note_config_in_log(held);
    for p in [2, 3, 4, 5] {
        l.note_config_ack(n(p), held);
    }
    let target = l.plan_change(Change::Remove(n(5))).unwrap();
    l.begin_membership(&target).unwrap();
    l.note_config_in_log(target.at());
    // Node 5 is still a member while the change is in flight, so it can and does acknowledge the
    // very change that removes it.
    l.note_config_ack(n(5), target.at());
    assert!(l.acked.contains_key(&n(5)));

    l.apply_committed_config(target).expect("a committed configuration");
    assert!(
        !l.acked.contains_key(&n(5)),
        "a node in no configuration kept its entry in `acked`: {:?}",
        l.acked
    );
    // And a later report about it is not recorded either.
    l.note_config_ack(n(5), CfgAt { version: 99, term: 99 });
    assert!(!l.acked.contains_key(&n(5)), "a node outside the configuration was recorded");
}

// ================================================================ R6: a learner first

#[test]
fn a_node_being_added_joins_as_a_learner_and_never_straight_as_a_voter() {
    let l = fresh_leader(1, cfg3(), 83);

    let target = l.plan_change(Change::AddLearner(n(4))).unwrap();
    assert_eq!(target.members(), cfg3().members(), "an addition moved a voter");
    assert_eq!(target.learners(), [n(4)], "the added node is not a learner");
    assert_eq!(
        target.quorum(),
        cfg3().quorum(),
        "admitting a learner enlarged the quorum: availability falls at the moment an operator \
         believes they are raising it"
    );

    // `Change` cannot express adding a voter, but a `Command::Membership` arriving over a transport
    // carries a whole `Config` and can. The proposal gate refuses it.
    let straight_to_voter = l.config().adding(n(4), l.term());
    assert!(straight_to_voter.contains(n(4)));
    let mut l2 = fresh_leader(1, cfg3(), 83);
    let err = l2.begin_membership(&straight_to_voter).unwrap_err();
    assert_refused(&err, "joins as a learner");

    // Promoting a node that is in no configuration is the same mistake asked a different way.
    assert_refused(&l.plan_change(Change::Promote(n(9))).unwrap_err(), "joins as a learner first");
}

// ================================================================ R7: promotion needs catch-up

#[test]
fn a_learner_is_promoted_only_once_it_holds_the_leaders_committed_round() {
    let (mut l, _) = leader_after_one_change(97);
    assert_eq!(l.config().learners(), [n(4)]);

    // Standing in for `replicate.rs`: the leader has committed through round 7 and the learner
    // holds three of them. `commit` is F2's to advance and `Progress::matched` is F2's to record.
    l.commit = 7;
    l.progress.get_mut(&n(4)).expect("apply_config did not open progress for the learner").matched = 3;

    let err = l.plan_change(Change::Promote(n(4))).unwrap_err();
    assert_refused(&err, "it holds through round 3 and the leader has committed through round 7");
    assert_eq!(l.config().quorum(), 2, "a refused promotion moved the quorum anyway");

    // Caught up, and only then.
    l.progress.get_mut(&n(4)).unwrap().matched = 7;
    let target = l.plan_change(Change::Promote(n(4))).expect("a caught-up learner was refused");
    assert!(target.contains(n(4)), "a promotion did not make the learner a voter");
    assert!(target.learners().is_empty(), "a promoted node stayed a learner as well");
    assert_eq!(target.quorum(), 3, "a fourth voter did not enlarge the quorum");

    // The rule is a comparison against this leader's committed round, not "has some rounds": a
    // cluster whose committed log is empty must still be able to promote, or a new cluster can
    // never grow. (Same shape as F1's `unjoined` watermark rule, and for the same reason.)
    let (fresh, _) = leader_after_one_change(98);
    assert_eq!(fresh.commit, 0);
    assert_eq!(fresh.progress.get(&n(4)).unwrap().matched, 0);
    fresh.plan_change(Change::Promote(n(4))).expect("a learner in an empty-log cluster can never be promoted");
}

#[test]
fn a_promotion_is_refused_for_a_node_with_no_replication_progress_at_all() {
    // Distinct from "behind": no `Progress` entry means the leader has never had a single answer
    // from that node, so it has no evidence at all. Treating absent evidence as satisfied evidence
    // is the same defect as counting `next` instead of `matched`.
    let mut l = Consensus::new(n(1), Config::new([n(1), n(2), n(3)], 2, 0).with_learners([n(4)]), 101);
    win_election(&mut l, &[2, 3]);
    l.commit = 5;
    l.progress.remove(&n(4));
    let err = l.plan_change(Change::Promote(n(4))).unwrap_err();
    assert_refused(&err, "no replication progress at all");
}

// ================================================================ R9: never an empty voter set

#[test]
fn a_change_may_not_leave_a_cluster_with_no_voters() {
    // An empty voter set has no majority, so no leader can ever be elected — including the one
    // that would repair it. It is the one membership outcome that cannot be undone by a later
    // change, which is why it is refused rather than warned about.
    let mut solo = fresh_leader(1, Config::new([n(1)], 1, 0), 113);
    let err = solo.plan_change(Change::Remove(n(1))).unwrap_err();
    assert_refused(&err, "the last voter");

    // And through the proposal gate, which is the path a `Command` takes: `Change` cannot express
    // an empty configuration, a message can.
    let empty = Config::new(Vec::<NodeId>::new(), solo.config().version + 1, solo.term());
    assert!(empty.is_empty());
    let err = solo.begin_membership(&empty).unwrap_err();
    assert_refused(&err, "empty voter set");
    assert_eq!(solo.config().len(), 1, "a refused change moved the configuration anyway");

    // Applying one is damage rather than a decision, so it is `Corruption` and it is refused.
    let err = solo.apply_committed_config(empty).unwrap_err();
    assert!(matches!(err, FerroError::Corruption(_)), "an empty voter set was installed: {err}");
    assert_eq!(solo.config().len(), 1);

    // The mirror: a two-voter cluster may drop to one.
    let pair = fresh_leader(1, Config::new([n(1), n(2)], 1, 0), 113);
    pair.plan_change(Change::Remove(n(2))).expect("a two-voter cluster could not shrink");
}

// ================================================================ the shape of one change

#[test]
fn a_change_moves_exactly_one_node() {
    // Two nodes apart is the whole hazard: `{1,2,3}` and `{1,2,3,4,5}` have majorities of 2 and 3,
    // which need not intersect, so `{1,2}` and `{3,4,5}` are two leaders of one term with every
    // node counting a correct majority of the set it believes in.
    let mut l = fresh_leader(1, cfg3(), 127);
    let term = l.term();

    let two_at_once = Config::new([n(1), n(2), n(4)], l.config().version + 1, term);
    let err = l.begin_membership(&two_at_once).unwrap_err();
    assert_refused(&err, "moves 2 nodes");

    // A change that moves nobody is refused too, and not as a harmless no-op: it burns a version
    // and consumes the precondition that serialises the real ones.
    let moves_nobody = Config::new([n(1), n(2), n(3)], l.config().version + 1, term);
    let err = l.begin_membership(&moves_nobody).unwrap_err();
    assert_refused(&err, "moves 0 nodes");

    // A promotion and an admission in one entry is two changes however it is spelled — which is why
    // the comparison is over each node's *standing* and not over the voter set alone.
    let mut m = Consensus::new(n(1), Config::new([n(1), n(2), n(3)], 2, 0).with_learners([n(4)]), 127);
    win_election(&mut m, &[2, 3]);
    m.commit = 0;
    let promote_and_admit =
        Config::new([n(1), n(2), n(3), n(4)], 3, m.term()).with_learners([n(5)]);
    let err = m.begin_membership(&promote_and_admit).unwrap_err();
    assert_refused(&err, "moves 2 nodes");

    // The mirror: exactly one is accepted.
    let one = l.config().adding_learner(n(4), term);
    l.begin_membership(&one).expect("a one-node change was refused");
}

#[test]
fn a_change_is_refused_unless_it_is_one_version_and_this_term() {
    let mut l = fresh_leader(1, cfg3(), 131);
    let v = l.config().version;
    let term = l.term();

    // A version that skips cannot be told from one built against a set this node has never held.
    let skips = Config::new([n(1), n(2), n(3)], v + 2, term).with_learners([n(4)]);
    assert_refused(&l.begin_membership(&skips).unwrap_err(), "at version");

    // A version that repeats is a change built on the set *before* the one in force.
    let repeats = Config::new([n(1), n(2), n(3)], v, term).with_learners([n(4)]);
    assert_refused(&l.begin_membership(&repeats).unwrap_err(), "at version");

    // An older term is a replay of a change a dead leader began. Counting acknowledgements of it
    // toward this term's change is exactly the ambiguity the (version, term) pair removes.
    let replay = Config::new([n(1), n(2), n(3)], v + 1, term - 1).with_learners([n(4)]);
    assert_refused(&l.begin_membership(&replay).unwrap_err(), "created in term");
}

// ================================================================ the seams

#[test]
fn the_report_of_this_nodes_log_may_move_down_but_never_below_the_applied_configuration() {
    let (mut l, first) = leader_after_one_change(137);
    let held = l.config().at();
    assert_eq!(held, first.at());

    // A change is begun, then the leader loses and regains office and its successor's appends
    // truncated the entry away. Without the report being allowed to move down, this leader would
    // believe a change was in flight for ever and refuse every change until it restarted.
    let next = l.plan_change(Change::Promote(n(4))).unwrap();
    l.begin_membership(&next).unwrap();
    assert!(l.change_in_flight());
    l.note_config_in_log(held);
    assert!(
        !l.change_in_flight(),
        "a truncated `Membership` entry left a leader refusing every change for ever"
    );
    l.plan_change(Change::Promote(n(4))).expect("a leader could not recover from a truncation");

    // It may not go below the applied configuration: a committed entry is never truncated, so such
    // a report cannot be true, and refusing to believe it keeps the guard on the strict side.
    l.note_config_in_log(CfgAt { version: 0, term: 0 });
    assert_eq!(
        l.acked.get(&l.id()).copied(),
        Some(held),
        "a report below the applied configuration was believed"
    );
}

#[test]
fn a_peers_acknowledgement_never_moves_backwards() {
    // Durability does not expire, so an older report is a reordered message rather than news.
    // Letting one move an entry down would make a majority that has been reached un-reach itself,
    // and the change it gated would be refused for ever.
    let (mut l, first) = leader_after_one_change(139);
    l.note_config_ack(n(3), first.at());
    l.note_config_ack(n(3), CfgAt { version: 1, term: 0 });
    assert_eq!(l.acked.get(&n(3)).copied(), Some(first.at()), "a reordered report moved a peer back");
}

#[test]
fn applying_a_committed_configuration_is_idempotent_and_never_goes_backwards() {
    let (mut l, first) = leader_after_one_change(149);
    let held = l.config().at();

    // Idempotent: a caller replaying its committed log on recovery must be able to apply the same
    // configuration twice without a refusal.
    let acts = l.apply_committed_config(first.clone()).expect("re-applying the configuration in force");
    assert!(acts.is_empty(), "re-applying the configuration in force did something: {acts:?}");
    assert_eq!(l.config().at(), held);

    // Backwards is refused: a configuration is replaced wholesale, so installing an older one moves
    // the quorum backwards, which is a majority counted against the wrong number.
    let older = Config::new([n(1), n(2), n(3), n(4), n(5)], held.version - 1, held.term);
    let err = l.apply_committed_config(older).unwrap_err();
    assert_refused(&err, "going backwards moves the quorum backwards");
    assert_eq!(l.config().at(), held, "an older configuration was installed anyway");
}

#[test]
fn applying_a_committed_configuration_clears_behind_and_leaves_unjoined_alone() {
    // F5 must not clear `unjoined` while installing a configuration: knowing the voter set says
    // nothing about holding a single round of the log, and clearing them together is how a node
    // added to a running cluster pre-votes on its very next tick holding nothing. The rule is
    // F1's; this pins that F5's wrapper did not quietly undo it.
    let mut joiner = Consensus::joining(n(4), 151);
    assert!(joiner.behind && joiner.unjoined);
    let cfg = Config::new([n(1), n(2), n(3)], 2, 1).with_learners([n(4)]);
    joiner.apply_committed_config(cfg).expect("a joining node could not be told its configuration");
    assert!(!joiner.behind, "applying a configuration did not clear `behind`");
    assert!(
        joiner.unjoined,
        "applying a configuration cleared `unjoined` too — they are cleared by different evidence"
    );
}

// ================================================================ R10: 3 -> 5

/// The four configurations 3 -> 5 passes through, in order, as one leader would create them.
fn growth_sequence() -> Vec<Config> {
    let c1 = cfg3();
    let c2 = c1.adding_learner(n(4), 1);
    let c3 = c2.adding(n(4), 1);
    let c4 = c3.adding_learner(n(5), 1);
    let c5 = c4.adding(n(5), 1);
    vec![c1, c2, c3, c4, c5]
}

#[test]
fn three_grows_to_five_one_node_at_a_time_and_each_change_waits_for_the_last() {
    let mut cl = Cluster::new(&[1, 2, 3], &[cfg3(), cfg3(), cfg3()], 7);
    // Nodes 4 and 5 are started to JOIN: they hold neither the configuration nor any of the log,
    // which is what `Consensus::joining` is for.
    cl.nodes.push(Consensus::joining(n(4), 4001));
    cl.nodes.push(Consensus::joining(n(5), 5001));

    // A leader, elected through real vote traffic.
    let out = win_election(cl.at(n(1)), &[2, 3]);
    for a in &out {
        if let Action::RoleChanged { role: Role::Leader, term, .. } = a {
            cl.leaders.entry(*term).or_default().insert(n(1));
        }
    }
    assert_eq!(cl.get(n(1)).role(), Role::Leader);

    let expected_quorums = [2usize, 2, 3, 3, 3];
    assert_eq!(cl.get(n(1)).config().quorum(), expected_quorums[0]);

    let changes = [
        Change::AddLearner(n(4)),
        Change::Promote(n(4)),
        Change::AddLearner(n(5)),
        Change::Promote(n(5)),
    ];

    for (i, change) in changes.into_iter().enumerate() {
        cl.heartbeat_round(1);

        // A learner is promoted only once it holds the leader's committed round. Standing in for
        // F2: replication has carried everything committed to the node about to be promoted.
        if let Change::Promote(p) = change {
            let commit = cl.get(n(1)).commit;
            cl.at(n(1)).progress.get_mut(&p).expect("no progress for the learner").matched = commit;
        }

        let target = cl
            .at(n(1))
            .plan_change(change)
            .unwrap_or_else(|e| panic!("change {i} ({change}) was refused: {e}"));
        assert_eq!(
            target,
            growth_sequence()[i + 1],
            "change {i} ({change}) did not produce the configuration 3 -> 5 passes through"
        );

        cl.at(n(1)).begin_membership(&target).unwrap_or_else(|e| panic!("gate refused {change}: {e}"));
        // The caller fsyncs and reports what its log holds.
        let at = target.at();
        cl.at(n(1)).note_config_in_log(at);

        // **The next change must be refused right here**, whatever it is — this is the precondition
        // doing its job in the middle of a real sequence rather than in a unit test of itself.
        assert!(
            cl.at(n(1)).plan_change(Change::AddLearner(n(9))).is_err(),
            "a second change was admitted while {change} was still in flight"
        );

        // A majority of the set that created it fsyncs the entry. Standing in for F2: the caller
        // sees each peer's `matched` cover the entry's round and reports the configuration it
        // therefore holds. Exactly a majority and no more, so that the creating-set count is
        // measured at the boundary rather than swamped.
        let creating = cl.get(n(1)).config().clone();
        assert_eq!(
            cl.get(n(1)).pending_change_is_acknowledged(),
            Some(false),
            "{change} was acknowledged by a majority of its creating set before any peer held it"
        );
        let mut acked = 1; // this node
        for p in creating.members().iter().copied().filter(|p| *p != n(1)) {
            if acked >= creating.quorum() {
                break;
            }
            cl.at(n(1)).note_config_ack(p, at);
            acked += 1;
        }
        assert_eq!(
            cl.get(n(1)).pending_change_is_acknowledged(),
            Some(true),
            "{change} reached a majority of the {} voters that created it and was not counted",
            creating.len()
        );

        // It commits, and every node that holds the entry applies it — including the node being
        // admitted, which is how it learns it is a member at all.
        let holders: Vec<NodeId> = cl.nodes.iter().map(|c| c.id()).collect();
        for h in holders {
            cl.at(h)
                .apply_committed_config(target.clone())
                .unwrap_or_else(|e| panic!("node {h} could not apply {change}: {e}"));
        }
        assert_eq!(
            cl.get(n(1)).config().quorum(),
            expected_quorums[i + 1],
            "after {change} the quorum is wrong, which is a majority counted against the wrong number"
        );
        assert!(!cl.at(n(1)).change_in_flight());

        // **A change that grew the voter set is not finished when it commits.** The majority that
        // committed it was a majority of the smaller set that created it, so the next change waits
        // until a majority of the LARGER set is known to hold it — which is the strengthening the
        // module header argues for, happening here in the middle of a real sequence.
        let now = cl.get(n(1)).config().clone();
        let holders = now
            .members()
            .iter()
            .filter(|m| cl.get(n(1)).acked.get(m).is_some_and(|a| *a >= at))
            .count();
        if !now.has_quorum(holders) {
            assert!(
                cl.at(n(1)).plan_change(Change::AddLearner(n(9))).is_err(),
                "after {change} the voter set grew to {} and only {holders} are known to hold \
                 version {}, yet the next change was admitted",
                now.len(),
                at.version
            );
        }
        // Replication continues, and every node that holds the entry is reported. This is not a
        // convenience: the node just promoted was promoted *because* its `matched` had reached the
        // committed round, so it holds this entry, and a test that never reported it would be
        // measuring a stall that the real system does not have.
        for p in now.members().iter().copied().filter(|p| *p != n(1)) {
            cl.at(n(1)).note_config_ack(p, at);
        }
    }

    let final_cfg = cl.get(n(1)).config().clone();
    assert_eq!(final_cfg.members(), [n(1), n(2), n(3), n(4), n(5)]);
    assert!(final_cfg.learners().is_empty());
    assert_eq!(final_cfg.quorum(), 3);
    assert_eq!(cl.get(n(1)).role(), Role::Leader, "the leader did not survive its own growth");

    // No term ever had two leaders. `Cluster::step` asserts this on every step; this is the same
    // claim stated once at the end so a reader does not have to trust a helper.
    for (term, holders) in &cl.leaders {
        assert_eq!(holders.len(), 1, "term {term} had leaders {holders:?}");
    }
}

#[test]
fn no_term_elects_two_leaders_while_the_configuration_is_changing_underneath() {
    // **The two-leader window is when some nodes have applied a change and others have not**, so
    // that is what this builds: every adjacent pair in the 3 -> 5 sequence, at every split of the
    // cluster between the old configuration and the new one, driven by an election storm in which
    // every node campaigns for real. Appends are dropped, so no node believes a leader exists and
    // every node campaigns on every window — the harshest case for the property.
    let seq = growth_sequence();
    let mut storms = 0;
    let mut elections = 0;
    for seed in [1u64, 2, 3, 5, 8, 13, 21] {
        for pair in seq.windows(2) {
            let (old, new) = (&pair[0], &pair[1]);
            let ids: Vec<u32> = {
                let mut v: Vec<u32> = old
                    .members()
                    .iter()
                    .chain(old.learners())
                    .chain(new.members())
                    .chain(new.learners())
                    .map(|x| x.0)
                    .collect();
                v.sort();
                v.dedup();
                v
            };
            for split in 0..=ids.len() {
                let configs: Vec<Config> = ids
                    .iter()
                    .enumerate()
                    .map(|(i, _)| if i < split { new.clone() } else { old.clone() })
                    .collect();
                let mut cl = Cluster::new(&ids, &configs, seed);
                cl.election_storm(60);
                storms += 1;
                elections += cl.leaders.values().map(|s| s.len()).sum::<usize>();
                for (term, holders) in &cl.leaders {
                    assert_eq!(
                        holders.len(),
                        1,
                        "seed {seed}, split {split}, {} -> {}: term {term} elected {holders:?}",
                        old.version,
                        new.version
                    );
                }
            }
        }
    }
    // A storm that elected nobody proves nothing: the property would hold over an empty record.
    assert!(storms >= 100, "only {storms} storms ran");
    assert!(
        elections >= storms,
        "only {elections} leaderships across {storms} storms — the storm is not electing anybody, \
         so the property holds over an empty record"
    );
}

// ================================================================ R11: a removed node

#[test]
fn a_removed_node_that_keeps_running_cannot_disrupt_the_cluster() {
    // Node 5 is removed and never finds out: nobody replicates to it any more, so it never applies
    // the change that removed it and it keeps its old five-node configuration for ever.
    let five = Config::new([n(1), n(2), n(3), n(4), n(5)], 2, 0);
    let mut cl = Cluster::new(&[1, 2, 3, 4, 5], &vec![five.clone(); 5], 17);

    let out = win_election(cl.at(n(1)), &[2, 3, 4, 5]);
    for a in &out {
        if let Action::RoleChanged { role: Role::Leader, term, .. } = a {
            cl.leaders.entry(*term).or_default().insert(n(1));
        }
    }
    let term_before = cl.get(n(1)).term();

    // Remove node 5, and apply it everywhere EXCEPT on node 5.
    cl.at(n(1)).note_config_in_log(five.at());
    for p in [2, 3, 4, 5] {
        cl.at(n(1)).note_config_ack(n(p), five.at());
    }
    let target = cl.at(n(1)).plan_change(Change::Remove(n(5))).expect("removing a voter");
    cl.at(n(1)).begin_membership(&target).unwrap();
    cl.at(n(1)).note_config_in_log(target.at());
    for p in [2, 3] {
        cl.at(n(1)).note_config_ack(n(p), target.at());
    }
    for h in [1, 2, 3, 4] {
        cl.at(n(h)).apply_committed_config(target.clone()).expect("a committed configuration");
    }
    assert_eq!(cl.get(n(1)).config().members(), [n(1), n(2), n(3), n(4)]);
    assert_eq!(cl.get(n(5)).config().members(), [n(1), n(2), n(3), n(4), n(5)], "node 5 was told");

    // The leader stops sending to it and stops counting it. `progress` is F1's `apply_config`;
    // `acked` is F5's.
    assert!(!cl.get(n(1)).progress.contains_key(&n(5)), "the leader still tracks a removed node");
    assert!(!cl.get(n(1)).acked.contains_key(&n(5)), "the leader still counts a removed node");

    // (i) With the leader alive, node 5 campaigns into a wall. Its pre-votes are refused by every
    //     node that is still being served, so **the cluster's term never rises** — which is the
    //     disruption. Pre-vote is what makes this true; F5's part is that node 5 is no longer
    //     replicated to, so it is the one node whose leader has gone quiet.
    for _ in 0..40 {
        cl.heartbeat_round(1);
        let out = cl.step(n(5), Event::Tick);
        let q: VecDeque<Message> = sends(&out).into();
        cl.deliver(q);
    }
    for id in [1, 2, 3, 4] {
        assert_eq!(
            cl.get(n(id)).term(),
            term_before,
            "a removed node raised node {id}'s term, which deposes a leader that never stopped working"
        );
        assert!(cl.get(n(id)).role() != Role::Candidate);
    }
    assert_eq!(cl.get(n(1)).role(), Role::Leader, "a removed node deposed a healthy leader");
    assert!(cl.get(n(5)).term() <= term_before, "a removed node raised its own term for real");

    // (ii) And with the leader gone, it still cannot win, because it has not been replicated to:
    //      the election restriction refuses a candidate whose log is less complete than the
    //      voter's. Standing in for F2: the cluster committed rounds while node 5 was out.
    for id in [1, 2, 3, 4] {
        let c = cl.at(n(id));
        c.last_term = c.term();
        c.last_round = 12;
        c.leader = None;
        c.since_heard = u32::MAX / 2;
    }
    let stale = cl.get(n(5)).last_round;
    assert!(stale < 12, "the harness did not leave node 5 behind");
    for _ in 0..40 {
        let out = cl.step(n(5), Event::Tick);
        let q: VecDeque<Message> = sends(&out).into();
        cl.deliver(q);
    }
    assert_ne!(
        cl.get(n(5)).role(),
        Role::Leader,
        "a removed node holding {stale} rounds was elected leader of a cluster that has 12"
    );
}

#[test]
fn a_leader_that_removes_itself_stops_leading_when_the_change_commits() {
    // Allowed, and it is the ordinary way an operator retires the node that happens to lead. The
    // step-down is F1's `apply_config`; what F5 owes is that the change is admissible at all and
    // that it is one node.
    let mut l = Consensus::new(n(1), Config::new([n(1), n(2), n(3)], 2, 0), 163);
    win_election(&mut l, &[2, 3]);
    l.note_config_in_log(l.config().at());
    let at = l.config().at();
    l.note_config_ack(n(2), at);
    let target = l.plan_change(Change::Remove(n(1))).expect("a leader could not retire itself");
    l.begin_membership(&target).unwrap();
    l.note_config_in_log(target.at());
    let acts = l.apply_committed_config(target).expect("a committed configuration");
    assert_eq!(l.role(), Role::Follower, "a leader voted out of its own configuration kept leading");
    assert!(
        acts.iter().any(|a| matches!(a, Action::RoleChanged { role: Role::Follower, .. })),
        "the step-down was not announced, so the surrounding server would go on serving writes: {acts:?}"
    );
}
