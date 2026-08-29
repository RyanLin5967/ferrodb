//! F5 — the membership rules, one named test per rule, each with a mutant recorded in
//! `scratchpad/F5-membership.md`.
//!
//! Three things about how these are written.
//!
//! **Nothing waits.** Time is `Event::Tick` and the network is a `Vec<Action>`, so an election storm
//! across a changing configuration is exact rather than probable, and every failure names a seed.
//!
//! **Every step in a cluster goes through [`Cluster::step`]**, which records every `RoleChanged`
//! into a per-term table and re-asserts on *every* action list the property this row exists to
//! protect: **no term ever has two leaders.** A rule checked in one test is a rule checked on one
//! path, and a membership change is precisely the thing that can produce a second leader of one term
//! without any node behaving incorrectly.
//!
//! **`replicate.rs` (F2) is `unimplemented!()` on this branch**, so `Event::Persisted`,
//! `Event::Propose` and any `Append`/`AppendResp` delivered through `step` panic. Two consequences,
//! both deliberate and both labelled at every site:
//!
//! * Appends are **dropped** by the router rather than delivered. That makes these tests harsher,
//!   not weaker: no node ever hears a heartbeat, so every node believes there is no leader and
//!   campaigns, which is the worst case for the property above.
//! * Where a test needs the effect of an append — a peer's `matched` advancing, a leader's `commit`
//!   advancing, a follower accepting a heartbeat — it writes the field and says which F2 handler it
//!   stands in for. The membership rules themselves are always driven through `plan_change` /
//!   `begin_membership` / `note_config_in_log` / `note_config_ack` / `note_bootstrap_config`, never
//!   by reaching into `acked` or `cfg`.
//!
//! The election-driving helpers mirror `tests_election.rs`'s because that module's are private to
//! it; they are the only duplication here, and they drive the real protocol rather than modelling it.

use super::{Change, OwnTermCommitted};
use crate::consensus::config::{CfgAt, Config};
use crate::consensus::*;
use crate::error::FerroError;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Every test that is not *about* rule 3 passes this, so the rule is stated once and the call sites
/// stay about the rule they are testing.
const YES: OwnTermCommitted = OwnTermCommitted::Yes;

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

fn why(e: &FerroError) -> String {
    match e {
        FerroError::Constraint(s) | FerroError::Corruption(s) => s.clone(),
        other => format!("{other}"),
    }
}

/// Assert a refusal names the rule that produced it.
///
/// Which rule refused is behaviour rather than decoration: "wait for a majority" and "this can never
/// be valid" call for opposite actions from an operator, and every refusal here is the same
/// `FerroError` variant, so the reason is the only thing that distinguishes them.
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
    assert_ne!(c.role(), Role::Follower, "node {} never left Follower in {budget} ticks", c.id());
    out
}

/// Drive one node to leader of its own configuration through real vote traffic.
///
/// No field is reached into: it times out, collects a pre-vote quorum, then a real vote quorum,
/// exactly as `tests_election.rs` does.
fn win_election(c: &mut Consensus, granters: &[u32]) -> Vec<Action> {
    let me = c.id().0;
    let mut out = precampaign(c);
    // A single-voter configuration is its own majority: `start_precampaign` runs straight through to
    // leader and there is nobody to ask.
    if c.role() == Role::Leader {
        return out;
    }
    assert_eq!(c.role(), Role::PreCandidate, "node {me} never pre-campaigned (term {})", c.term());

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

/// A leader of a cluster an operator has just started: elected through the protocol, with the
/// bootstrap configuration's provenance recorded.
///
/// `note_bootstrap_config` is not a convenience. Without it the configuration in force has no
/// recorded provenance and **every** change is refused — see
/// `a_configuration_with_no_recorded_provenance_refuses_rather_than_permits`.
fn fresh_leader(id: u32, cfg: Config, seed: u64) -> Consensus {
    let peers: Vec<u32> = cfg.members().iter().map(|m| m.0).filter(|p| *p != id).collect();
    let mut c = Consensus::new(n(id), cfg, seed);
    c.note_bootstrap_config();
    win_election(&mut c, &peers);
    c
}

/// A node whose configuration came from its log rather than from an operator, elected leader.
fn leader_from_log(id: u32, cfg: Config, term: Term, seed: u64) -> Consensus {
    let peers: Vec<u32> = cfg.members().iter().map(|m| m.0).filter(|p| *p != id).collect();
    let mut c = Consensus::new(n(id), cfg.clone(), seed);
    let mut out = Vec::new();
    c.note_config_in_log(cfg, &mut out).expect("what my log holds");
    c.hard.term = term;
    win_election(&mut c, &peers);
    c
}

/// A leader whose configuration in force arrived as a **change**, made through F5's own surface —
/// plan, begin, the caller's durability report, a peer's acknowledgement — rather than by writing
/// `cfg` and `acked`, so a test resting on it rests on the code under test.
fn leader_after_one_change(seed: u64) -> (Consensus, Config) {
    let mut l = fresh_leader(1, cfg3(), seed);
    let mut out = Vec::new();
    let target = l.plan_change(Change::AddLearner(n(4)), YES).expect("the first change");
    l.begin_membership(&target, YES, &mut out).expect("the first change");
    // The caller has fsynced the entry and reports what its log now durably holds.
    l.note_config_in_log(target.clone(), &mut out).expect("a configuration from the log");
    // A peer fsyncs it too, which is what commits it.
    l.note_config_ack(n(2), target.at());
    (l, target)
}

/// The configuration `cfg` becomes with `id` demoted, built the way `membership.rs` builds it.
fn demoted_config(cfg: &Config, id: NodeId, term: u64) -> Config {
    let members = cfg.members().iter().copied().filter(|x| *x != id);
    let mut learners: Vec<NodeId> = cfg.learners().to_vec();
    learners.push(id);
    Config::new(members, cfg.version + 1, term).with_learners(learners)
}

// ---------------------------------------------------------------- the cluster harness

/// N `Consensus` instances, a message queue, and the record that makes the two-leader property
/// checkable on every step any test takes.
struct Cluster {
    nodes: Vec<Consensus>,
    /// Every node that has ever announced itself leader, by the term it announced it in.
    leaders: BTreeMap<Term, BTreeSet<NodeId>>,
    /// Appends the router dropped, because F2 cannot receive them yet, and vote messages it
    /// delivered. Counted rather than ignored: a router that silently dropped *everything* would
    /// make every property here vacuous, and these are what tell a reader the traffic really flowed.
    dropped_appends: usize,
    delivered_votes: usize,
}

impl Cluster {
    /// `configs[i]` is what node `ids[i]` holds. Different configurations on different nodes is the
    /// point: the two-leader window a membership change opens exists precisely while some nodes hold
    /// the change and others do not.
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

    /// Record every leadership an action list announces, and re-assert that no term has two.
    ///
    /// Two leaders of one term is not a state any single node can detect — each behaves correctly
    /// given the configuration it believes in — so it is checked on every action list rather than in
    /// one test.
    fn record(&mut self, id: NodeId, out: &[Action]) {
        for a in out {
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
    }

    /// The only way this file steps a node in a cluster.
    fn step(&mut self, id: NodeId, ev: Event) -> Vec<Action> {
        let out = self.at(id).step(ev);
        self.record(id, &out);
        out
    }

    /// Deliver every message the queue holds, and everything they produce, until it drains.
    ///
    /// `Append`, `AppendResp` and the snapshot bodies are dropped.
    ///
    /// **The original reason — "`replicate.rs` is a stub and delivering one panics" — is no longer
    /// true**, and was left standing after F2 landed. The behaviour is kept anyway, and now for a
    /// reason rather than an obstacle: this harness exists to drive membership storms, and
    /// delivering replication would make every run also a replication test, with the election
    /// results a function of log convergence rather than of the configuration rules under test.
    /// `dropped_appends` counts what is skipped and is asserted on as an anti-vacuity witness, so
    /// the omission is measured rather than assumed.
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

    /// Let every node try to win, for as long as several campaigns each. The assertion lives in
    /// [`Cluster::record`], so this is a driver: it exists to make the state machine produce as many
    /// campaigns as possible against configurations that disagree.
    fn election_storm(&mut self, rounds: u32) {
        for _ in 0..rounds {
            self.tick_all();
        }
    }

    /// Stand in for `replicate.rs`: the leader's heartbeat reached every peer and every peer
    /// answered.
    ///
    /// `Progress::silent` is zeroed (F2 does this on an `AppendResp`) and each peer's
    /// `since_heard`/`leader` are set (F2 does this on an accepted `Append`). Without it a leader's
    /// lease dies within `lease` ticks and no test could hold a leader long enough to make two
    /// membership changes.
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

// ================================================================ rule 1: the log, not the apply

#[test]
fn the_configuration_a_node_counts_against_is_the_newest_in_its_log() {
    // **The defect this pins is a split brain that no per-term check can see.** Tie the denominator
    // to a node's *applied* configuration and nothing bounds how far it can lag: a node whose log is
    // complete through several changes but whose applier is behind counts a majority of the
    // three-node set the cluster left — two votes — and wins, while the real five-node cluster still
    // has a leader that hears 3 of 5 and keeps its lease. Two leaders, disjoint quorums, in
    // DIFFERENT terms, so no per-term uniqueness check anywhere can catch it.
    //
    // Tied to the log, the denominator is never staler than what this node's log proves, and the
    // election restriction does the rest.
    let five = Config::new([n(1), n(2), n(3), n(4), n(5)], 2, 0);
    let mut c = Consensus::new(n(1), cfg3(), 71);
    c.note_bootstrap_config();

    let mut out = Vec::new();
    c.note_config_in_log(five.clone(), &mut out).expect("a configuration from the log");
    assert_eq!(c.config(), &five, "the newest configuration in the log was not installed");

    // The observable: the campaign asks the FIVE-node set and needs three of it.
    let asks = precampaign(&mut c);
    let asked_peers: Vec<NodeId> = sends(&asks)
        .iter()
        .filter(|m| matches!(m.body, Body::PreVote { .. }))
        .map(|m| m.to)
        .collect();
    assert_eq!(
        asked_peers,
        vec![n(2), n(3), n(4), n(5)],
        "the campaign asked {asked_peers:?}: the denominator did not follow the log"
    );

    // And it needs a majority of five: this node plus TWO grants, where the three-node set it
    // would have counted against needs this node plus one.
    let asked = c.term() + 1;
    c.step(msg(2, 1, asked, Body::PreVoteResp { granted: true }));
    assert_eq!(
        c.role(),
        Role::PreCandidate,
        "one grant carried a campaign — the denominator is still the three-node cluster's, so this \
         node would win an election the five-node cluster did not hold"
    );
    c.step(msg(3, 1, asked, Body::PreVoteResp { granted: true }));
    assert_eq!(c.role(), Role::Candidate, "a majority of five did not carry the campaign");
}

#[test]
fn a_configuration_report_may_move_down_after_a_truncation_and_a_repeat_is_idempotent() {
    // A leader appends a change, loses office, and its successor's appends truncate the entry away.
    // The report must be allowed to move down, or this node counts against a configuration that is
    // in nobody's log and refuses every change for ever.
    let mut l = fresh_leader(1, cfg3(), 137);
    let mut out = Vec::new();
    let target = l.plan_change(Change::AddLearner(n(4)), YES).unwrap();
    l.begin_membership(&target, YES, &mut out).unwrap();
    assert_eq!(l.config(), &target);

    l.note_config_in_log(cfg3(), &mut out).expect("the newest surviving configuration");
    assert_eq!(l.config(), &cfg3(), "a truncated configuration was not given up");

    // A repeat of the configuration in force is what a recovery replay does: it records durability
    // and changes nothing else.
    l.note_config_in_log(cfg3(), &mut out).expect("a replayed configuration");
    assert_eq!(l.config(), &cfg3());
    assert_eq!(l.acked.get(&l.id()).copied(), Some(cfg3().at()));
}

#[test]
fn a_configuration_that_is_damage_latches_this_node_out_of_office() {
    // Refusing a configuration with an error alone is a caller obligation with nothing enforcing it,
    // and a node that goes on counting majorities against a configuration the cluster has left is
    // the one failure `mod.rs` says nothing later in the protocol can detect. So damage steps the
    // node down and sets `behind`: it can neither lead nor campaign until it installs a good one.
    for (label, damaged) in [
        ("an empty voter set", Config::new(Vec::<NodeId>::new(), 2, 0)),
        // Same (version, term) as the configuration in force, different members. Acknowledgements
        // are matched on that pair, so a collision makes a majority of one set count as a majority
        // of the other.
        ("a colliding identity", Config::new([n(7), n(8), n(9)], 1, 0)),
    ] {
        let mut l = fresh_leader(1, cfg3(), 149);
        let mut out = Vec::new();
        let err = l.note_config_in_log(damaged, &mut out).expect_err(label);
        assert!(matches!(err, FerroError::Corruption(_)), "{label} was not reported as damage: {err}");
        assert_eq!(l.role(), Role::Follower, "{label} left this node in office");
        assert!(l.behind, "{label} left this node believing its configuration is the cluster's");
        assert!(!l.may_campaign(), "{label} left this node able to campaign");
        assert_eq!(l.config(), &cfg3(), "{label} was installed anyway");
        assert!(
            out.iter().any(|a| matches!(a, Action::RoleChanged { role: Role::Follower, .. })),
            "the step-down was not announced, so the surrounding server would go on serving writes"
        );
    }
}

// ================================================================ provenance

#[test]
fn a_configuration_with_no_recorded_provenance_refuses_rather_than_permits() {
    // `acked` is not durable state and there is no recovery constructor, so an empty map is the
    // state of every node on every restart — it cannot be read as "no change has ever been made".
    // Permitting here would let a leader that crashed mid-change begin a second one with no
    // evidence at all about the first. A guard that cannot see its input must ask.
    let mut bare = Consensus::new(n(1), cfg3(), 11);
    win_election(&mut bare, &[2, 3]);
    let err = bare.plan_change(Change::AddLearner(n(4)), YES).unwrap_err();
    assert_refused(&err, "no recorded provenance");
    assert!(bare.change_in_flight(), "a configuration with no provenance was reported settled");

    // Either report gives it provenance. The operator's assertion, which starts a cluster:
    let boot = fresh_leader(1, cfg3(), 11);
    boot.plan_change(Change::AddLearner(n(4)), YES).expect("a bootstrap cluster cannot grow");

    // Or the log's, which is how a restarted node gets it — and then it waits for evidence like any
    // other configuration, rather than being exempt.
    let restored = leader_from_log(1, Config::new([n(1), n(2), n(3)], 4, 2), 2, 11);
    let err = restored.plan_change(Change::AddLearner(n(4)), YES).unwrap_err();
    assert_refused(&err, "held durably by 1 of its 3 voters");
}

// ================================================================ rule 3: the erratum

#[test]
fn a_membership_change_is_refused_until_this_leader_has_committed_an_entry_of_its_own_term() {
    // Without this, a leader elected WITHOUT an earlier leader's uncommitted configuration entry
    // counts against the configuration before it and proposes a different change from there. The two
    // configurations one step either side of a common parent can have disjoint majorities:
    // {1,2,3,4}+{5} and {1,2,3,4}+{6} have 3-of-5 majorities {1,2,5} and {3,4,6}, which share
    // nothing — two leaders of one term, each counting a correct majority of the set it believes in.
    let l = fresh_leader(1, cfg3(), 23);
    let err = l.plan_change(Change::AddLearner(n(4)), OwnTermCommitted::NotYet).unwrap_err();
    assert_refused(&err, "has not committed an entry of its own term");
    assert_eq!(
        l.may_change_membership(OwnTermCommitted::NotYet).unwrap_err(),
        err,
        "the two entry points disagree about the same rule"
    );

    // `NotYet` is also the answer a caller that cannot tell must give, so the refusal must not
    // depend on anything else being wrong: the same call with `Yes` is admitted.
    l.plan_change(Change::AddLearner(n(4)), YES).expect("an established term was refused");

    // And it is checked on the proposal path too, not only in the planner.
    let mut m = fresh_leader(1, cfg3(), 23);
    let target = m.plan_change(Change::AddLearner(n(4)), YES).unwrap();
    let mut out = Vec::new();
    let err = m.begin_membership(&target, OwnTermCommitted::NotYet, &mut out).unwrap_err();
    assert_refused(&err, "has not committed an entry of its own term");
    assert_eq!(m.config(), &cfg3(), "a refused change was installed anyway");
}

// ================================================================ only a leader

#[test]
fn only_a_leader_may_begin_a_membership_change() {
    // A follower cannot know what is in flight elsewhere, so its answer would be a guess. The
    // refusal is `NotLeader`, which is a redirect rather than a wait.
    let mut f = Consensus::new(n(2), cfg3(), 9);
    f.note_bootstrap_config();
    let err = f.plan_change(Change::AddLearner(n(4)), YES).unwrap_err();
    assert_eq!(err, FerroError::NotLeader { leader: None }, "a follower planned a membership change");
    assert_eq!(f.may_change_membership(YES).unwrap_err(), FerroError::NotLeader { leader: None });
    let hand_built = f.config().adding_learner(n(4), 0);
    let mut out = Vec::new();
    assert_eq!(
        f.begin_membership(&hand_built, YES, &mut out).unwrap_err(),
        FerroError::NotLeader { leader: None },
        "a follower began a membership change through the proposal gate"
    );

    // A candidate is not a leader either: its campaign may still lose, and a change begun in a term
    // nobody won is a change nobody can finish.
    let mut c = Consensus::new(n(1), cfg3(), 9);
    c.note_bootstrap_config();
    precampaign(&mut c);
    c.step(msg(2, 1, c.term() + 1, Body::PreVoteResp { granted: true }));
    assert_eq!(c.role(), Role::Candidate, "the harness did not produce a candidate");
    assert!(matches!(c.plan_change(Change::AddLearner(n(4)), YES), Err(FerroError::NotLeader { .. })));

    // The mirror, so a rule that refuses everything does not pass this test.
    let l = fresh_leader(1, cfg3(), 9);
    l.plan_change(Change::AddLearner(n(4)), YES).expect("a leader could not begin the first change");
    l.may_change_membership(YES).expect("a leader was told it may not change membership");
}

// ================================================================ rule 2: a majority holds it

#[test]
fn a_change_is_refused_until_the_configuration_in_force_is_durable_on_a_majority() {
    let mut l = fresh_leader(1, cfg3(), 21);
    assert!(!l.change_in_flight(), "a bootstrap cluster reported a change in flight");

    let mut out = Vec::new();
    let first = l.plan_change(Change::AddLearner(n(4)), YES).unwrap();
    l.begin_membership(&first, YES, &mut out).expect("the first change");
    assert_eq!(l.config(), &first, "the leader did not install the configuration it appended");
    assert!(
        l.change_in_flight(),
        "a change was begun and the leader does not know it is unfinished — the window between the \
         proposal and the caller's report after its fsync is exactly where a second change slips in"
    );

    // The leader does not count ITSELF until its own fsync is reported. An append is not durability,
    // and the rule that stops a follower acking an unfsynced round applies to this node too.
    assert_refused(
        &l.plan_change(Change::Promote(n(4)), YES).unwrap_err(),
        "held durably by 0 of its 3 voters",
    );

    l.note_config_in_log(first.clone(), &mut out).expect("the caller's report after its fsync");
    assert_refused(
        &l.plan_change(Change::Promote(n(4)), YES).unwrap_err(),
        "held durably by 1 of its 3 voters",
    );
    assert!(l.change_in_flight());

    // A majority, and only then.
    l.note_config_ack(n(2), first.at());
    assert!(!l.change_in_flight(), "a majority holding the configuration was not counted");
    l.plan_change(Change::Promote(n(4)), YES).expect("a change was refused after a majority held it");
}

#[test]
fn a_change_that_grows_the_voter_set_waits_for_a_majority_of_the_larger_set() {
    // `DISTRIBUTED.md` §F5 says "a majority of the set that created it". The set now in force is
    // counted instead, because no field retains the creating set — and it is the stronger of the
    // two, never the weaker: a majority of the new set intersects every majority of the creating
    // set. This is where the difference shows, and it is also how Raft counts a configuration
    // entry's commit.
    let (mut l, first) = leader_after_one_change(33);
    let mut out = Vec::new();
    let second = l.plan_change(Change::Promote(n(4)), YES).expect("a caught-up learner");
    l.begin_membership(&second, YES, &mut out).unwrap();
    l.note_config_in_log(second.clone(), &mut out).unwrap();
    assert!(first.at() < second.at(), "versions do not move forward");
    assert_eq!(l.config().quorum(), 3, "a fourth voter did not enlarge the quorum");

    // Two of the three voters that created it is a majority of THAT set — and not of the four in
    // force now, so the next change waits.
    l.note_config_ack(n(2), second.at());
    assert_refused(
        &l.plan_change(Change::AddLearner(n(5)), YES).unwrap_err(),
        "held durably by 2 of its 4 voters",
    );
    // The node just promoted holds it by construction: it was promoted *because* its `matched` had
    // reached the committed round, so this is a wait of one round-trip and not a stall.
    l.note_config_ack(n(4), second.at());
    l.plan_change(Change::AddLearner(n(5)), YES).expect("three of four voters is a majority");
}

#[test]
fn an_acknowledgement_of_another_terms_configuration_of_the_same_version_does_not_count() {
    // Two *different* configurations can both be version 2: one created by a leader of term 3 that
    // died before committing it, one created by a leader of term 4. A stale acknowledgement of the
    // first counted toward the second is how the precondition silently stops holding.
    let held = Config::new([n(1), n(2), n(3)], 2, 4);
    let mut l = leader_from_log(1, held.clone(), 4, 55);
    assert_eq!(l.term(), 5);
    assert_eq!(l.config().at(), CfgAt { version: 2, term: 4 });

    // The stale pair: same version, an earlier term. It is a different configuration.
    l.note_config_ack(n(2), CfgAt { version: 2, term: 3 });
    assert_refused(
        &l.plan_change(Change::AddLearner(n(4)), YES).unwrap_err(),
        "held durably by 1 of its 3 voters",
    );

    // The real one counts.
    l.note_config_ack(n(2), CfgAt { version: 2, term: 4 });
    l.plan_change(Change::AddLearner(n(4)), YES)
        .expect("an acknowledgement of the configuration in force was not counted");
}

#[test]
fn a_learners_acknowledgement_is_not_counted_toward_a_majority() {
    // A learner is replicated to and never counted. Counting one here would satisfy the precondition
    // on the word of a node that cannot vote, so the next change would begin against a set that has
    // not got the previous one.
    let held = Config::new([n(1), n(2), n(3)], 2, 0).with_learners([n(4)]);
    let mut l = leader_from_log(1, held.clone(), 0, 61);
    assert!(l.config().is_known(n(4)) && !l.config().contains(n(4)), "the harness set up no learner");

    l.note_config_ack(n(4), held.at());
    assert_refused(
        &l.plan_change(Change::Demote(n(3)), YES).unwrap_err(),
        "held durably by 1 of its 3 voters",
    );

    // A voter's acknowledgement does count, so this is not a rule that refuses everything.
    l.note_config_ack(n(2), held.at());
    l.plan_change(Change::Demote(n(3)), YES).expect("a voter's acknowledgement was not counted");
}

#[test]
fn a_departed_members_acknowledgement_is_dropped_when_the_change_that_dropped_it_is_installed() {
    // Hygiene rather than safety — the count filters to the current voters, so a stale entry could
    // not be counted anyway. It is here because `acked` is read on every change and a map that
    // accumulates every node a long-lived cluster has ever held is unbounded growth on a hot path.
    let held = Config::new([n(1), n(2), n(3), n(4)], 2, 0).with_learners([n(5)]);
    let mut l = leader_from_log(1, held.clone(), 0, 71);
    for p in [2, 3, 4, 5] {
        l.note_config_ack(n(p), held.at());
    }
    assert!(l.acked.contains_key(&n(5)), "a learner's acknowledgement was not recorded at all");

    let mut out = Vec::new();
    let target = l.plan_change(Change::Remove(n(5)), YES).expect("removing a learner");
    l.begin_membership(&target, YES, &mut out).unwrap();
    assert!(
        !l.acked.contains_key(&n(5)),
        "a node in no configuration kept its entry in `acked`: {:?}",
        l.acked
    );
    // And a later report about it is not recorded either.
    l.note_config_ack(n(5), CfgAt { version: 99, term: 99 });
    assert!(!l.acked.contains_key(&n(5)), "a node outside the configuration was recorded");
}

#[test]
fn a_peers_acknowledgement_never_moves_backwards_and_a_peer_cannot_state_this_nodes() {
    // An older report is a reordered message, not news. Letting one move an entry down would make a
    // majority that has been reached un-reach itself, and the change it gated would be refused for
    // ever.
    let (mut l, first) = leader_after_one_change(139);
    l.note_config_ack(n(3), first.at());
    l.note_config_ack(n(3), CfgAt { version: 1, term: 0 });
    assert_eq!(l.acked.get(&n(3)).copied(), Some(first.at()), "a reordered report moved a peer back");

    // This node's own entry is a fact about its own log, and only `note_config_in_log` may state it.
    let mine = l.acked.get(&l.id()).copied();
    l.note_config_ack(l.id(), CfgAt { version: 99, term: 99 });
    assert_eq!(l.acked.get(&l.id()).copied(), mine, "a peer report rewrote this node's own record");
}

// ================================================================ a learner first

#[test]
fn a_node_being_added_joins_as_a_learner_and_never_straight_as_a_voter() {
    let l = fresh_leader(1, cfg3(), 83);

    let target = l.plan_change(Change::AddLearner(n(4)), YES).unwrap();
    assert_eq!(target.members(), cfg3().members(), "an addition moved a voter");
    assert_eq!(target.learners(), [n(4)], "the added node is not a learner");
    assert_eq!(
        target.quorum(),
        cfg3().quorum(),
        "admitting a learner enlarged the quorum: availability falls at the moment an operator \
         believes they are raising it"
    );

    // `Change` cannot express adding a voter, but a `Command::Membership` carries a whole `Config`
    // and can. The proposal gate refuses it.
    let mut l2 = fresh_leader(1, cfg3(), 83);
    let mut out = Vec::new();
    let straight_to_voter = l2.config().adding(n(4), l2.term());
    assert!(straight_to_voter.contains(n(4)));
    assert_refused(
        &l2.begin_membership(&straight_to_voter, YES, &mut out).unwrap_err(),
        "joins as a learner",
    );
    assert_eq!(l2.config(), &cfg3(), "a refused change was installed anyway");

    // Promoting a node that is in no configuration is the same mistake asked a different way.
    assert_refused(&l.plan_change(Change::Promote(n(9)), YES).unwrap_err(), "joins as a learner first");
}

#[test]
fn a_learner_is_promoted_only_once_it_holds_the_leaders_committed_round() {
    let (mut l, _) = leader_after_one_change(97);
    assert_eq!(l.config().learners(), [n(4)]);

    // Standing in for `replicate.rs`: the leader has committed through round 7 and the learner holds
    // three of them. `commit` is F2's to advance and `Progress::matched` is F2's to record.
    l.commit = 7;
    l.progress.get_mut(&n(4)).expect("apply_config opened no progress for the learner").matched = 3;

    assert_refused(
        &l.plan_change(Change::Promote(n(4)), YES).unwrap_err(),
        "it holds through round 3 and the leader has committed through round 7",
    );
    assert_eq!(l.config().quorum(), 2, "a refused promotion moved the quorum anyway");

    // Caught up, and only then.
    l.progress.get_mut(&n(4)).unwrap().matched = 7;
    let target = l.plan_change(Change::Promote(n(4)), YES).expect("a caught-up learner was refused");
    assert!(target.contains(n(4)), "a promotion did not make the learner a voter");
    assert!(target.learners().is_empty(), "a promoted node stayed a learner as well");
    assert_eq!(target.quorum(), 3, "a fourth voter did not enlarge the quorum");

    // The comparison is against the leader's committed round, not "has some rounds": a cluster that
    // has committed nothing must still be able to grow, or a new cluster never can. Same shape as
    // F1's `unjoined` watermark clearing at zero, and for the same reason.
    let (fresh, _) = leader_after_one_change(98);
    assert_eq!(fresh.commit, 0);
    assert_eq!(fresh.progress.get(&n(4)).unwrap().matched, 0);
    fresh.plan_change(Change::Promote(n(4)), YES).expect("a new cluster can never promote anybody");
}

#[test]
fn a_promotion_is_refused_for_a_node_with_no_replication_progress_at_all() {
    // Absent evidence is not satisfied evidence — the same defect as counting `next` instead of
    // `matched`. Unreachable through the public surface (a leader has `progress` for every member of
    // its configuration), so this pins the DIRECTION of the arm rather than a reachable state: if it
    // ever becomes reachable, it must refuse.
    let held = Config::new([n(1), n(2), n(3)], 2, 0).with_learners([n(4)]);
    let mut l = leader_from_log(1, held.clone(), 0, 101);
    l.note_config_ack(n(2), held.at());
    l.commit = 5;
    l.progress.remove(&n(4));
    assert_refused(
        &l.plan_change(Change::Promote(n(4)), YES).unwrap_err(),
        "no replication progress at all",
    );
}

// ================================================================ a demotion before a removal

#[test]
fn a_voter_is_demoted_before_it_can_be_removed() {
    // A voter dropped in one step is never told: the leader drops it from `progress` in the same
    // step, so no further `Append` can reach it, and it keeps a configuration containing itself and
    // campaigns at a cluster it has left for ever. Demoted first, it receives the configuration that
    // stops it voting.
    let mut l = fresh_leader(1, cfg3(), 151);
    assert_refused(&l.plan_change(Change::Remove(n(3)), YES).unwrap_err(), "demote it to learner first");

    // And on the proposal path, where a hand-built `Config` can express it.
    let mut out = Vec::new();
    let dropped_outright = l.config().removing(n(3), l.term());
    assert_refused(
        &l.begin_membership(&dropped_outright, YES, &mut out).unwrap_err(),
        "refused to remove voter n3 in one step",
    );

    // The demotion is a one-node change, and it shrinks the quorum because a learner is not counted.
    let demoted = l.plan_change(Change::Demote(n(3)), YES).expect("a voter could not be demoted");
    assert_eq!(demoted.members(), [n(1), n(2)]);
    assert_eq!(demoted.learners(), [n(3)]);
    assert_eq!(demoted.quorum(), 2);

    l.begin_membership(&demoted, YES, &mut out).unwrap();
    l.note_config_in_log(demoted.clone(), &mut out).unwrap();
    l.note_config_ack(n(2), demoted.at());
    // Only now may it go.
    let gone = l.plan_change(Change::Remove(n(3)), YES).expect("a learner could not be removed");
    assert_eq!(gone.members(), [n(1), n(2)]);
    assert!(gone.learners().is_empty());
}

#[test]
fn a_demoted_node_can_never_campaign_again() {
    // This is what the demotion buys, and it is the only mechanism in this row that makes a
    // departing node harmless by TELLING it rather than by walling it off: a node that is not a voter
    // in its own configuration fails `may_campaign` for ever.
    let three = Config::new([n(1), n(2), n(3)], 1, 0);
    let mut departing = Consensus::new(n(3), three.clone(), 163);
    departing.note_bootstrap_config();
    assert!(departing.may_campaign(), "the harness produced a node that could not campaign anyway");

    let mut out = Vec::new();
    departing
        .note_config_in_log(demoted_config(&three, n(3), 1), &mut out)
        .expect("the configuration that demotes it");
    assert!(
        !departing.may_campaign(),
        "a demoted node can still campaign, so telling it it is no longer a voter changes nothing"
    );
    // And no number of ticks brings it back: time is not evidence about a configuration.
    for _ in 0..200 {
        departing.step(Event::Tick);
    }
    assert_eq!(departing.role(), Role::Follower, "a demoted node campaigned on a timer");
}

#[test]
fn a_leader_may_not_demote_itself() {
    // `election.rs` steps a leader down the moment it is not a voter in its own configuration, and
    // rule 1 installs a configuration when it is APPENDED — so a leader that demoted itself would
    // lose office before the change could commit, and the change would silently not happen.
    let l = fresh_leader(1, cfg3(), 167);
    assert_refused(&l.plan_change(Change::Demote(n(1)), YES).unwrap_err(), "which is this leader");
    // Promoting itself is refused for the ordinary reason: it is already a voter.
    assert_refused(&l.plan_change(Change::Promote(n(1)), YES).unwrap_err(), "already a voter");
}

// ================================================================ never an empty voter set

#[test]
fn a_change_may_not_leave_a_cluster_with_no_voters() {
    // An empty voter set has no majority, so no leader can ever be elected — including the one that
    // would repair it. It is the one membership outcome a later change cannot undo.
    //
    // A one-voter cluster's only voter is its leader, and a leader may not demote itself, so that
    // refusal is what holds the line on the planning path; `check_shape` holds it on the path a
    // hand-built `Config` takes.
    let mut solo = fresh_leader(1, Config::new([n(1)], 1, 0), 113);
    assert_refused(&solo.plan_change(Change::Demote(n(1)), YES).unwrap_err(), "which is this leader");

    let empty = Config::new(Vec::<NodeId>::new(), solo.config().version + 1, solo.term());
    assert!(empty.is_empty());
    let mut out = Vec::new();
    assert_refused(&solo.begin_membership(&empty, YES, &mut out).unwrap_err(), "empty voter set");
    assert_eq!(solo.config().len(), 1, "a refused change moved the configuration anyway");

    // The mirror: a two-voter cluster may shrink to one, which is a majority of one.
    let mut pair = fresh_leader(1, Config::new([n(1), n(2)], 1, 0), 113);
    let demoted = pair.plan_change(Change::Demote(n(2)), YES).expect("a two-voter cluster could not shrink");
    assert_eq!(demoted.members(), [n(1)]);
    pair.begin_membership(&demoted, YES, &mut out).unwrap();
    assert_eq!(pair.config().quorum(), 1);
}

// ================================================================ the shape of one change

#[test]
fn a_change_moves_exactly_one_node() {
    // Two nodes apart is the whole hazard: `{1,2,3}` and `{1,2,3,4,5}` have majorities of 2 and 3,
    // which need not intersect, so `{1,2}` and `{3,4,5}` are two leaders of one term with every node
    // counting a correct majority of the set it believes in.
    let mut l = fresh_leader(1, cfg3(), 127);
    let term = l.term();
    let v = l.config().version;
    let mut out = Vec::new();

    let two_at_once = Config::new([n(1), n(2), n(4)], v + 1, term);
    assert_refused(&l.begin_membership(&two_at_once, YES, &mut out).unwrap_err(), "moves 2 nodes");

    // A change that moves nobody is refused too, and not as a harmless no-op: it burns a version and
    // consumes the precondition that serialises the real ones.
    let moves_nobody = Config::new([n(1), n(2), n(3)], v + 1, term);
    assert_refused(&l.begin_membership(&moves_nobody, YES, &mut out).unwrap_err(), "moves 0 nodes");

    // A promotion and an admission in one entry is two changes however it is spelled — which is why
    // the comparison is over each node's *standing* and not over the voter set alone.
    let held = Config::new([n(1), n(2), n(3)], 2, 0).with_learners([n(4)]);
    let mut m = leader_from_log(1, held.clone(), 0, 127);
    m.note_config_ack(n(2), held.at());
    let promote_and_admit = Config::new([n(1), n(2), n(3), n(4)], 3, m.term()).with_learners([n(5)]);
    assert_refused(&m.begin_membership(&promote_and_admit, YES, &mut out).unwrap_err(), "moves 2 nodes");

    // The mirror: exactly one is accepted.
    let one = l.config().adding_learner(n(4), term);
    l.begin_membership(&one, YES, &mut out).expect("a one-node change was refused");
}

#[test]
fn a_change_is_refused_unless_it_is_one_version_and_this_term() {
    let mut l = fresh_leader(1, cfg3(), 131);
    let v = l.config().version;
    let term = l.term();
    let mut out = Vec::new();

    // A version that skips cannot be told from one built against a set this node has never held.
    let skips = Config::new([n(1), n(2), n(3)], v + 2, term).with_learners([n(4)]);
    assert_refused(&l.begin_membership(&skips, YES, &mut out).unwrap_err(), "at version");

    // A version that repeats is a change built on the set *before* the one in force.
    let repeats = Config::new([n(1), n(2), n(3)], v, term).with_learners([n(4)]);
    assert_refused(&l.begin_membership(&repeats, YES, &mut out).unwrap_err(), "at version");

    // An older term is a replay of a change a dead leader began. Counting acknowledgements of it
    // toward this term's change is exactly the ambiguity the (version, term) pair removes.
    let replay = Config::new([n(1), n(2), n(3)], v + 1, term - 1).with_learners([n(4)]);
    assert_refused(&l.begin_membership(&replay, YES, &mut out).unwrap_err(), "created in term");
    assert_eq!(l.config(), &cfg3(), "a refused change was installed anyway");
}

#[test]
fn installing_a_configuration_clears_behind_and_leaves_unjoined_alone() {
    // F5 must not clear `unjoined` while installing a configuration: knowing the voter set says
    // nothing about holding a single round of the log, and clearing them together is how a node added
    // to a running cluster pre-votes on its very next tick holding nothing. The rule is F1's; this
    // pins that F5's install path did not quietly undo it.
    let mut joiner = Consensus::joining(n(4), 151);
    assert!(joiner.behind && joiner.unjoined);
    let cfg = Config::new([n(1), n(2), n(3)], 2, 1).with_learners([n(4)]);
    let mut out = Vec::new();
    joiner.note_config_in_log(cfg, &mut out).expect("a joining node could not be told its configuration");
    assert!(!joiner.behind, "installing a configuration did not clear `behind`");
    assert!(
        joiner.unjoined,
        "installing a configuration cleared `unjoined` too — they are cleared by different evidence"
    );
}

// ================================================================ 3 -> 5

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

    cl.at(n(1)).note_bootstrap_config();
    let out = win_election(cl.at(n(1)), &[2, 3]);
    cl.record(n(1), &out);
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

        // A learner is promoted only once it holds the leader's committed round. Standing in for F2:
        // replication has carried everything committed to the node about to be promoted.
        if let Change::Promote(p) = change {
            let commit = cl.get(n(1)).commit;
            cl.at(n(1)).progress.get_mut(&p).expect("no progress for the learner").matched = commit;
        }

        let target = cl
            .at(n(1))
            .plan_change(change, YES)
            .unwrap_or_else(|e| panic!("change {i} ({change}) was refused: {e}"));
        assert_eq!(
            target,
            growth_sequence()[i + 1],
            "change {i} ({change}) did not produce the configuration 3 -> 5 passes through"
        );

        let mut out = Vec::new();
        cl.at(n(1))
            .begin_membership(&target, YES, &mut out)
            .unwrap_or_else(|e| panic!("the gate refused {change}: {e}"));
        cl.record(n(1), &out);
        assert_eq!(cl.get(n(1)).config(), &target, "the leader did not install what it appended");
        assert_eq!(
            cl.get(n(1)).config().quorum(),
            expected_quorums[i + 1],
            "after {change} the quorum is wrong, which is a majority counted against the wrong number"
        );

        // **The next change must be refused right here**, whatever it is — the precondition doing its
        // job in the middle of a real sequence rather than in a unit test of itself.
        assert!(
            cl.at(n(1)).plan_change(Change::AddLearner(n(9)), YES).is_err(),
            "a second change was admitted while {change} was still unacknowledged"
        );

        // The caller fsyncs and reports, then replication carries the entry to a majority of the
        // voters now in force — exactly a majority and no more, so the count is measured at its
        // boundary rather than swamped.
        cl.at(n(1)).note_config_in_log(target.clone(), &mut out).expect("the caller's report");
        let voters: Vec<NodeId> = target.members().to_vec();
        let quorum = target.quorum();
        for (k, p) in voters.iter().copied().filter(|p| *p != n(1)).enumerate() {
            if k + 2 > quorum {
                break;
            }
            assert!(
                cl.at(n(1)).change_in_flight(),
                "{change} was reported settled before a majority held it"
            );
            cl.at(n(1)).note_config_ack(p, target.at());
        }
        assert!(!cl.at(n(1)).change_in_flight(), "a majority holding {change} was not counted");

        // Every node that receives the entry installs it — including the node being admitted, which
        // is how it learns it is a member at all.
        let all: Vec<NodeId> = cl.nodes.iter().map(|c| c.id()).collect();
        for h in all {
            if h == n(1) {
                continue;
            }
            let mut o = Vec::new();
            cl.at(h)
                .note_config_in_log(target.clone(), &mut o)
                .unwrap_or_else(|e| panic!("node {h} could not install {change}: {e}"));
            cl.record(h, &o);
        }
    }

    let final_cfg = cl.get(n(1)).config().clone();
    assert_eq!(final_cfg.members(), [n(1), n(2), n(3), n(4), n(5)]);
    assert!(final_cfg.learners().is_empty());
    assert_eq!(final_cfg.quorum(), 3);
    assert_eq!(cl.get(n(1)).role(), Role::Leader, "the leader did not survive its own growth");
    // The nodes that joined hold the cluster's configuration and are voters in it — and are still
    // `unjoined`, because holding a configuration says nothing about holding the log. That is F1's
    // rule, and this is the composite state F5 hands it.
    for j in [4, 5] {
        assert_eq!(cl.get(n(j)).config(), &final_cfg, "node {j} was never told the configuration");
        assert!(cl.get(n(j)).unjoined, "node {j} may campaign holding none of the log");
    }

    for (term, holders) in &cl.leaders {
        assert_eq!(holders.len(), 1, "term {term} had leaders {holders:?}");
    }
}

#[test]
fn no_term_elects_two_leaders_while_the_configuration_is_changing_underneath() {
    // **The two-leader window is when some nodes hold a change and others do not**, so that is what
    // this builds: every adjacent pair in the 3 -> 5 sequence, at every split of the cluster between
    // the old configuration and the new one, driven by an election storm in which every node
    // campaigns for real. Appends are dropped, so no node believes a leader exists and every node
    // campaigns on every window — the harshest case for the property.
    let seq = growth_sequence();
    let mut storms = 0;
    let mut elections = 0;
    let mut dropped = 0;
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
                let configs: Vec<Config> = (0..ids.len())
                    .map(|i| if i < split { new.clone() } else { old.clone() })
                    .collect();
                let mut cl = Cluster::new(&ids, &configs, seed);
                cl.election_storm(60);
                storms += 1;
                elections += cl.leaders.values().map(|s| s.len()).sum::<usize>();
                dropped += cl.dropped_appends;
                for (term, holders) in &cl.leaders {
                    assert_eq!(
                        holders.len(),
                        1,
                        "seed {seed}, split {split}, v{} -> v{}: term {term} elected {holders:?}",
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
        "only {elections} leaderships across {storms} storms — the storm is not electing anybody, so \
         the property holds over an empty record"
    );
    assert!(dropped > 0, "no leader ever heartbeated, so no leader ever took office");
}

// ================================================================ a removed node

#[test]
fn a_removed_node_that_keeps_running_cannot_disrupt_the_cluster() {
    // Two protections, and they are different. A node that was TOLD (demoted) can never campaign
    // again — F5's own mechanism, in `a_demoted_node_can_never_campaign_again`. A node that was never
    // told is walled off instead, by F1's pre-vote and the election restriction, and this is that
    // composite: F5's part is that the leader stops tracking it and stops counting it, so it cannot
    // contribute to any majority either.
    let five = Config::new([n(1), n(2), n(3), n(4), n(5)], 2, 0);
    let cfgs = vec![five.clone(); 5];
    let mut cl = Cluster::new(&[1, 2, 3, 4, 5], &cfgs, 17);

    for id in [1, 2, 3, 4, 5] {
        let mut o = Vec::new();
        cl.at(n(id)).note_config_in_log(five.clone(), &mut o).unwrap();
    }
    let out = win_election(cl.at(n(1)), &[2, 3, 4, 5]);
    cl.record(n(1), &out);
    let term_before = cl.get(n(1)).term();
    for p in [2, 3, 4, 5] {
        cl.at(n(1)).note_config_ack(n(p), five.at());
    }

    // Demote node 5 and then drop it, and let nobody tell node 5 — the case the pre-vote wall is for.
    let mut out = Vec::new();
    let demoted = cl.at(n(1)).plan_change(Change::Demote(n(5)), YES).expect("demoting a voter");
    cl.at(n(1)).begin_membership(&demoted, YES, &mut out).unwrap();
    cl.at(n(1)).note_config_in_log(demoted.clone(), &mut out).unwrap();
    for p in [2, 3] {
        cl.at(n(1)).note_config_ack(n(p), demoted.at());
    }
    for h in [2, 3, 4] {
        let mut o = Vec::new();
        cl.at(n(h)).note_config_in_log(demoted.clone(), &mut o).unwrap();
        cl.record(n(h), &o);
    }
    let gone = cl.at(n(1)).plan_change(Change::Remove(n(5)), YES).expect("removing a learner");
    cl.at(n(1)).begin_membership(&gone, YES, &mut out).unwrap();
    cl.at(n(1)).note_config_in_log(gone.clone(), &mut out).unwrap();
    cl.record(n(1), &out);
    assert_eq!(cl.get(n(1)).config().members(), [n(1), n(2), n(3), n(4)]);
    assert_eq!(cl.get(n(5)).config().members(), [n(1), n(2), n(3), n(4), n(5)], "node 5 was told");

    // The leader stops sending to it and stops counting it. `progress` is F1's `apply_config`;
    // `acked` is F5's.
    assert!(!cl.get(n(1)).progress.contains_key(&n(5)), "the leader still tracks a removed node");
    assert!(!cl.get(n(1)).acked.contains_key(&n(5)), "the leader still counts a removed node");

    // (i) With the leader alive, node 5 campaigns into a wall: every node still being served refuses
    //     its pre-vote, so **the cluster's term never rises** — which is the disruption.
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
    }
    assert_eq!(cl.get(n(1)).role(), Role::Leader, "a removed node deposed a healthy leader");
    assert!(cl.get(n(5)).term() <= term_before, "a removed node raised its own term for real");
    assert!(cl.delivered_votes > 0, "node 5 never asked anybody for anything");

    // (ii) And with the leader gone it still cannot win, because it has not been replicated to: the
    //      election restriction refuses a candidate whose log is less complete than the voter's.
    //      Standing in for F2: the cluster committed rounds while node 5 was out.
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
