//! F1 — the election rules, one named test per rule, each with a mutant recorded in
//! `bench/evidence/F1-election.md`.
//!
//! Two things about how these are written.
//!
//! **Every step goes through [`step_checked`]**, which re-asserts on *every* action list any test
//! in this file ever produces the one rule whose violation cannot be seen from outside a crash:
//! `PersistHardState` precedes the `Send` of any vote it makes possible. A rule checked in one
//! test is a rule checked on one path.
//!
//! **Nothing here waits.** Time is `Event::Tick` and the network is a `Vec<Action>`, so a campaign,
//! a split vote and an expiring lease are all exact rather than probable.
//!
//! Where a test reaches into a field directly it is standing in for a handler that is not built
//! yet — `replicate.rs` (F2) zeroes `Progress::silent` on an `AppendResp` and sets `leader` on an
//! accepted `Append`. Each such line says so.

use crate::consensus::config::{CfgAt, Config};
use crate::consensus::*;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

// ---------------------------------------------------------------- harness

fn cfg3() -> Config {
    Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 1)
}

fn node(id: u32, cfg: Config, seed: u64) -> Consensus {
    Consensus::new(NodeId(id), cfg, seed)
}

fn msg(from: u32, to: u32, term: Term, body: Body) -> Event {
    Event::Recv(Message { from: NodeId(from), to: NodeId(to), term, body })
}

/// The only way this file steps a node.
///
/// **`PersistHardState` must be emitted BEFORE the corresponding `Send` of any vote.** A node that
/// votes, crashes, and comes back having forgotten the vote can vote twice in one term, which
/// elects two leaders of that term — and each of them behaves correctly given what it believes, so
/// nothing later in the protocol can notice. Checked here, on every action list, for both kinds of
/// vote: a grant this node gives somebody, and the vote it casts for itself when it asks.
fn step_checked(c: &mut Consensus, ev: Event) -> Vec<Action> {
    let out = c.step(ev);
    for (i, a) in out.iter().enumerate() {
        let Action::Send(m) = a else { continue };
        let expected = match &m.body {
            // A grant: the voter must already have written down whom it voted for.
            Body::RequestVoteResp { granted: true } => Some((m.term, m.to)),
            // An ask: a candidate votes for itself, and that vote is a vote.
            Body::RequestVote { .. } => Some((m.term, m.from)),
            _ => None,
        };
        let Some((term, who)) = expected else { continue };
        let durable = out[..i].iter().any(|p| {
            matches!(p, Action::PersistHardState { term: t, voted_for: Some(v) } if *t == term && *v == who)
        });
        assert!(
            durable,
            "a vote reached the wire before it reached the disk: {:?} at index {i} of {out:#?}\n\
             A node that votes, crashes and forgets the vote can vote twice in one term, which \
             elects two leaders of that term.",
            m.body
        );
    }
    out
}

fn tick_until(c: &mut Consensus, budget: u32, done: impl Fn(&Consensus) -> bool) -> Vec<Action> {
    let mut out = Vec::new();
    for _ in 0..budget {
        if done(c) {
            return out;
        }
        out.extend(step_checked(c, Event::Tick));
    }
    assert!(
        done(c),
        "condition not reached in {budget} ticks (role {}, term {})",
        c.role(),
        c.term()
    );
    out
}

/// Tick exactly `n` times and return everything that came back. Distinct from [`tick_until`]: a
/// test that a node does NOT campaign has no condition to wait for, and one written as a wait would
/// pass by timing out rather than by measuring anything.
fn tick_n(c: &mut Consensus, n: u32) -> Vec<Action> {
    let mut out = Vec::new();
    for _ in 0..n {
        out.extend(step_checked(c, Event::Tick));
    }
    out
}

/// Tick until this node starts a fresh pre-campaign, and return everything that came back —
/// including that campaign's own actions. The campaign is recognised by its `RoleChanged`, not by
/// the role itself, so that a node already pre-campaigning is driven to a *new* campaign rather
/// than returning immediately on the old one.
fn campaign(c: &mut Consensus) -> Vec<Action> {
    let budget = 4 * c.election_base + 4;
    let mut out = Vec::new();
    for _ in 0..budget {
        out.extend(step_checked(c, Event::Tick));
        if out.iter().any(|a| matches!(a, Action::RoleChanged { role: Role::PreCandidate, .. })) {
            return out;
        }
    }
    panic!("no pre-campaign started in {budget} ticks (role {}, term {})", c.role(), c.term());
}

/// Drive a node all the way to leader through the protocol — time out, collect a pre-vote quorum,
/// then a real vote quorum. No field is reached into.
fn win_election(c: &mut Consensus, granters: &[u32]) -> Vec<Action> {
    let me = c.id().0;
    let mut out = campaign(c);
    assert_eq!(c.role(), Role::PreCandidate, "the node never started a pre-campaign");

    let asked = c.term() + 1;
    for g in granters {
        if c.role() != Role::PreCandidate {
            break;
        }
        out.extend(step_checked(c, msg(*g, me, asked, Body::PreVoteResp { granted: true })));
    }
    assert_eq!(c.role(), Role::Candidate, "a pre-vote quorum did not raise the term");

    let term = c.term();
    for g in granters {
        if c.role() != Role::Candidate {
            break;
        }
        out.extend(step_checked(c, msg(*g, me, term, Body::RequestVoteResp { granted: true })));
    }
    assert_eq!(c.role(), Role::Leader, "a vote quorum did not elect");
    out
}

fn sends(out: &[Action]) -> Vec<&Message> {
    out.iter()
        .filter_map(|a| match a {
            Action::Send(m) => Some(m),
            _ => None,
        })
        .collect()
}

fn bodies_of<'a>(out: &'a [Action], want: fn(&Body) -> bool) -> Vec<&'a Message> {
    sends(out).into_iter().filter(|m| want(&m.body)).collect()
}

fn is_prevote(b: &Body) -> bool { matches!(b, Body::PreVote { .. }) }
fn is_requestvote(b: &Body) -> bool { matches!(b, Body::RequestVote { .. }) }
fn is_append(b: &Body) -> bool { matches!(b, Body::Append { .. }) }

/// The `granted` flag of the single vote answer in an action list.
fn vote_answer(out: &[Action]) -> bool {
    let found: Vec<bool> = sends(out)
        .iter()
        .filter_map(|m| match m.body {
            Body::RequestVoteResp { granted } => Some(granted),
            _ => None,
        })
        .collect();
    assert_eq!(found.len(), 1, "expected exactly one vote answer, got {found:?} in {out:#?}");
    found[0]
}

fn pre_vote_answer(out: &[Action]) -> bool {
    let found: Vec<bool> = sends(out)
        .iter()
        .filter_map(|m| match m.body {
            Body::PreVoteResp { granted } => Some(granted),
            _ => None,
        })
        .collect();
    assert_eq!(found.len(), 1, "expected exactly one pre-vote answer, got {found:?} in {out:#?}");
    found[0]
}

fn wrote_hard_state(out: &[Action]) -> bool {
    out.iter().any(|a| matches!(a, Action::PersistHardState { .. }))
}

// ---------------------------------------------------------------- Raft §5.4.1, the restriction

#[test]
fn the_election_restriction_compares_last_term_before_last_round() {
    // The voter holds (term 5, round 2). The candidate holds (term 3, round 9) — MORE rounds, but
    // written by an older leader, so its extra suffix was never committed while the voter's round 2
    // may have been. Electing it discards round 2: **acknowledged data, lost, silently.**
    //
    // Comparing rounds alone, or comparing the pair in the other order, is the same defect written
    // two ways, and both grant here.
    let mut v = node(2, cfg3(), 11);
    v.hard.term = 5;
    v.last_term = 5;
    v.last_round = 2;
    let out = step_checked(&mut v, msg(1, 2, 6, Body::RequestVote { last_term: 3, last_round: 9 }));
    assert!(
        !vote_answer(&out),
        "a vote was granted to a candidate whose log ends in an OLDER term but at a higher round; \
         electing it truncates the voter's round 2, which may be acknowledged"
    );

    // The mirror, so a rule that simply refuses everything does not pass this test: a candidate
    // whose term matches and whose round is behind is refused, and one at or beyond the voter is
    // granted.
    let mut w = node(2, cfg3(), 11);
    w.hard.term = 5;
    w.last_term = 5;
    w.last_round = 2;
    let out = step_checked(&mut w, msg(1, 2, 6, Body::RequestVote { last_term: 5, last_round: 1 }));
    assert!(!vote_answer(&out), "a vote was granted to a candidate a round behind in the same term");

    let mut x = node(2, cfg3(), 11);
    x.hard.term = 5;
    x.last_term = 5;
    x.last_round = 2;
    let out = step_checked(&mut x, msg(1, 2, 6, Body::RequestVote { last_term: 5, last_round: 2 }));
    assert!(vote_answer(&out), "an exactly-as-complete candidate was refused, so no election can ever finish");

    let mut y = node(2, cfg3(), 11);
    y.hard.term = 5;
    y.last_term = 5;
    y.last_round = 2;
    let out = step_checked(&mut y, msg(1, 2, 6, Body::RequestVote { last_term: 6, last_round: 1 }));
    assert!(vote_answer(&out), "a candidate whose log ends in a LATER term was refused for having fewer rounds");
}

#[test]
fn the_election_restriction_is_applied_to_pre_votes_too() {
    // A pre-vote that ignored the restriction would tell a candidate it can win an election it must
    // lose, which turns a harmless hypothetical into a real term change and a real disruption.
    let mut v = node(2, cfg3(), 11);
    v.hard.term = 5;
    v.last_term = 5;
    v.last_round = 2;
    let out = step_checked(&mut v, msg(1, 2, 6, Body::PreVote { last_term: 3, last_round: 9 }));
    assert!(!pre_vote_answer(&out), "a pre-vote ignored the election restriction");

    let out = step_checked(&mut v, msg(1, 2, 6, Body::PreVote { last_term: 5, last_round: 2 }));
    assert!(pre_vote_answer(&out), "an as-complete candidate was refused its pre-vote");
}

// ---------------------------------------------------------------- pre-vote raises no term

#[test]
fn a_pre_campaign_does_not_raise_this_nodes_term_and_writes_nothing_durable() {
    let mut c = node(1, cfg3(), 3);
    let t0 = c.term();
    let out = campaign(&mut c);

    assert_eq!(c.role(), Role::PreCandidate);
    assert_eq!(c.term(), t0, "a pre-campaign raised the campaigning node's own term");
    assert!(
        !wrote_hard_state(&out),
        "a pre-campaign wrote hard state; it promises nothing, and a node partitioned into a wall \
         must leave the cluster exactly as it found it however often it retries"
    );

    // The ask carries term + 1: the hypothetical it is asking about, from a node that has not
    // entered it.
    let asks = bodies_of(&out, is_prevote);
    assert_eq!(asks.len(), 2, "a three-node campaign asked {} peers", asks.len());
    for m in &asks {
        assert_eq!(m.term, t0 + 1, "a pre-vote did not carry the hypothetical term");
        assert_eq!(
            m.body,
            Body::PreVote { last_term: c.last_term, last_round: c.last_round },
            "a pre-vote did not carry the asker's log tail, so no voter could apply the restriction"
        );
    }
    assert!(bodies_of(&out, is_requestvote).is_empty(), "a pre-campaign sent real vote requests");
}

#[test]
fn answering_a_pre_vote_neither_raises_the_voters_term_nor_writes_hard_state() {
    // The grant is the interesting case: a handler that refused everything would pass a test that
    // only looked at refusals.
    let mut v = node(2, cfg3(), 5);
    let out = step_checked(&mut v, msg(1, 2, 1, Body::PreVote { last_term: 0, last_round: 0 }));

    assert!(pre_vote_answer(&out), "a node with no leader and an equal log refused a pre-vote");
    assert_eq!(v.term(), 0, "answering a pre-vote raised the voter's term — the disruption pre-vote exists to prevent, arriving through the mechanism meant to stop it");
    assert_eq!(v.role(), Role::Follower, "answering a pre-vote changed the voter's role");
    assert_eq!(v.hard.voted_for, None, "a pre-vote consumed the voter's real vote");
    assert!(!wrote_hard_state(&out), "a pre-vote answer cost an fsync; it promises nothing");

    // The answer is tagged with the term it is ABOUT, not the voter's own, or the campaign that
    // asked could not tell it from an answer to a different campaign.
    let answers = sends(&out);
    assert_eq!(answers[0].term, 1, "a pre-vote answer did not carry the term it answers");
}

// ---------------------------------------------------------------- the captured configuration

#[test]
fn the_campaign_configuration_is_captured_in_the_same_step_that_raises_the_term() {
    let mut c = node(1, cfg3(), 3);
    campaign(&mut c);
    let t0 = c.term();

    // One step: the pre-vote quorum arrives.
    let out = step_checked(&mut c, msg(2, 1, t0 + 1, Body::PreVoteResp { granted: true }));

    assert_eq!(c.term(), t0 + 1, "a pre-vote quorum did not raise the term");
    assert_eq!(
        c.campaign.as_ref(),
        Some(&cfg3()),
        "the step that raised the term did not capture the configuration this campaign counts against"
    );
    assert_eq!(c.hard.voted_for, Some(NodeId(1)), "the step that raised the term did not cast this node's own vote");

    let p = out
        .iter()
        .position(|a| matches!(a, Action::PersistHardState { term, voted_for: Some(v) } if *term == t0 + 1 && *v == NodeId(1)))
        .expect("raising the term wrote no hard state");
    let s = out
        .iter()
        .position(|a| matches!(a, Action::Send(m) if is_requestvote(&m.body)))
        .expect("raising the term asked nobody for a vote");
    assert!(p < s, "the candidate asked for votes before its own vote was durable: {out:#?}");
}

#[test]
fn a_membership_change_mid_campaign_does_not_move_the_denominator() {
    // Captured against five voters, so three votes carry it. A membership change to three voters
    // lands while the campaign is in flight. Two votes must still not be a majority, because they
    // were asked of the five — a majority counted against the wrong number elects two leaders of
    // one term, and nothing later in this protocol can notice.
    let five = Config::new((1..=5).map(NodeId), 1, 1);
    let mut c = node(1, five, 3);
    campaign(&mut c);
    let t0 = c.term();
    step_checked(&mut c, msg(2, 1, t0 + 1, Body::PreVoteResp { granted: true }));
    step_checked(&mut c, msg(3, 1, t0 + 1, Body::PreVoteResp { granted: true }));
    assert_eq!(c.role(), Role::Candidate, "three of five did not carry the pre-vote");

    // The cluster shrinks underneath the campaign.
    let mut sink = Vec::new();
    c.apply_config(Config::new((1..=3).map(NodeId), 2, 1), &mut sink);
    assert_eq!(c.config().quorum(), 2, "the installed configuration should now need only two");

    let term = c.term();
    step_checked(&mut c, msg(2, 1, term, Body::RequestVoteResp { granted: true }));
    assert_ne!(
        c.role(),
        Role::Leader,
        "two votes elected a leader of a campaign that asked five nodes — the denominator moved \
         underneath the campaign"
    );

    step_checked(&mut c, msg(3, 1, term, Body::RequestVoteResp { granted: true }));
    assert_eq!(c.role(), Role::Leader, "three of the captured five did not elect");
}

#[test]
fn a_vote_from_a_node_outside_the_campaign_configuration_is_not_counted() {
    let mut c = node(1, cfg3(), 3);
    campaign(&mut c);
    let t0 = c.term();

    step_checked(&mut c, msg(9, 1, t0 + 1, Body::PreVoteResp { granted: true }));
    assert_eq!(c.role(), Role::PreCandidate, "a stranger's pre-vote carried the campaign");
    step_checked(&mut c, msg(2, 1, t0 + 1, Body::PreVoteResp { granted: true }));
    assert_eq!(c.role(), Role::Candidate);

    let term = c.term();
    step_checked(&mut c, msg(9, 1, term, Body::RequestVoteResp { granted: true }));
    assert_eq!(c.role(), Role::Candidate, "a stranger's vote elected a leader");
    step_checked(&mut c, msg(3, 1, term, Body::RequestVoteResp { granted: true }));
    assert_eq!(c.role(), Role::Leader);
}

#[test]
fn a_pre_vote_answer_about_another_term_is_not_counted() {
    // Two half-quorums separated in time must not add up to one.
    let mut c = node(1, cfg3(), 3);
    campaign(&mut c);
    let t0 = c.term();

    step_checked(&mut c, msg(2, 1, t0, Body::PreVoteResp { granted: true }));
    assert_eq!(c.role(), Role::PreCandidate, "an answer about the term we are already IN carried the campaign");
    step_checked(&mut c, msg(3, 1, t0 + 7, Body::PreVoteResp { granted: true }));
    assert_eq!(c.role(), Role::PreCandidate, "an answer about a term nobody asked about carried the campaign");

    step_checked(&mut c, msg(2, 1, t0 + 1, Body::PreVoteResp { granted: true }));
    assert_eq!(c.role(), Role::Candidate, "the answer to the term actually asked about was ignored");
}

#[test]
fn a_pre_candidate_does_not_count_real_votes_it_never_asked_for() {
    let mut c = node(1, cfg3(), 3);
    campaign(&mut c);
    let t0 = c.term();
    step_checked(&mut c, msg(2, 1, t0, Body::RequestVoteResp { granted: true }));
    step_checked(&mut c, msg(3, 1, t0, Body::RequestVoteResp { granted: true }));
    assert_eq!(c.role(), Role::PreCandidate, "a pre-candidate was elected by real votes it never asked for");
    assert_eq!(c.term(), t0, "a pre-candidate's term moved");
}

// ---------------------------------------------------------------- one vote per term

#[test]
fn a_granted_vote_is_made_durable_before_it_is_sent() {
    let mut v = node(2, cfg3(), 7);
    let out = step_checked(&mut v, msg(1, 2, 1, Body::RequestVote { last_term: 0, last_round: 0 }));
    assert!(vote_answer(&out));

    let p = out
        .iter()
        .position(|a| matches!(a, Action::PersistHardState { term: 1, voted_for: Some(n) } if *n == NodeId(1)))
        .expect("the vote was never written down");
    let s = out
        .iter()
        .position(|a| matches!(a, Action::Send(m) if matches!(m.body, Body::RequestVoteResp { granted: true })))
        .expect("the grant was never sent");
    assert!(
        p < s,
        "the vote was sent before it was made durable — a node that votes, crashes and forgets \
         can vote twice in one term: {out:#?}"
    );
}

#[test]
fn a_node_does_not_vote_twice_in_one_term() {
    let mut v = node(2, cfg3(), 7);
    let a = step_checked(&mut v, msg(1, 2, 1, Body::RequestVote { last_term: 0, last_round: 0 }));
    assert!(vote_answer(&a), "the first candidate of a new term was refused");

    let b = step_checked(&mut v, msg(3, 2, 1, Body::RequestVote { last_term: 0, last_round: 0 }));
    assert!(
        !vote_answer(&b),
        "a second candidate of the same term was also granted a vote — both can now reach a \
         majority of the same three nodes, which is two leaders of one term"
    );

    // A retransmission of the same request is the same vote, not a second one. Without this a
    // dropped response costs an election every time the transport duplicates or retries.
    let c = step_checked(&mut v, msg(1, 2, 1, Body::RequestVote { last_term: 0, last_round: 0 }));
    assert!(vote_answer(&c), "a retransmitted request from the node already voted for was refused");
}

#[test]
fn a_new_term_is_a_new_vote() {
    let mut v = node(2, cfg3(), 7);
    step_checked(&mut v, msg(1, 2, 1, Body::RequestVote { last_term: 0, last_round: 0 }));
    // Term 2 is a different term: the vote cast in term 1 must not refuse its first candidate.
    let out = step_checked(&mut v, msg(3, 2, 2, Body::RequestVote { last_term: 0, last_round: 0 }));
    assert!(vote_answer(&out), "a vote cast in an earlier term was carried forward and refused the new term's first candidate");
    assert_eq!(v.hard.voted_for, Some(NodeId(3)));
    assert_eq!(v.term(), 2);
}

#[test]
fn one_term_never_elects_two_leaders_even_when_every_node_campaigns() {
    // A hand-routed three-node cluster driven for 3000 ticks.
    //
    // `Append` is dropped rather than delivered, because `replicate.rs` is not built and would
    // panic in its `unimplemented!`. That costs this test nothing it is measuring — every vote is
    // still delivered — and makes the case *harder*: with no heartbeats landing, every node times
    // out and campaigns repeatedly, so the run is nothing but overlapping elections.
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let cfg = Config::new(ids, 1, 1);
    let mut nodes: Vec<Consensus> =
        ids.iter().enumerate().map(|(i, id)| Consensus::new(*id, cfg.clone(), 1000 + i as u64)).collect();

    let mut queue: VecDeque<Message> = VecDeque::new();
    let mut dropped = 0usize;
    let mut delivered = 0usize;
    let mut leaders: BTreeMap<Term, BTreeSet<NodeId>> = BTreeMap::new();

    for _ in 0..3000 {
        for n in nodes.iter_mut() {
            for a in n.step(Event::Tick) {
                if let Action::Send(m) = a {
                    queue.push_back(m);
                }
            }
        }
        while let Some(m) = queue.pop_front() {
            if !matches!(
                m.body,
                Body::PreVote { .. } | Body::PreVoteResp { .. } | Body::RequestVote { .. } | Body::RequestVoteResp { .. }
            ) {
                dropped += 1;
                continue;
            }
            delivered += 1;
            let i = ids.iter().position(|x| *x == m.to).expect("a message addressed to nobody");
            for a in step_checked(&mut nodes[i], Event::Recv(m)) {
                if let Action::Send(o) = a {
                    queue.push_back(o);
                }
            }
        }
        for n in nodes.iter() {
            if n.role() == Role::Leader {
                leaders.entry(n.term()).or_default().insert(n.id());
            }
        }
        for (t, who) in &leaders {
            assert_eq!(who.len(), 1, "term {t} elected {} leaders: {who:?}", who.len());
        }
    }

    // A run that asserted nothing is not a pass.
    assert!(!leaders.is_empty(), "3000 ticks elected nobody, so this test measured nothing");
    assert!(leaders.len() > 1, "only one term was ever entered, so overlapping campaigns were never exercised");
    assert!(delivered > 100, "only {delivered} vote messages were delivered");
    assert!(dropped > 0, "no heartbeat was ever produced, so no leader ever took office");
}

// ---------------------------------------------------------------- the wall pre-vote campaigns into

#[test]
fn a_pre_vote_is_refused_by_a_node_that_is_still_being_served_by_a_leader() {
    // This is the wall. Without it a node that lost one link collects pre-votes from a healthy
    // cluster, raises the term for real, and deposes a leader that never stopped working — which
    // is precisely what pre-vote exists to prevent.
    let mut v = node(2, cfg3(), 7);
    // What `become_follower(term, Some(leader))` leaves behind when `replicate.rs` accepts an
    // `Append`: a known leader, and a countdown just restarted.
    v.hard.term = 4;
    v.leader = Some(NodeId(1));
    v.since_heard = 0;

    let out = step_checked(&mut v, msg(3, 2, 5, Body::PreVote { last_term: 0, last_round: 0 }));
    assert!(
        !pre_vote_answer(&out),
        "a node still hearing from its leader told a peer it could win, which raises the term on a \
         healthy cluster"
    );

    // The wall comes down on its own evidence: a full election window with nothing from the leader.
    v.since_heard = v.election_timeout;
    let out = step_checked(&mut v, msg(3, 2, 5, Body::PreVote { last_term: 0, last_round: 0 }));
    assert!(pre_vote_answer(&out), "a node whose leader has gone silent for a full window still refused a pre-vote — no election could ever start");
}

#[test]
fn a_vote_in_the_current_term_is_refused_by_a_node_that_still_has_a_live_leader() {
    // Only at the *current* term: a request carrying a later term has already taken this node with
    // it in `mod.rs`, clearing `leader`, so the refusal below cannot block a legitimate promotion.
    let mut v = node(2, cfg3(), 7);
    v.hard.term = 4;
    v.leader = Some(NodeId(1));
    v.since_heard = 0;

    let out = step_checked(&mut v, msg(3, 2, 4, Body::RequestVote { last_term: 0, last_round: 0 }));
    assert!(!vote_answer(&out), "a node being served by a leader of term 4 voted for a rival in term 4");

    let out = step_checked(&mut v, msg(3, 2, 5, Body::RequestVote { last_term: 0, last_round: 0 }));
    assert!(vote_answer(&out), "leader stickiness blocked a candidate carrying a LATER term, which is a promotion this node must accept");
}

// ---------------------------------------------------------------- `behind`

#[test]
fn a_node_that_knows_its_configuration_is_stale_does_not_campaign() {
    // A node counting votes against a set the cluster has left fences the healthy leader out of
    // office on every window it draws — a livelock in which no node with an up-to-date
    // configuration can hold the office and no node without one can win it.
    let mut c = node(1, cfg3(), 3);
    c.observe_config_at(CfgAt { version: 9, term: 3 });
    assert!(c.behind, "hearing a newer configuration version did not mark this node stale");

    let out = tick_n(&mut c, 500);
    assert_eq!(c.role(), Role::Follower, "a node holding a stale configuration campaigned");
    assert_eq!(c.term(), 0, "a node holding a stale configuration raised the cluster's term");
    assert!(bodies_of(&out, is_prevote).is_empty(), "a node holding a stale configuration asked for pre-votes");
}

#[test]
fn behind_is_cleared_by_applying_a_configuration_and_by_nothing_else() {
    let mut c = node(1, cfg3(), 3);
    c.observe_config_at(CfgAt { version: 9, term: 3 });
    tick_n(&mut c, 1000);
    assert!(c.behind, "time cleared a flag that is a statement about a configuration");

    let mut out = Vec::new();
    c.apply_config(Config::new([NodeId(1), NodeId(2), NodeId(3)], 9, 3), &mut out);
    assert!(!c.behind, "applying a configuration did not clear `behind`");

    campaign(&mut c);
    assert_eq!(c.role(), Role::PreCandidate, "a node that has caught up still could not stand");
}

#[test]
fn an_older_configuration_report_does_not_mark_a_current_node_stale() {
    // A detector that fires on everything is not a detector.
    let mut c = node(1, Config::new([NodeId(1), NodeId(2), NodeId(3)], 9, 3), 3);
    c.observe_config_at(CfgAt { version: 8, term: 3 });
    c.observe_config_at(CfgAt { version: 9, term: 3 });
    assert!(!c.behind, "an equal or older configuration report marked a current node stale");
    c.observe_config_at(CfgAt { version: 9, term: 4 });
    assert!(c.behind, "a same-version configuration from a LATER term was ignored, which is what makes the pair a pair");
}

// ---------------------------------------------------------------- `unjoined`

#[test]
fn an_unjoined_node_is_not_cleared_by_any_number_of_ticks() {
    // The whole point: a node added to a running cluster holds none of its log. A timer would clear
    // the flag on a node that still holds nothing, which is the case the flag exists for.
    let mut c = Consensus::joining(NodeId(3), 3);
    let mut out = Vec::new();
    c.apply_config(Config::new([NodeId(1), NodeId(2), NodeId(3)], 2, 1), &mut out);
    assert!(!c.behind, "applying a configuration must clear `behind`");
    assert!(
        c.unjoined,
        "applying a configuration cleared `unjoined` as well — they are cleared by different \
         evidence, and knowing the voter set says nothing about holding a single round"
    );

    let acts = tick_n(&mut c, 2000);
    assert!(c.unjoined, "a timer cleared `unjoined`");
    assert_eq!(c.role(), Role::Follower, "a node holding none of the log campaigned");
    assert_eq!(c.term(), 0, "a node holding none of the log raised the cluster's term");
    assert!(bodies_of(&acts, is_prevote).is_empty(), "a node holding none of the log asked for pre-votes");
}

#[test]
fn an_unjoined_node_stands_only_once_it_holds_what_a_quorum_holds() {
    let mut c = Consensus::joining(NodeId(3), 3);
    let mut out = Vec::new();
    c.apply_config(Config::new([NodeId(1), NodeId(2), NodeId(3)], 2, 1), &mut out);

    // A leader's `Append` says a quorum holds 57 rounds. This node's own store says it holds none.
    c.observe_quorum_watermark(57);
    assert!(c.unjoined, "a watermark this node cannot match cleared the flag");

    // Half way is not caught up.
    c.durable = 30;
    c.observe_quorum_watermark(57);
    assert!(c.unjoined, "a node holding 30 of 57 rounds was declared caught up");

    // `replicate.rs` reports durability through `Event::Persisted`; this is what it leaves behind.
    c.durable = 57;
    c.observe_quorum_watermark(57);
    assert!(!c.unjoined, "a node holding everything a quorum holds was still blocked from standing");

    campaign(&mut c);
    assert_eq!(c.role(), Role::PreCandidate);
}

#[test]
fn joining_an_empty_cluster_still_lets_a_new_member_stand() {
    // **A cluster whose log is empty reports a watermark of zero**, which every node already
    // matches. A rule phrased as "hold some rounds", or one that required a non-zero watermark,
    // would leave this member unable ever to stand — and a cold-start cluster of new members would
    // never elect anybody at all.
    let mut c = Consensus::joining(NodeId(3), 3);
    let mut out = Vec::new();
    c.apply_config(Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0), &mut out);
    assert_eq!(c.durable, 0);

    c.observe_quorum_watermark(0);
    assert!(!c.unjoined, "joining a cluster with an empty log left a member that can never stand");

    campaign(&mut c);
    assert_eq!(c.role(), Role::PreCandidate);
}

// ---------------------------------------------------------------- the leader lease

#[test]
fn a_leader_that_stops_hearing_from_a_majority_demotes_itself_before_any_peer_can_win() {
    // Without this a partitioned leader keeps serving reads out of the state it held when the
    // partition began, and the same row is served by the old leader and the new one.
    let mut c = node(1, cfg3(), 3);
    win_election(&mut c, &[2, 3]);
    assert_eq!(c.role(), Role::Leader);

    let lease = c.lease_window();
    let base = c.election_base;

    let mut n = 0u32;
    let mut demotion = Vec::new();
    while c.role() == Role::Leader {
        demotion = step_checked(&mut c, Event::Tick);
        n += 1;
        assert!(n < 10_000, "the leader never demoted itself");
    }

    assert_eq!(n, lease, "the leader demoted after {n} ticks, not on its lease window of {lease}");
    assert!(
        n < base,
        "the lease fired after {n} ticks, which is not strictly shorter than the shortest election \
         timeout a peer can draw ({base}) — a peer can win while the old leader still believes it \
         leads, which is the two-leader overlap the lease exists to prevent"
    );
    assert_eq!(c.role(), Role::Follower);
    assert_eq!(c.leader(), None, "a demoted leader still names itself the leader");
    assert!(
        demotion.iter().any(|a| matches!(a, Action::RoleChanged { role: Role::Follower, .. })),
        "the demotion was silent, so the surrounding server would keep serving writes: {demotion:#?}"
    );
}

#[test]
fn a_leader_that_keeps_hearing_from_a_majority_keeps_its_office() {
    // A detector that has never been shown to stay quiet is not a detector either.
    let mut c = node(1, cfg3(), 3);
    win_election(&mut c, &[2, 3]);

    // Node 3 never answers again; node 2 answers every third tick. Self plus node 2 is a majority
    // of three, so the lease must hold for ever.
    for t in 0..2000u32 {
        if t % 3 == 0 {
            // What `replicate.rs` does on an `AppendResp`: `Progress::silent` is the seam, and
            // `mod.rs` documents it as "ticks since this peer last answered, feeding the leader's
            // own lease".
            c.progress.get_mut(&NodeId(2)).expect("no progress for a voter").silent = 0;
        }
        step_checked(&mut c, Event::Tick);
        assert_eq!(c.role(), Role::Leader, "a leader hearing from a majority demoted itself at tick {t}");
    }
    assert!(
        c.progress[&NodeId(3)].silent > c.lease_window(),
        "the silent peer was never actually silent, so this test proved nothing"
    );
}

#[test]
fn a_leader_that_hears_only_from_a_minority_still_demotes() {
    // Five voters need three. Self plus one is two, and two is not a majority however loud it is.
    let five = Config::new((1..=5).map(NodeId), 1, 1);
    let mut c = node(1, five, 3);
    win_election(&mut c, &[2, 3]);
    assert_eq!(c.role(), Role::Leader);

    let mut n = 0u32;
    while c.role() == Role::Leader {
        c.progress.get_mut(&NodeId(2)).expect("no progress for a voter").silent = 0;
        step_checked(&mut c, Event::Tick);
        n += 1;
        assert!(n < 10_000, "a leader hearing from two of five never demoted");
    }
    assert_eq!(n, c.lease_window(), "the lease did not fire on its own window");
}

#[test]
fn a_leader_removed_from_its_own_configuration_steps_down() {
    // Applying the configuration that removes it is the direct path.
    let mut c = node(1, cfg3(), 3);
    win_election(&mut c, &[2, 3]);
    let term = c.term();
    let mut out = Vec::new();
    c.apply_config(Config::new([NodeId(2), NodeId(3)], 2, term), &mut out);
    assert_eq!(c.role(), Role::Follower, "a leader voted out of its own configuration kept the office");
    assert_eq!(c.leader(), None);

    // And the tick guard, for a configuration that arrived any other way: a leader that is not a
    // voter has no majority to measure, so it cannot be answered by the lease.
    let mut d = node(1, cfg3(), 3);
    win_election(&mut d, &[2, 3]);
    d.cfg = Config::new([NodeId(2), NodeId(3)], 2, d.term());
    step_checked(&mut d, Event::Tick);
    assert_eq!(d.role(), Role::Follower, "a leader outside its own configuration kept the office through a tick");
}

// ---------------------------------------------------------------- taking office

#[test]
fn a_new_leader_appends_a_no_op_of_its_own_term_before_it_can_commit_anything() {
    // Raft §5.4.2 forbids committing an inherited round by counting replicas, so a leader needs a
    // round of its OWN term to commit before any earlier round may commit as a side effect. Without
    // it a leader that is never given a write cannot advance the commit index at all, and its
    // followers never learn what is committed: live and stuck at once.
    let mut c = node(1, cfg3(), 3);
    let out = win_election(&mut c, &[2, 3]);

    let entries = out
        .iter()
        .rev()
        .find_map(|a| match a {
            Action::Persist { entries } => Some(entries.clone()),
            _ => None,
        })
        .expect("a new leader appended nothing, so it can never commit anything");

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].command, Command::NoOp);
    assert_eq!(entries[0].term, c.term(), "the term-establishing entry does not carry the leader's own term");
    assert_eq!(entries[0].round, 1, "the first round of an empty log is 1");
    assert_eq!(c.last_round, 1, "the leader did not count its own entry into its log tail");
    assert_eq!(c.last_term, c.term());
}

#[test]
fn a_new_leader_asserts_its_office_at_once_and_then_on_its_heartbeat_interval() {
    let mut c = node(1, cfg3(), 3);
    let out = win_election(&mut c, &[2, 3]);
    let first = bodies_of(&out, is_append);
    assert_eq!(first.len(), 2, "a new leader did not tell both peers at once; every follower is already counting down");
    for m in &first {
        assert_eq!(m.term, c.term());
        assert_eq!(
            m.body,
            Body::Append { prev_round: c.last_round, prev_term: c.last_term, entries: Vec::new(), commit: c.commit_round() },
            "a heartbeat named a log position this leader cannot vouch for"
        );
    }

    let hb = c.heartbeat;
    for i in 1..hb {
        let a = step_checked(&mut c, Event::Tick);
        assert!(bodies_of(&a, is_append).is_empty(), "a heartbeat went out at tick {i}, before the interval of {hb}");
    }
    let a = step_checked(&mut c, Event::Tick);
    assert_eq!(bodies_of(&a, is_append).len(), 2, "no heartbeat at the interval of {hb}");
}

#[test]
fn a_new_leader_starts_every_peer_with_no_evidence_and_optimistic_next() {
    let mut c = node(1, cfg3(), 3);
    win_election(&mut c, &[2, 3]);
    for p in [NodeId(2), NodeId(3)] {
        let pr = c.progress.get(&p).expect("a new leader has no progress for a voter");
        assert_eq!(pr.matched, 0, "a new leader claimed evidence about a peer's log that it does not have — quorum is counted over `matched`");
        assert_eq!(pr.silent, 0, "a new leader started a peer already silent, shortening its own lease");
        assert!(!pr.needs_snapshot);
    }
}

#[test]
fn a_single_voter_configuration_elects_itself_without_asking_anybody() {
    let mut c = node(1, Config::new([NodeId(1)], 1, 1), 3);
    let out = tick_until(&mut c, 100, |c| c.role() == Role::Leader);
    assert_eq!(c.role(), Role::Leader);
    assert_eq!(c.term(), 1, "a one-node cluster took more than one term to elect itself");
    assert!(sends(&out).is_empty(), "a node that is its own majority asked somebody for a vote");
    assert!(
        out.iter().any(|a| matches!(a, Action::Persist { .. })),
        "a one-node leader skipped its term-establishing entry"
    );
}

#[test]
fn a_node_absent_from_its_own_configuration_never_campaigns() {
    let mut c = node(4, cfg3(), 5);
    let out = tick_n(&mut c, 500);
    assert_eq!(c.role(), Role::Follower, "a node that has been removed from the cluster stood for election in it");
    assert_eq!(c.term(), 0);
    assert!(sends(&out).is_empty());
}

// ---------------------------------------------------------------- timeouts

#[test]
fn every_campaign_redraws_its_timeout_inside_the_documented_window() {
    // Randomized per campaign so two nodes do not campaign in lockstep for ever, which is a split
    // vote that repeats.
    let mut c = node(1, cfg3(), 12345);
    let mut seen = BTreeSet::new();
    for _ in 0..60 {
        campaign(&mut c);
        assert!(
            c.election_timeout >= c.election_base && c.election_timeout < 2 * c.election_base,
            "a campaign drew {} outside [{}, {})",
            c.election_timeout,
            c.election_base,
            2 * c.election_base
        );
        seen.insert(c.election_timeout);
    }
    assert!(seen.len() > 1, "every campaign drew the same timeout, so two nodes campaign in lockstep for ever: {seen:?}");
}

#[test]
fn a_campaign_that_wins_nothing_is_retried_from_the_pre_vote_and_not_from_a_raised_term() {
    let mut c = node(1, cfg3(), 3);
    campaign(&mut c);
    let t0 = c.term();
    // Enter the term for real, then get no votes at all.
    step_checked(&mut c, msg(2, 1, t0 + 1, Body::PreVoteResp { granted: true }));
    assert_eq!(c.role(), Role::Candidate);
    let t1 = c.term();

    let out = campaign(&mut c);
    assert_eq!(c.role(), Role::PreCandidate, "a stalled campaign restarted as a real one");
    assert_eq!(c.term(), t1, "restarting a stalled campaign raised the term again without asking anybody first");
    assert!(!bodies_of(&out, is_prevote).is_empty(), "the retry asked nobody");
}

#[test]
fn a_campaign_in_flight_is_abandoned_when_this_node_learns_it_is_stale() {
    let mut c = node(1, cfg3(), 3);
    campaign(&mut c);
    assert_eq!(c.role(), Role::PreCandidate);

    c.observe_config_at(CfgAt { version: 9, term: 3 });
    let out = tick_until(&mut c, 500, |c| c.role() == Role::Follower);
    assert_eq!(c.role(), Role::Follower, "a node that has just learned its configuration is stale kept asking for votes");
    assert!(bodies_of(&out, is_prevote).is_empty());
}

// ---------------------------------------------------------------- pre-vote, end to end

#[test]
fn a_node_campaigning_into_a_wall_never_raises_the_clusters_term() {
    // The claim pre-vote exists to make, asserted end to end rather than as its pieces: a node on
    // the wrong side of a partition may retry for ever and the healthy cluster never notices.
    //
    // The partition is deliberately ONE-WAY — the lonely node's asks get through and only the
    // answers are lost to it in the sense that they cannot help it — because a symmetric partition
    // would prove nothing: a node whose messages never arrive obviously disturbs nobody.
    let mut leader = node(1, cfg3(), 3);
    win_election(&mut leader, &[2, 3]);
    let term = leader.term();

    let mut follower = node(2, cfg3(), 6);
    // What `become_follower(term, Some(leader))` leaves behind when `replicate.rs` accepts an
    // `Append`: this node is being served, and knows by whom.
    follower.hard.term = term;
    follower.leader = Some(NodeId(1));
    follower.since_heard = 0;

    let mut lonely = node(3, cfg3(), 77);
    lonely.hard.term = term;

    let mut refusals = 0usize;
    let mut asks = 0usize;
    for _ in 0..500 {
        let mut outbound = Vec::new();
        for a in step_checked(&mut lonely, Event::Tick) {
            if let Action::Send(m) = a {
                outbound.push(m);
            }
        }
        for m in outbound {
            if !matches!(m.body, Body::PreVote { .. } | Body::RequestVote { .. }) {
                continue;
            }
            asks += 1;
            let target: &mut Consensus = if m.to == NodeId(1) { &mut leader } else { &mut follower };
            for a in step_checked(target, Event::Recv(m)) {
                if let Action::Send(r) = a {
                    if matches!(
                        r.body,
                        Body::PreVoteResp { granted: false } | Body::RequestVoteResp { granted: false }
                    ) {
                        refusals += 1;
                    }
                    step_checked(&mut lonely, Event::Recv(r));
                }
            }
        }
        // The healthy side carries on: the leader keeps hearing from node 2, and node 2 keeps
        // hearing from the leader.
        leader.progress.get_mut(&NodeId(2)).expect("no progress for a voter").silent = 0;
        step_checked(&mut leader, Event::Tick);
        step_checked(&mut follower, Event::Tick);
        follower.since_heard = 0;
    }

    assert!(asks > 20, "the lonely node only campaigned {asks} times, so this test measured almost nothing");
    assert!(refusals > 20, "the wall never refused anything: {refusals} refusals in {asks} asks");
    assert_eq!(lonely.term(), term, "a node campaigning into a wall raised its own term");
    assert_ne!(lonely.role(), Role::Candidate, "a node campaigning into a wall entered a term for real");
    assert_eq!(leader.role(), Role::Leader, "a partitioned peer deposed a leader that never stopped working");
    assert_eq!(leader.term(), term, "a partitioned peer forced a term change on a healthy cluster");
    assert_eq!(follower.term(), term, "a partitioned peer forced a term change on a healthy follower");
    assert_eq!(follower.leader(), Some(NodeId(1)), "a healthy follower lost its leader to a partitioned peer");
}

// ---------------------------------------------------------------- replayability

#[test]
fn a_campaign_replays_identically_from_a_seed_and_its_windows_are_not_all_equal() {
    // `sim.rs` (F8) is worth nothing unless a failure replays from its seed, and it can only replay
    // if nothing in here reads a clock or a thread-seeded PRNG.
    let run = |seed: u64| {
        let mut c = node(1, cfg3(), seed);
        (0..400).map(|_| format!("{:?}", c.step(Event::Tick))).collect::<Vec<_>>()
    };
    assert_eq!(run(99), run(99), "the same seed produced a different campaign, so no failure is replayable");
    let traces: Vec<Vec<String>> = (1..=8u64).map(run).collect();
    assert!(
        traces.iter().any(|t| *t != traces[0]),
        "eight seeds produced one run, so a whole fleet campaigns in lockstep from a cold start"
    );

    // And one node's windows must keep differing, not merely start differently. `Consensus::new`
    // draws the first timeout in `mod.rs` from the seed, so a check that only reached the first
    // campaign passes even when `draw_timeout` returns a constant — it did, and mutant M27 is what
    // found it. Measured through `step` rather than by reading the field, because the tick a
    // campaign actually begins on is the thing a peer races against.
    let mut c = node(1, cfg3(), 4242);
    let mut starts: Vec<u32> = Vec::new();
    for t in 0..600u32 {
        let began = step_checked(&mut c, Event::Tick)
            .iter()
            .any(|a| matches!(a, Action::RoleChanged { role: Role::PreCandidate, .. }));
        if began {
            starts.push(t);
        }
    }
    assert!(starts.len() > 8, "only {} campaigns in 600 ticks, so the gaps below mean little", starts.len());
    let gaps: BTreeSet<u32> = starts.windows(2).map(|w| w[1] - w[0]).collect();
    assert!(
        gaps.len() > 1,
        "every campaign waited exactly the same number of ticks ({gaps:?}), so two nodes campaign in \
         lockstep for ever, which is a split vote that repeats"
    );
}
