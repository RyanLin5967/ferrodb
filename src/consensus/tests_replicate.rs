//! F2 — the replication rules, each named after the rule it pins and the loss it prevents.
//!
//! Every test here was run against a deliberately broken copy of the rule it names before it was
//! believed; the mutants and what each printed are in `scratchpad/F2-replicate.md`. A test that has
//! not been seen to fail is not evidence.
//!
//! Nothing here delivers `Event::Tick` or a vote message: `on_tick` and `on_vote_msg` belong to F1
//! and are `unimplemented!()` in this tree. Where a node has to *be* a leader, the test sets the
//! office directly — the state F1 would have set — and then goes through this file's own
//! [`Consensus::init_leader_progress`] for everything replication-shaped.

use std::collections::VecDeque;

use super::*;
use crate::consensus::Event;

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);
const N4: NodeId = NodeId(4);
const N5: NodeId = NodeId(5);

fn cfg3() -> Config {
    Config::new([N1, N2, N3], 1, 1)
}

fn cfg5() -> Config {
    Config::new([N1, N2, N3, N4, N5], 1, 1)
}

/// A redo batch whose bytes are a function of `mark`, so two commands are equal exactly when their
/// marks are.
fn wal(mark: u8) -> Command {
    Command::WalBatch { start_lsn: 100 + mark as u64, bytes: vec![mark; 16] }
}

// -------------------------------------------------------------------------------------------
// Harness. These build states F1 would build; they never compute an expected value.
// -------------------------------------------------------------------------------------------

/// Put `entries` into a node's log as rounds 1..n, as a crash-recovered node would hold them, and
/// mark them all durable. Goes through `append_own_entry` so the tail and the scalars that
/// describe it cannot part company.
fn seed(c: &mut Consensus, entries: &[(Term, Command)]) {
    let restore = c.hard.term;
    let mut sink = Vec::new();
    for (t, cmd) in entries {
        c.hard.term = *t;
        c.append_own_entry(cmd.clone(), &mut sink);
    }
    c.hard.term = restore.max(c.last_term);
    c.durable = c.last_round;
}

/// The office F1's election would confer, without F1's code. No `NoOp` — the tests that care about
/// round numbering are ports of a paper figure, and an extra entry would renumber it.
fn promote_bare(c: &mut Consensus, term: Term) {
    c.hard.term = term;
    c.hard.voted_for = Some(c.id());
    c.role = Role::Leader;
    c.leader = Some(c.id());
    c.votes.clear();
    c.campaign = None;
    c.init_leader_progress();
}

fn follower_of(c: &mut Consensus, term: Term, leader: NodeId) {
    c.hard.term = term;
    c.role = Role::Follower;
    c.leader = Some(leader);
}

fn append_msg(
    from: NodeId,
    to: NodeId,
    term: Term,
    prev_round: Round,
    prev_term: Term,
    entries: Vec<Entry>,
    commit: Round,
) -> Message {
    Message { from, to, term, body: Body::Append { prev_round, prev_term, entries, commit } }
}

fn resp_msg(
    from: NodeId,
    to: NodeId,
    term: Term,
    success: bool,
    matched: Round,
    hint: Round,
    digest: u64,
) -> Message {
    Message { from, to, term, body: Body::AppendResp { success, matched, hint, digest } }
}

fn sends(out: &[Action]) -> Vec<Message> {
    out.iter()
        .filter_map(|a| match a {
            Action::Send(m) => Some(m.clone()),
            _ => None,
        })
        .collect()
}

fn only_send(out: &[Action]) -> Message {
    let s = sends(out);
    assert_eq!(s.len(), 1, "expected exactly one message, got {s:#?}");
    s.into_iter().next().unwrap()
}

fn resp_of(m: &Message) -> (bool, Round, Round, u64) {
    match &m.body {
        Body::AppendResp { success, matched, hint, digest } => (*success, *matched, *hint, *digest),
        other => panic!("expected an AppendResp, got {other:?}"),
    }
}

fn truncations(out: &[Action]) -> Vec<Round> {
    out.iter()
        .filter_map(|a| match a {
            Action::Truncate { from } => Some(*from),
            _ => None,
        })
        .collect()
}

fn applies(out: &[Action]) -> Vec<Round> {
    out.iter()
        .filter_map(|a| match a {
            Action::Apply { through } => Some(*through),
            _ => None,
        })
        .collect()
}

fn persisted_entries(out: &[Action]) -> Vec<Entry> {
    out.iter()
        .filter_map(|a| match a {
            Action::Persist { entries } => Some(entries.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// Pump `Append`/`AppendResp` between one leader and one follower until the exchange settles,
/// fulfilling every `Persist` immediately so the acknowledgement rule 4 defers to
/// `Event::Persisted` actually arrives. Returns how many messages it took, which is what makes
/// "backs up in one step" measurable rather than asserted.
fn catch_up(leader: &mut Consensus, follower: &mut Consensus, budget: usize) -> usize {
    let lid = leader.id();
    let mut queue: VecDeque<Message> = VecDeque::new();
    let mut out = Vec::new();
    leader.send_append_to(follower.id(), &mut out);
    queue.extend(sends(&out));

    let mut msgs = 0usize;
    while let Some(m) = queue.pop_front() {
        msgs += 1;
        assert!(
            msgs <= budget,
            "the exchange took more than {budget} messages, which is a backwards probe or a \
             live-lock and not a one-step back-up"
        );
        let node: &mut Consensus = if m.to == lid { leader } else { follower };
        let acts = node.step(Event::Recv(m));
        queue.extend(sends(&acts));
        for a in &acts {
            if let Action::Persist { entries } = a {
                let round = entries.last().map(|e| e.round).unwrap_or(0);
                let term = node.term();
                let more = node.step(Event::Persisted { term, round });
                queue.extend(sends(&more));
            }
        }
    }
    msgs
}

/// A fault-free network of whole `Consensus` instances, used only where a rule is about what a
/// cluster converges to. Faults, seeds and replay are F8's row; this is the smallest thing that can
/// answer "does the healthy case stay healthy".
struct Net {
    nodes: Vec<Consensus>,
    wire: VecDeque<Message>,
}

impl Net {
    fn new(cfg: &Config, ids: &[NodeId]) -> Net {
        Net {
            nodes: ids.iter().map(|id| Consensus::new(*id, cfg.clone(), id.0 as u64 + 7)).collect(),
            wire: VecDeque::new(),
        }
    }

    fn at(&mut self, id: NodeId) -> &mut Consensus {
        let i = self.nodes.iter().position(|c| c.id() == id).expect("unknown node");
        &mut self.nodes[i]
    }

    fn get(&self, id: NodeId) -> &Consensus {
        self.nodes.iter().find(|c| c.id() == id).expect("unknown node")
    }

    /// Perform a node's actions: queue its sends, and fulfil every `Persist` immediately — a
    /// synchronous fsync, which is the only durability model under which a fault-free run is
    /// allowed to be boring.
    fn absorb(&mut self, id: NodeId, actions: Vec<Action>) {
        let mut q: VecDeque<Action> = actions.into();
        while let Some(a) = q.pop_front() {
            match a {
                Action::Send(m) => self.wire.push_back(m),
                Action::Persist { entries } => {
                    let round = entries.last().map(|e| e.round).unwrap_or(0);
                    let term = self.at(id).term();
                    for x in self.at(id).step(Event::Persisted { term, round }) {
                        q.push_back(x);
                    }
                }
                _ => {}
            }
        }
    }

    fn settle(&mut self) {
        let mut guard = 0u32;
        while let Some(m) = self.wire.pop_front() {
            guard += 1;
            assert!(
                guard < 100_000,
                "a fault-free network did not settle: the hint/back-up exchange is a live-lock, \
                 which is exactly the shape a follower that keeps refusing the same Append makes"
            );
            let to = m.to;
            let out = self.at(to).step(Event::Recv(m));
            self.absorb(to, out);
        }
    }
}

// -------------------------------------------------------------------------------------------
// Rule 1 — quorum is counted over `matched`, never over `next`.
// -------------------------------------------------------------------------------------------

#[test]
fn quorum_is_counted_over_matched_and_never_over_next() {
    // The defect this pins: `next` is where the leader HOPES a peer is. Counting it commits a round
    // that lives on one disk, and the next leader — which is under no obligation to hold it —
    // silently drops work a client was told had committed.
    let mut c = Consensus::new(N1, cfg3(), 11);
    seed(&mut c, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    promote_bare(&mut c, 1);
    assert_eq!(c.durable, 3, "this leader holds all three rounds on its own disk");

    // Both peers have answered for round 1 and only round 1, so `matched` is 1 and `next` is 2.
    let first = c.step(Event::Recv(resp_msg(N2, N1, 1, true, 1, 0, 0)));
    let second = c.step(Event::Recv(resp_msg(N3, N1, 1, true, 1, 0, 0)));
    assert_eq!(c.progress[&N2].matched, 1);
    assert!(
        c.progress[&N2].next >= c.progress[&N2].matched + 1,
        "`next` must be at least one past `matched`"
    );
    assert_eq!(
        c.progress[&N2].next, 4,
        "a success answer must not pull `next` BACKWARDS. It is where to send from, and this \
         leader had already sent through round 3; regressing it on a stale or zero-matched ack \
         re-sends a log the peer already holds"
    );

    assert_eq!(
        c.commit_round(),
        1,
        "a majority holds round 1 and only round 1. Counting `next` instead of `matched` makes the \
         quorum-held round 2, which lives on this leader's disk alone and which the next leader is \
         under no obligation to hold"
    );
    let handed: Vec<Round> =
        applies(&first).into_iter().chain(applies(&second)).collect();
    assert_eq!(
        handed,
        vec![1],
        "the storage engine was handed a round no majority holds"
    );
}

#[test]
fn a_leader_does_not_count_itself_twice_when_counting_a_quorum() {
    // The defect this pins: a leader keeps a `Progress` entry for ITSELF, because that entry is
    // where this file keeps the node's own log. Counting both that entry and the node's own
    // `durable` makes one node look like two, so a 3-node cluster commits on the leader alone.
    let mut c = Consensus::new(N1, cfg3(), 12);
    seed(&mut c, &[(1, wal(1))]);
    promote_bare(&mut c, 1);
    let out = c.step(Event::Persisted { term: 1, round: 1 });

    assert_eq!(
        c.commit_round(),
        0,
        "one node out of three made round 1 durable and nothing else has answered, so nothing is \
         committed; a leader that counted itself twice would have committed it"
    );
    assert!(applies(&out).is_empty(), "an uncommitted round was handed to the storage engine");
}

#[test]
fn a_learner_is_replicated_to_but_never_counted_toward_a_quorum() {
    // The defect this pins: counting a node that holds none of the log enlarges the denominator
    // without enlarging the set that can answer, which REDUCES availability at the moment an
    // operator believes they are increasing it.
    let cfg = cfg3().with_learners([N4]);
    let mut c = Consensus::new(N1, cfg, 13);
    seed(&mut c, &[(1, wal(1))]);
    promote_bare(&mut c, 1);

    let mut out = Vec::new();
    c.bcast_append(&mut out);
    let to: Vec<NodeId> = sends(&out).iter().map(|m| m.to).collect();
    assert!(to.contains(&N4), "a learner must still be sent the log; it was not sent to");

    c.step(Event::Persisted { term: 1, round: 1 });
    c.step(Event::Recv(resp_msg(N4, N1, 1, true, 1, 0, 0)));
    assert_eq!(
        c.commit_round(),
        0,
        "the leader and a LEARNER hold round 1. A learner is not a voter, so that is one voter of \
         three and nothing is committed"
    );

    c.step(Event::Recv(resp_msg(N2, N1, 1, true, 1, 0, 0)));
    assert_eq!(c.commit_round(), 1, "two voters of three holds a round of the leader's own term");
}

#[test]
fn a_removed_peers_acknowledgement_is_not_counted() {
    // A removed node keeps running and keeps answering. Its ack must not count toward a set it has
    // left, and it must not resurrect its own `Progress` entry.
    let mut c = Consensus::new(N1, cfg3(), 14);
    seed(&mut c, &[(1, wal(1))]);
    promote_bare(&mut c, 1);
    c.step(Event::Persisted { term: 1, round: 1 });

    let out = c.step(Event::Recv(resp_msg(N5, N1, 1, true, 1, 0, 0)));
    assert!(out.is_empty(), "a node outside the configuration was answered or acted on");
    assert!(!c.progress.contains_key(&N5), "a removed node's progress entry was resurrected");
    assert_eq!(c.commit_round(), 0);
}

#[test]
fn an_acknowledgement_never_goes_backwards() {
    // `AckTracker::record`'s rule, moved above it: a durability promise that can be withdrawn after
    // the fact is not a promise.
    let mut c = Consensus::new(N1, cfg3(), 15);
    seed(&mut c, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    promote_bare(&mut c, 1);

    c.step(Event::Recv(resp_msg(N2, N1, 1, true, 3, 0, 0)));
    assert_eq!(c.progress[&N2].matched, 3);
    c.step(Event::Recv(resp_msg(N2, N1, 1, true, 1, 0, 0)));
    assert_eq!(
        c.progress[&N2].matched, 3,
        "a later, smaller acknowledgement retracted one this leader had already counted"
    );
}

// -------------------------------------------------------------------------------------------
// Rule 2 — Raft §5.4.2. The figure-8 scenario from the paper.
// -------------------------------------------------------------------------------------------

/// Five nodes at the state the paper reaches just before the fork: `n1` leads term 4 and has
/// replicated its INHERITED round 2 (term 2) to `n2` and `n3`, so a majority holds it. `n5` still
/// holds a different round 2, of term 3, and is still electable.
fn figure8_through_c() -> Vec<Consensus> {
    let e1 = (1u64, wal(0x11));
    let e2a = (2u64, wal(0xAA));
    let e2b = (3u64, wal(0xBB));

    let mut nodes: Vec<Consensus> =
        [N1, N2, N3, N4, N5].iter().map(|n| Consensus::new(*n, cfg5(), n.0 as u64)).collect();

    seed(&mut nodes[0], &[e1.clone(), e2a.clone()]);
    seed(&mut nodes[1], &[e1.clone()]);
    seed(&mut nodes[2], &[e1.clone()]);
    seed(&mut nodes[3], &[e1.clone()]);
    seed(&mut nodes[4], &[e1.clone(), e2b.clone()]);

    // n1 restarts and wins term 4. No NoOp: the paper's rounds are 1 and 2, and this test is about
    // what happens BEFORE a round of the new term exists.
    promote_bare(&mut nodes[0], 4);

    // n1 replicates its inherited round 2 to n2 and n3. Each refuses once (it holds only round 1,
    // so `prev_round: 2` is a gap), and the hint puts the leader at the right place in one step.
    for i in [1usize, 2] {
        let peer = nodes[i].id();
        let mut out = Vec::new();
        nodes[0].send_append_to(peer, &mut out);
        let probe = only_send(&out);
        assert!(
            matches!(&probe.body, Body::Append { prev_round: 2, .. }),
            "the leader's first probe is at its own tail: {:?}",
            probe.body
        );

        let refusal = nodes[i].step(Event::Recv(probe));
        let r = only_send(&refusal);
        assert_eq!(resp_of(&r), (false, 0, 2, 0), "a gap must be refused with a hint at own tail");

        let retry = nodes[0].step(Event::Recv(r));
        let carry = only_send(&retry);
        match &carry.body {
            Body::Append { prev_round, prev_term, entries, .. } => {
                assert_eq!((*prev_round, *prev_term), (1, 1));
                assert_eq!(entries.len(), 1);
                assert_eq!(
                    entries[0].term, 2,
                    "a re-sent inherited entry keeps the term of the leader that CREATED it; \
                     re-stamping it with the current term would make it committable by count"
                );
            }
            other => panic!("expected an Append, got {other:?}"),
        }

        let accept = nodes[i].step(Event::Recv(carry));
        assert!(sends(&accept).is_empty(), "a follower acked before it had fsynced");
        let ack = nodes[i].step(Event::Persisted { term: 4, round: 2 });
        nodes[0].step(Event::Recv(only_send(&ack)));
    }

    nodes
}

#[test]
fn a_leader_does_not_commit_an_inherited_round_by_counting_replicas() {
    // The defect this pins is Raft figure 8. Round 2 is held by n1, n2 and n3 -- a majority of five
    // -- and it is NOT committed, because its term (2) is not this leader's term (4). Committing it
    // here is the classic way to lose acknowledged data, and the next test shows exactly how.
    let nodes = figure8_through_c();
    let n1 = &nodes[0];

    assert_eq!(n1.progress[&N2].matched, 2);
    assert_eq!(n1.progress[&N3].matched, 2);
    assert_eq!(n1.durable, 2, "the leader itself holds round 2 durably");
    assert_eq!(
        n1.commit_round(),
        0,
        "round 2 is on three of five nodes and is of term 2, not the leader's term 4. A leader may \
         commit a round of its OWN term directly; an inherited round commits only as a side effect \
         of a later one"
    );
}

#[test]
fn the_uncommitted_inherited_round_is_still_overwritable_which_is_why_it_must_not_commit() {
    // The proof obligation behind the test above: "round 2 is not committed" is a weaker claim than
    // "round 2 is not SAFE to commit". This plays out the overwrite.
    let mut nodes = figure8_through_c();

    // n5 is still electable: its log tail is (term 3, round 2), which is at least as complete as
    // n2's and n3's (term 2, round 2) under the (last_term, last_round) comparison, so they would
    // grant. n5 + n2 + n3 + n4 is four of five.
    let n5_tail = (nodes[4].last_term, nodes[4].last_round);
    for i in [1usize, 2, 3] {
        assert!(
            n5_tail >= (nodes[i].last_term, nodes[i].last_round),
            "n5 {:?} could not out-vote n{} {:?}, so the overwrite below is not reachable",
            n5_tail,
            i + 1,
            (nodes[i].last_term, nodes[i].last_round)
        );
    }
    assert!(nodes[4].config().has_quorum(4));

    // n5 wins term 5 and replicates ITS round 2 (term 3) outward.
    promote_bare(&mut nodes[4], 5);
    for i in [1usize, 2] {
        let (head, tail) = nodes.split_at_mut(4);
        let msgs = catch_up(&mut tail[0], &mut head[i], 4);
        assert!(msgs <= 4, "probe, refusal, corrected append, ack: {msgs} messages");
        assert_eq!(
            nodes[i].term_at(2),
            Some(3),
            "n{}'s round 2 was not replaced: the entry a majority held in the previous test is \
             still there, so the overwrite this test exists to demonstrate did not happen",
            i + 1
        );
    }

    // If the previous test's leader had committed round 2 on the replica count, it would have
    // handed `wal(0xAA)` to the storage engine and the log now says `wal(0xBB)`.
    assert_ne!(
        nodes[1].entries_from(2).first().map(|e| e.command.clone()),
        Some(wal(0xAA)),
        "the entry a majority held has been overwritten -- committing it by count would have been \
         acknowledged data loss"
    );
}

#[test]
fn an_inherited_round_commits_as_a_side_effect_of_a_round_of_the_leaders_own_term() {
    // The other half, and it is not optional: without it, "commit == 0" above is satisfied by an
    // implementation that never commits anything at all.
    let mut nodes = figure8_through_c();

    let out = nodes[0].step(Event::Propose(wal(0x33)));
    let e3 = persisted_entries(&out);
    assert_eq!(e3.len(), 1);
    assert_eq!((e3[0].term, e3[0].round), (4, 3), "the leader's own round takes its own term");

    let carriers: Vec<Message> =
        sends(&out).into_iter().filter(|m| m.to == N2 || m.to == N3).collect();
    assert_eq!(carriers.len(), 2, "the proposal was not replicated to both reachable followers");

    let out = nodes[0].step(Event::Persisted { term: 4, round: 3 });
    assert_eq!(nodes[0].commit_round(), 0, "one of five is not a majority");
    assert!(applies(&out).is_empty());

    let mut committed_at = None;
    for (k, m) in carriers.into_iter().enumerate() {
        let i = if m.to == N2 { 1 } else { 2 };
        let accept = nodes[i].step(Event::Recv(m));
        assert!(sends(&accept).is_empty(), "a follower acked before it had fsynced");
        let ack = nodes[i].step(Event::Persisted { term: 4, round: 3 });
        let leader_out = nodes[0].step(Event::Recv(only_send(&ack)));
        if nodes[0].commit_round() > 0 && committed_at.is_none() {
            committed_at = Some((k, applies(&leader_out)));
        }
    }

    assert_eq!(
        nodes[0].commit_round(),
        3,
        "a round of the leader's own term reached a majority and did not commit"
    );
    let (k, applied) = committed_at.expect("the commit watermark never moved");
    assert_eq!(k, 1, "the commit landed on the wrong acknowledgement");
    assert_eq!(
        applied,
        vec![3],
        "the commit watermark must jump 0 -> 3 in one step. Walking it up a round at a time tells \
         the storage engine that round 2 committed on its own, which is the very thing §5.4.2 \
         forbids"
    );
}

#[test]
fn the_commit_term_check_reads_the_round_being_committed_not_the_log_tail() {
    // The mutant a log-less implementation writes by accident: `self.last_term == self.hard.term`
    // instead of `term_at(quorum_matched) == self.hard.term`. The two agree whenever the leader's
    // tail IS the quorum's tail, which is every ordinary test -- and disagree exactly in figure 8.
    let mut c = Consensus::new(N1, cfg5(), 21);
    seed(&mut c, &[(1, wal(1)), (2, wal(2))]);
    promote_bare(&mut c, 4);
    let mut sink = Vec::new();
    c.append_own_entry(wal(3), &mut sink); // round 3, term 4 -- the leader's tail
    c.durable = 3;
    c.progress.entry(N1).or_default().matched = 3;

    // Three of five hold round 2, and only this leader holds round 3.
    for peer in [N2, N3] {
        c.step(Event::Recv(resp_msg(peer, N1, 4, true, 2, 0, 0)));
    }

    assert_eq!(c.term_at(2), Some(2), "the round a majority holds is of term 2");
    assert_eq!(c.last_term, 4, "the log TAIL is of term 4");
    assert_eq!(
        c.commit_round(),
        0,
        "the quorum-matched round is 2 (term 2) and the tail is round 3 (term 4). Reading the \
         tail's term instead of the term of the round being committed commits an inherited round"
    );
}

// -------------------------------------------------------------------------------------------
// Rule 3 — matching, truncation, and a hint that backs the leader up in one step.
// -------------------------------------------------------------------------------------------

#[test]
fn a_conflicting_suffix_is_refused_with_a_hint_at_the_first_round_of_the_conflicting_term_run() {
    // The defect this pins: hinting `prev_round` (or `prev_round - 1`) is a linear probe -- one
    // round trip per diverged round, on every follower at once, after every ordinary leader change.
    let mut f = Consensus::new(N2, cfg3(), 31);
    seed(&mut f, &[(1, wal(1)), (2, wal(2)), (2, wal(3)), (2, wal(4))]);
    follower_of(&mut f, 2, N1);

    let out = f.step(Event::Recv(append_msg(N1, N2, 5, 4, 5, vec![], 0)));
    let r = only_send(&out);
    assert_eq!(
        resp_of(&r),
        (false, 0, 2, 0),
        "the follower holds rounds 2..4 in term 2 and the leader disagrees at round 4, so the \
         first round in question is 2. Hinting 4 or 3 would cost another round trip each"
    );
    assert!(
        truncations(&out).is_empty(),
        "a refusal must not truncate: the leader has not yet said what the follower should hold \
         instead, and an entry deleted on a guess is one no retry can bring back"
    );
}

#[test]
fn a_gap_is_refused_with_a_hint_at_the_followers_own_tail() {
    let mut f = Consensus::new(N2, cfg3(), 32);
    seed(&mut f, &[(1, wal(1)), (1, wal(2))]);
    follower_of(&mut f, 1, N1);

    let out = f.step(Event::Recv(append_msg(N1, N2, 1, 7, 1, vec![], 0)));
    assert_eq!(resp_of(&only_send(&out)), (false, 0, 3, 0));
}

#[test]
fn a_refusal_carries_no_claim_about_the_followers_log() {
    // `matched` and `digest` are positive claims. A refusal established no agreement, so both are
    // zero and the `hint` is the only information it carries -- the reading `mod.rs` already
    // applies on the stale-term path.
    let mut f = Consensus::new(N2, cfg3(), 33);
    seed(&mut f, &[(1, wal(1)), (1, wal(2))]);
    follower_of(&mut f, 1, N1);

    let out = f.step(Event::Recv(append_msg(N1, N2, 1, 9, 1, vec![], 0)));
    let (ok, matched, hint, digest) = resp_of(&only_send(&out));
    assert!(!ok);
    assert_eq!(matched, 0, "a refusal claimed progress the leader had not matched");
    assert_eq!(digest, 0, "a refusal claimed a digest over a prefix neither side agreed on");
    assert!(hint > 0);
}

#[test]
fn a_follower_truncates_its_conflicting_suffix_and_the_leader_recovers_in_one_extra_message() {
    let mut leader = Consensus::new(N1, cfg3(), 34);
    seed(&mut leader, &[(1, wal(1)), (5, wal(50)), (5, wal(51))]);
    promote_bare(&mut leader, 5);

    let mut f = Consensus::new(N2, cfg3(), 35);
    seed(&mut f, &[(1, wal(1)), (2, wal(2)), (2, wal(3)), (2, wal(4))]);
    follower_of(&mut f, 2, N1);

    let mut msgs = 0usize;

    let mut out = Vec::new();
    leader.send_append_to(N2, &mut out);
    let mut m = only_send(&out);
    msgs += 1;

    let reply = f.step(Event::Recv(m));
    m = only_send(&reply);
    msgs += 1;
    assert_eq!(resp_of(&m), (false, 0, 2, 0));

    let retry = leader.step(Event::Recv(m));
    m = only_send(&retry);
    msgs += 1;
    let accept = f.step(Event::Recv(m));

    assert_eq!(
        msgs, 3,
        "probe, refusal, corrected append: three messages. A backwards probe costs two more per \
         diverged round"
    );
    assert_eq!(truncations(&accept), vec![2], "the conflicting suffix was not truncated");
    let persisted = persisted_entries(&accept);
    assert_eq!(persisted.len(), 2);
    assert_eq!((persisted[0].round, persisted[0].term), (2, 5));
    assert_eq!((f.last_round, f.last_term), (3, 5));
    assert_eq!(f.term_at(2), Some(5), "the follower kept an entry the leader replaced");
    assert_eq!(f.term_at(4), None, "the truncated suffix is still readable");
}

#[test]
fn a_truncation_retracts_the_durability_it_was_about() {
    // A round that has been truncated away cannot still be acknowledged: `durable` is a claim about
    // entries, and those entries are gone.
    let mut f = Consensus::new(N2, cfg3(), 36);
    seed(&mut f, &[(1, wal(1)), (2, wal(2)), (2, wal(3))]);
    follower_of(&mut f, 2, N1);
    assert_eq!(f.durable, 3);

    let replacement = Entry { term: 5, round: 2, command: wal(0x77) };
    let out = f.step(Event::Recv(append_msg(N1, N2, 5, 1, 1, vec![replacement], 0)));
    assert_eq!(truncations(&out), vec![2]);
    assert_eq!(
        f.durable, 1,
        "the follower still claimed rounds 2 and 3 durable after deleting them, so its next ack \
         would promise a leader entries that no longer exist"
    );
}

#[test]
fn a_follower_refuses_to_truncate_a_committed_round() {
    // A committed round is on a majority and every electable leader holds it, so a leader asking
    // for one to be replaced is not one this node can follow. Refusing is the only answer that does
    // not unmake acknowledged work.
    let mut f = Consensus::new(N2, cfg3(), 37);
    seed(&mut f, &[(1, wal(1)), (2, wal(2)), (2, wal(3))]);
    follower_of(&mut f, 2, N1);
    f.commit = 2;

    let bad = Entry { term: 9, round: 2, command: wal(0x99) };
    let out = f.step(Event::Recv(append_msg(N1, N2, 9, 1, 1, vec![bad], 0)));
    assert!(truncations(&out).is_empty(), "a COMMITTED round was truncated");
    let (ok, matched, hint, _) = resp_of(&only_send(&out));
    assert!(!ok, "an append that would unmake a committed round was accepted");
    assert_eq!(matched, 0);
    assert_eq!(
        hint, 3,
        "the refusal must still move the leader, to the first round genuinely in question. A hint \
         of 0 carries no information, so the leader would re-send the identical append for ever"
    );
    assert_eq!(f.term_at(2), Some(2), "the committed entry was replaced");
}

#[test]
fn a_duplicate_append_changes_nothing_and_does_not_truncate() {
    // F8 injects duplication and reordering. A re-delivered append overlaps entries the follower
    // already holds; treating the overlap as a conflict would truncate and re-fetch a correct log
    // on every duplicate.
    let mut f = Consensus::new(N2, cfg3(), 38);
    seed(&mut f, &[(1, wal(1))]);
    follower_of(&mut f, 1, N1);

    let e = vec![Entry { term: 1, round: 2, command: wal(2) }];
    let m = append_msg(N1, N2, 1, 1, 1, e, 0);

    let first = f.step(Event::Recv(m.clone()));
    assert_eq!(persisted_entries(&first).len(), 1);
    f.step(Event::Persisted { term: 1, round: 2 });
    let before = (f.last_round, f.last_term, f.digest_at(2));

    let second = f.step(Event::Recv(m));
    assert!(truncations(&second).is_empty(), "a duplicate append truncated a correct log");
    assert!(persisted_entries(&second).is_empty(), "a duplicate append was written twice");
    assert_eq!((f.last_round, f.last_term, f.digest_at(2)), before);
    let (ok, matched, ..) = resp_of(&only_send(&second));
    assert!(ok, "a duplicate append must be acknowledged, not refused");
    assert_eq!(matched, 2);
}

#[test]
fn a_stale_append_does_not_move_the_commit_watermark_backwards() {
    let mut f = Consensus::new(N2, cfg3(), 39);
    seed(&mut f, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    follower_of(&mut f, 1, N1);

    f.step(Event::Recv(append_msg(N1, N2, 1, 3, 1, vec![], 3)));
    assert_eq!(f.commit_round(), 3);
    f.step(Event::Recv(append_msg(N1, N2, 1, 3, 1, vec![], 1)));
    assert_eq!(f.commit_round(), 3, "a reordered older append retracted the commit watermark");
}

#[test]
fn a_follower_clamps_the_leaders_commit_to_the_rounds_it_actually_holds() {
    // The defect this pins: taking `commit` raw makes a follower report -- and try to apply -- a
    // round it has never seen. `min` with its own tail is what makes the watermark a fact about
    // this node.
    let mut f = Consensus::new(N2, cfg3(), 40);
    seed(&mut f, &[(1, wal(1)), (1, wal(2))]);
    follower_of(&mut f, 1, N1);

    let out = f.step(Event::Recv(append_msg(N1, N2, 1, 2, 1, vec![], 99)));
    assert_eq!(
        f.commit_round(),
        2,
        "the leader said 99 and this node holds 2; a follower cannot commit what it does not hold"
    );
    assert_eq!(applies(&out), vec![2]);
}

#[test]
fn a_prev_round_below_the_snapshot_floor_moves_the_leader_forward_not_backwards() {
    // The one hint that points UP. A round below the floor is gone, so there is nothing to check
    // and nothing to back up to; the leader must move to the first round this node can serve.
    let mut f = Consensus::new(N2, cfg3(), 41);
    f.snapshot_round = 10;
    f.snapshot_term = 3;
    f.last_round = 10;
    f.last_term = 3;
    f.durable = 10;
    follower_of(&mut f, 3, N1);

    let out = f.step(Event::Recv(append_msg(N1, N2, 3, 4, 2, vec![], 0)));
    assert_eq!(resp_of(&only_send(&out)), (false, 0, 11, 0));
}

#[test]
fn a_peer_whose_next_is_below_the_snapshot_floor_is_marked_for_a_snapshot_not_sent_entries() {
    let mut c = Consensus::new(N1, cfg3(), 42);
    c.snapshot_round = 10;
    c.snapshot_term = 3;
    c.last_round = 10;
    c.last_term = 3;
    c.durable = 10;
    promote_bare(&mut c, 3);
    let mut sink = Vec::new();
    c.append_own_entry(wal(1), &mut sink);

    c.progress.entry(N2).or_default().next = 5;
    let mut out = Vec::new();
    c.send_append_to(N2, &mut out);
    assert!(
        sends(&out).is_empty(),
        "entries were sent for a round this leader has checkpointed away, which is a hole and not \
         a catch-up"
    );
    assert!(c.progress[&N2].needs_snapshot, "the peer was not marked as needing state transfer");
}

#[test]
fn one_append_carries_a_bounded_number_of_entries() {
    // A peer at the other end does not get to choose this process's memory usage.
    let mut c = Consensus::new(N1, cfg3(), 43);
    let log: Vec<(Term, Command)> = (0..200u8).map(|i| (1u64, wal(i))).collect();
    seed(&mut c, &log);
    promote_bare(&mut c, 1);
    c.progress.entry(N2).or_default().next = 1;

    let mut out = Vec::new();
    c.send_append_to(N2, &mut out);
    match &only_send(&out).body {
        Body::Append { entries, .. } => assert_eq!(entries.len(), MAX_ENTRIES_PER_APPEND),
        other => panic!("expected an Append, got {other:?}"),
    }
}

// -------------------------------------------------------------------------------------------
// Rule 4 — fsync before ack.
// -------------------------------------------------------------------------------------------

#[test]
fn a_follower_acknowledges_on_persistence_and_never_on_receipt() {
    // The defect this pins: acking on receipt makes a quorum of volatile memory look like a quorum
    // of disks, so a correlated power loss -- a rack, a rolling deploy -- turns into acknowledged
    // data loss.
    let mut f = Consensus::new(N2, cfg3(), 51);
    follower_of(&mut f, 1, N1);

    let e = vec![Entry { term: 1, round: 1, command: wal(1) }];
    let out = f.step(Event::Recv(append_msg(N1, N2, 1, 0, 0, e, 0)));
    assert_eq!(persisted_entries(&out).len(), 1, "the entry was not handed to the disk at all");
    assert!(
        sends(&out).is_empty(),
        "the follower acknowledged a round on receipt. It has not fsynced it, so the leader is \
         counting a copy that a power cut removes"
    );

    let ack = f.step(Event::Persisted { term: 1, round: 1 });
    assert_eq!(resp_of(&only_send(&ack)).0, true);
    assert_eq!(resp_of(&only_send(&ack)).1, 1);
}

#[test]
fn a_heartbeat_that_carries_nothing_is_answered_immediately() {
    // Nothing to persist means no `Persisted` will arrive, so an ack that waited for one would
    // never be sent -- and a healthy follower would starve its own leader's lease, causing the
    // election the lease exists to prevent.
    let mut f = Consensus::new(N2, cfg3(), 52);
    seed(&mut f, &[(1, wal(1))]);
    follower_of(&mut f, 1, N1);

    let out = f.step(Event::Recv(append_msg(N1, N2, 1, 1, 1, vec![], 0)));
    assert!(persisted_entries(&out).is_empty());
    let (ok, matched, ..) = resp_of(&only_send(&out));
    assert!(ok);
    assert_eq!(matched, 1);
}

#[test]
fn a_leader_counts_its_own_replica_only_once_it_is_durable() {
    let mut c = Consensus::new(N1, cfg3(), 53);
    promote_bare(&mut c, 1);
    c.step(Event::Recv(resp_msg(N2, N1, 1, true, 0, 0, 0)));

    c.step(Event::Propose(wal(1)));
    assert_eq!(c.progress[&N1].matched, 0, "the leader counted a round it had not fsynced");
    c.step(Event::Recv(resp_msg(N2, N1, 1, true, 1, 0, 0)));
    assert_eq!(
        c.commit_round(),
        0,
        "one follower and a leader that has not fsynced is one copy, not two"
    );

    c.step(Event::Persisted { term: 1, round: 1 });
    assert_eq!(c.commit_round(), 1);
}

#[test]
fn work_is_applied_only_through_the_round_that_is_both_committed_and_durable_here() {
    // A round is committed because a QUORUM holds it, which does not mean this node does.
    let mut f = Consensus::new(N2, cfg3(), 54);
    follower_of(&mut f, 1, N1);

    let e: Vec<Entry> =
        (1..=3u64).map(|r| Entry { term: 1, round: r, command: wal(r as u8) }).collect();
    let out = f.step(Event::Recv(append_msg(N1, N2, 1, 0, 0, e, 3)));
    assert_eq!(f.commit_round(), 3);
    assert!(
        applies(&out).is_empty(),
        "the storage engine was handed work whose log record this node has not fsynced"
    );

    let out = f.step(Event::Persisted { term: 1, round: 2 });
    assert_eq!(applies(&out), vec![2]);
    let out = f.step(Event::Persisted { term: 1, round: 3 });
    assert_eq!(applies(&out), vec![3]);
}

// -------------------------------------------------------------------------------------------
// The divergence detector.
// -------------------------------------------------------------------------------------------

#[test]
fn the_divergence_detector_fires_on_a_flipped_byte_in_a_followers_redo_stream() {
    // `DISTRIBUTED.md` §F0 requires this to be forced to fire. A follower whose WAL bytes differ
    // digests differently at that round, and a leader that counted it anyway would report a round
    // as replicated onto bytes nobody can name.
    let mut leader = Consensus::new(N1, cfg3(), 61);
    seed(&mut leader, &[(1, wal(0xAA))]);
    promote_bare(&mut leader, 1);

    // The follower's honest digest over ONE FLIPPED BYTE, built by an independent node rather than
    // read off the leader.
    let mut bad = Consensus::new(N2, cfg3(), 62);
    let mut flipped = wal(0xAA);
    if let Command::WalBatch { bytes, .. } = &mut flipped {
        bytes[3] ^= 0x01;
    }
    seed(&mut bad, &[(1, flipped)]);
    let bad_digest = bad.digest_at(1).expect("the follower holds round 1");
    assert_ne!(bad_digest, 0);

    leader.step(Event::Recv(resp_msg(N2, N1, 1, true, 1, 0, bad_digest)));

    let why = leader.progress[&N2].diverged.clone().expect("the detector did not fire");
    assert!(why.contains("round 1"), "the latch does not name the round: {why}");
    assert!(why.contains("DIVERGED"), "the latch does not name what happened: {why}");

    leader.step(Event::Persisted { term: 1, round: 1 });
    assert_eq!(
        leader.commit_round(),
        0,
        "a diverged peer was counted toward a quorum: the leader and it are two of three, so \
         without the latch round 1 would have committed"
    );

    // Latched: a later, agreeing ack does not clear it.
    let good = leader.digest_at(1).unwrap();
    leader.step(Event::Recv(resp_msg(N2, N1, 1, true, 1, 0, good)));
    assert!(
        leader.progress[&N2].diverged.is_some(),
        "the latch cleared itself, so an operator reading `it caught up again` would take a \
         divergence for a transient"
    );
}

#[test]
fn the_divergence_detector_stays_quiet_on_a_healthy_cluster() {
    // A detector that has never been shown to stay quiet is a detector nobody can act on.
    let mut net = Net::new(&cfg3(), &[N1, N2, N3]);
    {
        let c = net.at(N1);
        c.hard.term = 1;
        c.role = Role::Leader;
        c.leader = Some(N1);
    }
    let mut out = Vec::new();
    net.at(N1).on_became_leader(&mut out);
    net.absorb(N1, out);
    net.settle();

    for i in 0..200u8 {
        let out = net.at(N1).step(Event::Propose(wal(i)));
        net.absorb(N1, out);
        net.settle();
    }

    for peer in [N2, N3] {
        assert!(
            net.get(N1).progress[&peer].diverged.is_none(),
            "the detector fired on a healthy cluster: {:?}",
            net.get(N1).progress[&peer].diverged
        );
    }
    assert_eq!(net.get(N1).commit_round(), 201, "200 proposals plus the term-establishing NoOp");
    for id in [N1, N2, N3] {
        assert_eq!(net.get(id).last_round, 201, "n{} did not converge", id.0);
        assert_eq!(net.get(id).applied, 201, "n{} did not apply everything committed", id.0);
        assert_eq!(net.get(id).digest_at(201), net.get(N1).digest_at(201), "logs disagree");
    }
}

// -------------------------------------------------------------------------------------------
// The digest itself.
// -------------------------------------------------------------------------------------------

#[test]
fn the_hash_matches_the_published_fnv1a_64_vectors() {
    // Pinned against the published FNV-1a-64 vectors, not against the crate's other copy: two
    // copies checked against each other can only prove they agree, including on a wrong answer.
    assert_eq!(fnv64_update(FNV_OFFSET, b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv64_update(FNV_OFFSET, b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(fnv64_update(FNV_OFFSET, b"b"), 0xaf63_df4c_8601_f1a5);
    assert_eq!(fnv64_update(FNV_OFFSET, b"foobar"), 0x8594_4171_f739_67e8);
    // Resumable: folding in two pieces equals folding the concatenation.
    assert_eq!(
        fnv64_update(fnv64_update(FNV_OFFSET, b"foo"), b"bar"),
        fnv64_update(FNV_OFFSET, b"foobar")
    );
}

#[test]
fn a_variable_length_piece_is_length_prefixed_so_two_framings_cannot_agree() {
    // The defect this pins: without the prefix, ("ab", "c") and ("a", "bc") fold identically, so
    // two nodes that framed the same bytes differently would agree -- which is precisely the
    // divergence the digest exists to catch.
    assert_ne!(fold_bytes(fold_bytes(0, b"ab"), b"c"), fold_bytes(fold_bytes(0, b"a"), b"bc"));
}

#[test]
fn the_entry_digest_separates_a_command_from_the_round_and_term_that_carry_it() {
    // Log matching is a claim about `(term, round)` and not about the payload: the same command at
    // the same round under different terms is not the same log.
    let c = wal(7);
    let a = fold_entry(0, &Entry { term: 1, round: 1, command: c.clone() });
    let b = fold_entry(0, &Entry { term: 2, round: 1, command: c.clone() });
    let d = fold_entry(0, &Entry { term: 1, round: 2, command: c });
    assert_ne!(a, b, "two terms folded to one digest");
    assert_ne!(a, d, "two rounds folded to one digest");

    let e = fold_entry(0, &Entry { term: 1, round: 1, command: Command::NoOp });
    assert_ne!(a, e, "two commands folded to one digest");
}

#[test]
fn a_real_digest_is_never_the_value_reserved_for_no_claim() {
    let mut c = Consensus::new(N1, cfg3(), 71);
    seed(&mut c, &[(1, Command::NoOp), (1, wal(0)), (1, Command::Checkpoint)]);
    for r in 1..=3 {
        assert_ne!(c.digest_at(r), Some(0), "round {r} digested to the reserved `no claim` value");
    }
    assert_eq!(c.digest_at(0), Some(0), "the empty prefix claims nothing");
}

// -------------------------------------------------------------------------------------------
// The log's unusual home, and the detector that guards it.
// -------------------------------------------------------------------------------------------

#[test]
#[should_panic(expected = "was cleared by code that did not know")]
fn clearing_the_progress_map_destroys_this_nodes_log_and_says_so() {
    // The sharp edge of keeping the log in `progress[self_id]` (see the module header). A silent
    // loss would answer every `term_at` with `None` and refuse every append for ever, which reads
    // like a network fault.
    let mut c = Consensus::new(N1, cfg3(), 81);
    seed(&mut c, &[(1, wal(1)), (1, wal(2))]);
    promote_bare(&mut c, 1);
    c.progress.clear();
    c.step(Event::Persisted { term: 1, round: 2 });
}

#[test]
#[should_panic(expected = "no longer describes its own tail")]
fn moving_last_round_outside_the_append_path_is_refused_loudly() {
    let mut c = Consensus::new(N1, cfg3(), 82);
    seed(&mut c, &[(1, wal(1))]);
    promote_bare(&mut c, 1);
    c.last_round += 1;
    c.step(Event::Persisted { term: 1, round: 2 });
}

#[test]
fn initialising_leader_progress_preserves_the_log_and_the_divergence_latches() {
    // The sanctioned alternative to `progress.clear()`.
    let mut c = Consensus::new(N1, cfg3(), 83);
    seed(&mut c, &[(1, wal(1)), (1, wal(2))]);
    promote_bare(&mut c, 1);
    c.progress.entry(N2).or_default().diverged = Some("earlier".to_string());

    c.init_leader_progress();
    assert_eq!(c.term_at(2), Some(1), "the log did not survive re-initialisation");
    assert_eq!(c.progress[&N2].matched, 0, "per-peer progress was not reset");
    assert_eq!(c.progress[&N2].next, 3);
    assert!(
        c.progress[&N2].diverged.is_some(),
        "a divergence latch was cleared by an election. It is a latch precisely so that it is not"
    );
    assert_eq!(c.progress[&N1].matched, c.durable, "the leader's own replica is its own durability");
}

#[test]
fn a_new_leader_appends_a_term_establishing_noop_of_its_own_term() {
    // Required, not decorative: without a round of its own term a leader can never satisfy §5.4.2,
    // so it can never advance the commit watermark at all and its followers never learn what is
    // committed.
    let mut c = Consensus::new(N1, cfg3(), 84);
    seed(&mut c, &[(1, wal(1))]);
    c.hard.term = 4;
    c.role = Role::Leader;
    c.leader = Some(N1);

    let mut out = Vec::new();
    c.on_became_leader(&mut out);
    let e = persisted_entries(&out);
    assert_eq!(e.len(), 1);
    assert_eq!((e[0].term, e[0].round, e[0].command.clone()), (4, 2, Command::NoOp));
    assert_eq!(c.term_at(1), Some(1), "the inherited log did not survive the promotion");
    assert_eq!(sends(&out).len(), 2, "the new leader did not reach both peers");
}

// -------------------------------------------------------------------------------------------
// Proposals, snapshots, and the flags this path is the only observer of.
// -------------------------------------------------------------------------------------------

#[test]
fn a_proposal_on_a_follower_is_refused_and_names_the_leader_it_knows() {
    let mut f = Consensus::new(N2, cfg3(), 91);
    follower_of(&mut f, 3, N1);
    let out = f.step(Event::Propose(wal(1)));
    assert_eq!(
        out,
        vec![Action::Refuse { why: FerroError::NotLeader { leader: Some("n1".to_string()) } }],
        "a follower must refuse a write rather than drop it or serve it locally"
    );
    assert_eq!(f.last_round, 0, "a follower appended a proposal to its own log");

    let mut lost = Consensus::new(N3, cfg3(), 92);
    lost.hard.term = 3;
    let out = lost.step(Event::Propose(wal(1)));
    assert_eq!(out, vec![Action::Refuse { why: FerroError::NotLeader { leader: None } }]);
}

#[test]
fn a_proposal_on_a_leader_becomes_a_round_of_its_own_term_and_reaches_every_peer() {
    let mut c = Consensus::new(N1, cfg3(), 93);
    seed(&mut c, &[(1, wal(1))]);
    promote_bare(&mut c, 6);

    let out = c.step(Event::Propose(wal(2)));
    let e = persisted_entries(&out);
    assert_eq!((e[0].term, e[0].round), (6, 2));
    assert_eq!(sends(&out).len(), 2);
    assert_eq!(c.commit_round(), 0, "a proposal committed before anyone had it");
}

#[test]
fn an_install_snapshot_is_answered_rather_than_dropped_or_fatal() {
    // This was written against F6's stub, which answered every snapshot with `received_through: 0`.
    // **F6 has landed and this test still passes, for a different reason**, which is worth stating
    // rather than leaving a reader to infer: `SnapshotMeta::default()` names round 0, and round 0 is
    // "before the log begins" rather than a round, so `snapshot.rs` refuses it. What the test pins
    // is what it always pinned and what is still F2's business — a snapshot body is answered rather
    // than dropped, and never panics. The rules about which snapshots are accepted belong to
    // `tests_snapshot.rs`, which owns them.
    let mut f = Consensus::new(N2, cfg3(), 94);
    follower_of(&mut f, 1, N1);
    let m = Message {
        from: N1,
        to: N2,
        term: 1,
        body: Body::InstallSnapshot {
            meta: crate::consensus::snapshot::SnapshotMeta::default(),
            offset: 0,
            data: vec![1, 2, 3],
            done: true,
        },
    };
    let out = f.step(Event::Recv(m));
    match &only_send(&out).body {
        Body::InstallSnapshotResp { received_through } => assert_eq!(*received_through, 0),
        other => panic!("expected an InstallSnapshotResp, got {other:?}"),
    }
    assert_eq!(f.snapshot_round, 0, "a snapshot was installed by a row that does not own it");
}

#[test]
fn unjoined_clears_on_an_observable_watermark_and_never_on_a_timer() {
    // Cleared by comparing the leader's quorum watermark against this node's own store. A cluster
    // whose log is empty reports zero, so joining an empty cluster must still be able to stand.
    let mut joining = Consensus::joining(N4, 95);
    joining.cfg = cfg5();
    joining.behind = false;

    // A running cluster with real work: the watermark is above this node's store, so it stays out.
    let e = vec![Entry { term: 1, round: 1, command: wal(1) }];
    joining.step(Event::Recv(append_msg(N1, N4, 1, 0, 0, e, 5)));
    assert!(
        joining.unjoined,
        "a node holding one round of a log a quorum has five of stood for election"
    );

    // Once its own store reaches the watermark, the evidence is in.
    joining.step(Event::Persisted { term: 1, round: 1 });
    joining.step(Event::Recv(append_msg(N1, N4, 1, 1, 1, vec![], 1)));
    assert!(!joining.unjoined, "the node caught up with the quorum and still could not stand");

    // The empty cluster: a watermark of zero is met by a store of zero.
    let mut fresh = Consensus::joining(N5, 96);
    fresh.cfg = cfg5();
    fresh.behind = false;
    fresh.step(Event::Recv(append_msg(N1, N5, 1, 0, 0, vec![], 0)));
    assert!(
        !fresh.unjoined,
        "joining a cluster whose log is empty left a member that can never stand -- the exact \
         failure a timer-based rule was rejected for"
    );
}

#[test]
fn an_append_of_the_current_term_makes_a_candidate_follow_its_leader() {
    // Not a higher term: a candidate that kept campaigning through a term that already has a leader
    // is a split vote that repeats.
    let mut c = Consensus::new(N2, cfg3(), 97);
    c.hard.term = 4;
    c.role = Role::Candidate;
    c.leader = None;

    c.step(Event::Recv(append_msg(N1, N2, 4, 0, 0, vec![], 0)));
    assert_eq!(c.role(), Role::Follower);
    assert_eq!(c.leader(), Some(N1));
}

#[test]
fn a_steady_heartbeat_does_not_announce_a_role_change_on_every_beat() {
    let mut f = Consensus::new(N2, cfg3(), 98);
    follower_of(&mut f, 1, N1);
    f.step(Event::Recv(append_msg(N1, N2, 1, 0, 0, vec![], 0)));
    let out = f.step(Event::Recv(append_msg(N1, N2, 1, 0, 0, vec![], 0)));
    assert!(
        !out.iter().any(|a| matches!(a, Action::RoleChanged { .. })),
        "a caller that starts and stops serving on RoleChanged would thrash once per heartbeat"
    );
    assert_eq!(f.since_heard, 0, "a heartbeat did not reset the election clock");
}


// -------------------------------------------------------------------------------------------
// What an acknowledgement is allowed to claim. Every test below pins a defect an independent
// adversarial review found in the first version of this file, each of which was reachable from
// an ordinary leader change with no injected fault.
// -------------------------------------------------------------------------------------------

#[test]
fn a_follower_that_refused_an_append_does_not_acknowledge_it_afterwards() {
    // The defect this pins, and it was a real one: `Event::Persisted` emitted an acknowledgement
    // claiming the node's WHOLE durable log, with no regard for whether the append that log came
    // from was this leader's -- or was even accepted. A node that was a leader a moment ago holds
    // a durable tail its successor has never seen, and claiming it makes that successor count a
    // phantom replica, commit a round only it holds, and hand it to the storage engine.
    let mut a = Consensus::new(N1, cfg3(), 101);
    seed(&mut a, &[(1, wal(1)), (1, wal(2))]);
    a.commit = 2;
    a.applied = 2;
    promote_bare(&mut a, 3);
    let mut sink = Vec::new();
    a.append_own_entry(Command::NoOp, &mut sink); // round 3, term 3
    a.step(Event::Persisted { term: 3, round: 3 });

    // B led term 2 and appended rounds 3 and 4 of its own; the fsync is still in flight.
    let mut b = Consensus::new(N2, cfg3(), 102);
    seed(&mut b, &[(1, wal(1)), (1, wal(2)), (2, Command::NoOp), (2, wal(4))]);
    b.commit = 2;
    b.applied = 2;
    b.durable = 2;
    b.role = Role::Follower;
    b.leader = None;

    // A's heartbeat is refused: B holds round 3 at term 2, not term 3.
    let refusal = b.step(Event::Recv(append_msg(N1, N2, 3, 3, 3, vec![], 2)));
    assert!(!resp_of(&only_send(&refusal)).0, "the setup is wrong: B did not refuse");
    assert_eq!(b.last_round, 4, "B still holds its own tail; nothing was truncated");

    // Now B's term-2 fsync completes.
    let ack = b.step(Event::Persisted { term: 2, round: 4 });
    assert_eq!(b.durable, 4, "the rounds really are durable on B");
    let (ok, matched, _, digest) = resp_of(&only_send(&ack));
    assert!(ok);
    assert_eq!(
        matched, 0,
        "B acknowledged its own unreplicated tail to a leader that never sent it. Nothing has been \
         established with this leader, so the only truthful claim is zero"
    );
    assert_eq!(digest, 0, "a claim of nothing carries no digest");

    a.step(Event::Recv(resp_msg(N2, N1, 3, ok, matched, 0, digest)));
    assert_eq!(a.progress[&N2].matched, 0);
    assert_eq!(
        a.commit_round(),
        2,
        "round 3 lives on this leader alone and was reported committed on a phantom replica"
    );
}

#[test]
fn an_acknowledgement_claims_only_the_prefix_the_append_established() {
    // Raft's `matchIndex` is `prevLogIndex + len(entries)` -- what the append PROVED -- and not the
    // follower's log length. They differ exactly when the follower holds a tail from some other
    // leader.
    let mut f = Consensus::new(N2, cfg3(), 103);
    seed(&mut f, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    follower_of(&mut f, 1, N1);
    assert_eq!(f.durable, 3, "the follower really does hold three durable rounds");

    let out = f.step(Event::Recv(append_msg(N1, N2, 1, 1, 1, vec![], 0)));
    let (_, matched, ..) = resp_of(&only_send(&out));
    assert_eq!(
        matched, 1,
        "the append established agreement through round 1 and no further. Claiming 3 tells the \
         leader that rounds it has never sent are replicated"
    );
}

#[test]
fn an_acknowledgement_claims_durability_and_not_log_length() {
    // The other half of rule 4, which no test pinned before: an entry appended but not yet fsynced
    // is exactly the entry a power loss removes after it was counted into a quorum.
    let mut f = Consensus::new(N2, cfg3(), 104);
    follower_of(&mut f, 1, N1);
    let e: Vec<Entry> =
        (1..=3u64).map(|r| Entry { term: 1, round: r, command: wal(r as u8) }).collect();
    f.step(Event::Recv(append_msg(N1, N2, 1, 0, 0, e, 0)));
    assert_eq!(f.last_round, 3, "all three are in the log");

    f.step(Event::Persisted { term: 1, round: 2 });
    let out = f.step(Event::Recv(append_msg(N1, N2, 1, 3, 1, vec![], 0)));
    let (_, matched, ..) = resp_of(&only_send(&out));
    assert_eq!(
        matched, 2,
        "the append established agreement through round 3, but only rounds 1 and 2 are on the \
         disk. An acknowledgement is the smaller of the two"
    );
}

#[test]
fn a_leader_ignores_an_acknowledgement_of_a_round_it_does_not_hold() {
    // A peer cannot have matched a round this leader does not hold, and such an ack is the one
    // place the divergence detector CANNOT fire -- `digest_at` has nothing to compare.
    let mut c = Consensus::new(N1, cfg3(), 105);
    seed(&mut c, &[(1, wal(1)), (1, wal(2))]);
    promote_bare(&mut c, 1);
    c.step(Event::Persisted { term: 1, round: 2 });

    let out = c.step(Event::Recv(resp_msg(N2, N1, 1, true, 9, 0, 0x1234)));
    assert!(out.is_empty(), "the leader acted on a claim about rounds it does not have");
    assert_eq!(c.progress[&N2].matched, 0, "a round beyond this leader's own tail was counted");
    assert_eq!(c.commit_round(), 0);
}

#[test]
fn a_success_answer_never_pulls_next_backwards() {
    let mut c = Consensus::new(N1, cfg3(), 106);
    seed(&mut c, &[(1, wal(1)), (1, wal(2)), (1, wal(3))]);
    promote_bare(&mut c, 1);
    assert_eq!(c.progress[&N2].next, 4);

    c.step(Event::Recv(resp_msg(N2, N1, 1, true, 0, 0, 0)));
    assert_eq!(
        c.progress[&N2].next, 4,
        "a zero-matched success answer rewound the send cursor, so the leader re-sends a log the \
         peer may already hold"
    );
}

#[test]
fn a_follower_does_not_commit_a_suffix_this_leader_never_sent() {
    // `commit = min(leaderCommit, last NEW round)`, not `min(leaderCommit, own tail)`. The follower
    // below holds a round 3 from a leader of term 2 that this leader has never seen; taking the
    // watermark against its own tail applies that stale entry as though a quorum had agreed to it.
    let mut f = Consensus::new(N2, cfg3(), 107);
    seed(&mut f, &[(1, wal(1)), (1, wal(2)), (2, wal(0x33))]);
    follower_of(&mut f, 5, N1);

    let out = f.step(Event::Recv(append_msg(N1, N2, 5, 2, 1, vec![], 3)));
    assert_eq!(
        f.commit_round(),
        2,
        "the append established agreement through round 2. Round 3 is this follower's own stale \
         suffix and no quorum ever agreed to it"
    );
    assert_eq!(applies(&out), vec![2], "a round no quorum committed was handed to the engine");
}

#[test]
fn a_persisted_from_before_a_truncation_does_not_make_the_replacement_durable() {
    // A round number does not name an entry. After a truncate-and-refill a DIFFERENT entry sits at
    // that round, and the fsync now completing made the old one durable -- so taking it would ack,
    // and let a leader commit, bytes that are not on this node's disk.
    let mut f = Consensus::new(N2, cfg3(), 108);
    seed(&mut f, &[(2, wal(1))]);
    follower_of(&mut f, 2, N1);

    // n1's round 2 is appended; its fsync is in flight.
    f.step(Event::Recv(append_msg(N1, N2, 2, 1, 2, vec![Entry { term: 2, round: 2, command: wal(0xAA) }], 0)));
    assert_eq!(f.durable, 1);

    // n3 wins term 3 and replaces round 2 before that fsync lands.
    let out = f.step(Event::Recv(append_msg(N3, N2, 3, 1, 2, vec![Entry { term: 3, round: 2, command: wal(0xBB) }], 0)));
    assert_eq!(truncations(&out), vec![2]);
    assert_eq!(f.term_at(2), Some(3), "the log now holds n3's entry at round 2");

    // The stale report finally arrives. It is honest about n1's round 2 -- and n1's round 2 is gone.
    let stale = f.step(Event::Persisted { term: 2, round: 2 });
    assert_eq!(
        f.durable, 1,
        "a report from before the truncation was taken as durability for what replaced it"
    );
    assert_eq!(resp_of(&only_send(&stale)).1, 1, "and it must not be acknowledged either");

    // The refill's own report is trusted.
    let fresh = f.step(Event::Persisted { term: 3, round: 2 });
    assert_eq!(f.durable, 2);
    assert_eq!(resp_of(&only_send(&fresh)).1, 2);
}

#[test]
fn a_replayed_refusal_does_not_demand_a_snapshot_from_a_healthy_peer() {
    // F8 injects duplication deliberately. A refusal this leader has already acted on arrives a
    // second time carrying the same hint; answering it with a snapshot demand silences a healthy
    // peer permanently, because `send_append_to` then refuses to send it anything and only a
    // successful acknowledgement -- which can no longer arrive -- clears the flag.
    let mut c = Consensus::new(N1, cfg3(), 109);
    seed(&mut c, &[(1, wal(1)), (1, wal(2)), (1, wal(3)), (1, wal(4)), (1, wal(5))]);
    promote_bare(&mut c, 1);
    assert_eq!(c.progress[&N2].next, 6);

    let first = c.step(Event::Recv(resp_msg(N2, N1, 1, false, 0, 3, 0)));
    assert_eq!(c.progress[&N2].next, 3);
    assert_eq!(sends(&first).len(), 1, "the leader must back up and re-send once");

    let replay = c.step(Event::Recv(resp_msg(N2, N1, 1, false, 0, 3, 0)));
    assert!(replay.is_empty(), "a replayed refusal was acted on a second time");
    assert!(
        !c.progress[&N2].needs_snapshot,
        "a healthy peer was marked as needing state transfer because a refusal arrived twice"
    );
    assert_eq!(c.progress[&N2].next, 3);
}

#[test]
fn a_malformed_entry_list_is_refused_rather_than_aborting_the_process() {
    // F7's own header says it proves the sender holds the key and NOT that the message is new, so a
    // replayed or spliced frame reaches this code. An unchecked hole used to reach the log's
    // contiguity assertion, so one message could abort any node in the cluster -- and the panic
    // text blamed the local log.
    let cases: Vec<(&str, Vec<Entry>)> = vec![
        ("a batch that starts above prev_round + 1", vec![Entry { term: 1, round: 5, command: Command::NoOp }]),
        (
            "a hole inside the batch",
            vec![
                Entry { term: 1, round: 1, command: Command::NoOp },
                Entry { term: 1, round: 3, command: Command::NoOp },
            ],
        ),
        ("an entry claiming a term above the envelope", vec![Entry { term: 9, round: 1, command: Command::NoOp }]),
    ];
    for (what, entries) in cases {
        let mut f = Consensus::new(N2, cfg3(), 110);
        follower_of(&mut f, 1, N1);
        let out = f.step(Event::Recv(append_msg(N1, N2, 1, 0, 0, entries, 0)));
        let (ok, matched, hint, digest) = resp_of(&only_send(&out));
        assert!(!ok, "{what} was accepted");
        assert_eq!((matched, hint, digest), (0, 0, 0), "{what}: a malformed message is no evidence");
        assert_eq!(f.last_round, 0, "{what} reached the log");
        assert!(persisted_entries(&out).is_empty(), "{what} was handed to the disk");
    }
}

#[test]
fn a_divergence_deeper_than_one_batch_converges_without_a_livelock() {
    // The fork is 200 rounds below the leader's tail and the batch cap is 64, so the repair takes
    // several round trips. The first version of this file oscillated here for ever: the follower
    // acknowledged its whole durable log, which put `next` straight back above the fork.
    let mut leader = Consensus::new(N1, cfg3(), 111);
    let mut log: Vec<(Term, Command)> = (0..100u8).map(|i| (1u64, wal(i))).collect();
    log.extend((0..200u8).map(|i| (3u64, wal(i))));
    seed(&mut leader, &log);
    promote_bare(&mut leader, 3);

    let mut f = Consensus::new(N2, cfg3(), 112);
    let f_log: Vec<(Term, Command)> = (0..200u8).map(|i| (1u64, wal(i))).collect();
    seed(&mut f, &f_log);
    follower_of(&mut f, 1, N1);

    assert_eq!((leader.last_round, f.last_round), (300, 200));
    let msgs = catch_up(&mut leader, &mut f, 60);

    assert_eq!(f.last_round, 300, "the follower did not converge in {msgs} messages");
    assert_eq!(f.digest_at(300), leader.digest_at(300), "the logs converged to different bytes");
    assert!(!leader.progress[&N2].needs_snapshot, "a repairable peer was sent to F6");
    assert_eq!(leader.progress[&N2].matched, 300);
    assert!(msgs < 60, "{msgs} messages for a 200-round repair at 64 entries a batch");
}

#[test]
fn a_membership_entry_in_the_log_tells_this_node_its_configuration_is_stale() {
    // The second seam into `election.rs`. Observing a newer configuration is not applying it --
    // applying happens when the round commits, which is F5's row -- but a node that has SEEN one
    // knows it is stale and must not campaign, or it fences a healthy leader out of office.
    let mut f = Consensus::new(N2, cfg3(), 113);
    follower_of(&mut f, 1, N1);
    assert!(!f.behind);

    let newer = Config::new([N1, N2, N3, N4], 2, 1);
    let e = vec![Entry { term: 1, round: 1, command: Command::Membership { config: newer } }];
    f.step(Event::Recv(append_msg(N1, N2, 1, 0, 0, e, 0)));
    assert!(
        f.behind,
        "this node replicated a configuration newer than its own and still believed it could \
         count a majority"
    );
    assert_eq!(f.config().version, 1, "observing a configuration must not apply it");
}

// -------------------------------------------------------------------------------------------
// The digest, continued.
// -------------------------------------------------------------------------------------------

#[test]
fn the_digest_is_chained_so_a_change_at_an_early_round_moves_every_later_one() {
    // The whole point of a ROLLING digest: divergence at round 1 must still be visible at round
    // 300, because that is the only round the leader and follower are comparing.
    let mut a = Consensus::new(N1, cfg3(), 114);
    let mut b = Consensus::new(N2, cfg3(), 115);
    seed(&mut a, &[(1, wal(0xAA)), (1, wal(2)), (1, wal(3))]);
    seed(&mut b, &[(1, wal(0xBB)), (1, wal(2)), (1, wal(3))]);

    assert_ne!(a.digest_at(1), b.digest_at(1), "round 1 differs and must digest differently");
    assert_ne!(
        a.digest_at(3),
        b.digest_at(3),
        "rounds 2 and 3 are identical on both nodes, so an unchained digest agrees at round 3 and \
         the detector never fires for a divergence that began two rounds earlier"
    );
}

#[test]
fn the_reserved_no_claim_value_is_never_produced_by_a_real_digest() {
    // Zero means "not claiming anything" everywhere in `AppendResp`, so a fold that happened to
    // land on zero would silently disarm the detector. Tested on the substitution directly,
    // because finding an entry whose fold is zero is the difficulty itself.
    assert_ne!(never_zero(0), 0, "the reserved value survived the substitution");
    assert_eq!(never_zero(FNV_OFFSET), FNV_OFFSET, "a real digest was rewritten");
    assert_eq!(never_zero(1), 1);
    assert_eq!(never_zero(u64::MAX), u64::MAX);
}

#[test]
fn adopting_a_new_leader_retracts_everything_established_with_the_last_one() {
    // The mutant that survived the first battery: `agreed` was never reset when this node changed
    // leaders, and the test that should have caught it started from a node whose `agreed` was
    // already zero, so the reset was a no-op there. Here it is not.
    let mut b = Consensus::new(N2, cfg3(), 116);
    follower_of(&mut b, 2, N3);

    // N3 leads term 2 and establishes four rounds with this node.
    let e: Vec<Entry> =
        (1..=4u64).map(|r| Entry { term: 2, round: r, command: wal(r as u8) }).collect();
    b.step(Event::Recv(append_msg(N3, N2, 2, 0, 0, e, 0)));
    let ack = b.step(Event::Persisted { term: 2, round: 4 });
    assert_eq!(
        resp_of(&only_send(&ack)).1,
        4,
        "the setup is wrong: four rounds were not established with the previous leader"
    );

    // N1 wins term 3 holding a different round 3. Its append is REFUSED, so nothing is established
    // with it — but this node has already accepted N1 as its leader.
    let refusal = b.step(Event::Recv(append_msg(N1, N2, 3, 3, 3, vec![], 0)));
    assert!(!resp_of(&only_send(&refusal)).0, "the setup is wrong: N1's append was accepted");
    assert_eq!(b.leader(), Some(N1));

    let ack = b.step(Event::Persisted { term: 2, round: 4 });
    let (_, matched, _, digest) = resp_of(&only_send(&ack));
    assert_eq!(
        matched, 0,
        "four rounds established with the PREVIOUS leader were claimed to the new one. N1 has \
         never sent this node a round, so it would count a replica of entries it does not have"
    );
    assert_eq!(digest, 0, "and a claim of nothing carries no digest");
}
