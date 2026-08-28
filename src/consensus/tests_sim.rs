//! F8 — the simulator's own evidence: a reference state machine, one deliberate defect per rule,
//! and the sweeps that require each detector to fire and then to stay quiet.
//!
//! # Why there is a second state machine in here
//!
//! `election.rs` and `replicate.rs` are being written in parallel with this file and their handlers
//! are still `unimplemented!()`. A simulator checked only against a state machine that panics is a
//! simulator nobody has ever seen work, so this file supplies [`RefNode`]: a reference Raft written
//! against the same frozen contract — the same [`Event`]s in, the same [`Action`]s out — whose rules
//! can be switched off one at a time.
//!
//! It is a **fixture, not a second implementation**. It lives under `#[cfg(test)]` so it cannot be
//! mistaken for the shipped state machine or accidentally depended on, and it exists to answer one
//! question: *would this simulator notice?* The answer for every detector is below, with the mutant
//! that produces it. `the_real_consensus_state_machine_is_driven_by_this_simulator_...` runs the
//! real [`Consensus`] through the identical harness and starts asserting the moment F1 and F2 land,
//! with nobody having to remember to un-ignore anything.
//!
//! # The defects, and the detector each one is aimed at
//!
//! | Defect | Rule it breaks | Detector it must fire |
//! |---|---|---|
//! | [`D_NO_RESTRICTION`] | Raft §5.4.1 election restriction | `a new leader was missing a committed round` |
//! | [`D_COMMIT_INHERITED`] | Raft §5.4.2 — an inherited round committed by counting | a committed round is lost |
//! | [`D_QUORUM_OVER_NEXT`] | quorum counted over `matched`, never `next` | a committed round is lost |
//! | [`D_NO_VOTE_FSYNC`] | `PersistHardState` before the `Send` of a vote | `a vote was sent before its hard state was durable` |
//! | [`D_VOTE_TWICE`] | one vote per term | `two leaders in one term` |
//! | [`D_ACK_ON_RECEIPT`] | ack on `Persisted`, never on receipt | `an append was acknowledged before it was durable` |
//! | [`D_NO_LEASE`] | a leader demotes itself on its own lease | `two leaders overlapped for longer than the lease` |
//! | [`D_PREVOTE_RAISES_TERM`] | a pre-vote does not raise anybody's term | a partitioned node deposes a healthy leader |

use std::collections::{BTreeMap, BTreeSet};

// `super` is `sim`, not `consensus`: this module is attached from inside `sim.rs`. The glob picks
// up both the simulator's own items and the contract types `sim.rs` imports from `mod.rs`.
use super::*;
use crate::error::FerroError;

// ---------------------------------------------------------------------------------------------
// The deliberate defects
// ---------------------------------------------------------------------------------------------

/// Grant a vote to a candidate whose log is behind. Raft §5.4.1.
pub const D_NO_RESTRICTION: u32 = 1 << 0;
/// Send a vote without recording it durably first.
pub const D_NO_VOTE_FSYNC: u32 = 1 << 1;
/// Vote for a second candidate in a term already voted in.
pub const D_VOTE_TWICE: u32 = 1 << 2;
/// Acknowledge an append on receipt rather than on `Event::Persisted`.
pub const D_ACK_ON_RECEIPT: u32 = 1 << 3;
/// Commit a round inherited from an earlier term by counting replicas. Raft §5.4.2, figure 8.
pub const D_COMMIT_INHERITED: u32 = 1 << 4;
/// Count quorum over `Progress::next` — optimism — rather than over `matched`.
pub const D_QUORUM_OVER_NEXT: u32 = 1 << 5;
/// Keep the office after losing contact with a majority.
pub const D_NO_LEASE: u32 = 1 << 6;
/// Treat an incoming pre-vote as a real later term, raising this node's own.
pub const D_PREVOTE_RAISES_TERM: u32 = 1 << 7;

/// The reference machine with every rule intact.
type Correct = RefNode<0>;

// ---------------------------------------------------------------------------------------------
// The reference state machine
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq)]
struct Prog {
    next: Round,
    matched: Round,
    silent: u32,
}

/// A reference Raft over the frozen contract, parameterised by a bitmask of deliberate defects.
///
/// Const-generic rather than a runtime flag so that a mutant is a distinct *type*: there is no way
/// to leave a defect switched on by accident, and `sweep::<Correct>` and `sweep::<RefNode<D>>` are
/// visibly different calls at the site that makes the claim.
struct RefNode<const D: u32> {
    id: NodeId,
    cfg: Config,
    role: Role,
    term: Term,
    voted_for: Option<NodeId>,
    leader: Option<NodeId>,
    /// Rounds are contiguous from 1, so `log[i].round == i + 1`.
    log: Vec<Entry>,
    commit: Round,
    applied: Round,
    /// What the caller has told us is on the disk. Never inferred — only [`Event::Persisted`] moves
    /// it, which is the whole of the fsync-before-ack rule.
    durable: Round,
    votes: BTreeSet<NodeId>,
    progress: BTreeMap<NodeId, Prog>,
    since_heard: u32,
    since_heartbeat: u32,
    election_timeout: u32,
    election_base: u32,
    lease: u32,
    heartbeat: u32,
    rng: Rng,
    /// An append was accepted and handed to the disk; the acknowledgement is owed until it lands.
    ack_owed: bool,
    /// The round the **last accepted append confirmed** — `prev_round + entries.len()`.
    ///
    /// Not the log's length, and the difference is a data-loss bug the simulator found in this very
    /// file. A follower holding a stale suffix from a deposed leader matches on `prev_round`, keeps
    /// the suffix when `entries` is empty, and — acknowledging its own length — tells the leader it
    /// holds rounds that are somebody else's. The leader counts that toward quorum and commits a
    /// round only a minority really has.
    ack_through: Round,
}

/// Entries per `Append`, so a follower that is far behind catches up over several rounds and the
/// multi-entry path is exercised rather than being a special case nothing reaches.
const MAX_ENTRIES_PER_APPEND: usize = 16;

impl<const D: u32> RefNode<D> {
    fn has(bit: u32) -> bool {
        D & bit != 0
    }

    fn last_round(&self) -> Round {
        self.log.len() as Round
    }

    fn last_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    fn term_at(&self, r: Round) -> Term {
        if r == 0 {
            return 0;
        }
        self.log.get((r - 1) as usize).map(|e| e.term).unwrap_or(0)
    }

    /// A rolling hash of the durable prefix, for the divergence detector `AppendResp` carries.
    fn digest(&self) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for e in self.log.iter().take(self.durable as usize) {
            for v in [e.term, e.round] {
                h ^= v;
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        h
    }

    /// **Raft §5.4.1.** `(last_term, last_round)` compared lexicographically — a later term wins
    /// however short its log, and only within one term does length decide.
    fn log_is_up_to_date(&self, cand_term: Term, cand_round: Round) -> bool {
        if Self::has(D_NO_RESTRICTION) {
            return true;
        }
        (cand_term, cand_round) >= (self.last_term(), self.last_round())
    }

    fn voters(&self) -> Vec<NodeId> {
        self.cfg.members().to_vec()
    }

    fn peers(&self) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = self.cfg.members().to_vec();
        v.extend_from_slice(self.cfg.learners());
        v.retain(|n| *n != self.id);
        v
    }

    fn granted_by_voters(&self) -> usize {
        self.votes.iter().filter(|n| self.cfg.contains(**n)).count()
    }

    fn send(&self, to: NodeId, term: Term, body: Body) -> Action {
        Action::Send(Message { from: self.id, to, term, body })
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>, out: &mut Vec<Action>) {
        let changed = self.role != Role::Follower || self.term != term || self.leader != leader;
        if self.term != term {
            self.term = term;
            self.voted_for = None;
            out.push(Action::PersistHardState { term, voted_for: None });
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.since_heard = 0;
        if changed {
            out.push(Action::RoleChanged { role: Role::Follower, term, leader });
        }
    }

    // -- ticks -------------------------------------------------------------------------------

    fn on_tick(&mut self, out: &mut Vec<Action>) {
        if self.role == Role::Leader {
            for p in self.progress.values_mut() {
                p.silent = p.silent.saturating_add(1);
            }
            // **The leader lease.** A leader that has stopped hearing from a majority gives up the
            // office before anybody tells it to. Counted over voters only, and including itself.
            if !Self::has(D_NO_LEASE) {
                let lease = self.lease;
                let alive = 1 + self
                    .voters()
                    .iter()
                    .filter(|n| **n != self.id)
                    .filter(|n| self.progress.get(n).map(|p| p.silent < lease).unwrap_or(false))
                    .count();
                if !self.cfg.has_quorum(alive) {
                    let t = self.term;
                    self.become_follower(t, None, out);
                    return;
                }
            }
            self.since_heartbeat += 1;
            if self.since_heartbeat >= self.heartbeat {
                self.since_heartbeat = 0;
                for p in self.peers() {
                    self.send_append(p, out);
                }
            }
            return;
        }
        self.since_heard = self.since_heard.saturating_add(1);
        if self.since_heard >= self.election_timeout {
            self.start_pre_campaign(out);
        }
    }

    /// Pre-vote: ask whether a campaign could be won **without raising anybody's term**.
    fn start_pre_campaign(&mut self, out: &mut Vec<Action>) {
        if !self.cfg.contains(self.id) {
            self.since_heard = 0;
            return;
        }
        self.role = Role::PreCandidate;
        // **The office is vacant as far as this node is concerned.** Standing for election means
        // giving up the leader you had -- and it is also what lets peers that have done the same
        // grant each other a pre-vote. Without it every node's `since_heard` is reset by its own
        // campaign a tick before anyone can observe it as expired, and no pre-vote is ever granted
        // by anybody: the cluster sends pre-votes for ever and elects nobody. Measured, not
        // guessed: that is exactly what the first run of this file did.
        self.leader = None;
        self.since_heard = 0;
        self.election_timeout = self.election_base + (self.rng.next_u32() % self.election_base);
        self.votes.clear();
        self.votes.insert(self.id);
        if self.cfg.has_quorum(self.granted_by_voters()) {
            self.start_campaign(out);
            return;
        }
        let (lt, lr) = (self.last_term(), self.last_round());
        for p in self.peers() {
            // `term + 1` on the envelope while this node's own term is untouched: the receiver is
            // being asked about a hypothetical term, not told about a real one.
            out.push(self.send(p, self.term + 1, Body::PreVote { last_term: lt, last_round: lr }));
        }
    }

    fn start_campaign(&mut self, out: &mut Vec<Action>) {
        self.role = Role::Candidate;
        self.leader = None;
        self.term += 1;
        self.voted_for = Some(self.id);
        self.votes.clear();
        self.votes.insert(self.id);
        self.since_heard = 0;
        // **Durable before spoken.** A node that votes, crashes and forgets can vote twice in one
        // term, which elects two leaders of that term.
        if !Self::has(D_NO_VOTE_FSYNC) {
            out.push(Action::PersistHardState { term: self.term, voted_for: Some(self.id) });
        }
        if self.cfg.has_quorum(self.granted_by_voters()) {
            self.become_leader(out);
            return;
        }
        let (lt, lr) = (self.last_term(), self.last_round());
        for p in self.peers() {
            out.push(self.send(p, self.term, Body::RequestVote { last_term: lt, last_round: lr }));
        }
    }

    fn become_leader(&mut self, out: &mut Vec<Action>) {
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.votes.clear();
        self.since_heartbeat = 0;
        let next = self.last_round() + 1;
        self.progress = self
            .peers()
            .into_iter()
            .map(|n| (n, Prog { next, matched: 0, silent: 0 }))
            .collect();
        out.push(Action::RoleChanged {
            role: Role::Leader,
            term: self.term,
            leader: Some(self.id),
        });
        // The term-establishing entry. Without a round of its own term a leader can never commit,
        // because §5.4.2 forbids committing an inherited one by counting.
        let e = Entry { term: self.term, round: self.last_round() + 1, command: Command::NoOp };
        self.log.push(e.clone());
        out.push(Action::Persist { entries: vec![e] });
        for p in self.peers() {
            self.send_append(p, out);
        }
    }

    // -- votes -------------------------------------------------------------------------------

    fn on_vote_msg(&mut self, m: Message, out: &mut Vec<Action>) {
        match m.body {
            Body::PreVote { last_term, last_round } => {
                // Granted only if this node has itself given up on its leader -- either it never
                // had one, or it has not heard from the one it has for a whole election timeout.
                // Without this a healthy cluster answers every partitioned peer's hypothetical
                // with a yes and the pre-vote filters nothing; with it and nothing else, a cold
                // cluster grants nobody, because a node's own campaign resets the very counter its
                // peers are asking about.
                let no_leader_lately =
                    self.leader.is_none() || self.since_heard >= self.election_timeout;
                let granted = m.term > self.term
                    && no_leader_lately
                    && self.log_is_up_to_date(last_term, last_round);
                out.push(self.send(m.from, self.term, Body::PreVoteResp { granted }));
            }
            Body::PreVoteResp { granted } => {
                // **A refused pre-vote carries a real term, and this is the only place it can be
                // acted on.** `mod.rs` exempts every `PreVoteResp` from the "a later term takes its
                // receiver with it" rule, which is right for a *granted* one — that is an answer
                // about a hypothetical term nobody has entered. A *refusal* is different: the
                // responder is saying "my term is already at least as high as the one you asked
                // about", and that term is real.
                //
                // Without this, a node that restarts behind the cluster can only learn the current
                // term from an `Append` or a `RequestVote`, so if no leader is reachable it is
                // stuck for ever: its pre-votes are refused for being at a term nobody considers
                // future, and it never raises its own. The simulator deadlocked on exactly that in
                // figure 8 step 4 — four live nodes, two with the only complete log and a stale
                // term, two with a short log and the current term, and no election possible in
                // either direction. This is a **finding about the contract**, not a licence to
                // ignore it: the exemption in `mod.rs` is deliberate, so the handler carries the
                // repair. F1 has to do the same thing in `on_vote_msg`.
                if !granted && m.term > self.term {
                    self.become_follower(m.term, None, out);
                    return;
                }
                if self.role != Role::PreCandidate {
                    return;
                }
                if granted {
                    self.votes.insert(m.from);
                    if self.cfg.has_quorum(self.granted_by_voters()) {
                        self.start_campaign(out);
                    }
                }
            }
            Body::RequestVote { last_term, last_round } => {
                let free = Self::has(D_VOTE_TWICE)
                    || self.voted_for.is_none()
                    || self.voted_for == Some(m.from);
                let granted = m.term == self.term
                    && free
                    && self.log_is_up_to_date(last_term, last_round);
                if granted {
                    self.voted_for = Some(m.from);
                    self.since_heard = 0;
                    if !Self::has(D_NO_VOTE_FSYNC) {
                        out.push(Action::PersistHardState {
                            term: self.term,
                            voted_for: Some(m.from),
                        });
                    }
                }
                out.push(self.send(m.from, self.term, Body::RequestVoteResp { granted }));
            }
            Body::RequestVoteResp { granted } => {
                if self.role != Role::Candidate || m.term != self.term {
                    return;
                }
                if granted {
                    self.votes.insert(m.from);
                    if self.cfg.has_quorum(self.granted_by_voters()) {
                        self.become_leader(out);
                    }
                }
            }
            _ => {}
        }
    }

    // -- replication -------------------------------------------------------------------------

    fn send_append(&mut self, to: NodeId, out: &mut Vec<Action>) {
        let last = self.last_round();
        let next = self.progress.get(&to).map(|p| p.next).unwrap_or(1).clamp(1, last + 1);
        let prev_round = next - 1;
        let prev_term = self.term_at(prev_round);
        let end = (next as usize - 1 + MAX_ENTRIES_PER_APPEND).min(self.log.len());
        let entries: Vec<Entry> = self.log[(next as usize - 1)..end].to_vec();
        let sent_through = prev_round + entries.len() as Round;
        out.push(self.send(
            to,
            self.term,
            Body::Append { prev_round, prev_term, entries, commit: self.commit },
        ));
        // Optimism, corrected by the answer. `next` is what to send; it is deliberately NOT what
        // quorum is counted over.
        if let Some(p) = self.progress.get_mut(&to) {
            p.next = sent_through + 1;
        }
    }

    fn on_append_msg(&mut self, m: Message, out: &mut Vec<Action>) {
        match m.body {
            Body::Append { prev_round, prev_term, entries, commit } => {
                if self.role != Role::Follower || self.leader != Some(m.from) {
                    self.become_follower(m.term, Some(m.from), out);
                }
                self.since_heard = 0;

                if prev_round > self.last_round() {
                    let hint = self.last_round() + 1;
                    // `matched: 0` claims nothing: a refusal is not the place to make an assertion
                    // about a log this node has just said it cannot line up.
                    out.push(self.send(
                        m.from,
                        self.term,
                        Body::AppendResp { success: false, matched: 0, hint, digest: 0 },
                    ));
                    return;
                }
                if prev_round > 0 && self.term_at(prev_round) != prev_term {
                    // The **hint** walks back over the whole conflicting term, so the leader
                    // corrects in one step rather than probing backwards a round at a time. The
                    // **truncation** does not: only `prev_round` is known to conflict, and the
                    // rounds below it have not been examined. Deleting the whole term instead was
                    // a real defect in this file, and the simulator named it -- seed 7 dropped
                    // committed round 1 because rounds 1..4 all happened to share one term.
                    let bad = self.term_at(prev_round);
                    let mut first = prev_round;
                    while first > 1 && self.term_at(first - 1) == bad {
                        first -= 1;
                    }
                    out.push(Action::Truncate { from: prev_round });
                    self.log.truncate((prev_round - 1) as usize);
                    self.durable = self.durable.min(self.last_round());
                    self.ack_through = self.ack_through.min(self.last_round());
                    out.push(self.send(
                        m.from,
                        self.term,
                        Body::AppendResp { success: false, matched: 0, hint: first, digest: 0 },
                    ));
                    return;
                }

                // What THIS append confirms, fixed before the entries are consumed.
                let confirmed = prev_round + entries.len() as Round;
                let mut fresh = Vec::new();
                for e in entries {
                    let idx = (e.round - 1) as usize;
                    if idx < self.log.len() {
                        if self.log[idx].term == e.term {
                            continue;
                        }
                        out.push(Action::Truncate { from: e.round });
                        self.log.truncate(idx);
                        self.durable = self.durable.min(self.last_round());
                    }
                    self.log.push(e.clone());
                    fresh.push(e);
                }
                if !fresh.is_empty() {
                    out.push(Action::Persist { entries: fresh.clone() });
                }
                self.ack_through = confirmed;

                // **`min(leaderCommit, index of last new entry)`**, and the second half is
                // `confirmed`, not this node's log length. A heartbeat carrying no entries confirms
                // nothing past `prev_round`, so a follower still holding a deposed leader's entry
                // above that point would otherwise apply it on the new leader's authority. The
                // simulator found exactly that: round 19 applied as term 2 where term 3 was
                // already committed.
                let c = commit.min(confirmed);
                if c > self.commit {
                    self.commit = c;
                }
                if self.commit > self.applied {
                    self.applied = self.commit;
                    out.push(Action::Apply { through: self.commit });
                }

                if fresh.is_empty() {
                    // Nothing was handed to the disk, so the durable watermark is already honest.
                    let d = self.ack_through.min(self.durable);
                    let dig = self.digest();
                    out.push(self.send(
                        m.from,
                        self.term,
                        Body::AppendResp { success: true, matched: d, hint: 0, digest: dig },
                    ));
                } else if Self::has(D_ACK_ON_RECEIPT) {
                    let claimed = self.ack_through;
                    let dig = self.digest();
                    out.push(self.send(
                        m.from,
                        self.term,
                        Body::AppendResp { success: true, matched: claimed, hint: 0, digest: dig },
                    ));
                } else {
                    // The acknowledgement is owed until `Event::Persisted` says the bytes landed.
                    self.ack_owed = true;
                }
            }
            Body::AppendResp { success, matched, hint, .. } => {
                if self.role != Role::Leader || m.term != self.term {
                    return;
                }
                if let Some(p) = self.progress.get_mut(&m.from) {
                    p.silent = 0;
                    if success {
                        p.matched = p.matched.max(matched);
                        p.next = p.matched + 1;
                    } else {
                        p.next = hint.max(1);
                    }
                }
                if success {
                    self.try_commit(out);
                } else {
                    self.send_append(m.from, out);
                }
            }
            _ => {}
        }
    }

    /// **Raft §5.4.2.** A round of the leader's own term commits when a majority holds it; a round
    /// inherited from an earlier term commits only as a side effect of one that is.
    fn try_commit(&mut self, out: &mut Vec<Action>) {
        let mut held: Vec<Round> = self
            .voters()
            .iter()
            .map(|n| {
                if *n == self.id {
                    self.durable
                } else if Self::has(D_QUORUM_OVER_NEXT) {
                    self.progress.get(n).map(|p| p.next.saturating_sub(1)).unwrap_or(0)
                } else {
                    self.progress.get(n).map(|p| p.matched).unwrap_or(0)
                }
            })
            .collect();
        held.sort_unstable();
        held.reverse();
        let q = self.cfg.quorum();
        if q == 0 || held.len() < q {
            return;
        }
        let cand = held[q - 1].min(self.last_round());
        if cand <= self.commit {
            return;
        }
        if self.term_at(cand) != self.term && !Self::has(D_COMMIT_INHERITED) {
            return;
        }
        self.commit = cand;
        if self.commit > self.applied {
            self.applied = self.commit;
            out.push(Action::Apply { through: self.commit });
        }
    }

    fn on_persisted(&mut self, _term: Term, round: Round, out: &mut Vec<Action>) {
        // Follow the store exactly rather than taking the maximum: a truncation lowers the durable
        // watermark, and a node that refused to hear that would acknowledge a round it no longer has.
        self.durable = round.min(self.last_round());
        if self.role == Role::Leader {
            self.try_commit(out);
            return;
        }
        if self.ack_owed {
            if let Some(l) = self.leader {
                self.ack_owed = false;
                let d = self.ack_through.min(self.durable);
                let dig = self.digest();
                out.push(self.send(
                    l,
                    self.term,
                    Body::AppendResp { success: true, matched: d, hint: 0, digest: dig },
                ));
            }
        }
    }

    fn on_propose(&mut self, c: Command, out: &mut Vec<Action>) {
        if self.role != Role::Leader {
            out.push(Action::Refuse {
                why: FerroError::NotLeader { leader: self.leader.map(|n| n.to_string()) },
            });
            return;
        }
        let e = Entry { term: self.term, round: self.last_round() + 1, command: c };
        self.log.push(e.clone());
        out.push(Action::Persist { entries: vec![e] });
        for p in self.peers() {
            self.send_append(p, out);
        }
    }

    /// The universal term rules, copied from `mod.rs` because a reference machine that skipped them
    /// would be testing the simulator against a protocol nobody is implementing.
    fn on_message(&mut self, m: Message, out: &mut Vec<Action>) {
        if m.term < self.term {
            if m.body.is_request() {
                let body = stale_refusal(&m.body);
                out.push(self.send(m.from, self.term, body));
            }
            return;
        }
        let hypothetical = !Self::has(D_PREVOTE_RAISES_TERM)
            && matches!(m.body, Body::PreVote { .. } | Body::PreVoteResp { .. });
        if m.term > self.term && !hypothetical {
            self.become_follower(m.term, None, out);
        }
        match m.body {
            Body::PreVote { .. }
            | Body::PreVoteResp { .. }
            | Body::RequestVote { .. }
            | Body::RequestVoteResp { .. } => self.on_vote_msg(m, out),
            _ => self.on_append_msg(m, out),
        }
    }
}

/// `mod.rs` keeps its own copy private, so the reference machine carries one. Kept identical on
/// purpose: a refusal that differed would be a difference in the protocol, not in the fixture.
fn stale_refusal(b: &Body) -> Body {
    match b {
        Body::PreVote { .. } => Body::PreVoteResp { granted: false },
        Body::RequestVote { .. } => Body::RequestVoteResp { granted: false },
        Body::Append { .. } => Body::AppendResp { success: false, matched: 0, hint: 0, digest: 0 },
        Body::InstallSnapshot { .. } => Body::InstallSnapshotResp { received_through: 0 },
        other => unreachable!("stale_refusal called on a response body: {other:?}"),
    }
}

impl<const D: u32> Peer for RefNode<D> {
    fn boot(id: NodeId, cfg: Config, seed: u64, hard: HardState, log: &[Entry]) -> Self {
        // The same windows `Consensus::new` draws, so a claim proved here is a claim about the
        // numbers the shipped state machine uses.
        let election_base = 10;
        let mut rng = Rng::new(seed);
        let election_timeout = election_base + (rng.next_u32() % election_base);
        RefNode {
            id,
            cfg,
            role: Role::Follower,
            term: hard.term,
            voted_for: hard.voted_for,
            leader: None,
            log: log.to_vec(),
            commit: 0,
            applied: 0,
            durable: log.len() as Round,
            votes: BTreeSet::new(),
            progress: BTreeMap::new(),
            since_heard: 0,
            since_heartbeat: 0,
            election_timeout,
            election_base,
            lease: election_base - 2,
            heartbeat: 3,
            rng,
            ack_owed: false,
            ack_through: 0,
        }
    }

    fn step(&mut self, ev: Event) -> Vec<Action> {
        let mut out = Vec::new();
        match ev {
            Event::Tick => self.on_tick(&mut out),
            Event::Recv(m) => self.on_message(m, &mut out),
            Event::Persisted { term, round } => self.on_persisted(term, round, &mut out),
            Event::Propose(c) => self.on_propose(c, &mut out),
        }
        out
    }

    fn id(&self) -> NodeId { self.id }
    fn role(&self) -> Role { self.role }
    fn term(&self) -> Term { self.term }
    fn lease_window(&self) -> u32 { self.lease }
    fn tail(&self) -> (Term, Round) { (self.last_term(), self.last_round()) }
}

// ---------------------------------------------------------------------------------------------
// Shared shapes for the sweeps
// ---------------------------------------------------------------------------------------------

/// Seed bases are distinct per sweep so that two tests never claim the same evidence twice, and so
/// that a failure names which sweep found it.
const SEED_SAFETY: u64 = 0x5EED_0001;
const SEED_MUTANT: u64 = 0x5EED_1000;
const SEED_LIVENESS: u64 = 0x5EED_2000;

/// How many seeds the default `cargo test` run sweeps.
///
/// Overridable **upwards only**, and an unparseable value is refused rather than defaulted: a knob
/// that silently fell back to the floor would let a CI run that meant to sweep ten thousand sweep
/// four hundred and report success. `FERRODB_SIM_SEEDS=10000 cargo test --release` is the number
/// `DISTRIBUTED.md`'s exit criterion 1 asks for.
fn sweep_seeds(floor: u64) -> u64 {
    match std::env::var("FERRODB_SIM_SEEDS") {
        Err(_) => floor,
        Ok(v) => {
            let n: u64 = v.parse().unwrap_or_else(|_| {
                panic!(
                    "FERRODB_SIM_SEEDS is {v:?}; it takes a seed count. Refusing to guess, because \
                     guessing the floor would report a ten-thousand-seed sweep that never ran."
                )
            });
            assert!(
                n >= floor,
                "FERRODB_SIM_SEEDS={n} is below this sweep's floor of {floor}. The knob raises \
                 coverage; it does not lower it."
            );
            n
        }
    }
}

fn chaos_cfg() -> SimConfig {
    SimConfig::chaos(5, 240)
}

/// Assert a sweep found nothing **and** did enough to be able to find something. A sweep that
/// elected no leader and committed no round violates nothing and proves nothing.
fn expect_quiet(s: &Sweep, what: &str) {
    if let Some(v) = &s.violation {
        panic!("{what}: the simulator found a real violation on seed {}\n{v}", v.seed);
    }
    let t = &s.totals;
    assert!(
        t.elections > 0,
        "{what}: {} seeds elected nobody, so nothing here could have violated a leader rule: {t:?}",
        s.seeds_run
    );
    assert!(
        t.committed_rounds > 0,
        "{what}: {} seeds committed no round, so 'a committed round is never lost' is vacuous: {t:?}",
        s.seeds_run
    );
}

// ---------------------------------------------------------------------------------------------
// The harness itself
// ---------------------------------------------------------------------------------------------

/// **Breaking shape:** a reference machine that cannot elect or commit would make every sweep below
/// green for the wrong reason.
#[test]
fn a_healthy_five_node_cluster_elects_one_leader_and_commits_what_clients_propose() {
    let mut sim = Sim::<Correct>::new(7, SimConfig::healthy(5, 200));
    let report = sim.run().expect("a healthy cluster violated an invariant");
    assert!(report.elections > 0, "fixture: nobody was ever elected: {report:?}");
    assert_eq!(sim.leaders().len(), 1, "a healthy cluster ended with {:?}", sim.leaders());
    assert!(
        report.committed_rounds >= 20,
        "only {} rounds committed in 200 ticks; the load is too light for the safety sweeps to \
         mean anything: {report:?}",
        report.committed_rounds
    );
    assert_eq!(
        report.max_overlap_ticks, 0,
        "two nodes believed they led at once on a network that was never partitioned"
    );
    assert!(report.refusals > 0, "fixture: no proposal ever reached a follower, so the NotLeader path is untested");
}

/// **Breaking shape:** anything drawn from outside the seed — a clock, a `HashMap` iteration order —
/// would make a failing seed unreplayable, which is the one thing this simulator has to promise.
#[test]
fn the_same_seed_replays_the_same_run() {
    for seed in [1u64, 2, 99, 0x5EED_BEEF] {
        let a = Sim::<Correct>::new(seed, chaos_cfg()).run().expect("seed a");
        let b = Sim::<Correct>::new(seed, chaos_cfg()).run().expect("seed b");
        assert_eq!(
            a.digest, b.digest,
            "seed {seed} produced two different runs, so no failure it finds can be replayed"
        );
        assert_eq!(a, b, "seed {seed} produced different totals across two runs");
    }
}

/// The anti-vacuity twin of the test above: a digest that was constant would pass that one and
/// prove nothing at all.
#[test]
fn two_different_seeds_do_not_produce_the_same_run() {
    let mut seen = BTreeSet::new();
    for seed in 1..=24u64 {
        let r = Sim::<Correct>::new(seed, chaos_cfg()).run().expect("clean run");
        seen.insert(r.digest);
    }
    assert!(
        seen.len() >= 22,
        "24 seeds produced only {} distinct runs; the seed is barely being read",
        seen.len()
    );
}

/// **Breaking shape:** a fault model that never fires. Every knob is asserted to have actually done
/// something, because a sweep under a network that behaved perfectly is a sweep of the happy path
/// wearing a chaos label.
#[test]
fn the_fault_model_injects_every_fault_it_claims_to() {
    let s = sweep::<Correct>(SEED_SAFETY, 60, &chaos_cfg());
    let t = &s.totals;
    assert!(s.violation.is_none(), "unexpected violation:\n{}", s.violation.as_ref().unwrap());
    assert!(t.dropped_loss > 0, "no message was ever dropped: {t:?}");
    assert!(t.duplicated > 0, "no message was ever duplicated: {t:?}");
    assert!(
        t.duplicates_delivered > 0,
        "every duplicate was dropped before it arrived, so nothing was ever asked to be idempotent: \
         {t:?}"
    );
    assert!(
        t.reordered > 0,
        "no message ever arrived after a later-sent one on the same link. Reorder is emergent here \
         rather than injected — it comes from drawing each latency independently — which is exactly \
         why it is asserted rather than assumed: {t:?}"
    );
    assert!(t.dropped_partition > 0, "no message was ever cut off by a partition: {t:?}");
    assert!(
        t.one_way_partitions > 0,
        "every partition was symmetric, so the one-way case DISTRIBUTED.md singles out was never \
         reached: {t:?}"
    );
    assert!(t.crashes > 0, "no node ever crashed: {t:?}");
    assert!(t.restarts > 0, "no node ever came back: {t:?}");
    assert!(
        t.discarded_entries > 0,
        "no crash ever took away an unfsynced entry, so 'loses everything not persisted' is a \
         claim this model never tested: {t:?}"
    );
    assert!(t.heals > 0, "the network never healed, so nothing could make progress: {t:?}");
    assert!(
        t.unsent_at_crash > 0,
        "no crash ever destroyed a message that was still waiting behind an fsync, so the ordering \
         the contract requires of a caller was never actually enforced: {t:?}"
    );
    eprintln!("fault model over {} seeds: {t:?}", s.seeds_run);
}

// ---------------------------------------------------------------------------------------------
// The two properties DISTRIBUTED.md §F8 names
// ---------------------------------------------------------------------------------------------

/// **At most one leader per term**, across a seeded chaos sweep.
///
/// Breaking shape: a vote counted twice — most often a vote sent before its `PersistHardState`
/// landed, cast again by the same node after a crash.
#[test]
fn at_most_one_leader_per_term_across_a_chaos_sweep() {
    let n = sweep_seeds(2_000);
    let s = sweep::<Correct>(SEED_SAFETY, n, &chaos_cfg());
    expect_quiet(&s, "at most one leader per term");
    eprintln!(
        "at_most_one_leader_per_term: {} seeds, {} elections, {} committed rounds, max term {}",
        s.seeds_run, s.totals.elections, s.totals.committed_rounds, s.totals.max_term
    );
}

/// **A committed round is never lost**, across a seeded chaos sweep.
///
/// Breaking shape: the figure-8 scenario — an inherited round committed by counting replicas, then
/// overwritten by a later leader that never held it.
#[test]
fn a_committed_round_is_never_lost_across_a_chaos_sweep() {
    let n = sweep_seeds(2_000);
    let s = sweep::<Correct>(SEED_SAFETY + 500_000, n, &chaos_cfg());
    expect_quiet(&s, "a committed round is never lost");
    eprintln!(
        "a_committed_round_is_never_lost: {} seeds, {} committed rounds, {} crashes, {} restarts",
        s.seeds_run, s.totals.committed_rounds, s.totals.crashes, s.totals.restarts
    );
}

/// The same properties under partitions but no crashes, so that a failure here names the network
/// rather than the disk.
#[test]
fn the_safety_properties_hold_under_partitions_alone() {
    let mut cfg = chaos_cfg();
    cfg.faults = Faults::partitioned();
    let s = sweep::<Correct>(SEED_SAFETY + 900_000, sweep_seeds(1_500), &cfg);
    expect_quiet(&s, "safety under partitions alone");
    assert!(
        s.totals.one_way_partitions > 0,
        "fixture: the partition-only preset produced no one-way cut: {:?}",
        s.totals
    );
}


// ---------------------------------------------------------------------------------------------
// The mutants: every detector forced to fire, and then required to stay quiet
// ---------------------------------------------------------------------------------------------

/// Run a mutant until it trips something, and insist that it did.
///
/// Returns the violation so the caller can name the seed in the test output — which is the whole
/// point of the exercise: a detector that fires without saying which seed is a detector nobody can
/// act on.
fn expect_fires<P: Peer>(defect: &str, want: &[&str], seeds: u64, cfg: &SimConfig) -> Violation {
    let s = sweep::<P>(SEED_MUTANT, seeds, cfg);
    let v = s.violation.unwrap_or_else(|| {
        panic!(
            "the {defect} mutant survived {} seeds ({} elections, {} committed rounds). A detector \
             that cannot be made to fire is not a detector: {:?}",
            s.seeds_run, s.totals.elections, s.totals.committed_rounds, s.totals
        )
    });
    assert!(
        want.contains(&v.rule),
        "the {defect} mutant tripped {:?}, which is not one of the rules this defect is supposed \
         to break ({want:?}). A detector firing for the wrong reason is not evidence.\n{v}",
        v.rule
    );
    eprintln!(
        "MUTANT {defect}: killed on seed {} at unit {} after {} seeds -- {}\n  {}",
        v.seed, v.at, s.seeds_run, v.rule, v.detail
    );
    v
}

/// The quiet half. The same sweep with every rule intact must find nothing.
fn expect_correct_is_quiet(defect: &str, seeds: u64, cfg: &SimConfig) {
    let s = sweep::<Correct>(SEED_MUTANT, seeds, cfg);
    expect_quiet(&s, &format!("{defect}: the rule restored"));
}

/// **Raft §5.4.1.** A vote granted to a candidate whose log is behind loses acknowledged data.
#[test]
fn removing_the_election_restriction_is_caught_and_the_seed_is_named() {
    let cfg = chaos_cfg();
    expect_fires::<RefNode<D_NO_RESTRICTION>>(
        "no election restriction",
        &[
            "a new leader was missing a committed round",
            "two different commands were committed at one round",
            "a committed round was dropped from a node's log",
            "a committed round was overwritten",
        ],
        200,
        &cfg,
    );
    expect_correct_is_quiet("no election restriction", 200, &cfg);
}

/// Quorum counted over `Progress::next` — optimism — commits rounds nobody holds.
#[test]
fn counting_quorum_over_next_instead_of_matched_is_caught_and_the_seed_is_named() {
    let cfg = chaos_cfg();
    expect_fires::<RefNode<D_QUORUM_OVER_NEXT>>(
        "quorum over next",
        &[
            "two different commands were committed at one round",
            "a committed round was dropped from a node's log",
            "a committed round was overwritten",
            "a new leader was missing a committed round",
            "a node applied a round it does not hold",
        ],
        200,
        &cfg,
    );
    expect_correct_is_quiet("quorum over next", 200, &cfg);
}

/// A follower that acknowledges a round it has not fsynced turns a correlated power loss into
/// acknowledged data loss. Caught structurally, at the moment the acknowledgement leaves the state
/// machine, rather than statistically after a crash — so it does not need luck to fire.
#[test]
fn acknowledging_an_append_before_the_fsync_is_caught_and_the_seed_is_named() {
    let cfg = chaos_cfg();
    expect_fires::<RefNode<D_ACK_ON_RECEIPT>>(
        "ack on receipt",
        &["an append was acknowledged before it was durable"],
        20,
        &cfg,
    );
    expect_correct_is_quiet("ack on receipt", 20, &cfg);
}

/// `PersistHardState` must reach the disk before the `Send` of any vote. A node that votes, crashes
/// and forgets the vote can vote twice in one term, which elects two leaders of that term.
#[test]
fn sending_a_vote_before_its_hard_state_is_durable_is_caught_and_the_seed_is_named() {
    let cfg = chaos_cfg();
    expect_fires::<RefNode<D_NO_VOTE_FSYNC>>(
        "no hard-state fsync before a vote",
        &["a vote was sent before its hard state was durable"],
        20,
        &cfg,
    );
    expect_correct_is_quiet("no hard-state fsync before a vote", 20, &cfg);
}

/// One vote per term. Two grants in one term is the two-leader bug arriving through the front door.
#[test]
fn voting_twice_in_one_term_is_caught_and_the_seed_is_named() {
    let cfg = chaos_cfg();
    expect_fires::<RefNode<D_VOTE_TWICE>>(
        "vote twice in a term",
        &[
            "two leaders in one term",
            "two different commands were committed at one round",
            "a committed round was dropped from a node's log",
            "a committed round was overwritten",
            "a new leader was missing a committed round",
        ],
        200,
        &cfg,
    );
    expect_correct_is_quiet("vote twice in a term", 200, &cfg);
}

/// The replay promise, tested rather than asserted: a violation's seed, fed back in, produces the
/// same violation at the same instant — and this time with the trace attached.
#[test]
fn a_failing_seed_replays_to_the_same_failure_with_a_trace() {
    let cfg = chaos_cfg();
    let first = expect_fires::<RefNode<D_ACK_ON_RECEIPT>>(
        "ack on receipt (for replay)",
        &["an append was acknowledged before it was durable"],
        20,
        &cfg,
    );
    assert!(first.trace.is_empty(), "fixture: a sweep should not be paying for tracing");

    let again = Sim::<RefNode<D_ACK_ON_RECEIPT>>::replay(first.seed, cfg)
        .expect_err("the replay of a failing seed did not fail");
    assert_eq!(again.rule, first.rule, "the replay tripped a different rule");
    assert_eq!(again.at, first.at, "the replay failed at a different instant");
    assert_eq!(again.detail, first.detail, "the replay failed with a different detail");
    assert!(
        !again.trace.is_empty(),
        "the replay produced no trace, so a failing seed still cannot be looked at"
    );
    eprintln!("REPLAY seed {} reproduced at unit {} with {} trace lines", again.seed, again.at, again.trace.len());
}

// ---------------------------------------------------------------------------------------------
// Scripted scenarios — the rules a random fault process reaches too rarely to be evidence
// ---------------------------------------------------------------------------------------------

/// A network that does nothing wrong and a fault process that does nothing at all: the scenario
/// tests below break the cluster themselves, at instants they choose.
fn scripted() -> SimConfig {
    let mut c = SimConfig::scripted(5, 0);
    c.trace = false;
    c
}

/// Advance a unit at a time until `f` holds, or give up. Unit granularity because a role change and
/// the messages it emits are separated by less than one tick.
fn advance_until<P: Peer>(
    sim: &mut Sim<P>,
    units: u64,
    mut f: impl FnMut(&Sim<P>) -> bool,
) -> Result<bool, Violation> {
    for _ in 0..units {
        if f(sim) {
            return Ok(true);
        }
        sim.run_units(1)?;
    }
    Ok(f(sim))
}

fn wait_for_leader<P: Peer>(sim: &mut Sim<P>, ticks: u64) -> Result<Option<NodeId>, Violation> {
    advance_until(sim, ticks * UNITS_PER_TICK, |s| s.leader().is_some())?;
    Ok(sim.leader())
}

/// **`DISTRIBUTED.md` exit criterion 4.** A partitioned leader cannot commit, and demotes itself on
/// its own lease without being told.
#[test]
fn a_partitioned_leader_demotes_itself_before_anybody_tells_it() {
    let mut sim = Sim::<Correct>::new(11, scripted());
    let l = wait_for_leader(&mut sim, 60).unwrap().expect("fixture: nobody was elected");
    let lease = 8u64; // `Consensus::new`: election_base - 2, and `RefNode` draws the same window.
    sim.isolate(l);
    let demoted = advance_until(&mut sim, (lease + 4) * UNITS_PER_TICK, |s| {
        s.role_of(l) != Some(Role::Leader)
    })
    .unwrap();
    assert!(
        demoted,
        "{l} still believed it led {} ticks after losing every peer; its lease is {lease}",
        lease + 4
    );
    let next = wait_for_leader(&mut sim, 80).unwrap();
    assert!(next.is_some(), "the majority never elected a replacement for {l}");
    assert_ne!(next, Some(l), "the isolated node was somehow re-elected");
}

/// **The asymmetric case.** Only the *acknowledgements* are cut off: the leader's heartbeats still
/// arrive, so no peer times out and nobody will ever tell this leader it has been replaced. It has
/// to work that out from its own lease, which is the whole reason the lease exists.
#[test]
fn a_leader_whose_acknowledgements_are_cut_off_one_way_still_gives_up_the_office() {
    let mut sim = Sim::<Correct>::new(23, scripted());
    let l = wait_for_leader(&mut sim, 60).unwrap().expect("fixture: nobody was elected");
    sim.isolate_inbound(l);
    let demoted =
        advance_until(&mut sim, 14 * UNITS_PER_TICK, |s| s.role_of(l) != Some(Role::Leader))
            .unwrap();
    assert!(
        demoted,
        "{l} kept the office while hearing from nobody, because its own heartbeats kept its peers \
         from ever telling it"
    );

    // And the mutant does not, which is what makes the assertion above a claim about the rule
    // rather than about the scenario.
    let mut broken = Sim::<RefNode<D_NO_LEASE>>::new(23, scripted());
    let bl = wait_for_leader(&mut broken, 60).unwrap().expect("fixture: nobody was elected");
    broken.isolate_inbound(bl);
    let broken_demoted =
        advance_until(&mut broken, 14 * UNITS_PER_TICK, |s| s.role_of(bl) != Some(Role::Leader))
            .unwrap();
    assert!(
        !broken_demoted,
        "the no-lease mutant gave up the office anyway, so this scenario is not testing the lease"
    );
    eprintln!("LEASE: {l} demoted itself on its own lease; the mutant {bl} did not");
}

/// The lease detector inside the simulator, forced to fire: with the rule removed, a partitioned
/// leader and its replacement both hold the office for longer than any lease allows.
#[test]
fn a_leader_that_never_gives_up_its_lease_is_caught_and_the_seed_is_named() {
    let mut sim = Sim::<RefNode<D_NO_LEASE>>::new(11, scripted());
    let l = wait_for_leader(&mut sim, 60).unwrap().expect("fixture: nobody was elected");
    sim.isolate(l);
    let err = (|| -> Violation {
        for _ in 0..120 {
            if let Err(v) = sim.run_ticks(1) {
                return v;
            }
        }
        panic!(
            "the no-lease mutant led alongside a replacement for 120 ticks without tripping the \
             overlap detector; max overlap seen was {} ticks",
            sim.report().max_overlap_ticks
        )
    })();
    assert_eq!(err.rule, "two leaders overlapped for longer than the lease", "{err}");
    eprintln!("MUTANT no lease expiry: killed on seed {} at unit {} -- {}", err.seed, err.at, err.detail);

    // The quiet half, in the same scenario.
    let mut ok = Sim::<Correct>::new(11, scripted());
    let l2 = wait_for_leader(&mut ok, 60).unwrap().expect("fixture: nobody was elected");
    ok.isolate(l2);
    ok.run_ticks(120).expect("the rule restored, the same scenario must be clean");
    assert_eq!(
        ok.report().max_overlap_ticks,
        0,
        "with the lease intact no two nodes should have overlapped at all"
    );
}

// ---------------------------------------------------------------------------------------------
// Figure 8
// ---------------------------------------------------------------------------------------------

/// What one run of the figure-8 script did.
struct FigureEight {
    /// The inherited round the script sets up to be committed and then overwritten.
    round: Round,
    /// Whether the simulator recorded that round as committed at all.
    committed: bool,
    /// The node that ends up able to overwrite it, if it won its election.
    usurper_led: bool,
    violation: Option<Violation>,
}

/// Raft's figure 8, driven rather than waited for.
///
/// `DISTRIBUTED.md` §F2 asks for this scenario by name, and the reason it is scripted rather than
/// swept for is measured, not assumed: **400 seeds of chaos — 1262 elections, 11279 committed
/// rounds, 1641 crashes — never once produced it.** The sequence needs a leader to partially
/// replicate, be deposed, come back, and re-replicate the same rounds under a new term while a node
/// holding a *higher-term* but *shorter* log is still alive to be elected around it. Random faults
/// reach that arrangement far too rarely to be evidence, so the script builds it:
///
/// 1. `L1` leads and commits a common prefix of `p` rounds.
/// 2. `L1` is cut down to one follower `F` and appends 16 more rounds — replicated to `F`, held by
///    two nodes out of five, committed by nobody. The last is round `r`.
/// 3. `L1` and `F` stop. The other three elect `L2`, which appends its term-establishing entry at
///    round `p+1` — a *different* entry at a round `L1` and `F` already hold — and is cut off
///    before it reaches anyone.
/// 4. `L2` stops, `L1` and `F` return. One of them wins, because their logs are longer, and
///    re-replicates rounds `p+1..=r` to a majority. **Those rounds are of an earlier term.**
/// 5. The instant `r` is reported committed, the two long-logged nodes stop and `L2` returns. Its
///    log is *shorter* but its last term is *higher*, so the election restriction lets it win — and
///    it overwrites round `r`.
///
/// With §5.4.2 enforced, step 4 does not commit `r` on its own, `r` only becomes committed once a
/// round of the new leader's term is, and by then the majority's last term is high enough that
/// `L2` can never win step 5. With §5.4.2 removed, `r` is committed at step 4 and gone at step 5.
fn figure_8<P: Peer>(seed: u64) -> FigureEight {
    let mut sim = Sim::<P>::new(seed, scripted());
    let run = |sim: &mut Sim<P>, units: u64| -> Option<Violation> { sim.run_units(units).err() };

    macro_rules! bail {
        ($sim:expr, $v:expr, $round:expr) => {
            if let Some(v) = $v {
                return FigureEight {
                    round: $round,
                    committed: $sim.committed().contains_key(&$round),
                    usurper_led: false,
                    violation: Some(v),
                };
            }
        };
    }

    // 1. A common prefix everybody holds.
    let l1 = match wait_for_leader(&mut sim, 60) {
        Ok(Some(l)) => l,
        Ok(None) => panic!("fixture: figure 8 needs a leader to start from and none was elected"),
        Err(v) => panic!("fixture: a violation before the scenario even began\n{v}"),
    };
    for _ in 0..3 {
        let cmd = Command::WalBatch { start_lsn: 900, bytes: vec![9] };
        if let Err(v) = sim.propose_to(l1, cmd) {
            panic!("fixture: {v}");
        }
        bail!(sim, run(&mut sim, 2 * UNITS_PER_TICK), 0);
    }
    let p = sim.durable_log(l1).len() as Round;
    assert!(p >= 4, "fixture: the common prefix is only {p} rounds, too short to cut into");

    // 2. Cut L1 down to one follower and append a batch that reaches exactly two nodes of five.
    //    The other three are isolated from each other too, so nobody elects a replacement yet.
    let others: Vec<NodeId> = (1..=5u32).map(NodeId).filter(|n| *n != l1).collect();
    let f = others[0];
    for n in &others[1..] {
        sim.isolate(*n);
    }
    for k in 0..MAX_ENTRIES_PER_APPEND {
        let cmd = Command::WalBatch { start_lsn: 1000 + k as u64, bytes: vec![k as u8] };
        if let Err(v) = sim.propose_to(l1, cmd) {
            panic!("fixture: {v}");
        }
        bail!(sim, run(&mut sim, 1), 0);
    }
    bail!(sim, run(&mut sim, 2 * UNITS_PER_TICK), 0);
    let r = p + MAX_ENTRIES_PER_APPEND as Round;
    assert_eq!(
        sim.durable_log(l1).len() as Round,
        r,
        "fixture: the partitioned leader did not append the whole batch"
    );
    assert_eq!(
        sim.durable_log(f).len() as Round,
        r,
        "fixture: the one reachable follower did not receive the batch, so only one node holds it"
    );
    assert!(
        !sim.committed().contains_key(&r),
        "fixture: round {r} was committed while only two nodes of five held it"
    );

    // 3. The long-logged pair stops; the other three elect L2, which is cut off the instant it wins
    //    so its term-establishing entry reaches nobody.
    sim.crash(l1);
    sim.crash(f);
    sim.heal();
    let l2 = match wait_for_leader(&mut sim, 120) {
        Ok(Some(l)) => l,
        Ok(None) => panic!("fixture: the remaining three never elected a leader"),
        Err(v) => panic!("fixture: {v}"),
    };
    sim.isolate(l2);
    bail!(sim, run(&mut sim, 2 * UNITS_PER_TICK), r);
    for n in &others[1..] {
        if *n != l2 {
            assert_eq!(
                sim.durable_log(*n).len() as Round,
                p,
                "fixture: {l2}'s entry escaped to {n}, so there is no divergent round to lose"
            );
        }
    }
    assert!(
        sim.durable_log(l2).len() as Round > p,
        "fixture: {l2} never appended its own term-establishing entry"
    );

    // 4. L2 stops, the long-logged pair returns, and one of them re-replicates the earlier term's
    //    rounds to a majority.
    sim.crash(l2);
    sim.restart(l1);
    sim.restart(f);
    sim.heal();
    let l3 = match wait_for_leader(&mut sim, 160) {
        Ok(Some(l)) => l,
        Ok(None) => panic!("fixture: nobody was elected in step 4"),
        Err(v) => {
            return FigureEight { round: r, committed: false, usurper_led: false, violation: Some(v) }
        }
    };
    assert!(
        l3 == l1 || l3 == f,
        "fixture: {l3} won step 4 despite a shorter log; the election restriction is not doing what \
         this script assumes"
    );

    // 5. The instant the inherited round is reported committed, stop the pair that holds it and let
    //    the shorter-but-higher-term node stand.
    let saw = match advance_until(&mut sim, 60 * UNITS_PER_TICK, |s| s.committed().contains_key(&r))
    {
        Ok(b) => b,
        Err(v) => {
            return FigureEight { round: r, committed: true, usurper_led: false, violation: Some(v) }
        }
    };
    sim.crash(l1);
    sim.crash(f);
    sim.restart(l2);
    sim.heal();
    let usurper = match wait_for_leader(&mut sim, 160) {
        Ok(l) => l,
        Err(v) => {
            return FigureEight { round: r, committed: saw, usurper_led: false, violation: Some(v) }
        }
    };
    let v = run(&mut sim, 40 * UNITS_PER_TICK);
    FigureEight { round: r, committed: saw, usurper_led: usurper == Some(l2), violation: v }
}

/// **Raft §5.4.2, figure 8.** An inherited round committed by counting replicas is the classic way
/// to lose acknowledged data on a leader change — and this is the simulator being shown to catch it.
#[test]
fn committing_an_inherited_round_by_counting_replicas_loses_it_in_the_figure_8_scenario() {
    let seed = 4242;
    let broken = figure_8::<RefNode<D_COMMIT_INHERITED>>(seed);
    assert!(
        broken.committed,
        "fixture: the mutant never committed round {}, so there was nothing to lose",
        broken.round
    );
    let v = broken.violation.unwrap_or_else(|| {
        panic!(
            "the §5.4.2 mutant committed round {} and the shorter-but-higher-term node {} win its \
             election, and the simulator said nothing. A detector that cannot be made to fire is \
             not a detector.",
            broken.round,
            if broken.usurper_led { "did" } else { "did not" }
        )
    });
    // Leader completeness is the one that fires, and it fires *before* any byte is deleted: the
    // moment a node holding none of round `r` wins an election, `r` is lost whatever happens next.
    // The three truncation rules are listed with it because a different implementation may reach
    // the same loss by the slower route.
    assert!(
        [
            "a new leader was missing a committed round",
            "a committed round was dropped from a node's log",
            "a committed round was overwritten",
            "two different commands were committed at one round",
        ]
        .contains(&v.rule),
        "figure 8 tripped {:?}, which is not a loss of the committed round\n{v}",
        v.rule
    );
    eprintln!(
        "MUTANT commit inherited round: killed by figure 8 on seed {} at unit {} -- {}\n  {}",
        v.seed, v.at, v.rule, v.detail
    );

    // The quiet half: the identical script with §5.4.2 enforced.
    let ok = figure_8::<Correct>(seed);
    if let Some(v) = ok.violation {
        panic!("figure 8 with §5.4.2 enforced still lost data:\n{v}");
    }
    assert!(
        ok.committed,
        "fixture: with the rule enforced round {} was never committed at all, so 'and it was not \
         lost' says nothing",
        ok.round
    );
    assert!(
        !ok.usurper_led,
        "with §5.4.2 enforced the shorter log must not have been electable once round {} was \
         committed",
        ok.round
    );
}

// ---------------------------------------------------------------------------------------------
// Pre-vote: a partitioned node must not depose a healthy leader
// ---------------------------------------------------------------------------------------------

/// Run a cluster to a leader, cut one node's *inbound* traffic so it campaigns for ever into a wall
/// while the majority stays healthy, and report what the majority's leadership looked like before
/// and after. `None` means no leader was ever elected in the first place.
///
/// The one-way cut is the point: the partitioned node's pre-votes still reach everybody, so this
/// asks whether they are *answered* harmlessly, not whether they arrive.
fn survives_a_partitioned_campaigner<P: Peer>(seed: u64) -> Option<(NodeId, Term, Option<NodeId>, Term)> {
    let mut sim = Sim::<P>::new(seed, scripted());
    let l = wait_for_leader(&mut sim, 80).ok()??;
    let before = sim.term_of(l)?;
    let odd = (1..=5u32).map(NodeId).find(|n| *n != l).unwrap();
    sim.isolate_inbound(odd);
    sim.run_ticks(120).expect("no safety violation is expected here; this is a liveness claim");
    let after = (1..=5u32).map(NodeId).filter_map(|n| sim.term_of(n)).max().unwrap_or(before);
    Some((l, before, sim.leader(), after))
}

/// **The pre-vote rule: a pre-vote must not raise anybody's term.**
///
/// Two halves, and the second is the more damning of the two. A node partitioned away from the
/// cluster must not disturb it by campaigning into a wall — and a receiver that treats the
/// deliberately-one-higher term on a pre-vote as a real one does not merely disturb the cluster,
/// it takes elections away from it altogether: every node raises its term to the hypothetical, and
/// the pre-vote it raised the term for is then refused for not being about a future term. Nobody
/// can ever win.
#[test]
fn a_pre_vote_must_not_raise_anybodys_term() {
    let (l, before, still, after) = survives_a_partitioned_campaigner::<Correct>(31)
        .expect("fixture: the healthy cluster never elected anybody");
    assert_eq!(
        before, after,
        "a node that could hear nobody raised the healthy cluster's term from {before} to {after} \
         over 120 ticks; that is the disruption pre-vote exists to prevent"
    );
    assert_eq!(
        still,
        Some(l),
        "{l} lost the office to a node that was campaigning into a one-way partition"
    );

    // The mutant, over a spread of seeds so this is not one unlucky draw.
    let seeds: Vec<u64> = (31..41).collect();
    let broken: Vec<u64> = seeds
        .iter()
        .copied()
        .filter(|s| survives_a_partitioned_campaigner::<RefNode<D_PREVOTE_RAISES_TERM>>(*s).is_some())
        .collect();
    let healthy: Vec<u64> = seeds
        .iter()
        .copied()
        .filter(|s| survives_a_partitioned_campaigner::<Correct>(*s).is_some())
        .collect();
    assert_eq!(
        healthy.len(),
        seeds.len(),
        "fixture: the correct machine failed to elect on {:?}, so 'the mutant cannot elect' would \
         say nothing about pre-vote",
        seeds.iter().filter(|s| !healthy.contains(s)).collect::<Vec<_>>()
    );
    assert!(
        broken.is_empty(),
        "the mutant that treats a pre-vote as a real later term still elected leaders on {broken:?}"
    );
    eprintln!(
        "MUTANT pre-vote raises the term: killed on all of {seeds:?} -- raising the term on the \
         hypothetical leaves a healthy five-node cluster unable to elect anybody at all, while the \
         correct machine elected on every one of them and held term {before} throughout a \
         one-way partition"
    );
}

// ---------------------------------------------------------------------------------------------
// Liveness — asserted only where the faults have healed, and shown able to fail
// ---------------------------------------------------------------------------------------------

/// **`DISTRIBUTED.md` exit criterion 1.** Three nodes elect a leader from a cold start, over many
/// seeds. Run at five as well, because an even split is only reachable above three.
#[test]
fn a_cold_cluster_elects_exactly_one_leader_within_an_election_window() {
    let window = 60u64; // election_timeout is drawn in [10, 20) ticks; this allows several rounds.
    for nodes in [3u32, 5] {
        let mut worst = 0u64;
        let seeds = sweep_seeds(200);
        for k in 0..seeds {
            let seed = SEED_LIVENESS + k;
            let mut sim = Sim::<Correct>::new(seed, SimConfig::healthy(nodes, window));
            let mut at = None;
            for t in 0..window {
                sim.run_ticks(1).unwrap_or_else(|v| panic!("seed {seed}:\n{v}"));
                if sim.leader().is_some() {
                    at = Some(t);
                    break;
                }
            }
            let at = at.unwrap_or_else(|| {
                panic!(
                    "seed {seed}: {nodes} healthy nodes elected nobody in {window} ticks, with \
                     leaders {:?}",
                    sim.leaders()
                )
            });
            worst = worst.max(at);
        }
        eprintln!("cold start, {nodes} nodes, {seeds} seeds: slowest election was {worst} ticks");
    }
}

/// The anti-vacuity twin of the test above: the liveness check must be able to fail, or "a leader
/// was elected" is a sentence about the harness rather than about the protocol.
#[test]
fn a_cluster_cut_into_two_minorities_elects_nobody() {
    let mut sim = Sim::<Correct>::new(77, scripted());
    sim.cut(&[NodeId(1), NodeId(2)]);
    sim.isolate(NodeId(5));
    sim.run_ticks(120).expect("a partition is not a safety violation");
    assert!(
        sim.leaders().is_empty(),
        "a cluster with no majority anywhere elected {:?}; the liveness assertions elsewhere in \
         this file would then be measuring nothing",
        sim.leaders()
    );
    sim.heal();
    let back = wait_for_leader(&mut sim, 120).unwrap();
    assert!(back.is_some(), "the cluster never recovered after the partition healed");
}

/// **`DISTRIBUTED.md` exit criterion 2**, at simulator level: the leader is killed under load, a
/// new one takes over, and nothing that had been committed is lost.
#[test]
fn killing_the_leader_under_load_costs_no_committed_round() {
    let seeds = sweep_seeds(60);
    let mut failovers = 0u64;
    let mut preserved = 0u64;
    for k in 0..seeds {
        let seed = SEED_LIVENESS + 100_000 + k;
        let mut sim = Sim::<Correct>::new(seed, SimConfig::healthy(5, 0));
        let Some(l) = wait_for_leader(&mut sim, 60).unwrap() else {
            panic!("seed {seed}: fixture: nobody was elected");
        };
        // Load, then kill mid-flight.
        for j in 0..12u64 {
            let cmd = Command::WalBatch { start_lsn: 7000 + j, bytes: vec![j as u8] };
            sim.propose_to(l, cmd).unwrap_or_else(|v| panic!("seed {seed}:\n{v}"));
            sim.run_units(3).unwrap_or_else(|v| panic!("seed {seed}:\n{v}"));
        }
        sim.run_ticks(4).unwrap_or_else(|v| panic!("seed {seed}:\n{v}"));
        let before: BTreeMap<Round, Entry> = sim.committed().clone();
        assert!(
            !before.is_empty(),
            "seed {seed}: fixture: nothing was committed before the kill, so nothing could be lost"
        );
        sim.crash(l);

        let next = wait_for_leader(&mut sim, 120).unwrap_or_else(|v| panic!("seed {seed}:\n{v}"));
        let next = next.unwrap_or_else(|| panic!("seed {seed}: no leader after {l} was killed"));
        assert_ne!(next, l, "seed {seed}: the killed leader was somehow still leading");
        failovers += 1;
        sim.run_ticks(30).unwrap_or_else(|v| panic!("seed {seed}:\n{v}"));

        // Every round committed before the kill must still be committed, with the same command, and
        // the new leader must hold it.
        for (r, e) in &before {
            assert_eq!(
                sim.committed().get(r),
                Some(e),
                "seed {seed}: round {r} was committed before {l} was killed and is not the same \
                 entry afterwards"
            );
            let held = sim.durable_log(next);
            assert_eq!(
                held.get((*r - 1) as usize),
                Some(e),
                "seed {seed}: the new leader {next} does not hold committed round {r}"
            );
        }
        preserved += before.len() as u64;
    }
    eprintln!(
        "failover: {failovers} leader kills over {seeds} seeds, {preserved} committed rounds \
         checked across them"
    );
}

// ---------------------------------------------------------------------------------------------
// The crash model
// ---------------------------------------------------------------------------------------------

/// **A restart keeps exactly what was fsynced, and no more.**
///
/// Anti-vacuity is the whole test: if no crash ever landed in the window between a write and its
/// durability, the "loses everything not persisted" model would be an unexercised claim and every
/// restart in this file would be a pause.
#[test]
fn a_restarted_node_comes_back_with_only_what_it_had_fsynced() {
    let mut lost_something = 0u64;
    let mut total_lost = 0u64;
    let seeds = sweep_seeds(80);
    for k in 0..seeds {
        let seed = SEED_LIVENESS + 200_000 + k;
        let mut sim = Sim::<Correct>::new(seed, chaos_cfg());
        let r = sim.run().unwrap_or_else(|v| panic!("seed {seed}:\n{v}"));
        if r.discarded_entries > 0 {
            lost_something += 1;
            total_lost += r.discarded_entries;
        }
    }
    assert!(
        lost_something > 0,
        "across {seeds} seeds no crash ever discarded an unfsynced entry, so the crash model never \
         did the one thing it exists to do"
    );
    eprintln!(
        "crash model: {lost_something} of {seeds} seeds lost unfsynced entries, {total_lost} \
         entries in total"
    );
}

// ---------------------------------------------------------------------------------------------
// The real state machine
// ---------------------------------------------------------------------------------------------

/// The same harness, pointed at the shipped [`Consensus`].
///
/// While `election.rs` and `replicate.rs` are `unimplemented!()` this records that fact and says so
/// out loud. It is **not** `#[ignore]`d: in this repo `#[ignore]` means "known-open defect", and an
/// ignored test also needs somebody to remember to un-ignore it. This one starts asserting on its
/// own the moment F1 and F2 land, and until then it insists the panic is the *expected* one — a
/// half-built handler that panics for some other reason is not the same fact.
#[test]
fn the_real_consensus_state_machine_runs_under_this_simulator_the_moment_f1_and_f2_land() {
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(|| {
        let mut sim = Sim::<Consensus>::new(5, SimConfig::healthy(3, 80));
        sim.run().map(|r| (r, sim.leaders()))
    });
    std::panic::set_hook(hook);

    match outcome {
        Err(p) => {
            let msg = p
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            assert!(
                msg.contains("not built yet"),
                "Consensus panicked with {msg:?}, which is not the `unimplemented!(\"... not built \
                 yet\")` that election.rs and replicate.rs still carry. Either a handler is half \
                 built, or this test is now hiding a real crash."
            );
            eprintln!(
                "F8: the real Consensus is still unimplemented ({}); the simulator drove it until \
                 the first handler panicked and will assert on it as soon as F1 and F2 land.",
                msg.lines().next().unwrap_or("")
            );
        }
        Ok(Ok((report, leaders))) => {
            assert!(
                report.elections > 0,
                "the real Consensus ran 80 ticks on a healthy three-node network and elected \
                 nobody: {report:?}"
            );
            assert_eq!(leaders.len(), 1, "the real Consensus ended with leaders {leaders:?}");
            assert!(
                report.committed_rounds > 0,
                "the real Consensus elected a leader but committed nothing: {report:?}"
            );
            eprintln!("F8: the real Consensus is live under the simulator: {report:?}");
        }
        Ok(Err(v)) => panic!("the real Consensus violated an invariant:\n{v}"),
    }
}



/// **A send still waiting behind an fsync has not left the machine, and the crash that killed the
/// machine kills it too.**
///
/// This is a rule about the *simulator*, and it is here because the simulator got it wrong. Seed
/// 1592682576 of a 100 000-seed sweep reported `two leaders in one term` against a state machine
/// that had done nothing wrong: a node crashed with its vote still queued behind an unfinished
/// fsync, the vote was delivered anyway and completed one quorum, and the node came back having
/// forgotten a vote it had never durably cast and completed a second. Delivering that message is
/// modelling a disk that reports a write it did not keep, and no consensus protocol survives one.
///
/// The seed is pinned rather than described: it is the only thing that proves the repair holds.
#[test]
fn a_send_still_waiting_on_its_fsync_does_not_survive_the_crash() {
    let r = Sim::<Correct>::replay(1_592_682_576, chaos_cfg()).unwrap_or_else(|v| {
        panic!(
            "the seed that exposed the unsound crash model is failing again. If the rule below \
             still holds, this is a real protocol defect; if it does not, the simulator has gone \
             back to letting a crashed node speak.\n{v}"
        )
    });
    assert_eq!(
        r.unsent_at_crash, 1,
        "seed 1592682576 is supposed to crash a node with exactly one message queued behind an \
         unfinished fsync; it destroyed {} instead, so this is no longer the scenario it pins: {r:?}",
        r.unsent_at_crash
    );

    let s = sweep::<Correct>(SEED_SAFETY, 400, &chaos_cfg());
    expect_quiet(&s, "the crash model");
    assert!(
        s.totals.unsent_at_crash > 0,
        "400 seeds never once crashed a node with a message still behind an fsync, so this rule is \
         not being exercised at all: {:?}",
        s.totals
    );
    eprintln!(
        "crash model: {} messages destroyed by a crash before they left the machine, over 400 seeds",
        s.totals.unsent_at_crash
    );
}
