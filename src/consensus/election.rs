//! F1 — who leads. Terms, pre-vote, the election restriction, and the leader lease.
//!
//! **OWNER: agent F1.** Every rule in `DISTRIBUTED.md` §F1 gets a named test here, and a mutant
//! that kills it. The universal term rules are already applied in `mod.rs` before anything here is
//! called — in particular `PreVote` deliberately does NOT raise the receiver's term, and that
//! exception is already handled; do not re-implement it.
//!
//! # The shape of this file
//!
//! Two entry points, [`Consensus::on_tick`] and [`Consensus::on_vote_msg`], and the transitions
//! between the four roles. Everything else is a private helper, because a rule that can be reached
//! by two paths is a rule that can be forgotten on one of them: `may_campaign` is checked in
//! exactly one place, the election restriction is compared in exactly one place, and the term is
//! raised in exactly one place.
//!
//! # Three seams with files that are not built yet
//!
//! This file owns the *rules* about three pieces of state whose *evidence* arrives through another
//! owner's handler. Each is a `pub(crate)` method here, called from there:
//!
//! * [`Consensus::observe_quorum_watermark`] — `replicate.rs` calls this with the `commit` carried
//!   by an `Append`. It is the only thing that clears `unjoined`.
//! * [`Consensus::observe_config_at`] — `replicate.rs`/`snapshot.rs` call this when a peer reports
//!   a configuration newer than ours. It is the only thing that sets `behind`.
//! * [`Consensus::apply_config`] — the caller calls this when a `Command::Membership` commits. It
//!   is the only thing that clears `behind`.
//!
//! The seam is deliberate: the flags gate *campaigning*, so the decision belongs to the election,
//! and the observation belongs to whoever sees the message. Putting the rule beside the
//! observation is how `behind` and `unjoined` end up cleared together, which is the defect
//! `mod.rs` names in the doc comment on `unjoined`.

use super::config::{CfgAt, Config};
use super::replicate::Progress;
use super::{Action, Body, Command, Consensus, Entry, Message, NodeId, Role, Round, Term};

#[cfg(test)]
#[path = "tests_election.rs"]
mod tests_election;

impl Consensus {
    // ---------------------------------------------------------------- the clock

    /// One unit of time. The state machine owns no clock, so this is the *only* thing that makes a
    /// timeout expire, a heartbeat go out, or a lease die.
    pub(crate) fn on_tick(&mut self, out: &mut Vec<Action>) {
        match self.role {
            Role::Leader => self.leader_tick(out),
            Role::Follower | Role::PreCandidate | Role::Candidate => self.voter_tick(out),
        }
    }

    /// A node that is not leading counts down to a campaign.
    fn voter_tick(&mut self, out: &mut Vec<Action>) {
        self.since_heard = self.since_heard.saturating_add(1);
        if self.since_heard < self.election_timeout {
            return;
        }
        if self.may_campaign() {
            self.start_precampaign(out);
            return;
        }
        // Blocked by `behind`, by `unjoined`, or by not being a voter in its own configuration.
        //
        // **Neither flag is cleared here, and that is the rule, not an omission.** Time is not
        // evidence about a configuration or about a log, and a node that campaigned on a timer
        // would fence a healthy leader out of office on every window it drew. The countdown is
        // restarted so the node re-tests on a fresh window instead of on every subsequent tick,
        // and a campaign already in flight is abandoned rather than left to expire — a node that
        // has just learned its configuration is stale must stop asking for votes at once.
        if self.role != Role::Follower {
            let term = self.hard.term;
            self.become_follower(term, None, out);
        }
        self.since_heard = 0;
        self.election_timeout = self.draw_timeout();
    }

    /// A leader's tick: age every peer, check the lease, then heartbeat.
    fn leader_tick(&mut self, out: &mut Vec<Action>) {
        // A leader voted out of its own configuration is not a leader, and this is answered before
        // the lease rather than by it: such a node is no longer one of the voters whose silence the
        // lease measures, so there is no majority to be short of. Answering it second would leave a
        // guard that can never fire, which is not a guard.
        if !self.cfg.contains(self.self_id) {
            let term = self.hard.term;
            self.become_follower(term, None, out);
            return;
        }

        // Every known peer grows one tick more silent. Only an answer resets this, and only the
        // append handler sees answers — `Progress::silent` is the whole seam between this file and
        // `replicate.rs`, and `mod.rs` documents it as "ticks since this peer last answered,
        // feeding the leader's own lease".
        for id in self.peer_ids() {
            if let Some(p) = self.progress.get_mut(&id) {
                p.silent = p.silent.saturating_add(1);
            }
        }

        // **The lease: a leader that stops hearing from a majority stops leading, before anybody
        // tells it.** Without this a partitioned leader keeps serving reads out of the state it
        // held when the partition began, and the same row is served by the old leader and the new
        // one — with no message anywhere in the protocol that could reveal it.
        //
        // The window is `lease`, which is NOT `election_timeout` and NOT `election_base`. They were
        // once one number and that was the defect: a peer drawing the shortest timeout campaigns
        // while a leader on a long one still believes it holds office, producing exactly the
        // two-leader overlap the lease exists to prevent. `lease < election_base <= any drawn
        // timeout`, so this fires strictly before any peer can win.
        self.since_quorum = self.quorum_silence();
        if self.since_quorum >= self.lease {
            let term = self.hard.term;
            self.become_follower(term, None, out);
            return;
        }

        self.since_heartbeat = self.since_heartbeat.saturating_add(1);
        if self.since_heartbeat >= self.heartbeat {
            self.since_heartbeat = 0;
            self.broadcast_heartbeat(out);
        }
    }

    /// How long ago this leader last had a majority behind it, in ticks.
    ///
    /// Derived from the per-peer evidence rather than accumulated in a counter of its own, because
    /// two counters of the same fact drift and the drift is invisible: sorting the voters'
    /// silences and taking the quorum-th smallest *is* the age of the most recent majority
    /// acknowledgement, by definition. Self counts as silence zero — a node always hears itself.
    ///
    /// Only called on a leader that is a voter in its own configuration, so there is a majority to
    /// measure. Read out with `get` rather than indexed anyway: a panic here would take down a
    /// leader on a tick, which is a worse answer to an impossible state than "maximally silent".
    fn quorum_silence(&self) -> u32 {
        let mut ages: Vec<u32> = self
            .cfg
            .members()
            .iter()
            .map(|p| {
                if *p == self.self_id {
                    0
                } else {
                    // A voter with no progress entry has never answered.
                    self.progress.get(p).map_or(u32::MAX, |pr| pr.silent)
                }
            })
            .collect();
        ages.sort_unstable();
        ages.get(self.cfg.quorum().saturating_sub(1)).copied().unwrap_or(u32::MAX)
    }

    /// The heartbeat, which is an `Append` carrying no entries.
    ///
    /// `prev` is this leader's own tail and not the peer's `next - 1`, because `Consensus` holds no
    /// log — it emits `Action::Persist` and never reads back — so the leader's tail is the only
    /// position it can name truthfully. A follower that is behind answers `success: false` with a
    /// `hint`, which is the documented back-up mechanism rather than a special case: "the first
    /// round the follower *can* accept, so a leader backs up in one step".
    ///
    /// Learners are heartbeated too. They receive the log; they are simply never counted.
    fn broadcast_heartbeat(&mut self, out: &mut Vec<Action>) {
        for to in self.peer_ids() {
            out.push(Action::Send(Message {
                from: self.self_id,
                to,
                term: self.hard.term,
                body: Body::Append {
                    prev_round: self.last_round,
                    prev_term: self.last_term,
                    entries: Vec::new(),
                    commit: self.commit,
                },
            }));
        }
    }

    // ---------------------------------------------------------------- vote messages

    /// `PreVote`, `PreVoteResp`, `RequestVote`, `RequestVoteResp`.
    pub(crate) fn on_vote_msg(&mut self, m: Message, out: &mut Vec<Action>) {
        let Message { from, term, body, .. } = m;
        match body {
            Body::PreVote { last_term, last_round } => {
                self.on_pre_vote(from, term, last_term, last_round, out)
            }
            Body::PreVoteResp { granted } => self.on_pre_vote_resp(from, term, granted, out),
            Body::RequestVote { last_term, last_round } => {
                self.on_request_vote(from, last_term, last_round, out)
            }
            Body::RequestVoteResp { granted } => self.on_request_vote_resp(from, term, granted, out),
            // Unreachable: `mod.rs` routes exactly the four bodies above here. An explicit panic so
            // that adding a fifth without routing it fails loudly instead of being dropped.
            other => unreachable!("on_vote_msg received a non-vote body: {other:?}"),
        }
    }

    /// Answer a pre-vote — a question about a term nobody has entered.
    ///
    /// `asked_term` is the candidate's term **plus one**: the hypothetical it is asking about. It
    /// is not adopted, not compared to as a later term, and nothing durable is written, because a
    /// pre-vote promises nothing. That is the whole of pre-vote: a node partitioned away from the
    /// cluster asks whether it *could* win, is told no, and the healthy majority's term is
    /// untouched. Answering with a durable vote, or by stepping into `asked_term`, reintroduces
    /// exactly the disruption pre-vote exists to prevent, through the mechanism meant to stop it.
    fn on_pre_vote(
        &mut self,
        from: NodeId,
        asked_term: Term,
        last_term: Term,
        last_round: Round,
        out: &mut Vec<Action>,
    ) {
        // Three independent reasons to refuse, each load-bearing.
        let granted =
            // (1) The hypothetical must be genuinely ahead. A candidate asking about a term we
            //     are already in or past has nothing to win.
            asked_term > self.hard.term
            // (2) We must not already be served by a leader. This is the wall a partitioned node
            //     campaigns into: without it, a node that lost one link collects pre-votes from a
            //     healthy cluster, raises the term for real, and deposes a leader that never
            //     stopped working.
            && !self.leader_is_live()
            // (3) The election restriction.
            && self.log_is_at_least_as_complete(last_term, last_round);

        // The answer carries the term it is *about*, not ours. A response tagged with the voter's
        // own term could not be matched to the campaign that asked, and matching it is what stops
        // an answer from a previous campaign counting toward this one.
        out.push(Action::Send(Message {
            from: self.self_id,
            to: from,
            term: asked_term,
            body: Body::PreVoteResp { granted },
        }));
    }

    /// Count a pre-vote answer, and raise the term for real once a majority says the campaign is
    /// winnable.
    fn on_pre_vote_resp(&mut self, from: NodeId, asked_term: Term, granted: bool, out: &mut Vec<Action>) {
        if self.role != Role::PreCandidate {
            return;
        }
        // The answer must be about *this* campaign's hypothetical term. An answer to an earlier
        // campaign carries an earlier hypothetical, and counting it would let two half-quorums
        // separated in time add up to one.
        if asked_term != self.hard.term.saturating_add(1) {
            return;
        }
        // A vote from a node outside the configuration this campaign counts against is not counted
        // at all — not as a grant, and not toward the denominator.
        if !self.campaign.as_ref().is_some_and(|c| c.contains(from)) {
            return;
        }
        if !granted {
            return;
        }
        self.votes.insert(from);
        if self.campaign.as_ref().is_some_and(|c| c.has_quorum(self.votes.len())) {
            self.become_candidate(out);
        }
    }

    /// Answer a real vote request. **The grant is made durable before it is sent.**
    fn on_request_vote(&mut self, from: NodeId, last_term: Term, last_round: Round, out: &mut Vec<Action>) {
        // `mod.rs` has already applied the term rules: a lower term was refused and returned, and a
        // higher one already took this node with it — stepping down into it with `voted_for`
        // cleared, because a new term is a new vote. So the only term in play here is our own.
        let unpledged = match self.hard.voted_for {
            None => true,
            // Re-answering the same candidate is not a second vote; it is the same vote, and a
            // transport that duplicates or retries must not turn it into a refusal.
            Some(v) => v == from,
        };

        let granted = unpledged
            // A node still being served by a leader does not help depose it. When the request
            // carried a later term this is already false: stepping down cleared `leader`.
            && !self.leader_is_live()
            && self.log_is_at_least_as_complete(last_term, last_round);

        if granted {
            self.hard.voted_for = Some(from);
            // Having endorsed a candidate, give it a full window to win before standing ourselves.
            self.since_heard = 0;

            // **PersistHardState BEFORE the Send, always, and re-emitted even when the vote is
            // unchanged.** A node that votes, crashes, and comes back having forgotten the vote
            // can vote twice in one term, which elects two leaders of that term — and every one of
            // those leaders behaves correctly given what it believes, so nothing later in the
            // protocol can notice. Re-emitting on a duplicate request costs one fsync and makes
            // the invariant checkable on every single step rather than only on the first: every
            // granted response in this action list is preceded in it by the vote it reports.
            out.push(Action::PersistHardState {
                term: self.hard.term,
                voted_for: Some(from),
            });
        }

        out.push(Action::Send(Message {
            from: self.self_id,
            to: from,
            term: self.hard.term,
            body: Body::RequestVoteResp { granted },
        }));
    }

    /// Count a real vote, and take office once a majority of the captured configuration has voted.
    fn on_request_vote_resp(&mut self, from: NodeId, term: Term, granted: bool, out: &mut Vec<Action>) {
        if self.role != Role::Candidate {
            return;
        }
        if term != self.hard.term {
            return;
        }
        if !self.campaign.as_ref().is_some_and(|c| c.contains(from)) {
            return;
        }
        if !granted {
            return;
        }
        self.votes.insert(from);
        if self.campaign.as_ref().is_some_and(|c| c.has_quorum(self.votes.len())) {
            self.become_leader(out);
        }
    }

    // ---------------------------------------------------------------- transitions

    /// Stand for election *hypothetically*: ask whether a campaign could be won, without entering
    /// the term it would be won in.
    ///
    /// **Nothing durable is written here and the term does not move.** A `PreCandidate` that never
    /// reaches a quorum leaves the cluster exactly as it found it, which is the only reason a node
    /// on the wrong side of a partition may retry for ever without harming anybody.
    fn start_precampaign(&mut self, out: &mut Vec<Action>) {
        self.role = Role::PreCandidate;
        self.leader = None;
        // The configuration this campaign counts against. Re-captured in the step that raises the
        // term (see `become_candidate`), so the two halves of a campaign each count against the
        // set that was in force when they began.
        self.campaign = Some(self.cfg.clone());
        self.votes.clear();
        self.votes.insert(self.self_id);
        self.since_heard = 0;
        self.election_timeout = self.draw_timeout();

        out.push(Action::RoleChanged {
            role: Role::PreCandidate,
            term: self.hard.term,
            leader: None,
        });

        // A single-voter configuration is its own majority; there is nobody to ask.
        if self.campaign.as_ref().is_some_and(|c| c.has_quorum(self.votes.len())) {
            self.become_candidate(out);
            return;
        }

        let asked_term = self.hard.term.saturating_add(1);
        let (last_term, last_round) = (self.last_term, self.last_round);
        for to in self.campaign_voters() {
            out.push(Action::Send(Message {
                from: self.self_id,
                to,
                term: asked_term,
                body: Body::PreVote { last_term, last_round },
            }));
        }
    }

    /// Enter the term for real.
    ///
    /// **One step raises the term, casts this node's vote for itself, and captures the
    /// configuration the votes will be counted against.** They are one step because they are one
    /// decision: a membership change landing between the raise and the capture would move the
    /// denominator underneath a campaign already in flight, and a majority counted against the
    /// wrong number elects two leaders of one term.
    fn become_candidate(&mut self, out: &mut Vec<Action>) {
        self.hard.term = self.hard.term.saturating_add(1);
        self.hard.voted_for = Some(self.self_id);
        self.campaign = Some(self.cfg.clone());
        self.role = Role::Candidate;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.self_id);
        self.since_heard = 0;
        self.election_timeout = self.draw_timeout();

        // Durable before anything is sent, for the same reason as a vote for somebody else: this
        // node's vote for itself is a vote, and forgetting it across a crash lets it be cast again.
        out.push(Action::PersistHardState {
            term: self.hard.term,
            voted_for: Some(self.self_id),
        });
        out.push(Action::RoleChanged {
            role: Role::Candidate,
            term: self.hard.term,
            leader: None,
        });

        if self.campaign.as_ref().is_some_and(|c| c.has_quorum(self.votes.len())) {
            self.become_leader(out);
            return;
        }

        let (term, last_term, last_round) = (self.hard.term, self.last_term, self.last_round);
        for to in self.campaign_voters() {
            out.push(Action::Send(Message {
                from: self.self_id,
                to,
                term,
                body: Body::RequestVote { last_term, last_round },
            }));
        }
    }

    /// Take office.
    fn become_leader(&mut self, out: &mut Vec<Action>) {
        self.role = Role::Leader;
        self.leader = Some(self.self_id);
        // The campaign is over; a late duplicate of its answers must not be counted into anything.
        self.campaign = None;
        self.votes.clear();
        self.since_quorum = 0;
        self.since_heartbeat = 0;

        // A new term knows nothing about any peer's log. `matched` starts at zero — the leader has
        // no evidence — and `next` at its own tail plus one, which is optimism and is corrected by
        // the first refusal. Quorum is counted over `matched` and never over `next`.
        self.progress.clear();
        let next = self.last_round.saturating_add(1);
        for id in self.peer_ids() {
            self.progress.insert(
                id,
                Progress { next, matched: 0, silent: 0, needs_snapshot: false },
            );
        }

        out.push(Action::RoleChanged {
            role: Role::Leader,
            term: self.hard.term,
            leader: Some(self.self_id),
        });

        // **The term-establishing entry, and it is required rather than decorative.** Raft §5.4.2
        // forbids committing an inherited round by counting replicas, so a leader needs a round of
        // its *own* term to commit before any earlier round may commit as a side effect. Without
        // it, a leader that is never given a write cannot advance the commit index at all and its
        // followers never learn what is committed — the cluster is live and stuck at once.
        let round = self.last_round.saturating_add(1);
        let entry = Entry { term: self.hard.term, round, command: Command::NoOp };
        self.last_round = round;
        self.last_term = self.hard.term;
        out.push(Action::Persist { entries: vec![entry] });

        // Assert the office immediately rather than on the next heartbeat boundary: every follower
        // is already counting down, and up to a whole heartbeat interval of that countdown is
        // avoidable.
        self.broadcast_heartbeat(out);
    }

    // ---------------------------------------------------------------- the two flags

    /// Install a configuration, which is the evidence — and the **only** evidence — that clears
    /// `behind`.
    ///
    /// It deliberately does **not** clear `unjoined`. Clearing them together is the defect: knowing
    /// the configuration makes this node's majority the cluster's majority and says nothing
    /// whatever about whether it holds a single round of the log, so a node added to a running
    /// cluster would be told the configuration and campaign on its very next tick — holding
    /// nothing, with a term one above everybody's.
    // TRANSITIONAL `dead_code` ALLOW — one of the set tracked by ledger row **F-cleanup**.
    //
    // These three are the seams between F1's rules and the evidence that drives them, and their
    // callers are not built yet: `replicate.rs` must call `observe_quorum_watermark` and
    // `observe_config_at`, and the command applier must call `apply_config`. CI builds with
    // `-D dead_code`, so without this the branch cannot be green between one lane landing and the
    // next.
    //
    // Scoped to these three methods and NOT to the impl block or the module, deliberately: a wider
    // allow would also silence genuinely dead code added to this file later, which is the defect
    // the gate exists to catch — the lint would read as "on" while protecting nothing.
    //
    // **Removal condition, exact:** delete each attribute once its named caller exists. If the
    // build still passes with it gone, it was doing nothing; if it fails, that caller is missing
    // and THAT is the bug — `replicate.rs` failing to call `observe_quorum_watermark` means no node
    // added to a running cluster can ever campaign, which no test in this file can see.
    #[allow(dead_code)]
    pub(crate) fn apply_config(&mut self, cfg: Config, out: &mut Vec<Action>) {
        self.cfg = cfg;
        self.behind = false;

        if self.role == Role::Leader {
            // Peers may have come or gone. A departed peer's progress is dropped so it cannot be
            // counted; an arrived one starts with no evidence, exactly as at election time.
            let known = self.peer_ids();
            self.progress.retain(|id, _| known.contains(id));
            let next = self.last_round.saturating_add(1);
            for id in known {
                self.progress
                    .entry(id)
                    .or_insert(Progress { next, matched: 0, silent: 0, needs_snapshot: false });
            }
            if !self.cfg.contains(self.self_id) {
                let term = self.hard.term;
                self.become_follower(term, None, out);
            }
        }
    }

    /// A member reported a configuration newer than the one this node holds, so this node now
    /// *knows* it is stale and must not campaign until it has caught up.
    ///
    /// Without this a node with an old configuration stands on its own timeout, raises the term,
    /// and fences a healthy leader out of office — repeatedly, in a livelock where no node with an
    /// up-to-date configuration can hold the office and no node without one can win it.
    #[allow(dead_code)]
    pub(crate) fn observe_config_at(&mut self, at: CfgAt) {
        if at > self.cfg.at() {
            self.behind = true;
        }
    }

    /// The observable that clears `unjoined`, and the only thing that does.
    ///
    /// `watermark` is what a leader's `Append` says a quorum holds — its `commit`. This node's own
    /// store says what it holds — `durable`. The comparison of the two is the evidence: a node
    /// added to a running cluster holds none of the log and must not campaign, and it finds out
    /// that it has caught up by catching up, not by waiting.
    ///
    /// **Never a timer.** A timer would clear the flag on a node that still holds nothing, which is
    /// the case the flag exists for.
    ///
    /// **And a cluster with an empty log reports a watermark of zero**, which every node matches —
    /// so joining an empty cluster clears the flag at once and does not leave a member that can
    /// never stand. That case is why the rule is a comparison and not merely "have some rounds".
    #[allow(dead_code)]
    pub(crate) fn observe_quorum_watermark(&mut self, watermark: Round) {
        if self.unjoined && self.durable >= watermark {
            self.unjoined = false;
        }
    }

    // ---------------------------------------------------------------- small shared rules

    /// **Raft §5.4.1, the election restriction:** a vote is granted only to a candidate whose log
    /// is at least as complete as the voter's.
    ///
    /// Compared as the pair `(term, round)`, lexicographically — **term first**. Getting this wrong
    /// loses acknowledged data, and it loses it silently: a candidate with more rounds but a
    /// staler term holds a suffix that was never committed, and electing it discards a shorter
    /// suffix that *was*. Comparing rounds alone, or the pair in the other order, is the same
    /// defect written two ways.
    fn log_is_at_least_as_complete(&self, cand_term: Term, cand_round: Round) -> bool {
        (cand_term, cand_round) >= (self.last_term, self.last_round)
    }

    /// Whether this node is currently being served by a leader it accepts.
    ///
    /// A leader always hears itself, so `since_heard` never grows while leading and this is true
    /// for the whole of a leader's term — until its lease dies and it steps down, at which point
    /// `leader` is `None` and this is false.
    fn leader_is_live(&self) -> bool {
        self.leader.is_some() && self.since_heard < self.election_timeout
    }

    /// A fresh randomized election timeout in `[base, 2 * base)`.
    ///
    /// Randomized per campaign so two nodes do not campaign in lockstep for ever, which is a split
    /// vote that repeats. Drawn from this node's own owned PRNG, never a clock, so a campaign
    /// replays identically from a seed.
    fn draw_timeout(&mut self) -> u32 {
        let base = self.election_base.max(1);
        base + (self.rng.next_u32() % base)
    }

    /// Everybody this node sends to: voters and learners, minus itself.
    fn peer_ids(&self) -> Vec<NodeId> {
        self.cfg
            .members()
            .iter()
            .chain(self.cfg.learners().iter())
            .copied()
            .filter(|n| *n != self.self_id)
            .collect()
    }

    /// Everybody this campaign asks: the voters of the *captured* configuration, minus itself.
    /// Learners are never asked, because a learner's answer would never be counted.
    fn campaign_voters(&self) -> Vec<NodeId> {
        self.campaign
            .as_ref()
            .map(|c| c.members().iter().copied().filter(|n| *n != self.self_id).collect())
            .unwrap_or_default()
    }
}
