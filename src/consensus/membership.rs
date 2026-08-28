//! F5 — single-node membership changes, gated on the previous change being acknowledged durably by
//! a majority before the next one may begin.
//!
//! **OWNER: agent F5.**
//!
//! # One node at a time, and never joint consensus
//!
//! Joint consensus (Raft §6) is the general answer and it is not the one taken here: it needs a
//! configuration that is *two* voter sets at once, which would make [`super::config::Config`] a
//! pair rather than one immutable value — and `config.rs` exists to say that a voter set which is
//! secretly a pair is how "a majority of the wrong number elects two leaders of one term" gets
//! written. Single-node changes need no such type, because **any majority of a set and any
//! majority of that set with one node's standing changed intersect**:
//!
//! ```text
//! |C| = n, |C'| = n+1, C' = C + {x}
//!   a majority of C'  has  floor((n+1)/2)+1  nodes, at most one of which is x,
//!   so at least floor((n+1)/2) of them are in C, and  floor((n+1)/2) + floor(n/2)+1 > n.
//! ```
//!
//! That holds for **one** change and says nothing two changes apart: `{1,2,3}` and `{1,2,3,4,5}`
//! have majorities of 2 and 3, and `{1,2}` and `{3,4,5}` are two leaders of one term with every node
//! counting a correct majority of the set it believes in. So the rules below exist to make sure that
//! at most two sets are ever countable at once, and that they are adjacent.
//!
//! # The three rules, and what each is for
//!
//! **1. The configuration a node counts against is the newest one in its own durable log** — not
//! the newest one it has *applied*. [`Consensus::note_config_in_log`] installs it, and the leader
//! installs its own the moment it appends one.
//!
//! This is Raft §4.1 and it is not a preference. Tie the denominator to apply progress instead and
//! nothing bounds how far it can lag: a node whose applied configuration is four changes old counts
//! a majority of a set the cluster left, while its *log* is complete enough that no voter can refuse
//! it under the election restriction. Two leaders, disjoint quorums, an acknowledged round lost —
//! and they hold *different* terms, so no per-term uniqueness check anywhere can see it. Tied to the
//! log, the two mechanisms compose instead: the commit quorum of a change and the vote quorum of a
//! campaign are taken over the same configuration and therefore intersect, so a candidate counting
//! against a stale set must be granted by a node holding an entry it lacks, and the election
//! restriction refuses it.
//!
//! It also removes the deadlock the other choice implies. If a node that holds a configuration it
//! has not applied were instead forbidden to campaign, then a leader that dies after replicating a
//! `Membership` entry and before propagating its commit leaves every node that fsynced the entry
//! unable to stand and the entry unable to commit — permanently, on a change (`AddLearner`) that
//! moves no voter at all. A node that holds it simply *uses* it, and elects normally.
//!
//! **2. A new change may not begin until the one in force is held durably by a majority of its own
//! voters.** [`Consensus::acked`](super::Consensus::acked) holds a [`CfgAt`] — a `(version, term)`
//! **pair** — per member for exactly this, and it is the whole of the precondition: because rule 1
//! makes `cfg` the newest configuration in this node's log, "`cfg` is on a majority of `cfg`" *is*
//! "the last change committed", counted the way Raft counts a configuration entry's commit — in the
//! new configuration.
//!
//! The pair and never the version alone: two *different* configurations can both be version 2,
//! created by different leaders in different terms, one of them uncommitted and later discarded.
//! Counting an acknowledgement of `(2, term 3)` toward `(2, term 5)` is how the precondition
//! silently stops holding.
//!
//! Acknowledgements are kept across terms, and the reason is **not** that durability never expires —
//! it does; an uncommitted entry is removed by [`Action::Truncate`](super::Action::Truncate). It is
//! that versions only move forward, so a stale pair is strictly less than any later pair the check
//! asks for. Three things enforce that, and removing any one of them breaks this: `check_shape`'s
//! `version == cfg.version + 1`, `note_config_in_log`'s refusal of a configuration that claims an
//! identity already held by a different one, and rule 2 itself.
//!
//! **3. A leader may not change membership until it has committed an entry of its own term.** This
//! is the erratum to Raft's single-server membership change, and without it the row is unsafe even
//! with rules 1 and 2: a leader elected *without* an earlier leader's uncommitted `Membership`
//! entry counts against the configuration before it, proposes a *different* change from there, and
//! the two configurations one step either side of a common parent can have disjoint majorities —
//! `{1,2,3,4}+{5}` and `{1,2,3,4}+{6}` are each one node from `{1,2,3,4}` and their 3-of-5
//! majorities `{1,2,5}` and `{3,4,6}` share nothing. Committing an entry of its own term first puts
//! that leader's log on a majority, which makes every earlier uncommitted entry unelectable.
//!
//! `Consensus` holds no log, so it cannot see this: the fact is supplied by the caller as
//! [`OwnTermCommitted`], and the caller's simplest sufficient answer is "every entry above `commit`
//! in my log carries `term`". `NotYet` refuses, which is the safe direction.
//!
//! # A learner first, always — and a demotion before a removal
//!
//! A node being added joins as a **learner** — replicated to, never counted — and is promoted only
//! once its `matched` reaches the leader's committed round. Counting a node that holds none of the
//! log enlarges the denominator without enlarging the set that can answer: **availability falls at
//! the exact moment an operator believes they are raising it.**
//!
//! Removal is the mirror, and it is two changes for a reason. A voter is **demoted to learner**
//! first, and only a learner may be removed. A voter dropped in one step is a node that is still
//! running, still holds a configuration containing itself, and is never told otherwise — the leader
//! drops it from `progress` in the same step, so no further `Append` can ever reach it. Demoted
//! first, it *receives* the configuration that demotes it, and a node that is not a voter in its own
//! configuration can never campaign again (`may_campaign`). The pre-vote wall and the election
//! restriction are what make even an untold node harmless; the demotion is what makes a told one
//! provably harmless.
//!
//! # This file adds entry points beside `step`, which is an amendment to the contract
//!
//! `mod.rs` says `step` is "**the only entry point**", and it stays the only entry point for
//! *events*. But the frozen `Event` enum has no variant for "my log now holds this configuration",
//! and `Consensus` holds no log, so the facts in rules 1 and 3 cannot arrive through it. They arrive
//! as method calls instead — and each that can produce work takes `&mut Vec<Action>`, exactly as
//! `apply_config` and `become_follower` do, so the caller appends into the same list it is draining
//! from `step` and there is still only one ordered sequence of actions.
//!
//! # Refusals reuse `error.rs`'s existing classes on purpose
//!
//! `error.rs` is not this row's file and a new variant there would conflict with every other lane,
//! so: a change asked of a node that does not lead is [`FerroError::NotLeader`], whose `leader`
//! field is left `None` because it is documented as a dialable **address** and `Consensus` holds
//! `NodeId`s — the transport, which holds addresses, can fill it in rather than this layer inventing
//! something a client cannot connect to. Every other refusal of a *request* is
//! [`FerroError::Constraint`]. A configuration that is **damage** rather than a request — an empty
//! voter set, or two different configurations claiming one `(version, term)` — is
//! [`FerroError::Corruption`], and it also latches this node out of office: it steps down and sets
//! `behind`, so it can neither lead nor campaign until it installs a configuration that is not
//! damaged. Returning an error to a caller with no obligation attached would leave a node counting
//! majorities against a configuration the cluster has left, which `mod.rs` says nothing later in the
//! protocol can detect.

use super::config::{CfgAt, Config};
use super::{Action, Consensus, NodeId, Role};
use crate::error::FerroError;

#[cfg(test)]
#[path = "tests_membership.rs"]
mod tests_membership;

/// One membership change.
///
/// **This enum cannot express a two-node change**, and adding a voter or removing one each take two
/// of them, so neither the learner step nor the demotion step is a rule that a call site can forget.
/// It constrains only what *this* node plans; `check_shape` is what holds the same line against a
/// `Config` that arrives inside a `Command::Membership`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    /// Admit a node as a non-voting learner: replicated to, never counted in any majority.
    AddLearner(NodeId),
    /// Make a caught-up learner a voter. Refused unless its `matched` has reached the leader's
    /// committed round.
    Promote(NodeId),
    /// Make a voter a non-voting learner — the first half of removing it, and what lets it find out.
    Demote(NodeId),
    /// Remove a learner. A voter must be demoted first.
    Remove(NodeId),
}

impl std::fmt::Display for Change {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Change::AddLearner(n) => write!(f, "add {n} as a learner"),
            Change::Promote(n) => write!(f, "promote {n} to voter"),
            Change::Demote(n) => write!(f, "demote {n} to learner"),
            Change::Remove(n) => write!(f, "remove learner {n}"),
        }
    }
}

/// The caller's answer to the one question about the log that decides whether a membership change is
/// safe and that `Consensus` cannot answer for itself: **has this leader committed an entry of its
/// current term?**
///
/// Equivalently, and this is the cheapest way for a caller to answer it: *is every entry above
/// `commit` in my log of this term?* A caller that cannot tell must answer [`Self::NotYet`], which
/// refuses — see rule 3 in the module header for what goes wrong when a leader changes membership
/// before its own term is established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnTermCommitted {
    Yes,
    NotYet,
}

/// What one node is to a configuration.
///
/// Three states rather than a `bool`, because "voter" and "learner" are different memberships: a
/// promotion and a demotion each move one node's standing exactly as an admission does, and a rule
/// that compared only voter sets would let one proposal admit a learner *and* promote another node,
/// which is two changes in one entry however it is spelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Standing {
    Absent,
    Learner,
    Voter,
}

fn standing(cfg: &Config, n: NodeId) -> Standing {
    if cfg.contains(n) {
        Standing::Voter
    } else if cfg.learners().contains(&n) {
        Standing::Learner
    } else {
        Standing::Absent
    }
}

impl Consensus {
    // ---------------------------------------------------------------- the operator's surface

    /// The configuration `change` would produce here, or the reason it is refused.
    ///
    /// Pure: it decides nothing and records nothing, so an operator surface can ask "could I?"
    /// without consuming anything — and because it is only true of the state it was asked in,
    /// [`Consensus::begin_membership`] re-checks every rule at the moment the change is begun. A
    /// check performed only in the planner is a check a retry walks around.
    ///
    /// The intended sequence is `plan_change` for the configuration, then the ordinary proposal
    /// path — `step(Event::Propose(Command::Membership { config }))` — because a second way to
    /// append an entry would be a second place for these rules to be applied differently.
    pub fn plan_change(
        &self,
        change: Change,
        own_term: OwnTermCommitted,
    ) -> Result<Config, FerroError> {
        // Answered before the change is even shaped: a follower asking what a change would produce
        // is asking about a configuration it may not hold, and "not the leader" is a redirect
        // rather than a complaint about the change.
        if self.role != Role::Leader {
            return Err(FerroError::NotLeader { leader: None });
        }
        let target = self.target_config(change)?;
        self.check_membership(&target, own_term)?;
        Ok(target)
    }

    /// Whether a membership change could begin here at all, whatever the change would be.
    ///
    /// Separate from [`Consensus::plan_change`] so an operator can be told *why* a cluster is not
    /// accepting changes: "not the leader" is a redirect, "the previous change is not yet on a
    /// majority" is a wait, and a surface that could only say "refused" would make those look alike.
    pub fn may_change_membership(&self, own_term: OwnTermCommitted) -> Result<(), FerroError> {
        if self.role != Role::Leader {
            return Err(FerroError::NotLeader { leader: None });
        }
        self.check_own_term(own_term)?;
        self.check_precondition()
    }

    // ---------------------------------------------------------------- the seams the caller drives

    /// The configuration this node was **started with by an operator**, asserted to be held by every
    /// member of it.
    ///
    /// One of exactly two ways a node's configuration acquires provenance, and the caller must use
    /// one of them before any membership change can be judged — see
    /// [`Consensus::note_config_in_log`] for the other. Without provenance
    /// [`Consensus::check_precondition`] **refuses**: a guard that cannot see its input must ask
    /// rather than allow, and "nobody has told me what my log holds" is indistinguishable from "no
    /// change has ever been made" unless the caller says which.
    ///
    /// Handing a configuration to `Consensus::new` is the operator's assertion that they configured
    /// the cluster's members with it, so recording every member as holding it is that assertion
    /// written down — not an exemption from rule 2, which is then satisfied by evidence like any
    /// other configuration's.
    pub fn note_bootstrap_config(&mut self) {
        let at = self.cfg.at();
        let members: Vec<NodeId> = self.cfg.members().to_vec();
        for m in members {
            self.acked.insert(m, at);
        }
    }

    /// **The newest configuration in this node's durable log is now this one.** Installs it.
    ///
    /// Rule 1: this is what the node counts majorities against. The caller owns the log, so the
    /// caller is the only thing that can say. It must be called on exactly four occasions, and each
    /// is a fact `Consensus` cannot see for itself:
    ///
    /// * a `Membership` entry was appended and fsynced — on a follower, from
    ///   `Body::Append { entries }`; on the leader, [`Consensus::begin_membership`] has already
    ///   installed it at append time and this call is what records its *durability*;
    /// * a truncation removed one, and the newest surviving configuration is now an older one — the
    ///   only case in which the value moves **down**, and it must, or a node whose successor
    ///   overwrote its change refuses every change for ever;
    /// * this node started, and its log or its snapshot holds a configuration;
    /// * a snapshot was installed, carrying `SnapshotMeta::config`.
    ///
    /// Refuses **damage** and latches this node out of office when it does — an empty voter set, or a
    /// configuration claiming a `(version, term)` already held by a *different* configuration, which
    /// would break the identity rule 2 rests on. An error alone would leave the node counting
    /// majorities against a configuration the cluster has left, so it also steps down and sets
    /// `behind`: it can then neither lead nor campaign until it installs one that is not damaged.
    pub fn note_config_in_log(
        &mut self,
        cfg: Config,
        out: &mut Vec<Action>,
    ) -> Result<(), FerroError> {
        if cfg.is_empty() {
            return Err(self.latch_damaged(
                format!(
                    "a configuration with an empty voter set reached this node's log ({}). No \
                     majority exists in it, so no leader can ever be elected — including the one \
                     that would repair it. F5 refuses to create one, so this is damage rather than \
                     a decision.",
                    describe(&cfg)
                ),
                out,
            ));
        }
        if cfg.at() == self.cfg.at() {
            if cfg != self.cfg {
                return Err(self.latch_damaged(
                    format!(
                        "two different configurations claim one identity: {} and the {} already in \
                         force. Acknowledgements are matched on that pair, so a collision makes a \
                         majority of one set count as a majority of the other.",
                        describe(&cfg),
                        describe(&self.cfg)
                    ),
                    out,
                ));
            }
            // Identical: the caller is re-reporting, which a recovery replay does. Record the
            // durability and change nothing else.
            self.acked.insert(self.self_id, cfg.at());
            return Ok(());
        }

        // No shape check, and that is a decision rather than an omission. This is not the proposing
        // path: whatever is here has been written to a log, either by a leader that ran
        // `check_shape` or by a snapshot that may legitimately be many versions ahead. **Refusing a
        // configuration the cluster has installed is divergence, not safety** — this node would go
        // on counting majorities against a set the cluster has left, which is the one failure
        // `mod.rs` says nothing later in the protocol can detect. The one-node-at-a-time rule is a
        // proposer-side invariant, enforced on the only node allowed to propose, and keeping a
        // foreign proposer off the wire is F7's job, not this file's.
        let at = cfg.at();
        // Acknowledgements from nodes that are in no configuration any more are dropped. The count
        // in `check_precondition` filters to the current voters, so this is not what makes the
        // precondition safe -- it is what stops the map from growing without bound as members come
        // and go on a long-lived cluster.
        self.acked.retain(|n, _| cfg.is_known(*n));
        self.acked.insert(self.self_id, at);
        self.apply_config(cfg, out);
        Ok(())
    }

    /// The caller reports the newest configuration a **peer** holds durably.
    ///
    /// Derived rather than sent: `Body::AppendResp` is frozen and carries no configuration field.
    /// The caller owns the log, so for each peer it compares `Progress::matched` against the round
    /// of the newest `Membership` entry in *its own* log — **whoever created it**, not only entries
    /// this leader wrote. Scoping it to a leader's own changes would leave every leader elected after
    /// a change unable to gather evidence for the configuration in force, and therefore unable ever
    /// to make another change. A peer whose `matched` covers that round holds that entry, by the
    /// log-matching property.
    ///
    /// Monotone: a report older than one already held is a reordered message, not news. A truncation
    /// *can* make a recorded acknowledgement false, but only a later leader's appends truncate, and
    /// this node is not leading then; what keeps a stale pair harmless afterwards is that versions
    /// only move forward, so it is strictly less than any later pair the check asks for.
    ///
    /// A node outside the configuration is not recorded at all, and this node's own entry is not
    /// settable here — it is a fact about this node's log, and
    /// [`Consensus::note_config_in_log`] is the only thing that may state it.
    pub fn note_config_ack(&mut self, node: NodeId, at: CfgAt) {
        if node == self.self_id || !self.cfg.is_known(node) {
            return;
        }
        let e = self.acked.entry(node).or_default();
        if at > *e {
            *e = at;
        }
    }

    /// Whether the configuration in force is held durably by a majority of its own voters — rule 2,
    /// and therefore whether the last change has committed.
    pub fn config_is_durable_on_a_majority(&self) -> bool {
        if !self.acked.contains_key(&self.self_id) {
            return false;
        }
        let want = self.cfg.at();
        let holders = self
            .cfg
            .members()
            .iter()
            .filter(|n| self.acked.get(n).is_some_and(|a| *a >= want))
            .count();
        self.cfg.has_quorum(holders)
    }

    /// Whether a change is begun and not finished: the configuration in force is not yet known to be
    /// on a majority of its own voters.
    ///
    /// The complement of [`Consensus::config_is_durable_on_a_majority`], named separately because
    /// the two read as opposite questions at a call site and a caller asking "may I change
    /// membership?" should not have to negate.
    pub fn change_in_flight(&self) -> bool {
        !self.config_is_durable_on_a_majority()
    }

    /// The gate on the proposal path: check every rule, and **begin** the change.
    ///
    /// Called by `replicate.rs`'s `on_propose` when it sees a `Command::Membership`, before the entry
    /// is appended. On `Ok` the configuration is installed at once — rule 1: the leader counts
    /// against the newest configuration in its own log from the moment it puts one there, and an
    /// entry it is about to send to its peers is in its log.
    ///
    /// It deliberately does **not** record this node as *holding* it: that is a claim about
    /// durability, `Action::Persist` has not been fulfilled yet, and the acknowledgement that lets
    /// the *next* change begin must wait for the caller's report after its fsync — which is the same
    /// rule as "a follower that acks a round it has not fsynced converts a correlated power loss
    /// into acknowledged data loss", applied to this node.
    // TRANSITIONAL `dead_code` ALLOW — tracked by ledger row **F-cleanup**, the same set as the two
    // remaining ones in `election.rs`.
    //
    // Its caller does not exist yet: `replicate.rs::on_propose` is `unimplemented!()` on this
    // branch, because F5 is a wave-B row and F2 has not merged. CI builds with `-D dead_code`, so
    // without this the branch cannot be green.
    //
    // Scoped to this one method and NOT to the impl block or the module, deliberately: a wider allow
    // would also silence genuinely dead code added to this file later, which is the defect the gate
    // exists to catch.
    //
    // **Removal condition, exact:** delete it once `on_propose` calls this. If the build still passes
    // with it gone, it was doing nothing; if it fails, `on_propose` is not calling the gate and THAT
    // is the bug — an ungated proposal path means two membership changes can be in flight at once,
    // which no test in this file can see.
    #[allow(dead_code)]
    pub(crate) fn begin_membership(
        &mut self,
        cfg: &Config,
        own_term: OwnTermCommitted,
        out: &mut Vec<Action>,
    ) -> Result<(), FerroError> {
        self.check_membership(cfg, own_term)?;
        self.acked.retain(|n, _| cfg.is_known(*n));
        self.apply_config(cfg.clone(), out);
        Ok(())
    }

    // ---------------------------------------------------------------- the rules

    fn check_membership(
        &self,
        cfg: &Config,
        own_term: OwnTermCommitted,
    ) -> Result<(), FerroError> {
        // Order matters for what an operator is told: not-the-leader is a redirect, an unestablished
        // term and an unacknowledged previous change are waits, and a malformed change is a bug in
        // the caller. Reporting a wait first would tell an operator to retry a change that can never
        // be valid.
        if self.role != Role::Leader {
            return Err(FerroError::NotLeader { leader: None });
        }
        self.check_own_term(own_term)?;
        self.check_shape(cfg)?;
        self.check_precondition()
    }

    /// Rule 3: a leader may not change membership until it has committed an entry of its own term.
    fn check_own_term(&self, own_term: OwnTermCommitted) -> Result<(), FerroError> {
        if own_term == OwnTermCommitted::Yes {
            return Ok(());
        }
        Err(FerroError::Constraint(format!(
            "refused a membership change: this leader (term {}) has not committed an entry of its \
             own term, or the caller cannot tell. Until it has, an earlier leader's uncommitted \
             configuration entry may still be elected on, and the two configurations one step \
             either side of a common parent can have disjoint majorities — which is two leaders of \
             one term with every node counting a correct majority of the set it believes in.",
            self.hard.term
        )))
    }

    /// The shape of a proposed configuration: one node's standing, one version, this term, and never
    /// an empty voter set.
    ///
    /// Enforced here rather than only by [`Change`]'s shape because this also runs on the
    /// `Command::Membership` path, where the `Config` is a whole value built by whatever asked. That
    /// path is local — `Event::Propose` is delivered by this node's own caller, and a configuration
    /// that arrived over the wire is inside `Body::Append`'s entries and never reaches here — so the
    /// case this catches is a bug or an older build in *this* process, not a hostile peer.
    fn check_shape(&self, cfg: &Config) -> Result<(), FerroError> {
        if cfg.is_empty() {
            return Err(FerroError::Constraint(format!(
                "refused a membership change to an empty voter set: {} would leave a cluster with \
                 no majority and therefore no way to elect the leader that would repair it.",
                describe(cfg)
            )));
        }
        // A change builds on the configuration in force **here**, and carries the term of the leader
        // that created it. Both are what make `CfgAt` a usable identity: a proposal that skipped a
        // version could not be told from one built against a set this node has never seen, and one
        // carrying an older term is a replay of a dead leader's change.
        if cfg.version != self.cfg.version + 1 {
            return Err(FerroError::Constraint(format!(
                "refused a membership change at version {} against the configuration in force at \
                 version {}: a change is one step from the set it was created against, and a \
                 version that skips or repeats cannot be told from one built on a set this node has \
                 never held.",
                cfg.version, self.cfg.version
            )));
        }
        if cfg.term != self.hard.term {
            return Err(FerroError::Constraint(format!(
                "refused a membership change created in term {} by a leader of term {}: a change \
                 carrying an older term is a replay of one a dead leader began, and counting \
                 acknowledgements of it toward this term's change is exactly the ambiguity the \
                 (version, term) pair exists to remove.",
                cfg.term, self.hard.term
            )));
        }

        // **Exactly one node's standing may differ.** Not "at most one": a proposal that moves
        // nobody still burns a version and still consumes the precondition, so it is a caller bug
        // and is refused as one rather than committed as a no-op.
        let moved = self.standings_moved(cfg);
        if moved.len() != 1 {
            return Err(FerroError::Constraint(format!(
                "refused a membership change that moves {} nodes ({}): changes are one node at a \
                 time, because any majority of a set and any majority of that set with one node's \
                 standing changed intersect — and two nodes apart they need not, which is two \
                 leaders of one term with every node counting a correct majority of the set it \
                 believes in.",
                moved.len(),
                if moved.is_empty() {
                    "nothing changes".to_string()
                } else {
                    moved
                        .iter()
                        .map(|(n, b, a)| format!("{n}: {b:?} -> {a:?}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            )));
        }

        let (n, before, after) = moved[0];
        match (before, after) {
            // Straight from absent to voter is the learner rule skipped, and it is refused whatever
            // built the configuration -- `Change` cannot express it, a hand-built `Command` can.
            (Standing::Absent, Standing::Voter) => Err(FerroError::Constraint(format!(
                "refused to add {n} directly as a voter: a node being added joins as a learner \
                 first, because counting a node that holds none of the log enlarges the \
                 denominator without enlarging the set that can answer — availability falls at the \
                 moment an operator believes they are raising it."
            ))),
            // And straight from voter to absent is the demotion rule skipped.
            (Standing::Voter, Standing::Absent) => Err(FerroError::Constraint(format!(
                "refused to remove voter {n} in one step: a voter is demoted to learner first, so \
                 that it receives the configuration that stops it voting. Dropped outright it keeps \
                 a configuration containing itself, the leader drops it from `progress` in the same \
                 step so no further Append can reach it, and it campaigns at a cluster it has left \
                 for ever."
            ))),
            (Standing::Learner, Standing::Voter) => {
                self.check_not_self(n, "promote")?;
                self.check_caught_up(n)
            }
            (Standing::Voter, Standing::Learner) => self.check_not_self(n, "demote"),
            _ => Ok(()),
        }
    }

    /// A leader may not demote or remove itself.
    ///
    /// Not squeamishness: `election.rs` steps a leader down the moment it is not a voter in its own
    /// configuration, and rule 1 installs the configuration when it is **appended**. So a leader
    /// that demoted itself would stop leading before the entry could commit, leaving a change that
    /// silently did not happen and a term with no leader. Refusing says so; the operator's path is
    /// to let leadership move first and demote the node from its successor.
    fn check_not_self(&self, n: NodeId, verb: &str) -> Result<(), FerroError> {
        if n != self.self_id {
            return Ok(());
        }
        Err(FerroError::Constraint(format!(
            "refused to {verb} {n}, which is this leader: a leader that is not a voter in its own \
             configuration steps down at once, and this configuration takes effect when it is \
             appended — so the change would lose its leader before it could commit and would \
             silently not happen. Let leadership move first, then {verb} this node from its \
             successor."
        )))
    }

    /// A learner may become a voter only once it holds what the leader has committed.
    ///
    /// Satisfiable under load, which is not obvious: `commit` is the quorum-th largest `matched`
    /// among voters, not the leader's tail, so a learner that keeps up with the *slowest member of
    /// the majority* satisfies this. One that cannot is precisely a node that must not be promoted —
    /// as a voter it would be in the denominator while unable to answer.
    ///
    /// A leader whose `commit` is still 0 has committed nothing, so `0 >= 0` passes and a new
    /// cluster can still grow — the same rule, and the same reason, as F1's `unjoined` watermark
    /// clearing at zero. Rule 3 is what stops that from being vacuous on a restarted node with a
    /// long log: by the time a change is allowed at all, this leader has committed an entry of its
    /// own term, so `commit` is past its own no-op.
    fn check_caught_up(&self, n: NodeId) -> Result<(), FerroError> {
        match self.progress.get(&n).map(|p| p.matched) {
            Some(m) if m >= self.commit => Ok(()),
            matched => Err(FerroError::Constraint(format!(
                "refused to promote {n} to voter: it holds through round {} and the leader has \
                 committed through round {}. A voter that holds none of the log enlarges the \
                 denominator without enlarging the set that can answer, so the quorum grows while \
                 the number of nodes that can satisfy it does not.",
                matched.map_or_else(
                    || "nothing (no replication progress at all)".to_string(),
                    |m| m.to_string()
                ),
                self.commit
            ))),
        }
    }

    /// Rule 2: the configuration in force must be held durably by a majority of its own voters.
    fn check_precondition(&self) -> Result<(), FerroError> {
        if self.role != Role::Leader {
            // `leader` is documented as a dialable address and this layer holds NodeIds, so it is
            // left `None` rather than filled with a node number no client can connect to. The
            // transport, which does hold addresses, is where that is answered.
            return Err(FerroError::NotLeader { leader: None });
        }

        // **Absent evidence refuses.** `acked` is not durable state and there is no recovery
        // constructor, so an empty map is the state of every node on every restart -- it cannot be
        // read as "no change has ever been made". A guard that cannot see its input must ask.
        if !self.acked.contains_key(&self.self_id) {
            return Err(FerroError::Constraint(format!(
                "refused a membership change: this node's configuration ({}) has no recorded \
                 provenance. The caller must report either `note_bootstrap_config()` — the \
                 operator started this cluster with it — or `note_config_in_log(..)` — this is what \
                 my log holds — before any change can be judged. Absent evidence refuses rather \
                 than permits: an empty record is also the state of a node that has just restarted \
                 with an unacknowledged change in its log.",
                describe(&self.cfg)
            )));
        }

        let want = self.cfg.at();
        let holders = self
            .cfg
            .members()
            .iter()
            .filter(|n| self.acked.get(n).is_some_and(|a| *a >= want))
            .count();
        if !self.cfg.has_quorum(holders) {
            return Err(FerroError::Constraint(format!(
                "refused a membership change while the configuration in force (version {}, term \
                 {}) is held durably by {} of its {} voters, {} needed. A change may not begin \
                 until the previous one is acknowledged as durable by a majority — that majority is \
                 what any later election must contact, which is what makes the election restriction \
                 refuse a candidate that has not got the change.",
                want.version,
                want.term,
                holders,
                self.cfg.len(),
                self.cfg.quorum()
            )));
        }
        Ok(())
    }

    /// Every node whose standing differs between the configuration in force and `cfg`.
    fn standings_moved(&self, cfg: &Config) -> Vec<(NodeId, Standing, Standing)> {
        let mut seen: Vec<NodeId> = self
            .cfg
            .members()
            .iter()
            .chain(self.cfg.learners())
            .chain(cfg.members())
            .chain(cfg.learners())
            .copied()
            .collect();
        seen.sort();
        seen.dedup();
        seen.into_iter()
            .filter_map(|n| {
                let (before, after) = (standing(&self.cfg, n), standing(cfg, n));
                (before != after).then_some((n, before, after))
            })
            .collect()
    }

    /// A configuration that cannot be installed is damage, and damage takes this node out of office.
    ///
    /// It steps down and sets `behind` — F1's flag for "this node knows its configuration is not the
    /// cluster's", which is exactly true here — so it can neither lead nor campaign until it
    /// installs a configuration that is not damaged. An `Err` alone would be a caller obligation
    /// with nothing enforcing it, and the node would go on counting majorities against a
    /// configuration the cluster may have left.
    fn latch_damaged(&mut self, why: String, out: &mut Vec<Action>) -> FerroError {
        let term = self.hard.term;
        self.become_follower(term, None, out);
        self.behind = true;
        FerroError::Corruption(format!(
            "{why} This node has stepped down and will neither lead nor vote in an election until \
             it installs a configuration that is not damaged."
        ))
    }

    /// What `change` would produce, before any rule is applied.
    fn target_config(&self, change: Change) -> Result<Config, FerroError> {
        let term = self.hard.term;
        match change {
            Change::AddLearner(n) => match standing(&self.cfg, n) {
                Standing::Voter => Err(FerroError::Constraint(format!(
                    "refused to add {n} as a learner: it is already a voter, and admitting it again \
                     would shrink the voter set by one while looking like an addition."
                ))),
                Standing::Learner => Err(FerroError::Constraint(format!(
                    "refused to add {n} as a learner: it already is one. The change moves nobody, \
                     and a change that moves nobody still burns a version and still consumes the \
                     precondition that serialises the real ones."
                ))),
                Standing::Absent => Ok(self.cfg.adding_learner(n, term)),
            },
            Change::Promote(n) => match standing(&self.cfg, n) {
                Standing::Voter => Err(FerroError::Constraint(format!(
                    "refused to promote {n}: it is already a voter."
                ))),
                Standing::Absent => Err(FerroError::Constraint(format!(
                    "refused to promote {n}: it is in no configuration here. A node being added \
                     joins as a learner first and is promoted only once its `matched` reaches the \
                     leader's committed round — there is nothing to promote and no progress to \
                     measure."
                ))),
                Standing::Learner => {
                    self.check_not_self(n, "promote")?;
                    self.check_caught_up(n)?;
                    Ok(self.cfg.adding(n, term))
                }
            },
            Change::Demote(n) => match standing(&self.cfg, n) {
                Standing::Learner => Err(FerroError::Constraint(format!(
                    "refused to demote {n}: it is already a learner and votes in nothing."
                ))),
                Standing::Absent => Err(FerroError::Constraint(format!(
                    "refused to demote {n}: it is in no configuration here."
                ))),
                Standing::Voter => {
                    // No separate "last voter" guard, and its absence is deliberate: a leader is
                    // always a voter in its own configuration, so the only voter of a one-voter
                    // cluster IS the leader and `check_not_self` refuses it. A guard that cannot
                    // fire is not a guard. The empty-voter-set outcome is held by `check_shape`,
                    // which is on the path a hand-built `Config` takes.
                    self.check_not_self(n, "demote")?;
                    Ok(demoting(&self.cfg, n, term))
                }
            },
            Change::Remove(n) => match standing(&self.cfg, n) {
                Standing::Voter => Err(FerroError::Constraint(format!(
                    "refused to remove voter {n}: demote it to learner first, so that it receives \
                     the configuration that stops it voting. A voter dropped outright is never \
                     told: it keeps a configuration containing itself and campaigns at a cluster it \
                     has left for ever."
                ))),
                Standing::Absent => Err(FerroError::Constraint(format!(
                    "refused to remove {n}: it is in no configuration here, so the change moves \
                     nobody while consuming a version and the precondition."
                ))),
                Standing::Learner => Ok(self.cfg.removing(n, term)),
            },
        }
    }
}

/// The set `cfg` would become with `n` moved from voter to learner.
///
/// The one shape `config.rs` does not provide, built out of its own constructors rather than by
/// adding a method to a file this row does not own — and it must be built from them, because
/// `Config::new` is what sorts and de-duplicates the voter set and `with_learners` is what refuses
/// to list a voter as a learner. Reimplementing either here would be a second answer to "what is a
/// voter set", which is the thing `config.rs` exists to have exactly one of.
fn demoting(cfg: &Config, n: NodeId, term: u64) -> Config {
    let members = cfg.members().iter().copied().filter(|x| *x != n);
    let mut learners: Vec<NodeId> = cfg.learners().to_vec();
    learners.push(n);
    Config::new(members, cfg.version + 1, term).with_learners(learners)
}

/// A configuration as an operator would read it in a refusal.
fn describe(cfg: &Config) -> String {
    let voters: Vec<String> = cfg.members().iter().map(|n| n.to_string()).collect();
    let learners: Vec<String> = cfg.learners().iter().map(|n| n.to_string()).collect();
    format!(
        "version {} term {}, voters [{}], learners [{}]",
        cfg.version,
        cfg.term,
        voters.join(" "),
        learners.join(" ")
    )
}
