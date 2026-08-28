//! F5 — single-node membership changes, gated on the previous change being acknowledged durably by
//! a majority of the set that created it.
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
//! majority of that set with one node added or removed intersect**, so no two leaders can be
//! elected against the two sets that are live during one change:
//!
//! ```text
//! |C| = n, |C'| = n+1, C' = C + {x}
//!   a majority of C'  has  floor((n+1)/2)+1  nodes, at most one of which is x,
//!   so at least floor((n+1)/2) of them are in C, and  floor((n+1)/2) + floor(n/2)+1 > n.
//! ```
//!
//! That property is the *whole* reason this row is allowed to be simple, and it holds for **one**
//! change. It says nothing about C and C'' two changes apart — `{1,2,3}` and `{1,2,3,4,5}` have
//! majorities of 2 and 3 which need not intersect, and `{1,2}` and `{3,4,5}` are two leaders of one
//! term with every node behaving correctly given what it believes. Hence:
//!
//! # The precondition, which is the whole row
//!
//! **A new change may not begin until the previous one is acknowledged as durable by a majority of
//! the set that created it.** [`Consensus::acked`](super::Consensus::acked) holds a
//! [`CfgAt`] — a `(version, term)` **pair** — per member for exactly this. A version alone is
//! ambiguous: two *different* configurations can both be version 2, created by different leaders in
//! different terms, one of them uncommitted and later discarded. Counting an acknowledgement of
//! `(2, term 3)` toward `(2, term 5)` is how the precondition silently stops holding, and nothing
//! afterwards can notice, because both leaders count correct majorities of the sets they believe in.
//!
//! Expressed here as three conditions, each with its own named test:
//!
//! 1. this node is the leader — a follower cannot know what is in flight elsewhere;
//! 2. **nothing newer than the applied configuration is in this node's log** — see the seams below;
//! 3. the applied configuration is held durably by a **majority of its own voters**, counted over
//!    `cfg.members()` so that neither a learner nor a departed member can make up the number.
//!
//! # Why a configuration takes effect at COMMIT here, and why that is safe
//!
//! `election.rs` fixes this: `apply_config` is "called when a `Command::Membership` commits". So
//! while a change is in flight `self.cfg` is still the **previous** configuration — which is
//! precisely "the set that created it" that the precondition counts a majority of.
//!
//! This departs from Raft §4.1, where a server uses the newest configuration in its log even
//! uncommitted. The departure is safe *given* one-at-a-time: at most two configurations are ever
//! live, they differ by one node, and by the arithmetic above their majorities intersect. It would
//! not be safe without the precondition, which is the other half of why the precondition is not
//! merely tidiness.
//!
//! # The two seams, and why they exist rather than being inferred
//!
//! `Consensus` **holds no log** — it emits [`Action::Persist`](super::Action::Persist) and never
//! reads back — and its field set is frozen with no field for "the change I proposed but have not
//! committed". Both facts point the same way: the caller owns the log, so the caller is the
//! authority on which configurations are *in* it.
//!
//! * [`Consensus::note_config_in_log`] — the caller reports the newest configuration in **this**
//!   node's durable log. This is what makes condition 2 checkable, and it is the only thing that
//!   can be: an append, a truncation, and a promotion of a node that inherited an uncommitted
//!   `Membership` entry from a dead leader are all invisible from inside the state machine.
//! * [`Consensus::note_config_ack`] — the caller reports the newest configuration a **peer** holds
//!   durably. `Body::AppendResp` is frozen and carries no configuration field, so this is derived
//!   rather than sent: the leader created the entry, so the caller knows which round holds it, and
//!   a peer whose `matched` covers that round holds that entry by the log-matching property.
//!
//! # A learner first, always
//!
//! A node being added joins as a **learner** — replicated to, never counted — and is promoted only
//! once its `matched` reaches the leader's committed round. Counting a node that holds none of the
//! log enlarges the denominator without enlarging the set that can answer, so a 3-node cluster
//! becomes a 4-node one needing 3 of 4 while only 3 nodes can actually reply: **availability falls
//! at the exact moment an operator believes they are raising it.** [`Change`] cannot express
//! adding a voter directly, so this is not a rule to be forgotten at one call site — it is a shape
//! the type does not have.
//!
//! # Refusals reuse `error.rs`'s existing classes on purpose
//!
//! `error.rs` is not this row's file and a new variant there would conflict with every other lane,
//! so: a change asked of a node that does not lead is
//! [`FerroError::NotLeader`], whose `leader` field is left `None` because it is documented as a
//! dialable **address** and `Consensus` holds `NodeId`s — the transport, which holds addresses, can
//! fill it in rather than this layer inventing something a client cannot connect to. Every other
//! refusal is [`FerroError::Constraint`], which is the class for "the request is not valid here";
//! `Internal` would be wrong, because none of these are bugs in ferrodb. The one exception is an
//! **empty** voter set arriving to be applied, which is [`FerroError::Corruption`]: this file
//! refuses to create one, so one turning up in a committed entry is damage rather than a decision.

use super::config::{CfgAt, Config};
use super::{Action, Consensus, NodeId, Role};
use crate::error::FerroError;

#[cfg(test)]
#[path = "tests_membership.rs"]
mod tests_membership;

/// One membership change.
///
/// **This enum cannot express a two-node change, and that is the point.** A joint configuration, a
/// swap, or "add these three" is not a value it has, so no code path can be written that forgets to
/// refuse one. Adding a voter takes two changes — [`Change::AddLearner`] then [`Change::Promote`] —
/// each with its own precondition, because that is what stops an added node from being counted
/// before it holds anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    /// Admit a node as a non-voting learner: replicated to, never counted in any majority.
    AddLearner(NodeId),
    /// Make a caught-up learner a voter. Refused unless its `matched` has reached the leader's
    /// committed round.
    Promote(NodeId),
    /// Remove a voter or a learner. Refused if it would leave no voters at all.
    Remove(NodeId),
}

impl std::fmt::Display for Change {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Change::AddLearner(n) => write!(f, "add {n} as a learner"),
            Change::Promote(n) => write!(f, "promote {n} to voter"),
            Change::Remove(n) => write!(f, "remove {n}"),
        }
    }
}

/// What one node is to a configuration. Used only to count how many nodes a proposed change moves.
///
/// Three states rather than a `bool` because "voter" and "learner" are different memberships, so a
/// promotion is a change of one node's standing exactly as an admission is — and a rule that
/// compared only voter sets would let a single proposal admit a learner *and* promote another node,
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
    /// without consuming anything. The answer is only true of the state it was asked in, which is
    /// why [`Consensus::begin_membership`] re-checks it at the moment the change is actually
    /// begun — the gap between planning and proposing is a real window on a live leader, and a
    /// check performed only in the planner is a check a retry walks around.
    ///
    /// The intended sequence is `plan_change` for the configuration, then the ordinary proposal
    /// path — `step(Event::Propose(Command::Membership { config }))` — because `step` is the only
    /// entry point and a second way to append an entry would be a second place for the rules to be
    /// applied differently.
    pub fn plan_change(&self, change: Change) -> Result<Config, FerroError> {
        let target = self.target_config(change)?;
        self.check_membership(&target)?;
        Ok(target)
    }

    /// Whether a membership change could begin here at all, ignoring what the change would be.
    ///
    /// Separate from [`Consensus::plan_change`] so an operator can be told *why* a cluster is not
    /// accepting changes — "the previous one is not yet on a majority" is a wait, and "this node is
    /// not the leader" is a redirect, and a surface that could only report "refused" would make
    /// those look alike.
    pub fn may_change_membership(&self) -> Result<(), FerroError> {
        self.check_precondition()
    }

    // ---------------------------------------------------------------- the seams the caller drives

    /// The caller reports the newest configuration in **this** node's durable log.
    ///
    /// The caller owns the log, so it is the only authority on this, and this is the fact condition
    /// 2 of the precondition is made of: a leader must not begin a change while one it has already
    /// appended is unacknowledged, and "already appended" is not visible from inside a state
    /// machine that holds no log.
    ///
    /// **It may move down.** A truncation removes a `Membership` entry the leader's successor
    /// overwrote, and a leader that went on believing that change was in flight would refuse every
    /// change for ever — a liveness failure with no way out short of a restart.
    ///
    /// It may **not** move below the applied configuration: a committed entry is never truncated,
    /// so a report below `cfg.at()` cannot be true. Such a report is refused as evidence rather
    /// than acted on, which keeps the guard on the strict side — the direction that refuses a
    /// change rather than admitting one.
    pub fn note_config_in_log(&mut self, at: CfgAt) {
        let floor = self.cfg.at();
        self.acked.insert(self.self_id, at.max(floor));
    }

    /// The caller reports the newest configuration a **peer** holds durably.
    ///
    /// Derived rather than sent: `Body::AppendResp` is frozen and carries no configuration field.
    /// The leader created the entry, so the caller knows the round that holds it, and a peer whose
    /// `matched` covers that round holds that entry — the log-matching property, which is exactly
    /// what makes deriving this sound instead of a guess.
    ///
    /// Monotone for a peer, and only for a peer: durability does not expire, so a report older than
    /// one already held is a reordered message rather than news. (Truncation can strip an
    /// *uncommitted* configuration from a follower, but only a new leader's appends do that, and
    /// this node is not leading then; while it does lead, nothing it replicated is taken back.)
    ///
    /// A node outside the configuration is not recorded at all. The count in
    /// [`Consensus::check_precondition`] filters to the current voters and is the load-bearing
    /// guard; this one keeps the map from accumulating members that left, which on a long-lived
    /// cluster is unbounded growth in a `BTreeMap` that is read on every change.
    pub fn note_config_ack(&mut self, node: NodeId, at: CfgAt) {
        if node == self.self_id {
            // The log is authoritative about this node, and only that report may move down.
            self.note_config_in_log(at);
            return;
        }
        if !self.cfg.is_known(node) {
            return;
        }
        let e = self.acked.entry(node).or_default();
        if at > *e {
            *e = at;
        }
    }

    /// A `Command::Membership` committed: install it.
    ///
    /// This is the *only* way a configuration changes after construction, and it wraps
    /// `election.rs`'s [`Consensus::apply_config`] rather than repeating it — that method owns
    /// clearing `behind`, refreshing per-peer progress, and stepping a leader down that has been
    /// voted out of its own configuration, and a second copy of any of those would be a second
    /// place for them to be got wrong.
    ///
    /// No shape check here, deliberately. Applying is not proposing: a snapshot install (F6) may
    /// carry a configuration many versions ahead, and refusing it because it is not one node away
    /// from this node's would leave a receiver unable to count a majority — which is the very thing
    /// `SnapshotMeta::config` exists to prevent.
    ///
    /// Refuses, rather than warning, in the two cases where installing would do damage: a
    /// configuration older than the one in force (which would move the quorum backwards), and an
    /// empty voter set (a cluster nobody can ever lead — unrecoverable, since no majority exists to
    /// elect the leader that would fix it).
    pub fn apply_committed_config(&mut self, cfg: Config) -> Result<Vec<Action>, FerroError> {
        if cfg.is_empty() {
            return Err(FerroError::Corruption(format!(
                "a committed Membership entry carried an empty voter set (version {}, term {}). \
                 F5 refuses to create one, so this is damage rather than a decision: installing it \
                 would leave a cluster with no majority, and therefore no way to elect the leader \
                 that would repair it.",
                cfg.version, cfg.term
            )));
        }
        let at = cfg.at();
        let held = self.cfg.at();
        if at < held {
            return Err(FerroError::Constraint(format!(
                "refused to install configuration (version {}, term {}) over the newer one already \
                 in force (version {}, term {}): a configuration is replaced wholesale and going \
                 backwards moves the quorum backwards, which is a majority counted against the \
                 wrong number.",
                at.version, at.term, held.version, held.term
            )));
        }
        if at == held {
            // Idempotent on purpose: a caller replaying its committed log on recovery must be able
            // to apply the same configuration twice without a refusal, and re-installing the
            // configuration already in force changes nothing.
            return Ok(Vec::new());
        }

        // Acknowledgements from nodes that are in no configuration any more are dropped. The count
        // filters to the current voters, so this is not what makes the precondition safe -- it is
        // what stops the map from growing without bound as members come and go.
        self.acked.retain(|n, _| cfg.is_known(*n));
        // This node now holds this configuration, durably, by definition: it is applying a
        // committed entry. Recording it here is also what makes "no previous change to wait for"
        // distinguishable from "a previous change nobody has acknowledged" -- see
        // `check_precondition`.
        self.acked.insert(self.self_id, at);

        let mut out = Vec::new();
        self.apply_config(cfg, &mut out);
        Ok(out)
    }

    /// The gate on the proposal path: check the precondition and **begin** the change.
    ///
    /// Called by `replicate.rs`'s `on_propose` when it sees a `Command::Membership`, before the
    /// entry is appended. On `Ok` the change is begun, which is recorded at once as this node
    /// holding that configuration in its log.
    ///
    /// **Recording it before the fsync is deliberate, and it is not the "ack before durable" sin
    /// that `Event::Persisted` exists to prevent.** That sin is counting an un-fsynced entry toward
    /// a *commit*; commits are counted over `Progress::matched` and this touches none of it. The
    /// only thing this record does is refuse the *next* change, so getting it early can only
    /// refuse, never permit — and the window it closes is real: between the proposal and the
    /// caller's report after its fsync, nothing else would say a change was in flight.
    // TRANSITIONAL `dead_code` ALLOW — tracked by ledger row **F-cleanup**, the same set as the two
    // remaining ones in `election.rs`.
    //
    // Its caller does not exist yet: `replicate.rs::on_propose` is `unimplemented!()` on this
    // branch, because F5 is a wave-B row and F2 has not merged. CI builds with `-D dead_code`, so
    // without this the branch cannot be green.
    //
    // Scoped to this one method and NOT to the impl block or the module, deliberately: a wider
    // allow would also silence genuinely dead code added to this file later, which is the defect
    // the gate exists to catch.
    //
    // **Removal condition, exact:** delete it once `on_propose` calls this. If the build still
    // passes with it gone, it was doing nothing; if it fails, `on_propose` is not calling the gate
    // and THAT is the bug — an ungated proposal path means two membership changes can be in flight
    // at once, which no test in this file can see.
    #[allow(dead_code)]
    pub(crate) fn begin_membership(&mut self, cfg: &Config) -> Result<(), FerroError> {
        self.check_membership(cfg)?;
        self.acked.insert(self.self_id, cfg.at());
        Ok(())
    }

    // ---------------------------------------------------------------- the rules

    /// The shape of a proposed configuration: one node, one step, and never an empty voter set.
    ///
    /// Checked here as well as being unrepresentable in [`Change`], because `begin_membership`
    /// receives a `Config` that reached it inside a `Command` — over a transport, from another
    /// process, possibly from an older build. A rule enforced only by the type a *local* caller
    /// uses is not enforced on the path a message takes.
    fn check_shape(&self, cfg: &Config) -> Result<(), FerroError> {
        if cfg.is_empty() {
            return Err(FerroError::Constraint(format!(
                "refused a membership change to an empty voter set: {} would leave a cluster with \
                 no majority and therefore no way to elect the leader that would repair it.",
                describe(cfg)
            )));
        }
        // A change builds on the configuration in force **here**, and carries the term of the
        // leader that created it. Both are what make `CfgAt` a usable identity: a proposal that
        // skipped a version could not be told from one that was created against a set this node
        // has never seen, and one carrying an older term is a replay of a dead leader's change.
        if cfg.version != self.cfg.version + 1 {
            return Err(FerroError::Constraint(format!(
                "refused a membership change at version {} against the configuration in force at \
                 version {}: a change is one step from the set it was created against, and a \
                 version that skips or repeats cannot be told from one built on a set this node \
                 has never held.",
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
        let mut moved: Vec<(NodeId, Standing, Standing)> = Vec::new();
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
        for n in seen {
            let (before, after) = (standing(&self.cfg, n), standing(cfg, n));
            if before != after {
                moved.push((n, before, after));
            }
        }
        if moved.len() != 1 {
            return Err(FerroError::Constraint(format!(
                "refused a membership change that moves {} nodes ({}): changes are one node at a \
                 time, because any majority of a set and any majority of that set with one node \
                 added or removed intersect — and two nodes apart they need not, which is two \
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

        // A promotion is the one move with a precondition of its own: a voter that holds none of
        // the log enlarges the denominator without enlarging the set that can answer.
        let (n, before, after) = moved[0];
        if before == Standing::Learner && after == Standing::Voter {
            self.check_caught_up(n)?;
        }
        // Straight from absent to voter is the same defect without even the learner step, and it is
        // refused whatever built the configuration -- `Change` cannot express it, but a `Command`
        // arriving over a transport can.
        if before == Standing::Absent && after == Standing::Voter {
            return Err(FerroError::Constraint(format!(
                "refused to add {n} directly as a voter: a node being added joins as a learner \
                 first, because counting a node that holds none of the log enlarges the \
                 denominator without enlarging the set that can answer — availability falls at the \
                 moment an operator believes they are raising it."
            )));
        }
        Ok(())
    }

    /// A learner may become a voter only once it holds what the leader has committed.
    fn check_caught_up(&self, n: NodeId) -> Result<(), FerroError> {
        let matched = self.progress.get(&n).map(|p| p.matched);
        match matched {
            Some(m) if m >= self.commit => Ok(()),
            _ => Err(FerroError::Constraint(format!(
                "refused to promote {n} to voter: it holds through round {} and the leader has \
                 committed through round {}. A voter that holds none of the log enlarges the \
                 denominator without enlarging the set that can answer, so the quorum grows while \
                 the number of nodes that can satisfy it does not.",
                matched.map_or("nothing (no replication progress at all)".to_string(), |m| m.to_string()),
                self.commit
            ))),
        }
    }

    /// **The precondition.** Conditions 1 and 3 of the three in the module header; condition 2 is
    /// [`Consensus::change_in_flight`], checked here too.
    fn check_precondition(&self) -> Result<(), FerroError> {
        if self.role != Role::Leader {
            // `leader` is documented as a dialable address and this layer holds NodeIds, so it is
            // left `None` rather than filled with a node number no client can connect to. The
            // transport, which does hold addresses, is where that is answered.
            return Err(FerroError::NotLeader { leader: None });
        }

        let held = self.cfg.at();
        if let Some(in_log) = self.acked.get(&self.self_id) {
            if *in_log > held {
                return Err(FerroError::Constraint(format!(
                    "refused a membership change while the previous one (version {}, term {}) is \
                     still in this node's log unapplied, over the configuration in force (version \
                     {}, term {}). Two changes in flight over an unacknowledged one is how a \
                     single-node change stops being safe: the first and third sets are two apart, \
                     and their majorities need not intersect.",
                    in_log.version, in_log.term, held.version, held.term
                )));
            }
        } else {
            // **No previous change to wait for.** A configuration handed to `Consensus::new` is the
            // operator's assertion about a cluster they are starting, not a change some leader
            // made, so there is no entry for a majority to have acknowledged and nothing this
            // record could be waiting on. Every configuration after it is installed by
            // `apply_committed_config`, which records this node — so this branch is reachable
            // exactly once in a cluster's life, and requiring evidence here would refuse the first
            // change for ever.
            return Ok(());
        }

        let holders = self
            .cfg
            .members()
            .iter()
            .filter(|n| self.acked.get(n).is_some_and(|a| *a >= held))
            .count();
        if !self.cfg.has_quorum(holders) {
            return Err(FerroError::Constraint(format!(
                "refused a membership change while the previous one (version {}, term {}) is held \
                 durably by {} of the {} voters that created it, {} needed. A change may not begin \
                 until the previous one is acknowledged as durable by a majority of the set that \
                 created it — a majority of that set is what any later election must contact, so \
                 it is what makes the election restriction refuse a candidate that has not got the \
                 change.",
                held.version,
                held.term,
                holders,
                self.cfg.len(),
                self.cfg.quorum()
            )));
        }
        Ok(())
    }

    /// Whether this node's log holds a configuration newer than the one applied — a change begun
    /// and not yet finished.
    ///
    /// Reported so a caller can distinguish "wait" from "refused", and read by the tests that pin
    /// the precondition.
    pub fn change_in_flight(&self) -> bool {
        self.acked.get(&self.self_id).is_some_and(|at| *at > self.cfg.at())
    }

    fn check_membership(&self, cfg: &Config) -> Result<(), FerroError> {
        // Order matters for what an operator is told: not-the-leader is a redirect, a malformed
        // change is a bug in the caller, and the precondition is a wait. Reporting the wait first
        // would tell an operator to retry a change that will never be valid.
        if self.role != Role::Leader {
            return Err(FerroError::NotLeader { leader: None });
        }
        self.check_shape(cfg)?;
        self.check_precondition()
    }

    /// What `change` would produce, before any precondition is applied.
    fn target_config(&self, change: Change) -> Result<Config, FerroError> {
        let term = self.hard.term;
        match change {
            Change::AddLearner(n) => {
                match standing(&self.cfg, n) {
                    Standing::Voter => Err(FerroError::Constraint(format!(
                        "refused to add {n} as a learner: it is already a voter, and admitting it \
                         again would shrink the voter set by one while looking like an addition."
                    ))),
                    Standing::Learner => Err(FerroError::Constraint(format!(
                        "refused to add {n} as a learner: it already is one. The change moves \
                         nobody, and a change that moves nobody still burns a version and still \
                         consumes the precondition that serialises real ones."
                    ))),
                    Standing::Absent => Ok(self.cfg.adding_learner(n, term)),
                }
            }
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
                    self.check_caught_up(n)?;
                    Ok(self.cfg.adding(n, term))
                }
            },
            Change::Remove(n) => {
                if standing(&self.cfg, n) == Standing::Absent {
                    return Err(FerroError::Constraint(format!(
                        "refused to remove {n}: it is in no configuration here, so the change \
                         moves nobody while consuming a version and the precondition."
                    )));
                }
                if self.cfg.contains(n) && self.cfg.len() == 1 {
                    return Err(FerroError::Constraint(format!(
                        "refused to remove {n}, the last voter: it would leave a cluster with no \
                         majority and therefore no way to elect the leader that would repair it."
                    )));
                }
                Ok(self.cfg.removing(n, term))
            }
        }
    }
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
