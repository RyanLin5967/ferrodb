//! F9 — agent isolation on a **cluster**: what reaches quorum, what does not, and what a merge's
//! verdict is still worth when the base it was computed against has moved.
//!
//! Design authority: `DISTRIBUTED.md` §F9 and its "What happens to an agent's in-flight branch
//! when its node dies". `DESIGN.md` §4 is the authority for the gate itself and is not superseded.
//!
//! # The three claims this file has to make true
//!
//! **1. An agent's speculative writes never reach a quorum; only the accepted result does.** A
//! fork copies zero data pages, so the agent's branch costs one *metadata* command and the agent
//! is not blocked on it. Its writes cost nothing at all — they land in a private arena that no
//! other node ever sees. The merge is the one thing that blocks on a quorum. That is the whole
//! performance argument, and [`ConsensusCost`] exists so it is a *measurement* rather than a
//! sentence in a README: the tests read those counters instead of asserting on timing.
//!
//! **2. A merge carries the base round its gate verdict was computed against, and a merge whose
//! base is no longer the committed head is RE-EVALUATED, not applied.** `DESIGN.md` §4 already
//! says the gate must run as an optimistic transaction, because base can move under it. On one
//! node that window is closed by `MergeEvaluation`'s base fingerprint. On a cluster it becomes an
//! *ordering* question, and the answer has to be one every node computes identically — a node that
//! decided by re-reading its own storage would decide differently on a follower that holds none of
//! the branch. So the rule is a pure function of the committed log:
//!
//! > `Branch { op: Merge { branch, base_round } }` at round `M` applies **iff** no round in
//! > `(base_round, M)` carries a command that can change what the gate read.
//!
//! Every node applies the same entries in the same order, so every node reaches the same verdict.
//! [`moves_base`] is the classification that decides it, and it is deliberately public: a rule
//! nobody can name is a rule nobody can test.
//!
//! **3. Branch *metadata* is replicated even though branch *contents* are not.** Not as a hedge —
//! `DISTRIBUTED.md` names the reason: the two destructive, unrecoverable decisions in the branch
//! engine are **reaping** (a `BranchId` generation makes a wrong reap permanent) and **extent
//! allocation** (two nodes handing out one physical page produce pages that pass every checksum).
//! Those must be agreed even though the rows they concern are not. [`BranchLedger`] is that
//! agreement, derived from nothing but the committed log.
//!
//! # The cluster's branch namespace, and why it is not the catalog's
//!
//! `LogBranchCatalog` and `MemBranchCatalog` mint branch ids from a node-local `AtomicU64`. Two
//! nodes therefore both mint `b_1`, and a replicated `BranchOp::Fork { child: 1, .. }` from each
//! is the same aliasing failure F4 removed from arena extents and transaction ids. Rather than add
//! a fourth leader-granted counter, the namespace is **partitioned by owner**:
//! [`ClusterBranchId`] packs the node that owns a branch's pages beside that node's own id for it.
//! Collision is impossible by construction rather than detected after the fact, and the id carries
//! a fact worth having — *which node holds the rows* — which is exactly the question a promoted
//! leader has to answer about every branch it inherits.
//!
//! # What happens to an agent's in-flight branch when its node dies
//!
//! The decision is `DISTRIBUTED.md`'s and this file implements it rather than reopening it: **a
//! branch is a transaction.** No database survives the loss of a node mid-transaction with the
//! uncommitted work intact; what survives is what was committed, and here that means what was
//! **merged**. So:
//!
//! * a branch owned by a node other than this one holds **no rows here** — [`BranchLedger`] knows
//!   that from the id alone, and says so rather than letting a caller find out by reading empty;
//! * [`BranchLedger::orphans_of`] names the branches a dead node was working on, so they are
//!   *disposed of by a replicated decision* (`BranchOp::Abandon`) rather than left for two nodes to
//!   disagree about;
//! * the cost is larger here than for an ordinary transaction and is stated rather than buried: an
//!   agent branch holds a 15-minute lease and is expected to run long, so a leader failure can
//!   discard an hour of an agent's work where a normal database would discard milliseconds. The
//!   opt-in that removes it is `BEGIN AGENT SESSION ... DURABLE`, which is meaningless until F10
//!   gives the effect log a durable form.
//!
//! # The one window this cannot close, stated rather than left to be discovered
//!
//! The merge round is the linearization point, and the rows it publishes are ordinary redo that
//! reaches the other nodes as `Command::WalBatch` on a **later** round. Two log entries are not
//! atomic. If the owning node dies in the interval between applying its own merge round and its
//! publish's redo reaching a quorum, a promoted leader holds a branch sealed as merged whose rows
//! never arrived. Nothing *acknowledged* is lost — [`ClusterAgents::merge`] returns only after the
//! publish — but the branch record and the trunk disagree, and no node can repair it, because the
//! branch's rows were node-local to the node that died.
//!
//! Closing it needs the merge command to carry the changeset, or a second variant confirming the
//! rows landed. `consensus/mod.rs` is frozen and carries neither, so this is named here rather
//! than worked around silently. It is the same boundary `... DURABLE` sits on: a branch whose
//! frames are in the log can be merged by whoever is leader, and one whose frames are not, cannot.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::agent_sql::runtime::{AgentRuntime, ExecCtx, RunIdentity, DEFAULT_LEASE_MILLIS};
use crate::agent_sql::session::AgentSession;
use crate::agent_sql::MergeReport;
use crate::branch::types::{BranchId, BranchState};
use crate::consensus::node::{Applier, Node};
use crate::consensus::{BranchOp, Command, Entry, NodeId, Round};
use crate::error::FerroError;

// =================================================================================================
// The cluster's branch namespace
// =================================================================================================

/// How many low bits of a [`ClusterBranchId`] belong to the owning node's own branch id.
///
/// 40 bits is 1.1e12 branches on one node before the namespace is exhausted, against 24 bits —
/// 16.7 million — of node id. Both are far past anything real; the split is stated as a constant
/// so the arithmetic below has one definition rather than three literals.
const LOCAL_BITS: u32 = 40;
const LOCAL_MASK: u64 = (1u64 << LOCAL_BITS) - 1;

/// A branch id in the **cluster's** namespace: the node that owns the branch's pages, and that
/// node's own id for it.
///
/// # Why a packed id and not a leader-granted counter
///
/// F4 made arena extents and transaction ids leader-granted because a node must not issue a value
/// the leader has not given it. Branch ids look like a fourth instance of that and are not, for one
/// reason: an extent and a txn id are *cluster* resources — two nodes issuing one is aliasing on a
/// shared thing — whereas a branch's pages live on exactly one node and are never shipped. The
/// namespace can therefore be partitioned instead of arbitrated, which costs no round trip, cannot
/// be got wrong by a node that has just been promoted and has not yet learned a watermark, and
/// carries the owner in the id where every reader of the replicated log can see it.
///
/// **The trunk is the exception and it is not an exception to the rule.** Trunk is the replicated
/// database itself, the same branch on every node, so it is id `0` everywhere and has no owner.
/// Local id `0` is trunk in every catalog in this crate (`LogBranchCatalog::in_memory` seeds slot
/// 0 and `next_id` starts at 1), so the two agree by construction rather than by convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterBranchId(pub u64);

impl ClusterBranchId {
    /// The trunk: the same branch on every node, owned by none of them.
    pub const TRUNK: ClusterBranchId = ClusterBranchId(0);

    /// The cluster id of `local` as owned by `node`.
    ///
    /// **Refuses** rather than truncates when a local id does not fit. Truncating would alias two
    /// of one node's own branches onto one cluster id, which is the failure this type exists to
    /// make impossible — arriving through the function that exists to prevent it.
    pub fn of(node: NodeId, local: BranchId) -> Result<ClusterBranchId, FerroError> {
        if local.id == 0 {
            return Ok(ClusterBranchId::TRUNK);
        }
        if local.id > LOCAL_MASK {
            return Err(FerroError::Branch(format!(
                "branch id {} does not fit the {LOCAL_BITS}-bit per-node cluster namespace on \
                 {node}: refusing rather than truncating, because a truncated id is two of this \
                 node's branches sharing one cluster identity",
                local.id
            )));
        }
        if node.0 == 0 {
            return Err(FerroError::Branch(format!(
                "node id 0 cannot own a branch: cluster branch id 0 is the trunk, and a node \
                 numbered 0 would mint ids that collide with it. Number cluster nodes from 1, as \
                 {} does",
                NodeId(1)
            )));
        }
        Ok(ClusterBranchId(((node.0 as u64) << LOCAL_BITS) | local.id))
    }

    /// The node whose disk holds this branch's pages, or `None` for the trunk.
    pub fn owner(self) -> Option<NodeId> {
        if self.0 == 0 {
            None
        } else {
            Some(NodeId((self.0 >> LOCAL_BITS) as u32))
        }
    }

    /// The owning node's own id for this branch. `0` for the trunk.
    pub fn local_id(self) -> u64 {
        self.0 & LOCAL_MASK
    }

    /// Whether this node holds the rows behind this branch.
    ///
    /// The whole of the "a branch is a transaction" contract in one predicate: a branch owned by
    /// somebody else has metadata here and nothing else, and a caller that reads it expecting rows
    /// gets an empty answer that looks exactly like a branch with no writes.
    pub fn rows_are_on(self, node: NodeId) -> bool {
        match self.owner() {
            None => true,
            Some(o) => o == node,
        }
    }
}

impl std::fmt::Display for ClusterBranchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.owner() {
            None => f.write_str("trunk"),
            Some(n) => write!(f, "{n}/b{}", self.local_id()),
        }
    }
}

// =================================================================================================
// What moves the base
// =================================================================================================

/// Whether a committed command can change what a verification gate read.
///
/// **This is the rule the re-evaluation decision is made of**, so it is public, exhaustively
/// matched, and pinned by a test that names every variant. An exhaustive `match` and not a
/// `matches!` with a `_` arm: a variant added to `Command` must fail to compile here rather than
/// silently default to "harmless", because defaulting to harmless is the direction that publishes
/// a stale merge.
///
/// | command | moves the base | why |
/// |---|---|---|
/// | `WalBatch` | **yes** | arbitrary row changes on the merge target |
/// | `Catalog` | **yes** | the shape of the tables the gate re-checked its guards against |
/// | `Branch { Merge }` | **yes, conditionally** | a merge that *applies* publishes rows; see below |
/// | `Branch { Fork }` | no | copies zero data pages (`DESIGN.md` criterion 1) |
/// | `Branch { Abandon }` | no | discards a branch's private, unpublished work |
/// | `Branch { Reap }` | no | frees a dead branch's arenas; never removes a published row |
/// | `ArenaGrant` / `TxnIdRange` | no | allocation bookkeeping, no visible state |
/// | `LeaseTick` | no | the cluster clock. A reap *caused* by one is its own command |
/// | `Checkpoint` | no | discards a WAL prefix; no logical change |
/// | `Membership` | no | the voter set |
/// | `NoOp` | no | the term-establishing entry |
///
/// The three `no`s that matter for liveness are `LeaseTick`, `NoOp` and `ArenaGrant`: those are the
/// rounds a healthy idle cluster produces, and classifying them as base-moving would re-evaluate
/// every merge on every cluster for ever. That is why this is a table and not "anything that
/// committed".
///
/// **`Branch { Merge }` is the one conditional entry**, and [`BranchLedger`] owns the condition:
/// a merge that was itself re-evaluated published nothing, so it moved no base. Only the ledger
/// knows which happened, because only the ledger has the history the verdict is computed from.
/// This function answers the question it can answer — *could* this command move the base — and the
/// ledger narrows it.
pub fn moves_base(c: &Command) -> bool {
    match c {
        Command::WalBatch { .. } => true,
        Command::Catalog { .. } => true,
        Command::Branch { op } => match op {
            BranchOp::Merge { .. } => true,
            BranchOp::Fork { .. } | BranchOp::Abandon { .. } | BranchOp::Reap { .. } => false,
        },
        Command::ArenaGrant { .. } => false,
        Command::TxnIdRange { .. } => false,
        Command::LeaseTick { .. } => false,
        Command::Checkpoint => false,
        Command::Membership { .. } => false,
        Command::NoOp => false,
    }
}

// =================================================================================================
// The replicated branch ledger
// =================================================================================================

/// What the cluster agrees about one branch.
///
/// Distinct from `BranchState` in `branch/types.rs`, which is what *this node's* catalog holds:
/// that one knows about pages, arenas and quarantine, none of which any other node can see. This
/// one is the part every node agrees on, and it exists for the two decisions that are destructive
/// and unrecoverable — reaping, and the extent allocation a live branch justifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicatedState {
    /// Forked and not yet disposed of. The owning node may be writing to it right now.
    Live,
    /// Merged at this round. **The linearization point**, and what a promoted leader inherits.
    Merged { at: Round },
    /// Dropped with its work, by a replicated decision rather than by one node's opinion.
    Abandoned { at: Round },
    /// Reaped, at the generation the id slot moved to. A wrong reap is permanent, which is why
    /// this is a committed decision and never a node-local one.
    Reaped { at: Round, generation: u32 },
}

impl ReplicatedState {
    pub fn is_live(&self) -> bool {
        matches!(self, ReplicatedState::Live)
    }
}

/// One branch, as the cluster knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedBranch {
    pub id: ClusterBranchId,
    pub parent: ClusterBranchId,
    /// The owning node's epoch at the fork. Node-local by nature — epochs come from that node's
    /// catalog — and carried so a reader can order one node's forks against each other, never so
    /// two nodes' epochs can be compared.
    pub fork_epoch: u64,
    pub lease_millis: u64,
    /// The round whose `BranchOp::Fork` created this entry.
    pub forked_at: Round,
    pub state: ReplicatedState,
}

/// What a `Branch { op: Merge }` round decided.
///
/// The verdict is a function of the committed log alone, so every node produces the same one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeVerdict {
    /// The base did not move. This round **is** the merge's linearization point; the owning node
    /// publishes and every other node records that the branch merged here.
    Applied { branch: ClusterBranchId, base_round: Round },
    /// Something base-moving committed after the gate read its base, so the verdict this command
    /// carries is a verdict about a database that no longer exists. **Nothing is applied** — on
    /// any node — and the proposer re-evaluates against the base as it now stands.
    ReEvaluate { branch: ClusterBranchId, base_round: Round, moved_at: Round },
    /// The command cannot be applied at all: no committed fork for this branch, or it is not live.
    /// A refusal and not a silent skip, because a merge of a branch nobody agreed exists is a
    /// proposer that has lost track of its own state.
    Refused { branch: ClusterBranchId, why: String },
}

/// What applying one entry did to the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchEffect {
    /// Not a `Command::Branch`. `base_moved` is what [`moves_base`] said about it.
    Other { base_moved: bool },
    /// A round at or below the high-water mark arrived again. A committed round may be
    /// re-delivered, so this is ordinary and must be a **no-op** — a fork applied twice would
    /// refuse itself as a collision, and a merge applied twice would move the base twice.
    AlreadyApplied,
    Forked(ClusterBranchId),
    Merged(MergeVerdict),
    Abandoned(ClusterBranchId),
    Reaped { branch: ClusterBranchId, generation: u32 },
    /// The op could not be applied. Recorded rather than returned as an error, because an
    /// `Applier` that fails stops the node, and one node stopping because another node proposed
    /// nonsense is a cluster taken down by its least careful member.
    Rejected { why: String },
}

/// How many merge verdicts are kept for a proposer to read back.
///
/// Bounded on purpose: an unbounded map grows with the life of the cluster for the benefit of a
/// caller that reads its own verdict microseconds after the round is applied. A proposer that asks
/// for a verdict this many merges later gets `None`, which [`ClusterAgents::merge`] reports as a
/// refusal naming the bound rather than guessing.
const VERDICT_HISTORY: usize = 256;

/// **The cluster's branch metadata, derived from nothing but the committed log.**
///
/// Every node builds this from the same entries in the same order, so every node holds the same
/// answer. That is the property the re-evaluation rule rests on: a decision that consulted local
/// storage would come out differently on a node that holds none of the branch.
#[derive(Debug, Default)]
pub struct BranchLedger {
    branches: BTreeMap<u64, ReplicatedBranch>,
    /// Highest round applied. Rounds arrive once and in order; this is what makes a re-delivery a
    /// no-op instead of a second application.
    last_applied: Round,
    /// The highest applied round that changed what a gate would read. **The whole of the
    /// re-evaluation rule is a comparison against this number.**
    last_base_move: Round,
    verdicts: VecDeque<(Round, MergeVerdict)>,
    /// Ops the log carried that this ledger would not apply, newest last.
    ///
    /// Kept rather than returned as an error: an `Applier` that fails stops its node, and one node
    /// stopping because another proposed nonsense is a cluster taken down by its least careful
    /// member. Kept rather than dropped, because every one of these is a proposer that has lost
    /// track of the cluster's state, and a silent skip is how that goes unnoticed until a reap.
    rejections: VecDeque<String>,
}

impl BranchLedger {
    /// An empty ledger, holding only the trunk.
    ///
    /// The trunk is seeded rather than forked: it exists before the first round, on every node, and
    /// a fork of it in round 1 has to find a parent.
    pub fn new() -> BranchLedger {
        let mut branches = BTreeMap::new();
        branches.insert(
            0u64,
            ReplicatedBranch {
                id: ClusterBranchId::TRUNK,
                parent: ClusterBranchId::TRUNK,
                fork_epoch: 0,
                lease_millis: u64::MAX,
                forked_at: 0,
                state: ReplicatedState::Live,
            },
        );
        BranchLedger {
            branches,
            last_applied: 0,
            last_base_move: 0,
            verdicts: VecDeque::new(),
            rejections: VecDeque::new(),
        }
    }

    pub fn last_applied(&self) -> Round {
        self.last_applied
    }

    /// The highest applied round that moved the base. See [`moves_base`].
    pub fn last_base_move(&self) -> Round {
        self.last_base_move
    }

    pub fn get(&self, id: ClusterBranchId) -> Option<&ReplicatedBranch> {
        self.branches.get(&id.0)
    }

    /// Every branch, in cluster-id order. Ordered because two nodes comparing their ledgers must
    /// compare the same sequence.
    pub fn all(&self) -> impl Iterator<Item = &ReplicatedBranch> {
        self.branches.values()
    }

    /// Every op this ledger refused, oldest first. Bounded by [`VERDICT_HISTORY`] for the same
    /// reason the verdicts are.
    pub fn rejections(&self) -> Vec<String> {
        self.rejections.iter().cloned().collect()
    }

    /// The verdict this round produced, if it is still in the bounded history.
    pub fn verdict_at(&self, round: Round) -> Option<&MergeVerdict> {
        self.verdicts.iter().find(|(r, _)| *r == round).map(|(_, v)| v)
    }

    /// Branches still live whose pages are on `node`.
    pub fn live_owned_by(&self, node: NodeId) -> Vec<ClusterBranchId> {
        self.branches
            .values()
            .filter(|b| b.state.is_live() && b.id.owner() == Some(node))
            .map(|b| b.id)
            .collect()
    }

    /// **The work a dead node took with it.**
    ///
    /// Live branches owned by `dead`. Their rows are gone — they were node-local by design and
    /// were never replicated — so what is left is a disposal decision, and it has to be a
    /// *replicated* one: two nodes disagreeing about whether one of these is live is exit
    /// criterion 9, and the disagreement ends in a reap, which is unrecoverable.
    pub fn orphans_of(&self, dead: NodeId) -> Vec<ClusterBranchId> {
        self.live_owned_by(dead)
    }

    /// Merges that were sealed by a branch owned by `node`. What a promoted leader inherits.
    pub fn merges_owned_by(&self, node: NodeId) -> Vec<(ClusterBranchId, Round)> {
        self.branches
            .values()
            .filter_map(|b| match b.state {
                ReplicatedState::Merged { at } if b.id.owner() == Some(node) => Some((b.id, at)),
                _ => None,
            })
            .collect()
    }

    /// Apply one committed entry.
    ///
    /// Infallible by design: see [`BranchEffect::Rejected`].
    pub fn apply(&mut self, entry: &Entry) -> BranchEffect {
        if entry.round <= self.last_applied {
            return BranchEffect::AlreadyApplied;
        }
        self.last_applied = entry.round;

        let effect = match &entry.command {
            Command::Branch { op } => self.apply_branch(entry.round, op),
            other => {
                let base_moved = moves_base(other);
                if base_moved {
                    self.last_base_move = entry.round;
                }
                BranchEffect::Other { base_moved }
            }
        };
        if let BranchEffect::Rejected { why } = &effect {
            self.rejections.push_back(why.clone());
            while self.rejections.len() > VERDICT_HISTORY {
                self.rejections.pop_front();
            }
        }
        effect
    }

    fn apply_branch(&mut self, round: Round, op: &BranchOp) -> BranchEffect {
        match op {
            BranchOp::Fork { child, parent, fork_epoch, lease_millis } => {
                let id = ClusterBranchId(*child);
                let parent_id = ClusterBranchId(*parent);
                if id == ClusterBranchId::TRUNK {
                    return BranchEffect::Rejected {
                        why: "a fork whose child is the trunk was proposed: the trunk is the \
                              replicated database and is not forkable into"
                            .to_string(),
                    };
                }
                // Not idempotence — a round is applied once, so a second fork of one id is a
                // second *branch* claiming it. Refused: the whole point of the packed namespace is
                // that this cannot happen, so if it does, something upstream is minting ids it does
                // not own and overwriting the first branch would hide it.
                if let Some(existing) = self.branches.get(&id.0) {
                    return BranchEffect::Rejected {
                        why: format!(
                            "round {round} forks {id}, which round {} already created: refusing \
                             rather than overwriting, because the second branch would inherit the \
                             first one's arenas and the reaper frees exactly what a record names",
                            existing.forked_at
                        ),
                    };
                }
                match self.branches.get(&parent_id.0) {
                    Some(p) if p.state.is_live() => {}
                    Some(p) => {
                        return BranchEffect::Rejected {
                            why: format!(
                                "round {round} forks {id} off {parent_id}, which is {:?}",
                                p.state
                            ),
                        }
                    }
                    None => {
                        return BranchEffect::Rejected {
                            why: format!(
                                "round {round} forks {id} off {parent_id}, which no committed \
                                 round created"
                            ),
                        }
                    }
                }
                self.branches.insert(
                    id.0,
                    ReplicatedBranch {
                        id,
                        parent: parent_id,
                        fork_epoch: *fork_epoch,
                        lease_millis: *lease_millis,
                        forked_at: round,
                        state: ReplicatedState::Live,
                    },
                );
                BranchEffect::Forked(id)
            }

            BranchOp::Merge { branch, base_round } => {
                let id = ClusterBranchId(*branch);
                let verdict = self.merge_verdict(round, id, *base_round);
                if let MergeVerdict::Applied { .. } = verdict {
                    if let Some(b) = self.branches.get_mut(&id.0) {
                        b.state = ReplicatedState::Merged { at: round };
                    }
                    // **The conditional half of `moves_base`.** A merge that applied published
                    // rows into its target, so every verdict computed against an earlier base is
                    // now stale. A merge that was re-evaluated published nothing and moves nothing.
                    self.last_base_move = round;
                }
                self.remember(round, verdict.clone());
                BranchEffect::Merged(verdict)
            }

            BranchOp::Abandon { branch } => {
                let id = ClusterBranchId(*branch);
                match self.branches.get_mut(&id.0) {
                    Some(b) if b.state.is_live() => {
                        b.state = ReplicatedState::Abandoned { at: round };
                        BranchEffect::Abandoned(id)
                    }
                    Some(b) => BranchEffect::Rejected {
                        why: format!("round {round} abandons {id}, which is {:?}", b.state),
                    },
                    None => BranchEffect::Rejected {
                        why: format!("round {round} abandons {id}, which no round created"),
                    },
                }
            }

            BranchOp::Reap { branch, generation } => {
                let id = ClusterBranchId(*branch);
                match self.branches.get_mut(&id.0) {
                    // Live or abandoned may both be reaped: a lease expires whether or not anyone
                    // said ABANDON, which is the non-cooperative half of the lease rule.
                    Some(b) => match b.state {
                        ReplicatedState::Reaped { generation: g, .. } => BranchEffect::Rejected {
                            why: format!(
                                "round {round} reaps {id} again; it was already reaped at \
                                 generation {g}. Reaping is destructive and a BranchId generation \
                                 makes it unrecoverable, so a second reap is refused rather than \
                                 repeated"
                            ),
                        },
                        _ => {
                            b.state = ReplicatedState::Reaped { at: round, generation: *generation };
                            BranchEffect::Reaped { branch: id, generation: *generation }
                        }
                    },
                    None => BranchEffect::Rejected {
                        why: format!("round {round} reaps {id}, which no round created"),
                    },
                }
            }
        }
    }

    /// **The rule.** A merge applies only if nothing that could change what its gate read has
    /// committed since the base it read.
    fn merge_verdict(&self, round: Round, id: ClusterBranchId, base_round: Round) -> MergeVerdict {
        match self.branches.get(&id.0) {
            None => {
                return MergeVerdict::Refused {
                    branch: id,
                    why: format!(
                        "round {round} merges {id}, whose fork no committed round created. A \
                         merge of a branch the cluster never agreed exists is refused, not applied"
                    ),
                }
            }
            Some(b) if !b.state.is_live() => {
                return MergeVerdict::Refused {
                    branch: id,
                    why: format!("round {round} merges {id}, which is {:?}", b.state),
                }
            }
            Some(_) => {}
        }
        // Strictly greater. A base move *at* `base_round` is one the gate saw — `base_round` is the
        // committed head the evaluation read, so the round itself is inside the state it read.
        // `>=` here would re-evaluate every merge proposed immediately after any write, for ever.
        if self.last_base_move > base_round {
            MergeVerdict::ReEvaluate { branch: id, base_round, moved_at: self.last_base_move }
        } else {
            MergeVerdict::Applied { branch: id, base_round }
        }
    }

    fn remember(&mut self, round: Round, v: MergeVerdict) {
        self.verdicts.push_back((round, v));
        while self.verdicts.len() > VERDICT_HISTORY {
            self.verdicts.pop_front();
        }
    }
}

// =================================================================================================
// The applier: where the log becomes the ledger
// =================================================================================================

/// Lock a mutex, ignoring poisoning.
///
/// A panic in one merge must not make every later branch decision on this node refuse; the state
/// behind these locks is plain data with no invariant a panic can break halfway. The same argument
/// `cluster::lock` makes for the process authority.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The [`Applier`] that keeps a [`BranchLedger`] in step with the committed log.
///
/// Chains to an inner applier so a server can put its WAL applier behind it in one `Node`: the
/// branch ledger has to see **every** committed round, not only the branch ones, because
/// [`moves_base`] is a statement about the rounds *between* two branch commands.
pub struct BranchApplier {
    ledger: Arc<Mutex<BranchLedger>>,
    next: Option<Box<dyn Applier + Send>>,
}

impl BranchApplier {
    pub fn new(ledger: Arc<Mutex<BranchLedger>>) -> BranchApplier {
        BranchApplier { ledger, next: None }
    }

    /// Hand every entry on to `next` after the ledger has seen it.
    pub fn chained(ledger: Arc<Mutex<BranchLedger>>, next: Box<dyn Applier + Send>) -> BranchApplier {
        BranchApplier { ledger, next: Some(next) }
    }

    pub fn ledger(&self) -> &Arc<Mutex<BranchLedger>> {
        &self.ledger
    }
}

impl Applier for BranchApplier {
    fn apply(&mut self, entry: &Entry) -> Result<(), FerroError> {
        lock(&self.ledger).apply(entry);
        match self.next.as_mut() {
            Some(n) => n.apply(entry),
            None => Ok(()),
        }
    }
}

// =================================================================================================
// The seam to consensus
// =================================================================================================

/// **Everything the agent layer needs from consensus, and nothing more.**
///
/// A trait and not `Node` directly, for the reason the whole of `consensus/` is stepped: a merge
/// rule tested against a real socket is a merge rule tested on whichever interleaving the machine
/// happened to produce. With this seam the race the rule exists for — a base-moving round landing
/// between a gate's read and its merge's commit — is *scripted* rather than waited for.
///
/// It is deliberately not a "consensus client": no membership, no snapshots, no roles beyond the
/// one question a write path has to ask.
pub trait Replicated: Send + Sync {
    /// Ask for a command to be committed, returning the round the leader assigned it.
    ///
    /// **The round is not an acknowledgement.** It is acknowledged when
    /// [`Replicated::committed_head`] reaches it. A non-leader must return
    /// [`FerroError::NotLeader`] rather than accept and drop, or a client cannot tell a dropped
    /// write from a committed one.
    fn propose(&self, c: Command) -> Result<Round, FerroError>;

    /// The highest round known committed.
    fn committed_head(&self) -> Round;

    /// Make progress: deliver time and messages once. The caller loops on it.
    ///
    /// A `pump` rather than a blocking `await_commit(round, timeout)`, because a timeout is a
    /// clock and a clock in this path is what makes a distributed test a sleep. The real driver
    /// polls its socket; a test steps its own scripted log.
    fn pump(&self) -> Result<(), FerroError>;

    /// Who this node believes leads, if anyone.
    fn leader(&self) -> Option<NodeId>;
}

/// [`Replicated`] over the real driver: a real clock, a real socket, a real fsync.
///
/// The mutex is what turns `Node`'s `&mut self` into something an `Arc<dyn Replicated>` can hold.
/// It is not contention worth avoiding: every method here is one turn of a loop that is already
/// serialised by the node's single event queue.
pub struct NodeReplicator {
    node: Mutex<Node<BranchApplier>>,
    poll: Duration,
}

impl NodeReplicator {
    /// `poll` is how long one [`Replicated::pump`] may block waiting for a message. Short enough
    /// that a caller polling in a loop stays responsive; the node never sleeps past its own tick
    /// whatever this says, which is `Node::poll`'s own guarantee.
    pub fn new(node: Node<BranchApplier>, poll: Duration) -> NodeReplicator {
        NodeReplicator { node: Mutex::new(node), poll }
    }

    /// Reach the node directly — for a caller that owns the server loop and needs the parts of
    /// `Node` this seam deliberately does not expose.
    pub fn with_node<R>(&self, f: impl FnOnce(&mut Node<BranchApplier>) -> R) -> R {
        f(&mut lock(&self.node))
    }

    pub fn shutdown(&self) {
        lock(&self.node).shutdown();
    }
}

impl Replicated for NodeReplicator {
    fn propose(&self, c: Command) -> Result<Round, FerroError> {
        let mut n = lock(&self.node);
        // Anything already queued is a refusal nobody read, which can only come from a caller that
        // reached past this seam with `with_node`. Dropped here rather than reported as this
        // proposal's answer: attributing an earlier call's refusal to this one is worse than
        // losing it, and this method is the only proposer that exists in normal operation.
        let _ = n.take_refusals();
        let before = n.last_round();
        let round = n.propose(c)?;
        if let Some(why) = n.take_refusals().into_iter().next() {
            return Err(why);
        }
        if round <= before {
            return Err(FerroError::Internal(format!(
                "the state machine neither assigned a round nor refused this proposal: the log \
                 tail is still {before}. A proposal that is silently dropped is indistinguishable \
                 from one that committed"
            )));
        }
        Ok(round)
    }

    fn committed_head(&self) -> Round {
        lock(&self.node).commit_round()
    }

    fn pump(&self) -> Result<(), FerroError> {
        lock(&self.node).poll(self.poll)
    }

    fn leader(&self) -> Option<NodeId> {
        lock(&self.node).leader()
    }
}

// =================================================================================================
// What consensus costs, measured rather than claimed
// =================================================================================================

/// **The performance claim, as three counters.**
///
/// `DISTRIBUTED.md` §F9: *"an agent's speculative writes never reach a quorum; only the accepted
/// result does. Fan out a hundred agents and consensus is paid once per merge, not once per
/// write."* That is a claim about counts, so it is tested against counts. Asserting it against
/// wall-clock time would pass on a fast machine whatever the code did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConsensusCost {
    /// Commands handed to the replicated log. A fork costs one; a hundred agent writes cost none.
    pub proposals: u64,
    /// **Round trips.** Times a caller was *blocked* until the cluster caught up — a pump loop
    /// that had to run at least once. A fork never enters one; a merge enters exactly one per
    /// attempt, because a merge is the linearization point and nothing weaker will do.
    ///
    /// Counted only when blocking actually happened. A merge whose fork committed while the agent
    /// was working — which is every merge that is not immediate — therefore reads exactly one.
    pub quorum_waits: u64,
    /// Merge attempts discarded because the base moved after the gate read it. Not a failure
    /// count: it is how often the rule this row exists for actually fired.
    pub reevaluations: u64,
}

/// An agent session on a cluster: the node-local session, and what the cluster knows about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSession {
    pub session: AgentSession,
    pub cluster_id: ClusterBranchId,
    /// The round the fork command was assigned. **Not** an acknowledgement — see
    /// [`Replicated::propose`]. The merge is what waits for it.
    pub fork_round: Round,
}

impl ClusterSession {
    pub fn branch(&self) -> BranchId {
        self.session.branch
    }
}

/// A merge, and what the cluster's ordering rule did to it.
#[derive(Debug)]
pub struct ClusterMergeReport {
    /// The node-local report: outcome, per-row results, whether it published.
    pub report: MergeReport,
    /// The committed head the *last* evaluation read, and what the applied command carried.
    pub base_round: Round,
    /// The round that linearized this merge. `None` when the gate declined it, which costs no
    /// consensus at all: nothing is proposed for a merge that would not have been published.
    pub merge_round: Option<Round>,
    /// How many attempts were discarded because the base moved under them.
    pub reevaluations: u32,
}

fn not_leader(id: Option<NodeId>, book: &BTreeMap<NodeId, String>) -> FerroError {
    // `FerroError::NotLeader` documents `leader` as an *address*, because reconnecting is the only
    // thing a refused client can do with it and a node number is not dialable. When this node
    // knows who leads but has no configured client address for them, say exactly that rather than
    // fall back to `None` — `None` means "an election is in progress", and reporting a healthy
    // cluster as mid-election sends an operator looking for a problem that is not there.
    let addr = id.map(|n| match book.get(&n) {
        Some(a) => a.clone(),
        None => format!("{n} (no client address for it is configured on this node)"),
    });
    FerroError::NotLeader { leader: addr }
}

// =================================================================================================
// The coordinator
// =================================================================================================

/// How many pumps of the consensus driver a blocking wait may take before it refuses.
///
/// A bound and not a timeout: a timeout is a clock, and the point of the whole `consensus/` design
/// is that this layer owns none. Large enough that a real localhost round trip never reaches it,
/// finite so a partitioned node refuses instead of hanging a client for ever.
///
/// **D173 made this bound load-bearing where it used to be nearly unreachable.** [`pump_until`] no
/// longer short-circuits on a leadership lapse, so the one caller that now spends the whole budget
/// is a node that is *partitioned* — which is exactly the case the budget was written for, and the
/// case that used to exit early with a `NotLeader` it had no grounds to issue. A deposed but
/// connected node still returns as soon as the round reaches it by replication, which is the
/// common case and costs nothing extra. The wall-clock cost of exhausting it is
/// `budget * NodeReplicator::poll`, and it is paid only by a caller that would otherwise have been
/// told a falsehood.
const DEFAULT_PUMP_BUDGET: u32 = 100_000;

/// How many times a merge may be re-evaluated before it is refused.
///
/// **This is the honest cost of optimistic concurrency and it is a bound, not a bug.** The gate is
/// an optimistic read by `DESIGN.md`'s choice, so a merge racing sustained writes to its own base
/// can be starved; the answer is to say so with the round that moved, not to spin. The mechanism
/// that removes it is ordering the merge ahead of concurrent proposals, which belongs to whoever
/// owns the leader's proposal loop and not to this file.
const DEFAULT_MAX_REEVALUATIONS: u32 = 8;

/// **Agent isolation, on a cluster.**
///
/// Wraps a node-local [`AgentRuntime`] with the two things a cluster adds: a fork's metadata
/// reaches the log without the agent waiting for it, and a merge is ordered by consensus against
/// the base its gate read.
pub struct ClusterAgents {
    node: NodeId,
    runtime: Arc<AgentRuntime>,
    repl: Arc<dyn Replicated>,
    ledger: Arc<Mutex<BranchLedger>>,
    cost: Mutex<ConsensusCost>,
    client_addresses: BTreeMap<NodeId, String>,
    pump_budget: u32,
    max_reevaluations: u32,
}

impl ClusterAgents {
    /// `ledger` must be the same one the node's [`BranchApplier`] holds: the coordinator reads the
    /// verdict the applier computed, and two ledgers would be two answers to a question that has
    /// exactly one.
    pub fn new(
        node: NodeId,
        runtime: Arc<AgentRuntime>,
        repl: Arc<dyn Replicated>,
        ledger: Arc<Mutex<BranchLedger>>,
    ) -> ClusterAgents {
        ClusterAgents {
            node,
            runtime,
            repl,
            ledger,
            cost: Mutex::new(ConsensusCost::default()),
            client_addresses: BTreeMap::new(),
            pump_budget: DEFAULT_PUMP_BUDGET,
            max_reevaluations: DEFAULT_MAX_REEVALUATIONS,
        }
    }

    /// Where a client refused by this node should reconnect, per node id.
    pub fn with_client_addresses(mut self, book: BTreeMap<NodeId, String>) -> ClusterAgents {
        self.client_addresses = book;
        self
    }

    /// Lower the re-evaluation bound. Tests use it to reach the refusal without proposing eight
    /// rounds; a server has no reason to.
    pub fn with_max_reevaluations(mut self, n: u32) -> ClusterAgents {
        self.max_reevaluations = n;
        self
    }

    pub fn with_pump_budget(mut self, n: u32) -> ClusterAgents {
        self.pump_budget = n;
        self
    }

    pub fn id(&self) -> NodeId {
        self.node
    }

    pub fn runtime(&self) -> &Arc<AgentRuntime> {
        &self.runtime
    }

    pub fn ledger(&self) -> &Arc<Mutex<BranchLedger>> {
        &self.ledger
    }

    /// What consensus has cost this coordinator so far. See [`ConsensusCost`].
    pub fn cost(&self) -> ConsensusCost {
        *lock(&self.cost)
    }

    /// Whether this node holds the rows behind a branch, or only its metadata.
    pub fn rows_are_here(&self, id: ClusterBranchId) -> bool {
        id.rows_are_on(self.node)
    }

    /// **Fork: node-local, and the agent is not blocked on the cluster.**
    ///
    /// Three steps in this order, and the order is the whole of it.
    ///
    /// 1. **Refuse if this node does not lead.** Exit criterion 10's anti-vacuity half: a write to
    ///    a follower must be refused, never silently served. A branch created here would take
    ///    writes that no quorum will ever see.
    /// 2. **Create the branch locally.** Zero data pages copied — that is criterion 1 — and zero
    ///    consensus.
    /// 3. **Send the metadata to the log, and do not wait for it.** The fork is replicated because
    ///    reaping and extent allocation must be agreed, not so the agent can start. Nothing a peer
    ///    could say would change the answer: the branch names pages only this node holds. A
    ///    proposal that is *refused* is different — then the branch would be invisible to the
    ///    cluster for ever, so the local one is abandoned and the error is returned.
    pub fn fork(
        &self,
        id: RunIdentity<'_>,
        parent: BranchId,
    ) -> Result<ClusterSession, FerroError> {
        self.require_leader()?;
        let parent_cid = ClusterBranchId::of(self.node, parent)?;

        let session = self.runtime.begin_session_as(id, parent)?;
        let cluster_id = match ClusterBranchId::of(self.node, session.branch) {
            Ok(c) => c,
            Err(e) => {
                let _ = self.runtime.abandon(session.branch);
                return Err(e);
            }
        };
        let fork_epoch = match self.runtime.branches().get(session.branch) {
            Ok(rec) => rec.fork_epoch.0,
            Err(e) => {
                let _ = self.runtime.abandon(session.branch);
                return Err(e);
            }
        };

        let op = BranchOp::Fork {
            child: cluster_id.0,
            parent: parent_cid.0,
            fork_epoch,
            lease_millis: DEFAULT_LEASE_MILLIS,
        };
        match self.repl.propose(Command::Branch { op }) {
            Ok(fork_round) => {
                lock(&self.cost).proposals += 1;
                Ok(ClusterSession { session, cluster_id, fork_round })
            }
            Err(e) => {
                // The cluster will never know about this branch, so neither may this node: a
                // branch the log does not carry cannot be reaped by a replicated decision, and an
                // unreapable branch pins its arenas for ever.
                //
                // A failure to abandon is swallowed in favour of the proposal's own error, which
                // is the one the caller can act on. The record it leaves behind is not lost work:
                // leases are non-cooperative, so it expires and is reaped like any other.
                let _ = self.runtime.abandon(session.branch);
                Err(e)
            }
        }
    }

    /// Drop a branch and its work, by a replicated decision.
    ///
    /// Not awaited, and that is safe in the one direction that matters: if this node dies before
    /// the abandon commits, the branch stays live in the ledger, its lease expires, and it is
    /// reaped — which is the same disposal, reached later. Waiting would buy nothing and would
    /// make `ABANDON` cost a round trip that `MERGE` is supposed to be the only one to pay.
    pub fn abandon(&self, s: &ClusterSession) -> Result<(), FerroError> {
        self.require_leader()?;
        self.runtime.abandon(s.branch())?;
        self.repl.propose(Command::Branch { op: BranchOp::Abandon { branch: s.cluster_id.0 } })?;
        lock(&self.cost).proposals += 1;
        Ok(())
    }

    /// Propose the reap of a branch the cluster has agreed is finished.
    ///
    /// The *decision* is what is replicated; freeing the pages is local and only the owner can do
    /// it. Two nodes must never disagree about whether a branch is live (exit criterion 9), and a
    /// wrong reap is unrecoverable because `BranchId` carries a generation — so this never reads a
    /// clock and never decides on its own. Whoever runs the lease scan proposes; every node
    /// applies the same answer.
    pub fn propose_reap(&self, id: ClusterBranchId, generation: u32) -> Result<Round, FerroError> {
        self.require_leader()?;
        let round =
            self.repl.propose(Command::Branch { op: BranchOp::Reap { branch: id.0, generation } })?;
        lock(&self.cost).proposals += 1;
        Ok(round)
    }

    /// **Merge: the linearization point, and the one thing that costs a quorum.**
    ///
    /// The loop is optimistic concurrency control with the log as its arbiter, and each step is
    /// there for a reason a shorter version gets wrong:
    ///
    /// 1. **Read the committed head, then evaluate.** That head is the base the gate's verdict is
    ///    about, and it is what the command carries. Reading it *after* evaluating would name a
    ///    base the gate did not see.
    /// 2. **A merge the gate declines costs no consensus at all.** Nothing is proposed for a merge
    ///    that would not have been published, so a quarantined branch does not spend a round trip.
    ///    The policy for a declined merge — quarantine, or leave a conflicting branch alive with
    ///    its predicate — is [`AgentRuntime::merge`]'s and is not copied here; this path
    ///    re-evaluates through it rather than keeping a second copy of a decision that has to
    ///    match. One extra evaluation on the *refused* path buys one definition of the policy.
    /// 3. **Propose, then wait for the round to be applied** — not merely committed. The verdict
    ///    is computed by the applier, on every node, from the log; waiting for the commit index
    ///    would read a verdict that has not been computed yet.
    /// 4. **Re-evaluate, never apply, when the base moved.** The command is a no-op on every node,
    ///    including this one, and the next attempt is scored against the base as it now stands.
    /// 5. **Publish only after the verdict.** `publish_evaluation` re-checks the base fingerprint,
    ///    so the node-local window between this node's own evaluate and publish is closed by the
    ///    mechanism that already existed; this loop closes the cluster-wide one between propose
    ///    and commit. Neither subsumes the other: the fingerprint is precise and local, the round
    ///    is coarse and agreed.
    pub fn merge(
        &self,
        ctx: &mut ExecCtx,
        branch: BranchId,
    ) -> Result<ClusterMergeReport, FerroError> {
        // Defence in depth, and honestly labelled as such: `propose` below refuses on a follower
        // too, so removing this line changes no outcome and no mutant kills it. What it buys is
        // that a node that cannot commit does not run a full `evaluate_merge` — two scans of every
        // touched table — to reach a verdict it can never act on.
        self.require_leader()?;
        let cid = ClusterBranchId::of(self.node, branch)?;
        if cid == ClusterBranchId::TRUNK {
            return Err(FerroError::Merge(
                "the trunk is the merge target, not a branch that can be merged".to_string(),
            ));
        }

        // **A held branch is held, on a cluster exactly as on one node.** `evaluate_merge` scores a
        // branch without asking whether it may be merged at all, so a coordinator built out of
        // evaluate-then-publish walks straight through quarantine — and a hold a merge can walk
        // through is advisory, which is not a hold. Caught by
        // `a_merge_the_gate_declines_costs_no_consensus_at_all`, which published a quarantined
        // branch before this guard existed.
        //
        // The refusal itself is `AgentRuntime::merge`'s, reason and all: there is one copy of that
        // wording and this is not a second.
        if self.runtime.branches().get(branch)?.state == BranchState::Quarantined {
            return match self.runtime.merge(ctx, branch) {
                Err(e) => Err(e),
                Ok(_) => Err(FerroError::Internal(format!(
                    "{branch} is quarantined and AgentRuntime::merge published it anyway"
                ))),
            };
        }

        // A merge names a branch the cluster agreed exists. Ordinarily this has been true for as
        // long as the agent has been running and costs nothing; an agent that merges in the same
        // breath as it forks waits here, and that wait is the fork's, not the merge's.
        //
        // A fork the cluster *refused* never arrives, so the wait would otherwise end in a timeout
        // that says nothing about why. The ledger recorded the refusal; report that instead.
        if let Err(timeout) = self.pump_until(&format!("the fork of {cid}"), |l| l.get(cid).is_some())
        {
            let refusal = lock(&self.ledger)
                .rejections()
                .into_iter()
                .rev()
                .find(|r| r.contains(&cid.to_string()));
            return Err(match refusal {
                Some(w) => FerroError::Merge(format!(
                    "{cid} cannot be merged: the cluster refused its fork — {w}"
                )),
                None => timeout,
            });
        }

        let mut reevaluations = 0u32;
        loop {
            let base_round = self.repl.committed_head();
            let eval = self.runtime.evaluate_merge(ctx, branch, &[])?;

            if !eval.is_admissible() {
                drop(eval);
                let report = self.runtime.merge(ctx, branch)?;
                return Ok(ClusterMergeReport {
                    report,
                    base_round,
                    merge_round: None,
                    reevaluations,
                });
            }

            let round = self.repl.propose(Command::Branch {
                op: BranchOp::Merge { branch: cid.0, base_round },
            })?;
            lock(&self.cost).proposals += 1;
            self.pump_until(&format!("round {round}, the merge of {cid}"), |l| {
                l.last_applied() >= round
            })?;

            let verdict = lock(&self.ledger).verdict_at(round).cloned();
            match verdict {
                Some(MergeVerdict::Applied { .. }) => {
                    // **Past this point the branch is sealed on every node**, so a publish that
                    // fails here leaves a merge the cluster agreed on whose rows did not land. The
                    // error says so rather than reading as an ordinary refusal, because the two
                    // call for entirely different actions: an ordinary refusal means run `MERGE`
                    // again, and this one cannot be retried at all — a second merge of a sealed
                    // branch is refused as not-live, by design.
                    //
                    // It is narrow rather than merely unlikely. `publish_evaluation` re-checks the
                    // base fingerprint, and the only writer that could have moved the base is this
                    // process — which cannot, because `ExecCtx` holds `&mut Catalog` and
                    // `pgwire`'s catalog is one `Mutex<Catalog>` shared by every connection, so no
                    // other statement runs at all while this merge holds it. What is left is a
                    // suffix replayed onto this node by a promotion, and an I/O error.
                    let report = self.runtime.publish_evaluation(ctx, eval).map_err(|e| {
                        FerroError::Merge(format!(
                            "round {round} committed the merge of {cid} and every node has sealed                              it, but publishing its rows here then failed: {e}. This merge cannot                              be re-run — the branch is no longer live — and the target does not                              hold what the cluster agreed it would"
                        ))
                    })?;
                    return Ok(ClusterMergeReport {
                        report,
                        base_round,
                        merge_round: Some(round),
                        reevaluations,
                    });
                }
                Some(MergeVerdict::ReEvaluate { moved_at, .. }) => {
                    // **The rule.** The evaluation is dropped, not published: it is a verdict about
                    // a database that no longer exists.
                    drop(eval);
                    reevaluations += 1;
                    lock(&self.cost).reevaluations += 1;
                    if reevaluations > self.max_reevaluations {
                        return Err(FerroError::Merge(format!(
                            "{cid} was re-evaluated {reevaluations} times and its base moved every \
                             time — most recently at round {moved_at}, after the gate read round \
                             {base_round}. Refusing rather than spinning: the gate is an optimistic \
                             read, so a merge racing sustained writes to its own base can be \
                             starved, and saying so is more use than a merge that never returns"
                        )));
                    }
                    continue;
                }
                Some(MergeVerdict::Refused { why, .. }) => {
                    drop(eval);
                    return Err(FerroError::Merge(why));
                }
                None => {
                    // The round was applied and carried something else, or its verdict has aged
                    // out. The first is a real event: a leader that lost office has its
                    // uncommitted tail truncated and replaced, so the round this merge was
                    // assigned can belong to another leader's entry entirely.
                    drop(eval);
                    return Err(FerroError::Merge(format!(
                        "round {round} was applied but carries no verdict for {cid}: either \
                         another leader's log replaced this node's tail at that round, or more \
                         than {VERDICT_HISTORY} merges have been decided since. Nothing was \
                         published; re-run MERGE"
                    )));
                }
            }
        }
    }

    /// **What a promoted leader inherits, and what it does not.**
    ///
    /// `DISTRIBUTED.md`: *a branch is a transaction.* The branches `dead` was working on hold rows
    /// that were node-local by design and are gone with it. What is left is the metadata, and the
    /// only correct disposal is a **replicated** one — two nodes disagreeing about whether one of
    /// these is live ends in a reap, and a reap is unrecoverable.
    ///
    /// Returns the rounds the abandonments were assigned. Refuses, rather than reporting a partial
    /// sweep, if any proposal is refused: a half-disposed set is worse than an untouched one,
    /// because the next caller cannot tell which half.
    pub fn abandon_orphans_of(&self, dead: NodeId) -> Result<Vec<(ClusterBranchId, Round)>, FerroError> {
        self.require_leader()?;
        if dead == self.node {
            return Err(FerroError::Branch(format!(
                "{dead} is this node: a node does not declare itself dead, and abandoning every \
                 branch it is working on is not a recovery step"
            )));
        }
        let orphans = lock(&self.ledger).orphans_of(dead);
        let mut out = Vec::with_capacity(orphans.len());
        for id in orphans {
            let round = self
                .repl
                .propose(Command::Branch { op: BranchOp::Abandon { branch: id.0 } })?;
            lock(&self.cost).proposals += 1;
            out.push((id, round));
        }
        Ok(out)
    }

    fn require_leader(&self) -> Result<(), FerroError> {
        match self.repl.leader() {
            Some(n) if n == self.node => Ok(()),
            other => Err(not_leader(other, &self.client_addresses)),
        }
    }

    /// Pump the driver until `done` holds, counting a quorum wait only if it had to.
    ///
    /// # A leadership lapse observed here does not end the wait
    ///
    /// **Both callers reach this function after their command has already been proposed**, and
    /// [`Replicated::propose`] documents its round as *not* an acknowledgement. So a lapse seen
    /// from inside the wait says nothing whatever about that command: it may already sit on a
    /// quorum's disk and commit under the next leader. This loop used to return `NotLeader` the
    /// moment it saw one, which told the client the operation had **failed** while the cluster
    /// went on to apply it — wrong in the one direction a client cannot recover from, because
    /// "it failed" is exactly what makes it retry an operation that already happened.
    ///
    /// A deposed leader is still a follower. It keeps receiving the new leader's entries, so the
    /// round being waited on can still arrive *here*, by replication, and the honest thing is to
    /// keep looking for it until the budget says stop. The lapse is not discarded — it is named
    /// in the timeout below, where it is a diagnosis rather than a verdict.
    ///
    /// **And continuing is not merely honest, it is the repair.** The merge's rows are node-local
    /// to this node; if the round commits and this node refuses to publish because it lost office,
    /// the cluster holds a branch sealed as merged whose rows never arrived — the one window this
    /// module's header says it cannot close, opened on purpose by a node that was still alive and
    /// still held the rows.
    ///
    /// The cost is stated rather than hidden: a node that is deposed and stays deposed now burns
    /// the whole pump budget before refusing, where it used to refuse at once. That is the price
    /// of not lying, and the bound is still finite.
    ///
    /// The **pre-propose** check in [`ClusterAgents::require_leader`] is untouched and is where a
    /// follower is turned away: a refusal issued before anything is proposed is a true statement
    /// that nothing happened.
    fn pump_until(
        &self,
        what: &str,
        mut done: impl FnMut(&BranchLedger) -> bool,
    ) -> Result<(), FerroError> {
        if done(&lock(&self.ledger)) {
            return Ok(());
        }
        // The last non-leader reading this wait took, if it took one. `Some(_)` is "a lapse
        // happened at all"; the inner value is who led when it was last looked at, which is what
        // a refused client would need to reconnect.
        let mut lapsed: Option<Option<NodeId>> = None;
        for _ in 0..self.pump_budget {
            match self.repl.leader() {
                Some(n) if n == self.node => {}
                other => lapsed = Some(other),
            }
            self.repl.pump()?;
            if done(&lock(&self.ledger)) {
                lock(&self.cost).quorum_waits += 1;
                return Ok(());
            }
        }
        // One copy of the address-book wording, `not_leader`'s: a second would be a second answer
        // to "where does this client reconnect".
        let lapse = match lapsed.map(|o| not_leader(o, &self.client_addresses)) {
            None => String::new(),
            Some(FerroError::NotLeader { leader: Some(addr) }) => format!(
                ", and this node lost the leadership while waiting — the leader is now at {addr}"
            ),
            Some(_) => ", and this node lost the leadership while waiting, with no node known to \
                        lead since — an election is in progress, or this node is partitioned"
                .to_string(),
        };
        Err(FerroError::Merge(format!(
            "{what} was not observed to apply within {} turns of the consensus driver{lapse}. \
             **This is a timeout on OBSERVATION, not a report that nothing happened.** The \
             command was already proposed before this wait began and a round is not an \
             acknowledgement, so it may have reached a quorum and may yet commit. What is known \
             is local: this node published no rows. What is not known, and is not decided here, \
             is whether the cluster applies the operation. Read the ledger for the round before \
             retrying — a second attempt at an operation that committed is not a no-op",
            self.pump_budget
        )))
    }
}
