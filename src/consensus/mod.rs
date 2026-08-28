//! Phase F — consensus. The part of ferrodb that decides **who leads** and **what is committed**.
//!
//! This module is the shared contract for the whole distribution layer. Everything else in
//! `consensus/` is an `impl Consensus` block in a sibling file; the *fields* all live here, because
//! a state machine whose invariants are spread across files is one whose invariants are not
//! checkable.
//!
//! # It is stepped, never self-driving
//!
//! [`Consensus::step`] is the only entry point. The state machine:
//!
//! * **never reads a clock** — time arrives as [`Event::Tick`], delivered by whoever owns time;
//! * **never dials, listens or spawns** — it *returns* [`Action::Send`] and the caller sends;
//! * **never touches the disk** — it returns [`Action::Persist`] and the caller reports back with
//!   [`Event::Persisted`].
//!
//! That is not a style preference, it is the reason any of this can be tested. Every claim
//! consensus makes is a claim about what happens during a partition, a reorder, or a crash, and
//! none of those are observable from outside a real socket. Because time and the network belong to
//! the caller, `sim.rs` can replay an entire campaign — partition, split vote, stale-log candidate —
//! from a seed. A state machine that calls `SystemTime::now()` internally can only be tested by
//! sleeping, and a distributed protocol tested by sleeping is tested on the happy path.
//!
//! # What a node counts in: rounds, not LSNs
//!
//! **ferrodb's LSN is a byte offset** (`wal/log.rs`), and an offset is node-local. Three separate
//! reasons it cannot be the thing nodes agree on:
//!
//! 1. Two nodes holding the same records hold them at *different* offsets, so nothing derived from
//!    one node's offset means anything on another.
//! 2. A follower given offsets cannot tell a **hole** from a **gap**, because no record names the
//!    offset before it. [`crate::replication::ReplicaApplier::apply`] already meets this and can
//!    only answer by latching `diverged`, whose sole remedy is a fresh base backup — which in a
//!    cluster would mean re-seeding a node on every ordinary leader change.
//! 3. A **checkpoint truncates the WAL**, so neither an offset nor a record count survives one.
//!
//! So a [`Round`] is the unit: contiguous, identical on every node that applied the same rounds in
//! the same order, and recoverable after both a crash and a checkpoint. The round after `n` is
//! `n + 1`, so a hole is visible by arithmetic instead of by reconciling two files.
//!
//! The WAL is *not* replaced. A round's payload is usually [`Command::WalBatch`], the same redo
//! bytes replication ships today — now quorum-committed instead of best-effort, and safe to
//! re-deliver because ferrodb's redo is idempotent by page LSN.

use crate::error::FerroError;

pub mod config;
pub mod election;
pub mod log;
pub mod membership;
/// The driver: the clock, the socket and the disk. `Consensus` performs none of the three, so
/// nothing runs the state machine without this. Declared here because a module `mod.rs` does not
/// name is not compiled at all.
pub mod node;
pub mod replicate;
pub mod signing;
/// F8 -- the deterministic simulator. Declared here because a module that `mod.rs` does not name is
/// not compiled at all, and nothing outside this file can declare a child of `consensus`. This one
/// line is the whole of F8's edit to the frozen contract: no type, no variant, no `Consensus` field.
pub mod sim;
pub mod snapshot;
pub mod transport;

#[cfg(test)]
mod tests_contract;

/// A leader's fencing token. Raised by an election, never lowered.
pub type Term = u64;

/// Position in the replicated log. Contiguous from 1; round 0 means "before the log begins".
pub type Round = u64;

/// Stable for the life of a cluster. A reused id is a second node claiming another's votes, so
/// ids are assigned by configuration and never recycled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u32);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// What one round commits. The state machine that applies these is ferrodb's ordinary local
/// storage — this enum is deliberately wide enough to carry the state that is node-local today and
/// must become cluster state, each variant closing a named hole.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// The leader's redo stream: raw WAL frames beginning at the leader's `start_lsn`.
    ///
    /// The LSN travels for the *applier's* benefit — it is how the existing
    /// [`crate::replication::ReplicaApplier`] places the bytes — and is explicitly **not** the
    /// thing nodes agree on. Agreement is on the round that carries it.
    WalBatch { start_lsn: u64, bytes: Vec<u8> },

    /// A schema change, as a replicated decision.
    ///
    /// Closes the hole named by the live test `the_catalog_is_not_replicated_and_that_is_a_stated_limit`.
    /// `RecKind::Ddl`'s own doc says the catalog "is written outside the WAL ... recovery does not
    /// replay it and the catalog is authoritative for the running database". A cluster cannot have
    /// a node-authoritative catalog: a follower promoted to leader would serve a schema no other
    /// node has. The existing `RecKind::Ddl` feed record stays exactly where it is, still for
    /// readers of the log; this is the *driving* record it never was.
    /// `columns` is `(name, type, nullable)`, matching [`crate::wal::log::RecKind::Ddl`] exactly so
    /// the two descriptions of one schema cannot drift.
    ///
    /// Deliberately **logical**: it carries no `dir_root` or `time_travel_root`, because those are
    /// page ids and a page id is node-local. Each node applies the DDL and computes its own roots.
    /// Shipping the leader's roots would make every follower's catalog a copy of the leader's
    /// physical layout, which is the same mistake as agreeing on byte offsets.
    Catalog {
        op: crate::wal::log::DdlOp,
        table: String,
        columns: Vec<(String, crate::catalog::column::DataType, bool)>,
    },

    /// Creation, fork, merge, abandonment or reaping of a branch.
    ///
    /// Branch *metadata* is cluster state; branch *contents* are not. A fork copies zero data pages
    /// (`DESIGN.md` criterion 1), so replicating the decision is cheap and replicating the pages
    /// would be the thing that made agent isolation expensive.
    Branch { op: BranchOp },

    /// A leader-granted extent range. **Without this, two nodes hand the same physical page to
    /// different branches.**
    ///
    /// `branch/arena.rs` allocates from a node-local `AtomicU32`, and `examples/repl_primary.rs`
    /// names the consequence exactly: "every such page still passes its checksum, so refusing here
    /// is the only detection point." A node holding no grant must refuse to allocate rather than
    /// fall back to its own counter — a fallback is the failure, not the guard.
    ArenaGrant { node: NodeId, first_page: u32, page_count: u32 },

    /// A leader-granted transaction-id range, for the same reason.
    ///
    /// The TEL's `stamp()` leads with `TxnId` to order writes across branches (ledger R8), so two
    /// nodes independently issuing txn 5 does not merely duplicate an integer — it silently
    /// corrupts merge ordering, which is the one thing the merge engine cannot detect.
    TxnIdRange { node: NodeId, lo: u64, hi: u64 },

    /// The cluster's opinion of the current time, for lease expiry.
    ///
    /// `branch/types.rs` computes `lease_deadline` from `SystemTime::now()`. Wall clocks disagree,
    /// and reaping is **destructive** — `BranchId` carries a generation precisely so a reaped id
    /// can never be mistaken for live, which also means a wrongly-reaped branch is unrecoverable.
    /// So no node reaps on its own clock; expiry is a replicated decision like any other.
    LeaseTick { unix_millis: u64 },

    /// Discard the WAL prefix, on every node at the same round.
    ///
    /// **Checkpointing has to be a replicated decision**, which is not obvious and is the reason
    /// this variant exists. `TxnManager` checkpoints on a node-local counter and calls
    /// `wal.truncate`; two nodes doing that at different moments have different WAL byte streams
    /// from then on. Since an LSN is an offset into that stream, a follower promoted to leader
    /// would then append into an offset space its own followers do not share.
    ///
    /// The alternative — letting each node checkpoint freely and having followers rewrite LSNs into
    /// their own space — is not available: page LSNs live *inside pages*, so rewriting them changes
    /// page contents and destroys the idempotence the applier depends on.
    Checkpoint,

    /// A voter-set change. See `membership.rs` for why these are single-node.
    Membership { config: config::Config },

    /// The term-establishing entry a new leader appends before it may commit anything.
    ///
    /// Required, not decorative: Raft §5.4.2 forbids committing an inherited round by counting
    /// replicas, so a leader needs a round *of its own term* to commit before earlier rounds can
    /// commit as a side effect. Without it a leader that never gets a write cannot advance the
    /// commit index at all, and its followers never learn what is committed.
    NoOp,
}

/// The branch-lifecycle half of [`Command::Branch`].
#[derive(Debug, Clone, PartialEq)]
pub enum BranchOp {
    Fork { child: u64, parent: u64, fork_epoch: u64, lease_millis: u64 },
    Merge { branch: u64, base_round: Round },
    Abandon { branch: u64 },
    Reap { branch: u64, generation: u32 },
}

/// One entry in the replicated log.
///
/// `term` is the term of the leader that *created* the entry, not the term that replicated it.
/// That distinction is the whole of the log-matching property: two entries with the same
/// `(term, round)` hold the same command on every node, and every entry before them matches too.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub term: Term,
    pub round: Round,
    pub command: Command,
}

/// Which office a node currently holds.
///
/// `PreCandidate` is a real role and not a bookkeeping flag: a node in it has **not** raised its
/// term. That is the entire point of pre-vote — a node partitioned away from the cluster would
/// otherwise campaign into a wall, raise the term on every retry, and force a term change on the
/// healthy majority the moment the partition heals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Role::Follower => "follower",
            Role::PreCandidate => "pre-candidate",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
        };
        f.write_str(s)
    }
}

/// Everything one node says to another.
///
/// One type rather than several, because a transport that has to know the difference is a
/// transport that has to change every time the protocol does.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
    /// The sender's term. A message from an **earlier** term is refused rather than applied — that
    /// is what stops a partitioned former leader from writing. A message from a **later** term
    /// takes its receiver with it, which is how a promotion reaches the nodes that missed it.
    pub term: Term,
    pub body: Body,
}

/// The payload half of a [`Message`].
#[derive(Debug, Clone, PartialEq)]
pub enum Body {
    /// Pre-vote ask. Carries the candidate's log tail so a voter can apply the election
    /// restriction *without* anyone raising a term.
    PreVote { last_term: Term, last_round: Round },
    PreVoteResp { granted: bool },

    /// Real vote ask, sent only after a pre-vote quorum said the campaign could be won.
    RequestVote { last_term: Term, last_round: Round },
    RequestVoteResp { granted: bool },

    /// Log replication, and the heartbeat when `entries` is empty.
    ///
    /// `prev_term` is the term of the round immediately before `entries[0]`, or `0` for a round the
    /// sender inherited rather than wrote.
    Append {
        prev_round: Round,
        prev_term: Term,
        entries: Vec<Entry>,
        /// How far the leader has committed. A follower may apply up to this, and no further.
        commit: Round,
    },
    /// `hint` is the first round the follower *can* accept, so a leader backs up in one step
    /// rather than probing backwards one round at a time.
    ///
    /// `digest` is a rolling hash of the follower's WAL through `matched`, and it is the **only**
    /// thing that can catch byte-level divergence before a promotion turns it into corruption with
    /// no record of when it began. A leader whose own digest at that round disagrees latches the
    /// follower as diverged and stops counting it toward quorum. Zero means "not claiming
    /// anything" — sent with a refusal, where a digest would be a claim about a log the follower
    /// has not matched.
    AppendResp { success: bool, matched: Round, hint: Round, digest: u64 },

    /// State transfer for a follower whose needed rounds have been checkpointed away.
    InstallSnapshot { meta: snapshot::SnapshotMeta, offset: u64, data: Vec<u8>, done: bool },
    InstallSnapshotResp { received_through: u64 },
}

/// Everything that can happen *to* a node. The caller supplies these; the state machine never
/// generates one for itself.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// One unit of time. The caller decides what a unit is worth; the state machine only counts.
    Tick,
    /// A message arrived. Already authenticated by the transport — see `signing.rs`.
    Recv(Message),
    /// The caller made durable everything through `round`, and the hard state at `term`.
    ///
    /// Separate from the append itself because **a follower that acks a round it has not fsynced
    /// converts a correlated power loss into acknowledged data loss.** The ack is emitted on this
    /// event, never on receipt.
    Persisted { term: Term, round: Round },
    /// A client asked for a command to be committed. Only meaningful on a leader; anywhere else it
    /// produces a `NotLeader` refusal rather than a silent drop.
    Propose(Command),
}

/// Everything a node wants done on its behalf. The state machine returns these; it performs none
/// of them.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Send(Message),
    /// Append these entries and fsync, then report back with [`Event::Persisted`].
    Persist { entries: Vec<Entry> },
    /// Record `(term, voted_for)` durably before any vote is sent.
    ///
    /// A node that votes, crashes, and comes back having forgotten the vote can vote twice in one
    /// term, which elects two leaders of that term. This must reach the disk *before* the
    /// corresponding `Send`, which is why it is a separate action and not a field on one.
    PersistHardState { term: Term, voted_for: Option<NodeId> },
    /// Everything through this round is committed: hand it to the local storage engine.
    Apply { through: Round },
    /// Truncate the local log from this round upward — the suffix conflicts with the leader's.
    Truncate { from: Round },
    /// Role changed. Emitted so the surrounding server can start or stop serving writes, and so
    /// tests can assert on transitions rather than on timing.
    RoleChanged { role: Role, term: Term, leader: Option<NodeId> },
    /// A proposal could not be accepted here.
    Refuse { why: FerroError },
}

/// Durable state that must survive a crash, distinct from everything recomputable.
///
/// Small on purpose: every field here costs an fsync on the critical path, and every field *not*
/// here is one a crash may silently change.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

/// The consensus state machine.
///
/// Fields live here rather than beside the code that uses them because they are one invariant, not
/// several: `role`, `term`, `voted_for`, `cfg` and `campaign` are read together on every vote, and
/// a rule that reads a stale combination of them elects two leaders.
///
/// This struct carried a transitional `#[allow(dead_code)]` while `election.rs` and `replicate.rs`
/// were stubs — the contract had to land before its implementations so that eight agents could
/// branch from one frozen type set instead of inventing eight. Both are implemented now, so the
/// allow was **removed 2026-08-28** and the build was re-run under CI's exact flags
/// (`RUSTFLAGS="-D duplicate_macro_attributes -D dead_code"`, see `.github/workflows/tests.yml`)
/// to prove every field below is genuinely read. Do not reintroduce it: a field that goes unread
/// here is a bug in the implementation, not a reason to silence the lint.
pub struct Consensus {
    pub(crate) self_id: NodeId,
    pub(crate) role: Role,
    pub(crate) hard: HardState,
    /// Who this node believes leads the current term, if anyone.
    pub(crate) leader: Option<NodeId>,

    /// **Who is in the cluster**, and what a majority is counted against.
    ///
    /// One immutable value rather than a peer list plus a size, because those are a pair that has
    /// to change together: a majority of the wrong number elects two leaders of one term, and
    /// nothing later in the protocol can notice.
    pub(crate) cfg: config::Config,

    /// The configuration **this campaign** counts against, captured in the same step that raised
    /// the term. A membership change landing mid-campaign must not move the denominator these
    /// votes are counted against, and a vote from a node outside this configuration is not counted
    /// at all.
    pub(crate) campaign: Option<config::Config>,

    /// The newest configuration each member says it holds **durably**, as a `(version, term)`
    /// pair.
    ///
    /// The pair and not the version alone, because a version is ambiguous across terms: a new
    /// leader's first change is version 1 in its own term, and a follower's stale acknowledgement
    /// of some earlier term's version 1 would be counted toward it. This is the leader's evidence
    /// that a change reached a majority of the set that created it, which is the precondition for
    /// beginning the next one — see `membership.rs`.
    pub(crate) acked: std::collections::BTreeMap<NodeId, config::CfgAt>,

    /// This node knows its **configuration** is not the cluster's, so it must not campaign: it
    /// would count votes against a set the cluster has left.
    ///
    /// Two causes — a node started to *join* a running cluster has not been told the configuration
    /// yet, and a node that hears a later version from a member has just found out it is stale.
    /// Cleared by *applying a configuration*, which is the evidence that answers both.
    ///
    /// Without it, either node stands on its own timeout, raises the term, and fences a healthy
    /// leader out of office repeatedly — a livelock in which no node with an up-to-date
    /// configuration can hold the office and no node without one can win it.
    pub(crate) behind: bool,

    /// This node was **added to a running cluster and holds none of its log**, so it must not
    /// campaign either.
    ///
    /// A separate flag from `behind` and not a second cause of it, because the two are cleared by
    /// different evidence: applying a configuration makes this node's majority the cluster's
    /// majority and says *nothing* about whether it holds a single round. Clearing them together
    /// is the defect — the `Membership` that adds a node is followed on its very next tick by a
    /// pre-vote, whose term is deliberately one above everybody's, from a node holding nothing.
    ///
    /// Cleared by an **observable**: a leader's `Append` says how many rounds a quorum holds, and
    /// this node's own store says how many it holds. Never a timer — and never the flag alone,
    /// since a cluster whose log is empty reports a watermark of zero, so joining an empty cluster
    /// must not leave a member that can never stand.
    pub(crate) unjoined: bool,

    /// Ticks since this node last heard from a leader it accepts.
    pub(crate) since_heard: u32,
    /// Ticks the current leader has gone without hearing from a majority. See `lease` below.
    pub(crate) since_quorum: u32,
    /// Randomized per campaign, in ticks. Randomized so two nodes do not campaign in lockstep for
    /// ever, which is a split vote that repeats.
    pub(crate) election_timeout: u32,
    /// The base election timeout; the drawn one lies in `[base, 2*base)`.
    pub(crate) election_base: u32,
    /// How long a leader may act on its office without hearing from a majority.
    ///
    /// **A window is the lease and NOT the election timeout.** They were once the same number and
    /// that was the defect: a peer that draws a short timeout campaigns while a leader on a long
    /// one still holds its lease, producing exactly the two-leader overlap the lease exists to
    /// prevent. The lease must expire strictly before any peer can win.
    pub(crate) lease: u32,
    /// Ticks between heartbeats while leading.
    pub(crate) heartbeat: u32,
    pub(crate) since_heartbeat: u32,

    /// Votes granted in the current campaign, pre-vote or real.
    pub(crate) votes: std::collections::BTreeSet<NodeId>,

    /// Per-peer replication progress. `next` is the round to send; `matched` is the highest round
    /// known to be on that peer. `matched` is what quorum is counted over — `next` is optimism and
    /// counting it commits rounds nobody holds.
    pub(crate) progress: std::collections::BTreeMap<NodeId, replicate::Progress>,

    /// Highest round known committed. Never decreases.
    pub(crate) commit: Round,
    /// Highest round handed to the storage engine. `applied <= commit` always.
    pub(crate) applied: Round,
    /// Highest round this node has made **durable**. Distinct from the log's length: an entry that
    /// is appended but not yet fsynced must not be acked.
    pub(crate) durable: Round,

    /// Log tail as this node knows it, for the election restriction.
    pub(crate) last_term: Term,
    pub(crate) last_round: Round,

    /// Where the log begins, once a snapshot has discarded a prefix. Rounds at or below this are
    /// no longer serveable from the log and need `InstallSnapshot`.
    pub(crate) snapshot_round: Round,
    pub(crate) snapshot_term: Term,

    /// Deterministic randomness for timeout selection.
    ///
    /// Seeded and owned, never `SystemTime`- or thread-seeded, because a campaign has to replay
    /// identically from a seed for `sim.rs` to be worth anything.
    pub(crate) rng: Rng,
}

impl Consensus {
    /// A node that already knows the cluster it belongs to.
    pub fn new(self_id: NodeId, cfg: config::Config, seed: u64) -> Self {
        let election_base = 10;
        let mut rng = Rng::new(seed);
        let election_timeout = election_base + (rng.next_u32() % election_base);
        Consensus {
            self_id,
            role: Role::Follower,
            hard: HardState::default(),
            leader: None,
            cfg,
            campaign: None,
            acked: Default::default(),
            behind: false,
            unjoined: false,
            since_heard: 0,
            since_quorum: 0,
            election_timeout,
            election_base,
            // Strictly below `election_base` so a leader's lease always expires before any peer
            // can win, however the peer's timeout was drawn. Asserted in `tests_contract`.
            lease: election_base - 2,
            heartbeat: 3,
            since_heartbeat: 0,
            votes: Default::default(),
            progress: Default::default(),
            commit: 0,
            applied: 0,
            durable: 0,
            last_term: 0,
            last_round: 0,
            snapshot_round: 0,
            snapshot_term: 0,
            rng,
        }
    }

    /// A node started to **join** a cluster it has not been told about yet.
    ///
    /// Both flags set: it knows neither the configuration nor any of the log, and each is cleared
    /// by its own evidence.
    pub fn joining(self_id: NodeId, seed: u64) -> Self {
        let mut c = Consensus::new(self_id, config::Config::empty(), seed);
        c.behind = true;
        c.unjoined = true;
        c
    }

    pub fn role(&self) -> Role { self.role }
    pub fn term(&self) -> Term { self.hard.term }
    pub fn leader(&self) -> Option<NodeId> { self.leader }
    pub fn commit_round(&self) -> Round { self.commit }
    pub fn config(&self) -> &config::Config { &self.cfg }
    pub fn lease_window(&self) -> u32 { self.lease }
    pub fn id(&self) -> NodeId { self.self_id }

    /// Whether this node may stand for election at all.
    ///
    /// Both flags are checked here rather than at each call site, so a new campaign path cannot be
    /// added that forgets one. Its transitional `allow(dead_code)` was removed with the struct's,
    /// once `election.rs` landed and became its caller.
    pub(crate) fn may_campaign(&self) -> bool {
        !self.behind && !self.unjoined && self.cfg.contains(self.self_id)
    }

    /// **The only entry point.** Everything the node wants done comes back in the returned actions.
    pub fn step(&mut self, ev: Event) -> Vec<Action> {
        let mut out = Vec::new();
        match ev {
            Event::Tick => self.on_tick(&mut out),
            Event::Recv(m) => self.on_message(m, &mut out),
            Event::Persisted { term, round } => self.on_persisted(term, round, &mut out),
            Event::Propose(c) => self.on_propose(c, &mut out),
        }
        out
    }

    /// The **term rules**, which apply to every message regardless of kind, before any handler sees
    /// it.
    ///
    /// These live here rather than in `election.rs` and `replicate.rs` because they are the whole of
    /// consensus safety and there must be exactly one copy. Two handlers each implementing "and if
    /// the term is higher, step down" is two chances to get it subtly different, and the difference
    /// only shows up as two leaders in one term.
    fn on_message(&mut self, m: Message, out: &mut Vec<Action>) {
        // A message from an EARLIER term is stale: refuse it rather than act on it. This is what
        // fences a partitioned former leader out of office -- it will keep sending appends, and
        // every node that has moved on ignores them.
        //
        // The reply carries *our* term, which is how that former leader finds out. Answering only
        // requests, not responses, because replying to a response is how two nodes ping-pong stale
        // terms at each other for ever.
        if m.term < self.hard.term {
            if m.body.is_request() {
                out.push(Action::Send(Message {
                    from: self.self_id,
                    to: m.from,
                    term: self.hard.term,
                    body: m.body.stale_refusal(),
                }));
            }
            return;
        }

        // A message from a LATER term takes its receiver with it: that is how a promotion reaches
        // the nodes that missed the election.
        //
        // **PreVote is the exception, and it is the entire point of pre-vote.** A pre-vote
        // deliberately carries `term + 1` while its sender has NOT raised its own term. Treating
        // that as a later term would raise the term of every healthy node each time a partitioned
        // peer retried -- which is exactly the disruption pre-vote exists to prevent, arriving
        // through the mechanism meant to stop it. A `PreVoteResp` is likewise not evidence of a
        // real term: it is an answer about a hypothetical one.
        let hypothetical = matches!(m.body, Body::PreVote { .. } | Body::PreVoteResp { .. });
        if m.term > self.hard.term && !hypothetical {
            // `voted_for` is cleared: a new term is a new vote. Carrying the old one forward would
            // let this node refuse the first candidate of the new term for a vote it cast in the
            // last one.
            self.become_follower(m.term, None, out);
        }

        match m.body {
            Body::PreVote { .. }
            | Body::PreVoteResp { .. }
            | Body::RequestVote { .. }
            | Body::RequestVoteResp { .. } => self.on_vote_msg(m, out),
            Body::Append { .. }
            | Body::AppendResp { .. }
            | Body::InstallSnapshot { .. }
            | Body::InstallSnapshotResp { .. } => self.on_append_msg(m, out),
        }
    }

    /// Step down to follower in `term`, emitting the durability action and the role change.
    ///
    /// Shared because both `election.rs` and `replicate.rs` need it and a second copy would be a
    /// second place for the "did we persist the hard state before acting on it" question to be
    /// answered differently.
    pub(crate) fn become_follower(
        &mut self,
        term: Term,
        leader: Option<NodeId>,
        out: &mut Vec<Action>,
    ) {
        let changed = self.role != Role::Follower || self.hard.term != term || self.leader != leader;
        if self.hard.term != term {
            self.hard.term = term;
            self.hard.voted_for = None;
            out.push(Action::PersistHardState { term, voted_for: None });
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.campaign = None;
        self.votes.clear();
        self.since_heard = 0;
        if changed {
            out.push(Action::RoleChanged { role: Role::Follower, term, leader });
        }
    }
}

impl Body {
    /// Whether this body is a request that deserves an answer, as opposed to an answer itself.
    ///
    /// Used only by the stale-term rule: replying to a stale *response* would have two nodes
    /// exchanging refusals for ever.
    pub fn is_request(&self) -> bool {
        matches!(
            self,
            Body::PreVote { .. }
                | Body::RequestVote { .. }
                | Body::Append { .. }
                | Body::InstallSnapshot { .. }
        )
    }

    /// The negative answer that matches this request, sent when its term is stale.
    fn stale_refusal(&self) -> Body {
        match self {
            Body::PreVote { .. } => Body::PreVoteResp { granted: false },
            Body::RequestVote { .. } => Body::RequestVoteResp { granted: false },
            // `matched: 0` is not a claim about the log -- a stale leader must not learn anything
            // about this node's progress from a refusal it had no right to ask for. The `hint` is
            // likewise zero; the only information carried is the term on the envelope.
            Body::Append { .. } => Body::AppendResp { success: false, matched: 0, hint: 0, digest: 0 },
            Body::InstallSnapshot { .. } => Body::InstallSnapshotResp { received_through: 0 },
            // Unreachable: `is_request` gates every call, and responses are not requests. Written
            // as an explicit panic rather than a silent fallback so that adding a request variant
            // without adding its refusal fails loudly here instead of sending a wrong answer.
            other => unreachable!("stale_refusal called on a response body: {other:?}"),
        }
    }
}

/// A small deterministic PRNG (xorshift64*), owned so that a seed reproduces a campaign exactly.
///
/// Not `rand`: this crate carries **zero runtime dependencies** and that is a product claim, not an
/// accident. Quality beyond "spreads timeouts" is not needed and would not be worth a dependency.
#[derive(Debug, Clone, PartialEq)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        // A zero state is a fixed point of xorshift and would return zero for ever, making every
        // node draw the same timeout — a split vote that never resolves. Refused by substitution
        // rather than by assertion, because a seed of 0 is the most likely one a caller picks.
        Rng { state: if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed } }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }
}
