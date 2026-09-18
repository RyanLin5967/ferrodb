//! F8 — the deterministic simulator. It owns **time**, **the network** and **the disk**, so that a
//! whole campaign — partition, split vote, stale-log candidate, crash between the vote and its
//! fsync — is a pure function of one `u64` seed.
//!
//! # Why this file is the one that makes the rest a claim
//!
//! `mod.rs` states the reason [`Consensus`] returns [`Action`]s instead of performing them: *a
//! distributed protocol tested by waiting is tested on the happy path.* This file is the other half
//! of that sentence. Every claim consensus makes is a claim about what happens during a partition,
//! a reorder, or a crash, and none of those are observable from outside a real socket. Here they
//! are the ordinary case, drawn from a seed, and a failing run is replayed by passing the same
//! `u64` back in.
//!
//! `DISTRIBUTED.md` §F8 lists what must be injected, and each is a named knob below: **asymmetric**
//! partitions (one-way, which is the shape that breaks a naive implementation and which a symmetric
//! cut can never produce), drops, reorder, duplication, and crash/restart that loses everything not
//! persisted.
//!
//! # What the simulator refuses to take on trust
//!
//! The simulator never asks a node what it thinks is true and then believes it. It holds each
//! node's disk itself, so the answer to "did this round survive the crash" comes from the store and
//! not from the state machine that wanted it to survive. Two consequences worth naming:
//!
//! * **The log lives here.** [`Action::Persist`] hands entries *out*; [`Event::Persisted`] hands a
//!   watermark *back*. So the sim's [`Store`] is the only complete record of what a node holds, and
//!   the committed history it checks against is built from that record.
//! * **A crash is modelled as loss, not as a pause.** Everything a node wrote but did not fsync is
//!   gone, in-flight fsyncs are cancelled rather than completed, and the node is rebuilt with
//!   [`Peer::boot`] from the durable bytes alone. A restart that kept one field of volatile state
//!   would be a restart that cannot fail the way a real one does.
//!
//! # The detectors, and what each is for
//!
//! A detector that has never fired is not a detector, so every rule below has a mutant in
//! `tests_sim.rs` that fires it, and the same mutant switched off is required to leave it quiet.
//! The rule strings are the ones that appear in a [`Violation`].
//!
//! | Rule | What it catches |
//! |---|---|
//! | `two leaders in one term` | a vote counted twice — usually a vote that was sent before it was durable |
//! | `two leaders overlapped for longer than the lease` | a leader that keeps its office after losing a majority |
//! | `two different commands were committed at one round` | state-machine safety: the classic figure-8 loss |
//! | `a committed round was dropped from a node's log` | a committed entry truncated away |
//! | `a committed round was overwritten` | a committed entry replaced by a different one |
//! | `a new leader was missing a committed round` | the §5.4.1 election restriction, caught at the election rather than at the loss |
//! | `a vote was sent before its hard state was durable` | the fsync-before-vote rule, caught structurally |
//! | `an append was acknowledged before it was durable` | the fsync-before-ack rule, caught structurally |
//! | `a node applied a round it does not hold` | an apply watermark ahead of the log |
//! | `an apply watermark went backwards` | a commit index that regressed while the node stayed up |
//! | `a hole was persisted into the log` | rounds are contiguous from 1, and arithmetic is how a hole is seen |
//!
//! The first six are *safety* properties and hold under every fault. Liveness is not among them and
//! must not be: under a partition that never heals there is no leader and that is correct. The
//! liveness claims are made by scenario tests that heal the network first.
//!
//! # Determinism, and the thing that would quietly destroy it
//!
//! Every choice — latency, drop, duplication, which node is cut off, which node crashes, how far a
//! node's clock drifts — is drawn from one [`Rng`] seeded once. There is no `SystemTime`, no
//! `HashMap` iteration (every collection here is a `BTree*`, and that is load-bearing rather than
//! stylistic), and no threading. `the_same_seed_replays_the_same_run` in `tests_sim.rs` compares a
//! digest of two runs of one seed, and its anti-vacuity twin requires two *different* seeds to
//! disagree — a digest that is constant would pass the first test and prove nothing.
//!
//! # What this simulator does NOT model, stated rather than left to be discovered
//!
//! A guard whose blind spots are undocumented is one whose silence means nothing.
//!
//! * **Snapshots — and F6 landed WITHOUT extending this file, which is a decision rather than an
//!   omission.** [`Store`] requires rounds contiguous from 1 and refuses a write that leaves a
//!   hole, so a node whose log begins above `snapshot_round` is outside the model; `entry_at`,
//!   `durable_round`, `persist` and `would_drop_committed` all read a round as an index into one
//!   `Vec`, and every one of them feeds the two safety properties this file sweeps over 100 000
//!   seeds. Teaching the model a floor is a change to all four.
//!
//!   `Body::Append` and `Body::InstallSnapshot` are still routed to the state machine, so a
//!   snapshot message in a run is delivered and answered — but no run here ever produces one,
//!   because nothing in the model checkpoints. **So the properties below are asserted for clusters
//!   whose logs still begin at round 1, and that is the whole of the coverage claim.** F6's
//!   protocol is proven deterministically instead, in `consensus/tests_snapshot.rs` (every rule
//!   there fired by a mutant, and the battery is `bench/evidence/f6_mutants.py`) and against three real
//!   drivers, sockets and page files in
//!   `tests/integration_cluster_snapshot.rs`. Extending `Store` with a floor remains open, and
//!   whoever takes it should read this paragraph first rather than the sentence that used to be
//!   here, which said F6 would.
//! * **Membership changes.** The configuration is fixed for the life of a run. A
//!   `Command::Membership` in the log is carried like any other command and changes nothing about
//!   who the simulator counts. F5 extends this file.
//! * **Torn or lying writes.** The crash model loses the unfsynced suffix whole. It does not tear a
//!   record, corrupt one, or keep a write that was reported as failed — and, deliberately, it does
//!   **not** lose a write that was reported as succeeding. That last one is not an omission: no
//!   consensus protocol survives a lying fsync, so injecting one produces violations that say
//!   nothing about the protocol. `storage/sim.rs` is where that class of fault belongs, and
//!   `log.rs` (F0b) is where the two meet.
//! * **Byzantine behaviour.** Every node here runs the same code and tells the truth as it knows
//!   it. A peer that lies is F7's problem and is refused at the transport, before the state machine.
//! * **Message corruption and partial frames.** The wire carries whole `Message` values; framing is
//!   F3's.
//!
//! # Relationship to `storage/sim.rs`
//!
//! That file is the other simulator: a durable-IO fabric that faults individual writes. It is not
//! reused here because the two answer different questions — it owns a byte stream and one planned
//! fault, this owns a cluster and a fault *process*. `log.rs` (F0b) is where the two meet, and the
//! crash model here is deliberately the weaker of the two: whole-suffix loss, never a torn record.
//! Naming that limit is the point of this paragraph; a simulator whose blind spots are undocumented
//! is one whose silence means nothing.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use super::config::Config;
use super::{Action, Body, Command, Consensus, Entry, Event, HardState, Message, NodeId, Rng, Role, Round, Term};

/// Sub-tick resolution. A message crosses the wire in a fraction of a tick or in several of them,
/// and two nodes' ticks do not land on the same instant — both of which are only expressible if the
/// clock is finer than the unit the state machine counts in.
pub const UNITS_PER_TICK: u64 = 16;

/// How many trace lines are kept for a failure report. A whole run is far too much to print and the
/// tail is what explains the violation.
const TRACE_TAIL: usize = 160;

// ---------------------------------------------------------------------------------------------
// What the simulator can drive
// ---------------------------------------------------------------------------------------------

/// A state machine the simulator can own the time and the network of.
///
/// This exists so the harness can be finished, and *checked*, before `election.rs` and
/// `replicate.rs` exist. `tests_sim.rs` implements it a second time with a reference machine whose
/// rules can be broken one at a time; without that, every detector here would be a detector nobody
/// had ever seen fire.
///
/// It is deliberately narrow. Everything the simulator needs to *decide* something comes from
/// `step` and from its own store; the accessors below are for observation and for rebuilding a node
/// after a crash, and nothing else.
pub trait Peer: Sized {
    /// Build a node from durable state alone.
    ///
    /// Used for the cold start (`hard` default, `log` empty) and for every restart. The simulator
    /// passes exactly what survived the crash, so a `boot` that reconstructed anything else would
    /// be hiding the bug this simulator exists to find.
    fn boot(id: NodeId, cfg: Config, seed: u64, hard: HardState, log: &[Entry]) -> Self;
    fn step(&mut self, ev: Event) -> Vec<Action>;
    fn id(&self) -> NodeId;
    fn role(&self) -> Role;
    fn term(&self) -> Term;
    /// How long this node may act on its office without hearing from a majority, in its own ticks.
    fn lease_window(&self) -> u32;
    /// What this node believes its log tail to be, for the store-versus-machine cross-check.
    fn tail(&self) -> (Term, Round);
}

impl Peer for Consensus {
    fn boot(id: NodeId, cfg: Config, seed: u64, hard: HardState, log: &[Entry]) -> Self {
        let mut c = Consensus::new(id, cfg, seed);
        // Set rather than replayed, because the contract has no recovery entry point: `Consensus`
        // holds no log and offers no way to hand one back. See the summary for F8 -- this is the
        // one place the gap is load-bearing, and it is why these fields are `pub(crate)`.
        c.hard = hard;
        if let Some(e) = log.last() {
            c.last_term = e.term;
            c.last_round = e.round;
            c.durable = e.round;
        }
        c
    }
    fn step(&mut self, ev: Event) -> Vec<Action> { Consensus::step(self, ev) }
    fn id(&self) -> NodeId { Consensus::id(self) }
    fn role(&self) -> Role { Consensus::role(self) }
    fn term(&self) -> Term { Consensus::term(self) }
    fn lease_window(&self) -> u32 { Consensus::lease_window(self) }
    fn tail(&self) -> (Term, Round) { (self.last_term, self.last_round) }
}

// ---------------------------------------------------------------------------------------------
// The fault model
// ---------------------------------------------------------------------------------------------

/// Relative weights for what the fault process does at each churn point.
///
/// Weights rather than probabilities so a preset can say "mostly quiet, occasionally brutal"
/// without any of them having to sum to anything.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Churn {
    pub quiet: u32,
    /// Cut a node off in both directions.
    pub isolate: u32,
    /// **One-way**: the node can send but hears nothing. Its peers keep hearing from it, so they do
    /// not time out, while it concludes it has lost the cluster.
    pub isolate_in: u32,
    /// **One-way**: the node hears everything but reaches nobody. A leader in this state still sees
    /// no acknowledgements and must give up its office on its own lease.
    pub isolate_out: u32,
    /// Split the cluster in two, both directions.
    pub cut: u32,
    /// Split the cluster in two, **one direction only** — the shape a symmetric partition cannot
    /// produce and the one that breaks a naive implementation.
    pub cut_one_way: u32,
    /// Break a single directed link.
    pub link: u32,
    pub heal: u32,
    pub crash: u32,
    pub restart: u32,
}

impl Churn {
    fn total(&self) -> u32 {
        self.quiet + self.isolate + self.isolate_in + self.isolate_out + self.cut
            + self.cut_one_way + self.link + self.heal + self.crash + self.restart
    }

    fn pick(&self, r: u32) -> ChurnKind {
        let mut acc = 0;
        for (w, k) in [
            (self.quiet, ChurnKind::Quiet),
            (self.isolate, ChurnKind::Isolate),
            (self.isolate_in, ChurnKind::IsolateIn),
            (self.isolate_out, ChurnKind::IsolateOut),
            (self.cut, ChurnKind::Cut),
            (self.cut_one_way, ChurnKind::CutOneWay),
            (self.link, ChurnKind::Link),
            (self.heal, ChurnKind::Heal),
            (self.crash, ChurnKind::Crash),
            (self.restart, ChurnKind::Restart),
        ] {
            acc += w;
            if r < acc {
                return k;
            }
        }
        ChurnKind::Quiet
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChurnKind {
    Quiet,
    Isolate,
    IsolateIn,
    IsolateOut,
    Cut,
    CutOneWay,
    Link,
    Heal,
    Crash,
    Restart,
}

/// Everything the network and the disks are allowed to do wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Faults {
    /// Percent of sends discarded outright.
    pub drop_pct: u32,
    /// Percent of sends delivered a second time, with an independently drawn latency — so a
    /// duplicate may arrive before its original, which is reorder and duplication at once.
    pub dup_pct: u32,
    /// Inclusive latency range in units. Independent draws are what produce reordering; a fixed
    /// latency would deliver everything in send order and hide every ordering bug.
    pub latency: (u64, u64),
    /// Inclusive fsync latency range in units. Non-zero on purpose: a crash is only interesting in
    /// the window between a write and its durability, and a zero-cost fsync deletes that window.
    pub fsync: (u64, u64),
    /// Percent by which a node's tick period may differ from nominal, either way. Clocks that agree
    /// exactly are a fiction, and lockstep ticking is a determinism artefact that hides split votes.
    pub drift_pct: u64,
    /// Units between draws from `churn`. Zero disables the fault process entirely.
    pub churn_every: u64,
    pub churn: Churn,
}

impl Faults {
    /// A network that does nothing wrong. Latency and fsync cost remain, because they are not
    /// faults — they are the reason ordering exists.
    pub fn none() -> Self {
        Faults {
            drop_pct: 0,
            dup_pct: 0,
            latency: (1, 6),
            fsync: (1, 4),
            drift_pct: 8,
            churn_every: 0,
            churn: Churn::default(),
        }
    }

    /// Loss, duplication and wide latency, but no partitions and no crashes: the shape that finds
    /// retry and idempotence bugs without ever removing a quorum.
    pub fn lossy() -> Self {
        Faults {
            drop_pct: 12,
            dup_pct: 10,
            latency: (1, 24),
            fsync: (1, 8),
            drift_pct: 10,
            churn_every: 0,
            churn: Churn::default(),
        }
    }

    /// Partitions, weighted towards the asymmetric ones, with no crashes. Separated from `chaos` so
    /// that a failure here names the network rather than the disk.
    pub fn partitioned() -> Self {
        Faults {
            drop_pct: 4,
            dup_pct: 4,
            latency: (1, 20),
            fsync: (1, 8),
            drift_pct: 10,
            churn_every: 6 * UNITS_PER_TICK,
            churn: Churn {
                quiet: 24,
                isolate: 6,
                isolate_in: 8,
                isolate_out: 8,
                cut: 6,
                cut_one_way: 10,
                link: 6,
                heal: 26,
                ..Churn::default()
            },
        }
    }

    /// Everything at once. This is the preset the seed sweeps run.
    pub fn chaos() -> Self {
        Faults {
            drop_pct: 8,
            dup_pct: 8,
            latency: (1, 28),
            fsync: (1, 10),
            drift_pct: 12,
            churn_every: 4 * UNITS_PER_TICK,
            churn: Churn {
                quiet: 20,
                isolate: 5,
                isolate_in: 7,
                isolate_out: 7,
                cut: 5,
                cut_one_way: 8,
                link: 5,
                heal: 24,
                crash: 8,
                restart: 14,
            },
        }
    }
}

/// One run's inputs, other than the seed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimConfig {
    pub nodes: u32,
    /// Length of the run in **nominal** ticks; a node whose clock drifts sees slightly more or
    /// fewer of its own.
    pub ticks: u64,
    pub faults: Faults,
    /// Units between client proposals. Zero means the run makes none, which is what a scenario test
    /// that drives its own proposals wants.
    pub propose_every: u64,
    /// Keep a rolling tail of human-readable lines, attached to any violation. Off for sweeps,
    /// on for the replay of a seed that failed.
    pub trace: bool,
}

impl SimConfig {
    /// A five-node cluster under everything the fault model can do. Five rather than three because
    /// a three-node cluster has only one interesting cut, and the §5.4.2 figure-8 scenario needs
    /// two disjoint minorities to be reachable at all.
    pub fn chaos(nodes: u32, ticks: u64) -> Self {
        SimConfig {
            nodes,
            ticks,
            faults: Faults::chaos(),
            propose_every: 3 * UNITS_PER_TICK,
            trace: false,
        }
    }

    /// A healthy network. Used for the liveness claims and as the "and then it stays quiet" half of
    /// every detector.
    pub fn healthy(nodes: u32, ticks: u64) -> Self {
        SimConfig {
            nodes,
            ticks,
            faults: Faults::none(),
            propose_every: 3 * UNITS_PER_TICK,
            trace: false,
        }
    }

    /// No faults, no proposals: the caller drives everything itself.
    pub fn scripted(nodes: u32, ticks: u64) -> Self {
        SimConfig { nodes, ticks, faults: Faults::none(), propose_every: 0, trace: true }
    }
}

// ---------------------------------------------------------------------------------------------
// What a failure looks like
// ---------------------------------------------------------------------------------------------

/// A broken invariant, carrying everything needed to see it again.
///
/// Returned rather than panicked, because a sweep has to be able to say *which* seed of ten
/// thousand failed, and a panic in the middle of one loses the other nine thousand results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The whole input. Everything else in the run is a function of this and of [`SimConfig`].
    pub seed: u64,
    /// Simulator time, in units.
    pub at: u64,
    /// The named rule. Stable strings — tests match on them.
    pub rule: &'static str,
    pub detail: String,
    /// The tail of the run, present when the run was traced.
    pub trace: Vec<String>,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "consensus simulator: {}", self.rule)?;
        writeln!(f, "  seed   : {} (0x{:016x})", self.seed, self.seed)?;
        writeln!(f, "  at     : unit {} (tick {})", self.at, self.at / UNITS_PER_TICK)?;
        writeln!(f, "  detail : {}", self.detail)?;
        writeln!(
            f,
            "  replay : Sim::<P>::replay({}, cfg) -- the seed and the config are the whole input",
            self.seed
        )?;
        if !self.trace.is_empty() {
            writeln!(f, "  trace  : last {} events", self.trace.len())?;
            for line in &self.trace {
                writeln!(f, "    {line}")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for Violation {}

/// What one run did. Reported so that a green sweep can be checked for having done anything at all:
/// a simulator that elects no leader and commits no round violates nothing and proves nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub seeds: u64,
    pub ticks: u64,
    pub sent: u64,
    pub delivered: u64,
    pub dropped_partition: u64,
    pub dropped_loss: u64,
    pub dropped_down: u64,
    pub duplicated: u64,
    /// Duplicates that actually arrived. `duplicated` counts copies put on the wire; a copy dropped
    /// by a partition tested nothing, so the two numbers are reported separately.
    pub duplicates_delivered: u64,
    /// Deliveries that arrived after a later-sent message on the same directed link. Named in
    /// `DISTRIBUTED.md` §F8 alongside drops and duplication, and reported because reorder here is
    /// an *emergent* consequence of independent latency draws rather than an injected fault — the
    /// kind of thing that quietly stops happening when a constant is changed.
    pub reordered: u64,
    pub crashes: u64,
    pub restarts: u64,
    pub partitions: u64,
    /// Of those, how many were **one-way**. Reported because a sweep that only ever cut both
    /// directions has not exercised the case `DISTRIBUTED.md` §F8 singles out, and a report that
    /// did not distinguish them would let that go unnoticed.
    pub one_way_partitions: u64,
    pub heals: u64,
    /// Entries a crash discarded because they had been written but not fsynced. Zero means the
    /// crash model never actually took anything away, which makes every "survived a restart" claim
    /// in this file vacuous — so it is asserted on, not merely reported.
    pub discarded_entries: u64,
    /// Messages a crash destroyed because they were still waiting behind an fsync and had therefore
    /// not left the machine. See [`Sim::crash`] — a simulator that delivered these reports two
    /// leaders in one term against a protocol that did nothing wrong.
    pub unsent_at_crash: u64,
    /// Crashes that destroyed a `(term, voted_for)` that had been written and not yet fsynced.
    ///
    /// This is the **stated breaking shape of the headline detector**: `mod.rs` says a node that
    /// votes, crashes and forgets the vote can vote twice in one term. If this is zero, that
    /// scenario never happened and "at most one leader per term" was asserted over runs in which
    /// the only way to break it was never reached — so it is asserted on, not merely reported.
    pub forgotten_votes: u64,
    /// Transitions into [`Role::Leader`].
    pub elections: u64,
    pub proposals: u64,
    pub refusals: u64,
    pub committed_rounds: u64,
    pub max_term: Term,
    /// The largest number of a node's own ticks spent leading while another node also led. A
    /// correct implementation on a healthy network reports zero; the lease bounds it elsewhere.
    pub max_overlap_ticks: u32,
    /// Order-sensitive hash of every event in the run. Equal digests mean identical runs.
    pub digest: u64,
}

impl Report {
    fn absorb(&mut self, other: &Report) {
        self.seeds += other.seeds;
        self.ticks += other.ticks;
        self.sent += other.sent;
        self.delivered += other.delivered;
        self.dropped_partition += other.dropped_partition;
        self.dropped_loss += other.dropped_loss;
        self.dropped_down += other.dropped_down;
        self.duplicated += other.duplicated;
        self.duplicates_delivered += other.duplicates_delivered;
        self.reordered += other.reordered;
        self.crashes += other.crashes;
        self.restarts += other.restarts;
        self.partitions += other.partitions;
        self.one_way_partitions += other.one_way_partitions;
        self.heals += other.heals;
        self.discarded_entries += other.discarded_entries;
        self.unsent_at_crash += other.unsent_at_crash;
        self.forgotten_votes += other.forgotten_votes;
        self.elections += other.elections;
        self.proposals += other.proposals;
        self.refusals += other.refusals;
        self.committed_rounds += other.committed_rounds;
        self.max_term = self.max_term.max(other.max_term);
        self.max_overlap_ticks = self.max_overlap_ticks.max(other.max_overlap_ticks);
        self.digest ^= other.digest.rotate_left((other.seeds % 64) as u32);
    }
}

/// The result of running many seeds. Carries the totals whether or not it found anything, because
/// the totals are how a caller checks the sweep was not vacuous.
#[derive(Debug, Clone)]
pub struct Sweep {
    pub seeds_run: u64,
    pub totals: Report,
    /// The **first** failing seed. A sweep stops there: the second failure is usually the first one
    /// again and the interesting thing is the smallest reproducer.
    pub violation: Option<Violation>,
}

// ---------------------------------------------------------------------------------------------
// A node's disk
// ---------------------------------------------------------------------------------------------

/// One scheduled fsync completion.
///
/// It carries the hard state **by value** rather than a flag saying "and take whatever is pending".
/// With two `PersistHardState` actions outstanding, a flag lets the first completion install the
/// second one's value — a write landing before it was issued, which is not a fault a disk has.
#[derive(Debug, Clone, PartialEq)]
struct Flush {
    at: u64,
    /// Log length this flush makes durable.
    ///
    /// Clamped by any truncation that happens before it lands: an fsync covers the **bytes that
    /// were there when it was issued**, and a log that has since been truncated and re-grown holds
    /// different entries at those positions. Crediting the new ones is the same lying fsync this
    /// file has now been caught modelling three times — see [`Sim::crash`] for the first.
    len: usize,
    hard: Option<HardState>,
}

/// The bytes a node would still have after `kill -9`.
///
/// The simulator owns this rather than the state machine, which is the point: "did the round
/// survive" is answered by the store, not by the node that wanted it to.
#[derive(Debug, Clone, Default, PartialEq)]
struct Store {
    /// Durable `(term, voted_for)`.
    hard: HardState,
    /// Written but not yet fsynced. A crash discards it — that is the whole reason a vote that is
    /// sent before its `PersistHardState` completes can be cast twice.
    hard_pending: Option<HardState>,
    /// The node's log. `log[i].round == i as Round + 1`, checked on every write.
    log: Vec<Entry>,
    /// `log[..durable_len]` has reached the disk. The rest is lost on a crash.
    durable_len: usize,
    flushes: VecDeque<Flush>,
    last_flush_at: u64,
}

impl Store {
    fn entry_at(&self, r: Round) -> Option<&Entry> {
        if r == 0 {
            return None;
        }
        self.log.get((r - 1) as usize)
    }

    fn durable_round(&self) -> Round {
        self.durable_len as Round
    }

    fn effective_hard(&self) -> HardState {
        self.hard_pending.clone().unwrap_or_else(|| self.hard.clone())
    }

    /// Would dropping everything from `idx` remove a round the cluster has committed?
    ///
    /// Only when the entry being removed **is** the committed one. A node holding some other
    /// entry at a committed round is an ordinary stale suffix and truncating it is the repair,
    /// not the defect — conflating the two would make the detector fire on healthy runs.
    fn would_drop_committed(
        &self,
        idx: usize,
        committed: &BTreeMap<Round, Entry>,
    ) -> Option<(&'static str, String)> {
        for e in self.log.iter().skip(idx) {
            if committed.get(&e.round) == Some(e) {
                return Some((
                    "a committed round was dropped from a node's log",
                    format!("round {} (term {}) was committed and is being discarded", e.round, e.term),
                ));
            }
        }
        None
    }

    fn persist(
        &mut self,
        entries: &[Entry],
        committed: &BTreeMap<Round, Entry>,
    ) -> Result<(), (&'static str, String)> {
        for e in entries {
            if e.round == 0 {
                return Err((
                    "a hole was persisted into the log",
                    "round 0 is 'before the log begins' and is not writable".to_string(),
                ));
            }
            let idx = (e.round - 1) as usize;
            if idx > self.log.len() {
                return Err((
                    "a hole was persisted into the log",
                    format!("round {} written onto a log of length {}", e.round, self.log.len()),
                ));
            }
            if let Some(c) = committed.get(&e.round) {
                if c != e {
                    return Err((
                        "a committed round was overwritten",
                        format!(
                            "round {} is committed as term {} but was written as term {}",
                            e.round, c.term, e.term
                        ),
                    ));
                }
            }
            if idx < self.log.len() {
                if self.log[idx] == *e {
                    continue;
                }
                if let Some(v) = self.would_drop_committed(idx, committed) {
                    return Err(v);
                }
                self.truncate_to(idx);
            }
            self.log.push(e.clone());
        }
        Ok(())
    }

    fn truncate(
        &mut self,
        from: Round,
        committed: &BTreeMap<Round, Entry>,
    ) -> Result<(), (&'static str, String)> {
        let idx = from.saturating_sub(1) as usize;
        if idx >= self.log.len() {
            return Ok(());
        }
        if let Some(v) = self.would_drop_committed(idx, committed) {
            return Err(v);
        }
        self.truncate_to(idx);
        Ok(())
    }

    /// Drop the log above `len`, lower the durable watermark to match, **and clamp every fsync
    /// still in flight**.
    ///
    /// The last clause is the one that is easy to leave out and impossible to notice: a `Flush`
    /// records a length, and a length means nothing once the entries at those positions have been
    /// replaced. Without the clamp, an fsync issued for ten old entries lands after a truncation to
    /// five and declares five *new* entries durable that no fsync ever covered — the follower then
    /// acknowledges them, and `check_send`'s "an append was acknowledged before it was durable"
    /// compares against the same inflated watermark and cannot see it. Found by review, measured at
    /// 151 such events in 4 000 chaos seeds.
    fn truncate_to(&mut self, len: usize) {
        self.log.truncate(len);
        self.durable_len = self.durable_len.min(len);
        for f in &mut self.flushes {
            f.len = f.len.min(len);
        }
    }

    /// Schedule an fsync, returning the unit at which it lands.
    ///
    /// Completions are forced monotone: a disk does not answer an earlier fsync of the same file
    /// after a later one, and a watermark that arrived out of order would be a fiction the state
    /// machine cannot be blamed for mishandling.
    fn schedule(&mut self, now: u64, latency: u64, hard: Option<HardState>) -> u64 {
        let at = (now + latency).max(self.last_flush_at + 1);
        self.last_flush_at = at;
        self.flushes.push_back(Flush { at, len: self.log.len(), hard });
        at
    }

    /// Everything not fsynced is gone.
    fn crash(&mut self) {
        self.log.truncate(self.durable_len);
        self.hard_pending = None;
        self.flushes.clear();
    }
}

// ---------------------------------------------------------------------------------------------
// A node
// ---------------------------------------------------------------------------------------------

/// One message on the wire.
struct Wired {
    /// The unit at which it actually left its sender — after every fsync it was queued behind. A
    /// crash before this instant destroys it; see [`Sim::crash`].
    released: u64,
    /// Whether this is the duplicate copy rather than the original.
    dup: bool,
    msg: Message,
}

struct Node<P> {
    id: NodeId,
    /// `None` while crashed.
    peer: Option<P>,
    store: Store,
    /// Units between this node's ticks. Drifts from nominal, per node, from the seed.
    period: u64,
    next_tick: u64,
    boots: u64,
    lease: u32,
    /// Consecutive ticks *of this node's own clock* spent leading while another node also led.
    overlap: u32,
    /// Highest round handed to this node's storage engine since it last booted.
    applied: Round,
    /// Highest round the simulator has already checked against the committed history for this node.
    /// Not reset by a boot: the check is about entries, and entries do not change across a restart.
    checked: Round,
}

// ---------------------------------------------------------------------------------------------
// The simulator
// ---------------------------------------------------------------------------------------------

/// One seeded run of a cluster.
pub struct Sim<P: Peer> {
    seed: u64,
    cfg: SimConfig,
    cluster: Config,
    rng: Rng,
    now: u64,
    seq: u64,
    nodes: Vec<Node<P>>,
    /// The wire, keyed by `(delivery unit, sequence)` so that delivery order is total and
    /// reproducible even when two messages land in the same unit.
    wire: BTreeMap<(u64, u64), Wired>,
    /// Highest sequence delivered on each directed link, for the reorder counter. Duplicates are
    /// excluded from it, so "reordered" means two *different* messages crossed, not a copy of one
    /// overtaking its original.
    link_seq: BTreeMap<(u32, u32), u64>,
    /// Directed blocks. `(a, b)` present means nothing from `a` reaches `b`; `(b, a)` absent means
    /// the reverse still works, which is the asymmetric case.
    blocked: BTreeSet<(u32, u32)>,
    committed: BTreeMap<Round, Entry>,
    leaders: BTreeMap<Term, NodeId>,
    next_payload: u64,
    report: Report,
    trace: VecDeque<String>,
}

impl<P: Peer> Sim<P> {
    pub fn new(seed: u64, cfg: SimConfig) -> Self {
        // Mixed so that adjacent sweep seeds do not produce adjacent first draws.
        let mut rng = Rng::new(seed ^ 0x243F_6A88_85A3_08D3);
        let cluster = Config::new((1..=cfg.nodes).map(NodeId), 1, 1);
        let mut nodes = Vec::with_capacity(cfg.nodes as usize);
        for k in 1..=cfg.nodes {
            let id = NodeId(k);
            let node_seed = seed ^ (k as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let peer = P::boot(id, cluster.clone(), node_seed, HardState::default(), &[]);
            let lease = peer.lease_window();
            let drift = cfg.faults.drift_pct;
            let span = 2 * drift + 1;
            let pct = 100 + (rng.next_u64() % span) as i64 - drift as i64;
            let period = ((UNITS_PER_TICK as i64 * pct) / 100).max(1) as u64;
            // Staggered so nodes do not tick in lockstep, which would make every split vote
            // simultaneous and hide the randomized-timeout rule that exists to break them.
            let next_tick = rng.next_u64() % period;
            nodes.push(Node {
                id,
                peer: Some(peer),
                store: Store::default(),
                period,
                next_tick,
                boots: 0,
                lease,
                overlap: 0,
                applied: 0,
                checked: 0,
            });
        }
        Sim {
            seed,
            cfg,
            cluster,
            rng,
            now: 0,
            seq: 0,
            nodes,
            wire: BTreeMap::new(),
            link_seq: BTreeMap::new(),
            blocked: BTreeSet::new(),
            committed: BTreeMap::new(),
            leaders: BTreeMap::new(),
            next_payload: 1,
            report: Report::default(),
            trace: VecDeque::new(),
        }
    }

    /// Re-run one seed with tracing on. This is what a failing sweep seed is fed back into.
    pub fn replay(seed: u64, cfg: SimConfig) -> Result<Report, Violation> {
        let mut c = cfg;
        c.trace = true;
        Sim::<P>::new(seed, c).run()
    }

    // -- observation ---------------------------------------------------------------------------

    pub fn now(&self) -> u64 { self.now }
    pub fn seed(&self) -> u64 { self.seed }
    pub fn committed(&self) -> &BTreeMap<Round, Entry> { &self.committed }
    pub fn report(&self) -> &Report { &self.report }
    pub fn cluster(&self) -> &Config { &self.cluster }
    pub fn trace_lines(&self) -> Vec<String> { self.trace.iter().cloned().collect() }

    /// Every node that currently believes it leads. More than one is not by itself a violation —
    /// they may be in different terms — which is exactly why the lease detector measures duration.
    pub fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter_map(|n| n.peer.as_ref())
            .filter(|p| p.role() == Role::Leader)
            .map(|p| p.id())
            .collect()
    }

    /// The single current leader, or `None` if there is not exactly one.
    pub fn leader(&self) -> Option<NodeId> {
        let ls = self.leaders();
        if ls.len() == 1 { Some(ls[0]) } else { None }
    }

    pub fn role_of(&self, n: NodeId) -> Option<Role> {
        self.node(n).and_then(|i| self.nodes[i].peer.as_ref()).map(|p| p.role())
    }

    pub fn term_of(&self, n: NodeId) -> Option<Term> {
        self.node(n).and_then(|i| self.nodes[i].peer.as_ref()).map(|p| p.term())
    }

    pub fn is_up(&self, n: NodeId) -> bool {
        self.node(n).map(|i| self.nodes[i].peer.is_some()).unwrap_or(false)
    }

    /// The rounds a node holds durably, for a test that wants to assert about the disk rather than
    /// about what the node says.
    pub fn durable_log(&self, n: NodeId) -> Vec<Entry> {
        match self.node(n) {
            Some(i) => self.nodes[i].store.log[..self.nodes[i].store.durable_len].to_vec(),
            None => Vec::new(),
        }
    }

    pub fn hard_state(&self, n: NodeId) -> HardState {
        match self.node(n) {
            Some(i) => self.nodes[i].store.hard.clone(),
            None => HardState::default(),
        }
    }

    fn node(&self, n: NodeId) -> Option<usize> {
        self.nodes.iter().position(|x| x.id == n)
    }

    // -- driving -------------------------------------------------------------------------------

    /// Run the whole configured length.
    pub fn run(&mut self) -> Result<Report, Violation> {
        self.run_ticks(self.cfg.ticks)?;
        let mut r = self.report.clone();
        r.seeds = 1;
        r.ticks = self.cfg.ticks;
        r.committed_rounds = self.committed.len() as u64;
        r.max_term = self.nodes.iter().filter_map(|n| n.peer.as_ref()).map(|p| p.term()).max().unwrap_or(0);
        self.report = r.clone();
        Ok(r)
    }

    /// Advance `ticks` nominal ticks. Scenario tests drive the sim with this between their own
    /// partitions and crashes.
    pub fn run_ticks(&mut self, ticks: u64) -> Result<(), Violation> {
        self.run_units(ticks * UNITS_PER_TICK)
    }

    /// Advance by sub-tick units.
    ///
    /// A scripted scenario needs this rather than [`Sim::run_ticks`]: it has to observe a role
    /// change and cut the network **before** the messages that change carries are delivered, and a
    /// tick is far too coarse for that — the whole election would be over inside one.
    pub fn run_units(&mut self, units: u64) -> Result<(), Violation> {
        let end = self.now + units;
        while self.now < end {
            self.now += 1;
            self.churn()?;
            self.deliver_messages()?;
            self.deliver_flushes()?;
            self.deliver_ticks()?;
            self.maybe_propose()?;
        }
        Ok(())
    }

    // -- the network, under the test's control -------------------------------------------------

    pub fn block(&mut self, from: NodeId, to: NodeId) {
        if from != to {
            self.blocked.insert((from.0, to.0));
        }
    }

    pub fn unblock(&mut self, from: NodeId, to: NodeId) {
        self.blocked.remove(&(from.0, to.0));
    }

    /// Cut a node off in both directions.
    pub fn isolate(&mut self, n: NodeId) {
        for other in self.ids() {
            self.block(n, other);
            self.block(other, n);
        }
    }

    /// One-way: `n` can send, but hears nothing.
    pub fn isolate_inbound(&mut self, n: NodeId) {
        for other in self.ids() {
            self.block(other, n);
        }
    }

    /// One-way: `n` hears everything, but reaches nobody.
    pub fn isolate_outbound(&mut self, n: NodeId) {
        for other in self.ids() {
            self.block(n, other);
        }
    }

    /// Split the cluster, symmetrically, between `side` and everyone else.
    pub fn cut(&mut self, side: &[NodeId]) {
        for a in self.ids() {
            for b in self.ids() {
                if side.contains(&a) != side.contains(&b) {
                    self.block(a, b);
                }
            }
        }
    }

    /// Split the cluster **one way**: nothing from `side` reaches the rest, but the reverse works.
    pub fn cut_one_way(&mut self, side: &[NodeId]) {
        for a in self.ids() {
            for b in self.ids() {
                if side.contains(&a) && !side.contains(&b) {
                    self.block(a, b);
                }
            }
        }
    }

    pub fn heal(&mut self) {
        self.blocked.clear();
    }

    pub fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked.contains(&(from.0, to.0))
    }

    fn ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|n| n.id).collect()
    }

    // -- crash and restart ---------------------------------------------------------------------

    /// `kill -9`: volatile state and unfsynced writes are gone, in-flight fsyncs never complete,
    /// and **anything still waiting behind one of those fsyncs was never sent.**
    ///
    /// That last clause is not a refinement, it is the difference between a sound crash model and
    /// an unsound one, and this simulator had it wrong. The contract makes the caller order
    /// `PersistHardState` before the `Send` of a vote, so a node that crashes before the fsync
    /// lands has not spoken. Letting the message escape anyway models a disk that reports a write
    /// it did not keep — and **no consensus protocol survives a lying fsync**, so every safety
    /// property becomes unfalsifiable noise.
    ///
    /// It was found rather than reasoned about: seed 1592682576 of the 100 000-seed sweep reported
    /// two leaders in term 2. n4 crashed at unit 1152; its vote for n2, still queued behind an
    /// unfinished fsync, was delivered at unit 1172 and completed n2's quorum; n4 came back at unit
    /// 1856 having forgotten a vote it had never durably cast, and won the same term with the other
    /// two nodes. The protocol was correct throughout. **A simulator's own model is the first thing
    /// a violation impugns.**
    ///
    /// `>= now` rather than `> now` because a crash is processed before the fsyncs of that same
    /// unit: a completion scheduled for exactly now has not landed yet.
    pub fn crash(&mut self, n: NodeId) {
        let Some(i) = self.node(n) else { return };
        if self.nodes[i].peer.is_none() {
            return;
        }
        self.nodes[i].peer = None;
        let now = self.now;
        let before = self.wire.len();
        self.wire.retain(|_, w| !(w.msg.from == n && w.released >= now));
        self.report.unsent_at_crash += (before - self.wire.len()) as u64;
        let lost = self.nodes[i].store.log.len() - self.nodes[i].store.durable_len;
        self.report.discarded_entries += lost as u64;
        let st = &self.nodes[i].store;
        if st.hard_pending.as_ref().is_some_and(|h| *h != st.hard) {
            self.report.forgotten_votes += 1;
        }
        self.nodes[i].store.crash();
        self.nodes[i].applied = 0;
        self.nodes[i].overlap = 0;
        self.report.crashes += 1;
        self.log_line(format!("{n} CRASH durable={}", self.nodes[i].store.durable_round()));
    }

    /// Rebuild from the durable bytes alone.
    pub fn restart(&mut self, n: NodeId) {
        let Some(i) = self.node(n) else { return };
        if self.nodes[i].peer.is_some() {
            return;
        }
        self.nodes[i].boots += 1;
        let seed = self.seed
            ^ (n.0 as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ self.nodes[i].boots.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let hard = self.nodes[i].store.hard.clone();
        let log: Vec<Entry> = self.nodes[i].store.log[..self.nodes[i].store.durable_len].to_vec();
        let peer = P::boot(n, self.cluster.clone(), seed, hard, &log);
        self.nodes[i].lease = peer.lease_window();
        self.nodes[i].peer = Some(peer);
        self.report.restarts += 1;
        self.log_line(format!("{n} RESTART durable={}", log.len()));
    }

    // -- proposals -----------------------------------------------------------------------------

    /// Hand a command to a node as a client would. A follower refuses; that path is exercised on
    /// purpose rather than routed around.
    pub fn propose_to(&mut self, n: NodeId, c: Command) -> Result<(), Violation> {
        let Some(i) = self.node(n) else { return Ok(()) };
        if self.nodes[i].peer.is_none() {
            return Ok(());
        }
        self.report.proposals += 1;
        self.log_line(format!("{n} PROPOSE"));
        let acts = self.nodes[i].peer.as_mut().unwrap().step(Event::Propose(c));
        self.handle_actions(i, acts)
    }

    /// A distinct command per proposal, so that "a different command at a committed round" is a
    /// question with an answer. `WalBatch` because that is the payload a real cluster carries.
    fn next_command(&mut self) -> Command {
        let id = self.next_payload;
        self.next_payload += 1;
        Command::WalBatch { start_lsn: id, bytes: vec![(id & 0xff) as u8, (id >> 8) as u8] }
    }

    fn maybe_propose(&mut self) -> Result<(), Violation> {
        if self.cfg.propose_every == 0 || self.now % self.cfg.propose_every != 0 {
            return Ok(());
        }
        let up: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|n| n.peer.is_some())
            .map(|n| n.id)
            .collect();
        if up.is_empty() {
            return Ok(());
        }
        let ls = self.leaders();
        // Mostly to a leader, so the run actually commits something; sometimes to anyone, so the
        // refusal path is a path the simulator has walked.
        let target = if !ls.is_empty() && self.rng.next_u64() % 4 != 0 {
            ls[(self.rng.next_u64() % ls.len() as u64) as usize]
        } else {
            up[(self.rng.next_u64() % up.len() as u64) as usize]
        };
        let c = self.next_command();
        self.propose_to(target, c)
    }

    // -- the fault process ---------------------------------------------------------------------

    /// The fault process.
    ///
    /// Counting is done from the **outcome** rather than from the action taken: a cut that blocks
    /// nothing — because it drew the whole cluster, or because those links were already down —
    /// must not be reported as a partition. `one_way_partitions` is the counter
    /// `the_fault_model_injects_every_fault_it_claims_to` uses as evidence that the asymmetric case
    /// `DISTRIBUTED.md` §F8 singles out was reached at all, so an inflated one is worse than none.
    /// It was inflated: the subset draw below could select every node, which cuts nobody off from
    /// anybody, and the old code counted it anyway.
    fn churn(&mut self) -> Result<(), Violation> {
        let every = self.cfg.faults.churn_every;
        if every == 0 || self.now % every != 0 {
            return Ok(());
        }
        let total = self.cfg.faults.churn.total();
        if total == 0 {
            return Ok(());
        }
        let pick = (self.rng.next_u64() % total as u64) as u32;
        let kind = self.cfg.faults.churn.pick(pick);
        let n = self.cfg.nodes;
        let before = self.blocked.clone();
        match kind {
            ChurnKind::Quiet => {}
            ChurnKind::Isolate => {
                let k = self.pick_node();
                self.isolate(k);
                self.log_line(format!("NET isolate {k}"));
            }
            ChurnKind::IsolateIn => {
                let k = self.pick_node();
                self.isolate_inbound(k);
                self.log_line(format!("NET isolate-inbound {k} (one-way)"));
            }
            ChurnKind::IsolateOut => {
                let k = self.pick_node();
                self.isolate_outbound(k);
                self.log_line(format!("NET isolate-outbound {k} (one-way)"));
            }
            ChurnKind::Cut | ChurnKind::CutOneWay => {
                // A non-empty **proper** subset: `1 ..= 2^n - 2` excludes both the empty set and the
                // whole cluster, so the cut always separates somebody from somebody. `2^n - 1` was
                // reachable before and cut nothing at all, one draw in 31 at five nodes.
                if n >= 2 {
                    let mask = 1 + self.rng.next_u64() % ((1u64 << n) - 2);
                    let side: Vec<NodeId> =
                        (0..n).filter(|k| mask & (1 << k) != 0).map(|k| NodeId(k + 1)).collect();
                    if kind == ChurnKind::Cut {
                        self.cut(&side);
                        self.log_line(format!("NET cut {side:?}"));
                    } else {
                        self.cut_one_way(&side);
                        self.log_line(format!("NET cut-one-way {side:?} -> rest"));
                    }
                }
            }
            ChurnKind::Link => {
                let a = self.pick_node();
                let b = self.pick_node();
                if a != b {
                    self.block(a, b);
                    self.log_line(format!("NET block {a}->{b}"));
                }
            }
            ChurnKind::Heal => {
                if !self.blocked.is_empty() {
                    self.report.heals += 1;
                    self.log_line("NET heal".to_string());
                }
                self.heal();
            }
            ChurnKind::Crash => {
                let k = self.pick_node();
                self.crash(k);
            }
            ChurnKind::Restart => {
                let down: Vec<NodeId> =
                    self.nodes.iter().filter(|x| x.peer.is_none()).map(|x| x.id).collect();
                if !down.is_empty() {
                    let k = down[(self.rng.next_u64() % down.len() as u64) as usize];
                    self.restart(k);
                }
            }
        }
        // Counted from what actually changed, not from what was attempted.
        let added: Vec<(u32, u32)> =
            self.blocked.difference(&before).copied().collect();
        if !added.is_empty() {
            self.report.partitions += 1;
            // Asymmetric only if one of the links this event added has an open reverse. A one-way
            // *action* over links that were already cut both ways produces a symmetric network, and
            // calling that a one-way partition is how the evidence stops being evidence.
            if added.iter().any(|(a, b)| !self.blocked.contains(&(*b, *a))) {
                self.report.one_way_partitions += 1;
            }
        }
        Ok(())
    }

    fn pick_node(&mut self) -> NodeId {
        NodeId(1 + (self.rng.next_u64() % self.cfg.nodes as u64) as u32)
    }

    fn draw(&mut self, range: (u64, u64)) -> u64 {
        let (lo, hi) = range;
        if hi <= lo {
            return lo;
        }
        lo + self.rng.next_u64() % (hi - lo + 1)
    }

    fn draw_pct(&mut self, pct: u32) -> bool {
        pct > 0 && (self.rng.next_u64() % 100) < pct as u64
    }

    // -- the loop's phases ---------------------------------------------------------------------

    fn deliver_messages(&mut self) -> Result<(), Violation> {
        let due: Vec<(u64, u64)> = self
            .wire
            .range((0, 0)..=(self.now, u64::MAX))
            .map(|(k, _)| *k)
            .collect();
        for key in due {
            let Some(w) = self.wire.remove(&key) else { continue };
            let m = w.msg;
            let Some(i) = self.node(m.to) else { continue };
            // Checked again at delivery: a partition raised while a message was on the wire eats
            // it, which is what a real cut does to packets already in flight.
            if self.blocked.contains(&(m.from.0, m.to.0)) {
                self.report.dropped_partition += 1;
                continue;
            }
            if self.nodes[i].peer.is_none() {
                self.report.dropped_down += 1;
                continue;
            }
            self.report.delivered += 1;
            let link = (m.from.0, m.to.0);
            if w.dup {
                self.report.duplicates_delivered += 1;
            } else {
                let last = self.link_seq.entry(link).or_insert(0);
                if key.1 < *last {
                    self.report.reordered += 1;
                } else {
                    *last = key.1;
                }
            }
            self.mix_digest(&[self.now, m.from.0 as u64, m.to.0 as u64, m.term, body_code(&m.body)]);
            self.log_line(format!(
                "{}->{} t{} {}",
                m.from,
                m.to,
                m.term,
                body_summary(&m.body)
            ));
            let acts = self.nodes[i].peer.as_mut().unwrap().step(Event::Recv(m));
            self.handle_actions(i, acts)?;
        }
        Ok(())
    }

    fn deliver_flushes(&mut self) -> Result<(), Violation> {
        for i in 0..self.nodes.len() {
            loop {
                let due = match self.nodes[i].store.flushes.front() {
                    Some(f) if f.at <= self.now => f.clone(),
                    _ => break,
                };
                self.nodes[i].store.flushes.pop_front();
                if self.nodes[i].peer.is_none() {
                    continue;
                }
                let st = &mut self.nodes[i].store;
                let landed = due.len.min(st.log.len());
                st.durable_len = st.durable_len.max(landed);
                if let Some(h) = due.hard {
                    st.hard = h;
                }
                if !st.flushes.iter().any(|f| f.hard.is_some()) {
                    st.hard_pending = None;
                }
                let term = st.hard.term;
                let round = st.durable_round();
                self.mix_digest(&[self.now, 0xF5, i as u64, term, round]);
                self.log_line(format!("{} FSYNC term={term} round={round}", self.nodes[i].id));
                let acts =
                    self.nodes[i].peer.as_mut().unwrap().step(Event::Persisted { term, round });
                self.handle_actions(i, acts)?;
            }
        }
        Ok(())
    }

    fn deliver_ticks(&mut self) -> Result<(), Violation> {
        for i in 0..self.nodes.len() {
            while self.nodes[i].next_tick <= self.now {
                self.nodes[i].next_tick += self.nodes[i].period;
                if self.nodes[i].peer.is_none() {
                    continue;
                }
                self.check_overlap(i)?;
                self.mix_digest(&[self.now, 0x71, i as u64]);
                let acts = self.nodes[i].peer.as_mut().unwrap().step(Event::Tick);
                self.handle_actions(i, acts)?;
            }
        }
        Ok(())
    }

    /// **The lease rule.** A leader that has lost a majority must stop leading before anybody tells
    /// it to, so two nodes may not both hold the office for longer than the lease window — measured
    /// in the *stale* node's own ticks, because that is the clock its lease is counted on.
    fn check_overlap(&mut self, i: usize) -> Result<(), Violation> {
        let me_leader =
            self.nodes[i].peer.as_ref().map(|p| p.role() == Role::Leader).unwrap_or(false);
        if !me_leader {
            self.nodes[i].overlap = 0;
            return Ok(());
        }
        let others = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(j, n)| {
                *j != i && n.peer.as_ref().map(|p| p.role() == Role::Leader).unwrap_or(false)
            })
            .count();
        if others == 0 {
            self.nodes[i].overlap = 0;
            return Ok(());
        }
        self.nodes[i].overlap += 1;
        self.report.max_overlap_ticks = self.report.max_overlap_ticks.max(self.nodes[i].overlap);
        if self.nodes[i].overlap > self.nodes[i].lease {
            let who: Vec<String> = self.leaders().iter().map(|n| n.to_string()).collect();
            let detail = format!(
                "{} has led for {} of its own ticks alongside another leader; its lease is {}. \
                 Leaders now: {}",
                self.nodes[i].id,
                self.nodes[i].overlap,
                self.nodes[i].lease,
                who.join(", ")
            );
            return Err(self.violation("two leaders overlapped for longer than the lease", detail));
        }
        Ok(())
    }

    // -- performing what a node asked for -------------------------------------------------------

    fn handle_actions(&mut self, i: usize, actions: Vec<Action>) -> Result<(), Violation> {
        // A send is released only once every persist that preceded it in the same batch has
        // reached the disk. That is the caller's obligation under the contract -- `PersistHardState`
        // exists as a separate action precisely so it can be ordered before the `Send`. The
        // simulator honours it, and separately *checks* the state machine's half of the same rule,
        // because a state machine that relies on the caller ordering for it is one that breaks
        // against a caller that does not.
        let mut release = self.now;
        for a in actions {
            match a {
                Action::PersistHardState { term, voted_for } => {
                    let hs = HardState { term, voted_for };
                    self.nodes[i].store.hard_pending = Some(hs.clone());
                    let lat = self.draw(self.cfg.faults.fsync);
                    let at = self.nodes[i].store.schedule(self.now, lat, Some(hs));
                    release = release.max(at);
                    self.log_line(format!(
                        "{} ACT PersistHardState term={term} voted_for={voted_for:?}",
                        self.nodes[i].id
                    ));
                }
                Action::Persist { entries } => {
                    let span = match (entries.first(), entries.last()) {
                        (Some(f), Some(l)) => format!("{}..={}", f.round, l.round),
                        _ => "empty".to_string(),
                    };
                    if let Err((rule, detail)) =
                        self.nodes[i].store.persist(&entries, &self.committed)
                    {
                        let d = format!("{} on {}", detail, self.nodes[i].id);
                        return Err(self.violation(rule, d));
                    }
                    let lat = self.draw(self.cfg.faults.fsync);
                    let at = self.nodes[i].store.schedule(self.now, lat, None);
                    release = release.max(at);
                    self.log_line(format!("{} ACT Persist {span}", self.nodes[i].id));
                }
                Action::Truncate { from } => {
                    if let Err((rule, detail)) = self.nodes[i].store.truncate(from, &self.committed)
                    {
                        let d = format!("{} on {}", detail, self.nodes[i].id);
                        return Err(self.violation(rule, d));
                    }
                    self.log_line(format!("{} ACT Truncate from={from}", self.nodes[i].id));
                }
                Action::Send(m) => {
                    self.check_send(i, &m)?;
                    self.enqueue(m, release);
                }
                Action::Apply { through } => {
                    self.on_apply(i, through)?;
                }
                Action::RoleChanged { role, term, leader } => {
                    self.on_role(i, role, term, leader)?;
                }
                Action::Refuse { why } => {
                    self.report.refusals += 1;
                    self.log_line(format!("{} ACT Refuse {why}", self.nodes[i].id));
                }
            }
        }
        Ok(())
    }

    /// **The two durability-before-speech rules**, checked at the moment the message leaves the
    /// state machine rather than at the moment it reaches the wire.
    ///
    /// Strict on purpose. A caller is not obliged to hold a send back until an fsync lands, so a
    /// state machine that emits a vote or an acknowledgement before the corresponding durability
    /// has been *recorded* is broken even when this particular caller happens to save it.
    fn check_send(&mut self, i: usize, m: &Message) -> Result<(), Violation> {
        let eh = self.nodes[i].store.effective_hard();
        match &m.body {
            Body::RequestVote { .. } => {
                if eh.term != m.term || eh.voted_for != Some(m.from) {
                    let detail = format!(
                        "{} asked for votes in term {} while its hard state records {:?} in term {}",
                        m.from, m.term, eh.voted_for, eh.term
                    );
                    return Err(
                        self.violation("a vote was sent before its hard state was durable", detail)
                    );
                }
            }
            Body::RequestVoteResp { granted: true } => {
                if eh.term != m.term || eh.voted_for != Some(m.to) {
                    let detail = format!(
                        "{} granted {} the vote in term {} while its hard state records {:?} in term {}",
                        m.from, m.to, m.term, eh.voted_for, eh.term
                    );
                    return Err(
                        self.violation("a vote was sent before its hard state was durable", detail)
                    );
                }
            }
            Body::AppendResp { success: true, matched, .. } => {
                let durable = self.nodes[i].store.durable_round();
                if *matched > durable {
                    let detail = format!(
                        "{} acknowledged round {} to {} with only {} fsynced",
                        m.from, matched, m.to, durable
                    );
                    return Err(
                        self.violation("an append was acknowledged before it was durable", detail)
                    );
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn enqueue(&mut self, m: Message, release: u64) {
        self.report.sent += 1;
        if self.node(m.to).is_none() {
            return;
        }
        if self.blocked.contains(&(m.from.0, m.to.0)) {
            self.report.dropped_partition += 1;
            return;
        }
        if self.draw_pct(self.cfg.faults.drop_pct) {
            self.report.dropped_loss += 1;
            return;
        }
        let at = release + self.draw(self.cfg.faults.latency);
        self.seq += 1;
        self.wire.insert((at, self.seq), Wired { released: release, dup: false, msg: m.clone() });
        if self.draw_pct(self.cfg.faults.dup_pct) {
            // An independent latency draw, so a copy may land before its original: duplication and
            // reordering at once, which is what a retransmitting network actually does.
            let at2 = release + self.draw(self.cfg.faults.latency);
            self.seq += 1;
            self.wire.insert((at2, self.seq), Wired { released: release, dup: true, msg: m });
            self.report.duplicated += 1;
        }
    }

    /// **State-machine safety.** A round is committed the first time any node applies it, and from
    /// then on no node may apply a different command there.
    fn on_apply(&mut self, i: usize, through: Round) -> Result<(), Violation> {
        if through < self.nodes[i].applied {
            let detail = format!(
                "{} applied through {} having already applied through {}",
                self.nodes[i].id, through, self.nodes[i].applied
            );
            return Err(self.violation("an apply watermark went backwards", detail));
        }
        self.nodes[i].applied = through;
        self.log_line(format!("{} ACT Apply through={through}", self.nodes[i].id));
        let from = self.nodes[i].checked + 1;
        for r in from..=through {
            let Some(e) = self.nodes[i].store.entry_at(r).cloned() else {
                let detail = format!(
                    "{} applied through {} but holds only {} rounds",
                    self.nodes[i].id,
                    through,
                    self.nodes[i].store.log.len()
                );
                return Err(self.violation("a node applied a round it does not hold", detail));
            };
            match self.committed.get(&r) {
                Some(c) if *c != e => {
                    let detail = format!(
                        "round {r}: {} applied term {} ({}) where term {} ({}) was already committed",
                        self.nodes[i].id,
                        e.term,
                        command_summary(&e.command),
                        c.term,
                        command_summary(&c.command)
                    );
                    return Err(
                        self.violation("two different commands were committed at one round", detail)
                    );
                }
                Some(_) => {}
                None => {
                    self.committed.insert(r, e);
                }
            }
            self.nodes[i].checked = r;
        }
        Ok(())
    }

    /// **At most one leader per term**, and **leader completeness**.
    fn on_role(
        &mut self,
        i: usize,
        role: Role,
        term: Term,
        leader: Option<NodeId>,
    ) -> Result<(), Violation> {
        self.log_line(format!(
            "{} ROLE {role} term={term} leader={leader:?}",
            self.nodes[i].id
        ));
        if role != Role::Leader {
            self.nodes[i].overlap = 0;
            return Ok(());
        }
        self.report.elections += 1;
        let me = self.nodes[i].id;
        if let Some(prev) = self.leaders.get(&term) {
            if *prev != me {
                let detail = format!("term {term} was led by {prev} and then by {me}");
                return Err(self.violation("two leaders in one term", detail));
            }
        } else {
            self.leaders.insert(term, me);
        }
        // The election restriction, caught where it is cheap: a node that could not have held every
        // committed round must not have been able to win. Sound because the simulator only records
        // a round as committed once a node has applied it, which is at or after the real commit.
        let missing: Vec<Round> = self
            .committed
            .iter()
            .filter(|(r, e)| self.nodes[i].store.entry_at(**r) != Some(*e))
            .map(|(r, _)| *r)
            .collect();
        if !missing.is_empty() {
            let detail = format!(
                "{me} became leader of term {term} without committed round(s) {:?} (holds {} rounds)",
                &missing[..missing.len().min(8)],
                self.nodes[i].store.log.len()
            );
            return Err(self.violation("a new leader was missing a committed round", detail));
        }
        Ok(())
    }

    // -- bookkeeping ---------------------------------------------------------------------------

    fn violation(&self, rule: &'static str, detail: String) -> Violation {
        Violation {
            seed: self.seed,
            at: self.now,
            rule,
            detail,
            trace: self.trace.iter().cloned().collect(),
        }
    }

    fn log_line(&mut self, line: String) {
        if !self.cfg.trace {
            return;
        }
        if self.trace.len() == TRACE_TAIL {
            self.trace.pop_front();
        }
        self.trace.push_back(format!("u{:<6} {line}", self.now));
    }

    fn mix_digest(&mut self, vs: &[u64]) {
        for v in vs {
            let mut x = self.report.digest ^ v.wrapping_add(0x9E37_79B9_7F4A_7C15);
            x ^= x >> 30;
            x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            x ^= x >> 27;
            x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
            x ^= x >> 31;
            self.report.digest = x;
        }
    }
}

/// Run many seeds of one configuration, stopping at the first failure.
///
/// Returns the totals either way: a sweep that found nothing has proved nothing unless it also
/// elected leaders and committed rounds, and the caller is expected to assert that it did.
pub fn sweep<P: Peer>(first_seed: u64, count: u64, cfg: &SimConfig) -> Sweep {
    let mut totals = Report::default();
    for k in 0..count {
        let seed = first_seed.wrapping_add(k);
        let mut sim = Sim::<P>::new(seed, cfg.clone());
        match sim.run() {
            Ok(r) => totals.absorb(&r),
            Err(v) => return Sweep { seeds_run: k + 1, totals, violation: Some(v) },
        }
    }
    Sweep { seeds_run: count, totals, violation: None }
}

// ---------------------------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------------------------

fn body_code(b: &Body) -> u64 {
    match b {
        Body::PreVote { .. } => 1,
        Body::PreVoteResp { .. } => 2,
        Body::RequestVote { .. } => 3,
        Body::RequestVoteResp { .. } => 4,
        Body::Append { .. } => 5,
        Body::AppendResp { .. } => 6,
        Body::InstallSnapshot { .. } => 7,
        Body::InstallSnapshotResp { .. } => 8,
    }
}

fn body_summary(b: &Body) -> String {
    match b {
        Body::PreVote { last_term, last_round } => format!("PreVote(lt={last_term},lr={last_round})"),
        Body::PreVoteResp { granted } => format!("PreVoteResp({granted})"),
        Body::RequestVote { last_term, last_round } => {
            format!("RequestVote(lt={last_term},lr={last_round})")
        }
        Body::RequestVoteResp { granted } => format!("RequestVoteResp({granted})"),
        Body::Append { prev_round, prev_term, entries, commit } => format!(
            "Append(prev={prev_round}/{prev_term},n={},commit={commit})",
            entries.len()
        ),
        Body::AppendResp { success, matched, hint, digest } => {
            format!("AppendResp({success},matched={matched},hint={hint},digest={digest})")
        }
        Body::InstallSnapshot { meta, offset, data, done } => format!(
            "InstallSnapshot(round={},offset={offset},len={},done={done})",
            meta.last_round,
            data.len()
        ),
        Body::InstallSnapshotResp { received_through } => {
            format!("InstallSnapshotResp({received_through})")
        }
    }
}

fn command_summary(c: &Command) -> String {
    match c {
        Command::WalBatch { start_lsn, bytes } => format!("WalBatch(lsn={start_lsn},{}b)", bytes.len()),
        Command::Catalog { table, .. } => format!("Catalog({table})"),
        Command::Branch { .. } => "Branch".to_string(),
        Command::ArenaGrant { node, .. } => format!("ArenaGrant({node})"),
        Command::TxnIdRange { node, .. } => format!("TxnIdRange({node})"),
        Command::LeaseTick { unix_millis } => format!("LeaseTick({unix_millis})"),
        Command::Checkpoint => "Checkpoint".to_string(),
        Command::Membership { config } => format!("Membership(v{})", config.version),
        Command::NoOp => "NoOp".to_string(),
    }
}

// The house style is a sibling `tests_*.rs` declared from `mod.rs`, and this deviates from it for
// one reason: `mod.rs` is the shared contract and is frozen for this phase, so the only line added
// there is the `pub mod sim;` without which this file is not compiled at all. Attaching the tests
// here costs one attribute and no edit to a file five other agents are building against.
#[cfg(test)]
#[path = "tests_sim.rs"]
mod tests_sim;
