//! **The driver.** `Consensus` is a pure function of its events — it never reads a clock, never
//! touches a socket and never writes a byte. Something has to do those three things, and this is
//! that something.
//!
//! # The whole contract, in four lines
//!
//! * deliver [`Event::Tick`] on a real clock;
//! * route [`Action::Send`] through the [`Transport`], and inbound frames back as [`Event::Recv`];
//! * fulfil [`Action::Persist`] against the durable [`RoundLog`], then feed [`Event::Persisted`]
//!   back in — **never before the fsync returns**, because an ack for a round that is not on this
//!   node's disk is how a correlated power loss becomes acknowledged data loss;
//! * fulfil [`Action::PersistHardState`] before any `Send` that follows it in the same batch.
//!
//! # Why the ordering inside one action batch is load-bearing
//!
//! [`Consensus::step`] returns a `Vec<Action>` whose **order is part of the contract**, not an
//! implementation detail. `PersistHardState` precedes the `Send` of the vote it authorises, and the
//! reason is stated on the variant itself: a node that votes, crashes, and comes back having
//! forgotten the vote can vote twice in one term, which elects two leaders of that term. So
//! [`Node::perform`] is called in order and each action completes — fsync included — before the
//! next begins. Batching the sends, or performing the persists on another thread, silently
//! reintroduces exactly that bug and no test in `consensus/` can see it, because the state machine
//! emitted a correct batch either way.
//!
//! # What this module deliberately does not do
//!
//! No snapshot install (F6), no signing (F7). Both are refused loudly where they would be needed
//! rather than skipped quietly — a driver that ignores an action it cannot perform is a driver that
//! reports healthy while the cluster stalls.

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::config::Config;
use super::log::{LogError, RoundLog};
use super::transport::{Transport, TransportOptions};
use super::{Action, Command, Consensus, Entry, Event, HardState, NodeId, Role, Round, Term};
use crate::error::FerroError;

/// Where a committed entry goes once consensus has agreed on it.
///
/// A trait rather than a concrete type because the two callers want different things and neither is
/// a subset of the other: a server hands `Command::WalBatch` to
/// [`crate::replication::ReplicaApplier`], while a test wants to record what arrived and assert on
/// it. The state machine is indifferent — it has agreed on the round either way.
///
/// **`apply` must be idempotent for `WalBatch`.** ferrodb's redo is idempotent by page LSN, which
/// is what makes re-delivery after a restart safe; an applier that is not loses that property for
/// the whole cluster.
pub trait Applier {
    fn apply(&mut self, entry: &Entry) -> Result<(), FerroError>;
}

/// An applier that keeps the committed rounds and nothing else.
///
/// Useful on its own: a node that only needs to *agree* — a witness, or a test asserting that
/// nothing acknowledged was lost — does not need a storage engine attached to answer that.
#[derive(Debug, Default)]
pub struct RecordingApplier {
    pub applied: Vec<Entry>,
}

impl Applier for RecordingApplier {
    fn apply(&mut self, entry: &Entry) -> Result<(), FerroError> {
        self.applied.push(entry.clone());
        Ok(())
    }
}

/// How this node is reachable, and how fast its clock runs.
pub struct NodeOptions {
    /// Directory for the round log and the hard-state record. Created if absent.
    pub dir: PathBuf,
    /// Every other member's address. **Must not contain `self_id`** — the transport refuses that,
    /// because a sender thread for one's own id dials this process's own listener for ever.
    pub peers: BTreeMap<NodeId, SocketAddr>,
    /// Wall-clock duration of one [`Event::Tick`].
    ///
    /// The state machine counts ticks and knows nothing about seconds; every timeout in
    /// `config.rs` is a tick count. This is the only place the two meet, so it is the only knob
    /// that turns a correct-but-slow cluster into a correct-and-fast one.
    pub tick: Duration,
    /// Seed for the election-timeout jitter. Two nodes with the same seed draw the same timeout and
    /// split the vote every term, so a cluster must give each node a different one.
    pub seed: u64,
    pub transport: TransportOptions,
}

impl NodeOptions {
    pub fn new(dir: impl Into<PathBuf>, peers: BTreeMap<NodeId, SocketAddr>, seed: u64) -> Self {
        NodeOptions {
            dir: dir.into(),
            peers,
            tick: Duration::from_millis(50),
            seed,
            transport: TransportOptions::default(),
        }
    }

    /// Set the wall-clock duration of one tick.
    pub fn tick_of(mut self, d: Duration) -> Self {
        assert!(!d.is_zero(), "a zero tick makes every timeout in config.rs expire instantly");
        self.tick = d;
        self
    }
}

/// A running node: the state machine, its durable log, its socket, and the clock that drives them.
pub struct Node<A: Applier> {
    sm: Consensus,
    log: RoundLog,
    net: Transport,
    applier: A,
    dir: PathBuf,
    tick: Duration,
    next_tick: Instant,
    /// Highest round handed to the applier. Distinct from the state machine's own `applied`, which
    /// is what it has *asked* for; this is what actually reached the engine.
    applied: Round,
    pending: VecDeque<Event>,
    /// Refusals the state machine produced, oldest first. Held rather than logged because the
    /// proposer is the only thing that can act on one.
    refusals: Vec<FerroError>,
    /// Role transitions since the last drain, for a caller that wants them without polling.
    transitions: Vec<(Role, Term, Option<NodeId>)>,
}

/// The hard-state record: `term`, then a tagged `voted_for`, then a checksum over both.
///
/// Fixed width and checksummed rather than a text format, for the same reason the WAL is: this file
/// is written on the critical path of every vote, and a torn or truncated write must be
/// *detectable*. A parse that guesses would answer "term 0, voted for nobody" for a corrupt record,
/// which is precisely the amnesia the record exists to prevent.
const HARD_LEN: usize = 8 + 1 + 4 + 4;

fn hard_encode(h: &HardState) -> [u8; HARD_LEN] {
    let mut b = [0u8; HARD_LEN];
    b[0..8].copy_from_slice(&h.term.to_le_bytes());
    match h.voted_for {
        Some(NodeId(n)) => {
            b[8] = 1;
            b[9..13].copy_from_slice(&n.to_le_bytes());
        }
        None => b[8] = 0,
    }
    let sum = crc32(&b[0..13]);
    b[13..17].copy_from_slice(&sum.to_le_bytes());
    b
}

fn hard_decode(b: &[u8]) -> Result<HardState, FerroError> {
    if b.len() != HARD_LEN {
        return Err(FerroError::Io(format!(
            "the hard-state record is {} bytes, not {HARD_LEN}: it was torn by a crash mid-write, \
             and guessing a term here is how a node votes twice in one term",
            b.len()
        )));
    }
    let stored = u32::from_le_bytes([b[13], b[14], b[15], b[16]]);
    if stored != crc32(&b[0..13]) {
        return Err(FerroError::Io(
            "the hard-state record's checksum does not match its body; refusing to guess a term \
             from a corrupt record, because the wrong guess elects two leaders of one term"
                .to_string(),
        ));
    }
    let term = u64::from_le_bytes(b[0..8].try_into().expect("8 bytes"));
    let voted_for = match b[8] {
        0 => None,
        1 => Some(NodeId(u32::from_le_bytes(b[9..13].try_into().expect("4 bytes")))),
        other => {
            return Err(FerroError::Io(format!(
                "the hard-state record's voted_for tag is {other}, which is neither 0 nor 1"
            )))
        }
    };
    Ok(HardState { term, voted_for })
}

/// CRC-32 (IEEE), computed rather than pulled in: the record is 13 bytes and this is called once
/// per vote, so a dependency would buy nothing.
fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// A drain that never terminates is a hung node, and a hung node is indistinguishable from a slow
/// one from outside. Each `Persisted` fed back must advance the durable round, so the queue is
/// bounded in practice; this catches the case where it does not and says which node stalled.
const MAX_DRAIN_STEPS: usize = 10_000;

impl<A: Applier> Node<A> {
    /// Start a node, reserving the listener first.
    ///
    /// `listener` rather than an address because a cluster cannot be stood up race-free otherwise:
    /// every node's peer map needs every other node's address, so binding A to discover its port
    /// and then constructing B is circular. `Transport::from_listener` says the same thing.
    pub fn start(
        self_id: NodeId,
        cfg: Config,
        listener: TcpListener,
        opts: NodeOptions,
        applier: A,
    ) -> Result<Node<A>, FerroError> {
        fs::create_dir_all(&opts.dir).map_err(|e| FerroError::Io(e.to_string()))?;

        let log = RoundLog::open(&opts.dir).map_err(LogError::into_ferro)?;
        let hard = load_hard_state(&opts.dir)?;

        let mut sm = Consensus::new(self_id, cfg, opts.seed);
        // Restore BEFORE the first tick. A node that campaigns on a fresh `HardState` while its
        // disk says otherwise is the double-vote bug arriving through the restart path instead of
        // the message path.
        let entries = read_all(&log)?;
        sm.restore(hard, log.snapshot_round(), log.snapshot_term(), entries);

        let net = Transport::from_listener(self_id, listener, opts.peers, opts.transport)?;
        let now = Instant::now();
        Ok(Node {
            sm,
            log,
            net,
            applier,
            dir: opts.dir,
            tick: opts.tick,
            next_tick: now + opts.tick,
            applied: 0,
            pending: VecDeque::new(),
            refusals: Vec::new(),
            transitions: Vec::new(),
        })
    }

    pub fn id(&self) -> NodeId { self.sm.id() }
    pub fn role(&self) -> Role { self.sm.role() }
    pub fn term(&self) -> Term { self.sm.term() }
    pub fn leader(&self) -> Option<NodeId> { self.sm.leader() }
    pub fn commit_round(&self) -> Round { self.sm.commit_round() }
    pub fn last_round(&self) -> Round { self.log.last_round() }
    /// The highest round actually handed to the applier.
    pub fn applied(&self) -> Round { self.applied }
    pub fn applier(&self) -> &A { &self.applier }
    pub fn local_addr(&self) -> SocketAddr { self.net.local_addr() }
    pub fn take_refusals(&mut self) -> Vec<FerroError> { std::mem::take(&mut self.refusals) }
    pub fn take_transitions(&mut self) -> Vec<(Role, Term, Option<NodeId>)> {
        std::mem::take(&mut self.transitions)
    }

    /// Ask for a command to be committed. Only meaningful on a leader; anywhere else the state
    /// machine produces a `NotLeader` refusal, which lands in [`Node::take_refusals`].
    ///
    /// Returns the round the leader assigned. **That is not an acknowledgement** — the round is
    /// acknowledged when [`Node::commit_round`] reaches it, which is the only moment a quorum has
    /// it on disk.
    pub fn propose(&mut self, c: Command) -> Result<Round, FerroError> {
        let before = self.log.last_round();
        self.pending.push_back(Event::Propose(c));
        self.drain()?;
        Ok(self.log.last_round().max(before))
    }

    /// One turn of the loop: collect what has arrived, fire the tick if it is due, run the state
    /// machine to quiescence.
    ///
    /// Blocks at most `budget`, and never past the next tick — a driver that sleeps through a tick
    /// makes the election timeout a function of message arrival, so a silent cluster never notices
    /// its leader died.
    pub fn poll(&mut self, budget: Duration) -> Result<(), FerroError> {
        let now = Instant::now();
        let until_tick = self.next_tick.saturating_duration_since(now);
        let wait = budget.min(until_tick);

        if wait.is_zero() {
            while let Some(m) = self.net.try_recv() {
                self.pending.push_back(Event::Recv(m));
            }
        } else if let Some(m) = self.net.recv_timeout(wait) {
            self.pending.push_back(Event::Recv(m));
            while let Some(m) = self.net.try_recv() {
                self.pending.push_back(Event::Recv(m));
            }
        }

        // Catch up whole ticks rather than one per poll. A driver that fell behind — a long fsync, a
        // descheduled thread — otherwise silently slows every timeout in the cluster by exactly the
        // amount it is behind, and the symptom is an election that will not start.
        let now = Instant::now();
        while self.next_tick <= now {
            self.pending.push_back(Event::Tick);
            self.next_tick += self.tick;
        }

        self.drain()
    }

    fn drain(&mut self) -> Result<(), FerroError> {
        let mut steps = 0usize;
        while let Some(ev) = self.pending.pop_front() {
            steps += 1;
            if steps > MAX_DRAIN_STEPS {
                return Err(FerroError::Internal(format!(
                    "node {:?} ran {MAX_DRAIN_STEPS} state-machine steps without the event queue \
                     emptying. Each Persist should feed back exactly one Persisted and stop, so \
                     this is a feedback loop, not a busy node.",
                    self.sm.id()
                )));
            }
            for a in self.sm.step(ev) {
                self.perform(a)?;
            }
        }
        Ok(())
    }

    /// Perform one action, completely, before returning. See the module header for why the order
    /// matters and why none of this may be deferred.
    fn perform(&mut self, a: Action) -> Result<(), FerroError> {
        match a {
            Action::Send(m) => {
                // A send that cannot be delivered is dropped and counted by the transport, not an
                // error here: consensus is designed for a lossy network, and treating one
                // undeliverable heartbeat as a node failure would take a healthy node down.
                let _ = self.net.send(&m);
            }

            Action::PersistHardState { term, voted_for } => {
                store_hard_state(&self.dir, &HardState { term, voted_for })?;
            }

            Action::Persist { entries } => {
                if entries.is_empty() {
                    return Ok(());
                }
                // The term to report back is this node's term NOW, not the term on the entries. The
                // state machine compares it against the term in which these rounds were last
                // rewritten, to reject a persist that a truncation overtook — see `LogTail`'s
                // `rewritten_in`. Reporting the entry's own term would make that check compare a
                // value against itself and always pass.
                let term = self.sm.term();
                let round = entries.last().expect("non-empty above").round;
                self.log.append(&entries).map_err(LogError::into_ferro)?;
                self.log.sync().map_err(LogError::into_ferro)?;
                self.pending.push_back(Event::Persisted { term, round });
            }

            Action::Truncate { from } => {
                self.log.truncate_from(from).map_err(LogError::into_ferro)?;
            }

            Action::Apply { through } => {
                while self.applied < through {
                    let next = self.applied + 1;
                    if next <= self.log.snapshot_round() {
                        // F6's row. Refusing here rather than skipping: a node that silently steps
                        // over rounds it cannot read has applied a different history from its
                        // peers, and nothing downstream can tell.
                        return Err(FerroError::Internal(format!(
                            "round {next} is below the snapshot floor {}, so applying it needs \
                             state transfer (F6), which is not implemented. Refusing rather than \
                             skipping the round.",
                            self.log.snapshot_round()
                        )));
                    }
                    let e = self.log.entry(next).map_err(LogError::into_ferro)?;
                    self.applier.apply(&e)?;
                    self.applied = next;
                }
            }

            Action::RoleChanged { role, term, leader } => {
                self.transitions.push((role, term, leader));
            }

            Action::Refuse { why } => self.refusals.push(why),
        }
        Ok(())
    }

    /// Stop the transport and join its threads. Idempotent.
    pub fn shutdown(&self) {
        self.net.shutdown();
    }
}

/// Every entry the durable log holds above its snapshot floor.
fn read_all(log: &RoundLog) -> Result<Vec<Entry>, FerroError> {
    if log.is_empty() {
        return Ok(Vec::new());
    }
    log.range(log.first_round(), usize::MAX, usize::MAX).map_err(LogError::into_ferro)
}

fn hard_path(dir: &Path) -> PathBuf {
    dir.join("hardstate")
}

fn load_hard_state(dir: &Path) -> Result<HardState, FerroError> {
    let p = hard_path(dir);
    match File::open(&p) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HardState::default()),
        Err(e) => Err(FerroError::Io(e.to_string())),
        Ok(mut f) => {
            let mut buf = Vec::new();
            f.read_to_end(&mut buf).map_err(|e| FerroError::Io(e.to_string()))?;
            hard_decode(&buf)
        }
    }
}

/// Write the hard state and **return only once it is on the device**.
///
/// Temp-then-rename, with the directory fsynced after: a rename is atomic, so a crash leaves either
/// the old record or the new one and never a torn one. Writing in place would be one syscall
/// shorter and would allow exactly the torn record `hard_decode` has to refuse.
fn store_hard_state(dir: &Path, h: &HardState) -> Result<(), FerroError> {
    let tmp = dir.join("hardstate.tmp");
    let dst = hard_path(dir);
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| FerroError::Io(e.to_string()))?;
        f.write_all(&hard_encode(h)).map_err(|e| FerroError::Io(e.to_string()))?;
        f.sync_all().map_err(|e| FerroError::Io(e.to_string()))?;
    }
    fs::rename(&tmp, &dst).map_err(|e| FerroError::Io(e.to_string()))?;
    // The rename itself must be durable, or a crash can resurrect the previous vote.
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| FerroError::Io(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
#[path = "tests_node.rs"]
mod tests_node;
