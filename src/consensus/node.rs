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
//! # State transfer, and the two things only a driver can do
//!
//! `Consensus` decides *whether* a snapshot may be sent, accepted or installed; it can do none of
//! the three, because all three are bytes on a disk. So this file owns exactly two things F6 needs
//! and nothing else:
//!
//! * **the retention policy.** A leader that never checkpoints never has a follower below its
//!   floor, so state transfer is code that never runs. [`NodeOptions::retain_rounds`] is how many
//!   rounds of log to keep above what the storage engine has applied; the log below that is
//!   discarded on both the state machine and the disk *in the same step*, because a floor that
//!   moved on one and not the other is refused by `ensure_log` on the next event.
//! * **the bytes.** An arriving chunk goes from the frame to a spool file without ever being
//!   retained by the state machine (`snapshot.rs` explains why that asymmetry is deliberate), and a
//!   completed spool is installed through [`SnapshotStore`] before — never after — the state
//!   machine is told, via the `Persisted` event that already means "this is on the disk".
//!
//! A node with no [`SnapshotStore`] configured **refuses loudly** where one would be needed rather
//! than skipping quietly: a driver that ignores work it cannot perform is a driver that reports
//! healthy while the cluster stalls.
//!
//! # What this module deliberately does not do
//!
//! No signing (F7).

/// The snapshot record: the digest at this node's log floor, which nothing else can recompute.
///
/// `RoundLog`'s header carries the floor's round and term, and that is enough to *read* the log.
/// It is not enough to *answer for* it: `AppendResp.digest` is a rolling hash chained from the
/// digest at the floor, and every entry that value was folded over has been discarded by the
/// checkpoint that created the floor. A node that came back and anchored its chain at zero would
/// disagree with its leader at every round above the floor and be latched as diverged — a healthy
/// node, permanently out of the quorum, reported as byte-level corruption.
///
/// Kept beside the hard state rather than inside the log's header because the log's header is a
/// durable format F0b owns and a fifth field there is a migration; this is a new file that did not
/// exist before, so it has no old readers to break.
const SNAP_MAGIC: u32 = 0xF6_5A_00_01;
const SNAP_LEN: usize = 4 + 8 + 8 + 8 + 4;

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::io::Read;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::config::Config;
use super::log::{LogError, RoundLog};
use super::signing::Key;
use super::transport::{Transport, TransportOptions};
use super::{Action, Command, Consensus, Entry, Event, HardState, NodeId, Role, Round, Term};
use crate::error::FerroError;
use crate::storage::atomic_file::{replace_atomically, OsFileOps};

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

/// Where this node's [`Event::Tick`]s come from.
///
/// The state machine counts ticks and knows nothing about seconds; this is the one decision about
/// what a tick *is*.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clock {
    /// One tick per [`NodeOptions::tick`] of wall time, caught up whole when the driver falls
    /// behind. **A server's clock, and the default.**
    Wall,
    /// **A test harness's clock: one tick per [`Node::poll`], and no read of the wall on the way.**
    ///
    /// D73. A harness that turns every node of a cluster from one loop, on the wall clock, makes an
    /// election a function of how long the loop took: an SQL statement, an fsync or a descheduled
    /// thread between two turns is time to the state machine, and on a loaded machine it is enough
    /// time to lapse the leader's lease and elect somebody else. Under this clock only a turn is
    /// time, so nothing the loop does between turns reaches a timeout in `config.rs`.
    ///
    /// **The socket is not read in `poll` either**, and that half matters as much as the first.
    /// Messages are moved into the event queue by `Node::collect`, which the harness calls for
    /// every node *between* turns, once `Node::frames` summed over the fleet says nothing is in
    /// flight. Read inside `poll`, a message from a node turned earlier in the same turn would land
    /// in this turn or the next according to whether TCP beat the loop to the next node — the same
    /// dependence on scheduling, moved from the clock to the network. Collected between turns,
    /// every message takes exactly one turn.
    ///
    /// **And the transport's idle close is switched off** (`Node::start` says why): it is the one
    /// wall clock left on the delivery path. What remains on the wall is connection setup: a dial
    /// retried every `reconnect_delay`, and a handshake the receiver gives up on after
    /// `handshake_deadline`. Frames wait in the sender's queue until a connection exists, so setup
    /// can delay a turn; a receiver that gives up on a handshake the dialler already finished loses
    /// the dialler's first frame uncounted, which a harness waiting for delivery refuses by name.
    /// Neither can change what a turn does.
    ///
    /// ⛔ **Never a server's clock.** The objection in
    /// `a_tick_is_delivered_on_the_clock_and_missed_ticks_are_caught_up` still holds for any
    /// process that polls its own node: a driver that ticks once per poll makes every timeout a
    /// function of how often the caller polls, so a busy node's leader is declared dead by everyone
    /// else while it believes it is fine. It does not hold for a harness only because one caller
    /// turns *every* node, once each, per turn — so no node can be busier than another.
    Pumped,
}

/// One node's count of consensus frames, for a harness that must know a fleet has gone quiet.
///
/// Summed over every node of a fleet, `sent == received + lost` says every frame sent has come off
/// its socket and been counted: received, or lost by a route the transport counts. `lost` is every
/// such route, at either end: a queue that overflowed or a write that broke (the sender's), and a
/// frame refused for want of inbox space, for naming another addressee, or for failing to
/// authenticate (the receiver's). A received frame can still be a few instructions short of the
/// inbox — the counter moves just before the channel send — which is why `Node::collect` waits for
/// the count rather than draining whatever the channel holds.
///
/// **What it cannot see, stated rather than left to be found:** a frame written whole and then
/// lost with its connection, and a frame the reader closed its connection over (an unknown tag, a
/// decode failure). Neither is counted anywhere, so a fleet that suffers one never balances. That
/// is the direction to fail in: a harness waiting for the balance refuses by name, where one that
/// took the turn anyway would run it with a message missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Frames {
    pub sent: u64,
    pub received: u64,
    pub lost: u64,
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
    ///
    /// Unused under [`Clock::Pumped`], where a tick is a poll and not a duration.
    pub tick: Duration,
    /// Where ticks come from. [`Clock::Wall`] unless a test harness asks otherwise.
    pub clock: Clock,
    /// Seed for the election-timeout jitter. Two nodes with the same seed draw the same timeout and
    /// split the vote every term, so a cluster must give each node a different one.
    pub seed: u64,
    pub transport: TransportOptions,
    /// The cluster's signing key, if this node's traffic is authenticated (F7, `signing.rs`).
    ///
    /// **`None` means every frame this node sends is unsigned and every frame it receives is
    /// believed.** That is a real and supported configuration — a cluster on a network the operator
    /// already trusts — but it is the one in which anything that can reach the port can assert a
    /// later term and demote a healthy leader. Set it with [`NodeOptions::signed_with`].
    ///
    /// It lives here rather than on [`TransportOptions`] for the reason `transport.rs` gives: the
    /// options struct is knobs with defensible defaults, and a key has no default that is right.
    pub signing_key: Option<Arc<Key>>,
    /// How many rounds of log to keep above what the storage engine has applied.
    ///
    /// `None` disables checkpointing altogether, which is the right default for a node with no
    /// storage engine attached: discarding a log you cannot snapshot is discarding it.
    ///
    /// **`Some(0)` compacts to `applied` on every turn**, which is what makes a follower one round
    /// behind need state transfer. That is the anti-vacuity setting for
    /// `tests/integration_cluster_snapshot.rs`: without it the snapshot path is code that a passing
    /// test never entered.
    pub retain_rounds: Option<u64>,
    /// How this node captures and installs its own state. `None` means it cannot do either, and it
    /// says so rather than stalling.
    pub snapshots: Option<Box<dyn super::snapshot::SnapshotStore>>,
}

impl NodeOptions {
    pub fn new(dir: impl Into<PathBuf>, peers: BTreeMap<NodeId, SocketAddr>, seed: u64) -> Self {
        NodeOptions {
            dir: dir.into(),
            peers,
            tick: Duration::from_millis(50),
            clock: Clock::Wall,
            seed,
            transport: TransportOptions::default(),
            signing_key: None,
            retain_rounds: None,
            snapshots: None,
        }
    }

    /// Authenticate this node's traffic with the cluster key.
    ///
    /// Every frame it sends carries an HMAC-SHA256 tag over its own body, and every frame it
    /// receives is verified before it is parsed — so a peer that cannot produce a tag reaches
    /// neither the decoder nor the state machine. See [`super::signing`] for what that proves
    /// (possession of the key) and what it does not (**freshness** — there is no replay
    /// protection).
    ///
    /// Every node in the cluster must be given the same key: there is no negotiation, so a node
    /// with a key and a node without one cannot talk to each other in either direction.
    pub fn signed_with(mut self, key: Arc<Key>) -> Self {
        self.signing_key = Some(key);
        self
    }

    /// Attach a storage engine this node can snapshot and be snapshotted into, and say how much log
    /// to keep above what it has applied.
    pub fn with_snapshots(
        mut self,
        store: Box<dyn super::snapshot::SnapshotStore>,
        retain_rounds: u64,
    ) -> Self {
        self.snapshots = Some(store);
        self.retain_rounds = Some(retain_rounds);
        self
    }

    /// Set the wall-clock duration of one tick.
    pub fn tick_of(mut self, d: Duration) -> Self {
        assert!(!d.is_zero(), "a zero tick makes every timeout in config.rs expire instantly");
        self.tick = d;
        self
    }

    /// Tick once per [`Node::poll`] instead of once per [`NodeOptions::tick`] of wall time. **A
    /// test harness's setting and never a server's** — see [`Clock::Pumped`] for both halves.
    #[doc(hidden)]
    pub fn pumped(mut self) -> Self {
        self.clock = Clock::Pumped;
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
    /// Where ticks come from. Under [`Clock::Pumped`], `tick` and `next_tick` are never read.
    clock: Clock,
    /// Messages [`Node::collect`] has taken from the transport, over this node's life. Compared
    /// with the transport's own `received`, which is how `collect` knows it holds the whole turn.
    pulled: u64,
    /// Highest round handed to the applier. Distinct from the state machine's own `applied`, which
    /// is what it has *asked* for; this is what actually reached the engine.
    applied: Round,
    pending: VecDeque<Event>,
    /// Refusals the state machine produced, oldest first. Held rather than logged because the
    /// proposer is the only thing that can act on one.
    refusals: Vec<FerroError>,
    /// Role transitions since the last drain, for a caller that wants them without polling.
    transitions: Vec<(Role, Term, Option<NodeId>)>,
    /// F6. See the module header.
    snapshots: Option<Box<dyn super::snapshot::SnapshotStore>>,
    retain_rounds: Option<u64>,
    /// Bytes of the incoming snapshot on this node's spool, and which payload they are of.
    ///
    /// **The driver's own account of what it wrote, never inferred from the state machine's.** The
    /// state machine digests the bytes it saw; the driver installs the bytes it wrote; if the two
    /// are allowed to disagree about which chunk went where, a payload that digests correctly can
    /// still install from a spool that is a mixture of two transfers. So a chunk is written only
    /// when the state machine's cursor and this pair agree on exactly where it belongs, and a chunk
    /// the state machine accepted and the driver cannot place is refused **loudly** — it means the
    /// two rules have drifted, which is the one failure neither side can detect on its own.
    spooled: u64,
    spooled_header: Option<super::snapshot::PayloadHeader>,
    /// The round of an install this driver has already performed and is waiting to have confirmed.
    ///
    /// The confirmation is an `Event::Persisted` pushed to the back of the queue, so several
    /// further events are drained before the state machine sees it — and without this the install
    /// would be attempted again on each of them, against a spool the first one has already removed.
    installed_round: Option<Round>,
    /// How many transfers this node has **armed** as a leader — captured a snapshot for a peer
    /// that could not be served entries. Named for what it counts and not for what a reader would
    /// like it to mean: arming is not sending, and a leader that arms one and then loses its office
    /// has still armed it. Whether a transfer *finished* is a fact about the receiver, and
    /// `snapshots_installed` on that node is where it is counted.
    snapshots_armed: u64,
    snapshots_installed: u64,
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
        let (base_digest, digest_note) =
            load_snapshot_record(&opts.dir, log.snapshot_round(), log.snapshot_term())?;
        sm.restore(hard, log.snapshot_round(), log.snapshot_term(), base_digest, entries);

        // **Before the first event, and before the socket exists.** A node whose storage was left
        // half-replaced by an interrupted install must not join a cluster and start answering for
        // it; refusing here is the only point at which the answer is still "this node cannot serve",
        // rather than a database nobody can tell is a mixture of two.
        if let Some(store) = opts.snapshots.as_ref() {
            store.check_ready()?;
        }

        // **The transport's idle close is the one wall clock left on a pumped node's delivery
        // path, so it is switched off there.** It closes a connection that has been quiet for
        // `idle_deadline` of wall time, and a turn-driven cluster leaves connections quiet for as
        // long as its test likes — two followers under a stable leader never speak to each other.
        // The next frame written into a closed connection dies without being counted, and the
        // harness waiting for it refuses the turn. How long a pause lasted is exactly the fact
        // `Clock::Pumped` exists to keep from deciding anything.
        let mut transport = opts.transport;
        if opts.clock == Clock::Pumped {
            transport.idle_deadline = Duration::MAX;
        }
        let net = match opts.signing_key {
            Some(key) => {
                Transport::from_listener_with_key(self_id, listener, opts.peers, transport, key)?
            }
            None => Transport::from_listener(self_id, listener, opts.peers, transport)?,
        };
        let now = Instant::now();
        let floor = log.snapshot_round();
        let mut node = Node {
            sm,
            log,
            net,
            applier,
            dir: opts.dir,
            tick: opts.tick,
            next_tick: now + opts.tick,
            clock: opts.clock,
            pulled: 0,
            // **Seeded from the floor, not from zero.** Rounds at or below it were applied by
            // whoever produced the snapshot that created it, and they no longer exist anywhere on
            // this node — walking up from 0 would ask the log for the first of them and be refused.
            applied: floor,
            pending: VecDeque::new(),
            refusals: Vec::new(),
            transitions: Vec::new(),
            snapshots: opts.snapshots,
            retain_rounds: opts.retain_rounds,
            spooled: 0,
            spooled_header: None,
            installed_round: None,
            snapshots_armed: 0,
            snapshots_installed: 0,
        };
        if let Some(why) = digest_note {
            // Degraded, not wrong: a zero base digest is the one value `AppendResp` reads as "not
            // claiming anything", so the divergence detector goes quiet for this node rather than
            // firing on a log it cannot describe. Surfaced through `take_refusals` because a
            // detector that is off and says nothing is the failure this project keeps writing down.
            node.refusals.push(why);
        }
        Ok(node)
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
    /// Where this node's log begins. Everything at or below it is covered by a snapshot.
    pub fn snapshot_round(&self) -> Round { self.log.snapshot_round() }
    /// Transfers this node has **armed** as a leader. See the field for why it is not "sent".
    pub fn snapshots_armed(&self) -> u64 { self.snapshots_armed }
    /// Transfers this node has installed, as a follower. **The anti-vacuity counter**: a
    /// convergence test that never sees this move proved only that ordinary replication works.
    pub fn snapshots_installed(&self) -> u64 { self.snapshots_installed }
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
    ///
    /// Under [`Clock::Pumped`] it never blocks, `budget` is unused, and one call is exactly one
    /// tick: what arrived was queued by [`Node::collect`] before this turn began.
    pub fn poll(&mut self, budget: Duration) -> Result<(), FerroError> {
        if self.clock == Clock::Pumped {
            // No clock and no socket on this path — both are the scheduling dependence the pumped
            // clock removes, and `Clock::Pumped` says why the socket counts as much as the clock.
            self.pending.push_back(Event::Tick);
            return self.drain();
        }
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

    /// **The pumped clock's delivery step:** move every message this node's transport has counted
    /// as received into the event queue, ordered by sender. Returns how many.
    ///
    /// A harness calls it for every node *between* turns, once [`Node::frames`] summed over the
    /// fleet says nothing is in flight; [`Clock::Pumped`] says why it is not part of `poll`.
    ///
    /// **Ordered by sender, stably.** Two peers' frames reach the inbox on two socket threads, and
    /// which one gets there first is the scheduler's choice. The state machine accepts any order —
    /// the network is allowed to reorder — but a fleet that is to replay one election from one set
    /// of seeds needs an order that is not the scheduler's. Stable, so each sender's frames keep
    /// the order its one connection delivered them in.
    ///
    /// `within` bounds the wait for a frame the transport has counted but not yet handed over — its
    /// counter moves just before the channel send. A failure bound, not a measurement: running out
    /// is refused by name and never read as "nothing arrived".
    ///
    /// Refused on a [`Clock::Wall`] node, whose `poll` reads the socket itself: two readers of one
    /// inbox would each hold part of a turn.
    #[doc(hidden)]
    pub fn collect(&mut self, within: Duration) -> Result<usize, FerroError> {
        if self.clock != Clock::Pumped {
            return Err(FerroError::Internal(format!(
                "node {:?} runs on the wall clock, whose `poll` reads the socket itself. `collect` \
                 is the pumped clock's delivery step, and two readers of one inbox would each hold \
                 part of a turn",
                self.sm.id()
            )));
        }
        let mut arrived = Vec::new();
        while self.pulled < self.net.received() {
            let Some(m) = self.net.recv_timeout(within) else {
                return Err(FerroError::Internal(format!(
                    "node {:?}'s transport counted {} frames received and had handed over {} when \
                     it went {within:?} without producing another. A counted frame that never \
                     reaches the inbox is a transport defect, and taking the turn without it \
                     would make the turn depend on when it turned up",
                    self.sm.id(),
                    self.net.received(),
                    self.pulled
                )));
            };
            self.pulled += 1;
            arrived.push(m);
        }
        arrived.sort_by_key(|m| m.from);
        let n = arrived.len();
        self.pending.extend(arrived.into_iter().map(Event::Recv));
        Ok(n)
    }

    /// This node's side of a fleet's delivery ledger. See [`Frames`] for what the sum means, and
    /// for the two losses it cannot see.
    #[doc(hidden)]
    pub fn frames(&self) -> Frames {
        Frames {
            sent: self.net.sent(),
            received: self.net.received(),
            lost: self.net.dropped()
                + self.net.lost_in_flight()
                + self.net.inbound_dropped()
                + self.net.misrouted()
                + self.net.unauthenticated(),
        }
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
            // The chunk is taken before the step, because the step consumes the event and the
            // bytes are needed after the state machine has ruled on them.
            let chunk = snapshot_chunk_of(&ev);

            for a in self.sm.step(ev) {
                self.perform(a)?;
            }

            if let Some((offset, data)) = chunk {
                self.spool_accepted_chunk(offset, &data)?;
            }
            // Inside the loop, because both are about the event just stepped: the spool write is
            // the bytes it carried, and the install feeds a `Persisted` back into this same queue.
            // Release first: on the iteration that carries the `Persisted`, the state machine has
            // already cleared its cursor, and `install_pending_snapshot` clears `installed_round`
            // on its way out — so a release that ran after it would find nothing to release and
            // leak the spool file.
            self.release_installed_spool();
            self.install_pending_snapshot()?;
        }
        // **Outside the loop**, because neither is about any one event and both are expensive.
        // A checkpoint rewrites the log and fsyncs three times; a capture copies the page file.
        // Running either once per event rather than once per drain multiplies that by the number
        // of rounds in a batch — and a driver that spends longer in a drain than a leader's lease
        // is a driver that loses the office while doing bookkeeping.
        self.serve_snapshots()?;
        self.checkpoint()?;
        Ok(())
    }

    /// Write a chunk the state machine accepted, at the offset it accepted it at.
    ///
    /// The condition is an exact agreement between two independent accounts and never an inference
    /// from one of them:
    ///
    /// * the state machine says it now holds `offset + data.len()` bytes of a payload whose header
    ///   it names, and
    /// * this driver says its spool holds exactly `offset` bytes of that same payload — or the
    ///   chunk is at offset 0, which starts a payload and truncates whatever was there.
    ///
    /// **A chunk this driver cannot place is skipped, not refused**, and the reason is that the
    /// cursor alone cannot tell an accept from a duplicate.
    ///
    /// A re-sent chunk at offset X, arriving after the receiver has already accepted it, leaves the
    /// cursor at exactly `X + len` — the same value an accept produces. The first version of this
    /// returned a hard error on that case, which killed the node: a leader re-sends from its own
    /// `acked` on every heartbeat, so any transfer of more than one chunk that spanned a heartbeat
    /// took the process down. (The integration test escaped it only because its fixture database
    /// was one chunk.) Skipping is safe in every reading: the spool can end up SHORT, never mixed,
    /// because a write only ever happens at exactly the length the spool already holds — and a
    /// short spool is refused, by name, in `install_pending_snapshot`, before a byte of it is
    /// installed.
    fn spool_accepted_chunk(&mut self, offset: u64, data: &[u8]) -> Result<(), FerroError> {
        let Some(cur) = self.sm.snapshot_incoming() else {
            // Refused, or superseded. Nothing was accepted, so nothing is written.
            return Ok(());
        };
        let header = *cur.header();
        if cur.received() != offset + data.len() as u64 {
            // The state machine did not accept this chunk here.
            return Ok(());
        }
        let starting = offset == 0;
        let continuing = self.spooled_header == Some(header) && self.spooled == offset;
        if !starting && !continuing {
            return Ok(());
        }
        self.spool_chunk(offset, data)?;
        self.spooled = offset + data.len() as u64;
        self.spooled_header = Some(header);
        Ok(())
    }

    /// Positioned rather than appended, and the file is truncated to the new length afterwards, so
    /// a transfer that restarts at offset 0 overwrites the old one instead of leaving its tail
    /// behind.
    ///
    /// **Deliberately no fsync per chunk.** There would be nothing to resume from: the receive
    /// cursor lives in `Consensus::progress` and `spooled`/`spooled_header` are plain fields, none
    /// of which survives a restart, so a crash restarts every transfer at offset 0 whatever reached
    /// the device. A flush per megabyte would buy thousands of device flushes inside the drain loop
    /// and no durability at all — and this file's own drain comment warns that a driver which
    /// spends longer in a drain than a leader's lease loses the office. The one fsync that IS
    /// load-bearing happens once, in `install_pending_snapshot`, before the bytes are read back.
    fn spool_chunk(&mut self, offset: u64, data: &[u8]) -> Result<(), FerroError> {
        let path = spool_path(&self.dir);
        let f = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| FerroError::Io(format!("open the snapshot spool: {e}")))?;
        crate::storage::disk_manager::pwrite(&f, data, offset)
            .map_err(|e| FerroError::Io(format!("write the snapshot spool: {e}")))?;
        f.set_len(offset + data.len() as u64)
            .map_err(|e| FerroError::Io(format!("size the snapshot spool: {e}")))?;
        Ok(())
    }

    /// A fully received snapshot: install it, make it durable, and only then tell the state machine.
    ///
    /// The order is the whole of the correctness argument and is the same one `Action::Persist`
    /// makes for entries: the state machine moves its floor on the strength of `Event::Persisted`,
    /// so that event must not be fed until the bytes are on this node's device. An install reported
    /// early is a node answering for a history it does not hold.
    fn install_pending_snapshot(&mut self) -> Result<(), FerroError> {
        let Some(round) = self.sm.pending_install_round() else {
            self.installed_round = None;
            return Ok(());
        };
        if self.installed_round == Some(round) {
            // Already installed and waiting for the `Persisted` this pushed, which is at the back
            // of a queue with other events in front of it. Doing it again would open a spool the
            // first install has already removed — which is exactly how this was found.
            return Ok(());
        }
        let meta = self
            .sm
            .snapshot_incoming()
            .expect("a pending round implies a cursor")
            .meta()
            .clone();
        let digest = self
            .sm
            .snapshot_incoming()
            .expect("a pending round implies a cursor")
            .header()
            .base_digest;

        // **The two accounts are reconciled here, once, at the only moment it can be done with
        // certainty.** The state machine has digested a whole payload; this driver has written what
        // it could place. If they do not agree on the length, the spool is SHORT of what was
        // digested — a chunk was skipped because the driver could not place it — and installing
        // from it would install bytes nothing verified. Refused by name rather than discovered as a
        // truncated read halfway through `store.install`.
        let path = spool_path(&self.dir);
        let spooled_len = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        if self.spooled != meta.total_bytes || spooled_len != meta.total_bytes {
            return Err(FerroError::Internal(format!(
                "the state machine verified a {}-byte snapshot for round {round} and this node's \
                 spool holds {} bytes ({spooled_len} on disk). The two accounts of the transfer \
                 have drifted, and installing from a spool the state machine did not digest is how \
                 a payload passes its own checksum while being a mixture of two transfers.",
                meta.total_bytes, self.spooled
            )));
        }
        // The one fsync that matters, and the only one on this path: everything read back below
        // must be on the device, because the state machine moves its floor on the strength of the
        // install having returned.
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .and_then(|f| f.sync_all())
            .map_err(|e| FerroError::Io(format!("fsync the snapshot spool: {e}")))?;

        let Some(store) = self.snapshots.as_mut() else {
            return Err(FerroError::Internal(format!(
                "a snapshot covering round {round} was received whole, and this node has no \
                 storage engine attached to install it into. Refusing rather than acknowledging a \
                 transfer that changed nothing: the follower would report itself caught up and \
                 serve a database it never received."
            )));
        };
        store.install(&meta, &path)?;

        // The durable log's floor moves with the state machine's, in this step and not another.
        // The whole tail goes: a node being re-seeded holds rounds the leader serving it cannot
        // vouch for, which is why it is being re-seeded.
        if !self.log.is_empty() {
            self.log.truncate_from(self.log.first_round()).map_err(LogError::into_ferro)?;
        }
        self.log.discard_prefix(round, meta.last_term).map_err(LogError::into_ferro)?;
        store_snapshot_record(&self.dir, round, meta.last_term, digest)?;

        self.applied = self.applied.max(round);
        self.installed_round = Some(round);
        self.snapshots_installed += 1;
        // **The spool is NOT removed here.** The state machine has not yet been told, and
        // `finish_install` can still refuse — a configuration that is damage latches this node out
        // of office rather than being applied. Deleting the bytes at this point would leave nothing
        // to re-drive the install from, with the log already truncated: a node whose state machine
        // and whose disk describe different histories, permanently. `drain` removes the spool once
        // the `Persisted` below has been accepted.
        self.pending.push_back(Event::Persisted { term: self.sm.term(), round });
        Ok(())
    }

    /// The state machine has accepted the install. Release what was held for it.
    ///
    /// Separate from the install itself because the install cannot know: `finish_install` runs
    /// inside the state machine's own step, after this returns, and it can still refuse.
    fn release_installed_spool(&mut self) {
        if self.installed_round.is_some() && self.sm.pending_install_round().is_none() {
            let _ = fs::remove_file(spool_path(&self.dir));
            self.spooled = 0;
            self.spooled_header = None;
            self.installed_round = None;
        }
    }

    /// Capture a snapshot for every peer this leader cannot serve with entries.
    ///
    /// Nothing is sent here: `Consensus::send_append_to` sends the first chunk on the next
    /// heartbeat and every later one on the peer's acknowledgement, so every byte on the wire still
    /// leaves through an `Action::Send` the state machine emitted.
    fn serve_snapshots(&mut self) -> Result<(), FerroError> {
        let waiting = self.sm.peers_needing_snapshot();
        if waiting.is_empty() {
            return Ok(());
        }
        let Some(point) = self.sm.snapshot_point() else {
            // Nothing applied, so there is no state to send. The peer stays marked and is served as
            // soon as this leader applies a round, which its own term-establishing `NoOp` produces.
            return Ok(());
        };
        let Some(store) = self.snapshots.as_mut() else {
            self.refusals.push(FerroError::Internal(format!(
                "peer(s) {waiting:?} need state transfer and this node has no storage engine to \
                 capture a snapshot from, so they cannot be repaired. Naming it rather than \
                 leaving them silently stalled."
            )));
            return Ok(());
        };
        // ONE capture for every waiting peer: the payload is an image at a round, and taking it per
        // peer would copy the database once per follower.
        let snap = std::sync::Arc::new(store.capture(&point)?);
        for peer in waiting {
            if let Err(why) = self.sm.offer_snapshot_to(peer, std::sync::Arc::clone(&snap)) {
                self.refusals.push(why.into_ferro());
            } else {
                self.snapshots_armed += 1;
            }
        }
        Ok(())
    }

    /// Discard log this node no longer needs, on the state machine and the disk together.
    fn checkpoint(&mut self) -> Result<(), FerroError> {
        let Some(keep) = self.retain_rounds else { return Ok(()) };
        if self.snapshots.is_none() {
            // Discarding a log you cannot snapshot is discarding it: a follower below the floor
            // could never then be repaired. Refused by doing nothing, which is the safe direction.
            return Ok(());
        }
        let through = self.applied.saturating_sub(keep);
        if through <= self.log.snapshot_round() || through == 0 {
            return Ok(());
        }
        let term = self.log.term_at(through).map_err(LogError::into_ferro)?;
        // The state machine first: it refuses a floor above what the engine has applied, and a
        // disk floor moved past a refusal would be unrecoverable.
        self.sm.compact(through).map_err(|e| e.into_ferro())?;
        self.log.discard_prefix(through, term).map_err(LogError::into_ferro)?;
        store_snapshot_record(&self.dir, through, term, self.sm.floor_digest())?;
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
                        // Unreachable in a healthy node: `applied` is seeded from the floor on
                        // start and moved to it by an install, so nothing ever asks for a round
                        // below it. Kept as a refusal because if it ever does happen, silently
                        // stepping over the round would leave this node having applied a different
                        // history from its peers, and nothing downstream could tell.
                        return Err(FerroError::Internal(format!(
                            "round {next} is at or below the snapshot floor {}, so the entry that \
                             carried it no longer exists on this node. The applied cursor was left \
                             behind a checkpoint or an install; refusing rather than skipping the \
                             round.",
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

fn snap_path(dir: &Path) -> PathBuf {
    dir.join("snapshot")
}

/// Where an in-flight `InstallSnapshot` is spooled. One per node: a node receives from at most one
/// leader at a time, and a second concurrent transfer would be a second leader of one term.
fn spool_path(dir: &Path) -> PathBuf {
    dir.join("snapshot.incoming")
}

/// The chunk an event carries, if it carries one. Cloned before the step, because the step consumes
/// the event and the bytes are needed after the state machine has ruled on them.
fn snapshot_chunk_of(ev: &Event) -> Option<(u64, Vec<u8>)> {
    match ev {
        Event::Recv(m) => match &m.body {
            super::Body::InstallSnapshot { offset, data, .. } => Some((*offset, data.clone())),
            _ => None,
        },
        _ => None,
    }
}

fn snap_encode(round: Round, term: Term, digest: u64) -> [u8; SNAP_LEN] {
    let mut b = [0u8; SNAP_LEN];
    b[0..4].copy_from_slice(&SNAP_MAGIC.to_le_bytes());
    b[4..12].copy_from_slice(&round.to_le_bytes());
    b[12..20].copy_from_slice(&term.to_le_bytes());
    b[20..28].copy_from_slice(&digest.to_le_bytes());
    let sum = crc32(&b[0..28]);
    b[28..32].copy_from_slice(&sum.to_le_bytes());
    b
}

fn store_snapshot_record(
    dir: &Path,
    round: Round,
    term: Term,
    digest: u64,
) -> Result<(), FerroError> {
    replace_atomically(&OsFileOps, &snap_path(dir), &snap_encode(round, term, digest))
        .map_err(|e| FerroError::Io(e.to_string()))
}

/// Read back the digest at this node's log floor, refusing to use one that is not about this floor.
///
/// Returns `(digest, note)`. A `note` means the digest could not be trusted and zero was used
/// instead — which switches the divergence detector **off** for this node rather than making it
/// wrong, because zero is the value `AppendResp` already reads as "not claiming anything". A record
/// naming a different `(round, term)` than the log's header is not a record about this log: a
/// crash between the two writes leaves exactly that, and taking it would anchor the digest chain at
/// a value from a floor this node no longer has.
fn load_snapshot_record(
    dir: &Path,
    floor: Round,
    floor_term: Term,
) -> Result<(u64, Option<FerroError>), FerroError> {
    if floor == 0 {
        // No snapshot has ever moved this node's floor, so the chain starts at the beginning of the
        // log and zero is not a fallback but the correct anchor.
        return Ok((0, None));
    }
    let p = snap_path(dir);
    let mut buf = Vec::new();
    match File::open(&p) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((
                0,
                Some(FerroError::Internal(format!(
                    "this node's log begins after round {floor} and there is no snapshot record \
                     beside it, so the rolling digest at that floor cannot be recovered — every \
                     entry it was folded over has been discarded. Continuing with a zero digest, \
                     which reports 'not claiming anything' and therefore turns the divergence \
                     detector OFF for this node until its log is rebuilt from round 1."
                ))),
            ));
        }
        Err(e) => return Err(FerroError::Io(e.to_string())),
        Ok(mut f) => {
            f.read_to_end(&mut buf).map_err(|e| FerroError::Io(e.to_string()))?;
        }
    }
    let bad = |why: String| -> Result<(u64, Option<FerroError>), FerroError> {
        Ok((0, Some(FerroError::Internal(format!(
            "{why} Continuing with a zero digest, which turns the divergence detector OFF for this \
             node rather than making it report a divergence that is not there."
        )))))
    };
    if buf.len() != SNAP_LEN {
        return bad(format!(
            "the snapshot record is {} bytes, not {SNAP_LEN}: it was torn by a crash mid-write.",
            buf.len()
        ));
    }
    if u32::from_le_bytes(buf[0..4].try_into().expect("4 bytes")) != SNAP_MAGIC {
        return bad("the snapshot record does not begin with its magic.".into());
    }
    if u32::from_le_bytes(buf[28..32].try_into().expect("4 bytes")) != crc32(&buf[0..28]) {
        return bad("the snapshot record's checksum does not match its body.".into());
    }
    let round = u64::from_le_bytes(buf[4..12].try_into().expect("8 bytes"));
    let term = u64::from_le_bytes(buf[12..20].try_into().expect("8 bytes"));
    if (round, term) != (floor, floor_term) {
        return bad(format!(
            "the snapshot record describes the floor at round {round} term {term} and this node's \
             log begins after round {floor} term {floor_term}, so it is not a record about this \
             log — a crash between the two writes leaves exactly this."
        ));
    }
    Ok((u64::from_le_bytes(buf[20..28].try_into().expect("8 bytes")), None))
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
/// Temp-then-rename with both fsyncs, so a crash leaves either the old record or the new one and
/// never a torn one. Writing in place would be one syscall shorter and would allow exactly the torn
/// record `hard_decode` has to refuse.
///
/// Delegated to [`replace_atomically`] rather than spelled out here. The hand-rolled version fsynced
/// the directory as `File::open(dir)?.sync_all()`, which is `ERROR_ACCESS_DENIED` on Windows — a
/// directory needs `FILE_FLAG_BACKUP_SEMANTICS` there — so every store failed on one of the three CI
/// platforms. `OsFileOps::sync_dir` already carries that platform split, and states in its own doc
/// comment that the Windows arm is a real gap rather than parity.
fn store_hard_state(dir: &Path, h: &HardState) -> Result<(), FerroError> {
    replace_atomically(&OsFileOps, &hard_path(dir), &hard_encode(h))
        .map_err(|e| FerroError::Io(e.to_string()))
}

#[cfg(test)]
#[path = "tests_node.rs"]
mod tests_node;
