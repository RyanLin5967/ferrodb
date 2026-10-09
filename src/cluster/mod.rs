//! Node-local counters that must become **cluster** state, and the guards that refuse when they
//! are not.
//!
//! Design authority: `DISTRIBUTED.md` §F4. Three counters in this database are `fetch_add` on a
//! node-local atomic, and every one of them is a silent corruption the moment a second node runs:
//!
//! | counter | what two nodes do to it | why nothing downstream notices |
//! |---|---|---|
//! | `branch/arena.rs` `next_extent_start` | both allocate the extent at page 66 | `examples/repl_primary.rs`: *"every such page still passes its checksum, so refusing here is the only detection point."* |
//! | `branch/arena.rs` `next_arena_id` | both name a different extent `a7` | `BranchRecord::arenas` then points two branches at one arena, and the reaper frees exactly `record.arenas` |
//! | `wal/txn.rs` `next_txn_id` | both issue txn 5 | the TEL's `stamp()` leads with `TxnId` to order writes across branches (ledger R8), so the duplicate corrupts *merge ordering*, which the merge engine cannot detect |
//! | `branch/types.rs` `SystemTime::now()` | two clocks disagree about a lease | reaping is destructive **and unrecoverable**: `BranchId` carries a generation precisely so a reaped id can never be mistaken for live |
//!
//! # The shape of the fix: one consume path, two authorities
//!
//! There is exactly **one** way to obtain a value from any of these counters — [`Grants::take`],
//! which consumes from a range this node was granted. There is no second path and no fallback,
//! because a fallback *is* the bug: a node that cannot prove it owns page 66 and allocates it
//! anyway has produced a page that passes its own checksum on two machines.
//!
//! What differs between a single node and a cluster is not how a value is consumed but **who may
//! create a range**:
//!
//! * [`Authority::Standalone`] — no cluster is configured, so this node *is* the leader, and it
//!   grants itself a chunk whenever it runs dry. Single-node ferrodb therefore drives the whole
//!   grant machinery on every allocation, which is what makes the 1349 existing tests evidence
//!   that the machinery works rather than evidence that it is bypassed.
//! * [`Authority::Member`] — this node is one of several. Ranges arrive only as applied
//!   [`crate::consensus::Command::ArenaGrant`] / [`crate::consensus::Command::TxnIdRange`]
//!   entries, and running dry is a **refusal**, not a self-grant.
//!
//! # Why the authority is process-scoped and not a constructor argument
//!
//! A process is a node. Threading a node identity into `ArenaPageStore::new`, `TxnManager::new`
//! and — the one that decides it — `LeaseDeadline::from_now`, an associated function with no
//! `self` reached from five subsystems, would put the same question in five places and let four of
//! them answer it differently. One of those four forgetting to join is the aliasing bug arriving
//! through a constructor nobody thought was load-bearing.
//!
//! So [`join`] flips the whole process at once, and every counter reads [`authority`] at the
//! moment it issues. The precedent for process-scoped configuration is already here:
//! `wal/txn.rs`'s `FERRODB_CHECKPOINT_INTERVAL` `OnceLock`.
//!
//! # The epoch, which is the guard that is easy to miss
//!
//! A store that self-granted pages `[256, 512)` while standalone, in a process that *then* joins a
//! cluster, is holding space no leader knows it has — and the leader will hand it to somebody
//! else. So every held range records the [`AuthorityEpoch`] it was granted under, [`join`] and
//! [`leave`] bump that epoch, and [`Grants::take`] discards any range from a stale one. A node
//! that changes authority forgets everything it was holding, which is the only safe answer.
//!
//! # What this module does NOT do
//!
//! It does not decide *when* to ask for a grant, and it does not send anything. Consensus returns
//! [`crate::consensus::Action`]s and the surrounding server applies them; the appliers here
//! ([`Grants::apply_grant`], [`apply_lease_tick`]) are the receiving end of
//! [`crate::consensus::Action::Apply`] and nothing more. Deciding that a node is running low and
//! proposing the next `ArenaGrant` belongs to whoever owns the leader loop.

use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

use crate::consensus::NodeId;
use crate::error::FerroError;

/// Who may create a range for a counter.
///
/// Not a boolean, because the interesting case carries an identity: a grant names the node it is
/// for, every node applies every entry in the log, and **only the addressed node may take it**.
/// A `bool` cannot express the refusal that matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Authority {
    /// No cluster configured. This node is its own leader and grants itself everything.
    Standalone,
    /// A member of a cluster, identified. Only grants addressed to this id are usable.
    Member(NodeId),
}

/// A generation counter over [`Authority`] changes.
///
/// Held ranges are stamped with it so that a change of authority invalidates space granted under
/// the previous one. See the module header.
pub type AuthorityEpoch = u64;

/// Why a counter refused to issue a value.
///
/// Every variant is a refusal. There is deliberately no variant meaning "carried on anyway".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantError {
    /// **The refusal this module exists for.** This node is a cluster member and holds no granted
    /// range large enough to answer, so it will not allocate.
    ///
    /// The caller's remedy is to obtain a grant from the leader, never to fall back to a local
    /// counter.
    Exhausted { counter: &'static str, node: NodeId, need: u64 },

    /// A grant addressed to a different node arrived here.
    ///
    /// Every node applies every committed entry, so this is the ordinary case for the grants of
    /// other nodes and it is refused rather than ignored: a node that takes another node's range
    /// is precisely the two-nodes-one-page failure.
    WrongNode { counter: &'static str, granted_to: NodeId, self_id: NodeId },

    /// A grant arrived at a node with no cluster configured.
    ///
    /// Refused rather than accepted-because-it-looks-harmless: a standalone node's counter is its
    /// own, and accepting an outside range would let it issue values its own watermark does not
    /// cover.
    NotClustered { counter: &'static str },

    /// A grant with `hi <= lo` carries no values. Refused rather than treated as a no-op, because
    /// it means whoever proposed it computed a range wrong.
    EmptyRange { counter: &'static str, lo: u64, hi: u64 },

    /// The counter's own value space is used up: there is nothing left to grant, on any node.
    ///
    /// Distinct from [`GrantError::Exhausted`], which says "ask the leader". Nobody can answer
    /// this one, and reporting it as a missing grant would send an operator looking for a leader
    /// problem that is not there.
    SpaceExhausted { counter: &'static str, issued_through: u64 },

    /// Lease time was read on a cluster member that has applied no
    /// [`crate::consensus::Command::LeaseTick`].
    ///
    /// There is no safe answer: a local clock reading is the divergence this exists to stop, and a
    /// fabricated one either reaps a live branch (destructive, unrecoverable) or never reaps
    /// (silently defeats exit criterion 8).
    NoClusterTime { node: NodeId },
}

impl std::fmt::Display for GrantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GrantError::Exhausted { counter, node, need } => write!(
                f,
                "{node} holds no leader-granted {counter} range covering {need} value(s): refusing \
                 to allocate. A node that allocates without a grant hands the same value to two \
                 branches, and every page it writes still passes its own checksum"
            ),
            GrantError::WrongNode { counter, granted_to, self_id } => write!(
                f,
                "a {counter} grant addressed to {granted_to} reached {self_id}: refused. Every \
                 node applies every entry; only the addressed node may take one"
            ),
            GrantError::NotClustered { counter } => write!(
                f,
                "a {counter} grant reached a node with no cluster configured: refused. A \
                 standalone node grants itself and accepts no outside range"
            ),
            GrantError::EmptyRange { counter, lo, hi } => {
                write!(f, "a {counter} grant of [{lo}, {hi}) carries no values: refused")
            }
            GrantError::SpaceExhausted { counter, issued_through } => write!(
                f,
                "the {counter} value space is exhausted at {issued_through}: no node can grant \
                 more"
            ),
            GrantError::NoClusterTime { node } => write!(
                f,
                "{node} has applied no LeaseTick, so it does not know the cluster's time and will \
                 not decide a lease. Reaping is destructive and a BranchId generation makes it \
                 unrecoverable, so there is no safe default here"
            ),
        }
    }
}

impl std::error::Error for GrantError {}

impl From<GrantError> for FerroError {
    /// Mapped onto [`FerroError::Branch`], which is the class every caller of these counters
    /// already returns, and **not** onto [`FerroError::NotLeader`].
    ///
    /// That distinction is load-bearing. `NotLeader` tells a client to reconnect elsewhere, and
    /// reconnecting elsewhere does not help here: the *leader itself* refuses when it holds no
    /// grant, and a follower redirecting a client to a leader that is equally ungranted would send
    /// it in a circle. The refusal is "this value cannot be issued yet", not "ask another node".
    fn from(e: GrantError) -> Self {
        FerroError::Branch(e.to_string())
    }
}

// ---- process authority -------------------------------------------------------------------------

struct ProcessState {
    authority: Authority,
    epoch: AuthorityEpoch,
    /// The cluster's opinion of the time, in unix milliseconds, as of the last applied
    /// [`crate::consensus::Command::LeaseTick`]. `None` until one has been applied.
    cluster_millis: Option<u64>,
}

fn process() -> &'static Mutex<ProcessState> {
    static P: OnceLock<Mutex<ProcessState>> = OnceLock::new();
    P.get_or_init(|| {
        Mutex::new(ProcessState { authority: Authority::Standalone, epoch: 0, cluster_millis: None })
    })
}

/// Lock the process state, ignoring poisoning.
///
/// A panic in a test that held this lock must not make every later counter refuse — that would
/// turn one failing test into a cascade whose first cause is invisible. The state behind it is
/// three plain values with no invariant a panic can break halfway.
fn lock() -> MutexGuard<'static, ProcessState> {
    process().lock().unwrap_or_else(PoisonError::into_inner)
}

/// Who may create a range for this process's counters, right now.
pub fn authority() -> Authority {
    lock().authority
}

/// The current authority epoch. Bumped by [`join`] and [`leave`].
pub fn epoch() -> AuthorityEpoch {
    lock().epoch
}

/// Authority and epoch, read together.
///
/// One lock acquisition and not two, because they are a pair: a `take` that read the authority
/// before a `join` and the epoch after it would issue from a range the join was about to
/// invalidate, under the authority that no longer holds. Every caller here uses this.
pub fn authority_at() -> (Authority, AuthorityEpoch) {
    let p = lock();
    (p.authority, p.epoch)
}

/// Whether this process belongs to a cluster.
pub fn is_clustered() -> bool {
    matches!(authority(), Authority::Member(_))
}

/// Join a cluster as `node`.
///
/// Bumps the authority epoch, which invalidates every range any counter self-granted while
/// standalone: that space is not known to the leader, and the leader will hand it to somebody
/// else. Clears the cluster clock for the same reason — a wall-clock reading taken while
/// standalone is not this cluster's time.
pub fn join(node: NodeId) {
    let mut p = lock();
    p.authority = Authority::Member(node);
    p.epoch += 1;
    p.cluster_millis = None;
}

/// Leave the cluster and return to being this node's own leader.
///
/// Bumps the epoch for the mirror-image reason: ranges granted by a leader this node no longer
/// answers to are not this node's to issue from.
pub fn leave() {
    let mut p = lock();
    p.authority = Authority::Standalone;
    p.epoch += 1;
    p.cluster_millis = None;
}

/// Apply a committed [`crate::consensus::Command::LeaseTick`].
///
/// **Monotone.** A tick that would move time backwards is ignored rather than refused, because a
/// re-delivered suffix of the log is normal and expected — `WalBatch` is idempotent for the same
/// reason — and because lease expiry that can move backwards would un-expire a branch a peer has
/// already decided to reap. Returns the resulting cluster time.
///
/// Refuses on a standalone node: it has no leader to be told the time by, and accepting one would
/// make the two clocks disagree in the direction nothing detects.
pub fn apply_lease_tick(unix_millis: u64) -> Result<u64, GrantError> {
    let mut p = lock();
    match p.authority {
        Authority::Member(_) => {}
        Authority::Standalone => {
            return Err(GrantError::NotClustered { counter: "lease-tick" })
        }
    }
    let now = fold_tick(p.cluster_millis, unix_millis);
    p.cluster_millis = Some(now);
    Ok(now)
}

/// Where a lease decision is entitled to read time from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseSource {
    /// This node is its own leader, so its own clock *is* the cluster's time.
    ///
    /// Named for the wall clock it read until F2, and kept under that name because
    /// `cluster::tests` pins the rule by it. What it reads now is [`local_lease_millis`]: the
    /// wall clock ONCE per process, then a monotonic clock.
    LocalWall,
    /// The cluster's time as of the last applied tick.
    Cluster(u64),
}

/// The rule, as a pure function of the two things that decide it.
///
/// Separated from [`lease_now_millis`] so the rule can be tested without arming the process — a
/// `tests/` binary runs its tests as threads of one process, so a unit test that joined a cluster
/// would make every sibling test in the same binary refuse.
fn lease_source(auth: Authority, cluster_millis: Option<u64>) -> Result<LeaseSource, GrantError> {
    match (auth, cluster_millis) {
        (Authority::Standalone, _) => Ok(LeaseSource::LocalWall),
        (Authority::Member(_), Some(ms)) => Ok(LeaseSource::Cluster(ms)),
        (Authority::Member(n), None) => Err(GrantError::NoClusterTime { node: n }),
    }
}

/// Fold an incoming tick into the cluster clock. **Monotone** — see [`apply_lease_tick`].
///
/// The `Some` arm takes the maximum rather than the incoming value, which is the whole content of
/// the monotonicity guarantee: a re-delivered suffix of the log is normal, and letting an older
/// tick win would move lease expiry backwards and un-expire a branch a peer has already reaped.
fn fold_tick(prev: Option<u64>, incoming: u64) -> u64 {
    match prev {
        Some(p) => p.max(incoming),
        None => incoming,
    }
}

/// The time a lease decision must be made against.
///
/// * Standalone — the local wall clock, which is this one-node cluster's time by definition.
/// * A member that has applied a tick — that tick.
/// * A member that has not — [`GrantError::NoClusterTime`]. There is no third answer; see the
///   variant's own documentation.
pub fn lease_now_millis() -> Result<u64, GrantError> {
    let (auth, ms) = {
        let p = lock();
        (p.authority, p.cluster_millis)
    };
    match lease_source(auth, ms)? {
        LeaseSource::LocalWall => Ok(local_lease_millis()),
        LeaseSource::Cluster(ms) => Ok(ms),
    }
}

/// A standalone node's lease clock, in milliseconds on the unix-epoch scale.
///
/// # F2 — monotonic within a process, anchored to the wall clock once
///
/// This returned `SystemTime::now()` on every call, so a forward step of the wall clock — an NTP
/// step, a VM resuming, an operator setting the date — expired every lease whose deadline it
/// crossed, in one scan, with no client having been idle at all. Reaping is destructive and a
/// `BranchId` generation makes it unrecoverable, so a clock that can jump is the wrong input.
///
/// So the wall clock is read **once per process**, at the first lease reading, and every reading
/// after that is that anchor plus [`std::time::Instant`]'s elapsed time. `Instant` is monotonic:
/// within one process the lease clock never moves backwards and no step of the wall clock moves it
/// at all. Deadlines stay on the unix-epoch millisecond scale they have always had, so every
/// durable deadline already written keeps its meaning.
///
/// **Across processes the anchor is taken again**, and whatever the wall clock did between the two
/// — a step, a slew, the machine being off — lands in the gap between one process's last reading
/// and the next one's first. That gap is exactly what F1's restart grace measures as downtime and
/// adds to every live lease (`BranchCatalog::resume_leases`), so a step between processes is
/// credited rather than charged. Together the two rules are Chubby's (§2.8–2.9): the lease timer
/// runs only while the authority does, and deadlines only ever move forward.
///
/// ⚠ **A suspended machine stops this clock.** `Instant` does not advance while the host sleeps
/// (`CLOCK_UPTIME_RAW` on macOS; `CLOCK_MONOTONIC` on Linux does not count suspend either), so a
/// lease does not run down while the process is frozen. That is the stopped timer again — a
/// suspended process is an authority that is down — and it is the direction that keeps a lease
/// rather than reaps one. Anything else that reads the lease clock as a wall-clock timestamp
/// inherits the same lag after a sleep; `AgentRuntime::begin_session_as_staged` stamps a run's
/// `started_at` from it.
///
/// Private on purpose, for the reason [`local_wall_millis`] gives.
fn local_lease_millis() -> u64 {
    static ANCHOR: OnceLock<(u64, std::time::Instant)> = OnceLock::new();
    let (wall, at) = *ANCHOR.get_or_init(|| (local_wall_millis(), std::time::Instant::now()));
    anchored_millis(wall, at.elapsed())
}

/// The lease reading `since_anchor` after a wall-clock anchor of `anchor_wall_millis`.
///
/// Pure, and its signature is the F2 property: the wall clock is not an argument, so no reading of
/// it after the anchor can reach a lease decision. Saturates rather than wrapping — a wrapped
/// reading is a time in 1970, which reaps everything.
fn anchored_millis(anchor_wall_millis: u64, since_anchor: std::time::Duration) -> u64 {
    anchor_wall_millis.saturating_add(u64::try_from(since_anchor.as_millis()).unwrap_or(u64::MAX))
}

/// The local wall clock in unix milliseconds.
///
/// **The only `SystemTime::now()` a lease decision may reach**, and only via
/// [`local_lease_millis`]'s anchor on a standalone node — once per process since F2. Private on
/// purpose: a caller that can name this function can reintroduce the divergence the module exists
/// to remove.
fn local_wall_millis() -> u64 {
    let real = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    #[cfg(test)]
    let real = wall_step::apply(real);
    real
}

/// **A file's modification time, as the OS stamped it — on the WALL clock** — and the only thing in
/// this crate that measures an age against the wall clock (D198 reviews 3 and 4, C1 and C3).
///
/// The FirstStart policy needs the wall-clock age of a catalog's (or a legacy log's) last write. A
/// function from an integer stamp to its age would hand out a wall READING to any caller that
/// passes 0 — the door [`local_wall_millis`]'s privacy closes, reopened by a comment. So the stamp is
/// a type whose one constructor reads a FILE, and **no single call returns a wall reading**.
///
/// **That is all the type buys** (D198 review 5, C3). Any two independent functions of the stamp
/// and the wall clock let the wall clock be solved for: `s.millis() + s.wall_age_millis()` IS the
/// wall reading, for any stamp, and an inline `SystemTime::now()` is always available anyway. So
/// `millis()` has exactly two named consumers (its doc), the arithmetic goes through
/// [`Self::lease_scale_age_millis`] and [`Self::wall_age_millis`], and the mutant that rebuilds a
/// reading from a stamp (M58b) is killed by a TEST, not by this type.
///
/// Why a stamp and not `fn(&Path) -> age`: the evidence is the file's time when the catalog was
/// OPENED, and its age is needed at the resume, after this process may have written the file
/// itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FileWallStamp(u64);

impl FileWallStamp {
    /// `path`'s last modification, in unix milliseconds, or `None` if it cannot be read — including
    /// a time before 1970, which is no evidence of anything.
    pub(crate) fn of(path: &std::path::Path) -> Option<FileWallStamp> {
        let modified = std::fs::metadata(path).ok()?.modified().ok()?;
        let since = modified.duration_since(std::time::UNIX_EPOCH).ok()?;
        u64::try_from(since.as_millis()).ok().map(FileWallStamp)
    }

    /// The stamp, in unix milliseconds on the wall clock. **Exactly two consumers, named here**
    /// (review 5, C3): `TableBranchCatalog::first_start_credit`, which prints it in the startup
    /// report (`LeaseResume::FirstStartFromFileTime::file_mtime`), and the `#[cfg(test)]`
    /// `table_catalog::file_mtime_millis`. Arithmetic uses [`Self::lease_scale_age_millis`] or
    /// [`Self::wall_age_millis`]; a third consumer can rebuild a wall reading with the latter.
    pub(crate) fn millis(self) -> u64 {
        self.0
    }

    /// `lease_now − stamp`, saturating at 0: the file's age on the LEASE scale. **Correct only under
    /// R2** (`table_catalog::FirstStartEvidence`): it reads the stamp as the lease reading of a
    /// writer whose lease clock was the wall clock — every pre-D198 build. For a file last written
    /// by a D198 process whose lease clock lagged, it is short by that lag (review 5, C1b/C1d; a
    /// stated residual, never below the pre-policy 0).
    pub(crate) fn lease_scale_age_millis(self, lease_now: u64) -> u64 {
        lease_now.saturating_sub(self.0)
    }

    /// How long ago, on the wall clock, the file was written: `W(now) − stamp`, saturating at 0 for
    /// a stamp in the future. Through `local_wall_millis`, the one `SystemTime::now()` reader, so a
    /// test's `wall_step` moves it as it moves everything else.
    pub(crate) fn wall_age_millis(self) -> u64 {
        local_wall_millis().saturating_sub(self.0)
    }
}

/// **This node's lease clock if it is standalone, else `None` — one acquisition of the process
/// lock** (D198 review 4, C6). The soft mark's reading: a cluster member's lease time is the
/// replicated tick, and it writes no mark of either kind. One acquisition, not `is_clustered()`
/// then `try_now_millis()`: those are two, on a path that runs inside `logical` on every commit of
/// an unmarked catalog, and between them a `join` plus an applied tick could stamp a tick as a
/// standalone reading.
pub(crate) fn standalone_lease_millis() -> Option<u64> {
    let authority = lock().authority;
    match authority {
        Authority::Standalone => Some(local_lease_millis()),
        Authority::Member(_) => None,
    }
}

/// **A test's hand on the wall clock — THIS THREAD's view of it only, and only in `cfg(test)`.**
///
/// F2's claim is that no step of the wall clock inside a process moves a lease decision, and a
/// claim about a clock step cannot be tested without a clock that steps. The real one cannot be
/// stepped from a test, so this adds a signed offset to what [`local_wall_millis`] — the process's
/// one `SystemTime::now()` reader — returns on the calling thread. With F2 in place the step is
/// invisible to every lease decision, because the lease clock read the wall once, at its anchor;
/// with F2 reverted (the decision site reading `local_wall_millis` directly, as it did at `9aa6968`)
/// the step lands in the decision, and `cluster::tests::f2_a_wall_clock_step_…` fails.
///
/// Thread-local so that stepping cannot touch a sibling test in the same binary, even under that
/// mutant. Stated blind spot: a mutant that calls `SystemTime::now()` inline at the decision site,
/// bypassing `local_wall_millis`, is not steppable by this and would pass the test; the one-reader
/// rule in `local_wall_millis`'s doc is what stands against that.
#[cfg(test)]
pub(crate) mod wall_step {
    use std::cell::Cell;

    thread_local! {
        static STEP_MILLIS: Cell<i64> = const { Cell::new(0) };
    }

    /// Step this thread's wall clock by `millis` (negative is backwards) until the guard drops.
    pub(crate) fn by(millis: i64) -> Guard {
        STEP_MILLIS.with(|c| c.set(millis));
        Guard
    }

    pub(super) fn apply(real: u64) -> u64 {
        real.saturating_add_signed(STEP_MILLIS.with(|c| c.get()))
    }

    /// Restores the real clock, including when the test that stepped it panics.
    pub(crate) struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            STEP_MILLIS.with(|c| c.set(0));
        }
    }
}

// ---- the granted counter -----------------------------------------------------------------------

/// A half-open range of values this node may issue.
///
/// Carries no epoch: the epoch lives once on [`Grants`], because every range in `held` was granted
/// under the same authority — a change of authority clears the whole vector. Stamping each range
/// separately was two places to get one fact right.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Held {
    lo: u64,
    hi: u64,
}

/// What [`Grants::apply_grant`] did with an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    /// The grant was new; `usable` values were added to what this node may issue. Fewer than
    /// `hi - lo` when part of the range had already been issued before a crash.
    Accepted { usable: u64 },
    /// The grant had already been applied in full. A committed round may be re-delivered, so this
    /// is an ordinary outcome and not an error — but it must be a **no-op**, because re-adding a
    /// range this node has already issued from hands the same value out twice.
    Duplicate,
}

/// One leader-granted counter.
///
/// Wrapped in a `Mutex` by [`GrantedCounter`]; this is the bare state so the rules can be read in
/// one place.
#[derive(Debug)]
struct Grants {
    /// Names the counter in every refusal, so a log line says which one ran dry.
    counter: &'static str,
    /// Ranges this node may still issue from, sorted by `lo` and disjoint.
    held: Vec<Held>,
    /// The highest `hi` of any grant ever accepted. What makes re-delivery idempotent.
    accepted_through: u64,
    /// The watermark this node has issued through. **This is the value the old node-local counter
    /// held**, and it is what has to be durable: a restart that forgets it re-issues.
    issued: u64,
    /// How much a standalone node takes for itself when it runs dry.
    chunk: u64,
    /// The authority epoch every range in `held` was granted under, and that `accepted_through`
    /// describes. See [`Grants::observe_epoch`].
    epoch: AuthorityEpoch,
}

impl Grants {
    fn new(counter: &'static str, start: u64, chunk: u64) -> Self {
        assert!(chunk > 0, "a self-grant chunk of 0 would never satisfy a take");
        Grants { counter, held: Vec::new(), accepted_through: start, issued: start, chunk, epoch: 0 }
    }

    /// Notice an authority change, and forget everything that belonged to the old one.
    ///
    /// Two things are dropped, for two different reasons.
    ///
    /// **`held`**, because a range granted under a superseded authority is not this node's to issue
    /// from: a store that self-granted pages `[256, 512)` while standalone, in a process that then
    /// joins a cluster, is sitting on space no leader knows it has and will hand to somebody else.
    ///
    /// **`accepted_through` down to `issued`**, which is the subtler half and was a defect for a
    /// while. That field is the highest `hi` ever accepted, and it does two jobs — recognising a
    /// re-delivered grant, and clamping a grant that reaches below what this node already issued.
    /// Both are statements *about the authority that issued those ranges*. Carried across a
    /// `leave()`/`join()` it keeps clamping the NEW leader's grants against the old membership's
    /// high-water, so the node refuses a range it legitimately holds and then never allocates
    /// again. Safety was never at risk — it only ever refuses — but a guard that deadlocks
    /// liveness is still a guard that is wrong.
    ///
    /// `issued` is the floor and is never lowered: safety here is "never issue a value twice", and
    /// `issued` is the record of which values those are.
    fn observe_epoch(&mut self, now_epoch: AuthorityEpoch) {
        if self.epoch != now_epoch {
            self.epoch = now_epoch;
            self.held.clear();
            self.accepted_through = self.issued;
        }
    }

    /// Take `n` consecutive values, returning the first.
    ///
    /// Scans for the first held range wide enough. A range too narrow is **kept, not split and not
    /// dropped**: `n` is 1 for ids and a whole extent for pages, so a narrow range can still answer
    /// a later id take, and dropping it would leak space no leader will grant again.
    fn take(&mut self, n: u64, auth: Authority, now_epoch: AuthorityEpoch) -> Result<u64, GrantError> {
        self.observe_epoch(now_epoch);
        if let Some(v) = self.take_from_held(n) {
            return Ok(v);
        }
        match auth {
            // Its own leader: grant itself and go round exactly once more, through the same
            // consume path. One path, so the clustered case is never a special case of the
            // single-node one.
            Authority::Standalone => {
                let lo = self.accepted_through.max(self.issued);
                let hi = lo.saturating_add(self.chunk.max(n));
                self.push_range(lo, hi);
                self.take_from_held(n).ok_or(GrantError::SpaceExhausted {
                    counter: self.counter,
                    issued_through: self.issued,
                })
            }
            // **The refusal.** No fallback: see the type's documentation.
            Authority::Member(node) => {
                Err(GrantError::Exhausted { counter: self.counter, node, need: n })
            }
        }
    }

    /// How many values are held and not yet issued **under `now_epoch`**.
    ///
    /// Takes the epoch and applies it rather than reading `held` raw. Without that this reports
    /// space [`Grants::take`] would refuse — a diagnostic that disagrees with the guard, which is
    /// the worst kind: a leader loop reading it would see a node as well supplied and never grant
    /// it anything, and the node would refuse every allocation for ever. Caught by
    /// `space_a_node_self_granted_while_standalone_is_revoked_when_it_joins`.
    fn remaining_values(&mut self, now_epoch: AuthorityEpoch) -> u64 {
        self.observe_epoch(now_epoch);
        self.held.iter().map(|h| h.hi - h.lo).sum()
    }

    fn take_from_held(&mut self, n: u64) -> Option<u64> {
        let idx = self.held.iter().position(|h| h.hi - h.lo >= n)?;
        let h = &mut self.held[idx];
        let v = h.lo;
        h.lo += n;
        if h.lo == h.hi {
            self.held.remove(idx);
        }
        self.issued = self.issued.max(v.saturating_add(n));
        Some(v)
    }

    fn push_range(&mut self, lo: u64, hi: u64) {
        if hi > lo {
            self.held.push(Held { lo, hi });
            self.held.sort_unstable_by_key(|h| h.lo);
        }
        self.accepted_through = self.accepted_through.max(hi);
    }

    /// Apply a committed grant addressed to `to`.
    fn apply_grant(
        &mut self,
        to: NodeId,
        lo: u64,
        hi: u64,
        auth: Authority,
        now_epoch: AuthorityEpoch,
    ) -> Result<Applied, GrantError> {
        match auth {
            Authority::Member(me) if me == to => {}
            Authority::Member(me) => {
                return Err(GrantError::WrongNode {
                    counter: self.counter,
                    granted_to: to,
                    self_id: me,
                })
            }
            Authority::Standalone => {
                return Err(GrantError::NotClustered { counter: self.counter })
            }
        }
        if hi <= lo {
            return Err(GrantError::EmptyRange { counter: self.counter, lo, hi });
        }
        self.observe_epoch(now_epoch);

        // Already applied in full. A committed round may be re-delivered, and re-adding a range
        // already issued from is how one page reaches two branches on a single node.
        if hi <= self.accepted_through {
            return Ok(Applied::Duplicate);
        }

        // Clamped, not refused, when the grant reaches back below what this node has already
        // issued. That is the ordinary restart: the durable watermark says pages up to `issued`
        // are spoken for, the log still holds the grant that covered them, and the usable part is
        // the suffix. Refusing here would refuse every recovery of a partly-consumed grant.
        let lo = lo.max(self.accepted_through).max(self.issued);
        let usable = hi.saturating_sub(lo);
        self.push_range(lo, hi);
        Ok(Applied::Accepted { usable })
    }
}

/// A leader-granted counter, shareable.
///
/// The lock is a leaf: nothing inside it calls out, so it may be taken while holding a caller's
/// own lock (`TxnManager` takes it inside the active-transaction table's).
#[derive(Debug)]
pub struct GrantedCounter(Mutex<Grants>);

impl GrantedCounter {
    /// A counter that has issued everything below `start`, self-granting `chunk` at a time while
    /// standalone.
    pub fn new(counter: &'static str, start: u64, chunk: u64) -> Self {
        GrantedCounter(Mutex::new(Grants::new(counter, start, chunk)))
    }

    fn inner(&self) -> MutexGuard<'_, Grants> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Take `n` consecutive values and return the first, or refuse.
    pub fn take(&self, n: u64) -> Result<u64, GrantError> {
        let (auth, ep) = authority_at();
        self.inner().take(n, auth, ep)
    }

    /// Apply a committed grant addressed to `to`, covering `[lo, hi)`.
    pub fn apply_grant(&self, to: NodeId, lo: u64, hi: u64) -> Result<Applied, GrantError> {
        let (auth, ep) = authority_at();
        self.inner().apply_grant(to, lo, hi, auth, ep)
    }

    /// The watermark this counter has issued through — **the value that must be checkpointed.**
    pub fn issued_through(&self) -> u64 {
        self.inner().issued
    }

    /// Raise the watermark to `issued`, from a durable image or from a log replay.
    ///
    /// **Monotone, and it never lowers.** Two independent recoveries feed this: the arena's
    /// checkpoint image, and `wal/recovery.rs`, which scans every retained record and raises the
    /// counter one past the highest id it saw. Both are statements of the form "at least this much
    /// was already issued", and taking the maximum is the only way to combine two of those without
    /// one of them un-issuing what the other proved. Lowering it would re-issue.
    ///
    /// Held ranges are trimmed to match: a range wholly below the new watermark is dropped, and
    /// one that straddles it keeps only its suffix. Anything else would hand out a value the
    /// watermark says is already spoken for.
    pub fn raise_issued_through(&self, issued: u64) {
        let mut g = self.inner();
        g.issued = g.issued.max(issued);
        g.accepted_through = g.accepted_through.max(g.issued);
        let w = g.issued;
        g.held.retain_mut(|h| {
            h.lo = h.lo.max(w);
            h.lo < h.hi
        });
    }

    /// How many values this node may still issue without a new grant. Diagnostic only: a caller
    /// that branches on it is re-implementing the guard.
    pub fn remaining(&self) -> u64 {
        let (_, ep) = authority_at();
        self.inner().remaining_values(ep)
    }

    /// The authority epoch this counter last acted under. Diagnostic, and how a test observes that
    /// an authority change was noticed at all rather than merely not mattering yet.
    pub fn observed_epoch(&self) -> AuthorityEpoch {
        self.inner().epoch
    }
}

// ---- test scoping ------------------------------------------------------------------------------

/// Arms the process for a cluster and restores what was there on drop.
///
/// The authority is process-scoped, and a `tests/*.rs` binary runs its tests as threads of one
/// process, so a test that joined a cluster would otherwise make every sibling test in the file
/// refuse. Every scope serializes against every other, which is what makes those tests
/// independent rather than merely usually-passing.
///
/// Not `#[cfg(test)]`: integration tests in `tests/` link the crate as an ordinary dependency and
/// cannot see a `cfg(test)` item.
pub struct ClusterScope {
    _serialize: MutexGuard<'static, ()>,
}

fn scope_lock() -> &'static Mutex<()> {
    static L: OnceLock<Mutex<()>> = OnceLock::new();
    L.get_or_init(|| Mutex::new(()))
}

impl ClusterScope {
    /// Join as `node` for the life of the returned guard.
    pub fn joined(node: NodeId) -> Self {
        let g = scope_lock().lock().unwrap_or_else(PoisonError::into_inner);
        join(node);
        ClusterScope { _serialize: g }
    }

    /// Hold the process standalone for the life of the guard, excluding any concurrent
    /// [`ClusterScope::joined`].
    pub fn standalone() -> Self {
        let g = scope_lock().lock().unwrap_or_else(PoisonError::into_inner);
        leave();
        ClusterScope { _serialize: g }
    }
}

impl Drop for ClusterScope {
    fn drop(&mut self) {
        leave();
    }
}

#[cfg(test)]
mod tests;

impl Grants {
    #[allow(dead_code)]
    fn coalesce_for_mutant(&mut self) {
        let mut merged: Vec<Held> = Vec::new();
        for h in self.held.iter().copied() {
            match merged.last_mut() {
                Some(p) if p.hi == h.lo => p.hi = h.hi,
                _ => merged.push(h),
            }
        }
        self.held = merged;
    }
}
