//! Core identity and error types for the branch engine.
//!
//! Design authority: DESIGN.md section 1 ("Branch engine").
//!
//! A branch IS a root pointer. Fork sets `child.root_page_id = parent.root_page_id` and appends
//! `fork_epoch` to the parent's sorted live-children array. Nothing else. In particular there is
//! no parent-chain walk on the read path, no content addressing, no refcounts.

use std::fmt::{Display, Formatter};

use crate::error::FerroError;

/// A page identifier, matching the width used everywhere else in ferrodb.
pub type PageId = u32;

/// Identity of a branch.
///
/// `generation` exists so a reaped id can never be mistaken for a live one: the id slot may be
/// recycled, but the generation is bumped on every reap, so a stale handle presenting an old
/// generation is a hard error (`BranchError::Reaped`) rather than a silent read of somebody
/// else's data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchId {
    pub id: u64,
    pub generation: u32,
}

impl BranchId {
    /// The trunk. Always generation 0 and never reaped.
    pub const TRUNK: BranchId = BranchId { id: 0, generation: 0 };

    pub const fn new(id: u64, generation: u32) -> Self {
        BranchId { id, generation }
    }

    pub const fn is_trunk(&self) -> bool {
        self.id == 0
    }

    /// The same id slot at the next generation. Produced when a branch is reaped.
    pub const fn bump(&self) -> Self {
        BranchId { id: self.id, generation: self.generation + 1 }
    }
}

impl Display for BranchId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "b{}@g{}", self.id, self.generation)
    }
}

/// A monotonic global counter stamped into every page at birth and into every branch at fork.
///
/// The whole GC algebra is expressed in epochs: page `p` is reclaimable iff no live child has
/// `fork_epoch` in `[birth(p), free(p))`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Epoch(pub u64);

impl Epoch {
    pub const ZERO: Epoch = Epoch(0);

    pub const fn next(&self) -> Epoch {
        Epoch(self.0 + 1)
    }

    pub const fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Epoch {
    fn from(v: u64) -> Self {
        Epoch(v)
    }
}

impl Display for Epoch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "e{}", self.0)
    }
}

/// Identifies one private extent (~1MB of contiguous pages) owned by a writing branch.
///
/// Two payoffs from one mechanism: shadow pages stay physically clustered, and reaping a
/// childless branch is an extent-level free rather than a per-page sharing analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ArenaId(pub u32);

impl ArenaId {
    /// Arena 0 is the shared/trunk arena; branch-private arenas start at 1.
    pub const SHARED: ArenaId = ArenaId(0);

    pub const fn get(&self) -> u32 {
        self.0
    }
}

impl Display for ArenaId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "a{}", self.0)
    }
}

/// Identifies a committed state of a branch. `TxnFrame::base` names the state the frame was
/// written against, which is what makes three-way merge against the fork point possible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitHash(pub [u8; 32]);

impl CommitHash {
    pub const ZERO: CommitHash = CommitHash([0u8; 32]);

    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in self.0.iter() {
            s.push_str(&format!("{:02x}", b));
        }
        s
    }
}

impl Display for CommitHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.to_hex()[..16])
    }
}

/// Wall-clock deadline, unix epoch milliseconds.
///
/// Leases are the answer to the abandoned-agent problem (DESIGN.md exit criterion 8): a
/// background scan hard-reaps anything past deadline **without the client ever calling close**.
/// Every branch carries one; there is no exemption class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LeaseDeadline(pub u64);

impl LeaseDeadline {
    /// Milliseconds since the unix epoch **as this cluster reckons them**.
    ///
    /// # F4: this used to be `SystemTime::now()`, and that is a cluster bug
    ///
    /// Wall clocks disagree. Two nodes reading their own clocks disagree about whether a branch is
    /// live, and disagreeing about that is not a stale read — reaping frees the branch's arenas
    /// and then bumps its `BranchId` generation, which exists precisely so a reaped id can never
    /// be mistaken for a live one, and which therefore makes a wrongly-reaped branch
    /// **unrecoverable**. Exit criterion 9 is that two nodes never disagree here.
    ///
    /// So the reading comes from [`crate::cluster::lease_now_millis`]:
    ///
    /// * standalone — the local wall clock, unchanged, because a node with no cluster configured
    ///   *is* its own leader and its clock is this one-node cluster's time;
    /// * a cluster member — the last applied [`crate::consensus::Command::LeaseTick`], which every
    ///   member of the cluster applies at the same round and therefore agrees on.
    ///
    /// # Why it panics rather than returning a wrong number
    ///
    /// A cluster member that has applied no tick does not know the time, and this signature cannot
    /// say so. Every value it could return is worse than aborting: a local reading is exactly the
    /// divergence being removed; a value in the past reaps live branches, destructively and
    /// unrecoverably; a value in the future silently stops reaping and defeats exit criterion 8
    /// with no symptom. The house already answers this shape the same way — see the
    /// `unreachable!` in `consensus::Body::stale_refusal`, chosen so that a missing case "fails
    /// loudly here instead of sending a wrong answer".
    ///
    /// **It is unreachable on a single node**, where [`crate::cluster::lease_now_millis`] never
    /// fails, which is why the existing suite is untouched. Cluster callers use
    /// [`LeaseDeadline::try_now_millis`], which returns the refusal instead.
    #[track_caller]
    pub fn now_millis() -> u64 {
        Self::try_now_millis().unwrap_or_else(|e| {
            panic!(
                "lease time was read on a node that does not know the cluster's time: {e}. Use \
                 LeaseDeadline::try_now_millis to handle this rather than abort."
            )
        })
    }

    /// [`LeaseDeadline::now_millis`], refusing instead of aborting. **The cluster-facing path.**
    pub fn try_now_millis() -> Result<u64, crate::cluster::GrantError> {
        crate::cluster::lease_now_millis()
    }

    /// A deadline `millis` from the cluster's now.
    ///
    /// Deterministic across nodes: each node computes `tick + millis` from the same applied tick,
    /// so a fork replicated as `BranchOp::Fork { lease_millis, .. }` produces the *same* deadline
    /// everywhere it is applied — which is what makes the durable `lease_deadline` in the branch
    /// record agree between nodes without shipping it.
    ///
    /// Panics on a cluster member with no applied tick; see [`LeaseDeadline::now_millis`]. Use
    /// [`LeaseDeadline::try_from_now`] on a cluster path.
    #[track_caller]
    pub fn from_now(millis: u64) -> Self {
        LeaseDeadline(Self::now_millis().saturating_add(millis))
    }

    /// [`LeaseDeadline::from_now`], refusing instead of aborting. **The cluster-facing path.**
    pub fn try_from_now(millis: u64) -> Result<Self, crate::cluster::GrantError> {
        Ok(LeaseDeadline(Self::try_now_millis()?.saturating_add(millis)))
    }

    /// Whether this deadline has passed at `now_millis`.
    ///
    /// Deliberately still a **pure comparison** with the clock supplied by the caller, and it is
    /// what `reaper::reap_expired` uses. Keeping it pure is the reason two nodes cannot disagree:
    /// the only thing left that could differ is the `now` they were given, and that now comes from
    /// the replicated tick.
    pub fn is_expired_at(&self, now_millis: u64) -> bool {
        now_millis >= self.0
    }

    /// Whether this deadline has passed **as the cluster reckons time**.
    ///
    /// Fallible, and returns a refusal rather than `false`, because this is the destructive
    /// direction: `false` would read as "still live" and silently stop reaping.
    ///
    /// This replaces an infallible `is_expired(&self)` that read `SystemTime::now()` itself. That
    /// method had no callers anywhere in the repository, and it was the one remaining way to make
    /// a reap decision from a node-local clock — removed rather than guarded, because a guard on a
    /// method nobody calls is a guard somebody deletes.
    pub fn is_expired_now(&self) -> Result<bool, crate::cluster::GrantError> {
        Ok(self.is_expired_at(Self::try_now_millis()?))
    }
}

/// Lifecycle of a branch. `Reaping` is observable: the reaper marks before it frees, so a crash
/// mid-reap resumes rather than leaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BranchState {
    Live,
    /// Held after a verification gate declined to merge it. **Unmerged but still queryable** —
    /// that pairing is the whole point: a branch that failed a heuristic check has not been shown
    /// to be wrong, so discarding it destroys the evidence needed to decide whether it was.
    Quarantined,
    Reaping,
    Reaped,
}

impl BranchState {
    pub fn as_u8(&self) -> u8 {
        match self {
            BranchState::Live => 0,
            BranchState::Reaping => 1,
            BranchState::Reaped => 2,
            // Appended, so every tag already on disk keeps its meaning.
            BranchState::Quarantined => 3,
        }
    }

    pub fn from_u8(v: u8) -> Result<Self, BranchError> {
        match v {
            0 => Ok(BranchState::Live),
            1 => Ok(BranchState::Reaping),
            2 => Ok(BranchState::Reaped),
            3 => Ok(BranchState::Quarantined),
            other => Err(BranchError::Corrupt(format!("unknown branch state {}", other))),
        }
    }
}

/// Maximum ancestry depth before a branch is collapsed (materialised to a fresh root and
/// re-parented to trunk). Cheap because ancestry lives only in branch metadata.
pub const MAX_BRANCH_DEPTH: u8 = 8;

/// Default arena extent size in pages (~1MB at 4KB pages).
pub const ARENA_EXTENT_PAGES: u32 = 256;

/// Errors specific to the branch engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchError {
    /// The branch does not exist at all.
    NotFound(BranchId),
    /// The id exists but at a newer generation: the handle refers to a reaped branch. This is a
    /// hard error by design, never stale data.
    Reaped { requested: BranchId, current_generation: u32 },
    /// The branch is mid-reap and cannot accept reads or writes.
    Reaping(BranchId),
    /// The lease expired; the branch is eligible for non-cooperative reaping.
    LeaseExpired { branch: BranchId, deadline: LeaseDeadline, now_millis: u64 },
    /// Forking here would exceed `MAX_BRANCH_DEPTH`; collapse first.
    DepthExceeded { branch: BranchId, depth: u8 },
    /// A write was attempted against a read-only or already-merged branch.
    NotWritable(BranchId),
    /// On-disk branch metadata failed to parse or failed its checksum.
    Corrupt(String),
    /// Arena bookkeeping failure (exhausted, double free, wrong owner).
    Arena(String),
}

impl Display for BranchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BranchError::NotFound(b) => write!(f, "branch {} not found", b),
            BranchError::Reaped { requested, current_generation } => write!(
                f,
                "branch {} has been reaped (id slot is now at generation {})",
                requested, current_generation
            ),
            BranchError::Reaping(b) => write!(f, "branch {} is being reaped", b),
            BranchError::LeaseExpired { branch, deadline, now_millis } => write!(
                f,
                "lease on branch {} expired at {} (now {})",
                branch, deadline.0, now_millis
            ),
            BranchError::DepthExceeded { branch, depth } => write!(
                f,
                "branch {} is at ancestry depth {}, max is {}",
                branch, depth, MAX_BRANCH_DEPTH
            ),
            BranchError::NotWritable(b) => write!(f, "branch {} is not writable", b),
            BranchError::Corrupt(s) => write!(f, "corrupt branch metadata: {}", s),
            BranchError::Arena(s) => write!(f, "arena error: {}", s),
        }
    }
}

impl std::error::Error for BranchError {}

impl From<BranchError> for FerroError {
    fn from(e: BranchError) -> Self {
        FerroError::Branch(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_bump_makes_reaped_id_distinguishable() {
        let b = BranchId::new(7, 0);
        assert_ne!(b, b.bump());
        assert_eq!(b.bump().id, b.id);
    }

    #[test]
    fn lease_expiry_is_a_pure_comparison() {
        let d = LeaseDeadline(1000);
        assert!(!d.is_expired_at(999));
        assert!(d.is_expired_at(1000));
        assert!(d.is_expired_at(1001));
    }

    #[test]
    fn branch_state_roundtrips() {
        for s in [BranchState::Live, BranchState::Reaping, BranchState::Reaped] {
            assert_eq!(BranchState::from_u8(s.as_u8()).unwrap(), s);
        }
        assert!(BranchState::from_u8(9).is_err());
    }
}
