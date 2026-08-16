//! Core identity and error types for the branch engine.
//!
//! Design authority: DESIGN.md section 1 ("Branch engine").
//!
//! A branch IS a root pointer. Fork sets `child.root_page_id = parent.root_page_id` and appends
//! `fork_epoch` to the parent's sorted live-children array. Nothing else. In particular there is
//! no parent-chain walk on the read path, no content addressing, no refcounts.

use std::fmt::{Display, Formatter};
use std::time::{SystemTime, UNIX_EPOCH};

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
    /// Milliseconds since the unix epoch, right now.
    pub fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// A deadline `millis` from now.
    pub fn from_now(millis: u64) -> Self {
        LeaseDeadline(Self::now_millis().saturating_add(millis))
    }

    pub fn is_expired_at(&self, now_millis: u64) -> bool {
        now_millis >= self.0
    }

    pub fn is_expired(&self) -> bool {
        self.is_expired_at(Self::now_millis())
    }
}

/// Lifecycle of a branch. `Reaping` is observable: the reaper marks before it frees, so a crash
/// mid-reap resumes rather than leaking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BranchState {
    Live,
    Reaping,
    Reaped,
}

impl BranchState {
    pub fn as_u8(&self) -> u8 {
        match self {
            BranchState::Live => 0,
            BranchState::Reaping => 1,
            BranchState::Reaped => 2,
        }
    }

    pub fn from_u8(v: u8) -> Result<Self, BranchError> {
        match v {
            0 => Ok(BranchState::Live),
            1 => Ok(BranchState::Reaping),
            2 => Ok(BranchState::Reaped),
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
