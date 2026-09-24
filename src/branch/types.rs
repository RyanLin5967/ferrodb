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

/// Lease deadline, in milliseconds on the unix-epoch scale **as the lease clock reckons them**.
///
/// Leases are the answer to the abandoned-agent problem (DESIGN.md exit criterion 8): a
/// background scan hard-reaps anything past deadline **without the client ever calling close**.
/// Every branch carries one; there is no exemption class.
///
/// The scale is the wall clock's, and on a standalone node the clock is anchored to it — but since
/// F2 it is not a wall-clock reading: it advances monotonically inside a process and is stopped
/// while no process is running (see `cluster::local_lease_millis` and [`LeaseResume`]).
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
        LeaseDeadline(Self::saturating_deadline(Self::now_millis(), millis))
    }

    /// [`LeaseDeadline::from_now`], refusing instead of aborting. **The cluster-facing path.**
    pub fn try_from_now(millis: u64) -> Result<Self, crate::cluster::GrantError> {
        Ok(LeaseDeadline(Self::saturating_deadline(Self::try_now_millis()?, millis)))
    }

    /// `base + millis`, saturating at **one below** `u64::MAX` — D206, ported from `31364b3`
    /// (branch `F4-clusterstate`, 2026-08-28), which was written and never merged.
    ///
    /// The last value is reserved: `branch/catalog.rs`'s `TRUNK_LEASE` is `LeaseDeadline(u64::MAX)`
    /// and means *never expires*. A plain `saturating_add` therefore turns an over-long lease — a
    /// huge `lease_millis`, or a cluster tick far in the future — into a branch that is
    /// indistinguishable from trunk and can never be reaped, which defeats exit criterion 8 with no
    /// symptom at all: nothing errors, the branch simply stays for ever.
    ///
    /// Saturating one lower keeps the sentinel unforgeable while costing a millisecond nobody can
    /// observe — `u64::MAX - 1` ms after the epoch is roughly 584 million years.
    ///
    /// **Every deadline computation goes through this**, not only the two above: the virtual lease
    /// clock's `v + D` and its `D += downtime` (`TableBranchCatalog::to_lease_clock`,
    /// `resume_leases`) are computations too, and a restart must not be what forges the sentinel.
    /// `pub(crate)` for that reason; `31364b3` had it private because `from_now` was its only
    /// caller.
    ///
    /// # The rule: arithmetic never produces the sentinel; a caller may still state it
    ///
    /// **Decided by the lead, 2026-09-24 (D198/D206 review of `a34b92c`).** What this clamps is
    /// ARITHMETIC — a sum that would saturate onto `u64::MAX`. A caller that passes
    /// `LeaseDeadline(u64::MAX)` explicitly (`TRUNK_LEASE`, and the benches and tests that fork with
    /// it to mean "never reap this") is stating "never" on purpose, and that value is kept exactly:
    /// it is a fixed point of the table catalog's virtual-clock translations
    /// (`table_catalog::stored`, `StoredDeadline::outward` / `inward`) and is not clamped here or
    /// anywhere. Clamping explicit values would change what existing callers store, for no
    /// correctness gain: the defect was forging, and forging is what this closes.
    ///
    /// **Blind spot, stated rather than solved:** a record ALREADY holding a forged `u64::MAX` —
    /// written by a build before this fix with an over-long lease or an absurd tick — cannot be told
    /// from an explicit "never", and stays un-reapable exactly as it is today. Nothing rewrites it.
    pub(crate) fn saturating_deadline(base: u64, millis: u64) -> u64 {
        base.saturating_add(millis).min(u64::MAX - 1)
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

/// What resuming the lease clock at startup did — F1's restart grace.
///
/// # The rule, and whose it is
///
/// Chubby §2.9: *"The authoritative timer for session leases runs at the master, so until a new
/// master is elected the session lease timer is stopped; this is legal because it is equivalent to
/// extending the client's lease."* And §2.8: the master *"is free to advance this timeout further
/// into the future, but may not move it backwards in time."*
///
/// Before this existed, a database restarted after an outage longer than a branch's remaining
/// lease reaped that branch on its first scan: the outage was charged to every lease, the agent
/// never had a chance to act, and nothing could tell "abandoned before the outage" from "expired
/// because of it". Now each catalog that can keeps a durable **last-alive mark** — the lease clock
/// reading at which leases were last being enforced — and at startup extends every lease by the
/// measured downtime.
///
/// **D198: the extension is O(1), not a rewrite.** The catalog keeps a durable cumulative
/// downtime offset `D`, stores every deadline in virtual time `v = lease − D`, and reads it back as
/// `v + D`; a restart does `D += downtime` in one write, and every lease is extended at once
/// (`TableBranchCatalog::to_lease_clock`). A first version rewrote each live deadline instead —
/// O(live branches) writes per restart, a wall at 10⁶ branches. A lease that had already run out
/// before the mark reads `downtime` later too and is still expired: `v + D <= mark` gives
/// `v + D + downtime <= now`.
///
/// Every variant is an outcome to REPORT. Two of them mean the grace was not applied, and both
/// say why rather than looking like a restart that happened to extend nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseResume {
    /// This catalog keeps no last-alive mark, so downtime cannot be measured and no lease was
    /// extended — a lease that lapsed while the process was down is reaped on the first scan, as
    /// before F1. The trait's default; `TableBranchCatalog`, which both binaries open, overrides it.
    NoMark,
    /// This process is a cluster member. Its lease time is the replicated `LeaseTick`, identical on
    /// every member; extending deadlines on one node's restart would make the nodes disagree about
    /// whether a branch is live, which exit criterion 9 forbids. Stopping the timer during a
    /// cluster outage is the tick's proposer's to do. Nothing was extended.
    Clustered,
    /// No mark had been recorded yet — a new database, or the first start of a build with this
    /// grace. The downtime before this start cannot be measured, so nothing was extended; the mark
    /// is now `now_millis`, and every later start is measured from marks.
    FirstStart { now_millis: u64 },
    /// **The FirstStart policy (lead, SCALE-DESIGN "D198 addendum — the FirstStart policy", as
    /// corrected after review 3).** No mark had been recorded, but the catalog holds a live lease
    /// and there is evidence of the outage before this start. `D` starts at `credited_millis`
    /// instead of 0:
    /// - `recorded_millis`: the downtime the last D198 writer that wrote this catalog and never
    ///   resumed it found owed when it opened it, carried in its soft mark (`[0x0A]`) — 0 if none;
    /// - plus the larger of two measures of the time since:
    ///   - `now_millis − writer_mark`, where `writer_mark` is that writer's LEASE-clock reading at
    ///     its last commit — a mark's own arithmetic, lease clock minus lease clock, exact whatever
    ///     that clock's lag;
    ///   - the age of `file_mtime` (unix ms, wall clock): the catalog file's last write before this
    ///     process opened it; for a migration, the SOURCE log's; with a legacy log beside a catalog
    ///     no D198 build wrote, the earlier of the two. The age is
    ///     `max(W(now) − mtime, L(now) − mtime)`, so a backward wall-clock step after this process
    ///     started cannot shrink it below the lease clock's own reading. Its wall half over-credits
    ///     by this process's own lag; it is the only wall term (review 4, C1: `recorded_millis` is
    ///     on the writer's lease clock).
    ///
    /// The clock of every term, and the relations the bound relies on, are in
    /// `table_catalog::FirstStartEvidence`'s doc.
    ///
    /// **What it is not** (review 3, C1/C7): it is not "never an early reap". A D198 writer's soft
    /// mark makes the credit exact, plus the time between its last commit and its end — the
    /// accepted over-credit, as with a mark. Over a catalog no D198 build wrote, the file age
    /// over-credits the first start after an upgrade from a build whose lease clock was the wall
    /// clock, which is the case it exists for, and falls short, never below the 0 a plain
    /// `FirstStart` credits, only if something else wrote the file after its last authority
    /// stopped. Derivations: `bench/lease_grace/PREREG.md` amendments 10 and 11.
    FirstStartFromFileTime {
        now_millis: u64,
        file_mtime: Option<u64>,
        writer_mark: Option<u64>,
        recorded_millis: u64,
        credited_millis: u64,
    },
    /// The clock was resumed. `downtime_millis` is `now_millis - last_alive`, saturating at zero
    /// when the lease clock reads earlier than the mark (the wall clock stepped back between
    /// processes: every deadline then already has more time than it had, and nothing moves
    /// backwards). Every lease now reads `downtime_millis` later; `offset_millis` is the catalog's
    /// cumulative downtime `D` after this resume, i.e. how much later than it was written each
    /// deadline now reads. No count of leases is reported: producing one would be the O(live
    /// branches) walk at open that D198 removed.
    Resumed { last_alive: u64, now_millis: u64, downtime_millis: u64, offset_millis: u64 },
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

// **There is no maximum ancestry depth — D60.** `MAX_BRANCH_DEPTH = 8` used to live here, with
// `collapse` named as the escape and no production caller, so the ninth fork of any chain simply
// failed. Measured before removing it (`bench/d60_depth_premise.txt`): fork and read are FLAT from
// depth 1 to 250, because a branch's root is its parent's root at fork (CoW) and a read never
// walks ancestry. The one thing the cap did protect was the recursion depth of
// `has_live_children` over a chain of reaped interior nodes, which is iterative now
// (`table_catalog.rs`). See `SCALE-DESIGN.md` D60.

/// **Largest** arena extent size in pages (~1MB at 4KB pages).
///
/// This is a CAP, not the size every extent takes. See [`next_extent_pages`].
pub const ARENA_EXTENT_PAGES: u32 = 256;

/// Size of the FIRST extent a branch takes, in pages.
///
/// **D31 — every branch used to pay 1 MiB for its first 4 KiB page.** Extents were one fixed
/// size, so a branch that wrote a single page reserved `ARENA_EXTENT_PAGES` of them. Measured on
/// `examples/branch_curve_writes.rs`: 4000 branches writing one page each produced a 4.19 GB file
/// holding 16 MB of data (262x), and `pages live` equalled the branch count throughout, so every
/// branch really did hold exactly one page. Extrapolated to the 10^6 branches this project is
/// aimed at, that is ~1.05 TB. The existing 10^6 result was fork-only and never paid it.
pub const ARENA_FIRST_EXTENT_PAGES: u32 = 1;

/// How big a branch's next extent should be, given the size of the one it is currently filling.
///
/// Geometric growth, doubling per extent and capped at [`ARENA_EXTENT_PAGES`]. A branch that
/// writes one page costs one page; a branch that writes a million still lands on 256-page extents
/// within its first 511, so **per-extent reclamation stays coarse exactly where coarseness pays**
/// — the reaper's fast path frees `record.arenas` wholesale and does no per-page analysis, which
/// is the entire reason extents exist.
///
/// This is engineering, not invention. XFS grows a file's allocation geometrically for the same
/// reason (small files stay small, large files stop fragmenting), ext4 does the same, and the
/// doubling-to-a-cap shape is what slab allocators call size classes.
///
/// Pure, and takes the previous size rather than a store, so the sequence can be pinned without
/// allocating anything.
pub fn next_extent_pages(current: Option<u32>) -> u32 {
    match current {
        None => ARENA_FIRST_EXTENT_PAGES.min(ARENA_EXTENT_PAGES).max(1),
        Some(p) => p.saturating_mul(2).min(ARENA_EXTENT_PAGES).max(1),
    }
}

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
    /// A [`crate::branch::BranchCatalog::set_state`] transition was refused: the branch is not in
    /// the state its caller read. **D41.** The transition is published from a state that no longer
    /// holds, so applying it would undo whatever moved the branch — lifting a quarantine that was
    /// already lifted, or re-marking a branch somebody else is reaping.
    UnexpectedState { branch: BranchId, expected: BranchState, actual: BranchState },
    /// The lease expired; the branch is eligible for non-cooperative reaping.
    ///
    /// **Raised, not just defined, since F1's adjacent point.** It was constructed nowhere, so a
    /// branch whose lease had run out but which the scan had not reached yet — up to one scan
    /// interval for every branch that expires — could still be forked from (the child then pinned
    /// it) and written to (the write was then thrown away with it). `AgentRuntime` now refuses both
    /// with this; see `AgentRuntime::refuse_if_lease_expired`. Reads are not refused.
    LeaseExpired { branch: BranchId, deadline: LeaseDeadline, now_millis: u64 },
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
            BranchError::UnexpectedState { branch, expected, actual } => write!(
                f,
                "branch {} is {:?}, not {:?}: the state transition was computed against a record \
                 that has since moved",
                branch, actual, expected
            ),
            BranchError::LeaseExpired { branch, deadline, now_millis } => write!(
                f,
                "lease on branch {} expired at {} (now {})",
                branch, deadline.0, now_millis
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

    /// D206 on a standalone node. `tests/integration_cluster_grants.rs` carries `31364b3`'s own
    /// test for the cluster path (`try_from_now` under an applied tick); this is the same rule for
    /// `from_now`, which every production fork reaches. Red against `9aa6968`.
    #[test]
    fn an_over_long_lease_from_now_does_not_forge_the_never_expires_sentinel() {
        let d = LeaseDeadline::from_now(u64::MAX);
        assert_ne!(d.0, u64::MAX, "an over-long lease forged the trunk sentinel");
        assert_eq!(d.0, u64::MAX - 1);
        assert!(d.is_expired_at(u64::MAX), "the clamped deadline must still be reachable");
    }

    #[test]
    fn branch_state_roundtrips() {
        for s in [BranchState::Live, BranchState::Reaping, BranchState::Reaped] {
            assert_eq!(BranchState::from_u8(s.as_u8()).unwrap(), s);
        }
        assert!(BranchState::from_u8(9).is_err());
    }
}
