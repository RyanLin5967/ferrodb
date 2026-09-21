//! Intra-page delta encoding: storing a branch's divergence at the size of what changed.
//!
//! In-repo authorities (this crate has no `DESIGN.md`; the other modules' "Design authority"
//! lines point at a document that lives in a different repository, so this header cites only
//! things a reader can open here):
//! [`crate::branch::arena::ArenaPageStore::cow_page`] for the cost being attacked,
//! [`crate::branch::mod`] invariant 2 for the cost this must not introduce, and
//! [`crate::cow::node`] for the page layout the deltas are taken over.
//!
//! # The problem, stated as a measured cost and not as a slogan
//!
//! `cow_page` shadows a **whole page** the first time a branch touches one it inherited:
//!
//! ```text
//! frame.data[PAGE_HEADER_SIZE..].copy_from_slice(&source[PAGE_HEADER_SIZE..]);
//! ```
//!
//! That is 4072 bytes of payload copied and 4096 bytes of file allocated, whatever the branch
//! actually changed. An agent branch that updates four rows — tens of bytes of real divergence —
//! pays four whole pages. Content-defined chunking does not address this and is orthogonal to it:
//! chunking changes *where* boundaries fall between stored units, and the problem here is that the
//! stored unit is larger than the change by three orders of magnitude no matter where its
//! boundaries fall. ForkBase's own footnote concedes exactly this case.
//!
//! Two honest qualifications on the size of the prize, because the amplification is **not** paid
//! per write:
//!
//! 1. `cow_page` has an in-place path. A page the branch already owns and that was born at or
//!    after its privacy barrier is mutated in place, with no copy at all. So the cost is per
//!    *first touch of an inherited page*, not per write, and a hot branch rewriting one page a
//!    thousand times pays one page, not a thousand.
//! 2. The amplification is therefore governed by how many **distinct** pages a change lands on.
//!    Scattered rows are the bad case (each row its own page); clustered rows are much less bad.
//!    The sweep below measures the scattered case and says so.
//!
//! # Prior art, deflated
//!
//! Nothing in this module is novel. Each line below says what is taken and what does not fit.
//!
//! - **git packfiles.** Object deltas with a bounded chain (`pack.depth`, default 50) and a
//!   window search for a similar base. TAKEN: the chain bound and the collapse-to-full rule are
//!   git's, essentially unchanged. DOES NOT APPLY: git spends a window search to *find* a base
//!   because its objects are mutually unrelated blobs. A branch page's base is not searched for —
//!   it is the page the branch forked from, known exactly and for free. The expensive half of
//!   packfile delta compression is simply absent here.
//! - **Xdelta / VCDIFF (RFC 3284).** A COPY/ADD instruction stream against a source window.
//!   [`PageDelta`] *is* a degenerate VCDIFF: the source window is exactly one page, and the only
//!   instruction is ADD, because COPY-from-base is the implicit default for every byte not named.
//!   DOES NOT APPLY: VCDIFF's string matcher exists to *recover* which bytes moved by comparing
//!   two opaque blobs. The write path already knows which ranges it touched —
//!   `NodeMut::replace_cell_at` is the code that wrote them. The value here is not a better
//!   differ; it is not discarding knowledge the writer already had.
//! - **RocksDB merge operators, blob separation, LSM deltas.** Defer the combine, let reads
//!   consult several levels, reconcile during compaction. DOES NOT FIT: the branch store states
//!   *"No refcounts, no content addressing, no compaction"* (invariant 3) and has no level cascade
//!   in which to hide a deferred merge. The reclamation algebra is an epoch-interval rule with no
//!   notion of a level, so importing LSM machinery would contradict a stated invariant rather than
//!   extend it.
//! - **Neon's WAL-record-as-delta (pageserver delta layers, collapsed by periodic image layers).**
//!   The closest prior art by a wide margin, and the honest verdict is that **this module is
//!   Neon's model applied per branch page**. It is also a warning and not only a precedent:
//!   invariant 2 cites BranchBench measuring parent-chain-walking reads at up to 4000x
//!   degradation, and an unbounded delta chain is that same shape wearing different clothes. The
//!   chain bound below is therefore not a tuning knob — it is the whole safety argument, which is
//!   why [`MAX_CHAIN_DEPTH`] is a constant the tests bind to rather than a comment someone must
//!   remember to revisit.
//! - **ferrodb's own Typed Effect Log, [`crate::tel`].** The sharpest deflation, because it is
//!   already in this repository: the TEL records per-cell typed effects durably and merges by
//!   composing them, so ferrodb *already has* a logical delta whose size is proportional to the
//!   cells changed. The overlap has to be stated exactly rather than waved at. The TEL is on the
//!   **merge channel** and no read ever consults it; the B+tree pages are the **read channel** and
//!   cost a whole page each. Materialising a page by replaying its delta chain *is* log replay,
//!   scoped to one page instead of to the whole log. **If the TEL ever became the read path, this
//!   module would be redundant and should be deleted** — that is the stated condition for
//!   removing it, recorded here so it does not have to be rediscovered.
//!
//! So: no invention is claimed. What is new is only the *placement* — under the branch CoW store,
//! at the point where `cow_page`'s full-payload `copy_from_slice` currently throws away
//! information the writer was holding. The claim worth defending is a measurement, not an idea.
//!
//! # Scope: this is the mechanism, not yet the write path
//!
//! [`DeltaStore`] is a self-contained store used to encode, bound and measure the mechanism. It is
//! deliberately **not** wired into `ArenaPageStore::cow_page`: doing that edits files this change
//! does not own. What is established here is the encoding, the two bounds, and the slope. What is
//! not established here is behaviour under the real buffer pool, checksums, or the reaper.

use std::collections::HashMap;

use crate::branch::types::PageId;
use crate::cow::node::PAYLOAD_LEN;
use crate::error::FerroError;
use crate::storage::disk_manager::PAGE_SIZE;

/// Bytes of framing a single run costs on the wire: `at: u16` plus `len: u16`.
///
/// Also the coalescing threshold. Merging two runs separated by `g` identical bytes costs `g`
/// bytes of payload and saves one run's framing, so merging pays for itself exactly when
/// `g <= RUN_FRAME`. The threshold is this constant rather than a tuned number because it is not a
/// tuning choice — it is arithmetic over the encoding, and it moves if and only if the encoding
/// does.
pub const RUN_FRAME: usize = 4;

/// Bytes of framing a whole delta costs: `base: u32`, `depth: u8`, `run_count: u16`.
pub const DELTA_HEADER: usize = 7;

/// A run offset is a `u16`, and `at: start as u16` is a **wrapping** cast.
///
/// Made unrepresentable rather than documented. This previously said only that an offset which
/// does not fit "is a bug rather than a large page", which is true and useless: a `PAGE_SIZE`
/// change is one constant away, and the cast would then truncate silently and corrupt every
/// delta rather than failing. A compile error is the one failure mode that cannot be missed.
const _: () = assert!(
    PAYLOAD_LEN <= u16::MAX as usize,
    "a page payload no longer fits a u16 run offset; DeltaRun::at must widen"
);

/// Largest a stored delta may be before it is refused in favour of a full page.
///
/// A quarter of the payload, and the quarter is load-bearing rather than aesthetic: together with
/// [`MAX_CHAIN_DEPTH`] it is what bounds read amplification. Materialising a page touches one base
/// page plus at most `MAX_CHAIN_DEPTH` deltas of at most `DELTA_BUDGET` each, i.e. at most
/// `PAGE_SIZE + MAX_CHAIN_DEPTH * DELTA_BUDGET` bytes. At 8 and a quarter that is 12240 bytes,
/// under three pages. [`delta_tests::the_read_amplification_bound_is_arithmetic_not_hope`] asserts
/// that product, so changing either constant without re-deriving the bound fails the suite.
pub const DELTA_BUDGET: usize = PAYLOAD_LEN / 4;

/// How many deltas may stand between a read and a full page.
///
/// See [`DELTA_BUDGET`] for why this number and that one are a pair. An unbounded chain is the
/// failure mode invariant 2 exists to forbid, so this bound is enforced at write time by
/// collapsing to a full page, and is asserted by the tests rather than described.
pub const MAX_CHAIN_DEPTH: u8 = 8;

/// One contiguous run of bytes that differ from the base.
///
/// `at` is an offset into the page **payload**, not the page: the first 24 bytes are the
/// self-describing page header, which the store owns and re-stamps. Diffing them would encode a
/// birth epoch and a checksum into a delta, which is how a delta chain would come to carry GC
/// metadata that the GC algebra reads. `u16` is sufficient and deliberate — the payload is 4072
/// bytes, so an offset that does not fit is a bug rather than a large page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaRun {
    pub at: u16,
    pub bytes: Vec<u8>,
}

impl DeltaRun {
    /// Bytes this run costs when stored, framing included.
    pub fn encoded_len(&self) -> usize {
        RUN_FRAME + self.bytes.len()
    }
}

/// A page stored as its difference from another page.
///
/// Every byte not named by a run is inherited from `base`. That implicit-copy default is what
/// makes the encoding small; it is also why a delta is meaningless without its base.
///
/// **Nothing here keeps a base alive.** An earlier version of this comment claimed that
/// [`DeltaStore`] "refuses to drop a page that something still deltas against", and that was
/// false — the store has no drop, remove or free method at all, and no pinning of any kind. The
/// sentence was aspirational and is corrected here rather than deleted, because a caller wiring
/// this under a real page store would have read it as protection that exists. Base liveness is
/// entirely the caller's problem, and on the real write path it is a sharp one: `cow_page` frees
/// the page it shadows, and [`crate::branch::record::reclaimable`] answers liveness from epoch
/// intervals with no notion of a delta referencing a base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageDelta {
    base: PageId,
    depth: u8,
    runs: Vec<DeltaRun>,
}

impl PageDelta {
    /// Encode `new_payload` as a difference from `base_payload`.
    ///
    /// Both must be exactly `PAYLOAD_LEN`; a short slice is a caller bug and is refused rather
    /// than padded, because padding would silently encode a page that materialises to the wrong
    /// length.
    pub fn between(
        base: PageId,
        base_payload: &[u8],
        new_payload: &[u8],
        depth: u8,
    ) -> Result<Self, FerroError> {
        if base_payload.len() != PAYLOAD_LEN || new_payload.len() != PAYLOAD_LEN {
            return Err(FerroError::Cow(format!(
                "delta needs two {PAYLOAD_LEN}-byte payloads, got {} and {}",
                base_payload.len(),
                new_payload.len()
            )));
        }

        // Pass 1: the raw differing intervals.
        let mut raw: Vec<(usize, usize)> = Vec::new();
        let mut i = 0;
        while i < PAYLOAD_LEN {
            if base_payload[i] != new_payload[i] {
                let start = i;
                while i < PAYLOAD_LEN && base_payload[i] != new_payload[i] {
                    i += 1;
                }
                raw.push((start, i));
            } else {
                i += 1;
            }
        }

        // Pass 2: coalesce neighbours whose gap is not worth a second run's framing. Done as a
        // separate pass because fusing it into pass 1 produces an off-by-one on the gap that is
        // invisible in the output size and shows up only as a wrong byte after `apply`.
        let mut merged: Vec<(usize, usize)> = Vec::with_capacity(raw.len());
        for (start, end) in raw {
            match merged.last_mut() {
                Some((_, prev_end)) if start - *prev_end <= RUN_FRAME => *prev_end = end,
                _ => merged.push((start, end)),
            }
        }

        let runs = merged
            .into_iter()
            .map(|(start, end)| DeltaRun { at: start as u16, bytes: new_payload[start..end].to_vec() })
            .collect();

        Ok(PageDelta { base, depth, runs })
    }

    /// The page this delta is taken against.
    pub fn base(&self) -> PageId {
        self.base
    }

    /// How many deltas stand between this page and a full one, this delta included.
    pub fn depth(&self) -> u8 {
        self.depth
    }

    pub fn runs(&self) -> &[DeltaRun] {
        &self.runs
    }

    /// Bytes this delta costs when stored, all framing included.
    ///
    /// This is the number every byte column in `bench/d93_delta_encoding.txt` is built from, so
    /// it must describe a form that really exists: [`PageDelta::encode`] produces exactly this
    /// many bytes, and a test asserts the equality. Until that encoder was written, `RUN_FRAME`
    /// and `DELTA_HEADER` were free parameters — halving either one shrank every banked delta and
    /// inflated every ratio with the suite still green.
    pub fn encoded_len(&self) -> usize {
        DELTA_HEADER + self.runs.iter().map(DeltaRun::encoded_len).sum::<usize>()
    }

    /// Serialise. Big-endian throughout, matching the rest of ferrodb.
    ///
    /// Layout: `base u32 | depth u8 | run_count u16 | run_count * { at u16 | len u16 | bytes }`.
    pub fn encode(&self) -> Result<Vec<u8>, FerroError> {
        self.validate_runs()?;
        let count = u16::try_from(self.runs.len())
            .map_err(|_| FerroError::Cow(format!("a delta may hold at most {} runs", u16::MAX)))?;
        let mut out = Vec::with_capacity(self.encoded_len());
        out.extend_from_slice(&self.base.to_be_bytes());
        out.push(self.depth);
        out.extend_from_slice(&count.to_be_bytes());
        for run in &self.runs {
            let len = u16::try_from(run.bytes.len())
                .map_err(|_| FerroError::Cow("a run is longer than a page".to_string()))?;
            out.extend_from_slice(&run.at.to_be_bytes());
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(&run.bytes);
        }
        Ok(out)
    }

    /// Parse a delta produced by [`PageDelta::encode`].
    ///
    /// Validates the run shape rather than trusting it. This is the constructor that makes
    /// overlapping or out-of-order runs reachable at all — `between` cannot emit them — so it
    /// refuses them here instead of letting `apply` produce a page that depends on run order.
    pub fn decode(bytes: &[u8]) -> Result<PageDelta, FerroError> {
        let too_short =
            || FerroError::Cow("delta is truncated: not enough bytes for its framing".to_string());
        if bytes.len() < DELTA_HEADER {
            return Err(too_short());
        }
        let base = PageId::from_be_bytes(bytes[0..4].try_into().expect("4 bytes"));
        let depth = bytes[4];
        let count = u16::from_be_bytes(bytes[5..7].try_into().expect("2 bytes")) as usize;

        let mut runs = Vec::with_capacity(count);
        let mut cursor = DELTA_HEADER;
        for _ in 0..count {
            if cursor + RUN_FRAME > bytes.len() {
                return Err(too_short());
            }
            let at = u16::from_be_bytes(bytes[cursor..cursor + 2].try_into().expect("2 bytes"));
            let len = u16::from_be_bytes(bytes[cursor + 2..cursor + 4].try_into().expect("2 bytes"))
                as usize;
            cursor += RUN_FRAME;
            if cursor + len > bytes.len() {
                return Err(too_short());
            }
            runs.push(DeltaRun { at, bytes: bytes[cursor..cursor + len].to_vec() });
            cursor += len;
        }
        if cursor != bytes.len() {
            return Err(FerroError::Cow(format!(
                "delta has {} trailing bytes after its {count} runs",
                bytes.len() - cursor
            )));
        }

        let delta = PageDelta { base, depth, runs };
        delta.validate_runs()?;
        Ok(delta)
    }

    /// Payload bytes this delta writes when applied. The read-cost instrument.
    pub fn applied_bytes(&self) -> usize {
        self.runs.iter().map(|r| r.bytes.len()).sum()
    }

    /// Check every run before any of them is written.
    ///
    /// Split out from [`PageDelta::apply`] so that applying is all-or-nothing, and shared with
    /// [`PageDelta::decode`] so a delta read from disk cannot carry a shape `between` would never
    /// emit. Runs must be ordered and disjoint: overlapping runs would make the page depend on
    /// the order they are applied in, which is a silent wrong-page bug rather than an error.
    fn validate_runs(&self) -> Result<(), FerroError> {
        let mut cursor = 0usize;
        for run in &self.runs {
            let at = run.at as usize;
            let end = at.checked_add(run.bytes.len()).ok_or_else(|| {
                FerroError::Cow(format!("delta run at {at} has an overflowing length"))
            })?;
            if end > PAYLOAD_LEN {
                return Err(FerroError::Cow(format!(
                    "delta run at {at} of {} bytes runs past the payload",
                    run.bytes.len()
                )));
            }
            if at < cursor {
                return Err(FerroError::Cow(format!(
                    "delta run at {at} overlaps or precedes the previous run ending at {cursor}"
                )));
            }
            cursor = end;
        }
        Ok(())
    }

    /// Apply this delta onto a materialised base payload, in place.
    ///
    /// **All-or-nothing.** Every run is checked before any byte is written, because the previous
    /// version checked each run inside the write loop and so left the caller's buffer partly
    /// written when a later run was refused. That was invisible while the only caller applied
    /// onto a scratch `Vec` it dropped on the error — and would have corrupted a live pinned page
    /// frame the moment someone applied in place, which is exactly how this is meant to be wired.
    pub fn apply(&self, payload: &mut [u8]) -> Result<(), FerroError> {
        if payload.len() != PAYLOAD_LEN {
            return Err(FerroError::Cow(format!(
                "delta applies to a {PAYLOAD_LEN}-byte payload, got {}",
                payload.len()
            )));
        }
        self.validate_runs()?;
        for run in &self.runs {
            let at = run.at as usize;
            payload[at..at + run.bytes.len()].copy_from_slice(&run.bytes);
        }
        Ok(())
    }
}

/// Why a write stored a whole page instead of a delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Collapse {
    /// The chain was already [`MAX_CHAIN_DEPTH`] deep. This is the bound that protects reads.
    DepthLimit,
    /// The delta was not enough smaller than the page to be worth the indirection.
    Budget,
}

/// What one write actually stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Written {
    Delta { bytes: usize },
    Full { bytes: usize, why: Collapse },
}

impl Written {
    pub fn bytes(&self) -> usize {
        match self {
            Written::Delta { bytes } | Written::Full { bytes, .. } => *bytes,
        }
    }
}

/// How a page stands in the store.
#[derive(Debug, Clone)]
enum Stored {
    Full(Box<[u8]>),
    Delta(PageDelta),
}

/// What materialising a page cost. Counters, not timings: the headline numbers have to survive
/// being measured on a machine running ten other agents, and a count does while a duration
/// does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadCost {
    /// Deltas applied. This is the quantity [`MAX_CHAIN_DEPTH`] bounds.
    pub deltas_applied: usize,
    /// Payload bytes written while materialising, the base image included.
    pub bytes_touched: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeltaStats {
    pub deltas: u64,
    pub full_by_depth: u64,
    pub full_by_budget: u64,
    /// Deltas applied while materialising the base a write is taken against.
    ///
    /// There is a [`ReadCost`] and there was no counterpart for writes, which understated the
    /// mechanism: `write` materialises its base (up to `MAX_CHAIN_DEPTH` applications) and then
    /// diffs a whole payload, and wiring it under `cow_page` replaces a single 4072-byte
    /// `copy_from_slice` with all of that. Counted here so the cost appears somewhere.
    pub write_deltas_applied: u64,
    /// Payload bytes the differ scanned across all writes. Every write scans a whole payload.
    pub write_bytes_scanned: u64,
}

/// A page store that keeps a branch's divergence as deltas against the page it forked from.
///
/// Self-contained by construction: no buffer pool, no arenas, no checksums. It exists to hold the
/// encoding and the two bounds where they can be tested and measured without standing up the
/// engine. See the module header on why it is not wired into `cow_page`.
#[derive(Debug, Default)]
pub struct DeltaStore {
    pages: HashMap<PageId, Stored>,
    next_id: PageId,
    stats: DeltaStats,
}

impl DeltaStore {
    pub fn new() -> Self {
        DeltaStore::default()
    }

    pub fn stats(&self) -> DeltaStats {
        self.stats
    }

    fn fresh_id(&mut self) -> PageId {
        self.next_id += 1;
        self.next_id
    }

    /// Store a page whole. The root of every chain.
    pub fn insert_full(&mut self, payload: &[u8]) -> Result<PageId, FerroError> {
        if payload.len() != PAYLOAD_LEN {
            return Err(FerroError::Cow(format!(
                "a page payload is {PAYLOAD_LEN} bytes, got {}",
                payload.len()
            )));
        }
        let id = self.fresh_id();
        self.pages.insert(id, Stored::Full(payload.to_vec().into_boxed_slice()));
        Ok(id)
    }

    /// Store a delta directly, bypassing the write-time bounds.
    ///
    /// Test-only seam, and it earns its place: `write` refuses by construction to build a chain
    /// past [`MAX_CHAIN_DEPTH`], so the read path's own refusal is unreachable through the public
    /// API and was therefore untested — it could be deleted outright with the whole suite green.
    /// Proving a guard fires means building the state it guards against.
    #[cfg(test)]
    fn insert_delta_unchecked(&mut self, delta: PageDelta) -> PageId {
        let id = self.fresh_id();
        self.pages.insert(id, Stored::Delta(delta));
        id
    }

    /// Chain depth of a stored page: zero for a full page.
    pub fn depth_of(&self, id: PageId) -> Result<u8, FerroError> {
        match self.pages.get(&id) {
            None => Err(FerroError::Cow(format!("no such page {id}"))),
            Some(Stored::Full(_)) => Ok(0),
            Some(Stored::Delta(d)) => Ok(d.depth),
        }
    }

    /// Bytes this page occupies. A full page is charged a whole `PAGE_SIZE`, which is what
    /// `cow_page` actually allocates — charging only the payload would flatter this store's own
    /// collapses.
    pub fn stored_bytes(&self, id: PageId) -> Result<usize, FerroError> {
        match self.pages.get(&id) {
            None => Err(FerroError::Cow(format!("no such page {id}"))),
            Some(Stored::Full(_)) => Ok(PAGE_SIZE),
            Some(Stored::Delta(d)) => Ok(d.encoded_len()),
        }
    }

    /// Write `new_payload` as a child of `base`, choosing a delta or a full page.
    ///
    /// **Both bounds are enforced here, at write time, and neither is advisory.** A chain that has
    /// reached [`MAX_CHAIN_DEPTH`] collapses; a delta that does not fit [`DELTA_BUDGET`]
    /// collapses. A read therefore cannot encounter a chain the bound forbids, which is a stronger
    /// statement than checking on read and is the reason the read path below needs no policy at
    /// all.
    pub fn write(&mut self, base: PageId, new_payload: &[u8]) -> Result<(PageId, Written), FerroError> {
        if new_payload.len() != PAYLOAD_LEN {
            return Err(FerroError::Cow(format!(
                "a page payload is {PAYLOAD_LEN} bytes, got {}",
                new_payload.len()
            )));
        }
        let base_depth = self.depth_of(base)?;
        let (base_payload, base_cost) = self.materialise(base)?;
        self.stats.write_deltas_applied += base_cost.deltas_applied as u64;
        self.stats.write_bytes_scanned += PAYLOAD_LEN as u64;

        if base_depth < MAX_CHAIN_DEPTH {
            let delta = PageDelta::between(base, &base_payload, new_payload, base_depth + 1)?;
            let bytes = delta.encoded_len();
            if bytes <= DELTA_BUDGET {
                let id = self.fresh_id();
                self.pages.insert(id, Stored::Delta(delta));
                self.stats.deltas += 1;
                return Ok((id, Written::Delta { bytes }));
            }
            let id = self.fresh_id();
            self.pages.insert(id, Stored::Full(new_payload.to_vec().into_boxed_slice()));
            self.stats.full_by_budget += 1;
            return Ok((id, Written::Full { bytes: PAGE_SIZE, why: Collapse::Budget }));
        }

        let id = self.fresh_id();
        self.pages.insert(id, Stored::Full(new_payload.to_vec().into_boxed_slice()));
        self.stats.full_by_depth += 1;
        Ok((id, Written::Full { bytes: PAGE_SIZE, why: Collapse::DepthLimit }))
    }

    /// Rebuild a page's payload by applying its chain onto the full page beneath it.
    pub fn materialise(&self, id: PageId) -> Result<(Vec<u8>, ReadCost), FerroError> {
        let mut chain: Vec<&PageDelta> = Vec::new();
        let mut cursor = id;
        loop {
            match self.pages.get(&cursor) {
                None => return Err(FerroError::Cow(format!("no such page {cursor}"))),
                Some(Stored::Full(base)) => {
                    let mut payload = base.to_vec();
                    let mut bytes_touched = payload.len();
                    // The chain was collected leaf-first, so apply it base-first.
                    for delta in chain.iter().rev() {
                        delta.apply(&mut payload)?;
                        bytes_touched += delta.applied_bytes();
                    }
                    return Ok((
                        payload,
                        ReadCost { deltas_applied: chain.len(), bytes_touched },
                    ));
                }
                Some(Stored::Delta(delta)) => {
                    // Refuse rather than loop forever. Reaching here means a chain longer than the
                    // write path can build, i.e. a bug in this file — and an unbounded read is
                    // precisely the outcome the bound exists to prevent, so it must not be
                    // reachable by falling through.
                    // `>=`, checked BEFORE the push. With `>` a ninth delta was pushed and then
                    // materialised, so a chain one past the bound read back fine and reported
                    // `deltas_applied = 9` — past the ceiling
                    // `the_read_amplification_bound_is_arithmetic_not_hope` certifies — while the
                    // error text named 8 and actually refused at 10.
                    if chain.len() >= MAX_CHAIN_DEPTH as usize {
                        return Err(FerroError::Cow(format!(
                            "chain from page {id} is deeper than MAX_CHAIN_DEPTH ({MAX_CHAIN_DEPTH})"
                        )));
                    }
                    chain.push(delta);
                    cursor = delta.base;
                }
            }
        }
    }
}

#[cfg(test)]
mod delta_tests {
    use super::*;
    use crate::cow::node::{leaf_cell, NodeMut};
    use crate::cow::page_header::PAGE_HEADER_SIZE;
    use std::collections::HashSet;

    const KEY_LEN: usize = 8;
    const VALUE_LEN: usize = 24;
    /// Cell as `src/cow/node.rs` encodes a leaf entry: `key_len u32 | key | value`.
    const CELL_LEN: usize = 4 + KEY_LEN + VALUE_LEN;

    /// xorshift64. A named, seeded generator so the sweep is reproducible from the seed printed
    /// in the banked file, rather than from whatever the machine's entropy was that night.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    fn key_of(row: u64) -> Vec<u8> {
        row.to_be_bytes().to_vec()
    }

    /// A fixed-width value, so an update replaces a cell with one of equal size. That is the
    /// friendly case for the *node*, not for the delta: an equal-size replace still writes a whole
    /// new cell into fresh heap space (`NodeMut::write_cell` always appends), so the delta has to
    /// encode it either way.
    ///
    /// **High-entropy bytes, deliberately, and this is not cosmetic.** A zero-filled value
    /// flatters a byte differ in two separate places, and the first version of this fixture was
    /// zero-filled and did exactly that. A cell written into virgin heap diffs only in its
    /// non-zero bytes, because `NodeMut::init` zeroes the payload; and a cell *shifted* by a width
    /// change does not diff at all wherever zeros land on zeros. Neither is true of real column
    /// data, so measuring against zeros would report a delta arm that no real table could
    /// reproduce.
    fn value_of(row: u64, generation: u64, len: usize) -> Vec<u8> {
        let mut seed = row.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ generation.wrapping_mul(0xD6E8_FEB8_6659_FD93);
        if seed == 0 {
            seed = 0xABCD_EF01_2345_6789;
        }
        let mut rng = Rng(seed);
        (0..len).map(|_| (rng.next() >> 33) as u8).collect()
    }

    /// `heap_end` out of a payload. The layout is documented in the `src/cow/node.rs` header:
    /// bytes 4..8 of the payload, big-endian. Read directly because the accessor is private to
    /// that module, and this is the only way to observe a compaction from outside it.
    fn heap_end_of(payload: &[u8]) -> usize {
        u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) as usize
    }

    /// A table laid out by the real slotted encoder.
    ///
    /// `rows_per_page` is the knob that decides how much free heap a page has, and therefore how
    /// many in-place updates it absorbs before `compact()` rewrites the whole cell area. It is
    /// explicit rather than derived because the two fixtures below need different answers: the
    /// sweep wants a realistic post-split page, and the chain walk wants headroom so that the
    /// DEPTH bound is the only thing that can fire.
    struct Fixture {
        pages: Vec<[u8; PAGE_SIZE]>,
        rows_per_page: usize,
    }

    impl Fixture {
        fn build(rows: usize, rows_per_page: usize) -> Self {
            let page_count = rows.div_ceil(rows_per_page);
            let mut pages = Vec::with_capacity(page_count);
            for p in 0..page_count {
                let mut page = [0u8; PAGE_SIZE];
                let first = p * rows_per_page;
                let last = ((p + 1) * rows_per_page).min(rows);
                let entries: Vec<(Vec<u8>, Vec<u8>)> = (first..last)
                    .map(|row| (key_of(row as u64), value_of(row as u64, 0, VALUE_LEN)))
                    .collect();
                {
                    let mut node = NodeMut::new(&mut page);
                    node.init();
                    node.fill_leaf(&entries).expect("fixture page must fit");
                }
                pages.push(page);
            }
            Fixture { pages, rows_per_page }
        }

        fn payload(&self, page: usize) -> &[u8] {
            &self.pages[page][PAGE_HEADER_SIZE..]
        }

        fn page_of(&self, row: usize) -> usize {
            row / self.rows_per_page
        }

        /// Update one row through the real node encoder. Returns whether the node compacted.
        fn update_row(&mut self, row: usize, generation: u64) -> bool {
            self.update_row_with_len(row, generation, VALUE_LEN)
        }

        /// Update one row to a value of a chosen length.
        ///
        /// The length is a parameter because it is the variable that decides whether a later
        /// compaction is cheap or ruinous. `compact()` rewrites cells in slot order descending
        /// from `PAYLOAD_LEN`, so while every cell keeps the width it had at fill time, compaction
        /// puts each one back at exactly the offset `fill_leaf` chose — it is layout-*restoring*,
        /// and the diff is bounded by the cells that actually diverged. Change one cell's width
        /// and every cell below it moves instead, so the diff grows toward the whole cell area.
        /// Measured: 1373 B against 2552 B on the same fixture.
        fn update_row_with_len(&mut self, row: usize, generation: u64, value_len: usize) -> bool {
            let page = self.page_of(row);
            let slot = row % self.rows_per_page;
            let before = heap_end_of(self.payload(page));
            let cell = leaf_cell(&key_of(row as u64), &value_of(row as u64, generation, value_len));
            {
                let mut node = NodeMut::new(&mut self.pages[page]);
                assert!(
                    node.replace_cell_at(slot, &cell).expect("replace must not error"),
                    "row {row} did not fit; the fixture is misconfigured"
                );
            }
            heap_end_of(self.payload(page)) > before
        }
    }

    fn pick_rows(rng: &mut Rng, total: usize, r: usize) -> Vec<usize> {
        let mut seen = HashSet::with_capacity(r);
        let mut out = Vec::with_capacity(r);
        while out.len() < r {
            let row = rng.below(total);
            if seen.insert(row) {
                out.push(row);
            }
        }
        out
    }

    #[derive(Debug, Clone, Copy)]
    struct SweepRow {
        r: usize,
        pages_touched: usize,
        whole_bytes: usize,
        delta_bytes: usize,
        compacted_pages: usize,
        collapsed_pages: usize,
    }

    impl SweepRow {
        fn ratio(&self) -> f64 {
            self.whole_bytes as f64 / self.delta_bytes as f64
        }

        fn delta_bytes_per_row(&self) -> f64 {
            self.delta_bytes as f64 / self.r as f64
        }
    }

    /// One point of the sweep.
    ///
    /// The delta arm runs through [`DeltaStore::write`], not through a parallel size calculation,
    /// so the number measured is produced by the same code that would store it — budget collapse
    /// included. A separate "what would the delta have been" calculation is exactly how an arm
    /// comes to report a size the real path would never have stored.
    fn sweep_point(rows: usize, rows_per_page: usize, r: usize, seed: u64) -> SweepRow {
        let mut rng = Rng(seed);
        let base = Fixture::build(rows, rows_per_page);
        let mut after = Fixture::build(rows, rows_per_page);

        let targets = pick_rows(&mut rng, rows, r);
        let mut compacted_pages = HashSet::new();
        let mut touched = HashSet::new();
        for row in &targets {
            let page = after.page_of(*row);
            touched.insert(page);
            if after.update_row(*row, 1) {
                compacted_pages.insert(page);
            }
        }

        let mut store = DeltaStore::new();
        let mut whole_bytes = 0usize;
        let mut delta_bytes = 0usize;
        let mut collapsed_pages = 0usize;
        let mut pages: Vec<usize> = touched.into_iter().collect();
        pages.sort_unstable();
        for page in &pages {
            let base_id = store.insert_full(base.payload(*page)).expect("base page");
            let (_, written) = store.write(base_id, after.payload(*page)).expect("delta write");
            delta_bytes += written.bytes();
            if matches!(written, Written::Full { .. }) {
                collapsed_pages += 1;
            }
            // What `cow_page` costs today: one whole freshly allocated page per touched page.
            whole_bytes += PAGE_SIZE;
        }

        SweepRow {
            r,
            pages_touched: pages.len(),
            whole_bytes,
            delta_bytes,
            compacted_pages: compacted_pages.len(),
            collapsed_pages,
        }
    }

    // ---- correctness -------------------------------------------------------------------

    #[test]
    fn an_unchanged_page_encodes_to_no_runs_at_all() {
        let fixture = Fixture::build(60, 60);
        let delta = PageDelta::between(1, fixture.payload(0), fixture.payload(0), 1).unwrap();
        assert!(delta.runs().is_empty(), "identical payloads must produce no runs");
        assert_eq!(delta.encoded_len(), DELTA_HEADER);
    }

    #[test]
    fn a_delta_round_trips_through_the_real_node_encoder() {
        let base = Fixture::build(600, 60);
        let mut after = Fixture::build(600, 60);
        for row in [3usize, 17, 44, 59] {
            after.update_row(row, 7);
        }

        let delta = PageDelta::between(1, base.payload(0), after.payload(0), 1).unwrap();
        let mut rebuilt = base.payload(0).to_vec();
        delta.apply(&mut rebuilt).unwrap();
        assert_eq!(rebuilt, after.payload(0), "applying the delta must reproduce the page exactly");
    }

    #[test]
    fn runs_coalesce_exactly_when_the_gap_is_not_worth_its_framing() {
        let base = vec![0u8; PAYLOAD_LEN];

        // Two single-byte changes separated by RUN_FRAME identical bytes: one run is cheaper.
        let mut near = base.clone();
        near[100] = 1;
        near[100 + 1 + RUN_FRAME] = 1;
        let delta = PageDelta::between(1, &base, &near, 1).unwrap();
        assert_eq!(delta.runs().len(), 1, "a gap of {RUN_FRAME} must coalesce");

        // One byte further apart and a second run is cheaper.
        let mut far = base.clone();
        far[100] = 1;
        far[100 + 2 + RUN_FRAME] = 1;
        let delta = PageDelta::between(1, &base, &far, 1).unwrap();
        assert_eq!(delta.runs().len(), 2, "a gap of {} must not coalesce", RUN_FRAME + 1);
    }

    #[test]
    fn a_delta_refuses_a_payload_that_is_not_a_page() {
        let short = vec![0u8; 10];
        let full = vec![0u8; PAYLOAD_LEN];
        assert!(PageDelta::between(1, &short, &full, 1).is_err());
        assert!(PageDelta::between(1, &full, &short, 1).is_err());

        let delta = PageDelta::between(1, &full, &full, 1).unwrap();
        let mut wrong = vec![0u8; 10];
        assert!(delta.apply(&mut wrong).is_err(), "applying onto a short buffer must refuse");
    }

    // ---- the bounds --------------------------------------------------------------------

    /// The read-amplification bound is a product of the two constants, so it is asserted rather
    /// than described. Raising either without re-deriving this fails here.
    #[test]
    fn the_read_amplification_bound_is_arithmetic_not_hope() {
        let worst_case = PAGE_SIZE + MAX_CHAIN_DEPTH as usize * DELTA_BUDGET;
        assert!(
            worst_case <= 3 * PAGE_SIZE,
            "materialising may touch at most three pages' worth of bytes, got {worst_case}"
        );
    }

    /// F-A. The chain bound holds for every chain length, and — the half that makes it a real
    /// test — the bound is *exercised*: collapses fire, and they fire for the depth reason, not
    /// because the budget quietly did the work instead.
    #[test]
    fn a_delta_chain_never_makes_a_read_deeper_than_the_bound() {
        // Twenty rows to a page leaves roughly 3180 bytes of free heap, i.e. about 88 replaces
        // before `compact()` can fire. Sixty-four generations therefore cannot compact, which is
        // what isolates the depth bound from the budget bound.
        let mut fixture = Fixture::build(20, 20);
        let mut store = DeltaStore::new();
        let mut current = store.insert_full(fixture.payload(0)).unwrap();
        let mut deepest = 0usize;
        let mut worst_bytes = 0usize;

        for generation in 1..=64u64 {
            let row = (generation as usize) % 20;
            assert!(!fixture.update_row(row, generation), "fixture must not compact here");
            let (next, written) = store.write(current, fixture.payload(0)).unwrap();
            current = next;

            // Charge the write what it actually stores, in BOTH directions. Asserting only an
            // upper bound let a mutant report zero bytes for a collapse and stay green.
            match written {
                Written::Delta { bytes } => {
                    assert!(bytes > 0 && bytes <= DELTA_BUDGET, "delta charged {bytes} B");
                    assert_eq!(
                        store.stored_bytes(current).unwrap(),
                        bytes,
                        "stored_bytes must agree with what the write reported"
                    );
                }
                Written::Full { bytes, why } => {
                    assert_eq!(why, Collapse::DepthLimit, "only the depth bound may fire here");
                    assert_eq!(bytes, PAGE_SIZE, "a collapse stores a whole page");
                    assert_eq!(
                        store.stored_bytes(current).unwrap(),
                        PAGE_SIZE,
                        "a collapsed page is charged a whole page"
                    );
                }
            }

            let (materialised, cost) = store.materialise(current).unwrap();
            assert!(
                cost.bytes_touched >= PAYLOAD_LEN,
                "materialising must touch at least the base payload"
            );
            if cost.deltas_applied > 0 {
                assert!(
                    cost.bytes_touched > PAYLOAD_LEN,
                    "applying {} deltas must count more than the base alone",
                    cost.deltas_applied
                );
            }
            assert_eq!(
                materialised,
                fixture.payload(0),
                "generation {generation} must materialise to the real page"
            );
            assert!(
                cost.deltas_applied <= MAX_CHAIN_DEPTH as usize,
                "generation {generation} needed {} deltas, bound is {MAX_CHAIN_DEPTH}",
                cost.deltas_applied
            );
            deepest = deepest.max(cost.deltas_applied);
            worst_bytes = worst_bytes.max(cost.bytes_touched);
        }

        let stats = store.stats();
        assert_eq!(
            stats.deltas + stats.full_by_depth,
            64,
            "every generation must be counted exactly once: {stats:?}"
        );
        assert!(
            stats.full_by_depth > 0,
            "the depth bound never fired, so this test proved nothing: {stats:?}"
        );
        assert_eq!(
            stats.full_by_budget, 0,
            "the budget bound fired in the depth fixture, so the two bounds are not isolated: {stats:?}"
        );
        assert_eq!(
            deepest, MAX_CHAIN_DEPTH as usize,
            "chains should reach the bound exactly before collapsing"
        );
        assert!(
            worst_bytes <= PAGE_SIZE + MAX_CHAIN_DEPTH as usize * DELTA_BUDGET,
            "worst materialisation touched {worst_bytes} bytes, past the derived bound"
        );
    }

    /// Build a chain of exactly `depth` deltas over a full base, bypassing the write-time bounds.
    fn hand_built_chain(store: &mut DeltaStore, base_payload: &[u8], depth: usize) -> PageId {
        let mut id = store.insert_full(base_payload).unwrap();
        for d in 1..=depth {
            let delta = PageDelta {
                base: id,
                depth: d as u8,
                runs: vec![DeltaRun { at: (d * 8) as u16, bytes: vec![d as u8; 4] }],
            };
            id = store.insert_delta_unchecked(delta);
        }
        id
    }

    /// The read path's own refusal, which `write` can never provoke and which was therefore
    /// untested: the whole suite stayed green with this guard deleted outright.
    ///
    /// Pins BOTH sides of the boundary, because the guard was off by one. It read
    /// `chain.len() > MAX_CHAIN_DEPTH` *before* the push, so a chain of nine materialised happily
    /// and reported `deltas_applied = 9` — past the ceiling
    /// [`the_read_amplification_bound_is_arithmetic_not_hope`] certifies — while refusing only at
    /// ten and naming eight in the message.
    #[test]
    fn materialise_accepts_a_chain_at_the_bound_and_refuses_one_past_it() {
        let base = vec![0u8; PAYLOAD_LEN];

        let mut store = DeltaStore::new();
        let at_bound = hand_built_chain(&mut store, &base, MAX_CHAIN_DEPTH as usize);
        let (_, cost) = store
            .materialise(at_bound)
            .expect("a chain exactly at the bound must materialise");
        assert_eq!(
            cost.deltas_applied, MAX_CHAIN_DEPTH as usize,
            "a chain at the bound must apply exactly MAX_CHAIN_DEPTH deltas"
        );

        let mut store = DeltaStore::new();
        let past_bound = hand_built_chain(&mut store, &base, MAX_CHAIN_DEPTH as usize + 1);
        assert!(
            store.materialise(past_bound).is_err(),
            "a chain one past the bound must be refused, not materialised"
        );
    }

    /// Applying is all-or-nothing. The previous version checked each run inside the write loop,
    /// so a refusal left the caller's buffer partly written — harmless only because the one
    /// caller applied onto a scratch `Vec`, and a live page frame the moment it is wired in.
    #[test]
    fn a_refused_apply_writes_no_bytes_at_all() {
        let delta = PageDelta {
            base: 1,
            depth: 1,
            runs: vec![
                DeltaRun { at: 0, bytes: vec![0xAA; 8] },
                // Starts inside the payload and runs off the end.
                DeltaRun { at: (PAYLOAD_LEN - 2) as u16, bytes: vec![0xBB; 8] },
            ],
        };
        let mut payload = vec![0u8; PAYLOAD_LEN];
        assert!(delta.apply(&mut payload).is_err(), "the overrunning run must be refused");
        assert!(
            payload.iter().all(|&b| b == 0),
            "apply refused but still wrote bytes; it is not all-or-nothing"
        );
    }

    /// Overlapping or out-of-order runs make the resulting page depend on the order they are
    /// applied in. `between` cannot emit them, but [`PageDelta::decode`] can be handed them.
    #[test]
    fn apply_refuses_runs_that_overlap_or_run_backwards() {
        let mut payload = vec![0u8; PAYLOAD_LEN];

        let overlapping = PageDelta {
            base: 1,
            depth: 1,
            runs: vec![
                DeltaRun { at: 10, bytes: vec![1; 4] },
                DeltaRun { at: 12, bytes: vec![2; 4] },
            ],
        };
        assert!(overlapping.apply(&mut payload).is_err(), "overlapping runs must be refused");

        let backwards = PageDelta {
            base: 1,
            depth: 1,
            runs: vec![
                DeltaRun { at: 20, bytes: vec![1; 4] },
                DeltaRun { at: 10, bytes: vec![2; 4] },
            ],
        };
        assert!(backwards.apply(&mut payload).is_err(), "unordered runs must be refused");
        assert!(payload.iter().all(|&b| b == 0), "neither refusal may write bytes");
    }

    /// `encoded_len` is what every byte column in `bench/d93_delta_encoding.txt` is built from, so
    /// it has to describe a form that exists. Until `encode` was written, `RUN_FRAME` and
    /// `DELTA_HEADER` were free parameters: halving either shrank every banked number and
    /// inflated every ratio with the suite still green.
    #[test]
    fn encoded_len_is_exactly_the_length_of_the_real_encoding() {
        let base = Fixture::build(600, 60);
        let mut after = Fixture::build(600, 60);
        for row in [3usize, 17, 44, 59] {
            after.update_row(row, 7);
        }
        let delta = PageDelta::between(9, base.payload(0), after.payload(0), 1).unwrap();

        let bytes = delta.encode().unwrap();
        assert_eq!(
            bytes.len(),
            delta.encoded_len(),
            "encoded_len does not describe what encode actually produces"
        );

        let decoded = PageDelta::decode(&bytes).unwrap();
        assert_eq!(decoded, delta, "a delta must survive a round trip through its encoding");

        let mut rebuilt = base.payload(0).to_vec();
        decoded.apply(&mut rebuilt).unwrap();
        assert_eq!(rebuilt, after.payload(0), "a decoded delta must rebuild the same page");
    }

    #[test]
    fn decode_refuses_a_delta_that_is_truncated_or_has_trailing_bytes() {
        let base = Fixture::build(600, 60);
        let mut after = Fixture::build(600, 60);
        after.update_row(11, 3);
        let delta = PageDelta::between(9, base.payload(0), after.payload(0), 1).unwrap();
        let bytes = delta.encode().unwrap();

        assert!(PageDelta::decode(&[]).is_err(), "an empty buffer is not a delta");
        assert!(
            PageDelta::decode(&bytes[..bytes.len() - 1]).is_err(),
            "a truncated delta must be refused, not silently short-read"
        );
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(
            PageDelta::decode(&trailing).is_err(),
            "trailing bytes mean the framing disagrees with the buffer"
        );

        // A hand-built encoding whose runs overlap must be refused at the door. `encode`
        // validates too, so these bytes are assembled by hand.
        let mut raw = Vec::new();
        raw.extend_from_slice(&1u32.to_be_bytes());
        raw.push(1);
        raw.extend_from_slice(&2u16.to_be_bytes());
        for (at, len) in [(10u16, 4u16), (11u16, 4u16)] {
            raw.extend_from_slice(&at.to_be_bytes());
            raw.extend_from_slice(&len.to_be_bytes());
            raw.extend_from_slice(&vec![1u8; len as usize]);
        }
        assert!(
            PageDelta::decode(&raw).is_err(),
            "decode must refuse overlapping runs rather than hand them to apply"
        );
    }

    /// Drive one page until `compact()` fires, returning the payload just before the compacting
    /// update and the payload just after it.
    fn drive_to_compaction(fixture: &mut Fixture) -> (u64, Vec<u8>, Vec<u8>) {
        for generation in 1..=400u64 {
            let previous = fixture.payload(0).to_vec();
            if fixture.update_row((generation as usize) % fixture.rows_per_page, generation) {
                return (generation, previous, fixture.payload(0).to_vec());
            }
        }
        panic!("the page never compacted; the fixture is misconfigured");
    }

    /// Size of the delta across the first compacting update, and what the store did with it.
    fn compaction_cost(mut fixture: Fixture) -> (u64, usize, Written, DeltaStats) {
        let (generation, before, after) = drive_to_compaction(&mut fixture);
        let delta = PageDelta::between(1, &before, &after, 1).unwrap();

        // A compacted page must still round-trip; a cheap encoding that lost bytes here would
        // look like a win in every size column.
        let mut rebuilt = before.clone();
        delta.apply(&mut rebuilt).unwrap();
        assert_eq!(rebuilt, after, "a compacted page must round-trip");

        let mut store = DeltaStore::new();
        let base = store.insert_full(&before).unwrap();
        let (id, written) = store.write(base, &after).unwrap();
        assert_eq!(
            store.stored_bytes(id).unwrap(),
            PAGE_SIZE,
            "a collapsed page must be charged a whole page, not its payload"
        );
        (generation, delta.encoded_len(), written, store.stats())
    }

    /// F-C. Compaction is the regime where the mechanism does not help, and the store must say so
    /// by refusing to call the result a delta.
    ///
    /// `compact()` moves every cell that has been rewritten since the page was filled back to the
    /// offset `fill_leaf` chose for it, so the diff across a compaction is the whole set of cells
    /// that diverged, not the one row the update touched. Pinned so that the caveat on the sweep
    /// cannot quietly stop being true.
    #[test]
    fn a_compacting_update_blows_the_budget_and_is_stored_whole() {
        let (generation, bytes, written, stats) = compaction_cost(Fixture::build(60, 60));
        println!(
            "equal-width compaction at generation {generation}: {bytes} B delta, budget {DELTA_BUDGET} B"
        );
        // The two collapse counters must not be interchangeable: this collapse is a BUDGET
        // collapse, and asserting only `full_by_depth > 0` elsewhere let one stand in for the
        // other. Both are pinned here, on the one path that produces a budget collapse.
        assert_eq!(
            stats,
            DeltaStats {
                deltas: 0,
                full_by_depth: 0,
                full_by_budget: 1,
                write_deltas_applied: 0,
                write_bytes_scanned: PAYLOAD_LEN as u64,
            },
            "a compacting write must count as exactly one budget collapse"
        );
        assert!(
            bytes > DELTA_BUDGET,
            "compaction produced only {bytes} B, inside the {DELTA_BUDGET} B budget — if this \
             holds, the disclosed limitation is not real and the sweep's caveat must be rewritten"
        );
        assert_eq!(
            written,
            Written::Full { bytes: PAGE_SIZE, why: Collapse::Budget },
            "a compacting update must be stored whole"
        );
    }

    /// The mechanism claim behind F-C, as an ordering rather than a number: a width change makes
    /// compaction strictly worse, because it moves cells that would otherwise have landed back on
    /// their original offsets untouched.
    #[test]
    fn a_width_change_makes_compaction_strictly_worse() {
        let (_, equal_width, _, _) = compaction_cost(Fixture::build(60, 60));

        let mut widened = Fixture::build(60, 60);
        // One row grows by eight bytes. Every cell below it is now misaligned with the offsets
        // `fill_leaf` chose, and compaction makes that visible in all of them at once.
        widened.update_row_with_len(0, 1, VALUE_LEN + 8);
        let (_, changed_width, _, _) = compaction_cost(widened);

        println!(
            "compaction delta: {equal_width} B equal-width vs {changed_width} B after a width change"
        );
        assert!(
            changed_width > equal_width,
            "a width change should make compaction worse, but it cost {changed_width} B against \
             {equal_width} B — the cell-shifting explanation in this module's header is wrong"
        );
    }

    /// F-B. The pre-registered bar: stored bytes must fall at least tenfold at four rows changed.
    #[test]
    fn four_changed_rows_cost_at_least_ten_times_less_than_four_whole_pages() {
        let row = sweep_point(16_384, 60, 4, 0x5eed_d93);
        assert_eq!(row.pages_touched, 4, "four scattered rows should land on four pages");
        assert_eq!(row.compacted_pages, 0, "no page should compact at r=4");
        assert!(
            row.ratio() >= 10.0,
            "F-B fired: r=4 stored {} bytes against {} whole-page bytes, ratio {:.1}x < 10x",
            row.delta_bytes,
            row.whole_bytes,
            row.ratio()
        );
    }

    /// The slope claim itself, as an assertion rather than only a printed table: per-row cost must
    /// stay roughly flat as r grows, which is what "proportional to r" means. Whole-page storage
    /// cannot do this — its per-row cost falls as rows start sharing pages, and that is the shape
    /// the ratio narrows against at large r.
    #[test]
    fn stored_bytes_track_rows_changed_rather_than_pages_touched() {
        let one = sweep_point(16_384, 60, 1, 0x5eed_d93);
        let many = sweep_point(16_384, 60, 256, 0x5eed_d93);
        let growth = many.delta_bytes_per_row() / one.delta_bytes_per_row();
        assert!(
            growth < 2.0,
            "per-row delta cost grew {growth:.2}x between r=1 ({:.1} B/row) and r=256 ({:.1} B/row); \
             that is not proportional to r",
            one.delta_bytes_per_row(),
            many.delta_bytes_per_row()
        );
    }

    // ---- the banked sweep --------------------------------------------------------------

    /// Prints the table banked in `bench/d93_delta_encoding.txt`.
    ///
    /// Ignored by default: it is a measurement, not a guard, and the guards above are the things
    /// that must run on every suite. Run it with
    /// `cargo test --lib branch::delta -- --ignored --nocapture`.
    #[test]
    #[ignore = "measurement, not a guard; run explicitly to regenerate the banked table"]
    fn d93_sweep() {
        const ROWS: usize = 16_384;
        const ROWS_PER_PAGE: usize = 60;
        const SEED: u64 = 0x5eed_d93;

        println!("ferrodb {}", crate::build_provenance());
        println!(
            "fixture: {ROWS} rows, {ROWS_PER_PAGE} rows/page, key {KEY_LEN} B, value {VALUE_LEN} B, \
             cell {CELL_LEN} B, seed {SEED:#x}"
        );
        println!(
            "constants: PAGE_SIZE={PAGE_SIZE} PAYLOAD_LEN={PAYLOAD_LEN} \
             MAX_CHAIN_DEPTH={MAX_CHAIN_DEPTH} DELTA_BUDGET={DELTA_BUDGET}"
        );
        let probe = Fixture::build(ROWS, ROWS_PER_PAGE);
        println!("pages in fixture: {}", probe.pages.len());
        println!();
        println!(
            "{:>6}  {:>6}  {:>12}  {:>12}  {:>9}  {:>11}  {:>9}  {:>9}",
            "r", "pages", "whole B", "delta B", "ratio", "delta B/row", "compact", "collapsed"
        );

        let mut r = 1usize;
        while r <= 4096 {
            let row = sweep_point(ROWS, ROWS_PER_PAGE, r, SEED);
            println!(
                "{:>6}  {:>6}  {:>12}  {:>12}  {:>8.1}x  {:>11.1}  {:>9}  {:>9}",
                row.r,
                row.pages_touched,
                row.whole_bytes,
                row.delta_bytes,
                row.ratio(),
                row.delta_bytes_per_row(),
                row.compacted_pages,
                row.collapsed_pages,
            );
            r *= 2;
        }

        println!();
        println!("chain depth: one page, one row updated per generation, 20 rows/page");
        println!(
            "{:>6}  {:>8}  {:>14}  {:>10}",
            "gen", "deltas", "bytes touched", "stored B"
        );
        let mut fixture = Fixture::build(20, 20);
        let mut store = DeltaStore::new();
        let mut current = store.insert_full(fixture.payload(0)).unwrap();
        for generation in 1..=24u64 {
            fixture.update_row((generation as usize) % 20, generation);
            let (next, _) = store.write(current, fixture.payload(0)).unwrap();
            current = next;
            let (_, cost) = store.materialise(current).unwrap();
            println!(
                "{:>6}  {:>8}  {:>14}  {:>10}",
                generation,
                cost.deltas_applied,
                cost.bytes_touched,
                store.stored_bytes(current).unwrap(),
            );
        }
        println!("chain stats after 24 generations: {:?}", store.stats());
    }
}

