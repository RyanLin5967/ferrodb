//! The durable branch metadata record, and the GC reclamation predicate that its shape exists
//! to answer.
//!
//! Design authority: DESIGN.md section 1.
//!
//! Fork = one durable `BranchRecord` + append `fork_epoch` to the parent's sorted
//! `live_children` array. No page is read, written, or refcounted, which is exit criterion 1.

use crate::branch::types::{
    ArenaId, BranchError, BranchId, BranchState, Epoch, LeaseDeadline, PageId, MAX_BRANCH_DEPTH,
};
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::wal::log::crc32;

/// Durable metadata for one branch. This record *is* the branch — there is nothing else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchRecord {
    /// Identity as minted, including the generation live at creation time.
    pub branch_id: BranchId,
    /// Current generation of this id slot. Equals `branch_id.generation` while the branch is
    /// `Live`; bumped when the branch is reaped so stale handles fail loudly.
    pub generation: u32,
    /// `None` only for the trunk.
    pub parent_id: Option<BranchId>,
    /// The epoch at which this branch forked from its parent. This is the single value the
    /// reclamation rule consults.
    pub fork_epoch: Epoch,
    /// The B+tree root. At fork this is byte-identical to the parent's root — that is the
    /// entire fork operation, and it is why the read path never walks the parent chain.
    pub root_page_id: PageId,
    /// Non-cooperative reaping deadline. Every branch has one; there is no exemption class.
    pub lease_deadline: LeaseDeadline,
    pub state: BranchState,
    /// Private extents this branch allocates novel pages from. Reaping a childless leaf frees
    /// these wholesale with no sharing analysis.
    pub arenas: Vec<ArenaId>,
    /// Fork epochs of this branch's **live** children, kept sorted ascending. The reclamation
    /// rule is a range-emptiness query over this array: O(log k).
    pub live_children: Vec<Epoch>,
    /// Ancestry depth; 0 for trunk. Collapse when this would exceed `MAX_BRANCH_DEPTH`.
    pub depth: u8,
    /// **What this branch is permitted to write.** `None` means no envelope was ever installed,
    /// which is the pre-envelope shape and is ungoverned — see [`CapabilityEnvelope`] for why
    /// that is the compatibility default rather than "deny everything".
    ///
    /// It lives *here*, in the durable record, and not in the runtime's `Mutex<State>`, because a
    /// policy held only in memory un-governs every running agent the moment the process restarts.
    /// Merge policy, escrow claims and quarantine reasons are all still in-memory; this one is not.
    pub envelope: Option<CapabilityEnvelope>,
}

/// Exact width of [`BranchRecord::serialize_core`]. Named so the encoder and the decoder cannot
/// drift: both assert against it.
pub const CORE_BYTES: usize = 51;

/// A branch record as it is **stored**: the fixed-width core, with `arenas` empty and `envelope`
/// `None` because both live in their own key spans and loading them costs a range scan.
///
/// ⛔ THIS TYPE EXISTS BECAUSE TWO BUGS SHIPPED THROUGH THE GAP IT CLOSES, and both were invisible
/// to the tests that were looking at the code around them:
///
///  1. `set_root` / `renew_lease` / `scan` read a core record and wrote it back. `write_record`
///     makes the arena span match the record it is handed, so the span was EMPTIED -- and the
///     reaper frees precisely `record.arenas`, so those pages leaked permanently. The obvious
///     assertion (`populated.reserved > baseline.reserved`) PASSED.
///  2. `fork` read the parent as a core record, so its `envelope` was `None`, and `fork_child`
///     does `parent.envelope.as_ref().map(CapabilityEnvelope::inherited)`. `None` maps to `None`,
///     which is the UNGOVERNED default -- so a child of a governed branch came back ungoverned.
///     **That is a capability escape, and it reached the remote before it was caught.**
///
/// Both are one defect: an incomplete value accepted where a complete one was required. Commenting
/// "remember to hydrate" is validation, and validation is forgotten; this is the parse, and its
/// result is carried in the type. (Alexis King, *Parse, don't validate*, 2019.)
///
/// **Deliberately absent, and each absence is load-bearing:** no `Deref`, no `AsRef<BranchRecord>`,
/// no `pub` field, no `into_inner()`. Any one of them reopens the hole. There are no `arenas()` or
/// `envelope()` accessors either -- those are exactly the two fields this value does not have, and
/// an accessor returning an empty vec would be a lie with a type signature.
///
/// It also lives in **this** module rather than beside its only consumer in `table_catalog.rs`,
/// because a newtype declared next to its consumer is honour-system: `.0` would be in scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreRecord(BranchRecord);

impl CoreRecord {
    /// The **only** way to a `BranchRecord`, and it demands the two fields the core is missing.
    /// You cannot obtain the complete value without supplying what made it incomplete.
    pub fn into_hydrated(
        mut self,
        arenas: Vec<ArenaId>,
        envelope: Option<CapabilityEnvelope>,
    ) -> BranchRecord {
        self.0.arenas = arenas;
        self.0.envelope = envelope;
        self.0
    }

    /// Narrow a whole record to its core. **Safe by direction**: this can only REMOVE information
    /// (`arenas`, `live_children`, `envelope`), never fabricate it, so it cannot manufacture the
    /// "looks whole but is not" value `CoreRecord` exists to make unrepresentable. It is here for
    /// the catalogs that hold whole records in memory (`LogBranchCatalog`, `MemBranchCatalog`) and
    /// must satisfy a trait method whose answer is core-only.
    pub fn narrow(rec: &BranchRecord) -> CoreRecord {
        let mut core = rec.clone();
        core.arenas = Vec::new();
        core.live_children = Vec::new();
        core.envelope = None;
        CoreRecord(core)
    }

    pub fn branch_id(&self) -> BranchId {
        self.0.branch_id
    }
    pub fn generation(&self) -> u32 {
        self.0.generation
    }
    pub fn state(&self) -> BranchState {
        self.0.state
    }
    pub fn lease_deadline(&self) -> LeaseDeadline {
        self.0.lease_deadline
    }
    pub fn depth(&self) -> u8 {
        self.0.depth
    }
    pub fn fork_epoch(&self) -> Epoch {
        self.0.fork_epoch
    }
    pub fn root_page_id(&self) -> PageId {
        self.0.root_page_id
    }
    /// Safe on a core record: it reads `generation` and `state`, and neither is an unbounded field.
    pub fn check_readable(&self, requested: BranchId) -> Result<(), BranchError> {
        self.0.check_readable(requested)
    }
    /// Re-encoding the core needs no unbounded field by definition, so this is exact.
    pub fn serialize_core(&self) -> Vec<u8> {
        self.0.serialize_core()
    }
}

impl BranchRecord {
    /// The trunk record. Never reaped, no parent, depth 0.
    pub fn trunk(root_page_id: PageId, lease_deadline: LeaseDeadline) -> Self {
        BranchRecord {
            branch_id: BranchId::TRUNK,
            generation: 0,
            parent_id: None,
            fork_epoch: Epoch::ZERO,
            root_page_id,
            lease_deadline,
            state: BranchState::Live,
            arenas: Vec::new(),
            live_children: Vec::new(),
            depth: 0,
            envelope: None,
        }
    }

    /// Build the child record for a fork. Does **not** mutate the parent — the caller must also
    /// call [`BranchRecord::add_live_child`] on the parent and durably record both.
    pub fn fork_child(
        parent: &BranchRecord,
        child_id: BranchId,
        fork_epoch: Epoch,
        lease_deadline: LeaseDeadline,
    ) -> Result<Self, BranchError> {
        Self::fork_child_parts(
            parent.branch_id,
            parent.state,
            parent.depth,
            parent.root_page_id,
            parent.envelope.as_ref(),
            child_id,
            fork_epoch,
            lease_deadline,
        )
    }

    /// Fork from a parent read as a [`CoreRecord`] plus its envelope, with **no arena scan**.
    ///
    /// `fork_child` takes a whole `BranchRecord`, and the only way to obtain one is `hydrate`,
    /// which range-scans the parent's entire arena span. `fork_child` never reads `arenas`, so
    /// every fork was paying for a vector it discarded — measured at ~42 ns per arena of the
    /// parent, x0.72 throughput at 2000 arenas (`bench/fork_parent_arena_scan.txt`). The type
    /// system pushed toward that scan, so the fix is this signature rather than a warning comment.
    ///
    /// It takes the envelope SEPARATELY rather than accepting a `BranchRecord` with empty `arenas`,
    /// which would tunnel under the guarantee `CoreRecord` exists to provide.
    #[allow(clippy::too_many_arguments)]
    pub fn fork_child_from_core(
        parent: &CoreRecord,
        parent_envelope: Option<&CapabilityEnvelope>,
        child_id: BranchId,
        fork_epoch: Epoch,
        lease_deadline: LeaseDeadline,
    ) -> Result<Self, BranchError> {
        Self::fork_child_parts(
            parent.branch_id(),
            parent.state(),
            parent.depth(),
            parent.root_page_id(),
            parent_envelope,
            child_id,
            fork_epoch,
            lease_deadline,
        )
    }

    /// The single body both entry points share, so the two can never drift — and drift here is a
    /// capability bug, since the envelope rule lives in it.
    #[allow(clippy::too_many_arguments)]
    fn fork_child_parts(
        parent_branch_id: BranchId,
        parent_state: BranchState,
        parent_depth: u8,
        parent_root_page_id: PageId,
        parent_envelope: Option<&CapabilityEnvelope>,
        child_id: BranchId,
        fork_epoch: Epoch,
        lease_deadline: LeaseDeadline,
    ) -> Result<Self, BranchError> {
        if parent_state != BranchState::Live {
            return Err(BranchError::NotWritable(parent_branch_id));
        }
        let depth = parent_depth + 1;
        if depth > MAX_BRANCH_DEPTH {
            return Err(BranchError::DepthExceeded { branch: parent_branch_id, depth });
        }
        Ok(BranchRecord {
            branch_id: child_id,
            generation: child_id.generation,
            parent_id: Some(parent_branch_id),
            fork_epoch,
            // The whole fork: the child's root IS the parent's root.
            root_page_id: parent_root_page_id,
            lease_deadline,
            state: BranchState::Live,
            arenas: Vec::new(),
            live_children: Vec::new(),
            depth,
            // **Inherited, never re-granted.** A child forked out of a governed branch is governed
            // too, or the envelope would be one `BEGIN AGENT SESSION` away from irrelevant — every
            // agent session in this system runs on a forked child. The child's budget is the
            // parent's REMAINING budget, so forking cannot mint row-writes the parent had already
            // used. See `CapabilityEnvelope::inherited` for the residual limit that leaves.
            envelope: parent_envelope.map(CapabilityEnvelope::inherited),
        })
    }

    /// Insert a child's fork epoch into the sorted live-children array.
    pub fn add_live_child(&mut self, fork_epoch: Epoch) {
        let at = self.live_children.partition_point(|e| *e < fork_epoch);
        self.live_children.insert(at, fork_epoch);
    }

    /// Remove one occurrence of a child's fork epoch (called when that child is reaped).
    /// Returns true if an entry was removed.
    pub fn remove_live_child(&mut self, fork_epoch: Epoch) -> bool {
        let at = self.live_children.partition_point(|e| *e < fork_epoch);
        if at < self.live_children.len() && self.live_children[at] == fork_epoch {
            self.live_children.remove(at);
            true
        } else {
            false
        }
    }

    /// A childless leaf takes the reaper's fast path: free its arenas wholesale, no sharing
    /// analysis at all. This is the overwhelming majority of abandoned agent branches.
    pub fn is_childless_leaf(&self) -> bool {
        self.live_children.is_empty()
    }

    /// **The reclamation rule.** Page `p` is reclaimable iff no live child of this branch has
    /// `fork_epoch` in `[birth, freed)`.
    ///
    /// Correctness: a child forked at epoch `e` sees pages live at `e`; `p` was live over
    /// `[birth, freed)`; so `p` is visible to that child iff `e` falls in that interval.
    pub fn page_reclaimable(&self, birth: Epoch, freed: Epoch) -> bool {
        reclaimable(&self.live_children, birth, freed)
    }

    /// Ordinary reads/writes reject anything not `Live`, and reject a stale generation outright.
    pub fn check_readable(&self, requested: BranchId) -> Result<(), BranchError> {
        if requested.generation != self.generation || self.state == BranchState::Reaped {
            return Err(BranchError::Reaped {
                requested,
                current_generation: self.generation,
            });
        }
        match self.state {
            BranchState::Live => Ok(()),
            // Queryable by design. A quarantined branch is being held for inspection, and a hold
            // you cannot read is a deletion with extra steps.
            BranchState::Quarantined => Ok(()),
            BranchState::Reaping => Err(BranchError::Reaping(self.branch_id)),
            BranchState::Reaped => Err(BranchError::Reaped {
                requested,
                current_generation: self.generation,
            }),
        }
    }

    /// Mark reaped and bump the generation so the id slot can never be confused for the branch
    /// that used to live in it.
    pub fn mark_reaped(&mut self) {
        self.state = BranchState::Reaped;
        self.generation += 1;
        self.arenas.clear();
    }

    // ---- durable form ------------------------------------------------------------------
    //
    // |branch_id.id u64|branch_id.generation u32|generation u32|has_parent u8|
    // |parent.id u64|parent.generation u32|fork_epoch u64|root_page_id u32|
    // |lease_deadline u64|state u8|depth u8|arena_len u32|arenas..u32|
    // |child_len u32|children..u64|<envelope>|crc32 u32|
    // All integers big-endian, matching the rest of ferrodb's on-disk encodings.
    //
    // `<envelope>` is:
    //   |present u8| and, when present:
    //   |verbs u8|max_row_writes u64|row_writes u64|table_len u32|
    //     per table: |table u32|col_len u32| per column: |col u32|has_floor u8|floor i64|
    //
    // **No version byte, and none is needed.** The record was ALREADY variable-length — the arena
    // and child arrays vary — and `deserialize` computes `body_len` from `bytes.len()`, never from
    // a fixed layout. So a record written before the envelope existed simply runs out of body
    // before the envelope tag, and the read defaults the field. That is one write and one tolerant
    // read; `a_record_written_before_envelopes_existed_still_loads` pins it against bytes built
    // the old way.
    //
    // What a version byte WOULD buy is the ability to change the meaning of bytes already written.
    // Appending a field does not do that, so paying for it here would have been a migration for a
    // problem that does not exist.

    /// Bytes of the **core** record: everything that is not unbounded.
    ///
    /// The table catalog stores this, and keeps `arenas`, `live_children` and `envelope` in their
    /// own key spans. Not a space optimisation — a correctness one. A B+tree leaf holds about 2 KB
    /// of entries in total, and all three of those fields are unbounded: trunk's `live_children` at
    /// 10⁶ branches is 8 MB, and a branch that has written ~230 MB owns ~900 arena ids. A record
    /// that can outgrow a page is a wall with no error message.
    ///
    /// Fixed 51 bytes, no length prefixes, because every field is fixed-width. `CORE_BYTES` is
    /// asserted on the way back in, so a short or long buffer is a refusal rather than a record
    /// deserialized out of the next one's bytes.
    pub fn serialize_core(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(CORE_BYTES);
        b.extend_from_slice(&self.branch_id.id.to_be_bytes());
        b.extend_from_slice(&self.branch_id.generation.to_be_bytes());
        b.extend_from_slice(&self.generation.to_be_bytes());
        match self.parent_id {
            Some(p) => {
                b.push(1);
                b.extend_from_slice(&p.id.to_be_bytes());
                b.extend_from_slice(&p.generation.to_be_bytes());
            }
            None => {
                b.push(0);
                b.extend_from_slice(&0u64.to_be_bytes());
                b.extend_from_slice(&0u32.to_be_bytes());
            }
        }
        b.extend_from_slice(&self.fork_epoch.0.to_be_bytes());
        b.extend_from_slice(&self.root_page_id.to_be_bytes());
        b.extend_from_slice(&self.lease_deadline.0.to_be_bytes());
        b.push(self.state.as_u8());
        b.push(self.depth);
        debug_assert_eq!(b.len(), CORE_BYTES);
        b
    }

    /// Inverse of [`Self::serialize_core`]. The three unbounded fields come back **empty**, and
    /// that is deliberate: a caller that needs them asks the catalog, which answers from an index.
    /// Silently returning an empty `live_children` where the old record had a full one would be a
    /// wrong answer, so every caller of those fields was moved onto catalog queries first.
    /// Decode the stored core. Returns a [`CoreRecord`], **not** a `BranchRecord`: the bytes on
    /// disk do not contain `arenas` or `envelope`, so the value this produces is not a whole
    /// record and must not be usable as one. See [`CoreRecord`].
    pub fn deserialize_core(bytes: &[u8]) -> Result<CoreRecord, BranchError> {
        if bytes.len() != CORE_BYTES {
            return Err(BranchError::Corrupt(format!(
                "core branch record must be exactly {CORE_BYTES} bytes, got {}",
                bytes.len()
            )));
        }
        let u64_at = |i: usize| u64::from_be_bytes(bytes[i..i + 8].try_into().unwrap());
        let u32_at = |i: usize| u32::from_be_bytes(bytes[i..i + 4].try_into().unwrap());
        let branch_id = BranchId::new(u64_at(0), u32_at(8));
        let generation = u32_at(12);
        let parent_id =
            if bytes[16] == 1 { Some(BranchId::new(u64_at(17), u32_at(25))) } else { None };
        Ok(CoreRecord(BranchRecord {
            branch_id,
            generation,
            parent_id,
            fork_epoch: Epoch(u64_at(29)),
            root_page_id: u32_at(37),
            lease_deadline: LeaseDeadline(u64_at(41)),
            state: BranchState::from_u8(bytes[49])?,
            depth: bytes[50],
            arenas: Vec::new(),
            live_children: Vec::new(),
            envelope: None,
        }))
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut b: Vec<u8> = Vec::with_capacity(64 + self.arenas.len() * 4 + self.live_children.len() * 8);
        b.extend_from_slice(&self.branch_id.id.to_be_bytes());
        b.extend_from_slice(&self.branch_id.generation.to_be_bytes());
        b.extend_from_slice(&self.generation.to_be_bytes());
        match self.parent_id {
            Some(p) => {
                b.push(1);
                b.extend_from_slice(&p.id.to_be_bytes());
                b.extend_from_slice(&p.generation.to_be_bytes());
            }
            None => {
                b.push(0);
                b.extend_from_slice(&0u64.to_be_bytes());
                b.extend_from_slice(&0u32.to_be_bytes());
            }
        }
        b.extend_from_slice(&self.fork_epoch.0.to_be_bytes());
        b.extend_from_slice(&self.root_page_id.to_be_bytes());
        b.extend_from_slice(&self.lease_deadline.0.to_be_bytes());
        b.push(self.state.as_u8());
        b.push(self.depth);
        b.extend_from_slice(&(self.arenas.len() as u32).to_be_bytes());
        for a in &self.arenas {
            b.extend_from_slice(&a.0.to_be_bytes());
        }
        b.extend_from_slice(&(self.live_children.len() as u32).to_be_bytes());
        for c in &self.live_children {
            b.extend_from_slice(&c.0.to_be_bytes());
        }
        match &self.envelope {
            None => b.push(0),
            Some(e) => {
                b.push(1);
                b.extend_from_slice(&e.serialize());
            }
        }
        let crc = crc32(&b);
        b.extend_from_slice(&crc.to_be_bytes());
        b
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, BranchError> {
        let mut c = Cursor::new(bytes);
        let body_len = bytes
            .len()
            .checked_sub(4)
            .ok_or_else(|| BranchError::Corrupt("record shorter than its checksum".into()))?;
        let stored = u32::from_be_bytes(
            bytes[body_len..]
                .try_into()
                .map_err(|_| BranchError::Corrupt("truncated checksum".into()))?,
        );
        if crc32(&bytes[..body_len]) != stored {
            return Err(BranchError::Corrupt("branch record checksum mismatch".into()));
        }

        let id = c.u64()?;
        let gen_at_birth = c.u32()?;
        let generation = c.u32()?;
        let has_parent = c.u8()?;
        let p_id = c.u64()?;
        let p_gen = c.u32()?;
        let parent_id = if has_parent == 1 { Some(BranchId::new(p_id, p_gen)) } else { None };
        let fork_epoch = Epoch(c.u64()?);
        let root_page_id = c.u32()?;
        let lease_deadline = LeaseDeadline(c.u64()?);
        let state = BranchState::from_u8(c.u8()?)?;
        let depth = c.u8()?;
        let arena_len = c.u32()? as usize;
        let mut arenas = Vec::with_capacity(arena_len);
        for _ in 0..arena_len {
            arenas.push(ArenaId(c.u32()?));
        }
        let child_len = c.u32()? as usize;
        let mut live_children = Vec::with_capacity(child_len);
        for _ in 0..child_len {
            live_children.push(Epoch(c.u64()?));
        }

        // **The tolerant read.** Fewer bytes consumed than the body holds means this record was
        // written before the envelope field existed, so the field defaults instead of erroring.
        // The bound is `body_len`, NOT `bytes.len()`: the last four bytes are the checksum and
        // reading them as an envelope tag would turn every old record into a corrupt one.
        let envelope = if c.at >= body_len {
            None
        } else {
            match c.u8()? {
                0 => None,
                1 => Some(CapabilityEnvelope::deserialize_from(&mut c)?),
                other => {
                    return Err(BranchError::Corrupt(format!(
                        "unknown capability envelope tag {other}"
                    )))
                }
            }
        };
        if c.at != body_len {
            // Trailing bytes inside a body that checksums mean this record was written by
            // something whose format this build does not know. Refuse rather than act on the part
            // of it that happened to parse.
            return Err(BranchError::Corrupt(format!(
                "branch record has {} byte(s) after the last field this build understands",
                body_len - c.at
            )));
        }

        Ok(BranchRecord {
            branch_id: BranchId::new(id, gen_at_birth),
            generation,
            parent_id,
            fork_epoch,
            root_page_id,
            lease_deadline,
            state,
            arenas,
            live_children,
            depth,
            envelope,
        })
    }

    /// Narrow this branch's envelope. **Widening is refused**, which is what makes the envelope a
    /// capability rather than a suggestion: a governed branch cannot vote itself more authority.
    ///
    /// A branch with no envelope is ungoverned, so installing the first one is a narrowing from
    /// "everything" and is always accepted.
    pub fn restrict(&mut self, next: CapabilityEnvelope) -> Result<(), CapabilityRefusal> {
        if let Some(current) = &self.envelope {
            current.permits(&next)?;
        }
        self.envelope = Some(next);
        Ok(())
    }
}

// ---- the capability envelope -------------------------------------------------------------

/// A verb, derived from what a statement DID to a row, never from the keyword that produced it.
///
/// The derivation is the whole point. `Stmt::Delete` and `OpKind::RowDelete` are *shapes*, and a
/// check keyed on a shape is walked around by any other shape with the same effect — which is the
/// defect already recorded at the write funnel, where a bound keyed on `Add(negative)` let
/// `SET qty = -100` past as a plain `Assign`. Presence-before against presence-after cannot be
/// walked around, because it is the effect itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Verb {
    Insert,
    Update,
    Delete,
}

impl Verb {
    pub const INSERT: u8 = 1;
    pub const UPDATE: u8 = 2;
    pub const DELETE: u8 = 4;
    /// Every verb. Convenience for an envelope that governs tables and columns but not verbs.
    pub const ALL: u8 = Verb::INSERT | Verb::UPDATE | Verb::DELETE;

    pub const fn bit(self) -> u8 {
        match self {
            Verb::Insert => Verb::INSERT,
            Verb::Update => Verb::UPDATE,
            Verb::Delete => Verb::DELETE,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Verb::Insert => "INSERT",
            Verb::Update => "UPDATE",
            Verb::Delete => "DELETE",
        }
    }
}

/// What one statement did to one row, read off the before/after images alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowEffect {
    /// The row exists on both sides and every cell is identical: nothing was written, so there is
    /// nothing for the envelope to govern.
    Unchanged,
    Wrote(Verb),
}

/// One row's before and after images, as the write funnel already holds them.
///
/// `None` means the row does not exist on that side. Both sides are images, not ops: that is the
/// rule this whole type exists to enforce.
#[derive(Debug, Clone, Copy)]
pub struct RowImage<'a> {
    pub row: u64,
    pub before: Option<&'a [Value]>,
    pub after: Option<&'a [Value]>,
}

impl<'a> RowImage<'a> {
    /// The verb this row transition amounts to, given the columns it authored.
    ///
    /// Takes the change set rather than recomputing it: `admit` needs both for every row, and
    /// computing it twice was two scans and two allocations per row on the write funnel.
    pub fn effect_given(&self, changed: &[u32]) -> RowEffect {
        match (self.before, self.after) {
            (None, Some(_)) => RowEffect::Wrote(Verb::Insert),
            // A row that vanishes from nowhere is still a removal as far as authority goes.
            (Some(_), None) | (None, None) => RowEffect::Wrote(Verb::Delete),
            (Some(_), Some(_)) => {
                if changed.is_empty() {
                    RowEffect::Unchanged
                } else {
                    RowEffect::Wrote(Verb::Update)
                }
            }
        }
    }

    /// The verb this row transition amounts to. Convenience for callers that do not already hold
    /// the change set; `admit` uses [`RowImage::effect_given`] instead.
    pub fn effect(&self) -> RowEffect {
        self.effect_given(&changed_columns(self.before, self.after))
    }
}

/// Column indices this statement authored, read off the two images.
///
/// **A row that appears or disappears authors every one of its cells.** An INSERT creates the
/// whole row and a DELETE destroys the whole row, so both report every column, and the value in
/// the cell is irrelevant to that. Only an in-place UPDATE reports a subset — the cells that
/// actually differ.
///
/// **That is why an INSERT cannot slip past a column allowlist**: its `Op` carries `col: None`, so
/// a check keyed on the op's column would see an INSERT touch no column at all and wave through a
/// row that wrote every one of them.
///
/// This used to compare the absent side against a pad of `Null`, which was subtly wrong in one
/// direction and the reason it no longer does: a cell written as SQL `NULL` matched the pad and
/// reported as *unchanged*, so `INSERT INTO t VALUES (9, NULL)` wrote a column that was never
/// granted and planted a `NULL` in a floored cell — while `UPDATE t SET qty = NULL`, which reaches
/// the identical end state, was refused. A guard that disagrees with itself about the same end
/// state depending on which statement produced it is not a guard.
///
/// The consequence is deliberate and worth stating: **DELETE needs authority over every column of
/// the row**, because a row removal destroys every cell in it. Granting `Verb::DELETE` with a
/// narrow column list therefore refuses every delete. Grant the columns, or do not grant the verb.
/// `a_delete_needs_authority_over_every_column_it_destroys` pins both halves.
pub fn changed_columns(before: Option<&[Value]>, after: Option<&[Value]>) -> Vec<u32> {
    match (before, after) {
        // The row survives: only the cells that actually differ were authored.
        (Some(b), Some(a)) => {
            let n = b.len().max(a.len());
            let mut out = Vec::new();
            for i in 0..n {
                let lhs = b.get(i).unwrap_or(&Value::Null);
                let rhs = a.get(i).unwrap_or(&Value::Null);
                if !same_stored_value(lhs, rhs) {
                    out.push(i as u32);
                }
            }
            out
        }
        // The row appeared or vanished: every cell of the side that exists was authored.
        (None, Some(r)) | (Some(r), None) => (0..r.len() as u32).collect(),
        (None, None) => Vec::new(),
    }
}

/// Do these two cells hold the same **stored** value?
///
/// Deliberately stricter than `Value`'s own `PartialEq`, which compares numerically across the
/// whole numeric band: it reports `Integer(5) == Float(5.0)` and, because trailing zeros do not
/// change a number, `Decimal("1.50") == Decimal("1.5")`. Those are the right answers in a `WHERE`
/// clause and the wrong ones here. A statement that swaps a cell's stored representation HAS
/// written that cell, and calling it unchanged would leave a column allowlist with a walk-around:
/// rewrite the cell as an equal value of another type and no column is reported as touched.
///
/// It matters in this engine specifically because `Delta::apply` promotes an `Integer` cell to
/// `Float` on a float delta, so a representation change is something an ordinary write produces,
/// not a contrived one.
fn same_stored_value(a: &Value, b: &Value) -> bool {
    match (a, b) {
        // The digit text, not the number: `1.50` and `1.5` are the same value and different bytes,
        // and `Value::Decimal`'s own documentation says the trailing zero is significant to a
        // consumer reading a price.
        (Value::Decimal(x), Value::Decimal(y)) => x == y,
        _ => std::mem::discriminant(a) == std::mem::discriminant(b) && a == b,
    }
}

/// A column this branch may write, and the floor its value may not be driven below.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnCapability {
    pub col: u32,
    /// Lowest value the branch may leave in this cell, or `None` for no bound.
    ///
    /// Checked against the **after-image**: whatever arithmetic, assignment or op shape produced
    /// the value, the value is what is compared. An `Assign` straight through the floor is refused
    /// by exactly the same line that refuses a decrement, because neither one is consulted.
    pub floor: Option<i64>,
}

impl ColumnCapability {
    /// Writable with no lower bound.
    pub fn open(col: u32) -> Self {
        ColumnCapability { col, floor: None }
    }

    /// Writable, but never below `floor`.
    pub fn floored(col: u32, floor: i64) -> Self {
        ColumnCapability { col, floor: Some(floor) }
    }

    /// Is `other` no wider than this? A floor may be raised, never lowered or dropped.
    fn permits(&self, other: &ColumnCapability) -> bool {
        match (self.floor, other.floor) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(mine), Some(theirs)) => theirs >= mine,
        }
    }
}

/// One table a branch may write, and which of its columns.
///
/// **`table` is a 32-bit FNV-1a of the table name** (`agent_sql::runtime::table_id`), and no name
/// is stored. That is the identity the rest of this system already keys on — workspaces, escrow
/// cells, every `Op` — so using anything else here would make the envelope disagree with the
/// funnel it guards. Stated because the consequence is different for a capability than for a
/// lookup key: two table names that collide share one allowlist entry, and the refusal path cannot
/// tell them apart, because the name in the message text comes from the caller and not from the
/// record. At the birthday bound that is around 77k tables in one database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCapability {
    pub table: u32,
    /// Sorted by column index. A column that is absent may not be written: default-deny.
    ///
    /// Private because [`TableCapability::column`] binary-searches it: an unsorted list makes a
    /// granted column resolve to "not on the allowlist". [`TableCapability::new`] is the only way
    /// to build one, and it sorts.
    columns: Vec<ColumnCapability>,
}

impl TableCapability {
    /// Build a table capability, normalising the column list so the encoding is canonical and a
    /// duplicate cannot loosen a floor: the tighter of two entries for one column wins.
    pub fn new(table: u32, mut columns: Vec<ColumnCapability>) -> Self {
        columns.sort_by_key(|c| (c.col, std::cmp::Reverse(c.floor)));
        columns.dedup_by_key(|c| c.col);
        TableCapability { table, columns }
    }

    pub fn columns(&self) -> &[ColumnCapability] {
        &self.columns
    }

    fn column(&self, col: u32) -> Option<&ColumnCapability> {
        self.columns.binary_search_by_key(&col, |c| c.col).ok().map(|i| &self.columns[i])
    }
}

/// A refusal by the envelope, carrying the reason an operator has to act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityRefusal(pub String);

impl std::fmt::Display for CapabilityRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "capability envelope: {}", self.0)
    }
}

impl std::error::Error for CapabilityRefusal {}

impl From<CapabilityRefusal> for FerroError {
    fn from(e: CapabilityRefusal) -> Self {
        FerroError::Constraint(e.to_string())
    }
}

/// **What one branch is permitted to write**, stored in that branch's own durable record.
///
/// # Default-deny, and what it is default-deny *about*
///
/// Once an envelope exists on a branch, every dimension of it is an allowlist: a table that is not
/// listed cannot be written, a column that is not listed cannot be written, a verb whose bit is
/// clear cannot be performed, and a row-write past the budget is refused. Nothing falls through to
/// allow, including a value the floor check cannot compare — a bound it cannot evaluate is a bound
/// that refuses, not one that waves the write past.
///
/// # What it does not govern, and this is the load-bearing sentence
///
/// "Cannot be written" is a claim about ONE funnel — `AgentRuntime::stage_all` — and it is exactly
/// as wide as that funnel and no wider. A statement that does not reach `stage_all` is not governed
/// at all, and there are **three** tiers of those. (This sentence has said "one" and then "two";
/// each time a review found another tier. Treat the number as the current floor, not a total.)
///
/// - **The agent verbs**, diverted by `is_agent_stmt` above everything else. Two of them write the
///   ROWS of a forbidden table: `REVERT MERGE ... CASCADE` replays a previous merge's writes
///   backwards, and `MERGE BRANCH <other>` publishes a *different* branch's private workspace. Both
///   return `Ok` and charge nothing on a branch whose envelope forbids the table. This is the
///   sharper half of the gap — shared *content*, not structure — and it is the half an earlier
///   version of this paragraph omitted entirely. Pinned by
///   `merge_and_revert_rewrite_a_forbidden_tables_rows_and_this_is_a_known_gap`.
/// - **The DDL that reaches the executor's `match`**: `CREATE INDEX`, `CREATE FULLTEXT INDEX`,
///   `CREATE TABLE`, `DROP TABLE` and `ANALYZE` all fall through to the **shared catalog**, with no
///   branch and no `MERGE`, so a branch that may not write one row of a table can still index it,
///   read every row of it into a full-text index, and drop it. (`ANALYZE` changes no schema and
///   nothing durable — it writes the in-memory `Catalog::stats` and never calls `persist`; it is
///   here because it is ungoverned, not because it is DDL.) `BEGIN` / `COMMIT` / `ROLLBACK` are
///   admitted too, though no row can land through them. Pinned by
///   `no_ddl_verb_is_governed_by_the_envelope_and_this_is_a_known_gap`, which drives each verb
///   against a table the envelope refuses, or — for `CREATE TABLE` — against a table it never
///   granted.
///
/// And the DDL does not only escape the envelope: `DROP TABLE` + `CREATE TABLE` **widens** it,
/// which `restrict` exists to make impossible. Table identity is the name and a column's is its
/// index, so rebuilding a granted table detaches a floor from the column it guarded and can turn a
/// standing refusal into a permission, without the envelope bytes ever changing. Pinned by
/// `dropping_and_recreating_a_granted_table_repoints_the_grant_at_different_columns` and
/// `the_substitution_strips_a_column_floor_and_unlocks_a_refused_delete`.
///
/// **None of that is a read escalation, because there is nothing to escalate.** The envelope has no
/// read dimension at all — [`Verb`] is `Insert | Update | Delete`, [`CapabilityEnvelope::admit`]
/// decides on row images, and it is consulted on the write path only. A governed branch can already
/// `SELECT` every row of a table it may not write. What the ungoverned DDL adds is authority over
/// shared *structure*, and, through `DROP TABLE` + `CREATE TABLE`, authority over what the
/// branch's own grant points at — see
/// `dropping_and_recreating_a_granted_table_repoints_the_grant_at_different_columns`.
///
/// - **The branch-scoped schema path.** `ALTER TABLE` inside an agent session is diverted at
///   `src/execution/executor.rs:216` to `run_agent_alter`, which calls `stage_schema_edit` — so it
///   reaches a branch's *own* staged state without passing `stage_all`. It is neither of the two
///   tiers above: not an agent verb, and it never reaches the executor's `match`. Detailed
///   immediately below, because it is also what falsified the single-funnel premise.
///
/// **And `stage_all` is no longer the only way into a branch's own staged state.** That was a
/// premise, not a guarantee, and B11's branch-scoped `ALTER TABLE` — merged at `398e361` — breaks
/// it: `AgentRuntime::stage_schema_edit` reaches the workspace directly, consulting no envelope and
/// charging no budget. A branch granted only `inventory` can `ADD`, `RENAME` and `RETYPE` the
/// columns of `payroll`, and `MERGE` publishes every one of those edits into the shared catalog.
/// Driven as SQL by `branch_scoped_alter_table_reaches_a_forbidden_table_and_this_is_a_known_gap`;
/// the funnel count itself is pinned by `the_envelope_reads_one_funnel_while_three_reach_branch_state`,
/// which is where the number to watch lives — one site reads the envelope, three reach branch state.
///
/// (The predecessor of those two was a text check written while `ALTER` did not exist in this tree.
/// It asserted the single-funnel premise and fired on the merge that falsified it, which is what it
/// was for.)
///
/// All of these are recorded gaps rather than oversights — but read them before reading the
/// paragraph above as "a session cannot touch what it was not granted", because that is not what it
/// says.
///
/// A branch with **no** envelope (`BranchRecord::envelope == None`) is ungoverned. That is stated
/// rather than implied, and `an_ungoverned_branch_writes_exactly_as_before` asserts it. It is the
/// compatibility default for two reasons: a record written before this field existed has to load
/// as *something*, and every branch in this database predates it. Making absence mean "deny
/// everything" would have turned one added field into a database that refuses every write it used
/// to accept. Governance starts where an operator installs an envelope, and
/// [`BranchRecord::fork_child`] carries it to every child from there, so installing one on trunk
/// governs every agent session forked out of it.
///
/// # Everything here is decided on the after-image
///
/// The verb comes from whether the row exists on each side; the column set comes from which cells
/// differ; the floor compares the value that would be left behind. No branch of this predicate
/// reads an `OpKind`, a `Stmt`, or the SQL text. That is deliberate, and it is the rule the write
/// funnel already learned the hard way — see the comment on `AgentRuntime::stage_all`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
///
/// # Why the fields are private
///
/// `table()` and `column()` binary-search their arrays, so an envelope whose arrays are not sorted
/// resolves a granted table to "not on the allowlist" and a granted column to the same. That
/// invariant used to be maintained only by `new`/`allow` while every field was `pub` and
/// `deserialize` rebuilt the structs raw — so a struct literal, or any record read off disk, could
/// carry an envelope that silently mis-resolved. Making the state unrepresentable is cheaper than
/// documenting it: the constructors are the only way in, and they sort.
pub struct CapabilityEnvelope {
    /// Bitmask of [`Verb`] bits.
    verbs: u8,
    /// Sorted by table id. A table that is absent may not be written: default-deny.
    tables: Vec<TableCapability>,
    /// How many row-writes this branch may perform over its whole life.
    ///
    /// A "row-write" is one row whose image a statement changed. Re-writing the same row in a
    /// later statement costs another one — this is a budget on writes, not a count of distinct
    /// rows, and it is named that way so nobody reads it as the latter.
    max_row_writes: u64,
    /// How many it has already spent. **Durable**, so a restart does not hand the budget back.
    row_writes: u64,
}

impl CapabilityEnvelope {
    /// An envelope granting `verbs` and `max_row_writes`, and no table at all until one is added.
    pub fn new(verbs: u8, max_row_writes: u64) -> Self {
        CapabilityEnvelope { verbs, tables: Vec::new(), max_row_writes, row_writes: 0 }
    }

    /// Add one table's capability, keeping the table list sorted and unique. A repeated table
    /// replaces the earlier entry rather than shadowing it, so the encoding stays canonical.
    pub fn allow(mut self, table: u32, columns: Vec<ColumnCapability>) -> Self {
        let cap = TableCapability::new(table, columns);
        match self.tables.binary_search_by_key(&table, |t| t.table) {
            Ok(i) => self.tables[i] = cap,
            Err(i) => self.tables.insert(i, cap),
        }
        self
    }

    pub fn table(&self, table: u32) -> Option<&TableCapability> {
        self.tables.binary_search_by_key(&table, |t| t.table).ok().map(|i| &self.tables[i])
    }

    pub fn verbs(&self) -> u8 {
        self.verbs
    }

    pub fn tables(&self) -> &[TableCapability] {
        &self.tables
    }

    pub fn max_row_writes(&self) -> u64 {
        self.max_row_writes
    }

    /// Row-writes already spent.
    pub fn row_writes(&self) -> u64 {
        self.row_writes
    }

    /// Spend `n` row-writes, refusing if they do not fit.
    ///
    /// Re-checks rather than trusting [`CapabilityEnvelope::admit`]'s answer, because the two are
    /// not one atomic step: `admit` decides against the envelope a statement read, and something
    /// else may have charged against the same branch in between. The check that must hold is the
    /// one at the moment of the charge.
    pub fn charge(&mut self, n: u64) -> Result<(), CapabilityRefusal> {
        if n > self.remaining() {
            return Err(CapabilityRefusal(format!(
                "charging {n} row-write(s) would exceed the branch's remaining budget of {}",
                self.remaining()
            )));
        }
        self.row_writes += n;
        Ok(())
    }

    /// Row-writes still available.
    /// The envelope's bytes, **without** the presence tag that `BranchRecord` writes before them.
    ///
    /// Extracted from `BranchRecord::serialize` rather than written a second time: the table
    /// catalog stores envelopes under their own key (`tree_keys::ENVELOPE`) because they are
    /// variable-length and `envelope_of` is already a separate query, and two encoders for one
    /// format drift the first time either is touched.
    pub fn serialize(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.push(self.verbs);
        b.extend_from_slice(&self.max_row_writes.to_be_bytes());
        b.extend_from_slice(&self.row_writes.to_be_bytes());
        b.extend_from_slice(&(self.tables.len() as u32).to_be_bytes());
        for t in &self.tables {
            b.extend_from_slice(&t.table.to_be_bytes());
            b.extend_from_slice(&(t.columns.len() as u32).to_be_bytes());
            for c in &t.columns {
                b.extend_from_slice(&c.col.to_be_bytes());
                match c.floor {
                    None => {
                        b.push(0);
                        b.extend_from_slice(&0i64.to_be_bytes());
                    }
                    Some(f) => {
                        b.push(1);
                        b.extend_from_slice(&f.to_be_bytes());
                    }
                }
            }
        }
        b
    }

    /// Inverse of [`Self::serialize`], reading from a standalone buffer.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, BranchError> {
        let mut c = Cursor::new(bytes);
        Self::deserialize_from(&mut c)
    }

    fn deserialize_from(c: &mut Cursor<'_>) -> Result<Self, BranchError> {
        let verbs = c.u8()?;
        let max_row_writes = c.u64()?;
        let row_writes = c.u64()?;
        let table_len = c.u32()? as usize;
        let mut tables = Vec::new();
        for _ in 0..table_len {
            let table = c.u32()?;
            let col_len = c.u32()? as usize;
            let mut columns = Vec::new();
            for _ in 0..col_len {
                let col = c.u32()?;
                // Anything but 0 or 1 is refused, not read as "no floor". A tag this read does not
                // understand turning a floored column into an unbounded one is the one direction
                // this type forbids: a bound it cannot evaluate refuses, it does not wave the
                // write past.
                let floor = match c.u8()? {
                    0 => {
                        c.i64()?;
                        None
                    }
                    1 => Some(c.i64()?),
                    other => {
                        return Err(BranchError::Corrupt(format!(
                            "unknown capability floor tag {other} on table {table} column {col}"
                        )))
                    }
                };
                columns.push(ColumnCapability { col, floor });
            }
            tables.push((table, columns));
        }
        // Built through `allow`, which sorts and deduplicates, so an envelope read off disk carries
        // the same invariant as one built in process. Reconstructing the struct raw is what let a
        // non-canonical record mis-resolve a table it had actually been granted.
        let mut e = CapabilityEnvelope::new(verbs, max_row_writes);
        for (table, columns) in tables {
            e = e.allow(table, columns);
        }
        e.row_writes = row_writes;
        Ok(e)
    }

    pub fn remaining(&self) -> u64 {
        self.max_row_writes.saturating_sub(self.row_writes)
    }

    /// The envelope a child inherits at fork: the same authority, with the parent's REMAINING
    /// budget as its own ceiling and a fresh counter.
    ///
    /// **Stated limit.** The budget is per-branch, so a governed branch that forks `k` children
    /// puts `k` copies of its remaining budget into the world. Closing that needs a shared pool
    /// across a branch family — the shape `EscrowLedger` already has for cells — and that is a
    /// design decision about what a family-wide quota means, not an implementation detail.
    /// `forking_does_not_mint_budget_the_parent_had_already_spent` pins what IS true here.
    pub fn inherited(&self) -> Self {
        CapabilityEnvelope {
            verbs: self.verbs,
            tables: self.tables.clone(),
            max_row_writes: self.remaining(),
            row_writes: 0,
        }
    }

    pub fn allows_verb(&self, verb: Verb) -> bool {
        self.verbs & verb.bit() != 0
    }

    /// Is `other` no wider than `self` in every dimension? Used by [`BranchRecord::restrict`] so a
    /// governed branch cannot vote itself more authority.
    pub fn permits(&self, other: &CapabilityEnvelope) -> Result<(), CapabilityRefusal> {
        let extra = other.verbs & !self.verbs;
        if extra != 0 {
            let names: Vec<&str> = [Verb::Insert, Verb::Update, Verb::Delete]
                .into_iter()
                .filter(|v| extra & v.bit() != 0)
                .map(|v| v.name())
                .collect();
            return Err(CapabilityRefusal(format!(
                "refusing to widen this branch's authority with verb(s) {}; an envelope may only \
                 be narrowed",
                names.join(", ")
            )));
        }
        for t in &other.tables {
            let Some(mine) = self.table(t.table) else {
                return Err(CapabilityRefusal(format!(
                    "refusing to widen this branch's authority to table {}; an envelope may only \
                     be narrowed",
                    t.table
                )));
            };
            for c in &t.columns {
                match mine.column(c.col) {
                    Some(m) if m.permits(c) => {}
                    Some(_) => {
                        return Err(CapabilityRefusal(format!(
                            "refusing to lower or drop the floor on table {} column {}; an \
                             envelope may only be narrowed",
                            t.table, c.col
                        )))
                    }
                    None => {
                        return Err(CapabilityRefusal(format!(
                            "refusing to widen this branch's authority to table {} column {}; an \
                             envelope may only be narrowed",
                            t.table, c.col
                        )))
                    }
                }
            }
        }
        if other.remaining() > self.remaining() {
            return Err(CapabilityRefusal(format!(
                "refusing to raise this branch's row-write budget from {} remaining to {}; an \
                 envelope may only be narrowed",
                self.remaining(),
                other.remaining()
            )));
        }
        Ok(())
    }

    /// **Admit or refuse one statement's worth of row images, as a batch.**
    ///
    /// Returns the row-writes to charge, and charges nothing itself: the caller applies the charge
    /// only once the whole statement has been admitted. Deciding for every row before recording
    /// any of it is the same rule the escrow batch check follows at this funnel, and for the same
    /// reason — a refusal on row two must not leave row one written.
    ///
    /// `table_name` appears only in the refusal text; `table` is what is matched.
    pub fn admit(
        &self,
        table: u32,
        table_name: &str,
        rows: &[RowImage<'_>],
    ) -> Result<u64, CapabilityRefusal> {
        // Default-deny at the table level, evaluated even when the statement matched no row: a
        // branch with no authority over a table has none whether or not the WHERE happened to
        // select anything.
        let Some(cap) = self.table(table) else {
            return Err(CapabilityRefusal(format!(
                "branch may not write table `{table_name}`: it is not on the allowlist \
                 ({} table(s) allowed)",
                self.tables.len()
            )));
        };

        let mut charged = 0u64;
        for image in rows {
            let changed = changed_columns(image.before, image.after);
            let RowEffect::Wrote(verb) = image.effect_given(&changed) else {
                // The images are identical, so this statement wrote nothing here. Governing a
                // non-write would charge budget for a no-op and refuse a statement that changed
                // nothing.
                continue;
            };
            if !self.allows_verb(verb) {
                return Err(CapabilityRefusal(format!(
                    "branch may not {} in `{table_name}` (row {}): the verb is not on the \
                     allowlist",
                    verb.name(),
                    image.row
                )));
            }
            for col in changed {
                let Some(column) = cap.column(col) else {
                    return Err(CapabilityRefusal(format!(
                        "branch may not write `{table_name}` column {col} (row {}): the column is \
                         not on the allowlist",
                        image.row
                    )));
                };
                let Some(floor) = column.floor else {
                    continue;
                };
                let after = image.after.and_then(|r| r.get(col as usize));
                if !at_or_above(after, floor) {
                    return Err(CapabilityRefusal(format!(
                        "branch may not leave `{table_name}` column {col} (row {}) at {} — the \
                         floor is {floor}",
                        image.row,
                        match after {
                            Some(v) => format!("{v:?}"),
                            None => "no value (the row was removed)".to_string(),
                        }
                    )));
                }
            }
            charged += 1;
        }

        if charged > self.remaining() {
            return Err(CapabilityRefusal(format!(
                "this statement writes {charged} row(s) in `{table_name}` but the branch has {} \
                 of its {} row-write budget left",
                self.remaining(),
                self.max_row_writes
            )));
        }
        Ok(charged)
    }
}

/// Is the value this write would leave behind at or above `floor`?
///
/// Comparison goes through `Value`'s own `Ord`, which compares the whole numeric band — `Integer`,
/// `BigInt`, `Float`, `Decimal` — **by value and exactly**, rather than widening anything into
/// anything else. Reimplementing it here is how a floor check ends up disagreeing with the guard
/// evaluator about whether `Float(2.0) > Integer(5)`; that exact bug is written up on `impl Ord for
/// Value` and it silently defeated every numeric guard while it lasted.
///
/// Everything outside that band refuses: a missing cell, `Null`, a boolean, a string, a timestamp,
/// and a non-finite float. A bound that cannot evaluate its input asks nothing and allows nothing —
/// `total_cmp` places `+NaN` above every real number, so falling through to the comparison would
/// have made `NaN` clear every floor in the database.
fn at_or_above(after: Option<&Value>, floor: i64) -> bool {
    match after {
        Some(Value::Integer(_)) | Some(Value::BigInt(_)) | Some(Value::Decimal(_)) => {
            *after.unwrap() >= Value::BigInt(floor)
        }
        Some(Value::Float(f)) if f.is_finite() => Value::Float(*f) >= Value::BigInt(floor),
        _ => false,
    }
}

/// Free-function form of the reclamation rule, so the GC path can evaluate it without holding a
/// whole `BranchRecord`.
///
/// `live_children` must be sorted ascending. Returns true iff the half-open interval
/// `[birth, freed)` contains no live child fork epoch. An empty or inverted interval is vacuously
/// reclaimable.
pub fn reclaimable(live_children: &[Epoch], birth: Epoch, freed: Epoch) -> bool {
    if freed <= birth {
        return true;
    }
    let lo = live_children.partition_point(|e| *e < birth);
    // first index with e >= freed
    let hi = live_children.partition_point(|e| *e < freed);
    lo == hi
}

/// A page that has been logically freed but must wait for the reclamation rule to clear it.
/// Slow-path reaping (branch had live children) parks entries here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingFree {
    pub page_id: PageId,
    pub arena_id: ArenaId,
    pub birth_epoch: Epoch,
    pub free_epoch: Epoch,
    /// The branch whose `live_children` array decides this entry.
    pub owner: BranchId,
}

/// One contiguous private extent. Reaping a childless leaf frees these whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArenaExtent {
    pub arena_id: ArenaId,
    pub owner: BranchId,
    pub start_page: PageId,
    pub page_count: u32,
    /// Next unallocated page within the extent, as an offset from `start_page`.
    pub next_free: u32,
}

impl ArenaExtent {
    pub fn remaining(&self) -> u32 {
        self.page_count.saturating_sub(self.next_free)
    }

    pub fn contains(&self, page_id: PageId) -> bool {
        page_id >= self.start_page && page_id < self.start_page + self.page_count
    }
}

struct Cursor<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(b: &'a [u8]) -> Self {
        Cursor { b, at: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], BranchError> {
        if self.at + n > self.b.len() {
            return Err(BranchError::Corrupt(format!(
                "branch record truncated at byte {} (wanted {})",
                self.at, n
            )));
        }
        let s = &self.b[self.at..self.at + n];
        self.at += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, BranchError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, BranchError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, BranchError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64, BranchError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_copies_the_root_pointer_and_nothing_else() {
        let parent = BranchRecord::trunk(42, LeaseDeadline(0));
        let child = BranchRecord::fork_child(
            &parent,
            BranchId::new(1, 0),
            Epoch(10),
            LeaseDeadline(9_999),
        )
        .unwrap();
        assert_eq!(child.root_page_id, parent.root_page_id);
        assert_eq!(child.parent_id, Some(BranchId::TRUNK));
        assert_eq!(child.depth, 1);
        assert!(child.arenas.is_empty());
    }

    #[test]
    fn depth_guard_fires_at_eight() {
        let mut r = BranchRecord::trunk(1, LeaseDeadline(0));
        r.depth = MAX_BRANCH_DEPTH;
        let err = BranchRecord::fork_child(&r, BranchId::new(2, 0), Epoch(1), LeaseDeadline(0));
        assert!(matches!(err, Err(BranchError::DepthExceeded { .. })));
    }

    #[test]
    fn live_children_stay_sorted() {
        let mut r = BranchRecord::trunk(1, LeaseDeadline(0));
        for e in [Epoch(50), Epoch(10), Epoch(30), Epoch(20)] {
            r.add_live_child(e);
        }
        assert_eq!(r.live_children, vec![Epoch(10), Epoch(20), Epoch(30), Epoch(50)]);
        assert!(r.remove_live_child(Epoch(30)));
        assert!(!r.remove_live_child(Epoch(30)));
        assert_eq!(r.live_children, vec![Epoch(10), Epoch(20), Epoch(50)]);
    }

    #[test]
    fn reclamation_rule_is_half_open_over_the_fork_epochs() {
        let children = vec![Epoch(10), Epoch(20), Epoch(30)];
        // no child forked inside [1, 5) -> reclaimable
        assert!(reclaimable(&children, Epoch(1), Epoch(5)));
        // child at 20 sits inside [15, 25) -> pinned
        assert!(!reclaimable(&children, Epoch(15), Epoch(25)));
        // birth exactly at a fork epoch: that child sees the page -> pinned
        assert!(!reclaimable(&children, Epoch(20), Epoch(21)));
        // free exactly at a fork epoch: page was already dead at that epoch -> reclaimable
        assert!(reclaimable(&children, Epoch(11), Epoch(20)));
        // no live children at all -> always reclaimable
        assert!(reclaimable(&[], Epoch(0), Epoch(u64::MAX)));
    }

    #[test]
    fn stale_generation_is_a_hard_error_not_stale_data() {
        let mut r = BranchRecord::trunk(1, LeaseDeadline(0));
        r.branch_id = BranchId::new(5, 0);
        r.generation = 0;
        assert!(r.check_readable(BranchId::new(5, 0)).is_ok());
        r.mark_reaped();
        let err = r.check_readable(BranchId::new(5, 0)).unwrap_err();
        assert!(matches!(err, BranchError::Reaped { current_generation: 1, .. }));
    }

    #[test]
    fn record_roundtrips_through_bytes() {
        let mut r = BranchRecord::trunk(77, LeaseDeadline(123_456));
        r.branch_id = BranchId::new(9, 3);
        r.generation = 3;
        r.parent_id = Some(BranchId::new(2, 1));
        r.fork_epoch = Epoch(4242);
        r.depth = 4;
        r.state = BranchState::Reaping;
        r.arenas = vec![ArenaId(1), ArenaId(9)];
        r.live_children = vec![Epoch(1), Epoch(2), Epoch(3)];
        let bytes = r.serialize();
        assert_eq!(BranchRecord::deserialize(&bytes).unwrap(), r);
    }

    #[test]
    fn corrupt_record_is_rejected_not_guessed() {
        let r = BranchRecord::trunk(77, LeaseDeadline(1));
        let mut bytes = r.serialize();
        bytes[0] ^= 0xff;
        assert!(matches!(BranchRecord::deserialize(&bytes), Err(BranchError::Corrupt(_))));
    }

    // ---- capability envelope ----------------------------------------------------------------

    const T: u32 = 7;

    fn full_envelope() -> CapabilityEnvelope {
        CapabilityEnvelope::new(Verb::ALL, 100).allow(
            T,
            vec![ColumnCapability::open(0), ColumnCapability::floored(1, 0)],
        )
    }

    fn row(vals: &[Value]) -> Vec<Value> {
        vals.to_vec()
    }

    /// **Breaking shape: a record serialized before this field existed.** Its body ends after the
    /// live-children array, so a reader that demands an envelope tag would reject every branch
    /// already on disk. Built by hand rather than by calling the current `serialize`, because a
    /// round-trip through the code under test cannot tell you what the OLD code wrote.
    fn serialize_pre_envelope(r: &BranchRecord) -> Vec<u8> {
        let mut b: Vec<u8> = Vec::new();
        b.extend_from_slice(&r.branch_id.id.to_be_bytes());
        b.extend_from_slice(&r.branch_id.generation.to_be_bytes());
        b.extend_from_slice(&r.generation.to_be_bytes());
        match r.parent_id {
            Some(p) => {
                b.push(1);
                b.extend_from_slice(&p.id.to_be_bytes());
                b.extend_from_slice(&p.generation.to_be_bytes());
            }
            None => {
                b.push(0);
                b.extend_from_slice(&0u64.to_be_bytes());
                b.extend_from_slice(&0u32.to_be_bytes());
            }
        }
        b.extend_from_slice(&r.fork_epoch.0.to_be_bytes());
        b.extend_from_slice(&r.root_page_id.to_be_bytes());
        b.extend_from_slice(&r.lease_deadline.0.to_be_bytes());
        b.push(r.state.as_u8());
        b.push(r.depth);
        b.extend_from_slice(&(r.arenas.len() as u32).to_be_bytes());
        for a in &r.arenas {
            b.extend_from_slice(&a.0.to_be_bytes());
        }
        b.extend_from_slice(&(r.live_children.len() as u32).to_be_bytes());
        for c in &r.live_children {
            b.extend_from_slice(&c.0.to_be_bytes());
        }
        let crc = crc32(&b);
        b.extend_from_slice(&crc.to_be_bytes());
        b
    }

    #[test]
    fn a_record_written_before_envelopes_existed_still_loads() {
        let mut r = BranchRecord::trunk(12, LeaseDeadline(88));
        r.branch_id = BranchId::new(4, 1);
        r.generation = 1;
        r.parent_id = Some(BranchId::TRUNK);
        r.arenas = vec![ArenaId(3)];
        r.live_children = vec![Epoch(6), Epoch(9)];

        let old_bytes = serialize_pre_envelope(&r);
        assert!(
            old_bytes.len() < r.serialize().len(),
            "the pre-envelope encoding must be the SHORTER one, or this test is not exercising \
             the tolerant read at all"
        );
        let loaded = BranchRecord::deserialize(&old_bytes)
            .expect("a record written before the envelope field must still load");
        assert_eq!(loaded, r);
        assert_eq!(loaded.envelope, None, "an absent envelope must default, not error");
    }

    #[test]
    fn a_record_with_an_envelope_roundtrips_through_bytes() {
        let mut r = BranchRecord::trunk(77, LeaseDeadline(5));
        r.live_children = vec![Epoch(1)];
        r.arenas = vec![ArenaId(2), ArenaId(4)];
        let mut e = full_envelope().allow(9, vec![ColumnCapability::floored(3, -42)]);
        e.charge(17).unwrap();
        r.envelope = Some(e);
        let bytes = r.serialize();
        assert_eq!(BranchRecord::deserialize(&bytes).unwrap(), r);
    }

    /// The envelope sits AFTER the variable-length arrays, so a record with a long child list and
    /// a record with a short one must both find it. Getting the cursor arithmetic wrong here would
    /// read children as envelope bytes.
    #[test]
    fn the_envelope_survives_records_of_every_shape() {
        for children in [0usize, 1, 5, 40] {
            for arenas in [0usize, 3] {
                let mut r = BranchRecord::trunk(1, LeaseDeadline(0));
                r.live_children = (0..children as u64).map(Epoch).collect();
                r.arenas = (0..arenas as u32).map(ArenaId).collect();
                r.envelope = Some(full_envelope());
                let back = BranchRecord::deserialize(&r.serialize()).unwrap();
                assert_eq!(back, r, "{children} children / {arenas} arenas");
            }
        }
    }

    #[test]
    fn a_corrupt_envelope_is_rejected_not_defaulted() {
        let mut r = BranchRecord::trunk(1, LeaseDeadline(0));
        r.envelope = Some(full_envelope());
        let mut bytes = r.serialize();
        // Flip a byte inside the envelope region: the checksum covers it, so this must fail the
        // record rather than silently produce a different (wider) envelope.
        let at = bytes.len() - 10;
        bytes[at] ^= 0xff;
        assert!(matches!(BranchRecord::deserialize(&bytes), Err(BranchError::Corrupt(_))));
    }

    /// **Breaking shape: an INSERT.** Its `Op` carries `col: None`, so a column check keyed on the
    /// op would see it touch no column and wave through a row that wrote every one of them. A row
    /// that appears or disappears authors every one of its cells, whatever those cells hold.
    ///
    /// **Breaking shape, second: a cell written as SQL `NULL`.** This used to compare the absent
    /// side against a pad of `Null`, so `INSERT INTO t VALUES (9, NULL)` reported column 1 as
    /// unchanged and slipped it past both the allowlist and the floor — while
    /// `UPDATE t SET qty = NULL`, which reaches the identical end state, was refused. The third
    /// assertion below is that case.
    #[test]
    fn changed_columns_reads_images_so_an_insert_reports_every_column_it_wrote() {
        let after = row(&[Value::Integer(1), Value::Integer(2), Value::Null]);
        assert_eq!(changed_columns(None, Some(&after)), vec![0, 1, 2]);
        assert_eq!(changed_columns(Some(&after), None), vec![0, 1, 2], "a delete takes them away");

        // A NULL an INSERT wrote is a cell it authored, exactly as the same NULL written by an
        // UPDATE would be.
        let nulled = row(&[Value::Integer(1), Value::Null]);
        assert_eq!(changed_columns(None, Some(&nulled)), vec![0, 1]);
        let before = row(&[Value::Integer(1), Value::Integer(9)]);
        assert_eq!(
            changed_columns(Some(&before), Some(&nulled)),
            vec![1],
            "the UPDATE that reaches the same end state must report the same column"
        );

        // An in-place update still reports only what differs, or nothing at all.
        let b3 = row(&[Value::Integer(1), Value::Integer(9), Value::Null]);
        assert_eq!(changed_columns(Some(&b3), Some(&after)), vec![1]);
        assert!(changed_columns(Some(&b3), Some(&b3)).is_empty());
    }

    /// The consequence of "a row removal destroys every cell": DELETE needs authority over every
    /// column of the row. Stated as a deliberate rule with both halves, so it reads as a decision
    /// rather than as something nobody noticed.
    #[test]
    fn a_delete_needs_authority_over_every_column_it_destroys() {
        let before = row(&[Value::Integer(1), Value::Integer(20)]);
        let del = [RowImage { row: 1, before: Some(&before), after: None }];

        let narrow = CapabilityEnvelope::new(Verb::ALL, 10).allow(T, vec![ColumnCapability::open(1)]);
        let err = narrow.admit(T, "t", &del).expect_err("a delete destroyed an ungranted column");
        assert!(format!("{err}").contains("column 0"), "got {err}");

        // Anti-vacuity: grant every column and the same delete goes through.
        let wide = CapabilityEnvelope::new(Verb::ALL, 10)
            .allow(T, vec![ColumnCapability::open(0), ColumnCapability::open(1)]);
        assert_eq!(wide.admit(T, "t", &del).unwrap(), 1);
    }

    /// An INSERT whose value is NULL is still a write of that column, and a NULL is still below
    /// every floor. Both halves of the hole the `Null` pad opened.
    #[test]
    fn an_insert_cannot_launder_a_column_or_a_floor_through_a_null() {
        let e = CapabilityEnvelope::new(Verb::ALL, 10)
            .allow(T, vec![ColumnCapability::open(0), ColumnCapability::floored(1, 0)]);

        let nulled = row(&[Value::Integer(9), Value::Null]);
        let err = e
            .admit(T, "t", &[RowImage { row: 9, before: None, after: Some(&nulled) }])
            .expect_err("an INSERT planted a NULL in a floored cell");
        assert!(format!("{err}").contains("the floor is 0"), "got {err}");

        // And through the column allowlist: column 0 is not granted here.
        let narrow = CapabilityEnvelope::new(Verb::ALL, 10)
            .allow(T, vec![ColumnCapability::open(1)]);
        let err = narrow
            .admit(T, "t", &[RowImage { row: 9, before: None, after: Some(&nulled) }])
            .expect_err("an INSERT wrote an ungranted column by leaving it NULL");
        assert!(format!("{err}").contains("column 0"), "got {err}");

        // Anti-vacuity: a value at the floor inserts fine.
        let ok = row(&[Value::Integer(9), Value::Integer(0)]);
        assert_eq!(e.admit(T, "t", &[RowImage { row: 9, before: None, after: Some(&ok) }]).unwrap(), 1);
    }

    /// An envelope whose arrays arrived unsorted must not resolve a granted table to "not on the
    /// allowlist". The fields are private so a struct literal cannot build one; this checks the
    /// two doors that remain — `allow` in any order, and a record read back off disk.
    #[test]
    fn a_granted_table_resolves_however_the_envelope_was_assembled() {
        let e = CapabilityEnvelope::new(Verb::ALL, 10)
            .allow(90, vec![ColumnCapability::open(1), ColumnCapability::open(0)])
            .allow(7, vec![ColumnCapability::open(0)])
            .allow(40, vec![ColumnCapability::open(0)]);
        for t in [7u32, 40, 90] {
            assert!(e.table(t).is_some(), "table {t} was granted and did not resolve");
        }
        assert!(e.table(8).is_none(), "and an ungranted table still does not");

        let mut r = BranchRecord::trunk(1, LeaseDeadline(0));
        r.envelope = Some(e.clone());
        let back = BranchRecord::deserialize(&r.serialize()).unwrap();
        assert_eq!(back.envelope.as_ref().unwrap(), &e);
        for t in [7u32, 40, 90] {
            assert!(back.envelope.as_ref().unwrap().table(t).is_some(), "table {t} lost on reload");
        }
    }

    /// A floor tag this build does not understand must refuse the record, not read as "no floor".
    /// Dropping a floor is the one direction the envelope forbids.
    #[test]
    fn an_unknown_floor_tag_is_refused_rather_than_read_as_unbounded() {
        let mut r = BranchRecord::trunk(1, LeaseDeadline(0));
        r.envelope =
            Some(CapabilityEnvelope::new(Verb::ALL, 10).allow(T, vec![ColumnCapability::floored(1, 5)]));
        let bytes = r.serialize();

        // The floor tag is the 9th byte from the end of the body: |col u32|tag u8|floor i64|crc u32|
        let tag_at = bytes.len() - 4 - 8 - 1;
        assert_eq!(bytes[tag_at], 1, "the byte being corrupted is not the floor tag");
        let mut broken = bytes.clone();
        broken[tag_at] = 2;
        // Re-checksum, so this tests the TAG check and not the crc.
        let body = broken.len() - 4;
        let crc = crc32(&broken[..body]);
        broken[body..].copy_from_slice(&crc.to_be_bytes());

        let err = BranchRecord::deserialize(&broken).expect_err("an unknown floor tag was accepted");
        assert!(format!("{err}").contains("floor tag"), "got {err}");
        // Anti-vacuity: the untouched record still loads, floor intact.
        assert_eq!(BranchRecord::deserialize(&bytes).unwrap(), r);
    }

    /// Bytes after the last field this build understands mean the record came from something else.
    /// Refuse rather than act on the part that happened to parse.
    #[test]
    fn trailing_bytes_inside_a_valid_checksum_are_refused() {
        let mut r = BranchRecord::trunk(1, LeaseDeadline(0));
        r.envelope = Some(full_envelope());
        let bytes = r.serialize();
        let mut longer = bytes[..bytes.len() - 4].to_vec();
        longer.extend_from_slice(&[0xAB, 0xCD]);
        let crc = crc32(&longer);
        longer.extend_from_slice(&crc.to_be_bytes());

        let err = BranchRecord::deserialize(&longer).expect_err("trailing bytes were ignored");
        assert!(format!("{err}").contains("after the last field"), "got {err}");
        assert!(BranchRecord::deserialize(&bytes).is_ok(), "anti-vacuity: the real record loads");
    }

    /// A cell rewritten as a numerically equal value of another type HAS been written. `Value`'s
    /// own equality says otherwise, which would let a column allowlist be walked around by
    /// changing the representation instead of the number.
    #[test]
    fn a_representation_change_counts_as_writing_the_cell() {
        let int = row(&[Value::Integer(5)]);
        let float = row(&[Value::Float(5.0)]);
        assert_eq!(int[0], float[0], "the premise: Value's own equality calls these equal");
        assert_eq!(changed_columns(Some(&int), Some(&float)), vec![0]);

        let a = row(&[Value::Decimal("1.50".into())]);
        let b = row(&[Value::Decimal("1.5".into())]);
        assert_eq!(a[0], b[0], "the premise, again");
        assert_eq!(changed_columns(Some(&a), Some(&b)), vec![0]);

        // Anti-vacuity: an identical cell is still unchanged, so this is not "everything changed".
        assert!(changed_columns(Some(&int), Some(&int)).is_empty());
        assert!(changed_columns(Some(&a), Some(&a)).is_empty());
    }

    #[test]
    fn the_verb_comes_from_the_images_not_from_a_statement_keyword() {
        let a = row(&[Value::Integer(1)]);
        let b = row(&[Value::Integer(2)]);
        let img = |before, after| RowImage { row: 1, before, after };
        assert_eq!(img(None, Some(&a[..])).effect(), RowEffect::Wrote(Verb::Insert));
        assert_eq!(img(Some(&a[..]), None).effect(), RowEffect::Wrote(Verb::Delete));
        assert_eq!(img(Some(&a[..]), Some(&b[..])).effect(), RowEffect::Wrote(Verb::Update));
        assert_eq!(img(Some(&a[..]), Some(&a[..])).effect(), RowEffect::Unchanged);
    }

    #[test]
    fn a_table_off_the_allowlist_is_refused_and_one_on_it_writes() {
        let e = full_envelope();
        let after = row(&[Value::Integer(1), Value::Integer(5)]);
        let img = [RowImage { row: 1, before: None, after: Some(&after) }];

        let err = e.admit(T + 1, "other", &img).expect_err("an unlisted table was writable");
        assert!(format!("{err}").contains("not on the allowlist"), "got {err}");
        // Anti-vacuity: the SAME statement against the allowed table is admitted.
        assert_eq!(e.admit(T, "allowed", &img).unwrap(), 1);
    }

    /// Default-deny is checked even when the statement matched no row: authority over a table is
    /// not a function of whether a WHERE happened to select anything.
    #[test]
    fn an_unlisted_table_is_refused_even_for_a_statement_that_matched_nothing() {
        let e = full_envelope();
        assert!(e.admit(T + 1, "other", &[]).is_err());
        assert_eq!(e.admit(T, "allowed", &[]).unwrap(), 0, "and an allowed table charges nothing");
    }

    #[test]
    fn a_verb_off_the_allowlist_is_refused_and_one_on_it_writes() {
        let before = row(&[Value::Integer(1), Value::Integer(5)]);
        let e = CapabilityEnvelope::new(Verb::UPDATE, 10)
            .allow(T, vec![ColumnCapability::open(0), ColumnCapability::open(1)]);

        let del = [RowImage { row: 1, before: Some(&before), after: None }];
        let err = e.admit(T, "t", &del).expect_err("DELETE ran without the verb");
        assert!(format!("{err}").contains("may not DELETE"), "got {err}");

        // Anti-vacuity: UPDATE, which IS granted, goes through.
        let after = row(&[Value::Integer(1), Value::Integer(6)]);
        let upd = [RowImage { row: 1, before: Some(&before), after: Some(&after) }];
        assert_eq!(e.admit(T, "t", &upd).unwrap(), 1);
    }

    #[test]
    fn a_column_off_the_allowlist_is_refused_and_one_on_it_writes() {
        let e = CapabilityEnvelope::new(Verb::ALL, 10).allow(T, vec![ColumnCapability::open(1)]);
        let before = row(&[Value::Integer(1), Value::Integer(5)]);

        let touch_zero = row(&[Value::Integer(2), Value::Integer(5)]);
        let err = e
            .admit(T, "t", &[RowImage { row: 1, before: Some(&before), after: Some(&touch_zero) }])
            .expect_err("column 0 is not on the allowlist");
        assert!(format!("{err}").contains("column 0"), "got {err}");

        let touch_one = row(&[Value::Integer(1), Value::Integer(6)]);
        assert_eq!(
            e.admit(T, "t", &[RowImage { row: 1, before: Some(&before), after: Some(&touch_one) }])
                .unwrap(),
            1
        );
    }

    /// **The breaking shape the column allowlist exists for.** An INSERT writes every column while
    /// its op names none, so a check keyed on the op admits it. Here column 0 is not granted and
    /// the insert must be refused.
    #[test]
    fn an_insert_cannot_write_a_column_it_was_not_granted() {
        let e = CapabilityEnvelope::new(Verb::ALL, 10).allow(T, vec![ColumnCapability::open(1)]);
        let after = row(&[Value::Integer(1), Value::Integer(5)]);
        let err = e
            .admit(T, "t", &[RowImage { row: 1, before: None, after: Some(&after) }])
            .expect_err("an INSERT wrote a column that was never granted");
        assert!(format!("{err}").contains("column 0"), "got {err}");

        // Anti-vacuity: granting column 0 as well admits the same insert.
        let wide = CapabilityEnvelope::new(Verb::ALL, 10)
            .allow(T, vec![ColumnCapability::open(0), ColumnCapability::open(1)]);
        assert_eq!(wide.admit(T, "t", &[RowImage { row: 1, before: None, after: Some(&after) }]).unwrap(), 1);
    }

    /// **Exit criterion: an ASSIGNMENT that lowers a bounded value is refused, not just a
    /// decrement.** The floor never sees the op that produced the value — only the value — so
    /// `SET qty = -100` and `SET qty = qty - 100` are the same input to it.
    #[test]
    fn the_floor_refuses_the_value_whatever_shape_produced_it() {
        let e = CapabilityEnvelope::new(Verb::ALL, 10)
            .allow(T, vec![ColumnCapability::open(0), ColumnCapability::floored(1, 0)]);
        let before = row(&[Value::Integer(1), Value::Integer(20)]);
        let check = |after: Vec<Value>| {
            e.admit(T, "t", &[RowImage { row: 1, before: Some(&before), after: Some(&after) }])
        };

        assert!(check(row(&[Value::Integer(1), Value::Integer(-100)])).is_err(), "assign past the floor");
        assert!(check(row(&[Value::Integer(1), Value::Integer(-1)])).is_err(), "one below the floor");
        assert!(check(row(&[Value::Integer(1), Value::Float(-0.5)])).is_err(), "a fractional value below the floor");
        assert!(check(row(&[Value::Integer(1), Value::BigInt(-1)])).is_err(), "a BigInt below the floor");
        assert!(check(row(&[Value::Integer(1), Value::Decimal("-0.0001".into())])).is_err(), "a decimal below the floor");
        assert!(check(row(&[Value::Integer(1), Value::Float(f64::NAN)])).is_err(), "NaN cleared the floor");
        assert!(check(row(&[Value::Integer(1), Value::Null])).is_err(), "NULL cleared the floor");
        assert!(check(row(&[Value::Integer(1), Value::Varchar("x".into())])).is_err(), "a string cleared the floor");

        // Anti-vacuity: everything at or above the floor still writes.
        assert!(check(row(&[Value::Integer(1), Value::Integer(0)])).is_ok(), "exactly the floor");
        assert!(check(row(&[Value::Integer(1), Value::Integer(8)])).is_ok());
        assert!(check(row(&[Value::Integer(1), Value::Float(0.5)])).is_ok());
        assert!(check(row(&[Value::Integer(1), Value::BigInt(9_000_000_000)])).is_ok());
        // And a column with no floor is not policed at all.
        assert!(check(row(&[Value::Integer(-999), Value::Integer(20)])).is_ok());
    }

    /// Removing the row removes the bounded value, which is a way past a floor that no comparison
    /// on a surviving cell can see. The floor treats "no value" as below itself.
    #[test]
    fn deleting_a_row_cannot_be_used_to_get_around_a_floor() {
        let e = CapabilityEnvelope::new(Verb::ALL, 10)
            .allow(T, vec![ColumnCapability::open(0), ColumnCapability::floored(1, 0)]);
        let before = row(&[Value::Integer(1), Value::Integer(20)]);
        let err = e
            .admit(T, "t", &[RowImage { row: 1, before: Some(&before), after: None }])
            .expect_err("a floored cell was deleted out from under its bound");
        assert!(format!("{err}").contains("the row was removed"), "got {err}");

        // Anti-vacuity: a row with no floored column deletes fine.
        let open = CapabilityEnvelope::new(Verb::ALL, 10)
            .allow(T, vec![ColumnCapability::open(0), ColumnCapability::open(1)]);
        assert_eq!(
            open.admit(T, "t", &[RowImage { row: 1, before: Some(&before), after: None }]).unwrap(),
            1
        );
    }

    /// **Breaking shape: one statement over many rows.** A budget checked and charged per row
    /// admits rows until it runs out and leaves them written; this one decides the whole batch
    /// before charging anything, and reports the count so the caller charges once.
    #[test]
    fn a_statement_that_overruns_the_budget_is_refused_as_a_whole() {
        let mut e = CapabilityEnvelope::new(Verb::ALL, 3).allow(T, vec![ColumnCapability::open(0)]);
        let rows: Vec<Vec<Value>> = (0..4).map(|i| row(&[Value::Integer(i)])).collect();
        let images: Vec<RowImage> =
            rows.iter().enumerate().map(|(i, r)| RowImage { row: i as u64, before: None, after: Some(r) }).collect();

        let err = e.admit(T, "t", &images).expect_err("4 rows fit in a budget of 3");
        assert!(format!("{err}").contains("row-write budget"), "got {err}");
        assert_eq!(e.row_writes(), 0, "a refused statement charged the budget anyway");

        // Anti-vacuity: three of them fit exactly, and a fourth then does not.
        assert_eq!(e.admit(T, "t", &images[..3]).unwrap(), 3);
        e.charge(3).unwrap();
        assert!(e.admit(T, "t", &images[..1]).is_err(), "the spent budget was handed back");
    }

    /// A row a statement touched but did not change is not a write, so it neither needs a verb nor
    /// costs budget. Charging it would let a no-op `SET qty = qty` exhaust an agent's quota.
    #[test]
    fn a_row_whose_image_did_not_change_costs_nothing() {
        let e = CapabilityEnvelope::new(0, 1).allow(T, vec![]);
        let same = row(&[Value::Integer(1)]);
        assert_eq!(
            e.admit(T, "t", &[RowImage { row: 1, before: Some(&same), after: Some(&same) }]).unwrap(),
            0
        );
    }

    #[test]
    fn an_envelope_can_be_narrowed_but_never_widened() {
        let mut r = BranchRecord::trunk(1, LeaseDeadline(0));
        // The first envelope is a narrowing from "ungoverned", so it installs.
        r.restrict(full_envelope()).expect("installing the first envelope must be allowed");

        let narrower = CapabilityEnvelope::new(Verb::UPDATE, 10)
            .allow(T, vec![ColumnCapability::floored(1, 5)]);
        r.restrict(narrower.clone()).expect("dropping a verb, a column and a budget is a narrowing");
        assert_eq!(r.envelope.as_ref().unwrap(), &narrower);

        for (label, wider) in [
            ("a verb", CapabilityEnvelope::new(Verb::ALL, 10).allow(T, vec![ColumnCapability::floored(1, 5)])),
            ("a table", CapabilityEnvelope::new(Verb::UPDATE, 10)
                .allow(T, vec![ColumnCapability::floored(1, 5)])
                .allow(T + 1, vec![ColumnCapability::open(0)])),
            ("a column", CapabilityEnvelope::new(Verb::UPDATE, 10)
                .allow(T, vec![ColumnCapability::floored(1, 5), ColumnCapability::open(0)])),
            ("a floor", CapabilityEnvelope::new(Verb::UPDATE, 10).allow(T, vec![ColumnCapability::floored(1, 4)])),
            ("a dropped floor", CapabilityEnvelope::new(Verb::UPDATE, 10).allow(T, vec![ColumnCapability::open(1)])),
            ("a budget", CapabilityEnvelope::new(Verb::UPDATE, 11).allow(T, vec![ColumnCapability::floored(1, 5)])),
        ] {
            let err = r.restrict(wider).unwrap_err();
            assert!(format!("{err}").contains("may only be narrowed"), "widening {label}: {err}");
            assert_eq!(r.envelope.as_ref().unwrap(), &narrower, "widening {label} landed anyway");
        }
    }

    #[test]
    fn forking_does_not_mint_budget_the_parent_had_already_spent() {
        let mut parent = BranchRecord::trunk(1, LeaseDeadline(0));
        let mut e = full_envelope();
        e.charge(60).unwrap();
        parent.envelope = Some(e);

        let child =
            BranchRecord::fork_child(&parent, BranchId::new(1, 0), Epoch(1), LeaseDeadline(0)).unwrap();
        let inherited = child.envelope.as_ref().expect("a child of a governed branch is governed");
        assert_eq!(inherited.max_row_writes(), 40, "the child got the parent's SPENT budget back");
        assert_eq!(inherited.row_writes(), 0);
        assert_eq!(inherited.tables(), parent.envelope.as_ref().unwrap().tables());
        assert_eq!(inherited.verbs(), parent.envelope.as_ref().unwrap().verbs());

        // Anti-vacuity: an ungoverned parent still forks an ungoverned child.
        let plain = BranchRecord::trunk(1, LeaseDeadline(0));
        let free = BranchRecord::fork_child(&plain, BranchId::new(2, 0), Epoch(1), LeaseDeadline(0)).unwrap();
        assert_eq!(free.envelope, None);
    }

    /// `charge` re-checks rather than trusting `admit`'s answer, because the two are not one
    /// atomic step — something else may have charged against the same branch in between. Without
    /// its own test this second check is a redundant guard that no mutant can reach, which is how
    /// a fire-check certifies nothing.
    #[test]
    fn charging_past_the_budget_is_refused_at_the_charge_not_only_at_admit() {
        let mut e = CapabilityEnvelope::new(Verb::ALL, 3).allow(T, vec![ColumnCapability::open(0)]);
        e.charge(2).expect("2 of 3 fits");
        assert_eq!(e.remaining(), 1);
        let err = e.charge(2).expect_err("4 row-writes fitted in a budget of 3");
        assert!(format!("{err}").contains("remaining budget of 1"), "got {err}");
        assert_eq!(e.row_writes(), 2, "a refused charge was applied anyway");
        // Anti-vacuity: the last one still fits.
        e.charge(1).expect("the final row-write must fit");
        assert_eq!(e.remaining(), 0);
    }

    #[test]
    fn a_duplicate_column_keeps_the_tighter_floor() {
        let cap = TableCapability::new(
            T,
            vec![ColumnCapability::floored(1, 0), ColumnCapability::open(1), ColumnCapability::floored(1, 5)],
        );
        assert_eq!(cap.columns(), &[ColumnCapability::floored(1, 5)]);
    }
}
#[cfg(test)]
mod core_record_tests {
    use super::*;

    /// Every field must survive the round trip. A silently dropped field here is a branch that
    /// comes back pointing at the wrong root page or with the wrong lease.
    #[test]
    fn the_core_record_round_trips_every_field_it_carries() {
        let mut rec = BranchRecord::trunk(7, LeaseDeadline(1234));
        rec.branch_id = BranchId::new(42, 3);
        rec.generation = 9;
        rec.parent_id = Some(BranchId::new(41, 2));
        rec.fork_epoch = Epoch(555);
        rec.root_page_id = 777;
        rec.lease_deadline = LeaseDeadline(u64::MAX - 1);
        rec.state = BranchState::Quarantined;
        rec.depth = 11;
        // The unbounded fields are deliberately NOT carried; they live in key spans.
        rec.arenas = vec![ArenaId(1), ArenaId(2)];
        rec.live_children = vec![Epoch(3), Epoch(4)];

        let bytes = rec.serialize_core();
        assert_eq!(bytes.len(), CORE_BYTES, "core record width drifted from the constant");
        // Through `into_hydrated`, NOT through `.0`. The field is reachable here because this
        // test shares the module, and using it would make the guard honour-system in exactly the
        // place that is supposed to prove it is not. Passing the empty arenas and absent envelope
        // explicitly is also the honest statement of what `serialize_core` drops.
        let back = BranchRecord::deserialize_core(&bytes)
            .expect("round trip")
            .into_hydrated(Vec::new(), None);

        assert_eq!(back.branch_id, rec.branch_id);
        assert_eq!(back.generation, rec.generation);
        assert_eq!(back.parent_id, rec.parent_id);
        assert_eq!(back.fork_epoch, rec.fork_epoch);
        assert_eq!(back.root_page_id, rec.root_page_id);
        assert_eq!(back.lease_deadline, rec.lease_deadline);
        assert_eq!(back.state, rec.state);
        assert_eq!(back.depth, rec.depth);
        assert!(back.arenas.is_empty(), "arenas must come back empty, not stale");
        assert!(back.live_children.is_empty(), "live_children must come back empty, not stale");
        assert!(back.envelope.is_none());
    }

    /// The guard itself, asserted rather than assumed: a `CoreRecord` must not be usable as a
    /// whole record. This is a compile-time property, so the test that matters is the one in
    /// `tools/` that tries it and expects rustc to refuse -- see `d5_core_record_has_no_bypass`.
    /// What is checkable here is that the ONLY exit carries both missing fields through.
    #[test]
    fn d5_into_hydrated_is_the_only_exit_and_it_carries_both_fields() {
        let mut rec = BranchRecord::trunk(1, LeaseDeadline(0));
        rec.arenas = vec![ArenaId(7), ArenaId(9)];
        let core = BranchRecord::deserialize_core(&rec.serialize_core()).unwrap();
        // The core genuinely lost them...
        let empty = core.clone().into_hydrated(Vec::new(), None);
        assert!(empty.arenas.is_empty(), "core must not resurrect arenas it never stored");
        // ...and the only way to a whole record is to supply them.
        let whole = core.into_hydrated(vec![ArenaId(7), ArenaId(9)], None);
        assert_eq!(whole.arenas, vec![ArenaId(7), ArenaId(9)]);
    }

    /// Trunk has no parent, and the absent-parent tag must not be confused with parent id 0 —
    /// which is trunk's own id, so getting this wrong makes trunk its own parent.
    #[test]
    fn an_absent_parent_is_distinguishable_from_parent_zero() {
        let trunk = BranchRecord::trunk(1, LeaseDeadline(0));
        assert_eq!(trunk.parent_id, None, "fixture");
        let back = BranchRecord::deserialize_core(&trunk.serialize_core())
            .unwrap()
            .into_hydrated(Vec::new(), None);
        assert_eq!(back.parent_id, None, "absent parent came back as Some");

        let mut child = trunk.clone();
        child.branch_id = BranchId::new(5, 0);
        child.parent_id = Some(BranchId::TRUNK);
        let back = BranchRecord::deserialize_core(&child.serialize_core())
            .unwrap()
            .into_hydrated(Vec::new(), None);
        assert_eq!(back.parent_id, Some(BranchId::TRUNK), "parent 0 came back as absent");
    }

    /// A buffer of the wrong length must refuse rather than read into whatever follows it.
    #[test]
    fn a_wrong_length_core_record_is_refused() {
        let rec = BranchRecord::trunk(1, LeaseDeadline(0));
        let bytes = rec.serialize_core();
        assert!(BranchRecord::deserialize_core(&bytes[..CORE_BYTES - 1]).is_err(), "short accepted");
        let mut long = bytes.clone();
        long.push(0);
        assert!(BranchRecord::deserialize_core(&long).is_err(), "long accepted");
        // An unknown state tag is corruption, not a default.
        let mut bad = bytes.clone();
        bad[49] = 200;
        assert!(BranchRecord::deserialize_core(&bad).is_err(), "unknown state tag accepted");
    }
}
