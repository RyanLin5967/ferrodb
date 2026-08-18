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
        if parent.state != BranchState::Live {
            return Err(BranchError::NotWritable(parent.branch_id));
        }
        let depth = parent.depth + 1;
        if depth > MAX_BRANCH_DEPTH {
            return Err(BranchError::DepthExceeded { branch: parent.branch_id, depth });
        }
        Ok(BranchRecord {
            branch_id: child_id,
            generation: child_id.generation,
            parent_id: Some(parent.branch_id),
            fork_epoch,
            // The whole fork: the child's root IS the parent's root.
            root_page_id: parent.root_page_id,
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
            envelope: parent.envelope.as_ref().map(CapabilityEnvelope::inherited),
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
                b.push(e.verbs);
                b.extend_from_slice(&e.max_row_writes.to_be_bytes());
                b.extend_from_slice(&e.row_writes.to_be_bytes());
                b.extend_from_slice(&(e.tables.len() as u32).to_be_bytes());
                for t in &e.tables {
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
        } else if c.u8()? == 0 {
            None
        } else {
            let verbs = c.u8()?;
            let max_row_writes = c.u64()?;
            let row_writes = c.u64()?;
            let table_len = c.u32()? as usize;
            let mut tables = Vec::with_capacity(table_len);
            for _ in 0..table_len {
                let table = c.u32()?;
                let col_len = c.u32()? as usize;
                let mut columns = Vec::with_capacity(col_len);
                for _ in 0..col_len {
                    let col = c.u32()?;
                    let has_floor = c.u8()?;
                    let floor = c.i64()?;
                    columns.push(ColumnCapability {
                        col,
                        floor: if has_floor == 1 { Some(floor) } else { None },
                    });
                }
                tables.push(TableCapability { table, columns });
            }
            Some(CapabilityEnvelope { verbs, tables, max_row_writes, row_writes })
        };

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
    /// The verb this row transition amounts to.
    pub fn effect(&self) -> RowEffect {
        match (self.before, self.after) {
            (None, Some(_)) => RowEffect::Wrote(Verb::Insert),
            (Some(_), None) => RowEffect::Wrote(Verb::Delete),
            // A row that vanishes from nowhere is still a removal as far as authority goes.
            (None, None) => RowEffect::Wrote(Verb::Delete),
            (Some(b), Some(a)) => {
                if changed_columns(Some(b), Some(a)).is_empty() {
                    RowEffect::Unchanged
                } else {
                    RowEffect::Wrote(Verb::Update)
                }
            }
        }
    }
}

/// Column indices whose value differs between the two images.
///
/// A side that does not exist reads as all-`Null`, and the shorter image is padded with `Null`, so
/// an INSERT reports every column it gave a value to and a DELETE reports every column it took one
/// away from. **That is why an INSERT cannot slip past a column allowlist**: its `Op` carries
/// `col: None`, so a check keyed on the op's column would see an INSERT touch no column at all and
/// wave through a row that wrote every one of them.
pub fn changed_columns(before: Option<&[Value]>, after: Option<&[Value]>) -> Vec<u32> {
    let b = before.unwrap_or(&[]);
    let a = after.unwrap_or(&[]);
    let n = b.len().max(a.len());
    let mut out = Vec::new();
    for i in 0..n {
        let lhs = b.get(i).unwrap_or(&Value::Null);
        let rhs = a.get(i).unwrap_or(&Value::Null);
        if lhs != rhs {
            out.push(i as u32);
        }
    }
    out
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCapability {
    pub table: u32,
    /// Sorted by column index. A column that is absent may not be written: default-deny.
    pub columns: Vec<ColumnCapability>,
}

impl TableCapability {
    /// Build a table capability, normalising the column list so the encoding is canonical and a
    /// duplicate cannot loosen a floor: the tighter of two entries for one column wins.
    pub fn new(table: u32, mut columns: Vec<ColumnCapability>) -> Self {
        columns.sort_by_key(|c| (c.col, std::cmp::Reverse(c.floor)));
        columns.dedup_by_key(|c| c.col);
        TableCapability { table, columns }
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
pub struct CapabilityEnvelope {
    /// Bitmask of [`Verb`] bits.
    pub verbs: u8,
    /// Sorted by table id. A table that is absent may not be written: default-deny.
    pub tables: Vec<TableCapability>,
    /// How many row-writes this branch may perform over its whole life.
    ///
    /// A "row-write" is one row whose image a statement changed. Re-writing the same row in a
    /// later statement costs another one — this is a budget on writes, not a count of distinct
    /// rows, and it is named that way so nobody reads it as the latter.
    pub max_row_writes: u64,
    /// How many it has already spent. **Durable**, so a restart does not hand the budget back.
    pub row_writes: u64,
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

    /// Row-writes still available.
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
            let RowEffect::Wrote(verb) = image.effect() else {
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
            for col in changed_columns(image.before, image.after) {
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
        e.row_writes = 17;
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
    /// op would see it touch no column and wave through a row that wrote every one of them.
    /// Reading the images instead reports every column the insert gave a value to.
    #[test]
    fn changed_columns_reads_images_so_an_insert_reports_every_column_it_wrote() {
        let after = row(&[Value::Integer(1), Value::Integer(2), Value::Null]);
        assert_eq!(changed_columns(None, Some(&after)), vec![0, 1]);
        assert_eq!(changed_columns(Some(&after), None), vec![0, 1], "a delete takes them away");

        let before = row(&[Value::Integer(1), Value::Integer(9), Value::Null]);
        assert_eq!(changed_columns(Some(&before), Some(&after)), vec![1]);
        assert!(changed_columns(Some(&before), Some(&before)).is_empty());
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
        assert_eq!(e.row_writes, 0, "a refused statement charged the budget anyway");

        // Anti-vacuity: three of them fit exactly, and a fourth then does not.
        assert_eq!(e.admit(T, "t", &images[..3]).unwrap(), 3);
        e.row_writes = 3;
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
        e.row_writes = 60;
        parent.envelope = Some(e);

        let child =
            BranchRecord::fork_child(&parent, BranchId::new(1, 0), Epoch(1), LeaseDeadline(0)).unwrap();
        let inherited = child.envelope.as_ref().expect("a child of a governed branch is governed");
        assert_eq!(inherited.max_row_writes, 40, "the child got the parent's SPENT budget back");
        assert_eq!(inherited.row_writes, 0);
        assert_eq!(inherited.tables, parent.envelope.as_ref().unwrap().tables);
        assert_eq!(inherited.verbs, parent.envelope.as_ref().unwrap().verbs);

        // Anti-vacuity: an ungoverned parent still forks an ungoverned child.
        let plain = BranchRecord::trunk(1, LeaseDeadline(0));
        let free = BranchRecord::fork_child(&plain, BranchId::new(2, 0), Epoch(1), LeaseDeadline(0)).unwrap();
        assert_eq!(free.envelope, None);
    }

    #[test]
    fn a_duplicate_column_keeps_the_tighter_floor() {
        let cap = TableCapability::new(
            T,
            vec![ColumnCapability::floored(1, 0), ColumnCapability::open(1), ColumnCapability::floored(1, 5)],
        );
        assert_eq!(cap.columns, vec![ColumnCapability::floored(1, 5)]);
    }
}
