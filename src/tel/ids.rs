//! Identifiers used throughout the Typed Effect Log.
//!
//! Design authority: DESIGN.md section 3.

use std::fmt::{Display, Formatter};

use crate::branch::types::BranchId;

/// **Immutable surrogate row identity, minted at insert.**
///
/// The primary key is a *constraint*, not identity. A merge that keyed rows by PK would treat an
/// UPDATE of the PK as a delete plus an insert and lose the row's history; `RowId` never changes
/// for the life of the row, which is what makes three-way merge well defined.
///
/// This is deliberately distinct from `storage::heap_file_manager::RecordId`, which names a
/// *physical* slot and moves whenever a version is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct RowId(pub u64);

impl RowId {
    /// **The table's shape, rather than one of its rows** — B11.
    ///
    /// A `ConflictReport` names a `(table, row)` because every conflict the merge engine could
    /// produce before this was about a cell. A column-level schema conflict is about the table
    /// itself, and there is no row to name. Rather than making `row` an `Option` — which would
    /// force every existing reader of a report to handle a case that only one producer can
    /// generate — the shape gets a reserved id of its own.
    ///
    /// `u64::MAX` is safe to reserve because a `RowId` is minted by `row_id_of`, an FNV-1a hash of
    /// the row's primary key, and `is_schema` below is the only thing that reads it back. A row
    /// that hashed to it would be reported as a schema conflict, which is why the value is at the
    /// far end of the space rather than a small number anything could collide with by accident.
    pub const SCHEMA: RowId = RowId(u64::MAX);

    /// True for [`RowId::SCHEMA`]. Read this rather than comparing against `u64::MAX` inline.
    pub fn is_schema(&self) -> bool {
        *self == RowId::SCHEMA
    }
}

impl Display for RowId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.is_schema() {
            return f.write_str("schema");
        }
        write!(f, "row{}", self.0)
    }
}

/// A table, by catalog id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TableId(pub u32);

impl Display for TableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "tbl{}", self.0)
    }
}

/// A column, by ordinal within its table's schema. Matches the `usize` offsets the binder
/// already produces in `BoundExpr::Column`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ColId(pub u32);

impl Display for ColId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "col{}", self.0)
    }
}

impl From<usize> for ColId {
    fn from(v: usize) -> Self {
        ColId(v as u32)
    }
}

/// Transaction identity, unique across branches.
///
/// Load-bearing at merge time: `OpKind::Add` is **not** idempotent, so replayed frames are
/// de-duplicated by `TxnId`. Two identical `qty -= 5` compose to -10, not -5 (the Cassandra
/// counter trap), and dropping the de-dup is how retries silently double-count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TxnId(pub u64);

impl Display for TxnId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "txn{}", self.0)
    }
}

/// A causal dot: which branch produced an element, and at what per-branch sequence number.
///
/// Used only by the observed-remove set operations, where a remove must name the inserts it
/// actually saw. Dots are bounded by the number of elements, not by the number of replicas —
/// there are no per-replica version vectors anywhere in this design, because ephemeral agents
/// would grow them without bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Dot {
    pub branch: BranchId,
    pub seq: u64,
}

impl Display for Dot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.branch, self.seq)
    }
}
