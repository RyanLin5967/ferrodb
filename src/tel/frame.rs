//! The unit of the Typed Effect Log: one transaction's ops, guards and claims.
//!
//! Design authority: DESIGN.md section 3 ("Log format").

use crate::branch::types::{BranchId, CommitHash};
use crate::tel::guard::Guard;
use crate::tel::ids::{ColId, RowId, TableId, TxnId};
use crate::tel::op::{EscrowClaim, Op};

/// Schema version the frame was written against. A merge across a schema change must fail loudly
/// rather than apply column ordinals from the wrong schema.
pub type SchemaVer = u32;

/// One transaction, as typed effects rather than page images.
///
/// `guards` is not a subset of `ops` and never derivable from them. That separation is the whole
/// point of the format: a byte WAL can reconstruct every op in `ops` and none of the entries in
/// `guards`.
#[derive(Debug, Clone, PartialEq)]
pub struct TxnFrame {
    pub txn_id: TxnId,
    pub branch: BranchId,
    /// The committed state this frame was written against. Merge is three-way against the fork
    /// point, and this is what identifies it.
    pub base: CommitHash,
    /// Position of this frame within its branch, ascending from 0.
    pub seq: u64,
    pub schema_ver: SchemaVer,
    pub ops: Vec<Op>,
    /// The predicates that made the ops legal. First class, separate from `ops`.
    pub guards: Vec<Guard>,
    pub claims: Vec<EscrowClaim>,
}

impl TxnFrame {
    pub fn new(txn_id: TxnId, branch: BranchId, base: CommitHash, seq: u64, schema_ver: SchemaVer) -> Self {
        TxnFrame {
            txn_id,
            branch,
            base,
            seq,
            schema_ver,
            ops: Vec::new(),
            guards: Vec::new(),
            claims: Vec::new(),
        }
    }

    pub fn push_op(&mut self, op: Op) {
        self.ops.push(op);
    }

    pub fn push_guard(&mut self, guard: Guard) {
        self.guards.push(guard);
    }

    pub fn push_claim(&mut self, claim: EscrowClaim) {
        self.claims.push(claim);
    }

    /// Every cell this frame wrote. The left half of the gate's `write-set \ read-set` metric.
    pub fn write_set(&self) -> Vec<(TableId, RowId, Option<ColId>)> {
        self.ops.iter().map(|o| (o.tbl, o.row, o.col)).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty() && self.guards.is_empty() && self.claims.is_empty()
    }
}
