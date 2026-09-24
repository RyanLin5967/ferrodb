//! Typed Effect Log.
//!
//! Design authority: DESIGN.md section 3.
//!
//! The log records what a transaction *meant*, not what its pages looked like. Two halves, and
//! they are not interchangeable:
//!
//! - [`op::Op`] — the effect. Recoverable in principle from before/after images.
//! - [`guard::Guard`] — the predicate that made the effect legal. **Not** recoverable from any
//!   log of values, which is why the query layer has to cooperate to capture it.
//!
//! Getting that distinction backwards (logging deltas, dropping guards) produces a merge engine
//! that composes arithmetic correctly and still lets a bounded counter go negative.

pub mod capture;
pub mod engine;
pub mod frame;
pub mod guard;
pub mod ids;
pub mod log;
pub mod merge;
pub mod op;
pub mod schema_merge;

pub use capture::{capture_assignment, capture_guard, to_guard_expr, ColMap, RowSnapshot};
pub use engine::{dedup_by_txn, ComposedState, Deduped, Side, ThreeWayMerger};
pub use frame::{SchemaVer, TxnFrame};
pub use log::{DurableEffectLog, MemEffectLog, RecoveryReport};
pub use guard::{ArithOp, CmpOp, Guard, GuardContext, GuardExpr};
pub use ids::{ColId, Dot, RowId, TableId, TxnId};
pub use merge::{
    ColumnPolicyLookup, ConflictKind, ConflictReport, Diff, DiscardedWrite, MergeOutcome,
    MergePolicy, Merger,
};
pub use op::{Delta, EscrowClaim, Op, OpKind};
pub use schema_merge::{
    merge_schema, ColumnShapeReq, SchemaEdit, SchemaMerge, SchemaMergeOutcome, SchemaPredicate,
};

use crate::error::FerroError;

/// Where captured frames go. The SQL layer appends here; merge reads back.
pub trait EffectLog: Send + Sync {
    fn append(&self, frame: &TxnFrame) -> Result<(), FerroError>;

    /// Would [`EffectLog::append`] accept `frame`? Everything `append` decides except the write:
    /// the same classification against what is already stored and, for a store with a format, the
    /// same encoding and the same size bound. **Mutates nothing.**
    ///
    /// D258: `AgentRuntime::stage_all` asks this before it changes anything, so every refusal the
    /// log owes a statement arrives while there is still nothing to undo. After `Ok` here, `append`
    /// of the same frame may still fail, but only on I/O.
    ///
    /// **No default body, deliberately.** `Ok(())` is the fail-open answer: a store that forgot to
    /// override it would admit a statement its own `append` then refuses, after the caller had
    /// already charged the budget and written the page tree. An implementor has to decide what it
    /// refuses, and implementing it by sharing `append`'s own code is how the two stay equal.
    fn check_append(&self, frame: &TxnFrame) -> Result<(), FerroError>;

    /// Frames written on `branch` at or after `from_seq`, in sequence order.
    fn frames_for(
        &self,
        branch: crate::branch::types::BranchId,
        from_seq: u64,
    ) -> Result<Vec<TxnFrame>, FerroError>;
}
