//! An in-memory [`EffectLog`].
//!
//! Design authority: DESIGN.md section 3 ("Log format").
//!
//! The log is append-only and keyed by `(branch, txn_id)`. Appending the *same* transaction twice
//! is a retry, not a second transaction, so the second append is a no-op — `OpKind::Add` is not
//! idempotent and a log that stores a replayed frame twice has already lost. Appending a
//! *different* frame under an id that is already present is a hard error rather than a silent
//! overwrite: one of the two is a bug, and picking a winner would hide it.
//!
//! Merge de-duplicates by `TxnId` again anyway ([`crate::tel::engine::dedup_by_txn`]) because
//! frames also arrive from elsewhere — a frame already merged once can reach a later merge from
//! both sides. Neither check makes the other redundant.

use std::sync::Mutex;

use crate::branch::types::BranchId;
use crate::error::FerroError;
use crate::tel::frame::TxnFrame;
use crate::tel::ids::TxnId;
use crate::tel::EffectLog;

/// An `EffectLog` held in memory. Durable enough for a branch that never outlives the process,
/// which — given non-cooperative lease reaping — is the common case for an agent task.
#[derive(Default)]
pub struct MemEffectLog {
    frames: Mutex<Vec<TxnFrame>>,
}

impl MemEffectLog {
    pub fn new() -> Self {
        MemEffectLog { frames: Mutex::new(Vec::new()) }
    }

    /// Every frame ever appended, in append order.
    pub fn all(&self) -> Vec<TxnFrame> {
        self.frames.lock().expect("effect log mutex poisoned").clone()
    }

    pub fn len(&self) -> usize {
        self.frames.lock().expect("effect log mutex poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The frame for one transaction on one branch, if it was ever appended.
    pub fn frame(&self, branch: BranchId, txn: TxnId) -> Option<TxnFrame> {
        self.frames
            .lock()
            .expect("effect log mutex poisoned")
            .iter()
            .find(|f| f.branch == branch && f.txn_id == txn)
            .cloned()
    }
}

impl EffectLog for MemEffectLog {
    fn append(&self, frame: &TxnFrame) -> Result<(), FerroError> {
        let mut frames = self.frames.lock().expect("effect log mutex poisoned");
        if let Some(existing) =
            frames.iter().find(|f| f.branch == frame.branch && f.txn_id == frame.txn_id)
        {
            if existing == frame {
                // A retry delivering the identical frame. Storing it again would double every
                // Add it carries.
                return Ok(());
            }
            return Err(FerroError::Merge(format!(
                "{} on branch {} was already logged with different contents; refusing to \
                 overwrite it",
                frame.txn_id, frame.branch
            )));
        }
        frames.push(frame.clone());
        Ok(())
    }

    fn frames_for(&self, branch: BranchId, from_seq: u64) -> Result<Vec<TxnFrame>, FerroError> {
        let frames = self.frames.lock().expect("effect log mutex poisoned");
        let mut out: Vec<TxnFrame> = frames
            .iter()
            .filter(|f| f.branch == branch && f.seq >= from_seq)
            .cloned()
            .collect();
        out.sort_by_key(|f| (f.seq, f.txn_id.0));
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::types::CommitHash;
    use crate::catalog::column::Value;
    use crate::tel::ids::{ColId, RowId, TableId};
    use crate::tel::op::{Delta, Op, OpKind};

    fn decrement(txn: u64, branch: u64, seq: u64, n: i64) -> TxnFrame {
        let mut f = TxnFrame::new(
            TxnId(txn),
            BranchId::new(branch, 0),
            CommitHash::ZERO,
            seq,
            1,
        );
        f.push_op(Op::new(
            TableId(1),
            RowId(1),
            Some(ColId(2)),
            OpKind::Add(Delta::Int(-n)),
        ));
        f
    }

    #[test]
    fn frames_come_back_in_sequence_order_per_branch() {
        let log = MemEffectLog::new();
        log.append(&decrement(2, 1, 5, 1)).unwrap();
        log.append(&decrement(1, 1, 2, 1)).unwrap();
        log.append(&decrement(3, 2, 1, 1)).unwrap();

        let b1 = log.frames_for(BranchId::new(1, 0), 0).unwrap();
        assert_eq!(b1.iter().map(|f| f.seq).collect::<Vec<_>>(), vec![2, 5]);
        assert_eq!(log.frames_for(BranchId::new(2, 0), 0).unwrap().len(), 1);
        assert_eq!(log.frames_for(BranchId::new(1, 0), 3).unwrap().len(), 1);
    }

    #[test]
    fn replaying_the_identical_frame_does_not_store_it_twice() {
        // The Cassandra counter trap, at the log level: a retried Add must not be stored twice.
        let log = MemEffectLog::new();
        let f = decrement(7, 1, 0, 5);
        log.append(&f).unwrap();
        log.append(&f).unwrap();
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn reusing_a_txn_id_for_different_contents_is_an_error_not_an_overwrite() {
        let log = MemEffectLog::new();
        log.append(&decrement(7, 1, 0, 5)).unwrap();
        assert!(log.append(&decrement(7, 1, 0, 9)).is_err());
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn the_same_txn_id_on_a_different_branch_is_a_different_frame() {
        let log = MemEffectLog::new();
        log.append(&decrement(7, 1, 0, 5)).unwrap();
        log.append(&decrement(7, 2, 0, 5)).unwrap();
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn guards_survive_the_round_trip_separately_from_ops() {
        use crate::tel::guard::{CmpOp, Guard, GuardExpr};
        let log = MemEffectLog::new();
        let mut f = decrement(1, 1, 0, 5);
        f.push_guard(
            Guard::holds(GuardExpr::cmp(
                GuardExpr::col(TableId(1), RowId(1), ColId(2)),
                CmpOp::Ge,
                GuardExpr::Literal(Value::Integer(0)),
            ))
            .with_source("qty >= 0"),
        );
        log.append(&f).unwrap();
        let back = &log.frames_for(BranchId::new(1, 0), 0).unwrap()[0];
        assert_eq!(back.guards.len(), 1);
        assert_eq!(back.guards[0].violated_predicate(), "qty >= 0");
        assert_eq!(back.ops.len(), 1);
    }
}
