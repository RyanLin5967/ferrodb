//! The `EffectLog` re-append contract, which the SQL surface and the typed effect log
//! implemented incompatibly.
//!
//! `AgentRuntime::stage` appends the agent task's frame once per staged row change, growing it
//! each time under one `TxnId`. `tel::MemEffectLog` refused any re-append whose contents differed
//! from the stored frame, so the SQL surface could not run on the canonical log at all: the
//! second write in a session was a hard error.


use ferrodb::branch::types::{BranchId, CommitHash};
use ferrodb::catalog::column::Value;
use ferrodb::tel::ids::{ColId, RowId, TableId, TxnId};
use ferrodb::tel::op::{Delta, Op, OpKind};
use ferrodb::tel::{EffectLog, MemEffectLog, TxnFrame};

fn frame(n: usize) -> TxnFrame {
    let mut f = TxnFrame::new(TxnId(1), BranchId::new(1, 0), CommitHash::ZERO, 0, 1);
    for i in 0..n {
        f.push_op(Op::new(
            TableId(1),
            RowId(i as u64),
            Some(ColId(2)),
            OpKind::Add(Delta::Int(-5)),
        ));
    }
    f
}

#[test]
fn an_open_task_frame_may_grow_under_one_txn_id() {
    let log = MemEffectLog::new();
    // Three staged row changes in one agent task, exactly as AgentRuntime::stage appends them.
    log.append(&frame(1)).unwrap();
    log.append(&frame(2)).unwrap();
    log.append(&frame(3)).unwrap();

    let back = log.frames_for(BranchId::new(1, 0), 0).unwrap();
    assert_eq!(back.len(), 1, "a growing frame became several frames");
    assert_eq!(back[0].ops.len(), 3, "the frame did not grow to its final contents");
}

#[test]
fn a_replayed_identical_frame_is_still_not_stored_twice() {
    // The Cassandra counter trap: two copies of one Add compose to -10.
    let log = MemEffectLog::new();
    let f = frame(2);
    log.append(&f).unwrap();
    log.append(&f).unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log.frames_for(BranchId::new(1, 0), 0).unwrap()[0].ops.len(), 2);
}

#[test]
fn a_txn_id_collision_between_different_transactions_is_still_a_hard_error() {
    // Growth is an extension of what is stored. A frame that contradicts the stored one is a
    // different transaction wearing the same id, and picking a winner would hide the bug.
    let log = MemEffectLog::new();
    log.append(&frame(3)).unwrap();

    let mut other = TxnFrame::new(TxnId(1), BranchId::new(1, 0), CommitHash::ZERO, 0, 1);
    other.push_op(Op::new(
        TableId(9),
        RowId(9),
        Some(ColId(9)),
        OpKind::Assign(Value::Integer(1)),
    ));
    let err = log.append(&other).unwrap_err();
    assert!(err.to_string().contains("refusing"), "got {}", err);

    // Truncation is not growth either: dropping ops from a stored frame is not an extension.
    let err = log.append(&frame(2)).unwrap_err();
    assert!(err.to_string().contains("refusing"), "got {}", err);
    assert_eq!(log.frames_for(BranchId::new(1, 0), 0).unwrap()[0].ops.len(), 3);
}
