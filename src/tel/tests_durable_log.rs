//! F10 — tests for the durable Typed Effect Log.
//!
//! Owner: agent F10. Every test names the rule it is about, and the ones that could pass vacuously
//! carry the half that proves they can fail: a `MemEffectLog` run through the same script, or an
//! assertion on the value that would still be there if the store had never worked.
//!
//! The fault-injected tests run on `storage::sim`, so a crash is *aimed* at a chosen operation
//! rather than staged by hand-editing a file after the fact. The one filesystem test at the bottom
//! runs the path a database actually takes, so a divergence between `impl Storage for File` and the
//! fabric cannot hide.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::*;
use crate::storage::sim::{Durability, FaultPlan, SimFabric, WriteShape};
use crate::tel::guard::GuardContext;
use crate::tel::merge::{ColumnPolicyLookup, MergeOutcome, MergePolicy, Merger};
use crate::tel::ThreeWayMerger;

const TEL: &str = "agent.tel";
const TBL: TableId = TableId(1);
const QTY: ColId = ColId(2);

fn b(n: u64) -> BranchId {
    BranchId::new(n, 0)
}

fn open_on(fabric: &Arc<SimFabric>) -> Result<DurableEffectLog, FerroError> {
    DurableEffectLog::with_storage(TEL, fabric.open(TEL))
}

/// A frame decrementing `qty` on row `row` by `n`, under `txn` on `branch`.
fn decrement(txn: u64, branch: u64, seq: u64, row: u64, n: i64) -> TxnFrame {
    let mut f = TxnFrame::new(TxnId(txn), b(branch), CommitHash::ZERO, seq, 1);
    f.push_op(
        Op::new(TBL, RowId(row), Some(QTY), OpKind::Add(Delta::Int(-n)))
            .with_witness(Value::Integer(20)),
    );
    f
}

/// `decrement`, grown by one more op — the shape `AgentRuntime::stage_all` re-appends.
fn grown(txn: u64, branch: u64, seq: u64, rows: &[(u64, i64)]) -> TxnFrame {
    let mut f = TxnFrame::new(TxnId(txn), b(branch), CommitHash::ZERO, seq, 1);
    for (row, n) in rows {
        f.push_op(
            Op::new(TBL, RowId(*row), Some(QTY), OpKind::Add(Delta::Int(-n)))
                .with_witness(Value::Integer(20)),
        );
    }
    f
}

/// Every op kind, every guard shape, every `Value` variant, an escrow claim — one frame that
/// touches the whole encoder.
fn kitchen_sink(txn: u64, branch: u64) -> TxnFrame {
    use crate::tel::guard::{ArithOp, CmpOp, GuardExpr};
    let mut f = TxnFrame::new(TxnId(txn), b(branch), CommitHash([7u8; 32]), 4, 9);
    let dot = |branch: u64, seq: u64| Dot { branch: b(branch), seq };

    f.push_op(Op::new(
        TBL,
        RowId(1),
        None,
        OpKind::RowCreate(vec![
            Value::Integer(i32::MIN),
            Value::Varchar("a string with \0 a nul and \u{1F600} an emoji".into()),
            Value::Float(f64::NEG_INFINITY),
            Value::Boolean(true),
            Value::Null,
            Value::BigInt(i64::MIN),
            // Trailing zeros are information: `Value::Decimal` says so, and the round-trip below
            // compares the TEXT because `Decimal("1.50") == Decimal("1.5")` is true.
            Value::Decimal("1.50".into()),
            Value::Decimal("-0.00000000000000000001".into()),
            Value::Timestamp(-1),
        ]),
    ));
    f.push_op(Op::new(TBL, RowId(2), None, OpKind::RowDelete));
    f.push_op(
        Op::new(TBL, RowId(3), Some(ColId(0)), OpKind::Assign(Value::Varchar(String::new())))
            .with_witness(Value::Null),
    );
    f.push_op(Op::new(TBL, RowId(4), Some(QTY), OpKind::Add(Delta::Float(-0.0))));
    f.push_op(Op::new(TBL, RowId(5), Some(QTY), OpKind::Max(Value::Float(f64::NAN))));
    f.push_op(Op::new(TBL, RowId(6), Some(QTY), OpKind::Min(Value::Integer(0))));
    f.push_op(Op::new(
        TBL,
        RowId::SCHEMA,
        Some(ColId(u32::MAX)),
        OpKind::SetInsert { elem: Value::Varchar("x".into()), dot: dot(9, 3) },
    ));
    f.push_op(Op::new(
        TBL,
        RowId(u64::MAX - 1),
        Some(QTY),
        OpKind::SetRemove {
            elem: Value::BigInt(i64::MAX),
            dots: vec![dot(1, 0), dot(2, u64::MAX)],
        },
    ));

    f.push_guard(
        Guard::holds(GuardExpr::And(vec![
            GuardExpr::cmp(
                GuardExpr::col(TBL, RowId(1), QTY),
                CmpOp::Ge,
                GuardExpr::arith(
                    GuardExpr::Literal(Value::Integer(1)),
                    ArithOp::Mul,
                    GuardExpr::Literal(Value::Decimal("2.00".into())),
                ),
            ),
            GuardExpr::Or(vec![
                GuardExpr::Not(Box::new(GuardExpr::IsNull(Box::new(GuardExpr::col(
                    TBL,
                    RowId(2),
                    ColId(1),
                ))))),
                GuardExpr::cmp(
                    GuardExpr::Literal(Value::Boolean(false)),
                    CmpOp::Ne,
                    GuardExpr::Literal(Value::Timestamp(0)),
                ),
            ]),
        ]))
        .with_source("qty >= 1 * 2.00 AND (col1 IS NOT NULL OR false <> 0)"),
    );
    // A guard with no source text, so the `None` arm of the option is exercised too.
    f.push_guard(Guard::new(
        GuardExpr::cmp(
            GuardExpr::arith(
                GuardExpr::Literal(Value::Float(1.5)),
                ArithOp::Sub,
                GuardExpr::Literal(Value::Float(0.5)),
            ),
            CmpOp::Lt,
            GuardExpr::Literal(Value::Integer(2)),
        ),
        Value::Boolean(true),
    ));

    f.push_claim(EscrowClaim {
        tbl: TBL,
        row: RowId(1),
        col: QTY,
        amount: Delta::Int(-12),
        floor: Some(Value::Integer(0)),
        ceiling: None,
    });
    f.push_claim(EscrowClaim {
        tbl: TBL,
        row: RowId(2),
        col: ColId(3),
        amount: Delta::Float(0.25),
        floor: None,
        ceiling: Some(Value::Decimal("100.00".into())),
    });
    f
}

/// `Debug`, not `PartialEq`. `Value`'s equality is **numeric** — `Integer(1) == Float(1.0)`,
/// `Decimal("1.50") == Decimal("1.5")` — so `assert_eq!` on two frames passes for a codec that
/// retyped every value or normalised every decimal. The derived `Debug` prints the variant and the
/// exact text, so comparing it is what actually pins the bytes.
fn exactly(f: &TxnFrame) -> String {
    format!("{f:?}")
}

// =================================================================================================
// The exit criterion: frames survive a restart
// =================================================================================================

/// **Exit criterion: frames survive a restart.**
///
/// Breaking shape: any workload at all, as long as the process that captured the frames is not the
/// process that reads them. `MemEffectLog` passes every effect-log test in this repo and fails this
/// one, which the anti-vacuity half at the bottom shows rather than asserts.
#[test]
fn frames_survive_a_restart_with_every_field_intact() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let sink = kitchen_sink(7, 3);
    let before = {
        let log = open_on(&fabric).unwrap();
        log.append(&decrement(1, 1, 0, 1, 5)).unwrap();
        log.append(&sink).unwrap();
        log.append(&decrement(2, 1, 3, 2, 1)).unwrap();
        // It answers before the restart too, or the assertion below would pass for a store that
        // never worked at all.
        assert_eq!(log.len(), 3);
        assert_eq!(exactly(&log.frame(b(3), TxnId(7)).unwrap()), exactly(&sink));
        log.frames_for(b(1), 0).unwrap()
    };

    let log = DurableEffectLog::with_storage(TEL, fabric.restart().open(TEL)).unwrap();
    assert_eq!(log.recovery().frames, 3, "the frames were not replayed");
    assert_eq!(log.recovery().extensions, 0, "nothing grew, so nothing should have extended");
    assert_eq!(log.discarded_tail_bytes(), 0, "a clean file discarded bytes");

    assert_eq!(
        exactly(&log.frame(b(3), TxnId(7)).unwrap()),
        exactly(&sink),
        "the whole encoder did not survive the round trip"
    );
    assert_eq!(log.frames_for(b(1), 0).unwrap(), before);
    assert_eq!(log.frames_for(b(1), 1).unwrap().len(), 1, "from_seq no longer filters");

    // Anti-vacuity: a branch nobody wrote to is still empty after a restart, so the answers above
    // are the file's and not a store that says yes to everything.
    assert!(log.frames_for(b(99), 0).unwrap().is_empty());
    assert!(log.frame(b(1), TxnId(999)).is_none());

    // And the same script against the in-memory store loses everything, which is what F10 exists
    // to close. Stated as an assertion so it cannot rot into a comment.
    let mem = MemEffectLog::new();
    mem.append(&decrement(1, 1, 0, 1, 5)).unwrap();
    drop(mem);
    assert!(
        MemEffectLog::new().frames_for(b(1), 0).unwrap().is_empty(),
        "MemEffectLog kept frames across a restart, so this test proves nothing"
    );
}

// =================================================================================================
// The trap: a re-appended frame REPLACES rather than duplicates
// =================================================================================================

/// **The Cassandra counter trap, at the durable layer, on the retry.**
///
/// `OpKind::Add` is not idempotent: two identical `qty -= 5` compose to −10. A naive append-only
/// log writes the retried frame a second time and a replay that concatenates the two copies
/// produces one frame holding the increment twice. The merge engine's `TxnId` de-dup **cannot save
/// it**, because that keys on the id and there is only one frame — it is the frame's own ops that
/// are doubled. So the assertion is on the op count and on the composed arithmetic, not on the
/// frame count alone.
#[test]
fn a_retried_frame_does_not_double_its_add_across_a_restart() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let f = decrement(7, 1, 0, 1, 5);
    let bytes_after_first = {
        let log = open_on(&fabric).unwrap();
        log.append(&f).unwrap();
        let one = fabric.durable_image().get(TEL).map(|v| v.len()).unwrap();
        log.append(&f).unwrap();
        log.append(&f).unwrap();
        let three = fabric.durable_image().get(TEL).map(|v| v.len()).unwrap();
        // A retry writes NOTHING. Not "writes a copy the reader collapses" — nothing.
        assert_eq!(one, three, "a retried frame put bytes in the file");
        assert_eq!(log.len(), 1);
        one
    };
    assert!(bytes_after_first > HEADER_SIZE as usize);

    let log = DurableEffectLog::with_storage(TEL, fabric.restart().open(TEL)).unwrap();
    assert_eq!(log.recovery().frames, 1);
    assert_eq!(log.recovery().extensions, 0);
    let back = log.frames_for(b(1), 0).unwrap();
    assert_eq!(back.len(), 1, "the retries became several frames");
    assert_eq!(back[0].ops.len(), 1, "the retried frame's Add was stored more than once");
    assert_eq!(
        composed_delta(&back),
        -5,
        "qty fell by more than the transaction asked for: the retry was replayed"
    );
}

/// **The same trap on the growth path, which is the one the SQL surface actually takes.**
///
/// `AgentRuntime::stage_all` re-appends the task's whole accumulating frame after every statement.
/// A file that stored each of those whole frames and concatenated them on replay would produce
/// `[-5, -5, -3]` from a task that ran `[-5, -3]` — and, again, `dedup_by_txn` would not notice,
/// because there is one frame.
#[test]
fn a_growing_frame_replays_to_its_final_contents_and_not_to_the_sum_of_its_appends() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    {
        let log = open_on(&fabric).unwrap();
        log.append(&grown(7, 1, 0, &[(1, 5)])).unwrap();
        log.append(&grown(7, 1, 0, &[(1, 5), (2, 3)])).unwrap();
        log.append(&grown(7, 1, 0, &[(1, 5), (2, 3), (3, 2)])).unwrap();
        // A retry of the final shape, mid-task, writes nothing.
        log.append(&grown(7, 1, 0, &[(1, 5), (2, 3), (3, 2)])).unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log.frame(b(1), TxnId(7)).unwrap().ops.len(), 3);
    }

    let log = DurableEffectLog::with_storage(TEL, fabric.restart().open(TEL)).unwrap();
    assert_eq!(log.recovery().frames, 1, "a growing frame became several frames");
    assert_eq!(log.recovery().extensions, 2, "the growth was not stored as two deltas");
    let back = log.frames_for(b(1), 0).unwrap();
    assert_eq!(back.len(), 1);
    assert_eq!(
        back[0].ops.len(),
        3,
        "the frame replayed with {} ops; a task of three statements ran three",
        back[0].ops.len()
    );
    assert_eq!(composed_delta(&back), -10, "the composed decrement is not -5 + -3 + -2");
    assert_eq!(exactly(&back[0]), exactly(&grown(7, 1, 0, &[(1, 5), (2, 3), (3, 2)])));
}

/// The whole point of the delta format, measured: growth is linear in the ops a task ran, not
/// quadratic. A whole-frame format would put 1+2+...+n op copies on the disk.
#[test]
fn growth_costs_the_delta_and_not_the_whole_frame_again() {
    let fabric = SimFabric::clean(Durability::WriteThrough);
    let log = open_on(&fabric).unwrap();
    let mut rows: Vec<(u64, i64)> = Vec::new();
    let mut steps: Vec<usize> = Vec::new();
    for i in 0..40u64 {
        rows.push((i, 1));
        log.append(&grown(7, 1, 0, &rows)).unwrap();
        steps.push(fabric.durable_image().get(TEL).map(|v| v.len()).unwrap());
    }
    let first = steps[1] - steps[0];
    let last = steps[39] - steps[38];
    assert!(
        last <= first + 8,
        "the 40th statement cost {last} bytes and the 2nd cost {first}: growth is not linear, so \
         the whole frame is being rewritten"
    );
    // And it is not free either, which is the anti-vacuity half: a store that wrote nothing at all
    // would also pass the check above.
    assert!(last >= 20, "a growth of one op wrote only {last} bytes");
}

/// A contradiction under an id already in use is refused, and — the durable half — **nothing
/// reaches the file**, so a reopened store does not carry an append the live store rejected.
#[test]
fn a_contradicting_reappend_is_refused_and_never_reaches_the_file() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let clean_len = {
        let log = open_on(&fabric).unwrap();
        log.append(&grown(7, 1, 0, &[(1, 5), (2, 3)])).unwrap();
        let at = fabric.durable_image().get(TEL).map(|v| v.len()).unwrap();

        // Rewrites an op already stored.
        let err = log.append(&grown(7, 1, 0, &[(1, 9), (2, 3)])).expect_err("a rewrite was accepted");
        assert!(format!("{err}").contains("does not extend"), "{err}");
        // Truncates.
        assert!(log.append(&grown(7, 1, 0, &[(1, 5)])).is_err(), "a truncation was accepted");
        // Reorders.
        assert!(log.append(&grown(7, 1, 0, &[(2, 3), (1, 5)])).is_err(), "a reorder was accepted");
        // Changes the base the frame was written against.
        let mut rebased = grown(7, 1, 0, &[(1, 5), (2, 3), (3, 1)]);
        rebased.base = CommitHash([1u8; 32]);
        assert!(log.append(&rebased).is_err(), "a changed base was accepted");

        assert_eq!(
            fabric.durable_image().get(TEL).map(|v| v.len()).unwrap(),
            at,
            "a refused append wrote to the file anyway"
        );
        assert_eq!(log.frame(b(1), TxnId(7)).unwrap().ops.len(), 2);

        // Anti-vacuity: the legal growth still lands, so the refusals above are about the frames
        // and not about a store that stopped accepting anything.
        log.append(&grown(7, 1, 0, &[(1, 5), (2, 3), (3, 1)])).unwrap();
        fabric.durable_image().get(TEL).map(|v| v.len()).unwrap()
    };

    let log = DurableEffectLog::with_storage(TEL, fabric.restart().open(TEL)).unwrap();
    assert_eq!(log.frame(b(1), TxnId(7)).unwrap().ops.len(), 3);
    assert_eq!(log.recovery().frames, 1);
    assert_eq!(log.recovery().extensions, 1);
    assert!(clean_len > 0);
}

/// The same `TxnId` on two branches is two frames, here as in memory. The key is `(branch, txn)`,
/// and a file keyed on the id alone would merge two agents' work into one transaction.
#[test]
fn the_same_txn_id_on_a_different_branch_is_a_different_frame_after_a_restart() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    {
        let log = open_on(&fabric).unwrap();
        log.append(&decrement(7, 1, 0, 1, 5)).unwrap();
        log.append(&decrement(7, 2, 0, 1, 5)).unwrap();
        // The generation is part of the key too: a reaped id at a new generation is a new branch.
        let mut next_gen = decrement(7, 1, 0, 1, 5);
        next_gen.branch = BranchId::new(1, 1);
        log.append(&next_gen).unwrap();
        assert_eq!(log.len(), 3);
    }
    let log = DurableEffectLog::with_storage(TEL, fabric.restart().open(TEL)).unwrap();
    assert_eq!(log.recovery().frames, 3);
    assert_eq!(log.frames_for(b(1), 0).unwrap().len(), 1);
    assert_eq!(log.frames_for(b(2), 0).unwrap().len(), 1);
    assert_eq!(log.frames_for(BranchId::new(1, 1), 0).unwrap().len(), 1);
}

// =================================================================================================
// The exit criterion: a merge computed after a restart agrees with one computed before it
// =================================================================================================

/// Compose every `Add(Delta::Int)` a set of frames carries. The instrument for "did an increment
/// get applied twice", stated in the units the trap is about.
fn composed_delta(frames: &[TxnFrame]) -> i64 {
    let mut total = 0i64;
    for f in frames {
        for op in &f.ops {
            if let OpKind::Add(Delta::Int(n)) = op.kind {
                total += n;
            }
        }
    }
    total
}

struct Policy;
impl ColumnPolicyLookup for Policy {
    fn policy(&self, _tbl: TableId, _col: ColId) -> MergePolicy {
        MergePolicy::Additive
    }
}

struct Cells(Vec<((TableId, RowId, ColId), Value)>);
impl GuardContext for Cells {
    fn column(&self, tbl: TableId, row: RowId, col: ColId) -> Result<Value, FerroError> {
        self.0
            .iter()
            .find(|(k, _)| *k == (tbl, row, col))
            .map(|(_, v)| v.clone())
            .ok_or_else(|| FerroError::CellAbsent(format!("{tbl} {row} {col}")))
    }
}

/// **Exit criterion: a merge computed after a restart agrees with one computed before it.**
///
/// The merge is computed the way the shipped surface computes one — a `Merger` over the frames the
/// log hands back (`agent_sql::SurfaceMerger::new(runtime.log())` in
/// `tests/agent_sql_surface.rs`) — so this is the log on the live path rather than a side record.
/// Both halves are asserted: the structured `MergeOutcome` and the `Diff`, which is the one that
/// reads the log itself.
#[test]
fn a_merge_computed_after_a_restart_agrees_with_one_computed_before_it() {
    use crate::branch::record::BranchRecord;
    use crate::branch::types::LeaseDeadline;
    use crate::tel::guard::{CmpOp, GuardExpr};

    let fabric = SimFabric::clean(Durability::SyncOnly);
    let lca = BranchRecord::trunk(1, LeaseDeadline(u64::MAX));
    let state = Cells(vec![((TBL, RowId(1), QTY), Value::Integer(20))]);

    // A task of three statements, with a retry in the middle, plus a second transaction on the same
    // branch — every re-append shape the SQL surface produces, in one workload.
    let script = |log: &dyn EffectLog| {
        let mut f = grown(7, 1, 0, &[(1, 5)]);
        f.push_guard(
            Guard::holds(GuardExpr::cmp(
                GuardExpr::col(TBL, RowId(1), QTY),
                CmpOp::Ge,
                GuardExpr::Literal(Value::Integer(0)),
            ))
            .with_source("qty >= 0"),
        );
        log.append(&f).unwrap();
        log.append(&f).unwrap(); // a retry
        f.push_op(Op::new(TBL, RowId(1), Some(QTY), OpKind::Add(Delta::Int(-3))));
        log.append(&f).unwrap(); // growth
        log.append(&decrement(8, 1, 1, 1, 2)).unwrap();
    };

    let (outcome_before, diff_before, frames_before) = {
        let log: Arc<dyn EffectLog> = Arc::new(open_on(&fabric).unwrap());
        script(&*log);
        let ours = log.frames_for(b(1), 0).unwrap();
        let m = ThreeWayMerger::with_log(Arc::clone(&log));
        (
            m.merge(&lca, &ours, &[], &Policy, &state).unwrap(),
            m.diff(BranchId::TRUNK, b(1)).unwrap(),
            ours,
        )
    };

    // The merge before the restart is a real answer about real effects, not an empty one — else
    // "they agree" would be two empty results agreeing.
    assert_eq!(composed_delta(&frames_before), -10, "the workload composed to something else");
    assert!(!diff_before.ops.is_empty(), "the diff before the restart was empty");
    assert!(
        diff_before.guards.iter().any(|g| g.violated_predicate() == "qty >= 0"),
        "the guard never reached the diff"
    );
    assert_ne!(outcome_before, MergeOutcome::Conflict(vec![]));

    let log: Arc<dyn EffectLog> =
        Arc::new(DurableEffectLog::with_storage(TEL, fabric.restart().open(TEL)).unwrap());
    let after = log.frames_for(b(1), 0).unwrap();
    let m = ThreeWayMerger::with_log(Arc::clone(&log));

    assert_eq!(after.len(), frames_before.len());
    for (a, before) in after.iter().zip(&frames_before) {
        assert_eq!(exactly(a), exactly(before), "a frame did not survive byte for byte");
    }
    assert_eq!(
        m.merge(&lca, &after, &[], &Policy, &state).unwrap(),
        outcome_before,
        "the merge computed after the restart disagrees with the one computed before it"
    );
    assert_eq!(m.diff(BranchId::TRUNK, b(1)).unwrap(), diff_before);

    // Anti-vacuity: the same script into a store that does not survive the restart gives a
    // DIFFERENT answer, so the agreement above is the file's doing.
    let empty: Arc<dyn EffectLog> = Arc::new(MemEffectLog::new());
    let m2 = ThreeWayMerger::with_log(Arc::clone(&empty));
    assert_ne!(
        m2.diff(BranchId::TRUNK, b(1)).unwrap(),
        diff_before,
        "an empty log produced the same diff, so this test cannot fail"
    );
}

// =================================================================================================
// Torn tails, and damage that is not at the tail
// =================================================================================================

/// **A torn tail is healed and REPORTED, never silently swallowed.**
///
/// Breaking shape: a process killed between a `pwrite` and its completion. A store that stopped
/// reading and said nothing would hand a merge a frame short of the ops it had been told about, and
/// the merge would be confidently wrong about a counter.
#[test]
fn a_partial_append_is_discarded_reported_and_then_written_over() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    {
        let log = open_on(&fabric).unwrap();
        log.append(&decrement(1, 1, 0, 1, 5)).unwrap();
        log.append(&grown(2, 1, 1, &[(1, 1)])).unwrap();
    }
    let mut image = fabric.durable_image();
    let clean_len = image.get(TEL).unwrap().len();

    // Half of another record: a length prefix promising more bytes than are there.
    let torn = image.get_mut(TEL).unwrap();
    torn.extend_from_slice(&999u32.to_be_bytes());
    torn.extend_from_slice(&[TAG_EXTEND, 0, 0, 0, 5]);
    let torn_len = torn.len();

    let restarted = SimFabric::from_images(image, None, Durability::SyncOnly);
    let log = open_on(&restarted).unwrap();
    assert_eq!(
        log.discarded_tail_bytes(),
        (torn_len - clean_len) as u64,
        "the torn tail was not reported"
    );
    assert_eq!(log.recovery().frames, 2, "everything before the tear should have survived");
    assert_eq!(
        restarted.durable_image().get(TEL).unwrap().len(),
        clean_len,
        "the tail was not healed"
    );

    // And the healed file accepts new appends that survive their own restart — a truncation that
    // left the write offset wrong would corrupt the next record instead of the last one.
    log.append(&grown(2, 1, 1, &[(1, 1), (2, 2)])).unwrap();
    log.append(&decrement(3, 1, 2, 3, 3)).unwrap();
    drop(log);
    let log = open_on(&restarted.restart()).unwrap();
    assert_eq!(log.discarded_tail_bytes(), 0);
    assert_eq!(log.recovery().frames, 3);
    assert_eq!(log.recovery().extensions, 1);
    assert_eq!(log.frame(b(1), TxnId(2)).unwrap().ops.len(), 2);
}

/// Re-CRC a record in place after editing it, so the reader believes the bytes and the check under
/// test is the semantic one rather than the checksum.
fn recrc(image: &mut BTreeMap<String, Vec<u8>>, start: usize) {
    let bytes = image.get_mut(TEL).unwrap();
    let total = u32::from_be_bytes(bytes[start..start + 4].try_into().unwrap()) as usize;
    let crc = crc32(&bytes[start..start + total - 4]);
    bytes[start + total - 4..start + total].copy_from_slice(&crc.to_be_bytes());
}

/// Offsets of every record in the file, in order.
fn record_offsets(image: &BTreeMap<String, Vec<u8>>) -> Vec<usize> {
    let bytes = image.get(TEL).unwrap();
    let mut at = HEADER_SIZE as usize;
    let mut out = Vec::new();
    while at + 4 <= bytes.len() {
        let total = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        if total < MIN_RECORD || at + total > bytes.len() {
            break;
        }
        out.push(at);
        at += total;
    }
    out
}

fn refuse(image: BTreeMap<String, Vec<u8>>, expect: &str) -> String {
    let fabric = SimFabric::from_images(image, None, Durability::SyncOnly);
    let err = open_on(&fabric).expect_err("damage in the middle of the file was accepted");
    let text = format!("{err}");
    assert!(text.contains(expect), "it failed, but not by this guard: {text}");
    text
}

/// **A `FrameExtend` naming a frame the file never declared is refused, not healed.**
///
/// This is damage in the MIDDLE of the file rather than at its tail, and healing it would mean
/// inventing a transaction: the tail alone replays to a frame short of the ops it ran, and every
/// merge computed from it would silently undercount.
#[test]
fn an_extend_for_a_frame_the_file_never_declared_is_refused() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    {
        let log = open_on(&fabric).unwrap();
        log.append(&grown(7, 1, 0, &[(1, 5)])).unwrap();
        log.append(&grown(7, 1, 0, &[(1, 5), (2, 3)])).unwrap();
    }
    let mut image = fabric.durable_image();
    let offsets = record_offsets(&image);
    assert_eq!(offsets.len(), 2, "expected one open and one extend");

    // Repoint the extend at TxnId(9), which was never opened. Layout after `total_len(4)`:
    // tag(1) | branch.id(8) | branch.generation(4) | txn_id(8) | ...
    let txn_at = offsets[1] + 4 + 1 + 8 + 4;
    image.get_mut(TEL).unwrap()[txn_at..txn_at + 8].copy_from_slice(&9u64.to_be_bytes());
    recrc(&mut image, offsets[1]);
    refuse(image, "never declared");
}

/// **A second `FrameOpen` for a key already declared is refused.** Two transactions wearing one id,
/// in the file. Composing their effects together would apply ops that never belonged to one
/// transaction, and there is no principled way to choose between them.
#[test]
fn a_second_open_for_one_key_is_refused() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    {
        let log = open_on(&fabric).unwrap();
        log.append(&decrement(7, 1, 0, 1, 5)).unwrap();
        // A second frame with the same shape and a different id, so the two records are the same
        // length and the id can be rewritten without re-framing anything.
        log.append(&decrement(8, 1, 0, 1, 5)).unwrap();
    }
    let mut image = fabric.durable_image();
    let offsets = record_offsets(&image);
    assert_eq!(offsets.len(), 2);
    let txn_at = offsets[1] + 4 + 1 + 8 + 4;
    image.get_mut(TEL).unwrap()[txn_at..txn_at + 8].copy_from_slice(&7u64.to_be_bytes());
    recrc(&mut image, offsets[1]);
    refuse(image, "already declared");
}

/// **A delta whose prior counts do not line up is refused: the hole check, by arithmetic.**
///
/// A record duplicated or dropped in the middle of the file would otherwise be concatenated
/// silently, and for `OpKind::Add` that IS the counter bug — the frame ends up holding an increment
/// twice, or not at all, with every byte matching a checksum of itself. This is the one check that
/// can see it.
#[test]
fn a_delta_whose_prior_counts_do_not_line_up_is_refused() {
    let build = || {
        let fabric = SimFabric::clean(Durability::SyncOnly);
        {
            let log = open_on(&fabric).unwrap();
            log.append(&grown(7, 1, 0, &[(1, 5)])).unwrap();
            log.append(&grown(7, 1, 0, &[(1, 5), (2, 3)])).unwrap();
            log.append(&grown(7, 1, 0, &[(1, 5), (2, 3), (3, 2)])).unwrap();
        }
        fabric.durable_image()
    };

    // (a) A delta duplicated. Its prior counts describe a frame with one op; replay has two.
    let mut image = build();
    let offsets = record_offsets(&image);
    assert_eq!(offsets.len(), 3);
    let second = image.get(TEL).unwrap()[offsets[1]..offsets[2]].to_vec();
    let mut doubled = image.get(TEL).unwrap()[..offsets[2]].to_vec();
    doubled.extend_from_slice(&second);
    doubled.extend_from_slice(&image.get(TEL).unwrap()[offsets[2]..]);
    image.insert(TEL.to_string(), doubled);
    let text = refuse(image, "missing or duplicated");
    assert!(text.contains("from 1 ops"), "the message did not name the counts: {text}");

    // (b) A delta dropped from the middle.
    let mut image = build();
    let offsets = record_offsets(&image);
    let mut holed = image.get(TEL).unwrap()[..offsets[1]].to_vec();
    holed.extend_from_slice(&image.get(TEL).unwrap()[offsets[2]..]);
    image.insert(TEL.to_string(), holed);
    refuse(image, "missing or duplicated");

    // Anti-vacuity: the untouched file opens and holds all three ops, so (a) and (b) failed on the
    // damage rather than on anything else about the fixture.
    let fabric = SimFabric::from_images(build(), None, Durability::SyncOnly);
    let log = open_on(&fabric).unwrap();
    assert_eq!(log.frame(b(1), TxnId(7)).unwrap().ops.len(), 3);
    assert_eq!(log.recovery().extensions, 2);
}

/// An unknown record tag, and trailing bytes after a record's fields, are both refused. The second
/// is `consensus::log`'s rule: a decoder that stops at the end of the fields it recognises accepts
/// two encodings of one record.
#[test]
fn an_unknown_tag_and_a_record_with_trailing_bytes_are_both_refused() {
    let build = || {
        let fabric = SimFabric::clean(Durability::SyncOnly);
        {
            let log = open_on(&fabric).unwrap();
            log.append(&decrement(7, 1, 0, 1, 5)).unwrap();
        }
        fabric.durable_image()
    };

    let mut image = build();
    let offsets = record_offsets(&image);
    image.get_mut(TEL).unwrap()[offsets[0] + 4] = 77;
    recrc(&mut image, offsets[0]);
    refuse(image, "unknown typed effect record tag 77");

    // Four extra bytes inside the record's own length, so the CRC covers them and the only thing
    // that can object is the decoder's own "did I consume it all" check.
    let mut image = build();
    let offsets = record_offsets(&image);
    let bytes = image.get_mut(TEL).unwrap();
    let total = u32::from_be_bytes(bytes[offsets[0]..offsets[0] + 4].try_into().unwrap()) as usize;
    let crc_at = offsets[0] + total - 4;
    let mut padded = bytes[..crc_at].to_vec();
    padded.extend_from_slice(&[0u8; 4]);
    padded.extend_from_slice(&bytes[crc_at..]);
    padded[offsets[0]..offsets[0] + 4].copy_from_slice(&((total + 4) as u32).to_be_bytes());
    image.insert(TEL.to_string(), padded);
    recrc(&mut image, offsets[0]);
    refuse(image, "body bytes");
}

/// A file that is not a typed effect log, and one whose header has been damaged, are both refused
/// rather than read as an empty log. An empty effect log and a misidentified file both answer "no
/// frames" for every branch, and only one of them is a fact about the database.
#[test]
fn a_foreign_file_and_a_damaged_header_are_both_refused() {
    let mut image = BTreeMap::new();
    image.insert(TEL.to_string(), b"this is somebody else's file, a long way from empty".to_vec());
    let fabric = SimFabric::from_images(image, None, Durability::SyncOnly);
    let err = open_on(&fabric).expect_err("a foreign file was opened");
    assert!(format!("{err}").contains("checksum"), "{err}");

    // A real header with one byte of its magic flipped. The CRC catches it first, which is the
    // point of checksumming a header the WAL's own format does not.
    let fabric = SimFabric::clean(Durability::SyncOnly);
    {
        let log = open_on(&fabric).unwrap();
        log.append(&decrement(1, 1, 0, 1, 5)).unwrap();
    }
    let mut image = fabric.durable_image();
    image.get_mut(TEL).unwrap()[1] ^= 0xFF;
    refuse(image, "checksum");

    // A header whose CRC agrees with a version this build does not read.
    let mut image = fabric.durable_image();
    {
        let bytes = image.get_mut(TEL).unwrap();
        bytes[4..8].copy_from_slice(&99u32.to_be_bytes());
        let crc = crc32(&bytes[0..8]);
        bytes[8..12].copy_from_slice(&crc.to_be_bytes());
    }
    refuse(image, "version 99");

    // A file too short to hold a header at all.
    let mut image = BTreeMap::new();
    image.insert(TEL.to_string(), vec![0u8; 3]);
    let fabric = SimFabric::from_images(image, None, Durability::SyncOnly);
    let err = open_on(&fabric).expect_err("a 3-byte file was opened");
    assert!(format!("{err}").contains("too short"), "{err}");

    // Anti-vacuity: an absent file is created, not refused.
    let fresh = SimFabric::clean(Durability::SyncOnly);
    assert_eq!(open_on(&fresh).expect("a fresh store was refused").len(), 0);
}

// =================================================================================================
// Refusals on the way in: what this store will not write
// =================================================================================================

/// A string longer than its length prefix can express is **refused**, not truncated.
///
/// `wal::log::write_str` writes `s.len() as u16` with a raw cast, and `Value::Decimal` documents
/// that its digit text has no cap — so an unguarded encoder would put a 65536-byte decimal on the
/// disk with a length prefix of zero, inside a record whose CRC covers exactly the bytes intended.
/// Durable, and permanently undecodable, with nothing downstream able to tell.
#[test]
fn a_value_too_long_for_its_length_prefix_is_refused_rather_than_truncated() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let log = open_on(&fabric).unwrap();
    let before = fabric.durable_image().get(TEL).map(|v| v.len()).unwrap();

    let mut f = TxnFrame::new(TxnId(1), b(1), CommitHash::ZERO, 0, 1);
    f.push_op(Op::new(
        TBL,
        RowId(1),
        Some(QTY),
        OpKind::Assign(Value::Decimal("9".repeat(u16::MAX as usize + 1))),
    ));
    let err = log.append(&f).expect_err("a 65536-byte decimal was written");
    assert!(format!("{err}").contains("over the 65535"), "{err}");
    assert_eq!(
        fabric.durable_image().get(TEL).map(|v| v.len()).unwrap(),
        before,
        "a refused append reached the file"
    );
    assert_eq!(log.len(), 0, "a refused append reached the index");

    // Exactly at the limit is accepted and survives, so the refusal is about the boundary and not
    // about long strings in general.
    let mut ok = TxnFrame::new(TxnId(2), b(1), CommitHash::ZERO, 0, 1);
    let max = "9".repeat(u16::MAX as usize);
    ok.push_op(Op::new(TBL, RowId(1), Some(QTY), OpKind::Assign(Value::Decimal(max.clone()))));
    log.append(&ok).unwrap();
    drop(log);
    let log = open_on(&fabric.restart()).unwrap();
    match &log.frame(b(1), TxnId(2)).unwrap().ops[0].kind {
        OpKind::Assign(Value::Decimal(d)) => assert_eq!(d.len(), u16::MAX as usize),
        other => panic!("the maximal decimal came back as {other:?}"),
    }
}

/// A guard nested past what the decoder can survive is refused on the way IN, so this store never
/// writes a record it could not read back. And a hand-crafted record that nests further is refused
/// on the way OUT rather than overflowing the stack — which is the failure every other malformed
/// record avoids and this one could not, since recursion is the only shape a tree decoder has.
#[test]
fn a_guard_nested_past_the_cap_is_refused_on_the_way_in_and_on_the_way_out() {
    use crate::tel::guard::GuardExpr;

    fn nest(depth: u32) -> GuardExpr {
        let mut e = GuardExpr::Literal(Value::Boolean(true));
        for _ in 0..depth {
            e = GuardExpr::Not(Box::new(e));
        }
        e
    }

    let fabric = SimFabric::clean(Durability::SyncOnly);
    let log = open_on(&fabric).unwrap();

    // At the cap: accepted, and it survives a restart.
    let mut ok = TxnFrame::new(TxnId(1), b(1), CommitHash::ZERO, 0, 1);
    ok.push_guard(Guard::holds(nest(MAX_GUARD_DEPTH)).with_source("deep but legal"));
    log.append(&ok).unwrap();

    // One past it: refused, and the refusal names the predicate.
    let mut too_deep = TxnFrame::new(TxnId(2), b(1), CommitHash::ZERO, 0, 1);
    too_deep.push_guard(Guard::holds(nest(MAX_GUARD_DEPTH + 1)).with_source("one too deep"));
    let err = log.append(&too_deep).expect_err("a guard past the cap was written");
    let text = format!("{err}");
    assert!(text.contains("one too deep"), "the refusal did not name the predicate: {text}");
    assert!(text.contains(&MAX_GUARD_DEPTH.to_string()), "{text}");
    assert_eq!(log.len(), 1, "the refused frame reached the index");
    drop(log);

    let log = open_on(&fabric.restart()).unwrap();
    assert_eq!(log.recovery().frames, 1);
    assert_eq!(log.frame(b(1), TxnId(1)).unwrap().guards.len(), 1);

    // Now the decoder's half. A record nesting 50_000 `Not`s cannot come from this encoder, so it
    // is built by hand — which is exactly the case the cap exists for: a corrupt or hostile file.
    let mut body = Vec::new();
    body.push(TAG_OPEN);
    put_key(&mut body, b(1), TxnId(3));
    body.extend_from_slice(&CommitHash::ZERO.0);
    body.extend_from_slice(&0u64.to_be_bytes());
    body.extend_from_slice(&1u32.to_be_bytes());
    body.extend_from_slice(&0u32.to_be_bytes()); // no ops
    body.extend_from_slice(&1u32.to_be_bytes()); // one guard
    body.extend(std::iter::repeat_n(6u8, 50_000)); // 50_000 nested Not tags
    body.push(0); // Literal
    body.push(3); // Boolean
    body.push(1); // true
    put_value(&mut body, &Value::Boolean(true)).unwrap(); // expected
    body.push(0); // no source text
    body.extend_from_slice(&0u32.to_be_bytes()); // no claims
    let rec = frame_record(&body).unwrap();

    let mut image = BTreeMap::new();
    let mut bytes = header_bytes().to_vec();
    bytes.extend_from_slice(&rec);
    image.insert(TEL.to_string(), bytes);
    refuse(image, "operators deep");
}

// =================================================================================================
// Aimed crashes
// =================================================================================================

/// **A crash at any point during an append leaves a log that opens, and loses nothing that an
/// earlier append had returned `Ok` for.**
///
/// Every faultable operation of one append is broken in turn, in each of the three shapes, under
/// both durability models. The invariant is exactly the promise: an append that returned `Ok` is
/// still there afterwards, and the append that was interrupted is either wholly there or wholly
/// absent — never half of a frame's ops, because half of a frame's ops is a counter that a merge
/// will get wrong.
#[test]
fn a_crash_at_any_point_during_an_append_loses_nothing_already_acknowledged() {
    let mut total = 0usize;
    for durability in [Durability::WriteThrough, Durability::SyncOnly] {
        total += sweep_an_append(durability);
    }
    // A sweep that collected nothing has not passed.
    assert!(total >= 8, "the sweep ran only {total} points, so it tested almost nothing");
}

fn sweep_an_append(durability: Durability) -> usize {
    const SEED: u64 = 0xF10;

    // A starting image from a normal, fault-free run: one three-statement task, acknowledged.
    let fabric = SimFabric::clean(durability);
    {
        let log = open_on(&fabric).unwrap();
        log.append(&grown(7, 1, 0, &[(1, 5)])).unwrap();
        log.append(&grown(7, 1, 0, &[(1, 5), (2, 3)])).unwrap();
        log.append(&decrement(8, 1, 1, 9, 4)).unwrap();
    }
    let base = fabric.restart().durable_image();

    // Census: the same next append with no fault, to learn which operation indices are faultable.
    let census = SimFabric::from_images(base.clone(), None, durability);
    let l = open_on(&census).unwrap();
    let mark = census.op_count();
    l.append(&grown(7, 1, 0, &[(1, 5), (2, 3), (3, 2)])).unwrap();
    drop(l);
    let points: Vec<u64> = census.faultable_ops().into_iter().filter(|i| *i >= mark).collect();
    assert!(!points.is_empty(), "no faultable operation in an append under {durability:?}");

    let mut ran = 0usize;
    for at in points {
        for shape in [WriteShape::Drop, WriteShape::Tear, WriteShape::Corrupt] {
            let f =
                SimFabric::from_images(base.clone(), Some(FaultPlan::at_shaped(at, SEED, shape)), durability);
            let broken = open_on(&f).unwrap();
            let outcome = broken.append(&grown(7, 1, 0, &[(1, 5), (2, 3), (3, 2)]));
            let fired = f.fired();
            let where_ = format!("{durability:?} seed {SEED:#x} at op {at} shape {shape:?}");
            assert!(fired.is_some(), "{where_}: no fault fired, so this point tested nothing");
            assert!(outcome.is_err(), "{where_}: an append survived its own crash");
            drop(broken);

            let restarted = f.restart();
            let after = match open_on(&restarted) {
                Ok(a) => a,
                Err(e) => panic!("{where_} ({:?}) left a log that will not open: {e}", fired.unwrap()),
            };
            // Everything acknowledged before the crash is still there, byte for byte.
            let held = after.frame(b(1), TxnId(7)).expect("the acknowledged frame is gone");
            assert_eq!(
                after.frame(b(1), TxnId(8)).map(|f| exactly(&f)),
                Some(exactly(&decrement(8, 1, 1, 9, 4))),
                "{where_} ({:?}) lost a frame that had been acknowledged",
                fired.clone().unwrap()
            );
            // And the interrupted append is all-or-nothing: two ops (it never landed) or three (it
            // did). Never one, and never four.
            assert!(
                held.ops.len() == 2 || held.ops.len() == 3,
                "{where_} ({:?}) left the frame holding {} ops, so an append landed in part",
                fired.clone().unwrap(),
                held.ops.len()
            );
            let expect = if held.ops.len() == 2 {
                grown(7, 1, 0, &[(1, 5), (2, 3)])
            } else {
                grown(7, 1, 0, &[(1, 5), (2, 3), (3, 2)])
            };
            assert_eq!(
                exactly(&held),
                exactly(&expect),
                "{where_} ({:?}) changed an op it had already stored",
                fired.clone().unwrap()
            );
            // The log still works after the crash it survived.
            after.append(&decrement(9, 1, 2, 4, 1)).unwrap();
            ran += 1;
        }
    }
    ran
}

/// **A failed append costs exactly one append, and nothing is latched off.**
///
/// `provenance::durable` poisons its store when a write fails, because its index has already
/// changed by then and every later write would deepen the disagreement. This store writes the
/// record first, so a failure leaves the index and the write offset exactly where they were: the
/// retry writes at the same offset, over whatever the failure left, and the file a later process
/// opens is the one the caller was told about. Breaking shape: a flush that reports failure, which
/// cannot happen against a real temp file and is therefore aimed.
#[test]
fn a_failed_append_leaves_a_store_that_still_works_and_a_file_that_still_opens() {
    let durability = Durability::WriteThrough;
    let fabric = SimFabric::clean(durability);
    {
        let log = open_on(&fabric).unwrap();
        log.append(&decrement(1, 1, 0, 1, 5)).unwrap();
    }
    let base = fabric.restart().durable_image();

    // Find the first faultable operation of the next append, and break it.
    let census = SimFabric::from_images(base.clone(), None, durability);
    let l = open_on(&census).unwrap();
    let mark = census.op_count();
    l.append(&decrement(2, 1, 1, 2, 3)).unwrap();
    drop(l);
    let at = *census
        .faultable_ops()
        .iter()
        .find(|i| **i >= mark)
        .expect("an append performs no faultable operation");

    let f = SimFabric::from_images(base, Some(FaultPlan::at_shaped(at, 0xF10, WriteShape::Drop)), durability);
    let log = open_on(&f).unwrap();
    let err = log.append(&decrement(2, 1, 1, 2, 3)).expect_err("a dropped write was swallowed");
    assert!(format!("{err}").contains("simulated"), "{err}");
    assert!(f.fired().is_some(), "no fault fired");

    // The index did not take the frame the file did not.
    assert!(log.frame(b(1), TxnId(2)).is_none(), "the index accepted an append the file refused");
    assert_eq!(log.len(), 1);
    // Reads still answer: what was already recorded is still true.
    assert_eq!(log.frame(b(1), TxnId(1)).unwrap().ops.len(), 1);

    // And the file is one a later process can open, holding exactly what was acknowledged.
    drop(log);
    let log = open_on(&f.restart()).expect("the file became unopenable");
    assert_eq!(log.recovery().frames, 1);
    assert_eq!(log.discarded_tail_bytes(), 0);
    assert!(log.frame(b(1), TxnId(2)).is_none());
    // Anti-vacuity: the reopened store is not latched off, so the refusal above was about the
    // fault and not about this store having stopped accepting anything.
    log.append(&decrement(2, 1, 1, 2, 3)).expect("a freshly opened store refused an append");
}

// =================================================================================================
// The real filesystem
// =================================================================================================

/// Everything above runs on the simulated fabric. This runs the path a database actually takes, so
/// a divergence between `impl Storage for File` and the fabric cannot hide — and it is what proves
/// `default_for_database` names the file the convention says it does.
#[test]
fn the_log_opens_on_a_real_file_and_recovers_from_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("agent.db");
    let db_str = db.to_str().unwrap().to_string();

    {
        let log = DurableEffectLog::default_for_database(&db_str).unwrap();
        log.append(&grown(7, 1, 0, &[(1, 5)])).unwrap();
        log.append(&grown(7, 1, 0, &[(1, 5), (2, 3)])).unwrap();
        log.append(&kitchen_sink(8, 1)).unwrap();
    }
    let tel = dir.path().join("agent.db.tel");
    assert!(tel.exists(), "default_for_database did not write <db>.tel");

    let log = DurableEffectLog::open(&tel).unwrap();
    assert_eq!(log.recovery().frames, 2);
    assert_eq!(log.recovery().extensions, 1);
    assert_eq!(log.discarded_tail_bytes(), 0);
    assert_eq!(log.frame(b(1), TxnId(7)).unwrap().ops.len(), 2);
    assert_eq!(exactly(&log.frame(b(1), TxnId(8)).unwrap()), exactly(&kitchen_sink(8, 1)));
    assert!(log.name().ends_with("agent.db.tel"), "{}", log.name());
    assert!(format!("{log:?}").contains("DurableEffectLog"));

    // A torn tail on a real file, healed and reported.
    let clean = std::fs::metadata(&tel).unwrap().len();
    let mut bytes = std::fs::read(&tel).unwrap();
    bytes.extend_from_slice(&4_000u32.to_be_bytes());
    bytes.push(TAG_OPEN);
    std::fs::write(&tel, &bytes).unwrap();
    let log = DurableEffectLog::open(&tel).unwrap();
    assert_eq!(log.discarded_tail_bytes(), bytes.len() as u64 - clean);
    assert_eq!(std::fs::metadata(&tel).unwrap().len(), clean);
    assert_eq!(log.frame(b(1), TxnId(7)).unwrap().ops.len(), 2);
}
