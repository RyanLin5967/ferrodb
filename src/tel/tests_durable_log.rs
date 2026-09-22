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
use std::sync::{Arc, Mutex};

use super::*;
use crate::storage::sim::{Durability, FaultPlan, SimFabric, WriteShape};
use crate::storage::storage::Storage;
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

/// **The two stores hold the identical frame, not merely an equal one.**
///
/// `Value`'s equality is numeric: `Integer(1) == Float(1.0)`, `Decimal("1.50") == Decimal("1.5")`.
/// So `extends` accepts a re-append that *retypes* a value already stored, and the question is what
/// each store then holds. The durable one writes only the new tail, so the disk keeps the bytes it
/// first accepted. `MemEffectLog` used to answer that by replacing the whole frame — which left the
/// live store holding the new variant and the file holding the old one, so a restart silently
/// changed which variant a merge composed. Extending instead makes them agree by construction.
///
/// Breaking shape: exactly this — a growth whose prefix is numerically equal and differently typed.
/// No producer sends it (`stage_all` only pushes), which is why it is built by hand rather than
/// hoped for from a workload.
#[test]
fn retyping_a_stored_value_under_growth_cannot_split_the_two_stores() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    let assign = |v: Value| Op::new(TBL, RowId(1), Some(QTY), OpKind::Assign(v));

    let mut first = TxnFrame::new(TxnId(7), b(1), CommitHash::ZERO, 0, 1);
    first.push_op(assign(Value::Integer(1)));
    let mut retyped = TxnFrame::new(TxnId(7), b(1), CommitHash::ZERO, 0, 1);
    retyped.push_op(assign(Value::Float(1.0)));
    retyped.push_op(assign(Value::Decimal("2.50".into())));
    // The premise: `extends` really does accept this, because the prefix compares equal.
    assert_eq!(first.ops[0], retyped.ops[0], "the two prefixes are not numerically equal");
    assert_ne!(format!("{:?}", first.ops[0]), format!("{:?}", retyped.ops[0]));

    let live = {
        let log = open_on(&fabric).unwrap();
        log.append(&first).unwrap();
        log.append(&retyped).unwrap();
        exactly(&log.frame(b(1), TxnId(7)).unwrap())
    };
    let log = open_on(&fabric.restart()).unwrap();
    assert_eq!(
        exactly(&log.frame(b(1), TxnId(7)).unwrap()),
        live,
        "the reopened store holds a differently typed value from the live one, so a restart changed \
         which variant a merge composes"
    );
    // And the value that survived is the one the disk first accepted, stated so the direction is
    // pinned rather than left to whichever store happened to win.
    match &log.frame(b(1), TxnId(7)).unwrap().ops[0].kind {
        OpKind::Assign(Value::Integer(1)) => {}
        other => panic!("the first-accepted variant did not survive: {other:?}"),
    }
    assert_eq!(log.frame(b(1), TxnId(7)).unwrap().ops.len(), 2);
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

    // **A file too short to hold a header is REINITIALISED, not refused**, and that boundary is
    // exact rather than generous. The very first thing a new log does is write its 12-byte header,
    // so a crash that tears that write leaves a few bytes and no records; refusing there would
    // brick a log over a file that has never held anything. Records begin at `HEADER_SIZE`, so
    // nothing can be lost by writing over a file no longer than it.
    for short in [1usize, 3, HEADER_SIZE as usize] {
        let mut image = BTreeMap::new();
        image.insert(TEL.to_string(), vec![0u8; short]);
        let fabric = SimFabric::from_images(image, None, Durability::SyncOnly);
        let log = open_on(&fabric)
            .unwrap_or_else(|e| panic!("a {short}-byte file was refused instead of initialised: {e}"));
        assert_eq!(log.len(), 0);
        log.append(&decrement(1, 1, 0, 1, 5)).unwrap();
        drop(log);
        let log = open_on(&fabric.restart()).unwrap();
        assert_eq!(log.recovery().frames, 1, "the reinitialised log did not keep its first frame");
    }

    // One byte PAST the header is where the refusal starts, because that byte could be a record's.
    let mut image = BTreeMap::new();
    image.insert(TEL.to_string(), vec![0u8; HEADER_SIZE as usize + 1]);
    let fabric = SimFabric::from_images(image, None, Durability::SyncOnly);
    let err = open_on(&fabric).expect_err("a garbage file with room for a record was opened");
    assert!(format!("{err}").contains("room for"), "{err}");

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

    // **And the refusal does not WALK the tree it is refusing.**
    //
    // `Guard::violated_predicate` falls back to `expr.to_string()` when there is no source text,
    // and `GuardExpr`'s `Display` is recursive — so building the message that way would recurse over
    // the whole tree on the one input the cap exists to protect against, which is a stack overflow
    // reached through the refusal path. The observable form of "does not walk it" is that the
    // message does not contain the rendered expression, and a synthesised guard is the only case
    // where the two differ.
    let mut synthesised = TxnFrame::new(TxnId(3), b(1), CommitHash::ZERO, 0, 1);
    synthesised.push_guard(Guard::new(nest(MAX_GUARD_DEPTH + 1), Value::Boolean(true)));
    let err = log.append(&synthesised).expect_err("a synthesised guard past the cap was written");
    let text = format!("{err}");
    assert!(
        text.contains("synthesised") && text.contains("no source text"),
        "the refusal did not say the guard carries no source: {text}"
    );
    assert!(
        !text.contains("NOT"),
        "the refusal rendered the expression it was refusing, so it walked the whole tree: {} \
         chars of message",
        text.len()
    );
    assert!(
        text.len() < 400,
        "the refusal is {} chars long, which is the rendered tree rather than a message",
        text.len()
    );
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

/// **A growth whose tail carries GUARDS and CLAIMS, not only ops.**
///
/// This is the shape `stage_all` produces on the second WHERE-carrying statement of every agent
/// task, and until this test existed every `FrameExtend` in the whole suite had an empty guards tail
/// and an empty claims tail — so `put_tail(.., &frame.ops[ops..], &[], &[])` would have kept the
/// suite green. `frame.rs` states the stakes itself: guards are not derivable from ops by anything,
/// so a guard dropped on the way to the disk cannot be reconstructed. The production failure is
/// quiet and expensive: the merge engine re-checks guards as preconditions, so a guard that never
/// replayed is never re-evaluated, and a merge that should be a `Conflict` comes back `Clean`.
#[test]
fn a_growth_whose_tail_carries_guards_and_claims_replays_with_them() {
    use crate::tel::guard::{CmpOp, GuardExpr};

    let guard = |n: i64| {
        Guard::holds(GuardExpr::cmp(
            GuardExpr::col(TBL, RowId(1), QTY),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(n as i32)),
        ))
        .with_source(format!("qty >= {n}"))
    };
    let claim = |n: i64| EscrowClaim {
        tbl: TBL,
        row: RowId(1),
        col: QTY,
        amount: Delta::Int(-n),
        floor: Some(Value::Integer(0)),
        ceiling: None,
    };

    let fabric = SimFabric::clean(Durability::SyncOnly);
    let mut f = TxnFrame::new(TxnId(7), b(1), CommitHash::ZERO, 0, 1);
    let live = {
        let log = open_on(&fabric).unwrap();

        // Statement one: an op, a guard and a claim.
        f.push_op(Op::new(TBL, RowId(1), Some(QTY), OpKind::Add(Delta::Int(-5))));
        f.push_guard(guard(5));
        f.push_claim(claim(5));
        log.append(&f).unwrap();

        // Statement two: the frame grows in ALL THREE vectors at once.
        f.push_op(Op::new(TBL, RowId(2), Some(QTY), OpKind::Add(Delta::Int(-3))));
        f.push_guard(guard(3));
        f.push_claim(claim(3));
        log.append(&f).unwrap();

        // Statement three grows guards only, so a tail that is empty in one vector and not another
        // is exercised too — the counts are per-vector and a shared count would pass the first case.
        f.push_guard(guard(1));
        log.append(&f).unwrap();

        let held = log.frame(b(1), TxnId(7)).unwrap();
        assert_eq!((held.ops.len(), held.guards.len(), held.claims.len()), (2, 3, 2));
        exactly(&held)
    };

    let log = open_on(&fabric.restart()).unwrap();
    assert_eq!(log.recovery().extensions, 2, "the growth was not stored as two deltas");
    let back = log.frame(b(1), TxnId(7)).unwrap();
    assert_eq!(
        (back.ops.len(), back.guards.len(), back.claims.len()),
        (2, 3, 2),
        "a guard or a claim was dropped on the way to the disk: {} ops / {} guards / {} claims",
        back.ops.len(),
        back.guards.len(),
        back.claims.len()
    );
    assert_eq!(exactly(&back), live, "the grown frame did not survive byte for byte");
    assert_eq!(exactly(&back), exactly(&f));
    // The predicates themselves, which are what an agent is handed back on a violation.
    let sources: Vec<String> = back.guards.iter().map(|g| g.violated_predicate()).collect();
    assert_eq!(sources, vec!["qty >= 5", "qty >= 3", "qty >= 1"]);
}

/// **A frame carrying a NaN delta can still be retried and still grow.**
///
/// `Delta` derives `PartialEq`, so `Float(NAN) != Float(NAN)`; `Value` avoids that by comparing
/// through `Ord`/`total_cmp`, but `OpKind::Add` and `EscrowClaim::amount` carry a `Delta` and bypass
/// `Value`. Without [`delta_eq`] the byte-identical retry below is reported as *a contradiction* —
/// "two transactions wearing one id" — and since `stage_all` re-appends the open frame once per
/// statement, that task's frame could never grow again.
///
/// Reachability, stated rather than assumed: not from SQL, because the parser yields ±inf and never
/// NaN. It is reachable through the public `EffectLog::append`, and through `Delta::compose`, which
/// turns `inf + -inf` into NaN. That is why this is a test and not a comment.
#[test]
fn a_nan_delta_does_not_turn_a_retry_into_a_contradiction() {
    // The premise: composing two ordinary deltas really does produce NaN.
    assert!(matches!(
        Delta::Float(f64::INFINITY).compose(&Delta::Float(f64::NEG_INFINITY)).unwrap(),
        Delta::Float(x) if x.is_nan()
    ));

    let fabric = SimFabric::clean(Durability::SyncOnly);
    let mut f = TxnFrame::new(TxnId(7), b(1), CommitHash::ZERO, 0, 1);
    f.push_op(Op::new(TBL, RowId(1), Some(QTY), OpKind::Add(Delta::Float(f64::NAN))));
    f.push_claim(EscrowClaim {
        tbl: TBL,
        row: RowId(1),
        col: QTY,
        amount: Delta::Float(f64::NAN),
        floor: None,
        ceiling: None,
    });

    let live = {
        let log = open_on(&fabric).unwrap();
        log.append(&f).unwrap();
        // The retry. Under the derived `PartialEq` this is an Err.
        log.append(&f).expect("a byte-identical retry of a NaN-carrying frame was refused");
        assert_eq!(log.len(), 1, "the retry was stored as a second frame");
        assert_eq!(log.frame(b(1), TxnId(7)).unwrap().ops.len(), 1, "the NaN Add was doubled");

        // And it can still grow, which is the half that matters to a multi-statement task.
        f.push_op(Op::new(TBL, RowId(2), Some(QTY), OpKind::Add(Delta::Int(-3))));
        log.append(&f).expect("a NaN-carrying frame could not grow");
        exactly(&log.frame(b(1), TxnId(7)).unwrap())
    };

    // `MemEffectLog` takes the same path, because `classify` is shared.
    let mem = MemEffectLog::new();
    mem.append(&f).unwrap();
    mem.append(&f).expect("MemEffectLog refused a NaN-carrying retry");
    assert_eq!(mem.len(), 1);

    let log = open_on(&fabric.restart()).unwrap();
    let back = log.frame(b(1), TxnId(7)).unwrap();
    assert_eq!(exactly(&back), live, "the NaN did not survive byte for byte");
    match back.ops[0].kind {
        OpKind::Add(Delta::Float(x)) => assert!(x.is_nan(), "the NaN came back as {x}"),
        ref other => panic!("the NaN op came back as {other:?}"),
    }
    // Anti-vacuity: a frame that genuinely contradicts is still refused, so the acceptance above is
    // about NaN and not about the contradiction check having been switched off.
    let mut rewritten = TxnFrame::new(TxnId(7), b(1), CommitHash::ZERO, 0, 1);
    rewritten.push_op(Op::new(TBL, RowId(1), Some(QTY), OpKind::Add(Delta::Float(1.0))));
    assert!(log.append(&rewritten).is_err(), "a rewritten prefix was accepted");
}

/// **A non-canonical boolean byte is refused.**
///
/// `take_u8(..) != 0` would give one value 255 encodings, which is the record-level defect the
/// trailing-bytes check refuses: two byte sequences that decode identically mean two files can be
/// byte-different and indistinguishable. Every other presence tag in this format already refuses
/// anything but 0 and 1; the boolean was the odd one out.
#[test]
fn a_boolean_byte_that_is_neither_zero_nor_one_is_refused() {
    let fabric = SimFabric::clean(Durability::SyncOnly);
    {
        let log = open_on(&fabric).unwrap();
        let mut f = TxnFrame::new(TxnId(1), b(1), CommitHash::ZERO, 0, 1);
        f.push_op(Op::new(TBL, RowId(1), Some(QTY), OpKind::Assign(Value::Boolean(true))));
        log.append(&f).unwrap();
    }
    let mut image = fabric.durable_image();
    // The only `true` byte in the file is the one the Assign wrote. Find the last 1 inside the
    // record and set it to 2; re-CRC so the reader believes the bytes.
    let offsets = record_offsets(&image);
    {
        let bytes = image.get_mut(TEL).unwrap();
        let total =
            u32::from_be_bytes(bytes[offsets[0]..offsets[0] + 4].try_into().unwrap()) as usize;
        // ... | kind tag 2 (Assign) | value tag 3 (Boolean) | 0x01 | witness-absent 0 | 3 counts...
        let at = bytes[offsets[0]..offsets[0] + total]
            .windows(3)
            .position(|w| w == [3u8, 1u8, 0u8])
            .expect("the encoded boolean was not found")
            + offsets[0]
            + 1;
        assert_eq!(bytes[at], 1);
        bytes[at] = 2;
    }
    recrc(&mut image, offsets[0]);
    refuse(image, "neither false");
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
    assert!(total >= 40, "the sweep ran only {total} points, so it tested almost nothing");
}

fn sweep_an_append(durability: Durability) -> usize {
    const SEED: u64 = 0xF10;

    // The workload: everything a fresh log does. The header write and its fsync, one `FrameOpen`,
    // two `FrameExtend`s as the task grows, and a second transaction — so the fault lands at every
    // operation of a real agent task rather than only inside one append.
    let workload = |log: &DurableEffectLog| -> Vec<usize> {
        let mut acked = Vec::new();
        for (n, rows) in [
            (1usize, &[(1u64, 5i64)][..]),
            (2, &[(1, 5), (2, 3)][..]),
            (3, &[(1, 5), (2, 3), (3, 2)][..]),
        ] {
            if log.append(&grown(7, 1, 0, rows)).is_err() {
                return acked;
            }
            acked.push(n);
        }
        if log.append(&decrement(8, 1, 1, 9, 4)).is_ok() {
            acked.push(99);
        }
        acked
    };

    // Census: the same workload with no fault, from an EMPTY fabric, to learn every faultable
    // operation index — the header's included.
    let census = SimFabric::clean(durability);
    let l = open_on(&census).unwrap();
    assert_eq!(workload(&l).len(), 4, "the census run did not complete under {durability:?}");
    drop(l);
    let points = census.faultable_ops();
    assert!(
        points.len() >= 6,
        "only {} faultable ops in a whole task under {durability:?}",
        points.len()
    );

    let mut ran = 0usize;
    for at in points {
        for shape in [WriteShape::Drop, WriteShape::Tear, WriteShape::Corrupt] {
            let f = SimFabric::with_fault(FaultPlan::at_shaped(at, SEED, shape), durability);
            let where_ = format!("{durability:?} seed {SEED:#x} at op {at} shape {shape:?}");
            // The fault can land on the header write, so opening is itself allowed to fail.
            let acked = match open_on(&f) {
                Ok(log) => {
                    let acked = workload(&log);
                    drop(log);
                    acked
                }
                Err(_) => Vec::new(),
            };
            let fired = f.fired();
            assert!(fired.is_some(), "{where_}: no fault fired, so this point tested nothing");

            let restarted = f.restart();
            let after = match open_on(&restarted) {
                Ok(a) => a,
                Err(e) => panic!("{where_} ({:?}) left a log that will not open: {e}", fired.unwrap()),
            };

            // **Everything acknowledged is still there, and nothing that was not is invented.**
            let held = after.frame(b(1), TxnId(7));
            let ops = held.as_ref().map(|f| f.ops.len()).unwrap_or(0);
            let highest = acked.iter().copied().filter(|n| *n <= 3).max().unwrap_or(0);
            assert!(
                ops >= highest,
                "{where_} ({:?}) came back with {ops} ops after {highest} statements had been \
                 acknowledged",
                fired.clone().unwrap()
            );
            // All-or-nothing per append: never half of a statement's effects, because half of a
            // statement's effects is a counter a merge will get wrong and never notice.
            assert!(ops <= 3, "{where_} ({:?}) invented ops: {ops}", fired.clone().unwrap());
            if let Some(f7) = &held {
                let rows: Vec<(u64, i64)> = [(1u64, 5i64), (2, 3), (3, 2)][..ops].to_vec();
                assert_eq!(
                    exactly(f7),
                    exactly(&grown(7, 1, 0, &rows)),
                    "{where_} ({:?}) changed effects it had already stored",
                    fired.clone().unwrap()
                );
            }
            if acked.contains(&99) {
                assert_eq!(
                    after.frame(b(1), TxnId(8)).map(|f| exactly(&f)),
                    Some(exactly(&decrement(8, 1, 1, 9, 4))),
                    "{where_} ({:?}) lost an acknowledged frame",
                    fired.clone().unwrap()
                );
            }
            // The log still works after the crash it survived.
            after.append(&decrement(10, 1, 2, 4, 1)).unwrap_or_else(|e| {
                panic!("{where_} ({:?}) left a log that refuses appends: {e}", fired.unwrap())
            });
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

/// A [`Storage`] that fails exactly one read, at a chosen offset, and behaves normally otherwise.
///
/// Written here rather than added to `storage::sim` because the fabric declares reads unfaultable,
/// and its stated reason — "a failed read leaves the durable image untouched, so it cannot produce
/// the class of bug this harness hunts" — is precisely the assumption a scan that truncates on a
/// read error breaks. Widening the fabric is the right fix for the tree and is not this row's file.
struct FailOneRead {
    bytes: Mutex<Vec<u8>>,
    fail_at: u64,
    /// Which read at that offset to break. **The scan reads the 4-byte length prefix and then the
    /// whole record at the SAME offset**, so an offset alone names two reads and only ever reaches
    /// the first — a mutant that reverted just the second one survived until this field existed.
    /// `Some(4)` breaks the length prefix, `Some(n)` the body, `None` either.
    fail_len: Option<usize>,
    reads_failed: Mutex<usize>,
}

impl FailOneRead {
    fn new(bytes: Vec<u8>, fail_at: u64, fail_len: Option<usize>) -> Arc<Self> {
        Arc::new(FailOneRead {
            bytes: Mutex::new(bytes),
            fail_at,
            fail_len,
            reads_failed: Mutex::new(0),
        })
    }
}

impl crate::storage::storage::Storage for FailOneRead {
    fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        let mut b = self.bytes.lock().unwrap();
        let end = offset as usize + buf.len();
        if b.len() < end {
            b.resize(end, 0);
        }
        b[offset as usize..end].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn pread(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        if offset == self.fail_at && self.fail_len.is_none_or(|n| n == buf.len()) {
            *self.reads_failed.lock().unwrap() += 1;
            return Err(std::io::Error::other("simulated media error: EIO"));
        }
        let b = self.bytes.lock().unwrap();
        if offset as usize >= b.len() {
            return Ok(0);
        }
        let n = buf.len().min(b.len() - offset as usize);
        buf[..n].copy_from_slice(&b[offset as usize..offset as usize + n]);
        Ok(n)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.bytes.lock().unwrap().resize(len as usize, 0);
        Ok(())
    }

    fn len(&self) -> std::io::Result<u64> {
        Ok(self.bytes.lock().unwrap().len() as u64)
    }
}

/// **A read that fails mid-file refuses the open. It does NOT truncate away the records after it.**
///
/// Both bounds in the scan are proved against the length `Storage::len` reported, so neither record
/// read can fail on end-of-data: the only way in is a real I/O error. An earlier version of this
/// scan wrote `if pread_all(..).is_err() { break offset }`, which made an unreadable sector
/// indistinguishable from a torn tail — so the heal `set_len`'d away every **undamaged** record
/// after it, `open` returned `Ok`, and the store came back short of effects it had reported durable
/// with the records themselves gone from the media. That is the worst outcome this module has: a
/// merge computed from it is confidently wrong, and nothing anywhere can tell.
///
/// Breaking shape: exactly that — a bad sector under one record of an otherwise intact file.
#[test]
fn a_read_error_mid_file_refuses_the_open_and_destroys_nothing() {
    // A clean file with three records: two frames and a growth.
    let fabric = SimFabric::clean(Durability::SyncOnly);
    {
        let log = open_on(&fabric).unwrap();
        log.append(&grown(7, 1, 0, &[(1, 5)])).unwrap();
        log.append(&grown(7, 1, 0, &[(1, 5), (2, 3)])).unwrap();
        log.append(&kitchen_sink(8, 1)).unwrap();
    }
    let image = fabric.durable_image();
    let bytes = image.get(TEL).unwrap().clone();
    let offsets = record_offsets(&image);
    assert_eq!(offsets.len(), 3, "expected three records to have something to lose");

    // The scan performs TWO reads at each record's offset — the 4-byte length prefix, then the
    // whole record — so **both** have to be broken, one at a time. Breaking only the first leaves
    // the second unexercised, and a mutant that reverted the second read alone survived until this
    // loop existed.
    let second_total =
        u32::from_be_bytes(bytes[offsets[1]..offsets[1] + 4].try_into().unwrap()) as usize;
    for (what, fail_len) in
        [("the length prefix", 4usize), ("the record body", second_total)]
    {
        let media = FailOneRead::new(bytes.clone(), offsets[1] as u64, Some(fail_len));
        let err = DurableEffectLog::with_storage(TEL, Arc::clone(&media) as Arc<dyn Storage>)
            .err()
            .unwrap_or_else(|| panic!("a read error on {what} was healed into a torn tail"));
        let text = format!("{err}");
        assert!(
            text.contains("refuses to open"),
            "{what}: it failed, but not by this guard: {text}"
        );
        assert!(
            text.contains("EIO"),
            "{what}: the refusal did not carry the underlying error: {text}"
        );
        assert_eq!(*media.reads_failed.lock().unwrap(), 1, "{what}: the fault did not fire");

        // **Nothing was destroyed.** Byte for byte, the file is what it was.
        assert_eq!(
            *media.bytes.lock().unwrap(),
            bytes,
            "{what}: the refused open truncated the file anyway, so the records after the bad \
             sector are gone"
        );
    }

    // And the healthy media still opens and still holds everything, which is what proves the
    // refusal was about the read and not about the file.
    let healthy = FailOneRead::new(bytes, u64::MAX, None);
    let log = DurableEffectLog::with_storage(TEL, healthy as Arc<dyn Storage>).unwrap();
    assert_eq!(log.recovery().frames, 2);
    assert_eq!(log.recovery().extensions, 1);
    assert_eq!(log.discarded_tail_bytes(), 0);
    assert_eq!(log.frame(b(1), TxnId(7)).unwrap().ops.len(), 2);
    assert_eq!(exactly(&log.frame(b(1), TxnId(8)).unwrap()), exactly(&kitchen_sink(8, 1)));
}

/// Appends from many threads at once land in the file, all of them, exactly once each.
///
/// The store hands out `&self` and every entry point is `Arc<dyn EffectLog>` shared across
/// `pgwire`'s thread-per-connection model, so this is the shape production takes. The claim under
/// test is the one the append path's comment makes: `end` and the index advance together under one
/// lock, so no two threads can write at the same offset and no record can be lost between them.
#[test]
fn concurrent_appends_all_reach_the_file_exactly_once() {
    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 25;

    let fabric = SimFabric::clean(Durability::WriteThrough);
    {
        let log: Arc<dyn EffectLog> = Arc::new(open_on(&fabric).unwrap());
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let log = Arc::clone(&log);
            handles.push(std::thread::spawn(move || {
                for i in 0..PER_THREAD {
                    // A distinct frame per (thread, i), then grown once, so both record kinds are
                    // written concurrently rather than only the simple one.
                    let txn = t * PER_THREAD + i + 1;
                    log.append(&grown(txn, 1, i, &[(1, 5)])).unwrap();
                    log.append(&grown(txn, 1, i, &[(1, 5), (2, 3)])).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().expect("a thread panicked");
        }
    }

    let log = open_on(&fabric.restart()).unwrap();
    let total = (THREADS * PER_THREAD) as usize;
    assert_eq!(log.recovery().frames, total, "a frame was lost or written twice");
    assert_eq!(log.recovery().extensions, total, "a growth was lost or written twice");
    assert_eq!(log.discarded_tail_bytes(), 0);
    for t in 0..THREADS {
        for i in 0..PER_THREAD {
            let txn = t * PER_THREAD + i + 1;
            let f = log
                .frame(b(1), TxnId(txn))
                .unwrap_or_else(|| panic!("txn{txn} is missing after the restart"));
            assert_eq!(f.ops.len(), 2, "txn{txn} came back with {} ops", f.ops.len());
            assert_eq!(exactly(&f), exactly(&grown(txn, 1, i, &[(1, 5), (2, 3)])));
        }
    }
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

/// **D138 — the position index is rebuilt correctly by REPLAY, not only by live appends.**
///
/// ⚠ **Scoped to what `replay` actually does, because the first version of this docstring claimed
/// more than the path can deliver.** It said replay "drives both index paths — a `FrameOpen`
/// pushes a new key, a `FrameExtend` grows a frame at the position the earlier open put it". It
/// does not. `DurableEffectLog::replay` folds every `Record::Extend` into a **local** `built`
/// map and only then runs `for key in order { mem.append(&built.remove(&key)) }` — exactly one
/// `append` per *distinct* key, every one of them a miss taking `Frames::push`. **The
/// `Reappend::Grew` arm is never reached during a replay**, and `recovery().extensions` counts
/// `Record::Extend` records decoded from the FILE, which proves `DurableEffectLog::append` chose
/// the delta encoding — not that the in-memory grow path ran. That path is covered instead by
/// phase 2 of `tests::the_position_index_and_the_frames_agree_per_key`.
///
/// What this test does prove, which nothing else does: **replay rebuilds the index at the
/// positions a live store would have pushed them**, in file order rather than in the
/// nondeterministic iteration order of `built`. A replay that fed the HashMap's order would still
/// recover every frame and still satisfy a membership check — and would put them at different
/// positions, which is what the `order`-vs-index assertion below catches.
///
/// Membership is asserted **per key in both directions** by `assert_index_agrees`; the
/// `frame()`-per-key loop is the behavioural half, so a correct-looking index that no lookup
/// actually reads cannot pass this.
#[test]
fn the_position_index_survives_a_replay_intact() {
    let fabric = SimFabric::clean(Durability::SyncOnly);

    // Interleaved across branches, with growth landing on frames that are NOT at the tail — so a
    // replay that extended "the last frame" rather than "the frame at this key's position" is
    // caught.
    let mut order: Vec<(BranchId, TxnId)> = Vec::new();
    {
        let log = open_on(&fabric).unwrap();
        for txn in 1u64..=3 {
            for branch in 1u64..=4 {
                log.append(&decrement(txn, branch, 0, 1, 1)).unwrap();
                order.push((b(branch), TxnId(txn)));
            }
        }
        // Grow three frames that are already buried under later ones.
        for (txn, branch) in [(1u64, 2u64), (2, 4), (1, 1)] {
            log.append(&grown(txn, branch, 0, &[(1, 1), (5, 2)])).unwrap();
        }
        log.append(&kitchen_sink(7, 9)).unwrap();
        order.push((b(9), TxnId(7)));
        log.mem.assert_index_agrees("before the restart");
    }

    let log = DurableEffectLog::with_storage(TEL, fabric.restart().open(TEL)).unwrap();
    assert_eq!(log.recovery().frames, 13, "the frames were not all replayed");
    assert_eq!(log.recovery().extensions, 3, "the three growths did not replay as extensions");

    log.mem.assert_index_agrees("after the replay");

    // The index must send each key to the position this test's own append order put it in — and
    // the grown frames must still be at their original positions, not moved to the tail.
    {
        let g = log.mem.frames.lock().unwrap();
        for (want_at, key) in order.iter().enumerate() {
            assert_eq!(
                g.by_key.get(key),
                Some(&want_at),
                "after replay, {key:?} was the {want_at}th key opened and the index disagrees"
            );
        }
    }

    // Behavioural half: every key is reachable through the lookup that reads the index, and a key
    // that was never written is not.
    for (branch, txn) in &order {
        let got = log
            .frame(*branch, *txn)
            .unwrap_or_else(|| panic!("frame() lost {branch:?}/{txn:?} across the replay"));
        assert_eq!((got.branch, got.txn_id), (*branch, *txn));
    }
    assert_eq!(log.frame(b(2), TxnId(1)).unwrap().ops.len(), 2, "a grown frame lost its tail");
    assert_eq!(log.frame(b(3), TxnId(1)).unwrap().ops.len(), 1, "an ungrown frame gained ops");
    assert!(log.frame(b(1), TxnId(999)).is_none(), "the replayed index invented a key");
}
