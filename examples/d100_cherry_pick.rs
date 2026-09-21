//! **D100 — what a cherry-pick costs, on two axes.**
//!
//! The claim this harness exists to test is the module header's: cherry-pick over an op log costs
//! **O(picked)**, not O(log), because it reads divergence through D86's `(tbl,row,col)` key and a
//! `partition_point` rather than scanning the log. One axis cannot separate those — a curve over
//! picked ops at a fixed log size is consistent with both — so this runs **both**:
//!
//!   AXIS 1:  picked ops 1 / 10 / 100 / 1000, log held at a fixed size.
//!   AXIS 2:  log size 1e3 / 1e4 / 1e5, picked ops held at 100.
//!   AXIS 2b: THE SAME as axis 2 against a log with no by-cell index, so axis 2's slope has
//!            something to be compared against rather than being read on its own.
//!
//! Axis 2 is the control for axis 1. **Read it against axis 2b and never on its own**: axis 2's
//! per-pick cost is not flat in the log size, so "it rose, therefore the pick is scanning" is not
//! a reading this harness supports. What separates the two hypotheses is the gap to the unindexed
//! arm over the same range — measured 5-6x against 41-45x over two runs on a machine that was
//! also building other work, so treat each figure as an upper bound and the GAP as the result.
//! The residual rise in axis 2 is real and its cause is not established here.
//!
//! Every reported number is wall time from `std::time::Instant`, median over the reps named in
//! the output. Two columns are reported and they are not the same measurement:
//!
//!   `plan us`        — `cherry::plan_cherry_pick` alone. THE ENGINE. This is what the O(picked)
//!                      claim is about, and it touches the target zero times.
//!   `plan+commit us` — that plus `MemCherryTarget::commit_all`, which is the TEST DOUBLE's cost,
//!                      not this module's.
//!
//! They are separated because the first version of this harness reported only the combined number
//! and the double's cost dominated it: `commit_all` then cloned the whole row map, so axis 2 read
//! 91.8x across a 100x larger target and the curve was measuring the harness. Target reset happens
//! outside every timed region.
//!
//! Run: `cargo run --release --example d100_cherry_pick`

use std::time::Instant;

use ferrodb::agent_sql::merge_engine::PolicyTable;
use ferrodb::branch::cherry::{
    cherry_pick, plan_cherry_pick, CherryConflictKind, CherryLog, MemCherryLog, MemCherryTarget,
    OpSelector, RecordedOp,
};
use ferrodb::branch::types::BranchId;
use ferrodb::catalog::column::Value;
use ferrodb::tel::ids::{ColId, RowId, TableId, TxnId};
use ferrodb::tel::op::{Delta, OpKind};

const T: TableId = TableId(1);
const C: ColId = ColId(1);

fn src() -> BranchId {
    BranchId::new(7, 0)
}
fn dst() -> BranchId {
    BranchId::new(3, 0)
}

/// A log over `n` rows in which **the target has diverged on every cell**, so the pick must
/// actually compute divergence.
///
/// Per row: op A (the source's `Add(+1)`, the one that gets picked) followed by op B (the
/// target's `Add(+10)`, already landed on the target). So the log is `2n` ops and the target row
/// holds 10 where the source op's witness says 0.
///
/// **This shape is load-bearing and the first version of this harness did not have it.** The
/// first fixture gave every op a witness equal to the target's value, which means `divergence`
/// returned at truth-table row 1 — *provably unchanged* — **before ever calling `ops_on_cell`**.
/// The index was never consulted, so axis 2 measured `BTreeMap` lookup growth and nothing else,
/// and the unindexed control arm (axis 2b) came back identical, which is what exposed it.
///
/// It is also self-checking. If `ops_on_cell` returned nothing, `divergence` would fall back to
/// an opaque `Assign(10)`, which does not commute with `Add(+1)`, so the pick would REFUSE under
/// the default `Reject` policy. Every `assert!(is_applied())` below is therefore an assertion
/// that the indexed path fired: this harness cannot report a number while measuring nothing.
fn build(n: usize) -> (MemCherryLog, MemCherryTarget, Vec<u64>, Vec<((u32, u64, u32), Vec<u64>)>) {
    let mut log = MemCherryLog::new();
    let mut target = MemCherryTarget::new();
    let mut seqs = Vec::with_capacity(n);
    let mut cells = Vec::with_capacity(n);
    for i in 0..n {
        let row = RowId(i as u64);
        // The target already carries B's effect: 0 + 10.
        target.insert(T, row, vec![Value::Integer(0), Value::Integer(10), Value::Integer(0)]);
        let a = log.push(RecordedOp {
            seq: 0,
            txn: TxnId(i as u64),
            branch: src(),
            table: "t".into(),
            tbl: T,
            row,
            col: Some(C),
            kind: OpKind::Add(Delta::Int(1)),
            before: Some(Value::Integer(0)),
            before_row: None,
        });
        let b = log.push(RecordedOp {
            seq: 0,
            txn: TxnId((n + i) as u64),
            branch: dst(),
            table: "t".into(),
            tbl: T,
            row,
            col: Some(C),
            kind: OpKind::Add(Delta::Int(10)),
            before: Some(Value::Integer(0)),
            before_row: None,
        });
        seqs.push(a);
        cells.push(((T.0, row.0, C.0), vec![a, b]));
    }
    (log, target, seqs, cells)
}

/// **The control arm: the same log with NO by-cell index.**
///
/// `op_at` delegates, so it is identical in both arms and the A/B isolates exactly one thing —
/// how `ops_on_cell` is answered. Here it is answered by a LINEAR SEARCH over the distinct cells,
/// which is the pre-D86 shape: a keyed question answered by scanning.
///
/// Without this arm, axis 2's number is uninterpretable. "1.99x across 100x the log" is only
/// evidence that the index works if something shows what NOT having it costs on the same machine,
/// in the same process, in the same run.
struct ScanCherryLog {
    inner: MemCherryLog,
    cells: Vec<((u32, u64, u32), Vec<u64>)>,
}

impl CherryLog for ScanCherryLog {
    fn op_at(&self, seq: u64) -> Option<&RecordedOp> {
        self.inner.op_at(seq)
    }

    fn ops_on_cell(&self, tbl: TableId, row: RowId, col: ColId) -> &[u64] {
        let key = (tbl.0, row.0, col.0);
        self.cells
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.as_slice())
            .unwrap_or(&[])
    }
}

/// The row the first selector touches — every op `build` emits is on row `seq/2`.
fn sel_row(sel: &[OpSelector]) -> RowId {
    RowId((sel[0].seq - 1) / 2)
}

/// `k` picked ops spread EVENLY over the table, which axis 2 and axis 2b both select with.
///
/// **Not `s[..k]`, and that is the whole reason axis 2b is worth running.** `ScanCherryLog`
/// answers `ops_on_cell` with a `find` over `cells` in row order, so picking the FIRST `k` rows
/// terminates every scan within `k` comparisons *no matter how long the log is*. The unindexed
/// arm then costs the same at 1e3 and at 1e5 log ops, and the A/B that exists to show what the
/// index buys measures nothing at all.
///
/// Measured with `s[..100]`, at 100,000 log ops, on the run that caught it: the "scanning" arm
/// came in at **50.375 us** against the indexed arm's **52.250 us** — the scan the arm was built
/// to expose was never performed, and the harness's own "N x FASTER" line was reading noise. This
/// is the same defect as the refusal arm's: a fixture whose stated premise does not hold, in a
/// detector that had only ever been observed quiet.
///
/// Striding puts the average matched cell in the middle of the list, so the scan's cost is
/// proportional to the log size — which is the thing the arm claims to show. Both arms select
/// identically, so the A/B still isolates exactly one variable.
fn spread(s: &[u64], k: usize) -> Vec<OpSelector> {
    let stride = (s.len() / k).max(1);
    s.iter().step_by(stride).take(k).copied().map(OpSelector::new).collect()
}

fn median(mut us: Vec<f64>) -> f64 {
    us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    us[us.len() / 2]
}

/// Median wall time of `reps` cherry-picks, reported as **(whole, plan)**.
///
/// The two are separated on purpose. `plan` is the engine — selector resolution, composition,
/// divergence through the by-cell index, `resolve_cell` — and it is what the O(picked) claim is
/// about. `whole` additionally includes `MemCherryTarget::commit_all`, which is a property of the
/// test double, not of this module. The first version of this harness reported only `whole` and
/// the target's own cost dominated it; keeping both is what makes that visible rather than
/// silently folded into the curve.
fn measure(
    log: &dyn CherryLog,
    sel: &[OpSelector],
    base: &MemCherryTarget,
    reps: usize,
) -> (f64, f64) {
    let policy = PolicyTable::new();
    let mut whole: Vec<f64> = Vec::with_capacity(reps);
    let mut plan: Vec<f64> = Vec::with_capacity(reps);
    for _ in 0..reps {
        // Reset outside the timed region: a landed pick moves the cells its witnesses name.
        let mut t = base.clone();

        // Plan only: touches nothing, so it can be timed against the same untouched base.
        let start = Instant::now();
        let p = plan_cherry_pick(log, src(), sel, dst(), &t, &policy).expect("engine error");
        plan.push(start.elapsed().as_secs_f64() * 1e6);
        assert!(p.is_applied(), "the harness must measure an APPLIED pick, got {:?}", p);

        // Plan + commit.
        let start = Instant::now();
        let r = cherry_pick(log, src(), sel, dst(), &mut t, &policy).expect("engine error");
        whole.push(start.elapsed().as_secs_f64() * 1e6);
        assert!(r.is_applied());
        assert_eq!(t.commits, 1, "one commit per pick");
        // 0 +10 (theirs, already on the target) +1 (ours) = 11. Reaching 11 proves `divergence`
        // read the op from the log: the image-comparison fallback yields an opaque Assign, which
        // does not commute with an Add and would have REFUSED.
        assert_eq!(
            t.cell(T, sel_row(sel), C),
            Some(&Value::Integer(11)),
            "the divergence must have been read from the log, not guessed"
        );
    }
    (median(whole), median(plan))
}

fn main() {
    println!("D100 — CHERRY-PICK COST. Built {}", ferrodb::build_provenance());
    println!();
    println!(
        "Instrument: std::time::Instant, median over the reps named per row. Release profile.\n\
         `plan us`        = branch::cherry::plan_cherry_pick alone — THE ENGINE, touches nothing.\n\
         `plan+commit us` = that plus MemCherryTarget::commit_all — the TEST DOUBLE's cost too.\n\
         Target is reset from a clone OUTSIDE every timed region.\n\
         Each op is an Assign on its own row, so this is the per-op path, not per-cell composition."
    );
    println!();

    // ---- AXIS 1: cost against the number of PICKED ops, log held fixed. ---------------------
    const ROWS: usize = 10_000;
    let (log, base, seqs, _) = build(ROWS);
    // Anti-vacuity: the index must be populated and must hold BOTH ops on each cell, or the
    // "reads divergence through the index" claim is measuring nothing.
    assert_eq!(log.len(), 2 * ROWS);
    assert_eq!(log.ops_on_cell(T, RowId(0), C).len(), 2, "the by-cell index must be populated");

    println!("AXIS 1 — picked ops, log fixed at {} ops", 2 * ROWS);
    println!(
        "  {:>8}  {:>14}  {:>14}  {:>16}",
        "picked", "plan us", "plan+commit us", "plan us per pick"
    );
    let mut axis1: Vec<(usize, f64)> = Vec::new();
    for &n in &[1usize, 10, 100, 1000] {
        let sel: Vec<OpSelector> = seqs[..n].iter().copied().map(OpSelector::new).collect();
        let reps = if n >= 1000 { 101 } else { 401 };
        let (whole, plan) = measure(&log, &sel, &base, reps);
        println!("  {:>8}  {:>14.3}  {:>14.3}  {:>16.4}", n, plan, whole, plan / n as f64);
        axis1.push((n, plan));
    }
    println!();
    let (n1, t1) = axis1[0];
    let (n4, t4) = axis1[3];
    println!(
        "  1 -> 1000 picked ops: plan {:.3} us -> {:.3} us = {:.1}x for {:.0}x the work.",
        t1,
        t4,
        t4 / t1,
        n4 as f64 / n1 as f64
    );
    println!(
        "  Plan cost per pick moved {:.4} us -> {:.4} us ({:.3}x).",
        t1 / n1 as f64,
        t4 / n4 as f64,
        (t4 / n4 as f64) / (t1 / n1 as f64)
    );

    // ---- AXIS 2: cost against the size of the LOG, picked held fixed. -----------------------
    //
    // The control. A pick that scanned the log would rise here; one that reads the by-cell key
    // should be flat, because the number of entries on any one cell does not change with the
    // log's size.
    //
    // **It is not flat, and the number is reported rather than explained.** With the strided
    // selection this arm rose 5.16x and 5.94x per pick across 100x the log on two runs. That is
    // not the scan signature — axis 2b rose 41.2x and 45.2x over the same range, on the same two
    // runs — but it is not the constant the paragraph above predicts either. The `ops_on_cell` key is a
    // `BTreeMap` whose depth and cache behaviour both move with the number of distinct cells,
    // and this harness does not separate those from the lookup itself, so the cause is UNVERIFIED
    // and stated as open. Reading "it rose, therefore the pick is scanning" off this arm alone
    // would be wrong.
    println!();
    println!("AXIS 2 — log size, picked held at 100  [THE CONTROL FOR AXIS 1]");
    println!(
        "  {:>10}  {:>14}  {:>14}  {:>16}",
        "log ops", "plan us", "plan+commit us", "plan us per pick"
    );
    let mut axis2: Vec<(usize, f64)> = Vec::new();
    for &rows in &[500usize, 5_000, 50_000] {
        let (l, b, s, _) = build(rows);
        let sel = spread(&s, 100);
        let (whole, plan) = measure(&l, &sel, &b, 201);
        println!("  {:>10}  {:>14.3}  {:>14.3}  {:>16.4}", 2 * rows, plan, whole, plan / 100.0);
        axis2.push((2 * rows, plan));
    }
    println!();
    let (s1, m1) = axis2[0];
    let (s3, m3) = axis2[2];
    println!(
        "  {}x more log, same 100 picks: plan {:.3} us -> {:.3} us = {:.2}x.",
        s3 / s1,
        m1,
        m3,
        m3 / m1
    );

    // ---- AXIS 2b: the SAME axis with the index removed. -------------------------------------
    //
    // Same process, same machine, same selection, same `op_at`. The ONLY difference is that
    // `ops_on_cell` scans instead of being keyed. A detector that has only ever been observed
    // quiet is not a clean result — this is the arm that makes it fire.
    //
    // It only fires because the selection is STRIDED across the table; see `spread`. Picking the
    // first 100 rows, which is what this did originally, bounds every scan at 100 comparisons
    // and makes this arm a copy of axis 2 wearing a different name.
    println!();
    println!("AXIS 2b — THE SAME AXIS WITH NO BY-CELL INDEX (ops_on_cell scans)");
    println!(
        "  {:>10}  {:>14}  {:>14}  {:>16}",
        "log ops", "plan us", "plan+commit us", "plan us per pick"
    );
    let mut axis2b: Vec<(usize, f64)> = Vec::new();
    for &rows in &[500usize, 5_000, 50_000] {
        let (l, b, s, cells) = build(rows);
        let scan = ScanCherryLog { inner: l, cells };
        let sel = spread(&s, 100); // the SAME selection axis 2 used, so one variable differs
        let (whole, plan) = measure(&scan, &sel, &b, 201);
        println!("  {:>10}  {:>14.3}  {:>14.3}  {:>16.4}", 2 * rows, plan, whole, plan / 100.0);
        axis2b.push((2 * rows, plan));
    }
    println!();
    let (_, b1) = axis2b[0];
    let (_, b3) = axis2b[2];
    println!(
        "  {}x more log, same 100 picks, NO INDEX: plan {:.3} us -> {:.3} us = {:.1}x.",
        s3 / s1,
        b1,
        b3,
        b3 / b1
    );
    println!();
    println!(
        "  ==> AT {} LOG OPS THE INDEXED PICK IS {:.1}x FASTER THAN THE SCANNING ONE\n               ({:.3} us vs {:.3} us), and the two arms' SLOPES across the same 100x are {:.2}x vs \
         {:.1}x. Same process, same run.",
        s3,
        b3 / m3,
        m3,
        b3,
        m3 / m1,
        b3 / b1
    );

    // ---- The refusal path, for completeness. ------------------------------------------------
    //
    // A refusal never reaches the writer, so it is strictly cheaper than an apply. Banked so the
    // curve cannot be read as "refusals are the expensive case".
    println!();
    let (mut rlog, rbase, rseqs, _) = build(1_000);
    let mut moved = rbase.clone();
    // Move one cell out from under the pick, contradictorily.
    //
    // **It cannot be one of `build`'s own rows, and the first version of this arm tried.** It
    // moved row 500's cell to -1 and asserted a refusal, on the premise that a value no recorded
    // op explains makes `divergence` fall back to an opaque `Assign`. The premise is false: every
    // row `build` emits carries the target's `Add(+10)` at a seq ABOVE the picked op, so
    // `ops_on_cell` still answers `Add(+10)`, which commutes with the picked `Add(+1)` whatever
    // the cell now holds. The arm applied, and the assertion — which was right — caught it.
    //
    // Reaching the fallback needs a cell with NO recorded op after the picked one, so the log
    // gets one extra row that only the SOURCE ever wrote. The target holds a value for it that
    // nothing in the log explains, `divergence` has no op to name, and the opaque `Assign` does
    // not commute with `Add`, so the whole pick refuses under `Reject`.
    let lone_row = RowId(1_000);
    let lone = rlog.push(RecordedOp {
        seq: 0,
        txn: TxnId(2_000),
        branch: src(),
        table: "t".into(),
        tbl: T,
        row: lone_row,
        col: Some(C),
        kind: OpKind::Add(Delta::Int(1)),
        before: Some(Value::Integer(0)),
        before_row: None,
    });
    moved.insert(T, lone_row, vec![Value::Integer(0), Value::Integer(-1), Value::Integer(0)]);
    // Still 1000 picked ops: 999 of `build`'s, plus the one on the lone row.
    let sel: Vec<OpSelector> = rseqs[..999]
        .iter()
        .copied()
        .chain(std::iter::once(lone))
        .map(OpSelector::new)
        .collect();
    let policy = PolicyTable::new();
    let mut us: Vec<f64> = Vec::new();
    for _ in 0..201 {
        let mut t = moved.clone();
        let start = Instant::now();
        let r = cherry_pick(&rlog, src(), &sel, dst(), &mut t, &policy).unwrap();
        us.push(start.elapsed().as_secs_f64() * 1e6);
        assert!(!r.is_applied(), "this arm must REFUSE");
        // ...and for the reason the fixture was built to produce. A refusal on any other
        // ground — `RowGone`, say — would time a path this arm is not claiming to describe.
        let refusal = r.refusal().expect("just asserted not applied");
        assert!(
            refusal.has(CherryConflictKind::TargetCellMoved),
            "the refusal must be the moved-cell one, got {:?}",
            refusal.kinds()
        );
        assert_eq!(t.commits, 0, "a refusal must not reach the writer");
    }
    println!(
        "REFUSAL PATH — 1000 picked, one cell moved: median {:.3} us, and commit_all was \
         entered 0 times in all 201 reps.",
        median(us)
    );
}
