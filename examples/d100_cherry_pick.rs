//! **D100 — what a cherry-pick costs, on two axes.**
//!
//! The claim this harness exists to test is the module header's: cherry-pick over an op log costs
//! **O(picked)**, not O(log), because it reads divergence through D86's `(tbl,row,col)` key and a
//! `partition_point` rather than scanning the log. One axis cannot separate those — a curve over
//! picked ops at a fixed log size is consistent with both — so this runs **both**:
//!
//!   AXIS 1: picked ops 1 / 10 / 100 / 1000, log held at a fixed size.
//!   AXIS 2: log size 1e3 / 1e4 / 1e5, picked ops held at 100.
//!
//! Axis 2 is the control for axis 1. If per-pick cost is flat in the log size, the index is doing
//! the work the design claims; if it rises, the pick is scanning something.
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
    cherry_pick, plan_cherry_pick, CherryLog, MemCherryLog, MemCherryTarget, OpSelector,
    RecordedOp,
};
use ferrodb::branch::types::BranchId;
use ferrodb::catalog::column::Value;
use ferrodb::tel::ids::{ColId, RowId, TableId, TxnId};
use ferrodb::tel::op::OpKind;

const T: TableId = TableId(1);
const C: ColId = ColId(1);

fn src() -> BranchId {
    BranchId::new(7, 0)
}
fn dst() -> BranchId {
    BranchId::new(3, 0)
}

/// A log of `n` ops, each an `Assign` on its own row, recorded by the source branch.
///
/// One row per op so no two selected ops share a cell: the cost being measured is the per-op
/// path, not the per-cell composition path.
fn build(n: usize) -> (MemCherryLog, MemCherryTarget, Vec<u64>) {
    let mut log = MemCherryLog::new();
    let mut target = MemCherryTarget::new();
    let mut seqs = Vec::with_capacity(n);
    for i in 0..n {
        let row = RowId(i as u64);
        target.insert(T, row, vec![Value::Integer(0), Value::Integer(0), Value::Integer(0)]);
        seqs.push(log.push(RecordedOp {
            seq: 0,
            txn: TxnId(i as u64),
            branch: src(),
            table: "t".into(),
            tbl: T,
            row,
            col: Some(C),
            kind: OpKind::Assign(Value::Integer(i as i32 + 1)),
            before: Some(Value::Integer(0)),
            before_row: None,
        }));
    }
    (log, target, seqs)
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
    const FIXED_LOG: usize = 20_000;
    let (log, base, seqs) = build(FIXED_LOG);
    // Sanity: the index is populated, or the "reads through the index" claim is vacuous.
    assert_eq!(log.len(), FIXED_LOG);
    assert_eq!(log.ops_on_cell(T, RowId(0), C).len(), 1, "the by-cell index must be populated");

    println!("AXIS 1 — picked ops, log fixed at {} ops", FIXED_LOG);
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
    println!();
    println!("AXIS 2 — log size, picked held at 100  [THE CONTROL FOR AXIS 1]");
    println!(
        "  {:>10}  {:>14}  {:>14}  {:>16}",
        "log ops", "plan us", "plan+commit us", "plan us per pick"
    );
    let mut axis2: Vec<(usize, f64)> = Vec::new();
    for &size in &[1_000usize, 10_000, 100_000] {
        let (l, b, s) = build(size);
        let sel: Vec<OpSelector> = s[..100].iter().copied().map(OpSelector::new).collect();
        let (whole, plan) = measure(&l, &sel, &b, 201);
        println!("  {:>10}  {:>14.3}  {:>14.3}  {:>16.4}", size, plan, whole, plan / 100.0);
        axis2.push((size, plan));
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
    println!();
    println!("AXIS 2b — THE SAME AXIS WITH NO BY-CELL INDEX (ops_on_cell scans)");
    println!(
        "  {:>10}  {:>14}  {:>14}  {:>16}",
        "log ops", "plan us", "plan+commit us", "plan us per pick"
    );
    let mut axis2b: Vec<(usize, f64)> = Vec::new();
    for &size in &[1_000usize, 10_000, 100_000] {
        let (l, b, s) = build(size);
        // Rebuild the by-cell map as an unindexed Vec. Op `i` is the only op on row `i`, col C.
        let cells: Vec<((u32, u64, u32), Vec<u64>)> =
            (0..size).map(|i| ((T.0, i as u64, C.0), vec![s[i]])).collect();
        let scan = ScanCherryLog { inner: l, cells };
        let sel: Vec<OpSelector> = s[..100].iter().copied().map(OpSelector::new).collect();
        let (whole, plan) = measure(&scan, &sel, &b, 201);
        println!("  {:>10}  {:>14.3}  {:>14.3}  {:>16.4}", size, plan, whole, plan / 100.0);
        axis2b.push((size, plan));
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
    let (rlog, rbase, rseqs) = build(1_000);
    let mut moved = rbase.clone();
    // Move one cell out from under the pick, contradictorily.
    moved.insert(T, RowId(500), vec![Value::Integer(0), Value::Integer(-1), Value::Integer(0)]);
    let sel: Vec<OpSelector> = rseqs[..1000].iter().copied().map(OpSelector::new).collect();
    let policy = PolicyTable::new();
    let mut us: Vec<f64> = Vec::new();
    for _ in 0..201 {
        let mut t = moved.clone();
        let start = Instant::now();
        let r = cherry_pick(&rlog, src(), &sel, dst(), &mut t, &policy).unwrap();
        us.push(start.elapsed().as_secs_f64() * 1e6);
        assert!(!r.is_applied(), "this arm must REFUSE");
        assert_eq!(t.commits, 0, "a refusal must not reach the writer");
    }
    println!(
        "REFUSAL PATH — 1000 picked, one cell moved: median {:.3} us, and commit_all was \
         entered 0 times in all 201 reps.",
        median(us)
    );
}
