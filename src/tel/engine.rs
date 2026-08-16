//! The three-way merge engine over the Typed Effect Log.
//!
//! Design authority: DESIGN.md section 3 ("Merge").
//!
//! The merge is three-way against the LCA (the fork point), so the result is
//! `l + (v1 - l) + (v2 - l)` with **no per-replica vectors that grow forever**. Having an LCA is
//! strictly stronger than being a CRDT replica.
//!
//! The order of the passes is load-bearing and is spelled out on [`crate::tel::merge::Merger`]:
//!
//! 1. de-duplicate frames by `TxnId` — `Add` is **not** idempotent, so a replayed frame that is
//!    not dropped here silently doubles a counter (the Cassandra trap);
//! 2. fold each side's ops per cell, then compose the two sides against the LCA value;
//! 3. **then** re-evaluate every guard and every escrow bound against the *composed* state;
//! 4. only then choose between `Clean`, `Commuting`, `Conflict` and `ResolvedWithLoss`.
//!
//! Step 3 cannot move earlier. A bounded counter is exactly the case: two `Add`s compose
//! arithmetically without anything going wrong, and only the post-merge re-check of `qty >= 0`
//! notices that the composition drove the counter through the floor.
//!
//! ## Known limits, stated rather than hidden
//!
//! - Set-valued columns (`SetInsert`/`SetRemove`) are composed at the **op** level with
//!   observed-remove semantics. They are not materialised into a scalar cell value, because
//!   `catalog::column::Value` has no set variant. A guard that reads a set-valued column therefore
//!   sees the LCA value, not the merged set.
//! - `MergePolicy::MultiValue` retains **both** ops in the composed list (nothing is discarded, so
//!   the outcome is not `ResolvedWithLoss`); the single scalar cell used for guard re-evaluation is
//!   the later of the two by `(seq, txn_id)`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::branch::record::BranchRecord;
use crate::branch::types::BranchId;
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::tel::frame::TxnFrame;
use crate::tel::guard::{CmpOp, Guard, GuardContext, GuardExpr};
use crate::tel::ids::{ColId, Dot, RowId, TableId, TxnId};
use crate::tel::merge::{
    ColumnPolicyLookup, ConflictKind, ConflictReport, Diff, DiscardedWrite, MergeOutcome,
    MergePolicy, Merger,
};
use crate::tel::op::{Delta, EscrowClaim, Op, OpKind};
use crate::tel::EffectLog;

/// A cell, fully qualified. `Option<ColId>` is `None` for whole-row ops.
type RowKey = (TableId, RowId);
type CellKey = (TableId, RowId, ColId);

/// Which side of the merge an op came from. `Ours` is the branch being merged **into** (main);
/// `Theirs` is the incoming branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Ours,
    Theirs,
}

impl Side {
    fn other(self) -> Side {
        match self {
            Side::Ours => Side::Theirs,
            Side::Theirs => Side::Ours,
        }
    }
}

/// An op plus enough provenance to order it and to attribute a discard to a branch.
#[derive(Debug, Clone)]
struct StampedOp {
    op: Op,
    branch: BranchId,
    txn: TxnId,
    seq: u64,
}

impl StampedOp {
    /// Ordering key for last-writer-wins. Branch-local `seq` first, then `txn_id` as a
    /// deterministic tie-break across branches — LWW across independent branches has no true
    /// global clock, and pretending otherwise would be worse than being explicit about it.
    fn stamp(&self) -> (u64, u64) {
        (self.seq, self.txn.0)
    }
}

/// Set-valued accumulation for one cell on one side.
#[derive(Debug, Clone, Default)]
struct SetChange {
    /// `(element, dot)` pairs inserted.
    inserts: Vec<(Value, Dot)>,
    /// `(element, observed dots)` pairs removed. Observed-remove: an insert this transaction did
    /// not see survives the remove.
    removes: Vec<(Value, Vec<Dot>)>,
}

/// One side's net effect on one cell, after folding that side's ops in order.
#[derive(Debug, Clone)]
enum SideEffect {
    /// The side determined an absolute value.
    Assign(Value),
    /// The side moved the cell by a delta relative to the LCA value.
    Add(Delta),
    Max(Value),
    Min(Value),
    Set(SetChange),
}

impl SideEffect {
    fn kind_name(&self) -> &'static str {
        match self {
            SideEffect::Assign(_) => "Assign",
            SideEffect::Add(_) => "Add",
            SideEffect::Max(_) => "Max",
            SideEffect::Min(_) => "Min",
            SideEffect::Set(_) => "Set",
        }
    }

    /// The absolute value this side would leave the cell at, given the LCA value.
    fn resolve(&self, base: Option<&Value>) -> Result<Value, FerroError> {
        match self {
            SideEffect::Assign(v) => Ok(v.clone()),
            SideEffect::Add(d) => {
                let b = base.ok_or_else(|| {
                    FerroError::Merge("cannot resolve an Add without the LCA value".into())
                })?;
                d.apply(b)
            }
            SideEffect::Max(v) => Ok(match base {
                Some(b) if b > v => b.clone(),
                _ => v.clone(),
            }),
            SideEffect::Min(v) => Ok(match base {
                Some(b) if b < v => b.clone(),
                _ => v.clone(),
            }),
            SideEffect::Set(_) => Err(FerroError::Merge(
                "set-valued columns have no scalar resolution".into(),
            )),
        }
    }
}

/// The composed state a guard is re-evaluated against: the LCA snapshot with the merge's writes
/// laid over it.
///
/// This exists because a guard checked against pre-merge state is checked against a state that
/// will not exist after the merge, which is precisely the bug the bounded-counter case exposes.
pub struct ComposedState<'a> {
    base: &'a dyn GuardContext,
    cells: HashMap<CellKey, Value>,
    created: HashMap<RowKey, Vec<Value>>,
    deleted: HashSet<RowKey>,
}

impl<'a> ComposedState<'a> {
    pub fn new(base: &'a dyn GuardContext) -> Self {
        ComposedState {
            base,
            cells: HashMap::new(),
            created: HashMap::new(),
            deleted: HashSet::new(),
        }
    }

    pub fn set_cell(&mut self, tbl: TableId, row: RowId, col: ColId, v: Value) {
        self.cells.insert((tbl, row, col), v);
    }

    pub fn create_row(&mut self, tbl: TableId, row: RowId, image: Vec<Value>) {
        self.deleted.remove(&(tbl, row));
        self.created.insert((tbl, row), image);
    }

    pub fn delete_row(&mut self, tbl: TableId, row: RowId) {
        self.deleted.insert((tbl, row));
    }
}

impl GuardContext for ComposedState<'_> {
    fn column(&self, tbl: TableId, row: RowId, col: ColId) -> Result<Value, FerroError> {
        if self.deleted.contains(&(tbl, row)) {
            // Deliberately an error, not a NULL: "the row is gone" is not "the row holds NULL",
            // and collapsing the two would let a guard quietly pass over a deleted row.
            return Err(FerroError::Merge(format!(
                "cell {}.{}[{}] was deleted by this merge",
                tbl, col, row
            )));
        }
        if let Some(v) = self.cells.get(&(tbl, row, col)) {
            return Ok(v.clone());
        }
        if let Some(image) = self.created.get(&(tbl, row)) {
            return image.get(col.0 as usize).cloned().ok_or_else(|| {
                FerroError::Merge(format!("column {} outside the RowCreate image for {}", col, row))
            });
        }
        self.base.column(tbl, row, col)
    }
}

/// Frames retained after `TxnId` de-duplication, and the ids that were dropped.
///
/// A dropped id is not a diagnostic curiosity: it is a replayed transaction whose `Add` would
/// otherwise have been applied twice.
#[derive(Debug, Clone, PartialEq)]
pub struct Deduped {
    pub kept: Vec<TxnId>,
    pub dropped: Vec<TxnId>,
}

/// De-duplicate frames by `TxnId`, first occurrence winning, `ours` before `theirs`.
///
/// `OpKind::Add` is not idempotent, so this pass is what stops a retried transaction from
/// double-counting. Everything else in the algebra would survive the duplicate unharmed, which is
/// exactly why the de-dup has to be unconditional rather than conditional on op kind: a frame
/// mixing an `Assign` and an `Add` must be dropped whole or applied whole.
pub fn dedup_by_txn<'a>(
    ours: &'a [TxnFrame],
    theirs: &'a [TxnFrame],
) -> (Vec<(&'a TxnFrame, Side)>, Deduped) {
    let mut seen: HashSet<TxnId> = HashSet::new();
    let mut kept = Vec::new();
    let mut report = Deduped { kept: Vec::new(), dropped: Vec::new() };
    for (frames, side) in [(ours, Side::Ours), (theirs, Side::Theirs)] {
        let mut ordered: Vec<&TxnFrame> = frames.iter().collect();
        ordered.sort_by_key(|f| (f.seq, f.txn_id.0));
        for f in ordered {
            if seen.insert(f.txn_id) {
                report.kept.push(f.txn_id);
                kept.push((f, side));
            } else {
                report.dropped.push(f.txn_id);
            }
        }
    }
    (kept, report)
}

/// Per-side, per-cell op accumulation.
#[derive(Default)]
struct SideIndex {
    cells: HashMap<CellKey, Vec<StampedOp>>,
    rows: HashMap<RowKey, Vec<StampedOp>>,
}

impl SideIndex {
    fn push(&mut self, s: StampedOp) {
        match s.op.col {
            Some(col) => self.cells.entry((s.op.tbl, s.op.row, col)).or_default().push(s),
            None => self.rows.entry((s.op.tbl, s.op.row)).or_default().push(s),
        }
    }

    fn touches_row(&self, key: &RowKey) -> bool {
        self.rows.contains_key(key)
            || self.cells.keys().any(|(t, r, _)| (*t, *r) == *key)
    }

    fn sort(&mut self) {
        for v in self.cells.values_mut() {
            v.sort_by_key(|s| s.stamp());
        }
        for v in self.rows.values_mut() {
            v.sort_by_key(|s| s.stamp());
        }
    }
}

/// The default [`Merger`]: three-way, LCA-based, guard-rechecking.
///
/// Holds an optional [`EffectLog`] so that `diff` has something to read. `merge` needs no log —
/// it is handed the frames.
#[derive(Clone, Default)]
pub struct ThreeWayMerger {
    log: Option<Arc<dyn EffectLog>>,
}

impl ThreeWayMerger {
    pub fn new() -> Self {
        ThreeWayMerger { log: None }
    }

    pub fn with_log(log: Arc<dyn EffectLog>) -> Self {
        ThreeWayMerger { log: Some(log) }
    }
}

/// Everything the composition pass produced, before the outcome is chosen.
struct Composition {
    composed: Vec<Op>,
    conflicts: Vec<ConflictReport>,
    discarded: Vec<DiscardedWrite>,
    cells: Vec<(CellKey, Value)>,
    created: Vec<(RowKey, Vec<Value>)>,
    deleted: Vec<RowKey>,
}

impl Merger for ThreeWayMerger {
    fn merge(
        &self,
        _lca: &BranchRecord,
        ours: &[TxnFrame],
        theirs: &[TxnFrame],
        policy: &dyn ColumnPolicyLookup,
        merged_state: &dyn GuardContext,
    ) -> Result<MergeOutcome, FerroError> {
        // ---- pass 1: de-duplicate by TxnId. Add is not idempotent. ----
        let (frames, _deduped) = dedup_by_txn(ours, theirs);

        // A frame written against a different schema version cannot have its column ordinals
        // trusted, so this fails loudly instead of applying ordinals from the wrong schema.
        if let Some(mismatch) = schema_mismatch(&frames) {
            return Ok(MergeOutcome::Conflict(vec![mismatch]));
        }

        let mut ours_ix = SideIndex::default();
        let mut theirs_ix = SideIndex::default();
        let mut ours_wrote = false;
        for (f, side) in &frames {
            for op in &f.ops {
                let s = StampedOp {
                    op: op.clone(),
                    branch: f.branch,
                    txn: f.txn_id,
                    seq: f.seq,
                };
                match side {
                    Side::Ours => {
                        ours_wrote = true;
                        ours_ix.push(s);
                    }
                    Side::Theirs => theirs_ix.push(s),
                }
            }
        }
        ours_ix.sort();
        theirs_ix.sort();

        // ---- pass 2: compose ----
        let comp = compose(&ours_ix, &theirs_ix, policy, merged_state)?;
        let Composition { composed, mut conflicts, discarded, cells, created, deleted } = comp;

        // ---- pass 3: re-evaluate guards and escrow bounds against the COMPOSED state ----
        let mut state = ComposedState::new(merged_state);
        for (key, v) in cells {
            state.set_cell(key.0, key.1, key.2, v);
        }
        for (key, image) in created {
            state.create_row(key.0, key.1, image);
        }
        for key in deleted {
            state.delete_row(key.0, key.1);
        }

        // Run every predicate rather than short-circuiting on the first: an agent that learns one
        // violation per round trip pays N round trips for N defects (DESIGN.md section 4).
        for (f, _side) in &frames {
            for g in &f.guards {
                match g.check(&state) {
                    Ok(true) => {}
                    Ok(false) => conflicts.push(guard_conflict(g, f)),
                    Err(e) => conflicts.push(unevaluable_conflict(g, f, &e)),
                }
            }
            for c in &f.claims {
                if let Some(report) = check_claim(c, &state) {
                    conflicts.push(report);
                }
            }
        }

        // ---- pass 4: choose the outcome. Not before now. ----
        if !conflicts.is_empty() {
            return Ok(MergeOutcome::Conflict(conflicts));
        }
        if !discarded.is_empty() {
            // A policy succeeded while throwing a write away. Reporting this as Clean is the most
            // dangerous thing this system can do to an agent.
            return Ok(MergeOutcome::ResolvedWithLoss { applied: composed, discarded });
        }
        if !ours_wrote {
            // Main untouched since the fork point: a fast-forward. Only knowable here, after the
            // constraint pass has run.
            return Ok(MergeOutcome::Clean);
        }
        Ok(MergeOutcome::Commuting { composed })
    }

    fn diff(&self, from: BranchId, to: BranchId) -> Result<Diff, FerroError> {
        let log = self.log.as_ref().ok_or_else(|| {
            FerroError::Merge("this merger has no effect log, so it cannot compute a diff".into())
        })?;
        let from_frames = log.frames_for(from, 0)?;
        let to_frames = log.frames_for(to, 0)?;
        let shared: HashSet<TxnId> = from_frames.iter().map(|f| f.txn_id).collect();

        let mut novel: Vec<&TxnFrame> =
            to_frames.iter().filter(|f| !shared.contains(&f.txn_id)).collect();
        novel.sort_by_key(|f| (f.seq, f.txn_id.0));

        let mut seen: HashSet<TxnId> = HashSet::new();
        let mut ops = Vec::new();
        let mut guards = Vec::new();
        for f in novel {
            if !seen.insert(f.txn_id) {
                continue;
            }
            ops.extend(f.ops.iter().cloned());
            guards.extend(f.guards.iter().cloned());
        }
        Ok(Diff { from, to, ops, guards })
    }
}

fn schema_mismatch(frames: &[(&TxnFrame, Side)]) -> Option<ConflictReport> {
    let mut base: Option<(u32, &TxnFrame)> = None;
    for (f, _) in frames {
        match base {
            None => base = Some((f.schema_ver, f)),
            Some((v, first)) if v != f.schema_ver => {
                let (tbl, row) = f
                    .ops
                    .first()
                    .map(|o| (o.tbl, o.row))
                    .unwrap_or((TableId::default(), RowId::default()));
                return Some(ConflictReport {
                    kind: ConflictKind::SchemaMismatch,
                    tbl,
                    row,
                    col: None,
                    violated_guard: None,
                    ours: None,
                    theirs: None,
                    detail: format!(
                        "{} was written against schema version {} but {} against {}",
                        f.txn_id, f.schema_ver, first.txn_id, v
                    ),
                });
            }
            Some(_) => {}
        }
    }
    None
}

fn guard_conflict(g: &Guard, f: &TxnFrame) -> ConflictReport {
    let (tbl, row, col) = guard_anchor(g, f);
    let mut r = ConflictReport::guard_failed(g.clone(), tbl, row, col);
    r.detail = format!(
        "guard from {} on branch {} no longer holds against the merged state",
        f.txn_id, f.branch
    );
    r
}

fn unevaluable_conflict(g: &Guard, f: &TxnFrame, e: &FerroError) -> ConflictReport {
    let (tbl, row, col) = guard_anchor(g, f);
    ConflictReport {
        kind: ConflictKind::GuardUnevaluable,
        tbl,
        row,
        col,
        violated_guard: Some(g.clone()),
        ours: None,
        theirs: None,
        detail: format!("guard from {} could not be evaluated: {}", f.txn_id, e),
    }
}

/// Where to anchor a guard's conflict report: the first cell the guard reads, falling back to the
/// frame's first op.
fn guard_anchor(g: &Guard, f: &TxnFrame) -> (TableId, RowId, Option<ColId>) {
    if let Some((t, r, c)) = g.expr.referenced_cells().first().copied() {
        return (t, r, Some(c));
    }
    match f.ops.first() {
        Some(o) => (o.tbl, o.row, o.col),
        None => (TableId::default(), RowId::default(), None),
    }
}

/// Bounded resources need no special merge logic: the `Add`s compose, and then the bound is
/// re-checked here against the merged state. The violated bound is handed back as a real
/// predicate so the agent can retry with feedback rather than guess.
fn check_claim(c: &EscrowClaim, state: &dyn GuardContext) -> Option<ConflictReport> {
    let current = match state.column(c.tbl, c.row, c.col) {
        Ok(v) => v,
        Err(e) => {
            return Some(ConflictReport {
                kind: ConflictKind::GuardUnevaluable,
                tbl: c.tbl,
                row: c.row,
                col: Some(c.col),
                violated_guard: None,
                ours: None,
                theirs: None,
                detail: format!("escrow claim could not be checked: {}", e),
            });
        }
    };
    for (bound, op, label) in [
        (c.floor.as_ref(), CmpOp::Ge, "floor"),
        (c.ceiling.as_ref(), CmpOp::Le, "ceiling"),
    ] {
        let Some(b) = bound else { continue };
        if !op.apply(&current, b) {
            let guard = Guard::holds(GuardExpr::cmp(
                GuardExpr::col(c.tbl, c.row, c.col),
                op,
                GuardExpr::Literal(b.clone()),
            ))
            .with_source(format!("{}.{}[{}] {} {:?}", c.tbl, c.col, c.row, op, b));
            let mut r = ConflictReport::guard_failed(guard, c.tbl, c.row, Some(c.col));
            r.detail = format!(
                "escrow {} violated by the merged state (value is {:?})",
                label, current
            );
            return Some(r);
        }
    }
    None
}

/// The composition pass: fold each side per cell, then merge the two sides against the LCA.
fn compose(
    ours: &SideIndex,
    theirs: &SideIndex,
    policy: &dyn ColumnPolicyLookup,
    base: &dyn GuardContext,
) -> Result<Composition, FerroError> {
    let mut out = Composition {
        composed: Vec::new(),
        conflicts: Vec::new(),
        discarded: Vec::new(),
        cells: Vec::new(),
        created: Vec::new(),
        deleted: Vec::new(),
    };

    // ---- whole-row ops first: a delete on one side poisons every write on the other ----
    let mut row_keys: BTreeSet<RowKey> = BTreeSet::new();
    row_keys.extend(ours.rows.keys().copied());
    row_keys.extend(theirs.rows.keys().copied());
    let mut poisoned: HashSet<RowKey> = HashSet::new();

    for key in &row_keys {
        let o = ours.rows.get(key).map(|v| v.as_slice()).unwrap_or(&[]);
        let t = theirs.rows.get(key).map(|v| v.as_slice()).unwrap_or(&[]);
        compose_row(*key, o, t, ours, theirs, &mut out, &mut poisoned);
    }

    // ---- per-cell ----
    // A row created *inside this merge* has no LCA value, so its initial image stands in as the
    // base. Without this, `INSERT` then `UPDATE ... SET qty = qty - 1` on the same branch has no
    // value for the delta to apply to.
    let fresh: HashMap<RowKey, Vec<Value>> = out.created.iter().cloned().collect();

    let mut cell_keys: BTreeSet<CellKey> = BTreeSet::new();
    cell_keys.extend(ours.cells.keys().copied());
    cell_keys.extend(theirs.cells.keys().copied());

    for key in cell_keys {
        let (tbl, row, col) = key;
        if poisoned.contains(&(tbl, row)) {
            continue; // already reported as DeleteVsWrite, or a contradictory RowCreate
        }
        let base_value = base.column(tbl, row, col).ok().or_else(|| {
            fresh.get(&(tbl, row)).and_then(|img| img.get(col.0 as usize).cloned())
        });
        let o = ours.cells.get(&key).map(|v| v.as_slice()).unwrap_or(&[]);
        let t = theirs.cells.get(&key).map(|v| v.as_slice()).unwrap_or(&[]);
        compose_cell(key, o, t, policy, base_value, &mut out)?;
    }

    Ok(out)
}

fn compose_row(
    key: RowKey,
    o: &[StampedOp],
    t: &[StampedOp],
    ours: &SideIndex,
    theirs: &SideIndex,
    out: &mut Composition,
    poisoned: &mut HashSet<RowKey>,
) {
    let o_last = o.last().cloned();
    let t_last = t.last().cloned();

    let o_deletes = matches!(o_last.as_ref().map(|s| &s.op.kind), Some(OpKind::RowDelete));
    let t_deletes = matches!(t_last.as_ref().map(|s| &s.op.kind), Some(OpKind::RowDelete));

    // A delete on one side against any surviving write on the other. Two deletes are not a
    // conflict — `RowDelete` is idempotent — so the check is only for a side that still expects
    // the row to exist.
    let clash = if o_deletes && !t_deletes && theirs.touches_row(&key) {
        o_last.clone()
    } else if t_deletes && !o_deletes && ours.touches_row(&key) {
        t_last.clone()
    } else {
        None
    };
    if let Some(d) = clash {
        poisoned.insert(key);
        out.conflicts.push(ConflictReport {
            kind: ConflictKind::DeleteVsWrite,
            tbl: key.0,
            row: key.1,
            col: None,
            violated_guard: None,
            ours: Some(d.op.clone()),
            theirs: None,
            detail: format!(
                "branch {} deleted {} while the other side wrote to it",
                d.branch, key.1
            ),
        });
        return;
    }

    if o_deletes || t_deletes {
        // Both sides deleted, or only one side touched the row at all. RowDelete is idempotent.
        out.deleted.push(key);
        let src = if o_deletes { o_last.unwrap() } else { t_last.unwrap() };
        out.composed.push(src.op.clone());
        poisoned.insert(key);
        return;
    }

    // RowCreate on both sides: identical images are not a conflict (equality detection).
    let o_create = o_last.as_ref().and_then(|s| match &s.op.kind {
        OpKind::RowCreate(img) => Some((s.clone(), img.clone())),
        _ => None,
    });
    let t_create = t_last.as_ref().and_then(|s| match &s.op.kind {
        OpKind::RowCreate(img) => Some((s.clone(), img.clone())),
        _ => None,
    });
    match (o_create, t_create) {
        (Some((so, a)), Some((st, b))) => {
            if a == b {
                out.created.push((key, a));
                out.composed.push(so.op.clone());
            } else {
                out.conflicts.push(ConflictReport {
                    kind: ConflictKind::ContradictoryAssign,
                    tbl: key.0,
                    row: key.1,
                    col: None,
                    violated_guard: None,
                    ours: Some(so.op.clone()),
                    theirs: Some(st.op.clone()),
                    detail: format!(
                        "both branches created {} with different initial images",
                        key.1
                    ),
                });
                poisoned.insert(key);
            }
        }
        (Some((so, a)), None) => {
            out.created.push((key, a));
            out.composed.push(so.op.clone());
        }
        (None, Some((st, b))) => {
            out.created.push((key, b));
            out.composed.push(st.op.clone());
        }
        (None, None) => {}
    }
}

fn compose_cell(
    key: CellKey,
    o: &[StampedOp],
    t: &[StampedOp],
    policy: &dyn ColumnPolicyLookup,
    base: Option<Value>,
    out: &mut Composition,
) -> Result<(), FerroError> {
    let (tbl, _row, col) = key;
    let o_eff = fold_side(o, base.as_ref())?;
    let t_eff = fold_side(t, base.as_ref())?;

    match (o_eff, t_eff) {
        (None, None) => Ok(()),
        (Some(e), None) | (None, Some(e)) => emit_single(key, &e, base.as_ref(), out),
        (Some(a), Some(b)) => {
            compose_two(key, a, b, o, t, base.as_ref(), policy.policy(tbl, col), out)
        }
    }
}

/// Fold one side's ops on one cell, in order, into a single net effect.
fn fold_side(ops: &[StampedOp], base: Option<&Value>) -> Result<Option<SideEffect>, FerroError> {
    let mut acc: Option<SideEffect> = None;
    for s in ops {
        acc = Some(match (acc.take(), &s.op.kind) {
            // --- Assign overwrites everything that came before on this side ---
            (_, OpKind::Assign(v)) => SideEffect::Assign(v.clone()),

            // --- Add ---
            (None, OpKind::Add(d)) => SideEffect::Add(*d),
            (Some(SideEffect::Add(d0)), OpKind::Add(d)) => SideEffect::Add(d0.compose(d)?),
            (Some(SideEffect::Assign(v)), OpKind::Add(d)) => SideEffect::Assign(d.apply(&v)?),
            (Some(prev @ (SideEffect::Max(_) | SideEffect::Min(_))), OpKind::Add(d)) => {
                SideEffect::Assign(d.apply(&prev.resolve(base)?)?)
            }
            (Some(SideEffect::Set(_)), OpKind::Add(_)) => {
                return Err(FerroError::Merge(
                    "cannot apply a numeric delta to a set-valued column".into(),
                ));
            }

            // --- Max / Min ---
            (None, OpKind::Max(v)) => SideEffect::Max(v.clone()),
            (None, OpKind::Min(v)) => SideEffect::Min(v.clone()),
            (Some(SideEffect::Max(a)), OpKind::Max(v)) => {
                SideEffect::Max(if a > *v { a } else { v.clone() })
            }
            (Some(SideEffect::Min(a)), OpKind::Min(v)) => {
                SideEffect::Min(if a < *v { a } else { v.clone() })
            }
            (Some(SideEffect::Assign(a)), OpKind::Max(v)) => {
                SideEffect::Assign(if a > *v { a } else { v.clone() })
            }
            (Some(SideEffect::Assign(a)), OpKind::Min(v)) => {
                SideEffect::Assign(if a < *v { a } else { v.clone() })
            }
            (Some(prev), OpKind::Max(v)) => {
                let r = prev.resolve(base)?;
                SideEffect::Assign(if r > *v { r } else { v.clone() })
            }
            (Some(prev), OpKind::Min(v)) => {
                let r = prev.resolve(base)?;
                SideEffect::Assign(if r < *v { r } else { v.clone() })
            }

            // --- sets ---
            (None, OpKind::SetInsert { elem, dot }) => {
                let mut c = SetChange::default();
                c.inserts.push((elem.clone(), *dot));
                SideEffect::Set(c)
            }
            (None, OpKind::SetRemove { elem, dots }) => {
                let mut c = SetChange::default();
                c.removes.push((elem.clone(), dots.clone()));
                SideEffect::Set(c)
            }
            (Some(SideEffect::Set(mut c)), OpKind::SetInsert { elem, dot }) => {
                c.inserts.push((elem.clone(), *dot));
                SideEffect::Set(c)
            }
            (Some(SideEffect::Set(mut c)), OpKind::SetRemove { elem, dots }) => {
                c.removes.push((elem.clone(), dots.clone()));
                SideEffect::Set(c)
            }
            (Some(prev), k @ (OpKind::SetInsert { .. } | OpKind::SetRemove { .. })) => {
                return Err(FerroError::Merge(format!(
                    "cannot fold {} after {} on the same cell: a column is set-valued or it is \
                     not, and mixing the two is a capture bug",
                    k.name(),
                    prev.kind_name()
                )));
            }

            // Whole-row ops never reach a cell index.
            (_, k @ (OpKind::RowCreate(_) | OpKind::RowDelete)) => {
                return Err(FerroError::Merge(format!(
                    "{} carried a column reference; whole-row ops must have col = None",
                    k.name()
                )));
            }
        });
    }
    Ok(acc)
}

/// One side wrote; the other did not. Nothing to reconcile.
fn emit_single(
    key: CellKey,
    e: &SideEffect,
    base: Option<&Value>,
    out: &mut Composition,
) -> Result<(), FerroError> {
    let (tbl, row, col) = key;
    match e {
        SideEffect::Set(c) => {
            let composed = compose_sets(tbl, row, col, c, &SetChange::default());
            out.composed.extend(composed);
        }
        other => {
            let v = other.resolve(base)?;
            out.composed.push(canonical_op(key, other));
            out.cells.push((key, v));
        }
    }
    Ok(())
}

fn canonical_op(key: CellKey, e: &SideEffect) -> Op {
    let (tbl, row, col) = key;
    let kind = match e {
        SideEffect::Assign(v) => OpKind::Assign(v.clone()),
        SideEffect::Add(d) => OpKind::Add(*d),
        SideEffect::Max(v) => OpKind::Max(v.clone()),
        SideEffect::Min(v) => OpKind::Min(v.clone()),
        SideEffect::Set(_) => unreachable!("set effects are emitted op-wise"),
    };
    Op::new(tbl, row, Some(col), kind)
}

#[allow(clippy::too_many_arguments)]
fn compose_two(
    key: CellKey,
    a: SideEffect,
    b: SideEffect,
    o: &[StampedOp],
    t: &[StampedOp],
    base: Option<&Value>,
    policy: MergePolicy,
    out: &mut Composition,
) -> Result<(), FerroError> {
    let (tbl, row, col) = key;
    match (&a, &b) {
        // --- the commuting cases: no policy decision needed (DESIGN.md's Commuting row) ---
        (SideEffect::Add(d1), SideEffect::Add(d2)) => {
            // Exit criterion 6. Two identical `qty -= 5` compose to -10, not -5.
            let composed = d1.compose(d2)?;
            let b0 = base.ok_or_else(|| {
                FerroError::Merge(format!(
                    "cannot compose two Adds on {}.{}[{}] without the LCA value",
                    tbl, col, row
                ))
            })?;
            out.cells.push((key, composed.apply(b0)?));
            out.composed.push(Op::new(tbl, row, Some(col), OpKind::Add(composed)));
        }
        (SideEffect::Max(v1), SideEffect::Max(v2)) => {
            let v = if v1 > v2 { v1.clone() } else { v2.clone() };
            let resolved = SideEffect::Max(v.clone()).resolve(base)?;
            out.cells.push((key, resolved));
            out.composed.push(Op::new(tbl, row, Some(col), OpKind::Max(v)));
        }
        (SideEffect::Min(v1), SideEffect::Min(v2)) => {
            let v = if v1 < v2 { v1.clone() } else { v2.clone() };
            let resolved = SideEffect::Min(v.clone()).resolve(base)?;
            out.cells.push((key, resolved));
            out.composed.push(Op::new(tbl, row, Some(col), OpKind::Min(v)));
        }
        (SideEffect::Set(c1), SideEffect::Set(c2)) => {
            out.composed.extend(compose_sets(tbl, row, col, c1, c2));
        }
        // Equality detection: two branches that wrote the *same* value are not in conflict, and
        // no write is lost by keeping one of them.
        (SideEffect::Assign(v1), SideEffect::Assign(v2)) if v1 == v2 => {
            out.cells.push((key, v1.clone()));
            out.composed.push(Op::new(tbl, row, Some(col), OpKind::Assign(v1.clone())));
        }
        // --- everything else needs the column's declared policy ---
        _ => match policy {
            MergePolicy::Reject => {
                out.conflicts.push(ConflictReport {
                    kind: ConflictKind::ContradictoryAssign,
                    tbl,
                    row,
                    col: Some(col),
                    violated_guard: None,
                    ours: o.last().map(|s| s.op.clone()),
                    theirs: t.last().map(|s| s.op.clone()),
                    detail: format!(
                        "concurrent {} and {} on {}.{}[{}] under policy {}",
                        a.kind_name(),
                        b.kind_name(),
                        tbl,
                        col,
                        row,
                        MergePolicy::Reject
                    ),
                });
            }
            MergePolicy::Additive => {
                // l + (v1 - l) + (v2 - l): the three-way formula, applied to absolute values.
                let b0 = base.ok_or_else(|| {
                    FerroError::Merge(format!(
                        "ADDITIVE merge of {}.{}[{}] needs the LCA value",
                        tbl, col, row
                    ))
                })?;
                let v1 = a.resolve(base)?;
                let v2 = b.resolve(base)?;
                let d1 = delta_between(b0, &v1)?;
                let d2 = delta_between(b0, &v2)?;
                let total = d1.compose(&d2)?;
                out.cells.push((key, total.apply(b0)?));
                out.composed.push(Op::new(tbl, row, Some(col), OpKind::Add(total)));
            }
            MergePolicy::Lww => {
                let o_stamp = o.last().map(|s| s.stamp()).unwrap_or((0, 0));
                let t_stamp = t.last().map(|s| s.stamp()).unwrap_or((0, 0));
                // Tie-break to `theirs`, the incoming branch: it is the side that asked to merge.
                let ours_wins = o_stamp > t_stamp;
                let (winner, loser_side, loser_ops) = if ours_wins {
                    (&a, Side::Ours.other(), t)
                } else {
                    (&b, Side::Theirs.other(), o)
                };
                let v = winner.resolve(base)?;
                out.cells.push((key, v));
                out.composed.push(canonical_op(key, winner));
                if let Some(lost) = loser_ops.last() {
                    out.discarded.push(DiscardedWrite {
                        branch: lost.branch,
                        op: lost.op.clone(),
                        policy: MergePolicy::Lww,
                        reason: format!(
                            "LWW on {}.{}[{}] kept the write from the {:?} side",
                            tbl,
                            col,
                            row,
                            loser_side.other()
                        ),
                    });
                }
            }
            MergePolicy::MultiValue => {
                // Nothing is discarded: both ops are retained and surfaced. The scalar used for
                // guard re-evaluation is the later of the two.
                if let Some(s) = o.last() {
                    out.composed.push(s.op.clone());
                }
                if let Some(s) = t.last() {
                    out.composed.push(s.op.clone());
                }
                let o_stamp = o.last().map(|s| s.stamp()).unwrap_or((0, 0));
                let t_stamp = t.last().map(|s| s.stamp()).unwrap_or((0, 0));
                let later = if o_stamp > t_stamp { &a } else { &b };
                out.cells.push((key, later.resolve(base)?));
            }
        },
    }
    Ok(())
}

/// The numeric distance from `from` to `to`, for the `l + (v1-l) + (v2-l)` formula.
fn delta_between(from: &Value, to: &Value) -> Result<Delta, FerroError> {
    Ok(match (from, to) {
        (Value::Integer(a), Value::Integer(b)) => Delta::Int(*b as i64 - *a as i64),
        (Value::Integer(a), Value::Float(b)) => Delta::Float(b - *a as f64),
        (Value::Float(a), Value::Integer(b)) => Delta::Float(*b as f64 - a),
        (Value::Float(a), Value::Float(b)) => Delta::Float(b - a),
        (a, b) => {
            return Err(FerroError::Merge(format!(
                "ADDITIVE policy on non-numeric values {:?} and {:?}",
                a, b
            )));
        }
    })
}

/// Observed-remove set merge. An insert survives unless some remove — from **either** side —
/// named its dot. An insert a remover never saw therefore survives, which is the whole point of
/// observed-remove.
fn compose_sets(
    tbl: TableId,
    row: RowId,
    col: ColId,
    a: &SetChange,
    b: &SetChange,
) -> Vec<Op> {
    let mut removed_dots: HashSet<Dot> = HashSet::new();
    let mut out = Vec::new();
    for c in [a, b] {
        for (elem, dots) in &c.removes {
            removed_dots.extend(dots.iter().copied());
            out.push(Op::new(
                tbl,
                row,
                Some(col),
                OpKind::SetRemove { elem: elem.clone(), dots: dots.clone() },
            ));
        }
    }
    let mut seen: HashSet<Dot> = HashSet::new();
    for c in [a, b] {
        for (elem, dot) in &c.inserts {
            if removed_dots.contains(dot) || !seen.insert(*dot) {
                continue;
            }
            out.push(Op::new(
                tbl,
                row,
                Some(col),
                OpKind::SetInsert { elem: elem.clone(), dot: *dot },
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::branch::types::{CommitHash, LeaseDeadline};
    use crate::tel::guard::CmpOp;

    const TBL: TableId = TableId(1);
    const QTY: ColId = ColId(2);
    const NAME: ColId = ColId(3);
    const R1: RowId = RowId(1);

    /// A fixed LCA snapshot.
    struct Base(Vec<(CellKey, Value)>);

    impl GuardContext for Base {
        fn column(&self, tbl: TableId, row: RowId, col: ColId) -> Result<Value, FerroError> {
            self.0
                .iter()
                .find(|(k, _)| *k == (tbl, row, col))
                .map(|(_, v)| v.clone())
                .ok_or_else(|| FerroError::Merge(format!("no cell {}.{}[{}]", tbl, col, row)))
        }
    }

    fn base(qty: i32) -> Base {
        Base(vec![
            ((TBL, R1, QTY), Value::Integer(qty)),
            ((TBL, R1, NAME), Value::Varchar("widget".into())),
        ])
    }

    struct AllReject;
    impl ColumnPolicyLookup for AllReject {
        fn policy(&self, _t: TableId, _c: ColId) -> MergePolicy {
            MergePolicy::Reject
        }
    }

    struct Fixed(MergePolicy);
    impl ColumnPolicyLookup for Fixed {
        fn policy(&self, _t: TableId, _c: ColId) -> MergePolicy {
            self.0
        }
    }

    fn lca() -> BranchRecord {
        BranchRecord::trunk(0, LeaseDeadline(u64::MAX))
    }

    fn frame(txn: u64, branch: u64, seq: u64) -> TxnFrame {
        TxnFrame::new(TxnId(txn), BranchId::new(branch, 0), CommitHash::ZERO, seq, 1)
    }

    fn decrement(txn: u64, branch: u64, n: i64) -> TxnFrame {
        let mut f = frame(txn, branch, 0);
        f.push_op(Op::new(TBL, R1, Some(QTY), OpKind::Add(Delta::Int(-n))));
        f
    }

    /// `qty - n >= 0`, the classic bounded decrement, expressed over the merged state.
    fn floor_guard(n: i32) -> Guard {
        Guard::holds(GuardExpr::cmp(
            GuardExpr::col(TBL, R1, QTY),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(0)),
        ))
        .with_source(format!("qty >= 0 (after -{})", n))
    }

    fn merged_qty(outcome: &MergeOutcome) -> Option<Delta> {
        let ops = match outcome {
            MergeOutcome::Commuting { composed } => composed,
            MergeOutcome::ResolvedWithLoss { applied, .. } => applied,
            _ => return None,
        };
        ops.iter().find_map(|o| match &o.kind {
            OpKind::Add(d) if o.col == Some(QTY) => Some(*d),
            _ => None,
        })
    }

    // ---- exit criterion 6 ----

    #[test]
    fn two_branches_decrementing_compose_arithmetically() {
        let m = ThreeWayMerger::new();
        let ours = vec![decrement(1, 1, 5)];
        let theirs = vec![decrement(2, 2, 3)];
        let out = m
            .merge(&lca(), &ours, &theirs, &AllReject, &base(20))
            .unwrap();
        assert_eq!(out.name(), "Commuting", "{}", out);
        assert_eq!(merged_qty(&out), Some(Delta::Int(-8)));
    }

    #[test]
    fn two_identical_decrements_from_different_txns_are_not_deduplicated() {
        // Add is NOT idempotent. Two distinct transactions each doing qty -= 5 must reach -10.
        let m = ThreeWayMerger::new();
        let out = m
            .merge(&lca(), &[decrement(1, 1, 5)], &[decrement(2, 2, 5)], &AllReject, &base(20))
            .unwrap();
        assert_eq!(merged_qty(&out), Some(Delta::Int(-10)));
    }

    // ---- double-apply ----

    #[test]
    fn replayed_frame_is_dropped_rather_than_applied_twice() {
        let m = ThreeWayMerger::new();
        let replayed = decrement(7, 2, 5);
        // the same txn arriving twice, as a retry would deliver it
        let theirs = vec![replayed.clone(), replayed.clone()];
        let out = m.merge(&lca(), &[], &theirs, &AllReject, &base(20)).unwrap();
        // main untouched, so Clean; the decrement is applied once, not twice
        assert_eq!(out.name(), "Clean", "{}", out);

        // and the composition itself counted it once: main's own -1 plus the incoming -5, not -11
        let out2 = m
            .merge(&lca(), &[decrement(1, 1, 1)], &theirs, &AllReject, &base(20))
            .unwrap();
        assert_eq!(merged_qty(&out2), Some(Delta::Int(-6)));
    }

    #[test]
    fn dedup_reports_which_txn_it_dropped() {
        let f = decrement(7, 2, 5);
        let twice = [f.clone(), f.clone()];
        let (kept, report) = dedup_by_txn(&[], &twice);
        assert_eq!(kept.len(), 1);
        assert_eq!(report.dropped, vec![TxnId(7)]);
        assert_eq!(report.kept, vec![TxnId(7)]);
    }

    #[test]
    fn a_duplicate_frame_across_sides_is_dropped_too() {
        // The same transaction reachable from both sides (already merged once) must not double.
        let f = decrement(7, 1, 5);
        let (ours, theirs) = ([f.clone()], [f.clone()]);
        let (kept, report) = dedup_by_txn(&ours, &theirs);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].1, Side::Ours);
        assert_eq!(report.dropped, vec![TxnId(7)]);
    }

    // ---- exit criterion 7 ----

    #[test]
    fn guard_failing_against_merged_state_is_a_conflict_with_the_predicate_returned() {
        // 8 in stock. Each branch legally takes 5 against its own snapshot; together they take 10.
        let m = ThreeWayMerger::new();
        let mut ours = decrement(1, 1, 5);
        ours.push_guard(floor_guard(5));
        let mut theirs = decrement(2, 2, 5);
        theirs.push_guard(floor_guard(5));

        let out = m
            .merge(&lca(), &[ours], &[theirs], &AllReject, &base(8))
            .unwrap();

        assert!(out.is_conflict(), "expected a conflict, got {}", out);
        let reports = out.conflicts();
        assert!(!reports.is_empty());
        assert!(reports.iter().all(|r| r.kind == ConflictKind::GuardFailed));
        // The violated predicate itself must come back, not a boolean.
        let g = reports[0].violated_guard.as_ref().expect("guard returned");
        assert!(g.violated_predicate().contains("qty >= 0"));
        assert!(reports[0].feedback().contains("qty >= 0"));
    }

    #[test]
    fn the_same_guard_holds_when_the_composition_leaves_headroom() {
        let m = ThreeWayMerger::new();
        let mut ours = decrement(1, 1, 5);
        ours.push_guard(floor_guard(5));
        let mut theirs = decrement(2, 2, 5);
        theirs.push_guard(floor_guard(5));
        // 20 in stock: -10 still clears the floor. Proves the guard check above is not a
        // detector that always fires.
        let out = m
            .merge(&lca(), &[ours], &[theirs], &AllReject, &base(20))
            .unwrap();
        assert_eq!(out.name(), "Commuting", "{}", out);
        assert_eq!(merged_qty(&out), Some(Delta::Int(-10)));
    }

    #[test]
    fn an_unevaluable_guard_is_distinct_from_a_failed_one() {
        let m = ThreeWayMerger::new();
        let mut theirs = decrement(2, 2, 1);
        theirs.push_guard(Guard::holds(GuardExpr::cmp(
            // a cell the base snapshot does not have at all
            GuardExpr::col(TBL, RowId(99), QTY),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(0)),
        )));
        let out = m.merge(&lca(), &[], &[theirs], &AllReject, &base(20)).unwrap();
        assert_eq!(out.conflicts()[0].kind, ConflictKind::GuardUnevaluable);
    }

    #[test]
    fn every_violated_guard_is_reported_not_just_the_first() {
        // One round trip per defect, not N round trips for N defects.
        let m = ThreeWayMerger::new();
        let mut theirs = decrement(2, 2, 30);
        theirs.push_guard(floor_guard(30));
        theirs.push_guard(
            Guard::holds(GuardExpr::cmp(
                GuardExpr::col(TBL, R1, QTY),
                CmpOp::Gt,
                GuardExpr::Literal(Value::Integer(100)),
            ))
            .with_source("qty > 100"),
        );
        let out = m.merge(&lca(), &[], &[theirs], &AllReject, &base(8)).unwrap();
        assert_eq!(out.conflicts().len(), 2, "{}", out);
    }

    // ---- escrow / bounded counters ----

    #[test]
    fn escrow_floor_is_rechecked_against_merged_state() {
        let m = ThreeWayMerger::new();
        let mut ours = decrement(1, 1, 5);
        ours.push_claim(EscrowClaim {
            tbl: TBL,
            row: R1,
            col: QTY,
            amount: Delta::Int(-5),
            floor: Some(Value::Integer(0)),
            ceiling: None,
        });
        let theirs = decrement(2, 2, 5);
        let out = m.merge(&lca(), &[ours], &[theirs], &AllReject, &base(8)).unwrap();
        assert!(out.is_conflict(), "{}", out);
        let r = &out.conflicts()[0];
        assert_eq!(r.kind, ConflictKind::GuardFailed);
        assert!(r.violated_guard.is_some(), "the violated bound must come back as a predicate");
    }

    // ---- outcomes ----

    #[test]
    fn main_untouched_is_clean() {
        let m = ThreeWayMerger::new();
        let out = m
            .merge(&lca(), &[], &[decrement(2, 2, 5)], &AllReject, &base(20))
            .unwrap();
        assert_eq!(out, MergeOutcome::Clean);
    }

    #[test]
    fn contradictory_assigns_under_the_default_policy_conflict() {
        let m = ThreeWayMerger::new();
        let mut ours = frame(1, 1, 0);
        ours.push_op(Op::new(TBL, R1, Some(NAME), OpKind::Assign(Value::Varchar("a".into()))));
        let mut theirs = frame(2, 2, 0);
        theirs.push_op(Op::new(TBL, R1, Some(NAME), OpKind::Assign(Value::Varchar("b".into()))));
        let out = m.merge(&lca(), &[ours], &[theirs], &AllReject, &base(20)).unwrap();
        assert_eq!(out.conflicts()[0].kind, ConflictKind::ContradictoryAssign);
        assert!(out.conflicts()[0].ours.is_some() && out.conflicts()[0].theirs.is_some());
    }

    #[test]
    fn identical_assigns_are_not_a_conflict() {
        let m = ThreeWayMerger::new();
        let mut ours = frame(1, 1, 0);
        ours.push_op(Op::new(TBL, R1, Some(NAME), OpKind::Assign(Value::Varchar("a".into()))));
        let mut theirs = frame(2, 2, 0);
        theirs.push_op(Op::new(TBL, R1, Some(NAME), OpKind::Assign(Value::Varchar("a".into()))));
        let out = m.merge(&lca(), &[ours], &[theirs], &AllReject, &base(20)).unwrap();
        assert_eq!(out.name(), "Commuting", "{}", out);
        assert!(!out.lost_a_write());
    }

    #[test]
    fn lww_reports_resolved_with_loss_and_names_the_discarded_write() {
        let m = ThreeWayMerger::new();
        let mut ours = frame(1, 1, 0);
        ours.push_op(Op::new(TBL, R1, Some(NAME), OpKind::Assign(Value::Varchar("a".into()))));
        let mut theirs = frame(2, 2, 4);
        theirs.push_op(Op::new(TBL, R1, Some(NAME), OpKind::Assign(Value::Varchar("b".into()))));
        let out = m
            .merge(&lca(), &[ours], &[theirs], &Fixed(MergePolicy::Lww), &base(20))
            .unwrap();
        assert!(out.lost_a_write(), "{}", out);
        assert_ne!(out.name(), "Clean");
        match out {
            MergeOutcome::ResolvedWithLoss { applied, discarded } => {
                assert_eq!(discarded.len(), 1);
                assert_eq!(discarded[0].branch, BranchId::new(1, 0));
                assert_eq!(discarded[0].policy, MergePolicy::Lww);
                assert_eq!(
                    applied.iter().find(|o| o.col == Some(NAME)).unwrap().kind,
                    OpKind::Assign(Value::Varchar("b".into()))
                );
            }
            other => panic!("expected ResolvedWithLoss, got {}", other),
        }
    }

    #[test]
    fn multi_value_keeps_both_writes() {
        let m = ThreeWayMerger::new();
        let mut ours = frame(1, 1, 0);
        ours.push_op(Op::new(TBL, R1, Some(NAME), OpKind::Assign(Value::Varchar("a".into()))));
        let mut theirs = frame(2, 2, 0);
        theirs.push_op(Op::new(TBL, R1, Some(NAME), OpKind::Assign(Value::Varchar("b".into()))));
        let out = m
            .merge(&lca(), &[ours], &[theirs], &Fixed(MergePolicy::MultiValue), &base(20))
            .unwrap();
        assert!(!out.lost_a_write());
        match &out {
            MergeOutcome::Commuting { composed } => {
                assert_eq!(composed.iter().filter(|o| o.col == Some(NAME)).count(), 2);
            }
            other => panic!("expected Commuting, got {}", other),
        }
    }

    #[test]
    fn additive_policy_uses_the_three_way_formula_on_absolute_writes() {
        // l=20, v1=25, v2=18  =>  20 + 5 + (-2) = 23
        let m = ThreeWayMerger::new();
        let mut ours = frame(1, 1, 0);
        ours.push_op(Op::new(TBL, R1, Some(QTY), OpKind::Assign(Value::Integer(25))));
        let mut theirs = frame(2, 2, 0);
        theirs.push_op(Op::new(TBL, R1, Some(QTY), OpKind::Assign(Value::Integer(18))));
        let out = m
            .merge(&lca(), &[ours], &[theirs], &Fixed(MergePolicy::Additive), &base(20))
            .unwrap();
        assert_eq!(merged_qty(&out), Some(Delta::Int(3)));
    }

    #[test]
    fn max_and_min_compose_without_a_policy() {
        let m = ThreeWayMerger::new();
        let mut ours = frame(1, 1, 0);
        ours.push_op(Op::new(TBL, R1, Some(QTY), OpKind::Max(Value::Integer(30))));
        let mut theirs = frame(2, 2, 0);
        theirs.push_op(Op::new(TBL, R1, Some(QTY), OpKind::Max(Value::Integer(25))));
        let out = m.merge(&lca(), &[ours], &[theirs], &AllReject, &base(20)).unwrap();
        match &out {
            MergeOutcome::Commuting { composed } => {
                assert_eq!(composed[0].kind, OpKind::Max(Value::Integer(30)));
            }
            other => panic!("expected Commuting, got {}", other),
        }
    }

    #[test]
    fn delete_on_one_side_versus_a_write_on_the_other_conflicts() {
        let m = ThreeWayMerger::new();
        let mut ours = frame(1, 1, 0);
        ours.push_op(Op::new(TBL, R1, None, OpKind::RowDelete));
        let theirs = decrement(2, 2, 5);
        let out = m.merge(&lca(), &[ours], &[theirs], &AllReject, &base(20)).unwrap();
        assert_eq!(out.conflicts()[0].kind, ConflictKind::DeleteVsWrite);
    }

    #[test]
    fn both_sides_deleting_the_same_row_is_idempotent() {
        let m = ThreeWayMerger::new();
        let mut ours = frame(1, 1, 0);
        ours.push_op(Op::new(TBL, R1, None, OpKind::RowDelete));
        let mut theirs = frame(2, 2, 0);
        theirs.push_op(Op::new(TBL, R1, None, OpKind::RowDelete));
        let out = m.merge(&lca(), &[ours], &[theirs], &AllReject, &base(20)).unwrap();
        match &out {
            MergeOutcome::Commuting { composed } => assert_eq!(composed.len(), 1),
            other => panic!("expected one RowDelete, got {}", other),
        }
    }

    #[test]
    fn a_schema_version_mismatch_fails_loudly() {
        let m = ThreeWayMerger::new();
        let ours = decrement(1, 1, 5);
        let mut theirs = decrement(2, 2, 5);
        theirs.schema_ver = 2;
        let out = m.merge(&lca(), &[ours], &[theirs], &AllReject, &base(20)).unwrap();
        assert_eq!(out.conflicts()[0].kind, ConflictKind::SchemaMismatch);
    }

    #[test]
    fn observed_remove_spares_an_insert_the_remover_never_saw() {
        let m = ThreeWayMerger::new();
        let seen = Dot { branch: BranchId::new(1, 0), seq: 1 };
        let unseen = Dot { branch: BranchId::new(2, 0), seq: 1 };

        let mut ours = frame(1, 1, 0);
        ours.push_op(Op::new(
            TBL,
            R1,
            Some(NAME),
            OpKind::SetRemove { elem: Value::Varchar("tag".into()), dots: vec![seen] },
        ));
        let mut theirs = frame(2, 2, 0);
        theirs.push_op(Op::new(
            TBL,
            R1,
            Some(NAME),
            OpKind::SetInsert { elem: Value::Varchar("tag".into()), dot: unseen },
        ));

        let out = m.merge(&lca(), &[ours], &[theirs], &AllReject, &base(20)).unwrap();
        match &out {
            MergeOutcome::Commuting { composed } => {
                assert!(composed.iter().any(|o| matches!(
                    &o.kind,
                    OpKind::SetInsert { dot, .. } if *dot == unseen
                )));
            }
            other => panic!("expected Commuting, got {}", other),
        }
    }

    #[test]
    fn a_removed_dot_does_not_survive_the_merge() {
        let m = ThreeWayMerger::new();
        let dot = Dot { branch: BranchId::new(2, 0), seq: 1 };
        let mut ours = frame(1, 1, 0);
        ours.push_op(Op::new(
            TBL,
            R1,
            Some(NAME),
            OpKind::SetRemove { elem: Value::Varchar("tag".into()), dots: vec![dot] },
        ));
        let mut theirs = frame(2, 2, 0);
        theirs.push_op(Op::new(
            TBL,
            R1,
            Some(NAME),
            OpKind::SetInsert { elem: Value::Varchar("tag".into()), dot },
        ));
        let out = m.merge(&lca(), &[ours], &[theirs], &AllReject, &base(20)).unwrap();
        match &out {
            MergeOutcome::Commuting { composed } => {
                assert!(!composed.iter().any(|o| matches!(o.kind, OpKind::SetInsert { .. })));
            }
            other => panic!("expected Commuting, got {}", other),
        }
    }

    #[test]
    fn folding_a_side_applies_its_own_ops_in_order() {
        // assign 10 then -3 on the same branch: the side's net effect is an absolute 7
        let m = ThreeWayMerger::new();
        let mut ours = frame(1, 1, 0);
        ours.push_op(Op::new(TBL, R1, Some(QTY), OpKind::Assign(Value::Integer(10))));
        ours.push_op(Op::new(TBL, R1, Some(QTY), OpKind::Add(Delta::Int(-3))));
        let out = m.merge(&lca(), &[ours], &[], &AllReject, &base(20)).unwrap();
        match &out {
            MergeOutcome::Commuting { composed } => {
                assert_eq!(composed[0].kind, OpKind::Assign(Value::Integer(7)));
            }
            other => panic!("expected Commuting, got {}", other),
        }
    }

    #[test]
    fn a_row_created_inside_the_merge_supplies_its_own_base_for_a_delta() {
        // INSERT then `SET qty = qty - 4` on the same branch: there is no LCA value for the
        // delta to apply to, so the RowCreate image has to stand in.
        let m = ThreeWayMerger::new();
        let fresh = RowId(77);
        let mut theirs = frame(2, 2, 0);
        theirs.push_op(Op::new(
            TBL,
            fresh,
            None,
            OpKind::RowCreate(vec![Value::Integer(77), Value::Null, Value::Integer(10)]),
        ));
        theirs.push_op(Op::new(TBL, fresh, Some(QTY), OpKind::Add(Delta::Int(-4))));
        theirs.push_guard(Guard::holds(GuardExpr::cmp(
            GuardExpr::col(TBL, fresh, QTY),
            CmpOp::Ge,
            GuardExpr::Literal(Value::Integer(0)),
        )));

        // main untouched, and the guard sees 10 - 4 = 6
        let out = m.merge(&lca(), &[], &[theirs.clone()], &AllReject, &base(20)).unwrap();
        assert_eq!(out, MergeOutcome::Clean, "{}", out);

        // and the same shape overdrawn is caught
        theirs.ops[1] = Op::new(TBL, fresh, Some(QTY), OpKind::Add(Delta::Int(-40)));
        let out = m.merge(&lca(), &[], &[theirs], &AllReject, &base(20)).unwrap();
        assert_eq!(out.conflicts()[0].kind, ConflictKind::GuardFailed, "{}", out);
    }

    // ---- diff (exit criterion 4) ----

    #[test]
    fn diff_returns_the_ops_and_guards_the_target_branch_added() {
        use crate::tel::log::MemEffectLog;

        let log = Arc::new(MemEffectLog::new());
        // shared history, present on both branches
        let mut shared = decrement(1, 1, 5);
        shared.branch = BranchId::new(1, 0);
        log.append(&shared).unwrap();
        let mut shared_on_child = shared.clone();
        shared_on_child.branch = BranchId::new(2, 0);
        log.append(&shared_on_child).unwrap();

        // novel work on the child only
        let mut novel = decrement(9, 2, 3);
        novel.push_guard(floor_guard(3));
        log.append(&novel).unwrap();

        let m = ThreeWayMerger::with_log(log);
        let d = m.diff(BranchId::new(1, 0), BranchId::new(2, 0)).unwrap();
        assert_eq!(d.from, BranchId::new(1, 0));
        assert_eq!(d.to, BranchId::new(2, 0));
        // the shared transaction is not part of the changeset; the novel one is
        assert_eq!(d.ops.len(), 1);
        assert_eq!(d.ops[0].kind, OpKind::Add(Delta::Int(-3)));
        assert_eq!(d.guards.len(), 1, "guards are part of the changeset, not derived from ops");
    }

    #[test]
    fn diff_without_a_log_is_an_error_not_an_empty_changeset() {
        // A changeset that came back empty because nothing could be read must not read as
        // "nothing changed".
        let m = ThreeWayMerger::new();
        assert!(m.diff(BranchId::new(1, 0), BranchId::new(2, 0)).is_err());
    }

    #[test]
    fn merging_frames_read_back_from_the_log_reaches_the_same_answer() {
        use crate::tel::log::MemEffectLog;

        let log = MemEffectLog::new();
        let mut ours = decrement(1, 1, 5);
        ours.push_guard(floor_guard(5));
        let mut theirs = decrement(2, 2, 5);
        theirs.push_guard(floor_guard(5));
        log.append(&ours).unwrap();
        log.append(&theirs).unwrap();
        // and the retry that a flaky agent would send
        log.append(&theirs).unwrap();

        let ours_back = log.frames_for(BranchId::new(1, 0), 0).unwrap();
        let theirs_back = log.frames_for(BranchId::new(2, 0), 0).unwrap();
        let m = ThreeWayMerger::new();

        assert_eq!(
            merged_qty(&m.merge(&lca(), &ours_back, &theirs_back, &AllReject, &base(20)).unwrap()),
            Some(Delta::Int(-10))
        );
        assert!(
            m.merge(&lca(), &ours_back, &theirs_back, &AllReject, &base(8))
                .unwrap()
                .is_conflict()
        );
    }

    #[test]
    fn a_guard_reading_a_row_this_merge_deleted_is_unevaluable() {
        let m = ThreeWayMerger::new();
        let mut ours = frame(1, 1, 0);
        ours.push_op(Op::new(TBL, R1, None, OpKind::RowDelete));
        ours.push_guard(floor_guard(0));
        let out = m.merge(&lca(), &[ours], &[], &AllReject, &base(20)).unwrap();
        assert_eq!(out.conflicts()[0].kind, ConflictKind::GuardUnevaluable);
    }
}
