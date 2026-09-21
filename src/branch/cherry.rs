//! **D100 — CHERRY-PICK: apply a SELECTED SUBSET of one branch's ops to another.**
//!
//! Design authority: DESIGN.md section 3 (the op algebra this replays is the merge engine's).
//!
//! ## Why this is a capability gap and not a performance one
//!
//! With a branch per autonomous agent, the natural operation is *"agent 7 found the right fix —
//! put **just that change** onto agent 3's branch, without agent 7's other twenty edits."*
//! `MERGE` cannot express it. A merge's unit is a whole branch: it takes every effect the source
//! recorded, or none of them. There is no argument you can pass `MERGE` that means "these three
//! cells and nothing else", and no sequence of merges that composes to one either — merging and
//! then reverting the twenty unwanted edits is a different operation with a different result, it
//! publishes the unwanted work to every reader in the window, and `REVERT` refuses any op whose
//! before-image was not recorded.
//!
//! Dolt ships `dolt cherry-pick` and `dolt rebase`. ferrodb shipped `merge`
//! (`agent_sql/runtime.rs:2612`) and `revert_merge` (`:4175`) and nothing between them. That is a
//! **capability** hole: no amount of making merge faster produces the operation.
//!
//! ## Why op-replay is the right substrate for it, and a structural merge is not
//!
//! ferrodb merges by **replaying recorded ops**: `AppliedOp` carries `(seq, txn, tbl, row, col,
//! kind, before, before_row)`, `State::applied` is the log of them, `State::applied_by_cell` is
//! D86's `(tbl,row,col) -> positions` index over that log, and `concurrent_op` answers "what did
//! the target absorb on this cell since we forked" by reading it.
//!
//! **A subset of ops is exactly what an op log already manipulates.** Cherry-pick is
//! `merge`'s pipeline with one line changed — the set of ops fed into it — because every stage
//! downstream of that set is already written against a *set of ops on a cell*, not against a
//! branch:
//!
//! * [`compose_ops`] folds however many ops you hand it, one or twenty, into one effect;
//! * [`resolve_cell`] decides a cell from `(base witness, target now, ours, theirs)` and has no
//!   idea whether `ours` came from a whole branch or from three ops someone picked by hand;
//! * D86's by-cell index answers the divergence question per cell, so the same index serves a
//!   pick over three cells and a merge over three thousand.
//!
//! So this module **reuses [`resolve_cell`] as its cell decision** rather than writing a second
//! one, and **reads divergence through the same by-cell key D86 built** rather than adding a
//! second index (see [`CherryLog::ops_on_cell`], whose contract is `applied_by_cell` + the
//! `partition_point` range `concurrent_op` already does). A concurrent agent is building the
//! structural alternative (merge3); this is deliberately the op-replay one.
//!
//! For a *structural* merge the same operation is much harder: a subset of a tree diff is not a
//! tree, so you must first decide what the pruned diff even means before you can apply it. An op
//! log has no such problem — a subset of a list is a list.
//!
//! ## What is NOT here
//!
//! **Nothing wires this to `runtime.rs`, and there is no `CHERRY PICK` at the SQL surface.** There
//! is no `impl CherryLog` or `impl CherryTarget` outside this file and its harness. This is the
//! engine and its contract, written against the op-log shape `AgentRuntime` already keeps, so that
//! wiring is a projection (`CherryLog::op_at` is `state.applied[i]`; `ops_on_cell` is
//! `applied_by_cell`) rather than a translation. Every claim below is proved against
//! `MemCherryLog` / `MemCherryTarget`; none of it is evidence about the runtime until that impl
//! exists and is tested, and `commit_all`'s all-or-nothing contract in particular is the part the
//! runtime will owe.
//!
//! ## Atomicity: made unrepresentable, not documented
//!
//! A half-applied cherry-pick is worse than a refused one: it leaves the target holding a state
//! neither branch ever had, and the agent that asked for the pick cannot tell which half landed.
//!
//! The half-applied state is not prevented by care here. It is **not expressible**:
//! [`CherryTarget`] has exactly one mutating method, [`CherryTarget::commit_all`], and it takes
//! the **whole** plan. The engine is handed `&mut dyn CherryTarget` and there is no call on it
//! that writes one op, so "apply op 1, discover op 2 conflicts" cannot be written in this module
//! even by mistake. Every refusal is decided during planning, which touches nothing, and returns
//! [`CherryResult::Refused`] with the plan dropped unexecuted.
//!
//! ## Why `revert_merge`'s undo machinery is the wrong shape for the rollback
//!
//! **In one line, as asked:** `undo_txn` (`runtime.rs:4213`) is a *compensating* undo keyed by
//! `TxnId` that writes inverses **after** the fact, so driving cherry-pick's rollback with it
//! would mean applying the picks and then un-applying them — which *is* the half-applied window
//! the atomicity requirement exists to forbid, and it inherits two refusals of its own
//! (`"cannot revert a delete with no before-image"`, `"row is gone; cannot revert"`) that would
//! strand a rollback halfway.
//!
//! What this module *does* reuse from it is the **primitive it is built out of**:
//! [`crate::agent_sql::merge_engine::invert`], the same function `undo_txn` calls, via
//! [`CherryPlan::inverse`]. Undoing a *landed* pick therefore goes through one undo path rather
//! than a second one — and it goes through `commit_all`, so the undo is atomic on the same terms
//! the pick was.
//!
//! ## The truth table
//!
//! Every row has exactly one test below, named for it.
//!
//! | #  | Situation                                                          | Verdict |
//! |----|--------------------------------------------------------------------|---------|
//! | 1  | cell op; target cell **==** the op's recorded witness               | **APPLY** `Clean` |
//! | 2  | cell op; target moved; ours & theirs commute (`Add`/`Add`)          | **APPLY** `Commuting`, composed onto the *target* value |
//! | 3  | cell op; target moved; ops contradict; policy `Reject`              | **REFUSE WHOLE** `TargetCellMoved` |
//! | 4  | cell op; target moved to the **same** value we would write          | **APPLY** `Clean` — two writes of one value are not a conflict |
//! | 5  | cell op; target moved; policy `Lww`                                 | **APPLY** `Lossy`, reporting the discarded write |
//! | 6  | cell op; **no** recorded witness, target holds a value              | **REFUSE WHOLE** `TargetCellMoved` — "did not move" is not establishable |
//! | 7  | cell op; the row no longer exists on the target                     | **REFUSE WHOLE** `RowGone` |
//! | 8  | cell op; the target row is too narrow to hold that column           | **REFUSE WHOLE** `ColumnAbsent` |
//! | 9  | `RowCreate`; row absent on target                                   | **APPLY** — materialise from the carried image |
//! | 10 | `RowCreate`; row already present **at that point in the selection**  | **REFUSE WHOLE** `RowExists` |
//! | 11 | `RowDelete`; row present on target                                  | **APPLY** — remove |
//! | 12 | `RowDelete`; row absent **at that point in the selection**           | **REFUSE WHOLE** `RowGone` |
//! | 13 | **two selected ops on one cell**, composable                        | **APPLY as ONE composed write** — see note below |
//! | 14 | **two selected ops on one cell**, composition undefined             | **REFUSE WHOLE** `ComposeFailed` |
//! | 15 | the **same op selected twice**                                      | **REFUSE WHOLE** `DuplicateSelector` — `Add` is not idempotent |
//! | 16 | cell op on a row **this same pick creates**                         | **APPLY** — folded into the `InsertRow` image, no divergence possible |
//! | 17 | cell op on a row **this same pick deletes**, after the delete       | **REFUSE WHOLE** `RowGone` |
//! | 18 | selector names a seq the source never recorded                      | **REFUSE WHOLE** `NoSuchOp` |
//! | 19 | selector names an op recorded by a **different** branch             | **REFUSE WHOLE** `NotFromSource` |
//! | 20 | **empty** selection                                                 | **REFUSE** `EmptySelection` — a pick that picked nothing has not picked |
//! | 21 | **any** one row refuses                                             | **NOTHING is written** — `commit_all` is never reached |
//! | 22 | the algebra cannot apply the op to a cell (the **set ops**)          | **REFUSE WHOLE** `OpNotApplicable` — not an engine `Err` |
//! | 23 | the op's own shape is wrong (cell kind, no column)                   | **REFUSE WHOLE** `MalformedOp` |
//! | 24 | **several whole-row ops** on one row — delete then recreate          | **APPLY** — one `InsertRow` replacing the prior image |
//! | 25 | several whole-row ops — **create** then delete, row on target        | **REFUSE WHOLE** `RowExists` — the create is checked, not just the last op |
//! | 26 | cell op **before** a `RowDelete` in the same pick                    | **APPLY** — subsumed by the delete; the row leaves |
//! | 27 | the target's value is **not explained** by any non-source op         | **REFUSE WHOLE** `TargetCellMoved` — an unexplained move is not composed with |
//!
//! **Note on row 13, which is the one that looks like it should be a conflict and is not.**
//! Two ops the *same* branch recorded on one cell are sequential, not concurrent, so there is no
//! contradiction to resolve: [`compose_ops`] already defines their fold, and picking a
//! non-contiguous subset of a cell's history is not an error — it is the *request*. "Take agent
//! 7's second edit to this cell and not the first" is precisely the operation this module exists
//! to provide, and refusing it would refuse the feature. What *is* refused is a composition the
//! algebra does not define (row 14) and a duplicated selector (row 15), because `Add` is not
//! idempotent and a double-counted increment is the Cassandra counter trap the merge engine's
//! own header warns about.
//!
//! Non-contiguity is still **reported**, in [`CherryApplied::straddled`], because the resulting
//! value is one the source branch never itself held and the caller should be able to see that.
//! It is reported, never decisive — the same posture `blind_writes` takes at the merge gate.
//!
//! **Which rows their tests actually discriminate.** Rows 3, 4 and 5 each have a passing test, but
//! in all three fixtures the target's recorded op assigns exactly the value the target already
//! holds, so reading the log and falling back to an opaque `Assign` produce the *same* `theirs`
//! and the tests would pass with the log read deleted. Row 2 and row 27 are the only two places
//! where reading the log is observable: row 2 applies **only** if the log is read (an `Add` and an
//! opaque `Assign` do not commute), and row 27 refuses **only** if the verification in
//! [`divergence`] rejects a composition that does not explain the target's value. That is stated
//! here rather than left for the next reader to discover, because three green tests that cannot
//! fail for the reason they name look exactly like three that can.

use std::collections::{BTreeMap, BTreeSet};

use crate::agent_sql::merge_engine::{apply_op, compose_ops, invert, resolve_cell, CellMerge, CellResolution};
use crate::branch::types::BranchId;
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::tel::ids::{ColId, RowId, TableId, TxnId};
use crate::tel::merge::{ColumnPolicyLookup, DiscardedWrite};
use crate::tel::op::OpKind;

// ---------------------------------------------------------------------------------------------
// What a recorded op looks like to this module.
// ---------------------------------------------------------------------------------------------

/// One op as an op log holds it.
///
/// **This mirrors `agent_sql::runtime::AppliedOp` field for field and is deliberately not that
/// type**: `AppliedOp` is private to `runtime.rs`, as is the `State` that owns the log, and this
/// module does not own that file. Keeping the shape identical is what makes the wiring a
/// projection rather than a translation — a `CherryLog` impl over `State` is
/// `state.applied[i].clone()` with `branch` filled in.
///
/// The one field `AppliedOp` does not carry is [`RecordedOp::branch`]. The runtime recovers a
/// published op's branch through `MergeRecord { branch, txns }` keyed by `txn`; an impl over the
/// runtime resolves it there once rather than making every caller do the join.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedOp {
    /// Strictly increasing over the whole log, which is what makes a cell's position list sorted
    /// in seq as well as in position — the property D86's `partition_point` range depends on.
    pub seq: u64,
    pub txn: TxnId,
    pub branch: BranchId,
    pub table: String,
    pub tbl: TableId,
    pub row: RowId,
    /// `None` for whole-row ops (`RowCreate`, `RowDelete`).
    pub col: Option<ColId>,
    pub kind: OpKind,
    /// Value on the cell immediately before this op landed. The anchor of the "has the target
    /// moved?" question, and the witness [`invert`] needs to undo an `Assign`.
    pub before: Option<Value>,
    /// Whole-row image before the op landed, for inverting `RowCreate` / `RowDelete`.
    pub before_row: Option<Vec<Value>>,
}

/// The source of recorded ops, and of the divergence question.
///
/// Two methods, because D86 established that these are the only two shapes the question comes in:
/// address one op, or address one cell's history.
pub trait CherryLog {
    /// The op recorded at `seq`, if any.
    fn op_at(&self, seq: u64) -> Option<&RecordedOp>;

    /// Every seq at which an op landed on one cell, **increasing**, oldest first.
    ///
    /// This is D86's `State::applied_by_cell` keyed exactly as it is keyed there — `(tbl.0,
    /// row.0, col.0)` — and the increasing order is the same property `concurrent_op`'s
    /// `partition_point` relies on. Implementations must not build a second index for it:
    /// the whole point of asking for this shape is that the one that already exists answers it.
    ///
    /// Empty for a cell nothing ever wrote, which is the common case and is what makes it worth
    /// indexing at all.
    fn ops_on_cell(&self, tbl: TableId, row: RowId, col: ColId) -> &[u64];
}

// ---------------------------------------------------------------------------------------------
// The target, and the single door that writes.
// ---------------------------------------------------------------------------------------------

/// One write a cherry-pick would make.
///
/// `before` / `before_row` travel with the write so [`CherryPlan::inverse`] can build the undo
/// without re-reading a target that has since moved.
#[derive(Debug, Clone, PartialEq)]
pub enum CherryWrite {
    /// Set one cell.
    Cell {
        table: String,
        tbl: TableId,
        row: RowId,
        col: ColId,
        value: Value,
        /// What the target held before this write. The witness [`invert`] needs.
        before: Option<Value>,
    },
    /// Materialise a row, with every picked cell op on it already folded into the image.
    ///
    /// `replaced` is what the target held at this key beforehand, which is `None` for an ordinary
    /// create and `Some` when a selection deleted the row and recreated it. Carried so
    /// [`CherryPlan::inverse`] restores the prior image instead of deleting a row that existed.
    InsertRow {
        table: String,
        tbl: TableId,
        row: RowId,
        image: Vec<Value>,
        replaced: Option<Vec<Value>>,
    },
    /// Remove a row.
    DeleteRow { table: String, tbl: TableId, row: RowId, before_row: Vec<Value> },
}

impl CherryWrite {
    pub fn tbl(&self) -> TableId {
        match self {
            CherryWrite::Cell { tbl, .. }
            | CherryWrite::InsertRow { tbl, .. }
            | CherryWrite::DeleteRow { tbl, .. } => *tbl,
        }
    }

    pub fn row(&self) -> RowId {
        match self {
            CherryWrite::Cell { row, .. }
            | CherryWrite::InsertRow { row, .. }
            | CherryWrite::DeleteRow { row, .. } => *row,
        }
    }
}

/// The branch being picked **onto**.
///
/// **There is exactly one mutating method and it takes the whole plan.**
///
/// **Exactly what that buys, and what it does not.** The type stops *this engine* from applying a
/// subset: `plan_cherry_pick` takes `&dyn CherryTarget` and can only read, and `cherry_pick` holds
/// no call that writes one op, so a pick that refuses cannot have written and a pick that applies
/// went through one `commit_all`. That half is enforced by the signature.
///
/// The other half is **not** enforced, and an earlier version of this comment overstated it.
/// Whether `commit_all` is itself all-or-nothing is a contract an implementor owes and this module
/// cannot check; nothing stops any caller from writing `commit_all(&[one])` in a loop.
/// `MemCherryTarget` below honours it by journalling the pre-image of every row it touches and
/// restoring them on failure, and it is *tested* for it
/// (`row21_a_commit_that_fails_partway_leaves_the_target_unchanged`, and the two-touches case
/// beside it). A runtime impl gets it from the one `Mutex` and the `PendingWrite` batch a merge
/// already publishes under — which is a claim that will need its own test when that impl exists,
/// not one this trait can make on its behalf.
pub trait CherryTarget {
    /// The target's current image of a row, or `None` if it has no such row.
    fn row_image(&self, tbl: TableId, row: RowId) -> Option<Vec<Value>>;

    /// **The only door that writes.** All of `writes`, or none of them.
    fn commit_all(&mut self, writes: &[CherryWrite]) -> Result<(), FerroError>;
}

// ---------------------------------------------------------------------------------------------
// Selection, plan, result.
// ---------------------------------------------------------------------------------------------

/// Names one op to pick, by the sequence number the log recorded it at.
///
/// A `seq` and not a `(txn, tbl, row, col)` tuple because a txn may write a cell more than once
/// and the point of this operation is to be able to name *one* of those writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OpSelector {
    pub seq: u64,
}

impl OpSelector {
    pub const fn new(seq: u64) -> Self {
        OpSelector { seq }
    }
}

impl From<u64> for OpSelector {
    fn from(seq: u64) -> Self {
        OpSelector { seq }
    }
}

/// Every write a cherry-pick decided on, against a target it has not touched.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CherryPlan {
    pub writes: Vec<CherryWrite>,
}

impl CherryPlan {
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.writes.len()
    }

    /// The plan that undoes this one, built from [`invert`] — **the same primitive `undo_txn`
    /// calls**, so a landed pick is undone by the machinery `REVERT` already uses rather than by
    /// a second undo path.
    ///
    /// Applied through [`CherryTarget::commit_all`] like any other plan, so the undo is atomic on
    /// exactly the terms the pick was. Order is reversed: the last write made is the first undone.
    ///
    /// Refuses, rather than guessing, wherever `invert` refuses — an `Assign` with no recorded
    /// before-image has no inverse, and a `DeleteRow` with no before-image cannot be resurrected.
    pub fn inverse(&self) -> Result<CherryPlan, FerroError> {
        let mut out = Vec::with_capacity(self.writes.len());
        for w in self.writes.iter().rev() {
            out.push(match w {
                CherryWrite::Cell { table, tbl, row, col, value, before } => {
                    // `invert` is asked for the inverse of the *Assign this write performed*, with
                    // the pre-image as its witness. That is the identical call `undo_txn` makes.
                    let back = invert(&OpKind::Assign(value.clone()), before.as_ref())?;
                    let restored = match back {
                        OpKind::Assign(v) => v,
                        other => {
                            return Err(FerroError::Merge(format!(
                                "inverting a cherry-picked cell write produced {}, which is not a value",
                                other.name()
                            )))
                        }
                    };
                    CherryWrite::Cell {
                        table: table.clone(),
                        tbl: *tbl,
                        row: *row,
                        col: *col,
                        value: restored,
                        before: Some(value.clone()),
                    }
                }
                CherryWrite::InsertRow { table, tbl, row, image, replaced } => match replaced {
                    // It replaced a row: put the prior image back, do not delete.
                    Some(prior) => CherryWrite::InsertRow {
                        table: table.clone(),
                        tbl: *tbl,
                        row: *row,
                        image: prior.clone(),
                        replaced: Some(image.clone()),
                    },
                    None => CherryWrite::DeleteRow {
                        table: table.clone(),
                        tbl: *tbl,
                        row: *row,
                        before_row: image.clone(),
                    },
                },
                CherryWrite::DeleteRow { table, tbl, row, before_row } => CherryWrite::InsertRow {
                    table: table.clone(),
                    tbl: *tbl,
                    row: *row,
                    image: before_row.clone(),
                    replaced: None,
                },
            });
        }
        Ok(CherryPlan { writes: out })
    }
}

/// Why a cherry-pick refused. One variant per refusing row of the truth table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CherryConflictKind {
    /// Row 3 / 6: the target cell is not what the source op observed, and the two effects do not
    /// commute — or the op recorded no witness, so "unchanged" cannot be established at all.
    TargetCellMoved,
    /// Rows 7 / 12 / 17: the row the op names is not on the target.
    RowGone,
    /// Row 10: a `RowCreate` for a row the target already has.
    RowExists,
    /// Row 8: the target's row is too narrow to hold the column the op names.
    ColumnAbsent,
    /// Row 14: the selected ops on one cell have no composition in the algebra.
    ComposeFailed,
    /// Row 22: the algebra cannot apply this op to a cell at all — the set ops, which
    /// `merge_engine::apply_op` has no arm for, and any cell whose composed effect will not
    /// apply to the value present. A caller can express this, so it is a refusal and not an
    /// engine error.
    OpNotApplicable,
    /// Row 23: the recorded op's own shape is wrong — a cell kind with no column, or a whole-row
    /// kind carrying one.
    MalformedOp,
    /// Row 15: the same op was selected more than once. `Add` is not idempotent.
    DuplicateSelector,
    /// Row 18: no op was recorded at that seq.
    NoSuchOp,
    /// Row 19: that op belongs to a branch other than the one being picked from.
    NotFromSource,
    /// Row 20: nothing was selected.
    EmptySelection,
}

impl CherryConflictKind {
    pub fn name(&self) -> &'static str {
        match self {
            CherryConflictKind::TargetCellMoved => "TargetCellMoved",
            CherryConflictKind::RowGone => "RowGone",
            CherryConflictKind::RowExists => "RowExists",
            CherryConflictKind::ColumnAbsent => "ColumnAbsent",
            CherryConflictKind::ComposeFailed => "ComposeFailed",
            CherryConflictKind::OpNotApplicable => "OpNotApplicable",
            CherryConflictKind::MalformedOp => "MalformedOp",
            CherryConflictKind::DuplicateSelector => "DuplicateSelector",
            CherryConflictKind::NoSuchOp => "NoSuchOp",
            CherryConflictKind::NotFromSource => "NotFromSource",
            CherryConflictKind::EmptySelection => "EmptySelection",
        }
    }
}

/// One reason a cherry-pick refused, carrying enough for the agent to re-select rather than guess.
#[derive(Debug, Clone, PartialEq)]
pub struct CherryConflict {
    pub kind: CherryConflictKind,
    /// The selector that caused it, where one did.
    pub seq: Option<u64>,
    pub tbl: Option<TableId>,
    pub row: Option<RowId>,
    pub col: Option<ColId>,
    pub detail: String,
}

/// A cell whose picked ops are not a contiguous run of that cell's source history.
///
/// Reported, never decisive — see the note on truth-table row 13.
#[derive(Debug, Clone, PartialEq)]
pub struct Straddled {
    pub tbl: TableId,
    pub row: RowId,
    pub col: ColId,
    /// The seqs that were picked.
    pub picked: Vec<u64>,
    /// The seqs on this cell that sit between picked ops and were **not** picked.
    pub skipped: Vec<u64>,
}

/// A cherry-pick that landed.
#[derive(Debug, Clone, PartialEq)]
pub struct CherryApplied {
    pub from: BranchId,
    pub onto: BranchId,
    /// How many ops were selected.
    pub picked: usize,
    /// The plan that was committed, retained so it can be [`CherryPlan::inverse`]d.
    pub plan: CherryPlan,
    /// Writes a policy threw away in order to succeed (truth-table row 5).
    pub discarded: Vec<DiscardedWrite>,
    /// Cells whose picked ops skipped over unpicked ones (truth-table row 13's note).
    pub straddled: Vec<Straddled>,
}

/// A cherry-pick that refused. **Nothing was written.**
#[derive(Debug, Clone, PartialEq)]
pub struct CherryRefusal {
    pub from: BranchId,
    pub onto: BranchId,
    /// Every reason found **at the stage that refused** — an agent re-selecting wants the whole
    /// list, not just the first.
    ///
    /// Selector resolution is its own stage and short-circuits: if a selector names no op, names
    /// another branch's, or repeats, nothing is planned at all, because a plan computed over a
    /// different op set than the one asked for is not an answer to the question. So a selection
    /// with both a bad selector and a moved cell reports only the bad selector.
    pub conflicts: Vec<CherryConflict>,
}

impl CherryRefusal {
    pub fn kinds(&self) -> Vec<CherryConflictKind> {
        self.conflicts.iter().map(|c| c.kind).collect()
    }

    pub fn has(&self, kind: CherryConflictKind) -> bool {
        self.conflicts.iter().any(|c| c.kind == kind)
    }
}

/// The outcome of a cherry-pick. There is no third state: it landed whole, or it wrote nothing.
#[derive(Debug, Clone, PartialEq)]
pub enum CherryResult {
    Applied(CherryApplied),
    Refused(CherryRefusal),
}

impl CherryResult {
    pub fn is_applied(&self) -> bool {
        matches!(self, CherryResult::Applied(_))
    }

    pub fn refusal(&self) -> Option<&CherryRefusal> {
        match self {
            CherryResult::Refused(r) => Some(r),
            _ => None,
        }
    }

    pub fn applied(&self) -> Option<&CherryApplied> {
        match self {
            CherryResult::Applied(a) => Some(a),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The engine.
// ---------------------------------------------------------------------------------------------

/// Apply a **selected subset** of `from`'s recorded ops to `onto`.
///
/// The three arguments the operation is *about* are `from`, `ops` and `onto`; `log`, `target` and
/// `policy` are the context it reads and writes through, exactly as `merge` takes an `ExecCtx`.
///
/// Returns `Err` only for a *malformed* request the caller could not have expressed correctly
/// (an impossible internal state). Everything a caller can legitimately hit — every row of the
/// truth table — comes back as [`CherryResult::Refused`], because a refusal is an answer and the
/// agent needs the reports.
///
/// **Nothing is written unless every selected op can be applied.** See the module header: the
/// half-applied state is unrepresentable, not merely avoided.
pub fn cherry_pick(
    log: &dyn CherryLog,
    from: BranchId,
    ops: &[OpSelector],
    onto: BranchId,
    target: &mut dyn CherryTarget,
    policy: &dyn ColumnPolicyLookup,
) -> Result<CherryResult, FerroError> {
    let planned = plan_cherry_pick(log, from, ops, onto, target, policy)?;
    match planned {
        CherryResult::Refused(r) => Ok(CherryResult::Refused(r)),
        CherryResult::Applied(a) => {
            // The single door. Reached only once every row has been decided.
            target.commit_all(&a.plan.writes)?;
            Ok(CherryResult::Applied(a))
        }
    }
}

/// Decide the whole pick **without touching the target**.
///
/// Split out from [`cherry_pick`] for the same reason `evaluate_merge` is split from
/// `publish_evaluation`: between the two calls the target is untouched, so K candidate selections
/// can be scored against one identical base. It is also what makes the atomicity test able to
/// assert that a refusal reached no writer at all.
pub fn plan_cherry_pick(
    log: &dyn CherryLog,
    from: BranchId,
    ops: &[OpSelector],
    onto: BranchId,
    target: &dyn CherryTarget,
    policy: &dyn ColumnPolicyLookup,
) -> Result<CherryResult, FerroError> {
    let mut conflicts: Vec<CherryConflict> = Vec::new();

    // ---- Row 20: a pick that picked nothing has not picked. -----------------------------------
    //
    // Refused rather than returned as an empty success, for the same reason a run that collected
    // zero tests has not passed: an empty plan is indistinguishable from a satisfied one at the
    // call site, and the caller that passed an empty selection made a mistake upstream.
    if ops.is_empty() {
        return Ok(CherryResult::Refused(CherryRefusal {
            from,
            onto,
            conflicts: vec![CherryConflict {
                kind: CherryConflictKind::EmptySelection,
                seq: None,
                tbl: None,
                row: None,
                col: None,
                detail: "cherry-pick selected no ops; an empty pick is refused, not treated as a \
                         satisfied one"
                    .into(),
            }],
        }));
    }

    // ---- Row 15: the same op twice. `Add` is not idempotent. ----------------------------------
    let mut seen: BTreeSet<u64> = BTreeSet::new();
    for s in ops {
        if !seen.insert(s.seq) {
            conflicts.push(CherryConflict {
                kind: CherryConflictKind::DuplicateSelector,
                seq: Some(s.seq),
                tbl: None,
                row: None,
                col: None,
                detail: format!(
                    "op {} was selected more than once; `Add` is not idempotent, so a replayed \
                     selector double-counts",
                    s.seq
                ),
            });
        }
    }

    // ---- Rows 18 / 19: resolve every selector against the log. --------------------------------
    let mut picked: Vec<&RecordedOp> = Vec::with_capacity(seen.len());
    for seq in &seen {
        match log.op_at(*seq) {
            None => conflicts.push(CherryConflict {
                kind: CherryConflictKind::NoSuchOp,
                seq: Some(*seq),
                tbl: None,
                row: None,
                col: None,
                detail: format!("no op was recorded at seq {}", seq),
            }),
            Some(op) if op.branch != from => conflicts.push(CherryConflict {
                kind: CherryConflictKind::NotFromSource,
                seq: Some(*seq),
                tbl: Some(op.tbl),
                row: Some(op.row),
                col: op.col,
                detail: format!(
                    "op {} was recorded by branch {}, not by the source branch {}",
                    seq, op.branch, from
                ),
            }),
            Some(op) => picked.push(op),
        }
    }

    // A selector that did not resolve makes every downstream decision meaningless — the plan
    // would be computed over a different set of ops than the one that was asked for.
    if !conflicts.is_empty() {
        return Ok(CherryResult::Refused(CherryRefusal { from, onto, conflicts }));
    }

    // `seen` is a BTreeSet, so `picked` is already in increasing seq. Replay order is log order.
    let selected: BTreeSet<u64> = seen;

    // Group by row. Every decision below is per row, because whole-row ops and cell ops on one
    // row interact and nothing across rows does.
    let mut by_row: BTreeMap<(u32, u64), Vec<&RecordedOp>> = BTreeMap::new();
    for op in &picked {
        by_row.entry((op.tbl.0, op.row.0)).or_default().push(op);
    }

    let mut writes: Vec<CherryWrite> = Vec::new();
    let mut discarded: Vec<DiscardedWrite> = Vec::new();
    let mut straddled: Vec<Straddled> = Vec::new();

    for (_, row_ops) in by_row {
        plan_one_row(
            log,
            from,
            &row_ops,
            &selected,
            target,
            policy,
            &mut writes,
            &mut discarded,
            &mut straddled,
            &mut conflicts,
        )?;
    }

    if !conflicts.is_empty() {
        // **Row 21.** Every refusal lands here, and here is before any writer exists. The plan
        // built so far is dropped.
        return Ok(CherryResult::Refused(CherryRefusal { from, onto, conflicts }));
    }

    Ok(CherryResult::Applied(CherryApplied {
        from,
        onto,
        picked: picked.len(),
        plan: CherryPlan { writes },
        discarded,
        straddled,
    }))
}

/// Decide one row's worth of the pick.
///
/// The row's selected ops arrive in increasing seq. There are two shapes and they are decided
/// separately, because whole-row ops determine whether the row even exists to have cells:
///
/// * **no whole-row op** — the ordinary case. Group the cell ops by column, compose each group
///   into one effect, and resolve it against the target through [`resolve_cell`].
/// * **any whole-row op** — walk every op for the row in **seq order** against a staged image,
///   and emit the one net write at the end.
///
/// The walk replaced a version that looked only at the *last* whole-row op
/// (`row_ops.iter().rev().find(..)`), which a fresh-context review falsified: a selection of
/// `RowCreate` then `RowDelete` onto a target that **already had the row** never checked the
/// create, so truth-table row 10's `RowExists` refusal was bypassed and the row was deleted with
/// no report that half the selection had been discarded. The mirror case over-refused: a
/// legitimate delete-then-recreate was rejected as `RowExists`. Both are covered by `d5_*` below.
#[allow(clippy::too_many_arguments)]
fn plan_one_row(
    log: &dyn CherryLog,
    from: BranchId,
    row_ops: &[&RecordedOp],
    selected: &BTreeSet<u64>,
    target: &dyn CherryTarget,
    policy: &dyn ColumnPolicyLookup,
    writes: &mut Vec<CherryWrite>,
    discarded: &mut Vec<DiscardedWrite>,
    straddled: &mut Vec<Straddled>,
    conflicts: &mut Vec<CherryConflict>,
) -> Result<(), FerroError> {
    let first = row_ops[0];
    let (tbl, row, table) = (first.tbl, first.row, first.table.clone());
    let real = target.row_image(tbl, row);

    if row_ops.iter().any(|o| o.col.is_none()) {
        return plan_row_with_whole_row_ops(row_ops, tbl, row, table, real, writes, conflicts);
    }

    // ---- The ordinary case: cell ops against a row the target already has. --------------------
    let image = match real {
        Some(i) => i,
        // Row 7.
        None => {
            for o in row_ops {
                conflicts.push(CherryConflict {
                    kind: CherryConflictKind::RowGone,
                    seq: Some(o.seq),
                    tbl: Some(tbl),
                    row: Some(row),
                    col: o.col,
                    detail: format!(
                        "op {} writes a cell of row {}, which the target does not have",
                        o.seq, row
                    ),
                });
            }
            return Ok(());
        }
    };

    let mut by_col: BTreeMap<u32, Vec<&RecordedOp>> = BTreeMap::new();
    for o in row_ops {
        by_col.entry(o.col.expect("filtered to cell ops").0).or_default().push(o);
    }

    for (col_raw, ops) in by_col {
        let col = ColId(col_raw);
        let idx = col_raw as usize;

        // Row 8.
        let target_now = match image.get(idx) {
            Some(v) => v.clone(),
            None => {
                conflicts.push(CherryConflict {
                    kind: CherryConflictKind::ColumnAbsent,
                    seq: Some(ops[0].seq),
                    tbl: Some(tbl),
                    row: Some(row),
                    col: Some(col),
                    detail: format!(
                        "op {} names column {} of a {}-column target row",
                        ops[0].seq,
                        col_raw,
                        image.len()
                    ),
                });
                continue;
            }
        };

        // Rows 13 / 14: compose the selected ops on this cell into one effect.
        let kinds: Vec<OpKind> = ops.iter().map(|o| o.kind.clone()).collect();
        let ours = match compose_ops(&kinds) {
            Ok(k) => k,
            Err(e) => {
                conflicts.push(compose_failed(tbl, row, col, &ops, &e));
                continue;
            }
        };

        // Row 13's note: report a non-contiguous selection, do not refuse it.
        let picked_seqs: Vec<u64> = ops.iter().map(|o| o.seq).collect();
        if let (Some(lo), Some(hi)) = (picked_seqs.first(), picked_seqs.last()) {
            let skipped: Vec<u64> = log
                .ops_on_cell(tbl, row, col)
                .iter()
                .copied()
                .filter(|s| s > lo && s < hi && !selected.contains(s))
                .collect();
            if !skipped.is_empty() {
                straddled.push(Straddled { tbl, row, col, picked: picked_seqs.clone(), skipped });
            }
        }

        // The witness is the *earliest* selected op's before-image: that is the value the replay
        // starts from, so it is the one the target must still hold.
        let witness = ops[0].before.clone();
        let theirs = divergence(log, from, tbl, row, col, witness.as_ref(), &target_now);

        // **Reuse, not reimplementation.** This is the merge engine's cell decision, unchanged.
        // `base` is the fork-point witness, `target` is what the target holds now, `ours` is the
        // composed *selected* effect, `theirs` is what the target absorbed. Only `ours` differs
        // from a merge, which is the whole thesis of this module.
        let cell = CellMerge {
            tbl,
            row,
            col,
            base: witness.clone(),
            target: Some(target_now.clone()),
            ours,
            theirs,
        };
        // **Row 22.** A caller can select an op whose kind the cell algebra cannot apply — the set
        // ops, which `apply_op` has no arm for. That is a refusal, not an engine error: the
        // contract on `cherry_pick` says `Err` is reserved for an impossible internal state.
        let resolved = match resolve_cell(&cell, from, policy) {
            Ok(r) => r,
            Err(e) => {
                conflicts.push(CherryConflict {
                    kind: CherryConflictKind::OpNotApplicable,
                    seq: Some(ops[0].seq),
                    tbl: Some(tbl),
                    row: Some(row),
                    col: Some(col),
                    detail: format!("op {} cannot be applied to this cell: {}", ops[0].seq, e),
                });
                continue;
            }
        };
        match resolved {
            // Rows 1 / 2 / 4.
            CellResolution::Clean { value, .. } | CellResolution::Commuting { value, .. } => {
                writes.push(CherryWrite::Cell {
                    table: table.clone(),
                    tbl,
                    row,
                    col,
                    value,
                    before: Some(target_now),
                })
            }
            // Row 5.
            CellResolution::Lossy { value, discarded: d, .. } => {
                discarded.push(d);
                writes.push(CherryWrite::Cell {
                    table: table.clone(),
                    tbl,
                    row,
                    col,
                    value,
                    before: Some(target_now),
                })
            }
            // Rows 3 / 6.
            CellResolution::Conflict(report) => conflicts.push(CherryConflict {
                kind: CherryConflictKind::TargetCellMoved,
                seq: Some(ops[0].seq),
                tbl: Some(tbl),
                row: Some(row),
                col: Some(col),
                detail: format!(
                    "the target moved under op {} and the two effects do not commute: {}",
                    ops[0].seq, report.detail
                ),
            }),
        }
    }
    Ok(())
}

/// Walk one row's selection in **seq order**, staging its existence and image, and emit the one
/// net write.
///
/// Every refusal returns immediately: once the row's history contradicts the target there is
/// nothing further to decide about it, and the whole pick is refused anyway.
///
/// **A cell op reached while the row was not created by this selection is skipped**, and that is
/// safe rather than lossy. In this branch the selection contains at least one whole-row op, so
/// such an op is either followed by a `RowDelete` — which subsumes it, the row is leaving — or by
/// a `RowCreate`, which refuses below because the row still exists. It cannot be the last word on
/// a row that survives.
fn plan_row_with_whole_row_ops(
    row_ops: &[&RecordedOp],
    tbl: TableId,
    row: RowId,
    table: String,
    real: Option<Vec<Value>>,
    writes: &mut Vec<CherryWrite>,
    conflicts: &mut Vec<CherryConflict>,
) -> Result<(), FerroError> {
    let mut image: Option<Vec<Value>> = real.clone();
    let mut created_here = false;

    for o in row_ops {
        match (&o.kind, o.col) {
            // Rows 9 / 10.
            (OpKind::RowCreate(initial), None) => {
                if image.is_some() {
                    conflicts.push(CherryConflict {
                        kind: CherryConflictKind::RowExists,
                        seq: Some(o.seq),
                        tbl: Some(tbl),
                        row: Some(row),
                        col: None,
                        detail: format!(
                            "op {} creates row {}, which already exists at that point in the \
                             selection",
                            o.seq, row
                        ),
                    });
                    return Ok(());
                }
                image = Some(initial.clone());
                created_here = true;
            }
            // Rows 11 / 12.
            (OpKind::RowDelete, None) => {
                if image.is_none() {
                    conflicts.push(CherryConflict {
                        kind: CherryConflictKind::RowGone,
                        seq: Some(o.seq),
                        tbl: Some(tbl),
                        row: Some(row),
                        col: None,
                        detail: format!(
                            "op {} deletes row {}, which does not exist at that point in the \
                             selection",
                            o.seq, row
                        ),
                    });
                    return Ok(());
                }
                image = None;
                created_here = false;
            }
            // Row 23: a cell kind recorded with no column.
            (kind, None) => {
                conflicts.push(CherryConflict {
                    kind: CherryConflictKind::MalformedOp,
                    seq: Some(o.seq),
                    tbl: Some(tbl),
                    row: Some(row),
                    col: None,
                    detail: format!(
                        "op {} is recorded with no column but kind {}, which is a cell op",
                        o.seq,
                        kind.name()
                    ),
                });
                return Ok(());
            }
            // Rows 16 / 17.
            (kind, Some(col)) => {
                let img = match image.as_mut() {
                    Some(i) => i,
                    None => {
                        conflicts.push(CherryConflict {
                            kind: CherryConflictKind::RowGone,
                            seq: Some(o.seq),
                            tbl: Some(tbl),
                            row: Some(row),
                            col: Some(col),
                            detail: format!(
                                "op {} writes a cell of row {}, which does not exist at that \
                                 point in the selection",
                                o.seq, row
                            ),
                        });
                        return Ok(());
                    }
                };
                if !created_here {
                    // Subsumed by a later delete, or the walk refuses at a later create. See the
                    // function header for why this cannot silently drop a surviving write.
                    continue;
                }
                let idx = col.0 as usize;
                if idx >= img.len() {
                    conflicts.push(CherryConflict {
                        kind: CherryConflictKind::ColumnAbsent,
                        seq: Some(o.seq),
                        tbl: Some(tbl),
                        row: Some(row),
                        col: Some(col),
                        detail: format!(
                            "op {} names column {} of a {}-column row image",
                            o.seq,
                            col.0,
                            img.len()
                        ),
                    });
                    return Ok(());
                }
                // No divergence check: the row does not exist on the target in this form, so
                // there is nothing on the target for the op to contend with.
                match apply_op(img.get(idx), kind) {
                    Ok(v) => img[idx] = v,
                    Err(e) => {
                        conflicts.push(CherryConflict {
                            kind: CherryConflictKind::OpNotApplicable,
                            seq: Some(o.seq),
                            tbl: Some(tbl),
                            row: Some(row),
                            col: Some(col),
                            detail: format!(
                                "op {} cannot be applied to the staged row image: {}",
                                o.seq, e
                            ),
                        });
                        return Ok(());
                    }
                }
            }
        }
    }

    // The one net write. A selection that creates and then deletes a row the target never had
    // writes nothing, which is the correct net effect and not a dropped op.
    match (real, image) {
        (Some(prior), Some(final_image)) => {
            if prior != final_image {
                writes.push(CherryWrite::InsertRow {
                    table,
                    tbl,
                    row,
                    image: final_image,
                    replaced: Some(prior),
                });
            }
        }
        (Some(prior), None) => {
            writes.push(CherryWrite::DeleteRow { table, tbl, row, before_row: prior })
        }
        (None, Some(final_image)) => writes.push(CherryWrite::InsertRow {
            table,
            tbl,
            row,
            image: final_image,
            replaced: None,
        }),
        (None, None) => {}
    }
    Ok(())
}

fn compose_failed(
    tbl: TableId,
    row: RowId,
    col: ColId,
    ops: &[&RecordedOp],
    e: &FerroError,
) -> CherryConflict {
    let seqs: Vec<String> = ops.iter().map(|o| o.seq.to_string()).collect();
    CherryConflict {
        kind: CherryConflictKind::ComposeFailed,
        seq: Some(ops[0].seq),
        tbl: Some(tbl),
        row: Some(row),
        col: Some(col),
        detail: format!(
            "the selected ops [{}] on this cell have no composition in the algebra: {}",
            seqs.join(", "),
            e
        ),
    }
}

/// What **the target** absorbed on this cell, if anything.
///
/// Two rules, and the second one exists because a fresh-context review falsified the first
/// version of this function twice over.
///
/// **1. Only ops the source branch did NOT record can be the target's divergence.** The first
/// version filtered on `seq > the picked op's seq` and nothing else, copying `concurrent_op`'s
/// shape (`runtime.rs:3744`). That shape does not transfer, and the reason is worth stating: what
/// `concurrent_op` reads is `state.applied`, the log of **published** ops, cut at `fork_seq` — so
/// everything above its cut genuinely does belong to the target. Cherry-pick's input population is
/// different, because the source branch's ops must be in the log to be *selectable* at all. The
/// unfiltered read therefore scooped up the source's own unselected ops and offered them as "what
/// the target absorbed": with a source that assigned a cell twice and only the first pick taken,
/// the second (unselected) op equalled ours, `resolve_cell`'s same-value shortcut fired, and the
/// target's concurrent write was **silently overwritten**. See
/// `d1_an_unselected_source_op_is_not_the_targets_divergence`.
///
/// The seq cut is gone with it, and that fixed a second defect: `seq` is one global counter over a
/// log that interleaves branches, so the target's write to a cell is not obliged to come *after*
/// the source's. With the cut in place, truth-table row 2 — two `Add`s that should commute —
/// refused whenever the target happened to write first, which for cherry-pick is the likely order
/// (the fix being picked is recent; the branch it lands on has been writing that cell for a
/// while). See `d2_commuting_divergence_is_found_when_the_targets_op_has_the_lower_seq`.
///
/// **2. A composed divergence is trusted only if it EXPLAINS the value the target actually
/// holds.** Composing every non-source op on the cell can over-count: an op that landed before the
/// source forked is already folded into the source op's witness, and there is no fork point here
/// to cut it out with. So the composition is applied to the witness and checked against the
/// target's current value. If it does not reproduce it, something the log cannot account for
/// touched this cell, and the answer is the conservative one below rather than a guess wearing a
/// reading's clothes. See `row27_divergence_that_does_not_explain_the_targets_value_is_not_trusted`.
///
/// That check is also what makes the log read *testable*. Rows 3/4/5's fixtures cannot tell a log
/// read from the fallback — both yield the same `theirs` — so row 2 and the verification test are
/// the only two places where the difference is observable.
///
/// The fallback is an opaque `Assign` of whatever the target holds. Under the default `Reject`
/// policy that conflicts with everything: `OpKind::commutes_with` (`tel/op.rs:117`) pairs only
/// Add/Add, Max/Max, Min/Min and the set ops, and answers `false` for every pair involving an
/// `Assign`. So the fallback always refuses, which is the intended reading of "we cannot establish
/// what happened here".
fn divergence(
    log: &dyn CherryLog,
    from: BranchId,
    tbl: TableId,
    row: RowId,
    col: ColId,
    witness: Option<&Value>,
    target_now: &Value,
) -> Option<OpKind> {
    // Row 1: provably unchanged. The only case that needs no divergence at all.
    if witness == Some(target_now) {
        return None;
    }

    if let Some(w) = witness {
        // D86's by-cell key answers "which ops touched this cell" in one lookup. The branch
        // filter is applied to what it returns, not to a scan of the log.
        let kinds: Vec<OpKind> = log
            .ops_on_cell(tbl, row, col)
            .iter()
            .filter_map(|&s| log.op_at(s))
            .filter(|o| o.branch != from)
            .map(|o| o.kind.clone())
            .collect();
        if !kinds.is_empty() {
            if let Ok(composed) = compose_ops(&kinds) {
                if let Ok(reached) = apply_op(Some(w), &composed) {
                    if &reached == target_now {
                        return Some(composed);
                    }
                }
            }
        }
    }

    Some(OpKind::Assign(target_now.clone()))
}

// ---------------------------------------------------------------------------------------------
// In-memory implementations, used by the tests and by `examples/d100_cherry_pick.rs`.
// ---------------------------------------------------------------------------------------------

/// An op log with D86's by-cell index beside it.
///
/// **The index is maintained in `push` and nowhere else**, for the reason `State::push_applied`
/// gives: a second source of truth rots when someone adds a write and does not know about the
/// index, so the append is the only operation.
#[derive(Debug, Default, Clone)]
pub struct MemCherryLog {
    ops: BTreeMap<u64, RecordedOp>,
    by_cell: BTreeMap<(u32, u64, u32), Vec<u64>>,
    next_seq: u64,
}

impl MemCherryLog {
    pub fn new() -> Self {
        MemCherryLog::default()
    }

    /// Append an op, assigning it the next seq. Returns the seq, which is what a caller selects by.
    pub fn push(&mut self, mut op: RecordedOp) -> u64 {
        self.next_seq += 1;
        op.seq = self.next_seq;
        if let Some(col) = op.col {
            self.by_cell.entry((op.tbl.0, op.row.0, col.0)).or_default().push(op.seq);
        }
        let seq = op.seq;
        self.ops.insert(seq, op);
        seq
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

impl CherryLog for MemCherryLog {
    fn op_at(&self, seq: u64) -> Option<&RecordedOp> {
        self.ops.get(&seq)
    }

    fn ops_on_cell(&self, tbl: TableId, row: RowId, col: ColId) -> &[u64] {
        self.by_cell.get(&(tbl.0, row.0, col.0)).map(|v| v.as_slice()).unwrap_or(&[])
    }
}

/// A target whose `commit_all` is all-or-nothing **by construction**: it journals the pre-image
/// of every row the plan touches and restores them if any write fails.
///
/// **It journals the touched rows and not the whole map, and that is a measurement decision as
/// much as a design one.** The first version cloned `self.rows` and swapped, which is simpler and
/// equally correct — and it made `commit_all` cost O(rows in the target). The D100 cost harness
/// then reported a **91.8x** rise across a 100x larger target and it was entirely this clone:
/// the harness was the wall, not the engine. Journalling the touched rows is O(writes), so the
/// curve measures the pick.
///
/// It also counts the calls, which is what lets the atomicity test assert that a refusal reached
/// the writer zero times rather than merely that the visible state looks unchanged.
#[derive(Debug, Default, Clone)]
pub struct MemCherryTarget {
    rows: BTreeMap<(u32, u64), Vec<Value>>,
    /// How many times `commit_all` was entered. A refusal must leave this at its prior value.
    pub commits: usize,
    /// How many individual writes have been applied, across all commits.
    pub applied_writes: usize,
    /// When set, `commit_all` fails on the write at this index — to prove the swap, not just the
    /// planning, is atomic.
    pub fail_at: Option<usize>,
}

impl MemCherryTarget {
    pub fn new() -> Self {
        MemCherryTarget::default()
    }

    pub fn insert(&mut self, tbl: TableId, row: RowId, image: Vec<Value>) {
        self.rows.insert((tbl.0, row.0), image);
    }

    pub fn get(&self, tbl: TableId, row: RowId) -> Option<&Vec<Value>> {
        self.rows.get(&(tbl.0, row.0))
    }

    /// Drop a row, to set up the "a sibling deleted it" case without going through a plan.
    pub fn remove(&mut self, tbl: TableId, row: RowId) -> Option<Vec<Value>> {
        self.rows.remove(&(tbl.0, row.0))
    }

    pub fn cell(&self, tbl: TableId, row: RowId, col: ColId) -> Option<&Value> {
        self.rows.get(&(tbl.0, row.0)).and_then(|r| r.get(col.0 as usize))
    }

    /// Every row, for a byte-for-byte comparison against a pre-image.
    pub fn snapshot(&self) -> BTreeMap<(u32, u64), Vec<Value>> {
        self.rows.clone()
    }
}

impl CherryTarget for MemCherryTarget {
    fn row_image(&self, tbl: TableId, row: RowId) -> Option<Vec<Value>> {
        self.rows.get(&(tbl.0, row.0)).cloned()
    }

    fn commit_all(&mut self, writes: &[CherryWrite]) -> Result<(), FerroError> {
        self.commits += 1;
        let mut journal: Vec<((u32, u64), Option<Vec<Value>>)> = Vec::with_capacity(writes.len());
        match Self::apply_journalled(&mut self.rows, writes, self.fail_at, &mut journal) {
            Ok(()) => {
                self.applied_writes += writes.len();
                Ok(())
            }
            Err(e) => {
                // Restore in REVERSE, so a row touched more than once ends at the pre-image its
                // FIRST touch recorded — the one that predates this commit.
                for (key, pre) in journal.into_iter().rev() {
                    match pre {
                        Some(image) => {
                            self.rows.insert(key, image);
                        }
                        None => {
                            self.rows.remove(&key);
                        }
                    }
                }
                Err(e)
            }
        }
    }
}

impl MemCherryTarget {
    /// Apply every write, recording each touched row's pre-image before touching it, so the
    /// caller can undo exactly what was done and nothing else.
    fn apply_journalled(
        rows: &mut BTreeMap<(u32, u64), Vec<Value>>,
        writes: &[CherryWrite],
        fail_at: Option<usize>,
        journal: &mut Vec<((u32, u64), Option<Vec<Value>>)>,
    ) -> Result<(), FerroError> {
        for (i, w) in writes.iter().enumerate() {
            let key = (w.tbl().0, w.row().0);
            journal.push((key, rows.get(&key).cloned()));
            if fail_at == Some(i) {
                return Err(FerroError::Merge(format!(
                    "injected failure at write {} of {}",
                    i,
                    writes.len()
                )));
            }
            match w {
                CherryWrite::Cell { row, col, value, .. } => {
                    let image = rows.get_mut(&key).ok_or_else(|| {
                        FerroError::Merge(format!("commit: row {} vanished under the plan", row))
                    })?;
                    let idx = col.0 as usize;
                    if idx >= image.len() {
                        return Err(FerroError::Merge(format!(
                            "commit: column {} is past the {}-column row {}",
                            col.0,
                            image.len(),
                            row
                        )));
                    }
                    image[idx] = value.clone();
                }
                CherryWrite::InsertRow { image, .. } => {
                    rows.insert(key, image.clone());
                }
                CherryWrite::DeleteRow { .. } => {
                    rows.remove(&key);
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Tests: one per row of the truth table, named for it.
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_sql::merge_engine::PolicyTable;
    use crate::tel::merge::MergePolicy;
    use crate::tel::op::Delta;

    const T: TableId = TableId(1);
    const R: RowId = RowId(7);
    const C: ColId = ColId(1);

    /// The branch every op in these fixtures was recorded by.
    fn src() -> BranchId {
        BranchId::new(7, 0)
    }

    fn dst() -> BranchId {
        BranchId::new(3, 0)
    }

    fn op(kind: OpKind, col: Option<ColId>, before: Option<Value>) -> RecordedOp {
        RecordedOp {
            seq: 0, // assigned by `MemCherryLog::push`
            txn: TxnId(1),
            branch: src(),
            table: "t".into(),
            tbl: T,
            row: R,
            col,
            kind,
            before,
            before_row: None,
        }
    }

    fn cell_op(kind: OpKind, before: Option<Value>) -> RecordedOp {
        op(kind, Some(C), before)
    }

    fn int(i: i32) -> Value {
        Value::Integer(i)
    }

    /// A target holding one 3-column row, so `ColId(1)` is in range and `ColId(9)` is not.
    fn target_with(v: Value) -> MemCherryTarget {
        let mut t = MemCherryTarget::new();
        t.insert(T, R, vec![int(0), v, int(0)]);
        t
    }

    fn pick(
        log: &MemCherryLog,
        seqs: &[u64],
        target: &mut MemCherryTarget,
        policy: &PolicyTable,
    ) -> CherryResult {
        let sel: Vec<OpSelector> = seqs.iter().copied().map(OpSelector::new).collect();
        cherry_pick(log, src(), &sel, dst(), target, policy).expect("engine error")
    }

    // -- Row 1 ---------------------------------------------------------------------------------

    #[test]
    fn row01_unchanged_target_cell_applies_clean() {
        let mut log = MemCherryLog::new();
        let s = log.push(cell_op(OpKind::Assign(int(42)), Some(int(5))));
        let mut t = target_with(int(5)); // exactly the witness
        let r = pick(&log, &[s], &mut t, &PolicyTable::new());
        assert!(r.is_applied(), "expected APPLY, got {:?}", r);
        assert_eq!(t.cell(T, R, C), Some(&int(42)));
        assert_eq!(t.commits, 1);
    }

    // -- Row 2 ---------------------------------------------------------------------------------

    #[test]
    fn row02_target_moved_but_ops_commute_applies_composed_onto_target() {
        let mut log = MemCherryLog::new();
        // The source took 3 off a cell that held 20.
        let ours = log.push(cell_op(OpKind::Add(Delta::Int(-3)), Some(int(20))));
        // The target took 5 off the same cell, recorded in the same log.
        let mut theirs = cell_op(OpKind::Add(Delta::Int(-5)), Some(int(20)));
        theirs.branch = dst();
        log.push(theirs);

        let mut t = target_with(int(15)); // 20 - 5, the target's own effect already landed
        let r = pick(&log, &[ours], &mut t, &PolicyTable::new());
        assert!(r.is_applied(), "expected APPLY, got {:?}", r);
        // 20 - 5 - 3 = 12. Not 17 (ours alone) and not 15 (theirs alone).
        assert_eq!(t.cell(T, R, C), Some(&int(12)));
    }

    // -- Row 3 ---------------------------------------------------------------------------------

    #[test]
    fn row03_target_moved_and_ops_contradict_refuses_whole() {
        let mut log = MemCherryLog::new();
        let ours = log.push(cell_op(OpKind::Assign(int(1)), Some(int(0))));
        let mut theirs = cell_op(OpKind::Assign(int(2)), Some(int(0)));
        theirs.branch = dst();
        log.push(theirs);

        let mut t = target_with(int(2));
        let r = pick(&log, &[ours], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("expected REFUSE");
        assert!(refusal.has(CherryConflictKind::TargetCellMoved), "{:?}", refusal);
        assert_eq!(t.cell(T, R, C), Some(&int(2)), "the target must be untouched");
        assert_eq!(t.commits, 0, "the writer must never have been entered");
    }

    // -- Row 4 ---------------------------------------------------------------------------------

    #[test]
    fn row04_target_moved_to_the_same_value_is_not_a_conflict() {
        let mut log = MemCherryLog::new();
        let ours = log.push(cell_op(OpKind::Assign(int(7)), Some(int(0))));
        let mut theirs = cell_op(OpKind::Assign(int(7)), Some(int(0)));
        theirs.branch = dst();
        log.push(theirs);

        let mut t = target_with(int(7));
        let r = pick(&log, &[ours], &mut t, &PolicyTable::new());
        assert!(r.is_applied(), "two writes of one value are not a conflict; got {:?}", r);
        assert_eq!(t.cell(T, R, C), Some(&int(7)));
    }

    // -- Row 5 ---------------------------------------------------------------------------------

    #[test]
    fn row05_lww_policy_applies_and_reports_the_discarded_write() {
        let mut log = MemCherryLog::new();
        let ours = log.push(cell_op(OpKind::Assign(int(1)), Some(int(0))));
        let mut theirs = cell_op(OpKind::Assign(int(2)), Some(int(0)));
        theirs.branch = dst();
        log.push(theirs);

        let mut policy = PolicyTable::new();
        policy.set(T, C, MergePolicy::Lww);
        let mut t = target_with(int(2));
        let r = pick(&log, &[ours], &mut t, &policy);
        let a = r.applied().expect("LWW admits the pick");
        assert_eq!(t.cell(T, R, C), Some(&int(1)));
        assert_eq!(a.discarded.len(), 1, "a thrown-away write must be reported");
        assert_eq!(a.discarded[0].policy, MergePolicy::Lww);
    }

    // -- Row 6 ---------------------------------------------------------------------------------

    #[test]
    fn row06_no_recorded_witness_refuses_rather_than_assuming_unchanged() {
        let mut log = MemCherryLog::new();
        // `before: None` — nothing observed what this cell held when the op landed.
        let s = log.push(cell_op(OpKind::Assign(int(1)), None));
        let mut t = target_with(int(99));
        let r = pick(&log, &[s], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("cannot establish the cell did not move");
        assert!(refusal.has(CherryConflictKind::TargetCellMoved), "{:?}", refusal);
        assert_eq!(t.commits, 0);
    }

    // -- Row 7 ---------------------------------------------------------------------------------

    #[test]
    fn row07_cell_op_on_a_row_the_target_does_not_have_refuses() {
        let mut log = MemCherryLog::new();
        let s = log.push(cell_op(OpKind::Assign(int(1)), Some(int(0))));
        let mut t = MemCherryTarget::new(); // no rows at all
        let r = pick(&log, &[s], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("expected REFUSE");
        assert!(refusal.has(CherryConflictKind::RowGone), "{:?}", refusal);
        assert_eq!(t.commits, 0);
    }

    // -- Row 8 ---------------------------------------------------------------------------------

    #[test]
    fn row08_column_past_the_end_of_the_target_row_refuses() {
        let mut log = MemCherryLog::new();
        let s = log.push(op(OpKind::Assign(int(1)), Some(ColId(9)), Some(int(0))));
        let mut t = target_with(int(0)); // 3 columns; ColId(9) is not one of them
        let r = pick(&log, &[s], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("expected REFUSE");
        assert!(refusal.has(CherryConflictKind::ColumnAbsent), "{:?}", refusal);
        assert_eq!(t.commits, 0);
    }

    // -- Row 9 ---------------------------------------------------------------------------------

    #[test]
    fn row09_rowcreate_onto_an_absent_row_materialises_it() {
        let mut log = MemCherryLog::new();
        let s = log.push(op(OpKind::RowCreate(vec![int(1), int(2), int(3)]), None, None));
        let mut t = MemCherryTarget::new();
        let r = pick(&log, &[s], &mut t, &PolicyTable::new());
        assert!(r.is_applied(), "expected APPLY, got {:?}", r);
        assert_eq!(t.get(T, R), Some(&vec![int(1), int(2), int(3)]));
    }

    // -- Row 10 --------------------------------------------------------------------------------

    #[test]
    fn row10_rowcreate_onto_a_row_that_already_exists_refuses() {
        let mut log = MemCherryLog::new();
        let s = log.push(op(OpKind::RowCreate(vec![int(1), int(2), int(3)]), None, None));
        let mut t = target_with(int(0));
        let before = t.snapshot();
        let r = pick(&log, &[s], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("expected REFUSE");
        assert!(refusal.has(CherryConflictKind::RowExists), "{:?}", refusal);
        assert_eq!(t.snapshot(), before);
        assert_eq!(t.commits, 0);
    }

    // -- Row 11 --------------------------------------------------------------------------------

    #[test]
    fn row11_rowdelete_of_a_present_row_removes_it() {
        let mut log = MemCherryLog::new();
        let s = log.push(op(OpKind::RowDelete, None, None));
        let mut t = target_with(int(5));
        let r = pick(&log, &[s], &mut t, &PolicyTable::new());
        assert!(r.is_applied(), "expected APPLY, got {:?}", r);
        assert_eq!(t.get(T, R), None);
    }

    // -- Row 12 --------------------------------------------------------------------------------

    #[test]
    fn row12_rowdelete_of_an_absent_row_refuses() {
        let mut log = MemCherryLog::new();
        let s = log.push(op(OpKind::RowDelete, None, None));
        let mut t = MemCherryTarget::new();
        let r = pick(&log, &[s], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("expected REFUSE");
        assert!(refusal.has(CherryConflictKind::RowGone), "{:?}", refusal);
        assert_eq!(t.commits, 0);
    }

    // -- Row 13 --------------------------------------------------------------------------------

    #[test]
    fn row13_two_selected_ops_on_one_cell_compose_into_one_write() {
        let mut log = MemCherryLog::new();
        let a = log.push(cell_op(OpKind::Add(Delta::Int(3)), Some(int(10))));
        let b = log.push(cell_op(OpKind::Add(Delta::Int(4)), Some(int(13))));
        let mut t = target_with(int(10));
        let r = pick(&log, &[a, b], &mut t, &PolicyTable::new());
        let applied = r.applied().expect("expected APPLY");
        assert_eq!(t.cell(T, R, C), Some(&int(17)), "3 and 4 compose to +7 on 10");
        assert_eq!(
            applied.plan.len(),
            1,
            "two ops on one cell must produce ONE write, not two: {:?}",
            applied.plan
        );
    }

    #[test]
    fn row13_note_a_straddled_selection_is_reported_not_refused() {
        let mut log = MemCherryLog::new();
        let a = log.push(cell_op(OpKind::Add(Delta::Int(3)), Some(int(10))));
        let _skipped = log.push(cell_op(OpKind::Add(Delta::Int(100)), Some(int(13))));
        let c = log.push(cell_op(OpKind::Add(Delta::Int(4)), Some(int(113))));
        let mut t = target_with(int(10));
        let r = pick(&log, &[a, c], &mut t, &PolicyTable::new());
        let applied = r.applied().expect("picking a non-contiguous subset IS the feature");
        assert_eq!(t.cell(T, R, C), Some(&int(17)), "the skipped +100 must not land");
        assert_eq!(applied.straddled.len(), 1, "the skip must be reported: {:?}", applied);
        assert_eq!(applied.straddled[0].skipped, vec![_skipped]);
    }

    // -- Row 14 --------------------------------------------------------------------------------

    #[test]
    fn row14_ops_with_no_composition_in_the_algebra_refuse_whole() {
        let mut log = MemCherryLog::new();
        // Assign a string, then Add to it: `compose_ops` folds (Assign, Add) by applying the
        // delta to the assigned value, and a numeric delta has no meaning on a Varchar.
        let a = log.push(cell_op(OpKind::Assign(Value::Varchar("x".into())), Some(int(0))));
        let b = log.push(cell_op(OpKind::Add(Delta::Int(1)), Some(Value::Varchar("x".into()))));
        let mut t = target_with(int(0));
        let before = t.snapshot();
        let r = pick(&log, &[a, b], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("expected REFUSE");
        assert!(refusal.has(CherryConflictKind::ComposeFailed), "{:?}", refusal);
        assert_eq!(t.snapshot(), before);
        assert_eq!(t.commits, 0);
    }

    // -- Row 15 --------------------------------------------------------------------------------

    #[test]
    fn row15_the_same_op_selected_twice_refuses_because_add_is_not_idempotent() {
        let mut log = MemCherryLog::new();
        let s = log.push(cell_op(OpKind::Add(Delta::Int(5)), Some(int(10))));
        let mut t = target_with(int(10));
        let r = pick(&log, &[s, s], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("expected REFUSE");
        assert!(refusal.has(CherryConflictKind::DuplicateSelector), "{:?}", refusal);
        assert_eq!(t.cell(T, R, C), Some(&int(10)), "not 15, and certainly not 20");
        assert_eq!(t.commits, 0);
    }

    // -- Row 16 --------------------------------------------------------------------------------

    #[test]
    fn row16_a_cell_op_on_a_row_this_pick_creates_folds_into_the_insert() {
        let mut log = MemCherryLog::new();
        let create = log.push(op(OpKind::RowCreate(vec![int(1), int(2), int(3)]), None, None));
        let edit = log.push(cell_op(OpKind::Assign(int(99)), Some(int(2))));
        let mut t = MemCherryTarget::new();
        let r = pick(&log, &[create, edit], &mut t, &PolicyTable::new());
        let applied = r.applied().expect("expected APPLY");
        assert_eq!(t.get(T, R), Some(&vec![int(1), int(99), int(3)]));
        assert_eq!(
            applied.plan.len(),
            1,
            "a created row lands as ONE InsertRow, not an insert plus updates: {:?}",
            applied.plan
        );
    }

    // -- Row 17 --------------------------------------------------------------------------------

    #[test]
    fn row17_a_cell_op_after_a_delete_in_the_same_pick_refuses() {
        let mut log = MemCherryLog::new();
        let del = log.push(op(OpKind::RowDelete, None, None));
        let edit = log.push(cell_op(OpKind::Assign(int(1)), Some(int(0))));
        let mut t = target_with(int(0));
        let before = t.snapshot();
        let r = pick(&log, &[del, edit], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("expected REFUSE");
        assert!(refusal.has(CherryConflictKind::RowGone), "{:?}", refusal);
        assert_eq!(t.snapshot(), before, "the delete must not have landed either");
        assert_eq!(t.commits, 0);
    }

    // -- Row 18 --------------------------------------------------------------------------------

    #[test]
    fn row18_a_selector_naming_no_recorded_op_refuses() {
        let mut log = MemCherryLog::new();
        log.push(cell_op(OpKind::Assign(int(1)), Some(int(0))));
        let mut t = target_with(int(0));
        let r = pick(&log, &[9999], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("expected REFUSE");
        assert!(refusal.has(CherryConflictKind::NoSuchOp), "{:?}", refusal);
        assert_eq!(t.commits, 0);
    }

    // -- Row 19 --------------------------------------------------------------------------------

    #[test]
    fn row19_a_selector_naming_another_branchs_op_refuses() {
        let mut log = MemCherryLog::new();
        let mut foreign = cell_op(OpKind::Assign(int(1)), Some(int(0)));
        foreign.branch = BranchId::new(99, 0);
        let s = log.push(foreign);
        let mut t = target_with(int(0));
        let r = pick(&log, &[s], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("expected REFUSE");
        assert!(refusal.has(CherryConflictKind::NotFromSource), "{:?}", refusal);
        assert_eq!(t.commits, 0);
    }

    // -- Row 20 --------------------------------------------------------------------------------

    #[test]
    fn row20_an_empty_selection_is_refused_not_treated_as_satisfied() {
        let log = MemCherryLog::new();
        let mut t = target_with(int(0));
        let r = pick(&log, &[], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("an empty pick must refuse");
        assert!(refusal.has(CherryConflictKind::EmptySelection), "{:?}", refusal);
        assert_eq!(t.commits, 0);
    }

    /// Row 2 is proved for `Add`/`Add`; `Max`/`Max` and `Min`/`Min` are different pairs in
    /// `commutes_with` and had no test. Named here because the table says row 2 covers "ops that
    /// commute", not "two Adds".
    ///
    /// Each case names a witness the target's op could actually have reached the target's value
    /// from. That is not decoration: the first draft of this test used a witness of 0 for the
    /// `Min` case, where `Min(4)` applied to 0 yields 0 and not 4, and [`divergence`]'s
    /// verification correctly refused to trust a composition that did not explain the value — the
    /// fixture was inconsistent and the check caught it.
    #[test]
    fn row02_max_and_min_divergences_commute_as_well_as_add() {
        for (witness, ours, theirs, target, expect) in [
            (int(0), OpKind::Max(int(7)), OpKind::Max(int(5)), int(5), int(7)),
            (int(0), OpKind::Max(int(3)), OpKind::Max(int(9)), int(9), int(9)),
            (int(10), OpKind::Min(int(2)), OpKind::Min(int(4)), int(4), int(2)),
            (int(10), OpKind::Min(int(8)), OpKind::Min(int(3)), int(3), int(3)),
        ] {
            let mut log = MemCherryLog::new();
            let a = log.push(cell_op(ours.clone(), Some(witness.clone())));
            let mut t_op = cell_op(theirs.clone(), Some(witness.clone()));
            t_op.branch = dst();
            log.push(t_op);
            let mut t = target_with(target.clone());
            let r = pick(&log, &[a], &mut t, &PolicyTable::new());
            assert!(r.is_applied(), "{:?} vs {:?} must commute, got {:?}", ours, theirs, r);
            assert_eq!(t.cell(T, R, C), Some(&expect), "{:?} vs {:?}", ours, theirs);
        }
    }

    /// Row 8's wording is about "the target row", but the same check exists against the image a
    /// `RowCreate` in this selection produces, and that arm had no test.
    #[test]
    fn row08_a_column_past_the_end_of_a_created_row_image_also_refuses() {
        let mut log = MemCherryLog::new();
        let create = log.push(op(OpKind::RowCreate(vec![int(1), int(2)]), None, None));
        let edit = log.push(op(OpKind::Assign(int(9)), Some(ColId(5)), None));
        let mut t = MemCherryTarget::new();
        let r = pick(&log, &[create, edit], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("expected REFUSE");
        assert!(refusal.has(CherryConflictKind::ColumnAbsent), "{:?}", refusal);
        assert_eq!(t.get(T, R), None, "the create must not have landed either");
        assert_eq!(t.commits, 0);
    }

    // -- Row 21: atomicity ---------------------------------------------------------------------

    /// **The atomicity proof.** A selection whose *first* op applies cleanly and whose *second*
    /// conflicts. The clean op is the one that would land in a half-applied implementation, so
    /// this test fails loudly if the refusal ever moves after the writer.
    #[test]
    fn row21_a_refusal_leaves_no_partial_write_even_when_earlier_ops_were_clean() {
        let mut log = MemCherryLog::new();
        // Op A: a different row entirely, and it applies cleanly.
        let mut clean = cell_op(OpKind::Assign(int(42)), Some(int(5)));
        clean.row = RowId(1);
        let a = log.push(clean);
        // Op B: on row 7, whose cell the target has moved under us contradictorily.
        let b = log.push(cell_op(OpKind::Assign(int(1)), Some(int(0))));
        let mut theirs = cell_op(OpKind::Assign(int(2)), Some(int(0)));
        theirs.branch = dst();
        log.push(theirs);

        let mut t = MemCherryTarget::new();
        t.insert(T, RowId(1), vec![int(0), int(5), int(0)]);
        t.insert(T, R, vec![int(0), int(2), int(0)]);
        let before = t.snapshot();

        let r = pick(&log, &[a, b], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect("expected REFUSE");
        assert!(refusal.has(CherryConflictKind::TargetCellMoved), "{:?}", refusal);

        // The three independent ways of asking "did anything land?".
        assert_eq!(t.snapshot(), before, "state must be byte-identical to the pre-image");
        assert_eq!(t.cell(T, RowId(1), C), Some(&int(5)), "the CLEAN op must not have landed");
        assert_eq!(t.commits, 0, "commit_all must never have been entered");
        assert_eq!(t.applied_writes, 0);
    }

    /// The clean counterpart of the test above, against the identical fixture: with the target
    /// left where op B expects it, the *same* two-op selection lands whole. Without this, the
    /// refusal above proves only that the engine refuses, not that it can ever accept.
    #[test]
    fn row21_control_the_same_selection_applies_whole_when_nothing_moved() {
        let mut log = MemCherryLog::new();
        let mut clean = cell_op(OpKind::Assign(int(42)), Some(int(5)));
        clean.row = RowId(1);
        let a = log.push(clean);
        let b = log.push(cell_op(OpKind::Assign(int(1)), Some(int(0))));

        let mut t = MemCherryTarget::new();
        t.insert(T, RowId(1), vec![int(0), int(5), int(0)]);
        t.insert(T, R, vec![int(0), int(0), int(0)]); // still at op B's witness

        let r = pick(&log, &[a, b], &mut t, &PolicyTable::new());
        assert!(r.is_applied(), "expected APPLY, got {:?}", r);
        assert_eq!(t.cell(T, RowId(1), C), Some(&int(42)));
        assert_eq!(t.cell(T, R, C), Some(&int(1)));
        assert_eq!(t.commits, 1, "one commit, not two");
    }

    /// Atomicity of the **door**, not only of the decision: if `commit_all` itself fails partway,
    /// the target is unchanged. This is what makes `commit_all`'s all-or-nothing contract a
    /// tested claim rather than a docstring.
    #[test]
    fn row21_a_commit_that_fails_partway_leaves_the_target_unchanged() {
        let mut log = MemCherryLog::new();
        let mut first = cell_op(OpKind::Assign(int(42)), Some(int(5)));
        first.row = RowId(1);
        let a = log.push(first);
        let b = log.push(cell_op(OpKind::Assign(int(1)), Some(int(0))));

        let mut t = MemCherryTarget::new();
        t.insert(T, RowId(1), vec![int(0), int(5), int(0)]);
        t.insert(T, R, vec![int(0), int(0), int(0)]);
        let before = t.snapshot();
        t.fail_at = Some(1); // the plan has two writes; blow up on the second

        let sel = vec![OpSelector::new(a), OpSelector::new(b)];
        let err = cherry_pick(&log, src(), &sel, dst(), &mut t, &PolicyTable::new())
            .expect_err("the injected failure must surface");
        assert!(format!("{}", err).contains("injected failure"), "{}", err);
        assert_eq!(t.snapshot(), before, "a failed commit must leave nothing behind");
        assert_eq!(t.applied_writes, 0);
    }

    /// The journal restores in reverse, so a row touched **twice** by one plan still ends at the
    /// image it held before the commit — not at the intermediate the first write left.
    ///
    /// This is the case the whole-map clone got right for free and a journal can get wrong, which
    /// is exactly why swapping it for a journal needs its own test rather than inheriting the
    /// old one's green.
    #[test]
    fn a_failed_commit_that_touched_one_row_twice_restores_the_original_image() {
        let mut t = MemCherryTarget::new();
        t.insert(T, R, vec![int(0), int(1), int(2)]);
        let before = t.snapshot();
        t.fail_at = Some(2);
        let plan = vec![
            CherryWrite::Cell {
                table: "t".into(),
                tbl: T,
                row: R,
                col: ColId(1),
                value: int(11),
                before: Some(int(1)),
            },
            CherryWrite::Cell {
                table: "t".into(),
                tbl: T,
                row: R,
                col: ColId(2),
                value: int(22),
                before: Some(int(2)),
            },
            // The third write is where the injected failure lands, after the row has been
            // touched twice.
            CherryWrite::Cell {
                table: "t".into(),
                tbl: T,
                row: R,
                col: ColId(0),
                value: int(33),
                before: Some(int(0)),
            },
        ];
        t.commit_all(&plan).expect_err("the injected failure must surface");
        assert_eq!(
            t.snapshot(),
            before,
            "a row touched twice must end at its pre-commit image, not at the first write's"
        );
    }

    // -- Rows 22-27, and the two divergence defects a fresh-context review found ---------------
    //
    // Every test in this block was written to FAIL against the version of this module that was
    // committed before it, and every one of them did. They are the specification of the fixes.

    /// **D1.** `divergence` must not count the SOURCE branch's own unselected ops as "what the
    /// target absorbed". Source assigns twice, only the first is picked; the target concurrently
    /// assigned something else. The unselected source op equals ours, so the same-value shortcut
    /// in `resolve_cell` fires and the target's write is silently overwritten.
    #[test]
    fn d1_an_unselected_source_op_is_not_the_targets_divergence() {
        let mut log = MemCherryLog::new();
        let mut theirs = cell_op(OpKind::Assign(int(5)), Some(int(0)));
        theirs.branch = dst();
        log.push(theirs); // seq 1: the TARGET's concurrent write
        let picked = log.push(cell_op(OpKind::Assign(int(7)), Some(int(0)))); // seq 2: ours
        log.push(cell_op(OpKind::Assign(int(7)), Some(int(7)))); // seq 3: source's own, NOT picked

        let mut t = target_with(int(5)); // the target carries ITS write
        let r = pick(&log, &[picked], &mut t, &PolicyTable::new());
        let refusal = r.refusal().expect(
            "the target moved contradictorily under us; an unselected SOURCE op is not the \
             target's divergence and must not excuse the conflict",
        );
        assert!(refusal.has(CherryConflictKind::TargetCellMoved), "{:?}", refusal);
        assert_eq!(t.cell(T, R, C), Some(&int(5)), "the target's write must not be lost");
    }

    /// **D2.** Truth-table row 2 must not depend on which branch happened to write first. This is
    /// `row02` with the two pushes swapped: the target's op has the LOWER seq.
    #[test]
    fn d2_commuting_divergence_is_found_when_the_targets_op_has_the_lower_seq() {
        let mut log = MemCherryLog::new();
        let mut theirs = cell_op(OpKind::Add(Delta::Int(-5)), Some(int(20)));
        theirs.branch = dst();
        log.push(theirs); // seq 1 — the target wrote FIRST
        let ours = log.push(cell_op(OpKind::Add(Delta::Int(-3)), Some(int(20)))); // seq 2

        let mut t = target_with(int(15));
        let r = pick(&log, &[ours], &mut t, &PolicyTable::new());
        assert!(r.is_applied(), "expected APPLY (row 2), got {:?}", r);
        assert_eq!(t.cell(T, R, C), Some(&int(12)), "20 - 5 - 3");
    }

    /// **D5.** Only the LAST whole-row op was examined, so a `RowCreate` earlier in the selection
    /// was never checked against the target and row 10's `RowExists` refusal was bypassed.
    #[test]
    fn row25_a_create_then_delete_selection_still_checks_the_create() {
        let mut log = MemCherryLog::new();
        let create = log.push(op(OpKind::RowCreate(vec![int(1), int(2), int(3)]), None, None));
        let del = log.push(op(OpKind::RowDelete, None, None));
        let mut t = target_with(int(5)); // the target ALREADY HAS the row
        let before = t.snapshot();
        let r = pick(&log, &[create, del], &mut t, &PolicyTable::new());
        let refusal = r
            .refusal()
            .expect("the RowCreate contradicts a target that already has the row");
        assert!(refusal.has(CherryConflictKind::RowExists), "{:?}", refusal);
        assert_eq!(t.snapshot(), before, "the row must not have been deleted");
    }

    /// **D5, mirror.** Delete-then-create onto a target that has the row is legitimate: the row
    /// ends existing with the created image.
    #[test]
    fn row24_a_delete_then_create_selection_replaces_the_row() {
        let mut log = MemCherryLog::new();
        let del = log.push(op(OpKind::RowDelete, None, None));
        let create = log.push(op(OpKind::RowCreate(vec![int(1), int(2), int(3)]), None, None));
        let mut t = target_with(int(5));
        let r = pick(&log, &[del, create], &mut t, &PolicyTable::new());
        assert!(r.is_applied(), "expected APPLY, got {:?}", r);
        assert_eq!(t.get(T, R), Some(&vec![int(1), int(2), int(3)]));
    }

    /// **D4.** `apply_op` has no arm for the set ops, so they reached the caller as an engine
    /// `Err`. The contract says `Err` is for impossible internal state and everything a caller
    /// can express comes back as a refusal.
    #[test]
    fn row22_a_set_op_on_a_cell_refuses_rather_than_erroring() {
        use crate::tel::ids::Dot;
        let mut log = MemCherryLog::new();
        let s = log.push(cell_op(
            OpKind::SetInsert { elem: int(1), dot: Dot { branch: src(), seq: 1 } },
            Some(int(0)),
        ));
        let mut t = target_with(int(0));
        let r = cherry_pick(&log, src(), &[OpSelector::new(s)], dst(), &mut t, &PolicyTable::new())
            .expect("a set op is something a caller can express; it must not be an engine Err");
        assert!(r.refusal().is_some(), "expected REFUSE, got {:?}", r);
        assert_eq!(t.commits, 0);
    }

    /// **D4, second half.** An op recorded with no column but a cell-shaped kind is malformed
    /// input, not an impossible internal state.
    #[test]
    fn row23_a_whole_row_op_with_a_cell_kind_refuses_rather_than_erroring() {
        let mut log = MemCherryLog::new();
        let s = log.push(op(OpKind::Assign(int(1)), None, None));
        let mut t = target_with(int(0));
        let r = cherry_pick(&log, src(), &[OpSelector::new(s)], dst(), &mut t, &PolicyTable::new())
            .expect("malformed input must refuse, not error");
        assert!(r.refusal().is_some(), "expected REFUSE, got {:?}", r);
        assert_eq!(t.commits, 0);
    }

    /// **The divergence verification.** When the log's non-source ops do NOT explain the value
    /// the target actually holds, the engine must not trust them: it falls back to an opaque
    /// `Assign`, which conflicts. Without this the composed `theirs` is a guess dressed as a
    /// reading, and rows 3/4/5's fixtures cannot tell the two apart.
    #[test]
    fn row27_divergence_that_does_not_explain_the_targets_value_is_not_trusted() {
        let mut log = MemCherryLog::new();
        let ours = log.push(cell_op(OpKind::Add(Delta::Int(1)), Some(int(0))));
        let mut theirs = cell_op(OpKind::Add(Delta::Int(10)), Some(int(0)));
        theirs.branch = dst();
        log.push(theirs);
        // The log says the target should hold 10. It holds 99 — something the log cannot account
        // for touched this cell.
        let mut t = target_with(int(99));
        let r = pick(&log, &[ours], &mut t, &PolicyTable::new());
        let refusal = r
            .refusal()
            .expect("an unexplained target value must not be composed with as if understood");
        assert!(refusal.has(CherryConflictKind::TargetCellMoved), "{:?}", refusal);
        assert_eq!(t.cell(T, R, C), Some(&int(99)));
    }

    /// Cell ops **before** a `RowDelete` in the same selection are subsumed by it, and the row
    /// still leaves. The companion of row 17, which covers the other order.
    #[test]
    fn row26_a_cell_op_before_a_delete_in_the_same_pick_is_subsumed() {
        let mut log = MemCherryLog::new();
        let edit = log.push(cell_op(OpKind::Assign(int(1)), Some(int(0))));
        let del = log.push(op(OpKind::RowDelete, None, None));
        let mut t = target_with(int(0));
        let r = pick(&log, &[edit, del], &mut t, &PolicyTable::new());
        assert!(r.is_applied(), "expected APPLY, got {:?}", r);
        assert_eq!(t.get(T, R), None, "the row leaves; the cell write is subsumed");
    }

    // -- Reuse of the REVERT machinery ---------------------------------------------------------

    /// A landed pick is undone through [`invert`] — the same primitive `undo_txn` calls — and the
    /// undo goes through the same single door, so it is atomic on the same terms.
    #[test]
    fn a_landed_pick_is_undone_by_the_inverse_plan_through_the_same_door() {
        let mut log = MemCherryLog::new();
        let s = log.push(cell_op(OpKind::Assign(int(42)), Some(int(5))));
        let mut t = target_with(int(5));
        let before = t.snapshot();

        let r = pick(&log, &[s], &mut t, &PolicyTable::new());
        let applied = r.applied().expect("expected APPLY").clone();
        assert_eq!(t.cell(T, R, C), Some(&int(42)));

        let undo = applied.plan.inverse().expect("the pick must be invertible");
        t.commit_all(&undo.writes).expect("undo commits");
        assert_eq!(t.snapshot(), before, "the inverse must restore the pre-image exactly");
    }

    #[test]
    fn the_inverse_of_a_create_is_a_delete_and_of_a_delete_a_create() {
        let mut log = MemCherryLog::new();
        let s = log.push(op(OpKind::RowCreate(vec![int(1), int(2), int(3)]), None, None));
        let mut t = MemCherryTarget::new();
        let before = t.snapshot();
        let applied = pick(&log, &[s], &mut t, &PolicyTable::new())
            .applied()
            .expect("expected APPLY")
            .clone();
        assert!(t.get(T, R).is_some());
        t.commit_all(&applied.plan.inverse().unwrap().writes).unwrap();
        assert_eq!(t.snapshot(), before);

        let mut log2 = MemCherryLog::new();
        let d = log2.push(op(OpKind::RowDelete, None, None));
        let mut t2 = target_with(int(5));
        let before2 = t2.snapshot();
        let applied2 = pick(&log2, &[d], &mut t2, &PolicyTable::new())
            .applied()
            .expect("expected APPLY")
            .clone();
        assert!(t2.get(T, R).is_none());
        t2.commit_all(&applied2.plan.inverse().unwrap().writes).unwrap();
        assert_eq!(t2.snapshot(), before2, "the row must come back with its image");
    }

    /// A `RowCreate` that REPLACED an existing row inverts back to that row's prior image, not to
    /// a delete. Without `CherryWrite::InsertRow::replaced` the undo of row 24 would destroy a row
    /// the target owned before the pick.
    #[test]
    fn the_inverse_of_a_replacing_create_restores_the_prior_image() {
        let mut log = MemCherryLog::new();
        let del = log.push(op(OpKind::RowDelete, None, None));
        let create = log.push(op(OpKind::RowCreate(vec![int(1), int(2), int(3)]), None, None));
        let mut t = target_with(int(5));
        let before = t.snapshot();
        let applied = pick(&log, &[del, create], &mut t, &PolicyTable::new())
            .applied()
            .expect("expected APPLY")
            .clone();
        assert_eq!(t.get(T, R), Some(&vec![int(1), int(2), int(3)]));
        t.commit_all(&applied.plan.inverse().unwrap().writes).unwrap();
        assert_eq!(t.snapshot(), before, "the row the target owned must come back, not vanish");
    }

    /// `invert` refuses an `Assign` with no witness rather than guessing, and that refusal must
    /// travel out through `inverse` rather than being swallowed.
    #[test]
    fn an_uninvertible_write_refuses_rather_than_guessing() {
        let plan = CherryPlan {
            writes: vec![CherryWrite::Cell {
                table: "t".into(),
                tbl: T,
                row: R,
                col: C,
                value: int(1),
                before: None,
            }],
        };
        let e = plan.inverse().expect_err("no before-image means no inverse");
        assert!(format!("{}", e).contains("before-image"), "{}", e);
    }

    // -- The claims the module header makes about its own reuse --------------------------------

    /// The straddle *reporter* must be able to fire and to stay silent. A detector that has only
    /// ever been observed silent is not a clean result.
    #[test]
    fn the_straddle_reporter_is_silent_on_a_contiguous_selection() {
        let mut log = MemCherryLog::new();
        let a = log.push(cell_op(OpKind::Add(Delta::Int(3)), Some(int(10))));
        let b = log.push(cell_op(OpKind::Add(Delta::Int(4)), Some(int(13))));
        let mut t = target_with(int(10));
        let applied = pick(&log, &[a, b], &mut t, &PolicyTable::new())
            .applied()
            .expect("expected APPLY")
            .clone();
        assert!(applied.straddled.is_empty(), "nothing was skipped: {:?}", applied.straddled);
    }

    /// The divergence read must exclude the selected ops themselves. If it did not, picking an op
    /// that is in the log would see itself as a concurrent write and conflict with itself — which
    /// is the failure mode that makes this worth a test rather than a comment.
    #[test]
    fn a_selected_op_is_not_counted_as_its_own_divergence() {
        let mut log = MemCherryLog::new();
        let a = log.push(cell_op(OpKind::Assign(int(1)), Some(int(0))));
        let b = log.push(cell_op(OpKind::Assign(int(2)), Some(int(1))));
        let mut t = target_with(int(0));
        // Pick BOTH. `b` sits after `a` on the same cell; if divergence counted `b` against `a`
        // the pick would report a contradictory assign against itself.
        let r = pick(&log, &[a, b], &mut t, &PolicyTable::new());
        assert!(r.is_applied(), "a pick must not conflict with itself: {:?}", r);
        assert_eq!(t.cell(T, R, C), Some(&int(2)), "the later Assign supersedes");
    }

    /// Every op of a multi-row pick lands, so `picked` and the plan agree about what happened.
    #[test]
    fn a_multi_row_pick_lands_every_row_in_one_commit() {
        let mut log = MemCherryLog::new();
        let mut seqs = Vec::new();
        for r in 0..5u64 {
            let mut o = cell_op(OpKind::Assign(int(r as i32 + 100)), Some(int(0)));
            o.row = RowId(r);
            seqs.push(log.push(o));
        }
        let mut t = MemCherryTarget::new();
        for r in 0..5u64 {
            t.insert(T, RowId(r), vec![int(0), int(0), int(0)]);
        }
        let applied = pick(&log, &seqs, &mut t, &PolicyTable::new())
            .applied()
            .expect("expected APPLY")
            .clone();
        assert_eq!(applied.picked, 5);
        assert_eq!(applied.plan.len(), 5);
        assert_eq!(t.commits, 1, "five rows, ONE commit");
        for r in 0..5u64 {
            assert_eq!(t.cell(T, RowId(r), C), Some(&int(r as i32 + 100)));
        }
    }
}
