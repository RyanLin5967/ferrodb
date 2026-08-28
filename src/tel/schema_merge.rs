//! Merging column-level schema changes between branches — B11.
//!
//! Design authority: DESIGN.md section 3 ("Merge"), applied to a subject it did not cover.
//!
//! # Why this is not part of the cell algebra
//!
//! `tel::engine` merges *cells*, keyed by `(TableId, RowId, ColId)`. It cannot merge a schema, and
//! bolting a schema variant onto [`crate::tel::op::OpKind`] would be actively unsafe: `ColId` is a
//! raw schema ordinal, so an op describing "the shape changed" would be keyed by a number whose
//! meaning the very change it describes has just moved. The two live side by side instead, and the
//! ordering rule is that schema composes **before** cells: the shape a merge publishes rows into is
//! the merged shape, not either side's.
//!
//! # What "theirs" is, and why it is a shape and not a list of edits
//!
//! There is no LCA computation anywhere in this system (`ThreeWayMerger` explicitly ignores its
//! `lca` argument; the fork point is materialised as `Workspace.base_rows` and `Op.witness`). The
//! schema merge follows the same pattern: the branch records the table's shape **as it stood at the
//! branch's first schema edit** — the fork point — and merge compares that against the target's
//! shape *now*. Whatever the difference is, it is what the other side did, already applied, and it
//! is a fact rather than a reconstruction.
//!
//! So `merge_schema` takes three things: the fork-point shape, the target's current shape, and our
//! pending edits. It never needs their edits as a list, and therefore never has to trust one.
//!
//! # The precondition, and why the conflict hands back a predicate
//!
//! Every edit carries the thing that must be true of the table's shape for it to mean what it said.
//! `ADD COLUMN note` means "there is no `note` yet"; `ALTER COLUMN qty TYPE BIGINT` written against
//! an `INTEGER` column means "`qty` is still `INTEGER`". These are **preconditions**, in exactly the
//! sense `tel::guard` uses the word: re-evaluated against the merged state before the edit is
//! applied, and reported with the predicate itself when they fail (DESIGN exit criterion 7 — "a
//! boolean does not satisfy it, the agent has to be handed the predicate back").
//!
//! DESIGN's warning about bounded counters applies here in its exact form. **The guard must name
//! what was observed, not the invariant.** A retype whose precondition were "the column has some
//! type" would be satisfied by every possible merged state and would enforce nothing at all; one
//! that names `INTEGER` fails the moment another branch made it `BIGINT`, which is the case this
//! has to catch.
//!
//! # Four outcomes, minus one, on purpose
//!
//! `Clean`, `Commuting` and `Conflict` occur. `ResolvedWithLoss` does not, and its absence is a
//! decision rather than an omission: it is the outcome for a *policy* that succeeds by discarding
//! a write, and there is no last-writer-wins for a table's shape. Two branches wanting a column to
//! be two different types is not resolvable by picking one — the loser's rows are already written,
//! and the agent that lost would be told its schema change landed when the column is a type it
//! never asked for. That is precisely the failure DESIGN calls "the most dangerous thing this
//! system can do to an agent", so the answer is `Conflict` and a retry with the predicate.
//!
//! # An edit whose effect is already present is absorbed, not refused
//!
//! Two agents that both add `note VARCHAR(20)`, or both retype `qty` to `BIGINT`, do not conflict:
//! the second one's intent is already satisfied by the merged shape, so it contributes nothing and
//! composes. This is the same rule `merge_engine::resolve_cell` applies to two *equal* `Assign`s,
//! and it is the reason a retype conflict has to be tested with two **different** target types —
//! two identical ones are not contradictory and refusing them would force a pointless retry.
//! Idempotence is a property of `Assign`, and a shape edit is an assign.

use std::fmt::{Display, Formatter};

use crate::catalog::column::{Column, DataType, Value};
use crate::catalog::schema::Schema;
use crate::error::FerroError;
use crate::tel::guard::{CmpOp, Guard, GuardExpr};
use crate::tel::ids::{RowId, TableId};
use crate::tel::merge::{ConflictKind, ConflictReport};

/// How a column that is not there is spelled in a predicate.
///
/// A total function on the shape: every column name has *some* answer, present or not, so a
/// precondition is never "could not be evaluated" — the outcome `tel::guard` reserves for a hard
/// reject rather than a retry. An `ADD COLUMN` whose precondition were unevaluable when the column
/// is absent would report the successful case as a hard failure.
pub const ABSENT: &str = "<absent>";

/// One column-level change, as a branch records it before it is published.
///
/// Distinct from `parser::AlterAction` because a retype must carry the type it was written
/// *against*. The parser has no reason to know it and the merge cannot work without it.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaEdit {
    /// A column appended at the end of the table.
    AddColumn(Column),
    /// A column renamed.
    RenameColumn { from: String, to: String },
    /// A column's type changed, from the type the branch observed to the one it asked for.
    RetypeColumn { column: String, from: DataType, to: DataType },
}

impl SchemaEdit {
    /// The column the edit is about, named as it is **after** the edit.
    pub fn column(&self) -> &str {
        match self {
            SchemaEdit::AddColumn(c) => &c.name,
            SchemaEdit::RenameColumn { to, .. } => to,
            SchemaEdit::RetypeColumn { column, .. } => column,
        }
    }

    /// What must be true of the table's shape for this edit to mean what it said.
    ///
    /// A rename has two, and they are not the same claim: the old name must still be there (or
    /// there is nothing to rename) and the new one must not (or the rename would collide).
    pub fn preconditions(&self, table: &str) -> Vec<SchemaPredicate> {
        match self {
            SchemaEdit::AddColumn(c) => {
                vec![SchemaPredicate::absent(table, &c.name)]
            }
            SchemaEdit::RenameColumn { from, to } => vec![
                SchemaPredicate::present(table, from),
                SchemaPredicate::absent(table, to),
            ],
            SchemaEdit::RetypeColumn { column, from, .. } => {
                vec![SchemaPredicate::of_type(table, column, from)]
            }
        }
    }

    /// Whether the merged shape already says exactly what this edit asks for.
    ///
    /// See the module header: an already-satisfied edit is absorbed rather than refused, the same
    /// way two equal `Assign`s to one cell are not a conflict.
    pub fn already_satisfied(&self, shape: &Schema) -> bool {
        match self {
            SchemaEdit::AddColumn(c) => shape.columns.iter().any(|e| {
                e.name == c.name && e.data_type == c.data_type && e.nullable == c.nullable
            }),
            SchemaEdit::RenameColumn { from, to } => {
                !shape.columns.iter().any(|c| &c.name == from)
                    && shape.columns.iter().any(|c| &c.name == to)
            }
            SchemaEdit::RetypeColumn { column, to, .. } => {
                shape.columns.iter().any(|c| &c.name == column && &c.data_type == to)
            }
        }
    }

    /// The statement that executes this edit.
    ///
    /// A branch records a `SchemaEdit` because it needs the *observed* type a retype was written
    /// against, which `AlterAction` has no reason to carry. Executing it goes back through the
    /// parser's own vocabulary so that a column an agent added reaches the catalog, the WAL and
    /// the change feed by exactly the path a column a human added takes. A second execution path
    /// for schema would be a second chance to disagree with the consumer about what an event
    /// means — which is the failure E69 records happening once already.
    pub fn as_action(&self) -> crate::parser::parser::AlterAction {
        use crate::parser::parser::AlterAction;
        match self {
            SchemaEdit::AddColumn(c) => AlterAction::AddColumn(c.clone()),
            SchemaEdit::RenameColumn { from, to } => {
                AlterAction::RenameColumn { from: from.clone(), to: to.clone() }
            }
            SchemaEdit::RetypeColumn { column, to, .. } => {
                AlterAction::RetypeColumn { column: column.clone(), to: to.clone() }
            }
        }
    }

    /// Apply to a shape. Callers check [`Self::preconditions`] first; this is the effect alone.
    pub fn apply(&self, shape: &mut Schema) -> Result<(), FerroError> {
        match self {
            SchemaEdit::AddColumn(c) => shape.columns.push(c.clone()),
            SchemaEdit::RenameColumn { from, to } => {
                let col = shape
                    .columns
                    .iter_mut()
                    .find(|c| &c.name == from)
                    .ok_or_else(|| FerroError::Merge(format!("no column '{from}' to rename")))?;
                col.name = to.clone();
            }
            SchemaEdit::RetypeColumn { column, to, .. } => {
                let col = shape
                    .columns
                    .iter_mut()
                    .find(|c| &c.name == column)
                    .ok_or_else(|| FerroError::Merge(format!("no column '{column}' to retype")))?;
                col.data_type = to.clone();
            }
        }
        Ok(())
    }
}

impl Display for SchemaEdit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaEdit::AddColumn(c) => write!(
                f,
                "ADD COLUMN {} {}{}",
                c.name,
                c.data_type,
                if c.nullable { "" } else { " NOT NULL" }
            ),
            SchemaEdit::RenameColumn { from, to } => write!(f, "RENAME COLUMN {from} TO {to}"),
            SchemaEdit::RetypeColumn { column, from, to } => {
                write!(f, "ALTER COLUMN {column} TYPE {to} (was {from})")
            }
        }
    }
}

/// What a column must look like for an edit to be legal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnShapeReq {
    /// The column must not exist. Rendered as [`ABSENT`].
    Absent,
    /// The column must exist, with any type.
    Present,
    /// The column must exist with this exact type, spelled as `DataType`'s `Display` spells it.
    OfType(String),
}

impl Display for ColumnShapeReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnShapeReq::Absent => f.write_str(ABSENT),
            ColumnShapeReq::Present => f.write_str("<any type>"),
            ColumnShapeReq::OfType(t) => f.write_str(t),
        }
    }
}

/// One precondition on a table's shape, and the text an agent is handed when it fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaPredicate {
    pub table: String,
    pub column: String,
    pub required: ColumnShapeReq,
}

impl SchemaPredicate {
    pub fn absent(table: &str, column: &str) -> Self {
        SchemaPredicate {
            table: table.to_string(),
            column: column.to_string(),
            required: ColumnShapeReq::Absent,
        }
    }

    pub fn present(table: &str, column: &str) -> Self {
        SchemaPredicate {
            table: table.to_string(),
            column: column.to_string(),
            required: ColumnShapeReq::Present,
        }
    }

    pub fn of_type(table: &str, column: &str, ty: &DataType) -> Self {
        SchemaPredicate {
            table: table.to_string(),
            column: column.to_string(),
            required: ColumnShapeReq::OfType(ty.to_string()),
        }
    }

    /// What the shape actually says about this column. Total: an absent column has an answer.
    pub fn observed(&self, shape: &Schema) -> String {
        match shape.columns.iter().find(|c| c.name == self.column) {
            Some(c) => c.data_type.to_string(),
            None => ABSENT.to_string(),
        }
    }

    pub fn holds(&self, shape: &Schema) -> bool {
        let found = shape.columns.iter().find(|c| c.name == self.column);
        match (&self.required, found) {
            (ColumnShapeReq::Absent, None) => true,
            (ColumnShapeReq::Present, Some(_)) => true,
            (ColumnShapeReq::OfType(t), Some(c)) => &c.data_type.to_string() == t,
            _ => false,
        }
    }

    /// The predicate as an agent reads it back: `typeof(inventory.qty) = INTEGER`.
    pub fn text(&self) -> String {
        format!("typeof({}.{}) = {}", self.table, self.column, self.required)
    }

    /// The precondition as a re-checkable [`Guard`], carrying what was found against what was
    /// required.
    ///
    /// It is a real guard and not a label: `check` on any context evaluates two literals and
    /// reproduces the failure, so a caller handed this report can re-derive the verdict rather
    /// than having to believe it. The rendered `source_text` is what
    /// [`Guard::violated_predicate`] returns and therefore what reaches the agent.
    pub fn as_guard(&self, shape: &Schema) -> Guard {
        Guard::holds(GuardExpr::cmp(
            GuardExpr::Literal(Value::Varchar(self.observed(shape))),
            CmpOp::Eq,
            GuardExpr::Literal(Value::Varchar(self.required.to_string())),
        ))
        .with_source(self.text())
    }
}

/// The result of merging one branch's schema edits into a target.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaMergeOutcome {
    /// Nothing to compose: either this branch changed no shape, or nothing else did.
    ///
    /// Named the same as `MergeOutcome::Clean` and meaning the same thing — a one-sided change is
    /// `Clean`, and `Commuting` is reserved for both sides having changed the same subject. That
    /// is `tel::engine`'s rule for cells (`engine.rs`: "Commuting requires BOTH sides to have
    /// written") and this follows it rather than inventing a second convention.
    Clean,
    /// Both sides changed the table's shape and the changes compose.
    Commuting { composed: Vec<SchemaEdit> },
    /// An edit's precondition no longer holds against the merged shape.
    Conflict(Vec<ConflictReport>),
}

impl SchemaMergeOutcome {
    pub fn name(&self) -> &'static str {
        match self {
            SchemaMergeOutcome::Clean => "Clean",
            SchemaMergeOutcome::Commuting { .. } => "Commuting",
            SchemaMergeOutcome::Conflict(_) => "Conflict",
        }
    }

    pub fn is_conflict(&self) -> bool {
        matches!(self, SchemaMergeOutcome::Conflict(_))
    }

    pub fn conflicts(&self) -> &[ConflictReport] {
        match self {
            SchemaMergeOutcome::Conflict(v) => v,
            _ => &[],
        }
    }
}

/// A merged schema, and how it was reached.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaMerge {
    pub outcome: SchemaMergeOutcome,
    /// The shape after the merge. Equal to `target_now` when the outcome is a conflict: nothing is
    /// half-applied.
    pub shape: Schema,
    /// The edits that must still be executed against the target to reach `shape`. Excludes any the
    /// target already satisfied.
    pub to_apply: Vec<SchemaEdit>,
}

/// Three-way merge of one branch's schema edits.
///
/// `base` is the table's shape at the branch's fork point, `target_now` is its shape in the target
/// right now, and `ours` are the branch's pending edits in the order the agent wrote them.
///
/// The order of operations is DESIGN's, and step 2 cannot move before step 1: a precondition
/// checked against the pre-merge shape is checked against a shape that will not exist after the
/// merge.
///
/// 1. absorb any edit the merged shape already satisfies;
/// 2. re-evaluate every remaining precondition against the running merged shape;
/// 3. apply, or report the violated predicate.
pub fn merge_schema(
    table: &str,
    tbl: TableId,
    base: &Schema,
    target_now: &Schema,
    ours: &[SchemaEdit],
) -> Result<SchemaMerge, FerroError> {
    let they_moved = base != target_now;
    let mut shape = target_now.clone();
    let mut to_apply = Vec::new();
    let mut conflicts = Vec::new();

    for edit in ours {
        if edit.already_satisfied(&shape) {
            // Absorbed. It composes by contributing nothing, which is not the same as being
            // dropped: the merged shape says what the edit asked for.
            continue;
        }
        let mut failed = None;
        for pred in edit.preconditions(table) {
            if !pred.holds(&shape) {
                failed = Some(pred);
                break;
            }
        }
        match failed {
            Some(pred) => conflicts.push(ConflictReport {
                kind: ConflictKind::SchemaMismatch,
                tbl,
                row: RowId::SCHEMA,
                // No ordinal: the column the predicate names may not exist in this shape at all,
                // and an ordinal that means nothing is worse than none.
                col: None,
                violated_guard: Some(pred.as_guard(&shape)),
                ours: None,
                theirs: None,
                detail: format!(
                    "`{edit}` was written against a shape of '{table}' that no longer holds; \
                     {} is now {}",
                    pred.column,
                    pred.observed(&shape)
                ),
            }),
            None => {
                edit.apply(&mut shape)?;
                to_apply.push(edit.clone());
            }
        }
    }

    if !conflicts.is_empty() {
        // Nothing half-applied: a conflicting merge publishes no schema at all, exactly as a
        // conflicting row merge publishes no rows.
        return Ok(SchemaMerge {
            outcome: SchemaMergeOutcome::Conflict(conflicts),
            shape: target_now.clone(),
            to_apply: Vec::new(),
        });
    }

    let outcome = if they_moved && !ours.is_empty() {
        SchemaMergeOutcome::Commuting { composed: ours.to_vec() }
    } else {
        SchemaMergeOutcome::Clean
    };
    Ok(SchemaMerge { outcome, shape, to_apply })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, ty: DataType) -> Column {
        Column { name: name.into(), data_type: ty, nullable: true }
    }

    fn base() -> Schema {
        Schema::new(vec![
            Column { name: "id".into(), data_type: DataType::Integer, nullable: false },
            col("qty", DataType::Integer),
        ])
    }

    fn merged(target: &Schema, ours: &[SchemaEdit]) -> SchemaMerge {
        merge_schema("inventory", TableId(1), &base(), target, ours).unwrap()
    }

    fn names(s: &Schema) -> Vec<&str> {
        s.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// **THE exit criterion, first half: two agents adding different columns compose.**
    ///
    /// Breaking shape: the second branch's edit checked against the shape it *forked* from rather
    /// than the shape the target has now. Against the fork point both adds are legal and both land
    /// — but so would two adds of the SAME column, and the result would be a table with two
    /// columns of one name. The precondition has to be re-evaluated against the merged shape,
    /// which is the whole of DESIGN's step 3.
    #[test]
    fn two_branches_adding_different_columns_compose() {
        // Agent A already merged `note`, so the target has moved on.
        let mut target = base();
        target.columns.push(col("note", DataType::Varchar(20)));

        // Agent B forked before that and adds `sku`.
        let m = merged(&target, &[SchemaEdit::AddColumn(col("sku", DataType::Varchar(8)))]);

        assert_eq!(m.outcome.name(), "Commuting", "{:?}", m.outcome);
        assert_eq!(names(&m.shape), vec!["id", "qty", "note", "sku"]);
        assert_eq!(m.to_apply.len(), 1, "the add still has to be executed against the target");
    }

    /// **THE exit criterion, second half: two agents retyping one column conflict, with the
    /// violated predicate returned.**
    ///
    /// Breaking shape: two *different* target types. Agent A made `qty` a `BIGINT`; agent B, which
    /// forked when it was `INTEGER`, wants a `DECIMAL`. A merge that only compared the edits to
    /// each other, or that checked "is `qty` still there", would let the second one through and
    /// silently overwrite the first agent's type.
    #[test]
    fn two_branches_retyping_one_column_conflict_and_name_the_predicate() {
        let mut target = base();
        target.columns[1].data_type = DataType::BigInt; // agent A landed INTEGER -> BIGINT

        let m = merged(
            &target,
            &[SchemaEdit::RetypeColumn {
                column: "qty".into(),
                from: DataType::Integer,
                to: DataType::Decimal,
            }],
        );

        assert!(m.outcome.is_conflict(), "{:?}", m.outcome);
        let report = &m.outcome.conflicts()[0];
        assert_eq!(report.kind, ConflictKind::SchemaMismatch);
        assert!(report.row.is_schema(), "a schema conflict pointed at a row: {}", report.row);

        // Exit criterion 7: the predicate itself, not a boolean.
        let guard = report.violated_guard.as_ref().expect("no predicate handed back");
        assert_eq!(guard.violated_predicate(), "typeof(inventory.qty) = INTEGER");
        assert!(report.feedback().contains("typeof(inventory.qty) = INTEGER"), "{report}");
        // And it says what it found, so the agent can retry against the truth.
        assert!(report.detail.contains("BIGINT"), "{}", report.detail);

        // Nothing half-applied.
        assert_eq!(m.shape, target);
        assert!(m.to_apply.is_empty());
    }

    /// The guard in a schema conflict is a real, re-checkable predicate rather than a label: it
    /// evaluates to false on its own, against any context.
    ///
    /// Breaking shape: a `ConflictReport` whose `violated_guard` were synthesised from the failure
    /// message. `check` would then either not compile or trivially pass, and a caller re-deriving
    /// the verdict would get the opposite answer from the one it was handed.
    #[test]
    fn the_returned_predicate_re_evaluates_to_false() {
        struct Nothing;
        impl crate::tel::guard::GuardContext for Nothing {
            fn column(
                &self,
                _t: TableId,
                _r: RowId,
                _c: crate::tel::ids::ColId,
            ) -> Result<Value, FerroError> {
                Err(FerroError::Merge("this context has no cells".into()))
            }
        }
        let mut target = base();
        target.columns[1].data_type = DataType::BigInt;
        let m = merged(
            &target,
            &[SchemaEdit::RetypeColumn {
                column: "qty".into(),
                from: DataType::Integer,
                to: DataType::Decimal,
            }],
        );
        let guard = m.outcome.conflicts()[0].violated_guard.clone().unwrap();
        assert_eq!(guard.check(&Nothing).unwrap(), false, "the returned guard does not fail");

        // Anti-vacuity: a guard built from a precondition that DOES hold re-evaluates to true, so
        // the assertion above is about this predicate and not about every guard this code builds.
        let ok = SchemaPredicate::of_type("inventory", "qty", &DataType::BigInt).as_guard(&target);
        assert_eq!(ok.check(&Nothing).unwrap(), true);
    }

    /// **Anti-vacuity for the conflict: adding the same column twice is refused too, and a
    /// one-sided change is not a conflict at all.**
    #[test]
    fn adding_the_same_column_twice_with_different_types_conflicts() {
        let mut target = base();
        target.columns.push(col("note", DataType::Varchar(20)));
        let m = merged(&target, &[SchemaEdit::AddColumn(col("note", DataType::Integer))]);
        assert!(m.outcome.is_conflict(), "{:?}", m.outcome);
        assert_eq!(
            m.outcome.conflicts()[0].violated_guard.as_ref().unwrap().violated_predicate(),
            "typeof(inventory.note) = <absent>"
        );
    }

    /// An edit the target already satisfies **identically** is absorbed, not refused. The same
    /// rule `resolve_cell` applies to two equal `Assign`s.
    ///
    /// Breaking shape: two agents that independently made the same change. Refusing the second
    /// would force a retry that has nothing to do.
    #[test]
    fn an_identical_edit_is_absorbed_rather_than_refused() {
        let mut target = base();
        target.columns.push(col("note", DataType::Varchar(20)));

        let add_same = merged(&target, &[SchemaEdit::AddColumn(col("note", DataType::Varchar(20)))]);
        assert!(!add_same.outcome.is_conflict(), "{:?}", add_same.outcome);
        assert_eq!(names(&add_same.shape), vec!["id", "qty", "note"]);
        assert!(add_same.to_apply.is_empty(), "an absorbed edit must not be executed twice");

        let mut retyped = base();
        retyped.columns[1].data_type = DataType::BigInt;
        let same_retype = merged(
            &retyped,
            &[SchemaEdit::RetypeColumn {
                column: "qty".into(),
                from: DataType::Integer,
                to: DataType::BigInt,
            }],
        );
        assert!(!same_retype.outcome.is_conflict(), "{:?}", same_retype.outcome);
        assert!(same_retype.to_apply.is_empty());
    }

    /// A branch that changed the shape while nothing else did is `Clean`, not `Commuting` —
    /// `Commuting` means both sides touched the same subject, which is `tel::engine`'s rule.
    #[test]
    fn a_one_sided_schema_change_is_clean() {
        let m = merged(&base(), &[SchemaEdit::AddColumn(col("note", DataType::Varchar(20)))]);
        assert_eq!(m.outcome.name(), "Clean", "{:?}", m.outcome);
        assert_eq!(names(&m.shape), vec!["id", "qty", "note"]);
        assert_eq!(m.to_apply.len(), 1);
    }

    /// And a branch that changed nothing is `Clean` even when the target moved underneath it.
    #[test]
    fn no_edits_is_clean_even_when_the_target_moved() {
        let mut target = base();
        target.columns.push(col("note", DataType::Varchar(20)));
        let m = merged(&target, &[]);
        assert_eq!(m.outcome.name(), "Clean");
        assert_eq!(m.shape, target, "a branch with no schema edits must not change the shape");
    }

    /// A rename carries two preconditions and they are different claims. Breaking shape: renaming
    /// to a name another branch has since taken.
    #[test]
    fn a_rename_onto_a_name_another_branch_created_conflicts() {
        let mut target = base();
        target.columns.push(col("quantity", DataType::Varchar(4)));
        let m = merged(
            &target,
            &[SchemaEdit::RenameColumn { from: "qty".into(), to: "quantity".into() }],
        );
        assert!(m.outcome.is_conflict(), "{:?}", m.outcome);
        assert_eq!(
            m.outcome.conflicts()[0].violated_guard.as_ref().unwrap().violated_predicate(),
            "typeof(inventory.quantity) = <absent>"
        );
    }

    /// The other half of the rename's precondition: the column being renamed has to still be
    /// there. This is the documented consequence of columns having no identity beyond their name —
    /// a branch that retyped `qty` and a branch that renamed it do NOT compose, and the agent is
    /// told exactly that rather than having one of the two changes silently vanish.
    #[test]
    fn renaming_a_column_another_branch_renamed_first_conflicts() {
        let mut target = base();
        target.columns[1].name = "amount".into();
        let m = merged(
            &target,
            &[SchemaEdit::RenameColumn { from: "qty".into(), to: "quantity".into() }],
        );
        assert!(m.outcome.is_conflict(), "{:?}", m.outcome);
        let g = m.outcome.conflicts()[0].violated_guard.as_ref().unwrap();
        assert_eq!(g.violated_predicate(), "typeof(inventory.qty) = <any type>");
    }

    /// A rename and an add of a *different* column compose, which is the anti-vacuity companion
    /// to the two rename conflicts above.
    #[test]
    fn a_rename_composes_with_an_unrelated_add() {
        let mut target = base();
        target.columns.push(col("note", DataType::Varchar(20)));
        let m = merged(
            &target,
            &[SchemaEdit::RenameColumn { from: "qty".into(), to: "quantity".into() }],
        );
        assert_eq!(m.outcome.name(), "Commuting", "{:?}", m.outcome);
        assert_eq!(names(&m.shape), vec!["id", "quantity", "note"]);
    }

    /// A predicate over an absent column must be *false*, never unevaluable. `tel::guard` maps
    /// "could not be evaluated" to a hard reject rather than a retry, so an `ADD COLUMN` whose
    /// precondition errored when the column was missing would report its own success as a fatal
    /// error.
    #[test]
    fn a_predicate_about_an_absent_column_is_answerable() {
        let p = SchemaPredicate::absent("inventory", "nosuch");
        assert!(p.holds(&base()));
        assert_eq!(p.observed(&base()), ABSENT);
        assert!(!SchemaPredicate::present("inventory", "nosuch").holds(&base()));
        assert!(!SchemaPredicate::of_type("inventory", "nosuch", &DataType::Integer).holds(&base()));
    }
}
