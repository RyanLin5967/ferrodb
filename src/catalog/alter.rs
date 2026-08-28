//! `ALTER TABLE ... ADD COLUMN / RENAME COLUMN / ALTER COLUMN TYPE` — B11.
//!
//! The catalog had exactly three mutating operations before this — `create_table`, `drop_table`,
//! `create_index` — all of them whole-table. Nothing could change a column, which is why the
//! change feed's `SchemaChange` had only `CreateTable` and `DropTable` to carry: there was no
//! column-level event because there was no column-level change.
//!
//! # Why a column change is a heap rewrite here, and not metadata
//!
//! A tuple is laid out **positionally against the schema that wrote it** and records nothing about
//! its own shape: a null bitmap sized `(ncols + 7) / 8`, then each column at an offset recomputed
//! from the schema on every read (`storage::tuple`). So a row written under a two-column schema is
//! not a prefix of the same row under a three-column one — the bitmap width can change, and the
//! reader walks off the end of the bytes looking for a column that was never written. The same
//! goes for a retype: `INTEGER` occupies four bytes and `BIGINT` eight, and every column after it
//! shifts.
//!
//! Three alternatives were considered and rejected:
//!
//! - **Tolerant deserialization** — treat a short tuple as "the missing columns are NULL". It works
//!   for a table with eight columns or fewer and silently produces garbage at the ninth, where the
//!   null bitmap grows by a byte and every offset after it moves. A correctness cliff at a column
//!   count is worse than no feature.
//! - **A per-tuple column count in the version header's two `reserved` bytes.** Fixes the bitmap
//!   problem, and does nothing at all for a retype, which changes the width of a column that is
//!   already there. Every tuple already on disk also carries `reserved = 0`, so the format would
//!   need a "written before this existed" sentinel whose meaning is "read me with today's schema"
//!   — which is exactly the wrong answer after an alter.
//! - **Rebuild into a fresh heap and swap the roots**, as `create_index` does for an index. This
//!   changes `first_directory_page_id`, and that field is the table's *identity in the change
//!   feed*: `logical.rs` keys its whole `dir_root -> table` mapping on it, and E69's DDL record
//!   comment records that a `DROP` has to name the same `dir_root` the `CREATE` did or a consumer
//!   cannot match the two events to one table.
//!
//! So the rewrite is in place, against the same `dir_root`, one tuple at a time.
//!
//! # What the rewrite is allowed to assume, and who enforces it
//!
//! **No transaction may be in flight.** [`Catalog::alter_table`] takes the [`TxnManager`] and
//! refuses if any transaction is active, rather than documenting the requirement and hoping. Two
//! things rest on it:
//!
//! - a concurrent reader would be handed rows in a shape its plan was built against and no longer
//!   matches;
//! - the rewrite **truncates every version chain** (it zeroes `prev_page`/`prev_slot`). A chain is
//!   only ever walked by [`crate::wal::visibility::resolve_visibility`] on behalf of a reader whose
//!   snapshot predates the head version. With nothing in flight, no such reader exists and none can
//!   be created afterwards: a new transaction's snapshot high-water is above every committed
//!   version, so it stops at the head. The superseded versions still sitting in the time-travel
//!   heap were written under the old shape and become unreachable, which is the honest outcome —
//!   they are not reinterpreted under a schema they were not written with. `drop_table` still frees
//!   them.
//!
//! # What is refused, and why each refusal is not conservatism
//!
//! - **`ADD COLUMN ... NOT NULL`.** There is no `DEFAULT` in this SQL surface, so every existing
//!   row would have to violate the constraint the statement just declared.
//! - **Retyping the primary key.** The rewrite repoints only the rows it moves, so the primary
//!   index would be left holding keys of two types at once. `Update::execute` already refuses to
//!   update the primary key for the neighbouring reason. (An earlier draft justified this by
//!   claiming cross-type values do not compare; they do — see the note at the refusal.)
//! - **Any conversion not in [`Widening`].** An allowlist. A denylist would only catch the
//!   conversions someone already thought of, and the cost of admitting a wrong one is a column of
//!   values that are silently different from what was stored.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::column::{DataType, Value};
use crate::catalog::schema::Schema;
use crate::catalog::stats::{ColumnStats, TableStats};
use crate::error::FerroError;
use crate::parser::parser::AlterAction;
use crate::provenance::ProvenanceStore;
use crate::storage::heap_file_manager::{HeapFileManager, RecordId};
use crate::storage::heap_page::{MAX_TUPLE_SIZE, SLOT_ENTRY_SIZE};
use crate::storage::index::BPlusTreeManager;
use crate::storage::tuple::{Tuple, VERSION_HEADER_SIZE};
use crate::wal::txn::TxnManager;

/// A column type change this database will perform on rows that already exist.
///
/// **The complete set, as one `match`.** `of` decides whether a pair is allowed and `apply`
/// converts a value, and they cannot drift apart because `apply` matches on the variant that `of`
/// produced rather than on the type pair a second time. Two functions that must agree about a
/// list is how a database ends up accepting a conversion in the planner that the storage layer
/// then performs incorrectly.
///
/// Every member is **total**: there is no value of the source type that has no image in the
/// target. That is the whole membership rule, and it is why `BIGINT -> INTEGER` and
/// `VARCHAR(20) -> VARCHAR(4)` are absent — both have values that do not fit — and why
/// `INTEGER -> FLOAT` is absent even though every `i32` is an exact `f64`: it is total on values
/// and not on *representations*, since the feed renders a float and an integer differently and a
/// consumer holding the old rows would have two encodings for one column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Widening {
    /// `INTEGER -> BIGINT`. Every `i32` is an `i64`.
    IntToBigInt,
    /// `INTEGER -> DECIMAL`. Decimal is exact digit text here, so an integer is one exactly.
    IntToDecimal,
    /// `BIGINT -> DECIMAL`. Same, and the reason `DECIMAL` is the widest target.
    BigIntToDecimal,
    /// `VARCHAR(n) -> VARCHAR(m)` with `m >= n`. The stored layout is identical — a `u16` length
    /// prefix and the bytes — so this one moves no byte at all; it widens what a *future* write
    /// may store.
    VarcharWider,
}

impl Widening {
    /// The allowlist. `None` is the refusal.
    pub fn of(from: &DataType, to: &DataType) -> Option<Widening> {
        match (from, to) {
            (DataType::Integer, DataType::BigInt) => Some(Widening::IntToBigInt),
            (DataType::Integer, DataType::Decimal) => Some(Widening::IntToDecimal),
            (DataType::BigInt, DataType::Decimal) => Some(Widening::BigIntToDecimal),
            (DataType::Varchar(n), DataType::Varchar(m)) if m >= n => Some(Widening::VarcharWider),
            _ => None,
        }
    }

    /// Convert one stored value. `NULL` is `NULL` under every widening — it is the absence of a
    /// value, not a value of the old type.
    ///
    /// An `Err` here means the row on disk did not hold the type its schema declared, which is
    /// corruption rather than a conversion failure, and is reported as such rather than being
    /// turned into a plausible-looking value.
    pub fn apply(&self, v: &Value) -> Result<Value, FerroError> {
        if matches!(v, Value::Null) {
            return Ok(Value::Null);
        }
        Ok(match (self, v) {
            (Widening::IntToBigInt, Value::Integer(i)) => Value::BigInt(*i as i64),
            (Widening::IntToDecimal, Value::Integer(i)) => Value::Decimal(i.to_string()),
            (Widening::BigIntToDecimal, Value::BigInt(i)) => Value::Decimal(i.to_string()),
            (Widening::VarcharWider, Value::Varchar(s)) => Value::Varchar(s.clone()),
            (w, other) => {
                return Err(FerroError::Internal(format!(
                    "a stored value {other:?} does not have the type its column declares; \
                     {w:?} cannot convert it"
                )));
            }
        })
    }

    /// How the pair reads in a refusal message.
    pub fn allowed_pairs() -> &'static str {
        "INTEGER -> BIGINT, INTEGER -> DECIMAL, BIGINT -> DECIMAL, \
         and VARCHAR(n) -> VARCHAR(m) where m >= n"
    }
}

/// One column of a table's shape, as the DDL record and the change feed carry it.
pub type ColumnShape = (String, DataType, bool);

fn shape_of(schema: &Schema) -> Vec<ColumnShape> {
    schema
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.data_type.clone(), c.nullable))
        .collect()
}

fn no_such_column(table: &str, column: &str, schema: &Schema) -> FerroError {
    FerroError::Bind(format!(
        "'{table}' has no column '{column}'; its columns are: {}",
        schema.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(", ")
    ))
}

/// The shape `action` produces, or the refusal it earns — **without touching anything**.
///
/// One definition, two callers, and the second one is why it exists. [`Catalog::alter_table`]
/// executes an alter against the shared tables; `AgentRuntime::stage_schema_edit` records one on a
/// branch, to be executed at `MERGE`. If the branch did not run the same rules at the moment the
/// agent typed the statement, an `ADD COLUMN ... NOT NULL` would be accepted, sit on the branch
/// through an arbitrary amount of further work, and be refused at merge time — feedback arriving
/// after everything that depended on it.
///
/// `row_count` is only used to make the NOT NULL refusal say how many rows would violate it.
pub fn resulting_schema(
    table: &str,
    schema: &Schema,
    action: &AlterAction,
    row_count: usize,
) -> Result<Schema, FerroError> {
    match action {
        AlterAction::RenameColumn { from, to } => {
            if !schema.columns.iter().any(|c| &c.name == from) {
                return Err(no_such_column(table, from, schema));
            }
            if schema.columns.iter().any(|c| &c.name == to) {
                return Err(FerroError::Constraint(format!(
                    "cannot rename '{table}.{from}' to '{to}': '{table}' already has a column \
                     called '{to}'"
                )));
            }
            let mut columns = schema.columns.clone();
            for c in columns.iter_mut() {
                if &c.name == from {
                    c.name = to.clone();
                }
            }
            Ok(Schema::new(columns))
        }
        AlterAction::AddColumn(col) => {
            if schema.columns.iter().any(|c| c.name == col.name) {
                return Err(FerroError::Constraint(format!(
                    "cannot add '{table}.{}': the table already has a column called '{}'",
                    col.name, col.name
                )));
            }
            if !col.nullable {
                return Err(FerroError::Constraint(format!(
                    "cannot add '{table}.{}' as NOT NULL: this SQL surface has no DEFAULT, so the \
                     {row_count} row(s) already in '{table}' would have no value for it and every \
                     one of them would violate the constraint the statement just declared. Add it \
                     nullable and fill it with UPDATE.",
                    col.name
                )));
            }
            let mut columns = schema.columns.clone();
            columns.push(col.clone());
            Ok(Schema::new(columns))
        }
        AlterAction::RetypeColumn { column, to } => {
            let idx = schema
                .columns
                .iter()
                .position(|c| &c.name == column)
                .ok_or_else(|| no_such_column(table, column, schema))?;
            if idx == 0 {
                // **Refused conservatively, and the reason stated here is the true one.**
                //
                // An earlier version of this message claimed that values of different types do not
                // compare and that a half-converted primary index would be unsearchable. That is
                // wrong: `Value::cmp` compares the numeric band by value, so an `Integer` key and
                // the `BigInt` it becomes are equal. The real reason is narrower and is about what
                // has been established rather than about what is impossible — the rewrite repoints
                // only the rows it MOVES, so a retyped primary key leaves the index holding keys of
                // two types at once, and every path that reads it (recovery's `rebuild_indexes`,
                // the range scan, a branch's own row store) would be resting on that cross-type
                // comparison holding everywhere. The cost of being wrong is a table in which no row
                // can be found by key. `UPDATE` refuses to move a primary key for the neighbouring
                // reason, and this follows it until someone measures the alternative.
                return Err(FerroError::Constraint(format!(
                    "column '{column}' of '{table}' is the primary key and its type cannot be \
                     changed: the rewrite repoints only the rows it moves, so the primary index \
                     would be left holding keys of two types at once. DELETE and re-INSERT under \
                     the new type, or rebuild the table. This is the same restriction UPDATE \
                     places on the primary key."
                )));
            }
            let from = schema.columns[idx].data_type.clone();
            if from == *to {
                return Err(FerroError::Constraint(format!(
                    "column '{column}' of '{table}' is already {from}; a no-op ALTER would still \
                     be published to the change feed as though something had changed"
                )));
            }
            if Widening::of(&from, to).is_none() {
                return Err(FerroError::Constraint(format!(
                    "cannot change '{table}.{column}' from {from} to {to}: this database performs \
                     only conversions it can prove total for every stored value, which are {}. \
                     Anything else would have to decide what to do with a value that does not fit, \
                     and every answer to that is data loss.",
                    Widening::allowed_pairs()
                )));
            }
            let mut columns = schema.columns.clone();
            columns[idx].data_type = to.clone();
            Ok(Schema::new(columns))
        }
    }
}

/// Convert a row written under `from` into one that fits `to`.
///
/// Used where a row and the shape it is about to be written into disagree because the shape moved
/// underneath it — specifically, a branch that forked before a sibling agent's `ADD COLUMN` merged.
/// Without it, publishing that branch's rows fails deep inside `Tuple::serialize` with a message
/// about value counts, for a situation that is neither an error nor the caller's fault.
///
/// `to` must be `from` plus appended columns, with each retained column either unchanged or a
/// [`Widening`] of its old type. That is exactly the set of shapes an `ALTER` in this database can
/// produce, and anything else is refused rather than guessed at — a row silently reinterpreted
/// under the wrong shape is the failure mode this whole module exists to avoid.
pub fn conform_row(values: &[Value], from: &Schema, to: &Schema) -> Result<Vec<Value>, FerroError> {
    if from == to {
        return Ok(values.to_vec());
    }
    if to.columns.len() < from.columns.len() {
        return Err(FerroError::Constraint(format!(
            "a row written against {} column(s) cannot be conformed to a shape with {}: this \
             database has no DROP COLUMN, so the shape did not narrow — the row was written \
             against a different table",
            from.columns.len(),
            to.columns.len()
        )));
    }
    if values.len() != from.columns.len() {
        return Err(FerroError::Internal(format!(
            "row has {} value(s) for a {}-column shape",
            values.len(),
            from.columns.len()
        )));
    }
    let mut out = Vec::with_capacity(to.columns.len());
    for (i, old) in from.columns.iter().enumerate() {
        let new = &to.columns[i];
        if old.data_type == new.data_type {
            out.push(values[i].clone());
            continue;
        }
        let w = Widening::of(&old.data_type, &new.data_type).ok_or_else(|| {
            FerroError::Constraint(format!(
                "column {i} moved from {} to {}, which is not a conversion this database performs",
                old.data_type, new.data_type
            ))
        })?;
        out.push(w.apply(&values[i])?);
    }
    // Appended columns. A column can only be appended nullable (see `resulting_schema`), so NULL
    // is the value the row would have had if it had been written after the ALTER.
    for c in &to.columns[from.columns.len()..] {
        if !c.nullable {
            return Err(FerroError::Constraint(format!(
                "column '{}' was appended NOT NULL, which this database refuses; a row from before \
                 it has no value for it",
                c.name
            )));
        }
        out.push(Value::Null);
    }
    Ok(out)
}

/// Everything a chain of alterations on one table has decided, with nothing yet written.
///
/// # Why a plan exists, and why it holds a SEQUENCE rather than one action
///
/// [`Catalog::alter_table`] was already two halves — decide everything the data can refuse, then
/// write — and [`prepare_rewrite`]'s header argues at length why that split is the whole safety
/// property rather than an optimisation: the rewrite is unlogged, so "refused" and "unchanged"
/// have to be the same state rather than two states something has to reconcile afterwards.
///
/// That argument covers ONE alteration and says nothing whatsoever about several. A branch merged
/// by `AgentRuntime::merge` carries however many schema edits its agent staged, and executing them
/// one `alter_table` call at a time made each individually atomic and the group not atomic at all:
/// an edit refused at position *k* left `1..k-1` installed in the catalog, flushed to disk and
/// emitted to the change feed, by a statement that reported failure. E82. A plan is the same split
/// raised to the group.
///
/// The chain is executed as a **single** pass over the heap, so no intermediate shape ever reaches
/// the disk — but each step is still measured on its own (see [`prepare_rewrite`]), because the
/// semantics being implemented are "these alterations, in this order", and collapsing them into
/// one pass must not accept a chain that running the statements by hand would have refused.
pub struct AlterPlan {
    table: String,
    /// The shape the heap on disk is written against, followed by the shape each action produces.
    /// Always one longer than `actions`.
    shapes: Vec<Schema>,
    actions: Vec<AlterAction>,
    /// Every tuple of the table, carried through the whole chain and serialized under the last
    /// shape. Empty is a legitimate plan: a table with no rows.
    prepared: Vec<Prepared>,
    carried: Option<Vec<ColumnStats>>,
    prov: Option<Arc<dyn ProvenanceStore>>,
    dir_root: u32,
    primary_root: u32,
}

impl AlterPlan {
    /// The shape the table will have once this plan is applied — which is the shape any row
    /// written after it has to be in.
    pub fn final_shape(&self) -> &Schema {
        self.shapes.last().expect("a plan always holds the shape it starts from")
    }

    /// The table this plan alters.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Each action paired with the shape it is applied **to**.
    ///
    /// That pairing is what `execution::executor::alteration_of` needs, and it is exactly what
    /// stops existing the moment the plan is applied: a rename's old name and a retype's old type
    /// live only in the shape before the action. A caller that logs the alterations reads them
    /// from here rather than from the catalog, for the same reason `alter_table` returns the shape
    /// it produced instead of letting the caller re-read it.
    pub fn steps(&self) -> impl Iterator<Item = (&AlterAction, &Schema)> {
        self.actions.iter().zip(self.shapes.iter())
    }
}

/// Refuse a row that cannot be written under `schema` — **every reason the write itself would
/// refuse it for, asked while nothing has been written.**
///
/// Three questions, one function, because a caller that asks only two of them has a merge that
/// refuses halfway:
///
/// - **arity and type**, from [`Tuple::serialize`] inside [`serialize_and_measure`], which is the
///   same call the executor's insert makes and which refuses a value whose variant contradicts the
///   column that declares it;
/// - **NOT NULL**, which nothing on the branch path checks. `AgentRuntime::branch_insert` proves
///   arity and a branch-visible duplicate key and stops there, and the binder carries `nullable`
///   without ever refusing on it, so a `NULL` in a NOT NULL column is accepted when the agent
///   types it and refused by `InsertOp::execute` at publication — which, now that the schema is
///   applied first, is after the merge has already changed the table.
/// - **page fit**, which is the question [`prepare_rewrite`] asks of the rows already there.
///
/// **The second caller is why [`serialize_and_measure`] exists.** `AgentRuntime::merge` publishes
/// a branch's rows into a table the same merge is altering, and "can this row be written in the
/// shape it is about to land in" is the identical question pass 1 asks about the rows already in
/// it. Two copies of [`MAX_TUPLE_SIZE`] is how two answers to one question start to disagree.
///
/// `which` names the row for the message — the caller knows whether it has a key to quote.
pub fn refuse_if_the_row_cannot_land(
    table: &str,
    schema: &Schema,
    values: &[Value],
    which: &str,
) -> Result<(), FerroError> {
    for (i, c) in schema.columns.iter().enumerate() {
        if c.nullable || !matches!(values.get(i), None | Some(Value::Null)) {
            continue;
        }
        return Err(FerroError::Constraint(format!(
            "this MERGE would publish a row into '{table}' with no value for '{}', which is \
             declared NOT NULL: {which}. Nothing has been written — the schema edits and the rows \
             are decided together and refused together, because a merge that applied one without \
             the other is exactly the half-applied state a merge exists to avoid.",
            c.name
        )));
    }
    let (tuple, fits) = serialize_and_measure(values, schema)?;
    if fits {
        return Ok(());
    }
    let size = tuple.data.len();
    Err(FerroError::Constraint(format!(
        "this MERGE would publish a row into '{table}' that does not fit: {which} would occupy \
         {size} bytes under the shape this merge leaves '{table}' in, past the {MAX_TUPLE_SIZE} \
         bytes a tuple can occupy. Nothing has been written — the schema edits and the rows are \
         decided together and refused together, because a merge that applied one without the \
         other is exactly the half-applied state a merge exists to avoid."
    )))
}

/// The sentence every row-width refusal ends with.
///
/// Exported so a caller can recognise its OWN refusal coming back out of a plan without matching
/// on the rest of the prose — `AgentRuntime::merge` appends what the advice means inside a merge,
/// and a caller testing for a marker that is built from this same constant cannot drift from the
/// message that carries it.
pub const NARROW_THE_ROW_FIRST: &str = "Narrow the row first";

/// Serialize a row under `schema` and say whether the result fits a page.
///
/// **One definition, two real callers, and that is the whole reason it is a function.**
/// [`prepare_rewrite`] measures the rows a table already holds and
/// [`refuse_if_the_row_cannot_land`] measures the rows a merge is about to publish into it. Same
/// question, same limit; answered in two places, the two answers start to differ.
fn serialize_and_measure(values: &[Value], schema: &Schema) -> Result<(Tuple, bool), FerroError> {
    let tuple = Tuple::serialize(values, schema, 0)?;
    let fits = tuple.data.len() <= MAX_TUPLE_SIZE;
    Ok((tuple, fits))
}

/// **No transaction may be in flight while a table is rewritten in place.**
///
/// Checked next to the rewrite that depends on it rather than in the executor, where it would be
/// one caller's discipline; see the module header for the two properties that rest on it. It is
/// checked twice — once when a plan is made and again when one is applied — because the guard
/// protects the rewrite, and between deciding and writing is exactly where a transaction that was
/// not there before could appear.
fn quiesce_guard(table: &str, txn: &TxnManager) -> Result<(), FerroError> {
    let active = txn.read_snapshot().active;
    if active.is_empty() {
        return Ok(());
    }
    let mut ids: Vec<u64> = active.into_iter().collect();
    ids.sort_unstable();
    Err(FerroError::Txn(format!(
        "ALTER TABLE rewrites '{table}' in place and cannot run while a transaction is open; {} \
         still active: {ids:?}. COMMIT or ROLLBACK first.",
        ids.len()
    )))
}

impl Catalog {
    /// Apply one column-level change, returning the table's **full shape after it**.
    ///
    /// The returned shape is what the DDL record carries and therefore what the retained schema
    /// declaration becomes, which is what makes the change survive a truncation (see
    /// [`TxnManager::log_ddl`]). Returning it rather than making the caller re-read the catalog is
    /// deliberate: the caller must log exactly the shape that was applied, and two reads of a
    /// mutable catalog are two chances to log a different one.
    ///
    /// One alteration is the one-element case of [`Catalog::plan_alters`] and is executed as one,
    /// so there is a single implementation of what an alteration refuses and a single
    /// implementation of what it writes.
    pub fn alter_table(
        &mut self,
        table: &str,
        action: &AlterAction,
        txn: &TxnManager,
        prov: Option<&Arc<dyn ProvenanceStore>>,
    ) -> Result<Vec<ColumnShape>, FerroError> {
        let plan = self.plan_alters(table, std::slice::from_ref(action), txn, prov)?;
        let mut shapes = self.apply_plan(plan, txn)?;
        shapes.pop().ok_or_else(|| {
            FerroError::Internal(format!("a plan over one alteration of '{table}' produced no shape"))
        })
    }

    /// **Decide a chain of alterations against `table`, and write nothing at all.**
    ///
    /// Every refusal any of `actions` can earn happens here, with the table exactly as it was: the
    /// shape rules in [`resulting_schema`], the statistics conversion in [`carried_stats`], and
    /// every data-dependent failure of the heap rewrite — including the row-width one that
    /// [`prepare_rewrite`] exists for. [`Catalog::apply_plan`] then writes what this decided.
    ///
    /// `actions` are applied in order, each against the shape the one before it produces, so an
    /// edit that only becomes illegal once its predecessor has landed is refused here rather than
    /// half way through the group.
    ///
    /// Asking for a plan over no actions is refused rather than answered with an empty one. An
    /// empty plan would still rewrite every tuple of the table for no change, and a caller with
    /// nothing to do has to be able to tell that from a caller with something to do.
    pub fn plan_alters(
        &self,
        table: &str,
        actions: &[AlterAction],
        txn: &TxnManager,
        prov: Option<&Arc<dyn ProvenanceStore>>,
    ) -> Result<AlterPlan, FerroError> {
        if actions.is_empty() {
            return Err(FerroError::Internal(format!(
                "plan_alters was asked for a plan over no alterations of '{table}'; applying it \
                 would rewrite every tuple of the table for no change. A caller with nothing to \
                 do must not ask for a plan."
            )));
        }
        quiesce_guard(table, txn)?;

        let entry = self.require_table(table)?;
        let old_schema = entry.schema.clone();
        let dir_root = entry.first_directory_page_id;
        let primary_root = entry.primary_index_root;
        let row_count = self.stats.get(table).map(|s| s.row_count).unwrap_or(0);

        // Every refusal lives in `resulting_schema`, shared with the branch path, so an agent is
        // told at the moment it types the statement exactly what it would be told at merge — and
        // it runs for EVERY action, against the shape the action before it produces, rather than
        // for the first one only.
        let mut shapes = vec![old_schema.clone()];
        for action in actions {
            let next = resulting_schema(table, shapes.last().unwrap(), action, row_count)?;
            shapes.push(next);
        }
        let new_schema = shapes.last().unwrap().clone();

        // The statistics are carried across the alteration by [`carried_stats`], and they are
        // computed HERE, before the rewrite, rather than inside `finish` where they used to be.
        // `finish` runs only after the heap has been converted, so a conversion that fails inside
        // it is a refusal that cannot leave the table as it was — the same defect as an unchecked
        // row width, one function further on. Every fallible step now happens while the heap is
        // still untouched.
        //
        // Computed from the first shape to the last rather than step by step, which is the same
        // answer: an appended column is NULL under every later shape too, and a chain of widenings
        // is itself a widening — `Integer -> BigInt -> Decimal` and `Integer -> Decimal` both take
        // `Integer(5)` to `Decimal("5")`, through the same [`Widening`] table.
        let carried = carried_stats(&old_schema, &new_schema, self.stats.get(table))?;

        let prepared = prepare_rewrite(&self.buffer_pool, table, dir_root, &shapes, actions, prov)?;

        Ok(AlterPlan {
            table: table.to_string(),
            shapes,
            actions: actions.to_vec(),
            prepared,
            carried,
            prov: prov.cloned(),
            dir_root,
            primary_root,
        })
    }

    /// **Execute a plan.** Returns the table's full shape after each of its actions, in order, so
    /// a caller can log one DDL record per action carrying the shape that action produced.
    ///
    /// Everything the data could refuse was refused by [`Catalog::plan_alters`] while the heap was
    /// still untouched. What is left here is the writing pass and the catalog install, and the
    /// failures it can still meet are environmental — a buffer pool with no evictable frame, a
    /// disk write that fails, a B+tree page that cannot be read. See [`prepare_rewrite`] for what
    /// that boundary is and why the answer to crossing it is to log the rewrite rather than to
    /// pretend it cannot happen.
    pub fn apply_plan(
        &mut self,
        plan: AlterPlan,
        txn: &TxnManager,
    ) -> Result<Vec<Vec<ColumnShape>>, FerroError> {
        // Re-checked at the moment of the rewrite rather than trusted from the plan: the guard is
        // about what is in flight while tuples move, and a plan can be held across a statement.
        quiesce_guard(&plan.table, txn)?;

        let AlterPlan { table, shapes, actions, prepared, carried, prov, dir_root, primary_root } =
            plan;

        // Reserve the space the relocations will need, before the first one happens.
        //
        // A row whose converted tuple no longer fits its page is relocated, and a relocation may
        // have to allocate. `DiskManager::allocate` refuses once the table region below the
        // copy-on-write arena floor is full — a real end-state, since the floor is fixed at
        // `DEFAULT_ARENA_HEADROOM` when the database is created — and discovering that in the
        // middle of `commit_rewrite` leaves rows converted under the old schema with no log to
        // repair them. Asking for the space first turns it into a refusal, and it is still a
        // refusal before the first tuple moves: the worst it can leave behind is empty pages the
        // heap's next insert will use.
        //
        // **It lives here, in the half that writes, and not in `plan_alters`, because it is not
        // free of writes.** `reserve_free_space` appends empty pages through `add_empty_page` —
        // an allocation, a page write, and an unlogged mutation of the page-directory chain. In
        // `plan_alters` it made "decide a chain, write nothing at all" false, and worse, it made
        // it false in the one place the claim matters: `AgentRuntime::merge` plans EVERY table and
        // then measures the rows it would publish, so a reservation for table A could be followed
        // by a refusal about table B, and a refusal that says "Nothing has been written" would
        // have left pages behind.
        //
        // Only rows that GREW are counted; a row that shrank or stayed the same is written in
        // place. The count is bytes rather than pages because that is what the page directory
        // reports, and it is aggregate rather than per-page — `reserve_free_space` says in its own
        // words that fragmentation can still defeat it, which is why `HeapFileManager::update`
        // also reserves each relocation's destination before freeing its source.
        let growth: usize = prepared
            .iter()
            .filter(|p| p.tuple.data.len() > p.was)
            .map(|p| p.tuple.data.len() + SLOT_ENTRY_SIZE)
            .sum();
        if growth > 0 {
            HeapFileManager::open(dir_root, self.buffer_pool.clone()).reserve_free_space(growth)?;
        }

        let primary_root_now =
            commit_rewrite(&self.buffer_pool, dir_root, primary_root, prepared, prov.as_ref())?;

        // An index records the column it covers by NAME (`IndexInfo.column_name`) and the planner
        // re-resolves it to an ordinal with `position()` on every statement. Miss this and the
        // index is not stale, it is unfindable: `open_table` turns the failed lookup into
        // `KeyNotFound` and every query against the table stops working. Applied in the chain's
        // own order, so `a -> b` followed by `b -> c` leaves the index covering `c`.
        //
        // Before `finish`, which is the `persist` both halves of the install ride on: two persists
        // would be two chances to store one half of an alteration.
        let renames: Vec<(&String, &String)> = actions
            .iter()
            .filter_map(|a| match a {
                AlterAction::RenameColumn { from, to } => Some((from, to)),
                _ => None,
            })
            .collect();
        if !renames.is_empty() {
            let entry = self.tables.get_mut(&table).ok_or(FerroError::KeyNotFound)?;
            for (from, to) in renames {
                // **Both lists, and the second one is not decoration.** A `TableEntry` keeps
                // ordinary indexes and full-text indexes in separate vectors, and BOTH record
                // their column by name. `planner::plan` resolves each of them with
                // `position(|c| c.name == info.column_name).ok_or(KeyNotFound)` — the full-text one
                // at `plan.rs:110` exactly as the ordinary one at `:102` — so missing either leaves
                // an index that is not stale but unfindable, and every write against the table
                // stops working. The pre-refactor rename arm walked only `indexes`; a full-text
                // index over a renamed column has been broken since `CREATE FULLTEXT INDEX`
                // existed, through the plain `ALTER TABLE` path as much as through a merge.
                for ind in entry.indexes.iter_mut() {
                    if &ind.column_name == from {
                        ind.column_name = to.clone();
                    }
                }
                for ind in entry.fulltext_indexes.iter_mut() {
                    if &ind.column_name == from {
                        ind.column_name = to.clone();
                    }
                }
            }
        }

        let new_schema = shapes
            .last()
            .cloned()
            .ok_or_else(|| FerroError::Internal(format!("a plan for '{table}' held no shape")))?;
        self.finish(&table, new_schema, primary_root_now, carried)?;
        Ok(shapes[1..].iter().map(shape_of).collect())
    }

    /// Install the new schema and the statistics [`carried_stats`] computed for it, persist, and
    /// hand back the shape.
    ///
    /// `primary_root_now` is the primary index's root as [`rewrite_heap`] left it. It is written
    /// back here, in the same `persist` as the schema, rather than through
    /// [`Catalog::update_primary_root`] — two persists would be two chances to store one half of an
    /// alteration.
    ///
    /// **Nothing in here may be fallible except the `persist` itself**, and that is a property to
    /// preserve rather than a coincidence. This function runs after `rewrite_heap` has converted
    /// every tuple on disk, so an `Err` returned from it produces exactly the state I19 exists to
    /// eliminate: rows in the new shape under the old catalog, from a statement that reported
    /// failure. Anything that can refuse belongs before the rewrite, next to the row-width
    /// precheck. `persist` is the commit point and cannot be moved.
    fn finish(
        &mut self,
        table: &str,
        new_schema: Schema,
        primary_root_now: u32,
        carried: Option<Vec<ColumnStats>>,
    ) -> Result<Vec<ColumnShape>, FerroError> {
        if let (Some(columns), Some(stats)) = (carried, self.stats.get_mut(table)) {
            stats.columns = columns;
        }
        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.schema = new_schema;
        entry.primary_index_root = primary_root_now;
        let shape = shape_of(&entry.schema);
        self.persist()?;
        Ok(shape)
    }
}

/// The statistics a table should have after `old -> new`, or `None` if it has none.
///
/// # The statistics are carried, not dropped, and the difference is not cosmetic
///
/// `analyze` stores one `ColumnStats` **positionally**, so a table that gained a column has
/// statistics one entry short of its columns. Dropping them is the safe-looking answer and it
/// is quietly expensive: with no statistics the cost model falls back to its defaults, and the
/// first thing that changes is that it stops choosing index scans. An `ALTER` would therefore
/// silently de-optimise every query against the table until somebody thought to run `ANALYZE` —
/// a performance cliff triggered by a schema change, with nothing to attribute it to. It is
/// also how a fire-check on this module first came back green for the wrong reason: two tests
/// meant to prove the indexes survived an alter were quietly running sequential scans.
///
/// Carrying them is *exact* here, not a guess, which is the only reason it is allowed:
///
/// - an added column is `NULL` in every existing row, so its statistics are exactly
///   `distinct: 0, nulls: row_count, min/max: None` — what `analyze` would compute;
/// - a renamed column keeps its position, and statistics are positional;
/// - a retyped column's `min`/`max` go through the very [`Widening`] the rows went through, so
///   they are the same values in the new type rather than an estimate of them.
///
/// A table with no statistics to begin with still has none afterwards.
///
/// # Why it is a separate function called before the rewrite
///
/// This is the only fallible part of installing an alteration: `Widening::apply` refuses a stored
/// value whose variant contradicts its column, which is corruption rather than a conversion
/// failure. It used to run inside [`Catalog::finish`], i.e. after `rewrite_heap` had already
/// converted the heap, where refusing leaves the table converted under its old schema — the same
/// shape of defect as an unchecked row width. It is now evaluated while the heap is untouched.
///
/// It is one definition with one caller rather than a dry run next to a real run: two lists that
/// have to agree about the same conversions is the mistake [`Widening`] was written to avoid.
///
/// Reachability, stated rather than implied: no SQL path is known to produce statistics whose
/// `min`/`max` variant contradicts its column, because `analyze` computes them from rows that
/// `Tuple::serialize` already type-checked, and a previous alter carried them through the same
/// widening. This is a `?` removed from a post-mutation path, not a measured failure.
fn carried_stats(
    old: &Schema,
    new: &Schema,
    stats: Option<&TableStats>,
) -> Result<Option<Vec<ColumnStats>>, FerroError> {
    let Some(stats) = stats else { return Ok(None) };
    let mut columns = stats.columns.clone();
    for i in 0..old.columns.len() {
        let (Some(o), Some(n)) = (old.columns.get(i), new.columns.get(i)) else { continue };
        if o.data_type == n.data_type {
            continue;
        }
        let Some(w) = Widening::of(&o.data_type, &n.data_type) else { continue };
        let Some(c) = columns.get_mut(i) else { continue };
        c.min = match &c.min {
            Some(v) => Some(w.apply(v)?),
            None => None,
        };
        c.max = match &c.max {
            Some(v) => Some(w.apply(v)?),
            None => None,
        };
    }
    // An appended column is NULL in every row that already exists.
    while columns.len() < new.columns.len() {
        columns.push(ColumnStats { distinct: 0, nulls: stats.row_count, min: None, max: None });
    }
    columns.truncate(new.columns.len());
    Ok(Some(columns))
}

/// One row, carried through a chain of alterations and serialized, waiting to be written.
///
/// `key` is the row's primary key value read BEFORE any conversion, which is what the index holds.
/// `prov` is its attribution, read in the deciding pass rather than after the move so that the
/// writing pass makes no fallible read of its own.
struct Prepared {
    rid: RecordId,
    key: Option<Value>,
    prov: Option<crate::provenance::ProvId>,
    tuple: Tuple,
    /// The size of the tuple this one replaces. A row that did not grow is written in place and
    /// needs no space reserved for it.
    was: usize,
}

/// Whether any shape in the chain changes what a row's bytes are.
///
/// True when a step changes a row's arity or any column's type. False for a chain of renames,
/// which touch only the catalog. `Schema` equality would be the wrong test: it compares names too,
/// so a rename would read as a change to the rows and it is not one.
fn rewrites_rows(shapes: &[Schema]) -> bool {
    shapes.windows(2).any(|w| {
        w[0].columns.len() != w[1].columns.len()
            || w[0]
                .columns
                .iter()
                .zip(w[1].columns.iter())
                .any(|(a, b)| a.data_type != b.data_type || a.nullable != b.nullable)
    })
}

/// Re-serialize every tuple of a heap through a chain of shapes. **Reads only.**
///
/// `shapes` is the shape the heap is written against followed by the shape each action produces,
/// so `shapes.len()` is one more than `actions.len()`. Each row is carried from one shape to the
/// next by [`conform_row`] — the same function that carries a branch's rows across a sibling's
/// merged `ADD COLUMN`, because "this row was written under shape A and has to become a row under
/// shape B" is one question and deserves one answer.
///
/// # Nothing is written until every row is known to be writable
///
/// The rewrite runs in two passes, and the split is the whole safety argument rather than an
/// optimisation. This pass reads every tuple, converts it and serializes it — producing exactly
/// the bytes [`commit_rewrite`] will lay down — and writes nothing at all. Every failure that
/// depends on the data therefore lands while the heap is still untouched:
///
/// - a tuple too short to hold a version header, or one that does not decode under the old schema;
/// - a stored value whose type disagrees with the column that declares it, which [`Widening::apply`]
///   reports as corruption;
/// - a converted row that [`Tuple::serialize`] refuses;
/// - **a converted row too wide for any page**, which is the one this function was rewritten for.
///   `INTEGER -> BIGINT` moves the retyped column to an eight-byte boundary and shifts every column
///   after it, and an appended column costs at least two bytes plus whatever the null bitmap grows
///   by, so an ordinary row a few bytes under the limit goes over it. Measured: a 22-column table of
///   `VARCHAR(200)`s, nothing oversized anywhere.
///
/// # Every step is measured, not only the last one
///
/// A chain is applied by [`commit_rewrite`] as one pass, so only the final bytes ever reach the
/// disk and only the final width is a *physical* constraint. The width is checked after **every**
/// action anyway, and that is load-bearing rather than defensive.
///
/// What is being implemented is "these alterations, in this order" — the same thing the agent gets
/// by typing the statements one at a time — and a group that accepts a chain the individual
/// statements would have refused is a group whose semantics have drifted from its parts.
///
/// **The two genuinely disagree, so this is not a redundant check.** Widths are not monotone
/// across [`Widening`]: `INTEGER -> DECIMAL` and `BIGINT -> DECIMAL` make a row *smaller*, because
/// a `BigInt` costs eight bytes at an eight-aligned offset while a `Decimal` costs a two-byte
/// length prefix and its digits with no padding. So `ALTER COLUMN n TYPE BIGINT` followed by
/// `ALTER COLUMN n TYPE DECIMAL` can have a final shape that fits and an intermediate that does
/// not — measured on a four-column row with a `VARCHAR(4030)` padding column, the intermediate is
/// 4072 bytes against a 4069-byte limit and the final is 4067. Checking only the last step would
/// accept that chain from a merge and refuse it from two statements.
///
/// # Why an oversized row is a refusal and not a rollback
///
/// Because a rollback here cannot be made to mean anything. The rewrite is deliberately unlogged —
/// [`HeapFileManager::open`] leaves `txn: None`, and the module header explains why the alter is a
/// direct heap mutation rather than a logged one — so there is no undo record, no CLR, and nothing
/// for `recover` to read. An undo would have to be a *second* unlogged mutation, replayed from an
/// in-memory list of the old tuples, whose own failure would have no repair at all; and it would
/// have to be crash-atomic to be worth writing, because a process that dies part-way through the
/// undo leaves precisely the state the undo existed to prevent, with nothing on disk saying an undo
/// was in progress. Deciding before the first write needs none of that: "refused" and "unchanged"
/// are the same state rather than two states that have to be reconciled afterwards.
///
/// It is also the only answer that is honest about `Catalog::finish`. `finish` installs the new
/// schema and runs only on success, so a rewrite that gets half way and returns `Err` leaves rows
/// on disk in the NEW shape under the OLD catalog — a table that reads back as a panic in
/// `Tuple::deserialize`, or as plausible wrong numbers, and stays that way across a checkpoint and
/// a reopen. That, and not the lost row alone, is what made this the most serious defect in B11.
///
/// # What the precheck does not cover, stated here rather than implied
///
/// [`commit_rewrite`] is not infallible; it is free of every failure the *data* can cause. What is
/// left is environmental — a buffer pool with no evictable frame, a disk write that fails, a
/// B+tree page that cannot be read — and one data-dependent case that is unreachable rather than
/// handled: the per-page provenance dictionary is capped at `MAX_PAGE_DICT_ENTRIES` (255) distinct
/// runs, while the page it belongs to has room for on the order of 135 tuples, so re-stamping the
/// rows that moved cannot fill it. `attribute` is read-only and is therefore done here, for the
/// same reason. If any of those does fire, the outcome is the half-rewritten heap described above;
/// the answer to that is to log the rewrite, which is a larger change than this one.
///
/// # Two things about the order of operations are load-bearing
///
/// 1. **Every tuple is read before any tuple is written.** `HeapFileManager::update` relocates a
///    tuple that no longer fits its page into a different page — possibly one the scan has not
///    reached yet — where the scan would find it again and convert it a second time. A row
///    converted twice is not a corrupt row, it is a plausible one with the wrong values.
/// 2. **The version header is carried across, and `prev` is zeroed.** `Tuple::serialize` writes a
///    fresh header, so `end_ts` would be lost and a deleted row would come back to life. The
///    `prev` pointer is deliberately not carried: it points into the time-travel heap at a version
///    written under the old shape. See the module header for why nothing can still want it.
fn prepare_rewrite(
    bp: &Arc<BufferPoolManager>,
    table: &str,
    dir_root: u32,
    shapes: &[Schema],
    actions: &[AlterAction],
    prov: Option<&Arc<dyn ProvenanceStore>>,
) -> Result<Vec<Prepared>, FerroError> {
    // **A chain that cannot change a single byte of a single row does no heap work at all.**
    //
    // Column names are not in the tuple bytes — a column is found by its ordinal — so a rename is
    // the one alteration that moves nothing, and a chain of nothing but renames moves nothing
    // either. Deciding that here rather than at the call site keeps it one rule: what makes a
    // rewrite necessary is that some shape in the chain changes a row's arity or a column's type,
    // which is a property of `shapes` and not of which `AlterAction` produced them. Rewriting
    // anyway would not be merely wasteful — `commit_rewrite` calls `HeapFileManager::update` on
    // every row, which is entitled to relocate one, for a change that by construction is not there.
    if !rewrites_rows(shapes) {
        return Ok(Vec::new());
    }

    let heap = HeapFileManager::open(dir_root, bp.clone());

    // This holds the whole table's converted tuples in memory, which the previous version of this
    // function did too — it held every row's decoded `Vec<Value>`, and the packed bytes are the
    // smaller of the two representations for every type in this database.
    let mut prepared: Vec<Prepared> = Vec::new();
    let mut scanned = 0usize;
    // Only the widest row is named in the refusal, so only the widest is kept: a table where every
    // row is oversized would otherwise clone every primary key to build one error message.
    let mut too_wide = 0usize;
    let mut widest: Option<(RecordId, Option<Value>, usize, usize)> = None;
    for item in heap.scan() {
        let (rid, tuple) = item?;
        scanned += 1;
        if tuple.data.len() < VERSION_HEADER_SIZE {
            return Err(FerroError::Internal(format!(
                "tuple at {rid:?} is {} bytes, shorter than a version header",
                tuple.data.len()
            )));
        }
        let mut header = [0u8; VERSION_HEADER_SIZE];
        header.copy_from_slice(&tuple.data[..VERSION_HEADER_SIZE]);
        let was = tuple.data.len();
        let mut values = tuple.deserialize(&shapes[0])?;
        let key = values.first().cloned();

        let mut converted: Option<Tuple> = None;
        let mut over = false;
        for step in 1..shapes.len() {
            values = conform_row(&values, &shapes[step - 1], &shapes[step])?;
            let (bytes, fits) = serialize_and_measure(&values, &shapes[step])?;
            if !fits {
                too_wide += 1;
                if widest.as_ref().is_none_or(|(_, _, w, _)| bytes.data.len() > *w) {
                    widest = Some((rid, key.clone(), bytes.data.len(), step - 1));
                }
                // No point carrying a row that has already refused the alteration through the rest
                // of the chain; the whole plan is about to be discarded.
                over = true;
                break;
            }
            converted = Some(bytes);
        }
        if over {
            continue;
        }
        let mut converted = converted.ok_or_else(|| {
            FerroError::Internal(format!("a rewrite of '{table}' converted a row through no shape"))
        })?;
        // begin_ts and end_ts exactly as they were; prev deliberately cleared.
        converted.data[..16].copy_from_slice(&header[..16]);
        converted.data[16..VERSION_HEADER_SIZE].fill(0);
        let attribution = match prov {
            Some(store) => {
                let who = store.attribute(rid)?;
                if who.is_none() { None } else { Some(who) }
            }
            None => None,
        };
        prepared.push(Prepared { rid, key, prov: attribution, tuple: converted, was });
    }

    if let Some((rid, key, widest, step)) = &widest {
        let which = match key {
            Some(k) => format!("the row whose first column is {k:?}"),
            None => format!("the row in heap slot {rid:?}"),
        };
        // A single alteration reads exactly as it always did. A chain names the edit that did it,
        // because "one of your five staged edits does not fit" is not an actionable message.
        let at = if actions.len() > 1 {
            format!(
                " at edit {} of {}, {:?},",
                step + 1,
                actions.len(),
                actions.get(*step).ok_or_else(|| FerroError::Internal(
                    "a rewrite refused at a step with no action".into()
                ))?
            )
        } else {
            String::new()
        };
        return Err(FerroError::Constraint(format!(
            "this ALTER would widen {too_wide} of the {scanned} row(s) in '{table}'{at} past the \
             {MAX_TUPLE_SIZE} bytes a tuple can occupy: {which} would become {widest} bytes. \
             Nothing has been written — the rewrite converts the heap in place and is not logged, \
             so it is refused before the first tuple moves rather than abandoned part way \
             through, which would leave rows in the new shape under the old schema. \
             {NARROW_THE_ROW_FIRST} (shorten an oversized VARCHAR with UPDATE, or move the wide \
             column into its own table) and run the ALTER again."
        )));
    }

    Ok(prepared)
}

/// Lay down the tuples [`prepare_rewrite`] produced, and return the primary index's root **as it
/// stands afterwards**.
///
/// Every remaining failure is environmental; see [`prepare_rewrite`]'s note on that boundary.
fn commit_rewrite(
    bp: &Arc<BufferPoolManager>,
    dir_root: u32,
    primary_root: u32,
    prepared: Vec<Prepared>,
    prov: Option<&Arc<dyn ProvenanceStore>>,
) -> Result<u32, FerroError> {
    let heap = HeapFileManager::open(dir_root, bp.clone());
    let primary = BPlusTreeManager::<Value, RecordId>::open(primary_root, bp.clone());
    for Prepared { rid, key, prov: attribution, tuple, was: _ } in prepared {
        let new_rid = heap.update(rid, tuple)?;
        if new_rid != rid {
            // The row moved pages. The primary index is the only structure that stores a
            // `RecordId` — a secondary entry is `(value, primary key)` and holds none — so it is
            // the only one that has to be repointed. The entry is moved only where it actually
            // pointed at this row: a deleted row keeps its heap tombstone AND its index entry,
            // and a key that is not in the index must not be re-inserted by a rewrite.
            if let Some(k) = key {
                if primary.search(&k)? == Some(rid) {
                    primary.delete(&k)?;
                    primary.insert(k, new_rid)?;
                }
            }
            // Provenance is keyed by `RecordId` too (a page-local dictionary slot). Without this
            // the answer to "which agent wrote this row" silently becomes "nobody" for every row
            // the rewrite happened to move.
            if let (Some(store), Some(who)) = (prov, attribution) {
                store.stamp(new_rid, who)?;
            }
        }
    }

    // A split during the repointing above can move the tree's root, and the caller records it.
    //
    // **This used to be a refusal, and a refusal was the wrong answer.** The observation behind it
    // is right — the catalog holds the root page id, and a root that is not written back leaves the
    // next open reading an interior page — but it ran after the entire heap had been rewritten, so
    // returning `Err` there was one more way for a "refused" ALTER to leave the table converted
    // under its old schema. A moved root is not an error in the first place: it is what a B+tree
    // does, and `create_index` and `sync_roots` both simply record the new one. So it is recorded,
    // in the same `persist` that installs the schema.
    //
    // Honest about reachability: no shape tried moved it. The rewrite deletes a key and immediately
    // re-inserts the same key, so the tree's key set is identical when it finishes, and the root
    // stayed put at 300 rows x 1300 bytes, 600 x 600 and 1200 x 60 — every row relocating, every
    // lookup still answering. This is therefore a latent path closed by reasoning rather than a
    // measured failure, and it is closed the way `create_index` already closes it rather than by
    // inventing a rule for it.
    Ok(primary.root_page_id.load(Ordering::Relaxed))
}
