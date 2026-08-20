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

impl Catalog {
    /// Apply one column-level change, returning the table's **full shape after it**.
    ///
    /// The returned shape is what the DDL record carries and therefore what the retained schema
    /// declaration becomes, which is what makes the change survive a truncation (see
    /// [`TxnManager::log_ddl`]). Returning it rather than making the caller re-read the catalog is
    /// deliberate: the caller must log exactly the shape that was applied, and two reads of a
    /// mutable catalog are two chances to log a different one.
    pub fn alter_table(
        &mut self,
        table: &str,
        action: &AlterAction,
        txn: &TxnManager,
        prov: Option<&Arc<dyn ProvenanceStore>>,
    ) -> Result<Vec<ColumnShape>, FerroError> {
        // The quiesce guard. Checked here, next to the rewrite that depends on it, rather than in
        // the executor where it would be one caller's discipline. See the module header for the
        // two properties that rest on it.
        let active = txn.read_snapshot().active;
        if !active.is_empty() {
            let mut ids: Vec<u64> = active.into_iter().collect();
            ids.sort_unstable();
            return Err(FerroError::Txn(format!(
                "ALTER TABLE rewrites '{table}' in place and cannot run while a transaction is \
                 open; {} still active: {ids:?}. COMMIT or ROLLBACK first.",
                ids.len()
            )));
        }

        let entry = self.require_table(table)?;
        let old_schema = entry.schema.clone();
        let dir_root = entry.first_directory_page_id;
        let primary_root = entry.primary_index_root;
        let indexes = entry.indexes.clone();
        let row_count = self.stats.get(table).map(|s| s.row_count).unwrap_or(0);

        // Every refusal lives in `resulting_schema`, shared with the branch path, so an agent is
        // told at the moment it types the statement exactly what it would be told at merge.
        let new_schema = resulting_schema(table, &old_schema, action, row_count)?;

        // The statistics are carried across the alteration by [`carried_stats`], and they are
        // computed HERE, before the rewrite, rather than inside `finish` where they used to be.
        // `finish` runs only after the heap has been converted, so a conversion that fails inside
        // it is a refusal that cannot leave the table as it was — the same defect as an unchecked
        // row width, one function further on. Every fallible step now happens while the heap is
        // still untouched.
        let carried = carried_stats(&old_schema, &new_schema, self.stats.get(table))?;

        match action {
            AlterAction::RenameColumn { from, to } => {
                // No heap work whatsoever. Column names are not in the tuple bytes — a column is
                // found by its ordinal — so a rename is the one alteration that moves nothing.
                let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
                entry.schema = new_schema;
                // An index records the column it covers by NAME (`IndexInfo.column_name`) and the
                // planner re-resolves it to an ordinal with `position()` on every statement. Miss
                // this and the index is not stale, it is unfindable: `open_table` turns the failed
                // lookup into `KeyNotFound` and every query against the table stops working.
                for ind in entry.indexes.iter_mut() {
                    if &ind.column_name == from {
                        ind.column_name = to.clone();
                    }
                }
                let shape = shape_of(&entry.schema);
                self.persist()?;
                Ok(shape)
            }

            AlterAction::AddColumn(_) => {
                // Appended at the end, so every existing column keeps its ordinal — which is what
                // every recorded `Op`, `Guard` and merge-policy key holds. The parser refuses any
                // positional placement for the same reason.
                let width = old_schema.columns.len();
                let (_, primary_root_now) = rewrite_heap(
                    &self.buffer_pool,
                    table,
                    dir_root,
                    primary_root,
                    &old_schema,
                    &new_schema,
                    prov,
                    |values| {
                        debug_assert_eq!(values.len(), width);
                        values.push(Value::Null);
                        Ok(())
                    },
                )?;
                self.finish(table, new_schema, primary_root_now, carried)
            }

            AlterAction::RetypeColumn { column, to } => {
                let idx = old_schema
                    .columns
                    .iter()
                    .position(|c| &c.name == column)
                    .ok_or_else(|| no_such_column(table, column, &old_schema))?;
                let from = old_schema.columns[idx].data_type.clone();
                // `resulting_schema` already refused every pair outside the allowlist, so this
                // cannot be `None`; it is unwrapped through the same function rather than a second
                // list so the two can never disagree about what is allowed.
                let widening = Widening::of(&from, to).ok_or_else(|| {
                    FerroError::Internal(format!(
                        "resulting_schema allowed {from} -> {to} but Widening does not"
                    ))
                })?;

                let (_, primary_root_now) = rewrite_heap(
                    &self.buffer_pool,
                    table,
                    dir_root,
                    primary_root,
                    &old_schema,
                    &new_schema,
                    prov,
                    |values| {
                        values[idx] = widening.apply(&values[idx])?;
                        Ok(())
                    },
                )?;

                // **A secondary index over the retyped column is deliberately NOT rebuilt, and
                // this reverses what an earlier version of this code did.**
                //
                // It rebuilt, on the stated grounds that `Value` orders by type rank so an
                // `Integer` key and a `BigInt` key sort into different regions of the tree and a
                // lookup for the new type walks past the old entries. That is **false**, and a
                // fire-check is what exposed it: `Value::cmp` compares the whole numeric band —
                // `Integer`, `BigInt`, `Float`, `Decimal` — against each other by VALUE, and falls
                // through to `type_rank` only for pairs outside it (`catalog::column`, and the
                // tests there pin exactly this). Every conversion in [`Widening`] stays inside that
                // band or does not change the type at all, so an entry written before the retype
                // compares equal to the same value written after it and the tree stays ordered.
                //
                // The rebuild was therefore unnecessary work — and worse than unnecessary. It
                // discarded every historical `(value, primary key)` entry the index holds, which
                // E66 keeps ON PURPOSE: a secondary entry is how a reader finds a row by a value it
                // *used* to have, and `Update` leaves the old entry in place for exactly that
                // reason. Rebuilding from the live heap silently threw that away.
                let _ = (idx, &indexes);
                self.finish(table, new_schema, primary_root_now, carried)
            }
        }
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

/// Re-serialize every tuple of a heap under a new schema.
///
/// Returns how many tuples were rewritten and the primary index's root **as it stands afterwards**.
/// The transform is handed the row's values decoded against the OLD schema and must leave them
/// matching the NEW one.
///
/// # Nothing is written until every row is known to be writable
///
/// The function runs in two passes, and the split is the whole safety argument rather than an
/// optimisation. Pass 1 reads every tuple, converts it and serializes it — producing exactly the
/// bytes pass 2 will lay down — and writes nothing at all. Every failure that depends on the data
/// therefore lands while the heap is still untouched:
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
/// # Why that is a refusal and not a rollback
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
/// Pass 2 is not infallible; it is free of every failure the *data* can cause. What is left is
/// environmental — a buffer pool with no evictable frame, a disk write that fails, a B+tree page
/// that cannot be read — and one data-dependent case that is unreachable rather than handled: the
/// per-page provenance dictionary is capped at `MAX_PAGE_DICT_ENTRIES` (255) distinct runs, while
/// the page it belongs to has room for on the order of 135 tuples, so re-stamping the rows that
/// moved cannot fill it. `attribute` is read-only and has been moved into pass 1 for the same
/// reason. If any of those does fire, the outcome is the half-rewritten heap described above; the
/// answer to that is to log the rewrite, which is a larger change than this one.
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
fn rewrite_heap(
    bp: &Arc<BufferPoolManager>,
    table: &str,
    dir_root: u32,
    primary_root: u32,
    old: &Schema,
    new: &Schema,
    prov: Option<&Arc<dyn ProvenanceStore>>,
    transform: impl Fn(&mut Vec<Value>) -> Result<(), FerroError>,
) -> Result<(usize, u32), FerroError> {
    let heap = HeapFileManager::open(dir_root, bp.clone());

    /// One row, converted and serialized, waiting to be written.
    ///
    /// `key` is the row's primary key value read BEFORE the transform, which is what the index
    /// holds. `prov` is its attribution, read here rather than after the move so that pass 2 makes
    /// no fallible read of its own.
    struct Prepared {
        rid: RecordId,
        key: Option<Value>,
        prov: Option<crate::provenance::ProvId>,
        tuple: Tuple,
        /// The size of the tuple this one replaces. A row that did not grow is written in place and
        /// needs no space reserved for it.
        was: usize,
    }

    // Pass 1: read, convert, serialize. No write of any kind.
    //
    // This holds the whole table's converted tuples in memory, which the previous version of this
    // function did too — it held every row's decoded `Vec<Value>`, and the packed bytes are the
    // smaller of the two representations for every type in this database.
    let mut prepared: Vec<Prepared> = Vec::new();
    // Only the widest row is named in the refusal, so only the widest is kept: a table where every
    // row is oversized would otherwise clone every primary key to build one error message.
    let mut too_wide = 0usize;
    let mut widest: Option<(RecordId, Option<Value>, usize)> = None;
    for item in heap.scan() {
        let (rid, tuple) = item?;
        if tuple.data.len() < VERSION_HEADER_SIZE {
            return Err(FerroError::Internal(format!(
                "tuple at {rid:?} is {} bytes, shorter than a version header",
                tuple.data.len()
            )));
        }
        let mut header = [0u8; VERSION_HEADER_SIZE];
        header.copy_from_slice(&tuple.data[..VERSION_HEADER_SIZE]);
        let was = tuple.data.len();
        let mut values = tuple.deserialize(old)?;
        let key = values.first().cloned();
        transform(&mut values)?;
        let mut converted = Tuple::serialize(&values, new, 0)?;
        // begin_ts and end_ts exactly as they were; prev deliberately cleared.
        converted.data[..16].copy_from_slice(&header[..16]);
        converted.data[16..VERSION_HEADER_SIZE].fill(0);
        let size = converted.data.len();
        if size > MAX_TUPLE_SIZE {
            too_wide += 1;
            if widest.as_ref().is_none_or(|(_, _, w)| size > *w) {
                widest = Some((rid, key.clone(), size));
            }
        }
        let attribution = match prov {
            Some(store) => {
                let who = store.attribute(rid)?;
                if who.is_none() { None } else { Some(who) }
            }
            None => None,
        };
        prepared.push(Prepared { rid, key, prov: attribution, tuple: converted, was });
    }

    if let Some((rid, key, widest)) = &widest {
        let which = match key {
            Some(k) => format!("the row whose first column is {k:?}"),
            None => format!("the row in heap slot {rid:?}"),
        };
        return Err(FerroError::Constraint(format!(
            "this ALTER would widen {} of the {} row(s) in '{table}' past the {MAX_TUPLE_SIZE} \
             bytes a tuple can occupy: {which} would become {widest} bytes. Nothing has been \
             written — the rewrite converts the heap in place and is not logged, so it is refused \
             before the first tuple moves rather than abandoned part way through, which would \
             leave rows in the new shape under the old schema. Narrow the row first (shorten an \
             oversized VARCHAR with UPDATE, or move the wide column into its own table) and run \
             the ALTER again.",
            too_wide,
            prepared.len(),
        )));
    }

    // Reserve the space the relocations will need, before the first one happens.
    //
    // A row whose converted tuple no longer fits its page is relocated, and a relocation may have
    // to allocate. `DiskManager::allocate` refuses once the table region below the copy-on-write
    // arena floor is full — a real end-state, since the floor is fixed at `DEFAULT_ARENA_HEADROOM`
    // when the database is created — and discovering that in the middle of pass 2 leaves rows
    // converted under the old schema with no log to repair them. Asking for the space first turns
    // it into a refusal: the worst this can leave behind is empty pages the heap's next insert
    // will use, and no tuple has moved.
    //
    // Only rows that GREW are counted; a row that shrank or stayed the same is written in place.
    // The count is bytes rather than pages because that is what the page directory reports, and it
    // is aggregate rather than per-page — `reserve_free_space` says in its own words that
    // fragmentation can still defeat it, which is why `HeapFileManager::update` also reserves each
    // relocation's destination before freeing its source.
    let growth: usize = prepared
        .iter()
        .filter(|p| p.tuple.data.len() > p.was)
        .map(|p| p.tuple.data.len() + SLOT_ENTRY_SIZE)
        .sum();
    if growth > 0 {
        heap.reserve_free_space(growth)?;
    }

    // Pass 2: write. Every remaining failure is environmental; see the note above.
    let primary = BPlusTreeManager::<Value, RecordId>::open(primary_root, bp.clone());
    let mut rewritten = 0usize;
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
        rewritten += 1;
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
    let root_now = primary.root_page_id.load(Ordering::Relaxed);
    Ok((rewritten, root_now))
}

