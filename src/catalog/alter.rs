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
//! - **Retyping the primary key.** `Update::execute` already refuses to update the primary key for
//!   the same reason — moving a key means moving every index entry that points at the row — and a
//!   retype moves every key at once. `Value`'s ordering is by type rank first, so a half-converted
//!   primary index is not merely stale, it is unsearchable.
//! - **Any conversion not in [`Widening`].** An allowlist. A denylist would only catch the
//!   conversions someone already thought of, and the cost of admitting a wrong one is a column of
//!   values that are silently different from what was stored.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::buffer::buffer_pool::BufferPoolManager;
use crate::catalog::catalog::Catalog;
use crate::catalog::catalog_page::IndexInfo;
use crate::catalog::column::{DataType, Value};
use crate::catalog::schema::Schema;
use crate::error::FerroError;
use crate::parser::parser::AlterAction;
use crate::provenance::ProvenanceStore;
use crate::storage::heap_file_manager::{HeapFileManager, RecordId};
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
                return Err(FerroError::Constraint(format!(
                    "column '{column}' of '{table}' is the primary key and its type cannot be \
                     changed: every entry in the primary index is keyed by the value itself, and \
                     values of different types do not compare — a half-converted index is not \
                     stale, it is unsearchable. This is the same restriction UPDATE places on the \
                     primary key."
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
                rewrite_heap(
                    &self.buffer_pool,
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
                self.finish(table, new_schema)
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

                rewrite_heap(
                    &self.buffer_pool,
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

                // A secondary index on the retyped column holds keys of the OLD type. `Value`
                // orders by type rank before value, so those keys do not merely look wrong — they
                // sort into a different region of the tree from every key written after this, and
                // a lookup for the new type walks past them. Rebuilt from the rewritten heap.
                // Indexes on other columns hold keys this change did not touch and are left alone.
                let stale: Vec<IndexInfo> =
                    indexes.iter().filter(|i| &i.column_name == column).cloned().collect();
                for info in &stale {
                    let new_root = rebuild_secondary(
                        &self.buffer_pool,
                        dir_root,
                        &new_schema,
                        idx,
                        info.root_page_id,
                    )?;
                    let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
                    if let Some(i) =
                        entry.indexes.iter_mut().find(|i| i.column_name == info.column_name)
                    {
                        i.root_page_id = new_root;
                    }
                }
                self.finish(table, new_schema)
            }
        }
    }

    /// Install the new schema, drop stale statistics, persist, and hand back the shape.
    ///
    /// `analyze` stores one `ColumnStats` per column *positionally*, so a table that gained a
    /// column has statistics one entry short and the cost model would index past them. They are
    /// dropped rather than extended with a guess: an absent statistic makes the optimizer fall
    /// back to its defaults, and a fabricated one makes it plan against data that does not exist.
    fn finish(
        &mut self,
        table: &str,
        new_schema: Schema,
    ) -> Result<Vec<ColumnShape>, FerroError> {
        let entry = self.tables.get_mut(table).ok_or(FerroError::KeyNotFound)?;
        entry.schema = new_schema;
        let shape = shape_of(&entry.schema);
        self.stats.remove(table);
        self.persist()?;
        Ok(shape)
    }
}

/// Re-serialize every tuple of a heap under a new schema.
///
/// Returns how many tuples were rewritten. The transform is handed the row's values decoded
/// against the OLD schema and must leave them matching the NEW one.
///
/// Two things about the order of operations are load-bearing:
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
    dir_root: u32,
    primary_root: u32,
    old: &Schema,
    new: &Schema,
    prov: Option<&Arc<dyn ProvenanceStore>>,
    transform: impl Fn(&mut Vec<Value>) -> Result<(), FerroError>,
) -> Result<usize, FerroError> {
    let heap = HeapFileManager::open(dir_root, bp.clone());

    // Pass 1: read everything. `(rid, header bytes, values)`.
    let mut rows: Vec<(RecordId, [u8; VERSION_HEADER_SIZE], Vec<Value>)> = Vec::new();
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
        rows.push((rid, header, tuple.deserialize(old)?));
    }

    // Pass 2: write everything.
    let primary = BPlusTreeManager::<Value, RecordId>::open(primary_root, bp.clone());
    let mut rewritten = 0usize;
    for (rid, header, mut values) in rows {
        let key = values.first().cloned();
        transform(&mut values)?;
        let mut tuple = Tuple::serialize(&values, new, 0)?;
        // begin_ts and end_ts exactly as they were; prev deliberately cleared.
        tuple.data[..16].copy_from_slice(&header[..16]);
        tuple.data[16..VERSION_HEADER_SIZE].fill(0);

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
            if let Some(store) = prov {
                let who = store.attribute(rid)?;
                if !who.is_none() {
                    store.stamp(new_rid, who)?;
                }
            }
        }
        rewritten += 1;
    }

    // A split during the repointing above can move the tree's root. `create_index` and
    // `sync_roots` both do this; a rewrite that forgot it would leave the catalog pointing at an
    // interior page and every lookup after the next restart would start from the wrong node.
    let root_now = primary.root_page_id.load(Ordering::Relaxed);
    if root_now != primary_root {
        return Err(FerroError::Internal(format!(
            "the primary index root moved from {primary_root} to {root_now} during an ALTER; \
             the catalog entry would be stale"
        )));
    }
    Ok(rewritten)
}

/// Rebuild one secondary index from the (already rewritten) heap, returning its new root page.
///
/// The same shape as `Catalog::create_index`'s build loop, against the new schema. The old tree's
/// pages are freed rather than orphaned.
fn rebuild_secondary(
    bp: &Arc<BufferPoolManager>,
    dir_root: u32,
    schema: &Schema,
    col_index: usize,
    old_root: u32,
) -> Result<u32, FerroError> {
    let tree = BPlusTreeManager::<(Value, Value), ()>::create(bp.clone())?;
    let heap = HeapFileManager::open(dir_root, bp.clone());
    for item in heap.scan() {
        let (_, tuple) = item?;
        let values = tuple.deserialize(schema)?;
        tree.insert((values[col_index].clone(), values[0].clone()), ())?;
    }
    BPlusTreeManager::<(Value, Value), ()>::open(old_root, bp.clone()).free_all()?;
    Ok(tree.root_page_id.load(Ordering::Relaxed))
}
