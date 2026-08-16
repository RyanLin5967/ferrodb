//! E9 — logical decoding: turning the WAL into row-level change events.
//!
//! Physical replication ships *pages*. It keeps a replica byte-identical and it cannot tell anyone
//! **what changed** — a consumer downstream of page images has to reimplement the storage engine to
//! find out. Logical decoding reads the same log and produces the other thing: `inventory` row
//! `(7, 70)` was inserted, `(7, 70)` became `(7, 999)`, `(3, 30)` was deleted.
//!
//! The substrate already carries everything needed. `HeapInsert`/`HeapDelete`/`HeapUpdate` hold
//! `dir_root` — which is a table's `first_directory_page_id`, so it identifies the table — plus the
//! tuple bytes, and updates carry both images. `Begin`/`Commit`/`Abort` give transaction
//! boundaries.
//!
//! # Two things the log says that a naive reading gets wrong
//!
//! Both were found by decoding what the executor actually writes rather than records built by
//! hand, and a decoder that misses either produces a feed that is confidently incorrect.
//!
//! **1. A SQL `DELETE` is an MVCC `HeapUpdate`, not a `HeapDelete`.** Deleting a row stamps the
//! live version's `end_ts` instead of removing it. A decoder that maps record kinds straight onto
//! change kinds therefore reports every delete as an update — and a consumer downstream keeps a row
//! that no longer exists, forever, with no later record to correct it. So the *new image's*
//! `end_ts` decides: zero means the row is still live and this is an `Update`; non-zero means the
//! version was killed and this is a `Delete`.
//!
//! **2. Half the records are internal MVCC traffic.** An update also writes the superseded version
//! into the table's separate `time_travel_root` heap. Those `HeapInsert`s are not user-visible
//! changes — emitting them would double-count every update as an insert as well — but they are not
//! errors either, so counting them as unresolved would be equally wrong. They are recognised and
//! counted in [`Decoded::internal`].
//!
//! # The property that makes a feed usable
//!
//! **Only committed transactions are emitted, and they are emitted in commit order.**
//!
//! This is the whole difference between a change feed and a pile of writes. A consumer that is
//! shown an aborted transaction's rows has been told about data that never existed and cannot be
//! un-told; a consumer shown an in-flight transaction's rows will act on a decision the database
//! has not made. So changes are buffered per transaction and released only when a `Commit` record
//! is reached: an `Abort` discards the buffer, and anything still open when the scan ends is
//! **reported rather than emitted**.
//!
//! Commit order is the log's order of `Commit` records, which is the order the database itself
//! made those transactions visible. Ordering by when a change was *written* would interleave
//! concurrent transactions and hand a consumer states no reader of this database ever saw.
//!
//! # Nothing is dropped silently
//!
//! A `dir_root` with no table in the catalog cannot be decoded — the schema is what turns bytes
//! into values, and a dropped or not-yet-created table has none. Guessing would be worse than
//! failing, and skipping quietly would be worst of all, because a CDC feed that loses changes
//! without saying so is indistinguishable from one that had nothing to report. So they are counted
//! and returned in [`Decoded::unresolved`], alongside the aborted and still-open transaction ids.
//! A caller that ignores those fields has chosen to; it has not been misled into it.
//!
//! # What this deliberately is not
//!
//! - **Not a wire format.** This produces values in memory. Serialising them for a downstream
//!   consumer is a separate concern with its own compatibility rules.
//! - **Not shippable without the catalog.** Decoding needs the schema, and the catalog lives
//!   outside the WAL — the same boundary physical replication already has. A decoder runs where a
//!   catalog is, which in practice means on the primary or on a replica that has been given one.
//! - **DDL is not decoded.** `CREATE`/`DROP TABLE` change the catalog rather than the heap, so a
//!   feed sees the rows and not the shape change that preceded them.
//! - **No snapshot.** This decodes a log range. A consumer that needs the state *before* that range
//!   needs a base backup, exactly as a replica does.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::catalog::catalog::Catalog;
use crate::catalog::column::{Column, DataType, Value};
use crate::catalog::schema::Schema;
use crate::error::FerroError;
use crate::storage::tuple::Tuple;
use crate::wal::log::{DdlOp, RecKind, WalManager};

/// What happened to one row.
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeOp {
    /// A row observed by an initial snapshot: it exists, and this is not news of a change.
    ///
    /// Distinct from `Insert` on purpose. Replaying a snapshot as inserts would make every
    /// pre-existing row look like fresh activity, and anything downstream that counts events would
    /// be wrong by the size of the table.
    Read { row: Vec<Value> },
    /// A schema change, carried in-band and in log order.
    ///
    /// In-band matters: a consumer that learns about a new column out of band has to guess when to
    /// apply it, and every guess is wrong for some row. Arriving in the stream at the position the
    /// DDL actually occupied means the events before it describe the old shape and the ones after
    /// describe the new one, with no ambiguity to resolve.
    Schema { change: SchemaChange, columns: Vec<ColumnSpec> },
    Insert { new: Vec<Value> },
    Update { old: Vec<Value>, new: Vec<Value> },
    Delete { old: Vec<Value> },
}

impl ChangeOp {
    pub fn name(&self) -> &'static str {
        match self {
            ChangeOp::Read { .. } => "READ",
            ChangeOp::Schema { change, .. } => match change {
                SchemaChange::CreateTable => "CREATE_TABLE",
                SchemaChange::DropTable => "DROP_TABLE",
            },
            ChangeOp::Insert { .. } => "INSERT",
            ChangeOp::Update { .. } => "UPDATE",
            ChangeOp::Delete { .. } => "DELETE",
        }
    }
}

/// Which schema change a `Schema` event describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaChange {
    CreateTable,
    DropTable,
}

/// One column, as the feed describes it to a consumer that must recreate the table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: String,
    /// `INTEGER`, `FLOAT`, `BOOLEAN`, or `VARCHAR(n)` — the width is part of the type, so a
    /// consumer recreating the column gets the same one.
    pub sql_type: String,
    pub nullable: bool,
}

/// One row-level change, attributed to the transaction that committed it.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeEvent {
    pub txn_id: u64,
    /// LSN of the record that produced this change — where it sits in the log.
    pub lsn: u64,
    /// LSN of the `Commit` that made it visible. Events are ordered by this.
    pub commit_lsn: u64,
    /// The LSN immediately **after** that commit record — where a consumer resumes.
    ///
    /// This exists because `commit_lsn + 1` is not a position: LSNs are byte offsets and records
    /// are variable length, so resuming one byte past a commit lands inside it and the walk dies
    /// with `eof before finished record`. A consumer that has processed this event stores
    /// `commit_end_lsn` and passes it as the next `from_lsn`. Found by a test that split a range at
    /// `commit_lsn + 1` and got exactly that error.
    pub commit_end_lsn: u64,
    pub table: String,
    /// Column names, positionally matching the values in `op`.
    ///
    /// A feed of positional values is unusable outside the process that produced it: the consumer
    /// would need this database's catalog to know what column three is. Shared via `Arc` because
    /// the names are per-table, not per-row, and a busy table produces a great many rows.
    pub columns: Arc<Vec<String>>,
    pub op: ChangeOp,
}

/// The result of a decode, including what could **not** be decoded.
///
/// The three report fields exist so that "no events" can be told apart from "events were dropped".
/// They are part of the answer, not diagnostics.
#[derive(Debug, Default)]
pub struct Decoded {
    /// Committed changes, in commit order.
    pub events: Vec<ChangeEvent>,
    /// `dir_root`s with no table in the catalog, and how many records each swallowed.
    pub unresolved: BTreeMap<u32, usize>,
    /// `dir_root`s that ARE known but whose tuple bytes would not deserialize against the table's
    /// schema, and how many records each swallowed.
    ///
    /// Split from `unresolved` because they mean opposite things and the first version of this
    /// conflated them: an unknown table is a decoder that has not been told about a table, and an
    /// undecodable tuple is a record whose bytes do not match the schema it is supposed to match.
    /// One is configuration; the other is a bug or a record that is not a row at all. Reporting
    /// both as "unresolved" sent me looking for the wrong thing.
    pub undecodable: BTreeMap<u32, usize>,
    /// Schema changes seen, in log order: `(lsn, table, change)`.
    pub schema_changes: Vec<(u64, String, SchemaChange)>,
    /// Records that belong to a table's time-travel heap: superseded versions archived by MVCC.
    /// Correctly not emitted, and correctly not an error.
    pub internal: usize,
    /// Transactions that rolled back. Their changes are correctly absent.
    pub aborted: BTreeSet<u64>,
    /// Transactions still open when the scan ended. Their changes are **withheld, not lost** — a
    /// later decode covering their commit will emit them.
    pub open: BTreeSet<u64>,
}

impl Decoded {
    /// True when something was seen that did not become an event, for any reason.
    pub fn is_complete(&self) -> bool {
        self.unresolved.is_empty() && self.undecodable.is_empty() && self.open.is_empty()
    }
}

/// Why a record did or did not become a row.
enum RowResult {
    Row(String, Arc<Vec<String>>, Vec<Value>),
    /// No table in the catalog has this `dir_root`.
    UnknownTable,
    /// The table is known and the bytes do not match its schema.
    Undecodable,
}

/// Reads a WAL range and produces committed row-level changes.
pub struct LogicalDecoder {
    /// `dir_root` -> (table name, schema, column names).
    tables: HashMap<u32, (String, Schema, Arc<Vec<String>>)>,
    /// Time-travel heap roots. Records against these are MVCC's own bookkeeping.
    time_travel: BTreeSet<u32>,
}

impl LogicalDecoder {
    /// Build a decoder from a catalog snapshot.
    ///
    /// The mapping is captured once. A table created *after* this is built decodes as unresolved
    /// rather than silently as some other table, which is the safe direction: `dir_root`s are page
    /// ids and a dropped table's id can be handed out again.
    pub fn new(catalog: &Catalog) -> Self {
        let mut tables = HashMap::new();
        let mut time_travel = BTreeSet::new();
        for (name, entry) in &catalog.tables {
            let columns: Vec<String> =
                entry.schema.columns.iter().map(|c| c.name.clone()).collect();
            tables.insert(
                entry.first_directory_page_id,
                (name.clone(), entry.schema.clone(), Arc::new(columns)),
            );
            time_travel.insert(entry.time_travel_root);
        }
        LogicalDecoder { tables, time_travel }
    }

    /// Build a decoder for a single table, without a catalog.
    ///
    /// A catalog is the normal source of the mapping, but it is not the only possible one: a
    /// consumer that already knows a table's shape — because it was told out of band, or because it
    /// is decoding an archived log whose catalog is long gone — has the same need. Column names are
    /// derived from the schema rather than passed separately, so they cannot drift out of step with
    /// the columns they name.
    pub fn for_table(
        dir_root: u32,
        name: &str,
        schema: Schema,
        time_travel_root: u32,
    ) -> Self {
        let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let mut tables = HashMap::new();
        tables.insert(dir_root, (name.to_string(), schema, Arc::new(columns)));
        LogicalDecoder { tables, time_travel: BTreeSet::from([time_travel_root]) }
    }

    /// A decoder that knows nothing at all.
    ///
    /// Useful on its own, because the log now carries `CREATE TABLE`: a blank decoder walking a
    /// range that begins with the DDL learns the tables as it goes and decodes the rows that follow.
    /// That is the difference between a feed that needs a catalog handed to it out of band and one
    /// that is self-describing.
    pub fn blank() -> Self {
        LogicalDecoder { tables: HashMap::new(), time_travel: BTreeSet::new() }
    }

    /// Number of tables this decoder can resolve. A decoder that knows no tables would report every
    /// record as unresolved, which is a configuration mistake rather than an empty database.
    pub fn known_tables(&self) -> usize {
        self.tables.len()
    }

    /// Whether a tuple's version has been killed. Non-zero `end_ts` means a later transaction
    /// ended this version, which for the LIVE row is what a `DELETE` looks like.
    fn is_dead(bytes: &[u8]) -> bool {
        Tuple { data: bytes.to_vec() }
            .version_header()
            .map(|h| h.end_ts != 0)
            .unwrap_or(false)
    }

    fn row_in(
        tables: &HashMap<u32, (String, Schema, Arc<Vec<String>>)>,
        dir_root: u32,
        bytes: &[u8],
    ) -> RowResult {
        let Some((name, schema, columns)) = tables.get(&dir_root) else {
            return RowResult::UnknownTable;
        };
        let tuple = Tuple { data: bytes.to_vec() };
        match tuple.deserialize(schema) {
            Ok(v) => RowResult::Row(name.clone(), Arc::clone(columns), v),
            // Reported as its own category rather than turned into a plausible-looking row.
            Err(_) => RowResult::Undecodable,
        }
    }

    /// Decode `[from_lsn, to_lsn)`.
    ///
    /// Walks once, buffering per transaction and releasing on commit.
    pub fn decode(
        &self,
        wal: &WalManager,
        from_lsn: u64,
        to_lsn: u64,
    ) -> Result<Decoded, FerroError> {
        let mut out = Decoded::default();

        // The constructor's mapping is a STARTING POINT, not the truth for the whole range. DDL in
        // the log evolves it as the walk proceeds, so records are decoded against the schema that
        // was in force where they sit rather than against whatever the catalog looks like now.
        // Without this, decoding any history that contains a `CREATE TABLE` requires a catalog from
        // the future, and a `DROP TABLE` makes the past undecodable entirely.
        let mut tables = self.tables.clone();
        let mut time_travel = self.time_travel.clone();

        // txn_id -> changes staged so far, in the order they were written.
        let mut staged: HashMap<u64, Vec<(u64, String, Arc<Vec<String>>, ChangeOp)>> = HashMap::new();

        let mut lsn = from_lsn;
        while lsn < to_lsn {
            let (rec, next) = wal.read_record(lsn)?;
            let txn = rec.txn_id;

            // MVCC bookkeeping: the superseded version being archived. Not a change, not an error.
            let internal_root = match &rec.kind {
                RecKind::HeapInsert { dir_root, .. }
                | RecKind::HeapDelete { dir_root, .. }
                | RecKind::HeapUpdate { dir_root, .. } => {
                    time_travel.contains(dir_root) && !tables.contains_key(dir_root)
                }
                _ => false,
            };
            if internal_root {
                out.internal += 1;
                if next <= lsn {
                    return Err(FerroError::Wal(format!(
                        "log walk did not advance at lsn {lsn}; refusing to loop forever"
                    )));
                }
                lsn = next;
                continue;
            }

            match &rec.kind {
                RecKind::HeapInsert { dir_root, tuple, .. } => {
                    match Self::row_in(&tables, *dir_root, tuple) {
                        RowResult::Row(table, columns, new) => staged
                            .entry(txn)
                            .or_default()
                            .push((lsn, table, columns, ChangeOp::Insert { new })),
                        RowResult::UnknownTable => {
                            *out.unresolved.entry(*dir_root).or_insert(0) += 1
                        }
                        RowResult::Undecodable => {
                            *out.undecodable.entry(*dir_root).or_insert(0) += 1
                        }
                    }
                }
                RecKind::HeapDelete { dir_root, old, .. } => match Self::row_in(&tables, *dir_root, old) {
                    RowResult::Row(table, columns, old) => staged
                        .entry(txn)
                        .or_default()
                        .push((lsn, table, columns, ChangeOp::Delete { old })),
                    RowResult::UnknownTable => *out.unresolved.entry(*dir_root).or_insert(0) += 1,
                    RowResult::Undecodable => *out.undecodable.entry(*dir_root).or_insert(0) += 1,
                },
                RecKind::HeapUpdate { dir_root, old, new, .. } => {
                    // Decided BEFORE the images are turned into values, because it is a property of
                    // the version header rather than of the columns.
                    let killed = Self::is_dead(new);
                    match (Self::row_in(&tables, *dir_root, old), Self::row_in(&tables, *dir_root, new)) {
                        (RowResult::Row(table, columns, old), RowResult::Row(_, _, new)) => {
                            let op = if killed {
                                // A SQL DELETE. Reporting it as an update would leave a consumer
                                // holding a row the database no longer has.
                                ChangeOp::Delete { old }
                            } else {
                                ChangeOp::Update { old, new }
                            };
                            staged.entry(txn).or_default().push((lsn, table, columns, op))
                        }
                        // Half a decoded update is not an update. Both images or neither.
                        (RowResult::UnknownTable, _) | (_, RowResult::UnknownTable) => {
                            *out.unresolved.entry(*dir_root).or_insert(0) += 1
                        }
                        _ => *out.undecodable.entry(*dir_root).or_insert(0) += 1,
                    }
                }
                RecKind::Commit => {
                    // Release, stamped with this commit's LSN. Ordering by commit is what gives a
                    // consumer the sequence the database itself made visible.
                    if let Some(changes) = staged.remove(&txn) {
                        for (change_lsn, table, columns, op) in changes {
                            out.events.push(ChangeEvent {
                                txn_id: txn,
                                lsn: change_lsn,
                                commit_lsn: lsn,
                                commit_end_lsn: next,
                                table,
                                columns,
                                op,
                            });
                        }
                    }
                }
                RecKind::Abort => {
                    // Rolled back: the rows never existed, so nothing is emitted.
                    staged.remove(&txn);
                    out.aborted.insert(txn);
                }
                RecKind::Ddl { op, table, dir_root, time_travel_root, columns } => {
                    let specs: Vec<ColumnSpec> = columns
                        .iter()
                        .map(|(name, ty, nullable)| ColumnSpec {
                            name: name.clone(),
                            sql_type: match ty {
                                DataType::Integer => "INTEGER".to_string(),
                                DataType::Float => "FLOAT".to_string(),
                                DataType::Boolean => "BOOLEAN".to_string(),
                                DataType::Varchar(n) => format!("VARCHAR({n})"),
                            },
                            nullable: *nullable,
                        })
                        .collect();

                    let change = match op {
                        DdlOp::CreateTable => {
                            let schema = Schema::new(
                                columns
                                    .iter()
                                    .map(|(name, ty, nullable)| Column {
                                        name: name.clone(),
                                        data_type: ty.clone(),
                                        nullable: *nullable,
                                    })
                                    .collect(),
                            );
                            let names: Vec<String> =
                                columns.iter().map(|(n, _, _)| n.clone()).collect();
                            tables.insert(
                                *dir_root,
                                (table.clone(), schema, Arc::new(names)),
                            );
                            time_travel.insert(*time_travel_root);
                            SchemaChange::CreateTable
                        }
                        DdlOp::DropTable => {
                            tables.remove(dir_root);
                            SchemaChange::DropTable
                        }
                    };
                    out.schema_changes.push((lsn, table.clone(), change));

                    // Emitted immediately rather than staged: DDL is refused inside a transaction
                    // (`executor.rs`, "DDL not allowed in txn"), so there is no commit to wait for
                    // and holding it back would place it after changes that actually followed it.
                    out.events.push(ChangeEvent {
                        txn_id: txn,
                        lsn,
                        commit_lsn: lsn,
                        commit_end_lsn: next,
                        table: table.clone(),
                        columns: Arc::new(
                            columns.iter().map(|(n, _, _)| n.clone()).collect::<Vec<_>>(),
                        ),
                        op: ChangeOp::Schema { change, columns: specs },
                    });
                }
                // `Clr` records are undo work, and undo only happens on the way to an `Abort`,
                // whose buffer is discarded whole. `Begin`, `TxnEnd` and `Checkpoint` carry no row
                // data. None of them produce events.
                RecKind::Begin | RecKind::TxnEnd | RecKind::Checkpoint | RecKind::Clr { .. } => {}
            }

            if next <= lsn {
                return Err(FerroError::Wal(format!(
                    "log walk did not advance at lsn {lsn}; refusing to loop forever"
                )));
            }
            lsn = next;
        }

        // Whatever is still staged belongs to transactions this range did not see commit. Withheld,
        // and said so.
        out.open = staged.keys().copied().collect();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::{Column, DataType};

    fn schema() -> Schema {
        Schema::new(vec![
            Column { name: "id".into(), data_type: DataType::Integer, nullable: false },
            Column { name: "qty".into(), data_type: DataType::Integer, nullable: true },
        ])
    }

    /// A decoder wired to one table at `dir_root` 7, without needing a real catalog.
    fn decoder() -> LogicalDecoder {
        let mut tables = HashMap::new();
        tables.insert(
            7u32,
            (
                "inventory".to_string(),
                schema(),
                Arc::new(vec!["id".to_string(), "qty".to_string()]),
            ),
        );
        // dir_root 8 is the table's time-travel heap: MVCC's own archive of superseded versions.
        LogicalDecoder { tables, time_travel: BTreeSet::from([8u32]) }
    }

    fn tuple_bytes(id: i32, qty: Option<i32>) -> Vec<u8> {
        let vals = vec![
            Value::Integer(id),
            qty.map(Value::Integer).unwrap_or(Value::Null),
        ];
        // begin_ts 0: the MVCC version header is skipped by `deserialize`, so its value is
        // irrelevant to what a change event reports.
        Tuple::serialize(&vals, &schema(), 0).expect("serialize").data
    }

    /// A tuple whose version has been ended — what the live row looks like after a SQL DELETE.
    fn dead_tuple_bytes(id: i32, qty: i32) -> Vec<u8> {
        let mut b = tuple_bytes(id, Some(qty));
        // end_ts occupies bytes 8..16 of the version header; non-zero means this version was
        // ended by some transaction.
        b[8..16].copy_from_slice(&42u64.to_be_bytes());
        b
    }

    fn wal(tag: &str) -> (tempfile::TempDir, WalManager) {
        let d = tempfile::tempdir().unwrap();
        let w = WalManager::new(d.path().join(format!("{tag}.wal"))).unwrap();
        (d, w)
    }

    fn insert(w: &WalManager, txn: u64, id: i32, qty: i32) {
        w.append(
            txn,
            0,
            &RecKind::HeapInsert { dir_root: 7, page_id: 1, slot: 0, tuple: tuple_bytes(id, Some(qty)) },
        )
        .unwrap();
    }

    fn decode_all(d: &LogicalDecoder, w: &WalManager) -> Decoded {
        use std::sync::atomic::Ordering;
        w.flush().unwrap();
        let base = w.base_lsn.load(Ordering::SeqCst);
        let end = w.next_lsn.load(Ordering::SeqCst);
        d.decode(w, base, end).unwrap()
    }

    #[test]
    fn a_committed_insert_becomes_a_typed_row_event() {
        let (_d, w) = wal("insert");
        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 7, 70);
        w.append(1, 0, &RecKind::Commit).unwrap();

        let out = decode_all(&decoder(), &w);
        assert_eq!(out.events.len(), 1, "expected one event, got {:?}", out.events);
        let e = &out.events[0];
        assert_eq!(e.table, "inventory");
        assert_eq!(e.txn_id, 1);
        assert!(e.commit_lsn > e.lsn, "the commit must come after the change it releases");
        assert!(
            e.commit_end_lsn > e.commit_lsn,
            "the resume point must be past the commit record, not inside it"
        );
        assert_eq!(
            e.op,
            ChangeOp::Insert { new: vec![Value::Integer(7), Value::Integer(70)] },
            "the row did not decode to its actual column values"
        );
        assert!(out.is_complete(), "something was dropped: {out:?}");
    }

    /// **The property the whole module exists for.** An aborted transaction's rows never appear.
    #[test]
    fn an_aborted_transaction_emits_nothing() {
        let (_d, w) = wal("abort");
        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 1, 10);
        w.append(1, 0, &RecKind::Commit).unwrap();

        w.append(2, 0, &RecKind::Begin).unwrap();
        insert(&w, 2, 99, 990); // this row must never be seen by anyone
        w.append(2, 0, &RecKind::Abort).unwrap();

        let out = decode_all(&decoder(), &w);
        assert_eq!(out.events.len(), 1, "the aborted transaction leaked: {:?}", out.events);
        assert_eq!(out.events[0].txn_id, 1);
        assert!(out.aborted.contains(&2), "the abort was not reported");

        // Stronger, and the assertion that would catch a decoder emitting the row under the wrong
        // transaction id rather than not at all.
        for e in &out.events {
            if let ChangeOp::Insert { new } = &e.op {
                assert_ne!(
                    new[0],
                    Value::Integer(99),
                    "a row from an aborted transaction was emitted"
                );
            }
        }
    }

    /// A transaction with no commit in range is withheld and reported, not emitted and not lost.
    #[test]
    fn an_open_transaction_is_withheld_and_reported() {
        let (_d, w) = wal("open");
        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 5, 50);
        // no Commit

        let out = decode_all(&decoder(), &w);
        assert!(out.events.is_empty(), "an uncommitted change was emitted: {:?}", out.events);
        assert!(out.open.contains(&1), "the open transaction was not reported");
        assert!(!out.is_complete(), "a withheld transaction must not report as complete");
    }

    /// Events come out in **commit** order, not write order. Two interleaved transactions where the
    /// second to write is the first to commit is the case that tells them apart.
    #[test]
    fn events_are_ordered_by_commit_not_by_write() {
        let (_d, w) = wal("order");
        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 1, 10); // written first
        w.append(2, 0, &RecKind::Begin).unwrap();
        insert(&w, 2, 2, 20); // written second
        w.append(2, 0, &RecKind::Commit).unwrap(); // committed FIRST
        w.append(1, 0, &RecKind::Commit).unwrap(); // committed second

        let out = decode_all(&decoder(), &w);
        assert_eq!(out.events.len(), 2, "{:?}", out.events);
        assert_eq!(
            (out.events[0].txn_id, out.events[1].txn_id),
            (2, 1),
            "events came out in write order; a consumer would see states no reader ever saw"
        );
        assert!(
            out.events[0].commit_lsn < out.events[1].commit_lsn,
            "commit lsns are not increasing"
        );
    }

    /// Updates carry both images, which is what lets a consumer compute a diff or key a change.
    #[test]
    fn an_update_carries_the_row_before_and_after() {
        let (_d, w) = wal("update");
        w.append(1, 0, &RecKind::Begin).unwrap();
        w.append(
            1,
            0,
            &RecKind::HeapUpdate {
                dir_root: 7,
                page_id: 1,
                slot: 0,
                old: tuple_bytes(7, Some(70)),
                new: tuple_bytes(7, Some(999)),
            },
        )
        .unwrap();
        w.append(1, 0, &RecKind::Commit).unwrap();

        let out = decode_all(&decoder(), &w);
        assert_eq!(out.events.len(), 1);
        assert_eq!(
            out.events[0].op,
            ChangeOp::Update {
                old: vec![Value::Integer(7), Value::Integer(70)],
                new: vec![Value::Integer(7), Value::Integer(999)],
            }
        );
    }

    /// **A SQL DELETE arrives as a `HeapUpdate`, and must not be reported as an update.**
    ///
    /// MVCC deletes by stamping the live version's `end_ts` rather than removing it. A decoder that
    /// maps record kinds onto change kinds reports this as an update, and a consumer downstream
    /// keeps a row the database no longer has, with no later record to correct it. Found by
    /// decoding what the executor actually writes.
    #[test]
    fn an_mvcc_delete_is_reported_as_a_delete_not_an_update() {
        let (_d, w) = wal("mvcc_delete");
        w.append(1, 0, &RecKind::Begin).unwrap();
        w.append(
            1,
            0,
            &RecKind::HeapUpdate {
                dir_root: 7,
                page_id: 1,
                slot: 0,
                old: tuple_bytes(3, Some(30)),
                new: dead_tuple_bytes(3, 30),
            },
        )
        .unwrap();
        w.append(1, 0, &RecKind::Commit).unwrap();

        let out = decode_all(&decoder(), &w);
        assert_eq!(out.events.len(), 1);
        assert_eq!(
            out.events[0].op,
            ChangeOp::Delete { old: vec![Value::Integer(3), Value::Integer(30)] },
            "a killed version was reported as {} rather than a delete",
            out.events[0].op.name()
        );

        // And a live update must still be an update, or the rule above would just relabel
        // everything as a delete.
        let (_d2, w2) = wal("mvcc_update");
        w2.append(1, 0, &RecKind::Begin).unwrap();
        w2.append(
            1,
            0,
            &RecKind::HeapUpdate {
                dir_root: 7,
                page_id: 1,
                slot: 0,
                old: tuple_bytes(3, Some(30)),
                new: tuple_bytes(3, Some(31)),
            },
        )
        .unwrap();
        w2.append(1, 0, &RecKind::Commit).unwrap();
        let out2 = decode_all(&decoder(), &w2);
        assert_eq!(out2.events[0].op.name(), "UPDATE", "a live update was relabelled");
    }

    /// Records against a table's time-travel heap are MVCC bookkeeping: not emitted, and not an
    /// error either. Counting them as unresolved would report normal operation as data loss.
    #[test]
    fn time_travel_records_are_counted_as_internal_not_as_lost() {
        let (_d, w) = wal("internal");
        w.append(1, 0, &RecKind::Begin).unwrap();
        // The archived old version goes to the time-travel root, not the table.
        w.append(
            1,
            0,
            &RecKind::HeapInsert { dir_root: 8, page_id: 9, slot: 0, tuple: tuple_bytes(3, Some(30)) },
        )
        .unwrap();
        insert(&w, 1, 4, 40);
        w.append(1, 0, &RecKind::Commit).unwrap();

        let out = decode_all(&decoder(), &w);
        assert_eq!(out.events.len(), 1, "the archived version leaked into the feed: {:?}", out.events);
        assert_eq!(out.internal, 1, "the time-travel record was not counted as internal");
        assert!(out.unresolved.is_empty(), "MVCC bookkeeping was reported as an unknown table");
        assert!(out.is_complete(), "normal MVCC traffic made the decode look incomplete: {out:?}");
    }

    /// A NULL must survive decoding as a NULL, not as a zero.
    #[test]
    fn a_null_column_decodes_as_null() {
        let (_d, w) = wal("null");
        w.append(1, 0, &RecKind::Begin).unwrap();
        w.append(
            1,
            0,
            &RecKind::HeapInsert { dir_root: 7, page_id: 1, slot: 0, tuple: tuple_bytes(4, None) },
        )
        .unwrap();
        w.append(1, 0, &RecKind::Commit).unwrap();

        let out = decode_all(&decoder(), &w);
        assert_eq!(
            out.events[0].op,
            ChangeOp::Insert { new: vec![Value::Integer(4), Value::Null] },
            "a NULL was not preserved through decoding"
        );
    }

    /// **Unknown tables are counted, not skipped.** A feed that loses changes without saying so is
    /// indistinguishable from one that had nothing to report.
    #[test]
    fn a_change_to_an_unknown_table_is_reported_rather_than_dropped() {
        let (_d, w) = wal("unknown");
        w.append(1, 0, &RecKind::Begin).unwrap();
        w.append(
            1,
            0,
            &RecKind::HeapInsert { dir_root: 404, page_id: 1, slot: 0, tuple: tuple_bytes(1, Some(1)) },
        )
        .unwrap();
        insert(&w, 1, 2, 20);
        w.append(1, 0, &RecKind::Commit).unwrap();

        let out = decode_all(&decoder(), &w);
        assert_eq!(out.events.len(), 1, "the known table's row should still decode");
        assert_eq!(out.unresolved.get(&404), Some(&1), "the unknown table was not reported");
        assert!(!out.is_complete(), "a decode that dropped a record must not report as complete");
    }

    /// A decoder that knows no tables is a configuration mistake, and is distinguishable from an
    /// empty log rather than looking like one.
    #[test]
    fn a_decoder_with_no_tables_reports_everything_as_unresolved() {
        let (_d, w) = wal("empty");
        let d = LogicalDecoder { tables: HashMap::new(), time_travel: BTreeSet::new() };
        assert_eq!(d.known_tables(), 0);

        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 1, 10);
        w.append(1, 0, &RecKind::Commit).unwrap();

        let out = decode_all(&d, &w);
        assert!(out.events.is_empty());
        assert_eq!(out.unresolved.get(&7), Some(&1), "the record vanished without being counted");
    }
}
