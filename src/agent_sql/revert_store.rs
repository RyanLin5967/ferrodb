//! **D212 — the durable substrate `REVERT MERGE` reads.**
//!
//! Design authority: `SCALE-DESIGN.md` "D212 — REVERT metadata survives a restart", and
//! `frontier/d212_design.md` §0, §3a and §6 in `artie-research`.
//!
//! Everything here lives in ordinary heap tables whose names start with
//! [`crate::catalog::catalog::INTERNAL_TABLE_PREFIX`]. That is a decision about atomicity, not
//! convenience: a heap table is the only structure that commits in the SAME WAL transaction as the
//! rows a merge publishes, so a crash can never leave a merge's rows without its history or its
//! history without its rows. The two alternatives `d212_design.md` §3a priced were rejected for
//! that reason — a WAL record kind is re-declared at every checkpoint (Θ(M²) over a database's
//! life), and a sidecar file cannot be ordered against the commit in either direction.
//!
//! # Step 0 (D217): the merge-id ceiling
//!
//! Merge ids are minted for every admission attempt — a conflicting or quarantined `MERGE` and a
//! merge into a sibling branch all get one — and the id is handed to the client whether or not
//! anything was published to the shared tables. So a counter persisted only when a merge PUBLISHES
//! cannot stop a restarted server from re-issuing an id some client is still holding. The runtime
//! therefore reserves ids in blocks of [`MERGE_ID_BLOCK`]: before it mints an id above the durable
//! ceiling it commits a new ceiling, and a restarted runtime starts minting above whatever ceiling
//! it finds. An id at or below the ceiling a runtime found when it started is a POSITIVE fact about
//! an earlier server run rather than an absence from a map.
//!
//! # Option (a): the five structures, written with the merge
//!
//! `d212_design.md` §0.2 showed REVERT reads five structures, and that persisting fewer is worse
//! than persisting none: a capture beside a reset version clock under-reports dependents. Each is
//! here, and each is written inside the publish transaction:
//!
//! | structure REVERT reads | where it lives | loaded |
//! |---|---|---|
//! | `merges` | [`MERGES_TABLE`], one row per published merge | by key, for a pre-restart target |
//! | `captures` | [`LOG_TABLE`], one `CAPTURE` record per txn per publish | from the target forward |
//! | `applied` | [`LOG_TABLE`], one `APPLIED` record per publish | from the target forward |
//! | `versions` | [`VERSIONS_TABLE`], one row per row ever published | all of it, at attach |
//! | `apply_seq` | [`Meta::apply_seq`] | at attach |
//!
//! plus the counters `next_txn` ([`Meta::txn_high`]) and `next_merge` ([`Meta::merge_ceiling`]),
//! and the REVERT markers D218 needs durable (`REVERTED` records, written inside the revert's own
//! transaction).
//!
//! **Lazily.** Only the counters and `versions` are read when a runtime attaches: `versions` is
//! consulted under the state lock for every row a read returns, so it has to be resident, and it is
//! bounded by rows ever published rather than by merges. `applied` and the captures are read only
//! when a `REVERT` targets a merge an earlier server run published, and only from that merge
//! forward — so walls #12 and #17 stay per process, and #18 becomes per database only for those
//! reverts (`d212_design.md` §2).
//!
//! **Retained for a window.** The log keeps the history of the last `W` published merges (the
//! runtime's `revert_retention`, [`DEFAULT_RETENTION_MERGES`] unless
//! `FERRODB_REVERT_RETENTION_MERGES` says otherwise). Pruning by publish order is sound: a
//! dependent of merge `m` read after `m` published, so it was published after `m`, and nothing
//! written before the window's oldest merge can be a dependent of a merge inside it. A REVERT older
//! than the window is refused, and the refusal names `W`. ⚠ The engine has no VACUUM: a pruned row
//! is a tombstone whose bytes the heap keeps, so `W` bounds what a REVERT reads, not the file.

use std::collections::BTreeMap;

use crate::agent_sql::runtime::{scan_table, scan_table_where, AppliedOp, ReadCtx};
use crate::branch::types::BranchId;
use crate::catalog::column::{Column, DataType, Value};
use crate::catalog::schema::Schema;
use crate::error::FerroError;
use crate::parser::parser::Expr;
use crate::parser::scanner::TokenType;
use crate::provenance::capture::{TimedPredicate, TxnProvenance, WriteRecord};
use crate::provenance::readset::{Bound, PredicateSummary, ReadSet, VersionRef};
use crate::provenance::ProvId;
use crate::storage::heap_file_manager::RecordId;
use crate::tel::ids::{ColId, RowId, TableId, TxnId};
use crate::tel::log::{
    put_branch, put_op, put_opt_value, put_value, take_branch, take_op, take_opt_value, take_value,
};
use crate::tel::op::Op;
use crate::wal::log::{take_str, take_u16, take_u32, take_u64, take_u8, write_str};

/// The one-row table of counters. See [`Meta`].
pub(crate) const META_TABLE: &str = "ferro:revert:meta";
/// One row per logical row any merge ever published: the version a later read of it is stamped
/// with. Bounded by rows, not by merges, and never pruned.
pub(crate) const VERSIONS_TABLE: &str = "ferro:revert:versions";
/// One row per published merge still inside the retention window.
pub(crate) const MERGES_TABLE: &str = "ferro:revert:merges";
/// The history REVERT replays: applied ops, captures and revert markers, as chunked records.
pub(crate) const LOG_TABLE: &str = "ferro:revert:log";

/// How many merge ids one durable reservation covers.
///
/// The cost trade is one committed transaction per `MERGE_ID_BLOCK` merge attempts, against ids
/// jumping by up to this much across a restart. A jump is harmless — ids are names, not a count —
/// and 64 keeps the reservation to ~1.6% of attempts while keeping post-restart ids short.
pub(crate) const MERGE_ID_BLOCK: u64 = 64;

/// Published merges a `REVERT` can reach when nothing configures it.
///
/// A count rather than a duration because a count is what the cost is linear in — a REVERT of the
/// oldest retained merge replays that many merges' captures — and because a test can pin it
/// without a clock. Operators who think in time get the count their merge rate implies.
pub(crate) const DEFAULT_RETENTION_MERGES: u64 = 1024;

/// How many hex characters one log row carries.
///
/// A record is hex so it fits a `VARCHAR` (the engine has no byte type), and chunked because a
/// tuple must fit a 4 KiB page: 2048 characters is 1 KiB of record per row with room for the other
/// columns and the tuple header. Most records — one op, one small capture — are one row.
pub(crate) const CHUNK_HEX: usize = 2048;

/// The durable counters, as one row.
///
/// Declared at full width from Step 0 on, so the table's shape is fixed from its first commit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Meta {
    /// Every merge id `m_1 ..= m_{merge_ceiling}` has been issued, or reserved, by some run.
    pub(crate) merge_ceiling: u64,
    /// The highest agent txn id any persisted record names. A restarted runtime mints txn ids above
    /// it, so no post-restart task can take a pre-restart task's capture key.
    pub(crate) txn_high: u64,
    /// The runtime's version clock as of the last publish.
    pub(crate) apply_seq: u64,
    /// The highest key the history log has used.
    pub(crate) log_high: u64,
    /// The smallest merge number whose history is still retained; 0 while nothing was pruned.
    pub(crate) retained_from: u64,
    /// How many published merges the history currently retains.
    pub(crate) retained_count: u64,
}

impl Meta {
    /// The primary key of the one row. Fixed, so an `UPDATE` can name it.
    pub(crate) const KEY: i32 = 1;

    const COLUMNS: [&'static str; 7] = [
        "k",
        "merge_ceiling",
        "txn_high",
        "apply_seq",
        "log_high",
        "retained_from",
        "retained_count",
    ];

    pub(crate) fn schema() -> Schema {
        let mut columns = vec![Column::new(Self::COLUMNS[0].to_string(), DataType::Integer, false)];
        for name in &Self::COLUMNS[1..] {
            columns.push(Column::new(name.to_string(), DataType::BigInt, false));
        }
        Schema::new(columns)
    }

    pub(crate) fn to_row(&self) -> Result<Vec<Value>, FerroError> {
        Ok(vec![
            Value::Integer(Self::KEY),
            big(self.merge_ceiling, "merge_ceiling")?,
            big(self.txn_high, "txn_high")?,
            big(self.apply_seq, "apply_seq")?,
            big(self.log_high, "log_high")?,
            big(self.retained_from, "retained_from")?,
            big(self.retained_count, "retained_count")?,
        ])
    }

    pub(crate) fn from_row(row: &[Value]) -> Result<Meta, FerroError> {
        if row.len() != Self::COLUMNS.len() {
            return Err(FerroError::Corruption(format!(
                "{META_TABLE} has a row of {} cells where its shape has {}; refusing to read \
                 counters out of a row this build did not write",
                row.len(),
                Self::COLUMNS.len()
            )));
        }
        if row[0] != Value::Integer(Self::KEY) {
            return Err(FerroError::Corruption(format!(
                "{META_TABLE}'s row is keyed {:?}, not {}",
                row[0],
                Self::KEY
            )));
        }
        Ok(Meta {
            merge_ceiling: unbig(&row[1], "merge_ceiling")?,
            txn_high: unbig(&row[2], "txn_high")?,
            apply_seq: unbig(&row[3], "apply_seq")?,
            log_high: unbig(&row[4], "log_high")?,
            retained_from: unbig(&row[5], "retained_from")?,
            retained_count: unbig(&row[6], "retained_count")?,
        })
    }
}

/// Every internal table the runtime keeps, with its shape, in the order it creates them.
pub(crate) fn tables() -> Vec<(&'static str, Schema)> {
    vec![
        (META_TABLE, Meta::schema()),
        (VERSIONS_TABLE, versions_schema()),
        (MERGES_TABLE, merges_schema()),
        (LOG_TABLE, log_schema()),
    ]
}

/// The durable counters as committed, or `None` when there are none yet.
///
/// `None` covers two states on purpose: the table does not exist (a database no merge of THIS build
/// has touched), and the table exists without its row (a crash between creating it and the first
/// reservation's commit). Both mean "nothing was ever reserved", and in both the next write inserts
/// the row. ⚠ "Nothing reserved" is not "nothing issued" for a database written before D217: that
/// build issued ids and kept no record of them, so there is nothing here to start above (the blind
/// spots are listed on `AgentRuntime::attach_history`).
pub(crate) fn read_meta(ctx: &ReadCtx) -> Result<Option<Meta>, FerroError> {
    if ctx.catalog.get_table(META_TABLE).is_none() {
        return Ok(None);
    }
    let rows = scan_table(META_TABLE, ctx)?;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => Meta::from_row(row).map(Some),
        many => Err(FerroError::Corruption(format!(
            "{META_TABLE} holds {} rows; it is one row by construction, so the counters it carries \
             cannot be trusted and no merge id will be minted from them",
            many.len()
        ))),
    }
}

// ---- versions ------------------------------------------------------------------------------------

/// The shape of [`VERSIONS_TABLE`]. `k` is the `(table, row)` pair as fixed-width hex, because the
/// primary key is one column and the pair does not fit one integer.
pub(crate) fn versions_schema() -> Schema {
    Schema::new(vec![
        Column::new("k".to_string(), DataType::Varchar(25), false),
        Column::new("tbl".to_string(), DataType::BigInt, false),
        Column::new("row_id".to_string(), DataType::BigInt, false),
        Column::new("begin_ts".to_string(), DataType::BigInt, false),
    ])
}

/// The primary key of a row's version entry.
pub(crate) fn version_key(tbl: u32, row: u64) -> Value {
    Value::Varchar(format!("{tbl:08x}:{row:016x}"))
}

pub(crate) fn version_row(v: &VersionRef) -> Result<Vec<Value>, FerroError> {
    Ok(vec![
        version_key(v.tbl.0, v.row.0),
        Value::BigInt(v.tbl.0 as i64),
        // A `RowId` is a full-width hash for non-integer keys, so it is stored by its bits.
        Value::BigInt(v.row.0 as i64),
        big(v.begin_ts, "begin_ts")?,
    ])
}

/// Every row's published version, or nothing when the table does not exist yet.
pub(crate) fn read_versions(ctx: &ReadCtx) -> Result<Vec<VersionRef>, FerroError> {
    if ctx.catalog.get_table(VERSIONS_TABLE).is_none() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for row in scan_table(VERSIONS_TABLE, ctx)? {
        let (tbl, row_id, begin_ts) = match row.as_slice() {
            [_, Value::BigInt(t), Value::BigInt(r), ts] => (*t, *r, unbig(ts, "begin_ts")?),
            other => {
                return Err(FerroError::Corruption(format!(
                    "{VERSIONS_TABLE} holds a row this build did not write: {other:?}"
                )))
            }
        };
        let tbl = u32::try_from(tbl).map_err(|_| {
            FerroError::Corruption(format!("{VERSIONS_TABLE} names table id {tbl}, not a u32"))
        })?;
        out.push(VersionRef {
            tbl: TableId(tbl),
            row: RowId(row_id as u64),
            rid: RecordId { page_id: 0, slot_num: 0 },
            begin_ts,
        });
    }
    Ok(out)
}

// ---- merges --------------------------------------------------------------------------------------

/// One published merge, as REVERT needs to find it after a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoredMerge {
    /// The `n` of `m_n`.
    pub(crate) number: u64,
    pub(crate) txn: TxnId,
    pub(crate) branch: BranchId,
    /// The first log key this merge's records occupy; its dependents' records all sit above it.
    pub(crate) log_from: u64,
}

pub(crate) fn merges_schema() -> Schema {
    Schema::new(
        ["m", "txn", "branch", "generation", "log_from"]
            .iter()
            .map(|n| Column::new(n.to_string(), DataType::BigInt, false))
            .collect(),
    )
}

impl StoredMerge {
    pub(crate) fn to_row(&self) -> Result<Vec<Value>, FerroError> {
        Ok(vec![
            big(self.number, "merge number")?,
            big(self.txn.0, "merge txn")?,
            big(self.branch.id, "merge branch")?,
            Value::BigInt(self.branch.generation as i64),
            big(self.log_from, "merge log_from")?,
        ])
    }

    fn from_row(row: &[Value]) -> Result<StoredMerge, FerroError> {
        let [m, txn, branch, generation, log_from] = row else {
            return Err(FerroError::Corruption(format!(
                "{MERGES_TABLE} holds a row of {} cells where its shape has 5",
                row.len()
            )));
        };
        let generation = unbig(generation, "merge generation")?;
        Ok(StoredMerge {
            number: unbig(m, "merge number")?,
            txn: TxnId(unbig(txn, "merge txn")?),
            branch: BranchId::new(
                unbig(branch, "merge branch")?,
                u32::try_from(generation).map_err(|_| {
                    FerroError::Corruption(format!(
                        "{MERGES_TABLE} holds generation {generation}, not a u32"
                    ))
                })?,
            ),
            log_from: unbig(log_from, "merge log_from")?,
        })
    }
}

/// The published merge `m_{number}`, if the retained history still holds it.
pub(crate) fn read_merge(ctx: &ReadCtx, number: u64) -> Result<Option<StoredMerge>, FerroError> {
    if ctx.catalog.get_table(MERGES_TABLE).is_none() {
        return Ok(None);
    }
    let pred = compare("m", TokenType::Equal, number);
    let rows = scan_table_where(MERGES_TABLE, None, Some(&pred), ctx)?;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => StoredMerge::from_row(row).map(Some),
        many => Err(FerroError::Corruption(format!(
            "{MERGES_TABLE} holds {} rows for m_{number}; merge numbers are unique by construction",
            many.len()
        ))),
    }
}

/// Every retained merge. Read only when pruning, which is amortised over many merges.
pub(crate) fn read_merges(ctx: &ReadCtx) -> Result<Vec<StoredMerge>, FerroError> {
    if ctx.catalog.get_table(MERGES_TABLE).is_none() {
        return Ok(Vec::new());
    }
    scan_table(MERGES_TABLE, ctx)?.iter().map(|r| StoredMerge::from_row(r)).collect()
}

// ---- the log -------------------------------------------------------------------------------------

/// The ops one publish applied, in sequence order, each with the row's author before it (D226).
pub(crate) const LOG_APPLIED: i32 = 1;
/// One task's retained reads and valued writes, as of one publish. A later record for the same txn
/// supersedes it: a live ancestor keeps reading after a descendant publishes its staged rows.
pub(crate) const LOG_CAPTURE: i32 = 2;
/// The txns one REVERT inverted, and the merge whose REVERT it was.
pub(crate) const LOG_REVERTED: i32 = 3;

/// One logical record of [`LOG_TABLE`], before it is chunked into rows or after it is reassembled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogRecord {
    pub(crate) kind: i32,
    /// The txn the record is about; 0 for a `REVERTED` record, which names several.
    pub(crate) txn: u64,
    pub(crate) body: Vec<u8>,
}

fn log_schema() -> Schema {
    Schema::new(vec![
        Column::new("k".to_string(), DataType::BigInt, false),
        Column::new("rec".to_string(), DataType::BigInt, false),
        Column::new("kind".to_string(), DataType::Integer, false),
        Column::new("txn".to_string(), DataType::BigInt, false),
        Column::new("last".to_string(), DataType::Boolean, false),
        Column::new("body".to_string(), DataType::Varchar(CHUNK_HEX as u16), false),
    ])
}

/// The rows that store `records`, keyed from `log_high + 1`, and the new high key.
///
/// Every chunk carries its record's first key (`rec`) and whether it is the record's last, so a
/// reader can reassemble in key order and refuse a record that is missing a piece.
pub(crate) fn log_rows(
    log_high: u64,
    records: &[LogRecord],
) -> Result<(Vec<Vec<Value>>, u64), FerroError> {
    let mut rows = Vec::new();
    let mut key = log_high;
    for r in records {
        let hex = to_hex(&r.body);
        let rec = key + 1;
        let mut start = 0usize;
        loop {
            let end = (start + CHUNK_HEX).min(hex.len());
            key += 1;
            let last = end == hex.len();
            rows.push(vec![
                big(key, "log key")?,
                big(rec, "log record")?,
                Value::Integer(r.kind),
                big(r.txn, "log txn")?,
                Value::Boolean(last),
                Value::Varchar(hex[start..end].to_string()),
            ]);
            if last {
                break;
            }
            start = end;
        }
    }
    Ok((rows, key))
}

/// Reassemble rows of [`LOG_TABLE`], in any order, into records in key order.
///
/// Refuses a chunk whose record's first chunk is not in the set, a gap inside a record, a record
/// with no last chunk, and a chunk that disagrees with its record about kind or txn. The set is
/// always read from a record's first key upward, and pruning always cuts at one, so a partial
/// record can only mean the table is not what this build wrote.
pub(crate) fn records_from_rows(rows: Vec<Vec<Value>>) -> Result<Vec<LogRecord>, FerroError> {
    struct Chunk {
        key: u64,
        rec: u64,
        kind: i32,
        txn: u64,
        last: bool,
        hex: String,
    }
    let mut chunks: Vec<Chunk> = Vec::with_capacity(rows.len());
    for row in rows {
        let [k, rec, Value::Integer(kind), txn, Value::Boolean(last), Value::Varchar(hex)] =
            row.as_slice()
        else {
            return Err(corrupt(format!("a row this build did not write: {row:?}")));
        };
        chunks.push(Chunk {
            key: unbig(k, "log key")?,
            rec: unbig(rec, "log record")?,
            kind: *kind,
            txn: unbig(txn, "log txn")?,
            last: *last,
            hex: hex.clone(),
        });
    }
    chunks.sort_by_key(|c| c.key);
    let mut out = Vec::new();
    let mut open: Option<(Chunk, String)> = None;
    for c in chunks {
        match open.take() {
            None => {
                if c.rec != c.key {
                    return Err(corrupt(format!(
                        "row {} continues record {} whose first chunk is not here",
                        c.key, c.rec
                    )));
                }
                if c.last {
                    out.push(LogRecord { kind: c.kind, txn: c.txn, body: from_hex(&c.hex)? });
                } else {
                    let hex = c.hex.clone();
                    open = Some((c, hex));
                }
            }
            Some((prev, mut hex)) => {
                if c.rec != prev.rec || c.key != prev.key + 1 || c.kind != prev.kind || c.txn != prev.txn
                {
                    return Err(corrupt(format!(
                        "record {} is broken at row {}: a chunk is missing or belongs elsewhere",
                        prev.rec, c.key
                    )));
                }
                hex.push_str(&c.hex);
                if c.last {
                    out.push(LogRecord { kind: c.kind, txn: c.txn, body: from_hex(&hex)? });
                } else {
                    open = Some((c, hex));
                }
            }
        }
    }
    if let Some((prev, _)) = open {
        return Err(corrupt(format!("record {} has no last chunk", prev.rec)));
    }
    Ok(out)
}

/// Every record from key `from` upward, in key order. `from` must be a record's first key.
pub(crate) fn read_log(ctx: &ReadCtx, from: u64) -> Result<Vec<LogRecord>, FerroError> {
    if ctx.catalog.get_table(LOG_TABLE).is_none() {
        return Ok(Vec::new());
    }
    let pred = compare("k", TokenType::GreaterEqual, from);
    records_from_rows(scan_table_where(LOG_TABLE, None, Some(&pred), ctx)?)
}

/// What a REVERT of a pre-restart merge needs, read from that merge forward.
#[derive(Debug, Default)]
pub(crate) struct DurableHistory {
    /// Published ops per txn, each with the row's author before its merge.
    pub(crate) applied: BTreeMap<u64, Vec<(AppliedOp, ProvId)>>,
    /// The latest capture per txn.
    pub(crate) captures: BTreeMap<u64, TxnProvenance>,
    /// Txns a REVERT inverted, and the merge whose REVERT it was.
    pub(crate) reverted: BTreeMap<u64, String>,
}

pub(crate) fn load_history(ctx: &ReadCtx, from: u64) -> Result<DurableHistory, FerroError> {
    let mut h = DurableHistory::default();
    for r in read_log(ctx, from)? {
        match r.kind {
            LOG_APPLIED => {
                let ops = decode_applied(&r.body)?;
                h.applied.entry(r.txn).or_default().extend(ops);
            }
            LOG_CAPTURE => {
                let p = decode_capture(&r.body)?;
                if p.txn.0 != r.txn {
                    return Err(corrupt(format!(
                        "a capture filed under txn {} decodes as {}",
                        r.txn, p.txn
                    )));
                }
                h.captures.insert(r.txn, p);
            }
            LOG_REVERTED => {
                let (by, txns) = decode_reverted(&r.body)?;
                for t in txns {
                    h.reverted.insert(t.0, by.clone());
                }
            }
            other => return Err(corrupt(format!("a record of kind {other}, which no build writes"))),
        }
    }
    Ok(h)
}

// ---- record bodies -------------------------------------------------------------------------------
//
// Values, ops and branch ids go through the effect log's codec (`tel::log`), which is exhaustive
// over `Value` and `OpKind` and refuses trailing bytes; strings through `wal::log::write_str`, which
// refuses what a length prefix cannot hold. This file adds only the framing of its own records.

pub(crate) fn applied_record(txn: TxnId, ops: &[(AppliedOp, ProvId)]) -> Result<LogRecord, FerroError> {
    let mut b = Vec::new();
    put_count(&mut b, ops.len(), "applied ops")?;
    for (a, prior) in ops {
        b.extend_from_slice(&a.seq.to_be_bytes());
        b.extend_from_slice(&a.txn.0.to_be_bytes());
        write_str(&mut b, &a.table, "table name")?;
        // The op's cell and kind, with its before-value in the witness slot: the effect log's
        // own encoding of exactly this shape.
        put_op(
            &mut b,
            &Op { tbl: a.tbl, row: a.row, col: a.col, kind: a.kind.clone(), witness: a.before.clone() },
        )?;
        match &a.before_row {
            None => b.push(0),
            Some(row) => {
                b.push(1);
                put_values(&mut b, row)?;
            }
        }
        b.extend_from_slice(&prior.0.to_be_bytes());
    }
    Ok(LogRecord { kind: LOG_APPLIED, txn: txn.0, body: b })
}

pub(crate) fn decode_applied(body: &[u8]) -> Result<Vec<(AppliedOp, ProvId)>, FerroError> {
    let mut at = 0usize;
    // seq(8) + txn(8) + table(2) + op(15) + before_row tag(1) + prior(4)
    let n = take_count(body, &mut at, 38, "applied ops")?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let seq = take_u64(body, &mut at)?;
        let txn = TxnId(take_u64(body, &mut at)?);
        let table = take_str(body, &mut at)?;
        let op = take_op(body, &mut at)?;
        let before_row = match take_u8(body, &mut at)? {
            0 => None,
            1 => Some(take_values(body, &mut at)?),
            other => return Err(corrupt(format!("a before-row with presence tag {other}"))),
        };
        let prior = ProvId(take_u32(body, &mut at)?);
        out.push((
            AppliedOp {
                seq,
                txn,
                table,
                tbl: op.tbl,
                row: op.row,
                col: op.col,
                kind: op.kind,
                before: op.witness,
                before_row,
            },
            prior,
        ));
    }
    end(body, at, "applied ops")?;
    Ok(out)
}

/// A task's capture as the dependency graph reads it: exact versions read, predicate reads with
/// the snapshot each read at, and valued writes.
///
/// **Exactly what `ProvenanceLog::dependency_graph` reads, and no more.** A reloaded capture's
/// `read_sets` holds its exact versions only: the `Predicate` entries there are the same summaries
/// as `predicate_reads` without their timestamps, and `record_read_sets` ignores them. Storing them
/// twice would double the read half's bytes to carry nothing the graph uses.
pub(crate) fn capture_record(p: &TxnProvenance) -> Result<LogRecord, FerroError> {
    let mut b = Vec::new();
    b.extend_from_slice(&p.txn.0.to_be_bytes());
    b.extend_from_slice(&p.prov.0.to_be_bytes());
    put_branch(&mut b, p.branch);
    let exact: Vec<&VersionRef> = p
        .read_sets
        .iter()
        .flat_map(|rs| match rs {
            ReadSet::ExactVersions(vs) => vs.iter().collect::<Vec<_>>(),
            ReadSet::Predicate(_) => Vec::new(),
        })
        .collect();
    put_count(&mut b, exact.len(), "exact reads")?;
    for v in exact {
        put_version(&mut b, v);
    }
    put_count(&mut b, p.predicate_reads.len(), "predicate reads")?;
    for t in &p.predicate_reads {
        put_summary(&mut b, &t.summary)?;
        b.extend_from_slice(&t.observed_at.to_be_bytes());
    }
    put_count(&mut b, p.writes.len(), "writes")?;
    for w in &p.writes {
        put_version(&mut b, &w.version);
        put_col(&mut b, w.col);
        put_opt_value(&mut b, &w.value)?;
    }
    Ok(LogRecord { kind: LOG_CAPTURE, txn: p.txn.0, body: b })
}

pub(crate) fn decode_capture(body: &[u8]) -> Result<TxnProvenance, FerroError> {
    let mut at = 0usize;
    let txn = TxnId(take_u64(body, &mut at)?);
    let prov = ProvId(take_u32(body, &mut at)?);
    let branch = take_branch(body, &mut at)?;
    let n = take_count(body, &mut at, VERSION_BYTES, "exact reads")?;
    let mut exact = Vec::with_capacity(n);
    for _ in 0..n {
        exact.push(take_version(body, &mut at)?);
    }
    let n = take_count(body, &mut at, SUMMARY_MIN + 8, "predicate reads")?;
    let mut predicate_reads = Vec::with_capacity(n);
    for _ in 0..n {
        let summary = take_summary(body, &mut at)?;
        let observed_at = take_u64(body, &mut at)?;
        predicate_reads.push(TimedPredicate { summary, observed_at });
    }
    let n = take_count(body, &mut at, VERSION_BYTES + 2, "writes")?;
    let mut writes = Vec::with_capacity(n);
    for _ in 0..n {
        let version = take_version(body, &mut at)?;
        let col = take_col(body, &mut at)?;
        let value = take_opt_value(body, &mut at)?;
        writes.push(WriteRecord::new(version, col, value));
    }
    end(body, at, "capture")?;
    let read_sets =
        if exact.is_empty() { Vec::new() } else { vec![ReadSet::ExactVersions(exact)] };
    Ok(TxnProvenance { txn, prov, branch, read_sets, predicate_reads, writes })
}

pub(crate) fn reverted_record(by: &str, txns: &[TxnId]) -> Result<LogRecord, FerroError> {
    let mut b = Vec::new();
    write_str(&mut b, by, "the reverting merge")?;
    put_count(&mut b, txns.len(), "reverted txns")?;
    for t in txns {
        b.extend_from_slice(&t.0.to_be_bytes());
    }
    Ok(LogRecord { kind: LOG_REVERTED, txn: 0, body: b })
}

pub(crate) fn decode_reverted(body: &[u8]) -> Result<(String, Vec<TxnId>), FerroError> {
    let mut at = 0usize;
    let by = take_str(body, &mut at)?;
    let n = take_count(body, &mut at, 8, "reverted txns")?;
    let mut txns = Vec::with_capacity(n);
    for _ in 0..n {
        txns.push(TxnId(take_u64(body, &mut at)?));
    }
    end(body, at, "revert marker")?;
    Ok((by, txns))
}

/// `tbl(4) | row(8) | page(4) | slot(2) | begin_ts(8)`.
const VERSION_BYTES: usize = 26;
/// The smallest summary: `tbl(4) | col-absent(1) | lo(1) | hi(1) | residual-absent(1) | rows(8)`.
const SUMMARY_MIN: usize = 16;

fn put_version(out: &mut Vec<u8>, v: &VersionRef) {
    out.extend_from_slice(&v.tbl.0.to_be_bytes());
    out.extend_from_slice(&v.row.0.to_be_bytes());
    out.extend_from_slice(&v.rid.page_id.to_be_bytes());
    out.extend_from_slice(&v.rid.slot_num.to_be_bytes());
    out.extend_from_slice(&v.begin_ts.to_be_bytes());
}

fn take_version(body: &[u8], at: &mut usize) -> Result<VersionRef, FerroError> {
    let tbl = TableId(take_u32(body, at)?);
    let row = RowId(take_u64(body, at)?);
    let page_id = take_u32(body, at)?;
    let slot_num = take_u16(body, at)?;
    let begin_ts = take_u64(body, at)?;
    Ok(VersionRef { tbl, row, rid: RecordId { page_id, slot_num }, begin_ts })
}

fn put_col(out: &mut Vec<u8>, col: Option<ColId>) {
    match col {
        None => out.push(0),
        Some(c) => {
            out.push(1);
            out.extend_from_slice(&c.0.to_be_bytes());
        }
    }
}

fn take_col(body: &[u8], at: &mut usize) -> Result<Option<ColId>, FerroError> {
    match take_u8(body, at)? {
        0 => Ok(None),
        1 => Ok(Some(ColId(take_u32(body, at)?))),
        other => Err(corrupt(format!("a column with presence tag {other}"))),
    }
}

fn put_bound(out: &mut Vec<u8>, b: &Bound) -> Result<(), FerroError> {
    match b {
        Bound::Unbounded => out.push(0),
        Bound::Included(v) => {
            out.push(1);
            put_value(out, v)?;
        }
        Bound::Excluded(v) => {
            out.push(2);
            put_value(out, v)?;
        }
    }
    Ok(())
}

fn take_bound(body: &[u8], at: &mut usize) -> Result<Bound, FerroError> {
    Ok(match take_u8(body, at)? {
        0 => Bound::Unbounded,
        1 => Bound::Included(take_value(body, at)?),
        2 => Bound::Excluded(take_value(body, at)?),
        other => return Err(corrupt(format!("a bound with tag {other}"))),
    })
}

fn put_summary(out: &mut Vec<u8>, s: &PredicateSummary) -> Result<(), FerroError> {
    out.extend_from_slice(&s.tbl.0.to_be_bytes());
    put_col(out, s.col);
    put_bound(out, &s.lo)?;
    put_bound(out, &s.hi)?;
    match &s.residual {
        None => out.push(0),
        Some(r) => {
            out.push(1);
            // A WHERE clause longer than a length prefix holds is refused here, and with it the
            // publish: a capture that silently dropped it would be a smaller region than the truth.
            write_str(out, r, "a scan's WHERE text")?;
        }
    }
    out.extend_from_slice(&s.rows_observed.to_be_bytes());
    Ok(())
}

fn take_summary(body: &[u8], at: &mut usize) -> Result<PredicateSummary, FerroError> {
    let tbl = TableId(take_u32(body, at)?);
    let col = take_col(body, at)?;
    let lo = take_bound(body, at)?;
    let hi = take_bound(body, at)?;
    let residual = match take_u8(body, at)? {
        0 => None,
        1 => Some(take_str(body, at)?),
        other => return Err(corrupt(format!("a residual with presence tag {other}"))),
    };
    let rows_observed = take_u64(body, at)?;
    Ok(PredicateSummary { tbl, col, lo, hi, residual, rows_observed })
}

fn put_values(out: &mut Vec<u8>, vals: &[Value]) -> Result<(), FerroError> {
    put_count(out, vals.len(), "values in a row image")?;
    for v in vals {
        put_value(out, v)?;
    }
    Ok(())
}

fn take_values(body: &[u8], at: &mut usize) -> Result<Vec<Value>, FerroError> {
    let n = take_count(body, at, 1, "values in a row image")?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(take_value(body, at)?);
    }
    Ok(out)
}

fn put_count(out: &mut Vec<u8>, n: usize, what: &str) -> Result<(), FerroError> {
    let n = u32::try_from(n)
        .map_err(|_| corrupt(format!("{n} {what} is more than a u32 count can express")))?;
    out.extend_from_slice(&n.to_be_bytes());
    Ok(())
}

/// A count, refused when the rest of the body is too short to hold that many items of the given
/// minimum width — so four bad bytes cannot become a request to allocate.
fn take_count(body: &[u8], at: &mut usize, min_item: usize, what: &str) -> Result<usize, FerroError> {
    let n = take_u32(body, at)? as usize;
    let left = body.len().saturating_sub(*at);
    if n > left / min_item.max(1) {
        return Err(corrupt(format!(
            "a record claims {n} {what}, but only {left} bytes remain"
        )));
    }
    Ok(n)
}

/// Trailing bytes are refused: a decoder that stops early accepts two encodings of one record.
fn end(body: &[u8], at: usize, what: &str) -> Result<(), FerroError> {
    if at != body.len() {
        return Err(corrupt(format!(
            "a {what} record has {} byte(s) after its last field",
            body.len() - at
        )));
    }
    Ok(())
}

fn corrupt(msg: String) -> FerroError {
    FerroError::Corruption(format!("{LOG_TABLE}: {msg}"))
}

fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    s
}

fn from_hex(s: &str) -> Result<Vec<u8>, FerroError> {
    let b = s.as_bytes();
    if b.len() % 2 != 0 {
        return Err(corrupt(format!("a record body of {} hex digits, an odd number", b.len())));
    }
    let nibble = |c: u8| -> Result<u8, FerroError> {
        match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            _ => Err(corrupt(format!("byte {c} in a record body is not a lowercase hex digit"))),
        }
    };
    let mut out = Vec::with_capacity(b.len() / 2);
    for pair in b.chunks(2) {
        out.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    Ok(out)
}

/// `WHERE <column> <op> <n>`, as the syntax tree a statement would bind — never as text.
fn compare(column: &str, operator: TokenType, n: u64) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::ColumnRef { table: None, column: column.to_string() }),
        operator,
        right: Box::new(Expr::Literal { value_type: TokenType::Number, value: n.to_string() }),
    }
}

/// A predicate a `DELETE` of everything below `n` in `column` carries.
pub(crate) fn below(column: &str, n: u64) -> Expr {
    compare(column, TokenType::Less, n)
}

/// A counter as a `BIGINT` cell. Refused above `i64::MAX` rather than wrapped: a wrapped counter
/// reads back smaller, and a smaller merge ceiling is a re-issued id.
fn big(n: u64, what: &str) -> Result<Value, FerroError> {
    i64::try_from(n).map(Value::BigInt).map_err(|_| {
        FerroError::Internal(format!(
            "the REVERT history's {what} = {n} does not fit a BIGINT; refusing to store it wrapped"
        ))
    })
}

fn unbig(v: &Value, what: &str) -> Result<u64, FerroError> {
    match v {
        Value::BigInt(n) if *n >= 0 => Ok(*n as u64),
        other => Err(FerroError::Corruption(format!(
            "the REVERT history's {what} holds {other:?}, which is not a non-negative BIGINT"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tel::op::{Delta, OpKind};

    /// The row round-trips, every field in its own column. Distinct values per field, so a
    /// transposition between two columns cannot read back equal.
    #[test]
    fn the_meta_row_round_trips_field_by_field() {
        let m = Meta {
            merge_ceiling: 64,
            txn_high: 7,
            apply_seq: 11,
            log_high: 13,
            retained_from: 3,
            retained_count: 5,
        };
        let row = m.to_row().unwrap();
        assert_eq!(row.len(), Meta::schema().columns.len());
        assert_eq!(Meta::from_row(&row).unwrap(), m);
    }

    #[test]
    fn a_row_this_build_did_not_write_is_refused() {
        let mut row = Meta::default().to_row().unwrap();
        row.pop();
        assert!(Meta::from_row(&row).is_err(), "a short row was read as counters");
        let mut row = Meta::default().to_row().unwrap();
        row[1] = Value::BigInt(-1);
        assert!(Meta::from_row(&row).is_err(), "a negative ceiling was read as a counter");
        assert!(big(u64::MAX, "merge_ceiling").is_err(), "a counter past i64::MAX was wrapped");
    }

    fn vref(row: u64, ts: u64) -> VersionRef {
        VersionRef { tbl: TableId(7), row: RowId(row), rid: RecordId { page_id: 3, slot_num: 9 }, begin_ts: ts }
    }

    fn op(seq: u64, kind: OpKind, before_row: Option<Vec<Value>>) -> AppliedOp {
        AppliedOp {
            seq,
            txn: TxnId(4),
            table: "inventory".into(),
            tbl: TableId(7),
            row: RowId(u64::MAX - seq),
            col: Some(ColId(1)),
            kind,
            before: Some(Value::Integer(20)),
            before_row,
        }
    }

    /// Every field of an applied op, and its prior author, survive the record. A `RowId` above
    /// `i64::MAX` is in the fixture on purpose: non-integer keys hash to the full width.
    #[test]
    fn an_applied_record_round_trips_every_op_and_its_prior_author() {
        let ops = vec![
            (op(5, OpKind::Add(Delta::Int(-3)), Some(vec![Value::Integer(1), Value::Integer(20)])), ProvId(2)),
            (op(6, OpKind::Assign(Value::Varchar("x".into())), None), ProvId::NONE),
        ];
        let rec = applied_record(TxnId(4), &ops).unwrap();
        assert_eq!(rec.kind, LOG_APPLIED);
        let back = decode_applied(&rec.body).unwrap();
        assert_eq!(back.len(), 2);
        for ((a, pa), (b, pb)) in ops.iter().zip(back.iter()) {
            assert_eq!(pa, pb);
            assert_eq!(
                (a.seq, a.txn, &a.table, a.tbl, a.row, a.col, &a.kind, &a.before, &a.before_row),
                (b.seq, b.txn, &b.table, b.tbl, b.row, b.col, &b.kind, &b.before, &b.before_row)
            );
        }
    }

    /// A capture keeps what the dependency graph reads: exact versions, timed predicates with their
    /// bounds and residual, and valued writes.
    #[test]
    fn a_capture_record_round_trips_what_the_graph_reads() {
        let p = TxnProvenance {
            txn: TxnId(9),
            prov: ProvId(3),
            branch: BranchId::new(5, 2),
            read_sets: vec![ReadSet::ExactVersions(vec![vref(1, 4), vref(2, 6)])],
            predicate_reads: vec![TimedPredicate {
                summary: PredicateSummary {
                    tbl: TableId(7),
                    col: Some(ColId(1)),
                    lo: Bound::Included(Value::Integer(20)),
                    hi: Bound::Excluded(Value::Integer(50)),
                    residual: Some("qty >= 20 AND qty < 50".into()),
                    rows_observed: 2,
                },
                observed_at: 11,
            }],
            writes: vec![
                WriteRecord::new(vref(1, 12), Some(ColId(1)), Some(Value::Integer(31))),
                WriteRecord::new(vref(2, 13), None, None),
            ],
        };
        let rec = capture_record(&p).unwrap();
        assert_eq!((rec.kind, rec.txn), (LOG_CAPTURE, 9));
        assert_eq!(decode_capture(&rec.body).unwrap(), p);
    }

    #[test]
    fn a_revert_marker_round_trips() {
        let rec = reverted_record("m_3", &[TxnId(4), TxnId(2)]).unwrap();
        assert_eq!(decode_reverted(&rec.body).unwrap(), ("m_3".to_string(), vec![TxnId(4), TxnId(2)]));
    }

    /// A record several chunks long, stored among others and read back in scrambled order, comes
    /// back byte for byte. Sized by the constant, so it keeps spanning chunks if the constant moves.
    #[test]
    fn a_record_spanning_chunks_reassembles_in_any_row_order() {
        let long: Vec<u8> = (0..(CHUNK_HEX * 2 + 7)).map(|i| (i % 251) as u8).collect();
        let records = vec![
            LogRecord { kind: LOG_APPLIED, txn: 4, body: vec![1, 2, 3] },
            LogRecord { kind: LOG_CAPTURE, txn: 4, body: long },
            LogRecord { kind: LOG_REVERTED, txn: 0, body: Vec::new() },
        ];
        let (mut rows, high) = log_rows(10, &records).unwrap();
        // 1 + 5 + 1 rows: the long body is 2*(2*CHUNK_HEX + 7) hex digits, which is five chunks.
        assert_eq!(rows.len(), 7, "the long record did not span the chunks it should");
        assert_eq!(high, 17);
        rows.reverse();
        rows.swap(1, 4);
        assert_eq!(records_from_rows(rows).unwrap(), records);
    }

    #[test]
    fn a_record_missing_a_chunk_is_refused() {
        let long: Vec<u8> = vec![7; CHUNK_HEX];
        let (mut rows, _) =
            log_rows(0, &[LogRecord { kind: LOG_CAPTURE, txn: 1, body: long }]).unwrap();
        assert!(rows.len() >= 2, "the fixture must span chunks");
        rows.remove(1);
        assert!(records_from_rows(rows).is_err(), "a record with a hole was reassembled");
    }
}
