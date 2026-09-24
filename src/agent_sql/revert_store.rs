//! **D212 — the durable substrate `REVERT MERGE` reads.**
//!
//! Design authority: `SCALE-DESIGN.md` "D212 — REVERT metadata survives a restart", and
//! `frontier/d212_design.md` §0 and §3a in `artie-research`.
//!
//! Everything here lives in ordinary heap tables whose names start with
//! [`crate::catalog::catalog::INTERNAL_TABLE_PREFIX`]. That is a decision about atomicity, not
//! convenience: a heap table is the only structure that commits in the SAME WAL transaction as the
//! rows a merge publishes, so a crash can never leave a merge's rows without its history or its
//! history without its rows. The two alternatives `d212_design.md` §3a priced were rejected for
//! that reason — a WAL record kind is re-declared at every checkpoint (Θ(M²) over a database's
//! life), and a sidecar file cannot be ordered against the commit in either direction.
//!
//! # What Step 0 keeps here (D217)
//!
//! One row: the **merge-id ceiling**. Merge ids are minted for every admission attempt — a
//! conflicting or quarantined `MERGE` and a merge into a sibling branch all get one — and the id is
//! handed to the client whether or not anything was published to the shared tables. So a counter persisted only when a merge
//! PUBLISHES cannot stop a restarted server from re-issuing an id some client is still holding. The
//! runtime therefore reserves ids in blocks of [`MERGE_ID_BLOCK`]: before it mints an id above the
//! durable ceiling it commits a new ceiling, and a restarted runtime starts minting above whatever
//! ceiling it finds. Ids never repeat, and an id at or below the ceiling a runtime found when it
//! started is a POSITIVE fact about an earlier server run rather than an absence from a map.
//!
//! The row is declared at full width now — the columns option (a) fills are here from the start —
//! so a database created by this code never needs its internal table altered.

use crate::agent_sql::runtime::{scan_table, ReadCtx};
use crate::catalog::column::{Column, DataType, Value};
use crate::catalog::schema::Schema;
use crate::error::FerroError;

/// The one-row table of counters. See [`Meta`].
pub(crate) const META_TABLE: &str = "ferro:revert:meta";

/// How many merge ids one durable reservation covers.
///
/// The cost trade is one committed transaction per `MERGE_ID_BLOCK` merge attempts, against ids
/// jumping by up to this much across a restart. A jump is harmless — ids are names, not a count —
/// and 64 keeps the reservation to ~1.6% of attempts while keeping post-restart ids short.
pub(crate) const MERGE_ID_BLOCK: u64 = 64;

/// The durable counters, as one row.
///
/// Step 0 reads and writes `merge_ceiling` only. The others are option (a)'s, declared here so the
/// table's shape is fixed from its first commit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Meta {
    /// Every merge id `m_1 ..= m_{merge_ceiling}` has been issued, or reserved, by some run.
    pub(crate) merge_ceiling: u64,
    /// The highest agent txn id any persisted record names.
    pub(crate) txn_high: u64,
    /// The runtime's version clock as of the last publish.
    pub(crate) apply_seq: u64,
    /// The highest key the history log has used.
    pub(crate) log_high: u64,
    /// The smallest merge number whose history is still retained.
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
    vec![(META_TABLE, Meta::schema())]
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

/// A counter as a `BIGINT` cell. Refused above `i64::MAX` rather than wrapped: a wrapped counter
/// reads back smaller, and a smaller merge ceiling is a re-issued id.
fn big(n: u64, what: &str) -> Result<Value, FerroError> {
    i64::try_from(n).map(Value::BigInt).map_err(|_| {
        FerroError::Internal(format!(
            "{META_TABLE}.{what} = {n} does not fit a BIGINT; refusing to store it wrapped"
        ))
    })
}

fn unbig(v: &Value, what: &str) -> Result<u64, FerroError> {
    match v {
        Value::BigInt(n) if *n >= 0 => Ok(*n as u64),
        other => Err(FerroError::Corruption(format!(
            "{META_TABLE}.{what} holds {other:?}, which is not a non-negative BIGINT"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
