//! **D212 (a') — what a REVERT history record MEANS.** The store (`wal::history`) keeps records
//! as opaque bytes; this file is the only thing that writes or reads a body.
//!
//! Design authority: SCALE-DESIGN "D212 (a') DECIDED" and "D212 (a') AMENDED", and the 07:41Z
//! "D212 addendum" for merge ids.
//!
//! # Two kinds of record
//!
//! | kind | written by | carries |
//! |---|---|---|
//! | `PUBLISH` | every publish, inside its transaction | the merge id, its txn and branch; the applied ops, each with the author its row had before (D226); one capture per txn the publish made published (the merging task's own and every inherited ancestor's); and the counters as they stand after it (C2) |
//! | `REVERTED` | every REVERT, inside its transaction | the reverting merge id and the txns whose ops it inverted (D218) |
//!
//! **C2 — the counters travel in every publish record.** `txn_high` and `apply_seq` are carried
//! forward, so the newest retained publish holds both, and the window always keeps the newest. There
//! is no mutable counters row: under C2 the history is insert-only, which is what lets retention be
//! physical (`wal::history`).
//!
//! **C1 — `versions` is not persisted.** At open it is derived from the window's applied ops: the
//! newest merge that wrote a row is inside the window whenever any in-window merge wrote it, so the
//! derived map equals the true one for every row an in-window merge wrote; a row outside it reads as
//! `begin_ts 0`, and a REVERT cannot target anything outside the window
//! (`frontier/d212_storage_design.md` §2).
//!
//! # Merge ids: `m_<nonce>_<n>`
//!
//! A merge id carries a random 64-bit nonce drawn when the runtime is built, plus a counter within
//! that run. Ids cannot repeat across restarts, upgrades, backup restores or standby promotions, and
//! "issued by an earlier server run" is read from the id itself — its nonce is not this run's — not
//! from a ceiling a restore can roll back. The ceiling Step 0 kept is gone with its table.

use std::collections::BTreeMap;

use crate::agent_sql::runtime::AppliedOp;
use crate::branch::types::BranchId;
use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::provenance::capture::{TimedPredicate, TxnProvenance, WriteRecord};
use crate::provenance::readset::{Bound, PredicateSummary, ReadSet, VersionRef};
use crate::provenance::ProvId;
use crate::storage::heap_file_manager::RecordId;
use crate::tel::ids::{ColId, RowId, TableId, TxnId};
use crate::tel::log::{
    put_branch, put_op, put_opt_value, put_value, take_branch, take_op, take_opt_value, take_value,
};
use crate::tel::op::Op;
use crate::wal::history::HistoryRecord;
use crate::wal::log::{take_str, take_u16, take_u32, take_u64, take_u8, write_str};

// ---- merge ids -------------------------------------------------------------------------------

/// `m_<nonce as 16 lowercase hex digits>_<n>`.
pub(crate) fn format_merge_id(nonce: u64, n: u64) -> String {
    format!("m_{nonce:016x}_{n}")
}

/// What a string given to `REVERT MERGE` is, by its shape alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeIdForm {
    /// `m_<nonce>_<n>`: minted by the run whose nonce it carries.
    Nonced { nonce: u64, n: u64 },
    /// `m_<n>`: minted by a build before the addendum; nothing can say which merge it named now.
    OldFormat(u64),
    /// Neither.
    Other,
}

pub(crate) fn parse_merge_id(id: &str) -> MergeIdForm {
    let Some(rest) = id.strip_prefix("m_") else { return MergeIdForm::Other };
    if let Some((nonce, n)) = rest.split_once('_') {
        let hex = nonce.len() == 16
            && nonce.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        return match (hex, u64::from_str_radix(nonce, 16), n.parse::<u64>()) {
            (true, Ok(nonce), Ok(n)) if n > 0 => MergeIdForm::Nonced { nonce, n },
            _ => MergeIdForm::Other,
        };
    }
    match rest.parse::<u64>() {
        Ok(n) if n > 0 => MergeIdForm::OldFormat(n),
        _ => MergeIdForm::Other,
    }
}

// ---- records ---------------------------------------------------------------------------------

const KIND_PUBLISH: u8 = 1;
const KIND_REVERTED: u8 = 2;

/// Everything one publish files.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PublishRecord {
    pub(crate) merge_id: String,
    /// The merging task's txn: the one `REVERT MERGE <id>` targets.
    pub(crate) txn: TxnId,
    pub(crate) branch: BranchId,
    /// C2: the highest txn id any record up to and including this one names.
    pub(crate) txn_high: u64,
    /// C2: the version clock once this publish's stamps are handed out.
    pub(crate) apply_seq: u64,
    /// The ops, each with the author its row had before this merge (D226).
    pub(crate) ops: Vec<(AppliedOp, ProvId)>,
    pub(crate) captures: Vec<TxnProvenance>,
}

/// A decoded record.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HistoryEntry {
    Publish(PublishRecord),
    /// `by` inverted the ops of `txns`.
    Reverted { by: String, txns: Vec<TxnId> },
}

pub(crate) fn encode_publish(p: &PublishRecord) -> Result<Vec<u8>, FerroError> {
    let mut b = vec![KIND_PUBLISH];
    write_str(&mut b, &p.merge_id, "a merge id")?;
    b.extend_from_slice(&p.txn.0.to_be_bytes());
    put_branch(&mut b, p.branch);
    b.extend_from_slice(&p.txn_high.to_be_bytes());
    b.extend_from_slice(&p.apply_seq.to_be_bytes());
    put_part(&mut b, &applied_body(&p.ops)?, "applied ops")?;
    put_count(&mut b, p.captures.len(), "captures")?;
    for c in &p.captures {
        put_part(&mut b, &capture_body(c)?, "a capture")?;
    }
    Ok(b)
}

pub(crate) fn encode_reverted(by: &str, txns: &[TxnId]) -> Result<Vec<u8>, FerroError> {
    let mut b = vec![KIND_REVERTED];
    write_str(&mut b, by, "the reverting merge")?;
    put_count(&mut b, txns.len(), "reverted txns")?;
    for t in txns {
        b.extend_from_slice(&t.0.to_be_bytes());
    }
    Ok(b)
}

/// Every refusal a publish record's encoding can make, made WITHOUT the publish's own valued writes:
/// a capture's bound or WHERE text over a length prefix. Called before the branch's schema edits
/// are applied, so the refusal leaves nothing written (E82).
pub(crate) fn check_encodable(captures: &[TxnProvenance]) -> Result<(), FerroError> {
    for c in captures {
        capture_body(c)?;
    }
    Ok(())
}

pub(crate) fn decode_entry(body: &[u8]) -> Result<HistoryEntry, FerroError> {
    let mut at = 0usize;
    match take_u8(body, &mut at)? {
        KIND_PUBLISH => {
            let merge_id = take_str(body, &mut at)?;
            let txn = TxnId(take_u64(body, &mut at)?);
            let branch = take_branch(body, &mut at)?;
            let txn_high = take_u64(body, &mut at)?;
            let apply_seq = take_u64(body, &mut at)?;
            let ops = decode_applied(take_part(body, &mut at)?)?;
            let n = take_count(body, &mut at, 4, "captures")?;
            let mut captures = Vec::with_capacity(n);
            for _ in 0..n {
                captures.push(decode_capture(take_part(body, &mut at)?)?);
            }
            end(body, at, "publish")?;
            Ok(HistoryEntry::Publish(PublishRecord {
                merge_id,
                txn,
                branch,
                txn_high,
                apply_seq,
                ops,
                captures,
            }))
        }
        KIND_REVERTED => {
            let by = take_str(body, &mut at)?;
            let n = take_count(body, &mut at, 8, "reverted txns")?;
            let mut txns = Vec::with_capacity(n);
            for _ in 0..n {
                txns.push(TxnId(take_u64(body, &mut at)?));
            }
            end(body, at, "revert marker")?;
            Ok(HistoryEntry::Reverted { by, txns })
        }
        other => Err(corrupt(format!("a record of kind {other}, which no build writes"))),
    }
}

// ---- what a runtime reads back -----------------------------------------------------------------

/// **What a runtime takes from the window when it attaches** — C2's counters and C1's `versions`.
#[derive(Debug, Default)]
pub(crate) struct AttachView {
    /// The highest txn id the window names: every txn this runtime mints is above it.
    pub(crate) txn_high: u64,
    /// The version clock the window last reached.
    pub(crate) apply_seq: u64,
    /// The highest publish ordinal held.
    pub(crate) last_ordinal: u64,
    /// `(table, row) -> the newest begin_ts an in-window merge gave it` (C1).
    pub(crate) versions: BTreeMap<(u32, u64), u64>,
}

pub(crate) fn attach_view(records: &[HistoryRecord]) -> Result<AttachView, FerroError> {
    let mut v = AttachView::default();
    for r in records {
        if let HistoryEntry::Publish(p) = decode_entry(&r.body)? {
            v.txn_high = v.txn_high.max(p.txn_high);
            v.apply_seq = v.apply_seq.max(p.apply_seq);
            v.last_ordinal = v.last_ordinal.max(r.ordinal);
            for (op, _) in &p.ops {
                let at = v.versions.entry((op.tbl.0, op.row.0)).or_insert(0);
                *at = (*at).max(op.seq);
            }
        }
    }
    Ok(v)
}

/// A publish looked up by its full id: `(hseq, ordinal, record)`.
pub(crate) fn find_publish(
    records: &[HistoryRecord],
    merge_id: &str,
) -> Result<Option<(u64, u64, PublishRecord)>, FerroError> {
    for r in records.iter().filter(|r| r.ordinal > 0) {
        if let HistoryEntry::Publish(p) = decode_entry(&r.body)? {
            if p.merge_id == merge_id {
                return Ok(Some((r.hseq, r.ordinal, p)));
            }
        }
    }
    Ok(None)
}

/// **What a REVERT of an earlier run's merge reads**, from that merge's record forward: every
/// dependent of it was published after it, so nothing before it can be one.
#[derive(Debug, Default)]
pub(crate) struct DurableHistory {
    /// txn -> its ops and each op's prior author.
    pub(crate) applied: BTreeMap<u64, Vec<(AppliedOp, ProvId)>>,
    /// txn -> its capture; a later record's capture of the same txn supersedes an earlier one.
    pub(crate) captures: BTreeMap<u64, TxnProvenance>,
    /// txn -> the REVERT that inverted its ops.
    pub(crate) reverted: BTreeMap<u64, String>,
}

pub(crate) fn history_from(
    records: &[HistoryRecord],
    from_hseq: u64,
) -> Result<DurableHistory, FerroError> {
    let mut h = DurableHistory::default();
    for r in records.iter().filter(|r| r.hseq >= from_hseq) {
        match decode_entry(&r.body)? {
            HistoryEntry::Publish(p) => {
                h.applied.entry(p.txn.0).or_default().extend(p.ops);
                for c in p.captures {
                    h.captures.insert(c.txn.0, c);
                }
            }
            HistoryEntry::Reverted { by, txns } => {
                for t in txns {
                    h.reverted.insert(t.0, by.clone());
                }
            }
        }
    }
    Ok(h)
}

// ---- record bodies -------------------------------------------------------------------------------
//
// Values, ops and branch ids go through the effect log's codec (`tel::log`), which is exhaustive
// over `Value` and `OpKind` and refuses trailing bytes; strings through `wal::log::write_str`, which
// refuses what a length prefix cannot hold. This file adds only the framing of its own records.

fn applied_body(ops: &[(AppliedOp, ProvId)]) -> Result<Vec<u8>, FerroError> {
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
    Ok(b)
}

fn decode_applied(body: &[u8]) -> Result<Vec<(AppliedOp, ProvId)>, FerroError> {
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
fn capture_body(p: &TxnProvenance) -> Result<Vec<u8>, FerroError> {
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
    Ok(b)
}

fn decode_capture(body: &[u8]) -> Result<TxnProvenance, FerroError> {
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

/// A length-prefixed sub-body, so each part is decoded, and checked for trailing bytes, on its own.
fn put_part(out: &mut Vec<u8>, part: &[u8], what: &str) -> Result<(), FerroError> {
    put_count(out, part.len(), what)?;
    out.extend_from_slice(part);
    Ok(())
}

fn take_part<'a>(body: &'a [u8], at: &mut usize) -> Result<&'a [u8], FerroError> {
    let n = take_u32(body, at)? as usize;
    let part = at
        .checked_add(n)
        .and_then(|end| body.get(*at..end))
        .ok_or_else(|| corrupt(format!("a part of {n} bytes runs past the record")))?;
    *at += n;
    Ok(part)
}

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
    FerroError::Corruption(format!("REVERT history record: {msg}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tel::op::{Delta, OpKind};

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

    fn capture() -> TxnProvenance {
        TxnProvenance {
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
        }
    }

    /// Every field of a publish survives the record: the header, each applied op with its prior
    /// author, and a capture's exact reads, timed predicates and valued writes. A `RowId` above
    /// `i64::MAX` is in the fixture on purpose: non-integer keys hash to the full width.
    #[test]
    fn a_publish_record_round_trips_every_field() {
        let p = PublishRecord {
            merge_id: format_merge_id(0xfeed_0000_0000_beef, 3),
            txn: TxnId(4),
            branch: BranchId::new(8, 1),
            txn_high: 9,
            apply_seq: 6,
            ops: vec![
                (op(5, OpKind::Add(Delta::Int(-3)), Some(vec![Value::Integer(1), Value::Integer(20)])), ProvId(2)),
                (op(6, OpKind::Assign(Value::Varchar("x".into())), None), ProvId::NONE),
            ],
            captures: vec![capture()],
        };
        let body = encode_publish(&p).unwrap();
        match decode_entry(&body).unwrap() {
            HistoryEntry::Publish(back) => {
                assert_eq!(
                    (&back.merge_id, back.txn, back.branch, back.txn_high, back.apply_seq),
                    (&p.merge_id, p.txn, p.branch, p.txn_high, p.apply_seq)
                );
                assert_eq!(back.captures, p.captures);
                assert_eq!(back.ops.len(), 2);
                for ((a, pa), (b, pb)) in p.ops.iter().zip(back.ops.iter()) {
                    assert_eq!(pa, pb);
                    assert_eq!(
                        (a.seq, a.txn, &a.table, a.tbl, a.row, a.col, &a.kind, &a.before, &a.before_row),
                        (b.seq, b.txn, &b.table, b.tbl, b.row, b.col, &b.kind, &b.before, &b.before_row)
                    );
                }
            }
            other => panic!("decoded as {other:?}"),
        }
        let mut long = body.clone();
        long.push(0);
        assert!(decode_entry(&long).is_err(), "a trailing byte was accepted");
    }

    #[test]
    fn a_revert_marker_round_trips() {
        let body = encode_reverted("m_00000000000000aa_3", &[TxnId(4), TxnId(2)]).unwrap();
        assert_eq!(
            decode_entry(&body).unwrap(),
            HistoryEntry::Reverted { by: "m_00000000000000aa_3".into(), txns: vec![TxnId(4), TxnId(2)] }
        );
    }

    /// C1 and C2 read back from a window: the counters are the newest publish's, and each row's
    /// version is the highest seq any in-window op gave it.
    #[test]
    fn the_attach_view_takes_the_counters_and_the_newest_version_per_row() {
        let mut first = op(5, OpKind::Add(Delta::Int(1)), None);
        first.row = RowId(1);
        let mut second = op(8, OpKind::Add(Delta::Int(1)), None);
        second.row = RowId(1);
        let rec = |hseq, ordinal, seq_op: AppliedOp, txn_high, apply_seq| HistoryRecord {
            hseq,
            ordinal,
            commit_lsn: 0,
            body: encode_publish(&PublishRecord {
                merge_id: format_merge_id(1, ordinal),
                txn: TxnId(txn_high),
                branch: BranchId::new(1, 1),
                txn_high,
                apply_seq,
                ops: vec![(seq_op, ProvId::NONE)],
                captures: Vec::new(),
            })
            .unwrap(),
        };
        let marker = HistoryRecord {
            hseq: 3,
            ordinal: 0,
            commit_lsn: 0,
            body: encode_reverted(&format_merge_id(1, 1), &[TxnId(2)]).unwrap(),
        };
        let v = attach_view(&[rec(1, 1, first, 2, 5), rec(2, 2, second, 4, 8), marker]).unwrap();
        assert_eq!((v.txn_high, v.apply_seq, v.last_ordinal), (4, 8, 2));
        assert_eq!(v.versions.get(&(7, 1)), Some(&8));
    }

    #[test]
    fn a_merge_id_is_read_by_its_shape() {
        assert_eq!(
            parse_merge_id(&format_merge_id(0xab, 7)),
            MergeIdForm::Nonced { nonce: 0xab, n: 7 }
        );
        assert_eq!(parse_merge_id("m_12"), MergeIdForm::OldFormat(12));
        assert_eq!(parse_merge_id("m_0"), MergeIdForm::Other);
        assert_eq!(parse_merge_id("m_00000000000000AB_1"), MergeIdForm::Other, "uppercase is not ours");
        assert_eq!(parse_merge_id("m_ab_1"), MergeIdForm::Other, "a short nonce is not ours");
        assert_eq!(parse_merge_id("x_1"), MergeIdForm::Other);
    }
}
